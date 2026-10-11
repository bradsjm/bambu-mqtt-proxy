package mcpserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"bambu-mqtt-proxy/internal/control"
	"bambu-mqtt-proxy/internal/module"
)

// ControlService is the allow-listed printer control backend. It is
// satisfied by *control.Service; nil registers no control tools.
type ControlService interface {
	Do(serial, origin string, req control.Request) (control.Result, error)
	Available(serial string) []string
}

// Control tool error codes.
const errUpstreamUnavailable = "upstream_unavailable"

// mcpOrigin names this surface in the activity log.
const mcpOrigin = "MCP"

// Control tool inputs.
type (
	// SerialIn names one printer.
	SerialIn struct {
		// Serial identifies the printer.
		Serial string `json:"serial"`
	}
	// ChamberLightIn switches the chamber light.
	ChamberLightIn struct {
		// Serial identifies the printer.
		Serial string `json:"serial"`
		// On selects the chamber light state.
		On bool `json:"on"`
	}
	// SpeedProfileIn selects a print speed profile.
	SpeedProfileIn struct {
		// Serial identifies the printer.
		Serial string `json:"serial"`
		// Profile selects the speed profile.
		Profile string `json:"profile"`
	}
)

// ControlOut is the shared result of every control tool. State is the
// fresh projection after a successful send; the printer's next report
// confirms the effect.
type ControlOut struct {
	// Serial identifies the printer.
	Serial string `json:"serial"`
	// Action identifies the command.
	Action string `json:"action"`
	// Sent reports whether the command was sent.
	Sent bool `json:"sent"`
	// State contains the current printer state after sending.
	State *PrinterState `json:"state,omitempty"`
	// Error describes an expected operational failure.
	Error *module.ToolError `json:"error,omitempty"`
}

// registerControlTools registers commands with their required inputs.
func (s *Server) registerControlTools() {
	for _, command := range []struct {
		// name identifies the MCP tool.
		name string
		// action identifies the printer command.
		action string
		// description explains the command.
		description string
	}{
		{"pause_print", control.ActionPause, "Pause the print."},
		{"resume_print", control.ActionResume, "Resume the print."},
		{"stop_print", control.ActionStop, "Emergency stop the print immediately."},
	} {
		mcp.AddTool(s.srv, &mcp.Tool{Name: command.name, Description: command.description,
			Annotations: module.Command(command.description, command.action == control.ActionStop, command.action != control.ActionStop),
			InputSchema: module.SerialInput(nil),
		}, func(_ context.Context, _ *mcp.CallToolRequest, in SerialIn) (*mcp.CallToolResult, ControlOut, error) {
			return s.doControl(in.Serial, control.Request{Action: command.action})
		})
	}
	mcp.AddTool(s.srv, &mcp.Tool{Name: "set_chamber_light", Description: "Switch the chamber light on or off.",
		Annotations: module.Command("Set chamber light", false, true),
		InputSchema: module.SerialInput(map[string]*jsonschema.Schema{"on": {Type: "boolean"}}, "on"),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in ChamberLightIn) (*mcp.CallToolResult, ControlOut, error) {
		return s.doControl(in.Serial, control.Request{Action: control.ActionLight, On: new(in.On)})
	})
	mcp.AddTool(s.srv, &mcp.Tool{Name: "set_speed_profile", Description: "Select the print speed profile.",
		Annotations: module.Command("Set speed profile", false, true),
		InputSchema: module.SerialInput(map[string]*jsonschema.Schema{"profile": {Type: "string", Enum: []any{"silent", "standard", "sport", "ludicrous"}}}, "profile"),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in SpeedProfileIn) (*mcp.CallToolResult, ControlOut, error) {
		return s.doControl(in.Serial, control.Request{Action: control.ActionSpeed, Profile: in.Profile})
	})
}

// doControl sends one control request and renders the shared result.
func (s *Server) doControl(serial string, req control.Request) (*mcp.CallToolResult, ControlOut, error) {
	out := ControlOut{Serial: serial, Action: req.Action}
	if _, known := s.printers[serial]; !known {
		out.Error = toolErr(module.CodeUnknownSerial, "serial is not configured: "+serial)
		return errorResult(), out, nil
	}
	if _, err := s.control.Do(serial, mcpOrigin, req); err != nil {
		code := module.CodeCommandFailed
		switch {
		case errors.Is(err, control.ErrUnknownPrinter):
			code = module.CodeUnknownSerial
		case errors.Is(err, control.ErrNotConnected):
			code = errUpstreamUnavailable
		}
		out.Error = toolErr(code, err.Error())
		return errorResult(), out, nil
	}
	return s.controlSent(out)
}

// controlSent completes a successful control result with fresh state.
func (s *Server) controlSent(out ControlOut) (*mcp.CallToolResult, ControlOut, error) {
	out.Sent = true
	if st, ok := s.buildPrinterState(out.Serial); ok {
		out.State = &st
	}
	return shortText(nil, fmt.Sprintf("%s: %s sent", out.Serial, out.Action)), out, nil
}
