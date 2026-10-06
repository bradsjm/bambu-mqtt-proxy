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
const (
	errInvalidState        = "invalid_state"
	errUpstreamUnavailable = "upstream_unavailable"
)

// mcpOrigin names this surface in the activity log.
const mcpOrigin = "MCP"

// Control tool inputs.
type (
	// SerialIn names one printer.
	SerialIn struct {
		Serial string `json:"serial"`
	}
	// ChamberLightIn switches the chamber light.
	ChamberLightIn struct {
		Serial string `json:"serial"`
		On     bool   `json:"on"`
	}
	// SpeedProfileIn selects a print speed profile.
	SpeedProfileIn struct {
		Serial  string `json:"serial"`
		Profile string `json:"profile"`
	}
)

// ControlOut is the shared result of every control tool. State is the
// fresh projection after a successful send; the printer's next report
// confirms the effect.
type ControlOut struct {
	Serial string            `json:"serial"`
	Action string            `json:"action"`
	Sent   bool              `json:"sent"`
	State  *PrinterState     `json:"state,omitempty"`
	Error  *module.ToolError `json:"error,omitempty"`
}

// boolRequiredProp declares a boolean property.
func boolRequiredProp(description string) *jsonschema.Schema {
	return &jsonschema.Schema{Type: "boolean", Description: description}
}

// registerControlTools adds the printer control tools. They exist only
// when a control service is wired.
func (s *Server) registerControlTools() {
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "pause_print",
		Description: "Pause the running print. Available while the printer reports RUNNING.",
		Annotations: module.Command("Pause print", false, true),
		InputSchema: module.SerialInput(nil),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in SerialIn) (*mcp.CallToolResult, ControlOut, error) {
		return s.doControl(in.Serial, control.Request{Action: control.ActionPause})
	})
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "resume_print",
		Description: "Resume a paused print. The printer reheats on its own; no temperature command is sent.",
		Annotations: module.Command("Resume print", false, true),
		InputSchema: module.SerialInput(nil),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in SerialIn) (*mcp.CallToolResult, ControlOut, error) {
		return s.doControl(in.Serial, control.Request{Action: control.ActionResume})
	})
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "stop_print",
		Description: "Emergency stop: immediately cancel the current print. No confirmation.",
		Annotations: module.Command("Stop print", true, false),
		InputSchema: module.SerialInput(nil),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in SerialIn) (*mcp.CallToolResult, ControlOut, error) {
		return s.doControl(in.Serial, control.Request{Action: control.ActionStop})
	})

	lightProps := map[string]*jsonschema.Schema{
		"on": boolRequiredProp("true switches the chamber light on, false switches it off."),
	}
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "set_chamber_light",
		Description: "Switch the chamber light on or off.",
		Annotations: module.Command("Set chamber light", false, true),
		InputSchema: module.SerialInput(lightProps, "on"),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in ChamberLightIn) (*mcp.CallToolResult, ControlOut, error) {
		on := in.On
		return s.doControl(in.Serial, control.Request{Action: control.ActionLight, On: &on})
	})

	profile := enumProp("Print speed profile.", []any{"silent", "standard", "sport", "ludicrous"}, "standard")
	profile.Default = nil
	speedProps := map[string]*jsonschema.Schema{"profile": profile}
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "set_speed_profile",
		Description: "Change the print speed profile of a running or paused print.",
		Annotations: module.Command("Set speed profile", false, true),
		InputSchema: module.SerialInput(speedProps, "profile"),
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
		case errors.Is(err, control.ErrNotAvailable):
			code = errInvalidState
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
	if st, ok := s.buildPrinterState(out.Serial, s.now()); ok {
		out.State = &st
	}
	return shortText(nil, fmt.Sprintf("%s: %s sent", out.Serial, out.Action)), out, nil
}
