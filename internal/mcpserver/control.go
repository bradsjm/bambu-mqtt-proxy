package mcpserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"bambu-mqtt-proxy/internal/control"
	"bambu-mqtt-proxy/internal/detection"
)

// ControlService is the allow-listed printer control backend. It is
// satisfied by *control.Service; nil registers no control tools.
type ControlService interface {
	Do(serial, origin string, req control.Request) (control.Result, error)
	Available(serial string) []string
}

// DetectorControl applies the per-print AI monitoring override. It is
// satisfied by *detection.Engine and stays nil when detection is off.
type DetectorControl interface {
	SetDetectionEnabled(serial string, enabled bool, sessionID string) (any, error)
}

// Control tool error codes.
const (
	errInvalidState        = "invalid_state"
	errUpstreamUnavailable = "upstream_unavailable"
	errCommandFailed       = "command_failed"
	errDetectionDisabled   = "detection_disabled"
	errNoPrintSession      = "no_print_session"
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
	// AIMonitoringIn switches AI monitoring for the current print.
	AIMonitoringIn struct {
		Serial  string `json:"serial"`
		Enabled bool   `json:"enabled"`
	}
)

// ControlOut is the shared result of every control tool. State is the
// fresh projection after a successful send; the printer's next report
// confirms the effect.
type ControlOut struct {
	Serial string        `json:"serial"`
	Action string        `json:"action"`
	Sent   bool          `json:"sent"`
	State  *PrinterState `json:"state,omitempty"`
	Error  *ToolError    `json:"error,omitempty"`
}

// controlAnnotations builds annotations for a tool that changes printer state.
func controlAnnotations(title string, destructive, idempotent bool) *mcp.ToolAnnotations {
	no := false
	return &mcp.ToolAnnotations{
		Title:           title,
		ReadOnlyHint:    false,
		DestructiveHint: &destructive,
		IdempotentHint:  idempotent,
		OpenWorldHint:   &no,
	}
}

// boolRequiredProp declares a boolean property.
func boolRequiredProp(description string) *jsonschema.Schema {
	return &jsonschema.Schema{Type: "boolean", Description: description}
}

// registerControlTools adds the printer control tools. They exist only
// when a control service is wired.
func (s *Server) registerControlTools() {
	serial := func() map[string]*jsonschema.Schema {
		return map[string]*jsonschema.Schema{"serial": strRequiredProp("Printer serial number.")}
	}
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "pause_print",
		Description: "Pause the running print. Available while the printer reports RUNNING.",
		Annotations: controlAnnotations("Pause print", false, true),
		InputSchema: objSchema(serial(), "serial"),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in SerialIn) (*mcp.CallToolResult, ControlOut, error) {
		return s.doControl(in.Serial, control.Request{Action: control.ActionPause})
	})
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "resume_print",
		Description: "Resume a paused print. The printer reheats on its own; no temperature command is sent.",
		Annotations: controlAnnotations("Resume print", false, true),
		InputSchema: objSchema(serial(), "serial"),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in SerialIn) (*mcp.CallToolResult, ControlOut, error) {
		return s.doControl(in.Serial, control.Request{Action: control.ActionResume})
	})
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "stop_print",
		Description: "Emergency stop: immediately cancel the current print. No confirmation.",
		Annotations: controlAnnotations("Stop print", true, false),
		InputSchema: objSchema(serial(), "serial"),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in SerialIn) (*mcp.CallToolResult, ControlOut, error) {
		return s.doControl(in.Serial, control.Request{Action: control.ActionStop})
	})

	lightProps := serial()
	lightProps["on"] = boolRequiredProp("true switches the chamber light on, false switches it off.")
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "set_chamber_light",
		Description: "Switch the chamber light on or off.",
		Annotations: controlAnnotations("Set chamber light", false, true),
		InputSchema: objSchema(lightProps, "serial", "on"),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in ChamberLightIn) (*mcp.CallToolResult, ControlOut, error) {
		on := in.On
		return s.doControl(in.Serial, control.Request{Action: control.ActionLight, On: &on})
	})

	speedProps := serial()
	profile := enumProp("Print speed profile.", []any{"silent", "standard", "sport", "ludicrous"}, "standard")
	profile.Default = nil
	speedProps["profile"] = profile
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "set_speed_profile",
		Description: "Change the print speed profile of a running or paused print.",
		Annotations: controlAnnotations("Set speed profile", false, true),
		InputSchema: objSchema(speedProps, "serial", "profile"),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in SpeedProfileIn) (*mcp.CallToolResult, ControlOut, error) {
		return s.doControl(in.Serial, control.Request{Action: control.ActionSpeed, Profile: in.Profile})
	})

	aiProps := serial()
	aiProps["enabled"] = boolRequiredProp("false turns AI failure detection off for the current print; true turns it back on.")
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "set_ai_monitoring",
		Description: "Turn AI failure detection off or back on for the current print. The override ends with the print.",
		Annotations: controlAnnotations("Set AI monitoring", false, true),
		InputSchema: objSchema(aiProps, "serial", "enabled"),
	}, s.toolSetAIMonitoring)
}

// doControl sends one control request and renders the shared result.
func (s *Server) doControl(serial string, req control.Request) (*mcp.CallToolResult, ControlOut, error) {
	out := ControlOut{Serial: serial, Action: req.Action}
	if _, known := s.printers[serial]; !known {
		out.Error = toolErr(errUnknownSerial, "serial is not configured: "+serial)
		return errorResult(), out, nil
	}
	if _, err := s.control.Do(serial, mcpOrigin, req); err != nil {
		code := errCommandFailed
		switch {
		case errors.Is(err, control.ErrUnknownPrinter):
			code = errUnknownSerial
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

// toolSetAIMonitoring applies the per-print AI override with the current
// print's session token.
func (s *Server) toolSetAIMonitoring(_ context.Context, _ *mcp.CallToolRequest, in AIMonitoringIn) (*mcp.CallToolResult, ControlOut, error) {
	out := ControlOut{Serial: in.Serial, Action: "ai_monitoring"}
	if _, known := s.printers[in.Serial]; !known {
		out.Error = toolErr(errUnknownSerial, "serial is not configured: "+in.Serial)
		return errorResult(), out, nil
	}
	if s.detCtl == nil {
		out.Error = toolErr(errDetectionDisabled, "AI detection is not configured")
		return errorResult(), out, nil
	}
	sessionID := ""
	if s.det != nil {
		if st, ok := s.det.DetectionStatus(in.Serial).(*detection.Status); ok && st != nil {
			sessionID = st.SessionID
		}
	}
	if _, err := s.detCtl.SetDetectionEnabled(in.Serial, in.Enabled, sessionID); err != nil {
		code := errCommandFailed
		switch {
		case errors.Is(err, detection.ErrUnknownPrinter):
			code = errUnknownSerial
		case errors.Is(err, detection.ErrNoPrintSession), errors.Is(err, detection.ErrStaleSession):
			code = errNoPrintSession
		}
		out.Error = toolErr(code, err.Error())
		return errorResult(), out, nil
	}
	return s.controlSent(out)
}
