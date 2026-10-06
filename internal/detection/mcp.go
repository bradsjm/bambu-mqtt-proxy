package detection

import (
	"context"
	"errors"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"bambu-mqtt-proxy/internal/module"
)

// Tool error code local to set_ai_monitoring: no print is active, or the
// echoed session token is stale, so there is nothing to override.
const CodeNoPrintSession = "no_print_session"

// AIMonitoringIn names one printer and the requested toggle.
type AIMonitoringIn struct {
	Serial  string `json:"serial"`
	Enabled bool   `json:"enabled"`
}

// AIMonitoringOut is the structured output of set_ai_monitoring. Detection
// carries the authoritative fresh status after the toggle, or nil when the
// toggle failed.
type AIMonitoringOut struct {
	Serial    string            `json:"serial"`
	Detection *Status           `json:"detection,omitempty"`
	Error     *module.ToolError `json:"error,omitempty"`
}

// registerMCP adds the detection module's MCP tool. The core calls it once
// in mcpserver.New, only when detection runs unblocked in the
// configuration. Like the camera wall's PUT /detection/{serial}, the tool
// applies the per-print override and records no activity entry.
func (e *Engine) registerMCP(srv *mcp.Server) {
	mcp.AddTool(srv, &mcp.Tool{
		Name: "set_ai_monitoring",
		Description: "Turn AI failure detection off or back on for the current print. The override ends with the print. " +
			"The fresh detection status after the change is served at state.modules.detection.",
		Annotations: module.Command("Set AI monitoring", false, true),
		InputSchema: module.SerialInput(map[string]*jsonschema.Schema{
			"enabled": {
				Type:        "boolean",
				Description: "false turns AI failure detection off for the current print; true turns it back on.",
			},
		}, "enabled"),
	}, func(_ context.Context, _ *mcp.CallToolRequest, in AIMonitoringIn) (*mcp.CallToolResult, AIMonitoringOut, error) {
		out := AIMonitoringOut{Serial: in.Serial}
		status, ok := e.DetectionStatus(in.Serial).(*Status)
		if !ok || status == nil {
			out.Error = &module.ToolError{Code: module.CodeUnknownSerial, Message: "serial is not configured: " + in.Serial}
			return errorResult(), out, nil
		}
		fresh, err := e.SetDetectionEnabled(in.Serial, in.Enabled, status.SessionID)
		if err != nil {
			code := module.CodeCommandFailed
			switch {
			case errors.Is(err, ErrUnknownPrinter):
				code = module.CodeUnknownSerial
			case errors.Is(err, ErrNoPrintSession), errors.Is(err, ErrStaleSession):
				code = CodeNoPrintSession
			}
			out.Error = &module.ToolError{Code: code, Message: err.Error()}
			return errorResult(), out, nil
		}
		if st, isStatus := fresh.(*Status); isStatus {
			out.Detection = st
		}
		return textResult(textFor(in.Serial, in.Enabled, out.Detection)), out, nil
	})
}

// textFor renders the one-line chat text for a successful toggle. It is
// the short human text; the structured output remains the contract.
func textFor(serial string, enabled bool, st *Status) string {
	state := "off"
	if enabled {
		state = "on"
	}
	line := serial + ": AI monitoring " + state
	if st != nil {
		line += " (state " + st.State + ")"
	}
	return line
}

// errorResult builds a tool result that carries only the structured error.
func errorResult() *mcp.CallToolResult { return &mcp.CallToolResult{IsError: true} }

// textResult builds a successful tool result with one text line.
func textResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}
