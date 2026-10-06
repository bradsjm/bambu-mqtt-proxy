package detection

// Wire-level tests for the set_ai_monitoring module tool. They drive a real
// mcp.Server through the SDK in-memory transports, so the SDK validates the
// input and output schemas and decodes the structured content.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/module"
)

// connectDetectionMCP connects an SDK client to a server that carries only
// the detection engine's tool. The fixture reuses the engine_test fakes
// through newEtWorld; the engine is never started, so tests drive the
// toggle directly just as production clients do over MCP.
func connectDetectionMCP(t *testing.T, e *Engine) *mcp.ClientSession {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	e.registerMCP(srv)
	ctx := context.Background()
	sTransport, cTransport := mcp.NewInMemoryTransports()
	serverSession, err := srv.Connect(ctx, sTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { serverSession.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	clientSession, err := client.Connect(ctx, cTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { clientSession.Close() })
	return clientSession
}

// newEngineMCP builds the engine under test without starting it.
func newEngineMCP(t *testing.T) (*Engine, *etWorld) {
	t.Helper()
	w := newEtWorld(t)
	return w.engine, w
}

// call runs the tool and returns the SDK-decoded result.
func callAIMonitoring(t *testing.T, client *mcp.ClientSession, serial string, enabled bool) *mcp.CallToolResult {
	t.Helper()
	res, err := client.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "set_ai_monitoring",
		Arguments: map[string]any{"serial": serial, "enabled": enabled},
	})
	if err != nil {
		t.Fatalf("call set_ai_monitoring: %v", err)
	}
	return res
}

// decodeOut reads the structured output into AIMonitoringOut.
func decodeOut(t *testing.T, res *mcp.CallToolResult) AIMonitoringOut {
	t.Helper()
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatalf("marshal structured: %v", err)
	}
	var out AIMonitoringOut
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal structured: %v (raw %s)", err, raw)
	}
	return out
}

// textOf returns the concatenated text content.
func textOf(res *mcp.CallToolResult) string {
	text := ""
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			text += tc.Text
		}
	}
	return text
}

func TestSetAIMonitoringUnknownSerial(t *testing.T) {
	e, _ := newEngineMCP(t)
	client := connectDetectionMCP(t, e)
	res := callAIMonitoring(t, client, "NOPE", false)
	if !res.IsError {
		t.Fatalf("unknown serial not flagged error: %+v", res)
	}
	out := decodeOut(t, res)
	if out.Error == nil || out.Error.Code != module.CodeUnknownSerial ||
		out.Error.Message != "serial is not configured: NOPE" {
		t.Fatalf("unknown serial output = %+v", out)
	}
	if out.Detection != nil {
		t.Fatalf("unknown serial carried detection: %+v", out.Detection)
	}
}

func TestSetAIMonitoringNoPrintSession(t *testing.T) {
	e, _ := newEngineMCP(t)
	client := connectDetectionMCP(t, e)
	// S1 is configured but has no active print session, so disabling has
	// nothing to override.
	res := callAIMonitoring(t, client, "S1", false)
	if !res.IsError {
		t.Fatalf("no session not flagged error: %+v", res)
	}
	out := decodeOut(t, res)
	if out.Error == nil || out.Error.Code != CodeNoPrintSession {
		t.Fatalf("no session output = %+v", out)
	}
	if out.Serial != "S1" || out.Detection != nil {
		t.Fatalf("no session fields = %+v", out)
	}
}

func TestSetAIMonitoringSuccessFreshStatus(t *testing.T) {
	e, w := newEngineMCP(t)
	w.startSession(1, 1)
	client := connectDetectionMCP(t, e)

	res := callAIMonitoring(t, client, "S1", false)
	if res.IsError {
		t.Fatalf("expected success, got error result: %+v", res)
	}
	out := decodeOut(t, res)
	if out.Serial != "S1" || out.Error != nil {
		t.Fatalf("success output = %+v", out)
	}
	if out.Detection == nil || out.Detection.Enabled {
		t.Fatalf("fresh detection status = %+v, want Enabled=false", out.Detection)
	}
	if out.Detection.State != StateDisabled || out.Detection.Reason != ReasonUserDisabled {
		t.Fatalf("fresh state = %+v", out.Detection)
	}
	if textOf(res) == "" {
		t.Fatalf("success result has no text content: %+v", res.Content)
	}

	// Toggling back on returns an enabled fresh status from one lookup.
	res = callAIMonitoring(t, client, "S1", true)
	if res.IsError {
		t.Fatalf("re-enable flagged error: %+v", res)
	}
	out = decodeOut(t, res)
	if out.Detection == nil || !out.Detection.Enabled {
		t.Fatalf("re-enabled detection = %+v", out.Detection)
	}
}

func TestSetAIMonitoringModuleRegistration(t *testing.T) {
	// Module must declare the MCP hook through a valid tool schema; AddTool
	// panics on a malformed input schema.
	e := New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}},
		&etClient{}, &etFrames{}, &etSessions{}, &etPauser{}, &etGens{gen: 1},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	m := e.Module()
	if m.Name != "detection" || m.MCP == nil || m.State == nil {
		t.Fatalf("module = %+v", m)
	}
	m.MCP(mcp.NewServer(&mcp.Implementation{Name: "test", Version: "0"}, nil))
}
