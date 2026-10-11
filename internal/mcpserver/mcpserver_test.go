package mcpserver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/control"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/printer"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// testControl supplies successful commands or an offline outcome.
type testControl struct{ offline bool }

// Do returns the selected command outcome.
func (c *testControl) Do(string, string, control.Request) (control.Result, error) {
	if c.offline {
		return control.Result{}, control.ErrNotConnected
	}
	return control.Result{}, nil
}

// Available supplies display hints without gating commands.
func (*testControl) Available(string) []string {
	return []string{"pause", "resume", "stop", "light", "speed"}
}

// testCamera supplies one current JPEG frame.
type testCamera struct{ frame *camera.Frame }

// Acquire accepts capture interest.
func (*testCamera) Acquire(string) (chan struct{}, camera.Status) { return nil, camera.StatusOK }

// Release ends capture interest.
func (*testCamera) Release(string) {}

// Latest returns the fixture frame.
func (c *testCamera) Latest(string) *camera.Frame { return c.frame }

// Wait returns no later frame.
func (*testCamera) Wait(string, context.Context, uint64, time.Duration) *camera.Frame { return nil }

// connectTools connects an SDK client through real in-memory transports.
func connectTools(t *testing.T, s *Server) *mcp.ClientSession {
	t.Helper()
	st, ct := mcp.NewInMemoryTransports()
	ss, err := s.srv.Connect(context.Background(), st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ss.Close(); s.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(context.Background(), ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

// callTool calls a tool and requires SDK input and output validation to pass.
func callTool(t *testing.T, c *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := c.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return res
}

// decodeTool decodes the SDK's validated structured output.
func decodeTool[T any](t *testing.T, res *mcp.CallToolResult) T {
	t.Helper()
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestToolsValidate checks all core tool outputs and the store/module boundary.
func TestToolsValidate(t *testing.T) {
	printers := []config.Printer{{Serial: "01S1", Name: "Printer", Model: "P1S"}}
	store := printer.NewStore(printers)
	store.Apply("01S1", []byte(`{"print":{"gcode_state":"RUNNING","mc_percent":42}}`))
	store.SetOnline("01S1", true)
	log := activity.New(printers)
	log.Record("01S1", "print_started", activity.Info, "started")
	s := New(Deps{Printers: printers, State: store, Activity: log, Control: &testControl{},
		Cameras: &testCamera{frame: &camera.Frame{Seq: 1, Captured: time.Now(), JPEG: []byte{0xff, 0xd8, 0xff, 0xd9}}},
		Modules: []module.Module{{Name: "example", State: func(string) any { return map[string]bool{"enabled": true} }}},
	})
	c := connectTools(t, s)
	list, err := c.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Tools) != 9 {
		t.Fatalf("tool count = %d, want 9", len(list.Tools))
	}
	for _, tc := range []struct {
		name string
		args map[string]any
	}{
		{"list_printers", map[string]any{}},
		{"get_printer_state", map[string]any{"serial": "01S1"}},
		{"get_camera_snapshot", map[string]any{"serial": "01S1"}},
		{"watch_printer", map[string]any{"serial": "01S1", "timeout_seconds": 0}},
		{"pause_print", map[string]any{"serial": "01S1"}},
		{"resume_print", map[string]any{"serial": "01S1"}},
		{"stop_print", map[string]any{"serial": "01S1"}},
		{"set_chamber_light", map[string]any{"serial": "01S1", "on": true}},
		{"set_speed_profile", map[string]any{"serial": "01S1", "profile": "sport"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := callTool(t, c, tc.name, tc.args)
			if res.IsError {
				t.Fatalf("tool error: %+v", res)
			}
			if tc.name == "get_printer_state" {
				out := decodeTool[GetPrinterStateOut](t, res)
				if out.State.Progress == nil || *out.State.Progress != 42 || out.State.Modules["example"] == nil {
					t.Fatalf("state = %+v", out.State)
				}
			}
			if tc.name == "list_printers" {
				out := decodeTool[ListPrintersOut](t, res)
				if len(out.Printers) != 1 || out.Printers[0].Serial != "01S1" {
					t.Fatalf("fleet = %+v", out)
				}
			}
		})
	}
}

// TestToolErrors verifies expected errors remain typed and schema-valid.
func TestToolErrors(t *testing.T) {
	printers := []config.Printer{{Serial: "S1"}}
	c := connectTools(t, New(Deps{Printers: printers, State: printer.NewStore(printers), Control: &testControl{offline: true}}))
	for _, name := range []string{"get_printer_state", "get_camera_snapshot", "watch_printer", "pause_print", "resume_print", "stop_print", "set_chamber_light", "set_speed_profile"} {
		args := map[string]any{"serial": "NOPE"}
		if name == "set_chamber_light" {
			args["on"] = true
		}
		if name == "set_speed_profile" {
			args["profile"] = "standard"
		}
		res := callTool(t, c, name, args)
		out := decodeTool[struct {
			Error *module.ToolError `json:"error"`
		}](t, res)
		if !res.IsError || out.Error == nil || out.Error.Code != module.CodeUnknownSerial {
			t.Fatalf("%s: %+v", name, out)
		}
	}
	res := callTool(t, c, "pause_print", map[string]any{"serial": "S1"})
	out := decodeTool[ControlOut](t, res)
	if !res.IsError || out.Error == nil || out.Error.Code != errUpstreamUnavailable {
		t.Fatalf("offline output = %+v", out)
	}
}

// TestWatchAcrossRestart verifies new activity remains readable with an earlier process ID.
func TestWatchAcrossRestart(t *testing.T) {
	printers := []config.Printer{{Serial: "S1"}}
	first := activity.New(printers)
	first.Record("S1", "print_started", activity.Info, "started")
	after := first.Recent("S1")[0].ID
	time.Sleep(time.Millisecond) // The replacement log must be seeded at a later clock time.
	restarted := activity.New(printers)
	c := connectTools(t, New(Deps{Printers: printers, State: printer.NewStore(printers), Activity: restarted}))
	// The SDK call runs concurrently with the new activity producer.
	type response struct {
		result *mcp.CallToolResult
		err    error
	}
	result := make(chan response, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		res, err := c.CallTool(ctx, &mcp.CallToolParams{Name: "watch_printer", Arguments: map[string]any{"serial": "S1", "after_id": after, "timeout_seconds": 1}})
		result <- response{res, err}
	}()
	restarted.Record("S1", "print_paused", activity.Info, "paused")
	received := <-result
	if received.err != nil {
		t.Fatal(received.err)
	}
	out := decodeTool[WatchPrinterOut](t, received.result)
	if len(out.Events) != 1 || out.Events[0].Kind != "print_paused" || out.Events[0].ID <= after || out.AfterID != out.Events[0].ID {
		t.Fatalf("watch output = %+v", out)
	}
	again := decodeTool[WatchPrinterOut](t, callTool(t, c, "watch_printer", map[string]any{"serial": "S1", "after_id": out.AfterID, "timeout_seconds": 0}))
	if len(again.Events) != 0 || again.AfterID != out.AfterID {
		t.Fatalf("repeated output = %+v", again)
	}
}
