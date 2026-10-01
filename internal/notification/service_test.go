package notification

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// sentMsg captures one delivery through the fake send seam.
type sentMsg struct {
	title string
	body  string
	jpeg  []byte
}

// testPrinters is one named printer shared by the service tests.
func testPrinters() []config.Printer {
	return []config.Printer{{Serial: "S1", Name: "Shop"}}
}

// newTestService builds a service on a fresh telemetry cache whose send
// seam records into the returned channel; tests override send and
// snapshot before Start.
func newTestService(t *testing.T) (*Service, chan sentMsg) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	state := telemetry.NewCache(testPrinters(), log)
	s := New(config.Notifications{
		Enabled:  true,
		Provider: "pushover",
		Pushover: config.Pushover{AppToken: "app-token", UserKey: "user-key"},
	}, state, nil, log)
	sent := make(chan sentMsg, 8)
	s.send = func(_ context.Context, _ config.Notifications, title, body string, jpeg []byte) error {
		sent <- sentMsg{title, body, jpeg}
		return nil
	}
	return s, sent
}

// TestServiceSendsFinishedPrintWithSnapshot pins the happy path: one
// message whose title is the printer name, whose body carries the summary
// and the file line, and whose JPEG comes from the camera seam.
func TestServiceSendsFinishedPrintWithSnapshot(t *testing.T) {
	s, sent := newTestService(t)
	s.state.Observe("S1", []byte(`{"print":{"subtask_name":"benchy.3mf"}}`))
	s.snapshot = func(context.Context, string) (*camera.Frame, camera.Status) {
		return &camera.Frame{JPEG: []byte("jpeg-bytes")}, camera.StatusOK
	}
	s.Start()
	defer s.Close()

	s.Observe("S1", activity.Entry{Kind: "print_finished", Severity: activity.Info, Message: "Print finished"})
	select {
	case m := <-sent:
		if m.title != "Shop" {
			t.Fatalf("title = %q, want Shop", m.title)
		}
		if !strings.Contains(m.body, "Print finished") || !strings.Contains(m.body, "File: benchy.3mf") {
			t.Fatalf("body = %q", m.body)
		}
		if string(m.jpeg) != "jpeg-bytes" {
			t.Fatalf("jpeg = %q", m.jpeg)
		}
	case <-time.After(batchWindow + 3*time.Second):
		t.Fatal("finished print produced no notification")
	}
}

// TestServiceSkipsUnqualifiedPause pins that a lone print_paused without
// an active alert sends nothing.
func TestServiceSkipsUnqualifiedPause(t *testing.T) {
	s, sent := newTestService(t)
	s.Start()
	defer s.Close()

	s.Observe("S1", activity.Entry{Kind: "print_paused", Severity: activity.Warning, Message: "Print paused"})
	time.Sleep(batchWindow + 500*time.Millisecond)
	if n := len(sent); n != 0 {
		t.Fatalf("unqualified pause sent %d messages", n)
	}
}

// TestServiceBatchesPauseWithError pins the window: a pause followed by a
// printer error inside one batch becomes a single message with both
// summaries and the active print error alert.
func TestServiceBatchesPauseWithError(t *testing.T) {
	s, sent := newTestService(t)
	s.state.Observe("S1", []byte(`{"print":{"print_error":50348044}}`))
	s.Start()
	defer s.Close()

	s.Observe("S1", activity.Entry{Kind: "print_paused", Severity: activity.Warning, Message: "Print paused"})
	s.Observe("S1", activity.Entry{Kind: "print_error", Severity: activity.Error, Message: "Printer error"})
	select {
	case m := <-sent:
		if !strings.Contains(m.body, "Print paused") || !strings.Contains(m.body, "Printer error") {
			t.Fatalf("body = %q", m.body)
		}
		if !strings.Contains(m.body, "0300_400C") {
			t.Fatalf("body lacks the print error alert: %q", m.body)
		}
		if len(sent) != 0 {
			t.Fatalf("batch produced %d extra messages", len(sent))
		}
	case <-time.After(batchWindow + 3*time.Second):
		t.Fatal("paused-with-error batch produced no notification")
	}
}
