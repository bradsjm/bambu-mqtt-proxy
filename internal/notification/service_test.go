package notification

import (
	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// TestEventSummaryPlatecheckKinds pins the plate-check notification map:
// the stop lifecycle kinds notify with the recorded message, while routine
// skipped and stopped entries stay silent.
func TestEventSummaryPlatecheckKinds(t *testing.T) {
	for _, tc := range []struct {
		kind, message, want string
	}{
		{"platecheck_stop_requested",
			"Build plate may be occupied; stop requested. Check the printer and clear the plate before sending the print again.",
			"Build plate may be occupied; stop requested. Check the printer and clear the plate before sending the print again."},
		{"platecheck_stop_failed", "The stop command did not reach the printer.", "The stop command did not reach the printer."},
		{"platecheck_stop_unconfirmed", "Plate-check stop is unconfirmed. Check the printer now.", "Plate-check stop is unconfirmed. Check the printer now."},
		{"platecheck_skipped", "Startup check skipped: the evidence expired.", ""},
		{"platecheck_stopped", "Printer reports the job ended after the plate-check stop request.", ""},
	} {
		if got := eventSummary(activity.Entry{Kind: tc.kind, Message: tc.message}); got != tc.want {
			t.Errorf("eventSummary(%s) = %q, want %q", tc.kind, got, tc.want)
		}
	}
}

// TestEventSummaryPlatecheckLayer1Kinds pins which first-layer kinds
// notify: the warning and pause lifecycle kinds pass the recorded message
// through, while pass, confirmed, and skipped entries stay activity-only.
// The message text stands in for whatever the plate-check module records.
func TestEventSummaryPlatecheckLayer1Kinds(t *testing.T) {
	const message = "First-layer check recorded this message."
	for _, tc := range []struct {
		kind string
		want string
	}{
		{"platecheck_layer1_warning", message},
		{"platecheck_layer1_pause_sent", message},
		{"platecheck_layer1_pause_failed", message},
		{"platecheck_layer1_pause_unconfirmed", message},
		{"platecheck_layer1_pause_confirmed", ""},
		{"platecheck_layer1_passed", ""},
		{"platecheck_layer1_skipped", ""},
	} {
		entry := activity.Entry{Kind: tc.kind, Message: message}
		if got := eventSummary(entry); got != tc.want {
			t.Errorf("eventSummary(%s) = %q, want %q", tc.kind, got, tc.want)
		}
	}
}

// TestEventSummaryPrintOutcomeKinds protects print outcome notification summaries: failed
// and cancelled prints notify with short summaries, errors follow the HMS
// severity pattern, and cleared alerts stay activity-only.
func TestEventSummaryPrintOutcomeKinds(t *testing.T) {
	for _, tc := range []struct {
		kind, severity, want string
	}{
		{"print_failed", activity.Error, "Print failed"},
		{"print_cancelled", activity.Warning, "Print cancelled"},
		{"print_error", activity.Error, "Printer error"},
		{"print_error", activity.Warning, "Printer warning"},
		{"print_error", activity.Info, ""},
		{"hms_alert", activity.Error, "Printer error"},
		{"hms_alert", activity.Warning, "Printer warning"},
		{"hms_alert", activity.Info, ""},
		{"alerts_cleared", activity.Info, ""},
	} {
		entry := activity.Entry{Kind: tc.kind, Severity: tc.severity}
		if got := eventSummary(entry); got != tc.want {
			t.Errorf("eventSummary(%s/%s) = %q, want %q", tc.kind, tc.severity, got, tc.want)
		}
	}
}

// TestDelivery protects the activity-to-Pushover request and silent pause contracts.
func TestDelivery(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			return
		}
		defer r.MultipartForm.RemoveAll()
		if title := r.FormValue("title"); title != "Shop" {
			t.Errorf("title = %q", title)
		}
		body := r.FormValue("message")
		for _, text := range []string{"Print failed", "File: benchy.3mf", "0300_8001"} {
			if !strings.Contains(body, text) {
				t.Errorf("message %q lacks %q", body, text)
			}
		}
		file, header, err := r.FormFile("attachment")
		if err != nil {
			t.Error(err)
			return
		}
		defer file.Close()
		jpeg, _ := io.ReadAll(file)
		if string(jpeg) != "jpeg-bytes" || header.Header.Get("Content-Type") != "image/jpeg" {
			t.Errorf("attachment = %q", jpeg)
		}
		_, _ = w.Write([]byte(`{"status":1}`))
	}))
	defer server.Close()
	old := pushoverURL
	pushoverURL = server.URL
	defer func() { pushoverURL = old }()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	state := printer.NewStore([]config.Printer{{Serial: "S1"}})
	state.Apply("S1", []byte(`{"print":{"subtask_name":"benchy.3mf","print_error":50364417}}`))
	s := New(Settings{Pushover: Pushover{AppToken: "app", UserKey: "user"}}, []config.Printer{{Serial: "S1", Name: "Shop"}}, state, nil, log)
	s.snapshot = func(context.Context, string) (*camera.Frame, camera.Status) {
		return &camera.Frame{JPEG: []byte("jpeg-bytes")}, camera.StatusOK
	}
	s.Observe("S1", activity.Entry{Kind: "print_failed"})
	s.deliverBatch(context.Background(), "S1", []event{<-s.events})
	if calls.Load() != 1 {
		t.Fatalf("requests = %d", calls.Load())
	}
	state.Apply("S1", []byte(`{"print":{"print_error":0}}`))
	s.Observe("S1", activity.Entry{Kind: "print_paused"})
	s.deliverBatch(context.Background(), "S1", []event{<-s.events})
	if calls.Load() != 1 {
		t.Fatalf("unqualified pause sent a request")
	}
}

// TestFullQueueDrops protects nonblocking activity observation.
func TestFullQueueDrops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(Settings{}, nil, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		done := make(chan struct{})
		go func() {
			for range 17 {
				s.Observe("S1", activity.Entry{Kind: "print_failed"})
			}
			close(done)
		}()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Observe blocked on full queue")
		}
		if len(s.events) != 16 {
			t.Fatalf("queue length = %d", len(s.events))
		}
	})
}
