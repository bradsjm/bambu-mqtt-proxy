package notification

import (
	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/hmscodes"
	"bambu-mqtt-proxy/internal/printer"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

// TestEventSummaryPrintOutcomeKinds protects print outcome notification
// eligibility: failed and cancelled prints notify, printer problems follow
// severity, and info problems and resolutions stay activity-only.
func TestEventSummaryPrintOutcomeKinds(t *testing.T) {
	for _, tc := range []struct {
		kind, severity string
		wantNotify     bool
	}{
		{"print_failed", activity.Error, true},
		{"print_cancelled", activity.Warning, true},
		{"printer_problem", activity.Error, true},
		{"printer_problem", activity.Warning, true},
		{"printer_problem", activity.Info, false},
		{"problem_resolved", activity.Info, false},
	} {
		entry := activity.Entry{Kind: tc.kind, Severity: tc.severity}
		if got := eventSummary(entry); (got != "") != tc.wantNotify {
			t.Errorf("eventSummary(%s/%s) = %q, want notified = %v", tc.kind, tc.severity, got, tc.wantNotify)
		}
	}
}

func notificationCatalog(t *testing.T, serial string, health, screen map[string]string) *hmscodes.Catalog {
	t.Helper()
	type item struct {
		Ecode string `json:"ecode"`
		Intro string `json:"intro"`
	}
	items := func(entries map[string]string) []item {
		out := []item{}
		for code, intro := range entries {
			out = append(out, item{code, intro})
		}
		return out
	}
	raw, err := json.Marshal(map[string]any{"result": 0, "ver": 1, "data": map[string]any{
		"device_hms":   map[string]any{"ver": 1, "en": items(health)},
		"device_error": map[string]any{"ver": 1, "en": items(screen)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hms_en_"+serial[:3]+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return hmscodes.New("http://127.0.0.1:1", dir, []string{serial}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func notificationBodies(t *testing.T, s *Service, serial string, batches [][]event) []string {
	t.Helper()
	var bodies []string
	s.send = func(_ context.Context, _ Settings, _ string, body string, _ []byte) error {
		bodies = append(bodies, body)
		return nil
	}
	for _, batch := range batches {
		s.deliverBatch(context.Background(), serial, batch)
	}
	return bodies
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
		for _, text := range []string{"benchy.3mf", "0700-8004, 0700-7000-0002-0004"} {
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
	const serial = "01S00C351100139"
	state := printer.NewStore([]config.Printer{{Serial: serial}})
	state.SetCatalog(notificationCatalog(t, serial,
		map[string]string{"0700700000020004": "Failed to pull back the filament from the toolhead to AMS."},
		map[string]string{"07008004": "AMS failed to pull back filament."}))
	state.Apply(serial, []byte(`{"print":{"subtask_name":"benchy.3mf","print_error":117473284,"hms":[{"attr":117469184,"code":131076}]}}`))
	s := New(Settings{Pushover: Pushover{AppToken: "app", UserKey: "user"}}, []config.Printer{{Serial: serial, Name: "Shop"}}, state, nil, log)
	s.snapshot = func(context.Context, string) (*camera.Frame, camera.Status) {
		return &camera.Frame{JPEG: []byte("jpeg-bytes")}, camera.StatusOK
	}
	s.Observe(serial, activity.Entry{Kind: "print_failed"})
	s.deliverBatch(context.Background(), serial, []event{<-s.events})
	if calls.Load() != 1 {
		t.Fatalf("requests = %d", calls.Load())
	}
	state.Apply(serial, []byte(`{"print":{"print_error":0,"hms":[]}}`))
	s.Observe(serial, activity.Entry{Kind: "print_paused"})
	s.deliverBatch(context.Background(), serial, []event{<-s.events})
	if calls.Load() != 1 {
		t.Fatalf("unqualified pause sent a request")
	}
}

// TestDeliverBatchViewProblems pins View filename fallback, severity filtering,
// and one delivered line for a grouped screen and health pair.
func TestDeliverBatchViewProblems(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	newService := func(t *testing.T, serial, report string) (*Service, *printer.Store) {
		t.Helper()
		state := printer.NewStore([]config.Printer{{Serial: serial}})
		if len(serial) >= 3 {
			state.SetCatalog(notificationCatalog(t, serial,
				map[string]string{
					"0700700000020004": "Failed to pull back the filament from the toolhead to AMS.",
					"0300010000040007": "Other module information",
				},
				map[string]string{"07008004": "AMS failed to pull back filament."}))
		}
		state.Apply(serial, []byte(report))
		return New(Settings{}, []config.Printer{{Serial: serial}}, state, nil, log), state
	}
	t.Run("paired codes share one line", func(t *testing.T) {
		s, _ := newService(t, "01S00C351100139", `{"print":{"subtask_name":"benchy.3mf","print_error":117473284,"hms":[{"attr":117469184,"code":131076}]}}`)
		bodies := notificationBodies(t, s, "01S00C351100139", [][]event{{{serial: "01S00C351100139", summary: "Print failed"}}})
		if len(bodies) != 1 {
			t.Fatalf("deliveries = %d", len(bodies))
		}
		want := "Failed to pull back the filament from the toolhead to AMS. (0700-8004, 0700-7000-0002-0004)\n"
		if !strings.Contains(bodies[0], want) || strings.Count(bodies[0], "0700-8004") != 1 || strings.Count(bodies[0], "0700-7000-0002-0004") != 1 {
			t.Fatalf("paired lines = %q, want one %q", bodies[0], want)
		}
	})
	t.Run("error warning kept info dropped", func(t *testing.T) {
		lines := problemLines([]printer.Problem{
			{Severity: "error", Message: "Error condition", Codes: []printer.ProblemCode{{Code: "0700-8004"}}},
			{Severity: "warning", Message: "Warning condition", Codes: []printer.ProblemCode{{Code: "0700-2300-0003-0001"}, {Code: "0700-2300-0003-0002"}}},
			{Severity: "info", Message: "Info condition", Codes: []printer.ProblemCode{{Code: "0300-0100-0004-0007"}}},
		})
		if len(lines) != 2 || lines[0] != "Error condition (0700-8004)" || lines[1] != "Warning condition (0700-2300-0003-0001, 0700-2300-0003-0002)" {
			t.Fatalf("lines = %q", lines)
		}
		s, _ := newService(t, "01S00C351100139", `{"print":{"print_error":117473284,"hms":[{"attr":117469184,"code":131076},{"attr":50331904,"code":262151}]}}`)
		view, ok := s.state.View("01S00C351100139")
		if !ok {
			t.Fatal("unknown printer view")
		}
		lines = problemLines(view.Problems)
		if len(lines) != 1 || strings.Contains(lines[0], "Other module information") {
			t.Fatalf("view lines = %q", lines)
		}
		pause := event{serial: "01S00C351100139", summary: "Print paused", pause: true}
		if bodies := notificationBodies(t, s, "01S00C351100139", [][]event{{pause}}); len(bodies) != 1 || !strings.Contains(bodies[0], "Print paused\n") {
			t.Fatalf("qualified pause bodies = %q", bodies)
		}
	})
	t.Run("info only qualifies no pause and adds no lines", func(t *testing.T) {
		s, _ := newService(t, "01S00C351100139", `{"print":{"hms":[{"attr":50331904,"code":262151}]}}`)
		pause := event{serial: "01S00C351100139", summary: "Print paused", pause: true}
		if bodies := notificationBodies(t, s, "01S00C351100139", [][]event{{pause}}); len(bodies) != 0 {
			t.Fatalf("info-only pause delivered %q", bodies)
		}
		s.Observe("01S00C351100139", activity.Entry{Kind: "printer_problem", Severity: activity.Info})
		select {
		case ev := <-s.events:
			t.Fatalf("info problem queued %+v", ev)
		default:
		}
		s.Observe("01S00C351100139", activity.Entry{Kind: "problem_resolved", Severity: activity.Info})
		select {
		case ev := <-s.events:
			t.Fatalf("resolution queued %+v", ev)
		default:
		}
	})
	t.Run("filename fallback", func(t *testing.T) {
		s, state := newService(t, "01S00C351100139", `{"print":{"subtask_name":"","gcode_file":"fallback.3mf"}}`)
		bodies := notificationBodies(t, s, "01S00C351100139", [][]event{{{serial: "01S00C351100139", summary: "Print finished"}}})
		if len(bodies) != 1 || !strings.Contains(bodies[0], "fallback.3mf") {
			t.Fatalf("fallback body = %q", bodies)
		}
		state.Apply("01S00C351100139", []byte(`{"print":{"subtask_name":"Preferred name"}}`))
		bodies = notificationBodies(t, s, "01S00C351100139", [][]event{{{serial: "01S00C351100139", summary: "Print finished"}}})
		if len(bodies) != 1 || !strings.Contains(bodies[0], "Preferred name") || strings.Contains(bodies[0], "fallback.3mf") {
			t.Fatalf("preferred body = %q", bodies)
		}
		state.Apply("01S00C351100139", []byte(`{"print":{"subtask_name":"","gcode_file":""}}`))
		bodies = notificationBodies(t, s, "01S00C351100139", [][]event{{{serial: "01S00C351100139", summary: "Print finished"}}})
		if len(bodies) != 1 || strings.Contains(bodies[0], "fallback.3mf") || strings.Contains(bodies[0], "Preferred name") {
			t.Fatalf("empty filename body = %q", bodies)
		}
	})
	t.Run("unknown printer silent problems", func(t *testing.T) {
		s, _ := newService(t, "01S00C351100139", `{"print":{"subtask_name":"benchy.3mf"}}`)
		bodies := notificationBodies(t, s, "missing", [][]event{{{serial: "missing", summary: "Print finished"}}})
		if len(bodies) != 1 || strings.Contains(bodies[0], "File: ") || strings.Contains(bodies[0], "0700-") {
			t.Fatalf("unknown body = %q", bodies)
		}
		if len(problemLines(nil)) != 0 {
			t.Fatalf("nil problems = %q", problemLines(nil))
		}
	})
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
	})
}
