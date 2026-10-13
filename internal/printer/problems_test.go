package printer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/hmscodes"
)

const problemSerial = "01S00C351100139"
const retractionMessage = "Failed to pull back the filament from the toolhead to AMS. Possible cause: filament or spool stuck."

func problemCatalog(t *testing.T, health, screen map[string]string) *hmscodes.Catalog {
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
	if err := os.WriteFile(filepath.Join(dir, "hms_en_01S.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return hmscodes.New("http://127.0.0.1:1", dir, []string{problemSerial}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func problemDoc(screen uint32, health ...HMSAlert) Doc {
	alerts := make([]any, 0, len(health))
	for _, alert := range health {
		alerts = append(alerts, map[string]any{"attr": float64(alert.Attr), "code": float64(alert.Code)})
	}
	return Doc{"print": map[string]any{"print_error": float64(screen), "hms": alerts}}
}

func TestProblemsGrouping(t *testing.T) {
	warning := HMSAlert{0x07002300, 0x00030001}
	errorCode := HMSAlert{0x07007000, 0x00020004}
	other := HMSAlert{0x03000100, 0x00040007}
	catalog := problemCatalog(t, map[string]string{
		hmscodes.HMSCode(warning.Attr, warning.Code):     "Slot warning <check & retry>",
		hmscodes.HMSCode(errorCode.Attr, errorCode.Code): retractionMessage,
		hmscodes.HMSCode(other.Attr, other.Code):         "Other module information",
	}, map[string]string{"07008004": "Screen warning"})
	got := Problems(problemDoc(0x07008004, warning, other, errorCode, warning), problemSerial, catalog.Snapshot(problemSerial))
	if len(got) != 2 || len(got[0].Codes) != 3 || len(got[1].Codes) != 1 {
		t.Fatalf("grouping = %+v", got)
	}
	group := got[0]
	if group.Severity != "error" || group.Message != retractionMessage || group.URL != hmscodes.HMSURL(problemSerial, errorCode.Attr, errorCode.Code) {
		t.Fatalf("representative = %+v", group)
	}
	if group.Codes[0].Code != "0700-8004" || group.Codes[0].Source != "screen" || group.Codes[0].URL != hmscodes.ErrorTableURL || group.Codes[1].Code != "0700-2300-0003-0001" || group.Codes[1].Message != "Slot warning <check & retry>" || group.Codes[2].Message != retractionMessage {
		t.Fatalf("constituents = %+v", group.Codes)
	}
	if got[1].Severity != "info" || got[1].Message != "Other module information" {
		t.Fatalf("other group = %+v", got[1])
	}
}

func TestProblemsVisibility(t *testing.T) {
	health := HMSAlert{0x07007000, 0x00020004}
	catalog := problemCatalog(t, map[string]string{hmscodes.HMSCode(health.Attr, health.Code): ""}, map[string]string{"07008004": ""})
	snap := catalog.Snapshot(problemSerial)
	for _, screen := range []uint32{0, 0x07008004, 0x0300400C, 0x0500400E} {
		got := Problems(problemDoc(screen, health, HMSAlert{0x03000100, 0x00010007}), problemSerial, snap)
		if got == nil || len(got) != 0 {
			t.Fatalf("suppressed code %08X = %+v", screen, got)
		}
	}
	for _, tc := range []struct {
		code     uint32
		severity string
	}{{0x03004001, "error"}, {0x03008001, "warning"}, {0x0300C001, "info"}, {0x03001001, "error"}} {
		got := Problems(problemDoc(tc.code), problemSerial, snap)
		if len(got) != 1 || got[0].Severity != tc.severity || !strings.HasSuffix(got[0].Message, "has no text for this code.") {
			t.Fatalf("missing screen %08X = %+v", tc.code, got)
		}
	}
	loading := Problems(problemDoc(0x07008004, health), problemSerial, hmscodes.Snapshot{})
	if len(loading) != 1 || len(loading[0].Codes) != 2 || loading[0].Severity != "error" || loading[0].Message != "Printer reported code 0700-8004. Bambu's message list for this printer is still downloading." {
		t.Fatalf("loading = %+v", loading)
	}
	for _, tc := range []struct {
		level    uint32
		severity string
	}{{1, "error"}, {2, "error"}, {3, "warning"}, {4, "info"}, {9, "error"}} {
		got := Problems(problemDoc(0, HMSAlert{0x03000100, tc.level<<16 | 7}), problemSerial, hmscodes.Snapshot{})
		if len(got) != 1 || got[0].Severity != tc.severity {
			t.Fatalf("health level %d = %+v", tc.level, got)
		}
	}
}

func TestProblemsRepresentativeTies(t *testing.T) {
	first := HMSAlert{0x07002300, 0x00020001}
	second := HMSAlert{0x07007000, 0x00020004}
	catalog := problemCatalog(t, map[string]string{
		hmscodes.HMSCode(first.Attr, first.Code):   "First health message",
		hmscodes.HMSCode(second.Attr, second.Code): "Second health message",
	}, map[string]string{"07004004": "Screen error"})
	got := Problems(problemDoc(0x07004004, first, second), problemSerial, catalog.Snapshot(problemSerial))
	if len(got) != 1 || got[0].Message != "First health message" || got[0].URL != hmscodes.HMSURL(problemSerial, first.Attr, first.Code) {
		t.Fatalf("tie = %+v", got)
	}
	got = Problems(problemDoc(0x07004004, HMSAlert{0x07009900, 0x00020009}), problemSerial, catalog.Snapshot(problemSerial))
	if len(got) != 1 || got[0].Message != "Screen error" || got[0].URL != hmscodes.ErrorTableURL {
		t.Fatalf("screen representative = %+v", got)
	}
}

func TestProblemsFailedCatalog(t *testing.T) {
	published := make(chan struct{}, 1)
	logger := slog.New(&catalogFailureHandler{published: published})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	catalog := hmscodes.New(server.URL, "", []string{problemSerial}, logger)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); catalog.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("catalog did not stop")
		}
	}()
	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog did not publish its failure")
	}
	snap := catalog.Snapshot(problemSerial)
	lookup := snap.Error(0x07008004)
	if lookup.State != hmscodes.StateFailed || lookup.Err == "" {
		t.Fatalf("failure lookup = %+v", lookup)
	}
	got := Problems(problemDoc(0x07008004, HMSAlert{0x07007000, 0x00020004}), problemSerial, snap)
	if len(got) != 1 || len(got[0].Codes) != 2 {
		t.Fatalf("failed projection = %+v", got)
	}
	for _, code := range got[0].Codes {
		want := "Printer reported code " + code.Code + ". Bambu's message list for this printer could not be downloaded: " + lookup.Err
		if code.Message != want {
			t.Fatalf("failed message = %q, want %q", code.Message, want)
		}
	}
}

type catalogFailureHandler struct{ published chan struct{} }

func (h *catalogFailureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *catalogFailureHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		select {
		case h.published <- struct{}{}:
		default:
		}
	}
	return nil
}
func (h *catalogFailureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *catalogFailureHandler) WithGroup(string) slog.Handler      { return h }

func TestOneEntryPerFault(t *testing.T) {
	screen := `{"print":{"print_error":117473284}}`
	health := `{"print":{"hms":[{"attr":117469184,"code":131076}]}}`
	both := `{"print":{"print_error":117473284,"hms":[{"attr":117469184,"code":131076}]}}`
	clearScreen := `{"print":{"print_error":0}}`
	clearHealth := `{"print":{"hms":[]}}`
	clearBoth := `{"print":{"print_error":0,"hms":[]}}`
	for _, tc := range []struct {
		name    string
		reports []string
	}{{"together", []string{both, clearBoth}}, {"screen first", []string{screen, health, clearScreen, clearHealth}}, {"health first", []string{health, screen, clearHealth, clearScreen}}} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := problemCatalog(t, map[string]string{"0700700000020004": retractionMessage}, map[string]string{"07008004": "AMS failed to pull back filament."})
			identity := []config.Printer{{Serial: problemSerial}}
			store := NewStore(identity)
			store.SetCatalog(catalog)
			log := activity.New(identity)
			store.SetActivity(log)
			store.Apply(problemSerial, []byte(`{"print":{"gcode_state":"RUNNING"}}`))
			var kinds []string
			store.Subscribe(func(e Event) { kinds = append(kinds, e.Kinds...) })
			for i, report := range tc.reports {
				store.Apply(problemSerial, []byte(report))
				if i < len(tc.reports)-1 && len(kinds) != 1 {
					t.Fatalf("premature resolution or repeated announcement at %d: %v", i, kinds)
				}
				if i == 1 && tc.name != "together" {
					view, _ := store.View(problemSerial)
					if len(view.Problems) != 1 || view.Problems[0].Severity != "error" || len(view.Problems[0].Codes) != 2 {
						t.Fatalf("enriched view = %+v", view.Problems)
					}
				}
			}
			if !reflect.DeepEqual(kinds, []string{"printer_problem", "problem_resolved"}) {
				t.Fatalf("kinds = %v", kinds)
			}
			entries := log.Recent(problemSerial)
			wantSeverity := "error"
			if tc.name == "screen first" {
				wantSeverity = "warning"
			}
			if entries[1].Severity != wantSeverity || entries[0].Severity != "info" {
				t.Fatalf("entries = %+v", entries)
			}
			if tc.name == "together" {
				if entries[1].Message != "Printer error: "+retractionMessage+" (0700-8004, 0700-7000-0002-0004)" || entries[1].URL != hmscodes.HMSURL(problemSerial, 117469184, 131076) {
					t.Fatalf("announcement = %+v", entries[1])
				}
			}
			view, _ := store.View(problemSerial)
			if view.Problems == nil || len(view.Problems) != 0 {
				t.Fatalf("cleared view = %+v", view.Problems)
			}
		})
	}
}

func TestViewProblemJSONAndFilename(t *testing.T) {
	store := NewStore([]config.Printer{{Serial: problemSerial}})
	store.Apply(problemSerial, []byte(`{"print":{"subtask_name":"","gcode_file":"fallback.3mf"}}`))
	view, _ := store.View(problemSerial)
	if view.JobName == nil || *view.JobName != "fallback.3mf" {
		t.Fatalf("filename = %v", view.JobName)
	}
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["problems"]) != "[]" || fields["hms"] != nil || fields["print_error"] != nil {
		t.Fatalf("view JSON = %s", raw)
	}
	store.Apply(problemSerial, []byte(`{"print":{"subtask_name":"Preferred name"}}`))
	view, _ = store.View(problemSerial)
	if view.JobName == nil || *view.JobName != "Preferred name" {
		t.Fatalf("preferred filename = %v", view.JobName)
	}
}

func TestProblemReplacementAndSeverity(t *testing.T) {
	for _, severity := range []string{"error", "warning", "info"} {
		old := []Problem{{Severity: severity, Message: "Old condition", URL: "old", Codes: []ProblemCode{{Code: "0300-0001", Source: "screen"}}}}
		current := []Problem{{Severity: severity, Message: "New condition", URL: "new", Codes: []ProblemCode{{Code: "0300-0002", Source: "screen"}}}}
		doc := Doc{"print": map[string]any{"gcode_state": "RUNNING"}}
		got := Transitions(doc, doc, old, current)
		want := []Transition{
			{"printer_problem", severity, "Printer " + severity + ": New condition (0300-0002)", "new"},
			{"problem_resolved", "info", "Problem resolved: Old condition (0300-0001)", "old"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s replacement = %+v, want %+v", severity, got, want)
		}
	}
}
