package platecheck

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/configui"
)

func snapshotRequest(s *Service, raw []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/platecheck/snapshots", bytes.NewReader(raw))
	w := httptest.NewRecorder()
	s.handleSnapshots(w, req)
	return w
}

type snapshotStream struct {
	snapshotStart
	Results map[string]snapshotRow
	Rows    []snapshotRow
	Done    snapshotDone
}

func decodeStream(t *testing.T, w *httptest.ResponseRecorder) snapshotStream {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("HTTP %d %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("content type %s", w.Header().Get("Content-Type"))
	}
	var out snapshotStream
	out.Results = map[string]snapshotRow{}
	lines := bytes.Split(bytes.TrimSpace(w.Body.Bytes()), []byte("\n"))
	for i, line := range lines {
		var event struct{ Type string }
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		switch event.Type {
		case "start":
			if i != 0 {
				t.Fatal("start must be first")
			}
			if err := json.Unmarshal(line, &out.snapshotStart); err != nil {
				t.Fatal(err)
			}
		case "result":
			if i == 0 || i == len(lines)-1 {
				t.Fatal("result outside start/done")
			}
			var result snapshotResult
			if err := json.Unmarshal(line, &result); err != nil {
				t.Fatal(err)
			}
			if _, exists := out.Results[result.Printer.Serial]; exists {
				t.Fatal("duplicate result")
			}
			out.Results[result.Printer.Serial] = result.Printer
			out.Rows = append(out.Rows, result.Printer)
		case "done":
			if i != len(lines)-1 {
				t.Fatal("done must be last")
			}
			if err := json.Unmarshal(line, &out.Done); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unexpected event %q", event.Type)
		}
	}
	if out.Type != "start" || out.Done.Type != "done" || len(out.Results) != len(out.Printers) {
		t.Fatalf("incomplete stream %+v", out)
	}
	return out
}

func TestSnapshotRowsConfiguredOrderFailureContinuationAndNoControlEffects(t *testing.T) {
	printers := []config.Printer{{Serial: "01S1", Name: "First", Model: "P1S"}, {Serial: "00M1", Name: "Second", Model: "X1C"}, {Serial: "0391", Name: "Third", Model: "A1"}, {Serial: "dead", Name: "Unsupported", Model: "UNKNOWN"}}
	clock := &testClock{t: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}
	data := testJPEG(t)
	captures := []string{}
	var captureMu sync.Mutex
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		captureMu.Lock()
		captures = append(captures, serial)
		captureMu.Unlock()
		if serial == "01S1" {
			return Frame{}, &safeError{code: "camera_unavailable"}
		}
		clock.advance(time.Millisecond)
		return Frame{JPEG: data, Seq: 7, Captured: clock.now(), Width: 8, Height: 6}, nil
	})
	commands := &fakeCommands{gen: 1}
	client := &fakeDecision{result: Result{PClear: .127, POccupied: 1 - .127, PAssessable: .962, Model: "clef"}}
	s := New(printers, testSettings(), client, frames, nil, commands, nil)
	s.now = clock.now
	s.SetActivity(activity.New(printers))
	factoryCalls := 0
	s.newClient = func(settings Settings, log *slog.Logger) DecisionClient { factoryCalls++; return client }
	before := s.statusMap()
	w := snapshotRequest(s, settingsJSON(testSettings()))
	out := decodeStream(t, w)
	if !out.DryRun || out.Model != "clef" || out.Cutoff != .5 || out.AssessableMin != .8 || len(out.Printers) != 4 || out.Done.Outcome != "completed" {
		t.Fatalf("response %+v", out)
	}
	sort.Strings(captures)
	if !reflect.DeepEqual(captures, []string{"00M1", "01S1", "0391"}) || client.calls != 2 || factoryCalls != 1 {
		t.Fatalf("captures %v calls %d", captures, client.calls)
	}
	for i, p := range printers {
		if out.Printers[i].Serial != p.Serial {
			t.Fatal("start order changed")
		}
		row := out.Results[p.Serial]
		if i == 3 {
			if row.Decision != "skipped" || row.ErrorCode != "unsupported_camera" || row.Image != nil || row.PClear != nil {
				t.Fatalf("unsupported row %+v", row)
			}
		} else if i == 0 {
			if row.Decision != "error" || row.PClear != nil || row.Image != nil {
				t.Fatalf("error row %+v", row)
			}
		} else {
			decoded, err := base64.StdEncoding.DecodeString(row.Image.Data)
			if err != nil || !bytes.Equal(decoded, data) || row.Decision != "would_stop" || row.FrameSequence != 7 || row.Image.Width != 8 || row.Image.Height != 6 {
				t.Fatalf("evaluated row %+v", row)
			}
		}
	}
	if commands.count() != 0 || len(commands.lights) != 0 || !reflect.DeepEqual(before, s.statusMap()) {
		t.Fatal("diagnostics changed controls/status")
	}
	for _, p := range printers {
		if len(s.activity.Recent(p.Serial)) != 0 || s.workers[p.Serial].processed != 0 {
			t.Fatal("diagnostics changed automatic markers/activity")
		}
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("headers")
	}
}

func TestSnapshotKeepsExactUploadedJPEGWithTLSProvider(t *testing.T) {
	var uploaded []byte
	calls := 0
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var in requestInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
		}
		if len(in.Images) != 1 {
			t.Error("not one image")
			return
		}
		uploaded, _ = base64.StdEncoding.DecodeString(in.Images[0].Base64)
		_, _ = io.WriteString(w, resultJSON(.127, .962))
	}))
	defer provider.Close()
	settings := testSettings()
	settings.Endpoint = provider.URL
	data := testJPEG(t)
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		return Frame{JPEG: data, Seq: 7, Captured: after.Add(time.Nanosecond), Width: 8, Height: 6}, nil
	})
	s := New([]config.Printer{{Serial: "01S1", Model: "P1S"}}, settings, nil, frames, nil, &fakeCommands{}, nil)
	s.newClient = func(settings Settings, log *slog.Logger) DecisionClient {
		c := NewClient(settings, log)
		c.http.Transport = provider.Client().Transport
		return c
	}
	out := decodeStream(t, snapshotRequest(s, settingsJSON(settings)))
	decoded, err := base64.StdEncoding.DecodeString(out.Results["01S1"].Image.Data)
	if err != nil || calls != 1 || !bytes.Equal(decoded, uploaded) || !bytes.Equal(decoded, data) {
		t.Fatal("diagnostic image differs from upload")
	}
}

func TestSnapshotValidationBeforeActivityAndEffectiveSettings(t *testing.T) {
	captures, factory := 0, 0
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		captures++
		return Frame{JPEG: testJPEG(t), Captured: after.Add(time.Nanosecond)}, nil
	})
	running := testSettings()
	running.Enabled = new(false)
	running.APIKey = "running-key"
	s := New([]config.Printer{{Serial: "01S1", Model: "P1S"}}, running, nil, frames, nil, &fakeCommands{}, nil)
	var got Settings
	s.newClient = func(settings Settings, log *slog.Logger) DecisionClient {
		factory++
		got = settings
		return &fakeDecision{result: Result{PClear: .9, POccupied: 1 - .9, PAssessable: .9}}
	}
	for _, raw := range [][]byte{[]byte(`null`), []byte(`{"unknown":1}`), []byte(`{} {}`), []byte(strings.Repeat("x", (16<<10)+1))} {
		w := snapshotRequest(s, raw)
		if w.Code != 400 {
			t.Fatalf("malformed HTTP %d", w.Code)
		}
	}
	submitted := testSettings()
	submitted.APIKey = ""
	submitted.Enabled = new(false)
	submitted.Model = "clef-flash"
	submitted.StopConfidence = .73
	out := decodeStream(t, snapshotRequest(s, settingsJSON(submitted)))
	if out.Model != "clef-flash" || out.Cutoff != .73 || got.Key() != "running-key" {
		t.Fatalf("effective settings %+v", got)
	}
}

func TestSnapshotNoSupportedCamerasMakesNoCapture(t *testing.T) {
	frames := frameFunc(func(context.Context, string, time.Time) (Frame, error) {
		t.Fatal("capture without supported cameras")
		return Frame{}, nil
	})
	s := New([]config.Printer{{Serial: "unknown", Model: "UNKNOWN"}}, testSettings(), nil, frames, nil, &fakeCommands{}, nil)
	s.newClient = func(Settings, *slog.Logger) DecisionClient { t.Fatal("client without supported cameras"); return nil }
	out := decodeStream(t, snapshotRequest(s, settingsJSON(testSettings())))
	if len(out.Printers) != 1 || out.Printers[0].Status != "unsupported" || out.Results["unknown"].Decision != "skipped" || out.Results["unknown"].ErrorCode != "unsupported_camera" {
		t.Fatalf("none %+v", out)
	}
}

func TestSnapshotInferenceFailureRetainsImageAndSafeErrors(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.s.newClient = func(Settings, *slog.Logger) DecisionClient {
		return &fakeDecision{err: errors.New("secret-endpoint credential-marker\nforged")}
	}
	out := decodeStream(t, snapshotRequest(f.s, settingsJSON(testSettings())))
	row := out.Results["01S1"]
	if row.Image == nil || row.PClear != nil || row.Decision != "error" || row.ErrorCode != "endpoint_unreachable" || strings.Contains(row.Error, "marker") || strings.Contains(row.Error, "forged") {
		t.Fatalf("error row %+v", row)
	}
}

func TestSnapshotCancellationStopsUploads(t *testing.T) {
	var captures, evaluations atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := frameFunc(func(ctx context.Context, _ string, _ time.Time) (Frame, error) {
		captures.Add(1)
		cancel()
		return Frame{}, ctx.Err()
	})
	s := New([]config.Printer{{Serial: "01S1", Model: "P1S"}, {Serial: "01S2", Model: "P1S"}}, testSettings(), nil, frames, nil, &fakeCommands{}, nil)
	s.newClient = func(Settings, *slog.Logger) DecisionClient { return &fakeDecision{hook: func() { evaluations.Add(1) }} }
	req := httptest.NewRequest("POST", "/platecheck/snapshots", bytes.NewReader(settingsJSON(testSettings()))).WithContext(ctx)
	w := httptest.NewRecorder()
	s.handleSnapshots(w, req)
	out := decodeStream(t, w)
	if captures.Load() != 2 || evaluations.Load() != 0 || len(out.Results) != 2 || out.Done.Outcome != "canceled" {
		t.Fatalf("canceled response %+v captures=%d uploads=%d", out, captures.Load(), evaluations.Load())
	}
	for _, row := range out.Results {
		if row.ErrorCode != "canceled" || row.Decision != "error" {
			t.Fatalf("canceled row %+v", row)
		}
	}
}

func TestSnapshotLogsSafeScoresAndMetadata(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	data := testJPEG(t)
	settings := testSettings()
	settings.APIKey = "credential-marker"
	settings.Endpoint = "https://endpoint-marker.example/accounts/account-marker"
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		return Frame{JPEG: data, Seq: 7, Captured: after.Add(time.Millisecond), Width: 8, Height: 6}, nil
	})
	s := New([]config.Printer{{Serial: "01S1", Model: "P1S"}}, settings, nil, frames, nil, &fakeCommands{}, logger)
	s.newClient = func(Settings, *slog.Logger) DecisionClient {
		return &fakeDecision{result: Result{PClear: .127, POccupied: 1 - .127, PAssessable: .962}}
	}
	_ = decodeStream(t, snapshotRequest(s, settingsJSON(settings)))
	for _, marker := range []string{"credential-marker", "endpoint-marker", "account-marker", base64.StdEncoding.EncodeToString(data)} {
		if strings.Contains(logs.String(), marker) {
			t.Fatalf("log leak %s", marker)
		}
	}
	for _, expected := range []string{"origin=platecheck", "operation=", "p_clear=0.127", "p_assessable=0.962", "frame_sequence=7", "width=8", "height=6", "image_bytes=", "snapshot test started", "snapshot test ended"} {
		if !strings.Contains(logs.String(), expected) {
			t.Fatalf("missing %s: %s", expected, logs.String())
		}
	}
}

func TestDiagnosticPOSTsNeverMoveSavedKeys(t *testing.T) {
	var contacts atomic.Int32
	attacker := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contacts.Add(1)
		_, _ = io.WriteString(w, resultJSON(.1, .9))
	}))
	defer attacker.Close()
	base := testSettings()
	base.Enabled = new(false)
	path := filepath.Join(t.TempDir(), "config.yaml")
	file := "platecheck:\n  enabled: false\n  provider: custom\n  endpoint: " + base.Endpoint + "\n  api_key: stored-secret\n  model: clef\n  stop_confidence: 0.5\n"
	if err := os.WriteFile(path, []byte(file), 0600); err != nil {
		t.Fatal(err)
	}
	store := configui.NewStore(path)
	mux := http.NewServeMux()
	store.Register(mux)
	oldProbe := probe
	defer func() { probe = oldProbe }()
	probe = func(ctx context.Context, settings Settings) error {
		client := NewClient(settings, nil)
		client.http.Transport = attacker.Client().Transport
		return client.Probe(ctx)
	}
	submitted := base
	submitted.Endpoint = attacker.URL
	submitted.APIKey = ""
	keyRequest := httptest.NewRequest("POST", "/config/platecheck/test", bytes.NewReader(settingsJSON(submitted)))
	keyResponse := httptest.NewRecorder()
	mux.ServeHTTP(keyResponse, keyRequest)
	if keyResponse.Code != 422 || !strings.Contains(keyResponse.Body.String(), endpointKeyMessage) {
		t.Fatalf("key route HTTP %d: %s", keyResponse.Code, keyResponse.Body.String())
	}
	captures := 0
	frames := frameFunc(func(context.Context, string, time.Time) (Frame, error) { captures++; return Frame{}, nil })
	s := New([]config.Printer{{Serial: "01S1", Model: "P1S"}}, base, nil, frames, nil, &fakeCommands{}, nil)
	s.newClient = func(settings Settings, log *slog.Logger) DecisionClient {
		client := NewClient(settings, log)
		client.http.Transport = attacker.Client().Transport
		return client
	}
	response := snapshotRequest(s, settingsJSON(submitted))
	if response.Code != 422 || !strings.Contains(response.Body.String(), endpointKeyMessage) {
		t.Fatalf("snapshot HTTP %d: %s", response.Code, response.Body.String())
	}
	if contacts.Load() != 0 || captures != 0 {
		t.Fatalf("credential attack caused contacts=%d captures=%d", contacts.Load(), captures)
	}
}

func TestDiagnosticCredentialBindingAllowsTypedKeyAndStoredFallback(t *testing.T) {
	base := testSettings()
	in := settingsView{Provider: ProviderCustom, Endpoint: "https://new.example/check", APIKey: "typed-key", Model: "clef", StopConfidence: .5, FirstLayer: FirstLayerSettings{PauseConfidence: DefaultPauseConfidence}}
	effective, err := resolveTestSettings(in, base)
	if err != nil || effective.APIKey != "typed-key" || effective.Endpoint != in.Endpoint {
		t.Fatalf("explicit destination %+v %v", effective, err)
	}
	in.APIKey = ""
	in.Endpoint = "  " + base.Endpoint + " "
	effective, err = resolveTestSettings(in, base)
	if err != nil || effective.Key() != base.Key() {
		t.Fatalf("trimmed endpoint binding %+v %v", effective, err)
	}
	in.Endpoint = "https://attacker.example/check"
	if _, err := resolveTestSettings(in, base); err == nil || err.Error() != endpointKeyMessage {
		t.Fatalf("stored key followed another endpoint: %v", err)
	}
}

func TestSnapshotStreamSkipsOfflineAndRunsInParallel(t *testing.T) {
	printers := []config.Printer{{Serial: "01S1", Model: "P1S"}, {Serial: "01S2", Model: "P1S"}, {Serial: "01S3", Model: "P1S"}}
	data := testJPEG(t)
	started := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		if serial == "01S3" {
			t.Error("offline printer captured")
			return Frame{}, errors.New("offline")
		}
		deadline, ok := ctx.Deadline()
		if !ok || deadline.After(time.Now().Add(snapshotCaptureTimeout)) {
			t.Error("missing short capture deadline")
		}
		started <- serial
		select {
		case <-release:
			return Frame{JPEG: data, Seq: 7, Captured: after.Add(time.Nanosecond)}, nil
		case <-ctx.Done():
			return Frame{}, ctx.Err()
		}
	})
	s := New(printers, testSettings(), nil, frames, nil, &fakeCommands{offline: map[string]bool{"01S3": true}}, nil)
	client := &fakeDecision{result: Result{PClear: .9, PAssessable: .9}}
	factories := 0
	s.newClient = func(Settings, *slog.Logger) DecisionClient { factories++; return client }
	srv := httptest.NewServer(http.HandlerFunc(s.handleSnapshots))
	defer func() { unblock(); srv.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, bytes.NewReader(settingsJSON(testSettings())))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/x-ndjson" || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("stream headers: %d %v", resp.StatusCode, resp.Header)
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	captured := make([]string, 0, 2)
	for range 2 {
		select {
		case serial := <-started:
			captured = append(captured, serial)
		case <-timer.C:
			t.Fatal("online captures did not start in parallel")
		}
	}
	sort.Strings(captured)
	if !reflect.DeepEqual(captured, []string{"01S1", "01S2"}) {
		t.Fatalf("captures %v", captured)
	}
	reader := bufio.NewReader(resp.Body)
	first, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var start snapshotStart
	if err := json.Unmarshal(first, &start); err != nil {
		t.Fatal(err)
	}
	if start.Type != "start" || len(start.Printers) != 3 || start.Printers[0].Status != "checking" || start.Printers[1].Status != "checking" || start.Printers[2].Status != "offline" {
		t.Fatalf("start %+v", start)
	}
	second, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var skipped snapshotResult
	if err := json.Unmarshal(second, &skipped); err != nil {
		t.Fatal(err)
	}
	if skipped.Type != "result" || skipped.Printer.Serial != "01S3" || skipped.Printer.Decision != "skipped" || skipped.Printer.ErrorCode != "printer_offline" {
		t.Fatalf("skipped %+v", skipped)
	}
	unblock()
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	_, _ = w.Write(append(append(first, second...), rest...))
	out := decodeStream(t, w)
	if out.Done.Outcome != "completed" || factories != 1 || client.calls != 2 {
		t.Fatalf("completion %+v, factories %d, calls %d", out.Done, factories, client.calls)
	}
	for _, serial := range captured {
		if out.Results[serial].Decision != "clear" || out.Results[serial].Image == nil {
			t.Fatalf("online row %+v", out.Results[serial])
		}
	}
}
