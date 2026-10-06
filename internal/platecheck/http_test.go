package platecheck

import (
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
	"strings"
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
func decodeSnapshots(t *testing.T, w *httptest.ResponseRecorder) snapshotResponse {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("HTTP %d %s", w.Code, w.Body.String())
	}
	var out snapshotResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSnapshotRowsConfiguredOrderFailureContinuationAndNoControlEffects(t *testing.T) {
	unsetEnv(t)
	printers := []config.Printer{{Serial: "01S1", Name: "First", Model: "P1S"}, {Serial: "00M1", Name: "Second", Model: "X1C"}, {Serial: "0391", Name: "Third", Model: "A1"}, {Serial: "dead", Name: "Unsupported", Model: "UNKNOWN"}}
	clock := &testClock{t: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}
	data := testJPEG(t)
	captures := []string{}
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		captures = append(captures, serial)
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
	out := decodeSnapshots(t, w)
	if !out.DryRun || out.Cameras != "available" || out.Model != "clef" || out.Cutoff != .5 || out.AssessableMin != .8 || len(out.Printers) != 4 {
		t.Fatalf("response %+v", out)
	}
	if !reflect.DeepEqual(captures, []string{"01S1", "00M1", "0391"}) || client.calls != 2 || factoryCalls != 1 {
		t.Fatalf("captures %v calls %d", captures, client.calls)
	}
	for i, row := range out.Printers {
		if row.Serial != printers[i].Serial {
			t.Fatal("order changed")
		}
		if i == 0 || i == 3 {
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
	if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Type") != "application/json" {
		t.Fatal("headers")
	}
}

func TestSnapshotKeepsExactUploadedJPEGWithTLSProvider(t *testing.T) {
	unsetEnv(t)
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
	s := New([]config.Printer{{Serial: "01S1", Model: "P1S"}}, settings, nil, frames, nil, nil, nil)
	s.newClient = func(settings Settings, log *slog.Logger) DecisionClient {
		c := NewClient(settings, log)
		c.http.Transport = provider.Client().Transport
		return c
	}
	out := decodeSnapshots(t, snapshotRequest(s, settingsJSON(settings)))
	decoded, err := base64.StdEncoding.DecodeString(out.Printers[0].Image.Data)
	if err != nil || calls != 1 || !bytes.Equal(decoded, uploaded) || !bytes.Equal(decoded, data) {
		t.Fatal("diagnostic image differs from upload")
	}
}

func TestSnapshotValidationBeforeActivityAndEffectiveSettings(t *testing.T) {
	unsetEnv(t)
	captures, factory := 0, 0
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		captures++
		return Frame{JPEG: testJPEG(t), Captured: after.Add(time.Nanosecond)}, nil
	})
	running := testSettings()
	running.Enabled = new(false)
	running.APIKey = "running-key"
	s := New([]config.Printer{{Serial: "01S1", Model: "P1S"}}, running, nil, frames, nil, nil, nil)
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
	out := decodeSnapshots(t, snapshotRequest(s, settingsJSON(submitted)))
	if out.Model != "clef-flash" || out.Cutoff != .73 || got.Key() != "running-key" {
		t.Fatalf("effective settings %+v", got)
	}
	t.Setenv(EnvEndpoint, "https://env.example/evaluate")
	t.Setenv(EnvAPIKey, "env-key")
	_ = decodeSnapshots(t, snapshotRequest(s, settingsJSON(submitted)))
	if got.Endpoint != "https://env.example/evaluate" || got.Key() != "env-key" {
		t.Fatalf("env precedence %+v", got)
	}
	t.Setenv(EnvAPIKey, "")
	w := snapshotRequest(s, settingsJSON(submitted))
	if w.Code != 422 || captures != 2 || factory != 2 || !strings.Contains(w.Body.String(), "set but empty") {
		t.Fatalf("validation side effect: %d %d %d %s", w.Code, captures, factory, w.Body.String())
	}
}

func TestSnapshotNoSupportedCamerasMakesNoCapture(t *testing.T) {
	unsetEnv(t)
	frames := frameFunc(func(context.Context, string, time.Time) (Frame, error) {
		t.Fatal("capture without supported cameras")
		return Frame{}, nil
	})
	s := New([]config.Printer{{Serial: "unknown", Model: "UNKNOWN"}}, testSettings(), nil, frames, nil, nil, nil)
	s.newClient = func(Settings, *slog.Logger) DecisionClient { t.Fatal("client without supported cameras"); return nil }
	out := decodeSnapshots(t, snapshotRequest(s, settingsJSON(testSettings())))
	if out.Cameras != "none" || len(out.Printers) != 0 {
		t.Fatalf("none %+v", out)
	}
}

func TestSnapshotInferenceFailureRetainsImageAndSafeErrors(t *testing.T) {
	unsetEnv(t)
	f := newFixture(t, "PREPARE")
	f.s.newClient = func(Settings, *slog.Logger) DecisionClient {
		return &fakeDecision{err: errors.New("secret-endpoint credential-marker\nforged")}
	}
	out := decodeSnapshots(t, snapshotRequest(f.s, settingsJSON(testSettings())))
	row := out.Printers[0]
	if row.Image == nil || row.PClear != nil || row.Decision != "error" || row.ErrorCode != "endpoint_unreachable" || strings.Contains(row.Error, "marker") || strings.Contains(row.Error, "forged") {
		t.Fatalf("error row %+v", row)
	}
}

func TestSnapshotCancellationStopsFurtherCapturesAndUploads(t *testing.T) {
	unsetEnv(t)
	captures, evaluations := 0, 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := frameFunc(func(context.Context, string, time.Time) (Frame, error) {
		captures++
		cancel()
		return Frame{}, context.Canceled
	})
	s := New([]config.Printer{{Serial: "01S1", Model: "P1S"}, {Serial: "01S2", Model: "P1S"}}, testSettings(), nil, frames, nil, nil, nil)
	s.newClient = func(Settings, *slog.Logger) DecisionClient { return &fakeDecision{hook: func() { evaluations++ }} }
	req := httptest.NewRequest("POST", "/platecheck/snapshots", bytes.NewReader(settingsJSON(testSettings()))).WithContext(ctx)
	w := httptest.NewRecorder()
	s.handleSnapshots(w, req)
	out := decodeSnapshots(t, w)
	if captures != 1 || evaluations != 0 || len(out.Printers) != 1 || out.Printers[0].ErrorCode != "canceled" {
		t.Fatalf("canceled response %+v captures=%d uploads=%d", out, captures, evaluations)
	}
}

func TestSnapshotLogsSafeScoresAndMetadata(t *testing.T) {
	unsetEnv(t)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	data := testJPEG(t)
	settings := testSettings()
	settings.APIKey = "credential-marker"
	settings.Endpoint = "https://endpoint-marker.example/accounts/account-marker"
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		return Frame{JPEG: data, Seq: 7, Captured: after.Add(time.Millisecond), Width: 8, Height: 6}, nil
	})
	s := New([]config.Printer{{Serial: "01S1", Model: "P1S"}}, settings, nil, frames, nil, nil, logger)
	s.newClient = func(Settings, *slog.Logger) DecisionClient {
		return &fakeDecision{result: Result{PClear: .127, POccupied: 1 - .127, PAssessable: .962}}
	}
	_ = decodeSnapshots(t, snapshotRequest(s, settingsJSON(settings)))
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

func TestDiagnosticPOSTsNeverMoveSavedOrEnvironmentKeys(t *testing.T) {
	for _, mode := range []string{"stored", "environment-key-only"} {
		t.Run(mode, func(t *testing.T) {
			unsetEnv(t)
			var contacts atomic.Int32
			attacker := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				contacts.Add(1)
				_, _ = io.WriteString(w, resultJSON(.1, .9))
			}))
			defer attacker.Close()
			base := testSettings()
			base.Enabled = new(false)
			if mode == "environment-key-only" {
				t.Setenv(EnvAPIKey, "environment-secret")
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			file := "platecheck:\n  enabled: false\n  endpoint: " + base.Endpoint + "\n  api_key: stored-secret\n  model: clef\n  stop_confidence: 0.5\n"
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
			s := New([]config.Printer{{Serial: "01S1", Model: "P1S"}}, base, nil, frames, nil, nil, nil)
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
		})
	}
}

func TestDiagnosticCredentialBindingAllowsTypedKeyAndPairedEnvironment(t *testing.T) {
	unsetEnv(t)
	base := testSettings()
	in := settingsView{Endpoint: "https://new.example/check", APIKey: "typed-key", Model: "clef", StopConfidence: .5}
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
	t.Setenv(EnvEndpoint, "https://configured.example/check")
	t.Setenv(EnvAPIKey, "paired-environment-key")
	in.Endpoint = "https://attacker.example/check"
	effective, err = resolveTestSettings(in, base)
	if err != nil || effective.Endpoint != "https://configured.example/check" || effective.APIKey != "paired-environment-key" {
		t.Fatalf("paired environment %+v %v", effective, err)
	}
}
