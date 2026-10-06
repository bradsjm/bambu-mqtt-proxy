package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/configui"
	"bambu-mqtt-proxy/internal/platecheck"
)

const platecheckContractSerial = "01S00CPLATE001"

// platecheckContractEndpoint is an HTTPS endpoint URL that fails fast: the
// diagnostics capture locally first, so no provider request can leave the
// test process.
const platecheckContractEndpoint = "https://127.0.0.1:1/ai/run/@cf/cloudflare/clef"

// TestPlatecheckConfigTestRoute pins that the key-check route exists in
// setup mode and at runtime, rejects cross-site posts, and reports missing
// credentials with the fixed sentences.
func TestPlatecheckConfigTestRoute(t *testing.T) {
	platecheckContractCleanEnv(t)
	setupBase := platecheckContractStore(t, filepath.Join(t.TempDir(), "absent.yaml"))
	for _, tc := range []struct {
		name, site, body, message string
		code                      int
	}{
		{"setup missing endpoint", "same-origin", `{"enabled":true,"model":"clef","stop_confidence":0.5}`, "No Clef endpoint is configured. Enter an HTTPS endpoint or set BMBPX_PLATECHECK_ENDPOINT.", 422},
		{"setup cross-site", "cross-site", `{"api_key":"must-not-be-probed","model":"clef"}`, "", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := detectionContractRequest(t, http.MethodPost, setupBase+"/config/platecheck/test", tc.body, tc.site)
			if code != tc.code {
				t.Fatalf("probe = %d %s, want %d", code, body, tc.code)
			}
			if tc.message != "" && detectionContractObject(t, body)["error"] != tc.message {
				t.Fatalf("probe body = %s, want error %q", body, tc.message)
			}
		})
	}

	platecheckContractCleanEnv(t)
	base := platecheckContractServe(t, false, true)
	code, body := detectionContractRequest(t, http.MethodPost, base+"/config/platecheck/test", `{"enabled":true,"model":"clef","stop_confidence":0.5}`, "same-origin")
	if code != 422 {
		t.Fatalf("runtime probe = %d %s, want 422", code, body)
	}
	if detectionContractObject(t, body)["error"] != "No Clef endpoint is configured. Enter an HTTPS endpoint or set BMBPX_PLATECHECK_ENDPOINT." {
		t.Fatalf("runtime probe body = %s", body)
	}
}

// TestPlatecheckSnapshotsWhenCamerasOn pins the diagnostics contract while
// the feature runs: the response keeps the dry-run shape, configured order,
// and a safe error row when no camera frame is available.
func TestPlatecheckSnapshotsWhenCamerasOn(t *testing.T) {
	platecheckContractCleanEnv(t)
	base := platecheckContractServe(t, true, true)
	code, body := platecheckContractRequest(t, http.MethodPost, base+"/platecheck/snapshots", `{"enabled":true,"model":"clef","stop_confidence":0.5}`, "same-origin")
	if code != http.StatusOK {
		t.Fatalf("snapshots = %d %s, want 200", code, body)
	}
	payload := detectionContractObject(t, body)
	if payload["dry_run"] != true {
		t.Fatalf("dry_run = %#v, want true", payload["dry_run"])
	}
	if payload["cameras"] != "available" {
		t.Fatalf("cameras = %#v, want available", payload["cameras"])
	}
	if payload["model"] != "clef" || payload["cutoff"] != 0.5 {
		t.Fatalf("model/cutoff = %#v/%#v, want clef/0.5", payload["model"], payload["cutoff"])
	}
	rows, ok := payload["printers"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("printers = %#v, want one row", payload["printers"])
	}
	row := rows[0].(map[string]any)
	if row["serial"] != platecheckContractSerial {
		t.Fatalf("row serial = %#v, want %s", row["serial"], platecheckContractSerial)
	}
	if code, _ := row["error_code"].(string); code == "" {
		t.Fatalf("unavailable camera row lacks a fixed error code: %s", body)
	}
	if row["p_clear"] != nil || row["p_occupied"] != nil || row["p_assessable"] != nil {
		t.Fatalf("failed row carries probabilities: %s", body)
	}
}

// TestPlatecheckSnapshotsWhileOff pins that the diagnostics route stays
// mounted while the feature is off and cameras serve, using submitted
// values without any stored or environment credential.
func TestPlatecheckSnapshotsWhileOff(t *testing.T) {
	platecheckContractCleanEnv(t)
	base := platecheckContractServe(t, false, true)
	body := fmt.Sprintf(`{"enabled":false,"endpoint":%q,"api_key":"contract-dummy-key","model":"clef","stop_confidence":0.5}`,
		platecheckContractEndpoint)
	code, raw := platecheckContractRequest(t, http.MethodPost, base+"/platecheck/snapshots", body, "same-origin")
	if code != http.StatusOK {
		t.Fatalf("snapshots while off = %d %s, want 200", code, raw)
	}
	payload := detectionContractObject(t, raw)
	if payload["dry_run"] != true {
		t.Fatalf("dry_run = %#v, want true", payload["dry_run"])
	}
	if _, exists := payload["printers"]; !exists {
		t.Fatalf("snapshots answer lacks printers: %s", raw)
	}
	// The off feature reports the status-only disabled state.
	waitFor(t, 10*time.Second, func() bool {
		_, raw := detectionContractRequest(t, http.MethodGet, base+"/status", "", "")
		states, ok := detectionContractObject(t, raw)["platecheck"].(map[string]any)
		if !ok {
			return false
		}
		state, ok := states[platecheckContractSerial].(map[string]any)
		return ok && state["state"] == "disabled"
	})
}

// TestPlatecheckAbsentWhenCamerasOff pins that the enabled feature with the
// camera feature disabled stays blocked: no diagnostics route, and the
// status reports the blocked state.
func TestPlatecheckAbsentWhenCamerasOff(t *testing.T) {
	platecheckContractCleanEnv(t)
	base := platecheckContractServe(t, true, false)
	if code, body := platecheckContractRequest(t, http.MethodPost, base+"/platecheck/snapshots", `{"enabled":true,"model":"clef","stop_confidence":0.5}`, "same-origin"); code != 404 {
		t.Fatalf("blocked snapshots = %d %s, want 404", code, body)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, raw := detectionContractRequest(t, http.MethodGet, base+"/status", "", "")
		states, ok := detectionContractObject(t, raw)["platecheck"].(map[string]any)
		if !ok {
			return false
		}
		state, ok := states[platecheckContractSerial].(map[string]any)
		return ok && state["state"] == "blocked"
	})
}

// TestPlatecheckRouteAfterReload pins that a saved configuration serves the
// diagnostics route again on the fresh camera manager instead of retaining
// the previous binding.
func TestPlatecheckRouteAfterReload(t *testing.T) {
	platecheckContractCleanEnv(t)
	t.Setenv("BMBPX_CAMERA_ENABLED", "true")
	t.Setenv(platecheck.EnvEndpoint, platecheckContractEndpoint)
	t.Setenv(platecheck.EnvAPIKey, "contract-dummy-key")
	httpPort := freePort(t)
	listenPort := freePort(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	fixture := fmt.Sprintf(`listen:
  - port: %d
    tls: false
printers:
  - serial: %q
    model: P1S
    address: "127.0.0.1:1"
    password: "00008888"
http:
  port: %d
log:
  level: error
`, listenPort, platecheckContractSerial, httpPort)
	if err := os.WriteFile(path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	// Like reload_stream_test.go, the serve loop must not run in parallel
	// with other fixtures: cameras bind the fixed raw camera port 6000.
	ctx, cancel := context.WithCancel(context.Background())
	serving := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-serving:
			if err != nil {
				t.Errorf("serve loop: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("serve loop did not stop within 15 seconds")
		}
	})
	store := configui.NewStore(path)
	go func() {
		for {
			next, err := serveOnce(ctx, path, "error", store)
			if err != nil || next == nil {
				serving <- err
				return
			}
		}
	}()
	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	waitFor(t, 15*time.Second, func() bool { return probeOK(base + "/livez") })

	// Save the unchanged view through the real endpoint to trigger reload.
	// The body is the raw config member, like the page sends it, with the
	// redacted printer access code supplied again.
	getResp, err := http.Get(base + "/config/api")
	if err != nil {
		t.Fatalf("GET /config/api: %v", err)
	}
	var doc struct {
		Config json.RawMessage `json:"config"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&doc); err != nil {
		t.Fatalf("decode /config/api: %v", err)
	}
	getResp.Body.Close()
	var cfg map[string]any
	if err := json.Unmarshal(doc.Config, &cfg); err != nil {
		t.Fatalf("decode config member: %v", err)
	}
	printers, ok := cfg["printers"].([]any)
	if !ok || len(printers) != 1 {
		t.Fatalf("config printers = %#v, want one printer", cfg["printers"])
	}
	printer := printers[0].(map[string]any)
	printer["access_code"] = "00008888"
	body, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	putReq, err := http.NewRequest(http.MethodPut, base+"/config/api", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	putReq.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT /config/api: %v", err)
	}
	putBody, _ := io.ReadAll(putResp.Body)
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT /config/api = %d, body: %s", putResp.StatusCode, putBody)
	}

	// The next /livez answer is the restarted server; the diagnostics route
	// must serve from the fresh camera binding.
	waitFor(t, 15*time.Second, func() bool { return probeOK(base + "/livez") })
	code, raw := platecheckContractRequest(t, http.MethodPost, base+"/platecheck/snapshots", `{"enabled":true,"model":"clef","stop_confidence":0.5}`, "same-origin")
	if code != http.StatusOK {
		t.Fatalf("snapshots after reload = %d %s, want 200", code, raw)
	}
	payload := detectionContractObject(t, raw)
	if payload["dry_run"] != true {
		t.Fatalf("after-reload dry_run = %#v, want true", payload["dry_run"])
	}
}

// platecheckContractServe starts one full serveOnce with the camera feature
// and, when enabled, the plate-check environment credentials. Like
// detectionContractServe, it must not run in parallel with other fixtures.
func platecheckContractServe(t *testing.T, feature, cameras bool) string {
	t.Helper()
	platecheckContractCleanEnv(t)
	t.Setenv("BMBPX_CAMERA_ENABLED", fmt.Sprint(cameras))
	if feature {
		t.Setenv(platecheck.EnvEndpoint, platecheckContractEndpoint)
		t.Setenv(platecheck.EnvAPIKey, "contract-dummy-key")
	}
	port := freePort(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	fixture := fmt.Sprintf(`listen:
  - port: %d
    tls: false
printers:
  - serial: %q
    model: P1S
    address: "127.0.0.1:1"
    password: "00008888"
http:
  port: %d
log:
  level: error
`, freePort(t), platecheckContractSerial, port)
	if err := os.WriteFile(path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := serveOnce(ctx, path, "error", configui.NewStore(path))
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serveOnce: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("serveOnce did not stop within 15 seconds")
		}
	})
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitFor(t, 15*time.Second, func() bool { return probeOK(base + "/livez") })
	return base
}

// platecheckContractRequest performs one request with a budget that covers
// the bounded camera-capture wait of the diagnostics handler.
func platecheckContractRequest(t *testing.T, method, url, body, site string) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if site != "" {
		req.Header.Set("Sec-Fetch-Site", site)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, raw
}

// platecheckContractStore serves one configuration store, like setup mode.
func platecheckContractStore(t *testing.T, path string) string {
	t.Helper()
	mux := http.NewServeMux()
	configui.NewStore(path).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// platecheckContractCleanEnv removes inherited BMBPX_* overrides for the
// test duration, so fixture settings cannot be replaced.
func platecheckContractCleanEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "BMBPX_") {
			t.Setenv(name, "")
			if err := os.Unsetenv(name); err != nil {
				t.Fatal(err)
			}
		}
	}
}
