package main

import (
	"bytes"
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

// platecheckContractEndpoint is an HTTPS endpoint URL that fails fast.
const platecheckContractEndpoint = "https://127.0.0.1:1/check"

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
		{"setup missing account", "same-origin", `{"enabled":true,"model":"clef","stop_confidence":0.5}`, "No Cloudflare account ID is configured. Enter the account ID.", 422},
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
	if detectionContractObject(t, body)["error"] != "No Cloudflare account ID is configured. Enter the account ID." {
		t.Fatalf("runtime probe body = %s", body)
	}
}

// TestPlatecheckSnapshotsWhenCamerasOn pins the streamed diagnostics contract.
// The configured printer never connects, so the snapshot is skipped as offline.
func TestPlatecheckSnapshotsWhenCamerasOn(t *testing.T) {
	platecheckContractCleanEnv(t)
	base := platecheckContractServe(t, true, true)
	snapBody := fmt.Sprintf(`{"enabled":true,"provider":"custom","endpoint":%q,"api_key":"contract-dummy-key","model":"clef","stop_confidence":0.5}`,
		platecheckContractEndpoint)
	code, body := platecheckContractRequest(t, http.MethodPost, base+"/platecheck/snapshots", snapBody, "same-origin")
	if code != http.StatusOK {
		t.Fatalf("snapshots = %d %s, want 200", code, body)
	}
	events := platecheckContractEvents(t, body)
	if len(events) != 3 {
		t.Fatalf("events = %v, want start, result, done", events)
	}
	start := events[0]
	if start["dry_run"] != true || start["model"] != "clef" || start["cutoff"] != 0.5 {
		t.Fatalf("start = %#v", start)
	}
	printers, ok := start["printers"].([]any)
	if !ok || len(printers) != 1 {
		t.Fatalf("printers = %#v, want one printer", start["printers"])
	}
	printer := printers[0].(map[string]any)
	if printer["serial"] != platecheckContractSerial || printer["status"] != "offline" {
		t.Fatalf("printer = %#v", printer)
	}
	row := events[1]["printer"].(map[string]any)
	if events[1]["type"] != "result" || row["serial"] != platecheckContractSerial || row["decision"] != "skipped" || row["error_code"] != "printer_offline" {
		t.Fatalf("result = %#v", events[1])
	}
	if row["p_clear"] != nil || row["p_occupied"] != nil || row["p_assessable"] != nil || row["image"] != nil {
		t.Fatalf("skipped row carries evidence: %s", body)
	}
	if events[2]["outcome"] != "completed" {
		t.Fatalf("done = %#v", events[2])
	}
}

// TestPlatecheckSnapshotsWhileOff pins that the diagnostics route stays
// mounted while the feature is off and cameras serve, using submitted
// values without any stored credential.
func TestPlatecheckSnapshotsWhileOff(t *testing.T) {
	platecheckContractCleanEnv(t)
	base := platecheckContractServe(t, false, true)
	body := fmt.Sprintf(`{"enabled":false,"provider":"custom","endpoint":%q,"api_key":"contract-dummy-key","model":"clef","stop_confidence":0.5}`,
		platecheckContractEndpoint)
	code, raw := platecheckContractRequest(t, http.MethodPost, base+"/platecheck/snapshots", body, "same-origin")
	if code != http.StatusOK {
		t.Fatalf("snapshots while off = %d %s, want 200", code, raw)
	}
	payload := platecheckContractEvents(t, raw)[0]
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

// platecheckContractServe starts one full serve with the camera feature
// and, when enabled, the plate-check section credentials. Like
// detectionContractServe, it must not run in parallel with other fixtures.
func platecheckContractServe(t *testing.T, feature, cameras bool) string {
	t.Helper()
	platecheckContractCleanEnv(t)
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
camera:
  enabled: %t
`, freePort(t), platecheckContractSerial, port, cameras)
	if feature {
		fixture += fmt.Sprintf("platecheck:\n  enabled: true\n  provider: %s\n  endpoint: %q\n  api_key: contract-dummy-key\n",
			platecheck.ProviderCustom, platecheckContractEndpoint)
	}
	fixture += "log:\n  level: error\n"
	if err := os.WriteFile(path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	serveInProcess(t, path)
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

// platecheckContractEvents parses a complete NDJSON diagnostic stream.
func platecheckContractEvents(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
	events := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("invalid stream line %q: %v", line, err)
		}
		events = append(events, event)
	}
	if len(events) < 2 || events[0]["type"] != "start" || events[len(events)-1]["type"] != "done" {
		t.Fatalf("incomplete stream: %s", raw)
	}
	return events
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
