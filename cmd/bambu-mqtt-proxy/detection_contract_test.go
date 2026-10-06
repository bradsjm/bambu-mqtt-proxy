package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/configui"
	"bambu-mqtt-proxy/internal/jobpreview"
)

const detectionContractSerial = "01S00CDETECT001"
const detectionContractEnv = "BMBPX_OCTOEVERYWHERE_API_KEY"

// TestDetectionHTTPContract pins no-report detection across wall JSON, SSE, health, and protected per-print writes.
func TestDetectionHTTPContract(t *testing.T) {
	base := detectionContractServe(t, true, true)
	// An unreachable printer has no reports: today's idle fixture displays
	// starting/awaiting_reports, not the idle state of a reported idle print.
	want := map[string]any{"state": "starting", "reason": "awaiting_reports", "pause_state": "none", "enabled": true}
	waitFor(t, 10*time.Second, func() bool {
		_, raw := detectionContractRequest(t, http.MethodGet, base+"/camera/status", "", "")
		payload := detectionContractObject(t, raw)
		return detectionContractTile(t, payload)["detection"].(map[string]any)["state"] == "starting"
	})
	_, raw := detectionContractRequest(t, http.MethodGet, base+"/camera/status", "", "")
	payload := detectionContractObject(t, raw)
	detectionContractFleet(t, payload)
	detectionContractState(t, detectionContractTile(t, payload)["detection"], want)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/camera/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d", resp.StatusCode)
	}
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("first SSE event: %v", err)
		}
		if strings.HasPrefix(line, "data: ") {
			event := detectionContractObject(t, []byte(strings.TrimPrefix(line, "data: ")))
			detectionContractFleet(t, event)
			detectionContractState(t, detectionContractTile(t, event)["detection"], want)
			break
		}
	}
	resp.Body.Close()

	_, raw = detectionContractRequest(t, http.MethodGet, base+"/status", "", "")
	status := detectionContractObject(t, raw)
	states, ok := status["detection"].(map[string]any)
	if !ok || len(states) != 1 {
		t.Fatalf("/status detection = %#v, want exactly one printer", status["detection"])
	}
	detectionContractState(t, states[detectionContractSerial], want)

	for _, tc := range []struct {
		name, serial, body, site string
		code                     int
		jsonError                bool
	}{
		{"cross-site", detectionContractSerial, `{"enabled":false}`, "cross-site", 403, false},
		{"no active print", detectionContractSerial, `{"enabled":false}`, "same-origin", 409, true},
		{"unknown serial", "UNKNOWN", `{"enabled":false}`, "same-origin", 404, true},
		{"malformed", detectionContractSerial, `{"enabled":`, "same-origin", 400, true},
		{"unknown field", detectionContractSerial, `{"enabled":false,"extra":true}`, "same-origin", 400, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := detectionContractRequest(t, http.MethodPut, base+"/detection/"+tc.serial, tc.body, tc.site)
			if code != tc.code {
				t.Fatalf("PUT = %d %s, want %d", code, body, tc.code)
			}
			if tc.jsonError {
				if msg, ok := detectionContractObject(t, body)["error"].(string); !ok || msg == "" {
					t.Fatalf("error body = %s", body)
				}
			}
		})
	}
}

// TestDetectionOffHTTPContract pins omission and the disabled route's status without fixing its error representation.
func TestDetectionOffHTTPContract(t *testing.T) {
	base := detectionContractServe(t, false, true)
	_, raw := detectionContractRequest(t, http.MethodGet, base+"/camera/status", "", "")
	if tile := detectionContractTile(t, detectionContractObject(t, raw)); tile["detection"] != nil {
		t.Fatalf("disabled tile contains detection: %s", raw)
	} else if _, exists := tile["detection"]; exists {
		t.Fatal("disabled tile has a null detection member")
	}
	_, raw = detectionContractRequest(t, http.MethodGet, base+"/status", "", "")
	if _, exists := detectionContractObject(t, raw)["detection"]; exists {
		t.Fatalf("disabled /status contains detection: %s", raw)
	}
	if code, _ := detectionContractRequest(t, http.MethodPut, base+"/detection/"+detectionContractSerial, `{"enabled":false}`, "same-origin"); code != 404 {
		t.Fatalf("disabled PUT status = %d, want 404", code)
	}
}

// TestDetectionCameraDisabledHTTPContract pins the enabled feature's camera-disabled blocked health state.
func TestDetectionCameraDisabledHTTPContract(t *testing.T) {
	base := detectionContractServe(t, true, false)
	waitFor(t, 10*time.Second, func() bool {
		_, raw := detectionContractRequest(t, http.MethodGet, base+"/status", "", "")
		states := detectionContractObject(t, raw)["detection"].(map[string]any)
		return states[detectionContractSerial].(map[string]any)["state"] == "blocked"
	})
	_, raw := detectionContractRequest(t, http.MethodGet, base+"/status", "", "")
	states := detectionContractObject(t, raw)["detection"].(map[string]any)
	detectionContractState(t, states[detectionContractSerial], map[string]any{
		"state": "blocked", "reason": "camera_disabled", "pause_state": "none", "enabled": true,
		"message": "detection requires the camera feature",
	})
}

// TestDetectionConfigHTTPShape pins the exact public member set and fills the no-key and nonempty-override coverage gaps.
func TestDetectionConfigHTTPShape(t *testing.T) {
	// Existing stable HTTP tests in internal/configui/configui_test.go cover
	// effective enabled/has_api_key values and secret redaction for stored,
	// environment, and empty keys (TestDetectionGetRedactsAndReportsEffectiveKey),
	// empty override metadata (TestDetectionEnvOverrideMetaReportsEmpty), and
	// non-persistence of env secrets (TestDetectionEnvSecretIsNeverPersisted).
	for _, tc := range []struct {
		name, fileKey, envKey string
		envPresent            bool
	}{
		{"stored", "contract-stored-secret", "", false},
		{"environment", "", "contract-env-secret", true},
		{"empty environment", "contract-stored-secret", "", true},
		{"no key", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			detectionContractCleanEnv(t)
			path := filepath.Join(t.TempDir(), "config.yaml")
			if tc.fileKey != "" {
				if err := os.WriteFile(path, []byte(fmt.Sprintf("detection:\n  api_key: %q\n", tc.fileKey)), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.envPresent {
				t.Setenv(detectionContractEnv, tc.envKey)
			}
			base := detectionContractStore(t, path)
			_, raw := detectionContractRequest(t, http.MethodGet, base+"/config/api", "", "")
			doc := detectionContractObject(t, raw)
			d := doc["config"].(map[string]any)["detection"].(map[string]any)
			if len(d) != 2 {
				t.Fatalf("detection = %#v, want only enabled and has_api_key", d)
			}
			for _, key := range []string{"enabled", "has_api_key"} {
				if _, ok := d[key].(bool); !ok {
					t.Fatalf("detection.%s = %#v, want boolean", key, d[key])
				}
			}
			if tc.name == "no key" && !reflect.DeepEqual(d, map[string]any{"enabled": false, "has_api_key": false}) {
				t.Fatalf("no-key detection = %#v", d)
			}
			if tc.envKey != "" {
				overrides := doc["meta"].(map[string]any)["env_overrides"].(map[string]any)
				if overrides["detection_api_key"] != detectionContractEnv {
					t.Fatalf("nonempty key override metadata = %#v", overrides)
				}
			}
		})
	}
}

// TestDetectionConfigProbePreflight pins missing-key messages and cross-origin rejection before any Gadget request.
func TestDetectionConfigProbePreflight(t *testing.T) {
	detectionContractCleanEnv(t)
	base := detectionContractStore(t, filepath.Join(t.TempDir(), "absent.yaml"))
	for _, tc := range []struct {
		name, site, body, message string
		code                      int
		emptyEnv                  bool
	}{
		{"no key", "same-origin", `{}`, "No Gadget API key is configured. Enter a key or set BMBPX_OCTOEVERYWHERE_API_KEY.", 422, false},
		{"cross-site", "cross-site", `{"api_key":"must-not-be-probed"}`, "", 403, false},
		{"empty override", "same-origin", `{"api_key":"must-not-be-probed"}`, "BMBPX_OCTOEVERYWHERE_API_KEY is set but empty, so there is no key to test. Set the variable to a valid key.", 422, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.emptyEnv {
				t.Setenv(detectionContractEnv, "")
			}
			code, body := detectionContractRequest(t, http.MethodPost, base+"/config/detection/test", tc.body, tc.site)
			if code != tc.code {
				t.Fatalf("probe = %d %s, want %d", code, body, tc.code)
			}
			if tc.message != "" && detectionContractObject(t, body)["error"] != tc.message {
				t.Fatalf("probe body = %s, want error %q", body, tc.message)
			}
		})
	}
}

// TestDetectionUnchangedConfigBytes pins byte-identical page saves with sorted module sections and a stored secret.
func TestDetectionUnchangedConfigBytes(t *testing.T) {
	detectionContractCleanEnv(t)
	// Keep the page-save fixture independent of the serializer. Module-owned
	// sections are sorted, so detection precedes notifications.
	const fixture = `# bambu-mqtt-proxy configuration, written by the /config page.
# Comments are not preserved when the page saves this file.
listen:
    - port: 8883
      tls: true
auth:
    mode: printer
printers: []
behavior:
    warmup_commands:
        - '{"pushing":{"sequence_id":"0","command":"pushall"}}'
    upstream_keepalive_seconds: 30
    upstream_connect_timeout_seconds: 5
    upstream_backoff_initial_seconds: 1
    upstream_backoff_max_seconds: 30
log:
    level: info
http:
    port: 8080
camera:
    enabled: true
mcp:
    enabled: true
detection:
    enabled: true
    api_key: contract-stored-secret
notifications:
    enabled: false
    provider: pushover
    pushover:
        app_token: ""
        user_key: ""
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	base := detectionContractStore(t, path)
	_, raw := detectionContractRequest(t, http.MethodGet, base+"/config/api", "", "")
	view, err := json.Marshal(detectionContractObject(t, raw)["config"])
	if err != nil {
		t.Fatal(err)
	}
	code, body := detectionContractRequest(t, http.MethodPut, base+"/config/api", string(view), "same-origin")
	if code != 200 {
		t.Fatalf("save = %d %s", code, body)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal([]byte(fixture), after) {
		t.Fatalf("unchanged save changed file bytes:\nbefore: %s\nafter: %s", fixture, after)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("saved file mode: info=%v error=%v, want 0600", info, err)
	}
}

func detectionContractCleanEnv(t *testing.T) {
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

func detectionContractStore(t *testing.T, path string) string {
	t.Helper()
	mux := http.NewServeMux()
	configui.NewStore(path).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

func detectionContractServe(t *testing.T, enabled, cameras bool, previews ...bool) string {
	t.Helper()
	detectionContractCleanEnv(t)
	if enabled {
		t.Setenv(detectionContractEnv, "contract-dummy-key")
	}
	t.Setenv("BMBPX_CAMERA_ENABLED", fmt.Sprint(cameras))
	if len(previews) > 0 {
		t.Setenv(jobpreview.EnvSwitch, fmt.Sprint(previews[0]))
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
`, freePort(t), detectionContractSerial, port)
	if err := os.WriteFile(path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	// Like reload_stream_test.go, do not run serveOnce fixtures in parallel:
	// cameras bind the fixed raw camera port 6000.
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

func detectionContractRequest(t *testing.T, method, url, body, site string) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	if method == http.MethodGet && resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d %s", url, resp.StatusCode, raw)
	}
	return resp.StatusCode, raw
}

func detectionContractObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		t.Fatalf("JSON object %s: %v", raw, err)
	}
	return out
}

func detectionContractTile(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	printers, ok := payload["printers"].([]any)
	if !ok || len(printers) != 1 {
		t.Fatalf("printers = %#v, want one tile", payload["printers"])
	}
	tile, ok := printers[0].(map[string]any)
	if !ok || tile["serial"] != detectionContractSerial {
		t.Fatalf("tile = %#v, want serial %s", printers[0], detectionContractSerial)
	}
	return tile
}

func detectionContractFleet(t *testing.T, payload map[string]any) {
	t.Helper()
	for _, key := range []string{"detection_suspended", "detection_message"} {
		if _, exists := payload[key]; exists {
			t.Fatalf("idle fleet has %s = %#v", key, payload[key])
		}
	}
}

func detectionContractState(t *testing.T, value any, want map[string]any) {
	t.Helper()
	state, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("detection = %#v, want object", value)
	}
	delete(state, "age_seconds")
	delete(state, "next_check_seconds")
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("detection = %#v, want %#v", state, want)
	}
}
