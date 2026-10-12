package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/platecheck"
)

const platecheckContractSerial = "01S00CPLATE001"

// platecheckContractEndpoint is an HTTPS endpoint URL that fails fast.
const platecheckContractEndpoint = "https://127.0.0.1:1/check"

// platecheckSnapshotBudget covers the bounded camera-capture wait of the diagnostics handler.
const platecheckSnapshotBudget = 45 * time.Second

// TestPlatecheckConfigTestRoute pins that the key-check route exists in
// setup mode and at runtime, rejects cross-site posts, and reports missing
// credentials with the fixed sentences.
func TestPlatecheckConfigTestRoute(t *testing.T) {
	clearBMBPXEnv(t)
	setupBase := configStoreServer(t, filepath.Join(t.TempDir(), "absent.yaml"))
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
	base := platecheckContractServe(t, true, true)
	snapBody := fmt.Sprintf(`{"enabled":true,"provider":"custom","endpoint":%q,"api_key":"contract-dummy-key","model":"clef","stop_confidence":0.5}`,
		platecheckContractEndpoint)
	code, body := contractRequest(t, platecheckSnapshotBudget, http.MethodPost, base+"/platecheck/snapshots", snapBody, "same-origin")
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
	base := platecheckContractServe(t, false, true)
	body := fmt.Sprintf(`{"enabled":false,"provider":"custom","endpoint":%q,"api_key":"contract-dummy-key","model":"clef","stop_confidence":0.5}`,
		platecheckContractEndpoint)
	code, raw := contractRequest(t, platecheckSnapshotBudget, http.MethodPost, base+"/platecheck/snapshots", body, "same-origin")
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
	base := platecheckContractServe(t, true, false)
	if code, body := contractRequest(t, platecheckSnapshotBudget, http.MethodPost, base+"/platecheck/snapshots", `{"enabled":true,"model":"clef","stop_confidence":0.5}`, "same-origin"); code != 404 {
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
	section := ""
	if feature {
		section = fmt.Sprintf("platecheck:\n  enabled: true\n  provider: %s\n  endpoint: %q\n  api_key: contract-dummy-key\n",
			platecheck.ProviderCustom, platecheckContractEndpoint)
	}
	return contractServe(t, platecheckContractSerial, cameras, section)
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
