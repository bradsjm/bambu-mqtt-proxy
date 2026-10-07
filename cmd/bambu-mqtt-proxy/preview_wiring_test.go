// Job preview wiring tests for the real run(): the versioned image route
// follows the feature switch and the camera/MCP consumer combinations, and
// the subprocess children exit cleanly through the shared teardown order.
// The fixture printer is unreachable, so no transfer can ever start and
// the route answers from cache only.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// startProxyChild starts the real run() in a subprocess with one
// unreachable printer and the shared HTTP listener on, then waits for
// /livez. The camera and MCP arguments are "" for the enabled default or
// "false" to write the disable switch into the file. The returned stop func
// terminates the child and requires a clean exit.
func startProxyChild(t *testing.T, cameraEnv, mcpEnv string) (url string, cmd *exec.Cmd, out *bytes.Buffer, stop func()) {
	t.Helper()
	httpPort := freePort(t)
	listenPort := freePort(t)
	printer := noKeyPrinter()
	var doc strings.Builder
	fmt.Fprintf(&doc, `listen:
  - port: %d
    tls: false
auth:
  mode: printer
printers:
  - serial: %q
    model: "P1S"
    address: %q
    password: %q
`, listenPort, printer.Serial, printer.Address, printer.Password)
	if cameraEnv != "" {
		fmt.Fprintf(&doc, "camera:\n  enabled: %s\n", cameraEnv)
	}
	if mcpEnv != "" {
		fmt.Fprintf(&doc, "mcp:\n  enabled: %s\n", mcpEnv)
	}
	configPath := filepath.Join(t.TempDir(), "bambu-mqtt-proxy.yaml")
	if err := os.WriteFile(configPath, []byte(doc.String()), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("test binary: %v", err)
	}
	env := append(bmbpxFreeEnv(),
		childEnv+"=1",
		config.EnvHTTPPort+"="+strconv.Itoa(httpPort),
		config.EnvLogLevel+"=error",
	)
	c := exec.Command(exe, "-config", configPath)
	c.Env = env
	out = &bytes.Buffer{}
	c.Stdout, c.Stderr = out, out
	if err := c.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = c.Process.Kill()
			_ = c.Wait()
		}
	})
	url = "http://127.0.0.1:" + strconv.Itoa(httpPort)
	waitFor(t, 10*time.Second, func() bool { return probeOK(url + "/livez") })
	return url, c, out, func() {
		stopped = true
		terminate(t, c, out, &stopped)
	}
}

// postPreview sends one POST to the preview route and returns the status.
// A registered GET-only pattern answers 405 with Allow: GET; an absent
// path stays a plain 404, so the method mismatch observes route
// registration through the running mux.
func postPreview(t *testing.T, url, path string) int {
	t.Helper()
	resp, err := http.Post(url+path, "text/plain", strings.NewReader(""))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusMethodNotAllowed && !strings.Contains(resp.Header.Get("Allow"), "GET") {
		t.Fatalf("POST %s = 405 without Allow: GET (headers: %v, body: %s)", path, resp.Header, body)
	}
	return resp.StatusCode
}

// getPreviewWrongVersion requests the route with a version no cached image
// can carry: the live route must answer 404 from cache with no network work.
func getPreviewWrongVersion(t *testing.T, url, path string) int {
	t.Helper()
	const wrongVersion = "?v=0000000000000000000000000000000000000000000000000000000000000000"
	get, err := http.Get(url + path + wrongVersion)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	get.Body.Close()
	return get.StatusCode
}

// TestJobPreviewRouteFollowsFeatureSwitch drives real run() child processes
// across the consumer combinations. The route exists for either consumer
// alone — MCP with cameras off, and cameras with MCP explicitly off. With
// both consumers off the route must be entirely absent so no FTPS service
// can exist behind it.
func TestJobPreviewRouteFollowsFeatureSwitch(t *testing.T) {
	const previewPath = "/camera/01NOKEYTEST0001/preview"
	cases := []struct {
		name        string
		cameraEnv   string
		mcpEnv      string
		wantPOST    int
		wantGETCode int
	}{
		{"enabled serves route for mcp without cameras", "false", "", http.StatusMethodNotAllowed, http.StatusNotFound},
		{"enabled serves route for cameras without mcp", "", "false", http.StatusMethodNotAllowed, http.StatusNotFound},
		{"no consumer removes the route", "false", "false", http.StatusNotFound, http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url, _, out, stop := startProxyChild(t, tc.cameraEnv, tc.mcpEnv)
			defer stop()

			if got := postPreview(t, url, previewPath); got != tc.wantPOST {
				t.Fatalf("POST %s = %d, want %d; child output:\n%s", previewPath, got, tc.wantPOST, out.String())
			}
			if got := getPreviewWrongVersion(t, url, previewPath); got != tc.wantGETCode {
				t.Fatalf("GET %s with wrong version = %d, want %d", previewPath, got, tc.wantGETCode)
			}
		})
	}
}

// TestModuleMCPWiring checks the real serveOnce module list in both
// configurations. Cameras are on with detection, because a blocked
// detection engine registers no tool. Job preview always runs with MCP as
// its consumer, so its tool is present in both configurations.
func TestModuleMCPWiring(t *testing.T) {
	for _, on := range []bool{true, false} {
		t.Run(strconv.FormatBool(on), func(t *testing.T) {
			base := detectionContractServe(t, on, on, on)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			body := `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"wiring","version":"0"},"io.modelcontextprotocol/clientCapabilities":{}}}}`
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/mcp", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")
			req.Header.Set("MCP-Protocol-Version", "2026-07-28")
			req.Header.Set("Mcp-Method", "tools/list")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("tools/list status = %d", resp.StatusCode)
			}
			var result struct {
				Result struct {
					Tools []struct {
						Name string `json:"name"`
					} `json:"tools"`
				} `json:"result"`
				Error json.RawMessage `json:"error"`
			}
			found := false
			sc := bufio.NewScanner(resp.Body)
			for sc.Scan() {
				if data, ok := strings.CutPrefix(sc.Text(), "data:"); ok {
					if err := json.Unmarshal([]byte(strings.TrimSpace(data)), &result); err != nil {
						t.Fatal(err)
					}
					found = true
					break
				}
			}
			if err := sc.Err(); err != nil {
				t.Fatal(err)
			}
			if !found || len(result.Error) > 0 {
				t.Fatalf("tools/list response = %+v", result)
			}
			counts := map[string]int{}
			for _, tool := range result.Result.Tools {
				counts[tool.Name]++
			}
			want := 0
			if on {
				want = 1
			}
			if got := counts["set_ai_monitoring"]; got != want {
				t.Fatalf("set_ai_monitoring count = %d, want %d", got, want)
			}
			// Job preview has no switch: with MCP enabled it always runs and
			// serves its tool in both configurations.
			if got := counts["get_job_preview"]; got != 1 {
				t.Fatalf("get_job_preview count = %d, want 1", got)
			}
		})
	}
}

// TestJobPreviewTileModulePath checks the jobpreview.job_preview tile member.
func TestJobPreviewTileModulePath(t *testing.T) {
	base, _, _, stop := startProxyChild(t, "", "false")
	defer stop()
	resp, err := http.Get(base + "/camera/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var payload struct {
		Printers []map[string]json.RawMessage `json:"printers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Printers) != 1 {
		t.Fatalf("tiles = %d", len(payload.Printers))
	}
	tile := payload.Printers[0]
	var state struct {
		JobPreview struct {
			Status string `json:"status"`
		} `json:"job_preview"`
	}
	if err := json.Unmarshal(tile["jobpreview"], &state); err != nil {
		t.Fatal(err)
	}
	if state.JobPreview.Status == "" {
		t.Fatalf("jobpreview.job_preview absent: %s", tile["jobpreview"])
	}
	for _, name := range []string{"job_preview", "job_metadata"} {
		if _, exists := tile[name]; exists {
			t.Fatalf("old tile member %q remains", name)
		}
	}
}
