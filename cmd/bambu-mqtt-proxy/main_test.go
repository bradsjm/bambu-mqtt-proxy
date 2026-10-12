package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/configui"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/health"
	"bambu-mqtt-proxy/internal/printer"
)

// childEnv marks a re-executed test binary as the serving child of
// TestNoKeyStartupStatus.
const childEnv = "BMBPX_TEST_RUN_NO_KEY_CHILD"

func TestMain(m *testing.M) {
	if os.Getenv(childEnv) != "" {
		os.Exit(runNoKeyChild())
	}
	os.Exit(m.Run())
}

// TestDetectionModuleNoKeyWiring checks that disabled modules add no status member.
func TestDetectionModuleNoKeyWiring(t *testing.T) {
	// An enabled module supplies status for every serial before Start.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := detection.New(
		[]config.Printer{{Serial: "01P1TESTENABLED", Model: "P1S"}},
		detection.NewGadgetClient("test-key"),
		detection.IdleFrames{}, nil, nil, logger,
	)
	value := engine.Module().StatusValue()
	statuses, ok := value.(map[string]any)
	if !ok || statuses["01P1TESTENABLED"] == nil {
		t.Fatalf("StatusValue missing serial: %v", value)
	}

	// The exact run() call shape for a no-key configuration.
	cfg := &config.Config{
		Listen:   []config.Listener{{Port: freePort(t), TLS: false}},
		Auth:     config.Auth{Mode: config.AuthModePrinter},
		Printers: []config.Printer{noKeyPrinter()},
	}
	cfg.ApplyDefaults()
	links := printer.New(cfg.Printers, logger, nil, nil)
	t.Cleanup(links.Close)

	mux := http.NewServeMux()
	health.Routes(mux, links, nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	var payload struct {
		Status    string          `json:"status"`
		Upstreams map[string]bool `json:"upstreams"`
		Detection json.RawMessage `json:"detection"`
	}
	decodeStatus(t, srv.URL, &payload)
	if len(payload.Detection) != 0 {
		t.Fatalf("/status served detection %s without a configured key", payload.Detection)
	}
	if payload.Upstreams["01NOKEYTEST0001"] {
		t.Fatal("unreachable printer must not report connected")
	}
}

// TestNoKeyStartupStatus starts the real run() in a subprocess with a YAML
// configuration file, no detection key, and one unreachable printer, then
// requires /status to serve without the detection field and the process to
// exit cleanly on SIGTERM.
func TestNoKeyStartupStatus(t *testing.T) {
	httpPort := freePort(t)
	listenPort := freePort(t)
	configPath := filepath.Join(t.TempDir(), "bambu-mqtt-proxy.yaml")
	configuration := fmt.Sprintf(`listen:
  - port: %d
    tls: false
auth:
  mode: printer
printers:
  - serial: "01NOKEYTEST0001"
    model: "P1S"
    address: "127.0.0.1:1"
    username: "bblp"
    password: "00008888"
camera:
  enabled: false
`, listenPort)
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	c := startChild(t, configPath, httpPort)
	url := c.url
	waitFor(t, 10*time.Second, func() bool { return probeOK(url + "/livez") })
	if !probeOK(url + "/status") {
		c.stop(t)
		t.Fatalf("/status failed while /livez served; child output:\n%s", c.out.String())
	}

	var payload struct {
		Upstreams map[string]bool `json:"upstreams"`
		Detection json.RawMessage `json:"detection"`
	}
	decodeStatus(t, url, &payload)
	if len(payload.Detection) != 0 {
		t.Fatalf("/status served detection %s without a configured key", payload.Detection)
	}
	if connected, ok := payload.Upstreams["01NOKEYTEST0001"]; !ok || connected {
		t.Fatalf("upstreams = %v, want the configured printer reporting false", payload.Upstreams)
	}

	c.stop(t)
}

// runNoKeyChild is the subprocess body: it serves the real run() until the
// parent signals and reports a run failure on stderr.
func runNoKeyChild() int {
	if err := run(); err != nil {
		io.WriteString(os.Stderr, "child run: "+err.Error()+"\n")
		return 1
	}
	return 0
}

// noKeyPrinter returns the single unreachable printer used by both tests.
func noKeyPrinter() config.Printer {
	return config.Printer{
		Serial:   "01NOKEYTEST0001",
		Model:    "P1S",
		Address:  "127.0.0.1:1",
		Username: "bblp",
		Password: "00008888",
	}
}

// bmbpxFreeEnv drops inherited BMBPX_* variables so the child sees only the
// configuration the test sets.
func bmbpxFreeEnv() []string {
	var out []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "BMBPX_") {
			out = append(out, kv)
		}
	}
	return out
}

// decodeStatus fetches /status once and decodes it into payload.
func decodeStatus(t *testing.T, url string, payload any) {
	t.Helper()
	resp, err := http.Get(url + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /status: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /status = %d, body: %s", resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, payload); err != nil {
		t.Fatalf("decode /status %s: %v", body, err)
	}
}

// probeOK reports whether the URL answers 200 within one request.
func probeOK(url string) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

// waitFor polls check until it passes or the budget is spent.
func waitFor(t *testing.T, budget time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", budget)
}

// freePort reserves an ephemeral port and releases it for later use.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// childProcess is one real run() subprocess started by startChild.
type childProcess struct {
	url    string        // HTTP base URL set through BMBPX_HTTP_PORT.
	out    *bytes.Buffer // combined stdout and stderr.
	cmd    *exec.Cmd
	done   chan error // receives the single cmd.Wait result.
	reaped bool       // stop already received from done.
}

// startChild starts the test binary as the real proxy on path with the HTTP
// port and error log level set through the environment. One goroutine owns
// cmd.Wait; test cleanup kills and reaps a child that stop did not end.
func startChild(t *testing.T, path string, httpPort int) *childProcess {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("test binary: %v", err)
	}
	cmd := exec.Command(exe, "-config", path)
	cmd.Env = append(bmbpxFreeEnv(), childEnv+"=1", config.EnvHTTPPort+"="+strconv.Itoa(httpPort), config.EnvLogLevel+"=error")
	out := &bytes.Buffer{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	c := &childProcess{
		url:  "http://127.0.0.1:" + strconv.Itoa(httpPort),
		out:  out,
		cmd:  cmd,
		done: make(chan error, 1),
	}
	go func() { c.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if !c.reaped {
			_ = cmd.Process.Kill()
			<-c.done
		}
	})
	return c
}

// stop sends SIGTERM and requires a clean exit within 15 seconds; on timeout
// it kills and reaps the child before failing.
func (c *childProcess) stop(t *testing.T) {
	t.Helper()
	if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal child: %v", err)
	}
	select {
	case err := <-c.done:
		c.reaped = true
		if err != nil {
			t.Fatalf("child exit after SIGTERM: %v; output:\n%s", err, c.out.String())
		}
	case <-time.After(15 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.done
		c.reaped = true
		t.Fatalf("child did not exit after SIGTERM; output:\n%s", c.out.String())
	}
}

// getConfigAPI fetches /config/api once.
func getConfigAPI(url string) (map[string]any, bool) {
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(url + "/config/api")
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	var doc map[string]any
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&doc) != nil {
		return nil, false
	}
	return doc, true
}

// writeChildConfig writes a one-printer config with unreachable upstream and
// an MQTT listener on a free port.
func writeChildConfig(t *testing.T, serial string, listenPort int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bambu-mqtt-proxy.yaml")
	doc := fmt.Sprintf(`printers:
  - serial: %q
    address: "127.0.0.1:1"
    tls: true
    insecure_skip_verify: true
    password: "orig-code"
listen:
  - port: %d
    tls: false
camera:
  enabled: false
log:
  level: error
`, serial, listenPort)
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestSaveRestartsProcessWithNewConfig starts the real binary, saves a new
// printer list through the HTTP API, and requires the same process to
// re-exec and serve the new list.
func TestSaveRestartsProcessWithNewConfig(t *testing.T) {
	const oldSerial, newSerial = "01S00CRESTARTOLD1", "01S00CRESTARTNEW2"
	httpPort := freePort(t)
	path := writeChildConfig(t, oldSerial, freePort(t))
	c := startChild(t, path, httpPort)
	url, out := c.url, c.out
	waitFor(t, 10*time.Second, func() bool { return probeOK(url + "/livez") })
	doc, ok := getConfigAPI(url)
	if !ok {
		t.Fatalf("GET /config/api failed; output:\n%s", out)
	}
	started := doc["meta"].(map[string]any)["started"]
	view := doc["config"].(map[string]any)
	view["printers"] = []any{map[string]any{"serial": newSerial, "address": "127.0.0.1:1", "tls": true,
		"insecure_skip_verify": true, "username": "bblp", "access_code": "new-code"}}
	body, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPut, url+"/config/api", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("PUT /config/api: %v", err)
	}
	resBody, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("PUT /config/api = %d: %s", res.StatusCode, resBody)
	}

	waitFor(t, 20*time.Second, func() bool {
		doc, ok := getConfigAPI(url)
		return ok && doc["meta"].(map[string]any)["started"] != started
	})
	var status struct {
		Upstreams map[string]bool `json:"upstreams"`
	}
	decodeStatus(t, url, &status)
	if _, ok := status.Upstreams[newSerial]; !ok || len(status.Upstreams) != 1 {
		t.Fatalf("upstreams = %v, want only %s; output:\n%s", status.Upstreams, newSerial, out)
	}
	c.stop(t)
}

// TestStartupFailureServesConfigPageWithError starts the binary with an MQTT
// port that is already held and requires health and /config to serve with
// the error shown, while /status is absent.
func TestStartupFailureServesConfigPageWithError(t *testing.T) {
	held, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	httpPort := freePort(t)
	path := writeChildConfig(t, "01S00CSTARTUPFAIL", held.Addr().(*net.TCPAddr).Port)
	c := startChild(t, path, httpPort)
	url := c.url

	var startupErr string
	waitFor(t, 15*time.Second, func() bool {
		doc, ok := getConfigAPI(url)
		if ok {
			startupErr, _ = doc["meta"].(map[string]any)["startup_error"].(string)
		}
		return startupErr != ""
	})
	if !strings.Contains(startupErr, "address already in use") {
		t.Fatalf("startup_error = %q, want the bind failure", startupErr)
	}
	if !probeOK(url + "/livez") {
		t.Fatal("/livez does not serve in the failure mode")
	}
	c.stop(t)
}

func resolvedDetection(t *testing.T, cfg *config.Config) detection.Settings {
	t.Helper()
	settings, err := detection.SettingsOf(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return settings
}

// TestResolveConfigDetectionKey covers the startup path for the persisted detection section: the YAML key enables on its own, and an
// explicit enabled: false stays off while its stored key stays usable.
func TestResolveConfigDetectionKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bambu-mqtt-proxy.yaml")
	const keyed = `printers:
  - serial: "01P00ADETECTION1"
    address: "127.0.0.1:1883"
    password: "00008888"
detection:
  api_key: "file-key"
`
	if err := os.WriteFile(path, []byte(keyed), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	t.Run("yaml key enables", func(t *testing.T) {
		cfg, err := resolveConfig(path)
		if err != nil {
			t.Fatalf("resolveConfig: %v", err)
		}
		if !resolvedDetection(t, cfg).On() || resolvedDetection(t, cfg).Key() != "file-key" {
			t.Fatalf("enabled=%v key=%q, want the yaml key in effect",
				resolvedDetection(t, cfg).On(), resolvedDetection(t, cfg).Key())
		}
	})

	t.Run("global false stays off while the yaml key stays usable", func(t *testing.T) {
		offPath := filepath.Join(dir, "off.yaml")
		const offDoc = `printers:
  - serial: "01P00ADETECTION1"
    address: "127.0.0.1:1883"
    password: "00008888"
detection:
  enabled: false
  api_key: "file-key"
`
		if err := os.WriteFile(offPath, []byte(offDoc), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		cfg, err := resolveConfig(offPath)
		if err != nil {
			t.Fatalf("resolveConfig: %v", err)
		}
		if resolvedDetection(t, cfg).On() {
			t.Fatal("enabled: false must keep detection off")
		}
		if resolvedDetection(t, cfg).Key() != "file-key" {
			t.Fatalf("key = %q, want the stored file key preserved", resolvedDetection(t, cfg).Key())
		}
	})
}

// serveInProcess runs serve for the config file until test cleanup, waits
// for /livez, and returns the base URL. The HTTP port comes from the file.
// Fixtures that use it must not run in parallel: cameras bind the fixed raw
// camera port 6000.
func serveInProcess(t *testing.T, path string) string {
	t.Helper()
	cfg, err := resolveConfig(path)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	logger, err := newLogger("error")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := serve(ctx, cfg, logger, configui.NewStore(path))
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("serve did not stop within 15 seconds")
		}
	})
	base := fmt.Sprintf("http://127.0.0.1:%d", cfg.HTTP.Port)
	waitFor(t, 15*time.Second, func() bool { return probeOK(base + "/livez") })
	return base
}

// clearBMBPXEnv removes inherited BMBPX_* overrides for the test duration, so fixture settings cannot be replaced.
func clearBMBPXEnv(t *testing.T) {
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

// configStoreServer serves one configuration store on its own test server, like setup mode.
func configStoreServer(t *testing.T, path string) string {
	t.Helper()
	mux := http.NewServeMux()
	configui.NewStore(path).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// contractServe runs serve in process for one unreachable P1S printer with
// the camera switch, an optional module section, and error-level logging,
// and returns the base URL. Fixtures that use it must not run in parallel:
// cameras bind the fixed raw camera port 6000.
func contractServe(t *testing.T, serial string, cameras bool, section string) string {
	t.Helper()
	clearBMBPXEnv(t)
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
`, freePort(t), serial, port, cameras) + section + "log:\n  level: error\n"
	if err := os.WriteFile(path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	return serveInProcess(t, path)
}

// contractRequest performs one request within timeout and returns the
// status and full body. A non-empty body is sent as JSON, and a non-empty
// site sets Sec-Fetch-Site.
func contractRequest(t *testing.T, timeout time.Duration, method, url, body, site string) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
