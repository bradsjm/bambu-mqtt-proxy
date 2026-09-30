package main

import (
	"bytes"
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
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/health"
	"bambu-mqtt-proxy/internal/upstream"
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

// TestDetectionSourceNoKeyWiring exercises the actual no-key wiring run()
// performs: the disabled detector is a typed-nil *detection.Engine, and
// detectionSource must convert it to a true nil interface so the served
// /status payload omits detection instead of panicking inside
// health.Routes.
func TestDetectionSourceNoKeyWiring(t *testing.T) {
	if got := detectionSource(nil); got != nil {
		t.Fatalf("detectionSource(nil) = %#v, want a true nil interface", got)
	}

	// Enabled branch: the helper forwards the live engine, which answers
	// for every configured serial even before Start.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := detection.New(
		[]config.Printer{{Serial: "01P1TESTENABLED", Model: "P1S"}},
		detection.NewGadgetClient("test-key"),
		idleFrames{}, nil, nil, nil, logger,
	)
	if got := detectionSource(engine); got == nil {
		t.Fatal("detectionSource(engine) = nil, want the engine")
	} else if m := got.DetectionMap(); m["01P1TESTENABLED"] == nil {
		t.Fatalf("DetectionMap missing serial: %v", m)
	}

	// The exact run() call shape for a no-key configuration.
	cfg := &config.Config{
		Listen:   []config.Listener{{Port: freePort(t), TLS: false}},
		Auth:     config.Auth{Mode: config.AuthModePrinter},
		Printers: []config.Printer{noKeyPrinter()},
	}
	cfg.ApplyDefaults()
	pool := upstream.NewPool(cfg.Printers, nil, cfg.Behavior, logger)
	t.Cleanup(pool.Stop)

	var detector *detection.Engine // no key configured: stays nil
	mux := http.NewServeMux()
	health.Routes(mux, pool, detectionSource(detector))
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

// TestNoKeyStartupStatus starts the real run() in a subprocess with an
// env-only configuration, no detection key, and one unreachable printer,
// then requires /status to serve without the detection field and the
// process to exit cleanly on SIGTERM.
func TestNoKeyStartupStatus(t *testing.T) {
	httpPort := freePort(t)
	listenPort := freePort(t)
	absent := filepath.Join(t.TempDir(), "absent.yaml")

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("test binary: %v", err)
	}
	cmd := exec.Command(exe, "-config", absent)
	cmd.Env = append(bmbpxFreeEnv(),
		childEnv+"=1",
		config.EnvPrinters+"=serial=01NOKEYTEST0001,address=127.0.0.1:1,password=00008888",
		config.EnvListenPort+"="+strconv.Itoa(listenPort),
		config.EnvListenTLS+"=false",
		config.EnvAuthMode+"="+config.AuthModePrinter,
		config.EnvHTTPPort+"="+strconv.Itoa(httpPort),
		config.EnvCameraEnable+"=false",
		config.EnvLogLevel+"=error",
		// Explicitly empty: an inherited key would enable detection.
		config.EnvOctoEverywhereAPIKey+"=",
	)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_ = cmd.Wait()
		}
	})

	url := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	waitFor(t, 10*time.Second, func() bool { return probeOK(url + "/livez") })
	if !probeOK(url + "/status") {
		terminate(t, cmd, &out, &stopped)
		t.Fatalf("/status failed while /livez served; child output:\n%s", out.String())
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

	terminate(t, cmd, &out, &stopped)
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

// terminate signals the child and requires a clean exit within bounds.
func terminate(t *testing.T, cmd *exec.Cmd, out *bytes.Buffer, stopped *bool) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal child: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		*stopped = true
		if err != nil {
			t.Fatalf("child exit after SIGTERM: %v; output:\n%s", err, out.String())
		}
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		*stopped = true
		t.Fatalf("child did not exit after SIGTERM; output:\n%s", out.String())
	}
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

// getConfigAPI fetches /config/api and returns its decoded payload.
func getConfigAPI(t *testing.T, url string) map[string]any {
	t.Helper()
	doc, ok := tryGetConfigAPI(url)
	if !ok {
		t.Fatal("GET /config/api failed")
	}
	return doc
}

// tryGetConfigAPI fetches /config/api, reporting failure instead of dying so
// waitFor can poll through the restart window when the endpoint is down.
func tryGetConfigAPI(url string) (map[string]any, bool) {
	resp, err := http.Get(url + "/config/api")
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, false
	}
	return doc, true
}

// TestConfigApplyStartupFailureRestoresPreviousConfig saves a config whose
// MQTT listener port is already held, then requires run() to keep the
// previous file bytes, report the apply failure on the page, and resume
// serving from the restored configuration.
func TestConfigApplyStartupFailureRestoresPreviousConfig(t *testing.T) {
	httpPort := freePort(t)
	listenPort := freePort(t)
	victimPort := freePort(t)
	// Held for the whole test: the saved config must fail to bind it.
	victim, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", victimPort))
	if err != nil {
		t.Fatalf("hold victim port: %v", err)
	}
	defer victim.Close()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "bambu-mqtt-proxy.yaml")
	original := fmt.Sprintf(`printers:
  - serial: "01P00A123456789"
    address: "127.0.0.1:1"
    tls: true
    insecure_skip_verify: true
    password: "orig-code"
listen:
  - port: %d
    tls: false
auth:
  mode: printer
http:
  port: %d
camera:
  enabled: false
log:
  level: error
`, listenPort, httpPort)
	if err := os.WriteFile(configPath, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("test binary: %v", err)
	}
	cmd := exec.Command(exe, "-config", configPath)
	cmd.Env = append(bmbpxFreeEnv(), childEnv+"=1")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_ = cmd.Wait()
		}
	})

	url := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	waitFor(t, 10*time.Second, func() bool { return probeOK(url + "/livez") })

	// Save the same printers but move the listener onto the held port. The
	// blank access code plus previous_serial reuses the stored code.
	doc := getConfigAPI(t, url)
	view, ok := doc["config"].(map[string]any)
	if !ok {
		t.Fatalf("config payload shape: %v", doc)
	}
	printers, ok := view["printers"].([]any)
	if !ok || len(printers) == 0 {
		t.Fatalf("printers payload shape: %v", view)
	}
	printer := printers[0].(map[string]any)
	printer["previous_serial"] = printer["serial"]
	printer["access_code"] = ""
	listen, ok := view["listen"].([]any)
	if !ok || len(listen) == 0 {
		t.Fatalf("listen payload shape: %v", view)
	}
	listen[0].(map[string]any)["port"] = float64(victimPort)
	body, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPut, url+"/config/api", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /config/api: %v", err)
	}
	resBody, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("PUT /config/api = %d: %s", res.StatusCode, resBody)
	}

	// The restart fails, the page reports it, and service resumes from the
	// restored file with a fresh generation.
	waitFor(t, 15*time.Second, func() bool {
		doc, ok := tryGetConfigAPI(url)
		if !ok {
			return false
		}
		meta, _ := doc["meta"].(map[string]any)
		errMsg, _ := meta["apply_error"].(string)
		return errMsg != ""
	})
	waitFor(t, 10*time.Second, func() bool { return probeOK(url + "/livez") })
	var meta map[string]any
	waitFor(t, 10*time.Second, func() bool {
		doc, ok := tryGetConfigAPI(url)
		if !ok {
			return false
		}
		meta, _ = doc["meta"].(map[string]any)
		gen, _ := meta["generation"].(float64)
		return gen >= 2
	})
	if meta == nil {
		t.Fatal("resumed service never recorded a new apply generation")
	}
	restored, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(restored) != original {
		t.Fatalf("config file was not restored byte-for-byte:\n%s", restored)
	}

	terminate(t, cmd, &out, &stopped)
}
