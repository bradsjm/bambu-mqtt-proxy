package configui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/notification"
	"bambu-mqtt-proxy/internal/safety"
)

func TestMain(m *testing.M) {
	config.RegisterSection(detection.ConfigSection)
	config.RegisterSection(notification.ConfigSection)
	config.RegisterSection(safety.ConfigSection)
	os.Exit(m.Run())
}

type detectionPage struct {
	Enabled   bool   `json:"enabled"`
	HasAPIKey bool   `json:"has_api_key"`
	APIKey    string `json:"api_key,omitempty"`
}

func detectionSettings(t *testing.T, c *config.Config) detection.Settings {
	t.Helper()
	s, err := detection.SettingsOf(c)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type getResponse struct {
	Config View `json:"config"`
	Meta   meta `json:"meta"`
}

func serve(t *testing.T, path string) (*Store, *httptest.Server) {
	t.Helper()
	for _, env := range []string{config.EnvLogLevel, config.EnvHTTPPort} {
		t.Setenv(env, "") // restores the original value after the test
		_ = os.Unsetenv(env)
	}
	store := NewStore(path)
	mux := http.NewServeMux()
	store.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return store, srv
}

// requireSaved waits a bounded time for the restart signal that follows a
// successful save's response.
func requireSaved(t *testing.T, store *Store) {
	t.Helper()
	select {
	case <-store.Saved():
	case <-time.After(5 * time.Second):
		t.Fatal("save did not signal a restart")
	}
}

func get(t *testing.T, srv *httptest.Server) getResponse {
	t.Helper()
	res, err := http.Get(srv.URL + "/config/api")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body struct {
		Config json.RawMessage `json:"config"`
		Meta   meta            `json:"meta"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	out := getResponse{Meta: body.Meta}
	if err := json.Unmarshal(body.Config, &out.Config); err != nil {
		t.Fatal(err)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(body.Config, &members); err != nil {
		t.Fatal(err)
	}
	var d detectionPage
	if err := json.Unmarshal(members["detection"], &d); err != nil {
		t.Fatal(err)
	}
	var n notificationsPage
	if err := json.Unmarshal(members["notifications"], &n); err != nil {
		t.Fatal(err)
	}
	var sf safety.Settings
	if err := json.Unmarshal(members["safety"], &sf); err != nil {
		t.Fatal(err)
	}
	out.Config.sections = map[string]any{"detection": &d, "notifications": &n, "safety": sf}
	return out
}

func put(t *testing.T, srv *httptest.Server, v View) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(v)
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/config/api", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

func TestNoFileServesDefaultsAndSaveCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "bambu-mqtt-proxy.yaml")
	store, srv := serve(t, path)

	got := get(t, srv)
	if got.Meta.FileExists || len(got.Config.Printers) != 0 || got.Config.HTTPPort != 8080 || len(got.Config.Listen) != 1 {
		t.Fatalf("defaults = %+v, meta = %+v", got.Config, got.Meta)
	}

	v := got.Config
	v.Printers = []PrinterView{{Serial: "01P00A123456789", Address: "192.168.1.42:8883", TLS: true,
		InsecureSkipVerify: true, AccessCode: "12345678"}}
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save = %d %v", code, body)
	}
	requireSaved(t, store)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %v, want 0600", info.Mode().Perm())
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Printers) != 1 || cfg.Printers[0].Password != "12345678" || cfg.Printers[0].Username != "bblp" {
		t.Fatalf("written printers = %+v", cfg.Printers)
	}
}

func TestAccessCodesAreNeverReturnedAndKeptWhenBlank(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeConfig(t, path)
	store, srv := serve(t, path)

	res, err := http.Get(srv.URL + "/config/api")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if strings.Contains(string(raw), "secret-code") {
		t.Fatal("GET returned a stored access code")
	}

	v := get(t, srv).Config
	if !v.Printers[0].HasAccessCode {
		t.Fatal("has_access_code = false")
	}
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.Printers[0].Name = "Garage"
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save = %d %v", code, body)
	}
	cfg, _ := config.Load(path)
	if cfg.Printers[0].Password != "secret-code" || cfg.Printers[0].Name != "Garage" {
		t.Fatalf("printer = %+v", cfg.Printers[0])
	}
	requireSaved(t, store)
}

func TestStoredCodeIsNotSentToANewAddress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeConfig(t, path)
	_, srv := serve(t, path)

	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.Printers[0].Address = "10.0.0.9:8883"
	if code, _ := put(t, srv, v); code != http.StatusUnprocessableEntity {
		t.Fatalf("address change without code = %d, want 422", code)
	}
	v.Printers[0].PreviousSerial = ""
	v.Printers[0].Address = "192.168.1.42:8883"
	if code, _ := put(t, srv, v); code != http.StatusUnprocessableEntity {
		t.Fatalf("new printer without code = %d, want 422", code)
	}
}

func TestInvalidConfigIsNotWritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	store, srv := serve(t, path)
	v := get(t, srv).Config
	v.AuthMode = "bogus"
	if code, _ := put(t, srv, v); code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid save = %d, want 422", code)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("invalid config was written")
	}
	select {
	case <-store.Saved():
		t.Fatal("invalid save requested a restart")
	default:
	}
}

// TestUnbindablePortIsRejectedBeforeWrite requires a save that moves HTTP to
// a port the process cannot bind to fail with a clear error, leave the file
// byte for byte unchanged, and request no restart.
func TestUnbindablePortIsRejectedBeforeWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeConfig(t, path)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	store, srv := serve(t, path)
	store.SetHTTPPort(8080)
	busy, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	port := busy.Addr().(*net.TCPAddr).Port
	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.HTTPPort = port
	code, body := put(t, srv, v)
	if msg, _ := body["error"].(string); code != http.StatusUnprocessableEntity || !strings.Contains(msg, fmt.Sprintf("%d", port)) {
		t.Fatalf("save to a busy port = %d %v, want 422 naming port %d", code, body, port)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("rejected save changed the file: %v", err)
	}
	select {
	case <-store.Saved():
		t.Fatal("rejected save requested a restart")
	default:
	}
}

func TestCrossOriginSaveIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	_, srv := serve(t, path)
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/config/api", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin save = %d, want 403", res.StatusCode)
	}
}

// busyRename points the store's rename seam at an always-failing rename,
// the failure a read-only or single-file bind mount target produces.
func busyRename(t *testing.T, store *Store) {
	t.Helper()
	store.rename = func(_, _ string) error {
		return syscall.EBUSY
	}
}

// tempFilesLeft lists leftover .bambu-mqtt-proxy-* files in the config
// directory; a failed save must clean up its temporary file.
func tempFilesLeft(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".bambu-mqtt-proxy-") {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestRenameFailureRejectsSaveAndKeepsOldBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeConfig(t, path)
	store, srv := serve(t, path)
	busyRename(t, store)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	code, body := put(t, srv, v)
	if code != http.StatusInternalServerError {
		t.Fatalf("save with failing rename = %d, want 500", code)
	}
	if msg, _ := body["error"].(string); !strings.Contains(msg, syscall.EBUSY.Error()) {
		t.Fatalf("save error = %q, want the rename failure cause", msg)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("failed save changed the config file")
	}
	if left := tempFilesLeft(t, path); len(left) != 0 {
		t.Fatalf("failed save left temporary files: %v", left)
	}
	select {
	case <-store.Saved():
		t.Fatal("failed save requested a restart")
	default:
	}

	store.rename = os.Rename
	if code, _ := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save after rename recovery = %d, want 200", code)
	}
}

func writeConfig(t *testing.T, path string) {
	t.Helper()
	const y = `printers:
  - serial: "01P00A123456789"
    address: "192.168.1.42:8883"
    tls: true
    insecure_skip_verify: true
    password: "secret-code"
`
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
}

// writeDetectionConfig stores the standard printer fixture plus a stored
// Gadget key, using the same owner-only write pattern as writeConfig.
func writeDetectionConfig(t *testing.T, path, key string) {
	t.Helper()
	y := fmt.Sprintf(`printers:
  - serial: "01P00A123456789"
    address: "192.168.1.42:8883"
    tls: true
    insecure_skip_verify: true
    password: "secret-code"
detection:
  api_key: %q
`, key)
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
}

// getRaw fetches the settings JSON body without decoding it, so tests can
// require that secrets never appear anywhere in the payload.
func getRaw(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	res, err := http.Get(srv.URL + "/config/api")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// assertStoredDetection loads the file and requires the stored key plus
// the persisted enabled switch to round trip.
func assertStoredDetection(t *testing.T, path, wantKey string, wantEnabled bool) {
	t.Helper()
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if detectionSettings(t, cfg).APIKey != wantKey {
		t.Fatalf("stored api_key = %q, want %q", detectionSettings(t, cfg).APIKey, wantKey)
	}
	if detectionSettings(t, cfg).Enabled == nil || *detectionSettings(t, cfg).Enabled != wantEnabled {
		t.Fatalf("stored enabled = %v, want %v", detectionSettings(t, cfg).Enabled, wantEnabled)
	}
	if got := detectionSettings(t, cfg).On(); got != wantEnabled {
		t.Fatalf("DetectionEnabled = %v, want %v", got, wantEnabled)
	}
}

// requireDetectionRejection fails unless the save was refused with the
// validation status.
func requireDetectionRejection(t *testing.T, code int, body map[string]any) {
	t.Helper()
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("save = %d %v, want 422", code, body)
	}
}

// TestDetectionGetRedactsAndReportsStoredKey requires the settings GET
// to keep the stored Gadget secret out of the payload while has_api_key
// reports whether the file stores a key.
func TestDetectionGetRedactsAndReportsStoredKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	const stored = "stored-detection-key"
	writeDetectionConfig(t, path, stored)
	_, srv := serve(t, path)

	// Stored key only: the flag is on and the secret never leaves the file.
	if raw := getRaw(t, srv); strings.Contains(raw, stored) {
		t.Fatal("GET returned the stored detection key")
	}
	if d := get(t, srv).Config.sections["detection"].(*detectionPage); !d.HasAPIKey || !d.Enabled || d.APIKey != "" {
		t.Fatalf("detection view = %+v, want the stored key reported without revealing it", d)
	}

	// A file without a detection section: the flag is off and nothing leaks.
	only := filepath.Join(t.TempDir(), "no-key.yaml")
	writeConfig(t, only)
	_, onlySrv := serve(t, only)
	if d := get(t, onlySrv).Config.sections["detection"].(*detectionPage); d.HasAPIKey || d.Enabled {
		t.Fatalf("detection view = %+v, want no key reported", d)
	}
}

// TestDetectionBlankSaveKeepsStoredKeyAndDisableRetains pins the save
// contract: a blank submission keeps the stored key (the page never
// receives it back, and incoming has_api_key flags are ignored),
// disabling keeps it too, a whitespace-only submission counts as blank,
// and the file keeps the owner-only write pattern.
func TestDetectionBlankSaveKeepsStoredKeyAndDisableRetains(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	const stored = "stored-detection-key"
	writeDetectionConfig(t, path, stored)
	_, srv := serve(t, path)

	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.sections["detection"].(*detectionPage).HasAPIKey = true // incoming flags are ignored on save
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save = %d %v", code, body)
	}
	assertStoredDetection(t, path, stored, true)
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %v err = %v, want 0600", info, err)
	}

	// Disabling retains the stored key: the switch, not a key removal, is
	// the off action.
	v = get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.sections["detection"].(*detectionPage).Enabled = false
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("disable save = %d %v", code, body)
	}
	assertStoredDetection(t, path, stored, false)

	// A whitespace-only submission is blank: the stored key survives.
	v = get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.sections["detection"].(*detectionPage).APIKey = "   "
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("padded blank save = %d %v", code, body)
	}
	assertStoredDetection(t, path, stored, false)
}

// TestDetectionEnableWithoutKeyIsRejected requires the effective-only key
// check to refuse every path to an enabled detection without a key,
// including a client-sent has_api_key flag that fakes one, and to leave
// both absent and existing files untouched.
func TestDetectionEnableWithoutKeyIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	store, srv := serve(t, path)

	// Fresh setup: no file, no stored key, no environment.
	v := get(t, srv).Config
	v.sections["detection"].(*detectionPage).Enabled = true
	code, body := put(t, srv, v)
	requireDetectionRejection(t, code, body)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("rejected save wrote a config file")
	}
	select {
	case <-store.Saved():
		t.Fatal("rejected save requested a restart")
	default:
	}

	// The has_api_key flag is display-only and cannot stand in for a key.
	v = get(t, srv).Config
	v.sections["detection"].(*detectionPage).Enabled = true
	v.sections["detection"].(*detectionPage).HasAPIKey = true
	code, body = put(t, srv, v)
	requireDetectionRejection(t, code, body)

	// Over an existing file without a detection section: the same
	// rejection, existing bytes untouched.
	writeConfig(t, path)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	v = get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.sections["detection"].(*detectionPage).Enabled = true
	code, body = put(t, srv, v)
	requireDetectionRejection(t, code, body)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("rejected save changed the config file")
	}
	select {
	case <-store.Saved():
		t.Fatal("rejected save requested a restart")
	default:
	}
}

// TestMetaEnvOverridesKeptVariables requires the page meta to report only
// the two surviving environment variables, by their presence rules, and
// nothing for removed variables.
func TestMetaEnvOverridesKeptVariables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeConfig(t, path)
	_, srv := serve(t, path)

	got := get(t, srv).Meta.EnvOverrides
	for _, field := range []string{"http_port", "log_level"} {
		if _, present := got[field]; present {
			t.Fatalf("unset variable %s listed as %q, want absent", field, got[field])
		}
	}
	t.Setenv(config.EnvHTTPPort, "9090")
	t.Setenv(config.EnvLogLevel, "debug")
	got = get(t, srv).Meta.EnvOverrides
	if got["http_port"] != config.EnvHTTPPort {
		t.Fatalf("http_port override listed as %q, want %s", got["http_port"], config.EnvHTTPPort)
	}
	if got["log_level"] != config.EnvLogLevel {
		t.Fatalf("log_level override listed as %q, want %s", got["log_level"], config.EnvLogLevel)
	}
	if len(got) != 2 {
		t.Fatalf("env overrides = %v, want only http_port and log_level", got)
	}
}

// probeRecorder records the printers the printer-test endpoint probes.
type probeRecorder struct {
	mu     sync.Mutex
	probed []string
}

// probe appends the probed address and reports success.
func (r *probeRecorder) probe(_ context.Context, p config.Printer) error {
	r.mu.Lock()
	r.probed = append(r.probed, p.Address)
	r.mu.Unlock()
	return nil
}

// addresses returns the recorded addresses.
func (r *probeRecorder) addresses() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.probed...)
}

// writeConfigAddress writes a one-printer file whose address is bare, so
// the stored address has no port.
func writeConfigAddress(t *testing.T, path, address string) {
	t.Helper()
	y := fmt.Sprintf(`printers:
  - serial: "01P00A123456789"
    address: %q
    tls: true
    insecure_skip_verify: true
    password: "secret-code"
`, address)
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestPrinterTestDefaultPortAndNormalizedComparisons requires the printer
// test to accept a bare host, normalize it to the default MQTT TLS port,
// and compare stored addresses with the same normalization everywhere: the
// destination duplicate check and the access-code address-change check.
func TestPrinterTestDefaultPortAndNormalizedComparisons(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeConfigAddress(t, path, "10.0.0.9")
	store, srv := serve(t, path)
	probe := &probeRecorder{}
	store.probePrinter = probe.probe

	// (a) A test POST with a bare host probes the normalized address.
	body := `{"serial":"02SNEWPRINTER000","address":"192.168.1.50","tls":true,
		"insecure_skip_verify":true,"username":"bblp","access_code":"12345678"}`
	res, err := http.Post(srv.URL+"/config/printers/test", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("printer test = %d, want 200", res.StatusCode)
	}
	got := probe.addresses()
	if len(got) != 1 || got[0] != "192.168.1.50:8883" {
		t.Fatalf("probe addresses = %v, want [192.168.1.50:8883]", got)
	}

	// (b) A test of a new serial at the stored address, given bare, is a
	// 409 duplicate reported with the normalized address.
	body = `{"serial":"03SNEWPRINTER000","address":"10.0.0.9","tls":true,
		"insecure_skip_verify":true,"username":"bblp","access_code":"12345678"}`
	res, err = http.Post(srv.URL+"/config/printers/test", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var dup map[string]any
	_ = json.NewDecoder(res.Body).Decode(&dup)
	res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate-address test = %d %v, want 409", res.StatusCode, dup)
	}
	if msg, _ := dup["error"].(string); !strings.Contains(msg, "10.0.0.9:8883") {
		t.Fatalf("duplicate message = %q, want the normalized address", msg)
	}
	if got := probe.addresses(); len(got) != 1 {
		t.Fatalf("duplicate check probed the printer: %v", got)
	}

	// (c) A save that round-trips the GET view keeps the stored access
	// code although the GET shows the address with the appended port.
	v := get(t, srv).Config
	if len(v.Printers) != 1 || v.Printers[0].Address != "10.0.0.9:8883" {
		t.Fatalf("view printers = %+v, want the normalized address", v.Printers)
	}
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.Printers[0].AccessCode = ""
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save = %d %v, want 200 with the blank access code", code, body)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Printers) != 1 || cfg.Printers[0].Password != "secret-code" {
		t.Fatalf("saved printers = %+v, want the stored access code kept", cfg.Printers)
	}
}

// TestSafetySwitchSavesAndKeepsPushoverCheck requires the safety switch to
// round trip through the page and to refuse enabling safety without stored
// Pushover credentials.
func TestSafetySwitchSavesAndKeepsPushoverCheck(t *testing.T) {
	bare := filepath.Join(t.TempDir(), "bare.yaml")
	writeConfig(t, bare)
	_, srv := serve(t, bare)
	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.sections["safety"] = safety.Settings{Enabled: true}
	if code, body := put(t, srv, v); code != http.StatusUnprocessableEntity {
		t.Fatalf("safety without Pushover credentials = %d %v, want 422", code, body)
	}

	path := filepath.Join(t.TempDir(), "c.yaml")
	writeNotificationsConfig(t, path)
	_, srv = serve(t, path)
	v = get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.sections["safety"] = safety.Settings{Enabled: true}
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("safety with credentials = %d %v, want 200", code, body)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if s, err := safety.SettingsOf(cfg); err != nil || !s.Enabled {
		t.Fatalf("stored safety = %+v, %v, want enabled", s, err)
	}
	if !strings.Contains(getRaw(t, srv), `"safety":{"enabled":true}`) {
		t.Fatal("GET does not report the safety switch")
	}
}
