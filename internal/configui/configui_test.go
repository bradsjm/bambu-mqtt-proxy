package configui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/jobpreview"
	"bambu-mqtt-proxy/internal/notification"
)

func TestMain(m *testing.M) {
	config.RegisterSection(detection.ConfigSection)
	config.RegisterSection(notification.ConfigSection)
	config.RegisterSection(jobpreview.ConfigSection)
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
	for _, env := range []string{config.EnvPrinters, config.EnvListenPort, config.EnvListenTLS, config.EnvCertFile,
		config.EnvKeyFile, config.EnvAuthMode, config.EnvLogLevel, config.EnvHTTPPort, config.EnvCameraEnable,
		config.EnvMCPEnable, jobpreview.EnvSwitch, detection.EnvAPIKey} {
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
	out.Config.sections = map[string]any{"detection": &d, "notifications": &n}
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
	select {
	case r := <-store.Reloads():
		if r.Existed {
			t.Fatal("reload reports an existing previous file")
		}
	default:
		t.Fatal("save did not request a reload")
	}
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
	r := <-store.Reloads()
	if !r.Existed || !strings.Contains(string(r.Previous), "secret-code") {
		t.Fatal("reload lacks the previous file for rollback")
	}
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
	v.Behavior.BackoffMaxSeconds = 5
	v.Behavior.BackoffInitialSeconds = 10
	if code, _ := put(t, srv, v); code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid save = %d, want 422", code)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("invalid config was written")
	}
	select {
	case <-store.Reloads():
		t.Fatal("invalid save requested a reload")
	default:
	}
}

func TestSaveWhileRestartingConflictsAndApplyReportsFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	store, srv := serve(t, path)
	v := get(t, srv).Config
	if code, _ := put(t, srv, v); code != http.StatusOK {
		t.Fatal("first save failed")
	}
	if code, _ := put(t, srv, v); code != http.StatusConflict {
		t.Fatalf("second save = %d, want 409", code)
	}
	r := <-store.Reloads()
	store.Failed(os.ErrPermission)
	if err := store.Restore(r); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("restore kept a file that did not exist before the save")
	}
	store.Applied()
	if got := get(t, srv).Meta; got.ApplyError == "" || got.Generation != 1 {
		t.Fatalf("meta = %+v", got)
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
	const remedy = "configuration saves require a writable directory mount; single-file bind mounts cannot be replaced atomically"
	if msg, _ := body["error"].(string); !strings.Contains(msg, remedy) {
		t.Fatalf("save error = %q, want the directory-mount guidance", msg)
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
	case <-store.Reloads():
		t.Fatal("failed save requested a reload")
	default:
	}

	// The failed save must leave pending false: the next save is not a 409.
	store.rename = os.Rename
	if code, _ := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save after rename recovery = %d, want 200", code)
	}
	if r := <-store.Reloads(); !r.Existed {
		t.Fatal("recovered save lacks the previous file for rollback")
	}
}

func TestWriteFileReplacesAtomicallyAndRestoreUsesSamePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "c.yaml")
	store := NewStore(path)

	// Directory creation is preserved and mode stays owner-only.
	if err := store.writeFile([]byte("first\n")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, want 0600", info.Mode().Perm())
	}

	// A failing rename keeps the old bytes and removes the temporary file.
	busyRename(t, store)
	if err := store.writeFile([]byte("second\n")); err == nil {
		t.Fatal("writeFile accepted a failing rename")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first\n" {
		t.Fatalf("failed rename changed the file: %q", got)
	}
	if left := tempFilesLeft(t, path); len(left) != 0 {
		t.Fatalf("failed writeFile left temporary files: %v", left)
	}

	// Restore goes through the same atomic path: a failing rename fails it,
	// a working rename puts the previous bytes back.
	if err := store.Restore(Reload{Existed: true, Previous: []byte("restored\n")}); err == nil {
		t.Fatal("Restore accepted a failing rename")
	}
	store.rename = os.Rename
	if err := store.Restore(Reload{Existed: true, Previous: []byte("restored\n")}); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "restored\n" {
		t.Fatalf("Restore wrote %q, want the previous bytes", got)
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

// TestJobPreviewStaysEnvOnlyAcrossConfigPageSave saves a configuration
// through the page while BMBPX_JOB_PREVIEW=false is set, then requires the
// written file to carry no job-preview field and the reload-time
// Load-plus-ApplyEnv sequence to keep the switch explicitly disabled.
func TestJobPreviewStaysEnvOnlyAcrossConfigPageSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bambu-mqtt-proxy.yaml")
	store, srv := serve(t, path)
	t.Setenv(jobpreview.EnvSwitch, "false")

	v := get(t, srv).Config
	v.Printers = []PrinterView{{Serial: "01P00A123456789", Address: "192.168.1.42:8883", TLS: true,
		InsecureSkipVerify: true, AccessCode: "12345678"}}
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save = %d %v", code, body)
	}
	select {
	case <-store.Reloads():
	default:
		t.Fatal("save did not request a reload")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "job_preview") || strings.Contains(string(raw), "jobpreview") {
		t.Fatalf("written file must not persist a job-preview field:\n%s", raw)
	}

	// The reload path re-applies the environment after every save.
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if jobpreview.Enabled(cfg) {
		t.Fatal("BMBPX_JOB_PREVIEW=false must survive the configuration-page save and reload")
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

// drainSave consumes the reload request of one successful save and records
// the apply so the next save is not a conflict.
func drainSave(t *testing.T, store *Store) Reload {
	t.Helper()
	r := <-store.Reloads()
	store.Applied()
	return r
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

// requireDetectionRejection fails unless the save was refused 422 by the
// effective detection check itself, not by an earlier validation.
func requireDetectionRejection(t *testing.T, code int, body map[string]any) {
	t.Helper()
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("save = %d %v, want 422", code, body)
	}
	msg, _ := body["error"].(string)
	if !strings.Contains(msg, "detection: enabled requires an API key") {
		t.Fatalf("save error = %q, want the detection key rejection", msg)
	}
}

// TestDetectionGetRedactsAndReportsEffectiveKey requires the settings GET
// to keep both the stored and the environment Gadget secret out of the
// payload while has_api_key reports the effective key in every direction:
// stored only, environment override, environment only, and a set-but-empty
// variable that clears an otherwise stored key.
func TestDetectionGetRedactsAndReportsEffectiveKey(t *testing.T) {
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

	// Environment key: the flag reports the effective override, still
	// without either secret in the payload.
	t.Setenv(detection.EnvAPIKey, "env-detection-key")
	if raw := getRaw(t, srv); strings.Contains(raw, "env-detection-key") || strings.Contains(raw, stored) {
		t.Fatal("GET returned a detection secret")
	}
	if d := get(t, srv).Config.sections["detection"].(*detectionPage); !d.HasAPIKey || !d.Enabled || d.APIKey != "" {
		t.Fatalf("detection view = %+v, want the environment override reported", d)
	}

	// A set-but-empty variable clears the effective key: the flag turns
	// off over a stored key.
	t.Setenv(detection.EnvAPIKey, "")
	if d := get(t, srv).Config.sections["detection"].(*detectionPage); d.HasAPIKey || d.Enabled {
		t.Fatalf("detection view = %+v, want the empty override to clear the flag", d)
	}

	// Environment-only deployment: no stored key, the variable alone sets
	// the flag.
	only := filepath.Join(t.TempDir(), "env-only.yaml")
	writeConfig(t, only)
	_, onlySrv := serve(t, only)
	t.Setenv(detection.EnvAPIKey, "env-detection-key")
	if d := get(t, onlySrv).Config.sections["detection"].(*detectionPage); !d.HasAPIKey || !d.Enabled || d.APIKey != "" {
		t.Fatalf("detection view = %+v, want the environment-only key reported", d)
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
	store, srv := serve(t, path)

	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.sections["detection"].(*detectionPage).HasAPIKey = true // incoming flags are ignored on save
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save = %d %v", code, body)
	}
	if r := drainSave(t, store); !r.Existed || !strings.Contains(string(r.Previous), stored) {
		t.Fatal("reload lacks the previous file for rollback")
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
	drainSave(t, store)
	assertStoredDetection(t, path, stored, false)

	// A whitespace-only submission is blank: the stored key survives.
	v = get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.sections["detection"].(*detectionPage).APIKey = "   "
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("padded blank save = %d %v", code, body)
	}
	drainSave(t, store)
	assertStoredDetection(t, path, stored, false)
}

// TestDetectionEnvSecretIsNeverPersisted saves through the page while the
// environment key supplies the secret: the written file must keep the
// stored key and never contain the environment secret, while the
// reload-time resolver still prefers the environment.
func TestDetectionEnvSecretIsNeverPersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	const stored = "stored-detection-key"
	writeDetectionConfig(t, path, stored)
	store, srv := serve(t, path)
	t.Setenv(detection.EnvAPIKey, "env-detection-secret")

	v := get(t, srv).Config // has_api_key true from the environment, api_key blank
	if !v.sections["detection"].(*detectionPage).Enabled || !v.sections["detection"].(*detectionPage).HasAPIKey {
		t.Fatalf("detection view = %+v, want the environment key reported as effective", v.sections["detection"].(*detectionPage))
	}
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save = %d %v", code, body)
	}
	drainSave(t, store)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "env-detection-secret") {
		t.Fatalf("the environment secret was written to the file:\n%s", raw)
	}
	if !strings.Contains(string(raw), stored) {
		t.Fatalf("the blank submission must keep the stored key:\n%s", raw)
	}

	// Reload time: the runtime resolver keeps environment precedence over
	// the stored file key.
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if detectionSettings(t, cfg).APIKey != "env-detection-secret" || !detectionSettings(t, cfg).On() {
		t.Fatalf("key = %q enabled = %v, want the environment key at reload",
			detectionSettings(t, cfg).APIKey, detectionSettings(t, cfg).On())
	}
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
	case <-store.Reloads():
		t.Fatal("rejected save requested a reload")
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
	case <-store.Reloads():
		t.Fatal("rejected save requested a reload")
	default:
	}
}

// TestDetectionEnvEmptyOverrideRejectsEnable covers the lock-out: the
// variable exists but is empty, so the stored key is effectively gone and
// enabling detection from the page must fail without touching the file.
func TestDetectionEnvEmptyOverrideRejectsEnable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	const stored = "stored-detection-key"
	writeDetectionConfig(t, path, stored)
	store, srv := serve(t, path)
	t.Setenv(detection.EnvAPIKey, "")

	if d := get(t, srv).Config.sections["detection"].(*detectionPage); d.HasAPIKey || d.Enabled {
		t.Fatalf("detection view = %+v, want the empty override to clear the flag", d)
	}
	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.sections["detection"].(*detectionPage).Enabled = true
	code, body := put(t, srv, v)
	requireDetectionRejection(t, code, body)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), stored) {
		t.Fatal("the rejected save must not disturb the stored key")
	}
	select {
	case <-store.Reloads():
		t.Fatal("rejected save requested a reload")
	default:
	}
}

// TestDetectionEnvOverrideMetaReportsEmpty requires the page lock to apply
// by presence: an empty variable still locks the key field even though no
// other override lists empty values.
func TestDetectionEnvOverrideMetaReportsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	_, srv := serve(t, path)

	if got := get(t, srv).Meta.EnvOverrides["detection_api_key"]; got != "" {
		t.Fatalf("unset variable listed as %q, want absent", got)
	}
	t.Setenv(detection.EnvAPIKey, "")
	if got := get(t, srv).Meta.EnvOverrides["detection_api_key"]; got != detection.EnvAPIKey {
		t.Fatalf("empty override listed as %q, want %s", got, detection.EnvAPIKey)
	}
}

// TestMetaCameraEnabledCarriesEffectiveOverride requires the GET meta to
// report the effective camera switch only while BMBPX_CAMERA_ENABLED
// validly applies, so the AI section never describes the stored switch as
// the cameras that will actually run. config.camera_enabled stays the
// stored value and the file bytes stay untouched.
func TestMetaCameraEnabledCarriesEffectiveOverride(t *testing.T) {
	ptr := func(b bool) *bool { return &b }
	const printerYAML = `printers:
  - serial: "01P00A123456789"
    address: "192.168.1.42:8883"
    tls: true
    insecure_skip_verify: true
    password: "secret-code"
`
	cases := []struct {
		name       string
		cameraYAML string
		env        string
		wantMeta   *bool
		wantStored bool
	}{
		{"env true overrides file false", "camera:\n  enabled: false\n", "true", ptr(true), false},
		{"env false overrides file true", "camera:\n  enabled: true\n", "false", ptr(false), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "c.yaml")
			y := printerYAML + tc.cameraYAML
			if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
				t.Fatal(err)
			}
			_, srv := serve(t, path)
			t.Setenv(config.EnvCameraEnable, tc.env)

			got := get(t, srv)
			if got.Meta.CameraEnabled == nil || *got.Meta.CameraEnabled != *tc.wantMeta {
				t.Fatalf("meta.camera_enabled = %v, want %v", got.Meta.CameraEnabled, *tc.wantMeta)
			}
			if got.Config.CameraEnabled != tc.wantStored {
				t.Fatalf("config.camera_enabled = %v, want the stored %v", got.Config.CameraEnabled, tc.wantStored)
			}
			// The stored switch itself is unchanged on disk.
			cfg, err := config.Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Camera.Enabled == nil || *cfg.Camera.Enabled != tc.wantStored {
				t.Fatalf("stored camera switch = %v, want %v", cfg.Camera.Enabled, tc.wantStored)
			}
		})
	}

	// No variable: the key is absent (JSON omitempty), the file decides.
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeConfig(t, path)
	_, srv := serve(t, path)
	got := get(t, srv)
	if got.Meta.CameraEnabled != nil {
		t.Fatalf("meta.camera_enabled = %v, want the key absent", *got.Meta.CameraEnabled)
	}
	if !got.Config.CameraEnabled {
		t.Fatal("config.camera_enabled must reflect the file")
	}

	// An unparseable override claims nothing: the field stays absent.
	t.Setenv(config.EnvCameraEnable, "yes")
	if got := get(t, srv).Meta.CameraEnabled; got != nil {
		t.Fatalf("meta.camera_enabled = %v for an invalid override, want absent", *got)
	}
}
