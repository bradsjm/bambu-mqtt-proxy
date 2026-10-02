package configui

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"bambu-mqtt-proxy/internal/config"
)

type getResponse struct {
	Config View `json:"config"`
	Meta   meta `json:"meta"`
}

func serve(t *testing.T, path string) (*Store, *httptest.Server) {
	t.Helper()
	for _, env := range []string{config.EnvPrinters, config.EnvListenPort, config.EnvListenTLS, config.EnvCertFile,
		config.EnvKeyFile, config.EnvAuthMode, config.EnvLogLevel, config.EnvHTTPPort, config.EnvCameraEnable,
		config.EnvMCPEnable, config.EnvJobPreview} {
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
	var out getResponse
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
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
	t.Setenv(config.EnvJobPreview, "false")

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
	if cfg.JobPreviewEnabled() {
		t.Fatal("BMBPX_JOB_PREVIEW=false must survive the configuration-page save and reload")
	}
	if cfg.JobPreview == nil || *cfg.JobPreview {
		t.Fatal("the explicit false must be retained after the re-apply")
	}
}
