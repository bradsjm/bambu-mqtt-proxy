package configui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bambu-mqtt-proxy/internal/config"
)

// writeNotificationsConfig stores a config with a printer access code and
// both Pushover credentials, so preservation can be checked per field.
func writeNotificationsConfig(t *testing.T, path string) {
	t.Helper()
	const y = `printers:
  - serial: "01P00A123456789"
    address: "192.168.1.42:8883"
    tls: true
    insecure_skip_verify: true
    password: "secret-code"
notifications:
  enabled: true
  provider: pushover
  pushover:
    app_token: "secret-app-token"
    user_key: "secret-user-key"
`
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestNotificationCredentialsAreNeverReturned pins that the sanitized GET
// reports only presence flags, never either credential.
func TestNotificationCredentialsAreNeverReturned(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeNotificationsConfig(t, path)
	_, srv := serve(t, path)

	res, err := http.Get(srv.URL + "/config/api")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	for _, secret := range []string{"secret-app-token", "secret-user-key"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("GET returned a stored notification credential")
		}
	}
	v := get(t, srv).Config
	if !v.Notifications.Enabled || v.Notifications.Provider != "pushover" {
		t.Fatalf("notifications view = %+v", v.Notifications)
	}
	if !v.Notifications.Pushover.HasAppToken || !v.Notifications.Pushover.HasUserKey {
		t.Fatalf("pushover view = %+v", v.Notifications.Pushover)
	}
	if v.Notifications.Pushover.AppToken != "" || v.Notifications.Pushover.UserKey != "" {
		t.Fatalf("GET returned credential fields: %+v", v.Notifications.Pushover)
	}
}

// TestSaveWithBlankNotificationCredentialsKeepsStored pins that a save
// leaving both credential inputs blank writes the stored values.
func TestSaveWithBlankNotificationCredentialsKeepsStored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeNotificationsConfig(t, path)
	store, srv := serve(t, path)

	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save = %d %v", code, body)
	}
	select {
	case <-store.Reloads():
	default:
		t.Fatal("save did not request a reload")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Notifications.Pushover.AppToken != "secret-app-token" ||
		cfg.Notifications.Pushover.UserKey != "secret-user-key" ||
		!cfg.Notifications.Enabled || cfg.Notifications.Provider != "pushover" {
		t.Fatalf("written notifications = %+v", cfg.Notifications)
	}
}

// TestSaveWithNotificationCredentialsWritesTrimmed pins that replacement
// credentials are written trimmed.
func TestSaveWithNotificationCredentialsWritesTrimmed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeNotificationsConfig(t, path)
	_, srv := serve(t, path)

	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.Notifications.Pushover.AppToken = "  new-app  "
	v.Notifications.Pushover.UserKey = "\tnew-user\n"
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save = %d %v", code, body)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Notifications.Pushover.AppToken != "new-app" || cfg.Notifications.Pushover.UserKey != "new-user" {
		t.Fatalf("written credentials = %+v", cfg.Notifications.Pushover)
	}
}

// TestFirstSaveWithoutStoredFileKeepsSubmittedNotifications pins that a
// save creating the file (setup mode) keeps submitted notification
// settings: nothing is stored to fall back on, but nothing discards them
// either.
func TestFirstSaveWithoutStoredFileKeepsSubmittedNotifications(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "c.yaml")
	_, srv := serve(t, path)

	v := get(t, srv).Config
	v.Notifications = NotificationsView{
		Enabled:  true,
		Provider: "  pushover ",
		Pushover: PushoverView{AppToken: " first-app ", UserKey: "first-user"},
	}
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("first save = %d %v", code, body)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Notifications.Pushover.AppToken != "first-app" ||
		cfg.Notifications.Pushover.UserKey != "first-user" ||
		!cfg.Notifications.Enabled || cfg.Notifications.Provider != "pushover" {
		t.Fatalf("written notifications = %+v", cfg.Notifications)
	}
}

// postTest posts one notifications view to the test endpoint.
func postTest(t *testing.T, srv *httptest.Server, v NotificationsView) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(v)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/config/notifications/test", bytes.NewReader(body))
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

// TestNotificationTestEndpointMergesStoredCredentials pins the send path:
// blank submissions resolve to the stored credentials, Enabled is forced,
// the fixed message is sent, and nothing is saved or reloaded.
func TestNotificationTestEndpointMergesStoredCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeNotificationsConfig(t, path)
	store, srv := serve(t, path)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var gotCfg config.Notifications
	var gotTitle, gotBody string
	var gotJPEG []byte
	store.sendTest = func(_ context.Context, cfg config.Notifications, title, body string, jpeg []byte) error {
		gotCfg, gotTitle, gotBody, gotJPEG = cfg, title, body, jpeg
		return nil
	}

	code, out := postTest(t, srv, NotificationsView{Enabled: false})
	if code != http.StatusOK || out["sent"] != true {
		t.Fatalf("test = %d %v", code, out)
	}
	if !gotCfg.Enabled || gotCfg.Provider != "pushover" ||
		gotCfg.Pushover.AppToken != "secret-app-token" || gotCfg.Pushover.UserKey != "secret-user-key" {
		t.Fatalf("sent settings = %+v", gotCfg)
	}
	if gotTitle != "bambu-mqtt-proxy test" || gotBody != "Pushover notifications are working." || gotJPEG != nil {
		t.Fatalf("message = %q %q %v", gotTitle, gotBody, gotJPEG)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("test endpoint changed the config file")
	}
	select {
	case <-store.Reloads():
		t.Fatal("test endpoint requested a reload")
	default:
	}
}

// TestNotificationTestEndpointSendError pins the 502 relay of the safe
// send error.
func TestNotificationTestEndpointSendError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeNotificationsConfig(t, path)
	store, srv := serve(t, path)
	store.sendTest = func(context.Context, config.Notifications, string, string, []byte) error {
		return errors.New("pushover did not accept the notification")
	}
	code, body := postTest(t, srv, NotificationsView{Pushover: PushoverView{AppToken: "t", UserKey: "u"}})
	if code != http.StatusBadGateway {
		t.Fatalf("send failure = %d, want 502", code)
	}
	if msg, _ := body["error"].(string); !strings.Contains(msg, "pushover did not accept the notification") {
		t.Fatalf("error body = %v", body)
	}
}

// TestNotificationTestEndpointRejectsInvalid pins the input checks: blank
// credentials without anything stored fail validation, and malformed JSON
// is a 400.
func TestNotificationTestEndpointRejectsInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	_, srv := serve(t, path)

	code, _ := postTest(t, srv, NotificationsView{Pushover: PushoverView{AppToken: " "}})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("blank credentials = %d, want 422", code)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/config/notifications/test", strings.NewReader("{oops"))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad JSON = %d, want 400", res.StatusCode)
	}
}
