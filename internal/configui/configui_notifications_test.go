package configui

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/notification"
)

// notificationsPage decodes the module-owned notifications API member.
type notificationsPage struct {
	Enabled  bool   `json:"enabled"`
	Provider string `json:"provider"`
	Pushover struct {
		HasAppToken bool   `json:"has_app_token"`
		HasUserKey  bool   `json:"has_user_key"`
		AppToken    string `json:"app_token,omitempty"`
		UserKey     string `json:"user_key,omitempty"`
	} `json:"pushover"`
}

// writeNotificationsConfig stores credentials to check redaction and preservation.
func writeNotificationsConfig(t *testing.T, path string) {
	t.Helper()
	const doc = `printers:
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
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestNotificationCredentialsAreNeverReturned(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeNotificationsConfig(t, path)
	_, srv := serve(t, path)
	res, err := http.Get(srv.URL + "/config/api")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-app-token", "secret-user-key"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("GET returned a stored notification credential")
		}
	}
	v := get(t, srv).Config.sections["notifications"].(*notificationsPage)
	if !v.Enabled || v.Provider != "pushover" || !v.Pushover.HasAppToken || !v.Pushover.HasUserKey {
		t.Fatalf("notifications view = %+v", v)
	}
	if v.Pushover.AppToken != "" || v.Pushover.UserKey != "" {
		t.Fatal("GET returned credential fields")
	}
}

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
	s, err := notification.SettingsOf(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := notification.Settings{Enabled: true, Provider: "pushover", Pushover: notification.Pushover{AppToken: "secret-app-token", UserKey: "secret-user-key"}}
	if s != want {
		t.Fatalf("written notifications = %+v, want %+v", s, want)
	}
}

// TestNotificationTestEndpointRejectsInvalid checks the generic route without
// calling an external provider; delivery and success are tested in notification.
func TestNotificationTestEndpointRejectsInvalid(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"undecodable", "{oops", http.StatusBadRequest},
		{"unknown field", `{"unknown":true}`, http.StatusBadRequest},
		{"missing credentials", `{}`, http.StatusUnprocessableEntity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "c.yaml")
			store, srv := serve(t, path)
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/config/notifications/test", bytes.NewBufferString(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			var body map[string]any
			if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if res.StatusCode != tc.status || body["error"] == nil {
				t.Fatalf("test = %d %v, want %d", res.StatusCode, body, tc.status)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("test action wrote config: %v", err)
			}
			select {
			case <-store.Reloads():
				t.Fatal("test action requested a reload")
			default:
			}
		})
	}
}
