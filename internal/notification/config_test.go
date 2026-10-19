package notification

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/configui"
	"gopkg.in/yaml.v3"
)

func TestMain(m *testing.M) {
	config.RegisterSection(ConfigSection)
	os.Exit(m.Run())
}

func settings(t *testing.T, c *config.Config) Settings {
	t.Helper()
	s, err := SettingsOf(c)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestNotificationsYAMLRoundTrip(t *testing.T) {
	const doc = `notifications:
  enabled: true
  provider: pushover
  pushover:
    app_token: token-1
    user_key: user-1
`
	c, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	want := Settings{Enabled: true, Provider: "pushover", Pushover: Pushover{AppToken: "token-1", UserKey: "user-1"}}
	if got := settings(t, c); got != want {
		t.Fatalf("parsed = %+v, want %+v", got, want)
	}
	raw, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	again, err := config.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got := settings(t, again); got != want {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestNotificationsProviderDefaultsToPushover(t *testing.T) {
	for _, c := range []*config.Config{nil, {}} {
		if c != nil {
			c.ApplyDefaults()
		}
		if got := settings(t, c).Provider; got != "pushover" {
			t.Fatalf("provider = %q", got)
		}
	}
	c := &config.Config{}
	c.SetSection("notifications", Settings{})
	before, _ := yaml.Marshal(c)
	if got := settings(t, c).Provider; got != "pushover" {
		t.Fatalf("provider = %q", got)
	}
	after, _ := yaml.Marshal(c)
	if string(before) != string(after) {
		t.Fatal("SettingsOf mutated configuration")
	}
}

func TestNotificationsValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      Settings
		wantErr bool
	}{
		{"disabled blank", Settings{}, false},
		{"disabled provider", Settings{Provider: "pushover"}, false},
		{"disabled stale", Settings{Pushover: Pushover{AppToken: "stale", UserKey: "stale"}}, false},
		{"empty provider", Settings{Enabled: true, Pushover: Pushover{AppToken: "t", UserKey: "u"}}, false},
		{"missing app", Settings{Enabled: true}, true},
		{"whitespace app", Settings{Enabled: true, Pushover: Pushover{AppToken: " \t", UserKey: "u"}}, true},
		{"missing user", Settings{Enabled: true, Pushover: Pushover{AppToken: "t"}}, true},
		{"whitespace user", Settings{Enabled: true, Pushover: Pushover{AppToken: "t", UserKey: " \n"}}, true},
		{"padded", Settings{Enabled: true, Pushover: Pushover{AppToken: " t ", UserKey: " u "}}, false},
		{"disabled unsupported", Settings{Provider: "email"}, true},
		{"enabled unsupported", Settings{Enabled: true, Provider: "email", Pushover: Pushover{AppToken: "t", UserKey: "u"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.in.Validate(); (err != nil) != tc.wantErr {
				t.Fatalf("Validate = %v, want error = %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateEffectiveSurfacesNotifications(t *testing.T) {
	c := &config.Config{}
	c.ApplyDefaults()
	c.SetSection("notifications", Settings{Enabled: true})
	if err := c.ValidateEffective(); err == nil {
		t.Fatal("ValidateEffective accepted enabled notifications without credentials")
	}
	c.SetSection("notifications", Settings{Enabled: true, Pushover: Pushover{AppToken: "t", UserKey: "u"}})
	if err := c.ValidateEffective(); err != nil {
		t.Fatal(err)
	}
}

func TestViewRedactsCredentials(t *testing.T) {
	c := &config.Config{}
	c.SetSection("notifications", Settings{Enabled: true, Pushover: Pushover{AppToken: "secret-app", UserKey: "secret-user"}})
	raw, err := json.Marshal(ConfigSection.View(c))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(raw), `{"enabled":true,"provider":"pushover","pushover":{"has_app_token":true,"has_user_key":true}}`; got != want {
		t.Fatalf("View = %s, want %s", got, want)
	}
}

func TestSaveTrimsAndKeepsCredentials(t *testing.T) {
	stored := &config.Config{}
	stored.SetSection("notifications", Settings{Enabled: true, Pushover: Pushover{AppToken: " old-app ", UserKey: " old-user "}})
	for _, tc := range []struct {
		name   string
		raw    string
		stored *config.Config
		want   Settings
	}{
		{"blank keeps", `{"enabled":true,"pushover":{"app_token":" ","user_key":"\t","has_app_token":false,"has_user_key":false}}`, stored, Settings{Enabled: true, Provider: "pushover", Pushover: Pushover{AppToken: "old-app", UserKey: "old-user"}}},
		{"replace trimmed", `{"enabled":true,"provider":"  pushover ","pushover":{"app_token":" new-app ","user_key":"\tnew-user\n"}}`, stored, Settings{Enabled: true, Provider: "pushover", Pushover: Pushover{AppToken: "new-app", UserKey: "new-user"}}},
		{"partial replace", `{"pushover":{"app_token":" new-app "}}`, stored, Settings{Provider: "pushover", Pushover: Pushover{AppToken: "new-app", UserKey: "old-user"}}},
		{"first save", `{"enabled":true,"provider":" pushover ","pushover":{"app_token":" first-app ","user_key":"first-user"}}`, nil, Settings{Enabled: true, Provider: "pushover", Pushover: Pushover{AppToken: "first-app", UserKey: "first-user"}}},
		{"missing member", "", stored, Settings{Provider: "pushover", Pushover: Pushover{AppToken: "old-app", UserKey: "old-user"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := &config.Config{}
			if err := ConfigSection.Save(json.RawMessage(tc.raw), tc.stored, out); err != nil {
				t.Fatal(err)
			}
			if got := settings(t, out); got != tc.want {
				t.Fatalf("Save = %+v, want %+v", got, tc.want)
			}
		})
	}
	if got := settings(t, stored).Pushover.AppToken; got != " old-app " {
		t.Fatal("Save mutated stored credentials")
	}
}

func TestSaveRejectsUndecodableSettings(t *testing.T) {
	for _, raw := range []string{`{oops`, `{"unknown":true}`, `{"enabled":"yes"}`, `{"pushover":{"unknown":true}}`} {
		out := &config.Config{}
		err := ConfigSection.Save(json.RawMessage(raw), nil, out)
		var pageErr *config.PageError
		if !errors.As(err, &pageErr) || pageErr.Status != http.StatusBadRequest {
			t.Fatalf("Save(%s) = %v, want 400", raw, err)
		}
		if len(out.Sections) != 0 {
			t.Fatal("invalid save changed output")
		}
	}
}

func TestTestMergesStoredCredentialsAndSends(t *testing.T) {
	oldSend := send
	t.Cleanup(func() { send = oldSend })
	stored := &config.Config{}
	stored.SetSection("notifications", Settings{Pushover: Pushover{AppToken: " old-app ", UserKey: " old-user "}})
	before, _ := yaml.Marshal(stored)
	reads, sends := 0, 0
	send = func(ctx context.Context, got Settings, title, body string, jpeg []byte) error {
		sends++
		want := Settings{Enabled: true, Provider: "pushover", Pushover: Pushover{AppToken: "old-app", UserKey: "new-user"}}
		if got != want {
			t.Fatalf("sent settings = %+v, want %+v", got, want)
		}
		if title == "" || body == "" || jpeg != nil {
			t.Fatalf("message = %q %q %v, want a text-only test notification", title, body, jpeg)
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 20*time.Second {
			t.Fatal("test send must have a 20 s deadline")
		}
		return nil
	}
	err := ConfigSection.Test(context.Background(), json.RawMessage(`{"enabled":false,"pushover":{"user_key":" new-user "}}`), func() (*config.Config, error) { reads++; return stored, nil })
	if err != nil {
		t.Fatal(err)
	}
	if reads != 1 || sends != 1 {
		t.Fatalf("reads=%d sends=%d", reads, sends)
	}
	after, _ := yaml.Marshal(stored)
	if string(before) != string(after) {
		t.Fatal("test action changed stored configuration")
	}
}

func TestTestFailures(t *testing.T) {
	oldSend := send
	t.Cleanup(func() { send = oldSend })
	sendErr := errors.New("pushover did not accept the notification")
	for _, tc := range []struct {
		name, raw            string
		readErr              error
		status, reads, sends int
	}{
		{"malformed", `{oops`, nil, 400, 0, 0},
		{"unknown", `{"unknown":true}`, nil, 400, 0, 0},
		{"invalid type", `{"enabled":"yes"}`, nil, 400, 0, 0},
		{"blank credentials", `{}`, nil, 422, 1, 0},
		{"unsupported provider", `{"provider":"email"}`, nil, 422, 1, 0},
		{"file error", `{}`, errors.New("read failed"), 500, 1, 0},
		{"send failure", `{"pushover":{"app_token":"t","user_key":"u"}}`, nil, 502, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads, sends := 0, 0
			send = func(context.Context, Settings, string, string, []byte) error { sends++; return sendErr }
			err := ConfigSection.Test(context.Background(), json.RawMessage(tc.raw), func() (*config.Config, error) { reads++; return nil, tc.readErr })
			status := http.StatusBadGateway
			var pageErr *config.PageError
			if errors.As(err, &pageErr) {
				status = pageErr.Status
			}
			if err == nil || status != tc.status || reads != tc.reads || sends != tc.sends {
				t.Fatalf("err=%v status=%d reads=%d sends=%d", err, status, reads, sends)
			}
			if tc.status == 502 && (!errors.Is(err, sendErr) || pageErr != nil) {
				t.Fatalf("send failure must remain plain: %v", err)
			}
		})
	}
}

// TestTestHTTPRoute verifies the generic handler with the module's send seam.
func TestTestHTTPRoute(t *testing.T) {
	oldSend := send
	t.Cleanup(func() { send = oldSend })
	path := filepath.Join(t.TempDir(), "config.yaml")
	const doc = "notifications:\n  pushover:\n    app_token: stored-app\n    user_key: stored-user\n"
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	store := configui.NewStore(path)
	mux := http.NewServeMux()
	store.Register(mux)
	for _, tc := range []struct {
		name   string
		err    error
		status int
		want   string
	}{
		{"success", nil, http.StatusOK, `{"ok":true}`},
		{"send failure", errors.New("pushover did not accept the notification"), http.StatusBadGateway, `{"error":"pushover did not accept the notification"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			send = func(_ context.Context, s Settings, title, body string, jpeg []byte) error {
				calls++
				if !s.Enabled || s.Provider != "pushover" || s.Pushover.AppToken != "stored-app" || s.Pushover.UserKey != "stored-user" {
					t.Fatalf("sent settings = %+v", s)
				}
				return tc.err
			}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/config/notifications/test", strings.NewReader("{}")))
			if w.Code != tc.status || strings.TrimSpace(w.Body.String()) != tc.want || calls != 1 {
				t.Fatalf("response = %d %s, sends = %d", w.Code, w.Body.String(), calls)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != doc {
				t.Fatalf("test changed file: %q %v", after, err)
			}
			select {
			case <-store.Saved():
				t.Fatal("test requested a restart")
			default:
			}
		})
	}
}
