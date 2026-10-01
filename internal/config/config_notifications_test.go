package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestNotificationsYAMLRoundTrip pins the YAML shape of the notifications
// section: parse, marshal, and re-parse must preserve every field.
func TestNotificationsYAMLRoundTrip(t *testing.T) {
	const y = `notifications:
  enabled: true
  provider: pushover
  pushover:
    app_token: token-1
    user_key: user-1
`
	cfg, err := Parse([]byte(y))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := cfg.Notifications
	if !got.Enabled || got.Provider != "pushover" ||
		got.Pushover.AppToken != "token-1" || got.Pushover.UserKey != "user-1" {
		t.Fatalf("parsed = %+v", got)
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	again, err := Parse(raw)
	if err != nil {
		t.Fatalf("re-Parse %q: %v", raw, err)
	}
	if again.Notifications != got {
		t.Fatalf("round trip = %+v, want %+v", again.Notifications, got)
	}
}

// TestNotificationsProviderDefaultsToPushover pins the ApplyDefaults rule.
func TestNotificationsProviderDefaultsToPushover(t *testing.T) {
	cfg := &Config{}
	cfg.ApplyDefaults()
	if cfg.Notifications.Provider != "pushover" {
		t.Fatalf("provider = %q, want pushover", cfg.Notifications.Provider)
	}
}

// TestNotificationsValidate pins when blank credentials pass and fail, and
// that unsupported providers are rejected.
func TestNotificationsValidate(t *testing.T) {
	// Disabled notifications may keep blank credentials.
	if err := (Notifications{}).Validate(); err != nil {
		t.Fatalf("disabled blank credentials: %v", err)
	}
	if err := (Notifications{Provider: "pushover"}).Validate(); err != nil {
		t.Fatalf("disabled blank with provider: %v", err)
	}
	// An empty provider means pushover.
	if err := (Notifications{Enabled: true, Pushover: Pushover{AppToken: "t", UserKey: "u"}}).Validate(); err != nil {
		t.Fatalf("empty provider with credentials: %v", err)
	}
	// Enabled notifications require both credentials.
	err := (Notifications{Enabled: true}).Validate()
	if err == nil || !strings.Contains(err.Error(), "notifications: pushover app token is required") {
		t.Fatalf("enabled without app token = %v", err)
	}
	err = (Notifications{Enabled: true, Pushover: Pushover{AppToken: "t"}}).Validate()
	if err == nil || !strings.Contains(err.Error(), "notifications: pushover user key is required") {
		t.Fatalf("enabled without user key = %v", err)
	}
	// Credentials only need to be non-blank.
	if err := (Notifications{Enabled: true, Pushover: Pushover{AppToken: " t ", UserKey: " u "}}).Validate(); err != nil {
		t.Fatalf("blank-padded credentials: %v", err)
	}
	// Unsupported providers are rejected, enabled or not.
	err = (Notifications{Provider: "email"}).Validate()
	if err == nil || !strings.Contains(err.Error(), `notifications: unsupported provider "email"`) {
		t.Fatalf("disabled provider email = %v", err)
	}
	err = (Notifications{Enabled: true, Provider: "email", Pushover: Pushover{AppToken: "t", UserKey: "u"}}).Validate()
	if err == nil || !strings.Contains(err.Error(), `notifications: unsupported provider "email"`) {
		t.Fatalf("enabled provider email = %v", err)
	}
}

// TestConfigValidateSurfacesNotifications pins that the top-level
// validation reports notification failures.
func TestConfigValidateSurfacesNotifications(t *testing.T) {
	cfg := &Config{}
	cfg.ApplyDefaults()
	cfg.Listen = []Listener{{Port: 8883, TLS: true}}
	cfg.Notifications = Notifications{Enabled: true}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "notifications: pushover app token is required") {
		t.Fatalf("Config.Validate = %v", err)
	}
	cfg.Notifications = Notifications{Enabled: true, Provider: "pushover",
		Pushover: Pushover{AppToken: "t", UserKey: "u"}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with credentials: %v", err)
	}
}
