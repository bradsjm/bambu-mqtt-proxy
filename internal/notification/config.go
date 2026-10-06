package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Settings configures optional print alerts sent to one recipient.
type Settings struct {
	Enabled  bool     `yaml:"enabled"`
	Provider string   `yaml:"provider"`
	Pushover Pushover `yaml:"pushover"`
}

// Pushover holds the Pushover application token and user key.
type Pushover struct {
	AppToken string `yaml:"app_token"`
	UserKey  string `yaml:"user_key"`
}

// Validate rejects unsupported providers and missing enabled credentials.
// Disabled notifications may keep blank or stale credentials.
func (s Settings) Validate() error {
	switch s.Provider {
	case "", "pushover":
	default:
		return fmt.Errorf("notifications: unsupported provider %q", s.Provider)
	}
	if !s.Enabled {
		return nil
	}
	if strings.TrimSpace(s.Pushover.AppToken) == "" {
		return fmt.Errorf("notifications: pushover app token is required")
	}
	if strings.TrimSpace(s.Pushover.UserKey) == "" {
		return fmt.Errorf("notifications: pushover user key is required")
	}
	return nil
}

// SettingsOf decodes notification settings and applies the provider default
// without changing the configuration.
func SettingsOf(c *config.Config) (Settings, error) {
	v, err := c.Section("notifications")
	if err != nil {
		return Settings{}, err
	}
	s := *v.(*Settings)
	if s.Provider == "" {
		s.Provider = "pushover"
	}
	return s, nil
}

// settingsView is the notification page representation with write-only credentials.
type settingsView struct {
	Enabled  bool         `json:"enabled"`
	Provider string       `json:"provider"`
	Pushover pushoverView `json:"pushover"`
}

// pushoverView reports stored credential presence and accepts replacement values.
type pushoverView struct {
	HasAppToken bool   `json:"has_app_token"`
	HasUserKey  bool   `json:"has_user_key"`
	AppToken    string `json:"app_token,omitempty"`
	UserKey     string `json:"user_key,omitempty"`
}

// decodeSettingsView strictly decodes submitted notification page settings.
func decodeSettingsView(raw json.RawMessage) (settingsView, error) {
	var in settingsView
	if len(raw) == 0 {
		return in, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return in, &config.PageError{Status: http.StatusBadRequest, Message: "The settings could not be read: " + err.Error()}
	}
	return in, nil
}

// toSettings trims submitted values and retains blank credentials from stored.
// Incoming presence flags do not affect the saved values.
func (v settingsView) toSettings(stored Settings) Settings {
	provider := strings.TrimSpace(v.Provider)
	if provider == "" {
		provider = strings.TrimSpace(stored.Provider)
	}
	if provider == "" {
		provider = "pushover"
	}
	out := Settings{Enabled: v.Enabled, Provider: provider, Pushover: Pushover{
		AppToken: strings.TrimSpace(v.Pushover.AppToken),
		UserKey:  strings.TrimSpace(v.Pushover.UserKey),
	}}
	if out.Pushover.AppToken == "" {
		out.Pushover.AppToken = strings.TrimSpace(stored.Pushover.AppToken)
	}
	if out.Pushover.UserKey == "" {
		out.Pushover.UserKey = strings.TrimSpace(stored.Pushover.UserKey)
	}
	return out
}

// ConfigSection owns notification file and configuration-page policy.
var ConfigSection = config.Section{
	Key: "notifications",
	New: func() any { return &Settings{} },
	Validate: func(c *config.Config) error {
		s, err := SettingsOf(c)
		if err != nil {
			return err
		}
		return s.Validate()
	},
	View: func(file *config.Config) any {
		s, _ := SettingsOf(file)
		return settingsView{Enabled: s.Enabled, Provider: s.Provider, Pushover: pushoverView{
			HasAppToken: s.Pushover.AppToken != "", HasUserKey: s.Pushover.UserKey != "",
		}}
	},
	Save: func(raw json.RawMessage, stored, out *config.Config) error {
		in, err := decodeSettingsView(raw)
		if err != nil {
			return err
		}
		previous, _ := SettingsOf(stored)
		out.SetSection("notifications", in.toSettings(previous))
		return nil
	},
	Test: func(ctx context.Context, raw json.RawMessage, stored func() (*config.Config, error)) error {
		in, err := decodeSettingsView(raw)
		if err != nil {
			return err
		}
		file, err := stored()
		if err != nil {
			return &config.PageError{Status: http.StatusInternalServerError, Message: err.Error()}
		}
		previous, _ := SettingsOf(file)
		s := in.toSettings(previous)
		s.Enabled = true
		if err := s.Validate(); err != nil {
			return &config.PageError{Status: http.StatusUnprocessableEntity, Message: err.Error()}
		}
		ctx, cancel := context.WithTimeout(ctx, notificationTestTimeout)
		defer cancel()
		return send(ctx, s, "bambu-mqtt-proxy test", "Pushover notifications are working.", nil)
	},
}

// notificationTestTimeout bounds a configuration-page test delivery.
const notificationTestTimeout = 20 * time.Second

// send delivers a configuration-page test message; tests replace this seam.
var send = Send
