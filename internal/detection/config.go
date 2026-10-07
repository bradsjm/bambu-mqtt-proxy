package detection

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Settings configures Gadget detection. An unset Enabled follows the key-based
// default; explicit true requires a key and explicit false stays off.
type Settings struct {
	Enabled *bool  `yaml:"enabled"`
	APIKey  string `yaml:"api_key"`
}

// On reports whether detection should run with the stored settings.
func (s Settings) On() bool {
	if s.Enabled != nil {
		return *s.Enabled
	}
	return s.Key() != ""
}

// Key returns the trimmed configured API key.
func (s Settings) Key() string { return strings.TrimSpace(s.APIKey) }

// SettingsOf decodes detection settings without changing the configuration.
func SettingsOf(c *config.Config) (Settings, error) {
	v, err := c.Section("detection")
	if err != nil {
		return Settings{}, err
	}
	return *v.(*Settings), nil
}

// settingsView is the write-only-key representation exchanged with the page.
type settingsView struct {
	Enabled   bool   `json:"enabled"`
	HasAPIKey bool   `json:"has_api_key"`
	APIKey    string `json:"api_key,omitempty"`
}

// decodeSettingsView strictly decodes submitted detection page settings.
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

// ConfigSection owns detection's file and configuration-page policy.
var ConfigSection = config.Section{
	Key: "detection",
	New: func() any { return &Settings{} },
	Validate: func(c *config.Config) error {
		s, err := SettingsOf(c)
		if err != nil {
			return err
		}
		if s.On() && s.Key() == "" {
			return errors.New("detection: enabled requires an API key (detection.api_key)")
		}
		return nil
	},
	View: func(file *config.Config) any {
		s, _ := SettingsOf(file)
		return settingsView{Enabled: s.On(), HasAPIKey: s.Key() != ""}
	},
	Save: func(raw json.RawMessage, stored, out *config.Config) error {
		in, err := decodeSettingsView(raw)
		if err != nil {
			return err
		}
		s := Settings{Enabled: &in.Enabled, APIKey: strings.TrimSpace(in.APIKey)}
		if s.APIKey == "" && stored != nil {
			if previous, err := SettingsOf(stored); err == nil {
				s.APIKey = previous.APIKey
			}
		}
		out.SetSection("detection", s)
		return nil
	},
	Test: func(ctx context.Context, raw json.RawMessage, stored func() (*config.Config, error)) error {
		in, err := decodeSettingsView(raw)
		if err != nil {
			return err
		}
		key := strings.TrimSpace(in.APIKey)
		if key == "" {
			file, err := stored()
			if err != nil {
				return &config.PageError{Status: http.StatusInternalServerError, Message: err.Error()}
			}
			if file != nil {
				if s, err := SettingsOf(file); err == nil {
					key = s.APIKey
				}
			}
		}
		if key == "" {
			return &config.PageError{Status: http.StatusUnprocessableEntity, Message: "No Gadget API key is configured. Enter a key."}
		}
		ctx, cancel := context.WithTimeout(ctx, detectionTestTimeout)
		defer cancel()
		if err := probe(ctx, key); err != nil {
			return errors.New(classifyDetectionError(err))
		}
		return nil
	},
}

// detectionTestTimeout bounds the key test; the client also bounds each request.
const detectionTestTimeout = 20 * time.Second

// probe creates and discards one context without processing any frames.
// Tests replace this package-local seam.
var probe = func(ctx context.Context, key string) error {
	_, err := NewGadgetClient(key).CreateContext(ctx)
	return err
}

// classifyDetectionError returns actionable, sanitized provider classifications.
func classifyDetectionError(err error) string {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if apiErr.AccountTerminal() {
			return "OctoEverywhere rejected the key or its account: the key may be invalid, disabled, out of free allowance, or restricted to other IP addresses. Check the key on octoeverywhere.com."
		}
		return "The Gadget service refused to create a context (" + apiErr.Error() + "). Try again later."
	}
	return "The Gadget service could not be reached: " + err.Error() + "."
}
