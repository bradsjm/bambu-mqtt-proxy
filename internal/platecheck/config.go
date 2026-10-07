// Package platecheck checks a fresh startup build-plate image and requests stop only.
package platecheck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

const (
	// ProviderCloudflare selects Cloudflare Workers AI.
	ProviderCloudflare = "cloudflare"
	// ProviderCustom selects a custom HTTPS endpoint.
	ProviderCustom = "custom"
	// platecheckTestTimeout bounds the configuration key test.
	platecheckTestTimeout = 20 * time.Second
	// endpointKeyMessage explains why a saved credential cannot move to a new destination.
	endpointKeyMessage = "Enter an API key for this provider. The stored key is only used with the account ID or endpoint it was saved with."
)

// Settings configures startup build-plate checks.
type Settings struct {
	// Enabled selects automatic protection; nil follows destination and key presence.
	Enabled *bool `yaml:"enabled"`
	// Provider selects Cloudflare or a custom endpoint.
	Provider string `yaml:"provider"`
	// AccountID identifies the Cloudflare account.
	AccountID string `yaml:"account_id"`
	// Endpoint is the full HTTPS custom endpoint.
	Endpoint string `yaml:"endpoint"`
	// APIKey is the private provider credential.
	APIKey string `yaml:"api_key"`
	// Model selects the provider model.
	Model string `yaml:"model"`
	// StopConfidence is the strict occupied-probability cutoff.
	StopConfidence float64 `yaml:"stop_confidence"`
}

// On reports whether automatic startup checks are enabled.
func (s Settings) On() bool {
	if s.Enabled != nil {
		return *s.Enabled
	}
	return s.URL() != "" && s.Key() != ""
}

// Key returns the trimmed private API key.
func (s Settings) Key() string { return strings.TrimSpace(s.APIKey) }

// normalized trims string fields and canonicalizes the Cloudflare account ID.
func (s Settings) normalized() Settings {
	s.Provider = strings.TrimSpace(s.Provider)
	s.AccountID = strings.ToLower(strings.TrimSpace(s.AccountID))
	s.Endpoint = strings.TrimSpace(s.Endpoint)
	s.APIKey = s.Key()
	s.Model = strings.TrimSpace(s.Model)
	return s
}

// URL returns the provider endpoint for these settings.
func (s Settings) URL() string {
	s = s.normalized()
	if s.Provider == ProviderCustom {
		return s.Endpoint
	}
	if s.AccountID == "" {
		return ""
	}
	return "https://api.cloudflare.com/client/v4/accounts/" + s.AccountID + "/ai/run/@cf/cloudflare/" + s.Model
}

// destination identifies the account or endpoint to which a credential belongs.
func (s Settings) destination() string {
	if s.Provider == ProviderCustom {
		return "custom " + s.Endpoint
	}
	return "cloudflare " + s.AccountID
}

// SettingsOf decodes and normalizes accepted settings without changing the config.
func SettingsOf(c *config.Config) (Settings, error) {
	v, err := c.Section("platecheck")
	if err != nil {
		return Settings{}, err
	}
	s := v.(*Settings).normalized()
	n := math.Round(s.StopConfidence * 100)
	if s.StopConfidence >= 0.5 && s.StopConfidence <= 0.99 && !math.IsNaN(n) && !math.IsInf(n, 0) && math.Abs(s.StopConfidence*100-n) <= 1e-9 {
		s.StopConfidence = n / 100
	}
	return s, nil
}

// settingsView exchanges page settings without returning the private key.
type settingsView struct {
	// Enabled selects automatic protection.
	Enabled bool `json:"enabled"`
	// Provider selects the service provider.
	Provider string `json:"provider"`
	// AccountID is the public Cloudflare account ID.
	AccountID string `json:"account_id"`
	// Endpoint is the public effective endpoint.
	Endpoint string `json:"endpoint"`
	// APIKey is accepted only on submission.
	APIKey string `json:"api_key,omitempty"`
	// HasAPIKey reports stored credential presence.
	HasAPIKey bool `json:"has_api_key"`
	// Model selects the provider model.
	Model string `json:"model"`
	// StopConfidence is the occupied-probability cutoff.
	StopConfidence float64 `json:"stop_confidence"`
}

// decodeSettingsView accepts known fields and defaults an absent page section.
func decodeSettingsView(raw json.RawMessage) (settingsView, error) {
	in := settingsView{Provider: ProviderCloudflare, Model: "clef", StopConfidence: 0.5}
	if len(raw) == 0 {
		return in, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return in, &config.PageError{Status: 400, Message: "The plate-check settings must be a JSON object."}
	}
	if err := dec.Decode(&in); err != nil {
		return in, &config.PageError{Status: 400, Message: "The plate-check settings could not be read."}
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return in, &config.PageError{Status: 400, Message: "The plate-check settings must contain one JSON object."}
	}
	return in, nil
}

// accountIDPattern matches a normalized Cloudflare account ID.
var accountIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// validateSettings validates and normalizes settings, optionally requiring credentials.
func validateSettings(s Settings, require bool) (Settings, error) {
	s = s.normalized()
	if s.Provider != ProviderCloudflare && s.Provider != ProviderCustom {
		return s, errors.New("The plate-check provider must be cloudflare or custom.")
	}
	n := math.Round(s.StopConfidence * 100)
	if math.IsNaN(s.StopConfidence) || math.IsInf(s.StopConfidence, 0) || s.StopConfidence < 0.5 || s.StopConfidence > 0.99 || math.Abs(s.StopConfidence*100-n) > 1e-9 {
		return s, errors.New("Stop above must be a whole percentage from 50 through 99 percent.")
	}
	s.StopConfidence = n / 100
	if s.Provider == ProviderCloudflare {
		if s.Endpoint != "" {
			return s, errors.New("The Cloudflare provider builds the endpoint from the account ID. Remove the endpoint or choose the custom provider.")
		}
		if s.Model != "clef" && s.Model != "clef-flash" {
			return s, errors.New("The Cloudflare model must be clef or clef-flash.")
		}
		if s.AccountID != "" && !accountIDPattern.MatchString(s.AccountID) {
			return s, errors.New("The Cloudflare account ID must be 32 hexadecimal characters.")
		}
		if require && s.AccountID == "" {
			return s, errors.New("No Cloudflare account ID is configured. Enter the account ID.")
		}
	} else {
		if s.Model == "" {
			return s, errors.New("Enter the model name for the custom endpoint.")
		}
		if s.Endpoint != "" {
			u, err := url.Parse(s.Endpoint)
			if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(s.Endpoint, "#") {
				return s, errors.New("The endpoint must be a full HTTPS URL without credentials, a query, or a fragment.")
			}
		}
		if require && s.Endpoint == "" {
			return s, errors.New("No endpoint is configured. Enter an HTTPS endpoint.")
		}
	}
	if require && s.Key() == "" {
		return s, errors.New("No API key is configured. Enter a key.")
	}
	return s, nil
}

// submittedSettings resolves a submitted page view with a blank-key fallback.
func submittedSettings(in settingsView, key string) Settings {
	if strings.TrimSpace(in.APIKey) != "" {
		key = in.APIKey
	}
	s := Settings{Enabled: &in.Enabled, Provider: in.Provider, AccountID: in.AccountID, Endpoint: in.Endpoint, APIKey: key, Model: in.Model, StopConfidence: in.StopConfidence}.normalized()
	if s.Provider == ProviderCustom {
		s.AccountID = ""
	} else {
		s.Endpoint = ""
	}
	return s
}

// resolveTestSettings binds the stored credential before diagnostic calls.
// The stored key only follows a submission for its own account or endpoint.
func resolveTestSettings(in settingsView, base Settings) (Settings, error) {
	s := submittedSettings(in, "")
	base = base.normalized()
	if s.Key() == "" {
		if s.destination() != base.destination() {
			return s, errors.New(endpointKeyMessage)
		}
		s.APIKey = base.Key()
	}
	return validateSettings(s, true)
}

// ConfigSection owns file settings and configuration-page operations.
var ConfigSection = config.Section{
	Key: "platecheck",
	New: func() any { return &Settings{Provider: ProviderCloudflare, Model: "clef", StopConfidence: 0.5} },
	Validate: func(c *config.Config) error {
		s, err := SettingsOf(c)
		if err != nil {
			return err
		}
		_, err = validateSettings(s, s.On())
		return err
	},
	View: func(file *config.Config) any {
		s, _ := SettingsOf(file)
		return settingsView{Enabled: s.On(), Provider: s.Provider, AccountID: s.AccountID, Endpoint: s.Endpoint, HasAPIKey: s.Key() != "", Model: s.Model, StopConfidence: s.StopConfidence}
	},
	Save: func(raw json.RawMessage, stored, out *config.Config) error {
		in, err := decodeSettingsView(raw)
		if err != nil {
			return err
		}
		old, err := SettingsOf(stored)
		if err != nil {
			return err
		}
		key := ""
		if submittedSettings(in, "").destination() == old.destination() {
			key = old.Key()
		}
		s := submittedSettings(in, key)
		checked, err := validateSettings(s, s.On())
		if err != nil {
			return &config.PageError{Status: 400, Message: err.Error()}
		}
		s.StopConfidence = checked.StopConfidence
		out.SetSection("platecheck", s)
		return nil
	},
	Test: func(ctx context.Context, raw json.RawMessage, stored func() (*config.Config, error)) error {
		in, err := decodeSettingsView(raw)
		if err != nil {
			return err
		}
		var old Settings
		if strings.TrimSpace(in.APIKey) == "" {
			file, err := stored()
			if err != nil {
				return &config.PageError{Status: 500, Message: "The stored plate-check settings could not be read."}
			}
			old, err = SettingsOf(file)
			if err != nil {
				return &config.PageError{Status: 500, Message: "The stored plate-check settings could not be read."}
			}
		}
		s, err := resolveTestSettings(in, old)
		if err != nil {
			return &config.PageError{Status: 422, Message: err.Error()}
		}
		ctx, cancel := context.WithTimeout(ctx, platecheckTestTimeout)
		defer cancel()
		if err := probe(ctx, s); err != nil {
			return errors.New(fixedSentence(errorCategory(err)))
		}
		return nil
	},
}

// probe tests connectivity without uploading images; package tests replace it.
var probe = func(ctx context.Context, s Settings) error { return NewClient(s, nil).Probe(ctx) }
