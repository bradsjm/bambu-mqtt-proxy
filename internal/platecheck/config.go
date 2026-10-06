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
	"os"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Environment settings replace file settings by presence, including empty values.
const (
	// EnvEndpoint overrides the stored Clef endpoint.
	EnvEndpoint = "BMBPX_PLATECHECK_ENDPOINT"
	// EnvAPIKey overrides the stored Clef API key.
	EnvAPIKey = "BMBPX_PLATECHECK_API_KEY"
	// platecheckTestTimeout bounds the configuration key test.
	platecheckTestTimeout = 20 * time.Second
	// endpointKeyMessage explains why a saved credential cannot move to a new destination.
	endpointKeyMessage = "Enter an API key for this endpoint. The stored key is only used with the endpoint it was saved with."
)

// Settings configures startup build-plate checks.
type Settings struct {
	// Enabled selects automatic protection; nil follows endpoint and key presence.
	Enabled *bool `yaml:"enabled"`
	// Endpoint is the full HTTPS Clef endpoint.
	Endpoint string `yaml:"endpoint"`
	// APIKey is the private provider credential.
	APIKey string `yaml:"api_key"`
	// Model selects clef or clef-flash.
	Model string `yaml:"model"`
	// StopConfidence is the strict occupied-probability cutoff.
	StopConfidence float64 `yaml:"stop_confidence"`
}

// On reports whether automatic startup checks are enabled.
func (s Settings) On() bool {
	if s.Enabled != nil {
		return *s.Enabled
	}
	return strings.TrimSpace(s.Endpoint) != "" && s.Key() != ""
}

// Key returns the trimmed private API key.
func (s Settings) Key() string { return strings.TrimSpace(s.APIKey) }

// SettingsOf decodes and normalizes accepted settings without changing the config.
func SettingsOf(c *config.Config) (Settings, error) {
	v, err := c.Section("platecheck")
	if err != nil {
		return Settings{}, err
	}
	s := *v.(*Settings)
	s.Endpoint, s.APIKey = strings.TrimSpace(s.Endpoint), s.Key()
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
	// Endpoint is the public effective endpoint.
	Endpoint string `json:"endpoint"`
	// APIKey is accepted only on submission.
	APIKey string `json:"api_key,omitempty"`
	// HasAPIKey reports effective credential presence.
	HasAPIKey bool `json:"has_api_key"`
	// Model selects the provider model.
	Model string `json:"model"`
	// StopConfidence is the occupied-probability cutoff.
	StopConfidence float64 `json:"stop_confidence"`
}

// decodeSettingsView accepts known fields and defaults an absent page section.
func decodeSettingsView(raw json.RawMessage) (settingsView, error) {
	in := settingsView{Model: "clef", StopConfidence: 0.5}
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

// applyEnv binds an overridden credential to its configured destination.
func applyEnv(s Settings) Settings {
	endpoint, endpointSet := os.LookupEnv(EnvEndpoint)
	key, keySet := os.LookupEnv(EnvAPIKey)
	if endpointSet {
		endpoint = strings.TrimSpace(endpoint)
		if !keySet && endpoint != strings.TrimSpace(s.Endpoint) {
			s.APIKey = ""
		}
		s.Endpoint = endpoint
	}
	if keySet {
		s.APIKey = key
	}
	s.Endpoint, s.APIKey = strings.TrimSpace(s.Endpoint), s.Key()
	return s
}

// validateSettings validates and normalizes settings, optionally requiring credentials.
func validateSettings(s Settings, require bool) (Settings, error) {
	s.Endpoint, s.APIKey = strings.TrimSpace(s.Endpoint), s.Key()
	if s.Model != "clef" && s.Model != "clef-flash" {
		return s, errors.New("The Clef model must be clef or clef-flash.")
	}
	n := math.Round(s.StopConfidence * 100)
	if math.IsNaN(s.StopConfidence) || math.IsInf(s.StopConfidence, 0) || s.StopConfidence < 0.5 || s.StopConfidence > 0.99 || math.Abs(s.StopConfidence*100-n) > 1e-9 {
		return s, errors.New("Stop above must be a whole percentage from 50 through 99 percent.")
	}
	s.StopConfidence = n / 100
	if s.Endpoint != "" {
		u, err := url.Parse(s.Endpoint)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(s.Endpoint, "#") {
			return s, errors.New("The Clef endpoint must be a full HTTPS URL without credentials, a query, or a fragment.")
		}
		for _, model := range []string{"clef", "clef-flash"} {
			if strings.HasSuffix(u.Path, "/ai/run/@cf/cloudflare/"+model) && s.Model != model {
				return s, errors.New("The Clef endpoint does not match the selected model.")
			}
		}
	}
	if require && s.Endpoint == "" {
		if _, ok := os.LookupEnv(EnvEndpoint); ok {
			return s, errors.New(EnvEndpoint + " is set but empty, so there is no endpoint to test. Set the variable to a valid HTTPS endpoint.")
		}
		return s, errors.New("No Clef endpoint is configured. Enter an HTTPS endpoint or set BMBPX_PLATECHECK_ENDPOINT.")
	}
	if require && s.Key() == "" {
		if _, ok := os.LookupEnv(EnvAPIKey); ok {
			return s, errors.New(EnvAPIKey + " is set but empty, so there is no key to test. Set the variable to a valid key.")
		}
		return s, errors.New("No Clef API key is configured. Enter a key or set BMBPX_PLATECHECK_API_KEY.")
	}
	return s, nil
}

// submittedSettings resolves a submitted page view with a blank-key fallback.
func submittedSettings(in settingsView, key string) Settings {
	if strings.TrimSpace(in.APIKey) != "" {
		key = in.APIKey
	}
	return Settings{Enabled: &in.Enabled, Endpoint: strings.TrimSpace(in.Endpoint), APIKey: strings.TrimSpace(key), Model: in.Model, StopConfidence: in.StopConfidence}
}

// resolveTestSettings binds fallback and environment credentials before diagnostic calls.
func resolveTestSettings(in settingsView, base Settings) (Settings, error) {
	s := submittedSettings(in, "")
	submittedEndpoint := s.Endpoint
	endpoint, endpointSet := os.LookupEnv(EnvEndpoint)
	key, keySet := os.LookupEnv(EnvAPIKey)
	if endpointSet {
		s.Endpoint = strings.TrimSpace(endpoint)
	}
	if keySet {
		// An environment key without a paired endpoint belongs to the existing endpoint.
		if !endpointSet && submittedEndpoint != strings.TrimSpace(base.Endpoint) {
			return s, errors.New(endpointKeyMessage)
		}
		s.APIKey = strings.TrimSpace(key)
	} else if s.Key() == "" {
		if submittedEndpoint != strings.TrimSpace(base.Endpoint) || s.Endpoint != strings.TrimSpace(base.Endpoint) {
			return s, errors.New(endpointKeyMessage)
		}
		s.APIKey = base.Key()
	} else if s.Endpoint != submittedEndpoint {
		// A typed key is an explicit choice for the submitted endpoint only.
		return s, errors.New(endpointKeyMessage)
	}
	return validateSettings(s, true)
}

// ConfigSection owns file settings, effective overrides, and page operations.
var ConfigSection = config.Section{
	Key: "platecheck",
	New: func() any { return &Settings{Model: "clef", StopConfidence: 0.5} },
	ApplyEnv: func(c *config.Config) error {
		s, err := SettingsOf(c)
		if err != nil {
			return err
		}
		c.SetSection("platecheck", applyEnv(s))
		return nil
	},
	Validate: func(c *config.Config) error {
		s, err := SettingsOf(c)
		if err != nil {
			return err
		}
		_, err = validateSettings(s, s.On())
		return err
	},
	EnvOverrides: func() map[string]string {
		out := map[string]string{}
		if _, ok := os.LookupEnv(EnvEndpoint); ok {
			out["platecheck_endpoint"] = EnvEndpoint
		}
		if _, ok := os.LookupEnv(EnvAPIKey); ok {
			out["platecheck_api_key"] = EnvAPIKey
		}
		return out
	},
	View: func(file *config.Config) any {
		s, _ := SettingsOf(file)
		s = applyEnv(s)
		return settingsView{Enabled: s.On(), Endpoint: s.Endpoint, HasAPIKey: s.Key() != "", Model: s.Model, StopConfidence: s.StopConfidence}
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
		if strings.TrimSpace(in.Endpoint) == old.Endpoint {
			key = old.Key()
		}
		s := submittedSettings(in, key)
		// A locked endpoint shown by View must not replace its stored file value.
		if _, overridden := os.LookupEnv(EnvEndpoint); overridden {
			s.Endpoint = old.Endpoint
			// A blank submission preserves the file credential with its restored endpoint.
			if strings.TrimSpace(in.APIKey) == "" {
				s.APIKey = old.Key()
			} else if strings.TrimSpace(in.Endpoint) != old.Endpoint {
				// Ignore the effective endpoint's typed key and preserve the original file pair.
				s.APIKey = old.Key()
			}
		}
		_, environmentKeySet := os.LookupEnv(EnvAPIKey)
		_, environmentEndpointSet := os.LookupEnv(EnvEndpoint)
		if environmentKeySet && !environmentEndpointSet && s.Endpoint != old.Endpoint {
			return &config.PageError{Status: 400, Message: endpointKeyMessage}
		}
		// Effective overrides are checked by the page's effective-config validation.
		checked, err := validateSettings(applyEnv(s), applyEnv(s).On())
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
		_, environmentKeySet := os.LookupEnv(EnvAPIKey)
		_, environmentEndpointSet := os.LookupEnv(EnvEndpoint)
		if strings.TrimSpace(in.APIKey) == "" || (environmentKeySet && !environmentEndpointSet) {
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
