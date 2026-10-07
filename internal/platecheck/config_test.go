package platecheck

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strings"
	"testing"

	"bambu-mqtt-proxy/internal/config"
)

func TestMain(m *testing.M) { config.RegisterSection(ConfigSection); os.Exit(m.Run()) }
func testSettings() Settings {
	return Settings{Provider: ProviderCustom, Endpoint: "https://vision.example/check", APIKey: "private-key", Model: "clef", StopConfidence: .5}
}
func settingsConfig(s Settings) *config.Config {
	c := &config.Config{}
	c.SetSection("platecheck", s)
	return c
}
func settingsJSON(s Settings) []byte {
	b, _ := json.Marshal(settingsView{Enabled: s.On(), Provider: s.Provider, AccountID: s.AccountID, Endpoint: s.Endpoint, APIKey: s.APIKey, Model: s.Model, StopConfidence: s.StopConfidence})
	return b
}

func TestConfigurationDefaultsAndEnable(t *testing.T) {
	s, err := SettingsOf(&config.Config{})
	if err != nil || s.On() || s.Provider != ProviderCloudflare || s.Model != "clef" || s.StopConfidence != .5 {
		t.Fatalf("default %+v %v", s, err)
	}
	for _, tc := range []struct {
		endpoint, key string
		enabled       *bool
		on            bool
	}{
		{"", "", nil, false}, {" https://a.example/ ", " key ", nil, true}, {"", "key", nil, false}, {"https://a.example/", "key", new(false), false}, {"", "", new(true), true},
	} {
		s := testSettings()
		s.Endpoint, s.APIKey, s.Enabled = tc.endpoint, tc.key, tc.enabled
		if s.On() != tc.on {
			t.Fatalf("On %+v", tc)
		}
	}
	parsed, err := config.Parse([]byte("platecheck:\n  enabled: false\n  provider: custom\n  api_key: 012345\n  endpoint: https://a.example/check\n  model: clef-flash\n  stop_confidence: 0.7\n"))
	if err != nil {
		t.Fatal(err)
	}
	s, err = SettingsOf(parsed)
	if err != nil || s.On() || s.Key() != "012345" || s.Model != "clef-flash" || s.StopConfidence != .7 {
		t.Fatalf("parsed %+v %v", s, err)
	}
}

func TestSettingsValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(*Settings)
		valid bool
	}{
		{"default", func(*Settings) {}, true},
		{"near integer", func(s *Settings) { s.StopConfidence = .58 + 1e-12 }, true},
		{"self hosted", func(s *Settings) { s.Endpoint = "https://self.example:8443/custom" }, true},
		{"99", func(s *Settings) { s.StopConfidence = .99 }, true},
		{"disabled blank", func(s *Settings) { s.Enabled = new(false); s.Endpoint = ""; s.APIKey = "" }, true},
		{"below", func(s *Settings) { s.StopConfidence = .49 }, false},
		{"above", func(s *Settings) { s.StopConfidence = 1 }, false},
		{"fractional percent", func(s *Settings) { s.StopConfidence = .585 }, false},
		{"nan", func(s *Settings) { s.StopConfidence = math.NaN() }, false},
		{"inf", func(s *Settings) { s.StopConfidence = math.Inf(1) }, false},
		{"blank disabled model", func(s *Settings) { s.Enabled = new(false); s.Model = "" }, false},
		{"http", func(s *Settings) { s.Endpoint = "http://example/check" }, false},
		{"relative", func(s *Settings) { s.Endpoint = "/check" }, false},
		{"credential", func(s *Settings) { s.Endpoint = "https://user:secret@example/check" }, false},
		{"query", func(s *Settings) { s.Endpoint = "https://example/check?key=secret" }, false},
		{"empty query", func(s *Settings) { s.Endpoint = "https://example/check?" }, false},
		{"fragment", func(s *Settings) { s.Endpoint = "https://example/check#secret" }, false},
		{"disabled invalid endpoint", func(s *Settings) { s.Enabled = new(false); s.Endpoint = "http://example/check" }, false},
		{"enabled missing key", func(s *Settings) { s.Enabled = new(true); s.APIKey = "" }, false},
		{"enabled missing endpoint", func(s *Settings) { s.Enabled = new(true); s.Endpoint = "" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testSettings()
			tc.edit(&s)
			got, err := validateSettings(s, s.On())
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if tc.valid && got.StopConfidence != float64(int(math.Round(s.StopConfidence*100)))/100 {
				t.Fatal("not normalized")
			}
		})
	}
}

func TestConfigViewSaveRedaction(t *testing.T) {
	s := testSettings()
	s.StopConfidence = .7
	stored := settingsConfig(s)
	raw, _ := json.Marshal(ConfigSection.View(stored))
	if strings.Contains(string(raw), s.APIKey) || strings.Contains(string(raw), `"api_key"`) {
		t.Fatalf("key exposed: %s", raw)
	}
	var view settingsView
	_ = json.Unmarshal(raw, &view)
	if !view.HasAPIKey || view.StopConfidence != .7 {
		t.Fatal(string(raw))
	}
	submitted := s
	submitted.APIKey = " "
	submitted.StopConfidence = .58 + 1e-12
	out := &config.Config{}
	if err := ConfigSection.Save(settingsJSON(submitted), stored, out); err != nil {
		t.Fatal(err)
	}
	got, _ := SettingsOf(out)
	if got.Key() != s.Key() || got.StopConfidence != .58 {
		t.Fatalf("save %+v", got)
	}
	// Disabling keeps the stored endpoint and key pair.
	out = &config.Config{}
	submitted.Enabled = new(false)
	if err := ConfigSection.Save(settingsJSON(submitted), stored, out); err != nil {
		t.Fatal(err)
	}
	got, _ = SettingsOf(out)
	if got.APIKey != s.APIKey || got.Endpoint != s.Endpoint {
		t.Fatalf("disable changed the stored pair: %+v", got)
	}
}

func TestStrictSubmittedSettings(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `{"unknown":"hostile"}`, `{"stop_confidence":"0.5"}`, `{"enabled":true} {}`, `{"api_key":"marker",`} {
		if _, err := decodeSettingsView([]byte(raw)); err == nil {
			t.Fatalf("accepted %q", raw)
		} else {
			var page *config.PageError
			if !errors.As(err, &page) || page.Status != 400 || strings.Contains(err.Error(), "marker") {
				t.Fatalf("unsafe %v", err)
			}
		}
	}
}

func TestConfigSectionProbeResolutionAndSafeFailure(t *testing.T) {
	old := probe
	defer func() { probe = old }()
	calls := 0
	var got Settings
	probe = func(ctx context.Context, s Settings) error {
		calls++
		got = s
		if _, ok := ctx.Deadline(); !ok {
			t.Error("missing deadline")
		}
		return nil
	}
	stored := func() (*config.Config, error) { return settingsConfig(testSettings()), nil }
	s := testSettings()
	s.APIKey = ""
	s.Model = "clef-flash"
	s.StopConfidence = .75
	if err := ConfigSection.Test(context.Background(), settingsJSON(s), stored); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || got.Key() != "private-key" || got.Model != "clef-flash" || got.StopConfidence != .75 {
		t.Fatalf("probe %+v", got)
	}
	probe = func(context.Context, Settings) error {
		return errors.New("https://secret.example/accounts/marker\nforged private-key")
	}
	err := ConfigSection.Test(context.Background(), settingsJSON(s), stored)
	if err == nil || strings.Contains(err.Error(), "marker") || strings.Contains(err.Error(), "secret.example") || strings.Contains(err.Error(), "private-key") {
		t.Fatalf("unsafe error %v", err)
	}
	var page *config.PageError
	if errors.As(err, &page) {
		t.Fatal("provider error must use 502 through configui")
	}
}

func TestStrictPolicyBoundaries(t *testing.T) {
	for _, tc := range []struct {
		cutoff, clear, assessable float64
		decision                  string
	}{
		{.5, .5, .8, "below_threshold"}, {.5, .49, .8, "would_stop"},
		{.7, .31, .8, "below_threshold"}, {.7, .29, .8, "would_stop"},
		{.58, .42, .8, "below_threshold"}, {.58, math.Nextafter(.42, 0), .8, "would_stop"}, {.58, math.Nextafter(.42, 1), .8, "below_threshold"},
		{.5, .01, .79, "inconclusive"}, {.5, .01, .8, "would_stop"}, {.5, .9, .9, "clear"},
	} {
		r := Result{PClear: tc.clear, POccupied: 1 - tc.clear, PAssessable: tc.assessable}
		if got := classify(r, tc.cutoff); got != tc.decision {
			t.Fatalf("cutoff %.17g clear %.17g assessable %g: %s", tc.cutoff, tc.clear, tc.assessable, got)
		}
	}
	s := testSettings()
	s.StopConfidence = .58 + 1e-12
	s, err := validateSettings(s, true)
	if err != nil || s.StopConfidence != .58 || classify(Result{PClear: .42, POccupied: 1 - .42, PAssessable: .9}, s.StopConfidence) == "would_stop" {
		t.Fatal("normalization boundary")
	}
}

func TestConfigBoundaryValidationBeforeNormalization(t *testing.T) {
	for _, cutoff := range []float64{math.Nextafter(.5, 0), math.Nextafter(.99, 1), .501} {
		s := testSettings()
		s.StopConfidence = cutoff
		if err := ConfigSection.Validate(settingsConfig(s)); err == nil {
			t.Fatalf("accepted out-of-range or fractional cutoff %.17g", cutoff)
		}
	}
}

func TestConfigTestMissingCredentials(t *testing.T) {
	original := probe
	defer func() { probe = original }()
	probe = func(context.Context, Settings) error { t.Fatal("invalid settings reached probe"); return nil }
	for _, field := range []string{"endpoint", "key"} {
		s := testSettings()
		if field == "endpoint" {
			s.Endpoint = ""
		} else {
			s.APIKey = ""
		}
		err := ConfigSection.Test(context.Background(), settingsJSON(s), func() (*config.Config, error) {
			missing := testSettings()
			missing.Endpoint = s.Endpoint
			missing.APIKey = ""
			return settingsConfig(missing), nil
		})
		var page *config.PageError
		if !errors.As(err, &page) || page.Status != 422 || !strings.HasPrefix(page.Message, "No ") {
			t.Fatalf("missing %s: %v", field, err)
		}
	}
}

func TestConfigSaveAbsentPageSectionUsesDisabledDefaults(t *testing.T) {
	out := &config.Config{}
	if err := ConfigSection.Save(nil, nil, out); err != nil {
		t.Fatal(err)
	}
	got, err := SettingsOf(out)
	if err != nil || got.On() || got.Model != "clef" || got.StopConfidence != .5 {
		t.Fatalf("absent section %+v %v", got, err)
	}
}

func TestSaveCredentialRetentionRequiresSameEndpoint(t *testing.T) {
	stored := testSettings()
	for _, tc := range []struct {
		endpoint, key string
		enabled       bool
		wantKey       string
		valid         bool
	}{
		{"  " + stored.Endpoint + "  ", "", true, stored.APIKey, true},
		{"https://different.example/check", "", false, "", true},
		{"https://different.example/check", "", true, "", false},
		{"https://different.example/check", "typed-for-destination", true, "typed-for-destination", true},
	} {
		in := stored
		in.Endpoint = tc.endpoint
		in.APIKey = tc.key
		in.Enabled = new(tc.enabled)
		out := &config.Config{}
		err := ConfigSection.Save(settingsJSON(in), settingsConfig(stored), out)
		if (err == nil) != tc.valid {
			t.Fatalf("Save endpoint %s: %v", tc.endpoint, err)
		}
		if err == nil {
			got, _ := SettingsOf(out)
			if got.Key() != tc.wantKey {
				t.Fatalf("unexpected saved credential %q", got.Key())
			}
		}
	}
}

func TestProviderValidationMessages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		edit    func(*Settings)
		require bool
		message string
	}{
		{"provider", func(s *Settings) { s.Provider = "" }, false, "The plate-check provider must be cloudflare or custom."},
		{"cutoff", func(s *Settings) { s.StopConfidence = .585 }, false, "Stop above must be a whole percentage from 50 through 99 percent."},
		{"cloudflare endpoint", func(s *Settings) { s.Endpoint = "https://old.example/check" }, false, "The Cloudflare provider builds the endpoint from the account ID. Remove the endpoint or choose the custom provider."},
		{"cloudflare model", func(s *Settings) { s.Model = "other" }, false, "The Cloudflare model must be clef or clef-flash."},
		{"account format", func(s *Settings) { s.AccountID = "xyz" }, false, "The Cloudflare account ID must be 32 hexadecimal characters."},
		{"account missing", func(s *Settings) { s.AccountID = "" }, true, "No Cloudflare account ID is configured. Enter the account ID."},
		{"custom model", func(s *Settings) { s.Provider = ProviderCustom; s.Model = "" }, false, "Enter the model name for the custom endpoint."},
		{"custom URL", func(s *Settings) { s.Provider = ProviderCustom; s.Endpoint = "http://example/check" }, false, "The endpoint must be a full HTTPS URL without credentials, a query, or a fragment."},
		{"endpoint missing", func(s *Settings) { s.Provider = ProviderCustom }, true, "No endpoint is configured. Enter an HTTPS endpoint."},
		{"key missing", func(s *Settings) { s.APIKey = "" }, true, "No API key is configured. Enter a key."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := Settings{Provider: ProviderCloudflare, AccountID: "0123456789abcdef0123456789abcdef", APIKey: "private-key", Model: "clef", StopConfidence: .5}
			tc.edit(&s)
			_, err := validateSettings(s, tc.require)
			if err == nil || err.Error() != tc.message {
				t.Fatalf("error %v, want %q", err, tc.message)
			}
		})
	}
	s := testSettings()
	s.Model = "  arbitrary model / with punctuation  "
	got, err := validateSettings(s, true)
	if err != nil || got.Model != strings.TrimSpace(s.Model) {
		t.Fatalf("custom model %+v: %v", got, err)
	}
	old, err := config.Parse([]byte("platecheck:\n  endpoint: https://old.example/check\n  api_key: key\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ConfigSection.Validate(old); err == nil || !strings.HasPrefix(err.Error(), "The Cloudflare provider builds") {
		t.Fatalf("legacy endpoint accepted: %v", err)
	}
}

func TestCloudflareURLAndKeyBinding(t *testing.T) {
	const account = "0123456789abcdef0123456789abcdef"
	const other = "fedcba9876543210fedcba9876543210"
	stored := Settings{Provider: ProviderCloudflare, AccountID: account, APIKey: "stored-key", Model: "clef-flash", StopConfidence: .5}
	want := "https://api.cloudflare.com/client/v4/accounts/" + account + "/ai/run/@cf/cloudflare/clef-flash"
	if stored.URL() != want || !stored.On() {
		t.Fatalf("URL %s, on %v", stored.URL(), stored.On())
	}
	in := stored
	in.AccountID = "  " + strings.ToUpper(account) + "  "
	in.Model, in.APIKey = "clef", ""
	out := &config.Config{}
	if err := ConfigSection.Save(settingsJSON(in), settingsConfig(stored), out); err != nil {
		t.Fatal(err)
	}
	saved, _ := SettingsOf(out)
	if saved.Key() != stored.Key() || saved.Model != "clef" || saved.AccountID != account {
		t.Fatalf("same account save %+v", saved)
	}
	in.AccountID = other
	in.Enabled = new(false)
	if err := ConfigSection.Save(settingsJSON(in), settingsConfig(stored), out); err != nil {
		t.Fatal(err)
	}
	saved, _ = SettingsOf(out)
	if saved.Key() != "" || saved.AccountID != other {
		t.Fatalf("changed account save %+v", saved)
	}
	// The stored key only follows a diagnostic submission for its own account.
	if _, err := resolveTestSettings(settingsView{Provider: ProviderCloudflare, AccountID: other, Model: "clef", StopConfidence: .5}, stored); err == nil || err.Error() != endpointKeyMessage {
		t.Fatalf("diagnostic moved stored key: %v", err)
	}
	if s, err := resolveTestSettings(settingsView{Provider: ProviderCloudflare, AccountID: account, Model: "clef", StopConfidence: .5}, stored); err != nil || s.Key() != stored.Key() {
		t.Fatalf("same account diagnostic %+v: %v", s, err)
	}
}
