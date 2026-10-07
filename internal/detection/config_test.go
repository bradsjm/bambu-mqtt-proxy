package detection

import (
	"bambu-mqtt-proxy/internal/config"
	"os"
	"reflect"
	"testing"
)

func TestMain(m *testing.M) {
	config.RegisterSection(ConfigSection)
	os.Exit(m.Run())
}

func settingsConfig(s Settings) *config.Config {
	c := &config.Config{}
	c.SetSection("detection", s)
	return c
}

func readSettings(t *testing.T, c *config.Config) Settings {
	t.Helper()
	s, err := SettingsOf(c)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestDetectionEnabledSwitchStates covers the persisted Detection switch
// itself: a nil Enabled keeps the historical key-based default in both
// directions, an explicit false stays off even with a key, and validation
// demands a usable key only from an enabled configuration.
func TestDetectionEnabledSwitchStates(t *testing.T) {
	on, off := true, false
	cases := []struct {
		name    string
		enabled *bool
		apiKey  string
		want    bool
	}{
		{"nil without a key is off", nil, "", false},
		{"nil with a stored key is on", nil, "stored-key", true},
		{"nil with a padded key is on", nil, "  stored-key\t", true},
		{"explicit false with a key is off", &off, "stored-key", false},
		{"explicit true with a key is on", &on, "stored-key", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := settingsConfig(Settings{Enabled: tc.enabled, APIKey: tc.apiKey})
			if got := readSettings(t, cfg).On(); got != tc.want {
				t.Fatalf("DetectionEnabled = %v, want %v", got, tc.want)
			}
		})
	}

	// A whitespace-only key is no key: the padded default turns off, and
	// only an enabled configuration demands one.
	ws := settingsConfig(Settings{APIKey: "   "})
	if readSettings(t, ws).On() {
		t.Fatal("a whitespace-only key must not enable detection")
	}
	if err := (settingsConfig(Settings{Enabled: &on})).ValidateEffective(); err == nil {
		t.Fatal("enabled without any usable key must fail validation")
	}
	if err := (settingsConfig(Settings{Enabled: &on, APIKey: "  k "})).ValidateEffective(); err != nil {
		t.Fatalf("enabled with a padded key must validate: %v", err)
	}
	if err := (settingsConfig(Settings{Enabled: &off})).ValidateEffective(); err != nil {
		t.Fatalf("disabled without a key must validate: %v", err)
	}
}

// TestParseDetectionSection pins the persisted file shape: the detection
// section round trips both fields, and its absence leaves them unset so
// the key-based default applies.
func TestParseDetectionSection(t *testing.T) {
	parsed, err := config.Parse([]byte("detection:\n  enabled: false\n  api_key: file-key\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if readSettings(t, parsed).Enabled == nil || *readSettings(t, parsed).Enabled {
		t.Fatalf("enabled = %v, want an explicit false", readSettings(t, parsed).Enabled)
	}
	if readSettings(t, parsed).APIKey != "file-key" {
		t.Fatalf("api_key = %q, want file-key", readSettings(t, parsed).APIKey)
	}

	empty, err := config.Parse([]byte("printers: []\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if readSettings(t, empty).Enabled != nil || readSettings(t, empty).APIKey != "" {
		t.Fatalf("detection = %+v, want the zero section", readSettings(t, empty))
	}

	// The stored key lives in the nested detection section; the yaml tags
	// keep the file round trip working.
	typ := reflect.TypeOf(config.Config{})
	f, ok := typ.FieldByName("Sections")
	if !ok || f.Tag.Get("yaml") != ",inline" {
		t.Fatal("Config.Sections must be tagged yaml:\",inline\"")
	}
	if ConfigSection.Key != "detection" {
		t.Fatal("the section must use the detection YAML key")
	}
	if k, ok := reflect.TypeOf(Settings{}).FieldByName("APIKey"); !ok || k.Tag.Get("yaml") != "api_key" {
		t.Fatal("Settings.APIKey must be tagged yaml:\"api_key\"")
	}
}

// TestParseDetectionNumericLookingKey preserves the text of plain YAML scalars.
func TestParseDetectionNumericLookingKey(t *testing.T) {
	parsed, err := config.Parse([]byte("detection:\n  api_key: 012345\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := readSettings(t, parsed).APIKey; got != "012345" {
		t.Fatalf("api_key = %q, want 012345", got)
	}
}

// TestEnabledRequiresKeyNamesOnlyTheYAMLKey pins the validation message:
// it must point at detection.api_key alone, without any variable name.
func TestEnabledRequiresKeyNamesOnlyTheYAMLKey(t *testing.T) {
	on := true
	err := (settingsConfig(Settings{Enabled: &on})).ValidateEffective()
	if err == nil || err.Error() != "detection: enabled requires an API key (detection.api_key)" {
		t.Fatalf("ValidateEffective = %v, want the detection.api_key message", err)
	}
}
