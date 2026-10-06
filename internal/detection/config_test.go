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

func unsetEnvForTest(t *testing.T, keys ...string) {
	t.Helper()
	for _, k := range keys {
		t.Setenv(k, "")
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("unset %s: %v", k, err)
		}
	}
}

func TestApplyEnvOctoEverywhereKey(t *testing.T) {
	cfg := &config.Config{}
	if readSettings(t, cfg).On() {
		t.Fatal("detection must be disabled without the key")
	}
	t.Setenv(EnvAPIKey, "prod_key_from_env")
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if readSettings(t, cfg).APIKey != "prod_key_from_env" || !readSettings(t, cfg).On() {
		t.Fatalf("key = %q, enabled = %v", readSettings(t, cfg).APIKey, readSettings(t, cfg).On())
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

// TestApplyEnvDetectionKeyOverridesStoredKey covers presence-based
// overriding of the stored key: unset leaves the file value, any set value
// replaces it trimmed, and set-but-empty or whitespace-only clears it so a
// nil switch disables and an explicit enable fails validation.
func TestApplyEnvDetectionKeyOverridesStoredKey(t *testing.T) {
	unsetEnvForTest(t, EnvAPIKey)
	on := true

	// Unset: the stored key survives untouched.
	stored := settingsConfig(Settings{APIKey: "stored-key"})
	if _, err := stored.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if readSettings(t, stored).APIKey != "stored-key" || !readSettings(t, stored).On() {
		t.Fatalf("key = %q enabled = %v, want the stored key kept and detection on",
			readSettings(t, stored).APIKey, readSettings(t, stored).On())
	}

	// A nonempty override replaces the stored key, trimmed.
	override := settingsConfig(Settings{APIKey: "stored-key"})
	t.Setenv(EnvAPIKey, "  env-key\t")
	if _, err := override.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if readSettings(t, override).APIKey != "env-key" {
		t.Fatalf("key = %q, want the trimmed environment override", readSettings(t, override).APIKey)
	}

	// Set but empty clears the stored key: an explicitly enabled
	// configuration then fails the effective-only startup check.
	cleared := settingsConfig(Settings{Enabled: &on, APIKey: "stored-key"})
	t.Setenv(EnvAPIKey, "")
	if _, err := cleared.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if readSettings(t, cleared).APIKey != "" {
		t.Fatalf("key = %q, want the stored key cleared", readSettings(t, cleared).APIKey)
	}
	if err := cleared.ValidateEffective(); err == nil {
		t.Fatal("an enabled config whose key the empty override cleared must fail validation")
	}

	// Whitespace-only is also an empty override.
	padded := settingsConfig(Settings{APIKey: "stored-key"})
	t.Setenv(EnvAPIKey, " \t ")
	if _, err := padded.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if readSettings(t, padded).APIKey != "" || readSettings(t, padded).On() {
		t.Fatalf("key = %q enabled = %v, want the whitespace override to clear and disable",
			readSettings(t, padded).APIKey, readSettings(t, padded).On())
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
