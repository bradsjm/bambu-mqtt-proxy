package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestApplyEnvFullConfig(t *testing.T) {
	t.Setenv(EnvPrinters,
		"serial=S1,address=10.0.0.1:8883,password=1111,name= Garage P1S ; serial=S2,address=10.0.0.2:1883,tls=false,password=2222,username=other")
	t.Setenv(EnvListenPort, "9999")
	t.Setenv(EnvListenTLS, "false")
	t.Setenv(EnvAuthMode, "accept_all")
	t.Setenv(EnvLogLevel, "warn")
	t.Setenv(EnvHTTPPort, "9090")
	t.Setenv(EnvCameraEnable, "false")

	cfg := &Config{}
	fromEnv, err := cfg.ApplyEnv()
	if err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if !fromEnv {
		t.Fatal("printers should come from env")
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if len(cfg.Listen) != 1 || cfg.Listen[0].Port != 9999 || cfg.Listen[0].TLS {
		t.Fatalf("listen = %+v, want single listener 9999/TLS-off", cfg.Listen)
	}
	if cfg.Auth.Mode != AuthModeAcceptAll || cfg.Log.Level != "warn" || cfg.HTTP.Port != 9090 {
		t.Fatalf("auth/log/http = %s/%s/%d", cfg.Auth.Mode, cfg.Log.Level, cfg.HTTP.Port)
	}
	if cfg.CameraEnabled() {
		t.Fatal("BMBPX_CAMERA_ENABLED=false must disable cameras")
	}
	if len(cfg.Printers) != 2 {
		t.Fatalf("printers = %d, want 2", len(cfg.Printers))
	}
	s1, s2 := cfg.Printers[0], cfg.Printers[1]
	if s1.Serial != "S1" || !s1.TLS || !s1.InsecureSkipVerify || s1.Username != "bblp" || s1.Name != "Garage P1S" {
		t.Fatalf("S1 = %+v", s1)
	}
	if s2.Serial != "S2" || s2.TLS || s2.Username != "other" || s2.Name != "" {
		t.Fatalf("S2 = %+v", s2)
	}
}

func TestApplyEnvOverridesFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "cfg.yaml")
	yamlDoc := `
listen:
  - port: 1234
    tls: false
auth:
  mode: accept_all
printers:
  - serial: "FILEP"
    address: "file-host:8883"
    password: "filecode"
`
	if err := os.WriteFile(file, []byte(yamlDoc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	t.Setenv(EnvPrinters, "serial=ENVP,address=1.2.3.4:8883,password=9")
	t.Setenv(EnvAuthMode, "printer")
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(cfg.Printers) != 1 || cfg.Printers[0].Serial != "ENVP" {
		t.Fatalf("env printers must replace file printers, got %+v", cfg.Printers)
	}
	if cfg.Auth.Mode != AuthModePrinter {
		t.Fatalf("auth = %s, want env override printer", cfg.Auth.Mode)
	}
	if len(cfg.Listen) != 1 || cfg.Listen[0].Port != 1234 {
		t.Fatalf("file listener must survive when no listen env vars are set, got %+v", cfg.Listen)
	}
}

func TestApplyEnvInvalidPort(t *testing.T) {
	t.Setenv(EnvListenPort, "70000")
	if _, err := (&Config{}).ApplyEnv(); err == nil {
		t.Fatal("invalid port should error")
	}
}

func TestParsePrinterEntryUnknownKey(t *testing.T) {
	if _, err := parsePrinterEntry("serial=S,address=a:1,password=p,bogus=1"); err == nil {
		t.Fatal("unknown key should error")
	}
	if _, err := parsePrinterEntry("serial=S,address=a:1"); err == nil {
		t.Fatal("missing password should error")
	}
}

func TestApplyEnvOctoEverywhereKey(t *testing.T) {
	cfg := &Config{}
	if cfg.DetectionEnabled() {
		t.Fatal("detection must be disabled without the key")
	}
	t.Setenv(EnvOctoEverywhereAPIKey, "prod_key_from_env")
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if cfg.Detection.APIKey != "prod_key_from_env" || !cfg.DetectionEnabled() {
		t.Fatalf("key = %q, enabled = %v", cfg.Detection.APIKey, cfg.DetectionEnabled())
	}

	// The stored key lives in the nested detection section; the yaml tags
	// keep the file round trip working.
	typ := reflect.TypeOf(Config{})
	f, ok := typ.FieldByName("Detection")
	if !ok || f.Tag.Get("yaml") != "detection" {
		t.Fatal("Detection must be tagged yaml:\"detection\"")
	}
	if k, ok := f.Type.FieldByName("APIKey"); !ok || k.Tag.Get("yaml") != "api_key" {
		t.Fatal("Detection.APIKey must be tagged yaml:\"api_key\"")
	}
}

// unsetEnvForTest clears inherited BMBPX_* values that would skew an
// isolated fixture. t.Setenv records the original value and restores it at
// cleanup; os.Unsetenv then removes the variable so LookupEnv reports
// absence. Setting "" alone would not do: an empty BMBPX_LISTEN_PORT still
// triggers listener replacement.
func unsetEnvForTest(t *testing.T, keys ...string) {
	t.Helper()
	for _, k := range keys {
		t.Setenv(k, "")
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("unset %s: %v", k, err)
		}
	}
}

// allEnvKeys is the full BMBPX_* surface. Focused fixtures unset every one
// so ambient values cannot alter the input a test means to establish.
var allEnvKeys = []string{
	EnvPrinters,
	EnvListenPort,
	EnvListenTLS,
	EnvCertFile,
	EnvKeyFile,
	EnvAuthMode,
	EnvLogLevel,
	EnvHTTPPort,
	EnvCameraEnable,
	EnvMCPEnable,
	EnvJobPreview,
	EnvOctoEverywhereAPIKey,
}

// TestApplyEnvDefaultListener pins the env-only startup path used when the
// container runs without a config file: with printers but no listen
// variables, defaults must produce the TLS 8883 endpoint instead of failing
// validation.
func TestApplyEnvDefaultListener(t *testing.T) {
	unsetEnvForTest(t, allEnvKeys...)
	t.Setenv(EnvPrinters, "serial=S1,address=10.0.0.1:8883,password=1111")
	cfg := &Config{HTTP: HTTP{Port: PortUnset}}
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(cfg.Listen) != 1 || cfg.Listen[0].Port != 8883 || !cfg.Listen[0].TLS {
		t.Fatalf("listen = %+v, want the default TLS 8883 listener", cfg.Listen)
	}
	if cfg.Listen[0].CertFile != "" || cfg.Listen[0].KeyFile != "" {
		t.Fatalf("default listener must use a generated certificate, got %+v", cfg.Listen[0])
	}
}

// TestFileConfigSurvivesWithoutEnvDefaults pins the container regression:
// a mounted YAML keeps its listeners and HTTP port when the image sets no
// BMBPX_* environment defaults. The disabled-HTTP case matters most: the
// old image default BMBPX_HTTP_PORT=8080 overrode a mounted http.port 0.
func TestFileConfigSurvivesWithoutEnvDefaults(t *testing.T) {
	unsetEnvForTest(t, allEnvKeys...)
	cases := []struct {
		name     string
		yamlDoc  string
		want     []Listener
		wantHTTP int
	}{
		{
			name: "custom listener and http port",
			yamlDoc: `
listen:
  - port: 1884
    tls: false
http:
  port: 9090
printers:
  - serial: "FILEP"
    address: "file-host:8883"
    password: "filecode"
`,
			want:     []Listener{{Port: 1884}},
			wantHTTP: 9090,
		},
		{
			name: "multiple listeners with cert paths and http disabled",
			yamlDoc: `
listen:
  - port: 8883
    tls: true
    cert_file: "/certs/proxy.crt"
    key_file: "/certs/proxy.key"
  - port: 1883
    tls: false
http:
  port: 0
printers:
  - serial: "FILEP"
    address: "file-host:8883"
    password: "filecode"
`,
			want: []Listener{
				{Port: 8883, TLS: true, CertFile: "/certs/proxy.crt", KeyFile: "/certs/proxy.key"},
				{Port: 1883},
			},
			wantHTTP: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "cfg.yaml")
			if err := os.WriteFile(file, []byte(tc.yamlDoc), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(file)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if _, err := cfg.ApplyEnv(); err != nil {
				t.Fatalf("ApplyEnv: %v", err)
			}
			cfg.ApplyDefaults()
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if !reflect.DeepEqual(cfg.Listen, tc.want) {
				t.Fatalf("listen = %+v, want %+v", cfg.Listen, tc.want)
			}
			if cfg.HTTP.Port != tc.wantHTTP {
				t.Fatalf("http.port = %d, want %d", cfg.HTTP.Port, tc.wantHTTP)
			}
		})
	}
}

// TestApplyEnvJobPreview covers the environment-only job preview switch:
// unset or empty keeps the enabled default, a parseable value wins, an
// invalid value is the existing-style startup error, and re-applying
// follows a removed variable back to the default.
func TestApplyEnvJobPreview(t *testing.T) {
	unsetEnvForTest(t, EnvJobPreview)
	cfg := &Config{}
	if !cfg.JobPreviewEnabled() || cfg.JobPreview != nil {
		t.Fatal("job preview must default to enabled with a nil switch")
	}

	// An empty value means unset.
	t.Setenv(EnvJobPreview, "")
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if cfg.JobPreview != nil || !cfg.JobPreviewEnabled() {
		t.Fatal("an empty variable must keep the default")
	}

	// An explicit false survives as a typed value.
	t.Setenv(EnvJobPreview, "false")
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if cfg.JobPreview == nil || *cfg.JobPreview || cfg.JobPreviewEnabled() {
		t.Fatal("BMBPX_JOB_PREVIEW=false must disable job preview")
	}

	// Numeric and word spellings parse like the other bool switches.
	t.Setenv(EnvJobPreview, "1")
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if cfg.JobPreview == nil || !*cfg.JobPreview || !cfg.JobPreviewEnabled() {
		t.Fatal("BMBPX_JOB_PREVIEW=1 must enable job preview")
	}

	// Invalid input is the existing-style startup error.
	t.Setenv(EnvJobPreview, "bogus")
	_, err := cfg.ApplyEnv()
	if err == nil || err.Error() != `BMBPX_JOB_PREVIEW: invalid bool "bogus"` {
		t.Fatalf("invalid value error = %v, want BMBPX_JOB_PREVIEW: invalid bool \"bogus\"", err)
	}

	// Re-applying after the variable disappears resets the switch, so the
	// post-save re-apply never keeps a stale value.
	unsetEnvForTest(t, EnvJobPreview)
	disabled := false
	cfg.JobPreview = &disabled
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if cfg.JobPreview != nil || !cfg.JobPreviewEnabled() {
		t.Fatal("re-apply without the variable must reset the switch")
	}

	// The field is environment-only: yaml:"-" keeps it out of files, and
	// a file cannot set it.
	typ := reflect.TypeOf(Config{})
	if f, ok := typ.FieldByName("JobPreview"); !ok || f.Tag.Get("yaml") != "-" {
		t.Fatal("JobPreview must be tagged yaml:\"-\"")
	}
	parsed, err := Parse([]byte("printers: []\njob_preview: false\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.JobPreview != nil {
		t.Fatal("a YAML job_preview field must not set the switch")
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
			cfg := &Config{Detection: Detection{Enabled: tc.enabled, APIKey: tc.apiKey}}
			if got := cfg.DetectionEnabled(); got != tc.want {
				t.Fatalf("DetectionEnabled = %v, want %v", got, tc.want)
			}
		})
	}

	// A whitespace-only key is no key: the padded default turns off, and
	// only an enabled configuration demands one.
	ws := &Config{Detection: Detection{APIKey: "   "}}
	if ws.DetectionEnabled() {
		t.Fatal("a whitespace-only key must not enable detection")
	}
	if err := (&Config{Detection: Detection{Enabled: &on}}).ValidateDetection(); err == nil {
		t.Fatal("enabled without any usable key must fail validation")
	}
	if err := (&Config{Detection: Detection{Enabled: &on, APIKey: "  k "}}).ValidateDetection(); err != nil {
		t.Fatalf("enabled with a padded key must validate: %v", err)
	}
	if err := (&Config{Detection: Detection{Enabled: &off}}).ValidateDetection(); err != nil {
		t.Fatalf("disabled without a key must validate: %v", err)
	}
}

// TestApplyEnvDetectionKeyOverridesStoredKey covers presence-based
// overriding of the stored key: unset leaves the file value, any set value
// replaces it trimmed, and set-but-empty or whitespace-only clears it so a
// nil switch disables and an explicit enable fails validation.
func TestApplyEnvDetectionKeyOverridesStoredKey(t *testing.T) {
	unsetEnvForTest(t, EnvOctoEverywhereAPIKey)
	on := true

	// Unset: the stored key survives untouched.
	stored := &Config{Detection: Detection{APIKey: "stored-key"}}
	if _, err := stored.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if stored.Detection.APIKey != "stored-key" || !stored.DetectionEnabled() {
		t.Fatalf("key = %q enabled = %v, want the stored key kept and detection on",
			stored.Detection.APIKey, stored.DetectionEnabled())
	}

	// A nonempty override replaces the stored key, trimmed.
	override := &Config{Detection: Detection{APIKey: "stored-key"}}
	t.Setenv(EnvOctoEverywhereAPIKey, "  env-key\t")
	if _, err := override.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if override.Detection.APIKey != "env-key" {
		t.Fatalf("key = %q, want the trimmed environment override", override.Detection.APIKey)
	}

	// Set but empty clears the stored key: an explicitly enabled
	// configuration then fails the effective-only startup check.
	cleared := &Config{Detection: Detection{Enabled: &on, APIKey: "stored-key"}}
	t.Setenv(EnvOctoEverywhereAPIKey, "")
	if _, err := cleared.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if cleared.Detection.APIKey != "" {
		t.Fatalf("key = %q, want the stored key cleared", cleared.Detection.APIKey)
	}
	if err := cleared.ValidateDetection(); err == nil {
		t.Fatal("an enabled config whose key the empty override cleared must fail validation")
	}

	// Whitespace-only is also an empty override.
	padded := &Config{Detection: Detection{APIKey: "stored-key"}}
	t.Setenv(EnvOctoEverywhereAPIKey, " \t ")
	if _, err := padded.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if padded.Detection.APIKey != "" || padded.DetectionEnabled() {
		t.Fatalf("key = %q enabled = %v, want the whitespace override to clear and disable",
			padded.Detection.APIKey, padded.DetectionEnabled())
	}
}

// TestParseDetectionSection pins the persisted file shape: the detection
// section round trips both fields, and its absence leaves them unset so
// the key-based default applies.
func TestParseDetectionSection(t *testing.T) {
	parsed, err := Parse([]byte("detection:\n  enabled: false\n  api_key: file-key\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Detection.Enabled == nil || *parsed.Detection.Enabled {
		t.Fatalf("enabled = %v, want an explicit false", parsed.Detection.Enabled)
	}
	if parsed.Detection.APIKey != "file-key" {
		t.Fatalf("api_key = %q, want file-key", parsed.Detection.APIKey)
	}

	empty, err := Parse([]byte("printers: []\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if empty.Detection.Enabled != nil || empty.Detection.APIKey != "" {
		t.Fatalf("detection = %+v, want the zero section", empty.Detection)
	}
}
