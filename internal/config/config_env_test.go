package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestApplyEnvOverridesFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "cfg.yaml")
	yamlDoc := `
listen:
  - port: 1234
    tls: false
log:
  level: info
http:
  port: 8080
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
	t.Setenv(EnvLogLevel, "warn")
	t.Setenv(EnvHTTPPort, "9090")
	if err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if cfg.Log.Level != "warn" {
		t.Fatalf("log.level = %s, want the env override warn", cfg.Log.Level)
	}
	if cfg.HTTP.Port != 9090 {
		t.Fatalf("http.port = %d, want the env override 9090", cfg.HTTP.Port)
	}
	if len(cfg.Listen) != 1 || cfg.Listen[0].Port != 1234 {
		t.Fatalf("file listener must survive env overrides, got %+v", cfg.Listen)
	}
}

func TestApplyEnvInvalidHTTPPort(t *testing.T) {
	t.Setenv(EnvHTTPPort, "70000")
	if err := (&Config{}).ApplyEnv(); err == nil {
		t.Fatal("invalid http port should error")
	}
}

func TestApplyEnvLogLevelEmptyKeepsFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "cfg.yaml")
	const doc = "log:\n  level: debug\nhttp:\n  port: 9097\n"
	if err := os.WriteFile(file, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	t.Setenv(EnvLogLevel, "")
	t.Setenv(EnvHTTPPort, "")
	if err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if cfg.Log.Level != "debug" || cfg.HTTP.Port != 9097 {
		t.Fatalf("empty overrides must keep the file values, got %s/%d", cfg.Log.Level, cfg.HTTP.Port)
	}
}

// unsetEnvForTest clears inherited BMBPX_* values that would skew an
// isolated fixture. t.Setenv records the original value and restores it at
// cleanup; os.Unsetenv then removes the variable so an environment with
// ambient defaults behaves like one that exports no BMBPX_* variables.
func unsetEnvForTest(t *testing.T, keys ...string) {
	t.Helper()
	for _, k := range keys {
		t.Setenv(k, "")
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("unset %s: %v", k, err)
		}
	}
}

// allEnvKeys is the BMBPX_* surface. Focused fixtures unset every variable
// so ambient values cannot alter the input a test means to establish.
var allEnvKeys = []string{
	EnvLogLevel,
	EnvHTTPPort,
}

// TestFileConfigSurvivesWithoutEnvDefaults pins the container regression:
// a mounted YAML keeps its listeners and HTTP port when the image sets no
// BMBPX_* environment defaults. The disabled-HTTP case matters most: a
// former image default BMBPX_HTTP_PORT=8080 overrode a mounted http.port 0.
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
			if err := cfg.ApplyEnv(); err != nil {
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
