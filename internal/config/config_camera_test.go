package config

import "testing"

// TestPortZeroDisablesHTTP pins the documented contract: an explicit
// http.port 0 (YAML or env) survives defaults and disables HTTP.
func TestPortZeroDisablesHTTP(t *testing.T) {
	cfg := &Config{
		Listen:   []Listener{{Port: 8883, TLS: false}},
		HTTP:     HTTP{Port: PortUnset},
		Printers: []Printer{{Serial: "S", Model: "P1S", Address: "h:8883", Password: "p"}},
	}
	t.Setenv(EnvHTTPPort, "0")
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	cfg.ApplyDefaults()
	if cfg.HTTP.Port != 0 {
		t.Fatalf("explicit port 0 became %d; 0 must disable HTTP", cfg.HTTP.Port)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// TestPortUnsetDefaultsTo8080 covers the no-config default path.
func TestPortUnsetDefaultsTo8080(t *testing.T) {
	cfg := &Config{HTTP: HTTP{Port: PortUnset}}
	cfg.ApplyDefaults()
	if cfg.HTTP.Port != 8080 {
		t.Fatalf("unset port = %d, want default 8080", cfg.HTTP.Port)
	}
}

// TestValidateAcceptsAnyModel keeps every model spelling valid
// configuration (they proxy MQTT; camera endpoints answer 422 for
// non-camera-capable models). A future or unrecognized model name must
// never prevent proxy startup.
func TestValidateAcceptsAnyModel(t *testing.T) {
	cfg := &Config{
		Listen: []Listener{{Port: 8883, TLS: false}},
		Auth:   Auth{Mode: AuthModePrinter},
		Printers: []Printer{
			{Serial: "X", Model: "X1C", Address: "h:8883", Password: "p"},
			{Serial: "Y", Model: "FutureModel9", Address: "h:8884", Password: "p"},
		},
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unknown models rejected: %v", err)
	}
	if CameraSupported("X1C") {
		t.Fatal("X1C must not be camera-capable")
	}
	if CameraSupported("FutureModel9") {
		t.Fatal("FutureModel9 must not be camera-capable")
	}
}
