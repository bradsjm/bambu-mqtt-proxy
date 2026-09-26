package config

import (
	"testing"
)

// TestModelFromSerial pins the serial-prefix inference table.
func TestModelFromSerial(t *testing.T) {
	cases := map[string]string{
		"01P00A123456789": "P1P",
		"01S00C351100139": "P1S", // live-printer verified prefix
		"030123456789012": "A1MINI",
		"039123456789012": "A1",
		"00M09A123456789": "", // X1-class: never chamber-image
		"ZZZ":             "",
		"":                "",
	}
	for serial, want := range cases {
		if got := ModelFromSerial(serial); got != want {
			t.Errorf("ModelFromSerial(%q) = %q, want %q", serial, got, want)
		}
	}
	if ModelFromSerial("01s00c351100139") != "P1S" {
		t.Fatal("serial inference must be case-insensitive")
	}
}

// TestCameraEligible pins precedence: explicit model wins; otherwise the
// serial prefix decides. Existing configs without model fields must keep
// working for P1/A1 printers.
func TestCameraEligible(t *testing.T) {
	if !CameraEligible("", "01S00C351100139") {
		t.Fatal("P1S serial without model must be camera-eligible")
	}
	if !CameraEligible("", "01P00A123456789") ||
		!CameraEligible("", "039123456789012") ||
		!CameraEligible("", "030123456789012") {
		t.Fatal("P1P/A1/A1MINI serials without model must be eligible")
	}
	if CameraEligible("", "00M09A123456789") || CameraEligible("", "ABC123") || CameraEligible("", "") {
		t.Fatal("X1-class or unknown serials must not be camera-eligible")
	}
	if CameraEligible("X1C", "01S00C351100139") {
		t.Fatal("explicit non-camera model must override camera-capable prefix")
	}
	if !CameraEligible("P1S", "00M09A123456789") {
		t.Fatal("explicit camera-capable model must override foreign prefix")
	}
}

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
