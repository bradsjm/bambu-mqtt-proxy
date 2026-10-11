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

// TestChamberTemperatureSupported matches the physical sensor whitelist:
// P1/A1 reports must not make the wall display a meaningless chamber value.
func TestChamberTemperatureSupported(t *testing.T) {
	for _, model := range []string{"P1P", "P1S", "A1", "A1MINI", "A1 Mini"} {
		if ChamberTemperatureSupported(model, "") {
			t.Errorf("%s must not report chamber temperature", model)
		}
	}
	for _, model := range []string{"X1", "X1C", "X1E", "X2D", "P2S", "H2C", "H2D", "H2DPRO", "H2S", "O1C", "N7"} {
		if !ChamberTemperatureSupported(model, "") {
			t.Errorf("%s has a chamber sensor", model)
		}
	}
	if ChamberTemperatureSupported("", "01S00C351100139") {
		t.Fatal("model-less P1S serial must not expose chamber temperature")
	}
	if ChamberTemperatureSupported("", "00M09A123456789") {
		t.Fatal("X1C serial prefix alone must not infer chamber sensor support")
	}
	if !ChamberTemperatureSupported("X1C", "01S00C351100139") {
		t.Fatal("explicit model must override serial-derived P1S")
	}
}

// TestPortZeroMeansUnset pins the port rule: 0 (file or environment) is
// unset and becomes 8080, the explicit zero stays visible for the warning,
// and old behavior keys still load.
func TestPortZeroMeansUnset(t *testing.T) {
	cfg, err := Parse([]byte("http:\n  port: 0\nbehavior:\n  upstream_keepalive_seconds: 1\n"))
	if err != nil {
		t.Fatalf("Parse with a behavior key: %v", err)
	}
	cfg.ApplyDefaults()
	if cfg.HTTP.Port != 8080 || !cfg.HTTP.ExplicitZero() {
		t.Fatalf("port = %d explicit zero = %v, want 8080 and true", cfg.HTTP.Port, cfg.HTTP.ExplicitZero())
	}
	unset := &Config{}
	unset.ApplyDefaults()
	if unset.HTTP.Port != 8080 || unset.HTTP.ExplicitZero() {
		t.Fatalf("unset port = %d explicit zero = %v, want 8080 and false", unset.HTTP.Port, unset.HTTP.ExplicitZero())
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
