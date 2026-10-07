package config

import "testing"

// TestWithDefaultPrinterPort covers the default-port append: a bare host or
// bracketed IPv6 address gains :8883, a value with a port and an empty
// value stay unchanged.
func TestWithDefaultPrinterPort(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},
		{"h", "h:8883"},
		{"h:1883", "h:1883"},
		{"10.0.0.1", "10.0.0.1:8883"},
		{"fe80::1", "[fe80::1]:8883"},
		{"[fe80::1]", "[fe80::1]:8883"},
		{"[fe80::1]:1", "[fe80::1]:1"},
	}
	for _, tc := range cases {
		if got := WithDefaultPrinterPort(tc.in); got != tc.want {
			t.Errorf("WithDefaultPrinterPort(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestHostOnly covers the host-or-IP address validation: host names and
// host:port pairs pass unchanged, a bare IPv6 address is bracketed, and
// every scheme, path, credential, or malformed port form is rejected.
func TestHostOnly(t *testing.T) {
	accepted := []struct {
		in, want string
	}{
		{"panda.local", "panda.local"},
		{"10.0.0.2:81", "10.0.0.2:81"},
		{"fe80::1", "[fe80::1]"},
		{"[fe80::1]:80", "[fe80::1]:80"},
	}
	for _, tc := range accepted {
		got, err := HostOnly(tc.in)
		if err != nil {
			t.Errorf("HostOnly(%q) rejected: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("HostOnly(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	rejected := []string{"", "ws://h/ws", "http://h", "h/ws", "u@h", "h:0", "h:x", "a:b:c", "h ws"}
	for _, in := range rejected {
		if _, err := HostOnly(in); err == nil {
			t.Errorf("HostOnly(%q) accepted, want rejection", in)
		}
	}
}

// TestApplyDefaultsPrinterPort requires ApplyDefaults to give every printer
// address the default port, after which Validate accepts the config.
func TestApplyDefaultsPrinterPort(t *testing.T) {
	cfg := &Config{
		Printers: []Printer{{Serial: "S1", Address: "h", Password: "1111"}},
	}
	cfg.ApplyDefaults()
	if cfg.Printers[0].Address != "h:8883" {
		t.Fatalf("address after ApplyDefaults = %q, want h:8883", cfg.Printers[0].Address)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}
