package config

import (
	"testing"
)

// TestMCPDefaultOn pins the default: a config with no mcp section and no
// env override serves the endpoint, and only an explicit false disables it.
func TestMCPDefaultOn(t *testing.T) {
	cfg := &Config{}
	cfg.ApplyDefaults()
	cfg.HTTP.Port = 8080
	if !cfg.MCPEnabled() {
		t.Fatal("MCP must default to on")
	}

	falseVal := false
	cfg.MCP.Enabled = &falseVal
	if cfg.MCPEnabled() {
		t.Fatal("explicit enabled=false must keep MCP off")
	}

	trueVal := true
	cfg.MCP.Enabled = &trueVal
	if !cfg.MCPEnabled() {
		t.Fatal("explicit enabled=true must turn MCP on")
	}
}

// TestMCPDisabledWithoutHTTP pins the graceful rule: the endpoint mounts on
// the shared HTTP listener, so http.port 0 keeps it off without failing
// validation.
func TestMCPDisabledWithoutHTTP(t *testing.T) {
	cfg := &Config{}
	cfg.ApplyDefaults()
	cfg.Listen = []Listener{{Port: 8883, TLS: true}}
	printers := []Printer{{Serial: "S1", Address: "10.0.0.1:8883", Password: "1111"}}
	cfg.Printers = printers
	trueVal := true
	cfg.MCP.Enabled = &trueVal
	cfg.HTTP.Port = 0
	if cfg.MCPEnabled() {
		t.Fatal("MCP must be off when the HTTP listener is disabled")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with http.port 0: %v", err)
	}
	cfg.HTTP.Port = 8080
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with http port: %v", err)
	}
}
