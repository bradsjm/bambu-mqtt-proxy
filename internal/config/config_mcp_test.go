package config

import (
	"testing"
)

// TestMCPDefaultOn pins the default: a config with no mcp section and no
// env override serves the endpoint, and only an explicit false disables it.
func TestMCPDefaultOn(t *testing.T) {
	cfg := &Config{}
	cfg.ApplyDefaults()
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
