package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestApplyEnvFullConfig(t *testing.T) {
	t.Setenv(EnvPrinters,
		"serial=S1,address=10.0.0.1:8883,password=1111; serial=S2,address=10.0.0.2:1883,tls=false,password=2222,username=other")
	t.Setenv(EnvListenPort, "9999")
	t.Setenv(EnvListenTLS, "false")
	t.Setenv(EnvAuthMode, "accept_all")
	t.Setenv(EnvLogLevel, "warn")
	t.Setenv(EnvHealthPort, "9090")

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
	if cfg.Auth.Mode != AuthModeAcceptAll || cfg.Log.Level != "warn" || cfg.Health.Port != 9090 {
		t.Fatalf("auth/log/health = %s/%s/%d", cfg.Auth.Mode, cfg.Log.Level, cfg.Health.Port)
	}
	if len(cfg.Printers) != 2 {
		t.Fatalf("printers = %d, want 2", len(cfg.Printers))
	}
	s1, s2 := cfg.Printers[0], cfg.Printers[1]
	if s1.Serial != "S1" || !s1.TLS || !s1.InsecureSkipVerify || s1.Username != "bblp" {
		t.Fatalf("S1 = %+v", s1)
	}
	if s2.Serial != "S2" || s2.TLS || s2.Username != "other" {
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
