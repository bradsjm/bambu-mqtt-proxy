package configui

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bambu-mqtt-proxy/internal/config"
)

// TestAliasGetReturnsStoredAlias requires the settings GET to return the
// stored alias alongside the real serial.
func TestAliasGetReturnsStoredAlias(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	const y = `printers:
  - serial: "01P00A123456789"
    alias: "01S00B987654321"
    address: "192.168.1.42:8883"
    tls: true
    insecure_skip_verify: true
    password: "secret-code"
`
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	_, srv := serve(t, path)

	v := get(t, srv).Config
	if len(v.Printers) != 1 {
		t.Fatalf("view printers = %d, want 1", len(v.Printers))
	}
	if v.Printers[0].Serial != "01P00A123456789" || v.Printers[0].Alias != "01S00B987654321" {
		t.Fatalf("view printer = %+v, want the stored alias returned", v.Printers[0])
	}
}

// TestAliasSaveTrimsAliasToFile requires a save to trim the submitted alias
// and persist it to the config file with the access code kept.
func TestAliasSaveTrimsAliasToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeConfig(t, path)
	_, srv := serve(t, path)

	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.Printers[0].Alias = "  01S00B987654321  "
	if code, body := put(t, srv, v); code != http.StatusOK {
		t.Fatalf("save = %d %v", code, body)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Printers) != 1 || cfg.Printers[0].Alias != "01S00B987654321" {
		t.Fatalf("written printers = %+v, want the trimmed alias stored", cfg.Printers)
	}
	if cfg.Printers[0].Password != "secret-code" {
		t.Fatalf("written password = %q, want the stored code kept", cfg.Printers[0].Password)
	}
}

// TestAliasDuplicateSerialRejected requires a save whose alias duplicates
// another printer's serial to be rejected with the validation status. The
// alias uniqueness check itself lives in config.Validate, owned by the
// parallel validation change; this test only pins the configui rejection path.
func TestAliasDuplicateSerialRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeConfig(t, path)
	_, srv := serve(t, path)

	v := get(t, srv).Config
	v.Printers[0].PreviousSerial = v.Printers[0].Serial
	v.Printers = append(v.Printers, PrinterView{
		Serial: "01S00B987654321", Address: "192.168.1.43:8883", TLS: true,
		InsecureSkipVerify: true, Username: "bblp", AccessCode: "87654321",
		Alias: "01P00A123456789",
	})
	code, body := put(t, srv, v)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("duplicate-alias save = %d %v, want 422", code, body)
	}
	msg, _ := body["error"].(string)
	if msg == "" || !strings.Contains(strings.ToLower(msg), "alias") {
		t.Fatalf("duplicate-alias error = %q, want the validation message to mention the alias", msg)
	}
	if cfg, err := config.Load(path); err != nil || len(cfg.Printers) != 1 || cfg.Printers[0].Alias != "" {
		t.Fatalf("rejected save changed the config file: %+v err = %v", cfg, err)
	}
}
