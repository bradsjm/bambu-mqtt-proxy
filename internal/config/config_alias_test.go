package config

import (
	"strings"
	"testing"
)

func validAliasConfig(printers []Printer) *Config {
	cfg := &Config{Printers: printers}
	cfg.ApplyDefaults()
	return cfg
}

func TestAliasValidation(t *testing.T) {
	good := func(serial, alias string) Printer {
		return Printer{Serial: serial, Address: "host", Password: "code", Alias: alias}
	}
	cases := []struct {
		name    string
		aliases []Printer
		wantErr string
	}{
		{"blank alias accepted", []Printer{good("S1", ""), good("S2", "")}, ""},
		{"valid alias accepted", []Printer{good("S1", "PRINTER1"), good("S2", "")}, ""},
		{"invalid characters", []Printer{good("S1", "PRINTER-1")}, `alias "PRINTER-1" must contain only uppercase letters and digits`},
		{"lowercase", []Printer{good("S1", "printer1")}, `alias "printer1" must contain only uppercase letters and digits`},
		{"alias equal to own serial", []Printer{good("S1", "S1")}, `alias "S1" duplicates a configured serial or alias`},
		{"alias equal to another serial", []Printer{good("S1", "S2"), good("S2", "")}, `alias "S2" duplicates a configured serial or alias`},
		{"duplicate aliases", []Printer{good("S1", "DUP"), good("S2", "DUP")}, `alias "DUP" duplicates a configured serial or alias`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validAliasConfig(tc.aliases).Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate accepted, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate error = %q, want substring %q", err.Error(), tc.wantErr)
			}
		})
	}
}
