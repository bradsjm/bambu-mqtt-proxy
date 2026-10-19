package config

import (
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
		wantErr bool
	}{
		{"blank alias accepted", []Printer{good("S1", ""), good("S2", "")}, false},
		{"valid alias accepted", []Printer{good("S1", "PRINTER1"), good("S2", "")}, false},
		{"invalid characters", []Printer{good("S1", "PRINTER-1")}, true},
		{"lowercase", []Printer{good("S1", "printer1")}, true},
		{"alias equal to own serial", []Printer{good("S1", "S1")}, true},
		{"alias equal to another serial", []Printer{good("S1", "S2"), good("S2", "")}, true},
		{"duplicate aliases", []Printer{good("S1", "DUP"), good("S2", "DUP")}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validAliasConfig(tc.aliases).Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate error = %v, want rejection %v", err, tc.wantErr)
			}
		})
	}
}
