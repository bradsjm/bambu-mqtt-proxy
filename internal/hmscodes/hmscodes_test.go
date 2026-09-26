package hmscodes

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPrintErrorLookup(t *testing.T) {
	info := PrintError(0x0300400C)
	if info.Title != "Print Cancelled" || info.Severity != "info" || info.Fix == "" {
		t.Fatalf("PrintError(0x0300400C) = %+v", info)
	}
	if want := codeBase + "hms-0300-400c/"; info.URL != want {
		t.Fatalf("URL = %q, want %q", info.URL, want)
	}
}

func TestHMSAttrLookup(t *testing.T) {
	info := HMS(0x07008010)
	if info.Title != "AMS Motor Overload" || info.Fix == "" {
		t.Fatalf("HMS(0x07008010) = %+v", info)
	}
	if want := codeBase + "hms-0700-8010/"; info.URL != want {
		t.Fatalf("URL = %q, want %q", info.URL, want)
	}
}

func TestUnknownCodeKeepsReferenceLink(t *testing.T) {
	// Print-module alerts report attr halves like 0300-0100, which the
	// dataset does not cover: the description stays empty but the link
	// must still resolve to the lookup tool.
	info := HMS(0x03000100)
	if info.Title != "" || info.Severity != "" || info.Fix != "" {
		t.Fatalf("unknown code must carry no description: %+v", info)
	}
	if want := codeBase + "?code=0300-0100"; info.URL != want {
		t.Fatalf("URL = %q, want %q", info.URL, want)
	}
}

func TestDatasetCoversAllKeys(t *testing.T) {
	var ds dataset
	if err := json.Unmarshal(codesJSON, &ds); err != nil {
		t.Fatalf("codes.json: %v", err)
	}
	keys := 0
	for _, e := range ds.Entries {
		if e.Title == "" || e.Fix == "" {
			t.Fatalf("entry %v missing title or fix", e.Codes)
		}
		switch e.Severity {
		case "error", "warning", "info":
		default:
			t.Fatalf("entry %v has severity %q", e.Codes, e.Severity)
		}
		for _, c := range e.Codes {
			keys++
			info, ok := lookup()[strings.ToUpper(c)]
			if !ok || info.Title == "" || info.URL == "" {
				t.Fatalf("code %s does not resolve: %+v", c, info)
			}
		}
	}
	// 260 codes as of 2026-09; regeneration may only grow this.
	if keys < 260 {
		t.Fatalf("dataset shrank to %d codes", keys)
	}
}
