// Package hmscodes maps Bambu Lab printer error codes to short,
// human-friendly descriptions for the status payload and the camera wall.
// The dataset in codes.json is generated from the Printara3D Bambu
// error-code lookup (https://printara3d.com/tools/bambu-error-codes/),
// whose entries the site author verified against Bambu's official wiki;
// every alert presented to a user links back to those pages.
//
// Regenerating codes.json: fetch the lookup page, take the
// wp-content/plugins/printara3d-tools/dist/hms-lookup.*.js bundle URL from
// its HTML, download the bundle, and decode the JSON object assigned to
// "const DATA". Each DATA entry becomes one codes.json entry with its
// English title, severity, and fix (whitespace-collapsed); the "article"
// list names the entry's codes that have a dedicated article page (the
// page's "Common error codes" link list). Sort entries by first code and
// codes within an entry so regeneration produces stable diffs.
package hmscodes

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// codeBase is the Printara3D lookup tool that resolves codes interactively.
const codeBase = "https://printara3d.com/tools/bambu-error-codes/"

//go:embed codes.json
var codesJSON []byte

// Info describes one printer error code in display form.
type Info struct {
	// Title is the short description, e.g. "AMS Motor Overload". Empty
	// means the dataset does not cover the code.
	Title string
	// Severity is the dataset's rating: "error", "warning", or "info".
	Severity string
	// Fix is the dataset's suggested remediation.
	Fix string
	// URL links the code to Printara3D: the dedicated article page when
	// one exists, otherwise the lookup tool with the code as a ?code=
	// hint. It is set even for codes without a description.
	URL string
}

// entry is one codes.json record: one description shared by the alias
// codes that report the same condition, plus the subset that has
// dedicated article pages.
type entry struct {
	Codes    []string `json:"codes"`
	Title    string   `json:"title"`
	Severity string   `json:"severity"`
	Fix      string   `json:"fix"`
	Article  []string `json:"article,omitempty"`
}

// dataset is the codes.json envelope.
type dataset struct {
	Entries []entry `json:"entries"`
}

// lookup maps normalized dataset keys, e.g. "0700-8010", to display info.
// The once-value keeps startup free of parsing until the first lookup.
var lookup = sync.OnceValue(func() map[string]Info {
	var ds dataset
	if err := json.Unmarshal(codesJSON, &ds); err != nil {
		panic(fmt.Sprintf("hmscodes: embedded codes.json: %v", err))
	}
	withArticle := make(map[string]struct{})
	for _, e := range ds.Entries {
		for _, c := range e.Article {
			withArticle[strings.ToUpper(c)] = struct{}{}
		}
	}
	m := make(map[string]Info, len(ds.Entries))
	for _, e := range ds.Entries {
		for _, c := range e.Codes {
			key := strings.ToUpper(c)
			url := codeBase + "?code=" + key
			if _, ok := withArticle[key]; ok {
				url = fmt.Sprintf("%shms-%s/", codeBase, strings.ToLower(key))
			}
			m[key] = Info{Title: e.Title, Severity: e.Severity, Fix: e.Fix, URL: url}
		}
	}
	return m
})

// PrintError describes a report's 32-bit print_error value, whose halves
// form the on-screen code, e.g. 0x0300400C as 0300-400C.
func PrintError(v uint32) Info {
	return describe(fmt.Sprintf("%04X-%04X", v>>16, v&0xFFFF))
}

// HMS describes an HMS alert by the attr word of its 64-bit code: the
// dataset keys HMS entries by the attr halves, so HMS_0700_8010_... looks
// up as 0700-8010. Print-module alerts whose attr halves have no dataset
// entry return only the reference link.
func HMS(attr uint32) Info {
	return describe(fmt.Sprintf("%04X-%04X", attr>>16, attr&0xFFFF))
}

// describe resolves one dataset key, falling back to the reference link.
func describe(k string) Info {
	if info, ok := lookup()[k]; ok {
		return info
	}
	return Info{URL: codeBase + "?code=" + k}
}
