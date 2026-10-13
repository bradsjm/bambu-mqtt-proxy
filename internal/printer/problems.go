package printer

import (
	"fmt"

	"bambu-mqtt-proxy/internal/hmscodes"
)

// ProblemCode preserves one reported identifier and its own catalog text and link.
type ProblemCode struct {
	// Code is the displayed screen or health identifier.
	Code string `json:"code"`
	// Source is screen or health.
	Source string `json:"source"`
	// Message is catalog text or the current catalog-state explanation.
	Message string `json:"message"`
	// URL links to this identifier's reference.
	URL string `json:"url"`
}

// Problem groups visible codes by module prefix, not by verified fault equivalence.
type Problem struct {
	// Severity is the maximum constituent severity.
	Severity string `json:"severity"`
	// Message is the representative code's message.
	Message string `json:"message"`
	// URL is the same representative code's link.
	URL string `json:"url"`
	// Codes retains every constituent in screen-first report order.
	Codes []ProblemCode `json:"codes"`
}

// projectedCode carries selection metadata without exposing it in the public view.
type projectedCode struct {
	// value is the public code projection.
	value ProblemCode
	// severity ranks the reported severity.
	severity string
	// ready marks an available official description.
	ready bool
}

// Problems projects visible printer conditions from one immutable catalog snapshot.
func Problems(d Doc, serial string, snap hmscodes.Snapshot) []Problem {
	// groups preserves screen-first order and report order for other modules.
	groups := [][]projectedCode{}
	// screenPrefix identifies the only module whose health codes can join the screen.
	screenPrefix := ""
	if n, _ := d.PrintError(); n != 0 && !isCancelCode(n) {
		// code and lookup retain the raw screen identity and catalog visibility decision.
		code := uint32(n)
		lookup := snap.Error(code)
		if !(lookup.State == hmscodes.StateReady && lookup.Listed && lookup.Message == "") {
			// display uses the screen form, independent of the catalog's key form.
			display := fmt.Sprintf("%04X-%04X", code>>16, code&0xFFFF)
			// severity defaults unknown printer levels to error.
			severity := "error"
			switch (code & 0xF000) >> 12 {
			case 8:
				severity = "warning"
			case 12:
				severity = "info"
			}
			groups = append(groups, []projectedCode{{ProblemCode{display, "screen", problemMessage(display, lookup), hmscodes.ErrorTableURL}, severity, lookup.State == hmscodes.StateReady && lookup.Message != ""}})
			screenPrefix = display[:4]
		}
	}
	// seen deduplicates full raw health identities, including their reported level.
	seen := make(map[HMSAlert]bool)
	// alerts retains the printer's health report order.
	alerts, _ := d.HMS()
	for _, alert := range alerts {
		if seen[alert] {
			continue
		}
		seen[alert] = true
		// lookup uses Studio's normalized long code through the catalog snapshot.
		lookup := snap.HMS(alert.Attr, alert.Code)
		if lookup.State == hmscodes.StateReady && lookup.Message == "" {
			continue
		}
		// display preserves the raw health code's four reported halves.
		display := fmt.Sprintf("%04X-%04X-%04X-%04X", alert.Attr>>16, alert.Attr&0xFFFF, alert.Code>>16, alert.Code&0xFFFF)
		// severity maps the health level to the public three-level vocabulary.
		severity := "error"
		switch alert.Severity() {
		case "common":
			severity = "warning"
		case "info":
			severity = "info"
		}
		// projected preserves per-code text and URL for grouped details.
		projected := projectedCode{ProblemCode{display, "health", problemMessage(display, lookup), hmscodes.HMSURL(serial, alert.Attr, alert.Code)}, severity, lookup.State == hmscodes.StateReady && lookup.Message != ""}
		if screenPrefix != "" && display[:4] == screenPrefix {
			groups[0] = append(groups[0], projected)
		} else {
			groups = append(groups, []projectedCode{projected})
		}
	}
	// out stays non-null even when the report has no visible conditions.
	out := make([]Problem, 0, len(groups))
	for _, group := range groups {
		// representative starts with the first code unless official text takes precedence.
		representative := 0
		// healthRepresentative tracks the strongest Ready health description.
		healthRepresentative := -1
		// problem aggregates severity but keeps every code's description.
		problem := Problem{Severity: "info", Codes: make([]ProblemCode, 0, len(group))}
		for i, code := range group {
			problem.Codes = append(problem.Codes, code.value)
			if problemSeverityRank(code.severity) > problemSeverityRank(problem.Severity) {
				problem.Severity = code.severity
			}
			if code.ready && code.value.Source == "health" && (healthRepresentative < 0 || problemSeverityRank(code.severity) > problemSeverityRank(group[healthRepresentative].severity)) {
				healthRepresentative = i
			}
		}
		if healthRepresentative >= 0 {
			representative = healthRepresentative
		}
		problem.Message = group[representative].value.Message
		problem.URL = group[representative].value.URL
		out = append(out, problem)
	}
	return out
}

// problemMessage describes the catalog state for a visible reported code.
func problemMessage(display string, lookup hmscodes.Lookup) string {
	if lookup.State == hmscodes.StateReady && lookup.Message != "" {
		return lookup.Message
	}
	// prefix identifies the reported code in every fallback explanation.
	prefix := "Printer reported code " + display + ". Bambu's message list for this printer "
	switch lookup.State {
	case hmscodes.StateFailed:
		return prefix + "could not be downloaded: " + lookup.Err
	case hmscodes.StateReady:
		return prefix + "has no text for this code."
	default:
		return prefix + "is still downloading."
	}
}

// problemSeverityRank orders the public severities for aggregation and selection.
func problemSeverityRank(severity string) int {
	switch severity {
	case "error":
		return 3
	case "warning":
		return 2
	default:
		return 1
	}
}
