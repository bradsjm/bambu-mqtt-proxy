package printer

import (
	"strings"

	"bambu-mqtt-proxy/internal/activity"
)

// Event describes one merged report or connectivity change.
type Event struct {
	// Serial identifies the printer.
	Serial string
	// Prev is the immutable document before the event.
	Prev Doc
	// Cur is the immutable document after the event.
	Cur Doc
	// Online is connectivity after the event.
	Online bool
	// Kinds contains transition kinds in table order.
	Kinds []string
	// NewJob marks the first event of a new print job; see NewJob.
	NewJob bool
}

// Transition is one user-facing change derived from printer reports.
type Transition struct {
	// Kind identifies the change.
	Kind string
	// Severity classifies the change.
	Severity string
	// Message contains display-ready text.
	Message string
	// URL links to an optional error reference.
	URL string
}

// NewJob reports whether cur begins a new print job: the first PREPARE or
// SLICING, or RUNNING with no PREPARE or SLICING before it. A resume from
// pause and the first state ever seen (state_initial) never begin a job.
// It is the only new-job rule; modules read Event.NewJob instead of
// deriving their own.
func NewJob(prev, cur Doc) bool {
	before, seen := prev.GcodeState()
	state, current := cur.GcodeState()
	if !seen || !current || state == before || before == "PREPARE" || before == "SLICING" {
		return false
	}
	switch state {
	case "PREPARE", "SLICING":
		return true
	case "RUNNING":
		return before != "PAUSE" && before != "PAUSED"
	}
	return false
}

// Transitions compares report state and displayed problems under one catalog snapshot.
func Transitions(prev, cur Doc, oldProblems, newProblems []Problem) []Transition {
	before, seen := prev.GcodeState()
	state, current := cur.GcodeState()
	name, _ := cur.JobName()
	filename := ""
	if name != "" {
		filename = " · " + name
	}
	var events []Transition
	add := func(kind, severity, message string) {
		events = append(events, Transition{Kind: kind, Severity: severity, Message: message})
	}
	if !seen && current {
		add("state_initial", activity.Info, "Printer "+strings.ToLower(state)+filename)
		return events
	}
	errCode, _ := cur.PrintError()
	if current && state != before {
		switch state {
		case "PREPARE", "SLICING":
			add("print_preparing", activity.Info, "Print preparing"+filename)
		case "RUNNING":
			if before == "PAUSE" || before == "PAUSED" {
				add("print_resumed", activity.Info, "Print resumed"+filename)
			} else {
				add("print_started", activity.Info, "Print started"+filename)
			}
		case "PAUSE", "PAUSED":
			if before != "PAUSE" && before != "PAUSED" {
				add("print_paused", activity.Warning, "Print paused"+filename)
			}
		case "FINISH":
			add("print_finished", activity.Info, "Print finished"+filename)
		case "FAILED":
			if isCancelCode(errCode) {
				add("print_cancelled", activity.Warning, "Print cancelled"+filename)
			} else {
				add("print_failed", activity.Error, "Print failed"+filename)
			}
		case "IDLE":
			if before == "RUNNING" || before == "PAUSE" || before == "PAUSED" {
				add("print_stopped", activity.Warning, "Print stopped before finishing"+filename)
			}
		}
	}
	// oldCodes and newCodes represent displayed identities, not inferred fault aliases.
	oldCodes := make(map[string]bool)
	newCodes := make(map[string]bool)
	for _, problem := range oldProblems {
		for _, code := range problem.Codes {
			oldCodes[code.Code] = true
		}
	}
	for _, problem := range newProblems {
		for _, code := range problem.Codes {
			newCodes[code.Code] = true
		}
	}
	for _, problem := range newProblems {
		// overlap suppresses another announcement when a group gains a companion code.
		overlap := false
		// codes preserves constituent order in the activity description.
		codes := make([]string, 0, len(problem.Codes))
		for _, code := range problem.Codes {
			overlap = overlap || oldCodes[code.Code]
			codes = append(codes, code.Code)
		}
		if !overlap {
			events = append(events, Transition{"printer_problem", problem.Severity, "Printer " + problem.Severity + ": " + problem.Message + " (" + strings.Join(codes, ", ") + ")", problem.URL})
		}
	}
	for _, problem := range oldProblems {
		// overlap keeps a partially cleared group active without a resolution.
		overlap := false
		// codes names every constituent in the resolved historical projection.
		codes := make([]string, 0, len(problem.Codes))
		for _, code := range problem.Codes {
			overlap = overlap || newCodes[code.Code]
			codes = append(codes, code.Code)
		}
		if !overlap {
			events = append(events, Transition{"problem_resolved", activity.Info, "Problem resolved: " + problem.Message + " (" + strings.Join(codes, ", ") + ")", problem.URL})
		}
	}
	oldLight, lightSeen := prev.ChamberLight()
	light, lightNow := cur.ChamberLight()
	if lightSeen && lightNow && oldLight != light {
		switch light {
		case "on":
			add("chamber_light_on", activity.Info, "Chamber light on")
		case "off":
			add("chamber_light_off", activity.Info, "Chamber light off")
		}
	}
	oldLayer, _ := prev.Layer()
	layer, _ := cur.Layer()
	if oldLayer <= 1 && layer >= 2 && (state == "RUNNING" || state == "PAUSE" || state == "PAUSED") {
		add("first_layer_complete", activity.Info, "First layer complete"+filename)
	}
	return events
}

// isCancelCode identifies printer user-cancel markers.
func isCancelCode(code int) bool { return uint32(code) == 0x0300400C || uint32(code) == 0x0500400E }
