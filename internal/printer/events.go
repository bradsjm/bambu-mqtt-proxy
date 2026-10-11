package printer

import (
	"fmt"
	"strings"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/hmscodes"
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

// Transitions derives ordered activity entries from immutable documents.
func Transitions(prev, cur Doc) []Transition {
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
	oldAlerts, _ := prev.HMS()
	alerts, _ := cur.HMS()
	oldIDs := make(map[string]bool, len(oldAlerts))
	newIDs := make(map[string]bool, len(alerts))
	for _, alert := range oldAlerts {
		oldIDs[alert.ID()] = true
	}
	for _, alert := range alerts {
		id := alert.ID()
		newIDs[id] = true
		if oldIDs[id] {
			continue
		}
		severity := activity.Info
		switch alert.Severity() {
		case "fatal", "serious":
			severity = activity.Error
		case "common":
			severity = activity.Warning
		}
		info := hmscodes.HMS(alert.Attr)
		message := "Printer alert " + id + ", no description available"
		if info.Title != "" {
			message = "Printer alert: " + info.Title + " (" + id + ")"
		}
		events = append(events, Transition{"hms_alert", severity, message, info.URL})
	}
	var cleared []string
	for _, alert := range oldAlerts {
		if !newIDs[alert.ID()] {
			title := hmscodes.HMS(alert.Attr).Title
			if title == "" {
				title = alert.ID()
			}
			cleared = append(cleared, title)
		}
	}
	oldError, _ := prev.PrintError()
	if oldError != errCode {
		if oldError != 0 && !isCancelCode(oldError) {
			cleared = append(cleared, clearedErrorItem(oldError))
		}
		if errCode != 0 && !isCancelCode(errCode) {
			code := uint32(errCode)
			info := hmscodes.PrintError(code)
			text := fmt.Sprintf("%04X-%04X", code>>16, code&0xFFFF)
			severity := activity.Error
			switch info.Severity {
			case "info":
				severity = activity.Info
			case "warning":
				severity = activity.Warning
			}
			message := "Printer error " + text + ", no description available"
			if info.Title != "" {
				message = "Printer error: " + info.Title + " (" + text + ")"
			}
			events = append(events, Transition{"print_error", severity, message, info.URL})
		}
	}
	if len(cleared) > 0 {
		add("alerts_cleared", activity.Info, "Printer alerts cleared: "+strings.Join(cleared, ", "))
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

// clearedErrorItem names a print error that has left the document.
func clearedErrorItem(code int) string {
	if title := hmscodes.PrintError(uint32(code)).Title; title != "" {
		return title
	}
	return fmt.Sprintf("error %04X-%04X", uint32(code)>>16, uint32(code)&0xFFFF)
}
