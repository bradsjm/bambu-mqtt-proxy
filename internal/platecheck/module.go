package platecheck

import (
	"fmt"
	"math"
	"net/http"

	"bambu-mqtt-proxy/internal/module"
)

// ModuleState is the last automatic check, with null probabilities before a valid result.
type ModuleState struct {
	// State is the fixed check or stop-lifecycle state.
	State string `json:"state"`
	// Reason is the skip or failure code behind State, empty otherwise.
	Reason string `json:"reason,omitempty"`
	// Homing marks the second startup pass, after the toolhead homed.
	Homing bool `json:"homing,omitempty"`
	// Cutoff is the configured strict occupied-probability threshold.
	Cutoff float64 `json:"cutoff"`
	// MinClarity is the configured image-clarity threshold for both checks.
	MinClarity float64 `json:"min_clarity"`
	// PClear is nil unless the last check returned a valid score.
	PClear *float64 `json:"p_clear"`
	// POccupied is nil unless the last check returned a valid score.
	POccupied *float64 `json:"p_occupied"`
	// PAssessable is nil unless the last check returned a valid score.
	PAssessable *float64 `json:"p_assessable"`
	// FirstLayer is the independently consumed post-layer-1 phase.
	FirstLayer FirstLayerState `json:"first_layer"`
}

// FirstLayerState is a stable phase outcome with a separate confirmed pause lifecycle.
type FirstLayerState struct {
	// State is the fixed first-layer outcome, independent of startup state.
	State string `json:"state"`
	// Reason is the skip or failure code behind State, empty otherwise.
	Reason string `json:"reason,omitempty"`
	// Pause is pending, confirmed, failed, or unconfirmed after a pause verdict.
	Pause string `json:"pause,omitempty"`
	// Cutoff is the configured strict pause-capable defect threshold.
	Cutoff float64 `json:"cutoff"`
	// MinClarity is the configured image-clarity threshold for both checks.
	MinClarity float64 `json:"min_clarity"`
	// PAssessable is nil until a valid five-answer response is available.
	PAssessable *float64 `json:"p_assessable"`
	// PTangled is the last valid abnormal filament tangle probability.
	PTangled *float64 `json:"p_tangled"`
	// PDetached is the last valid detached-part probability.
	PDetached *float64 `json:"p_detached"`
	// PNozzleBlob is the last valid nozzle-blob probability.
	PNozzleBlob *float64 `json:"p_nozzle_blob"`
	// PIncomplete is the last valid warning-only missing-material probability.
	PIncomplete *float64 `json:"p_incomplete"`
}

// Module declares report-driven workers, diagnostics, and display hooks.
func (s *Service) Module() module.Module {
	m := module.Module{Name: "platecheck", Run: s.Run, State: s.moduleState, Display: s.Display, StatusValue: s.statusMap}
	if s.blocked == "" {
		m.Routes = []module.Route{{Pattern: "POST /platecheck/snapshots", Handler: http.HandlerFunc(s.handleSnapshots)}}
	}
	return m
}

// moduleState returns an immutable automatic status, or nil when protection is off.
func (s *Service) moduleState(serial string) any {
	if !s.settings.On() {
		return nil
	}
	return s.status(serial)
}

// status copies one printer's status without network work or aging fields.
func (s *Service) status(serial string) any {
	w := s.workers[serial]
	if w == nil {
		return nil
	}
	if !s.settings.On() {
		return ModuleState{State: "disabled", Cutoff: s.settings.StopConfidence, MinClarity: s.settings.MinClarity, FirstLayer: FirstLayerState{State: "disabled", Cutoff: s.settings.FirstLayer.PauseConfidence, MinClarity: s.settings.MinClarity}}
	}
	if s.blocked != "" {
		return ModuleState{State: "blocked", Cutoff: s.settings.StopConfidence, MinClarity: s.settings.MinClarity, FirstLayer: FirstLayerState{State: "blocked", Cutoff: s.settings.FirstLayer.PauseConfidence, MinClarity: s.settings.MinClarity}}
	}
	w.mu.Lock()
	v := w.status
	w.mu.Unlock()
	// Copy optional scalars so callers cannot mutate shared display values.
	for _, score := range []**float64{&v.PClear, &v.POccupied, &v.PAssessable, &v.FirstLayer.PAssessable, &v.FirstLayer.PTangled, &v.FirstLayer.PDetached, &v.FirstLayer.PNozzleBlob, &v.FirstLayer.PIncomplete} {
		if *score != nil {
			*score = new(**score)
		}
	}
	return v
}

// statusMap returns status-only disabled states for an off feature.
func (s *Service) statusMap() any {
	out := make(map[string]any, len(s.workers))
	for serial := range s.workers {
		out[serial] = s.status(serial)
	}
	return out
}

// Display renders the approved fixed-text panel without controls or overlay badges.
func (s *Service) Display(serial string) *module.Display {
	value := s.moduleState(serial)
	if value == nil {
		return nil
	}
	v := value.(ModuleState)
	var rows []module.Row
	rows = append(rows, plateRow(v))
	rows = append(rows, module.Row{
		Label:   "Rule",
		Value:   fmt.Sprintf("Stops the print if the plate looks occupied (over %s) and image clarity is at least %s", wholeText(v.Cutoff), wholeText(v.MinClarity)),
		Section: "Before printing",
	})
	if v.POccupied != nil {
		row := module.Row{
			Label:   "Scores",
			Value:   fmt.Sprintf("Occupied %s · image clarity %s (needs %s)", scoreText(*v.POccupied, v.Cutoff), clarityText(v.PAssessable, v.MinClarity), wholeText(v.MinClarity)),
			Section: "Before printing",
		}
		if v.PAssessable != nil && *v.PAssessable < v.MinClarity {
			row.Tone = "warn"
		}
		rows = append(rows, row)
	}
	rows = append(rows, firstLayerRow(v.FirstLayer))
	if v.FirstLayer.State != "disabled" {
		rows = append(rows, module.Row{
			Label:   "Rule",
			Value:   fmt.Sprintf("Pauses the print if tangle, detachment, or nozzle blob is over %s and image clarity is at least %s", wholeText(v.FirstLayer.Cutoff), wholeText(v.FirstLayer.MinClarity)),
			Section: "After layer 1",
		})
	}
	if v.FirstLayer.PAssessable != nil {
		tone := ""
		if *v.FirstLayer.PAssessable < v.FirstLayer.MinClarity {
			tone = "warn"
		}
		rows = append(rows, module.Row{
			Label:   "Image clarity",
			Value:   fmt.Sprintf("%s (needs %s)", scoreText(*v.FirstLayer.PAssessable, v.FirstLayer.MinClarity), wholeText(v.FirstLayer.MinClarity)),
			Tone:    tone,
			Section: "After layer 1",
		})
	}
	if v.FirstLayer.PTangled != nil && v.FirstLayer.PDetached != nil && v.FirstLayer.PNozzleBlob != nil && v.FirstLayer.PIncomplete != nil {
		row := module.Row{
			Label:   "Findings",
			Value:   fmt.Sprintf("Tangle %s · detached %s · nozzle blob %s · missing material %s (warning only)", scoreText(*v.FirstLayer.PTangled, v.FirstLayer.Cutoff), scoreText(*v.FirstLayer.PDetached, v.FirstLayer.Cutoff), scoreText(*v.FirstLayer.PNozzleBlob, v.FirstLayer.Cutoff), scoreText(*v.FirstLayer.PIncomplete, v.FirstLayer.Cutoff)),
			Section: "After layer 1",
		}
		if v.FirstLayer.State == "inconclusive" {
			row.Note = "Image not clear enough: findings not used"
		}
		rows = append(rows, row)
	}
	return &module.Display{Panel: &module.Panel{Title: "Print checks", Level: 3, Rows: rows}}
}

// wholeText renders a whole-percentage threshold without decimals.
func wholeText(p float64) string {
	return fmt.Sprintf("%.0f%%", math.Round(p*100))
}

// scoreText renders a score as a whole percentage, adding decimals only
// while the rendered score would read as equal to the compared threshold,
// so a score never seems to match a cutoff it is strictly above or below.
func scoreText(score, ref float64) string {
	digits := 0
	text := fmt.Sprintf("%.*f", digits, score*100)
	for score != ref && digits < 9 && text == fmt.Sprintf("%.*f", digits, ref*100) {
		digits++
		text = fmt.Sprintf("%.*f", digits, score*100)
	}
	return text + "%"
}

// clarityText renders a nil-safe image-clarity percentage against its minimum.
func clarityText(p *float64, min float64) string {
	if p == nil {
		return scoreText(0, min)
	}
	return scoreText(*p, min)
}

// plateRow renders the Before printing Plate row for one startup state.
func plateRow(v ModuleState) module.Row {
	row := module.Row{Label: "Plate", Section: "Before printing"}
	name := "Plate check"
	if v.Homing {
		row.Label = "Plate (after homing)"
		name = "Plate check after homing"
	}
	switch v.State {
	case "idle":
		row.Value = "Not checked yet"
		row.Note = "Checks the plate when a print starts and again after homing"
	case "checking":
		row.Value, row.Tone = "Checking", "info"
	case "clear":
		row.Value, row.Tone = "Clear", "calm"
	case "below_threshold":
		row.Value, row.Tone = "Possibly occupied, below the stop threshold", "info"
		row.Note = "Printing continued"
	case "inconclusive":
		row.Value = "Image unclear"
		row.Note = "Image not clear enough to judge; printing continued"
	case "error":
		row.Value, row.Tone = "Check failed", "warn"
		row.Note = reasonNote(name, v.Reason)
	case "skipped":
		row.Value = "Not checked"
		row.Note = reasonNote(name, v.Reason)
	case "blocked":
		row.Value, row.Tone = "Unavailable", "warn"
		row.Note = reasonNote("Plate check", "camera_disabled")
	case "stop_requested":
		row.Value, row.Tone = "Stop requested", "error"
		row.Note = "Waiting for the printer to confirm"
	case "stopped":
		row.Value, row.Tone = "Print ended after stop request", "error"
		row.Note = "Clear the plate before sending the print again"
	case "stop_failed":
		row.Value, row.Tone = "Stop command failed", "error"
		row.Note = "Check the printer now"
	case "stop_unconfirmed":
		row.Value, row.Tone = "Stop not confirmed", "error"
		row.Note = "Check the printer now"
	default:
		row.Value = "Not checked yet"
		row.Note = "Checks the plate when a print starts and again after homing"
	}
	return row
}

// firstLayerRow renders the After layer 1 First layer row for one phase outcome.
func firstLayerRow(v FirstLayerState) module.Row {
	row := module.Row{Label: "First layer", Section: "After layer 1"}
	switch v.State {
	case "disabled":
		row.Value = "Off"
	case "idle":
		row.Value = "Not checked yet"
		row.Note = "Checks after the first layer is printed"
	case "checking":
		row.Value, row.Tone = "Checking", "info"
	case "passed":
		row.Value, row.Tone = "Looks good", "calm"
	case "warning":
		row.Value, row.Tone = "Possible problem", "warn"
		row.Note = "Printing continued; check the printer"
	case "inconclusive":
		row.Value = "Image unclear"
		row.Note = "Image not clear enough to judge; printing continued"
	case "blocked":
		row.Value, row.Tone = "Unavailable", "warn"
		row.Note = reasonNote("First-layer check", "camera_disabled")
	case "error", "skipped":
		row.Value = "Not checked"
		row.Note = reasonNote("First-layer check", v.Reason)
	case "pause":
		switch v.Pause {
		case "pending":
			row.Value, row.Tone = "Pause sent", "error"
			row.Note = "Waiting for the printer to confirm"
		case "confirmed":
			row.Value, row.Tone = "Printer reports paused", "warn"
			row.Note = "Check the first layer before resuming"
		case "failed":
			row.Value, row.Tone = "Pause command failed", "error"
			row.Note = "Check the printer now"
		case "unconfirmed":
			row.Value, row.Tone = "Pause not confirmed", "error"
			row.Note = "Check the printer now"
		default:
			row.Value, row.Tone = "Possible failure", "warn"
		}
	default:
		row.Value = "Not checked yet"
		row.Note = "Checks after the first layer is printed"
	}
	return row
}
