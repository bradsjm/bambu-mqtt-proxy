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

// Module declares lifecycle, startup and first-layer activity interest, diagnostics, and display hooks.
func (s *Service) Module() module.Module {
	on := s.settings.On() && s.blocked == ""
	m := module.Module{Name: "platecheck", Start: s.Start, Stop: s.Close, NeedsReports: on, State: s.moduleState, Display: s.Display, StatusValue: s.statusMap}
	if on {
		m.ObserveActivity = s.ObserveActivity
	}
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
		return ModuleState{State: "disabled", Cutoff: s.settings.StopConfidence, FirstLayer: FirstLayerState{State: "disabled", Cutoff: s.settings.FirstLayer.PauseConfidence}}
	}
	if s.blocked != "" {
		return ModuleState{State: "blocked", Cutoff: s.settings.StopConfidence, FirstLayer: FirstLayerState{State: "blocked", Cutoff: s.settings.FirstLayer.PauseConfidence}}
	}
	w.mu.Lock()
	v := w.status
	w.mu.Unlock()
	// Copy optional scalars so callers cannot mutate shared display values.
	if v.PClear != nil {
		v.PClear = new(*v.PClear)
	}
	if v.POccupied != nil {
		v.POccupied = new(*v.POccupied)
	}
	if v.PAssessable != nil {
		v.PAssessable = new(*v.PAssessable)
	}
	if v.FirstLayer.PAssessable != nil {
		v.FirstLayer.PAssessable = new(*v.FirstLayer.PAssessable)
	}
	if v.FirstLayer.PTangled != nil {
		v.FirstLayer.PTangled = new(*v.FirstLayer.PTangled)
	}
	if v.FirstLayer.PDetached != nil {
		v.FirstLayer.PDetached = new(*v.FirstLayer.PDetached)
	}
	if v.FirstLayer.PNozzleBlob != nil {
		v.FirstLayer.PNozzleBlob = new(*v.FirstLayer.PNozzleBlob)
	}
	if v.FirstLayer.PIncomplete != nil {
		v.FirstLayer.PIncomplete = new(*v.FirstLayer.PIncomplete)
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
		Value:   fmt.Sprintf("Stops the print if the plate looks occupied (over %.0f%%)", math.Round(v.Cutoff*100)),
		Section: "Before printing",
	})
	if v.POccupied != nil {
		rows = append(rows, module.Row{
			Label:   "Scores",
			Value:   fmt.Sprintf("Occupied %.0f%% · view clarity %.0f%%", math.Round(*v.POccupied*100), clarity(v.PAssessable)),
			Section: "Before printing",
		})
	}
	rows = append(rows, firstLayerRow(v.FirstLayer))
	if v.FirstLayer.State != "disabled" {
		rows = append(rows, module.Row{
			Label:   "Rule",
			Value:   fmt.Sprintf("Pauses the print if tangle, detachment, or nozzle blob is over %.0f%%", math.Round(v.FirstLayer.Cutoff*100)),
			Section: "After layer 1",
		})
	}
	if v.FirstLayer.PAssessable != nil {
		tone := ""
		if *v.FirstLayer.PAssessable < 0.8 {
			tone = "warn"
		}
		rows = append(rows, module.Row{
			Label:   "View clarity",
			Value:   fmt.Sprintf("%.0f%% (needs 80%%)", math.Round(*v.FirstLayer.PAssessable*100)),
			Tone:    tone,
			Section: "After layer 1",
		})
	}
	if v.FirstLayer.PTangled != nil && v.FirstLayer.PDetached != nil && v.FirstLayer.PNozzleBlob != nil && v.FirstLayer.PIncomplete != nil {
		row := module.Row{
			Label:   "Findings",
			Value:   fmt.Sprintf("Tangle %.0f%% · detached %.0f%% · nozzle blob %.0f%% · missing material %.0f%% (warning only)", math.Round(*v.FirstLayer.PTangled*100), math.Round(*v.FirstLayer.PDetached*100), math.Round(*v.FirstLayer.PNozzleBlob*100), math.Round(*v.FirstLayer.PIncomplete*100)),
			Section: "After layer 1",
		}
		if v.FirstLayer.State == "inconclusive" {
			row.Note = "Low confidence: camera view too unclear"
		}
		rows = append(rows, row)
	}
	return &module.Display{Panel: &module.Panel{Title: "Print checks", Level: 3, Rows: rows}}
}

// clarity renders a nil-safe view-clarity percentage.
func clarity(p *float64) float64 {
	if p == nil {
		return 0
	}
	return math.Round(*p * 100)
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
		row.Value = "Inconclusive"
		row.Note = "Camera view too unclear to judge; printing continued"
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
		row.Value = "Inconclusive"
		row.Note = "Camera view too unclear to judge; printing continued"
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
