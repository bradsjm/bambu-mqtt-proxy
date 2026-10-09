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
	row := module.Row{Label: "Result"}
	switch v.State {
	case "idle":
		row.Value = "No check yet"
	case "checking":
		row.Value, row.Tone = "Checking", "info"
	case "blocked":
		row.Value, row.Tone = "Camera unavailable · print continued", "warn"
	case "clear":
		row.Value, row.Tone = "Clear", "calm"
	case "below_threshold":
		row.Value, row.Tone = "Below cutoff · print continued", "info"
	case "inconclusive":
		row.Value = "Inconclusive · print continued"
	case "error":
		row.Value, row.Tone = "Check failed · print continued", "warn"
	case "skipped":
		row.Value = "Check skipped · print continued"
	case "stop_requested":
		row.Value, row.Tone = "Stop requested", "error"
	case "stopped":
		row.Value, row.Tone = "Print ended after stop request", "error"
	case "stop_failed":
		row.Value, row.Tone = "Stop command failed · check printer", "error"
	case "stop_unconfirmed":
		row.Value, row.Tone = "Stop unconfirmed · check printer", "error"
	default:
		row.Value = "No check yet"
	}
	rows := []module.Row{row}
	if v.State != "idle" {
		rows = append(rows, module.Row{Label: "Stop above", Value: fmt.Sprintf("%.0f%%", math.Round(v.Cutoff*100))})
	}
	if v.POccupied != nil {
		rows = append(rows, module.Row{Label: "P(occupied)", Value: fmt.Sprintf("%.1f%%", *v.POccupied*100)})
	}
	layer := module.Row{Label: "First layer"}
	switch v.FirstLayer.State {
	case "disabled":
		layer.Value = "Disabled"
	case "checking":
		layer.Value, layer.Tone = "Checking two views", "info"
	case "passed":
		layer.Value, layer.Tone = "Passed", "calm"
	case "warning":
		layer.Value, layer.Tone = "Possible defect · print continued", "warn"
	case "inconclusive":
		layer.Value = "Inconclusive · print continued"
	case "blocked", "error", "skipped":
		layer.Value = "Check unavailable · print continued"
	case "pause":
		switch v.FirstLayer.Pause {
		case "pending":
			layer.Value, layer.Tone = "Pause sent · awaiting confirmation", "error"
		case "confirmed":
			layer.Value, layer.Tone = "Pause confirmed · check print", "warn"
		case "failed":
			layer.Value, layer.Tone = "Pause command failed · check printer", "error"
		case "unconfirmed":
			layer.Value, layer.Tone = "Pause unconfirmed · check printer", "error"
		default:
			layer.Value, layer.Tone = "Possible failure", "warn"
		}
	default:
		layer.Value = "No check yet"
	}
	rows = append(rows, layer)
	if v.FirstLayer.State != "idle" && v.FirstLayer.State != "disabled" {
		rows = append(rows, module.Row{Label: "Pause above", Value: fmt.Sprintf("%.0f%%", math.Round(v.FirstLayer.Cutoff*100))})
	}
	for _, score := range []struct {
		label string
		value *float64
	}{
		{"P(layer assessable)", v.FirstLayer.PAssessable},
		{"P(tangled)", v.FirstLayer.PTangled},
		{"P(detached)", v.FirstLayer.PDetached},
		{"P(nozzle blob)", v.FirstLayer.PNozzleBlob},
		{"P(incomplete) · warning only", v.FirstLayer.PIncomplete},
	} {
		if score.value != nil {
			rows = append(rows, module.Row{Label: score.label, Value: fmt.Sprintf("%.1f%%", *score.value*100)})
		}
	}
	return &module.Display{Panel: &module.Panel{Title: "Plate check", Level: 3, Rows: rows}}
}
