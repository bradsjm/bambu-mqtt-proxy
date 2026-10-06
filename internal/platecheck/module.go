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
}

// Module declares lifecycle, startup activity interest, diagnostics, and display hooks.
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
		return ModuleState{State: "disabled", Cutoff: s.settings.StopConfidence}
	}
	if s.blocked != "" {
		return ModuleState{State: "blocked", Cutoff: s.settings.StopConfidence}
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
	return &module.Display{Panel: &module.Panel{Title: "Plate check", Level: 3, Rows: rows}}
}
