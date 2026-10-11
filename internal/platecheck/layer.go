package platecheck

import (
	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// checkLayer evaluates two camera views and sends one pause for a severe defect.
func (w *worker) checkLayer(parent context.Context) {
	ctx, cancel := context.WithTimeout(context.WithValue(parent, operationKey{}, operations.Add(1)), 25*time.Second)
	defer cancel()
	doc, online, _ := w.s.state.Get(w.printer.Serial)
	state, _ := doc.GcodeState()
	if !online || state != "RUNNING" {
		w.layerSkip("skipped", "print_paused", 0)
		return
	}
	w.setLayerStatus("checking", "", nil)
	boundary := w.s.now()
	if light, _ := doc.ChamberLight(); light == "off" {
		if w.s.commands.Light(w.printer.Serial, true) == nil {
			boundary = w.s.now()
		}
	}
	images := make([][]byte, 0, 2)
	for range 2 {
		frame, err := w.s.frames.Capture(ctx, w.printer.Serial, boundary)
		if err != nil {
			w.layerSkip("error", errorCategory(err), 0)
			return
		}
		images = append(images, frame.JPEG)
		boundary = frame.Captured.Add(2 * time.Second)
	}
	result, err := w.s.client.EvaluateFirstLayer(ctx, images, config.DisplayModel(w.printer.Model, w.printer.Serial))
	if err != nil || !validLayerResult(result) {
		reason := "bad_response"
		if err != nil {
			reason = errorCategory(err)
		}
		w.layerSkip("error", reason, 0)
		return
	}
	decision := classifyLayer(result, w.s.settings.FirstLayer.PauseConfidence)
	w.setLayerStatus(decision, "", &result)
	switch decision {
	case "inconclusive":
		w.layerEvent("platecheck_layer1_skipped", activity.Info, "First-layer view inconclusive, continuing print", 0)
	case "warning":
		w.layerEvent("platecheck_layer1_warning", activity.Warning, "Possible first-layer problem: "+layerIssues(result, .5, true)+", continuing print", 0)
	case "passed":
		w.layerEvent("platecheck_layer1_passed", activity.Info, "First-layer check passed, continuing print", 0)
	case "pause":
		doc, online, _ = w.s.state.Get(w.printer.Serial)
		state, _ = doc.GcodeState()
		if !online || state != "RUNNING" {
			w.layerSkip("skipped", "print_paused", 0)
			return
		}
		if ctx.Err() != nil {
			w.layerSkip("error", errorCategory(ctx.Err()), 0)
			return
		}
		issues := layerIssues(result, w.s.settings.FirstLayer.PauseConfidence, false)
		layer, _ := doc.Layer()
		if err := w.s.commands.Pause(w.printer.Serial); err != nil {
			w.setLayerPause("failed")
			w.layerEvent("platecheck_layer1_pause_failed", activity.Error, "First-layer pause command failed: "+issues+", check the printer", 0)
			return
		}
		w.layerPending = &pendingCommand{layer: layer}
		w.setLayerPause("pending")
		w.layerEvent("platecheck_layer1_pause_sent", activity.Error, "Possible first-layer failure: "+issues+", pause sent", 0)
	}
}

// layerIssues names only the validated scores that triggered this decision.
func layerIssues(r LayerResult, cutoff float64, includeIncomplete bool) string {
	var issues []string
	for _, defect := range []struct {
		label string
		score float64
	}{
		{"filament tangled", r.PTangled},
		{"part detached", r.PDetached},
		{"nozzle blob", r.PNozzleBlob},
	} {
		if defect.score > cutoff {
			issues = append(issues, fmt.Sprintf("%s (%.1f%%)", defect.label, defect.score*100))
		}
	}
	if includeIncomplete && r.PIncomplete > cutoff {
		issues = append(issues, fmt.Sprintf("incomplete layer (%.1f%%, warning only)", r.PIncomplete*100))
	}
	return strings.Join(issues, ", ")
}

// setLayerStatus replaces only first-layer state and stores copies of all scores.
// Every other status change clears the skip reason; layerSkip sets it.
func (w *worker) setLayerStatus(state, pause string, r *LayerResult) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.setLayerStatusLocked(state, pause, r, "")
}

// setLayerStatusLocked publishes the first-layer status and reason. The caller holds w.mu.
func (w *worker) setLayerStatusLocked(state, pause string, r *LayerResult, reason string) {
	v := FirstLayerState{State: state, Pause: pause, Cutoff: w.s.settings.FirstLayer.PauseConfidence, Reason: reason}
	if r != nil {
		v.PAssessable = new(r.PAssessable)
		v.PTangled = new(r.PTangled)
		v.PDetached = new(r.PDetached)
		v.PNozzleBlob = new(r.PNozzleBlob)
		v.PIncomplete = new(r.PIncomplete)
	}
	w.status.FirstLayer = v
}

// setLayerPause changes only the pause lifecycle, preserving scores and phase outcome.
func (w *worker) setLayerPause(pause string) {
	w.mu.Lock()
	w.status.FirstLayer.Pause = pause
	w.mu.Unlock()
}

// layerSkip records a cataloged fail-open outcome without retaining previous images or scores.
func (w *worker) layerSkip(state, reason string, operation uint64) {
	w.mu.Lock()
	w.setLayerStatusLocked(state, "", nil, reason)
	w.mu.Unlock()
	w.layerEvent("platecheck_layer1_skipped", activity.Info, activitySentence("First-layer check", state, reason), operation)
}

// layerEvent records only fixed text and allowlisted local identifiers.
func (w *worker) layerEvent(kind, severity, message string, operation uint64) {
	w.s.activity.Record(w.printer.Serial, kind, severity, message)
	level := slog.LevelInfo
	if severity == activity.Warning {
		level = slog.LevelWarn
	} else if severity == activity.Error {
		level = slog.LevelError
	}
	w.s.log.Log(context.Background(), level, "First-layer check activity", "event", kind, "operation", operation, "serial", w.printer.Serial)
}
