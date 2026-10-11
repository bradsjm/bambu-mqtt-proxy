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
	operation := operationOf(ctx)
	started := time.Now()
	doc, online, _ := w.s.state.Get(w.printer.Serial)
	state, _ := doc.GcodeState()
	layer, hasLayer := doc.Layer()
	if !online || state != "RUNNING" {
		w.layerSkip("skipped", "print_paused", operation)
		w.logLayerOutcome(operation, "skipped", "print_paused", nil, state, layer, hasLayer, nil, time.Since(started).Milliseconds())
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
	captured := make([]Frame, 0, 2)
	for range 2 {
		frame, err := w.s.frames.Capture(ctx, w.printer.Serial, boundary)
		if err != nil {
			reason := errorCategory(err)
			w.layerSkip("error", reason, operation)
			w.logLayerOutcome(operation, "error", reason, nil, state, layer, hasLayer, capturedOrNil(captured), time.Since(started).Milliseconds())
			return
		}
		images = append(images, frame.JPEG)
		captured = append(captured, frame)
		boundary = frame.Captured.Add(2 * time.Second)
	}
	result, err := w.s.client.EvaluateFirstLayer(ctx, images, config.DisplayModel(w.printer.Model, w.printer.Serial))
	if err != nil || !validLayerResult(result) {
		reason := "bad_response"
		if err != nil {
			reason = errorCategory(err)
		}
		w.layerSkip("error", reason, operation)
		w.logLayerOutcome(operation, "error", reason, nil, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
		return
	}
	decision := classifyLayer(result, w.s.settings.FirstLayer.PauseConfidence)
	w.setLayerStatus(decision, "", &result)
	switch decision {
	case "inconclusive":
		w.layerEvent("platecheck_layer1_skipped", activity.Info, "First-layer view inconclusive, continuing print", operation)
		w.logLayerOutcome(operation, decision, "", &result, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
	case "warning":
		w.layerEvent("platecheck_layer1_warning", activity.Warning, "Possible first-layer problem: "+layerIssues(result, .5, true)+", continuing print", operation)
		w.logLayerOutcome(operation, decision, "", &result, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
	case "passed":
		w.layerEvent("platecheck_layer1_passed", activity.Info, "First-layer check passed, continuing print", operation)
		w.logLayerOutcome(operation, decision, "", &result, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
	case "pause":
		doc, online, _ = w.s.state.Get(w.printer.Serial)
		state, _ = doc.GcodeState()
		layer, hasLayer = doc.Layer()
		if !online || state != "RUNNING" {
			w.layerSkip("skipped", "print_paused", operation)
			w.logLayerOutcome(operation, "skipped", "print_paused", nil, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
			return
		}
		if ctx.Err() != nil {
			reason := errorCategory(ctx.Err())
			w.layerSkip("error", reason, operation)
			w.logLayerOutcome(operation, "error", reason, nil, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
			return
		}
		issues := layerIssues(result, w.s.settings.FirstLayer.PauseConfidence, false)
		pauseErr := w.s.commands.Pause(w.printer.Serial)
		if pauseErr != nil {
			w.setLayerPause("failed")
			w.layerEvent("platecheck_layer1_pause_failed", activity.Error, "First-layer pause command failed: "+issues+", check the printer", operation)
			w.logLayerPause(operation, &result, state, layer, hasLayer, true, 0)
			w.logLayerOutcome(operation, decision, "", &result, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
			return
		}
		w.layerPending = &pendingCommand{layer: layer}
		w.setLayerPause("pending")
		w.layerEvent("platecheck_layer1_pause_sent", activity.Error, "Possible first-layer failure: "+issues+", pause sent", operation)
		w.logLayerPause(operation, &result, state, layer, hasLayer, false, layer)
		w.logLayerOutcome(operation, decision, "", &result, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
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

// capturedOrNil keeps empty frame slices out of outcome logs.
func capturedOrNil(frames []Frame) []Frame {
	if len(frames) == 0 {
		return nil
	}
	return frames
}

// logLayerOutcome records one debug line for a first-layer decision.
func (w *worker) logLayerOutcome(operation uint64, decision, reason string, result *LayerResult, state string, layer int, hasLayer bool, frames []Frame, durationMs int64) {
	attrs := []any{"serial", w.printer.Serial, "operation", operation, "decision", decision, "reason", reason, "cutoff", w.s.settings.FirstLayer.PauseConfidence, "gcode_state", state}
	if result != nil {
		attrs = append(attrs, "p_assessable", result.PAssessable, "p_tangled", result.PTangled, "p_detached", result.PDetached, "p_nozzle_blob", result.PNozzleBlob, "p_incomplete", result.PIncomplete)
	}
	if hasLayer {
		attrs = append(attrs, "layer", layer)
	}
	for i, frame := range frames {
		suffix := "_1"
		if i > 0 {
			suffix = "_2"
		}
		attrs = append(attrs, "frame_seq"+suffix, frame.Seq, "frame_age_ms"+suffix, w.s.now().Sub(frame.Captured).Milliseconds())
	}
	attrs = append(attrs, "duration_ms", durationMs)
	w.s.log.Debug("First-layer check outcome", attrs...)
}

// logLayerPause records the first-layer pause command result.
func (w *worker) logLayerPause(operation uint64, result *LayerResult, state string, layer int, hasLayer, failed bool, pendingLayer int) {
	attrs := []any{"serial", w.printer.Serial, "operation", operation, "decision", "pause", "error", failed, "pending_layer", pendingLayer}
	if result != nil {
		attrs = append(attrs, "p_assessable", result.PAssessable, "p_tangled", result.PTangled, "p_detached", result.PDetached, "p_nozzle_blob", result.PNozzleBlob, "p_incomplete", result.PIncomplete)
	}
	attrs = append(attrs, "cutoff", w.s.settings.FirstLayer.PauseConfidence, "gcode_state", state)
	if hasLayer {
		attrs = append(attrs, "layer", layer)
	}
	w.s.log.Debug("First-layer check pause command sent", attrs...)
}
