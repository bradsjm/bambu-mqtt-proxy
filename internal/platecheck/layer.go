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

// layerRetryDelay is the minimum wait before the one retry of a low-clarity first-layer result.
const layerRetryDelay = 15 * time.Second

// layerLiveReason returns the cataloged reason a first-layer check must not run on the live printer state, or empty.
// A retry also requires layer 2 so that it never leaks into a later layer.
func layerLiveReason(online bool, state string, layer int, hasLayer, retry bool) string {
	switch {
	case retry && !online:
		return "printer_offline"
	case !online || state != "RUNNING":
		return "print_paused"
	case retry && !hasLayer:
		return "layer_not_reported"
	case retry && layer != 2:
		return "print_past_start"
	}
	return ""
}

// checkLayer evaluates two camera images and sends one pause for a severe defect.
// A retry keeps the earlier result visible until a valid replacement is published.
func (w *worker) checkLayer(parent context.Context, retry bool) {
	ctx, cancel := context.WithTimeout(context.WithValue(parent, operationKey{}, operations.Add(1)), 25*time.Second)
	defer cancel()
	operation := operationOf(ctx)
	started := time.Now()
	doc, online, _ := w.s.state.Get(w.printer.Serial)
	state, _ := doc.GcodeState()
	layer, hasLayer := doc.Layer()
	if reason := layerLiveReason(online, state, layer, hasLayer, retry); reason != "" {
		w.layerCheckSkip("skipped", reason, operation, retry)
		w.logLayerOutcome(operation, "skipped", reason, nil, state, layer, hasLayer, nil, time.Since(started).Milliseconds())
		return
	}
	if !retry {
		w.setLayerStatus("checking", "", nil)
	}
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
			w.layerCheckSkip("error", reason, operation, retry)
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
		w.layerCheckSkip("error", reason, operation, retry)
		w.logLayerOutcome(operation, "error", reason, nil, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
		return
	}
	decision := classifyLayer(result, w.s.settings.FirstLayer.PauseConfidence, w.s.settings.MinClarity)
	// limited is true when image clarity is below the minimum, so findings describe visible areas only.
	limited := result.PAssessable < w.s.settings.MinClarity
	if decision != "pause" {
		// A pause verdict is published only after its live-state guards pass.
		w.setLayerStatus(decision, "", &result)
		if !retry && limited && hasLayer && layer == 2 {
			// The retry runs on the first eligible layer 2 report after the delay and is never repeated.
			w.layerRetryAt = w.s.now().Add(layerRetryDelay)
		}
	}
	switch decision {
	case "inconclusive":
		w.layerEvent("platecheck_layer1_skipped", activity.Info, fmt.Sprintf("First-layer check could not assess the print reliably: image clarity %s, needs %s. No defect score exceeds 50%%. Printing continued.", scoreText(result.PAssessable, w.s.settings.MinClarity), wholeText(w.s.settings.MinClarity)), operation)
		w.logLayerOutcome(operation, decision, "image_unclear", &result, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
	case "warning":
		// message is the existing warning text for assessable images and the limited-view text otherwise.
		message := "Possible first-layer problem: " + layerIssues(result, .5, true) + ", continuing print"
		if limited {
			message = fmt.Sprintf("Possible first-layer problem in a limited view: %s. Image clarity %s, needs %s. No pause sent. Check the printer.", layerIssues(result, .5, true), scoreText(result.PAssessable, w.s.settings.MinClarity), wholeText(w.s.settings.MinClarity))
		}
		w.layerEvent("platecheck_layer1_warning", activity.Warning, message, operation)
		w.logLayerOutcome(operation, decision, "", &result, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
	case "passed":
		w.layerEvent("platecheck_layer1_passed", activity.Info, "First-layer check passed, continuing print", operation)
		w.logLayerOutcome(operation, decision, "", &result, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
	case "pause":
		doc, online, _ = w.s.state.Get(w.printer.Serial)
		state, _ = doc.GcodeState()
		layer, hasLayer = doc.Layer()
		if reason := layerLiveReason(online, state, layer, hasLayer, retry); reason != "" {
			w.layerCheckSkip("skipped", reason, operation, retry)
			w.logLayerOutcome(operation, "skipped", reason, nil, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
			return
		}
		if ctx.Err() != nil {
			reason := errorCategory(ctx.Err())
			w.layerCheckSkip("error", reason, operation, retry)
			w.logLayerOutcome(operation, "error", reason, nil, state, layer, hasLayer, captured, time.Since(started).Milliseconds())
			return
		}
		w.setLayerStatus(decision, "", &result)
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
			issues = append(issues, fmt.Sprintf("%s (%s)", defect.label, scoreText(defect.score, cutoff)))
		}
	}
	if includeIncomplete && r.PIncomplete > cutoff {
		issues = append(issues, fmt.Sprintf("incomplete layer (%s, warning only)", scoreText(r.PIncomplete, cutoff)))
	}
	return strings.Join(issues, ", ")
}

// setLayerStatus replaces only first-layer state and stores copies of all scores.
// Every other status change clears the reason; layerSkip sets it, and a failed
// retry sets it on a retained warning or inconclusive result.
func (w *worker) setLayerStatus(state, pause string, r *LayerResult) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.setLayerStatusLocked(state, pause, r, "")
}

// setLayerStatusLocked publishes the first-layer status and reason. The caller holds w.mu.
func (w *worker) setLayerStatusLocked(state, pause string, r *LayerResult, reason string) {
	v := FirstLayerState{State: state, Pause: pause, Cutoff: w.s.settings.FirstLayer.PauseConfidence, MinClarity: w.s.settings.MinClarity, Reason: reason}
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

// layerCheckSkip records a first-layer skip or failure for the first attempt or the retry.
// The first attempt replaces the status through layerSkip. A retry keeps the earlier
// outcome and scores and records only why it could not complete.
func (w *worker) layerCheckSkip(state, reason string, operation uint64, retry bool) {
	if !retry {
		w.layerSkip(state, reason, operation)
		return
	}
	w.mu.Lock()
	w.status.FirstLayer.Reason = reason
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
	attrs := []any{"serial", w.printer.Serial, "operation", operation, "decision", decision, "reason", reason, "cutoff", w.s.settings.FirstLayer.PauseConfidence, "min_clarity", w.s.settings.MinClarity, "gcode_state", state}
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
	// clarity names the image clarity in saved debug file names.
	var clarity *float64
	if result != nil {
		clarity = &result.PAssessable
	}
	if paths := w.saveDebugImages("layer1", operation, frames, clarity); paths != nil {
		attrs = append(attrs, "image_files", paths)
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
