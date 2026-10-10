package platecheck

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// pendingPause retains evidence, never images, for one non-replayed publication.
type pendingPause struct {
	// bound retains only scalar evidence for the original job and session.
	bound boundCheck
	// dispatched is the local publication boundary, before the command call.
	dispatched time.Time
	// deadline bounds the one permitted confirmation wait.
	deadline time.Time
	// issues is local fixed-label text derived only from validated scores.
	issues string
}

// authorizeLayer rereads the current print and reports whether the bound
// first-layer check may proceed. It returns the coherent session view plus
// a failure reason, or an empty reason when authorized.
func (w *worker) authorizeLayer(b boundCheck) (telemetry.SessionView, string) {
	j, v, ok := w.s.state.Print(w.printer.Serial)
	if !ok || j.Generation != b.generation || !j.Active {
		return v, "print_ended"
	}
	if !w.s.commands.Online(w.printer.Serial) {
		return v, "printer_offline"
	}
	if j.State != "RUNNING" {
		return v, "print_paused"
	}
	layer := v.CurrentLayer()
	if layer == nil {
		return v, "layer_not_reported"
	}
	if *layer < 2 {
		return v, "first_layer_not_complete"
	}
	return v, ""
}

// stepLayer admits one queued first-layer hint without inventing events.
func (w *worker) stepLayer(ctx context.Context) {
	if !w.s.settings.FirstLayer.On() {
		return
	}
	w.mu.Lock()
	h := w.layerHint
	if h == nil || h.generation <= w.layerProcessed {
		w.mu.Unlock()
		return
	}
	now := w.s.now()
	j, v, ok := w.s.state.Print(w.printer.Serial)
	if !ok {
		w.mu.Unlock()
		return
	}
	layer := v.CurrentLayer()
	switch {
	case j.Generation != h.generation || !j.Active:
		w.consumeLayerLocked(h.generation)
		w.mu.Unlock()
		w.layerSkip("skipped", "print_ended", 0)
		return
	case w.startupStopped == j.Generation:
		w.consumeLayerLocked(h.generation)
		w.mu.Unlock()
		w.layerSkip("skipped", "startup_stopped", 0)
		return
	case j.State == "PAUSE" || j.State == "PAUSED" || j.State == "PREPARE" || j.State == "SLICING":
		h.waitReason = ""
		w.mu.Unlock()
		return
	case !w.s.commands.Online(w.printer.Serial):
		if w.waitExpired("printer_offline", now, &h.waitReason, &h.waitSince) {
			w.consumeLayerLocked(h.generation)
			w.mu.Unlock()
			w.layerSkip("skipped", "printer_offline", 0)
			return
		}
		w.mu.Unlock()
		return
	case layer == nil:
		if w.waitExpired("layer_not_reported", now, &h.waitReason, &h.waitSince) {
			w.consumeLayerLocked(h.generation)
			w.mu.Unlock()
			w.layerSkip("skipped", "layer_not_reported", 0)
			return
		}
		w.mu.Unlock()
		return
	case *layer < 2:
		w.consumeLayerLocked(h.generation)
		w.mu.Unlock()
		w.layerSkip("skipped", "first_layer_not_complete", 0)
		return
	default:
		w.consumeLayerLocked(h.generation)
		w.mu.Unlock()
		w.checkLayer(ctx, boundCheck{generation: h.generation, admitted: now, operation: operations.Add(1)})
		return
	}
}

// consumeLayerLocked marks a first-layer hint consumed. The caller holds w.mu.
func (w *worker) consumeLayerLocked(generation uint64) {
	if generation <= w.layerProcessed {
		return
	}
	w.layerProcessed = generation
	if w.layerHint != nil && w.layerHint.generation == generation {
		w.layerHint = nil
	}
}

// layerEvidence rereads authorization, then validates the complete budget and both frames.
func (w *worker) layerEvidence(ctx context.Context, b boundCheck, frames []Frame) (telemetry.SessionView, string) {
	v, reason := w.authorizeLayer(b)
	if reason != "" {
		return v, reason
	}
	if w.s.now().Sub(b.admitted) > w.s.kCheck || ctx.Err() == context.DeadlineExceeded {
		return v, "check_timeout"
	}
	if ctx.Err() == context.Canceled {
		return v, "canceled"
	}
	for _, frame := range frames {
		age := w.s.now().Sub(frame.Captured)
		if frame.Captured.IsZero() || age < 0 || age > 30*time.Second {
			return v, "capture_timeout"
		}
	}
	return v, ""
}

// checkLayer captures two separated views, validates both, and makes one fail-open decision.
func (w *worker) checkLayer(parent context.Context, b boundCheck) {
	// Queueing and source-observation age consume the same overall work budget.
	remaining := w.s.kCheck - w.s.now().Sub(b.admitted)
	ctx, cancel := context.WithTimeout(context.WithValue(parent, operationKey{}, b.operation), remaining)
	defer cancel()
	w.setLayerStatus("checking", "", nil)
	boundary := w.s.now()
	if _, reason := w.authorizeLayer(b); reason != "" {
		w.layerSkip("skipped", reason, b.operation)
		return
	} else if ctx.Err() != nil {
		w.layerSkip("skipped", w.layerBudgetReason(ctx), b.operation)
		return
	}
	if v, reason := w.authorizeLayer(b); v.ChamberLight == "off" {
		if reason != "" {
			w.layerSkip("skipped", reason, b.operation)
			return
		}
		if err := w.s.commands.Light(w.printer.Serial, true); err == nil {
			boundary = w.s.now()
		}
	}
	frames := make([]Frame, 0, 2)
	for range 2 {
		if _, reason := w.layerEvidence(ctx, b, frames); reason != "" {
			w.layerSkip("skipped", reason, b.operation)
			return
		}
		frame, err := w.s.frames.Capture(ctx, w.printer.Serial, boundary)
		if err != nil {
			w.layerSkip("error", errorCategory(err), b.operation)
			return
		}
		if !frame.Captured.After(boundary) || (len(frames) != 0 && frame.Seq == frames[0].Seq) {
			w.layerSkip("skipped", "capture_timeout", b.operation)
			return
		}
		frames = append(frames, frame)
		if _, reason := w.layerEvidence(ctx, b, frames); reason != "" {
			w.layerSkip("skipped", reason, b.operation)
			return
		}
		boundary = frame.Captured.Add(2 * time.Second)
	}
	result, err := w.s.client.EvaluateFirstLayer(ctx, [][]byte{frames[0].JPEG, frames[1].JPEG}, config.DisplayModel(w.printer.Model, w.printer.Serial))
	if err != nil {
		w.layerSkip("error", errorCategory(err), b.operation)
		return
	}
	if !validLayerResult(result) {
		w.layerSkip("error", "bad_response", b.operation)
		return
	}
	if _, reason := w.layerEvidence(ctx, b, frames); reason != "" {
		w.layerSkip("skipped", reason, b.operation)
		return
	}
	decision := classifyLayer(result, w.s.settings.FirstLayer.PauseConfidence)
	w.setLayerStatus(decision, "", &result)
	w.s.log.InfoContext(ctx, "First-layer check evaluated", "operation", b.operation, "serial", w.printer.Serial, "decision", decision, "cutoff", w.s.settings.FirstLayer.PauseConfidence, "p_assessable", result.PAssessable, "p_tangled", result.PTangled, "p_detached", result.PDetached, "p_nozzle_blob", result.PNozzleBlob, "p_incomplete", result.PIncomplete)
	switch decision {
	case "inconclusive":
		w.layerEvent("platecheck_layer1_skipped", activity.Info, "First-layer view inconclusive, continuing print", b.operation)
	case "warning":
		w.layerEvent("platecheck_layer1_warning", activity.Warning, "Possible first-layer problem: "+layerIssues(result, .5, true)+", continuing print", b.operation)
	case "passed":
		w.layerEvent("platecheck_layer1_passed", activity.Info, "First-layer check passed, continuing print", b.operation)
	case "pause":
		// Reread all action evidence after evaluation, immediately before publication.
		if _, reason := w.layerEvidence(ctx, b, frames); reason != "" {
			w.layerSkip("skipped", reason, b.operation)
			return
		}
		p := &pendingPause{bound: b, dispatched: w.s.now(), issues: layerIssues(result, w.s.settings.FirstLayer.PauseConfidence, false)}
		p.deadline = p.dispatched.Add(w.s.kConfirm)
		_, reason := w.authorizeLayer(b)
		if reason != "" {
			w.layerSkip("skipped", reason, b.operation)
			return
		}
		if err := w.s.commands.Pause(w.printer.Serial); err != nil {
			w.setLayerPause("failed")
			w.layerEvent("platecheck_layer1_pause_failed", activity.Error, "First-layer pause command failed: "+p.issues+", check the printer", b.operation)
			return
		}
		w.layerPending = p
		w.setLayerPause("pending")
		w.layerEvent("platecheck_layer1_pause_sent", activity.Error, "Possible first-layer failure: "+p.issues+", pause sent", b.operation)
	}
}

// layerBudgetReason maps a failed first-layer budget recheck to its skip
// code: Close cancellation gives "canceled", and any other failure gives
// "check_timeout".
func (w *worker) layerBudgetReason(ctx context.Context) string {
	if ctx.Err() == context.Canceled {
		return "canceled"
	}
	return "check_timeout"
}

// confirmPause accepts a same-job pause report inside the deadline. The
// state read sits between two deadline checks: the first avoids work
// after expiry, and the second rejects a report processed after the
// deadline, so a late pause cannot confirm.
func (w *worker) confirmPause() {
	p := w.layerPending
	now := w.s.now()
	if !now.Before(p.deadline) {
		w.unconfirmedPause()
		return
	}
	j, _, ok := w.s.state.Print(w.printer.Serial)
	if !ok {
		return
	}
	if now = w.s.now(); !now.Before(p.deadline) {
		w.unconfirmedPause()
		return
	}
	if j.Generation != p.bound.generation || !j.Active {
		w.unconfirmedPause()
		return
	}
	if j.State == "PAUSE" || j.State == "PAUSED" {
		w.setLayerPause("confirmed")
		w.layerEvent("platecheck_layer1_pause_confirmed", activity.Warning, "Printer reports paused after first-layer pause request, check the first layer before resuming", p.bound.operation)
		w.layerPending = nil
	}
}

// unconfirmedPause closes the attempt without implying the printer paused.
func (w *worker) unconfirmedPause() {
	p := w.layerPending
	w.setLayerPause("unconfirmed")
	w.layerEvent("platecheck_layer1_pause_unconfirmed", activity.Error, "First-layer pause not confirmed, check the printer", p.bound.operation)
	w.layerPending = nil
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
