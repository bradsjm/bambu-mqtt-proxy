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
	// obs is the final pre-dispatch real session observation counter.
	obs uint64
	// dispatched is the local publication boundary, before the command call.
	dispatched time.Time
	// deadline bounds the one permitted confirmation wait.
	deadline time.Time
	// issues is local fixed-label text derived only from validated scores.
	issues string
}

// layerIdentityValid requires fresh current-connection RUNNING evidence in both projections.
func (w *worker) layerIdentityValid(j telemetry.JobView, v telemetry.SessionView, connection uint64) bool {
	age := w.s.now().Sub(v.ObsAt)
	return connection != 0 && w.s.commands.Connected(w.printer.Serial) && j.Active && j.Generation != 0 && j.State == "RUNNING" && v.State == "RUNNING" && v.Active && v.SessionGen != 0 && v.LayerNum != nil && *v.LayerNum >= 2 && !v.ObsAt.IsZero() && age >= 0 && age <= w.s.kFresh
}

// authorizeLayer rejects any identity, running epoch, session, or connection change.
func (w *worker) authorizeLayer(b boundCheck) (telemetry.SessionView, bool) {
	j, v, connection, fresh := w.snapshot()
	w.mu.Lock()
	stopped := w.startupStopped == b.job.Generation
	w.mu.Unlock()
	return v, fresh && !stopped && w.layerIdentityValid(j, v, connection) && j.Generation == b.job.Generation && j.Revision == b.job.Revision && j.RunningEpoch == b.job.RunningEpoch && v.SessionGen == b.session.SessionGen && v.Epoch == b.session.Epoch && connection == b.connection
}

// stepLayer consumes the event once before any network work and expires original admission.
func (w *worker) stepLayer(ctx context.Context) {
	if !w.s.settings.FirstLayer.On() {
		return
	}
	w.mu.Lock()
	hint := w.layerHint
	if hint == nil || hint.job.Generation <= w.layerProcessed {
		w.mu.Unlock()
		return
	}
	w.layerProcessed = hint.job.Generation
	w.layerHint = nil
	w.mu.Unlock()
	b := *hint
	b.operation = operations.Add(1)
	// The work budget starts at the originating evidence time bound at the
	// event and never restarts after queueing.
	if _, ok := w.authorizeLayer(b); !ok || ctx.Err() != nil || w.s.now().Sub(b.admitted) > w.s.kCheck {
		w.layerSkip("skipped", b.operation)
		return
	}
	w.checkLayer(ctx, b)
}

// layerEvidence rereads authorization, then validates the complete budget and both frames.
func (w *worker) layerEvidence(ctx context.Context, b boundCheck, frames []Frame) (telemetry.SessionView, bool) {
	v, valid := w.authorizeLayer(b)
	now := w.s.now()
	age := now.Sub(b.admitted)
	if !valid || ctx.Err() != nil || age < 0 || age > w.s.kCheck {
		return v, false
	}
	for _, frame := range frames {
		age = now.Sub(frame.Captured)
		if frame.Captured.IsZero() || age < 0 || age > 30*time.Second {
			return v, false
		}
	}
	return v, true
}

// checkLayer captures two separated views, validates both, and makes one fail-open decision.
func (w *worker) checkLayer(parent context.Context, b boundCheck) {
	// Queueing and source-observation age consume the same overall work budget.
	remaining := w.s.kCheck - w.s.now().Sub(b.admitted)
	ctx, cancel := context.WithTimeout(context.WithValue(parent, operationKey{}, b.operation), remaining)
	defer cancel()
	w.setLayerStatus("checking", "", nil)
	boundary := w.s.now()
	v, valid := w.authorizeLayer(b)
	if !valid || ctx.Err() != nil {
		w.layerSkip("skipped", b.operation)
		return
	}
	if v.ChamberLight == "off" {
		if _, valid := w.layerEvidence(ctx, b, nil); !valid {
			w.layerSkip("skipped", b.operation)
			return
		}
		if err := w.s.commands.SetChamberLight(w.printer.Serial, b.connection, true); err == nil {
			boundary = w.s.now()
		}
	}
	frames := make([]Frame, 0, 2)
	for range 2 {
		if _, valid := w.layerEvidence(ctx, b, frames); !valid {
			w.layerSkip("skipped", b.operation)
			return
		}
		frame, err := w.s.frames.Capture(ctx, w.printer.Serial, boundary)
		if err != nil {
			w.layerSkip("error", b.operation)
			return
		}
		if !frame.Captured.After(boundary) || (len(frames) != 0 && frame.Seq == frames[0].Seq) {
			w.layerSkip("skipped", b.operation)
			return
		}
		frames = append(frames, frame)
		if _, valid := w.layerEvidence(ctx, b, frames); !valid {
			w.layerSkip("skipped", b.operation)
			return
		}
		boundary = frame.Captured.Add(2 * time.Second)
	}
	result, err := w.s.client.EvaluateFirstLayer(ctx, [][]byte{frames[0].JPEG, frames[1].JPEG}, config.DisplayModel(w.printer.Model, w.printer.Serial))
	if err != nil || !validLayerResult(result) {
		w.layerSkip("error", b.operation)
		return
	}
	if _, valid := w.layerEvidence(ctx, b, frames); !valid {
		w.layerSkip("skipped", b.operation)
		return
	}
	decision := classifyLayer(result, w.s.settings.FirstLayer.PauseConfidence)
	w.setLayerStatus(decision, "", &result)
	w.s.log.InfoContext(ctx, "First-layer check evaluated", "operation", b.operation, "serial", w.printer.Serial, "decision", decision, "cutoff", w.s.settings.FirstLayer.PauseConfidence, "p_assessable", result.PAssessable, "p_tangled", result.PTangled, "p_detached", result.PDetached, "p_nozzle_blob", result.PNozzleBlob, "p_incomplete", result.PIncomplete)
	switch decision {
	case "inconclusive":
		w.layerEvent("platecheck_layer1_skipped", activity.Info, "First-layer view was inconclusive. The print continued.", b.operation)
	case "warning":
		w.layerEvent("platecheck_layer1_warning", activity.Warning, "Possible first-layer defect: "+layerIssues(result, .5, true)+". The print continued; check the printer.", b.operation)
	case "passed":
		w.layerEvent("platecheck_layer1_passed", activity.Info, "First-layer check passed.", b.operation)
	case "pause":
		// Reread all action evidence after evaluation, immediately before publication.
		v, valid = w.layerEvidence(ctx, b, frames)
		if !valid {
			w.layerSkip("skipped", b.operation)
			return
		}
		p := &pendingPause{bound: b, obs: v.Obs, dispatched: w.s.now(), issues: layerIssues(result, w.s.settings.FirstLayer.PauseConfidence, false)}
		p.deadline = p.dispatched.Add(w.s.kConfirm)
		if err := w.s.commands.PausePrint(w.printer.Serial, b.connection); err != nil {
			w.setLayerPause("failed")
			w.layerEvent("platecheck_layer1_pause_failed", activity.Error, "First-layer pause command failed for "+p.issues+". Check the printer now.", b.operation)
			return
		}
		w.layerPending = p
		w.setLayerPause("pending")
		w.layerEvent("platecheck_layer1_pause_sent", activity.Error, "Possible first-layer failure: "+p.issues+". Pause sent but not yet confirmed; check the printer.", b.operation)
	}
}

// confirmPause accepts only the first fresh same-session pause boundary after dispatch.
func (w *worker) confirmPause() {
	p := w.layerPending
	j, v, connection, fresh := w.snapshot()
	same := w.s.commands.Connected(w.printer.Serial) && j.Generation == p.bound.job.Generation && j.Revision == p.bound.job.Revision && connection == p.bound.connection && v.SessionGen == p.bound.session.SessionGen
	age := w.s.now().Sub(v.ObsAt)
	fresh = fresh && !v.ObsAt.IsZero() && age >= 0 && age <= w.s.kFresh
	paused := j.State == "PAUSE" || j.State == "PAUSED"
	pauseBoundary := paused && j.RunningEpoch == p.bound.job.RunningEpoch+1 && v.Epoch == p.bound.session.Epoch+1
	if same && fresh && j.Active && v.Active && pauseBoundary && v.Obs > p.obs && j.ObsAt.After(p.dispatched) && v.ObsAt.After(p.dispatched) && j.ObsAt.Before(p.deadline) && v.ObsAt.Before(p.deadline) && !w.s.now().After(p.deadline) {
		w.setLayerPause("confirmed")
		w.layerEvent("platecheck_layer1_pause_confirmed", activity.Warning, "Printer reports paused after the first-layer pause request. Check the print before resuming.", p.bound.operation)
		w.layerPending = nil
		return
	}
	runningBoundary := j.State == "RUNNING" && j.RunningEpoch == p.bound.job.RunningEpoch && v.Epoch == p.bound.session.Epoch
	if !same || (!runningBoundary && !pauseBoundary) || !w.s.now().Before(p.deadline) {
		w.unconfirmedPause()
	}
}

// unconfirmedPause closes the attempt without implying the printer paused.
func (w *worker) unconfirmedPause() {
	p := w.layerPending
	w.setLayerPause("unconfirmed")
	w.layerEvent("platecheck_layer1_pause_unconfirmed", activity.Error, "First-layer pause is unconfirmed for "+p.issues+". Check the printer now.", p.bound.operation)
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
func (w *worker) setLayerStatus(state, pause string, r *LayerResult) {
	v := FirstLayerState{State: state, Pause: pause, Cutoff: w.s.settings.FirstLayer.PauseConfidence}
	if r != nil {
		v.PAssessable = new(r.PAssessable)
		v.PTangled = new(r.PTangled)
		v.PDetached = new(r.PDetached)
		v.PNozzleBlob = new(r.PNozzleBlob)
		v.PIncomplete = new(r.PIncomplete)
	}
	w.mu.Lock()
	w.status.FirstLayer = v
	w.mu.Unlock()
}

// setLayerPause changes only the pause lifecycle, preserving scores and phase outcome.
func (w *worker) setLayerPause(pause string) {
	w.mu.Lock()
	w.status.FirstLayer.Pause = pause
	w.mu.Unlock()
}

// layerSkip records a fixed fail-open outcome without retaining previous images or scores.
func (w *worker) layerSkip(state string, operation uint64) {
	w.setLayerStatus(state, "", nil)
	w.layerEvent("platecheck_layer1_skipped", activity.Info, "First-layer check unavailable or evidence changed. The print continued.", operation)
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
