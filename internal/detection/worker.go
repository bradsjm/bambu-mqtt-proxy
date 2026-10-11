package detection

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/printer"
)

// worker serializes camera and Gadget requests for one printer.
type worker struct {
	// e supplies shared dependencies.
	e *Engine
	// serial identifies the printer.
	serial string
	// eligible marks supported camera models.
	eligible bool
	// wake coalesces report and toggle wakeups.
	wake chan struct{}
	// outcomes preserves resolved pause outcomes without blocking reports.
	outcomes chan string
	// commandMu serializes toggles with command dispatch.
	commandMu sync.Mutex
	// mu protects report state and the display projection.
	mu sync.Mutex
	// doc is the latest authoritative printer document.
	doc printer.Doc
	// online is the current printer link state.
	online bool
	// counter identifies the module's current print.
	counter uint64
	// disabled stores the user's per-print toggle.
	disabled bool
	// latched permits only one pause attempt per print.
	latched bool
	// pauseLayer is the reported layer at command dispatch.
	pauseLayer *int
	// lightDone permits one auto-light attempt per enabled print.
	lightDone bool
	// cancel interrupts a toggle-disabled camera or API wait.
	cancel context.CancelFunc
	// st contains the last inspection status.
	st Status
	// resultAt is the display-only last analysis time.
	resultAt time.Time
	// due is the next recommended inspection time.
	due time.Time
	// degraded tracks an interruption episode.
	degraded bool
}

// apply processes a report under mu without sending a command.
func (w *worker) apply(ev printer.Event) {
	if reason := sessionResetReason(ev); reason != "" {
		w.counter++
		w.disabled, w.latched, w.lightDone = false, false, false
		w.pauseLayer = nil
		w.st = Status{State: StateMonitoring, PauseState: PauseNone, Enabled: true}
		w.due, w.resultAt = time.Time{}, time.Time{}
		w.degraded = false
		state, _ := ev.Cur.GcodeState()
		w.e.log.Debug("detection session reset", "serial", w.serial, "reason", reason, "counter", w.counter, "gcode_state", state)
	}
	w.doc, w.online = ev.Cur, ev.Online
	if !active(w.doc) {
		w.disabled = false
	}
	if w.st.PauseState != PausePending {
		return
	}
	state, _ := w.doc.GcodeState()
	kind := ""
	switch {
	case state == "PAUSE" || state == "PAUSED" || !active(w.doc):
		w.st.PauseState = PauseConfirmed
		w.st.Message = "printer confirmed the AI pause"
		kind = "ai_pause_confirmed"
	case state == "RUNNING" && w.pauseLayer != nil:
		if layer, ok := w.doc.Layer(); ok && layer > *w.pauseLayer {
			w.st.PauseState = PauseUnconfirmed
			w.st.Reason, w.st.Message = ReasonPauseUnconfirmed, "printer kept printing after the AI pause command"
			kind = "ai_pause_unconfirmed"
		}
	}
	if kind != "" {
		select {
		case w.outcomes <- kind:
		default:
			w.e.log.Warn("detection outcome queue full; dropping outcome", "serial", w.serial, "kind", kind)
		}
	}
}

// sessionResetReason reports why an event starts a new detection session.
// Attaching to a print that is still preparing also starts one, because its
// later RUNNING report is not a new job and would otherwise leave no session.
func sessionResetReason(ev printer.Event) string {
	state, _ := ev.Cur.GcodeState()
	switch {
	case ev.NewJob:
		return "new_job"
	case slices.Contains(ev.Kinds, "state_initial") && (active(ev.Cur) || state == "PREPARE" || state == "SLICING"):
		return "state_initial"
	default:
		return ""
	}
}

// nudge wakes a worker without waiting for its network work.
func (w *worker) nudge() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// view returns a concurrent-safe display projection.
func (w *worker) view() *Status {
	w.mu.Lock()
	s := w.st
	if active(w.doc) && w.counter != 0 {
		s.SessionID = strconv.FormatUint(w.counter, 10)
	}
	if !w.resultAt.IsZero() {
		s.AgeSeconds = time.Since(w.resultAt).Seconds()
	}
	if !w.due.IsZero() && active(w.doc) {
		s.NextCheckSeconds = max(0, time.Until(w.due).Seconds())
	}
	s.Enabled = !w.disabled
	_, hasState := w.doc.GcodeState()
	switch {
	case w.e.blocked != "":
		s.State, s.Reason, s.Message = StateBlocked, w.e.blocked, "detection requires the camera feature"
	case !w.eligible:
		s.State, s.Reason, s.Message = StateUnsupported, ReasonCameraUnsupported, "printer model does not support camera frames"
	case w.disabled:
		s.State, s.Reason, s.DisabledUntil = StateDisabled, ReasonUserDisabled, DisabledUntilPrintEnd
		s.Processing, s.CameraLost, s.NextCheckSeconds = false, false, 0
		s.Message = "AI inspections are off for this print; they return with the next print or after a proxy restart"
	case !hasState:
		s.State, s.Reason = StateStarting, ReasonAwaitingReports
	case !active(w.doc):
		s.State, s.Reason = StateIdle, ""
		s.SessionID = ""
	default:
		state, _ := w.doc.GcodeState()
		if state == "PAUSE" || state == "PAUSED" {
			s.State = StatePaused
		}
	}
	w.mu.Unlock()
	if suspended, message := w.e.AccountSuspended(); suspended {
		s.Suspended, s.Message, s.NextCheckSeconds = true, message, 0
	}
	return &s
}

// run inspects only RUNNING reports and follows Gadget's recommendation.
func (w *worker) run(ctx context.Context) {
	var gadget Session
	var contextCounter uint64
	var lastSeq uint64
	var held bool
	defer func() {
		if held {
			w.e.frames.Release(w.serial)
		}
	}()
	for {
		for len(w.outcomes) > 0 {
			kind := <-w.outcomes
			severity, message := activity.Info, "Printer confirmed the AI pause"
			if kind == "ai_pause_unconfirmed" {
				severity, message = activity.Warning, "Printer kept printing after the AI pause command"
			}
			w.e.activity.Record(w.serial, kind, severity, message)
		}
		if ctx.Err() != nil {
			return
		}
		suspended, _ := w.e.AccountSuspended()
		w.mu.Lock()
		state, _ := w.doc.GcodeState()
		allowed := w.e.blocked == "" && w.eligible && !suspended && !w.disabled && w.online && state == "RUNNING" && w.counter != 0 && w.st.Reason != ReasonRequestInvalid
		counter, doc, due := w.counter, w.doc, w.due
		w.mu.Unlock()
		if !allowed {
			if held {
				w.e.frames.Release(w.serial)
				held = false
			}
			select {
			case <-ctx.Done():
				return
			case <-w.wake:
				continue
			}
		}
		if left := time.Until(due); left > 0 {
			timer := time.NewTimer(left)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-w.wake:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		if contextCounter != counter {
			gadget = Session{}
			contextCounter = counter
			lastSeq = 0
		}
		workCtx, cancel := context.WithCancel(ctx)
		w.mu.Lock()
		w.cancel = cancel
		w.mu.Unlock()
		if !held {
			held = w.e.frames.Acquire(w.serial)
		}
		if !held {
			cancel()
			w.fail(counter, ReasonCameraLost, "camera frames are unavailable", nil, 0)
			continue
		}
		w.autoLight(counter)
		if gadget.ContextID == "" {
			session, err := w.e.client.CreateContext(workCtx)
			if err != nil {
				cancel()
				w.fail(counter, ReasonAPIRetrying, "Gadget context creation failed", err, 0)
				continue
			}
			gadget = session
		}
		frame, ok := w.e.frames.WaitFrame(w.serial, workCtx, lastSeq, 15*time.Second)
		if !ok {
			cancel()
			w.fail(counter, ReasonCameraLost, "camera frames are unavailable", nil, 0)
			continue
		}
		lastSeq = frame.Seq
		w.mu.Lock()
		w.st.Processing = true
		fallback := w.st.UsingFallback
		w.mu.Unlock()
		url := gadget.ProcessURL
		if fallback && gadget.FallbackURL != "" {
			url = gadget.FallbackURL
		}
		started := time.Now()
		res, err := w.e.client.Process(workCtx, url, frame.JPEG)
		cancel()
		w.e.log.Debug("detection inspection completed", "serial", w.serial, "counter", counter, "frame_seq", frame.Seq, "duration_ms", time.Since(started).Milliseconds(), "failed", err != nil)
		if err != nil {
			w.fail(counter, ReasonAPIRetrying, "Gadget inspection failed", err, res.Intervals.Recommended)
			continue
		}
		w.accept(counter, doc, res)
	}
}

// fail reports fail-open degradation and schedules a bounded dependency retry.
func (w *worker) fail(counter uint64, reason, message string, err error, recommended int) {
	w.mu.Lock()
	if counter != w.counter || w.disabled {
		w.mu.Unlock()
		return
	}
	w.st.Processing = false
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if apiErr.AccountTerminal() {
			w.mu.Unlock()
			w.e.suspend(apiErr.Error())
			return
		}
		if apiErr.WorkerTerminal() {
			reason = ReasonRequestInvalid
		}
		if apiErr.SwitchToFallback() {
			w.st.UsingFallback = true
		}
	}
	first := !w.degraded
	w.degraded = true
	w.st.State, w.st.Reason, w.st.Message = StateDegraded, reason, message
	w.st.CameraLost = reason == ReasonCameraLost
	delay := 30 * time.Second
	if recommended > 0 {
		delay = time.Duration(recommended) * time.Second
	}
	w.due = time.Now().Add(delay)
	w.mu.Unlock()
	attrs := []any{"serial", w.serial, "counter", counter, "reason", reason, "retry_in_ms", delay.Milliseconds()}
	attrs = append(attrs, safeErrorAttrs(err)...)
	w.e.log.Debug("detection inspection failed", attrs...)
	if first {
		w.e.activity.Record(w.serial, "ai_degraded", activity.Warning, "AI inspection interrupted, "+message)
		w.e.log.Warn("AI inspection interrupted", "serial", w.serial, "reason", reason)
	}
}

// safeErrorAttrs renders only error detail the client tests prove safe.
func safeErrorAttrs(err error) []any {
	if err == nil {
		return nil
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return []any{"error_type", apiErr.Type, "http_status", apiErr.Status}
	}
	var transportErr *TransportError
	if errors.As(err, &transportErr) {
		return []any{"error_class", transportErr.Error()}
	}
	var validationErr *responseValidationError
	if errors.As(err, &validationErr) {
		return []any{"error_class", validationErr.Error()}
	}
	var requestErr *localRequestError
	if errors.As(err, &requestErr) {
		return []any{"error_class", requestErr.Error()}
	}
	var tooLarge *frameTooLargeError
	if errors.As(err, &tooLarge) {
		return []any{"error_class", tooLarge.Error()}
	}
	return []any{"error_class", "unknown"}
}

// accept discards another print's result and pauses at most once per print.
func (w *worker) accept(counter uint64, doc printer.Doc, res Result) {
	w.commandMu.Lock()
	defer w.commandMu.Unlock()
	w.mu.Lock()
	if counter != w.counter || w.disabled || !active(w.doc) {
		w.st.Processing = false
		cause := "inactive"
		switch {
		case counter != w.counter:
			cause = "stale_counter"
		case w.disabled:
			cause = "disabled"
		}
		w.mu.Unlock()
		w.e.log.Debug("detection result discarded", "serial", w.serial, "counter", counter, "cause", cause)
		return
	}
	recovered, warning := w.degraded, !w.st.Warning && res.WarningSuggested
	w.degraded = false
	w.st.Processing, w.st.CameraLost = false, false
	w.st.State, w.st.Reason, w.st.Message = StateMonitoring, "", ""
	w.st.Quality, w.st.Warning, w.st.FasterInspection = res.PrintQuality, res.WarningSuggested, res.FasterInspectionSuggested
	if res.WarningSuggested {
		w.st.State = StateAttention
	}
	attrs := []any{"serial", w.serial, "counter", counter, "print_quality", res.PrintQuality, "warning_suggested", res.WarningSuggested, "pause_suggested", res.PauseSuggested, "faster_inspection_suggested", res.FasterInspectionSuggested, "recommended_interval_s", res.Intervals.Recommended}
	if layer, ok := doc.Layer(); ok {
		w.st.LastInspectedLayer = new(layer)
		attrs = append(attrs, "layer", layer)
	}
	w.resultAt = time.Now()
	w.due = w.resultAt.Add(time.Duration(res.Intervals.Recommended) * time.Second)
	state, _ := w.doc.GcodeState()
	pause := res.PauseSuggested && !w.latched && state == "RUNNING" && w.online
	if pause {
		w.latched = true
		if layer, ok := w.doc.Layer(); ok {
			w.pauseLayer = new(layer)
		}
		w.st.PauseState, w.st.State, w.st.Message = PausePending, StateAttention, "pause sent; waiting for printer confirmation"
	}
	attrs = append(attrs, "pause_sent", pause)
	w.mu.Unlock()
	w.e.log.Debug("detection result accepted", attrs...)
	if recovered {
		w.e.activity.Record(w.serial, "ai_recovered", activity.Info, "AI inspection recovered")
	}
	if warning {
		w.e.activity.Record(w.serial, "ai_warning", activity.Warning, "AI inspection found a possible print failure")
	}
	if pause {
		w.e.activity.Record(w.serial, "ai_pause_sent", activity.Warning, "AI inspection requested a printer pause")
		if err := w.e.control.Pause(w.serial); err != nil {
			w.mu.Lock()
			if counter == w.counter && w.st.PauseState == PausePending {
				w.st.PauseState, w.st.Reason, w.st.Message = PauseUnconfirmed, ReasonPauseFailed, "pause command failed"
			}
			w.mu.Unlock()
			w.e.activity.Record(w.serial, "ai_pause_unconfirmed", activity.Warning, "AI pause command could not be sent")
			w.e.log.Warn("AI pause command failed", "serial", w.serial)
		} else {
			w.e.log.Debug("detection pause sent", "serial", w.serial, "counter", counter)
		}
	}
}

// autoLight enables the chamber light before capturing the inspection frame.
func (w *worker) autoLight(counter uint64) {
	w.commandMu.Lock()
	defer w.commandMu.Unlock()
	w.mu.Lock()
	state, _ := w.doc.GcodeState()
	send := counter == w.counter && !w.disabled && !w.lightDone && w.online && state == "RUNNING"
	if send {
		w.lightDone = true
		mode, _ := w.doc.ChamberLight()
		send = mode != "on"
	}
	w.mu.Unlock()
	lightErr := false
	if send {
		if err := w.e.control.Light(w.serial, true); err == nil {
			w.e.activity.Record(w.serial, "ai_light_on", activity.Info, "Chamber light turned on for AI inspection")
		} else {
			lightErr = true
		}
	}
	w.e.log.Debug("detection auto light", "serial", w.serial, "counter", counter, "light_sent", send, "light_error", lightErr)
}
