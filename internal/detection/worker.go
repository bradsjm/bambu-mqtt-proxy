package detection

import (
	"context"
	"errors"
	"fmt"
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
	// held stores the user's idle toggle off that survives prints until re-enabled.
	held bool
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
	// troubleSince is when the open interruption began; zero when none is open.
	troubleSince time.Time
	// failures counts failed checks in the open interruption.
	failures int
	// successes counts consecutive successful checks since the last failure.
	successes int
	// lastCause is the plain-language cause of the latest failure.
	lastCause string
}

// recoverAfter is the number of consecutive successful checks that ends an
// interruption. One success between failures is not a recovery.
const recoverAfter = 3

// apply processes a report under mu without sending a command.
func (w *worker) apply(ev printer.Event) {
	if reason := sessionResetReason(ev); reason != "" {
		w.counter++
		w.disabled, w.latched, w.lightDone = false, false, false
		w.pauseLayer = nil
		w.st = Status{State: StateMonitoring, PauseState: PauseNone, Enabled: !w.held}
		w.due, w.resultAt = time.Time{}, time.Time{}
		w.troubleSince, w.failures, w.successes, w.lastCause = time.Time{}, 0, 0, ""
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
	s.Enabled = !w.disabled && !w.held
	_, hasState := w.doc.GcodeState()
	switch {
	case w.e.blocked != "":
		s.State, s.Reason, s.Message = StateBlocked, w.e.blocked, "detection requires the camera feature"
	case !w.eligible:
		s.State, s.Reason, s.Message = StateUnsupported, ReasonCameraUnsupported, "printer model does not support camera frames"
	case w.held:
		s.State, s.Reason, s.DisabledUntil = StateDisabled, ReasonUserDisabled, DisabledUntilUserEnables
		s.Processing, s.CameraLost, s.NextCheckSeconds = false, false, 0
		s.Message = "AI inspections are off until turned back on; a proxy restart also turns them back on"
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
		allowed := w.e.blocked == "" && w.eligible && !suspended && !w.disabled && !w.held && w.online && state == "RUNNING" && w.counter != 0 && w.st.Reason != ReasonRequestInvalid
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
			w.fail(counter, ReasonCameraLost, "the camera stream is not available", nil, 0)
			continue
		}
		w.autoLight(counter)
		if gadget.ContextID == "" {
			session, err := w.e.client.CreateContext(workCtx)
			if err != nil {
				cancel()
				w.fail(counter, ReasonAPIRetrying, "could not start a Gadget session: "+describe(err), err, 0)
				continue
			}
			gadget = session
		}
		frame, ok := w.e.frames.WaitFrame(w.serial, workCtx, lastSeq, 15*time.Second)
		if !ok {
			cancel()
			w.fail(counter, ReasonCameraLost, "no new camera frame arrived within 15 s", nil, 0)
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
			w.fail(counter, ReasonAPIRetrying, describe(err), err, res.Intervals.Recommended)
			continue
		}
		w.accept(counter, doc, res)
	}
}

// fail records one failed check, keeps the print running, and schedules a
// retry. Repeated failures extend one interruption instead of starting new
// ones; cause is the plain-language explanation shown to users.
func (w *worker) fail(counter uint64, reason, cause string, err error, recommended int) {
	w.mu.Lock()
	if counter != w.counter || w.disabled || w.held {
		w.mu.Unlock()
		return
	}
	w.st.Processing = false
	var apiErr *APIError
	switched := false
	if errors.As(err, &apiErr) {
		if apiErr.AccountTerminal() {
			w.mu.Unlock()
			w.e.suspend(apiErr.Reason(), apiErr)
			return
		}
		if apiErr.WorkerTerminal() {
			reason = ReasonRequestInvalid
		}
		if apiErr.SwitchToFallback() && !w.st.UsingFallback {
			w.st.UsingFallback, switched = true, true
		}
	}
	now := time.Now()
	first := w.troubleSince.IsZero()
	if first {
		w.troubleSince, w.failures = now, 0
	}
	w.failures++
	w.successes = 0
	w.lastCause = cause
	delay := 30 * time.Second
	if recommended > 0 {
		delay = time.Duration(recommended) * time.Second
	}
	w.due = now.Add(delay)
	failures, lasted, backup := w.failures, now.Sub(w.troubleSince), w.st.UsingFallback
	terminal := reason == ReasonRequestInvalid
	w.st.State, w.st.Reason = StateDegraded, reason
	w.st.Message = fmt.Sprintf("%s · %s · next try in %s", cause, failedChecks(failures, lasted), seconds(delay))
	if terminal {
		w.st.Message = cause + " · AI checks stopped for this print and resume with the next print"
	}
	w.st.CameraLost = reason == ReasonCameraLost
	w.mu.Unlock()
	attrs := []any{"serial", w.serial, "counter", counter, "cause", cause, "reason", reason, "failure_count", failures, "interruption_s", int(lasted.Seconds()), "backup_server", backup}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	if apiErr != nil && apiErr.Body != "" {
		attrs = append(attrs, "response_body", apiErr.Body)
	}
	if terminal {
		w.e.log.Error("AI inspection stopped for this print", append(attrs, "next_action", "AI checks resume with the next print")...)
		w.e.activity.Record(w.serial, "ai_degraded", activity.Error, "AI checks stopped for this print: "+cause+". The print continues without AI checks; they resume with the next print")
		return
	}
	w.e.log.Warn("AI inspection check failed", append(attrs, "retry_in_ms", delay.Milliseconds())...)
	if first {
		w.e.activity.Record(w.serial, "ai_degraded", activity.Warning, fmt.Sprintf("AI inspection interrupted: %s. Retrying in %s; the print continues without AI checks until they succeed again", cause, seconds(delay)))
	}
	if switched {
		w.e.log.Info("AI inspection switching to the Gadget backup server", "serial", w.serial, "counter", counter, "error", err.Error())
		w.e.activity.Record(w.serial, "ai_backup_server", activity.Info, "AI inspection switched to the Gadget backup server because "+cause)
	}
}

// describe returns the plain-language cause of a Gadget request failure.
func describe(err error) string {
	var te *TransportError
	var ae *APIError
	var ve *responseValidationError
	switch {
	case errors.As(err, &te):
		return te.Reason()
	case errors.As(err, &ae):
		return ae.Reason()
	case errors.As(err, &ve):
		return "Gadget sent an answer the proxy could not use: " + ve.message
	}
	return err.Error()
}

// failedChecks summarizes an interruption, for example "4 failed checks in 3 min".
func failedChecks(n int, lasted time.Duration) string {
	if n == 1 {
		return "1 failed check"
	}
	if lasted < time.Minute {
		return fmt.Sprintf("%d failed checks in %s", n, seconds(lasted))
	}
	return fmt.Sprintf("%d failed checks in %d min", n, int(lasted.Round(time.Minute).Minutes()))
}

// seconds renders a duration as whole seconds, for example "30 s".
func seconds(d time.Duration) string {
	return fmt.Sprintf("%d s", int(d.Round(time.Second).Seconds()))
}

// accept discards another print's result and pauses at most once per print.
func (w *worker) accept(counter uint64, doc printer.Doc, res Result) {
	w.commandMu.Lock()
	defer w.commandMu.Unlock()
	w.mu.Lock()
	if counter != w.counter || w.disabled || w.held || !active(w.doc) {
		w.st.Processing = false
		cause := "inactive"
		switch {
		case counter != w.counter:
			cause = "stale_counter"
		case w.disabled || w.held:
			cause = "disabled"
		}
		w.mu.Unlock()
		w.e.log.Debug("detection result discarded", "serial", w.serial, "counter", counter, "cause", cause)
		return
	}
	warning := !w.st.Warning && res.WarningSuggested
	recovered := false
	failures, lasted, lastCause := w.failures, time.Since(w.troubleSince), w.lastCause
	if !w.troubleSince.IsZero() {
		w.successes++
		if w.successes >= recoverAfter {
			recovered = true
			w.troubleSince, w.failures, w.successes, w.lastCause = time.Time{}, 0, 0, ""
		}
	}
	w.st.Processing, w.st.CameraLost = false, false
	w.st.State, w.st.Reason, w.st.Message = StateMonitoring, "", ""
	if !w.troubleSince.IsZero() {
		w.st.Message = fmt.Sprintf("Gadget is answering again: %d of %d checks in a row succeeded after %s; last failure: %s", w.successes, recoverAfter, failedChecks(w.failures, time.Since(w.troubleSince)), w.lastCause)
	}
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
		w.e.log.Info("AI inspection recovered", "serial", w.serial, "counter", counter, "failure_count", failures, "interruption_s", int(lasted.Seconds()), "last_cause", lastCause)
		w.e.activity.Record(w.serial, "ai_recovered", activity.Info, fmt.Sprintf("AI inspection recovered: %d checks in a row succeeded after %s. Last failure: %s", recoverAfter, failedChecks(failures, lasted), lastCause))
	}
	if warning {
		w.e.activity.Record(w.serial, "ai_warning", activity.Warning, "AI inspection found a possible print failure")
	}
	if pause {
		w.e.activity.Record(w.serial, "ai_pause_sent", activity.Warning, "AI inspection requested a printer pause")
		if err := w.e.control.Pause(w.serial); err != nil {
			w.mu.Lock()
			if counter == w.counter && w.st.PauseState == PausePending {
				w.st.PauseState, w.st.Reason, w.st.Message = PauseUnconfirmed, ReasonPauseFailed, "pause command could not be sent: "+err.Error()+" · not retried; check the printer and pause it manually"
			}
			w.mu.Unlock()
			w.e.activity.Record(w.serial, "ai_pause_unconfirmed", activity.Warning, "AI inspection asked to pause the print, but the pause command could not be sent: "+err.Error()+". The proxy will not retry; check the printer and pause it manually if needed")
			w.e.log.Warn("AI pause command failed", "serial", w.serial, "counter", counter, "error", err.Error(), "next_action", "no retry this print; the operator must check and pause the printer")
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
	send := counter == w.counter && !w.disabled && !w.held && !w.lightDone && w.online && state == "RUNNING"
	if send {
		w.lightDone = true
		mode, _ := w.doc.ChamberLight()
		send = mode != "on"
	}
	w.mu.Unlock()
	var lightErr error
	if send {
		if lightErr = w.e.control.Light(w.serial, true); lightErr == nil {
			w.e.activity.Record(w.serial, "ai_light_on", activity.Info, "Chamber light turned on for AI inspection")
		} else {
			w.e.log.Warn("AI inspection could not turn on the chamber light", "serial", w.serial, "counter", counter, "error", lightErr.Error(), "next_action", "inspection continues with the light unchanged; no retry this print")
		}
	}
	w.e.log.Debug("detection auto light", "serial", w.serial, "counter", counter, "light_sent", send, "light_failed", lightErr != nil)
}
