package platecheck

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// ReasonCameraDisabled explains why automatic protection is blocked.
const ReasonCameraDisabled = "camera_disabled"

// StateSource supplies the current job and session views in one coherent read.
type StateSource interface {
	// Print returns the preview-job and session projections for one serial.
	Print(string) (telemetry.JobView, telemetry.SessionView, bool)
}

// PrinterCommands supplies connection status and guarded light, stop, and pause commands.
type PrinterCommands interface {
	// Connected reports whether the upstream MQTT connection is established.
	Connected(string) bool
	// Generation returns the current upstream connection generation.
	Generation(string) uint64
	// StopPrint sends a generation-guarded QoS 0 stop request.
	StopPrint(string, uint64) error
	// PausePrint sends one generation-guarded QoS 0 pause request.
	PausePrint(string, uint64) error
	// SetChamberLight sends a generation-guarded light request.
	SetChamberLight(string, uint64, bool) error
}

// operationKey identifies a local operation in client contexts.
type operationKey struct{}

// operations allocates process-local operation identifiers.
var operations atomic.Uint64

// operationOf returns the local operation identifier, or zero for an unscoped call.
func operationOf(ctx context.Context) uint64 { v, _ := ctx.Value(operationKey{}).(uint64); return v }

// Service owns per-printer startup and first-layer workers plus startup-only diagnostics.
type Service struct {
	// printers retains configured order for diagnostics.
	printers []config.Printer
	// settings is the construction-time effective configuration.
	settings Settings
	// client evaluates automatic startup and first-layer frames; nil while off.
	client DecisionClient
	// frames supplies fresh shared camera JPEGs.
	frames FrameSource
	// state supplies coherent job and session snapshots.
	state StateSource
	// commands owns generation-guarded printer commands.
	commands PrinterCommands
	// log is the host logger scoped to this module.
	log *slog.Logger
	// activity records operator-facing stop events.
	activity *activity.Log
	// blocked parks automatic workers and removes snapshot routes.
	blocked string
	// workers owns the fixed configured printer set.
	workers map[string]*worker
	// newClient creates isolated diagnostic clients; tests replace it.
	newClient func(Settings, *slog.Logger) DecisionClient
	// now is the package-test clock seam.
	now func() time.Time
	// kPoll bounds pending revalidation intervals.
	kPoll time.Duration
	// kHint bounds layer and connectivity waits during admission.
	kHint time.Duration
	// kCheck bounds capture plus evaluation and result age.
	kCheck time.Duration
	// kConfirm bounds stop confirmation.
	kConfirm time.Duration
	// lifeMu serializes startup and shutdown ownership.
	lifeMu sync.Mutex
	// started prevents duplicate workers.
	started bool
	// closed prevents restart after shutdown.
	closed bool
	// cancel ends all workers and their waits.
	cancel context.CancelFunc
	// wg joins all worker goroutines.
	wg sync.WaitGroup
}

// candidate is an observed startup hint waiting for admission. The wait
// fields track one expiring wait condition; waitExpired owns them.
type candidate struct {
	// generation is the hinted job generation.
	generation uint64
	// waitReason names the current wait condition.
	waitReason string
	// waitSince is when the current wait condition started.
	waitSince time.Time
}

// layerHint is an observed first-layer hint waiting for admission.
type layerHint struct {
	// generation is the hinted job generation.
	generation uint64
	// waitReason names the current wait condition.
	waitReason string
	// waitSince is when the current wait condition started.
	waitSince time.Time
}

// boundCheck retains admission evidence through evaluation and both stop attempts.
type boundCheck struct {
	// generation is the admitted job generation.
	generation uint64
	// prepare allows only one PREPARE-to-first-RUNNING transition.
	prepare bool
	// admitted is the step admission time, the boundary that starts the
	// overall-check budget.
	admitted time.Time
	// captured is the exact image capture time.
	captured time.Time
	// operation links local check and stop records.
	operation uint64
}

// pendingStop tracks one bound verdict and its limited confirmation window.
type pendingStop struct {
	// bound keeps the original admission evidence.
	bound boundCheck
	// attempts counts publications, including failed publications.
	attempts int
	// firstPrepare permits one first-RUNNING follow-up.
	firstPrepare bool
	// dispatched is the last dispatch-completion time.
	dispatched time.Time
	// deadline bounds confirmation after the last permitted dispatch.
	deadline time.Time
}

// worker keeps short shared state under mu and pending state on its worker goroutine.
type worker struct {
	// s owns fixed dependencies and timing.
	s *Service
	// printer is the configured printer identity.
	printer config.Printer
	// wake coalesces activity without blocking report processing.
	wake chan struct{}
	// mu guards startup and first-layer hints, processed markers, and display status.
	mu sync.Mutex
	// candidate is the latest observed startup hint.
	candidate *candidate
	// processed prevents repeated checks for a generation.
	processed uint64
	// status is the last automatic status, never diagnostic state.
	status ModuleState
	// pending is owned exclusively by the worker loop.
	pending *pendingStop
	// layerHint is the latest observed first-layer hint, independent of
	// startup admission.
	layerHint *layerHint
	// layerProcessed prevents replay after any first-layer outcome.
	layerProcessed uint64
	// startupStopped remembers every generation with a startup stop attempt.
	startupStopped uint64
	// layerPending is owned exclusively by the worker loop.
	layerPending *pendingPause
}

// New constructs the module without opening connections or starting goroutines.
func New(printers []config.Printer, settings Settings, client DecisionClient, frames FrameSource, state StateSource, commands PrinterCommands, log *slog.Logger) *Service {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	log = log.With("origin", "platecheck")
	if frames == nil {
		frames = IdleFrames{}
	}
	if settings.On() && client == nil {
		client = NewClient(settings, log.With("component", "vision_client"))
	}
	s := &Service{printers: append([]config.Printer(nil), printers...), settings: settings, client: client, frames: frames, state: state, commands: commands, log: log, workers: map[string]*worker{}, now: time.Now, kPoll: time.Second, kHint: 30 * time.Second, kCheck: 25 * time.Second, kConfirm: 30 * time.Second}
	s.newClient = func(settings Settings, log *slog.Logger) DecisionClient { return NewClient(settings, log) }
	for _, p := range printers {
		layerState := "idle"
		if !settings.FirstLayer.On() {
			layerState = "disabled"
		}
		s.workers[p.Serial] = &worker{s: s, printer: p, wake: make(chan struct{}, 1), status: ModuleState{State: "idle", Cutoff: settings.StopConfidence, FirstLayer: FirstLayerState{State: layerState, Cutoff: settings.FirstLayer.PauseConfidence}}}
	}
	return s
}

// SetActivity attaches the optional event log before startup.
func (s *Service) SetActivity(log *activity.Log) { s.activity = log }

// SetBlocked sets the fixed startup blocker before Module or Start.
func (s *Service) SetBlocked(reason string) { s.blocked = reason }

// Start launches one worker per printer only while enabled and unblocked.
func (s *Service) Start() {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	if s.started || s.closed || !s.settings.On() || s.blocked != "" {
		return
	}
	s.started = true
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	for _, w := range s.workers {
		s.wg.Add(1)
		go func(w *worker) { defer s.wg.Done(); w.run(ctx) }(w)
	}
}

// Close cancels all outstanding waits and joins all workers; repeated calls are safe.
func (s *Service) Close() {
	s.lifeMu.Lock()
	s.closed = true
	cancel := s.cancel
	s.lifeMu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
}

// ObserveActivity records startup and first-layer hints without blocking on network work.
func (s *Service) ObserveActivity(serial string, entry activity.Entry) {
	if !s.settings.On() || s.blocked != "" || strings.HasPrefix(entry.Kind, "platecheck_") {
		return
	}
	w := s.workers[serial]
	if w == nil || s.state == nil || s.commands == nil {
		return
	}
	if entry.Kind == "first_layer_complete" && s.settings.FirstLayer.On() {
		// The event must carry the print that produced it: its JobGen
		// must name the current active job. Missing or mismatched
		// evidence is rejected outright — never repaired from current
		// telemetry, never queued, never consuming the current job — so
		// a late event cannot act on the print that replaced it.
		j, _, ok := s.state.Print(serial)
		if ok && j.Active && entry.Observation.JobGen != 0 && entry.Observation.JobGen == j.Generation {
			w.mu.Lock()
			if j.Generation > w.layerProcessed && (w.layerHint == nil || w.layerHint.generation != j.Generation) {
				w.layerHint = &layerHint{generation: j.Generation}
			}
			w.mu.Unlock()
		}
	}
	if entry.Kind == "print_preparing" || entry.Kind == "print_started" || entry.Kind == "state_initial" {
		j, _, ok := s.state.Print(serial)
		if !ok {
			return
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		if entry.Kind == "state_initial" {
			// Mid-print attach: the proxy never saw this print begin, so
			// there is no startup to check. Mark it processed without a
			// check and without recording activity.
			if j.Active && !j.StartObserved {
				w.processed = max(w.processed, j.Generation)
				w.candidate = nil
				w.setStatusLocked("skipped", nil, "start_not_observed")
			}
			return
		}
		// A start hint binds only when the entry's job is still the
		// current active job and this proxy saw it begin. Delayed
		// delivery of an older generation's hint creates no candidate.
		if j.Active && j.StartObserved && entry.Observation.JobGen == j.Generation && j.Generation > w.processed &&
			(w.candidate == nil || w.candidate.generation != j.Generation) {
			w.candidate = &candidate{generation: j.Generation}
		}
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// run serializes both phases' checks and pending confirmations for one printer.
func (w *worker) run(ctx context.Context) {
	ticker := time.NewTicker(w.s.kPoll)
	defer ticker.Stop()
	defer func() {
		if w.layerPending != nil {
			w.unconfirmedPause()
		}
		if w.pending != nil {
			w.unconfirmed()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-ticker.C:
		}
		if ctx.Err() != nil {
			return
		}
		w.step(ctx)
	}
}

// waitExpired tracks one expiring wait condition on a stored candidate or
// layer hint. Call it with w.mu held, on the stored fields, never on a
// copy. When the reason changes it stamps the new wait and returns false;
// otherwise it returns whether the wait has reached kHint.
func (w *worker) waitExpired(reason string, now time.Time, hint *string, since *time.Time) bool {
	if *hint != reason {
		*hint = reason
		*since = now
		return false
	}
	return now.Sub(*since) >= w.s.kHint
}

// authorize rereads the current print and reports whether the bound startup
// check may proceed. It returns the coherent job and session views plus a
// failure reason, or an empty reason when authorized.
func (w *worker) authorize(b boundCheck) (telemetry.JobView, telemetry.SessionView, string) {
	j, v, ok := w.s.state.Print(w.printer.Serial)
	if !ok || j.Generation != b.generation || !j.Active {
		return j, v, "print_ended"
	}
	if !w.s.commands.Connected(w.printer.Serial) {
		return j, v, "printer_offline"
	}
	layer := v.CurrentLayer()
	if layer != nil && *layer > 0 {
		return j, v, "print_past_start"
	}
	if j.State == "PREPARE" || j.State == "SLICING" {
		return j, v, ""
	}
	if j.State == "RUNNING" {
		if layer == nil {
			return j, v, "layer_not_reported"
		}
		return j, v, ""
	}
	return j, v, "print_paused"
}

// commandGate reads the upstream connection generation first, then
// authorizes the bound check. Every command sends with the returned
// generation immediately: the generation is never re-read after
// authorization, and the pool rejects the command if the connection
// changed after that read.
func (w *worker) commandGate(b boundCheck) (uint64, telemetry.SessionView, string) {
	gen, _, v, reason := w.commandGateFull(b)
	return gen, v, reason
}

// commandGateFull is commandGate with the authorized job view attached,
// so dispatch can read the PREPARE state from the same coherent read
// that authorized the stop instead of racing it with a second read.
func (w *worker) commandGateFull(b boundCheck) (uint64, telemetry.JobView, telemetry.SessionView, string) {
	gen := w.s.commands.Generation(w.printer.Serial)
	j, v, reason := w.authorize(b)
	return gen, j, v, reason
}

// step advances both phases: it confirms one pending pause or stop, then admits one startup or first-layer hint without inventing events.
func (w *worker) step(ctx context.Context) {
	if w.layerPending != nil {
		w.confirmPause()
		if w.layerPending != nil {
			return
		}
	}
	if w.pending != nil {
		w.confirm(ctx)
		if w.pending != nil {
			return
		}
	}
	w.mu.Lock()
	c := w.candidate
	if c == nil || c.generation <= w.processed {
		w.mu.Unlock()
		w.stepLayer(ctx)
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
	case j.Generation != c.generation || !j.Active:
		w.consumeLocked(c.generation)
		w.mu.Unlock()
		w.skip("skipped", "print_ended", 0)
		return
	case layer != nil && *layer > 0:
		w.consumeLocked(c.generation)
		w.mu.Unlock()
		w.skip("skipped", "print_past_start", 0)
		return
	case j.State == "SLICING" || j.State == "PAUSE" || j.State == "PAUSED":
		c.waitReason = ""
		w.mu.Unlock()
		return
	case !w.s.commands.Connected(w.printer.Serial):
		if w.waitExpired("printer_offline", now, &c.waitReason, &c.waitSince) {
			w.consumeLocked(c.generation)
			w.mu.Unlock()
			w.skip("skipped", "printer_offline", 0)
			return
		}
		w.mu.Unlock()
		return
	case j.State == "PREPARE":
		w.consumeLocked(c.generation)
		w.mu.Unlock()
		w.check(ctx, boundCheck{generation: c.generation, prepare: true, admitted: now, operation: operations.Add(1)})
		return
	case j.State == "RUNNING" && layer == nil:
		if w.waitExpired("layer_not_reported", now, &c.waitReason, &c.waitSince) {
			w.consumeLocked(c.generation)
			w.mu.Unlock()
			w.skip("skipped", "layer_not_reported", 0)
			return
		}
		w.mu.Unlock()
		return
	case j.State == "RUNNING":
		w.consumeLocked(c.generation)
		w.mu.Unlock()
		w.check(ctx, boundCheck{generation: c.generation, admitted: now, operation: operations.Add(1)})
		return
	default:
		w.mu.Unlock()
		return
	}
}

// consumeLocked marks a generation processed before camera or provider work
// starts. The caller holds w.mu.
func (w *worker) consumeLocked(generation uint64) {
	if generation <= w.processed {
		return
	}
	w.processed = generation
	if w.candidate != nil && w.candidate.generation == generation {
		w.candidate = nil
	}
}

// setStatus publishes immutable numeric values without retaining images.
// Every other status change clears the skip reason; skip sets it.
func (w *worker) setStatus(state string, r *Result) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.setStatusLocked(state, r, "")
}

// setStatusLocked publishes the status and reason. The caller holds w.mu.
func (w *worker) setStatusLocked(state string, r *Result, reason string) {
	v := ModuleState{State: state, Cutoff: w.s.settings.StopConfidence, Reason: reason}
	if r != nil {
		v.PClear = new(r.PClear)
		v.POccupied = new(r.POccupied)
		v.PAssessable = new(r.PAssessable)
	}
	v.FirstLayer = w.status.FirstLayer
	w.status = v
}

// skip records a cataloged fail-open reason and clears any previous scores.
func (w *worker) skip(state, reason string, operation uint64) {
	w.setStatusWithReason(state, nil, reason)
	w.s.log.Info("Plate check skipped", "operation", operation, "serial", w.printer.Serial, "state", state, "error_code", reason)
	kind, severity := "platecheck_skipped", activity.Info
	if state == "error" {
		kind, severity = "platecheck_failed", activity.Warning
	}
	w.s.activity.Record(w.printer.Serial, kind, severity, activitySentence("Plate check", state, reason))
}

// setStatusWithReason publishes the status with a skip reason.
func (w *worker) setStatusWithReason(state string, r *Result, reason string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.setStatusLocked(state, r, reason)
}

// check captures and evaluates once within the overall startup budget.
func (w *worker) check(parent context.Context, b boundCheck) {
	ctx, cancel := context.WithTimeout(context.WithValue(parent, operationKey{}, b.operation), w.s.kCheck)
	defer cancel()
	w.setStatus("checking", nil)
	w.s.log.InfoContext(ctx, "Plate check started", "operation", b.operation, "serial", w.printer.Serial)
	boundary := w.s.now()
	if _, _, reason := w.authorize(b); reason != "" {
		w.skip("skipped", reason, b.operation)
		return
	} else if ctx.Err() != nil {
		w.skip("skipped", w.contextReason(ctx), b.operation)
		return
	}
	if gen, v, reason := w.commandGate(b); v.ChamberLight == "off" {
		// The final local guard cannot eliminate a printer-side change after dispatch.
		if reason != "" {
			w.skip("skipped", reason, b.operation)
			return
		}
		if err := w.s.commands.SetChamberLight(w.printer.Serial, gen, true); err == nil {
			boundary = w.s.now()
		} else {
			w.s.log.DebugContext(ctx, "Plate-check light command failed", "operation", b.operation, "serial", w.printer.Serial, "error_code", "command_failed")
		}
	}
	frame, err := w.s.frames.Capture(ctx, w.printer.Serial, boundary)
	if err != nil {
		w.skip("error", errorCategory(err), b.operation)
		return
	}
	b.captured = frame.Captured
	if !frame.Captured.After(boundary) || frame.Captured.After(w.s.now()) {
		w.skip("skipped", "capture_timeout", b.operation)
		return
	}
	if _, _, reason := w.authorize(b); reason != "" {
		w.skip("skipped", reason, b.operation)
		return
	} else if ctx.Err() != nil || w.s.now().Sub(b.admitted) > w.s.kCheck {
		w.skip("skipped", w.budgetReason(ctx), b.operation)
		return
	}
	if w.s.log.Enabled(ctx, slog.LevelDebug) {
		w.s.log.DebugContext(ctx, "Plate-check frame captured", "operation", b.operation, "serial", w.printer.Serial, "model", w.s.settings.Model, "frame_sequence", frame.Seq, "captured_at", frame.Captured.UTC(), "width", frame.Width, "height", frame.Height, "image_bytes", len(frame.JPEG), "job_generation", b.generation)
	}
	result, err := w.s.client.Evaluate(ctx, frame.JPEG, config.DisplayModel(w.printer.Model, w.printer.Serial))
	if err != nil {
		w.skip("error", errorCategory(err), b.operation)
		return
	}
	if !validResult(result) {
		w.skip("error", "bad_response", b.operation)
		return
	}
	if _, _, reason := w.authorize(b); reason != "" {
		w.skip("skipped", reason, b.operation)
		return
	} else if ctx.Err() != nil || w.s.now().Sub(b.admitted) > w.s.kCheck || w.s.now().Sub(frame.Captured) > w.s.kCheck {
		w.skip("skipped", w.budgetReason(ctx), b.operation)
		return
	}
	decision := classify(result, w.s.settings.StopConfidence)
	w.s.log.InfoContext(ctx, "Plate check evaluated", "operation", b.operation, "serial", w.printer.Serial, "decision", decision, "cutoff", w.s.settings.StopConfidence, "p_clear", result.PClear, "p_occupied", result.POccupied, "p_assessable", result.PAssessable)
	if w.s.log.Enabled(ctx, slog.LevelDebug) {
		w.s.log.DebugContext(ctx, "Plate-check evaluation metadata", "operation", b.operation, "serial", w.printer.Serial, "model", w.s.settings.Model, "duration_ms", w.s.now().Sub(b.admitted).Milliseconds(), "input_tokens", result.InputTokens, "output_tokens", result.OutputTokens)
	}
	state := decision
	if state == "would_stop" {
		state = "checking"
	}
	w.setStatus(state, &result)
	switch decision {
	case "inconclusive":
		w.s.activity.Record(w.printer.Serial, "platecheck_skipped", activity.Info, "Plate view inconclusive, continuing print")
		return
	case "clear":
		w.s.activity.Record(w.printer.Serial, "platecheck_passed", activity.Info, "Plate check passed, plate clear")
		return
	case "below_threshold":
		w.s.activity.Record(w.printer.Serial, "platecheck_passed", activity.Info, "Plate check passed, plate may not be empty but is below the stop threshold")
		return
	case "would_stop":
		w.pending = &pendingStop{bound: b}
		w.dispatch(ctx)
	}
}

// contextReason maps a failed check context to its skip code: Close
// cancellation gives "canceled", and a passed deadline gives
// "check_timeout".
func (w *worker) contextReason(ctx context.Context) string {
	if ctx.Err() == context.Canceled {
		return "canceled"
	}
	return "check_timeout"
}

// budgetReason maps a failed budget recheck to its skip code: Close
// cancellation gives "canceled", and any other failure gives
// "check_timeout".
func (w *worker) budgetReason(ctx context.Context) string {
	if ctx.Err() == context.Canceled {
		return "canceled"
	}
	return "check_timeout"
}

// validResult rejects invalid fake or implementation-boundary probabilities.
func validResult(r Result) bool {
	return validAnswer(answer{Type: "noul", Noul: &r.PClear}) && validAnswer(answer{Type: "noul", Noul: &r.POccupied}) && validAnswer(answer{Type: "noul", Noul: &r.PAssessable}) && r.POccupied == 1-r.PClear
}

// dispatch revalidates the original evidence immediately before each stop attempt.
func (w *worker) dispatch(ctx context.Context) {
	p := w.pending
	gen, j, _, reason := w.commandGateFull(p.bound)
	if reason != "" {
		if p.attempts == 0 {
			w.pending = nil
			w.skip("skipped", reason, p.bound.operation)
		} else {
			w.unconfirmed()
		}
		return
	}
	p.attempts++
	if p.attempts == 1 {
		p.firstPrepare = j.State == "PREPARE"
	}
	w.mu.Lock()
	w.startupStopped = max(w.startupStopped, p.bound.generation)
	w.mu.Unlock()
	// Count and record the attempt even if local publication fails.
	err := w.s.commands.StopPrint(w.printer.Serial, gen)
	p.dispatched = w.s.now()
	p.deadline = p.dispatched.Add(w.s.kConfirm)
	w.setStatusKeepingScores("stop_requested")
	w.s.activity.Record(w.printer.Serial, "platecheck_stop_requested", activity.Warning, "Plate looks occupied, stopping print")
	w.s.log.WarnContext(ctx, "Plate-check stop requested", "operation", p.bound.operation, "serial", w.printer.Serial, "attempt", p.attempts)
	if err != nil {
		w.setStatusKeepingScores("stop_failed")
		w.s.activity.Record(w.printer.Serial, "platecheck_stop_failed", activity.Error, "Plate-check stop command failed, check the printer")
		w.s.log.ErrorContext(ctx, "Plate-check stop command failed", "operation", p.bound.operation, "serial", w.printer.Serial, "error_code", "command_failed")
	}
}

// setStatusKeepingScores changes the stop lifecycle without copying old check scores.
func (w *worker) setStatusKeepingScores(state string) {
	w.mu.Lock()
	w.status.State = state
	w.status.Reason = ""
	w.mu.Unlock()
}

// confirm waits for the printer to report the job ended after dispatch.
func (w *worker) confirm(ctx context.Context) {
	p := w.pending
	now := w.s.now()
	if !now.Before(p.deadline) {
		w.unconfirmed()
		return
	}
	j, _, ok := w.s.state.Print(w.printer.Serial)
	if !ok {
		return
	}
	if j.Generation != p.bound.generation {
		w.unconfirmed()
		return
	}
	if !j.Active {
		if j.State == "IDLE" || j.State == "FAILED" {
			w.setStatusKeepingScores("stopped")
			w.s.activity.Record(w.printer.Serial, "platecheck_stopped", activity.Info, "Print ended after plate-check stop request, clear the plate before resending")
			w.s.log.InfoContext(ctx, "Printer reports plate-check job ended", "operation", p.bound.operation, "serial", w.printer.Serial)
			w.pending = nil
			return
		}
		w.unconfirmed()
		return
	}
	if p.firstPrepare && p.attempts == 1 && j.State == "RUNNING" {
		w.dispatch(ctx)
	}
}

// unconfirmed closes an attempt without claiming printer termination.
func (w *worker) unconfirmed() {
	p := w.pending
	if p == nil {
		return
	}
	w.setStatusKeepingScores("stop_unconfirmed")
	w.s.activity.Record(w.printer.Serial, "platecheck_stop_unconfirmed", activity.Error, "Plate-check stop not confirmed, check the printer")
	w.s.log.Error("Plate-check stop unconfirmed", "operation", p.bound.operation, "serial", w.printer.Serial)
	w.pending = nil
}
