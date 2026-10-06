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

// StateSource supplies current job identity and session-scoped action evidence.
type StateSource interface {
	// Job returns coherent job-generation and real-report evidence.
	Job(string) (telemetry.JobView, bool)
	// Session returns connection-scoped layer and state evidence.
	Session(string) (telemetry.SessionView, bool)
}

// PrinterCommands supplies only guarded light and stop commands.
type PrinterCommands interface {
	// Generation returns the current upstream connection generation.
	Generation(string) uint64
	// StopPrint sends a generation-guarded QoS 0 stop request.
	StopPrint(string, uint64) error
	// SetChamberLight sends a generation-guarded light request.
	SetChamberLight(string, uint64, bool) error
}

// operationKey identifies a local operation in client contexts.
type operationKey struct{}

// operations allocates process-local operation identifiers.
var operations atomic.Uint64

// operationOf returns the local operation identifier, or zero for an unscoped call.
func operationOf(ctx context.Context) uint64 { v, _ := ctx.Value(operationKey{}).(uint64); return v }

// Service owns independent startup workers and read-only snapshot diagnostics.
type Service struct {
	// printers retains configured order for diagnostics.
	printers []config.Printer
	// settings is the construction-time effective configuration.
	settings Settings
	// client evaluates automatic startup frames; nil while off.
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
	// kHint bounds SLICING startup hints.
	kHint time.Duration
	// kRunningHint bounds direct RUNNING admission.
	kRunningHint time.Duration
	// kFresh bounds real-report freshness.
	kFresh time.Duration
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

// candidate binds an observed startup hint to its original identity and connection.
type candidate struct {
	// generation is the hinted job generation.
	generation uint64
	// revision is the hinted identity revision.
	revision uint64
	// connection is the hinted upstream transport generation.
	connection uint64
	// at is the original activity time.
	at time.Time
	// startedAt is the first print_started hint, independent of the SLICING boundary.
	startedAt time.Time
	// kind distinguishes PREPARE from direct RUNNING permission.
	kind string
}

// boundCheck retains admission evidence through evaluation and both stop attempts.
type boundCheck struct {
	// job is the admitted job identity and running epoch.
	job telemetry.JobView
	// session is the admitted session epoch.
	session telemetry.SessionView
	// connection is the admitted upstream generation.
	connection uint64
	// prepare allows only one PREPARE-to-first-RUNNING transition.
	prepare bool
	// admitted is the original overall-check budget boundary.
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
	// obs is the pre-dispatch real observation counter.
	obs uint64
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
	// mu guards startup hints, processed markers, suppression, and display status.
	mu sync.Mutex
	// candidate is the latest observed startup hint.
	candidate *candidate
	// processed prevents repeated checks for a generation.
	processed uint64
	// suppressed revokes initial attachment permission, including in-flight work.
	suppressed uint64
	// status is the last automatic status, never diagnostic state.
	status ModuleState
	// pending is owned exclusively by the worker loop.
	pending *pendingStop
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
		client = NewClient(settings, log.With("component", "clef_client"))
	}
	s := &Service{printers: append([]config.Printer(nil), printers...), settings: settings, client: client, frames: frames, state: state, commands: commands, log: log, workers: map[string]*worker{}, now: time.Now, kPoll: time.Second, kHint: 30 * time.Second, kRunningHint: 5 * time.Second, kFresh: 15 * time.Second, kCheck: 25 * time.Second, kConfirm: 30 * time.Second}
	s.newClient = func(settings Settings, log *slog.Logger) DecisionClient { return NewClient(settings, log) }
	for _, p := range printers {
		s.workers[p.Serial] = &worker{s: s, printer: p, wake: make(chan struct{}, 1), status: ModuleState{State: "idle", Cutoff: settings.StopConfidence}}
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

// ObserveActivity records startup hints without consuming telemetry report channels.
func (s *Service) ObserveActivity(serial string, entry activity.Entry) {
	if !s.settings.On() || s.blocked != "" || strings.HasPrefix(entry.Kind, "platecheck_") {
		return
	}
	w := s.workers[serial]
	if w == nil || s.state == nil || s.commands == nil {
		return
	}
	if entry.Kind == "print_preparing" || entry.Kind == "print_started" || entry.Kind == "state_initial" {
		job, ok := s.state.Job(serial)
		if ok && job.Active && job.Generation != 0 {
			connection := s.commands.Generation(serial)
			w.mu.Lock()
			if entry.Kind == "state_initial" {
				w.processed = max(w.processed, job.Generation)
				w.suppressed = job.Generation
				w.candidate = nil
			} else if job.Generation > w.processed {
				at := entry.Time
				if at.IsZero() {
					at = s.now()
				}
				// Repeated hints preserve the earliest boundary instead of extending its lifetime.
				if w.candidate == nil || w.candidate.generation != job.Generation {
					w.candidate = &candidate{generation: job.Generation, revision: job.Revision, connection: connection, at: at, kind: entry.Kind}
					if entry.Kind == "print_started" {
						w.candidate.startedAt = at
					}
				} else if entry.Kind == "print_started" && w.candidate.startedAt.IsZero() {
					w.candidate.kind = entry.Kind
					w.candidate.startedAt = at
				}
			}
			w.mu.Unlock()
		}
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// run serializes checks and pending confirmation for one printer.
func (w *worker) run(ctx context.Context) {
	ticker := time.NewTicker(w.s.kPoll)
	defer ticker.Stop()
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

// snapshot retries bounded coherent job and session reads before action authorization.
func (w *worker) snapshot() (telemetry.JobView, telemetry.SessionView, uint64, bool) {
	s := w.s
	if s.state == nil || s.commands == nil {
		return telemetry.JobView{}, telemetry.SessionView{}, 0, false
	}
	var j telemetry.JobView
	var v telemetry.SessionView
	var connection uint64
	for range 3 {
		connection = s.commands.Generation(w.printer.Serial)
		first, ok := s.state.Job(w.printer.Serial)
		if !ok {
			return first, v, connection, false
		}
		v, ok = s.state.Session(w.printer.Serial)
		if !ok {
			return first, v, connection, false
		}
		j, ok = s.state.Job(w.printer.Serial)
		if !ok {
			return j, v, connection, false
		}
		// Session and Job use separate clocks, so compare each projection with itself.
		lastSession, ok := s.state.Session(w.printer.Serial)
		if !ok {
			return j, v, connection, false
		}
		jobStable := first.Generation == j.Generation && first.Revision == j.Revision && first.State == j.State && first.Active == j.Active && first.RunningEpoch == j.RunningEpoch && first.StateGen == j.StateGen && first.ObsGen == j.ObsGen && first.ObsAt.Equal(j.ObsAt)
		sessionStable := v.Obs == lastSession.Obs && v.ObsAt.Equal(lastSession.ObsAt) && v.ObsGen == lastSession.ObsGen && v.StateGen == lastSession.StateGen && v.Epoch == lastSession.Epoch && v.SessionGen == lastSession.SessionGen && v.State == lastSession.State && v.Active == lastSession.Active && (v.LayerNum == nil) == (lastSession.LayerNum == nil)
		if sessionStable && v.LayerNum != nil {
			sessionStable = *v.LayerNum == *lastSession.LayerNum
		}
		if !jobStable || !sessionStable {
			continue
		}
		age := s.now().Sub(j.ObsAt)
		valid := connection != 0 && s.commands.Generation(w.printer.Serial) == connection && j.StateGen == connection && j.ObsGen == connection && !j.ObsAt.IsZero() && age >= 0 && age <= s.kFresh && v.State == j.State && v.StateGen == connection && v.ObsGen == connection
		return j, v, connection, valid
	}
	return j, v, connection, false
}

// authorize preserves original identity, connection, and transition budget for every action.
func (w *worker) authorize(b boundCheck) (telemetry.JobView, telemetry.SessionView, bool) {
	j, v, connection, ok := w.snapshot()
	w.mu.Lock()
	suppressed := w.suppressed == b.job.Generation
	w.mu.Unlock()
	if !ok || suppressed || !j.Active || j.Generation != b.job.Generation || j.Revision != b.job.Revision || connection != b.connection || j.RunningEpoch != b.job.RunningEpoch {
		return j, v, false
	}
	if j.State == "PREPARE" {
		return j, v, b.prepare && v.Epoch == b.session.Epoch && v.SessionGen == b.session.SessionGen
	}
	if j.State != "RUNNING" || !v.Active || v.LayerNum == nil || *v.LayerNum != 0 {
		return j, v, false
	}
	if b.prepare {
		return j, v, v.Epoch == b.session.Epoch+1 && v.SessionGen == b.session.SessionGen+1
	}
	return j, v, v.Epoch == b.session.Epoch && v.SessionGen == b.session.SessionGen
}

// step admits one hint or revalidates one pending stop without inventing startup events.
func (w *worker) step(ctx context.Context) {
	if w.pending != nil {
		w.confirm(ctx)
		if w.pending != nil {
			return
		}
	}
	w.mu.Lock()
	var hint candidate
	have := w.candidate != nil
	if have {
		hint = *w.candidate
		have = hint.generation > w.processed
	}
	w.mu.Unlock()
	if !have {
		return
	}
	j, v, connection, ok := w.snapshot()
	if j.Generation != hint.generation || j.Revision != hint.revision || connection != hint.connection || !j.Active || w.s.now().Sub(hint.at) < 0 || w.s.now().Sub(hint.at) > w.s.kHint {
		w.consume(hint.generation)
		w.skip("skipped", "startup_changed", 0)
		return
	}
	if !ok {
		return
	}
	if j.State == "SLICING" {
		return
	}
	prepare := j.State == "PREPARE"
	if !prepare && (j.State != "RUNNING" || hint.kind != "print_started" || hint.startedAt.IsZero() || w.s.now().Sub(hint.startedAt) < 0 || w.s.now().Sub(hint.startedAt) > w.s.kRunningHint || !v.Active || v.LayerNum == nil || *v.LayerNum != 0) {
		w.consume(hint.generation)
		w.skip("skipped", "startup_changed", 0)
		return
	}
	if !w.consume(hint.generation) {
		return
	}
	b := boundCheck{job: j, session: v, connection: connection, prepare: prepare, admitted: w.s.now(), operation: operations.Add(1)}
	w.check(ctx, b)
}

// consume marks a generation processed before camera or provider work starts.
func (w *worker) consume(generation uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if generation <= w.processed {
		return false
	}
	w.processed = generation
	if w.candidate != nil && w.candidate.generation == generation {
		w.candidate = nil
	}
	return true
}

// setStatus publishes immutable numeric values without retaining images.
func (w *worker) setStatus(state string, r *Result) {
	v := ModuleState{State: state, Cutoff: w.s.settings.StopConfidence}
	if r != nil {
		v.PClear = new(r.PClear)
		v.POccupied = new(r.POccupied)
		v.PAssessable = new(r.PAssessable)
	}
	w.mu.Lock()
	w.status = v
	w.mu.Unlock()
}

// skip records a fixed fail-open reason and clears any previous scores.
func (w *worker) skip(state, reason string, operation uint64) {
	w.setStatus(state, nil)
	w.s.log.Info("Plate check skipped", "operation", operation, "serial", w.printer.Serial, "state", state, "error_code", reason)
	w.s.activity.Record(w.printer.Serial, "platecheck_skipped", activity.Info, fixedSentence(reason))
}

// check captures and evaluates once within the overall startup budget.
func (w *worker) check(parent context.Context, b boundCheck) {
	ctx, cancel := context.WithTimeout(context.WithValue(parent, operationKey{}, b.operation), w.s.kCheck)
	defer cancel()
	w.setStatus("checking", nil)
	w.s.log.InfoContext(ctx, "Plate check started", "operation", b.operation, "serial", w.printer.Serial)
	boundary := w.s.now()
	_, v, valid := w.authorize(b)
	if !valid || ctx.Err() != nil {
		w.skip("skipped", "startup_changed", b.operation)
		return
	}
	if v.ChamberLight == "off" {
		// The final local guard cannot eliminate printer-side replacement after dispatch.
		if _, _, valid = w.authorize(b); !valid {
			w.skip("skipped", "startup_changed", b.operation)
			return
		}
		if err := w.s.commands.SetChamberLight(w.printer.Serial, b.connection, true); err == nil {
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
		w.skip("skipped", "startup_changed", b.operation)
		return
	}
	if _, _, valid = w.authorize(b); !valid || ctx.Err() != nil || w.s.now().Sub(b.admitted) > w.s.kCheck {
		w.skip("skipped", "startup_changed", b.operation)
		return
	}
	if w.s.log.Enabled(ctx, slog.LevelDebug) {
		w.s.log.DebugContext(ctx, "Plate-check frame captured", "operation", b.operation, "serial", w.printer.Serial, "model", w.s.settings.Model, "frame_sequence", frame.Seq, "captured_at", frame.Captured.UTC(), "width", frame.Width, "height", frame.Height, "image_bytes", len(frame.JPEG), "job_generation", b.job.Generation, "job_revision", b.job.Revision, "connection_generation", b.connection)
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
	if _, _, valid = w.authorize(b); !valid || ctx.Err() != nil || w.s.now().Sub(b.admitted) > w.s.kCheck || w.s.now().Sub(frame.Captured) > w.s.kCheck {
		w.skip("skipped", "startup_changed", b.operation)
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
	if decision == "inconclusive" {
		w.s.activity.Record(w.printer.Serial, "platecheck_skipped", activity.Info, "The build-plate view was inconclusive. The print continued.")
		return
	}
	if decision != "would_stop" {
		return
	}
	w.pending = &pendingStop{bound: b}
	w.dispatch(ctx)
}

// validResult rejects invalid fake or implementation-boundary probabilities.
func validResult(r Result) bool {
	return validAnswer(answer{Type: "noul", Noul: &r.PClear}) && validAnswer(answer{Type: "noul", Noul: &r.POccupied}) && validAnswer(answer{Type: "noul", Noul: &r.PAssessable}) && r.POccupied == 1-r.PClear
}

// dispatch revalidates the original evidence immediately before each stop attempt.
func (w *worker) dispatch(ctx context.Context) {
	p := w.pending
	j, v, ok := w.authorize(p.bound)
	if !ok || ctx.Err() != nil || w.s.now().Sub(p.bound.captured) > w.s.kCheck {
		if p.attempts == 0 {
			w.pending = nil
			w.skip("skipped", "startup_changed", p.bound.operation)
		} else {
			w.unconfirmed()
		}
		return
	}
	p.attempts++
	if p.attempts == 1 {
		p.firstPrepare = j.State == "PREPARE"
	}
	p.obs = v.Obs
	// Count and record the attempt even if local publication fails.
	err := w.s.commands.StopPrint(w.printer.Serial, p.bound.connection)
	p.dispatched = w.s.now()
	p.deadline = p.dispatched.Add(w.s.kConfirm)
	w.setStatusKeepingScores("stop_requested")
	w.s.activity.Record(w.printer.Serial, "platecheck_stop_requested", activity.Warning, "Build plate may be occupied; stop requested. Check the printer and clear the plate before sending the print again.")
	w.s.log.WarnContext(ctx, "Plate-check stop requested", "operation", p.bound.operation, "serial", w.printer.Serial, "attempt", p.attempts)
	if err != nil {
		w.setStatusKeepingScores("stop_failed")
		w.s.activity.Record(w.printer.Serial, "platecheck_stop_failed", activity.Error, "Plate-check stop command failed. Check the printer now.")
		w.s.log.ErrorContext(ctx, "Plate-check stop command failed", "operation", p.bound.operation, "serial", w.printer.Serial, "error_code", "command_failed")
	}
}

// setStatusKeepingScores changes the stop lifecycle without copying old check scores.
func (w *worker) setStatusKeepingScores(state string) {
	w.mu.Lock()
	w.status.State = state
	w.mu.Unlock()
}

// confirm requires a fresh same-job real terminal observation after dispatch.
func (w *worker) confirm(ctx context.Context) {
	p := w.pending
	j, v, connection, fresh := w.snapshot()
	same := j.Generation == p.bound.job.Generation && j.Revision == p.bound.job.Revision && connection == p.bound.connection
	if !same {
		w.unconfirmed()
		return
	}
	if fresh && !j.Active && v.Obs > p.obs && j.ObsAt.After(p.dispatched) && j.ObsAt.Before(p.deadline) {
		if j.State == "IDLE" || j.State == "FAILED" {
			w.setStatusKeepingScores("stopped")
			w.s.activity.Record(w.printer.Serial, "platecheck_stopped", activity.Info, "Printer reports the job ended after the plate-check stop request.")
			w.s.log.InfoContext(ctx, "Printer reports plate-check job ended", "operation", p.bound.operation, "serial", w.printer.Serial)
			w.pending = nil
			return
		}
		if j.State == "FINISH" {
			w.unconfirmed()
			return
		}
	}
	if !w.s.now().Before(p.deadline) {
		w.unconfirmed()
		return
	}
	if !j.Active {
		return
	}
	if _, _, valid := w.authorize(p.bound); !valid {
		// Stale reports may recover inside the window; identity and epoch changes cannot.
		if j.RunningEpoch != p.bound.job.RunningEpoch || j.State == "PAUSE" || j.State == "PAUSED" || (fresh && j.State == "RUNNING") {
			w.unconfirmed()
		}
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
	w.s.activity.Record(w.printer.Serial, "platecheck_stop_unconfirmed", activity.Error, "Plate-check stop is unconfirmed. Check the printer now.")
	w.s.log.Error("Plate-check stop unconfirmed", "operation", p.bound.operation, "serial", w.printer.Serial)
	w.pending = nil
}
