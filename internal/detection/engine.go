// Detection engine for the optional OctoEverywhere Gadget integration: one
// worker goroutine per configured printer schedules frame inspections during
// active prints, acts on pause suggestions under strict freshness and
// generation guards, and publishes display status for the HTTP endpoints.
// One request is in flight per printer by construction: each worker runs its
// loop serially and never holds locks across network calls.
package detection

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// Timing policy. Fixed internal constants keep the configuration surface at
// exactly one env var; values follow the official Gadget docs where they
// exist and conservative local choices where they do not.
const (
	// confirmWait bounds the wait for a printer PAUSE report after the
	// proxy's pause command.
	confirmWait = 30 * time.Second
	// resultValidity bounds how long an inspected frame may still authorize
	// a pause after the inspection returns.
	resultValidity = 30 * time.Second
	// reportFreshMax bounds telemetry staleness for action authorization.
	reportFreshMax = 15 * time.Second
	// frameFreshMax bounds camera frame staleness; every inspection needs a
	// frame unique to it (Seq strictly greater than the previous one).
	frameFreshMax = 5 * time.Second
	// frameWait bounds the wait for a fresh unique frame per inspection.
	frameWait = 15 * time.Second
	// tempBackoffInitial and tempBackoffMax bound temporary-failure retries.
	tempBackoffInitial = 20 * time.Second
	tempBackoffMax     = 10 * time.Minute
	// idlePoll paces parked workers; stalePoll re-checks authorization while
	// waiting for fresh current-generation telemetry; cameraRetry paces
	// retries while the camera cannot serve a fresh frame.
	idlePoll    = 5 * time.Second
	stalePoll   = 2 * time.Second
	cameraRetry = 10 * time.Second
)

// Pause lifecycle values for Status.PauseState.
const (
	PauseNone        = "none"
	PausePending     = "pending"
	PauseConfirmed   = "confirmed"
	PauseUnconfirmed = "unconfirmed"
)

// Display states for Status.State.
const (
	StateStarting    = "starting"
	StateIdle        = "idle"
	StateMonitoring  = "monitoring"
	StateAttention   = "attention"
	StatePaused      = "paused"
	StateDegraded    = "degraded"
	StateUnsupported = "unsupported"
	StateBlocked     = "blocked"
)

// Reason codes explaining state nuances.
const (
	ReasonCameraDisabled    = "camera_disabled"
	ReasonCameraUnsupported = "camera_unsupported"
	ReasonAwaitingReports   = "awaiting_reports"
	ReasonAwaitingTelemetry = "awaiting_fresh_telemetry"
	ReasonCameraLost        = "camera_unavailable"
	ReasonAPIRetrying       = "api_retrying"
	ReasonPauseUnconfirmed  = "pause_unconfirmed"
	ReasonPauseFailed       = "pause_command_failed"
)

// Per-print user override values for Status.State, Status.Reason, and
// Status.DisabledUntil: they mark a print the user excluded from inspection
// and explain that the override lives in process memory only, until the
// print ends or the proxy restarts.
const (
	StateDisabled         = "disabled"
	ReasonUserDisabled    = "user_disabled"
	DisabledUntilPrintEnd = "print_end_or_restart"
)

// Control errors from SetDetectionEnabled. The HTTP endpoint maps
// ErrUnknownPrinter to 404 and the rest to 409.
var (
	// ErrUnknownPrinter reports a serial that is not configured.
	ErrUnknownPrinter = errors.New("unknown printer")
	// ErrNoPrintSession reports that no print is currently active, so
	// there is nothing to disable.
	ErrNoPrintSession = errors.New("no active print session")
	// ErrStaleSession reports a session token that does not match the
	// current print.
	ErrStaleSession = errors.New("session token does not match the current print")
	// ErrDetectionUnavailable reports that detection cannot run for this
	// printer at all: the feature is blocked or the model is unsupported.
	ErrDetectionUnavailable = errors.New("detection is not available for this printer")
)

// Status is one printer's detection display state. It is the JSON object
// served as Tile.detection on the camera endpoints and as the values of the
// /status detection map. Only State and PauseState are always present.
type Status struct {
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
	Quality int    `json:"quality,omitempty"`
	// Processing is true only while the image-analysis request is in flight.
	Processing bool   `json:"processing,omitempty"`
	Warning    bool   `json:"warning,omitempty"`
	PauseState string `json:"pause_state"`
	// Enabled reports permission, not activity: false only while the
	// per-print user override is in force for the current print session.
	Enabled bool `json:"enabled"`
	// DisabledUntil explains the override's lifetime; set only while
	// Enabled is false.
	DisabledUntil string `json:"disabled_until,omitempty"`
	// SessionID is the opaque token of the active print session; clients
	// send it back to disable detection for that print.
	SessionID          string  `json:"session_id,omitempty"`
	LastInspectedLayer *int    `json:"last_inspected_layer,omitempty"`
	AgeSeconds         float64 `json:"age_seconds,omitempty"`
	NextCheckSeconds   float64 `json:"next_check_seconds,omitempty"`
	FasterInspection   bool    `json:"faster_inspection,omitempty"`
	CameraLost         bool    `json:"camera_lost,omitempty"`
	UsingFallback      bool    `json:"using_fallback,omitempty"`
	Suspended          bool    `json:"suspended,omitempty"`
	Message            string  `json:"message,omitempty"`
}

// StableKey returns the status without continuously aging fields so change
// detection on the camera events stream only fires on real state changes.
func (s Status) StableKey() any {
	s.AgeSeconds = 0
	s.NextCheckSeconds = 0
	return s
}

// Frame is one captured camera frame handed to the engine. Buffers are
// immutable once published.
type Frame struct {
	JPEG     []byte
	Seq      uint64
	Captured time.Time
}

// FrameSource is the narrow camera contract the engine needs. Acquire must
// start (or join) the shared capture; Release balances it. WaitFrame waits
// up to timeout for a frame with Seq greater than after.
type FrameSource interface {
	Acquire(serial string) bool
	Release(serial string)
	WaitFrame(serial string, ctx context.Context, after uint64, timeout time.Duration) (Frame, bool)
}

// sessionSource exposes the telemetry projection for one printer.
type sessionSource interface {
	Session(serial string) (telemetry.SessionView, bool)
}

// Pauser sends the guarded upstream pause; it must fail rather than act on a
// connection older than generation.
type Pauser interface {
	PausePrint(serial string, generation uint64) error
}

// GenerationSource reports the printer's current upstream connection
// generation; every reconnect or loss changes the value.
type GenerationSource interface {
	Generation(serial string) uint64
}

// gadgetClient is the API surface the engine uses; *Client implements it.
type gadgetClient interface {
	CreateContext(ctx context.Context) (Session, error)
	Process(ctx context.Context, url string, jpeg []byte) (Result, error)
}

// Engine owns one worker per configured printer.
type Engine struct {
	client   gadgetClient
	frames   FrameSource
	sessions sessionSource
	pauser   Pauser
	gens     GenerationSource
	log      *slog.Logger
	activity *activity.Log // optional recent-event log

	specs   map[string]config.Printer
	blocked string // non-empty: feature blocked; the value is the reason
	// nonce prefixes every session token this engine issues, so tokens
	// from a previous proxy process can never match a new print that
	// restarted the session generations from a low value.
	nonce string
	// now returns the current time. It defaults to time.Now and is
	// overridable by package tests only, so lifecycle tests can drive the
	// worker deterministically without real sleeps.
	now func() time.Time
	// Timing knobs, defaulted below and overridable by package tests only;
	// they are not configuration.
	kConfirmWait    time.Duration
	kResultValidity time.Duration
	kReportFreshMax time.Duration
	kFrameFreshMax  time.Duration
	kFrameWait      time.Duration
	kIdlePoll       time.Duration
	kStalePoll      time.Duration
	kCameraRetry    time.Duration
	mu              sync.Mutex
	suspended       bool
	suspendMsg      string
	suspendCh       chan struct{}

	workers map[string]*worker
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// New builds the engine; Start spawns the workers.
func New(printers []config.Printer, client gadgetClient, frames FrameSource,
	sessions sessionSource, pauser Pauser, gens GenerationSource, log *slog.Logger) *Engine {
	specs := make(map[string]config.Printer, len(printers))
	workers := make(map[string]*worker, len(printers))
	e := &Engine{
		client:          client,
		frames:          frames,
		sessions:        sessions,
		pauser:          pauser,
		gens:            gens,
		log:             log,
		specs:           specs,
		now:             time.Now,
		nonce:           newSessionNonce(),
		suspendCh:       make(chan struct{}),
		workers:         workers,
		kConfirmWait:    confirmWait,
		kResultValidity: resultValidity,
		kReportFreshMax: reportFreshMax,
		kFrameFreshMax:  frameFreshMax,
		kFrameWait:      frameWait,
		kIdlePoll:       idlePoll,
		kStalePoll:      stalePoll,
		kCameraRetry:    cameraRetry,
	}
	for _, p := range printers {
		specs[p.Serial] = p
		workers[p.Serial] = newWorker(e, p.Serial)
	}
	return e
}

// SetActivity attaches the optional in-memory event log. Call before Start.
func (e *Engine) SetActivity(log *activity.Log) {
	e.activity = log
}

// SetBlocked parks every worker in the blocked state before Start: the key
// is present but the camera feature is disabled, so detection must be
// visible yet perform no camera or API activity.
func (e *Engine) SetBlocked(reason string) {
	e.blocked = reason
}

// Start spawns the worker goroutines. Call once.
func (e *Engine) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	for serial, w := range e.workers {
		w.wake = e.sessionsWatch(serial)
		e.wg.Add(1)
		go func(w *worker) {
			defer e.wg.Done()
			w.run(ctx)
		}(w)
	}
}

// Close stops every worker and waits for exit. Idempotent.
func (e *Engine) Close() {
	if e.cancel == nil {
		return
	}
	e.cancel()
	e.wg.Wait()
	e.cancel = nil
}

// sessionsWatch bridges the telemetry cache to the worker wake channel.
func (e *Engine) sessionsWatch(serial string) <-chan struct{} {
	if wc, ok := e.sessions.(interface {
		WatchDetection(string) <-chan struct{}
	}); ok {
		return wc.WatchDetection(serial)
	}
	return nil
}

// suspend suspends all detection account-wide until restart. The first call
// wins; workers observe it through the closed channel.
func (e *Engine) suspend(message string) {
	e.mu.Lock()
	if e.suspended {
		e.mu.Unlock()
		return
	}
	e.suspended = true
	e.suspendMsg = message
	close(e.suspendCh)
	e.mu.Unlock()
	e.log.Error("detection suspended until restart", "reason", message)
	for serial := range e.workers {
		e.activity.Record(serial, "ai_suspended", activity.Error, "AI inspection suspended: "+message)
	}
}

// isSuspended reports the account-wide suspension state.
func (e *Engine) isSuspended() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.suspended
}

// AccountSuspended returns the suspension state and reason for /status.
func (e *Engine) AccountSuspended() (bool, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.suspended, e.suspendMsg
}

// DetectionStatus returns one printer's status, or nil for unknown serials.
func (e *Engine) DetectionStatus(serial string) any {
	w, ok := e.workers[serial]
	if !ok {
		return nil
	}
	return w.view()
}

// DetectionMap returns every serial's status for the /status detection map.
func (e *Engine) DetectionMap() map[string]any {
	out := make(map[string]any, len(e.workers))
	for serial, w := range e.workers {
		out[serial] = w.view()
	}
	return out
}

// worker drives detection for one printer.
type worker struct {
	e      *Engine
	serial string
	wake   <-chan struct{}
	log    *slog.Logger

	mu       sync.Mutex
	st       Status
	resultAt time.Time
	nextDue  time.Time

	// Control plane for the per-print user override (PUT /detection).
	// controlMu serializes override transitions against the final pause
	// dispatch in onResult, so a disable either cancels a pause before it
	// is sent or observes one already sent; lock order is controlMu before
	// mu. workGen invalidates in-flight inspection artifacts on every
	// override change, workCtx carries that cancellation into the camera
	// wait and the API calls, and controlWake wakes the loop afterwards.
	controlMu   sync.Mutex
	controlWake chan struct{}

	// Shared loop state, guarded by mu. camHeld is also released by the
	// control path when a disable lands mid-inspection.
	camHeld     bool
	overrideGen uint64 // 0 = no override
	workGen     uint64 // bumped on every override change
	workCtx     context.Context
	workCancel  context.CancelFunc

	// Loop state, owned by the worker goroutine only.
	lastSeq       uint64
	session       Session
	haveSession   bool
	useFallback   bool
	sessionGen    uint64
	awaitingClear bool
	pauseState    string
	pauseDeadline time.Time
	degraded      string
	tempAttempts  int
}

func newWorker(e *Engine, serial string) *worker {
	return &worker{
		e:           e,
		serial:      serial,
		log:         e.log.With("serial", serial),
		controlWake: make(chan struct{}, 1),
		pauseState:  PauseNone,
	}
}

// clearOverrideForSession drops the per-print override only when fresh
// telemetry confirms that the print it applies to is no longer the current
// active session. The decision re-reads telemetry inside controlMu — the
// lock the recording path holds — instead of trusting the snapshot that
// triggered the clear, so a worker processing a stale snapshot can never
// erase a disable that was just recorded for the new current session.
func (w *worker) clearOverrideForSession() {
	w.controlMu.Lock()
	var cur telemetry.SessionView
	if w.e.sessions != nil {
		cur, _ = w.e.sessions.Session(w.serial)
	}
	w.mu.Lock()
	if w.overrideGen != 0 && (!cur.Active || cur.SessionGen != w.overrideGen) {
		w.overrideGen = 0
	}
	w.mu.Unlock()
	w.controlMu.Unlock()
}

// SetDetectionEnabled applies the user's per-print AI toggle and returns
// the authoritative status. enabled=false requires the opaque token of the
// current print session; without an active session, or with a stale token,
// it fails with ErrNoPrintSession or ErrStaleSession. The override lives in
// process memory for the current print only: it survives pause/resume,
// stale telemetry, and printer reconnects, and it clears when the print
// ends, a new print session starts, or the proxy restarts.
func (e *Engine) SetDetectionEnabled(serial string, enabled bool, sessionID string) (any, error) {
	w, ok := e.workers[serial]
	if !ok {
		return nil, ErrUnknownPrinter
	}
	return w.setEnabled(enabled, sessionID)
}

// newSessionNonce draws the per-process prefix for session tokens: 16
// random bytes, hex-encoded, distinct across restarts in practice. On the
// module's Go version crypto/rand.Read always fills the slice or panics,
// so there is no error path.
func newSessionNonce() string {
	var b [16]byte
	crand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// sessionToken renders a print session as the opaque token clients echo
// back to disable detection for that print: the engine's per-process
// nonce plus the session generation. The generation changes only when a
// genuinely new print session starts, and the nonce makes tokens issued
// by a previous proxy process unmatchable, so a stale browser cannot
// disable a new print that reused a low generation.
func (e *Engine) sessionToken(gen uint64) string {
	return e.nonce + "-" + strconv.FormatUint(gen, 10)
}

// setEnabled applies one toggle request. Disabling validates the session
// token against fresh telemetry, records the override for the current
// session generation, invalidates in-flight inspection work, cancels its
// camera wait and API calls, and drops the detection camera hold. The
// supersede and pause-dispatch serialization lives in onResult under the
// same controlMu. Re-enabling only clears the override; the loop then
// resumes with normal eligibility.
func (w *worker) setEnabled(enabled bool, sessionID string) (any, error) {
	if enabled {
		w.controlMu.Lock()
		w.mu.Lock()
		had := w.overrideGen != 0
		w.overrideGen = 0
		if had {
			w.workGen++
		}
		w.mu.Unlock()
		w.controlMu.Unlock()
		if had {
			w.recomputeState()
			w.nudge()
		}
		return w.view(), nil
	}
	if w.e.blocked != "" {
		return nil, ErrDetectionUnavailable
	}
	if spec, ok := w.e.specs[w.serial]; !ok || !config.CameraEligible(spec.Model, spec.Serial) {
		return nil, ErrDetectionUnavailable
	}
	var snap telemetry.SessionView
	ok := false
	if w.e.sessions != nil {
		snap, ok = w.e.sessions.Session(w.serial)
	}
	if !ok || !snap.Active || snap.SessionGen == 0 {
		return nil, ErrNoPrintSession
	}
	if sessionID != w.e.sessionToken(snap.SessionGen) {
		return nil, ErrStaleSession
	}
	w.controlMu.Lock()
	w.mu.Lock()
	changed := w.overrideGen != snap.SessionGen
	w.overrideGen = snap.SessionGen
	if changed {
		// Invalidate any in-flight inspection and cancel its camera wait
		// and API calls; the result can never publish or pause afterwards.
		w.workGen++
	}
	cancel := w.workCancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if changed {
		w.releaseCamera()
	}
	w.controlMu.Unlock()
	if changed {
		w.nudge()
	}
	return w.view(), nil
}

// nudge wakes the worker loop so it re-evaluates after a control change.
func (w *worker) nudge() {
	select {
	case w.controlWake <- struct{}{}:
	default:
	}
}

// cancelWork cancels the current inspection scope, if one exists.
func (w *worker) cancelWork() {
	w.mu.Lock()
	cancel := w.workCancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// publish stores display state changes.
func (w *worker) publish(fn func(st *Status)) {
	w.mu.Lock()
	fn(&w.st)
	w.mu.Unlock()
}

// view snapshots the published status with computed ages. It derives the
// per-print override projection: while the user's disable matches the
// current print session the display reports state disabled with the
// override's lifetime, and every active print carries the session token
// clients echo back to disable.
func (w *worker) view() *Status {
	// Engines wired without a telemetry source (feature-visible wiring
	// probes) still project a valid status.
	var snap telemetry.SessionView
	if w.e.sessions != nil {
		snap, _ = w.e.sessions.Session(w.serial)
	}
	w.mu.Lock()
	s := w.st
	// PauseState lives only in the loop state; the display copy is derived
	// so the two can never disagree.
	s.PauseState = w.pauseState
	overridden := w.overrideGen != 0 && snap.Active && w.overrideGen == snap.SessionGen
	if snap.Active && snap.SessionGen != 0 {
		s.SessionID = w.e.sessionToken(snap.SessionGen)
	} else {
		s.SessionID = ""
	}
	if !w.resultAt.IsZero() {
		s.AgeSeconds = w.e.now().Sub(w.resultAt).Seconds()
	}
	switch s.State {
	case StateMonitoring, StateAttention, StateDegraded:
		if !w.nextDue.IsZero() {
			d := w.nextDue.Sub(w.e.now()).Seconds()
			if d < 0 {
				d = 0
			}
			s.NextCheckSeconds = d
		}
	}
	if w.e.isSuspended() {
		s.Suspended = true
	}
	w.mu.Unlock()
	// Enabled is permission, not activity: false only while the user's
	// override is in force for this exact print session. The disabled
	// projection also refuses to present an in-flight request, a countdown,
	// or camera loss as current.
	if overridden {
		s.Enabled = false
		s.State = StateDisabled
		s.Reason = ReasonUserDisabled
		s.DisabledUntil = DisabledUntilPrintEnd
		s.Processing = false
		s.NextCheckSeconds = 0
		s.CameraLost = false
		s.Message = "AI inspections are off for this print; they return with the next print or after a proxy restart"
	} else {
		s.Enabled = true
		s.DisabledUntil = ""
	}
	return &s
}

// run is the worker main loop. All network waits happen without locks; the
// loop serially enforces one request in flight per printer.
func (w *worker) run(ctx context.Context) {
	defer w.releaseCamera()
	defer w.cancelWork()
	if w.e.blocked != "" {
		w.park(ctx, Status{State: StateBlocked, Reason: w.e.blocked, Message: "detection requires the camera feature"})
		return
	}
	spec, ok := w.e.specs[w.serial]
	if ok && !config.CameraEligible(spec.Model, spec.Serial) {
		w.park(ctx, Status{State: StateUnsupported, Reason: ReasonCameraUnsupported,
			Message: "printer model does not support camera frames"})
		return
	}
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		if suspended, message := w.e.AccountSuspended(); suspended {
			w.parkSuspended(ctx, message)
			return
		}
		delay := w.step(ctx)
		if delay < 0 {
			return
		}
		if !w.sleep(ctx, timer, delay) {
			return
		}
	}
}

// park fixes a terminal display state and waits for shutdown.
func (w *worker) park(ctx context.Context, st Status) {
	w.publish(func(s *Status) { *s = st })
	<-ctx.Done()
}

// parkSuspended keeps the last display state, marks the suspension, releases
// the camera hold, and stops all activity until restart or shutdown.
func (w *worker) parkSuspended(ctx context.Context, message string) {
	w.releaseCamera()
	w.publish(func(s *Status) {
		s.Suspended = true
		s.Message = message
		s.NextCheckSeconds = 0
	})
	<-ctx.Done()
}

// step runs one loop decision and returns the next sleep duration, or -1 on
// shutdown.
func (w *worker) step(ctx context.Context) time.Duration {
	snap, ok := w.e.sessions.Session(w.serial)
	switch {
	case !ok || snap.ObsAt.IsZero():
		// No report observed yet: nothing is known about this printer.
		w.releaseCamera()
		w.publish(func(s *Status) {
			s.State, s.Reason, s.Message = StateStarting, ReasonAwaitingReports, ""
			s.NextCheckSeconds = 0
		})
		return w.e.kIdlePoll

	case !snap.Active:
		w.onIdle()
		return w.e.kIdlePoll

	case w.overrideActive(snap):
		// The user disabled detection for this exact print: park without
		// camera or API activity until it ends or a new print starts.
		return w.onDisabled(snap)

	case !isRunning(snap.State):
		return w.onPauseHold(snap)

	default:
		return w.onRunning(ctx, snap)
	}
}

// overrideActive reports whether the user's disable override is in force
// for the print session in snap.
func (w *worker) overrideActive(snap telemetry.SessionView) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.overrideGen != 0 && w.overrideGen == snap.SessionGen
}

// isRunning reports whether the merged state means the printer is executing.
func isRunning(state string) bool { return state == "RUNNING" }

// onIdle resets session-scoped state when no print session is active.
func (w *worker) onIdle() {
	w.releaseCamera()
	// The session view that routed here may be stale; clear the override
	// only if fresh telemetry agrees its print is gone.
	w.clearOverrideForSession()
	w.mu.Lock()
	hadSession := w.haveSession || w.pauseState != PauseNone
	w.haveSession = false
	w.useFallback = false
	w.lastSeq = 0
	w.sessionGen = 0
	w.awaitingClear = false
	w.pauseState = PauseNone
	w.pauseDeadline = time.Time{}
	w.degraded = ""
	w.tempAttempts = 0
	w.nextDue = time.Time{}
	w.mu.Unlock()
	if hadSession {
		w.publish(func(s *Status) {
			*s = Status{State: StateIdle}
		})
		return
	}
	w.publish(func(s *Status) {
		s.State, s.Reason, s.Message = StateIdle, "", ""
	})
}

// onPauseHold covers an active session that is not running: the printer is
// paused, either by the proxy or by the user. Inspections stop and the
// camera hold is released while the scene is static.
func (w *worker) onPauseHold(snap telemetry.SessionView) time.Duration {
	w.releaseCamera()
	w.mu.Lock()
	switch {
	case w.pauseState == PausePending && isPauseState(snap.State):
		// The printer report confirmed the pause.
		w.pauseState = PauseConfirmed
		w.degraded = ""
		w.mu.Unlock()
		w.publish(func(s *Status) {
			s.Reason, s.Message = "", ""
			s.State = StatePaused
		})
		w.log.Info("pause confirmed by printer report")
		w.e.activity.Record(w.serial, "ai_pause_confirmed", activity.Warning, "Printer confirmed AI pause")
		return w.e.kIdlePoll
	case w.pauseState == PausePending && w.e.now().After(w.pauseDeadline):
		w.pauseState = PauseUnconfirmed
		w.mu.Unlock()
		w.publish(func(s *Status) {
			s.Reason = ReasonPauseUnconfirmed
			s.Message = "pause command was not confirmed within 30s"
			s.State = StateAttention
		})
		w.e.activity.Record(w.serial, "ai_pause_unconfirmed", activity.Error, "Pause was not confirmed within 30 seconds")
		return w.e.kIdlePoll
	default:
		st := w.st.State
		pause := w.pauseState
		w.mu.Unlock()
		w.publish(func(s *Status) {
			s.State = st
			if pause == PauseConfirmed {
				s.State = StatePaused
			}
			s.NextCheckSeconds = 0
		})
		return w.e.kIdlePoll
	}
}

// onDisabled parks a print whose detection the user disabled: no camera
// hold, no inspections, no API calls. Pause outcomes dispatched before the
// override keep updating through the same lifecycle rules as active
// monitoring, so the display never hides a sent pause; the disabled
// display projection is derived in view over anything those rules publish.
func (w *worker) onDisabled(snap telemetry.SessionView) time.Duration {
	w.releaseCamera()
	w.mu.Lock()
	w.nextDue = time.Time{}
	w.mu.Unlock()
	if isRunning(snap.State) {
		w.guardPauseOnRunning()
	} else {
		w.onPauseHold(snap)
	}
	return w.e.kIdlePoll
}

// isPauseState reports whether the report state means the printer is paused.
func isPauseState(state string) bool {
	return state == "PAUSE" || state == "PAUSED"
}

// onRunning handles an active, running session: hold the camera, respect the
// schedule, and run at most one inspection per due tick.
func (w *worker) onRunning(ctx context.Context, snap telemetry.SessionView) time.Duration {
	if w.overrideActive(snap) {
		// The override landed while this iteration was starting: park
		// before acquiring the camera or doing any work.
		return w.onDisabled(snap)
	}
	w.mu.Lock()
	held := w.camHeld
	w.mu.Unlock()
	if !held {
		if !w.e.frames.Acquire(w.serial) {
			return w.degrade(ReasonCameraLost, "camera frames are unavailable", w.e.kCameraRetry, true)
		}
		w.mu.Lock()
		w.camHeld = true
		w.mu.Unlock()
	}
	if w.sessionEpochChanged(snap) {
		w.resetForNewSession(snap.SessionGen)
	}
	w.guardPauseOnRunning()

	now := w.e.now()
	w.mu.Lock()
	due := w.nextDue
	if due.IsZero() {
		// A session just became running: the first inspection fires
		// immediately; the documented 20s default only paces later attempts
		// through the interval floors until the first server response.
		w.nextDue = now
		due = now
	}
	w.mu.Unlock()
	if now.Before(due) {
		w.publish(func(s *Status) {
			if s.State == StateIdle || s.State == StateStarting {
				s.State = StateMonitoring
			}
		})
		return due.Sub(now)
	}
	return w.inspect(ctx, snap)
}

// sessionEpochChanged reports whether the current report belongs to a print
// session other than the one this worker last inspected. Telemetry bumps the
// session generation only at real print starts (a control state entered from
// outside a session, or a new identity while a session is active), so an
// ordinary PAUSE-to-RUNNING resume keeps the context while a coalesced
// FINISH-to-RUNNING overlap that skipped the idle gap still shows up here.
func (w *worker) sessionEpochChanged(snap telemetry.SessionView) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sessionGen != 0 && w.sessionGen != snap.SessionGen
}

// resetForNewSession drops everything scoped to the previous print: the
// Gadget context, any pause lifecycle, and the sticky result. The next
// inspection creates a fresh context and fires immediately. sessionGen is
// the new session's generation, anchored so the reset fires only once.
func (w *worker) resetForNewSession(sessionGen uint64) {
	// A new print never inherits the previous print's override, but a
	// disable recorded for the incoming session must survive this reset;
	// the helper re-validates against fresh telemetry.
	w.clearOverrideForSession()
	w.mu.Lock()
	w.session = Session{}
	w.haveSession = false
	w.useFallback = false
	w.sessionGen = sessionGen
	w.awaitingClear = false
	w.pauseState = PauseNone
	w.pauseDeadline = time.Time{}
	w.degraded = ""
	w.tempAttempts = 0
	w.nextDue = time.Time{}
	w.resultAt = time.Time{}
	w.mu.Unlock()
	w.publish(func(s *Status) { *s = Status{State: StateMonitoring} })
	w.log.Info("new print session detected; gadget context reset")
}

// guardPauseOnRunning applies the confirm-window rules to a RUNNING report:
// a pending pause is never cancelled by RUNNING samples inside the window
// (the printer reacts late); past the deadline it becomes unconfirmed. A
// confirmed pause followed by RUNNING means the user resumed the print, so
// the lifecycle returns to none; the suggestion stays single-shot until a
// fresh clear result rearms it.
func (w *worker) guardPauseOnRunning() {
	w.mu.Lock()
	pause, deadline := w.pauseState, w.pauseDeadline
	w.mu.Unlock()
	switch pause {
	case PausePending:
		if w.e.now().After(deadline) {
			w.mu.Lock()
			w.pauseState = PauseUnconfirmed
			w.mu.Unlock()
			w.publish(func(s *Status) {
				s.Reason = ReasonPauseUnconfirmed
				s.Message = "pause command was not confirmed within 30s"
			})
			w.recomputeState()
			w.e.activity.Record(w.serial, "ai_pause_unconfirmed", activity.Error, "Pause was not confirmed within 30 seconds")
		}
	case PauseConfirmed:
		w.mu.Lock()
		w.pauseState = PauseNone
		w.pauseDeadline = time.Time{}
		w.mu.Unlock()
		w.publish(func(s *Status) { s.Reason, s.Message = "", "" })
		w.recomputeState()
		w.log.Info("print resumed after confirmed pause")
	}
}

// inspect runs exactly one inspection attempt: authorize, capture a fresh
// unique frame, ensure a context, upload, and act on the result. The
// camera wait and both API calls run on the cancellable work context, so a
// control change aborts the attempt instead of letting it finish unnoticed.
func (w *worker) inspect(ctx context.Context, snap telemetry.SessionView) time.Duration {
	gen := w.e.gens.Generation(w.serial)
	if !w.authorized(snap, gen) {
		// Stale or older-generation telemetry: a reconnect's cached RUNNING
		// report must never authorize an action before a fresh observation
		// arrives on the current connection.
		w.publish(func(s *Status) {
			s.Reason = ReasonAwaitingTelemetry
		})
		return w.e.kStalePoll
	}
	// Snapshot this attempt's work scope: startGen is the marker onResult
	// compares against, and wctx carries control-path cancellation into the
	// camera wait and the API calls below. The scope is created here, not
	// only in run, so every attempt — including one after a control change
	// cancelled the previous scope — has a live one; scopes are reused
	// until something cancels them.
	w.mu.Lock()
	if w.workCtx == nil || w.workCtx.Err() != nil {
		w.workCtx, w.workCancel = context.WithCancel(ctx)
	}
	startGen, wctx := w.workGen, w.workCtx
	overridden := w.overrideGen != 0
	w.mu.Unlock()
	if overridden {
		return w.e.kIdlePoll
	}
	frame, ok := w.e.frames.WaitFrame(w.serial, wctx, w.lastSeq, w.e.kFrameWait)
	if wctx.Err() != nil {
		// Superseded by a control change: re-evaluate instead of recording
		// a camera failure.
		return w.e.kStalePoll
	}
	if !ok || w.e.now().Sub(frame.Captured) > w.e.kFrameFreshMax {
		return w.degrade(ReasonCameraLost, "no fresh camera frame for inspection", w.e.kCameraRetry, true)
	}
	w.lastSeq = frame.Seq
	// Layer alignment: the layer the frame actually captured is read from a
	// snapshot taken right after the capture. The entry snapshot stays the
	// authorization anchor, so a session change after this point still
	// invalidates the result below.
	layer := snap.LayerNum
	if cur, ok := w.e.sessions.Session(w.serial); ok {
		layer = cur.LayerNum
	}
	w.mu.Lock()
	w.sessionGen = snap.SessionGen
	w.mu.Unlock()

	if !w.hasSession() {
		rctx, cancel := context.WithTimeout(wctx, requestTimeout)
		session, err := w.e.client.CreateContext(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return -1
			}
			if wctx.Err() != nil {
				return w.e.kStalePoll
			}
			return w.handleFailure(err, false)
		}
		w.mu.Lock()
		w.session = session
		w.haveSession = true
		w.useFallback = false
		w.mu.Unlock()
		w.log.Info("gadget context created")
		w.e.activity.Record(w.serial, "ai_monitoring", activity.Info, "AI inspection started")
	}

	url := w.sessionURL()
	rctx, cancel := context.WithTimeout(wctx, requestTimeout)
	w.publish(func(s *Status) { s.Processing = true })
	res, err := w.e.client.Process(rctx, url, frame.JPEG)
	cancel()
	w.publish(func(s *Status) { s.Processing = false })
	if err != nil {
		if ctx.Err() != nil {
			return -1
		}
		if wctx.Err() != nil {
			// Superseded by a control change: never an API failure.
			return w.e.kStalePoll
		}
		return w.handleFailure(err, true)
	}
	if !w.onResult(snap, gen, frame, res, layer, startGen) {
		// The session ended or changed while the request was in flight: the
		// stale result is never published or acted on. Re-evaluate promptly.
		w.mu.Lock()
		w.nextDue = w.e.now().Add(w.e.kStalePoll)
		w.mu.Unlock()
		return w.e.kStalePoll
	}
	w.mu.Lock()
	delay := w.nextDelay(res)
	w.nextDue = w.e.now().Add(delay)
	w.mu.Unlock()
	return delay
}

// authorized reports whether fresh telemetry on the current connection
// authorizes action for this printer right now. The RUNNING must have been
// explicitly restated by a gcode_state report on the current connection
// (StateGen); a temperature-only delta on a new connection never makes a
// pre-reconnect RUNNING authorize an upload.
func (w *worker) authorized(snap telemetry.SessionView, gen uint64) bool {
	return gen != 0 && snap.ObsGen == gen &&
		snap.StateGen == gen &&
		w.e.now().Sub(snap.ObsAt) <= w.e.kReportFreshMax &&
		isRunning(snap.State)
}

// hasSession reports whether the current print has a Gadget context.
func (w *worker) hasSession() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.haveSession
}

// sessionURL returns the primary process URL, or the sticky fallback after a
// connection/server failure for the rest of this context.
func (w *worker) sessionURL() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.useFallback && w.session.FallbackURL != "" {
		return w.session.FallbackURL
	}
	return w.session.ProcessURL
}

// onResult publishes a successful analysis and enforces the pause policy.
// It first re-validates continuity: a result computed for a print that has
// since ended or changed is discarded entirely, so it can never publish
// stale layers or rearm the pause latch for the next print, and a result
// whose work scope was invalidated by an override change is discarded even
// across a disable/re-enable pair. It returns false when the result was
// discarded.
func (w *worker) onResult(snap telemetry.SessionView, gen uint64, frame Frame, res Result, layer *int, workGen uint64) bool {
	if current, ok := w.e.sessions.Session(w.serial); !ok || !current.Active || current.Epoch != snap.Epoch {
		w.log.Info("inspection result discarded; print session changed during upload")
		return false
	}
	w.mu.Lock()
	w.tempAttempts = 0
	wasDegraded := w.degraded != ""
	w.degraded = ""
	wasWarning := w.st.Warning
	pauseWanted := res.PauseSuggested && !w.awaitingClear
	w.mu.Unlock()

	// The supersede check and the final pause dispatch share controlMu, so
	// a disable that lands during the upload either cancels the pause
	// before it is sent or observes it already sent in the disable
	// response; the two can never interleave halfway.
	w.controlMu.Lock()
	w.mu.Lock()
	superseded := workGen != w.workGen || w.overrideGen != 0
	w.mu.Unlock()
	if superseded {
		w.controlMu.Unlock()
		w.log.Info("inspection result discarded; detection availability changed during upload")
		return false
	}
	pauseState := ""
	message := ""
	reason := ""
	pauseAuthorized := false
	attempted := false
	if pauseWanted {
		pauseAuthorized = w.pauseAuthorized(snap, gen, frame)
		if pauseAuthorized {
			err := w.e.pauser.PausePrint(w.serial, gen)
			attempted = true
			if err != nil {
				// Guarded and final: never retried, never replayed.
				pauseState = PauseUnconfirmed
				reason = ReasonPauseFailed
				message = "pause command failed: " + err.Error()
				w.log.Warn("pause command rejected", "error", err.Error())
			} else {
				pauseState = PausePending
				message = "pause sent; waiting for printer confirmation"
				w.log.Warn("pause command published", "quality", res.PrintQuality)
			}
		} else {
			// The print changed while the request was in flight (PAUSE to
			// RUNNING, a reconnect, or stale telemetry): discard the result.
			w.log.Info("pause suggestion discarded; print state changed during inspection")
		}
	}
	// The pause outcome moves into loop state while controlMu is still
	// held, so a disable acknowledgment taken after this section can never
	// report pause_state=none for a pause that was already sent.
	w.mu.Lock()
	clear := !res.WarningSuggested && !res.PauseSuggested
	if attempted {
		// Loop state tracks every terminal pause outcome, including a
		// rejected command, so the display can never contradict it.
		w.pauseState = pauseState
		// The suggestion is single-shot for any attempted pause, sent or
		// rejected: nothing is retried until a fresh clear result rearms it.
		w.awaitingClear = true
		if pauseState == PausePending {
			w.pauseDeadline = w.e.now().Add(w.e.kConfirmWait)
		}
	}
	if clear {
		w.awaitingClear = false
	}
	w.resultAt = w.e.now()
	w.mu.Unlock()
	w.controlMu.Unlock()

	w.publish(func(s *Status) {
		s.Quality = res.PrintQuality
		s.Warning = res.WarningSuggested
		s.FasterInspection = res.FasterInspectionSuggested
		s.CameraLost = false
		s.LastInspectedLayer = layer
		s.Reason = reason
		s.Message = message
		if clear && pauseState == "" {
			s.Reason, s.Message = "", ""
		}
	})
	if wasDegraded {
		w.e.activity.Record(w.serial, "ai_recovered", activity.Info, "AI inspection recovered")
	}
	if res.WarningSuggested && !wasWarning {
		w.e.activity.Record(w.serial, "ai_warning", activity.Warning,
			fmt.Sprintf("Possible print failure · quality %d/10", res.PrintQuality))
	} else if !res.WarningSuggested && wasWarning {
		w.e.activity.Record(w.serial, "ai_warning_cleared", activity.Info, "AI warning cleared")
	}
	if attempted {
		if pauseState == PausePending {
			w.e.activity.Record(w.serial, "ai_pause_sent", activity.Error, "AI requested a pause; waiting for printer confirmation")
		} else {
			w.e.activity.Record(w.serial, "ai_pause_failed", activity.Error, "AI pause command could not be sent")
		}
	}
	w.recomputeState()
	return true
}

// pauseAuthorized re-validates every action precondition after the upload
// returns: same session epoch (so a PAUSE to RUNNING transition during the
// in-flight request cancels the pause), still running, telemetry fresh on
// the same connection generation with RUNNING explicitly restated on it,
// and a still-recent frame.
func (w *worker) pauseAuthorized(snap telemetry.SessionView, gen uint64, frame Frame) bool {
	if w.e.now().Sub(frame.Captured) > w.e.kResultValidity {
		return false
	}
	current, ok := w.e.sessions.Session(w.serial)
	if !ok || !current.Active {
		return false
	}
	nowGen := w.e.gens.Generation(w.serial)
	return current.Epoch == snap.Epoch &&
		isRunning(current.State) &&
		nowGen == gen && gen != 0 &&
		current.ObsGen == nowGen &&
		current.StateGen == nowGen &&
		w.e.now().Sub(current.ObsAt) <= w.e.kReportFreshMax
}

// nextDelay computes the next inspection interval from the server's timing
// guidance, never below its minimum; the client applies the documented
// interval floors when a response omits them.
func (w *worker) nextDelay(res Result) time.Duration {
	d := time.Duration(res.Intervals.Recommended) * time.Second
	if min := time.Duration(res.Intervals.Minimum) * time.Second; min > d {
		d = min
	}
	return d
}

// handleFailure classifies an API or transport failure: terminal account
// errors suspend detection account-wide; everything else backs off with the
// last result left in place (sticky fallback) and, for connection or server
// failures, switches to the context's fallback URL.
func (w *worker) handleFailure(err error, processCall bool) time.Duration {
	var apiErr *APIError
	switch {
	case errors.As(err, &apiErr) && apiErr.AccountTerminal():
		w.e.suspend(apiErr.Error())
		return -1
	case IsFrameTooLarge(err):
		// Locally rejected before any request; the next fresh frame will
		// likely also exceed the cap, so back off without URL changes.
		return w.degrade(ReasonAPIRetrying, err.Error(), w.tempBackoff(), false)
	default:
		delay := w.tempBackoff()
		if processCall {
			var apiErr *APIError
			isAPI := errors.As(err, &apiErr)
			switch {
			case isAPI && apiErr.SwitchToFallback():
				w.mu.Lock()
				if !w.useFallback {
					w.useFallback = true
					w.log.Info("switching to fallback process URL for this context")
				}
				w.mu.Unlock()
			case !isAPI:
				// Transport-level failure: switch for this context.
				w.mu.Lock()
				if !w.useFallback {
					w.useFallback = true
					w.log.Info("switching to fallback process URL after transport failure")
				}
				w.mu.Unlock()
			}
		}
		return w.degrade(ReasonAPIRetrying, "inspection temporarily failing; will retry", delay, false)
	}
}

// tempBackoff returns the bounded exponential retry delay for temporary
// failures: 20s doubling to a 10m cap with +-20% jitter, reset on success.
func (w *worker) tempBackoff() time.Duration {
	w.mu.Lock()
	n := w.tempAttempts
	w.tempAttempts++
	w.mu.Unlock()
	d := tempBackoffInitial
	for i := 0; i < n && d < tempBackoffMax; i++ {
		d *= 2
	}
	if d > tempBackoffMax {
		d = tempBackoffMax
	}
	return time.Duration(float64(d) * (1 + (rand.Float64()*0.4 - 0.2)))
}

// degrade records a degraded condition with the sticky result preserved.
func (w *worker) degrade(reason, message string, delay time.Duration, cameraLost bool) time.Duration {
	w.mu.Lock()
	newDegradation := w.degraded == ""
	w.degraded = reason
	w.nextDue = w.e.now().Add(delay)
	w.mu.Unlock()
	w.publish(func(s *Status) {
		s.Reason = reason
		s.Message = message
		s.CameraLost = s.CameraLost || cameraLost
	})
	w.recomputeState()
	if newDegradation {
		w.e.activity.Record(w.serial, "ai_degraded", activity.Warning, message)
	}
	return delay
}

// recomputeState derives the display state from the pause lifecycle and the
// degraded condition.
func (w *worker) recomputeState() {
	w.mu.Lock()
	var state string
	switch {
	case w.pauseState == PauseConfirmed:
		state = StatePaused
	case w.degraded != "":
		state = StateDegraded
	case w.pauseState == PausePending || w.pauseState == PauseUnconfirmed || w.st.Warning:
		state = StateAttention
	default:
		state = StateMonitoring
	}
	w.st.State = state
	w.mu.Unlock()
}

// releaseCamera drops the camera hold exactly once per acquisition.
func (w *worker) releaseCamera() {
	w.mu.Lock()
	held := w.camHeld
	w.camHeld = false
	w.mu.Unlock()
	if held {
		w.e.frames.Release(w.serial)
	}
}

// sleep waits for the next tick, a report wake, a suspension, or shutdown.
// It returns false only on shutdown.
func (w *worker) sleep(ctx context.Context, timer *time.Timer, d time.Duration) bool {
	if d < time.Millisecond {
		d = time.Millisecond
	}
	timer.Reset(d)
	if w.wake == nil {
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return false
		case <-w.e.suspendCh:
			stopTimer(timer)
			return true
		case <-w.controlWake:
			stopTimer(timer)
			return true
		case <-timer.C:
			return true
		}
	}
	select {
	case <-ctx.Done():
		stopTimer(timer)
		return false
	case <-w.e.suspendCh:
		stopTimer(timer)
		return true
	case <-w.wake:
		stopTimer(timer)
		return true
	case <-w.controlWake:
		stopTimer(timer)
		return true
	case <-timer.C:
		return true
	}
}

// stopTimer stops a timer and drains a pending fire so Reset stays safe.
func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
