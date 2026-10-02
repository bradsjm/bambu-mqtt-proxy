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
	// intensivePeriod holds inspection at the minimum interval after a
	// print starts, resumes, or recovers from unavailable monitoring.
	intensivePeriod = 2 * time.Minute
	// intensiveClearCount and intensiveClearSpan define clear evidence for
	// leaving a risk-triggered intensive period.
	intensiveClearCount = 3
	intensiveClearSpan  = 30 * time.Second
	// idlePoll paces parked workers; stalePoll re-checks authorization while
	// waiting for fresh current-generation telemetry; cameraRetry paces
	// retries while the camera cannot serve a fresh frame.
	idlePoll    = 5 * time.Second
	stalePoll   = 5 * time.Second
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
	ReasonRequestInvalid    = "request_invalid"
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
	Intensive          bool    `json:"intensive,omitempty"`
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

// PrinterControl sends guarded upstream print commands; each action must fail
// rather than act on a connection older than generation.
type PrinterControl interface {
	PausePrint(serial string, generation uint64) error
	SetSpeedProfile(serial string, generation uint64, profile int) error
	SetChamberLight(serial string, generation uint64, on bool) error
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
	control  PrinterControl
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
	sessions sessionSource, control PrinterControl, gens GenerationSource, log *slog.Logger) *Engine {
	if log != nil {
		log = log.With("origin", "detection")
	}
	if logger, ok := client.(interface{ setLogger(*slog.Logger) }); ok && log != nil {
		logger.setLogger(log.With("component", "gadget_client"))
	}
	specs := make(map[string]config.Printer, len(printers))
	workers := make(map[string]*worker, len(printers))
	e := &Engine{
		client:          client,
		frames:          frames,
		sessions:        sessions,
		control:         control,
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
	// lightGen is the session generation whose chamber light auto-on was
	// attempted or found unnecessary because the light was already on.
	// Guarded by mu.
	lightGen uint64

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
	// Policy state stays within one print. latestIntervals governs cadence
	// and retries; nextDue survives pause/resume and override transitions.
	latestIntervals IntervalSec
	haveIntervals   bool
	intensiveUntil  time.Time
	riskActive      bool
	clearCount      int
	clearSince      time.Time
	lastLayer       *int
	wasPaused       bool
	blockedReason   string
	lastFailure     string

	// Speed ownership and warning episode state are guarded by controlMu.
	// One warning episode sends at most one slowdown and restores only a
	// profile the proxy owns.
	warningEpisode  bool
	speedAttempted  bool
	speedPrior      *int
	speedPrintGen   uint64
	speedCommandGen uint64
	speedCommandObs uint64
	speedAckObs     uint64
	speedPending    bool
	speedOwned      bool
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

// diagnosticAttrs returns concise engine state for info and warning events.
func (w *worker) diagnosticAttrs() []any {
	now := w.e.now()
	attrs := []any{"connection_generation", w.e.gens.Generation(w.serial)}
	if w.e.sessions != nil {
		if snap, ok := w.e.sessions.Session(w.serial); ok {
			attrs = append(attrs,
				"print_state", snap.State,
				"session_generation", snap.SessionGen,
				"session_epoch", snap.Epoch,
				"telemetry_generation", snap.ObsGen,
				"telemetry_age_ms", now.Sub(snap.ObsAt).Milliseconds())
			if snap.LayerNum != nil {
				attrs = append(attrs, "layer", *snap.LayerNum)
			}
		}
	}
	w.mu.Lock()
	attrs = append(attrs,
		"using_fallback", w.useFallback,
		"pause_state", w.pauseState,
		"retry_attempt", w.tempAttempts,
		"intensive_inspection", w.intensiveLocked(now),
		"degraded_reason", w.degraded)
	w.mu.Unlock()
	return attrs
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
// same controlMu. Re-enabling clears the override and permits one new
// chamber-light attempt for the active session.
func (w *worker) setEnabled(enabled bool, sessionID string) (any, error) {
	if enabled {
		w.controlMu.Lock()
		w.mu.Lock()
		had := w.overrideGen != 0
		w.overrideGen = 0
		if had {
			w.workGen++
			// Re-enabled AI can make one new light-on attempt.
			w.lightGen = 0
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
	s.Intensive = w.intensiveLocked(w.e.now())
	s.UsingFallback = w.useFallback
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
	w.mu.Lock()
	blocked := w.blockedReason
	w.mu.Unlock()
	if blocked != "" {
		w.releaseCamera()
		w.publish(func(s *Status) {
			s.State, s.Reason, s.Message = StateBlocked, ReasonRequestInvalid, blocked
			s.Processing = false
			s.NextCheckSeconds = 0
		})
		return w.e.kIdlePoll
	}
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

	default:
		// Session ownership follows real print generations. Never restore an
		// old print's profile when a new session or a detection disable lands.
		w.controlMu.Lock()
		w.ensureSpeedSession(snap)
		if !w.overrideActive(snap) {
			w.reconcileSpeed(snap)
		}
		w.controlMu.Unlock()
		if w.overrideActive(snap) {
			// The user disabled detection for this exact print: park without
			// camera or API activity until it ends or a new print starts.
			return w.onDisabled(snap)
		}
		w.ensureLight(snap)
		if !isRunning(snap.State) {
			return w.onPauseHold(snap)
		}
		return w.onRunning(ctx, snap)
	}
}

// ensureLight tries once per AI-monitored print session to turn the
// chamber light on, so inspections see the print. A failed attempt is not
// retried; the user can switch the light on.
func (w *worker) ensureLight(snap telemetry.SessionView) {
	w.mu.Lock()
	if snap.SessionGen == 0 || w.lightGen == snap.SessionGen {
		w.mu.Unlock()
		return
	}
	if snap.ChamberLight == "on" {
		w.lightGen = snap.SessionGen
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()
	gen := w.e.gens.Generation(w.serial)
	if gen != snap.ObsGen {
		return
	}
	w.mu.Lock()
	w.lightGen = snap.SessionGen
	w.mu.Unlock()
	if err := w.e.control.SetChamberLight(w.serial, gen, true); err != nil {
		w.log.Warn("chamber light command failed", append(w.diagnosticAttrs(), "error", err)...)
		return
	}
	w.e.activity.Record(w.serial, "ai_light_on", activity.Info, "Chamber light turned on for AI inspection")
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
	w.controlMu.Lock()
	w.mu.Lock()
	hadSession := w.haveSession || w.pauseState != PauseNone
	w.haveSession = false
	w.useFallback = false
	w.lastSeq = 0
	w.sessionGen = 0
	w.awaitingClear = false
	w.resetSpeedState(0)
	w.pauseState = PauseNone
	w.pauseDeadline = time.Time{}
	w.degraded = ""
	w.tempAttempts = 0
	w.nextDue = time.Time{}
	w.latestIntervals = IntervalSec{}
	w.haveIntervals = false
	w.intensiveUntil = time.Time{}
	w.riskActive = false
	w.clearCount = 0
	w.clearSince = time.Time{}
	w.lastLayer = nil
	w.wasPaused = false
	w.lastFailure = ""
	w.mu.Unlock()
	w.controlMu.Unlock()
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
	if isPauseState(snap.State) {
		w.wasPaused = true
	}
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
		w.log.Info("pause confirmed by printer report", w.diagnosticAttrs()...)
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
	if isRunning(snap.State) {
		w.noteRunningTransition()
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
	if w.sessionEpochChanged(snap) || w.sessionGen == 0 {
		w.resetForNewSession(snap.SessionGen)
	}
	w.noteRunningTransition()
	w.mu.Lock()
	w.lastLayer = copyLayer(snap.LayerNum)
	w.mu.Unlock()
	w.guardPauseOnRunning()

	now := w.e.now()
	freshAuthorized := w.authorized(snap, w.e.gens.Generation(w.serial))

	// Layer gate: never acquire frames or upload while the current print's
	// layer is unknown or zero. The session view exposes layer_num only when
	// telemetry observed it on this print session and connection, so a
	// previous job's sticky layer can never authorize a new print. While
	// waiting, the degrade below sets a poll, and the freshness recovery
	// under it fires immediately on the first report that carries an
	// eligible layer. The wait releases the camera hold, so a print that
	// cannot be inspected yet does not retain the capture.
	if layerValue(snap.LayerNum) <= 0 {
		if w.log.Enabled(context.Background(), slog.LevelDebug) {
			w.log.Debug("detection inspection deferred", append(w.diagnosticAttrs(),
				"operation", "process", "reason", "layer_unknown")...)
		}
		w.releaseCamera()
		return w.degrade(ReasonAwaitingTelemetry, "waiting for the printer's current layer report", w.e.kStalePoll, false)
	}

	// Authorization gate: stale telemetry or an old transport generation is
	// rejected here, before the camera hold or any request, so a print
	// waiting for fresh evidence never takes the camera. inspect keeps its
	// own recheck as the defense for the race between this gate and the
	// capture.
	if !freshAuthorized {
		if w.log.Enabled(context.Background(), slog.LevelDebug) {
			w.log.Debug("detection inspection deferred", append(w.diagnosticAttrs(),
				"operation", "process", "reason", "telemetry_not_authorized")...)
		}
		w.releaseCamera()
		return w.degrade(ReasonAwaitingTelemetry, "waiting for fresh printer telemetry", w.e.kStalePoll, false)
	}

	w.mu.Lock()
	// A fresh current-generation state observation can arrive before the
	// stale-telemetry poll expires. No upload was made while authorization was
	// stale, so do not make the printer wait for that poll after it recovers.
	if w.degraded == ReasonAwaitingTelemetry && freshAuthorized {
		w.nextDue = now
	}
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
	w.reserveAttempt()
	w.mu.Lock()
	held := w.camHeld
	w.mu.Unlock()
	if !held {
		if !w.e.frames.Acquire(w.serial) {
			if w.log.Enabled(context.Background(), slog.LevelDebug) {
				w.log.Debug("detection inspection deferred", append(w.diagnosticAttrs(),
					"operation", "process", "reason", "camera_acquire_failed",
					"retry_seconds", w.e.kCameraRetry.Seconds())...)
			}
			return w.degrade(ReasonCameraLost, "camera frames are unavailable; waiting before retry", w.e.kCameraRetry, true)
		}
		w.mu.Lock()
		w.camHeld = true
		w.mu.Unlock()
	}
	return w.inspect(ctx, snap)
}

// gadgetRequestContext attaches correlation and inspection details to one API request.
func (w *worker) gadgetRequestContext(ctx context.Context, action string, snap telemetry.SessionView,
	gen uint64, frame *Frame, layer *int) (context.Context, requestLogContext) {
	now := w.e.now()
	detail := requestLogContext{
		requestID:            fmt.Sprintf("detect-%s-%s-%d-%d-%d-%d", w.serial, action, snap.SessionGen, snap.Epoch, gen, w.lastSeq),
		action:               action,
		serial:               w.serial,
		inspectionReason:     "scheduled_inspection",
		printState:           snap.State,
		sessionGeneration:    snap.SessionGen,
		sessionEpoch:         snap.Epoch,
		connectionGeneration: gen,
		telemetryAgeMS:       now.Sub(snap.ObsAt).Milliseconds(),
		layer:                copyLayer(layer),
	}
	w.mu.Lock()
	if !w.haveSession {
		detail.inspectionReason = "first_inspection_for_print"
	} else if w.tempAttempts > 0 {
		detail.inspectionReason = "retry_after_failure"
	} else if w.riskActive {
		detail.inspectionReason = "risk_triggered_intensive_inspection"
	} else if w.lastLayer != nil && *w.lastLayer > 0 && *w.lastLayer <= 3 {
		detail.inspectionReason = "early_layer_intensive_inspection"
	} else if now.Before(w.intensiveUntil) {
		detail.inspectionReason = "startup_or_recovery_intensive_inspection"
	} else {
		detail.inspectionReason = "routine_provider_interval"
	}
	detail.frameSequence = w.lastSeq
	detail.retryAttempt = w.tempAttempts
	detail.intensive = w.intensiveLocked(now)
	detail.usingFallback = w.useFallback
	w.mu.Unlock()
	if frame != nil {
		detail.frameSequence = frame.Seq
		detail.frameBytes = len(frame.JPEG)
		detail.frameAgeMS = now.Sub(frame.Captured).Milliseconds()
	}
	return withRequestLogContext(ctx, detail), detail
}

// noteRunningTransition starts a two-minute intensive period after a
// previously observed PAUSE or PAUSED state returns to RUNNING.
func (w *worker) noteRunningTransition() {
	w.mu.Lock()
	if w.wasPaused {
		w.intensiveUntil = w.e.now().Add(intensivePeriod)
		w.wasPaused = false
	}
	w.mu.Unlock()
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
	w.controlMu.Lock()
	w.mu.Lock()
	w.session = Session{}
	w.haveSession = false
	w.useFallback = false
	w.sessionGen = sessionGen
	w.resetSpeedState(sessionGen)
	w.awaitingClear = false
	w.pauseState = PauseNone
	w.pauseDeadline = time.Time{}
	w.degraded = ""
	w.tempAttempts = 0
	w.nextDue = time.Time{}
	w.resultAt = time.Time{}
	w.latestIntervals = IntervalSec{}
	w.haveIntervals = false
	w.intensiveUntil = w.e.now().Add(intensivePeriod)
	w.riskActive = false
	w.clearCount = 0
	w.clearSince = time.Time{}
	w.lastLayer = nil
	w.wasPaused = false
	w.lastFailure = ""
	w.mu.Unlock()
	w.controlMu.Unlock()
	w.publish(func(s *Status) { *s = Status{State: StateMonitoring} })
	w.log.Info("new print session detected; gadget context reset", w.diagnosticAttrs()...)
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
		w.log.Info("print resumed after confirmed pause", w.diagnosticAttrs()...)
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
		if w.log.Enabled(context.Background(), slog.LevelDebug) {
			w.log.Debug("detection inspection deferred", append(w.diagnosticAttrs(),
				"operation", "process", "reason", "telemetry_not_authorized",
				"inspection_session_generation", snap.SessionGen,
				"inspection_session_epoch", snap.Epoch,
				"inspection_connection_generation", gen,
				"inspection_telemetry_generation", snap.ObsGen,
				"inspection_state_generation", snap.StateGen,
				"inspection_state", snap.State,
				"inspection_telemetry_age_ms", w.e.now().Sub(snap.ObsAt).Milliseconds())...)
		}
		return w.degrade(ReasonAwaitingTelemetry, "waiting for fresh printer telemetry", w.e.kStalePoll, false)
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
	frameWaitStarted := time.Now()
	frame, ok := w.e.frames.WaitFrame(w.serial, wctx, w.lastSeq, w.e.kFrameWait)
	frameWaitDuration := time.Since(frameWaitStarted)
	if wctx.Err() != nil {
		// Superseded by a control change: re-evaluate instead of recording
		// a camera failure.
		return w.e.kStalePoll
	}
	if !ok || w.e.now().Sub(frame.Captured) > w.e.kFrameFreshMax {
		if w.log.Enabled(context.Background(), slog.LevelDebug) {
			w.log.Debug("detection inspection deferred", append(w.diagnosticAttrs(),
				"operation", "process", "reason", "no_fresh_camera_frame",
				"last_frame_sequence", w.lastSeq,
				"frame_wait_duration_ms", frameWaitDuration.Milliseconds(),
				"frame_wait_limit_ms", w.e.kFrameWait.Milliseconds())...)
		}
		return w.degrade(ReasonCameraLost, "no fresh camera frame for inspection", w.e.kCameraRetry, true)
	}
	if w.log.Enabled(context.Background(), slog.LevelDebug) {
		w.log.Debug("fresh camera frame acquired for inspection", append(w.diagnosticAttrs(),
			"operation", "process", "frame_sequence", frame.Seq,
			"frame_bytes", len(frame.JPEG),
			"frame_age_ms", w.e.now().Sub(frame.Captured).Milliseconds(),
			"frame_wait_duration_ms", frameWaitDuration.Milliseconds(),
			"frame_wait_limit_ms", w.e.kFrameWait.Milliseconds())...)
	}
	w.lastSeq = frame.Seq
	// Layer alignment: the layer the frame actually captured is read from a
	// snapshot taken right after the capture. The entry snapshot stays the
	// authorization anchor, so a session change after this point still
	// invalidates the result below.
	layer := snap.LayerNum
	cur, ok := w.e.sessions.Session(w.serial)
	if ok {
		layer = cur.LayerNum
	}
	// Capture-time recheck: the world moved while the camera waited. The
	// frame may only upload against the print and connection it was
	// captured for: the pool generation must still match the generation
	// captured at entry (a reconnect must not ride a restated state), the
	// current view must keep the same print session and epoch, the full
	// entry authorization must still hold for the current view — authorized
	// covers RUNNING on the current connection, its generations, and the
	// ObsAt freshness bound at the capture moment — and the layer the frame
	// captured must be known and positive. onResult would discard such a
	// result anyway; catching it here also keeps the request from happening.
	if !ok ||
		w.e.gens.Generation(w.serial) != gen ||
		cur.Epoch != snap.Epoch || cur.SessionGen != snap.SessionGen ||
		!w.authorized(cur, gen) ||
		layerValue(layer) <= 0 {
		if w.log.Enabled(context.Background(), slog.LevelDebug) {
			w.log.Debug("detection inspection deferred", append(w.diagnosticAttrs(),
				"operation", "process", "reason", "print_changed_during_capture",
				"inspection_epoch", snap.Epoch, "current_epoch", cur.Epoch,
				"current_session_generation", cur.SessionGen,
				"current_connection_generation", w.e.gens.Generation(w.serial),
				"captured_connection_generation", gen,
				"current_telemetry_generation", cur.ObsGen,
				"current_layer", layerValue(layer))...)
		}
		return w.degrade(ReasonAwaitingTelemetry, "waiting for a fresh printer report during frame capture", w.e.kStalePoll, false)
	}
	w.mu.Lock()
	w.sessionGen = snap.SessionGen
	w.lastLayer = copyLayer(layer)
	w.mu.Unlock()

	if !w.hasSession() {
		rctx, cancel := context.WithTimeout(wctx, requestTimeout)
		rctx, detail := w.gadgetRequestContext(rctx, "create_context", snap, gen, &frame, layer)
		started := time.Now()
		session, err := w.e.client.CreateContext(rctx)
		cancel()
		if err != nil {
			if w.log.Enabled(context.Background(), slog.LevelDebug) {
				completion := append(detail.logAttrs(), "operation", "create",
					"duration_ms", time.Since(started).Milliseconds(), "error", err.Error())
				w.log.Debug("gadget operation completed", completion...)
			}
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
		w.log.Info("gadget context created", "operation", "create",
			"session_generation", snap.SessionGen,
			"connection_generation", gen,
			"frame_sequence", frame.Seq,
			"inspection_reason", detail.inspectionReason,
			"duration_ms", time.Since(started).Milliseconds())
		w.e.activity.Record(w.serial, "ai_monitoring", activity.Info, "AI inspection started")
	}

	url := w.sessionURL()
	rctx, cancel := context.WithTimeout(wctx, requestTimeout)
	rctx, detail := w.gadgetRequestContext(rctx, "process", snap, gen, &frame, layer)
	w.publish(func(s *Status) { s.Processing = true })
	started := time.Now()
	res, err := w.e.client.Process(rctx, url, frame.JPEG)
	if validIntervals(res.Intervals) {
		w.rememberIntervals(res.Intervals)
	}
	cancel()
	w.publish(func(s *Status) { s.Processing = false })
	if w.log.Enabled(context.Background(), slog.LevelDebug) {
		completion := append(detail.logAttrs(),
			"operation", "process",
			"duration_ms", time.Since(started).Milliseconds(),
			"using_fallback", w.usingFallback(),
			"response_minimum_interval_seconds", res.Intervals.Minimum,
			"response_recommended_interval_seconds", res.Intervals.Recommended,
			"print_quality", res.PrintQuality,
			"warning_suggested", res.WarningSuggested,
			"pause_suggested", res.PauseSuggested,
			"faster_inspection_suggested", res.FasterInspectionSuggested,
			"score", res.Score)
		if err != nil {
			completion = append(completion, "error", err.Error())
		}
		w.log.Debug("gadget operation completed", completion...)
	}
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
	// The session ended or changed while the request was in flight: onResult
	// discards the stale result, but valid response timing still paces this
	// same print context.
	accepted := w.onResult(snap, gen, frame, res, layer, startGen)
	delay := w.scheduleResponse(res.Intervals)
	if w.log.Enabled(context.Background(), slog.LevelDebug) {
		w.log.Debug("gadget response cadence scheduled", append(w.responseCadenceAttrs(delay),
			"result_accepted", accepted)...)
	}
	return delay
}

// responseCadenceAttrs explains the policy inputs that selected the next check.
func (w *worker) responseCadenceAttrs(delay time.Duration) []any {
	w.mu.Lock()
	defer w.mu.Unlock()
	intensive := w.intensiveLocked(w.e.now())
	source := "provider_recommended_interval"
	if intensive {
		source = "provider_minimum_interval_intensive"
	} else if !w.haveIntervals {
		source = "default_retry_floor"
	}
	return []any{
		"session_generation", w.sessionGen,
		"connection_generation", w.e.gens.Generation(w.serial),
		"using_fallback", w.useFallback,
		"cadence_source", source,
		"provider_minimum_interval_seconds", w.latestIntervals.Minimum,
		"provider_recommended_interval_seconds", w.latestIntervals.Recommended,
		"required_retry_floor_seconds", w.requiredFloorLocked().Seconds(),
		"intensive_inspection", intensive,
		"next_check_seconds", delay.Seconds(),
		"retry_attempt", w.tempAttempts,
	}
}

// copyLayer snapshots a telemetry layer pointer for status and cadence.
func copyLayer(layer *int) *int {
	if layer == nil {
		return nil
	}
	v := *layer
	return &v
}

// validIntervals accepts positive guidance that fits a time.Duration.
func validIntervals(v IntervalSec) bool {
	return v.Minimum > 0 && v.Recommended > 0 &&
		int64(v.Minimum) <= maxIntervalSeconds && int64(v.Recommended) <= maxIntervalSeconds
}

// rememberIntervals stores valid same-context provider timing.
func (w *worker) rememberIntervals(v IntervalSec) {
	if !validIntervals(v) {
		return
	}
	w.mu.Lock()
	w.latestIntervals = v
	w.haveIntervals = true
	w.mu.Unlock()
}

// usingFallback reports the active Process URL mode for diagnostics.
func (w *worker) usingFallback() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.useFallback
}

// scheduleResponse stores a due time from the latest response guidance.
func (w *worker) scheduleResponse(intervals IntervalSec) time.Duration {
	w.rememberIntervals(intervals)
	w.mu.Lock()
	delay := w.nextDelayLocked()
	w.nextDue = w.e.now().Add(delay)
	w.mu.Unlock()
	return delay
}

// reserveAttempt preserves a safe retry floor if a control change cancels
// the attempt before a provider response supplies newer timing.
func (w *worker) reserveAttempt() {
	w.mu.Lock()
	// Reserve a safe next due time for an attempt that may be canceled, but
	// count backoff only when the attempt actually fails.
	delay := w.backoffDelayLocked(w.tempAttempts)
	w.nextDue = w.e.now().Add(delay)
	w.mu.Unlock()
}

// requiredFloorLocked returns the max(Minimum, Recommended) provider retry floor.
func (w *worker) requiredFloorLocked() time.Duration {
	providerFloor := time.Duration(0)
	if w.haveIntervals {
		minimum := time.Duration(w.latestIntervals.Minimum) * time.Second
		recommended := time.Duration(w.latestIntervals.Recommended) * time.Second
		providerFloor = max(minimum, recommended)
	}
	return max(tempBackoffInitial, providerFloor)
}

// nextDelayLocked applies the normal or protection-first success cadence.
func (w *worker) nextDelayLocked() time.Duration {
	if !w.haveIntervals {
		return tempBackoffInitial
	}
	minimum := time.Duration(w.latestIntervals.Minimum) * time.Second
	if w.intensiveLocked(w.e.now()) {
		return minimum
	}
	recommended := time.Duration(w.latestIntervals.Recommended) * time.Second
	if recommended > minimum {
		return recommended
	}
	return minimum
}

// retryDelayLocked applies bounded jitter without dropping below provider
// timing or exceeding ten minutes unless the required provider floor does.
func (w *worker) retryDelayLocked() time.Duration {
	n := w.tempAttempts
	w.tempAttempts++
	return w.backoffDelayLocked(n)
}

// backoffDelayLocked calculates one jittered retry delay without changing the
// failure count. max(Minimum, Recommended) is always a floor, even in intensive mode.
func (w *worker) backoffDelayLocked(n int) time.Duration {
	d := tempBackoffInitial
	for range n {
		if d >= tempBackoffMax/2 {
			d = tempBackoffMax
			break
		}
		d *= 2
	}
	if d > tempBackoffMax {
		d = tempBackoffMax
	}
	d = time.Duration(float64(d) * (1 + (rand.Float64()*0.4 - 0.2)))
	floor := w.requiredFloorLocked()
	if d < floor {
		d = floor
	}
	cap := tempBackoffMax
	if floor > cap {
		cap = floor
	}
	if d > cap {
		d = cap
	}
	return d
}

// intensiveLocked reports the timed, layer-based, or risk-based fast mode.
func (w *worker) intensiveLocked(now time.Time) bool {
	return (!w.intensiveUntil.IsZero() && now.Before(w.intensiveUntil)) ||
		(w.lastLayer != nil && *w.lastLayer > 0 && *w.lastLayer <= 3) || w.riskActive
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

// resetSpeedState drops every profile decision tied to one print.
func (w *worker) resetSpeedState(sessionGen uint64) {
	w.warningEpisode = false
	w.speedAttempted = false
	w.speedPrior = nil
	w.speedPrintGen = sessionGen
	w.speedCommandGen = 0
	w.speedCommandObs = 0
	w.speedAckObs = 0
	w.speedPending = false
	w.speedOwned = false
}

// ensureSpeedSession drops ownership at a print boundary. A saved profile is
// never carried into another print.
func (w *worker) ensureSpeedSession(snap telemetry.SessionView) {
	if w.speedPrintGen != snap.SessionGen {
		w.resetSpeedState(snap.SessionGen)
	}
}

// freshSpeed reports a profile that belongs to this active print and the
// current connection, rather than a cached value from an earlier session.
func (w *worker) freshSpeed(snap telemetry.SessionView, gen uint64) bool {
	return gen != 0 && snap.Active && snap.SessionGen != 0 &&
		snap.SpeedProfile != nil && snap.SpeedSessionGen == snap.SessionGen &&
		snap.SpeedGen == gen && !snap.SpeedAt.IsZero() &&
		w.e.now().Sub(snap.SpeedAt) <= w.e.kReportFreshMax
}

// speedAuthorized rechecks the same print, state, connection, and result
// freshness required for a printer action, plus a current speed report.
func (w *worker) speedAuthorized(snap telemetry.SessionView, gen uint64, frame Frame) (telemetry.SessionView, bool) {
	if !w.pauseAuthorized(snap, gen, frame) {
		return telemetry.SessionView{}, false
	}
	current, ok := w.e.sessions.Session(w.serial)
	if !ok || !w.freshSpeed(current, gen) || current.Epoch != snap.Epoch {
		return telemetry.SessionView{}, false
	}
	return current, true
}

// reconcileSpeed confirms the proxy's Silent command only from a newer
// report. It also relinquishes ownership when a later report shows a manual
// profile change, and performs a deferred restore when its guards are met.
// The caller holds controlMu.
func (w *worker) reconcileSpeed(snap telemetry.SessionView) {
	// Re-read under the ownership lock boundary so a report that arrived
	// after the step snapshot can confirm Silent or release ownership.
	current, ok := w.e.sessions.Session(w.serial)
	if !ok || !current.Active || current.SessionGen != snap.SessionGen {
		return
	}
	snap = current
	gen := w.e.gens.Generation(w.serial)
	if !w.freshSpeed(snap, gen) {
		return
	}
	profile := *snap.SpeedProfile
	if w.speedPending {
		if gen != w.speedCommandGen {
			w.relinquishSpeed("upstream connection changed before confirmation")
			return
		}
		if snap.SpeedObs > w.speedCommandObs {
			if profile == 1 {
				w.speedPending = false
				w.speedOwned = true
				w.speedAckObs = snap.SpeedObs
				w.log.Info("warning speed override confirmed", "profile", 1)
			}
		}
	}
	if w.speedOwned && snap.SpeedObs > w.speedAckObs && profile != 1 {
		w.relinquishSpeed("printer reported a manual profile change")
		return
	}
	if w.warningEpisode || !w.speedOwned || profile != 1 || !isRunning(snap.State) {
		return
	}
	w.mu.Lock()
	pausePending := w.pauseState == PausePending
	w.mu.Unlock()
	if pausePending || w.speedPrior == nil || w.speedPrintGen != snap.SessionGen ||
		gen == 0 || snap.ObsGen != gen || snap.StateGen != gen ||
		w.e.now().Sub(snap.ObsAt) > w.e.kReportFreshMax {
		return
	}
	prior := *w.speedPrior
	if err := w.e.control.SetSpeedProfile(w.serial, gen, prior); err != nil {
		w.log.Warn("warning speed restore failed; not retrying", "reason",
			"guarded print-speed command was rejected; check LAN control and the current printer connection")
	} else {
		w.log.Info("warning speed profile restored", "profile", prior)
	}
	w.resetSpeedState(snap.SessionGen)
}

// relinquishSpeed drops the saved profile after the printer reports a
// profile the proxy did not set. The warning episode stays single-shot.
func (w *worker) relinquishSpeed(reason string) {
	w.speedPrior = nil
	w.speedPending = false
	w.speedOwned = false
	w.speedAttempted = true
	w.log.Info("warning speed override released", "reason", reason)
}

// applyWarningSpeed starts or clears a warning episode and issues at most
// one Silent command in that episode. Pause suggestions take precedence.
// The caller holds controlMu after accepting the result.
func (w *worker) applyWarningSpeed(snap telemetry.SessionView, gen uint64, frame Frame, res Result) {
	w.ensureSpeedSession(snap)
	if res.WarningSuggested {
		if !w.warningEpisode {
			w.warningEpisode = true
			if !w.speedPending && !w.speedOwned {
				w.speedAttempted = false
				w.speedPrior = nil
			}
		}
		if res.PauseSuggested {
			w.speedAttempted = true
			return
		}
		if w.speedAttempted || w.speedPending || w.speedOwned {
			return
		}
		w.mu.Lock()
		pauseActive := w.pauseState == PausePending || w.pauseState == PauseConfirmed
		w.mu.Unlock()
		if pauseActive {
			w.speedAttempted = true
			return
		}
		current, ok := w.speedAuthorized(snap, gen, frame)
		if !ok {
			return
		}
		profile := *current.SpeedProfile
		w.speedAttempted = true
		if profile == 1 {
			w.speedPrior = nil
			return
		}
		w.speedPrior = &profile
		w.speedPrintGen = current.SessionGen
		w.speedCommandGen = gen
		w.speedCommandObs = current.SpeedObs
		if err := w.e.control.SetSpeedProfile(w.serial, gen, 1); err != nil {
			w.speedPrior = nil
			w.log.Warn("warning speed override failed; not retrying this warning episode", "reason",
				"guarded print-speed command was rejected; check LAN control and the current printer connection")
			return
		}
		w.speedPending = true
		w.log.Warn("warning speed override sent; waiting for printer confirmation", "profile", 1)
		return
	}

	w.warningEpisode = false
	if w.speedPending || w.speedOwned {
		// A current Silent report may have arrived without a worker turn to
		// process it as a separate acknowledgement. Reconcile it now so a
		// clear result can restore in this same result transition.
		w.reconcileSpeed(snap)
		return
	}
	w.speedAttempted = false
	w.speedPrior = nil
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
// across a disable/re-enable pair. It then gates every result — clear,
// warning, and pause alike — on the same evidence-freshness rules the
// pause dispatch uses, so a stale analysis can never move risk counters,
// the latch, speed ownership, or the published analysis. A discarded
// response still counts as transport success, so the temporary-failure
// counter resets. It returns false when the result was discarded.
func (w *worker) onResult(snap telemetry.SessionView, gen uint64, frame Frame, res Result, layer *int, workGen uint64) bool {
	if current, ok := w.e.sessions.Session(w.serial); !ok || !current.Active || current.Epoch != snap.Epoch {
		w.log.Info("inspection result discarded; print session changed during upload",
			"operation", "process", "session_generation", snap.SessionGen,
			"session_epoch", snap.Epoch, "connection_generation", gen,
			"frame_sequence", frame.Seq, "reason", "print_session_changed")
		return false
	}
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
		w.log.Info("inspection result discarded; detection availability changed during upload",
			"operation", "process", "session_generation", snap.SessionGen,
			"session_epoch", snap.Epoch, "connection_generation", gen,
			"frame_sequence", frame.Seq, "reason", "detection_availability_changed")
		return false
	}
	// Recheck continuity after acquiring the control lock. A result that became
	// stale while waiting for the lock must not alter policy or pause state.
	if current, ok := w.e.sessions.Session(w.serial); !ok || !current.Active || current.Epoch != snap.Epoch {
		w.controlMu.Unlock()
		w.log.Info("inspection result discarded; print session changed during upload",
			"operation", "process", "session_generation", snap.SessionGen,
			"session_epoch", snap.Epoch, "connection_generation", gen,
			"frame_sequence", frame.Seq, "reason", "print_session_changed")
		return false
	}
	// The same freshness and continuity rules that authorize a pause also
	// authorize accepting any analysis. Evidence rejected here must leave
	// risk counters, clear counters, the degraded condition, resultAt, the
	// pause latch, and speed ownership untouched; only the transport
	// success resets tempAttempts.
	if reason := w.pauseAuthorizationReason(snap, gen, frame); reason != "" {
		w.mu.Lock()
		w.tempAttempts = 0
		w.mu.Unlock()
		w.controlMu.Unlock()
		w.log.Info("inspection result discarded; evidence rejected",
			"operation", "process", "session_generation", snap.SessionGen,
			"session_epoch", snap.Epoch, "connection_generation", gen,
			"frame_sequence", frame.Seq, "reason", reason)
		return false
	}
	w.mu.Lock()
	w.tempAttempts = 0
	wasDegraded := w.degraded != ""
	w.degraded = ""
	wasWarning := w.st.Warning
	newRisk := res.FasterInspectionSuggested || res.WarningSuggested || res.PauseSuggested || res.PrintQuality <= 5
	if newRisk {
		w.riskActive = true
		w.clearCount = 0
		w.clearSince = time.Time{}
		w.intensiveUntil = w.e.now().Add(intensivePeriod)
	} else if res.PrintQuality >= 6 {
		if w.clearCount == 0 {
			w.clearSince = w.e.now()
		}
		w.clearCount++
		if w.clearCount >= intensiveClearCount && w.e.now().Sub(w.clearSince) >= intensiveClearSpan {
			w.riskActive = false
			w.clearCount = 0
			w.clearSince = time.Time{}
		}
	} else {
		w.clearCount = 0
		w.clearSince = time.Time{}
	}
	if wasDegraded {
		w.intensiveUntil = w.e.now().Add(intensivePeriod)
		w.clearCount = 0
		w.clearSince = time.Time{}
	}
	pauseWanted := res.PauseSuggested && !w.awaitingClear
	w.mu.Unlock()
	pauseState := ""
	message := ""
	reason := ""
	pauseAuthorized := false
	attempted := false
	authorizationReason := "pause_not_suggested"
	if pauseWanted {
		authorizationReason = w.pauseAuthorizationReason(snap, gen, frame)
		pauseAuthorized = authorizationReason == ""
	} else if res.PauseSuggested {
		authorizationReason = "pause_latched_until_clear"
	}
	if pauseWanted {
		if pauseAuthorized {
			err := w.e.control.PausePrint(w.serial, gen)
			attempted = true
			if err != nil {
				// Guarded and final: never retried, never replayed.
				pauseState = PauseUnconfirmed
				reason = ReasonPauseFailed
				message = "pause command failed: " + err.Error()
				w.log.Warn("pause command rejected", "operation", "pause",
					"session_generation", snap.SessionGen, "session_epoch", snap.Epoch,
					"connection_generation", gen, "frame_sequence", frame.Seq,
					"quality", res.PrintQuality, "error", err.Error())
			} else {
				pauseState = PausePending
				message = "pause sent; waiting for printer confirmation"
				w.log.Warn("pause command published", "operation", "pause",
					"session_generation", snap.SessionGen, "session_epoch", snap.Epoch,
					"connection_generation", gen, "frame_sequence", frame.Seq,
					"quality", res.PrintQuality)
			}
		} else {
			// The print changed while the request was in flight (PAUSE to
			// RUNNING, a reconnect, or stale telemetry): discard the result.
			w.log.Info("pause suggestion discarded; print state changed during inspection",
				"operation", "pause", "session_generation", snap.SessionGen,
				"session_epoch", snap.Epoch, "connection_generation", gen,
				"frame_sequence", frame.Seq, "reason", authorizationReason)
		}
	}
	decision := "pause_not_suggested"
	switch {
	case pauseWanted && !pauseAuthorized:
		decision = "pause_rejected"
	case attempted && pauseState == PausePending:
		decision = "pause_sent"
	case attempted:
		decision = "pause_command_failed"
	case res.PauseSuggested:
		decision = "pause_suppressed_until_clear"
	}
	if w.log.Enabled(context.Background(), slog.LevelDebug) {
		w.log.Debug("gadget action evaluated", "operation", "process",
			"decision", decision, "session_generation", snap.SessionGen,
			"session_epoch", snap.Epoch, "connection_generation", gen,
			"frame_sequence", frame.Seq,
			"frame_age_ms", w.e.now().Sub(frame.Captured).Milliseconds(),
			"print_quality", res.PrintQuality,
			"warning_suggested", res.WarningSuggested,
			"pause_suggested", res.PauseSuggested,
			"pause_latch_awaiting_clear", !pauseWanted && res.PauseSuggested,
			"pause_authorized", pauseAuthorized,
			"pause_authorization_reason", authorizationReason)
		if layer != nil {
			w.log.Debug("gadget action evaluated", "layer", *layer)
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
	w.applyWarningSpeed(snap, gen, frame, res)
	w.controlMu.Unlock()

	w.publish(func(s *Status) {
		s.Quality = res.PrintQuality
		s.Warning = res.WarningSuggested
		s.FasterInspection = res.FasterInspectionSuggested
		s.CameraLost = false
		s.LastInspectedLayer = copyLayer(layer)
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
	} else if clear && wasWarning {
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
func layerValue(layer *int) int {
	if layer == nil {
		return 0
	}
	return *layer
}

func (w *worker) pauseAuthorizationReason(snap telemetry.SessionView, gen uint64, frame Frame) string {
	if w.e.now().Sub(frame.Captured) > w.e.kResultValidity {
		return "inspection_frame_expired"
	}
	current, ok := w.e.sessions.Session(w.serial)
	if !ok || !current.Active {
		return "no_active_print_session"
	}
	nowGen := w.e.gens.Generation(w.serial)
	switch {
	case current.Epoch != snap.Epoch:
		return "print_session_changed"
	case !isRunning(current.State):
		return "printer_not_running"
	case nowGen != gen || gen == 0:
		return "connection_generation_changed"
	case current.ObsGen != nowGen:
		return "telemetry_not_observed_on_current_connection"
	case current.StateGen != nowGen:
		return "running_state_not_observed_on_current_connection"
	case w.e.now().Sub(current.ObsAt) > w.e.kReportFreshMax:
		return "printer_telemetry_stale"
	default:
		return ""
	}
}

func (w *worker) pauseAuthorized(snap telemetry.SessionView, gen uint64, frame Frame) bool {
	return w.pauseAuthorizationReason(snap, gen, frame) == ""
}

// handleFailure classifies an API or transport failure: terminal account
// errors suspend detection account-wide; everything else backs off with the
// last result left in place (sticky fallback) and, for connection or server
// failures, switches to the context's fallback URL.
func (w *worker) handleFailure(err error, processCall bool) time.Duration {
	var apiErr *APIError
	operation := "create"
	if processCall {
		operation = "process"
	}
	httpStatus := 0
	if errors.As(err, &apiErr) {
		httpStatus = apiErr.Status
	}
	class := failureClass(err)
	typeName := class
	if errors.As(err, &apiErr) {
		typeName = safeAPIErrorType(apiErr)
	}
	reason := failureReason(err)
	if w.log.Enabled(context.Background(), slog.LevelDebug) {
		w.log.Debug("Gadget failure detail",
			"operation", operation,
			"http_status", httpStatus,
			"failure_class", class,
			"error_type", typeName,
			"error_detail", err.Error(),
			"frame_bytes", errorsFrameSize(err))
	}
	level := slog.LevelWarn
	delay := time.Duration(0)
	switch {
	case errors.As(err, &apiErr) && apiErr.AccountTerminal():
		level = slog.LevelError
		w.e.suspend(reason)
		delay = -1
	case errors.As(err, &apiErr) && apiErr.WorkerTerminal():
		level = slog.LevelError
		w.blockWorker(reason)
		w.e.activity.Record(w.serial, "ai_blocked", activity.Error, reason)
		delay = w.e.kIdlePoll
	case isLocalRequestError(err):
		level = slog.LevelError
		w.blockWorker(reason)
		w.e.activity.Record(w.serial, "ai_blocked", activity.Error, reason)
		delay = w.e.kIdlePoll
	case IsFrameTooLarge(err):
		// Locally rejected before any request; the next fresh frame will
		// likely also exceed the cap, so back off without URL changes.
		delay = w.tempBackoff()
		return w.logTemporaryFailure(operation, httpStatus, typeName, class, reason, delay)
	default:
		delay := w.tempBackoff()
		if processCall {
			switch {
			case errors.As(err, &apiErr) && apiErr.SwitchToFallback():
				w.mu.Lock()
				if !w.useFallback {
					w.useFallback = true
				}
				w.mu.Unlock()
			case isTransportError(err):
				w.mu.Lock()
				if !w.useFallback {
					w.useFallback = true
				}
				w.mu.Unlock()
			}
		}
		return w.logTemporaryFailure(operation, httpStatus, typeName, class, reason, delay)
	}
	fallback := w.usingFallback()
	w.log.Log(context.Background(), level, "Gadget inspection failed", append(w.diagnosticAttrs(),
		"operation", operation, "status", httpStatus, "type", typeName,
		"class", class, "retry_seconds", 0, "fallback", fallback, "reason", reason)...)
	return delay
}

// logTemporaryFailure records a retryable failure and publishes its safe
// diagnostic reason to status and activity.
func (w *worker) logTemporaryFailure(operation string, httpStatus int, typeName, class, reason string, delay time.Duration) time.Duration {
	w.log.Warn("Gadget inspection failed", append(w.diagnosticAttrs(),
		"operation", operation, "status", httpStatus, "type", typeName,
		"class", class, "retry_seconds", delay.Seconds(),
		"fallback", w.usingFallback(), "reason", reason)...)
	return w.degrade(ReasonAPIRetrying, reason, delay, false)
}

// tempBackoff returns the bounded exponential retry delay for temporary
// failures: 20s doubling to a 10m cap with +-20% jitter, reset on success.
func (w *worker) tempBackoff() time.Duration {
	w.mu.Lock()
	delay := w.retryDelayLocked()
	w.mu.Unlock()
	return delay
}

// blockWorker stops inspection for this printer and publishes its terminal reason.
func (w *worker) blockWorker(message string) {
	w.releaseCamera()
	w.mu.Lock()
	w.blockedReason = message
	w.degraded = ""
	w.mu.Unlock()
	w.publish(func(s *Status) {
		s.State = StateBlocked
		s.Reason = ReasonRequestInvalid
		s.Message = message
		s.Processing = false
		s.NextCheckSeconds = 0
	})
}

// safeAPIErrorType returns an allowlisted provider error type or its fixed unknown token.
func safeAPIErrorType(err *APIError) string {
	if err == nil {
		return errTypeUnknown
	}
	if knownErrTypes[err.Type] {
		return err.Type
	}
	return errTypeUnknown
}

// isTransportError reports whether err has the client's sanitized transport type.
func isTransportError(err error) bool {
	var target *TransportError
	return errors.As(err, &target)
}

// isLocalRequestError reports whether err is a locally rejected request.
func isLocalRequestError(err error) bool {
	var target *localRequestError
	return errors.As(err, &target)
}

// failureClass returns the stable diagnostic category without exposing err text.
func failureClass(err error) string {
	var apiErr *APIError
	switch {
	case errors.As(err, &apiErr):
		return "provider_api_" + safeAPIErrorType(apiErr)
	case isTransportError(err):
		return "transport"
	case isLocalRequestError(err):
		return "local_request"
	case IsFrameTooLarge(err):
		return "frame_too_large"
	default:
		var responseErr *responseValidationError
		if errors.As(err, &responseErr) {
			return "response_validation"
		}
		return "unknown"
	}
}

// errorsFrameSize returns the rejected frame size, or zero for other errors.
func errorsFrameSize(err error) int {
	var target *frameTooLargeError
	if errors.As(err, &target) {
		return target.size
	}
	return 0
}

// failureReason returns a safe actionable message for status and activity.
func failureReason(err error) string {
	var apiErr *APIError
	var transportErr *TransportError
	var responseErr *responseValidationError
	var localErr *localRequestError
	switch {
	case errors.As(err, &apiErr):
		return fmt.Sprintf("Gadget API request failed (HTTP %d, %s)", apiErr.Status, safeAPIErrorType(apiErr))
	case errors.As(err, &transportErr):
		return transportErr.message
	case errors.As(err, &responseErr):
		return responseErr.message
	case errors.As(err, &localErr):
		return localErr.message
	case IsFrameTooLarge(err):
		return fmt.Sprintf("camera frame is too large (%d bytes; limit %d bytes)", errorsFrameSize(err), maxImageBytes)
	default:
		return "Gadget request failed for an unknown reason"
	}
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
