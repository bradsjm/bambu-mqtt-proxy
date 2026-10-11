// Package detection inspects running prints with OctoEverywhere Gadget.
package detection

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
)

// Pause lifecycle values describe a single per-print command attempt.
const (
	PauseNone        = "none"
	PausePending     = "pending"
	PauseConfirmed   = "confirmed"
	PauseUnconfirmed = "unconfirmed"
)

// Display states describe monitoring availability and the last analysis.
const (
	StateStarting           = "starting"
	StateIdle               = "idle"
	StateMonitoring         = "monitoring"
	StateAttention          = "attention"
	StatePaused             = "paused"
	StateDegraded           = "degraded"
	StateUnsupported        = "unsupported"
	StateBlocked            = "blocked"
	StateDisabled           = "disabled"
	ReasonCameraDisabled    = "camera_disabled"
	ReasonCameraUnsupported = "camera_unsupported"
	ReasonAwaitingReports   = "awaiting_reports"
	ReasonCameraLost        = "camera_unavailable"
	ReasonAPIRetrying       = "api_retrying"
	ReasonPauseUnconfirmed  = "pause_unconfirmed"
	ReasonPauseFailed       = "pause_command_failed"
	ReasonRequestInvalid    = "request_invalid"
	ReasonUserDisabled      = "user_disabled"
	DisabledUntilPrintEnd   = "print_end_or_restart"
)

// Toggle errors distinguish an unknown printer from an unavailable print.
var (
	ErrUnknownPrinter       = errors.New("unknown printer")
	ErrNoPrintSession       = errors.New("no active print session")
	ErrStaleSession         = errors.New("session token does not match the current print")
	ErrDetectionUnavailable = errors.New("detection is not available for this printer")
)

// Status is the detection state served on wall tiles, MCP, and health.
type Status struct {
	// State describes monitoring availability.
	State string `json:"state"`
	// Reason explains the display state.
	Reason string `json:"reason,omitempty"`
	// Quality is Gadget's last quality score.
	Quality int `json:"quality,omitempty"`
	// Processing reports an in-flight inspection.
	Processing bool `json:"processing,omitempty"`
	// Warning is Gadget's last warning flag.
	Warning bool `json:"warning,omitempty"`
	// PauseState describes the once-per-print pause attempt.
	PauseState string `json:"pause_state"`
	// Enabled reports the user's per-print permission.
	Enabled bool `json:"enabled"`
	// DisabledUntil describes the override lifetime.
	DisabledUntil string `json:"disabled_until,omitempty"`
	// SessionID is the decimal print counter echoed by the wall toggle.
	SessionID string `json:"session_id,omitempty"`
	// LastInspectedLayer identifies the uploaded layer when reported.
	LastInspectedLayer *int `json:"last_inspected_layer,omitempty"`
	// AgeSeconds is the display-only age of the last analysis.
	AgeSeconds float64 `json:"age_seconds,omitempty"`
	// NextCheckSeconds is the remaining Gadget-recommended wait.
	NextCheckSeconds float64 `json:"next_check_seconds,omitempty"`
	// FasterInspection exposes Gadget's hint without changing its interval.
	FasterInspection bool `json:"faster_inspection,omitempty"`
	// CameraLost reports capture failure.
	CameraLost bool `json:"camera_lost,omitempty"`
	// UsingFallback reports use of Gadget's fallback endpoint.
	UsingFallback bool `json:"using_fallback,omitempty"`
	// Suspended marks an account-wide failure until restart.
	Suspended bool `json:"suspended,omitempty"`
	// Message gives the operator a short explanation.
	Message string `json:"message,omitempty"`
}

// StableKey excludes continuously aging display fields.
func (s Status) StableKey() any { s.AgeSeconds = 0; s.NextCheckSeconds = 0; return s }

// Frame contains an immutable captured JPEG.
type Frame struct {
	// JPEG is the image content.
	JPEG []byte
	// Seq identifies a distinct camera frame.
	Seq uint64
	// Captured is the camera capture time.
	Captured time.Time
}

// FrameSource supplies shared camera captures.
type FrameSource interface {
	Acquire(string) bool
	Release(string)
	WaitFrame(string, context.Context, uint64, time.Duration) (Frame, bool)
}

// stateSource supplies the authoritative document and ordered events.
type stateSource interface {
	Get(string) (printer.Doc, bool, bool)
	Subscribe(func(printer.Event))
}

// PrinterControl supplies plain pause and light commands.
type PrinterControl interface {
	Pause(string) error
	Light(string, bool) error
	Online(string) bool
}

// gadgetClient supplies one context per print and image analysis.
type gadgetClient interface {
	CreateContext(context.Context) (Session, error)
	Process(context.Context, string, []byte) (Result, error)
}

// Engine owns one inspection worker per configured printer.
type Engine struct {
	// client performs Gadget requests.
	client gadgetClient
	// frames supplies camera frames.
	frames FrameSource
	// control sends printer commands.
	control PrinterControl
	// log records operational failures.
	log *slog.Logger
	// activity receives module activity entries.
	activity *activity.Log
	// workers contains immutable printer membership.
	workers map[string]*worker
	// blocked parks the engine when cameras are disabled.
	blocked string
	// mu protects account suspension.
	mu sync.Mutex
	// suspended disables requests until restart.
	suspended bool
	// suspendMsg explains account suspension.
	suspendMsg string
}

// New constructs workers and subscribes without starting network work.
func New(printers []config.Printer, client gadgetClient, frames FrameSource, state stateSource, link PrinterControl, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	e := &Engine{client: client, frames: frames, control: link, log: log.With("origin", "detection"), workers: make(map[string]*worker)}
	if c, ok := client.(interface{ setLogger(*slog.Logger) }); ok {
		c.setLogger(e.log)
	}
	for _, p := range printers {
		e.workers[p.Serial] = &worker{e: e, serial: p.Serial, eligible: config.CameraEligible(p.Model, p.Serial), wake: make(chan struct{}, 1), outcomes: make(chan string, 64), st: Status{State: StateStarting, Reason: ReasonAwaitingReports, PauseState: PauseNone, Enabled: true}}
	}
	if state != nil {
		state.Subscribe(e.observe)
		for serial, w := range e.workers {
			if doc, online, ok := state.Get(serial); ok {
				w.mu.Lock()
				if w.doc == nil {
					w.apply(printer.Event{Serial: serial, Cur: doc, Online: online, Kinds: []string{"state_initial"}})
				}
				w.mu.Unlock()
			}
		}
	}
	return e
}

// SetActivity attaches the activity log before Run.
func (e *Engine) SetActivity(log *activity.Log) { e.activity = log }

// SetBlocked parks workers before Run when cameras are unavailable.
func (e *Engine) SetBlocked(reason string) { e.blocked = reason }

// Run starts one worker per printer and joins them on cancellation.
func (e *Engine) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, w := range e.workers {
		wg.Go(func() { w.run(ctx) })
	}
	wg.Wait()
	return nil
}

// observe updates the counter and document without commands or network work.
func (e *Engine) observe(ev printer.Event) {
	if w := e.workers[ev.Serial]; w != nil {
		w.mu.Lock()
		w.apply(ev)
		w.mu.Unlock()
		w.nudge()
	}
}

// AccountSuspended returns the shared suspension reason.
func (e *Engine) AccountSuspended() (bool, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.suspended, e.suspendMsg
}

// suspend disables account requests until restart.
func (e *Engine) suspend(message string) {
	e.mu.Lock()
	if e.suspended {
		e.mu.Unlock()
		return
	}
	e.suspended, e.suspendMsg = true, message
	e.mu.Unlock()
	e.log.Error("detection suspended until restart", "reason", message)
	for serial, w := range e.workers {
		e.activity.Record(serial, "ai_suspended", activity.Error, "AI inspection suspended until proxy restart, "+message)
		w.nudge()
	}
}

// DetectionStatus returns a printer's current detection status.
func (e *Engine) DetectionStatus(serial string) any {
	if w := e.workers[serial]; w != nil {
		return w.view()
	}
	return nil
}

// DetectionMap returns the detection health map.
func (e *Engine) DetectionMap() map[string]any {
	out := make(map[string]any, len(e.workers))
	for serial, w := range e.workers {
		out[serial] = w.view()
	}
	return out
}

// SetDetectionEnabled changes the per-print toggle and validates its token.
func (e *Engine) SetDetectionEnabled(serial string, enabled bool, sessionID string) (any, error) {
	w := e.workers[serial]
	if w == nil {
		return nil, ErrUnknownPrinter
	}
	w.commandMu.Lock()
	w.mu.Lock()
	var err error
	if !enabled {
		switch {
		case e.blocked != "" || !w.eligible:
			err = ErrDetectionUnavailable
		case !active(w.doc) || w.counter == 0:
			err = ErrNoPrintSession
		case sessionID != strconv.FormatUint(w.counter, 10):
			err = ErrStaleSession
		}
	}
	if err == nil {
		w.disabled = !enabled
		if enabled {
			w.lightDone = false
		}
		if w.cancel != nil {
			w.cancel()
		}
	}
	w.mu.Unlock()
	w.commandMu.Unlock()
	if err != nil {
		return nil, err
	}
	w.nudge()
	return w.view(), nil
}

// active reports the printer's running or paused print states.
func active(doc printer.Doc) bool {
	state, _ := doc.GcodeState()
	return state == "RUNNING" || state == "PAUSE" || state == "PAUSED"
}
