package platecheck

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
)

// ReasonCameraDisabled explains why automatic protection is blocked.
const ReasonCameraDisabled = "camera_disabled"

// StateSource supplies merged printer reports and ordered events.
type StateSource interface {
	// Get returns the document, connectivity, and presence.
	Get(string) (printer.Doc, bool, bool)
	// Subscribe installs a nonblocking report subscriber.
	Subscribe(func(printer.Event))
}

// PrinterCommands sends individual printer commands.
type PrinterCommands interface {
	// Online reports link connectivity.
	Online(string) bool
	// Stop sends one stop request.
	Stop(string) error
	// Pause sends one pause request.
	Pause(string) error
	// Light sends one chamber light request.
	Light(string, bool) error
}

// operationKey identifies a local vision operation.
type operationKey struct{}

// operations allocates local diagnostic identifiers.
var operations atomic.Uint64

// operationOf returns the operation identifier in the context.
func operationOf(ctx context.Context) uint64 { v, _ := ctx.Value(operationKey{}).(uint64); return v }

// Service owns independent printer workers and startup diagnostics.
type Service struct {
	// printers retains diagnostic display order.
	printers []config.Printer
	// settings holds effective provider settings.
	settings Settings
	// client evaluates camera images.
	client DecisionClient
	// frames supplies camera images.
	frames FrameSource
	// state supplies the printer document.
	state StateSource
	// commands sends printer commands.
	commands PrinterCommands
	// log records safe diagnostic messages.
	log *slog.Logger
	// activity records operator-facing outcomes.
	activity *activity.Log
	// blocked disables automatic protection and diagnostic routes.
	blocked string
	// workers holds configured printer workers.
	workers map[string]*worker
	// newClient constructs isolated diagnostic clients.
	newClient func(Settings, *slog.Logger) DecisionClient
	// now supplies camera capture boundaries and diagnostic timestamps.
	now func() time.Time
}

// pendingCommand holds the report boundary for one command outcome.
type pendingCommand struct {
	// layer is the reported layer at dispatch.
	layer int
	// prepare permits one PREPARE-to-RUNNING stop retry.
	prepare bool
}

// worker serializes report-driven work for one printer.
type worker struct {
	// s holds shared immutable dependencies.
	s *Service
	// printer identifies the configured printer.
	printer config.Printer
	// events buffers reports without blocking the report path.
	events chan printer.Event
	// mu protects display state.
	mu sync.Mutex
	// status holds the last automatic result.
	status ModuleState
	// pass1Done prevents duplicate first passes across PREPARE-to-RUNNING.
	pass1Done bool
	// homingSeen retains the reported homing stage for this print.
	homingSeen bool
	// homingDone prevents duplicate second passes.
	homingDone bool
	// stopSent prevents additional checks after a stop attempt.
	stopSent bool
	// pending tracks an unresolved startup stop.
	pending *pendingCommand
	// layerPending tracks an unresolved first-layer pause.
	layerPending *pendingCommand
}

// New constructs the module and subscribes without starting work.
func New(printers []config.Printer, settings Settings, client DecisionClient, frames FrameSource, state StateSource, commands PrinterCommands, log *slog.Logger) *Service {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if frames == nil {
		frames = IdleFrames{}
	}
	if settings.On() && client == nil {
		client = NewClient(settings, log)
	}
	s := &Service{printers: append([]config.Printer(nil), printers...), settings: settings, client: client, frames: frames, state: state, commands: commands, log: log.With("origin", "platecheck"), workers: make(map[string]*worker), now: time.Now}
	s.newClient = func(settings Settings, log *slog.Logger) DecisionClient { return NewClient(settings, log) }
	for _, p := range printers {
		s.workers[p.Serial] = &worker{s: s, printer: p, events: make(chan printer.Event, 64), status: s.idleStatus()}
	}
	if state != nil {
		state.Subscribe(s.observe)
	}
	return s
}

// idleStatus returns the status of a printer with no checks run for the current print.
func (s *Service) idleStatus() ModuleState {
	layerState := "idle"
	if !s.settings.FirstLayer.On() {
		layerState = "disabled"
	}
	return ModuleState{State: "idle", Cutoff: s.settings.StopConfidence, FirstLayer: FirstLayerState{State: layerState, Cutoff: s.settings.FirstLayer.PauseConfidence}}
}

// SetActivity attaches the activity log before Run.
func (s *Service) SetActivity(log *activity.Log) { s.activity = log }

// SetBlocked sets a construction-time blocker.
func (s *Service) SetBlocked(reason string) { s.blocked = reason }

// observe queues reports without sending commands or waiting for workers.
func (s *Service) observe(event printer.Event) {
	if !s.settings.On() || s.blocked != "" {
		return
	}
	if w := s.workers[event.Serial]; w != nil {
		select {
		case w.events <- event:
		default:
			s.log.Warn("Plate-check report queue full", "serial", event.Serial)
		}
	}
}

// Run owns worker lifetimes until cancellation.
func (s *Service) Run(ctx context.Context) error {
	if !s.settings.On() || s.blocked != "" || s.commands == nil || s.state == nil {
		<-ctx.Done()
		return nil
	}
	var wg sync.WaitGroup
	for _, w := range s.workers {
		wg.Add(1)
		go func() { defer wg.Done(); w.run(ctx) }()
	}
	wg.Wait()
	return nil
}

// run processes one printer's events in report order.
func (w *worker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-w.events:
			if ctx.Err() != nil {
				return
			}
			w.handle(ctx, event)
		}
	}
}

// handle derives passes and command outcomes directly from reports.
func (w *worker) handle(ctx context.Context, e printer.Event) {
	if slices.Contains(e.Kinds, "connected") || slices.Contains(e.Kinds, "disconnected") {
		return
	}
	state, _ := e.Cur.GcodeState()
	prev, _ := e.Prev.GcodeState()
	layer, hasLayer := e.Cur.Layer()
	w.resolve(e)
	preparing := slices.Contains(e.Kinds, "print_preparing")
	started := slices.Contains(e.Kinds, "print_started")
	initial := slices.Contains(e.Kinds, "state_initial")
	if e.NewJob {
		// A new print clears every finding and outstanding command from the previous print.
		w.pass1Done, w.homingSeen, w.homingDone, w.stopSent = false, false, false, false
		w.pending, w.layerPending = nil, nil
		w.mu.Lock()
		w.status = w.s.idleStatus()
		w.mu.Unlock()
		w.s.log.Debug("Plate-check new print reset", "serial", e.Serial, "gcode_state", state, "prev_gcode_state", prev)
	}
	if initial {
		// Attaching to a print never admits an automatic startup check.
		w.pass1Done, w.homingDone = true, true
		skipped := state == "RUNNING" || state == "PREPARE" || state == "SLICING" || state == "PAUSE" || state == "PAUSED"
		if skipped {
			w.setStatusWithReasonPass("skipped", nil, "start_not_observed", false)
		}
		w.s.log.Debug("Plate-check initial attach", "serial", e.Serial, "gcode_state", state, "start_not_observed", skipped)
	}
	if stage, ok := e.Cur.Stage(); ok && (stage == 13 || stage == 14) {
		if !w.homingSeen {
			w.s.log.Debug("Plate-check homing stage seen", "serial", e.Serial, "stage", stage)
		}
		w.homingSeen = true
	}
	if (preparing || started) && !w.pass1Done {
		w.pass1Done = true
		w.check(ctx, false)
	}
	if w.homingSeen && w.pass1Done && !w.homingDone && !w.stopSent {
		// PREPARE still reports the previous job's layer_num; only RUNNING resets it.
		if hasLayer && layer > 0 && state != "PREPARE" && state != "SLICING" {
			w.homingDone = true
			w.skipPass("skipped", "print_past_start", true)
			w.logStartupOutcome(0, "homing", "skipped", "print_past_start", nil, state, layer, hasLayer, false, nil, 0)
		} else if state == "RUNNING" && hasLayer && layer == 0 {
			w.homingDone = true
			w.check(ctx, true)
		}
	}
	if slices.Contains(e.Kinds, "first_layer_complete") && w.s.settings.FirstLayer.On() {
		if w.stopSent {
			w.layerSkip("skipped", "startup_stopped", 0)
			w.logLayerOutcome(0, "skipped", "startup_stopped", nil, state, layer, hasLayer, nil, 0)
		} else {
			w.checkLayer(ctx)
		}
	}
}

// startupReason checks current printer state once at an action boundary.
// PREPARE and SLICING still carry the previous job's layer_num, so the
// layer applies only after the printer leaves them.
func (w *worker) startupReason(doc printer.Doc, online bool) string {
	state, _ := doc.GcodeState()
	preparing := state == "PREPARE" || state == "SLICING"
	if layer, ok := doc.Layer(); ok && layer > 0 && !preparing {
		return "print_past_start"
	}
	if !online {
		return "printer_offline"
	}
	if preparing {
		return ""
	}
	if state == "RUNNING" {
		if _, ok := doc.Layer(); !ok {
			return "layer_not_reported"
		}
		return ""
	}
	return "print_ended"
}

// check evaluates one startup image and sends at most one stop.
func (w *worker) check(parent context.Context, homing bool) {
	ctx, cancel := context.WithTimeout(context.WithValue(parent, operationKey{}, operations.Add(1)), 25*time.Second)
	defer cancel()
	operation := operationOf(ctx)
	pass := "startup"
	if homing {
		pass = "homing"
	}
	started := time.Now()
	doc, online, _ := w.s.state.Get(w.printer.Serial)
	state, _ := doc.GcodeState()
	layer, hasLayer := doc.Layer()
	if reason := w.startupReason(doc, online); reason != "" {
		w.skipPass("skipped", reason, homing)
		w.logStartupOutcome(operation, pass, "skipped", reason, nil, state, layer, hasLayer, false, nil, time.Since(started).Milliseconds())
		return
	}
	w.setStatusWithReasonPass("checking", nil, "", homing)
	boundary := w.s.now()
	chamberLightSent := false
	if light, _ := doc.ChamberLight(); light == "off" {
		if w.s.commands.Light(w.printer.Serial, true) == nil {
			chamberLightSent = true
			boundary = w.s.now()
		}
	}
	frame, err := w.s.frames.Capture(ctx, w.printer.Serial, boundary)
	if err != nil {
		reason := errorCategory(err)
		w.skipPass("error", reason, homing)
		w.logStartupOutcome(operation, pass, "error", reason, nil, state, layer, hasLayer, chamberLightSent, nil, time.Since(started).Milliseconds())
		return
	}
	captured := &frame
	result, err := w.s.client.Evaluate(ctx, frame.JPEG, config.DisplayModel(w.printer.Model, w.printer.Serial))
	if err != nil || !validResult(result) {
		reason := "bad_response"
		if err != nil {
			reason = errorCategory(err)
		}
		w.skipPass("error", reason, homing)
		w.logStartupOutcome(operation, pass, "error", reason, nil, state, layer, hasLayer, chamberLightSent, captured, time.Since(started).Milliseconds())
		return
	}
	decision := classify(result, w.s.settings.StopConfidence)
	w.setStatusWithReasonPass(decision, &result, "", homing)
	switch decision {
	case "inconclusive":
		w.s.activity.Record(w.printer.Serial, "platecheck_skipped", activity.Info, checkName(homing)+" view inconclusive, continuing print")
		w.logStartupOutcome(operation, pass, decision, "", &result, state, layer, hasLayer, chamberLightSent, captured, time.Since(started).Milliseconds())
	case "clear", "below_threshold":
		w.s.activity.Record(w.printer.Serial, "platecheck_passed", activity.Info, checkName(homing)+" passed, continuing print")
		w.logStartupOutcome(operation, pass, decision, "", &result, state, layer, hasLayer, chamberLightSent, captured, time.Since(started).Milliseconds())
	case "would_stop":
		doc, online, _ = w.s.state.Get(w.printer.Serial)
		state, _ = doc.GcodeState()
		layer, hasLayer = doc.Layer()
		if reason := w.startupReason(doc, online); reason != "" {
			w.skipPass("skipped", reason, homing)
			w.logStartupOutcome(operation, pass, "skipped", reason, nil, state, layer, hasLayer, chamberLightSent, captured, time.Since(started).Milliseconds())
			return
		}
		if ctx.Err() != nil {
			reason := errorCategory(ctx.Err())
			w.skipPass("error", reason, homing)
			w.logStartupOutcome(operation, pass, "error", reason, nil, state, layer, hasLayer, chamberLightSent, captured, time.Since(started).Milliseconds())
			return
		}
		w.stopSent = true
		w.pending = &pendingCommand{layer: layer, prepare: state == "PREPARE"}
		w.sendStop(operation, decision, &result, state, layer, hasLayer)
		w.logStartupOutcome(operation, pass, decision, "", &result, state, layer, hasLayer, chamberLightSent, captured, time.Since(started).Milliseconds())
	}
}

// sendStop sends one command without report waits.
func (w *worker) sendStop(operation uint64, decision string, result *Result, state string, layer int, hasLayer bool) {
	err := w.s.commands.Stop(w.printer.Serial)
	w.setStatusKeepingScores("stop_requested")
	w.s.activity.Record(w.printer.Serial, "platecheck_stop_requested", activity.Warning, "Plate looks occupied, stopping print")
	pendingLayer, pendingPrepare := 0, false
	if w.pending != nil {
		pendingLayer, pendingPrepare = w.pending.layer, w.pending.prepare
	}
	attrs := []any{"serial", w.printer.Serial, "operation", operation, "decision", decision, "error", err != nil, "pending_layer", pendingLayer, "pending_prepare", pendingPrepare}
	if result != nil {
		attrs = append(attrs, "p_clear", result.PClear, "p_occupied", result.POccupied, "p_assessable", result.PAssessable)
	}
	attrs = append(attrs, "cutoff", w.s.settings.StopConfidence, "gcode_state", state)
	if hasLayer {
		attrs = append(attrs, "layer", layer)
	}
	w.s.log.Debug("Plate-check stop command sent", attrs...)
	if err != nil {
		w.setStatusKeepingScores("stop_failed")
		w.s.activity.Record(w.printer.Serial, "platecheck_stop_failed", activity.Error, "Plate-check stop command failed, check the printer")
	}
}

// resolve accepts later printer reports without confirmation clocks.
func (w *worker) resolve(e printer.Event) {
	state, _ := e.Cur.GcodeState()
	prev, _ := e.Prev.GcodeState()
	layer, ok := e.Cur.Layer()
	paused := state == "PAUSE" || state == "PAUSED"
	ended := state == "IDLE" || state == "FINISH" || state == "FAILED"
	if p := w.pending; p != nil {
		if paused || ended {
			w.setStatusKeepingScores("stopped")
			w.s.activity.Record(w.printer.Serial, "platecheck_stopped", activity.Info, "Print stopped after plate-check stop request, clear the plate before resending")
			w.pending = nil
			w.s.log.Debug("Plate-check stop resolved", "serial", e.Serial, "outcome", "stopped", "gcode_state", state, "prev_gcode_state", prev)
		} else {
			if p.prepare && prev == "PREPARE" && state == "RUNNING" {
				p.prepare = false
				doc, _, _ := w.s.state.Get(w.printer.Serial)
				p.layer, _ = doc.Layer()
				resendLayer, resendHasLayer := doc.Layer()
				resendState, _ := doc.GcodeState()
				w.sendStop(0, "would_stop", nil, resendState, resendLayer, resendHasLayer)
				w.s.log.Debug("Plate-check stop resolved", "serial", e.Serial, "outcome", "resent", "gcode_state", state, "prev_gcode_state", prev, "pending_layer", p.layer)
			} else if state == "RUNNING" && ok && layer > p.layer {
				w.setStatusKeepingScores("stop_unconfirmed")
				w.s.activity.Record(w.printer.Serial, "platecheck_stop_unconfirmed", activity.Error, "Plate-check stop not confirmed, check the printer")
				w.pending = nil
				w.s.log.Debug("Plate-check stop resolved", "serial", e.Serial, "outcome", "unconfirmed", "gcode_state", state, "prev_gcode_state", prev, "layer", layer, "pending_layer", p.layer)
			}
		}
	}
	if p := w.layerPending; p != nil {
		if paused {
			w.setLayerPause("confirmed")
			w.layerEvent("platecheck_layer1_pause_confirmed", activity.Warning, "Printer reports paused after first-layer pause request, check the first layer before resuming", 0)
			w.layerPending = nil
			w.s.log.Debug("First-layer check pause resolved", "serial", e.Serial, "outcome", "confirmed", "gcode_state", state, "prev_gcode_state", prev)
		} else if state == "RUNNING" && ok && layer > p.layer {
			w.setLayerPause("unconfirmed")
			w.layerEvent("platecheck_layer1_pause_unconfirmed", activity.Error, "First-layer pause not confirmed, check the printer", 0)
			w.layerPending = nil
			w.s.log.Debug("First-layer check pause resolved", "serial", e.Serial, "outcome", "unconfirmed", "gcode_state", state, "prev_gcode_state", prev, "layer", layer, "pending_layer", p.layer)
		}
	}
}

// setStatusWithReasonPass publishes startup scores and the pass outcome.
func (w *worker) setStatusWithReasonPass(state string, r *Result, reason string, homing bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	v := ModuleState{State: state, Cutoff: w.s.settings.StopConfidence, Reason: reason, Homing: homing, FirstLayer: w.status.FirstLayer}
	if r != nil {
		v.PClear = new(r.PClear)
		v.POccupied = new(r.POccupied)
		v.PAssessable = new(r.PAssessable)
	}
	w.status = v
}

// skipPass records a fail-open startup outcome.
func (w *worker) skipPass(state, reason string, homing bool) {
	w.setStatusWithReasonPass(state, nil, reason, homing)
	kind, severity := "platecheck_skipped", activity.Info
	if state == "error" {
		kind, severity = "platecheck_failed", activity.Warning
	}
	w.s.activity.Record(w.printer.Serial, kind, severity, activitySentence(checkName(homing), state, reason))
}

// checkName returns the startup pass label.
func checkName(homing bool) string {
	if homing {
		return "Plate check after homing"
	}
	return "Plate check"
}

// setStatusKeepingScores changes only the stop lifecycle.
func (w *worker) setStatusKeepingScores(state string) {
	w.mu.Lock()
	w.status.State, w.status.Reason = state, ""
	w.mu.Unlock()
}

// validResult checks probabilities at the vision boundary.
func validResult(r Result) bool {
	return validAnswer(answer{Type: "noul", Noul: &r.PClear}) && validAnswer(answer{Type: "noul", Noul: &r.POccupied}) && validAnswer(answer{Type: "noul", Noul: &r.PAssessable}) && r.POccupied == 1-r.PClear
}

// logStartupOutcome records one debug line for a startup pass decision.
func (w *worker) logStartupOutcome(operation uint64, pass, decision, reason string, result *Result, state string, layer int, hasLayer, chamberLightSent bool, frame *Frame, durationMs int64) {
	attrs := []any{"serial", w.printer.Serial, "operation", operation, "pass", pass, "decision", decision, "reason", reason, "cutoff", w.s.settings.StopConfidence, "gcode_state", state, "chamber_light_sent", chamberLightSent}
	if result != nil {
		attrs = append(attrs, "p_clear", result.PClear, "p_occupied", result.POccupied, "p_assessable", result.PAssessable)
	}
	if hasLayer {
		attrs = append(attrs, "layer", layer)
	}
	if frame != nil {
		attrs = append(attrs, "frame_seq", frame.Seq, "frame_age_ms", w.s.now().Sub(frame.Captured).Milliseconds())
	}
	attrs = append(attrs, "duration_ms", durationMs)
	w.s.log.Debug("Plate-check outcome", attrs...)
}
