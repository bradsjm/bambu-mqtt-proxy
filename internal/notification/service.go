package notification

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
)

// batchWindow is how long delivery gathers further events after the first one before building one message per printer.
const batchWindow = 2 * time.Second

// event is one queued activity entry reduced to what delivery needs.
// pause marks print_paused entries, which notify only when the printer's
// final state carries an active problem.
type event struct {
	// serial identifies the affected printer.
	serial string
	// summary holds the operator message.
	summary string
	// pause qualifies a pause against active problems.
	pause bool
}

// StateSource reads the core printer view.
type StateSource interface {
	View(string) (printer.View, bool)
}

// Service turns recorded activity into best-effort notifications: dropped queues and failed deliveries are logged, never fatal.
type Service struct {
	// names holds configured notification titles.
	names map[string]string
	// cfg holds delivery credentials.
	cfg Settings
	// state reads the core printer view.
	state StateSource
	// logger reports drops and delivery failures.
	logger *slog.Logger
	// events bounds pending activity entries to sixteen.
	events chan event

	// send and snapshot are seams for package tests. send defaults to Send; snapshot is nil without a camera manager.
	send func(context.Context, Settings, string, string, []byte) error
	// snapshot fetches a web-camera frame; nil when cameras are disabled.
	snapshot func(ctx context.Context, serial string) (*camera.Frame, camera.Status)
}

// New builds a service for one notification configuration.
func New(cfg Settings, printers []config.Printer, state StateSource, cameras *camera.Manager, logger *slog.Logger) *Service {
	s := &Service{
		cfg:    cfg,
		state:  state,
		logger: logger,
		events: make(chan event, 16),
	}
	s.names = make(map[string]string, len(printers))
	for _, p := range printers {
		s.names[p.Serial] = p.Name
	}
	s.send = Send
	if cameras != nil {
		s.snapshot = cameras.SnapshotContext
	}
	return s
}

// Observe queues one newly recorded entry.
func (s *Service) Observe(serial string, e activity.Entry) {
	summary := eventSummary(e)
	if summary == "" {
		return
	}
	ev := event{serial: serial, summary: summary, pause: e.Kind == "print_paused"}
	select {
	case s.events <- ev:
	default:
		s.logger.Warn("notification dropped", "serial", serial, "kind", e.Kind)
	}
}

// Run combines each printer's events in a two-second window and delivers them with one worker.
func (s *Service) Run(ctx context.Context) error {
	// pending retains each printer's batch until its own deadline.
	pending := make(map[string][]event)
	deadlines := make(map[string]time.Time)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	timer.Stop()
	for {
		var wake <-chan time.Time
		if len(deadlines) > 0 {
			var next time.Time
			for _, at := range deadlines {
				if next.IsZero() || at.Before(next) {
					next = at
				}
			}
			timer.Reset(time.Until(next))
			wake = timer.C
		}
		select {
		case <-ctx.Done():
			return nil
		case ev := <-s.events:
			if len(pending[ev.serial]) == 0 {
				deadlines[ev.serial] = time.Now().Add(batchWindow)
			}
			// Each summary needs at most one place in a printer's batch.
			if !slices.Contains(pending[ev.serial], ev) {
				pending[ev.serial] = append(pending[ev.serial], ev)
			}
		case now := <-wake:
			for serial, at := range deadlines {
				if !at.After(now) {
					s.deliverBatch(ctx, serial, pending[serial])
					delete(pending, serial)
					delete(deadlines, serial)
				}
			}
		}
		timer.Stop()
	}
}

// deliverBatch sends at most one message per printer, sorted for deterministic output.
func (s *Service) deliverBatch(ctx context.Context, serial string, evs []event) {
	if ctx.Err() != nil {
		return
	}
	view, ok := s.state.View(serial)
	// problems and filename come from one view; an unknown printer has neither.
	var problems []printer.Problem
	filename := ""
	if ok {
		problems = view.Problems
		if view.JobName != nil {
			filename = *view.JobName
		}
	}
	lines := problemLines(problems)

	// Distinct summaries in arrival order; an unqualified pause contributes only when the final state carries a problem.
	summaries := make([]string, 0, len(evs))
	for _, ev := range evs {
		if ev.pause && len(lines) == 0 {
			continue
		}
		if !slices.Contains(summaries, ev.summary) {
			summaries = append(summaries, ev.summary)
		}
	}
	if len(summaries) == 0 {
		return // A batch holding only an unqualified pause is silent.
	}

	title := s.names[serial]
	if title == "" {
		title = serial
	}
	var b strings.Builder
	for _, summary := range summaries {
		b.WriteString(summary)
		b.WriteString("\n")
	}
	if filename != "" {
		b.WriteString("File: " + filename + "\n")
	}
	for _, line := range lines {
		b.WriteString(line)
		b.WriteString("\n")
	}

	var jpeg []byte
	if s.snapshot != nil {
		snapCtx, cancelSnap := context.WithTimeout(ctx, 10*time.Second)
		frame, status := s.snapshot(snapCtx, serial)
		cancelSnap()
		if status == camera.StatusOK && frame != nil {
			jpeg = frame.JPEG
		}
	}
	sendCtx, cancelSend := context.WithTimeout(ctx, 20*time.Second)
	err := s.send(sendCtx, s.cfg, title, b.String(), jpeg)
	cancelSend()
	if err != nil {
		s.logger.Warn("notification delivery failed", "serial", serial, "error", err)
	}
}

// eventSummary maps an activity entry to its notification summary; empty means the kind never notifies.
func eventSummary(e activity.Entry) string {
	switch e.Kind {
	case "print_finished":
		return "Print finished"
	case "first_layer_complete":
		return "First layer complete"
	case "print_failed":
		return "Print failed"
	case "print_cancelled":
		return "Print cancelled"
	case "print_stopped":
		return "Print stopped before finishing"
	case "printer_problem":
		switch e.Severity {
		case activity.Warning:
			return "Printer warning"
		case activity.Error:
			return "Printer error"
		}
		return ""
	// Message-bearing kinds: the recording module owns the operator text.
	case "ai_warning", "ai_pause_sent", "ai_pause_failed", "ai_pause_unconfirmed",
		"platecheck_stop_requested", "platecheck_stop_failed", "platecheck_stop_unconfirmed",
		"platecheck_layer1_warning", "platecheck_layer1_pause_sent",
		"platecheck_layer1_pause_failed", "platecheck_layer1_pause_unconfirmed":
		return e.Message
	case "print_paused":
		return "Print paused"
	default:
		return ""
	}
}

// problemLines renders one `<message> (<codes>)` line per error or warning problem.
func problemLines(problems []printer.Problem) []string {
	var lines []string
	for _, problem := range problems {
		if problem.Severity != "error" && problem.Severity != "warning" {
			continue
		}
		// codes preserves the projector's constituent order.
		codes := make([]string, 0, len(problem.Codes))
		for _, code := range problem.Codes {
			codes = append(codes, code.Code)
		}
		lines = append(lines, problem.Message+" ("+strings.Join(codes, ", ")+")")
	}
	return lines
}
