package notification

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/hmscodes"
	"bambu-mqtt-proxy/internal/telemetry"
)

// batchWindow is how long delivery gathers further events after the first
// one before building one message per printer.
const batchWindow = 2 * time.Second

// event is one queued activity entry reduced to what delivery needs.
// pause marks print_paused entries, which notify only when the printer's
// final state carries an active alert.
type event struct {
	serial  string
	summary string
	pause   bool
}

// Service turns recorded activity into best-effort notifications: dropped
// queues and failed deliveries are logged, never fatal. One Service runs
// one delivery goroutine.
type Service struct {
	cfg       Settings
	state     *telemetry.Cache
	logger    *slog.Logger
	events    chan event
	done      chan struct{}
	cancel    context.CancelFunc
	closeOnce sync.Once

	// send and snapshot are seams for package tests. send defaults to
	// Send; snapshot is nil without a camera manager.
	send func(context.Context, Settings, string, string, []byte) error
	// snapshot fetches a web-camera frame; nil when cameras are disabled.
	snapshot func(ctx context.Context, serial string) (*camera.Frame, camera.Status)
}

// New builds a service for one notification configuration.
func New(cfg Settings, state *telemetry.Cache, cameras *camera.Manager, logger *slog.Logger) *Service {
	s := &Service{
		cfg:    cfg,
		state:  state,
		logger: logger,
		events: make(chan event, 32),
		done:   make(chan struct{}),
	}
	s.send = Send
	if cameras != nil {
		s.snapshot = cameras.SnapshotContext
	}
	return s
}

// Observe queues one newly recorded entry. It drops kinds that never
// notify and never blocks: a full queue logs a warning and loses the
// event. Report processing calls this method, so it must stay cheap.
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

// Start launches the single delivery goroutine. Call once, before Close.
func (s *Service) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go func() {
		defer close(s.done)
		s.loop(ctx)
	}()
}

// Close cancels the delivery loop and waits for it once. The event channel
// is never closed, so Observe stays safe during teardown and drops events
// when the queue is full.
func (s *Service) Close() {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		<-s.done
	})
}

// loop batches arrivals: the first queued event starts a batch, the batch
// then gathers further events for one window before delivery.
func (s *Service) loop(ctx context.Context) {
	for {
		var first event
		select {
		case first = <-s.events:
		case <-ctx.Done():
			return
		}
		batch := []event{first}
		timer := time.NewTimer(batchWindow)
	gather:
		for {
			select {
			case ev := <-s.events:
				batch = append(batch, ev)
			case <-timer.C:
				break gather
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
		timer.Stop()
		s.deliverBatch(ctx, batch)
	}
}

// deliverBatch sends at most one message per printer, sorted for
// deterministic output. Delivery failures are logged and never stop the
// proxy.
func (s *Service) deliverBatch(ctx context.Context, batch []event) {
	grouped := make(map[string][]event)
	serials := make([]string, 0, len(batch))
	for _, ev := range batch {
		if _, seen := grouped[ev.serial]; !seen {
			serials = append(serials, ev.serial)
		}
		grouped[ev.serial] = append(grouped[ev.serial], ev)
	}
	slices.Sort(serials)

	for _, serial := range serials {
		if ctx.Err() != nil {
			return
		}
		evs := grouped[serial]
		st, known := s.state.State(serial)
		alerts := activeAlerts(st, known)

		// Distinct summaries in arrival order; an unqualified pause
		// contributes only when the final state carries an alert.
		summaries := make([]string, 0, len(evs))
		for _, ev := range evs {
			if ev.pause && len(alerts) == 0 {
				continue
			}
			if !slices.Contains(summaries, ev.summary) {
				summaries = append(summaries, ev.summary)
			}
		}
		if len(summaries) == 0 {
			continue // a batch holding only an unqualified pause
		}

		title := serial
		if known && st.Name != "" {
			title = st.Name
		}
		var b strings.Builder
		for _, summary := range summaries {
			b.WriteString(summary)
			b.WriteString("\n")
		}
		if known && st.Filename != "" {
			b.WriteString("File: " + st.Filename + "\n")
		}
		for _, line := range alerts {
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
}

// eventSummary maps an activity entry to its notification summary; empty
// means the kind never notifies. Print pauses and HMS severities qualify
// further at delivery time. Routine plate-check lifecycle entries (a
// confirmed first-layer pause, a pass, or a skip) stay in the activity log
// only.
func eventSummary(e activity.Entry) string {
	switch e.Kind {
	case "print_finished":
		return "Print finished"
	case "first_layer_complete":
		return "First layer complete"
	case "print_failed":
		return "Print failed or was cancelled"
	case "print_stopped":
		return "Print stopped before finishing"
	case "print_error":
		return "Printer error"
	case "hms_alert":
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

// activeAlerts renders the printer's current alerts, one line each: the
// nonzero print error and every HMS alert of severity fatal, serious, or
// common.
func activeAlerts(st telemetry.State, known bool) []string {
	if !known {
		return nil
	}
	var lines []string
	if st.PrintError != 0 {
		code := fmt.Sprintf("%04X_%04X", uint32(st.PrintError)>>16, uint32(st.PrintError)&0xFFFF)
		lines = append(lines, alertLine(code, hmscodes.PrintError(uint32(st.PrintError))))
	}
	for _, a := range st.HMS {
		switch a.Severity() {
		case "fatal", "serious", "common":
			lines = append(lines, alertLine(a.ID(), hmscodes.HMS(a.Attr)))
		}
	}
	return lines
}

// alertLine formats one alert as `<code>: <Title> — <Fix>`, omitting empty
// parts.
func alertLine(code string, info hmscodes.Info) string {
	line := code
	if info.Title != "" {
		line += ": " + info.Title
	}
	if info.Fix != "" {
		line += " — " + info.Fix
	}
	return line
}
