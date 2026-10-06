package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"bambu-mqtt-proxy/internal/printerview"
)

// resourceNotifyTimeout bounds one ResourceUpdated fan-out. The SDK applies
// the same budget internally; the dispatcher never inherits a caller's
// deadline.
const resourceNotifyTimeout = 10 * time.Second

// sampler is the one shared sampling loop behind watch_printer and the
// resource notifications. It runs a single goroutine that walks every
// configured printer once per interval, derives each printer's
// notification-relevant signature, and wakes parked watchers on change.
//
// Entries exist for the whole configured fleet for the process lifetime, so
// per-printer revision counters are stable across watcher churn: a token
// handed out by any read path stays comparable against later watches, and a
// change that happens while nobody watches still moves the counter and
// turns stale tokens into an explicit resync_required. The map is bounded
// by the configured fleet. There is no per-client timer and no per-printer
// ticker, and the sampler never reads telemetry's WatchReports channel,
// which belongs to the detection engine.
type sampler struct {
	srv *Server

	mu      sync.Mutex
	entries map[string]*entry // serial -> wake state, one per configured printer

	notfCh   chan string // serials pending a resource-updated fan-out
	stopCh   chan struct{}
	stopOnce sync.Once
}

// entry holds the wake state for one serial. The wake channels are closed
// (never sent on) when their counter bumps, releasing every parked watcher
// of that mode at once; a fresh channel replaces each for the next park.
// A progress-only bump does not wake attention-mode watchers, while any
// attention change also closes wakeProg: progress mode is a superset of
// attention.
type entry struct {
	rev      revision
	wakeAtt  chan struct{}
	wakeProg chan struct{}
	sampled  sample
	have     bool
	events   []WatchEvent
}

// sample is the notification-relevant fingerprint of one printer. Value
// churn that no watcher may wake for — temperature deltas, countdown
// minutes, seconds-aged fields, progress within one 5-point bucket — is
// absent by construction.
type sample struct {
	state      string
	sessionGen uint64
	active     bool
	connected  bool
	fresh      bool
	// moduleKeys is a fresh ordered slice for every sample.
	moduleKeys []moduleSample
	// hmsKey is the sorted active HMS alert IDs joined by ","; printError
	// is the raw print_error code.
	hmsKey     string
	printError int
	// milestone is the current 5-percentage-point bucket while printing,
	// or -1 when not printing or progress is unknown.
	milestone int
}

// moduleSample records one module's name and change-sensitive state key.
type moduleSample struct {
	name string
	key  string
}

// moduleKey compares stable state when supplied, or the entire JSON value.
// A nil value has no key.
func moduleKey(value any) string {
	if value == nil {
		return ""
	}
	if stable, ok := value.(interface{ StableKey() any }); ok {
		value = stable.StableKey()
	}
	raw, _ := json.Marshal(value)
	return string(raw)
}

// newSampler builds the sampler with one primed entry per configured
// printer. The baseline sample is taken here, single-threaded, so the first
// revision token the server hands out already describes a coherent
// snapshot: only changes after construction can move a counter.
func newSampler(s *Server) *sampler {
	sm := &sampler{
		srv:     s,
		entries: make(map[string]*entry, len(s.serials)),
		notfCh:  make(chan string, 2*maxSubscriptions),
		stopCh:  make(chan struct{}),
	}
	now := s.now()
	for _, serial := range s.serials {
		e := &entry{wakeAtt: make(chan struct{}), wakeProg: make(chan struct{})}
		e.sampled = s.sampleNow(serial, now)
		e.have = true
		sm.entries[serial] = e
	}
	return sm
}

// Start launches the sampling loop and the notification dispatcher. Call
// once; Close stops both.
func (s *sampler) Start() {
	go s.loop()
	go s.dispatch()
}

// loop samples the fleet once per interval until stopped.
func (s *sampler) loop() {
	ticker := time.NewTicker(samplerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.step()
		case <-s.stopCh:
			return
		}
	}
}

// dispatch drains the notification queue. Deliveries run here, never on the
// sampling loop: the SDK bounds each fan-out internally, and a slow or dead
// subscriber therefore delays the queue, not the samples. Sends are
// best-effort hints; a full queue drops the hint because every client can
// re-read the resource for the authoritative state.
func (s *sampler) dispatch() {
	for {
		select {
		case serial := <-s.notfCh:
			ctx, cancel := context.WithTimeout(context.Background(), resourceNotifyTimeout)
			_ = s.srv.srv.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: stateURI(serial)})
			cancel()
		case <-s.stopCh:
			return
		}
	}
}

// stop ends the loop and the dispatcher. Idempotent.
func (s *sampler) stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
}

// step samples every configured printer once. Tests call it directly
// instead of running the loop.
func (s *sampler) step() {
	now := s.srv.now()
	s.mu.Lock()
	serials := make([]string, 0, len(s.entries))
	for serial := range s.entries {
		serials = append(serials, serial)
	}
	s.mu.Unlock()
	for _, serial := range serials {
		s.observe(serial, now)
	}
}

// observe compares one serial's fresh sample against its stored sample and
// bumps the wake counters on any notification-relevant change. An attention
// change bumps the attention counter and wakes attention watchers; a
// progress bucket bump increments only the progress counter. Progress mode
// is a superset of attention, so either change wakes progress watchers,
// exactly once per tick. A silent transition still refreshes the stored
// sample so the next diff starts from the present, but it moves no counter
// and wakes nobody.
func (s *sampler) observe(serial string, now time.Time) {
	cur := s.srv.sampleNow(serial, now)
	s.mu.Lock()
	e, ok := s.entries[serial]
	if !ok {
		s.mu.Unlock()
		return
	}
	if !e.have {
		e.sampled = cur
		e.have = true
		s.mu.Unlock()
		return
	}
	events := attentionEvents(e.sampled, cur)
	attChanged := len(events) > 0
	var milestone *float64
	if cur.milestone >= 0 && cur.milestone > e.sampled.milestone {
		pct := float64(cur.milestone * 5)
		milestone = &pct
	}
	if !attChanged && milestone == nil {
		// Silent transition: refresh the baseline so the next diff
		// compares against the present sample, not one from before the
		// silent resume or clear.
		e.sampled = cur
		s.mu.Unlock()
		return
	}
	if attChanged {
		e.rev.attention++
		close(e.wakeAtt)
		e.wakeAtt = make(chan struct{})
	}
	if milestone != nil {
		e.rev.progress++
		events = append(events, WatchEvent{Kind: kindProgressStep, Percent: milestone})
	}
	if attChanged || milestone != nil {
		// One close per tick releases progress watchers for either
		// change; the attention branch above must not close wakeProg a
		// second time.
		close(e.wakeProg)
		e.wakeProg = make(chan struct{})
	}
	e.sampled = cur
	e.events = events
	s.mu.Unlock()
	if s.srv.subscribed(serial) {
		s.enqueue(serial)
	}
}

// enqueue queues one resource-updated fan-out without ever blocking the
// sampler.
func (s *sampler) enqueue(serial string) {
	select {
	case s.notfCh <- serial:
	default:
		s.srv.log.Debug("resource notification queue full; hint dropped",
			"serial", serial, "uri", stateURI(serial), "queue_capacity", cap(s.notfCh))
	}
}

// watch returns the serial's current revision plus the wake channel for the
// mode to select on. The pair is consistent: any change relevant to that
// mode after this call closes exactly the returned channel. For progress
// mode that is any attention change or progress milestone; for attention
// mode it is attention changes only. Entries live for the process
// lifetime, so no teardown bookkeeping is needed when the watcher leaves.
// A nil channel (unknown serial) blocks a select forever; the tool
// handlers validate the serial before parking.
func (s *sampler) watch(serial, mode string) (revision, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[serial]
	if !ok {
		return revision{}, nil
	}
	if mode == "progress" {
		return e.rev, e.wakeProg
	}
	return e.rev, e.wakeAtt
}

// revision returns the serial's current revision without registering
// interest. Counters are fleet-stable, so the token is meaningful whether
// or not anyone is watching.
func (s *sampler) revision(serial string) revision {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[serial]; ok {
		return e.rev
	}
	return revision{}
}

// lastEvents returns the events of the most recent bump for the serial.
func (s *sampler) lastEvents(serial string) []WatchEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[serial]; ok {
		return e.events
	}
	return nil
}

// sampleNow reads the notification-relevant fingerprint of one printer.
func (s *Server) sampleNow(serial string, now time.Time) sample {
	sm := sample{milestone: -1}
	if sv, ok := s.state.Session(serial); ok {
		sm.state = sv.State
		sm.sessionGen = sv.SessionGen
		sm.active = sv.Active
		if !sv.ObsAt.IsZero() {
			sm.fresh = now.Sub(sv.ObsAt) <= printerview.FreshnessWindow &&
				s.generation(serial) == sv.ObsGen
		}
		if sv.Active && sv.Progress != nil {
			sm.milestone = int(*sv.Progress / 5)
		}
	}
	if st, ok := s.state.State(serial); ok {
		ids := make([]string, 0, len(st.HMS))
		for _, a := range st.HMS {
			ids = append(ids, a.ID())
		}
		sort.Strings(ids)
		sm.hmsKey = strings.Join(ids, ",")
		sm.printError = st.PrintError
	}
	sm.connected = s.connStatus(serial)
	sm.moduleKeys = make([]moduleSample, 0, len(s.modules))
	for _, m := range s.modules {
		if m.State != nil {
			sm.moduleKeys = append(sm.moduleKeys, moduleSample{name: m.Name, key: moduleKey(m.State(serial))})
		}
	}
	return sm
}

// attentionEvents diffs two samples into the stable attention event list.
// The transitions follow the printed contract: pause, fail, finish, job
// changes, stops, new HMS alerts and printer errors, connectivity,
// freshness, and module state. Ordinary resume and pure value churn stay silent.
func attentionEvents(prev, cur sample) []WatchEvent {
	var events []WatchEvent
	add := func(kind, detail string) {
		events = append(events, WatchEvent{Kind: kind, Detail: detail})
	}
	if cur.state != prev.state {
		switch cur.state {
		case "PAUSE", "PAUSED":
			if prev.state != "PAUSE" && prev.state != "PAUSED" {
				add(kindPrintPaused, "print state "+cur.state)
			}
		case "FAILED":
			add(kindPrintFailed, "print state FAILED")
		case "FINISH":
			add(kindPrintFinished, "print state FINISH")
		case "IDLE":
			if prev.state == "RUNNING" || prev.state == "PAUSE" || prev.state == "PAUSED" {
				add(kindPrintStopped, "print stopped before finishing")
			}
		}
	}
	if cur.hmsKey != prev.hmsKey && cur.hmsKey != "" {
		old := map[string]bool{}
		for _, id := range strings.Split(prev.hmsKey, ",") {
			old[id] = true
		}
		var added []string
		for _, id := range strings.Split(cur.hmsKey, ",") {
			if !old[id] {
				added = append(added, id)
			}
		}
		if len(added) > 0 {
			add(kindHMSAlert, "new HMS alerts "+strings.Join(added, ","))
		}
	}
	if cur.printError != prev.printError && cur.printError != 0 {
		v := uint32(cur.printError)
		add(kindPrintError, fmt.Sprintf("printer error %04X_%04X", v>>16, v&0xFFFF))
	}
	if cur.sessionGen > prev.sessionGen {
		if prev.active {
			add(kindJobChanged, "new print identity during a session")
		} else {
			add(kindPrintStarted, "print session started")
		}
	}
	if prev.connected && !cur.connected {
		add(kindConnectLost, "upstream connection lost")
	}
	if cur.connected && !prev.connected {
		add(kindConnectRestored, "upstream connection restored")
	}
	if prev.fresh && !cur.fresh {
		add(kindReportsStale, "real reports stopped arriving")
	}
	if cur.fresh && !prev.fresh {
		add(kindReportsFresh, "real reports are current again")
	}
	for i, m := range cur.moduleKeys {
		if m.key != prev.moduleKeys[i].key {
			events = append(events, WatchEvent{
				Kind: kindModuleChanged, Module: m.name, Detail: m.name + " state changed",
			})
		}
	}
	return events
}
