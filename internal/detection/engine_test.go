package detection

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// ---- deterministic fakes, prefixed et to stay clear of client_test ----

type etClock struct{ now time.Time }

func newEtClock() *etClock { return &etClock{now: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)} }

func (c *etClock) Now() time.Time          { return c.now }
func (c *etClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// etSessions serves one settable SessionView. Each Session call can hand out
// the next layer from a queue, so tests can pin layer alignment to the
// post-frame snapshot.
type etSessions struct {
	mu     sync.Mutex
	view   telemetry.SessionView
	ok     bool
	layers []int
	calls  int
}

func (s *etSessions) Session(string) (telemetry.SessionView, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(s.layers) > 0 {
		layer := s.layers[0]
		s.view.LayerNum = &layer
		s.view.LayerThisJob = true
		s.layers = s.layers[1:]
	}
	return s.view, s.ok
}

func (s *etSessions) set(v telemetry.SessionView, ok bool) {
	s.mu.Lock()
	s.view, s.ok = v, ok
	s.mu.Unlock()
}

// running reports one state observation on generation 1. epoch mirrors
// telemetry's decision epoch (bumped at every state boundary, including
// PAUSE-to-RUNNING); sessionGen mirrors the print session generation
// (bumped only when a genuinely new print starts).
func (s *etSessions) running(clock *etClock, epoch, sessionGen uint64, state string) {
	s.set(telemetry.SessionView{
		Serial: "S1", Active: true, State: state,
		SessionGen: sessionGen, Epoch: epoch,
		ObsAt: clock.Now(), Obs: 1, LayerNum: intPtr(4), LayerThisJob: true,
	}, true)
}

// etFrames hands out unique frames on the injected clock.
type etFrames struct {
	mu        sync.Mutex
	clock     *etClock
	acquireOK bool
	serve     bool
	stale     time.Duration
	seq       uint64
	held      int
	released  int
	waitCalls int
	lastAfter uint64
	// duringWait, when set, runs inside WaitFrame before the frame is
	// handed out, so tests can move the world during the real camera wait
	// instead of guessing Session call ordering.
	duringWait func()
}

func (f *etFrames) Acquire(string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held++
	return f.acquireOK
}

func (f *etFrames) Release(string) {
	f.mu.Lock()
	f.released++
	f.mu.Unlock()
}

func (f *etFrames) WaitFrame(_ string, _ context.Context, after uint64, _ time.Duration) (Frame, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waitCalls++
	f.lastAfter = after
	if f.duringWait != nil {
		f.duringWait()
	}
	if !f.serve {
		return Frame{}, false
	}
	f.seq++
	return Frame{JPEG: []byte("jpeg"), Seq: f.seq, Captured: f.clock.Now().Add(-f.stale)}, true
}

type etOutcome struct {
	res Result
	err error
}

// etClient records context creations and process uploads and replays a
// queued outcome per upload. beforeReply runs after the upload is recorded
// and before the outcome is returned: it simulates the world changing while
// the request is in flight.
type etClient struct {
	mu          sync.Mutex
	creates     int
	processes   int
	urls        []string
	queued      []etOutcome
	beforeReply func()
}

func (c *etClient) CreateContext(context.Context) (Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creates++
	return Session{
		ContextID:   "ctx-1",
		ProcessURL:  "https://gadget-pv1-oeapi.octoeverywhere.com/p",
		FallbackURL: "https://gadget-pv1-oeapi.octoeverywhere.com/f",
	}, nil
}

func (c *etClient) Process(_ context.Context, url string, _ []byte) (Result, error) {
	c.mu.Lock()
	c.processes++
	c.urls = append(c.urls, url)
	hook := c.beforeReply
	var out etOutcome
	if len(c.queued) > 0 {
		out = c.queued[0]
		c.queued = c.queued[1:]
	} else {
		out = etOutcome{err: errors.New("etClient: no queued outcome")}
	}
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	return out.res, out.err
}

func (c *etClient) queue(res Result) {
	c.mu.Lock()
	c.queued = append(c.queued, etOutcome{res: res})
	c.mu.Unlock()
}

func (c *etClient) queueErr(err error) {
	c.mu.Lock()
	c.queued = append(c.queued, etOutcome{err: err})
	c.mu.Unlock()
}

func (c *etClient) counts() (creates, processes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.creates, c.processes
}

type etPauser struct {
	mu       sync.Mutex
	pauses   int
	err      error
	lights   []bool
	lightErr error
	speeds   []int
	offline  map[string]bool
}

func (p *etPauser) Pause(string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pauses++
	return p.err
}

func (p *etPauser) Speed(_ string, profile int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.speeds = append(p.speeds, profile)
	return p.err
}

func (p *etPauser) speedCalls() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int(nil), p.speeds...)
}

func (p *etPauser) Light(_ string, on bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lights = append(p.lights, on)
	return p.lightErr
}

func (p *etPauser) lightCalls() []bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]bool(nil), p.lights...)
}

func (p *etPauser) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pauses
}

func (p *etPauser) Online(serial string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.offline[serial]
}

func (p *etPauser) setOffline(serial string, offline bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.offline == nil {
		p.offline = map[string]bool{}
	}
	p.offline[serial] = offline
}

// ---- harness ----

type etWorld struct {
	clock    *etClock
	client   *etClient
	frames   *etFrames
	sessions *etSessions
	pauser   *etPauser
	engine   *Engine
	worker   *worker
}

func newEtWorld(t *testing.T) *etWorld {
	t.Helper()
	w := &etWorld{
		clock:    newEtClock(),
		client:   &etClient{},
		frames:   &etFrames{clock: newEtClock(), acquireOK: true, serve: true},
		sessions: &etSessions{},
		pauser:   &etPauser{},
	}
	// The frame fake shares the world clock.
	w.frames.clock = w.clock
	e := New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}},
		w.client, w.frames, w.sessions, w.pauser,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.now = w.clock.Now
	// Tighten nothing: tests drive step() directly, so the defaults never
	// sleep for real.
	w.engine, w.worker = e, e.workers["S1"]
	return w
}

func TestEngineActivityEventsFollowLifecycle(t *testing.T) {
	w := newEtWorld(t)
	printers := []config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}}
	activities := activity.New(printers)
	w.engine.SetActivity(activities)

	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	w.sessions.running(w.clock, 2, 1, "PAUSE")
	w.worker.step(context.Background())
	for _, kind := range []string{"ai_monitoring", "ai_pause_sent", "ai_pause_confirmed"} {
		if !hasActivityKind(activities.Recent("S1"), kind) {
			t.Fatalf("activity is missing %q: %+v", kind, activities.Recent("S1"))
		}
	}

	// Rebuild the pending pause, then let the confirm window expire on a
	// RUNNING sample, which routes through the guard to unconfirmed.
	w.clock.Advance(21 * time.Second)
	w.sessions.running(w.clock, 3, 1, "RUNNING")
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	w.clock.Advance(21 * time.Second)
	w.sessions.running(w.clock, 4, 1, "RUNNING")
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	w.clock.Advance(31 * time.Second)
	w.sessions.running(w.clock, 5, 1, "RUNNING")
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	if !hasActivityKind(activities.Recent("S1"), "ai_pause_unconfirmed") {
		t.Fatalf("activity is missing the unconfirmed pause: %+v", activities.Recent("S1"))
	}

	w.engine.suspend("account error")
	for _, serial := range []string{"S1"} {
		if !hasActivityKind(activities.Recent(serial), "ai_suspended") {
			t.Fatalf("activity is missing %q: %+v", "ai_suspended", activities.Recent(serial))
		}
	}
}

// ---- stale-evidence gate ----

// withSpeed adds speed-report evidence for the current session to the view,
// so warning-speed authorization can pass.
func withSpeed(w *etWorld, profile int, obs uint64) {
	view, _ := w.sessions.Session("S1")
	p := profile
	view.SpeedProfile = &p
	view.SpeedObs = obs
	w.sessions.set(view, true)
}

// hzCleanWorld builds a world with no prior result: the observed upload is
// the worker's first inspection, so a clean baseline isolates each boundary.
func hzCleanWorld(t *testing.T) (*etWorld, *bytes.Buffer, *activity.Log) {
	t.Helper()
	w := newEtWorld(t)
	activities := activity.New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}})
	w.engine.SetActivity(activities)
	var logs bytes.Buffer
	w.worker.log = slog.New(slog.NewTextHandler(&logs, nil)).With("serial", "S1")
	w.startSession(1, 1)
	return w, &logs, activities
}

// gateWorld builds a world whose worker already accepted one warning
// result, so risk counters, the published analysis, and resultAt all carry
// baseline values a stale result must preserve.
func gateWorld(t *testing.T) (*etWorld, *bytes.Buffer, *activity.Log) {
	t.Helper()
	w, logs, activities := hzCleanWorld(t)
	w.client.queue(Result{Intervals: IntervalSec{Minimum: 5, Recommended: 20}, PrintQuality: 3, WarningSuggested: true})
	w.worker.step(context.Background())
	return w, logs, activities
}

// gateBoundary enumerates one way the world can move while an upload is in
// flight. logHint is the fixed reason the discard log must carry; accepted
// boundaries mutate nothing.
type gateBoundary struct {
	name     string
	accepted bool
	logHint  string
	mutate   func(t *testing.T, w *etWorld)
}

func gateBoundaries() []gateBoundary {
	disconnect := func(t *testing.T, w *etWorld) {
		w.pauser.setOffline("S1", true)
	}
	return []gateBoundary{
		{name: "accepted", accepted: true},
		{name: "frame-expired", logHint: "reason=inspection_frame_expired", mutate: func(t *testing.T, w *etWorld) {
			w.clock.Advance(31 * time.Second)
		}},
		{name: "printer-disconnected", logHint: "reason=printer_disconnected", mutate: disconnect},
		{name: "session-change", logHint: "print session changed during upload", mutate: func(t *testing.T, w *etWorld) {
			w.sessions.running(w.clock, 2, 2, "RUNNING")
		}},
		{name: "override-change", logHint: "detection availability changed", mutate: func(t *testing.T, w *etWorld) {
			if _, err := w.engine.SetDetectionEnabled("S1", false, w.engine.sessionToken(1)); err != nil {
				t.Errorf("disable during upload: %v", err)
			}
		}},
	}
}

// gateBaselines snapshots every piece of state a rejected result must
// preserve.
type gateBaselines struct {
	resultAt      time.Time
	awaitingClear bool
	riskActive    bool
	clearCount    int
	clearSince    time.Time
	pauseState    string
	speedOwned    bool
	speedPending  bool
	speedPrior    *int
	pauseCommands int
	speedCommands int
	activities    int
	quality       int
	warning       bool
	faster        bool
	state         string
}

func gateSnapshot(w *etWorld, activities *activity.Log) gateBaselines {
	w.worker.mu.Lock()
	base := gateBaselines{
		resultAt:      w.worker.resultAt,
		awaitingClear: w.worker.awaitingClear,
		riskActive:    w.worker.riskActive,
		clearCount:    w.worker.clearCount,
		clearSince:    w.worker.clearSince,
		pauseState:    w.worker.pauseState,
		speedOwned:    w.worker.speedOwned,
		speedPending:  w.worker.speedPending,
		speedPrior:    w.worker.speedPrior,
		pauseCommands: w.pauser.calls(),
		speedCommands: len(w.pauser.speedCalls()),
	}
	w.worker.mu.Unlock()
	st := w.status()
	base.quality, base.warning, base.faster = st.Quality, st.Warning, st.FasterInspection
	base.state = st.State
	base.activities = len(activities.Recent("S1"))
	return base
}

// gateAssertUnchanged fails when a rejected result moved any guarded state.
// strictDisplay also pins the published display state, which the user
// override legitimately re-projects.
func gateAssertUnchanged(t *testing.T, w *etWorld, activities *activity.Log, base gateBaselines, strictDisplay bool) {
	t.Helper()
	// status() re-enters w.mu through view, so snapshot it first.
	st := w.status()
	w.worker.mu.Lock()
	defer w.worker.mu.Unlock()
	if !w.worker.resultAt.Equal(base.resultAt) {
		t.Errorf("resultAt moved: %v -> %v", base.resultAt, w.worker.resultAt)
	}
	if w.worker.awaitingClear != base.awaitingClear {
		t.Errorf("awaitingClear changed: %v -> %v", base.awaitingClear, w.worker.awaitingClear)
	}
	if w.worker.riskActive != base.riskActive || w.worker.clearCount != base.clearCount || !w.worker.clearSince.Equal(base.clearSince) {
		t.Errorf("risk counters changed: active %v->%v count %d->%d since %v->%v",
			base.riskActive, w.worker.riskActive, base.clearCount, w.worker.clearCount, base.clearSince, w.worker.clearSince)
	}
	if w.worker.pauseState != base.pauseState {
		t.Errorf("pauseState changed: %q -> %q", base.pauseState, w.worker.pauseState)
	}
	if w.worker.speedOwned != base.speedOwned || w.worker.speedPending != base.speedPending {
		t.Errorf("speed ownership changed: owned %v->%v pending %v->%v", base.speedOwned, w.worker.speedOwned, base.speedPending, w.worker.speedPending)
	}
	if (w.worker.speedPrior == nil) != (base.speedPrior == nil) ||
		(w.worker.speedPrior != nil && base.speedPrior != nil && *w.worker.speedPrior != *base.speedPrior) {
		t.Errorf("speedPrior changed: %v -> %v", base.speedPrior, w.worker.speedPrior)
	}
	if w.pauser.calls() != base.pauseCommands {
		t.Errorf("pause commands changed: %d -> %d", base.pauseCommands, w.pauser.calls())
	}
	if len(w.pauser.speedCalls()) != base.speedCommands {
		t.Errorf("speed commands changed: %d -> %d", base.speedCommands, len(w.pauser.speedCalls()))
	}
	if st.Quality != base.quality || st.Warning != base.warning || st.FasterInspection != base.faster {
		t.Errorf("published analysis changed: quality %d->%d warning %v->%v faster %v->%v",
			base.quality, st.Quality, base.warning, st.Warning, base.faster, st.FasterInspection)
	}
	if strictDisplay && st.State != base.state {
		t.Errorf("display state changed: %q -> %q", base.state, st.State)
	}
	if got := len(activities.Recent("S1")); got != base.activities {
		t.Errorf("activity events changed: %d -> %d", base.activities, got)
	}
}

// runGateSchedule drives one upload whose evidence may go stale in flight:
// it becomes due again with fresh telemetry, then steps once.
func runGateSchedule(t *testing.T, w *etWorld, boundary gateBoundary, res Result) time.Duration {
	t.Helper()
	w.clock.Advance(5 * time.Second)
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	w.client.queue(res)
	if !boundary.accepted && boundary.mutate != nil {
		w.client.beforeReply = func() { boundary.mutate(t, w) }
	}
	return w.worker.step(context.Background())
}

func TestEngineResultGateRejectsStaleEvidence(t *testing.T) {
	results := map[string]Result{
		"clear":   {Intervals: IntervalSec{Minimum: 5, Recommended: 20}, PrintQuality: 8},
		"warning": {Intervals: IntervalSec{Minimum: 5, Recommended: 20}, PrintQuality: 3, WarningSuggested: true},
		"pause":   {Intervals: IntervalSec{Minimum: 5, Recommended: 20}, PrintQuality: 8, PauseSuggested: true},
	}
	for _, boundary := range gateBoundaries() {
		if boundary.accepted {
			continue
		}
		for name, res := range results {
			t.Run(boundary.name+"/"+name, func(t *testing.T) {
				w, logs, activities := gateWorld(t)
				base := gateSnapshot(w, activities)
				delay := runGateSchedule(t, w, boundary, res)
				if delay != 5*time.Second {
					t.Errorf("delay = %v, want provider-interval pacing to survive rejection", delay)
				}
				gateAssertUnchanged(t, w, activities, base, boundary.name != "override-change")
				log := logs.String()
				for _, hint := range []string{"inspection result discarded", boundary.logHint} {
					if !strings.Contains(log, hint) {
						t.Errorf("log missing %q: %s", hint, log)
					}
				}
			})
		}
	}
}

func TestEngineResultGateAcceptsFreshResults(t *testing.T) {
	w, logs, activities := gateWorld(t)

	// A fresh clear publishes, starts the clear sequence, and clears the
	// prior warning with an activity event.
	delay := runGateSchedule(t, w, gateBoundary{}, etClearResult())
	if delay != 5*time.Second {
		t.Fatalf("delay = %v, want provider pacing", delay)
	}
	if st := w.status(); st.Quality != 8 || st.Warning {
		t.Fatalf("clear not published: %+v", st)
	}
	w.worker.mu.Lock()
	clearCount, riskActive := w.worker.clearCount, w.worker.riskActive
	w.worker.mu.Unlock()
	if clearCount != 1 || !riskActive {
		t.Fatalf("clearCount/riskActive = %d/%v, want 1/true", clearCount, riskActive)
	}
	if !hasActivityKind(activities.Recent("S1"), "ai_warning_cleared") {
		t.Fatalf("activity missing ai_warning_cleared: %+v", activities.Recent("S1"))
	}

	// A fresh warning refreshes the risk window and the published analysis.
	w.clock.Advance(5 * time.Second)
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	w.client.queue(Result{Intervals: IntervalSec{Minimum: 5, Recommended: 20}, PrintQuality: 2, WarningSuggested: true})
	w.worker.step(context.Background())
	w.worker.mu.Lock()
	clearCount = w.worker.clearCount
	w.worker.mu.Unlock()
	if clearCount != 0 {
		t.Fatalf("clearCount = %d, want the risk window reset", clearCount)
	}
	if st := w.status(); st.Quality != 2 || !st.Warning {
		t.Fatalf("warning not published: %+v", st)
	}

	// A fresh pause suggestion still dispatches exactly one pause.
	w.clock.Advance(5 * time.Second)
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 1 {
		t.Fatalf("pause calls = %d, want one", calls)
	}
	if got := w.status().PauseState; got != PausePending {
		t.Fatalf("pause_state = %q, want pending", got)
	}
	w.worker.mu.Lock()
	latched := w.worker.awaitingClear
	w.worker.mu.Unlock()
	if !latched {
		t.Fatal("pause latch not set after a dispatched pause")
	}
	if !hasActivityKind(activities.Recent("S1"), "ai_pause_sent") {
		t.Fatalf("activity missing ai_pause_sent: %+v", activities.Recent("S1"))
	}
	if strings.Contains(logs.String(), "inspection result discarded") {
		t.Fatalf("fresh results were discarded: %s", logs.String())
	}
}

func TestEngineWarningSpeedEpisodeSendsOnceAndRestoresOnClear(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	withSpeed(w, 3, 1)
	w.client.queue(Result{Intervals: IntervalSec{Minimum: 5, Recommended: 20}, PrintQuality: 3, WarningSuggested: true})
	w.worker.step(context.Background())

	speeds := w.pauser.speedCalls()
	if len(speeds) != 1 || speeds[0] != 1 {
		t.Fatalf("speed calls = %+v, want one Silent command", speeds)
	}

	// A newer speed report on the same connection confirms the command.
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	withSpeed(w, 1, 2)
	snap, _ := w.sessions.Session("S1")
	w.worker.controlMu.Lock()
	w.worker.reconcileSpeed(snap)
	w.worker.controlMu.Unlock()

	// A fresh clear result restores the saved profile exactly once and
	// closes the episode.
	w.clock.Advance(5 * time.Second)
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	withSpeed(w, 1, 2)
	w.client.queue(etClearResult())
	w.worker.step(context.Background())

	speeds = w.pauser.speedCalls()
	if len(speeds) != 2 || speeds[0] != 1 || speeds[1] != 3 {
		t.Fatalf("speed calls = %+v, want one Silent then one restore of profile 3", speeds)
	}
	w.worker.controlMu.Lock()
	owned, pending, episode, prior := w.worker.speedOwned, w.worker.speedPending, w.worker.warningEpisode, w.worker.speedPrior
	w.worker.controlMu.Unlock()
	if owned || pending || episode || prior != nil {
		t.Fatalf("speed episode state = owned %v pending %v episode %v prior %v, want all clear", owned, pending, episode, prior)
	}
	if st := w.status(); st.Quality != 8 || st.Warning {
		t.Fatalf("clear not published: %+v", st)
	}
}

func TestEngineStaleClearKeepsSpeedOwnershipAndRiskCounters(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	withSpeed(w, 3, 1)
	w.client.queue(Result{Intervals: IntervalSec{Minimum: 5, Recommended: 20}, PrintQuality: 3, WarningSuggested: true})
	w.worker.step(context.Background())
	if len(w.pauser.speedCalls()) != 1 {
		t.Fatalf("speed calls = %+v, want the initial Silent", w.pauser.speedCalls())
	}

	// Confirm the Silent profile as owned.
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	withSpeed(w, 1, 2)
	snap, _ := w.sessions.Session("S1")
	w.worker.controlMu.Lock()
	w.worker.reconcileSpeed(snap)
	w.worker.controlMu.Unlock()

	activities := activity.New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}})
	w.engine.SetActivity(activities)
	base := gateSnapshot(w, activities)

	// The clear result goes stale during the upload: the connection drops.
	w.client.queue(etClearResult())
	w.client.beforeReply = func() {
		w.pauser.setOffline("S1", true)
	}
	w.clock.Advance(5 * time.Second)
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	withSpeed(w, 1, 2)
	delay := w.worker.step(context.Background())

	if delay != 5*time.Second {
		t.Fatalf("delay = %v, want provider pacing on a rejected clear", delay)
	}
	gateAssertUnchanged(t, w, activities, base, true)
	w.worker.mu.Lock()
	intensive := w.worker.intensiveLocked(w.clock.Now())
	owned, prior := w.worker.speedOwned, w.worker.speedPrior
	w.worker.mu.Unlock()
	if !intensive {
		t.Fatal("rejected clear exited the risk-triggered intensive mode")
	}
	if !owned || prior == nil || *prior != 3 {
		t.Fatalf("speed ownership after rejection = owned %v prior %v, want owned with profile 3 saved", owned, prior)
	}
}

// startSession starts a RUNNING print observation with an online printer.
func (w *etWorld) startSession(epoch, sessionGen uint64) {
	w.sessions.running(w.clock, epoch, sessionGen, "RUNNING")
}

func (w *etWorld) status() *Status {
	return w.engine.DetectionStatus("S1").(*Status)
}

func etClearResult() Result {
	return Result{Intervals: IntervalSec{Minimum: 5, Recommended: 20}, PrintQuality: 8}
}

func etPauseResult() Result {
	return Result{Intervals: IntervalSec{Minimum: 5, Recommended: 20}, PrintQuality: 8, PauseSuggested: true}
}

func intPtr(v int) *int { return &v }

// ---- tests ----

func TestEngineFirstInspectionFiresImmediately(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etClearResult())

	delay := w.worker.step(context.Background())
	if delay != 5*time.Second {
		t.Fatalf("delay after first inspection = %v, want intensive Minimum 5s", delay)
	}
	creates, processes := w.client.counts()
	if creates != 1 || processes != 1 {
		t.Fatalf("creates/processes = %d/%d, want 1/1 without any initial wait", creates, processes)
	}
	st := w.status()
	if st.State != StateMonitoring || st.PauseState != PauseNone {
		t.Fatalf("state/pause = %q/%q, want monitoring/none", st.State, st.PauseState)
	}
	if st.LastInspectedLayer == nil || *st.LastInspectedLayer != 4 {
		t.Fatalf("last_inspected_layer = %v, want the reported layer 4", st.LastInspectedLayer)
	}
	if st.AgeSeconds < 0 {
		t.Fatalf("age_seconds = %v, want a computed non-negative age", st.AgeSeconds)
	}
}

func TestEngineLayerAlignedToFrameCaptureNotUpload(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etClearResult())
	// Session calls during one inspection: step entry, post-frame alignment,
	// result continuity. The layer advanced between entry and capture.
	w.sessions.mu.Lock()
	w.sessions.layers = []int{3, 7, 7}
	w.sessions.mu.Unlock()

	w.worker.step(context.Background())
	if got := w.status().LastInspectedLayer; got == nil || *got != 7 {
		t.Fatalf("last_inspected_layer = %v, want the post-frame layer 7", got)
	}
}

// TestEngineFirstLayerWaitIsNeutralWithoutAcquireOrUpload pins the layer
// gate: a RUNNING print at layer 0 or with an unknown layer must not touch
// the camera or the API, and the first positive layer must start the first
// inspection immediately. The wait is the print's normal opening, so it
// must not raise the degraded state, record an ai_degraded warning, or
// claim an API retry.
func TestEngineFirstLayerWaitIsNeutralWithoutAcquireOrUpload(t *testing.T) {
	running := func(w *etWorld, layer *int) {
		w.sessions.set(telemetry.SessionView{
			Serial: "S1", Active: true, State: "RUNNING",
			SessionGen: 1, Epoch: 1,
			ObsAt: w.clock.Now(), Obs: 1, LayerNum: layer, LayerThisJob: true,
		}, true)
	}
	for _, tc := range []struct {
		name  string
		layer *int
	}{{"layer_zero", intPtr(0)}, {"layer_unknown", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			w := newEtWorld(t)
			activities := activity.New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}})
			w.engine.SetActivity(activities)
			running(w, tc.layer)

			delay := w.worker.step(context.Background())
			if delay != w.engine.kStalePoll {
				t.Fatalf("delay = %v, want the waiting stale poll", delay)
			}
			if w.frames.held != 0 || w.frames.waitCalls != 0 {
				t.Fatalf("camera acquires/waits = %d/%d, want none while the layer is not positive",
					w.frames.held, w.frames.waitCalls)
			}
			if creates, processes := w.client.counts(); creates != 0 || processes != 0 {
				t.Fatalf("creates/processes = %d/%d, want none while the layer is not positive", creates, processes)
			}
			st := w.status()
			if st.State != StateMonitoring || st.Reason != ReasonAwaitingFirstLayer {
				t.Fatalf("state/reason = %q/%q, want the neutral first-layer wait", st.State, st.Reason)
			}
			if st.NextCheckSeconds != 0 {
				t.Fatalf("next_check_seconds = %v, want none while reports alone gate the wait", st.NextCheckSeconds)
			}
			if w.worker.degraded != "" || !w.worker.nextDue.IsZero() {
				t.Fatalf("degraded/nextDue = %q/%v, want no internal waiting bookkeeping in the neutral wait",
					w.worker.degraded, w.worker.nextDue)
			}
			if hasActivityKind(activities.Recent("S1"), "ai_degraded") {
				t.Fatalf("activity recorded ai_degraded for the normal first-layer wait: %+v", activities.Recent("S1"))
			}

			// The first report with a positive layer starts the inspection
			// immediately: the waiting gate must not insert a schedule delay.
			running(w, intPtr(1))
			w.client.queue(etClearResult())
			delay = w.worker.step(context.Background())
			creates, processes := w.client.counts()
			if creates != 1 || processes != 1 {
				t.Fatalf("creates/processes = %d/%d, want the first inspection on the first positive layer", creates, processes)
			}
			if delay != 5*time.Second {
				t.Fatalf("delay after first inspection = %v, want the intensive Minimum 5s", delay)
			}
			if st := w.status(); st.State != StateMonitoring {
				t.Fatalf("state = %q, want monitoring after the inspection", st.State)
			}
		})
	}
}

// TestEngineLayerLossAfterReadinessDegrades pins the lost-evidence split:
// once a print has shown a positive layer, a missing layer is lost evidence
// and must degrade again instead of repeating the neutral first-layer wait.
// Readiness must survive a capture failure that never created the Gadget
// context, where haveSession alone cannot tell the two waits apart.
func TestEngineLayerLossAfterReadinessDegrades(t *testing.T) {
	// dropLayer simulates a lost layer report: the print keeps running on
	// the same live connection, but the layer is no longer reported.
	dropLayer := func(w *etWorld) {
		w.sessions.running(w.clock, 1, 1, "RUNNING")
		v, _ := w.sessions.Session("S1")
		v.LayerNum = nil
		v.LayerThisJob = false
		w.sessions.set(v, true)
	}
	t.Run("readiness_survives_capture_failure", func(t *testing.T) {
		w := newEtWorld(t)
		w.startSession(1, 1)
		w.frames.acquireOK = false
		if delay := w.worker.step(context.Background()); delay != w.engine.kCameraRetry {
			t.Fatalf("delay = %v, want the camera retry poll", delay)
		}
		if creates, _ := w.client.counts(); creates != 0 {
			t.Fatalf("creates = %d, want no Gadget context before the capture", creates)
		}

		dropLayer(w)
		delay := w.worker.step(context.Background())
		if delay != w.engine.kStalePoll {
			t.Fatalf("delay = %v, want the stale telemetry poll after the loss", delay)
		}
		if st := w.status(); st.State != StateDegraded || st.Reason != ReasonAwaitingTelemetry {
			t.Fatalf("state/reason = %q/%q, want the degraded lost-evidence wait", st.State, st.Reason)
		}
		if creates, processes := w.client.counts(); creates != 0 || processes != 0 {
			t.Fatalf("creates/processes = %d/%d, want the loss to add no API work", creates, processes)
		}
	})
	t.Run("clean_loss_warns", func(t *testing.T) {
		w := newEtWorld(t)
		activities := activity.New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}})
		w.engine.SetActivity(activities)
		w.startSession(1, 1)
		w.client.queue(etClearResult())
		w.worker.step(context.Background())
		if hasActivityKind(activities.Recent("S1"), "ai_degraded") {
			t.Fatalf("activity recorded ai_degraded for the clean first inspection: %+v", activities.Recent("S1"))
		}

		dropLayer(w)
		delay := w.worker.step(context.Background())
		if delay != w.engine.kStalePoll {
			t.Fatalf("delay = %v, want the stale telemetry poll after the loss", delay)
		}
		if st := w.status(); st.State != StateDegraded || st.Reason != ReasonAwaitingTelemetry {
			t.Fatalf("state/reason = %q/%q, want the degraded lost-evidence wait", st.State, st.Reason)
		}
		if hasActivityKind(activities.Recent("S1"), "ai_degraded") {
			t.Fatalf("activity recorded ai_degraded before the log delay: %+v", activities.Recent("S1"))
		}
		w.clock.Advance(degradeLogDelay)
		if left := w.worker.flushDegradedLog(w.clock.Now()); left != 0 {
			t.Fatalf("flush left = %v, want the interruption due", left)
		}
		if !hasActivityKind(activities.Recent("S1"), "ai_degraded") {
			t.Fatalf("activity is missing ai_degraded for the lost layer: %+v", activities.Recent("S1"))
		}
	})
}

// TestEngineUnauthorizedMissingLayerDegrades pins the gate order: an
// offline RUNNING with no layer must degrade as disconnected, never
// present the neutral first-layer wait, whatever the layer evidence
// history. The recovery phase proves a printer back online that still
// lacks a positive layer turns the wait neutral and clears the internal
// awaiting-telemetry degradation and its poll schedule, so no countdown
// remains and no new warning is recorded.
func TestEngineUnauthorizedMissingLayerDegrades(t *testing.T) {
	disconnect := func(w *etWorld) {
		w.pauser.setOffline("S1", true)
	}
	recover := func(w *etWorld) {
		w.pauser.setOffline("S1", false)
	}
	for _, tc := range []struct {
		name    string
		layer   *int
		shake   func(w *etWorld)
		recover func(w *etWorld)
	}{
		{"disconnected_zero_layer", intPtr(0), disconnect, recover},
		{"disconnected_unknown_layer", nil, disconnect, recover},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newEtWorld(t)
			activities := activity.New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}})
			w.engine.SetActivity(activities)
			w.sessions.set(telemetry.SessionView{
				Serial: "S1", Active: true, State: "RUNNING",
				SessionGen: 1, Epoch: 1,
				ObsAt: w.clock.Now(), Obs: 1, LayerNum: tc.layer, LayerThisJob: true,
			}, true)
			tc.shake(w)

			// The unauthorized observation degrades through the
			// authorization gate, whatever the layer.
			delay := w.worker.step(context.Background())
			if delay != w.engine.kStalePoll {
				t.Fatalf("delay = %v, want the stale telemetry poll", delay)
			}
			st := w.status()
			if st.State != StateDegraded || st.Reason != ReasonAwaitingTelemetry {
				t.Fatalf("state/reason = %q/%q, want degraded while disconnected", st.State, st.Reason)
			}
			if st.Message != "printer disconnected" {
				t.Fatalf("message = %q, want the authorization gate message", st.Message)
			}
			if w.frames.held != 0 {
				t.Fatalf("camera acquires = %d, want none while unauthorized", w.frames.held)
			}
			if creates, processes := w.client.counts(); creates != 0 || processes != 0 {
				t.Fatalf("creates/processes = %d/%d, want none while unauthorized", creates, processes)
			}
			// Recovery may legitimately record other events (for example the
			// chamber light auto-on); only the warning count is the contract.
			degradedCount := func() int {
				n := 0
				for _, event := range activities.Recent("S1") {
					if event.Kind == "ai_degraded" {
						n++
					}
				}
				return n
			}
			warnings := degradedCount()

			// A printer back online that still lacks a positive layer
			// recovers the neutral wait: the internal
			// awaiting-telemetry degradation and its poll schedule clear,
			// so no countdown remains and no new warning is recorded.
			tc.recover(w)
			delay = w.worker.step(context.Background())
			if delay != w.engine.kStalePoll {
				t.Fatalf("delay = %v, want the neutral waiting poll after recovery", delay)
			}
			st = w.status()
			if st.State != StateMonitoring || st.Reason != ReasonAwaitingFirstLayer {
				t.Fatalf("state/reason = %q/%q, want the neutral first-layer wait after recovery", st.State, st.Reason)
			}
			if st.NextCheckSeconds != 0 {
				t.Fatalf("next_check_seconds = %v, want no countdown after recovery", st.NextCheckSeconds)
			}
			if w.worker.degraded != "" || !w.worker.nextDue.IsZero() {
				t.Fatalf("degraded/nextDue = %q/%v, want the waiting bookkeeping cleared",
					w.worker.degraded, w.worker.nextDue)
			}
			if n := degradedCount(); n != warnings {
				t.Fatalf("ai_degraded entries = %d, want the pre-recovery %d unchanged", n, warnings)
			}
		})
	}
}

// TestEngineWorldChangeDuringCaptureDiscardsFrame pins the capture-time
// recheck: a new print boundary, a layer reset, or a disconnect while the
// camera waited must discard the frame without spending the upload.
func TestEngineWorldChangeDuringCaptureDiscardsFrame(t *testing.T) {
	for _, tc := range []struct {
		name  string
		shake func(w *etWorld)
	}{
		{"new_print_unknown_layer", func(w *etWorld) {
			v, _ := w.sessions.Session("S1")
			v.Epoch, v.SessionGen, v.LayerNum = 2, 2, nil
			v.LayerThisJob = false
			w.sessions.set(v, true)
		}},
		{"layer_reset_to_zero", func(w *etWorld) {
			v, _ := w.sessions.Session("S1")
			v.LayerNum = intPtr(0)
			v.LayerThisJob = true
			w.sessions.set(v, true)
		}},
		// The printer went offline during the camera wait: the frame may
		// not upload while the printer is disconnected.
		{"disconnected_during_capture", func(w *etWorld) {
			w.pauser.setOffline("S1", true)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newEtWorld(t)
			w.startSession(1, 1)
			// The world moves inside the real camera wait, so the entry
			// snapshot authorizes the attempt and the post-frame read sees
			// the shaken world.
			w.frames.duringWait = func() { tc.shake(w) }

			delay := w.worker.step(context.Background())
			if delay != w.engine.kStalePoll {
				t.Fatalf("delay = %v, want the waiting stale poll after the discard", delay)
			}
			if w.frames.waitCalls != 1 {
				t.Fatalf("frame waits = %d, want exactly one capture before the discard", w.frames.waitCalls)
			}
			if creates, processes := w.client.counts(); creates != 0 || processes != 0 {
				t.Fatalf("creates/processes = %d/%d, want the frame discarded without a request", creates, processes)
			}
			if st := w.status(); st.Reason != ReasonAwaitingTelemetry {
				t.Fatalf("reason = %q, want the waiting status after the discard", st.Reason)
			}
		})
	}
}

// TestEngineGateWaitsReleaseCameraWithoutAcquire pins the pre-acquire
// gates: a waiting layer or authorization must not take the camera, and a
// hold kept from an earlier inspection is released while the gate waits.
func TestEngineGateWaitsReleaseCameraWithoutAcquire(t *testing.T) {
	for _, tc := range []struct {
		name    string
		shake   func(w *etWorld)
		recover func(w *etWorld)
	}{
		{"layer_gone_unknown", func(w *etWorld) {
			v, _ := w.sessions.Session("S1")
			v.LayerNum = nil
			w.sessions.set(v, true)
		}, nil},
		{"disconnected", func(w *etWorld) {
			w.pauser.setOffline("S1", true)
		}, func(w *etWorld) {
			w.pauser.setOffline("S1", false)
		}},
		// The printer went offline mid-print: the cached RUNNING must not
		// authorize camera work until the printer is online again.
		{"offline_before_any_report",
			func(w *etWorld) {
				w.pauser.setOffline("S1", true)
			},
			func(w *etWorld) {
				w.pauser.setOffline("S1", false)
				w.sessions.running(w.clock, 1, 1, "RUNNING")
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newEtWorld(t)
			// One full inspection first: the camera hold and one upload exist.
			w.startSession(1, 1)
			w.client.queue(etClearResult())
			w.worker.step(context.Background())
			if creates, processes := w.client.counts(); creates != 1 || processes != 1 {
				t.Fatalf("creates/processes = %d/%d, want the baseline inspection", creates, processes)
			}
			if w.frames.held != 1 || w.frames.waitCalls != 1 {
				t.Fatalf("acquires/waits = %d/%d, want the baseline hold and capture", w.frames.held, w.frames.waitCalls)
			}

			// The gate condition appears before any new report arrives: the
			// next step must release the hold and neither acquire the
			// camera again nor call the API.
			tc.shake(w)
			w.worker.step(context.Background())
			if w.frames.held != 1 || w.frames.waitCalls != 1 {
				t.Fatalf("acquires/waits = %d/%d, want the gates to add no camera work", w.frames.held, w.frames.waitCalls)
			}
			if w.frames.released != 1 {
				t.Fatalf("releases = %d, want the waiting gate to release the hold", w.frames.released)
			}
			if _, processes := w.client.counts(); processes != 1 {
				t.Fatalf("processes = %d, want no upload while gated", processes)
			}
			if st := w.status(); st.Reason != ReasonAwaitingTelemetry {
				t.Fatalf("reason = %q, want the waiting status", st.Reason)
			}

			// The next fresh report with an eligible layer restarts the
			// inspection promptly. The Gadget context survives: the print
			// session never changed.
			if tc.recover != nil {
				tc.recover(w)
			} else {
				w.sessions.running(w.clock, 1, 1, "RUNNING")
			}
			w.client.queue(etClearResult())
			delay := w.worker.step(context.Background())
			if _, processes := w.client.counts(); processes != 2 {
				t.Fatalf("processes = %d, want the inspection to resume promptly", processes)
			}
			if delay != 5*time.Second {
				t.Fatalf("delay after the resumed inspection = %v, want the intensive Minimum 5s", delay)
			}
		})
	}
}

func TestEngineSecondInspectionWaitsForSchedule(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etClearResult())
	w.client.queue(etClearResult())
	w.worker.step(context.Background())

	// Not yet due: no new upload, and the returned delay is the remainder.
	w.clock.Advance(2 * time.Second)
	delay := w.worker.step(context.Background())
	if delay != 3*time.Second {
		t.Fatalf("delay = %v, want the 3s intensive-schedule remainder", delay)
	}
	if _, processes := w.client.counts(); processes != 1 {
		t.Fatalf("processes = %d, want the schedule to gate the second upload", processes)
	}

	w.clock.Advance(3 * time.Second)
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	w.worker.step(context.Background())
	if _, processes := w.client.counts(); processes != 2 {
		t.Fatalf("processes = %d, want the second upload once due", processes)
	}
	if delay := w.worker.nextDue.Sub(w.clock.Now()); delay != 5*time.Second {
		t.Fatalf("second upload schedule = %v, want the intensive 5s Minimum", delay)
	}
}

func TestEnginePauseLatchRearmsWhenPauseAndWarningFlagsClear(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 1 {
		t.Fatalf("pause calls = %d, want one initial pause", calls)
	}

	w.clock.Advance(5 * time.Second)
	w.sessions.running(w.clock, 2, 1, "RUNNING")
	w.client.queue(Result{
		Intervals:    IntervalSec{Minimum: 5, Recommended: 40},
		PrintQuality: 8, FasterInspectionSuggested: true,
	})
	w.worker.step(context.Background())
	if w.worker.awaitingClear {
		t.Fatal("pause latch stayed armed after both warning and pause flags cleared")
	}
	if got := w.status().FasterInspection; !got {
		t.Fatal("status did not preserve FasterInspectionSuggested")
	}

	w.clock.Advance(5 * time.Second)
	w.sessions.running(w.clock, 3, 1, "RUNNING")
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 2 {
		t.Fatalf("pause calls = %d, want the clear flags to rearm despite faster/quality cadence", calls)
	}
}

func TestEngineRunningInsideConfirmWindowKeepsPending(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if got := w.status().PauseState; got != PausePending {
		t.Fatalf("pause_state = %q, want pending after the pause command", got)
	}

	// Normal late printer reaction: RUNNING samples inside the 30s window
	// never cancel or confirm the pending pause.
	w.clock.Advance(21 * time.Second)
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	w.client.queue(etClearResult())
	w.worker.step(context.Background())

	if got := w.status().PauseState; got != PausePending {
		t.Fatalf("pause_state = %q, want pending to survive RUNNING inside the window", got)
	}
	if calls := w.pauser.calls(); calls != 1 {
		t.Fatalf("pause calls = %d, want exactly one command", calls)
	}
}

func TestEngineConfirmWindowExpiryMarksUnconfirmed(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())

	w.clock.Advance(31 * time.Second)
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	w.client.queue(etClearResult())
	w.worker.step(context.Background())

	st := w.status()
	if st.PauseState != PauseUnconfirmed {
		t.Fatalf("pause_state = %q, want unconfirmed past the deadline", st.PauseState)
	}
	if st.State != StateAttention {
		t.Fatalf("state = %q, want attention", st.State)
	}
}

func TestEnginePauseConfirmedOnceThenRearmsOnClear(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())

	// The printer reports PAUSE: the confirmation is a single transition.
	w.sessions.running(w.clock, 2, 1, "PAUSE")
	delay := w.worker.step(context.Background())
	st := w.status()
	if st.PauseState != PauseConfirmed || st.State != StatePaused {
		t.Fatalf("pause/state = %q/%q, want confirmed/paused", st.PauseState, st.State)
	}
	if delay != w.engine.kIdlePoll {
		t.Fatalf("delay = %v, want the idle poll while paused", delay)
	}

	// Suggestions while the latch is set never re-pause.
	w.clock.Advance(21 * time.Second)
	w.sessions.running(w.clock, 3, 1, "RUNNING")
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 1 {
		t.Fatalf("pause calls = %d, want the latch to block a second command", calls)
	}

	// A fresh clear result rearms the latch; the next suggestion pauses.
	w.clock.Advance(21 * time.Second)
	w.sessions.running(w.clock, 3, 1, "RUNNING")
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	w.clock.Advance(21 * time.Second)
	w.sessions.running(w.clock, 3, 1, "RUNNING")
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 2 {
		t.Fatalf("pause calls = %d, want the rearm to allow a new command", calls)
	}
	if got := w.status().PauseState; got != PausePending {
		t.Fatalf("pause_state = %q, want pending after the rearmed command", got)
	}
}

func TestEngineResumeAfterConfirmedPauseClearsLifecycle(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	w.sessions.running(w.clock, 2, 1, "PAUSE")
	w.worker.step(context.Background())

	// The user resumed the print: the pause is historical.
	w.sessions.running(w.clock, 3, 1, "RUNNING")
	w.worker.step(context.Background())
	st := w.status()
	if st.PauseState != PauseNone {
		t.Fatalf("pause_state = %q, want none after the user resumed", st.PauseState)
	}
	if st.State != StateMonitoring {
		t.Fatalf("state = %q, want monitoring after resume", st.State)
	}
}

func TestEnginePauseCommandFailureIsSingleShot(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.pauser.err = errors.New("printer offline")
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())

	st := w.status()
	if st.PauseState != PauseUnconfirmed || st.Reason != ReasonPauseFailed {
		t.Fatalf("pause/reason = %q/%q, want unconfirmed/%s", st.PauseState, st.Reason, ReasonPauseFailed)
	}

	// The rejected suggestion is final: further suggestions never re-send
	// until a fresh clear rearms.
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 1 {
		t.Fatalf("pause calls = %d, want a rejected command to stay final", calls)
	}
}

func TestEngineEpochChangeResetsContextAndLifecycle(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())

	// Coalesced FINISH-to-RUNNING: a new print with a new epoch, no idle gap.
	w.clock.Advance(40 * time.Second)
	w.sessions.running(w.clock, 2, 2, "RUNNING")
	delay := w.worker.step(context.Background())

	creates, processes := w.client.counts()
	if creates != 2 || processes != 2 {
		t.Fatalf("creates/processes = %d/%d, want a fresh context for the new session", creates, processes)
	}
	st := w.status()
	if st.PauseState != PauseNone {
		t.Fatalf("pause_state = %q, want the old lifecycle dropped", st.PauseState)
	}
	if st.Quality != 0 || st.Warning || st.LastInspectedLayer != nil {
		t.Fatalf("sticky result leaked into the new session: %+v", st)
	}
	if delay < 0 {
		t.Fatalf("delay = %v, want a live schedule after the reset", delay)
	}
	// The new session's first inspection fires immediately.
	if _, processes := w.client.counts(); processes != 2 {
		t.Fatalf("processes = %d, want an immediate inspection for the new print", processes)
	}
}

func TestEngineDelayedResultDiscardedOnEpochChange(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	// The result arrives after the print changed: a coalesced new session.
	w.client.beforeReply = func() {
		w.sessions.running(w.clock, 2, 2, "RUNNING")
	}
	delay := w.worker.step(context.Background())

	if calls := w.pauser.calls(); calls != 0 {
		t.Fatalf("pause calls = %d, want the stale suggestion discarded", calls)
	}
	if delay != w.engine.kStalePoll {
		t.Fatalf("delay = %v, want the stale-result re-evaluation delay", delay)
	}
	st := w.status()
	if st.Quality != 0 || st.Warning || st.LastInspectedLayer != nil {
		t.Fatalf("stale result published: %+v", st)
	}
	if st.PauseState != PauseNone {
		t.Fatalf("pause_state = %q, want none after discard", st.PauseState)
	}
}

func TestEngineDelayedResultDiscardedOnUserPause(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	w.client.beforeReply = func() {
		w.sessions.running(w.clock, 2, 1, "PAUSE")
	}
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 0 {
		t.Fatalf("pause calls = %d, want no command once the printer paused", calls)
	}
}

func TestEngineDelayedResultDiscardedOnDisconnect(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	w.client.beforeReply = func() {
		w.pauser.setOffline("S1", true)
	}
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 0 {
		t.Fatalf("pause calls = %d, want no command once the printer is offline", calls)
	}
}

func TestEngineDisconnectedNeverAuthorizes(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.pauser.setOffline("S1", true)

	delay := w.worker.step(context.Background())
	creates, processes := w.client.counts()
	if creates != 0 || processes != 0 {
		t.Fatalf("creates/processes = %d/%d, want no API activity while disconnected", creates, processes)
	}
	if got := w.status().Reason; got != ReasonAwaitingTelemetry {
		t.Fatalf("reason = %q, want %s", got, ReasonAwaitingTelemetry)
	}
	if delay != w.engine.kStalePoll {
		t.Fatalf("delay = %v, want the re-check poll", delay)
	}

	// A report gap while online never gates authorization: an old
	// observation with a live printer still uploads.
	w.pauser.setOffline("S1", false)
	w.clock.Advance(60 * time.Second)
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	if _, processes := w.client.counts(); processes != 1 {
		t.Fatalf("processes = %d, want the upload on a live printer despite the gap", processes)
	}
}

func TestEngineNoFreshFrameDegrades(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.frames.serve = false

	delay := w.worker.step(context.Background())
	st := w.status()
	if st.State != StateDegraded || st.Reason != ReasonCameraLost || !st.CameraLost {
		t.Fatalf("state/reason/camera_lost = %q/%q/%v", st.State, st.Reason, st.CameraLost)
	}
	if delay != w.engine.kCameraRetry {
		t.Fatalf("delay = %v, want the camera retry pacing", delay)
	}
	// The camera hold stays while monitoring continues.
	if w.frames.released != 0 {
		t.Fatalf("releases = %d, want the hold kept during monitoring", w.frames.released)
	}
}

func TestEngineTerminalErrorSuspendsAllPrinters(t *testing.T) {
	clock := newEtClock()
	client := &etClient{}
	frames := &etFrames{clock: clock, acquireOK: true, serve: true}
	sessions := &etSessions{}
	pauser := &etPauser{}
	printers := []config.Printer{{Serial: "S1", Name: "A", Model: "P1S"}, {Serial: "S2", Name: "B", Model: "P1S"}}
	e := New(printers, client, frames, sessions, pauser,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.now = clock.Now
	// Real goroutines with tiny pacing so the test stays fast.
	e.kIdlePoll, e.kStalePoll, e.kCameraRetry = 2*time.Millisecond, 2*time.Millisecond, 2*time.Millisecond
	sessions.running(clock, 1, 1, "RUNNING")
	client.queueErr(&APIError{Status: 401, Type: errTypeInvalidKey})
	e.Start()
	defer e.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if suspended, msg := e.AccountSuspended(); suspended && msg != "" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if suspended, msg := e.AccountSuspended(); !suspended || msg == "" {
		t.Fatalf("suspended=%v msg=%q, want account-wide suspension", suspended, msg)
	}
	// Every worker reflects the suspension and no client activity follows.
	s1 := e.DetectionStatus("S1").(*Status)
	s2 := e.DetectionStatus("S2").(*Status)
	if !s1.Suspended || !s2.Suspended {
		t.Fatalf("suspended flags = %v/%v, want both workers suspended", s1.Suspended, s2.Suspended)
	}
	creates, processes := client.counts()
	time.Sleep(50 * time.Millisecond)
	creates2, processes2 := client.counts()
	if creates2 != creates || processes2 != processes {
		t.Fatalf("client activity after suspension: %d/%d -> %d/%d", creates, processes, creates2, processes2)
	}
}

func TestEngineTemporaryErrorKeepsMonitoring(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queueErr(errors.New("connection reset"))
	delay := w.worker.step(context.Background())

	st := w.status()
	if st.State != StateDegraded || st.Reason != ReasonAPIRetrying {
		t.Fatalf("state/reason = %q/%q, want degraded/%s", st.State, st.Reason, ReasonAPIRetrying)
	}
	// The backoff starts at 20s with +-20% jitter.
	if delay < 16*time.Second || delay > 24*time.Second {
		t.Fatalf("delay = %v, want the temporary backoff to start at 20s", delay)
	}
	// The sticky result stays absent but the fallback URL activates only for
	// process failures; a create failure keeps the primary URL.
	w.clock.Advance(25 * time.Second)
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	if _, processes := w.client.counts(); processes != 2 {
		t.Fatalf("processes = %d, want the retry to proceed after backoff", processes)
	}
}

func TestEngineFailureDiagnosticsSanitizeUnknownError(t *testing.T) {
	w := newEtWorld(t)
	activities := activity.New([]config.Printer{{Serial: "S1"}})
	w.engine.SetActivity(activities)
	var logs bytes.Buffer
	w.worker.log = slog.New(slog.NewTextHandler(&logs, nil)).With("serial", "S1")

	w.worker.handleFailure(errors.New("secret context and provider detail"), false)
	const reason = "Gadget request failed for an unknown reason"
	if got := w.status().Message; got != reason {
		t.Fatalf("status message = %q, want %q", got, reason)
	}
	if len(activities.Recent("S1")) != 0 {
		t.Fatalf("activity = %+v, want nothing before the interruption delay", activities.Recent("S1"))
	}
	w.clock.Advance(degradeLogDelay)
	if left := w.worker.flushDegradedLog(w.clock.Now()); left != 0 {
		t.Fatalf("flush left = %v, want the interruption due", left)
	}
	events := activities.Recent("S1")
	if len(events) == 0 || events[0].Message != "AI inspection interrupted, "+reason {
		t.Fatalf("activity = %+v, want the same safe failure reason", events)
	}
	log := logs.String()
	for _, field := range []string{"operation=create", "status=0", "type=unknown", "class=unknown", "retry_seconds=", "fallback=false", reason} {
		if !strings.Contains(log, field) {
			t.Errorf("log %q does not contain %q", log, field)
		}
	}
	if strings.Contains(log, "secret context") {
		t.Fatalf("log exposed the unknown error: %q", log)
	}
}

func TestEngineFrameSizeFailureReasonIsActionable(t *testing.T) {
	w := newEtWorld(t)
	activities := activity.New([]config.Printer{{Serial: "S1"}})
	w.engine.SetActivity(activities)
	w.worker.handleFailure(&frameTooLargeError{size: maxImageBytes + 17}, true)
	want := fmt.Sprintf("camera frame is too large (%d bytes; limit %d bytes)", maxImageBytes+17, maxImageBytes)
	if got := w.status().Message; got != want {
		t.Fatalf("status message = %q, want %q", got, want)
	}
	w.clock.Advance(degradeLogDelay)
	if left := w.worker.flushDegradedLog(w.clock.Now()); left != 0 {
		t.Fatalf("flush left = %v, want the interruption due", left)
	}
	events := activities.Recent("S1")
	if len(events) == 0 || events[0].Message != "AI inspection interrupted, "+want {
		t.Fatalf("activity = %+v, want the same frame-size reason", events)
	}
}

func TestEngineMonitoringRecoveryStartsIntensiveWindow(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	snap, _ := w.sessions.Session("S1")
	w.worker.mu.Lock()
	w.worker.intensiveUntil = time.Time{}
	w.worker.clearCount = intensiveClearCount - 1
	w.worker.clearSince = w.clock.now.Add(-intensiveClearSpan)
	w.worker.mu.Unlock()
	w.worker.degrade(ReasonAPIRetrying, "temporary failure", time.Second, false)

	if !w.worker.onResult(snap, Frame{Seq: 1, Captured: w.clock.now}, etClearResult(), nil, 0) {
		t.Fatal("recovery result was discarded")
	}
	w.worker.mu.Lock()
	intensive := w.worker.intensiveLocked(w.clock.now)
	clearCount := w.worker.clearCount
	clearSince := w.worker.clearSince
	w.worker.mu.Unlock()
	if !intensive {
		t.Fatal("successful recovery did not start an intensive window")
	}
	if clearCount != 0 || !clearSince.IsZero() {
		t.Fatalf("clear sequence = %d since %v, want reset on recovery", clearCount, clearSince)
	}
}

// ---- telemetry-backed lifecycle tests ----
//
// These merge real report payloads through a real telemetry.Cache, so the
// decision epoch and the print session generation follow the production
// rules (epoch bumps at every state boundary; session generation only at
// real new prints) instead of synthetic constants. The engine keeps the
// shared fake clock: the cache stamps report times with the real clock, so
// freshness checks pass trivially and these tests isolate session and
// generation logic.

type etbWorld struct {
	clock    *etClock
	client   *etClient
	frames   *etFrames
	pauser   *etPauser
	engine   *Engine
	worker   *worker
	cache    *telemetry.Cache
	activity *activity.Log
}

func newEtbWorld(t *testing.T) *etbWorld {
	t.Helper()
	printers := []config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}}
	cache := telemetry.NewCache(printers,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	activities := activity.New(printers)
	cache.SetActivity(activities)
	w := &etbWorld{
		clock:    newEtClock(),
		client:   &etClient{},
		frames:   &etFrames{acquireOK: true, serve: true},
		pauser:   &etPauser{},
		cache:    cache,
		activity: activities,
	}
	w.frames.clock = w.clock
	e := New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}},
		w.client, w.frames, w.cache, w.pauser,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.now = w.clock.Now
	e.SetActivity(activities)
	w.engine, w.worker = e, e.workers["S1"]
	return w
}

// status snapshots the published display state.
func (w *etbWorld) status() *Status {
	return w.engine.DetectionStatus("S1").(*Status)
}

// report merges one printer report payload.
func (w *etbWorld) report(t *testing.T, payload string) {
	t.Helper()
	w.cache.Observe("S1", []byte(payload))
}

func etbRunning(project, task int, file string, layer int) string {
	return `{"print":{"gcode_state":"RUNNING","project_id":` + strconv.Itoa(project) +
		`,"task_id":` + strconv.Itoa(task) + `,"subtask_name":"` + file + `","layer_num":` + strconv.Itoa(layer) + `}}`
}

func etbFinish(project, task int, file string) string {
	return `{"print":{"gcode_state":"FINISH","project_id":` + strconv.Itoa(project) +
		`,"task_id":` + strconv.Itoa(task) + `,"subtask_name":"` + file + `"}}`
}

func TestEngineTelemetryResumeKeepsContextAndLatch(t *testing.T) {
	w := newEtbWorld(t)
	w.report(t, etbRunning(7, 9, "benchy.gcode.3mf", 4))
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 1 {
		t.Fatalf("pause calls = %d, want the first suggestion to pause", calls)
	}
	if !hasActivityKind(w.activity.Recent("S1"), "ai_pause_sent") || !hasActivityKind(w.activity.Recent("S1"), "ai_monitoring") {
		t.Fatalf("activity did not record monitoring and pause request: %+v", w.activity.Recent("S1"))
	}

	// The user pauses, then resumes: telemetry bumps the decision epoch at
	// both boundaries without starting a new print session.
	w.report(t, `{"print":{"gcode_state":"PAUSE"}}`)
	w.worker.step(context.Background())
	if got := w.status().PauseState; got != PauseConfirmed {
		t.Fatalf("pause_state = %q, want confirmed by the PAUSE report", got)
	}
	if !hasActivityKind(w.activity.Recent("S1"), "ai_pause_confirmed") {
		t.Fatalf("activity did not record confirmed pause: %+v", w.activity.Recent("S1"))
	}
	w.report(t, `{"print":{"gcode_state":"RUNNING"}}`)
	w.worker.step(context.Background())
	if got := w.status().PauseState; got != PauseNone {
		t.Fatalf("pause_state = %q, want the resume to close the lifecycle", got)
	}

	// The resume must not reset the session: the Gadget context survives
	// and the single-shot latch still swallows the next suggestion.
	w.clock.Advance(21 * time.Second)
	w.report(t, etbRunning(7, 9, "benchy.gcode.3mf", 5))
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if creates, _ := w.client.counts(); creates != 1 {
		t.Fatalf("creates = %d, want the context preserved across pause/resume", creates)
	}
	if calls := w.pauser.calls(); calls != 1 {
		t.Fatalf("pause calls = %d, want the latch preserved across resume", calls)
	}

	// A fresh clear result rearms the latch; the next suggestion pauses.
	w.clock.Advance(21 * time.Second)
	w.report(t, etbRunning(7, 9, "benchy.gcode.3mf", 6))
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	w.clock.Advance(21 * time.Second)
	w.report(t, etbRunning(7, 9, "benchy.gcode.3mf", 7))
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 2 {
		t.Fatalf("pause calls = %d, want the rearm to allow a new command", calls)
	}
}

// hasActivityKind reports whether an activity snapshot contains kind.
func hasActivityKind(events []activity.Entry, kind string) bool {
	for _, event := range events {
		if event.Kind == kind {
			return true
		}
	}
	return false
}

func TestEngineTelemetrySameFileNewPrintResetsSession(t *testing.T) {
	w := newEtbWorld(t)
	w.report(t, etbRunning(0, 11, "benchy.gcode.3mf", 4))
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	if creates, _ := w.client.counts(); creates != 1 {
		t.Fatalf("creates = %d, want the first print inspected", creates)
	}

	// The print ends; the camera wall keeps the file name.
	w.report(t, etbFinish(0, 11, "benchy.gcode.3mf"))
	w.worker.step(context.Background())

	// The same file prints again: identical subtask_name, new task. The
	// FINISH-to-RUNNING boundary must start a new session with a fresh
	// context, not continue the old one on the shared name.
	w.report(t, etbRunning(0, 12, "benchy.gcode.3mf", 1))
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	creates, processes := w.client.counts()
	if creates != 2 || processes != 2 {
		t.Fatalf("creates/processes = %d/%d, want a fresh context for the same-file new print", creates, processes)
	}
}

func TestEngineTelemetryCoalescedNewPrintResets(t *testing.T) {
	w := newEtbWorld(t)
	w.report(t, etbRunning(1, 11, "a.gcode", 4))
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if got := w.status().PauseState; got != PausePending {
		t.Fatalf("pause_state = %q, want pending before the coalescence", got)
	}

	// FINISH and the next print's RUNNING land before the worker wakes, so
	// it never observes an idle gap; the session generation carries the
	// boundary.
	w.report(t, etbFinish(1, 11, "a.gcode"))
	w.report(t, etbRunning(1, 12, "a.gcode", 1))
	w.client.queue(etClearResult())
	delay := w.worker.step(context.Background())
	creates, processes := w.client.counts()
	if creates != 2 || processes != 2 {
		t.Fatalf("creates/processes = %d/%d, want a fresh context for the coalesced new print", creates, processes)
	}
	st := w.status()
	if st.PauseState != PauseNone {
		t.Fatalf("pause_state = %q, want the old lifecycle dropped", st.PauseState)
	}
	if st.LastInspectedLayer == nil || *st.LastInspectedLayer != 1 {
		t.Fatalf("last_inspected_layer = %v, want the new print's first layer", st.LastInspectedLayer)
	}
	if delay < 0 {
		t.Fatalf("delay = %v, want a live schedule after the reset", delay)
	}
}

// TestEngineTelemetryStaleLayerDoesNotAuthorizeNewPrint merges real report
// payloads: the previous job's last layer must not survive into the new
// print's decisions, and the new print's first layer report must start the
// inspection immediately.
func TestEngineTelemetryStaleLayerDoesNotAuthorizeNewPrint(t *testing.T) {
	w := newEtbWorld(t)
	w.report(t, etbRunning(1, 11, "a.gcode", 4))
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	if creates, processes := w.client.counts(); creates != 1 || processes != 1 {
		t.Fatalf("creates/processes = %d/%d, want the first print inspected", creates, processes)
	}

	// The next print starts and reports before the worker wakes, but its
	// first report omits layer_num: the previous job's layer 4 must not
	// authorize an inspection of the new print.
	w.report(t, etbFinish(1, 11, "a.gcode"))
	w.report(t, `{"print":{"gcode_state":"RUNNING","project_id":1,"task_id":12,"subtask_name":"a.gcode"}}`)
	w.worker.step(context.Background())
	if _, processes := w.client.counts(); processes != 1 {
		t.Fatalf("processes = %d, want the stale previous-job layer to wait", processes)
	}

	// The new print's first layer report starts the inspection immediately
	// on a fresh context.
	w.report(t, `{"print":{"gcode_state":"RUNNING","layer_num":1}}`)
	w.client.queue(etClearResult())
	delay := w.worker.step(context.Background())
	creates, processes := w.client.counts()
	if creates != 2 || processes != 2 {
		t.Fatalf("creates/processes = %d/%d, want a fresh context and inspection for the new print", creates, processes)
	}
	if st := w.status(); st.LastInspectedLayer == nil || *st.LastInspectedLayer != 1 {
		t.Fatalf("last_inspected_layer = %v, want the new print's first layer", st.LastInspectedLayer)
	}
	if delay != 5*time.Second {
		t.Fatalf("delay after the new print's first inspection = %v, want the intensive Minimum 5s", delay)
	}
}

func TestEngineTelemetryDelayedResultDiscardedOnNewPrint(t *testing.T) {
	w := newEtbWorld(t)
	w.report(t, etbRunning(1, 11, "a.gcode", 4))
	w.client.queue(etPauseResult())
	w.client.beforeReply = func() {
		// The print ends and a new one starts while the upload is in flight.
		w.report(t, etbFinish(1, 11, "a.gcode"))
		w.report(t, etbRunning(1, 12, "a.gcode", 1))
	}
	delay := w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 0 {
		t.Fatalf("pause calls = %d, want the stale suggestion discarded", calls)
	}
	if delay != w.engine.kStalePoll {
		t.Fatalf("delay = %v, want the stale-result re-evaluation delay", delay)
	}
	st := w.status()
	if st.Quality != 0 || st.Warning || st.LastInspectedLayer != nil {
		t.Fatalf("stale result published: %+v", st)
	}
	if st.PauseState != PauseNone {
		t.Fatalf("pause_state = %q, want none after the discard", st.PauseState)
	}
}

func TestEngineTelemetryDelayedResultDiscardedOnUserPause(t *testing.T) {
	w := newEtbWorld(t)
	w.report(t, etbRunning(1, 11, "a.gcode", 4))
	w.client.queue(etPauseResult())
	w.client.beforeReply = func() {
		// The user paused while the upload was in flight: same print
		// session, but the decision epoch moved.
		w.report(t, `{"print":{"gcode_state":"PAUSE"}}`)
	}
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 0 {
		t.Fatalf("pause calls = %d, want no command once the printer paused", calls)
	}
	// A pause is not a new print: the context is kept for the resumed print.
	if creates, _ := w.client.counts(); creates != 1 {
		t.Fatalf("creates = %d, want the context kept across the discarded result", creates)
	}
}

func TestEngineTelemetryTempDeltaAuthorizes(t *testing.T) {
	w := newEtbWorld(t)
	w.report(t, etbRunning(1, 11, "a.gcode", 4))

	// A temperature delta plus the layer arrives while the merged RUNNING
	// still comes from an earlier report. The online printer authorizes
	// the upload; no state restatement is needed.
	w.report(t, `{"print":{"nozzle_temper":220,"bed_temper":55,"layer_num":4}}`)
	w.client.queue(etPauseResult())
	w.worker.step(context.Background())
	if creates, processes := w.client.counts(); creates != 1 || processes != 1 {
		t.Fatalf("creates/processes = %d/%d, want the upload on the online printer", creates, processes)
	}
	if calls := w.pauser.calls(); calls != 1 {
		t.Fatalf("pause calls = %d, want one command on the online printer", calls)
	}
}

func TestEngineTelemetryDisconnectDuringUploadDoesNotPause(t *testing.T) {
	w := newEtbWorld(t)
	w.report(t, etbRunning(1, 11, "a.gcode", 4))
	w.client.queue(etPauseResult())
	w.client.beforeReply = func() {
		// The printer goes offline during the upload while a
		// temperature-only delta refreshes the merged freshness.
		w.pauser.setOffline("S1", true)
		w.report(t, `{"print":{"nozzle_temper":220}}`)
	}
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 0 {
		t.Fatalf("pause calls = %d, want no command while the printer is offline", calls)
	}
}

// lightSession starts an AI-monitored print whose report carries the given
// chamber light mode.
func (w *etWorld) lightSession(sessionGen uint64, light string) {
	w.sessions.running(w.clock, sessionGen, sessionGen, "RUNNING")
	v, _ := w.sessions.Session("S1")
	v.ChamberLight = light
	w.sessions.set(v, true)
}

func TestChamberLightAutoOnOncePerSession(t *testing.T) {
	w := newEtWorld(t)
	printers := []config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}}
	activities := activity.New(printers)
	w.engine.SetActivity(activities)
	w.lightSession(1, "off")
	for i := 0; i < 3; i++ {
		w.client.queue(etClearResult())
		w.worker.step(context.Background())
		w.clock.Advance(30 * time.Second)
	}
	if got := w.pauser.lightCalls(); len(got) != 1 || !got[0] {
		t.Fatalf("light calls = %v, want exactly one on", got)
	}
	if !hasActivityKind(activities.Recent("S1"), "ai_light_on") {
		t.Fatalf("activity missing ai_light_on: %+v", activities.Recent("S1"))
	}
}

func TestChamberLightReenableAllowsOneNewAttempt(t *testing.T) {
	w := newEtWorld(t)
	w.lightSession(1, "off")
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	if got := w.pauser.lightCalls(); len(got) != 1 || !got[0] {
		t.Fatalf("initial light calls = %v, want one on", got)
	}
	if _, err := w.engine.SetDetectionEnabled("S1", false, w.engine.sessionToken(1)); err != nil {
		t.Fatal(err)
	}
	w.worker.step(context.Background())
	if _, err := w.engine.SetDetectionEnabled("S1", true, ""); err != nil {
		t.Fatal(err)
	}
	w.client.queue(etClearResult())
	w.worker.step(context.Background())
	if got := w.pauser.lightCalls(); len(got) != 2 || !got[0] || !got[1] {
		t.Fatalf("light calls after re-enable = %v, want two one-shot attempts", got)
	}
}

func TestChamberLightSkippedWhenOverriddenOrAlreadyOn(t *testing.T) {
	w := newEtWorld(t)
	w.lightSession(1, "off")
	if _, err := w.engine.SetDetectionEnabled("S1", false, w.engine.sessionToken(1)); err != nil {
		t.Fatal(err)
	}
	w.worker.step(context.Background())
	w.worker.step(context.Background())
	if got := w.pauser.lightCalls(); len(got) != 0 {
		t.Fatalf("light calls under override = %v", got)
	}

	w2 := newEtWorld(t)
	w2.lightSession(1, "on")
	w2.client.queue(etClearResult())
	w2.worker.step(context.Background())
	if got := w2.pauser.lightCalls(); len(got) != 0 {
		t.Fatalf("light calls with light already on = %v", got)
	}
}

func TestChamberLightFailureIsNotRetried(t *testing.T) {
	w := newEtWorld(t)
	w.pauser.lightErr = errors.New("offline")
	w.lightSession(1, "off")
	for i := 0; i < 3; i++ {
		w.client.queue(etClearResult())
		w.worker.step(context.Background())
		w.clock.Advance(31 * time.Second)
	}
	if got := w.pauser.lightCalls(); len(got) != 1 {
		t.Fatalf("light attempts = %d, want 1", len(got))
	}
}

// runTick drives the real run scheduling path: flush the pending
// interruption, run one step, and cap the sleep at the flush deadline,
// exactly as worker.run does between wakes.
func runTick(w *etWorld, ctx context.Context) time.Duration {
	left := w.worker.flushDegradedLog(w.clock.Now())
	delay := w.worker.step(ctx)
	if left > 0 && left < delay {
		delay = left
	}
	return delay
}

func degradedMessages(events []activity.Entry) []string {
	var out []string
	for i := len(events) - 1; i >= 0; i-- {
		e := events[i]
		if e.Kind == "ai_degraded" || e.Kind == "ai_recovered" {
			out = append(out, e.Kind+": "+e.Message)
		}
	}
	return out
}

func TestEngineDegradedRecoversSilentlyBeforeDelay(t *testing.T) {
	w := newEtWorld(t)
	activities := activity.New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}})
	w.engine.SetActivity(activities)
	w.startSession(1, 1)
	w.client.queueErr(errors.New("connection reset"))
	ctx := context.Background()

	delay := runTick(w, ctx)
	if w.worker.degraded == "" {
		t.Fatal("degraded is empty after the failure")
	}
	if left := delay; left > degradeLogDelay || left <= 0 {
		t.Fatalf("delay = %v, want the sleep capped at the interruption deadline", delay)
	}
	w.clock.Advance(4 * time.Second)
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	w.worker.mu.Lock()
	w.worker.nextDue = w.clock.Now()
	w.worker.mu.Unlock()
	w.client.queue(etClearResult())
	if delay := runTick(w, ctx); delay <= 0 {
		t.Fatalf("delay = %v, want a live schedule after recovery", delay)
	}
	if got := degradedMessages(activities.Recent("S1")); len(got) != 0 {
		t.Fatalf("degraded activity = %v, want nothing for a 4s gap", got)
	}
	if st := w.status(); st.State != StateMonitoring {
		t.Fatalf("state = %q, want monitoring after recovery", st.State)
	}
}

func TestEngineDegradedLogsOnceAfterDelayThenRecovers(t *testing.T) {
	w := newEtWorld(t)
	activities := activity.New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}})
	w.engine.SetActivity(activities)
	w.startSession(1, 1)
	w.client.queueErr(errors.New("connection reset"))
	ctx := context.Background()

	runTick(w, ctx)
	// Only one degrade call: the loop wakes at the capped deadline with no
	// second failure, still degraded, and the flush records it.
	w.clock.Advance(degradeLogDelay)
	if left := runTick(w, ctx); left <= 0 {
		t.Fatalf("delay = %v, want a live schedule at the deadline", left)
	}
	got := degradedMessages(activities.Recent("S1"))
	if len(got) != 1 {
		t.Fatalf("degraded activity = %v, want one interruption", got)
	}
	const want = "ai_degraded: AI inspection interrupted, Gadget request failed for an unknown reason"
	if got[0] != want {
		t.Fatalf("degraded activity = %q, want %q", got[0], want)
	}
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	w.worker.mu.Lock()
	w.worker.nextDue = w.clock.Now()
	w.worker.mu.Unlock()
	w.client.queue(etClearResult())
	if delay := runTick(w, ctx); delay <= 0 {
		t.Fatalf("delay = %v, want a live schedule after recovery", delay)
	}
	got = degradedMessages(activities.Recent("S1"))
	if len(got) != 2 || got[1] != "ai_recovered: AI inspection resumed, monitoring print" {
		t.Fatalf("degraded activity = %v, want interruption then recovery", got)
	}
}

func TestEngineDegradedReasonChangeKeepsSingleInterruption(t *testing.T) {
	w := newEtWorld(t)
	activities := activity.New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}})
	w.engine.SetActivity(activities)
	w.startSession(1, 1)
	w.client.queueErr(errors.New("connection reset"))
	ctx := context.Background()

	runTick(w, ctx)
	// A second failure with a different message joins the same episode: it
	// updates the message without re-arming the deadline.
	w.clock.Advance(10 * time.Second)
	if delay := w.worker.degrade(ReasonCameraLost, "no fresh camera frame for inspection", w.engine.kCameraRetry, true); delay <= 0 {
		t.Fatalf("degrade delay = %v, want the camera retry pacing", delay)
	}
	if left := w.worker.flushDegradedLog(w.clock.Now()); left != 20*time.Second {
		t.Fatalf("flush left = %v, want the original 30s deadline kept", left)
	}
	w.clock.Advance(20 * time.Second)
	if left := w.worker.flushDegradedLog(w.clock.Now()); left != 0 {
		t.Fatalf("flush left = %v, want the interruption due", left)
	}
	got := degradedMessages(activities.Recent("S1"))
	want := []string{"ai_degraded: AI inspection interrupted, no fresh camera frame for inspection"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("degraded activity = %v, want %v", got, want)
	}
	// A repeat flush after the same deadline records nothing further.
	if left := w.worker.flushDegradedLog(w.clock.Now()); left != 0 {
		t.Fatalf("flush left = %v, want nothing pending after the log", left)
	}
	if got := degradedMessages(activities.Recent("S1")); len(got) != 1 {
		t.Fatalf("degraded activity = %v, want a single interruption", got)
	}
	w.sessions.running(w.clock, 1, 1, "RUNNING")
	w.worker.mu.Lock()
	w.worker.nextDue = w.clock.Now()
	w.worker.mu.Unlock()
	w.client.queue(etClearResult())
	runTick(w, ctx)
	if got := degradedMessages(activities.Recent("S1")); len(got) != 2 {
		t.Fatalf("degraded activity = %v, want one interruption plus recovery", got)
	}
}

func TestEngineShutdownWhileDegradedRecordsNothingFurther(t *testing.T) {
	w := newEtWorld(t)
	activities := activity.New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}})
	w.startSession(1, 1)
	w.client.queueErr(errors.New("connection reset"))
	ctx, cancel := context.WithCancel(context.Background())

	runTick(w, ctx)
	before := len(activities.Recent("S1"))
	cancel()
	// Shutdown ends the loop without another flush: the 30s interruption
	// never fires and no recovery is recorded.
	w.clock.Advance(degradeLogDelay + time.Second)
	if got := len(activities.Recent("S1")); got != before {
		t.Fatalf("activity events = %d, want the pre-shutdown %d", got, before)
	}
	if got := degradedMessages(activities.Recent("S1")); len(got) != 0 {
		t.Fatalf("degraded activity = %v, want none after shutdown", got)
	}
}
