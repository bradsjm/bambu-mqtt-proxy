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
// (bumped only when a genuinely new print starts); StateGen 1 restates the
// state on the current connection.
func (s *etSessions) running(clock *etClock, epoch, sessionGen uint64, state string) {
	s.set(telemetry.SessionView{
		Serial: "S1", Active: true, State: state,
		SessionGen: sessionGen, Epoch: epoch,
		ObsAt: clock.Now(), Obs: 1, ObsGen: 1, StateGen: 1, LayerNum: intPtr(4),
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
	mu   sync.Mutex
	gens []uint64
	err  error
}

func (p *etPauser) PausePrint(_ string, gen uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gens = append(p.gens, gen)
	return p.err
}

func (p *etPauser) SetSpeedProfile(string, uint64, int) error { return p.err }

func (p *etPauser) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.gens)
}

type etGens struct {
	mu  sync.Mutex
	gen uint64
}

func (g *etGens) Generation(string) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.gen
}

// ---- harness ----

type etWorld struct {
	clock    *etClock
	client   *etClient
	frames   *etFrames
	sessions *etSessions
	pauser   *etPauser
	gens     *etGens
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
		gens:     &etGens{gen: 1},
	}
	// The frame fake shares the world clock.
	w.frames.clock = w.clock
	e := New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}},
		w.client, w.frames, w.sessions, w.pauser, w.gens,
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

// startSession starts a fresh RUNNING print observation: fresh telemetry on
// generation 1 with the state restated on it.
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
	w.pauser.err = errors.New("generation changed")
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

func TestEngineDelayedResultDiscardedOnReconnect(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.client.queue(etPauseResult())
	w.client.beforeReply = func() {
		w.gens.mu.Lock()
		w.gens.gen = 2
		w.gens.mu.Unlock()
		view, _ := w.sessions.Session("S1")
		view.ObsGen = 2
		view.StateGen = 2
		view.ObsAt = w.clock.Now()
		w.sessions.set(view, true)
	}
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 0 {
		t.Fatalf("pause calls = %d, want no command on a newer connection", calls)
	}
}

func TestEngineStaleTelemetryNeverAuthorizes(t *testing.T) {
	w := newEtWorld(t)
	w.startSession(1, 1)
	w.clock.Advance(20 * time.Second) // beyond reportFreshMax, no new report

	delay := w.worker.step(context.Background())
	creates, processes := w.client.counts()
	if creates != 0 || processes != 0 {
		t.Fatalf("creates/processes = %d/%d, want no API activity on stale telemetry", creates, processes)
	}
	if got := w.status().Reason; got != ReasonAwaitingTelemetry {
		t.Fatalf("reason = %q, want %s", got, ReasonAwaitingTelemetry)
	}
	if delay != w.engine.kStalePoll {
		t.Fatalf("delay = %v, want the re-check poll", delay)
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
	gens := &etGens{gen: 1}
	printers := []config.Printer{{Serial: "S1", Name: "A", Model: "P1S"}, {Serial: "S2", Name: "B", Model: "P1S"}}
	e := New(printers, client, frames, sessions, pauser, gens,
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
	events := activities.Recent("S1")
	if len(events) == 0 || events[0].Message != reason {
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
	events := activities.Recent("S1")
	if len(events) == 0 || events[0].Message != want {
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

	if !w.worker.onResult(snap, 1, Frame{Seq: 1, Captured: w.clock.now}, etClearResult(), nil, 0) {
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
	gens     *etGens
	engine   *Engine
	worker   *worker
	cache    *telemetry.Cache
	activity *activity.Log
	seq      uint64
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
		gens:     &etGens{gen: 1},
		cache:    cache,
		activity: activities,
	}
	w.frames.clock = w.clock
	e := New([]config.Printer{{Serial: "S1", Name: "Shop", Model: "P1S"}},
		w.client, w.frames, w.cache, w.pauser, w.gens,
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

// report merges one upstream report payload on the current generation.
func (w *etbWorld) report(t *testing.T, payload string) {
	t.Helper()
	w.seq++
	w.cache.ObserveReport("S1", w.seq, w.gens.gen, []byte(payload))
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

func TestEngineTelemetryTempDeltaAfterReconnectDoesNotAuthorize(t *testing.T) {
	w := newEtbWorld(t)
	w.report(t, etbRunning(1, 11, "a.gcode", 4))

	// Mid-print reconnect: a temperature-only delta arrives on the new
	// connection while the merged RUNNING still comes from the old one.
	w.gens.gen = 2
	w.report(t, `{"print":{"nozzle_temper":220,"bed_temper":55}}`)
	w.client.queue(etPauseResult())
	delay := w.worker.step(context.Background())
	if creates, processes := w.client.counts(); creates != 0 || processes != 0 {
		t.Fatalf("creates/processes = %d/%d, want no API activity on a pre-reconnect RUNNING", creates, processes)
	}
	if got := w.status().Reason; got != ReasonAwaitingTelemetry {
		t.Fatalf("reason = %q, want %s", got, ReasonAwaitingTelemetry)
	}
	if delay != w.engine.kStalePoll {
		t.Fatalf("delay = %v, want the re-check poll", delay)
	}

	// An explicit state report on the new connection reauthorizes; the
	// guarded pause then fires on the current generation.
	w.client.queue(etPauseResult())
	w.report(t, etbRunning(1, 11, "a.gcode", 5))
	w.worker.step(context.Background())
	if _, processes := w.client.counts(); processes != 1 {
		t.Fatalf("processes = %d, want the upload once the state is restated", processes)
	}
	if calls := w.pauser.calls(); calls != 1 {
		t.Fatalf("pause calls = %d, want one command after the reconfirm", calls)
	}
	if gen := w.pauser.gens[0]; gen != 2 {
		t.Fatalf("pause generation = %d, want the current connection 2", gen)
	}
}

func TestEngineTelemetryReconnectDuringUploadDoesNotPause(t *testing.T) {
	w := newEtbWorld(t)
	w.report(t, etbRunning(1, 11, "a.gcode", 4))
	w.client.queue(etPauseResult())
	w.client.beforeReply = func() {
		// The connection drops during the upload and a temperature-only
		// delta on the new connection refreshes the merged freshness.
		w.gens.gen = 2
		w.report(t, `{"print":{"nozzle_temper":220}}`)
	}
	w.worker.step(context.Background())
	if calls := w.pauser.calls(); calls != 0 {
		t.Fatalf("pause calls = %d, want no command on a newer connection", calls)
	}
}
