package platecheck

import (
	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/printer"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func timeInPast() time.Time { return time.Unix(1, 0) }

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *testClock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

type fakeCommands struct {
	mu                sync.Mutex
	offline           map[string]bool
	stops, lights     int
	stopErr, lightErr error
	stopHook          func()
	lightHook         func()
	pauses            int
	pauseErr          error
	pauseHook         func()
}

func (f *fakeCommands) Online(serial string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.offline[serial]
}
func (f *fakeCommands) Stop(string) error {
	f.mu.Lock()
	f.stops++
	f.mu.Unlock()
	if f.stopHook != nil {
		f.stopHook()
	}
	return f.stopErr
}
func (f *fakeCommands) Pause(string) error {
	f.mu.Lock()
	f.pauses++
	f.mu.Unlock()
	if f.pauseHook != nil {
		f.pauseHook()
	}
	return f.pauseErr
}
func (f *fakeCommands) Light(string, bool) error {
	f.mu.Lock()
	f.lights++
	f.mu.Unlock()
	if f.lightHook != nil {
		f.lightHook()
	}
	return f.lightErr
}
func (f *fakeCommands) count() int { f.mu.Lock(); defer f.mu.Unlock(); return f.stops }

type fakeDecision struct {
	mu          sync.Mutex
	result      Result
	err         error
	calls       int
	hook        func()
	layerResult LayerResult
	layerCalls  int
	layerHook   func()
	layerImages [][]byte
}

func (f *fakeDecision) Probe(context.Context) error { return f.err }
func (f *fakeDecision) Evaluate(context.Context, []byte, string) (Result, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.hook != nil {
		f.hook()
	}
	result := f.result
	result.POccupied = 1 - result.PClear
	return result, f.err
}
func (f *fakeDecision) EvaluateFirstLayer(_ context.Context, frames [][]byte, _ string) (LayerResult, error) {
	f.mu.Lock()
	f.layerCalls++
	f.layerImages = frames
	f.mu.Unlock()
	if f.layerHook != nil {
		f.layerHook()
	}
	return f.layerResult, f.err
}

type frameFunc func(context.Context, string, time.Time) (Frame, error)

func (f frameFunc) Capture(ctx context.Context, s string, t time.Time) (Frame, error) {
	return f(ctx, s, t)
}

type fakeState struct {
	doc       printer.Doc
	subscribe func(printer.Event)
	offline   bool
}

func (f *fakeState) Get(string) (printer.Doc, bool, bool) { return f.doc, !f.offline, true }
func (f *fakeState) Subscribe(fn func(printer.Event))     { f.subscribe = fn }

type fixture struct {
	s        *Service
	w        *worker
	state    *fakeState
	commands *fakeCommands
	client   *fakeDecision
	clock    *testClock
	activity *activity.Log
}

func reportDoc(state string, layer, stage int) printer.Doc {
	return printer.Doc{"print": map[string]any{"gcode_state": state, "layer_num": float64(layer), "stg_cur": float64(stage)}}
}
func newFixture(t *testing.T, state string) *fixture {
	t.Helper()
	clock := &testClock{t: time.Unix(1, 0)}
	st := &fakeState{doc: reportDoc(state, 0, 0)}
	commands := &fakeCommands{}
	client := &fakeDecision{result: Result{PClear: .1, PAssessable: .9}}
	data := testJPEG(t)
	var seq uint64
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		clock.advance(after.Sub(clock.now()) + time.Millisecond)
		seq++
		return Frame{JPEG: data, Seq: seq, Captured: clock.now(), Width: 8, Height: 6}, nil
	})
	printers := []config.Printer{{Serial: "01S1", Model: "P1S"}}
	s := New(printers, testSettings(), client, frames, st, commands, nil)
	s.now = clock.now
	events := activity.New(printers)
	s.SetActivity(events)
	return &fixture{s: s, w: s.workers["01S1"], state: st, commands: commands, client: client, clock: clock, activity: events}
}
func (f *fixture) event(state string, layer, stage int, kinds ...string) {
	prev := f.state.doc
	f.state.doc = reportDoc(state, layer, stage)
	f.state.subscribe(printer.Event{Serial: "01S1", Prev: prev, Cur: f.state.doc, Online: true, Kinds: kinds, NewJob: printer.NewJob(prev, f.state.doc)})
	f.w.handle(context.Background(), <-f.w.events)
}
func (f *fixture) status() string { return f.s.moduleState("01S1").(ModuleState).State }
func (f *fixture) hasEvent(kind string) bool {
	for _, e := range f.activity.Recent("01S1") {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

func TestOccupiedPlateStopsOnceAndReportsResolve(t *testing.T) {
	for _, outcome := range []struct {
		state string
		layer int
		kind  string
	}{{"PAUSE", 0, "platecheck_stopped"}, {"FAILED", 0, "platecheck_stopped"}, {"FINISH", 0, "platecheck_stopped"}, {"RUNNING", 1, "platecheck_stop_unconfirmed"}} {
		t.Run(outcome.state, func(t *testing.T) {
			f := newFixture(t, "IDLE")
			f.event("RUNNING", 0, 0, "print_started")
			f.event("RUNNING", 0, 0)
			if f.commands.count() != 1 || f.client.calls != 1 {
				t.Fatal("expected one stop and one evaluation")
			}
			f.clock.advance(time.Hour)
			f.event(outcome.state, outcome.layer, 0)
			if !f.hasEvent(outcome.kind) {
				t.Fatalf("missing %s", outcome.kind)
			}
		})
	}
}
func TestPrepareStopRetriesOnlyAtRunning(t *testing.T) {
	f := newFixture(t, "IDLE")
	f.event("PREPARE", 0, 0, "print_preparing")
	f.event("PREPARE", 0, 0)
	f.event("RUNNING", 0, 0, "print_started")
	f.event("RUNNING", 0, 0)
	if f.commands.count() != 2 || f.client.calls != 1 {
		t.Fatalf("stops=%d checks=%d", f.commands.count(), f.client.calls)
	}
}
func TestHomingPassRunsOnceOrSkips(t *testing.T) {
	for _, layer := range []int{0, 1} {
		t.Run(fmt.Sprint(layer), func(t *testing.T) {
			f := newFixture(t, "IDLE")
			f.client.result.PClear = .9
			f.event("PREPARE", 0, 13, "print_preparing")
			f.event("PREPARE", 0, 14)
			f.event("RUNNING", layer, 0, "print_started")
			f.event("RUNNING", layer, 0)
			want := 2
			if layer > 0 {
				want = 1
				v := f.s.moduleState("01S1").(ModuleState)
				if v.Reason != "print_past_start" || !v.Homing {
					t.Fatalf("status=%+v", v)
				}
			}
			if f.client.calls != want {
				t.Fatalf("calls=%d want=%d", f.client.calls, want)
			}
			f.event("FINISH", layer, 0)
			f.event("PREPARE", 0, 0, "print_preparing")
			f.event("RUNNING", 0, 0, "print_started")
			if f.client.calls != want+1 {
				t.Fatal("homing flag leaked across prints")
			}
		})
	}
}
func TestBootMidPrintDoesNotSend(t *testing.T) {
	for _, layer := range []int{0, 8} {
		f := newFixture(t, "RUNNING")
		f.event("RUNNING", layer, 13, "state_initial")
		f.event("RUNNING", layer, 14)
		if f.commands.count() != 0 || f.commands.lights != 0 || f.client.calls != 0 {
			t.Fatal("boot issued a command or evaluated startup")
		}
	}
}
func TestFirstLayerPauseOutcomesFromReports(t *testing.T) {
	for _, confirmed := range []bool{true, false} {
		t.Run(fmt.Sprint(confirmed), func(t *testing.T) {
			f := newFixture(t, "RUNNING")
			f.client.layerResult = LayerResult{PAssessable: .9, PTangled: .9}
			f.event("RUNNING", 2, 0, "first_layer_complete")
			if f.commands.pauses != 1 {
				t.Fatal("expected one pause")
			}
			f.clock.advance(time.Hour)
			state, layer, kind := "RUNNING", 3, "platecheck_layer1_pause_unconfirmed"
			if confirmed {
				state, layer, kind = "PAUSED", 2, "platecheck_layer1_pause_confirmed"
			}
			f.event(state, layer, 0)
			if !f.hasEvent(kind) {
				t.Fatalf("missing %s", kind)
			}
			f.event(state, layer, 0)
			if f.commands.pauses != 1 {
				t.Fatal("pause retried")
			}
		})
	}
}

// TestStoreWorkerBoundary checks report callbacks, vision work, commands, and outcomes together.
func TestStoreWorkerBoundary(t *testing.T) {
	printers := []config.Printer{{Serial: "01S1", Model: "P1S"}}
	store := printer.NewStore(printers)
	store.SetOnline("01S1", true)
	commands := &fakeCommands{}
	client := &fakeDecision{result: Result{PClear: .1, PAssessable: .9}}
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		return Frame{JPEG: testJPEG(t), Captured: after.Add(time.Millisecond)}, nil
	})
	s := New(printers, testSettings(), client, frames, store, commands, nil)
	log := activity.New(printers)
	s.SetActivity(log)
	confirmed := make(chan struct{})
	log.SetObserver(func(serial string, e activity.Entry) {
		if e.Kind == "platecheck_stopped" {
			close(confirmed)
		}
	})
	commands.stopHook = func() { store.Apply("01S1", []byte(`{"print":{"gcode_state":"FAILED"}}`)) }
	store.Apply("01S1", []byte(`{"print":{"gcode_state":"IDLE"}}`))
	store.Apply("01S1", []byte(`{"print":{"gcode_state":"RUNNING","layer_num":0}}`))
	if commands.count() != 0 {
		t.Fatal("command ran inside the report callback")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Run(ctx) }()
	<-confirmed
	cancel()
	<-done
	if commands.count() != 1 {
		t.Fatal("expected one command across the real store boundary")
	}
}

// lowClarityExample is the saved-view case: 25% clarity with defect scores from 7% to 26%.
func lowClarityExample() LayerResult {
	return LayerResult{PAssessable: .25, PTangled: .07, PDetached: .17, PNozzleBlob: .23, PIncomplete: .26}
}

// visibleTangleLowClarity is a limited view that still shows a clear tangle.
func visibleTangleLowClarity() LayerResult {
	return LayerResult{PAssessable: .25, PTangled: .9}
}

// deliver hands the worker one report and leaves the live printer document unchanged.
func (f *fixture) deliver(ctx context.Context, cur printer.Doc, kinds ...string) {
	f.state.subscribe(printer.Event{Serial: "01S1", Prev: f.state.doc, Cur: cur, Online: true, Kinds: kinds, NewJob: printer.NewJob(f.state.doc, cur)})
	f.w.handle(ctx, <-f.w.events)
}

// layer returns the published first-layer state.
func (f *fixture) layer() FirstLayerState { return f.s.moduleState("01S1").(ModuleState).FirstLayer }

// countEvents counts activity entries of one kind.
func (f *fixture) countEvents(kind string) int {
	n := 0
	for _, e := range f.activity.Recent("01S1") {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// lastMessage returns the newest activity message of one kind.
func (f *fixture) lastMessage(kind string) string {
	var id uint64
	var message string
	for _, e := range f.activity.Recent("01S1") {
		if e.Kind == kind && e.ID >= id {
			id, message = e.ID, e.Message
		}
	}
	return message
}

// containsAll reports whether every value appears in text.
func containsAll(text string, values []string) bool {
	for _, value := range values {
		if !strings.Contains(text, value) {
			return false
		}
	}
	return true
}

// firstLayerLow runs the first check on layer 2 with a low-clarity result and checks the pending retry.
func (f *fixture) firstLayerLow(t *testing.T, result LayerResult) {
	t.Helper()
	f.client.layerResult = result
	f.event("RUNNING", 2, 0, "first_layer_complete")
	if f.client.layerCalls != 1 || f.commands.pauses != 0 || f.w.layerRetryAt.IsZero() {
		t.Fatalf("first check: calls %d pauses %d retry %v", f.client.layerCalls, f.commands.pauses, f.w.layerRetryAt)
	}
}

// retryDue advances past the retry delay and delivers a running layer 2 report.
func (f *fixture) retryDue() {
	f.clock.advance(layerRetryDelay)
	f.state.doc = reportDoc("RUNNING", 2, 0)
	f.deliver(context.Background(), f.state.doc)
}

func TestFirstLayerLowClarityRetriesOnceWithoutPause(t *testing.T) {
	for _, tc := range []struct {
		name, state, kind string
		contains          []string
		result            LayerResult
	}{
		{"example", "inconclusive", "platecheck_layer1_skipped", []string{"25%", "70%"}, lowClarityExample()},
		{"visible defect", "warning", "platecheck_layer1_warning", []string{"90%", "25%", "70%"}, visibleTangleLowClarity()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "RUNNING")
			f.firstLayerLow(t, tc.result)
			if got := f.layer(); got.State != tc.state || !containsAll(f.lastMessage(tc.kind), tc.contains) || f.hasEvent("platecheck_layer1_passed") {
				t.Fatalf("first result %+v message %q", got, f.lastMessage(tc.kind))
			}
			f.clock.advance(layerRetryDelay - time.Second)
			f.event("RUNNING", 2, 0)
			if f.client.layerCalls != 1 {
				t.Fatal("retry ran before it was due")
			}
			f.clock.advance(time.Second)
			f.event("RUNNING", 2, 0)
			if f.client.layerCalls != 2 || f.layer().State != tc.state || f.commands.pauses != 0 || f.countEvents(tc.kind) != 2 {
				t.Fatalf("retry: calls %d state %+v pauses %d", f.client.layerCalls, f.layer(), f.commands.pauses)
			}
			f.clock.advance(time.Hour)
			f.event("RUNNING", 2, 0)
			if f.client.layerCalls != 2 {
				t.Fatal("a third check ran")
			}
		})
	}
}

func TestFirstLayerRetryWithClearImagePausesOnce(t *testing.T) {
	f := newFixture(t, "RUNNING")
	f.firstLayerLow(t, lowClarityExample())
	f.client.layerResult = LayerResult{PAssessable: .9, PTangled: .9}
	f.retryDue()
	if got := f.layer(); f.commands.pauses != 1 || got.State != "pause" || got.Pause != "pending" || got.Reason != "" || !f.hasEvent("platecheck_layer1_pause_sent") {
		t.Fatalf("retry pause: pauses %d state %+v", f.commands.pauses, got)
	}
	f.deliver(context.Background(), reportDoc("PAUSED", 2, 0))
	if !f.hasEvent("platecheck_layer1_pause_confirmed") {
		t.Fatal("retry pause not resolved by the existing lifecycle")
	}
}

func TestFirstLayerRetryFailureKeepsFirstResult(t *testing.T) {
	providerErr := errors.New("provider down")
	captureErr := errors.New("camera down")
	for _, tc := range []struct {
		name   string
		arm    func(*fixture)
		reason string
	}{
		{"capture", func(f *fixture) {
			f.s.frames = frameFunc(func(context.Context, string, time.Time) (Frame, error) { return Frame{}, captureErr })
		}, errorCategory(captureErr)},
		{"provider", func(f *fixture) { f.client.err = providerErr }, errorCategory(providerErr)},
		{"invalid response", func(f *fixture) { f.client.layerResult = LayerResult{PAssessable: .9, PIncomplete: math.NaN()} }, "bad_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "RUNNING")
			f.firstLayerLow(t, visibleTangleLowClarity())
			first := f.layer()
			tc.arm(f)
			f.retryDue()
			got := f.layer()
			if got.State != "warning" || got.PTangled == nil || *got.PTangled != *first.PTangled || got.Reason != tc.reason || f.commands.pauses != 0 || f.countEvents("platecheck_layer1_warning") != 1 {
				t.Fatalf("after failed retry %+v", got)
			}
			if f.countEvents("platecheck_layer1_skipped")+f.countEvents("platecheck_layer1_pause_failed") == 0 {
				t.Fatal("retry failure not recorded")
			}
		})
	}
}

func TestFirstLayerRetryClearedBeforeDue(t *testing.T) {
	for _, tc := range []struct {
		name string
		cur  printer.Doc
	}{
		{"paused", reportDoc("PAUSED", 2, 0)},
		{"finished", reportDoc("FINISH", 2, 0)},
		{"layer missing", printer.Doc{"print": map[string]any{"gcode_state": "RUNNING"}}},
		{"layer 3", reportDoc("RUNNING", 3, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "RUNNING")
			f.firstLayerLow(t, visibleTangleLowClarity())
			f.deliver(context.Background(), tc.cur)
			f.state.doc = tc.cur
			if f.layer().State != "warning" {
				t.Fatalf("first result lost: %+v", f.layer())
			}
			f.clock.advance(time.Hour)
			f.state.doc = reportDoc("RUNNING", 2, 0)
			f.deliver(context.Background(), f.state.doc)
			if f.client.layerCalls != 1 || f.layer().Reason != "" || f.countEvents("platecheck_layer1_skipped") != 0 {
				t.Fatalf("cleared retry restarted: calls %d state %+v", f.client.layerCalls, f.layer())
			}
		})
	}
}

func TestFirstLayerNewJobClearsFindingsAndRetry(t *testing.T) {
	f := newFixture(t, "RUNNING")
	f.firstLayerLow(t, visibleTangleLowClarity())
	f.clock.advance(time.Hour)
	// The live and previous documents show an ended print, so a RUNNING layer 2 report starts a new job.
	f.state.doc = reportDoc("IDLE", 0, 0)
	f.deliver(context.Background(), reportDoc("RUNNING", 2, 0))
	if got := f.layer(); got.State != "idle" || got.PTangled != nil || got.Reason != "" || f.client.layerCalls != 1 || f.countEvents("platecheck_layer1_skipped") != 0 {
		t.Fatalf("new job kept prior findings: calls %d state %+v", f.client.layerCalls, got)
	}
	// A retry that survived the new-job reset would run now that its delay has elapsed.
	f.state.doc = reportDoc("RUNNING", 2, 0)
	f.clock.advance(time.Hour)
	f.deliver(context.Background(), f.state.doc)
	if f.client.layerCalls != 1 {
		t.Fatalf("new job kept the previous retry: calls %d", f.client.layerCalls)
	}
}

func TestFirstLayerRetryRejectsLiveStateChange(t *testing.T) {
	for _, tc := range []struct {
		name   string
		live   printer.Doc
		off    bool
		reason string
	}{
		{"offline", reportDoc("RUNNING", 2, 0), true, "printer_offline"},
		{"paused", reportDoc("PAUSED", 2, 0), false, "print_paused"},
		{"layer 3", reportDoc("RUNNING", 3, 0), false, "print_past_start"},
		{"layer missing", printer.Doc{"print": map[string]any{"gcode_state": "RUNNING"}}, false, "layer_not_reported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "RUNNING")
			f.firstLayerLow(t, visibleTangleLowClarity())
			f.clock.advance(layerRetryDelay)
			queued := reportDoc("RUNNING", 2, 0)
			f.state.doc, f.state.offline = tc.live, tc.off
			f.deliver(context.Background(), queued)
			got := f.layer()
			if f.client.layerCalls != 1 || f.commands.pauses != 0 || f.commands.lights != 0 || got.State != "warning" || got.PTangled == nil || got.Reason != tc.reason {
				t.Fatalf("queued retry ran or lost findings: calls %d state %+v", f.client.layerCalls, got)
			}
		})
	}
}

func TestFirstLayerCompletionAboveLayerTwoSchedulesNoRetry(t *testing.T) {
	f := newFixture(t, "RUNNING")
	f.client.layerResult = lowClarityExample()
	f.event("RUNNING", 3, 0, "first_layer_complete")
	if f.client.layerCalls != 1 || f.layer().State != "inconclusive" {
		t.Fatalf("calls %d state %+v", f.client.layerCalls, f.layer())
	}
	// A scheduled retry would run on the first due layer 2 report.
	f.retryDue()
	if f.client.layerCalls != 1 {
		t.Fatalf("retry scheduled above layer 2: calls %d", f.client.layerCalls)
	}
}

func TestFirstLayerRetryPauseGuards(t *testing.T) {
	for _, tc := range []struct {
		name   string
		arm    func(*fixture, context.CancelFunc)
		reason string
	}{
		{"state changes", func(f *fixture, _ context.CancelFunc) {
			f.client.layerHook = func() { f.state.doc = reportDoc("PAUSED", 2, 0) }
		}, "print_paused"},
		{"context expires", func(f *fixture, cancel context.CancelFunc) { f.client.layerHook = cancel }, errorCategory(context.Canceled)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "RUNNING")
			f.firstLayerLow(t, visibleTangleLowClarity())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.client.layerResult = LayerResult{PAssessable: .9, PTangled: .9}
			tc.arm(f, cancel)
			f.clock.advance(layerRetryDelay)
			f.deliver(ctx, reportDoc("RUNNING", 2, 0))
			got := f.layer()
			if f.commands.pauses != 0 || got.State != "warning" || got.Pause != "" || got.Reason != tc.reason || f.countEvents("platecheck_layer1_warning") != 1 {
				t.Fatalf("guarded retry pause: pauses %d state %+v", f.commands.pauses, got)
			}
		})
	}
}

func TestFirstLayerDisplayExplainsLimitedViews(t *testing.T) {
	rows := func(f *fixture) map[string]module.Row {
		out := map[string]module.Row{}
		for _, r := range f.s.Display("01S1").Panel.Rows {
			out[r.Label] = r
		}
		return out
	}
	t.Run("inconclusive", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		f.firstLayerLow(t, lowClarityExample())
		got := f.layer()
		r := rows(f)
		if got.State != "inconclusive" || r["First layer"].Tone != "" || r["Image clarity"].Tone != "warn" ||
			!containsAll(r["Image clarity"].Value, []string{"25%", "70%"}) ||
			!containsAll(r["Findings"].Value, []string{"7%", "17%", "23%", "26%"}) {
			t.Fatalf("rows %+v", r)
		}
	})
	t.Run("warning", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		f.firstLayerLow(t, visibleTangleLowClarity())
		got := f.layer()
		r := rows(f)
		if got.State != "warning" || r["First layer"].Tone != "warn" || r["Image clarity"].Tone != "warn" ||
			!containsAll(r["Image clarity"].Value, []string{"25%", "70%"}) ||
			!strings.Contains(r["Findings"].Value, "90%") {
			t.Fatalf("rows %+v", r)
		}
	})
	t.Run("assessable result", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		f.client.layerResult = LayerResult{PAssessable: .9}
		f.event("RUNNING", 2, 0, "first_layer_complete")
		got := f.layer()
		r := rows(f)
		if got.State != "passed" || r["First layer"].Tone != "calm" || !containsAll(r["Image clarity"].Value, []string{"90%", "70%"}) {
			t.Fatalf("rows %+v", r)
		}
	})
}
