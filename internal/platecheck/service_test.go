package platecheck

import (
	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
	"context"
	"fmt"
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
}

func (f *fakeState) Get(string) (printer.Doc, bool, bool) { return f.doc, true, true }
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
