package platecheck

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

func timeInPast() time.Time { return time.Unix(1, 0) }

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *testClock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

type fakeState struct {
	mu      sync.Mutex
	j       telemetry.JobView
	v       telemetry.SessionView
	jobHook func()
}

func (f *fakeState) Job(string) (telemetry.JobView, bool) {
	if f.jobHook != nil {
		f.jobHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.j, true
}
func (f *fakeState) Session(string) (telemetry.SessionView, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := f.v
	if v.LayerNum != nil {
		v.LayerNum = new(*v.LayerNum)
	}
	return v, true
}
func (f *fakeState) edit(fn func(*telemetry.JobView, *telemetry.SessionView)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(&f.j, &f.v)
}

type fakeCommands struct {
	mu                  sync.Mutex
	offline             map[string]bool
	gen                 uint64
	stops, lights       []uint64
	stopErr, lightErr   error
	stopHook, lightHook func()
	pauses              []uint64
	pauseErr            error
	pauseHook           func()
}

func (f *fakeCommands) Generation(string) uint64 { f.mu.Lock(); defer f.mu.Unlock(); return f.gen }
func (f *fakeCommands) Connected(serial string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.offline[serial]
}
func (f *fakeCommands) StopPrint(_ string, g uint64) error {
	f.mu.Lock()
	f.stops = append(f.stops, g)
	f.mu.Unlock()
	if f.stopHook != nil {
		f.stopHook()
	}
	return f.stopErr
}
func (f *fakeCommands) PausePrint(_ string, g uint64) error {
	f.mu.Lock()
	f.pauses = append(f.pauses, g)
	f.mu.Unlock()
	if f.pauseHook != nil {
		f.pauseHook()
	}
	return f.pauseErr
}
func (f *fakeCommands) SetChamberLight(_ string, g uint64, _ bool) error {
	f.mu.Lock()
	f.lights = append(f.lights, g)
	f.mu.Unlock()
	if f.lightHook != nil {
		f.lightHook()
	}
	return f.lightErr
}
func (f *fakeCommands) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.stops) }
func (f *fakeCommands) reconnect() { f.mu.Lock(); f.gen++; f.mu.Unlock() }

type frameFunc func(context.Context, string, time.Time) (Frame, error)

func (f frameFunc) Capture(ctx context.Context, s string, t time.Time) (Frame, error) {
	return f(ctx, s, t)
}

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

type fixture struct {
	s        *Service
	w        *worker
	state    *fakeState
	commands *fakeCommands
	client   *fakeDecision
	clock    *testClock
	activity *activity.Log
}

func newFixture(t *testing.T, state string) *fixture {
	t.Helper()
	clock := &testClock{t: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}
	st := &fakeState{j: telemetry.JobView{Generation: 1, Revision: 1, Active: true, State: state, StateGen: 1, ObsGen: 1, ObsAt: clock.now()}, v: telemetry.SessionView{State: state, Epoch: 3, StateGen: 1, ObsGen: 1, Obs: 10, ObsAt: clock.now()}}
	if state == "RUNNING" {
		st.v.Active = true
		st.v.SessionGen = 1
		st.v.LayerNum = new(0)
	}
	commands := &fakeCommands{gen: 1}
	client := &fakeDecision{result: Result{PClear: .1, POccupied: 1 - .1, PAssessable: .9, Model: "clef"}}
	data := testJPEG(t)
	frames := frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		clock.advance(time.Millisecond)
		return Frame{JPEG: data, Seq: 7, Captured: clock.now(), Width: 8, Height: 6}, nil
	})
	printers := []config.Printer{{Serial: "01S1", Model: "P1S"}}
	s := New(printers, testSettings(), client, frames, st, commands, nil)
	s.now = clock.now
	events := activity.New(printers)
	s.SetActivity(events)
	return &fixture{s: s, w: s.workers["01S1"], state: st, commands: commands, client: client, clock: clock, activity: events}
}
func (f *fixture) hint(kind string) {
	entry := activity.Entry{Kind: kind, Time: f.clock.now()}
	if kind == "first_layer_complete" {
		// Intentionally valid first-layer events carry the current source
		// evidence, exactly as firstlayer records them: a coherent
		// Job+Session snapshot at sampling time. Deliberately invalid
		// events must be built by hand so the rejection paths stay
		// reachable.
		j, _ := f.state.Job("01S1")
		v, _ := f.state.Session("01S1")
		entry.Observation = activity.Observation{SessionGen: v.SessionGen, Epoch: v.Epoch, StateGen: v.StateGen, ObsGen: v.ObsGen, ObsAt: v.ObsAt, JobGen: j.Generation, JobRevision: j.Revision, RunningEpoch: j.RunningEpoch}
	}
	f.s.ObserveActivity("01S1", entry)
}
func (f *fixture) step() { f.w.step(context.Background()) }
func (f *fixture) report(state string, layer *int) {
	f.clock.advance(time.Millisecond)
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
		old := j.State
		if old != state {
			v.Epoch++
			if old == "RUNNING" {
				j.RunningEpoch++
			}
			if state == "RUNNING" && !v.Active {
				v.SessionGen++
			}
		}
		j.State = state
		j.Active = state == "PREPARE" || state == "SLICING" || state == "RUNNING" || state == "PAUSE" || state == "PAUSED"
		j.ObsAt = f.clock.now()
		v.State = state
		v.Active = state == "RUNNING" || state == "PAUSE" || state == "PAUSED"
		v.Obs++
		v.ObsAt = f.clock.now()
		v.LayerNum = layer
	})
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

func TestPrepareStopThenSingleFirstRunningFollowup(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.hint("print_preparing")
	f.step()
	if f.commands.count() != 1 || f.client.calls != 1 || f.status() != "stop_requested" || !f.hasEvent("platecheck_stop_requested") {
		t.Fatalf("initial count=%d calls=%d state=%s", f.commands.count(), f.client.calls, f.status())
	}
	f.hint("print_preparing")
	f.step()
	if f.commands.count() != 1 {
		t.Fatal("timer retried PREPARE")
	}
	f.report("RUNNING", new(0))
	f.hint("print_started")
	f.step()
	if f.commands.count() != 2 || f.client.calls != 1 {
		t.Fatalf("followup count=%d calls=%d", f.commands.count(), f.client.calls)
	}
	for range 3 {
		f.hint("print_started")
		f.step()
	}
	if f.commands.count() != 2 {
		t.Fatal("repeated stop")
	}
	f.report("IDLE", nil)
	f.step()
	if f.status() != "stopped" || !f.hasEvent("platecheck_stopped") {
		t.Fatalf("confirmation %s", f.status())
	}
}

func TestDirectRunningRequiresFreshExplicitLayerZero(t *testing.T) {
	for _, tc := range []struct {
		name, kind string
		layer      *int
		age        time.Duration
		want       int
	}{
		{"zero", "print_started", new(0), 0, 1}, {"unknown", "print_started", nil, 0, 0}, {"positive", "print_started", new(1), 0, 0}, {"preparing hint", "print_preparing", new(0), 0, 0}, {"expired", "print_started", new(0), 6 * time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "RUNNING")
			f.state.v.LayerNum = tc.layer
			f.hint(tc.kind)
			f.clock.advance(tc.age)
			f.step()
			if f.commands.count() != tc.want {
				t.Fatalf("count %d", f.commands.count())
			}
			if tc.want != 0 {
				f.step()
				if f.commands.count() != 1 {
					t.Fatal("running retry")
				}
			}
		})
	}
}

func TestAttachNeverAuthorizesExistingPrint(t *testing.T) {
	for _, state := range []string{"PREPARE", "RUNNING", "SLICING"} {
		t.Run(state, func(t *testing.T) {
			f := newFixture(t, state)
			f.hint("state_initial")
			f.hint("print_started")
			f.hint("print_preparing")
			f.step()
			if f.client.calls != 0 || f.commands.count() != 0 {
				t.Fatal("attached check")
			}
		})
	}
}

func TestSlicingWaitsAndExpiresWithoutSyntheticStartup(t *testing.T) {
	f := newFixture(t, "SLICING")
	f.hint("print_preparing")
	f.step()
	if f.client.calls != 0 {
		t.Fatal("SLICING admitted")
	}
	f.clock.advance(5 * time.Second)
	f.report("PREPARE", nil)
	f.step()
	if f.commands.count() != 1 {
		t.Fatal("PREPARE not admitted")
	}
	f = newFixture(t, "SLICING")
	f.hint("print_preparing")
	f.clock.advance(31 * time.Second)
	f.step()
	if f.client.calls != 0 || f.status() != "skipped" {
		t.Fatal("expired hint admitted")
	}
	f = newFixture(t, "RUNNING")
	f.step()
	if f.client.calls != 0 {
		t.Fatal("ticker invented candidate")
	}
}

func TestNoCommandAfterEvidenceChangesInCapture(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*fixture)
	}{
		{"pause resume", func(f *fixture) { f.report("RUNNING", new(0)); f.report("PAUSE", new(0)); f.report("RUNNING", new(0)) }},
		{"revision", func(f *fixture) { f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Revision++ }) }},
		{"replacement", func(f *fixture) {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Generation++ })
		}},
		{"reconnect", func(f *fixture) { f.commands.reconnect() }},
		{"ended", func(f *fixture) { f.report("IDLE", nil) }},
		{"positive layer", func(f *fixture) { f.report("RUNNING", new(1)) }},
		{"unknown layer", func(f *fixture) { f.report("RUNNING", nil) }},
		{"initial suppression", func(f *fixture) { f.hint("state_initial") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "PREPARE")
			base := f.s.frames
			f.s.frames = frameFunc(func(ctx context.Context, s string, after time.Time) (Frame, error) {
				frame, err := base.Capture(ctx, s, after)
				tc.change(f)
				return frame, err
			})
			f.hint("print_preparing")
			f.step()
			if f.commands.count() != 0 || f.client.calls != 0 || f.status() != "skipped" {
				t.Fatalf("stale work count=%d calls=%d state=%s", f.commands.count(), f.client.calls, f.status())
			}
		})
	}
}

func TestOriginalEpochAllowsOnlyFirstRunningDuringCapture(t *testing.T) {
	f := newFixture(t, "PREPARE")
	base := f.s.frames
	f.s.frames = frameFunc(func(ctx context.Context, s string, after time.Time) (Frame, error) {
		frame, err := base.Capture(ctx, s, after)
		f.report("RUNNING", new(0))
		return frame, err
	})
	f.hint("print_preparing")
	f.step()
	if f.commands.count() != 1 || f.client.calls != 1 {
		t.Fatal("first RUNNING rejected")
	}
	f.step()
	if f.commands.count() != 1 {
		t.Fatal("RUNNING initial verdict retried")
	}
}

func TestNoCommandAfterInferenceOrBeforeSecondDispatch(t *testing.T) {
	for _, change := range []func(*fixture){
		func(f *fixture) { f.report("RUNNING", new(1)) },
		func(f *fixture) { f.report("RUNNING", nil) },
		func(f *fixture) { f.clock.advance(26 * time.Second) },
		func(f *fixture) { f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Revision++ }) },
	} {
		f := newFixture(t, "PREPARE")
		f.client.hook = func() { change(f) }
		f.hint("print_preparing")
		f.step()
		if f.commands.count() != 0 {
			t.Fatal("stale inference stopped")
		}
	}
	for _, layer := range []*int{nil, new(1)} {
		f := newFixture(t, "PREPARE")
		f.hint("print_preparing")
		f.step()
		f.report("RUNNING", layer)
		f.step()
		if f.commands.count() != 1 || f.status() != "stop_unconfirmed" {
			t.Fatalf("second dispatch not suppressed %s", f.status())
		}
	}
}

func TestErrorsAndQualityGateFailOpenAndClearScores(t *testing.T) {
	for _, tc := range []struct {
		name  string
		r     Result
		err   error
		state string
	}{
		{"clear", Result{PClear: .9, POccupied: 1 - .9, PAssessable: .9}, nil, "clear"},
		{"equality", Result{PClear: .5, POccupied: .5, PAssessable: .9}, nil, "below_threshold"},
		{"dark", Result{PClear: .01, POccupied: 1 - .01, PAssessable: .79}, nil, "inconclusive"},
		{"failed", Result{}, errors.New("hostile credential marker"), "error"},
		{"invalid", Result{PClear: -1, POccupied: 2, PAssessable: 1}, nil, "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "PREPARE")
			f.client.result, f.client.err = tc.r, tc.err
			f.hint("print_preparing")
			f.step()
			if f.commands.count() != 0 || f.status() != tc.state {
				t.Fatalf("state %s count %d", f.status(), f.commands.count())
			}
			if tc.state == "error" && f.s.moduleState("01S1").(ModuleState).POccupied != nil {
				t.Fatal("error shows score")
			}
		})
	}
	f := newFixture(t, "PREPARE")
	f.hint("print_preparing")
	f.step()
	f.report("IDLE", nil)
	f.step()
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
		j.Generation++
		j.Active = true
		j.State = "PREPARE"
		j.ObsAt = f.clock.now()
		v.State = "PREPARE"
		v.LayerNum = nil
	})
	f.s.frames = IdleFrames{}
	f.hint("print_preparing")
	f.step()
	if f.s.moduleState("01S1").(ModuleState).POccupied != nil {
		t.Fatal("new error retained old score")
	}
}

func TestLightGuardAndPostSendBoundary(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.state.v.ChamberLight = "off"
	f.commands.lightHook = func() { f.clock.advance(time.Second) }
	var boundary time.Time
	base := f.s.frames
	f.s.frames = frameFunc(func(ctx context.Context, s string, after time.Time) (Frame, error) {
		boundary = after
		return base.Capture(ctx, s, after)
	})
	start := f.clock.now()
	f.hint("print_preparing")
	f.step()
	if len(f.commands.lights) != 1 || !boundary.Equal(start.Add(time.Second)) {
		t.Fatalf("light boundary %v", boundary)
	}
	f = newFixture(t, "PREPARE")
	f.state.v.ChamberLight = "off"
	f.commands.lightErr = errors.New("private marker")
	f.hint("print_preparing")
	f.step()
	if f.commands.count() != 1 || len(f.commands.lights) != 1 {
		t.Fatal("failed lighting blocked fresh view")
	}
}

func TestStopConfirmationNeedsNewSameJobRealObservation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*fixture)
		state  string
	}{
		{"old idle", func(f *fixture) {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
				j.State = "IDLE"
				j.Active = false
				v.State = "IDLE"
				v.Active = false
			})
		}, "stop_requested"},
		{"ack only", func(*fixture) {}, "stop_requested"},
		{"finish", func(f *fixture) { f.report("FINISH", nil) }, "stop_unconfirmed"},
		{"replacement", func(f *fixture) {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Generation++ })
		}, "stop_unconfirmed"},
		{"revision", func(f *fixture) { f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Revision++ }) }, "stop_unconfirmed"},
		{"reconnect", func(f *fixture) { f.commands.reconnect() }, "stop_unconfirmed"},
		{"expiry", func(f *fixture) { f.clock.advance(30 * time.Second) }, "stop_unconfirmed"},
		{"failed terminal", func(f *fixture) { f.report("FAILED", nil) }, "stopped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "RUNNING")
			f.hint("print_started")
			f.step()
			if f.hasEvent("platecheck_stopped") {
				t.Fatal("local publication confirmed")
			}
			tc.change(f)
			f.step()
			if f.status() != tc.state {
				t.Fatalf("status %s want %s", f.status(), tc.state)
			}
			if tc.state != "stopped" && f.hasEvent("platecheck_stopped") {
				t.Fatal("false stop confirmation")
			}
		})
	}
}

func TestPublishFailuresCountAndSecondWindowResetsOnce(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.commands.stopErr = errors.New("unsafe transport text")
	f.hint("print_preparing")
	f.step()
	if f.status() != "stop_failed" || !f.hasEvent("platecheck_stop_failed") {
		t.Fatal("missing failure")
	}
	firstDeadline := f.w.pending.deadline
	f.clock.advance(10 * time.Second)
	f.report("RUNNING", new(0))
	f.step()
	if f.commands.count() != 2 || !f.w.pending.deadline.After(firstDeadline) {
		t.Fatal("followup reset")
	}
	f.clock.advance(30 * time.Second)
	f.step()
	if f.status() != "stop_unconfirmed" || f.commands.count() != 2 {
		t.Fatal("expiry retry")
	}
}

func TestStableSnapshotReadRejectsRevisionTear(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.hint("print_preparing")
	calls := 0
	f.state.jobHook = func() {
		calls++
		if calls == 2 {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Revision++ })
		}
	}
	f.step()
	if f.commands.count() != 0 || f.client.calls != 0 {
		t.Fatal("torn snapshot admitted")
	}
}

func TestCloseCancelsAndJoinsOutstandingFrameWait(t *testing.T) {
	f := newFixture(t, "PREPARE")
	entered := make(chan struct{})
	exited := make(chan struct{})
	f.s.frames = frameFunc(func(ctx context.Context, _ string, _ time.Time) (Frame, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return Frame{}, ctx.Err()
	})
	f.s.kPoll = time.Hour
	f.hint("print_preparing")
	f.s.Start()
	<-entered
	f.s.Close()
	<-exited
	f.s.Close()
	f.s.Start()
	if f.commands.count() != 0 {
		t.Fatal("command after close")
	}
}

func TestRealTelemetryIdentityContinuityAndCoalescedReplacement(t *testing.T) {
	printers := []config.Printer{{Serial: "01S1", Model: "P1S"}}
	for _, tc := range []string{"continuous", "pause resume", "replacement", "canceled"} {
		t.Run(tc, func(t *testing.T) {
			cache := telemetry.NewCache(printers, slog.New(slog.NewTextHandler(io.Discard, nil)))
			commands := &fakeCommands{gen: 1}
			client := &fakeDecision{result: Result{PClear: .1, POccupied: 1 - .1, PAssessable: .9}}
			seq := uint64(0)
			report := func(body string) { seq++; cache.ObserveReport("01S1", seq, 1, []byte(body)) }
			report(`{"print":{"gcode_state":"IDLE"}}`)
			report(`{"print":{"gcode_state":"PREPARE","project_id":1,"task_id":2,"subtask_name":"a.3mf"}}`)
			admission, _ := cache.Job("01S1")
			data := testJPEG(t)
			frames := frameFunc(func(ctx context.Context, _ string, after time.Time) (Frame, error) {
				report(`{"print":{"gcode_state":"RUNNING","layer_num":0}}`)
				if tc == "pause resume" {
					report(`{"print":{"gcode_state":"PAUSE"}}`)
					report(`{"print":{"gcode_state":"RUNNING","layer_num":0}}`)
				}
				if tc == "replacement" {
					report(`{"print":{"gcode_state":"IDLE"}}`)
					report(`{"print":{"gcode_state":"PREPARE","project_id":1,"task_id":3,"subtask_name":"b.3mf"}}`)
					report(`{"print":{"gcode_state":"RUNNING","layer_num":0}}`)
				}
				if tc == "canceled" {
					report(`{"print":{"gcode_state":"IDLE"}}`)
				}
				return Frame{JPEG: data, Seq: 7, Captured: time.Now(), Width: 8, Height: 6}, nil
			})
			s := New(printers, testSettings(), client, frames, cache, commands, nil)
			s.ObserveActivity("01S1", activity.Entry{Kind: "print_preparing", Time: time.Now()})
			s.workers["01S1"].step(context.Background())
			current, _ := cache.Job("01S1")
			if tc == "continuous" {
				if current.Generation != admission.Generation || current.Revision != admission.Revision || commands.count() != 1 {
					t.Fatalf("continuity %+v %+v count %d", admission, current, commands.count())
				}
			} else if commands.count() != 0 {
				t.Fatal("coalesced stale work dispatched")
			}
		})
	}
}

func TestModuleDisabledBlockedAndApprovedDisplay(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.s.settings.Enabled = new(false)
	m := f.s.Module()
	if m.NeedsReports || m.ObserveActivity != nil || len(m.Routes) != 1 || m.State("01S1") != nil || m.Display("01S1") != nil {
		t.Fatal("off contract")
	}
	if f.s.status("01S1").(ModuleState).State != "disabled" {
		t.Fatal("status off")
	}
	f.s.settings.Enabled = new(true)
	f.s.SetBlocked(ReasonCameraDisabled)
	m = f.s.Module()
	if m.NeedsReports || m.ObserveActivity != nil || len(m.Routes) != 0 || m.State("01S1").(ModuleState).State != "blocked" {
		t.Fatal("blocked contract")
	}
	f.s.Start()
	f.s.Close()
	if f.commands.count() != 0 {
		t.Fatal("blocked command")
	}
	f.s.SetBlocked("")
	for _, state := range []string{"idle", "checking", "blocked", "clear", "below_threshold", "inconclusive", "error", "skipped", "stop_requested", "stopped", "stop_failed", "stop_unconfirmed"} {
		f.w.setStatus(state, nil)
		d := f.s.Display("01S1")
		if d.Panel.Title != "Plate check" || d.Panel.Level != 3 || d.Panel.Rows[0].Value == "" || len(d.Overlay) != 0 {
			t.Fatalf("display %s", state)
		}
		// Result and First layer rows always render; non-idle states add Stop above.
		want := 3
		if state == "idle" {
			want = 2
		}
		if len(d.Panel.Rows) != want {
			t.Fatal("rows")
		}
	}
}

func TestAdmissionRequiresCurrentConnectionAndRealReportFreshness(t *testing.T) {
	for _, edit := range []func(*fixture){
		func(f *fixture) { f.commands.gen = 0 },
		func(f *fixture) { f.state.j.StateGen = 0 },
		func(f *fixture) { f.state.j.ObsGen = 0 },
		func(f *fixture) { f.state.v.StateGen = 0 },
		func(f *fixture) { f.state.v.ObsGen = 0 },
		func(f *fixture) { f.clock.advance(16 * time.Second) },
	} {
		f := newFixture(t, "PREPARE")
		edit(f)
		f.hint("print_preparing")
		f.step()
		if f.client.calls != 0 || f.commands.count() != 0 {
			t.Fatal("stale report admitted")
		}
	}
	f := newFixture(t, "PREPARE")
	f.hint("print_preparing")
	f.clock.advance(16 * time.Second)
	f.step()
	if f.client.calls != 0 {
		t.Fatal("old report uploaded")
	}
	f.report("PREPARE", nil)
	f.step()
	if f.commands.count() != 1 {
		t.Fatal("fresh report did not revalidate pending hint")
	}
}

func TestSlicingRunningHintUsesItsOwnFiveSecondBoundary(t *testing.T) {
	f := newFixture(t, "SLICING")
	f.hint("print_preparing")
	f.step()
	f.clock.advance(20 * time.Second)
	f.report("RUNNING", new(0))
	f.hint("print_started")
	f.step()
	if f.commands.count() != 1 {
		t.Fatal("fresh RUNNING hint inherited SLICING age")
	}
}

func TestCaptureOverallBudgetAndFrameFreshness(t *testing.T) {
	for _, mode := range []string{"slow", "equal", "future"} {
		f := newFixture(t, "PREPARE")
		data := testJPEG(t)
		f.s.frames = frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
			captured := after
			switch mode {
			case "slow":
				f.clock.advance(26 * time.Second)
				f.report("PREPARE", nil)
				captured = f.clock.now()
			case "future":
				captured = after.Add(time.Hour)
			}
			return Frame{JPEG: data, Captured: captured}, nil
		})
		f.hint("print_preparing")
		f.step()
		if f.client.calls != 0 || f.commands.count() != 0 || f.status() != "skipped" {
			t.Fatalf("%s stale frame passed", mode)
		}
	}
}

func TestTerminalAfterConfirmationWindowNeverConfirms(t *testing.T) {
	f := newFixture(t, "RUNNING")
	f.hint("print_started")
	f.step()
	f.clock.advance(31 * time.Second)
	f.report("IDLE", nil)
	f.step()
	if f.status() != "stop_unconfirmed" || f.hasEvent("platecheck_stopped") {
		t.Fatal("late terminal falsely confirmed")
	}
}

func TestOldCounterCannotConfirmAndNewGenerationGetsOneCheck(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.hint("print_preparing")
	f.step()
	oldObs := f.state.v.Obs
	f.report("IDLE", nil)
	f.state.v.Obs = oldObs
	f.step()
	if f.hasEvent("platecheck_stopped") {
		t.Fatal("ACK-only counter confirmed terminal")
	}
	f.clock.advance(time.Millisecond)
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.ObsAt = f.clock.now(); v.Obs++ })
	f.step()
	if f.status() != "stopped" {
		t.Fatal("new terminal observation not confirmed")
	}
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
		j.Generation++
		j.State = "PREPARE"
		j.Active = true
		j.ObsAt = f.clock.now()
		v.State = "PREPARE"
		v.Active = false
		v.Epoch++
		v.LayerNum = nil
	})
	f.hint("print_preparing")
	f.step()
	f.hint("print_preparing")
	f.step()
	if f.client.calls != 2 || f.commands.count() != 2 {
		t.Fatalf("per-generation count: calls=%d stops=%d", f.client.calls, f.commands.count())
	}
}

type evaluateFunc func(context.Context, []byte, string) (Result, error)

func (f evaluateFunc) Probe(context.Context) error { return nil }
func (f evaluateFunc) Evaluate(ctx context.Context, data []byte, model string) (Result, error) {
	return f(ctx, data, model)
}
func (f evaluateFunc) EvaluateFirstLayer(ctx context.Context, _ [][]byte, _ string) (LayerResult, error) {
	return LayerResult{}, ctx.Err()
}

func TestCloseCancelsOutstandingInferenceAndConcurrentReaders(t *testing.T) {
	f := newFixture(t, "PREPARE")
	entered := make(chan struct{})
	exited := make(chan struct{})
	f.s.client = evaluateFunc(func(ctx context.Context, _ []byte, _ string) (Result, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return Result{}, ctx.Err()
	})
	f.s.kPoll = time.Hour
	f.hint("print_preparing")
	f.s.Start()
	<-entered
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			f.s.Display("01S1")
			f.s.statusMap()
			f.hint("print_started")
		}
	}()
	f.s.Close()
	<-exited
	<-done
	if f.commands.count() != 0 {
		t.Fatal("stop after canceled inference")
	}
}

func TestAutomaticLogsAndActivityExcludeUnsafeFailureText(t *testing.T) {
	f := newFixture(t, "PREPARE")
	var logs bytes.Buffer
	f.s.log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})).With("origin", "platecheck")
	f.commands.stopErr = errors.New("credential-marker endpoint-marker\nforged-marker")
	f.hint("print_preparing")
	f.step()
	f.clock.advance(31 * time.Second)
	f.step()
	for _, marker := range []string{"credential-marker", "endpoint-marker", "forged-marker", base64.StdEncoding.EncodeToString(testJPEG(t))} {
		if strings.Contains(logs.String(), marker) {
			t.Fatalf("unsafe log %s", marker)
		}
		for _, entry := range f.activity.Recent("01S1") {
			if strings.Contains(entry.Message, marker) {
				t.Fatal("unsafe activity")
			}
		}
	}
	for _, expected := range []string{"origin=platecheck", "operation=", "p_clear=0.1", "p_assessable=0.9", "frame_sequence=7", "job_generation=1", "job_revision=1", "connection_generation=1", "error_code=command_failed", "stop unconfirmed"} {
		if !strings.Contains(logs.String(), expected) {
			t.Fatalf("missing %s: %s", expected, logs.String())
		}
	}
}

func TestLayerReportBetweenSessionAndJobPreventsBothStopDispatches(t *testing.T) {
	advanceLayer := func(f *fixture) {
		f.clock.advance(time.Millisecond)
		f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
			j.ObsAt = f.clock.now()
			v.Obs++
			v.ObsAt = f.clock.now()
			v.LayerNum = new(1)
		})
	}
	t.Run("first dispatch", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		calls := 0
		injected := false
		f.client.hook = func() {
			f.state.jobHook = func() {
				calls++
				// The result guard uses the first pair; dispatch uses the next pair.
				if calls == 4 {
					injected = true
					advanceLayer(f)
				}
			}
		}
		f.hint("print_started")
		f.step()
		if !injected || f.commands.count() != 0 {
			t.Fatalf("torn first dispatch injected=%v stops=%d", injected, f.commands.count())
		}
	})
	t.Run("PREPARE followup dispatch", func(t *testing.T) {
		f := newFixture(t, "PREPARE")
		f.hint("print_preparing")
		f.step()
		f.report("RUNNING", new(0))
		calls := 0
		injected := false
		f.state.jobHook = func() {
			calls++
			// Confirmation, follow-up authorization, then dispatch each read a pair.
			if calls == 6 {
				injected = true
				advanceLayer(f)
			}
		}
		f.step()
		if !injected || f.commands.count() != 1 {
			t.Fatalf("torn followup injected=%v stops=%d", injected, f.commands.count())
		}
	})
	t.Run("admission", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		f.hint("print_started")
		calls := 0
		f.state.jobHook = func() {
			calls++
			if calls == 2 {
				advanceLayer(f)
			}
		}
		f.step()
		if f.commands.count() != 0 || f.client.calls != 0 {
			t.Fatal("torn admission used old layer zero")
		}
	})
}

func TestSnapshotRetriesAreBoundedAndSessionCounterMustBeStable(t *testing.T) {
	f := newFixture(t, "RUNNING")
	calls := 0
	f.state.jobHook = func() {
		calls++
		if calls%2 == 0 {
			// Keep the job timestamp unchanged to exercise the independent session guard.
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { v.Obs++; v.ObsAt = v.ObsAt.Add(time.Nanosecond) })
		}
	}
	_, _, _, valid := f.w.snapshot()
	if valid || calls != 6 {
		t.Fatalf("unstable observations valid=%v Job reads=%d", valid, calls)
	}
}
