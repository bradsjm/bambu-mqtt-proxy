package platecheck

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/module"
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
	mu        sync.Mutex
	j         telemetry.JobView
	v         telemetry.SessionView
	printHook func()
}

func (f *fakeState) Print(string) (telemetry.JobView, telemetry.SessionView, bool) {
	if f.printHook != nil {
		f.printHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	v := f.v
	if v.LayerNum != nil {
		v.LayerNum = new(*v.LayerNum)
	}
	if len(v.StagesThisJob) > 0 {
		v.StagesThisJob = append([]int(nil), v.StagesThisJob...)
	}
	return f.j, v, true
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
	// strict models the pool's generation guard: sends with a stale
	// generation are rejected and never delivered.
	strict bool
	// race runs inside StopPrint before the generation check, so a test
	// can interleave a reconnect between the gate's generation read and
	// the send.
	race func()
	// delivered records generations the pool accepted.
	delivered []uint64
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
	race := f.race
	f.mu.Unlock()
	if race != nil {
		race()
	}
	if f.stopHook != nil {
		f.stopHook()
	}
	if f.stopErr != nil {
		return f.stopErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.strict && g != f.gen {
		return errors.New("connection generation changed")
	}
	f.delivered = append(f.delivered, g)
	return nil
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
	st := &fakeState{j: telemetry.JobView{Generation: 1, Active: true, State: state, StartObserved: true}, v: telemetry.SessionView{State: state, Epoch: 3, StateGen: 1, ObsGen: 1, Obs: 10, ObsAt: clock.now()}}
	if state == "RUNNING" {
		st.v.Active = true
		st.v.SessionGen = 1
		st.v.LayerNum = new(0)
		st.v.LayerThisJob = true
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
	if kind == "print_preparing" || kind == "print_started" || kind == "first_layer_complete" {
		// Intentionally valid start and first-layer events carry the
		// current job generation, exactly as telemetry and firstlayer
		// record them. Deliberately invalid events must be built by hand
		// so the rejection paths stay reachable.
		j, _, _ := f.state.Print("01S1")
		entry.Observation = activity.Observation{JobGen: j.Generation}
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
			if state == "RUNNING" && !v.Active {
				v.SessionGen++
			}
		}
		j.State = state
		j.Active = state == "PREPARE" || state == "SLICING" || state == "RUNNING" || state == "PAUSE" || state == "PAUSED"
		v.State = state
		v.Active = state == "RUNNING" || state == "PAUSE" || state == "PAUSED"
		v.Obs++
		v.ObsAt = f.clock.now()
		v.LayerNum = layer
		v.LayerThisJob = layer != nil
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

func TestDirectRunningAdmitsLayerZeroAndWaitsForMissingLayer(t *testing.T) {
	t.Run("zero admits", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		f.hint("print_started")
		f.step()
		if f.commands.count() != 1 || f.client.calls != 1 {
			t.Fatalf("count=%d calls=%d", f.commands.count(), f.client.calls)
		}
		f.step()
		if f.commands.count() != 1 {
			t.Fatal("running retry")
		}
	})
	t.Run("positive skips", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { v.LayerNum = new(1); v.LayerThisJob = true })
		f.hint("print_started")
		f.step()
		if f.commands.count() != 0 || f.client.calls != 0 || f.status() != "skipped" {
			t.Fatalf("positive layer acted: count=%d calls=%d state=%s", f.commands.count(), f.client.calls, f.status())
		}
		if got := f.s.moduleState("01S1").(ModuleState).Reason; got != "print_past_start" {
			t.Fatalf("reason = %q", got)
		}
	})
	t.Run("unknown waits then skips", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { v.LayerNum = nil })
		f.hint("print_started")
		f.step()
		if f.commands.count() != 0 || f.client.calls != 0 || f.status() != "idle" {
			t.Fatalf("wait acted: count=%d calls=%d state=%s", f.commands.count(), f.client.calls, f.status())
		}
		f.clock.advance(30 * time.Second)
		f.step()
		if f.commands.count() != 0 || f.status() != "skipped" {
			t.Fatalf("expired wait: count=%d state=%s", f.commands.count(), f.status())
		}
		if got := f.s.moduleState("01S1").(ModuleState).Reason; got != "layer_not_reported" {
			t.Fatalf("reason = %q", got)
		}
	})
	t.Run("layer arrives within wait", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { v.LayerNum = nil })
		f.hint("print_started")
		f.step()
		f.report("RUNNING", new(0))
		f.step()
		if f.commands.count() != 1 || f.client.calls != 1 {
			t.Fatalf("count=%d calls=%d", f.commands.count(), f.client.calls)
		}
	})
}

func TestAttachWithoutStartObservedSkipsWithoutActivity(t *testing.T) {
	for _, state := range []string{"PREPARE", "RUNNING", "SLICING"} {
		t.Run(state, func(t *testing.T) {
			f := newFixture(t, state)
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.StartObserved = false })
			f.s.ObserveActivity("01S1", activity.Entry{Kind: "state_initial", Time: f.clock.now(), Observation: activity.Observation{JobGen: 1}})
			f.hint("print_started")
			f.hint("print_preparing")
			f.step()
			if f.client.calls != 0 || f.commands.count() != 0 {
				t.Fatal("attached check")
			}
			if f.status() != "skipped" {
				t.Fatalf("state = %s", f.status())
			}
			if got := f.s.moduleState("01S1").(ModuleState).Reason; got != "start_not_observed" {
				t.Fatalf("reason = %q", got)
			}
			if len(f.activity.Recent("01S1")) != 0 {
				t.Fatal("attach recorded activity")
			}
		})
	}
}

func TestSlicingWaitsWithoutExpiring(t *testing.T) {
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
	// SLICING carries no wait condition, so a long SLICING phase never
	// expires the candidate on its own.
	f = newFixture(t, "SLICING")
	f.hint("print_preparing")
	f.clock.advance(31 * time.Second)
	f.step()
	if f.client.calls != 0 || f.status() != "idle" {
		t.Fatalf("SLICING wait acted: calls=%d state=%s", f.client.calls, f.status())
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
		reason string
	}{
		{"pause resume", func(f *fixture) { f.report("RUNNING", new(0)); f.report("PAUSE", new(0)); f.report("RUNNING", new(0)) }, ""},
		{"replacement", func(f *fixture) {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Generation++ })
		}, "print_ended"},
		{"ended", func(f *fixture) { f.report("IDLE", nil) }, "print_ended"},
		{"positive layer", func(f *fixture) { f.report("RUNNING", new(1)) }, "print_past_start"},
		{"unknown layer", func(f *fixture) { f.report("RUNNING", nil) }, "layer_not_reported"},
		{"offline", func(f *fixture) { f.commands.offline = map[string]bool{"01S1": true} }, "printer_offline"},
		{"paused", func(f *fixture) { f.report("PAUSE", new(0)) }, "print_paused"},
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
			if tc.reason == "" {
				// A same-job pause/resume that returns to an authorized
				// state does not revoke the check.
				if f.commands.count() != 1 || f.client.calls != 1 {
					t.Fatalf("continued work count=%d calls=%d state=%s", f.commands.count(), f.client.calls, f.status())
				}
				return
			}
			if f.commands.count() != 0 || f.client.calls != 0 || f.status() != "skipped" {
				t.Fatalf("stale work count=%d calls=%d state=%s", f.commands.count(), f.client.calls, f.status())
			}
			if got := f.s.moduleState("01S1").(ModuleState).Reason; got != tc.reason {
				t.Fatalf("reason = %q, want %q", got, tc.reason)
			}
		})
	}
	// A reconnect alone changes no authorization input, so the check
	// continues and the pool fake still accepts the same-generation send.
	f := newFixture(t, "PREPARE")
	base := f.s.frames
	f.s.frames = frameFunc(func(ctx context.Context, s string, after time.Time) (Frame, error) {
		frame, err := base.Capture(ctx, s, after)
		f.commands.reconnect()
		return frame, err
	})
	f.hint("print_preparing")
	f.step()
	if f.commands.count() != 1 || f.client.calls != 1 {
		t.Fatalf("reconnect revoked local authorization: count=%d calls=%d", f.commands.count(), f.client.calls)
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
	for _, tc := range []struct {
		name   string
		change func(*fixture)
		reason string
	}{
		{"positive layer", func(f *fixture) { f.report("RUNNING", new(1)) }, "print_past_start"},
		{"unknown layer", func(f *fixture) { f.report("RUNNING", nil) }, "layer_not_reported"},
		{"budget", func(f *fixture) { f.clock.advance(26 * time.Second) }, "check_timeout"},
		{"replacement", func(f *fixture) {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Generation++ })
		}, "print_ended"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "PREPARE")
			f.client.hook = func() { tc.change(f) }
			f.hint("print_preparing")
			f.step()
			if f.commands.count() != 0 {
				t.Fatal("stale inference stopped")
			}
			if got := f.s.moduleState("01S1").(ModuleState).Reason; got != tc.reason {
				t.Fatalf("reason = %q, want %q", got, tc.reason)
			}
		})
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

func TestStopConfirmationIsStateBased(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*fixture)
		state  string
	}{
		{"same idle", func(f *fixture) {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
				j.State = "IDLE"
				j.Active = false
				v.State = "IDLE"
				v.Active = false
			})
		}, "stopped"},
		{"ack only", func(*fixture) {}, "stop_requested"},
		{"finish", func(f *fixture) { f.report("FINISH", nil) }, "stop_unconfirmed"},
		{"replacement", func(f *fixture) {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Generation++ })
		}, "stop_unconfirmed"},
		{"reconnect", func(f *fixture) { f.commands.reconnect() }, "stop_requested"},
		{"expiry", func(f *fixture) { f.clock.advance(30 * time.Second) }, "stop_unconfirmed"},
		{"exact deadline", func(f *fixture) {
			f.clock.advance(30*time.Second - time.Millisecond)
			f.report("FAILED", nil)
		}, "stop_unconfirmed"},
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
			admission, _, _ := cache.Print("01S1")
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
			s.ObserveActivity("01S1", activity.Entry{Kind: "print_preparing", Time: time.Now(), Observation: activity.Observation{JobGen: admission.Generation}})
			s.workers["01S1"].step(context.Background())
			current, _, _ := cache.Print("01S1")
			if tc == "continuous" || tc == "pause resume" {
				if current.Generation != admission.Generation || commands.count() != 1 {
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
	blocked := f.s.Display("01S1")
	if blocked.Panel.Title != "Print checks" || blocked.Panel.Level != 3 {
		t.Fatalf("blocked panel %+v", blocked.Panel)
	}
	plate := blocked.Panel.Rows[0]
	if plate.Label != "Plate" || plate.Value != "Unavailable" || plate.Tone != "warn" || plate.Note != "Camera is disabled in configuration" || plate.Section != "Before printing" {
		t.Fatalf("blocked plate %+v", plate)
	}
	layer := rowByLabel(t, blocked, "First layer")
	if layer.Value != "Unavailable" || layer.Tone != "warn" || layer.Note != "Camera is disabled in configuration" || layer.Section != "After layer 1" {
		t.Fatalf("blocked layer %+v", layer)
	}
	if rowByLabel(t, blocked, "Rule").Section == "" {
		t.Fatal("blocked panel lost its sections")
	}
	f.s.Start()
	f.s.Close()
	if f.commands.count() != 0 {
		t.Fatal("blocked command")
	}
	f.s.SetBlocked("")
	type want struct {
		value, tone, note string
	}
	for _, tc := range []struct {
		state  string
		reason string
		want   want
	}{
		{"idle", "", want{"Not checked yet", "", "Checks the plate when a print starts and again after homing"}},
		{"checking", "", want{"Checking", "info", ""}},
		{"blocked", "", want{"Unavailable", "warn", "Camera is disabled in configuration"}},
		{"clear", "", want{"Clear", "calm", ""}},
		{"below_threshold", "", want{"Possibly occupied, below the stop threshold", "info", "Printing continued"}},
		{"inconclusive", "", want{"Inconclusive", "", "Camera view too unclear to judge; printing continued"}},
		{"error", "timeout", want{"Check failed", "warn", "AI provider timed out; printing continued"}},
		{"skipped", "start_not_observed", want{"Not checked", "", "Proxy started after this print began"}},
		{"skipped", "printer_offline", want{"Not checked", "", "Printer offline; no stop sent"}},
		{"stop_requested", "", want{"Stop requested", "error", "Waiting for the printer to confirm"}},
		{"stopped", "", want{"Print ended after stop request", "error", "Clear the plate before sending the print again"}},
		{"stop_failed", "", want{"Stop command failed", "error", "Check the printer now"}},
		{"stop_unconfirmed", "", want{"Stop not confirmed", "error", "Check the printer now"}},
	} {
		if tc.reason != "" {
			f.w.setStatusWithReason(tc.state, nil, tc.reason)
		} else {
			f.w.setStatus(tc.state, nil)
		}
		d := f.s.Display("01S1")
		if d.Panel.Title != "Print checks" || d.Panel.Level != 3 || len(d.Overlay) != 0 {
			t.Fatalf("display panel %+v", d.Panel)
		}
		got := d.Panel.Rows[0]
		if got.Label != "Plate" || got.Section != "Before printing" || got.Value != tc.want.value || got.Tone != tc.want.tone || got.Note != tc.want.note {
			t.Fatalf("state %s reason %q = %+v, want %+v", tc.state, tc.reason, got, tc.want)
		}
		rule := rowByLabel(t, d, "Rule")
		if rule.Section != "Before printing" || rule.Value != "Stops the print if the plate looks occupied (over 50%)" {
			t.Fatalf("startup rule %+v", rule)
		}
		first := rowByLabel(t, d, "First layer")
		if first.Section != "After layer 1" || first.Value == "" {
			t.Fatalf("first-layer row %+v", first)
		}
		var sections []string
		for _, row := range d.Panel.Rows {
			if row.Section != "" && (len(sections) == 0 || sections[len(sections)-1] != row.Section) {
				sections = append(sections, row.Section)
			}
		}
		if len(sections) != 2 || sections[0] != "Before printing" || sections[1] != "After layer 1" {
			t.Fatalf("state %s sections %v", tc.state, sections)
		}
	}
}

func TestDisplayScoresAndFirstLayerRows(t *testing.T) {
	f := newFixture(t, "RUNNING")
	f.w.setStatus("below_threshold", &Result{PClear: .4, POccupied: .6, PAssessable: .9})
	d := f.s.Display("01S1")
	scores := rowByLabel(t, d, "Scores")
	if scores.Section != "Before printing" || scores.Value != "Occupied 60% · view clarity 90%" {
		t.Fatalf("scores %+v", scores)
	}
	for _, tc := range []struct {
		state  string
		reason string
		pause  string
		value  string
		tone   string
		note   string
	}{
		{"disabled", "", "", "Off", "", ""},
		{"idle", "", "", "Not checked yet", "", "Checks after the first layer is printed"},
		{"checking", "", "", "Checking", "info", ""},
		{"passed", "", "", "Looks good", "calm", ""},
		{"warning", "", "", "Possible problem", "warn", "Printing continued; check the printer"},
		{"inconclusive", "", "", "Inconclusive", "", "Camera view too unclear to judge; printing continued"},
		{"blocked", "", "", "Unavailable", "warn", "Camera is disabled in configuration"},
		{"error", "timeout", "", "Not checked", "", "AI provider timed out; printing continued"},
		{"skipped", "printer_offline", "", "Not checked", "", "Printer offline; no pause sent"},
		{"skipped", "start_not_observed", "", "Not checked", "", "Proxy started after this print began"},
		{"pause", "", "pending", "Pause sent", "error", "Waiting for the printer to confirm"},
		{"pause", "", "confirmed", "Printer reports paused", "warn", "Check the first layer before resuming"},
		{"pause", "", "failed", "Pause command failed", "error", "Check the printer now"},
		{"pause", "", "unconfirmed", "Pause not confirmed", "error", "Check the printer now"},
		{"pause", "", "other", "Possible failure", "warn", ""},
	} {
		f.w.setLayerStatus(tc.state, tc.pause, nil)
		if tc.reason != "" {
			f.w.mu.Lock()
			f.w.setLayerStatusLocked(tc.state, tc.pause, nil, tc.reason)
			f.w.mu.Unlock()
		}
		got := rowByLabel(t, f.s.Display("01S1"), "First layer")
		if got.Section != "After layer 1" || got.Value != tc.value || got.Tone != tc.tone || got.Note != tc.note {
			t.Fatalf("layer %s/%s = %+v", tc.state, tc.pause, got)
		}
		if tc.state == "disabled" {
			continue
		}
		rule := layerRule(t, f.s.Display("01S1"))
		if rule.Value != "Pauses the print if tangle, detachment, or nozzle blob is over 70%" {
			t.Fatalf("layer rule %+v", rule)
		}
	}
	f.w.setLayerStatus("warning", "", &LayerResult{PAssessable: .9, PTangled: .6, PDetached: .1, PNozzleBlob: .2, PIncomplete: .3})
	d = f.s.Display("01S1")
	clarityRow := rowByLabel(t, d, "View clarity")
	if clarityRow.Value != "90% (needs 80%)" || clarityRow.Tone != "" {
		t.Fatalf("clarity %+v", clarityRow)
	}
	findings := rowByLabel(t, d, "Findings")
	if findings.Value != "Tangle 60% · detached 10% · nozzle blob 20% · missing material 30% (warning only)" || findings.Note != "" {
		t.Fatalf("findings %+v", findings)
	}
	f.w.setLayerStatus("inconclusive", "", &LayerResult{PAssessable: .5, PTangled: .1, PDetached: .1, PNozzleBlob: .1, PIncomplete: .1})
	d = f.s.Display("01S1")
	clarityRow = rowByLabel(t, d, "View clarity")
	if clarityRow.Value != "50% (needs 80%)" || clarityRow.Tone != "warn" {
		t.Fatalf("low clarity %+v", clarityRow)
	}
	if got := rowByLabel(t, d, "Findings").Note; got != "Low confidence: camera view too unclear" {
		t.Fatalf("inconclusive findings note %q", got)
	}
}

// TestDisplayHomingLabel pins that a second-pass status labels its Plate row
// "Plate (after homing)" while every other row stays the same.
func TestDisplayHomingLabel(t *testing.T) {
	f := newFixture(t, "RUNNING")
	f.w.setStatusWithReasonPass("clear", &Result{PClear: .9, POccupied: .1, PAssessable: .9}, "", true)
	d := f.s.Display("01S1")
	got := d.Panel.Rows[0]
	if got.Label != "Plate (after homing)" || got.Section != "Before printing" || got.Value != "Clear" || got.Tone != "calm" {
		t.Fatalf("homing plate row = %+v", got)
	}
	if rule := rowByLabel(t, d, "Rule"); rule.Section != "Before printing" || !strings.Contains(rule.Value, "Stops the print") {
		t.Fatalf("homing rule row = %+v", rule)
	}
	f.w.setStatusWithReasonPass("skipped", nil, "print_past_start", true)
	got = f.s.Display("01S1").Panel.Rows[0]
	if got.Label != "Plate (after homing)" || got.Value != "Not checked" || got.Note != "Printing had already started; printing continued" {
		t.Fatalf("homing skip row = %+v", got)
	}
}

func rowByLabel(t *testing.T, d *module.Display, label string) module.Row {
	t.Helper()
	for _, row := range d.Panel.Rows {
		if row.Label == label {
			return row
		}
	}
	t.Fatalf("missing row %q in %+v", label, d.Panel.Rows)
	return module.Row{}
}

// layerRule returns the Rule row of the After layer 1 section.
func layerRule(t *testing.T, d *module.Display) module.Row {
	t.Helper()
	for _, row := range d.Panel.Rows {
		if row.Label == "Rule" && row.Section == "After layer 1" {
			return row
		}
	}
	t.Fatalf("missing first-layer rule in %+v", d.Panel.Rows)
	return module.Row{}
}

func TestAdmissionNeedsConnectivityNotReportFreshness(t *testing.T) {
	// Stale stamps and a zero connection generation never block
	// admission: only Connected gates the check.
	for _, edit := range []func(*fixture){
		func(f *fixture) { f.commands.gen = 0 },
		func(f *fixture) { f.state.v.StateGen = 0 },
		func(f *fixture) { f.state.v.ObsGen = 0 },
		func(f *fixture) { f.clock.advance(16 * time.Second) },
		func(f *fixture) { f.state.v.ObsAt = time.Time{} },
	} {
		f := newFixture(t, "PREPARE")
		edit(f)
		f.hint("print_preparing")
		f.step()
		if f.client.calls != 1 || f.commands.count() != 1 {
			t.Fatal("fresh authorization inputs rejected")
		}
	}
	// A disconnect waits, then expires with printer_offline.
	f := newFixture(t, "PREPARE")
	f.commands.offline = map[string]bool{"01S1": true}
	f.hint("print_preparing")
	f.step()
	if f.client.calls != 0 || f.status() != "idle" {
		t.Fatalf("offline admitted: calls=%d state=%s", f.client.calls, f.status())
	}
	f.clock.advance(30 * time.Second)
	f.step()
	if f.status() != "skipped" || f.s.moduleState("01S1").(ModuleState).Reason != "printer_offline" {
		t.Fatalf("offline expiry: %+v", f.s.moduleState("01S1"))
	}
	// A reconnect during the wait admits without a new report.
	f = newFixture(t, "PREPARE")
	f.commands.offline = map[string]bool{"01S1": true}
	f.hint("print_preparing")
	f.step()
	f.commands.offline = nil
	f.step()
	if f.commands.count() != 1 {
		t.Fatal("reconnect did not admit the waiting hint")
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

// TestStopConfirmationRejectsReportProcessedAfterDeadline covers the
// post-read deadline check: the clock samples before the deadline, then
// advances past it between the first sample and the post-read sample
// while the state read already holds a same-job terminal report. The
// late state must not confirm.
func TestStopConfirmationRejectsReportProcessedAfterDeadline(t *testing.T) {
	f := newFixture(t, "RUNNING")
	f.hint("print_started")
	f.step()
	if f.w.pending == nil {
		t.Fatal("no pending stop after the check")
	}
	f.report("FAILED", nil)
	f.clock.advance(30*time.Second - 2*time.Millisecond)
	f.s.now = func() time.Time {
		t := f.clock.now()
		f.clock.advance(2 * time.Millisecond)
		return t
	}
	f.step()
	if f.status() != "stop_unconfirmed" || f.hasEvent("platecheck_stopped") {
		t.Fatalf("report processed after the deadline confirmed: %s", f.status())
	}
}

func TestSameJobTerminalConfirmsAndNewGenerationGetsOneCheck(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.hint("print_preparing")
	f.step()
	// A same-job terminal report confirms immediately: confirmation is
	// state-based, not counter-based.
	f.report("IDLE", nil)
	f.step()
	if f.status() != "stopped" || !f.hasEvent("platecheck_stopped") {
		t.Fatalf("terminal not confirmed: %s", f.status())
	}
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
		j.Generation++
		j.State = "PREPARE"
		j.Active = true
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
	for _, expected := range []string{"origin=platecheck", "operation=", "p_clear=0.1", "p_assessable=0.9", "frame_sequence=7", "job_generation=1", "error_code=command_failed", "stop unconfirmed"} {
		if !strings.Contains(logs.String(), expected) {
			t.Fatalf("missing %s: %s", expected, logs.String())
		}
	}
}

func TestLayerReportBeforeFollowupStopPreventsSecondDispatch(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.hint("print_preparing")
	f.step()
	// A layer-1 report arrives before the follow-up stop: the gate sees
	// print_past_start and the pending attempt goes unconfirmed instead
	// of sending a second stop to a printing job.
	f.report("RUNNING", new(1))
	f.step()
	if f.commands.count() != 1 || f.status() != "stop_unconfirmed" {
		t.Fatalf("followup sent: count=%d state=%s", f.commands.count(), f.status())
	}
}

func TestLayerOnlyDeltaBeforeEachGatePreventsStop(t *testing.T) {
	advanceLayer := func(f *fixture) {
		f.clock.advance(time.Millisecond)
		f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
			// A layer-only delta: no state change, just the reported
			// layer advancing to 1 inside the same job.
			v.LayerNum = new(1)
			v.LayerThisJob = true
		})
	}
	t.Run("during evaluation", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		f.client.hook = func() { advanceLayer(f) }
		f.hint("print_started")
		f.step()
		if f.commands.count() != 0 || f.client.calls != 1 || f.status() != "skipped" {
			t.Fatalf("stale verdict stopped: count=%d calls=%d state=%s", f.commands.count(), f.client.calls, f.status())
		}
		if got := f.s.moduleState("01S1").(ModuleState).Reason; got != "print_past_start" {
			t.Fatalf("reason = %q", got)
		}
	})
	t.Run("before dispatch", func(t *testing.T) {
		f := newFixture(t, "RUNNING")
		f.hint("print_started")
		reads := 0
		f.state.printHook = func() {
			reads++
			// step, check entry, light gate, and the post-capture
			// recheck all read first; the layer lands before the
			// post-evaluation gate that guards dispatch.
			if reads == 5 {
				advanceLayer(f)
			}
		}
		f.step()
		if reads < 5 || f.commands.count() != 0 {
			t.Fatalf("stale dispatch: reads=%d stops=%d", reads, f.commands.count())
		}
	})
	t.Run("PREPARE followup dispatch", func(t *testing.T) {
		f := newFixture(t, "PREPARE")
		f.hint("print_preparing")
		f.step()
		f.report("RUNNING", new(0))
		reads := 0
		f.state.printHook = func() {
			reads++
			// confirm's read lands first; the layer lands before
			// the follow-up dispatch gate.
			if reads == 2 {
				advanceLayer(f)
			}
		}
		f.step()
		if reads < 2 || f.commands.count() != 1 || f.status() != "stop_unconfirmed" {
			t.Fatalf("followup sent: reads=%d count=%d state=%s", reads, f.commands.count(), f.status())
		}
	})
}

func TestPreviousJobPositiveLayerWaitsAsUnknown(t *testing.T) {
	// A positive layer stamped with an older job generation belongs to an
	// earlier print: CurrentLayer reports unknown, so the candidate waits
	// instead of skipping with print_past_start.
	f := newFixture(t, "RUNNING")
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
		v.LayerNum = new(300)
		v.LayerThisJob = false
	})
	f.hint("print_started")
	f.step()
	if f.commands.count() != 0 || f.client.calls != 0 || f.status() != "idle" {
		t.Fatalf("stale positive layer acted: count=%d calls=%d state=%s", f.commands.count(), f.client.calls, f.status())
	}
	f.clock.advance(30 * time.Second)
	f.step()
	if f.status() != "skipped" || f.s.moduleState("01S1").(ModuleState).Reason != "layer_not_reported" {
		t.Fatalf("expiry: %+v", f.s.moduleState("01S1"))
	}
}

func TestPrepareLayerOneDeltaAtEveryGateSendsNoStop(t *testing.T) {
	// A layer-only delta reporting 1 before, during, and after evaluation,
	// and before the follow-up stop: no stop at any gate.
	t.Run("before admission", func(t *testing.T) {
		f := newFixture(t, "PREPARE")
		f.hint("print_preparing")
		f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
			v.LayerNum = new(1)
			v.LayerThisJob = true
		})
		f.step()
		if f.commands.count() != 0 || f.client.calls != 0 || f.status() != "skipped" {
			t.Fatalf("count=%d calls=%d state=%s", f.commands.count(), f.client.calls, f.status())
		}
		if got := f.s.moduleState("01S1").(ModuleState).Reason; got != "print_past_start" {
			t.Fatalf("reason = %q", got)
		}
	})
	t.Run("during capture", func(t *testing.T) {
		f := newFixture(t, "PREPARE")
		base := f.s.frames
		f.s.frames = frameFunc(func(ctx context.Context, s string, after time.Time) (Frame, error) {
			frame, err := base.Capture(ctx, s, after)
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
				v.LayerNum = new(1)
				v.LayerThisJob = true
			})
			return frame, err
		})
		f.hint("print_preparing")
		f.step()
		if f.commands.count() != 0 || f.status() != "skipped" {
			t.Fatalf("count=%d state=%s", f.commands.count(), f.status())
		}
	})
	t.Run("during evaluation", func(t *testing.T) {
		f := newFixture(t, "PREPARE")
		f.client.hook = func() {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
				v.LayerNum = new(1)
				v.LayerThisJob = true
			})
		}
		f.hint("print_preparing")
		f.step()
		if f.commands.count() != 0 || f.status() != "skipped" {
			t.Fatalf("count=%d state=%s", f.commands.count(), f.status())
		}
	})
	t.Run("before followup", func(t *testing.T) {
		f := newFixture(t, "PREPARE")
		f.hint("print_preparing")
		f.step()
		f.report("RUNNING", new(0))
		f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
			v.LayerNum = new(1)
			v.LayerThisJob = true
		})
		f.step()
		if f.commands.count() != 1 || f.status() != "stop_unconfirmed" {
			t.Fatalf("count=%d state=%s", f.commands.count(), f.status())
		}
	})
}

func TestDelayedOldPrintStartedCreatesNoCandidate(t *testing.T) {
	f := newFixture(t, "PREPARE")
	// Telemetry has moved to job 2, but the delivered print_started still
	// names job 1: it binds nothing and consumes nothing.
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Generation = 2 })
	f.s.ObserveActivity("01S1", activity.Entry{Kind: "print_started", Time: f.clock.now(), Observation: activity.Observation{JobGen: 1}})
	if f.w.candidate != nil || f.w.processed != 0 {
		t.Fatal("delayed hint queued or consumed")
	}
	f.step()
	if f.client.calls != 0 || f.commands.count() != 0 || f.status() != "idle" {
		t.Fatalf("delayed hint acted: calls=%d count=%d state=%s", f.client.calls, f.commands.count(), f.status())
	}
	// The current job keeps its own admission.
	f.hint("print_preparing")
	f.step()
	if f.client.calls != 1 || f.commands.count() != 1 {
		t.Fatal("current job lost its own admission")
	}
}

func TestCloseWithPendingStopRecordsUnconfirmed(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.hint("print_preparing")
	f.step()
	if f.w.pending == nil {
		t.Fatal("no pending stop to close over")
	}
	f.s.kPoll = time.Hour
	f.s.Start()
	f.s.Close()
	if f.status() != "stop_unconfirmed" || f.w.pending != nil || len(f.commands.stops) != 1 {
		t.Fatalf("shutdown: state=%s stops=%d", f.status(), len(f.commands.stops))
	}
	if !f.hasEvent("platecheck_stop_unconfirmed") {
		t.Fatal("missing platecheck_stop_unconfirmed")
	}
}

func TestStaleGenerationStopIsRejectedBeforeTheNewJob(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.commands.strict = true
	// A reconnect plus job change lands between the gate's generation
	// read and the send: the pool fake rejects the stale generation, so
	// no stop reaches the new job on the new connection.
	f.commands.race = func() {
		f.commands.reconnect()
		f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
			j.Generation = 2
			j.State = "RUNNING"
		})
	}
	f.hint("print_preparing")
	f.step()
	if len(f.commands.delivered) != 0 {
		t.Fatalf("stale stop delivered: %+v", f.commands.delivered)
	}
	if f.status() != "stop_failed" || !f.hasEvent("platecheck_stop_failed") {
		t.Fatalf("state=%s events=%+v", f.status(), f.activity.Recent("01S1"))
	}
	// The new job is untouched: confirmation sees the replacement and
	// closes the attempt as unconfirmed without another send.
	f.commands.race = nil
	f.step()
	if f.status() != "stop_unconfirmed" || len(f.commands.delivered) != 0 {
		t.Fatalf("state=%s delivered=%+v", f.status(), f.commands.delivered)
	}
}

func TestCommandGateSendsTheAuthorizedGeneration(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.commands.strict = true
	f.hint("print_preparing")
	f.step()
	if len(f.commands.delivered) != 1 || f.commands.delivered[0] != 1 || len(f.commands.lights) != 0 {
		t.Fatalf("stops=%+v delivered=%+v lights=%+v", f.commands.stops, f.commands.delivered, f.commands.lights)
	}
	// The light command uses the same gate: the generation read before
	// authorization is the generation sent.
	f = newFixture(t, "PREPARE")
	f.state.v.ChamberLight = "off"
	f.hint("print_preparing")
	f.step()
	if len(f.commands.lights) != 1 || f.commands.lights[0] != 1 {
		t.Fatalf("lights=%+v", f.commands.lights)
	}
}

// reportStages sets the SessionView stage history the way the printer
// reporting stg_cur values would: StagesThisJob holds the distinct stages
// reported during the current job, in first-seen order.
func reportStages(t *testing.T, f *fixture, stages ...int) {
	t.Helper()
	f.state.edit(func(_ *telemetry.JobView, v *telemetry.SessionView) {
		v.StagesThisJob = append([]int(nil), stages...)
	})
}

// pass1Clear runs pass 1 at PREPARE with a clear plate: exactly one check,
// one passed line, and the generation consumed, ready for pass 2.
func pass1Clear(t *testing.T, f *fixture) {
	t.Helper()
	f.client.result = Result{PClear: .9, POccupied: .1, PAssessable: .9, Model: "clef"}
	f.hint("print_preparing")
	f.step()
	if f.client.calls != 1 {
		t.Fatalf("pass 1 Evaluate calls = %d, want 1", f.client.calls)
	}
	if f.w.processed != 1 {
		t.Fatalf("processed = %d, want 1 after pass 1", f.w.processed)
	}
}

// homingEvents returns the plate-check activity messages in record order.
func homingEvents(f *fixture) []string {
	var out []string
	for _, e := range f.activity.Recent("01S1") {
		if e.Kind == "platecheck_passed" || e.Kind == "platecheck_skipped" || e.Kind == "platecheck_failed" {
			out = append([]string{e.Message}, out...)
		}
	}
	return out
}

// TestHomingPassRunsOnceAfterToolheadHoming pins pass 2: pass 1 checks at
// PREPARE, then a stage 14 reported at RUNNING layer 0 runs exactly one
// more check with the after-homing text.
func TestHomingPassRunsOnceAfterToolheadHoming(t *testing.T) {
	f := newFixture(t, "PREPARE")
	pass1Clear(t, f)
	f.report("RUNNING", new(0))
	reportStages(t, f, 14)
	f.step()
	if f.client.calls != 2 {
		t.Fatalf("Evaluate calls = %d, want 2 (one per pass)", f.client.calls)
	}
	if f.w.homingProcessed != 1 {
		t.Fatalf("homingProcessed = %d, want 1 after pass 2", f.w.homingProcessed)
	}
	got := homingEvents(f)
	want := []string{
		"Plate check passed, plate clear",
		"Plate check after homing passed, plate clear",
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("activity = %q, want %q", got, want)
	}
	if v := f.s.moduleState("01S1").(ModuleState); !v.Homing {
		t.Fatalf("Homing = false, want the pass 2 status marked: %+v", v)
	}
	// A later step runs nothing more for the generation.
	f.step()
	if f.client.calls != 2 {
		t.Fatalf("Evaluate calls = %d, want still 2", f.client.calls)
	}
}

// TestHomingTriggerStages pins which stages trigger pass 2: 13 and 14 do,
// while bed leveling (1) alone never does.
func TestHomingTriggerStages(t *testing.T) {
	for _, tc := range []struct {
		stage int
		want  int
	}{
		{13, 2},
		{14, 2},
		{1, 1},
		{2, 1},
		{77, 1},
		{255, 1},
	} {
		t.Run(fmt.Sprintf("stage_%d", tc.stage), func(t *testing.T) {
			f := newFixture(t, "PREPARE")
			pass1Clear(t, f)
			f.report("RUNNING", new(0))
			reportStages(t, f, tc.stage)
			f.step()
			if f.client.calls != tc.want {
				t.Fatalf("stage %d: Evaluate calls = %d, want %d", tc.stage, f.client.calls, tc.want)
			}
		})
	}
}

// TestHomingHistorySurvivesTheStageMovingOn pins pass 2 against the stage
// history: stage 14 reported, then replaced by stage 1 before RUNNING
// layer 0 arrives, still runs pass 2 once the print is ready.
func TestHomingHistorySurvivesTheStageMovingOn(t *testing.T) {
	f := newFixture(t, "PREPARE")
	pass1Clear(t, f)
	// Homing reports while the print is still in PREPARE: pass 2 is due
	// but cannot run yet.
	reportStages(t, f, 14)
	f.step()
	if f.client.calls != 1 || f.w.homingProcessed != 0 {
		t.Fatalf("calls = %d homingProcessed = %d, want the pass held without a check", f.client.calls, f.w.homingProcessed)
	}
	// The printer replaces the stage with bed leveling before the worker
	// steps at RUNNING: the history still holds 14.
	f.report("RUNNING", new(0))
	reportStages(t, f, 14, 1)
	f.step()
	if f.client.calls != 2 {
		t.Fatalf("Evaluate calls = %d, want 2 after the due pass ran", f.client.calls)
	}
	got := homingEvents(f)
	if len(got) != 2 || got[1] != "Plate check after homing passed, plate clear" {
		t.Fatalf("activity = %q, want the after-homing line second", got)
	}
}

// TestHomingStageFromThePreviousJobNeverTriggers pins that a stage 14
// reported in the previous job does not trigger pass 2 for the new job:
// the history holds only the current job's stages.
func TestHomingStageFromThePreviousJobNeverTriggers(t *testing.T) {
	f := newFixture(t, "PREPARE")
	pass1Clear(t, f)
	f.report("RUNNING", new(0))
	// The new job has reported no stage yet: an empty history means the
	// previous job's homing stage never triggers.
	reportStages(t, f)
	f.step()
	if f.client.calls != 1 || f.w.homingProcessed != 0 {
		t.Fatalf("calls = %d homingProcessed = %d, want no pass 2 without this job's stage", f.client.calls, f.w.homingProcessed)
	}
	// The job's own report of the same stage runs pass 2 normally.
	reportStages(t, f, 14)
	f.step()
	if f.client.calls != 2 {
		t.Fatalf("Evaluate calls = %d, want 2 after this job's own stage", f.client.calls)
	}
}

// TestHomingPassSkippedOnceLayerOnePrints pins the pass-2 layer race: a
// due pass that meets layer 1 before it can run records a
// print_past_start skip with the after-homing check name.
func TestHomingPassSkippedOnceLayerOnePrints(t *testing.T) {
	f := newFixture(t, "PREPARE")
	pass1Clear(t, f)
	reportStages(t, f, 14)
	f.step()
	if f.client.calls != 1 || f.w.homingProcessed != 0 {
		t.Fatalf("calls = %d homingProcessed = %d, want the pass due without a check at PREPARE", f.client.calls, f.w.homingProcessed)
	}
	f.report("RUNNING", new(1))
	f.step()
	if f.client.calls != 1 {
		t.Fatalf("Evaluate calls = %d, want 1 (the due pass skipped)", f.client.calls)
	}
	if f.w.homingProcessed != 1 {
		t.Fatalf("homingProcessed = %d, want the due pass consumed", f.w.homingProcessed)
	}
	got := homingEvents(f)
	want := "Plate check after homing skipped, printing had already started, continuing print"
	if len(got) != 2 || got[1] != want {
		t.Fatalf("activity = %q, want %q second", got, want)
	}
	v := f.s.moduleState("01S1").(ModuleState)
	if v.State != "skipped" || v.Reason != "print_past_start" || !v.Homing {
		t.Fatalf("status = %+v, want the after-homing skip", v)
	}
}

// TestHomingNeverRunsAfterAPass1Stop pins that a pass 1 stop blocks pass 2:
// startupStopped owns the generation, so the homing stage runs nothing.
func TestHomingNeverRunsAfterAPass1Stop(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.hint("print_preparing")
	f.step()
	if f.commands.count() != 1 || f.w.startupStopped != 1 {
		t.Fatalf("stops = %d startupStopped = %d, want the pass 1 stop", f.commands.count(), f.w.startupStopped)
	}
	f.report("RUNNING", new(0))
	reportStages(t, f, 14)
	f.step()
	if f.client.calls != 1 || f.w.homingProcessed != 0 {
		t.Fatalf("calls = %d homingProcessed = %d, want no pass 2", f.client.calls, f.w.homingProcessed)
	}
}

// TestHomingNeedsStartObserved pins that a mid-print attach runs no pass 2:
// processed is set without a check, but StartObserved is false.
func TestHomingNeedsStartObserved(t *testing.T) {
	f := newFixture(t, "PREPARE")
	f.state.edit(func(j *telemetry.JobView, _ *telemetry.SessionView) { j.StartObserved = false })
	f.s.ObserveActivity("01S1", activity.Entry{Kind: "state_initial", Time: f.clock.now(), Observation: activity.Observation{JobGen: 1}})
	f.report("RUNNING", new(0))
	reportStages(t, f, 14)
	f.step()
	if f.client.calls != 0 || f.w.homingProcessed != 0 {
		t.Fatalf("calls = %d homingProcessed = %d, want no pass 2 without StartObserved", f.client.calls, f.w.homingProcessed)
	}
	if got := len(f.activity.Recent("01S1")); got != 0 {
		t.Fatalf("activity entries = %d, want none", got)
	}
}
