package platecheck

import (
	"context"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/telemetry"
)

func TestFirstLayerPauseConfirmationRequiresRealFreshPauseBoundary(t *testing.T) {
	for _, state := range []string{"PAUSE", "PAUSED"} {
		t.Run(state, func(t *testing.T) {
			f := newLayerFixture(t)
			f.hint("first_layer_complete")
			f.step()
			if layerStatus(f).Pause != "pending" || f.hasEvent("platecheck_layer1_pause_confirmed") {
				t.Fatal("publish alone confirmed")
			}
			for range 3 {
				f.report("RUNNING", new(2))
				f.step()
				if layerStatus(f).Pause != "pending" || len(f.commands.pauses) != 1 {
					t.Fatal("RUNNING aborted wait or replayed")
				}
			}
			f.report(state, new(2))
			f.step()
			if layerStatus(f).Pause != "confirmed" || f.w.layerPending != nil || !f.hasEvent("platecheck_layer1_pause_confirmed") {
				t.Fatalf("pause not confirmed %+v", layerStatus(f))
			}
			for _, e := range f.activity.Recent("01S1") {
				if e.Kind == "platecheck_layer1_pause_sent" && e.Severity != activity.Error {
					t.Fatal("sent severity")
				}
				if e.Kind == "platecheck_layer1_pause_confirmed" && e.Severity != activity.Warning {
					t.Fatal("confirmed severity")
				}
			}
			f.report("RUNNING", new(2))
			f.hint("first_layer_complete")
			f.step()
			if len(f.commands.pauses) != 1 || f.client.layerCalls != 1 {
				t.Fatal("resume replayed phase")
			}
		})
	}
}

func TestFirstLayerConfirmationRejectsInvalidObservationAndExpires(t *testing.T) {
	for _, tc := range []struct {
		name       string
		invalidate func(*fixture, *pendingPause)
	}{
		{"old counter", func(f *fixture, p *pendingPause) { f.state.v.Obs = p.obs }},
		{"old job timestamp", func(f *fixture, p *pendingPause) { f.state.j.ObsAt = p.dispatched }},
		{"old session timestamp", func(f *fixture, p *pendingPause) { f.state.v.ObsAt = p.dispatched }},
		{"stale job", func(f *fixture, p *pendingPause) { f.state.j.ObsAt = f.clock.now().Add(-16 * time.Second) }},
		{"zero job", func(f *fixture, p *pendingPause) { f.state.j.ObsAt = time.Time{} }},
		{"zero session", func(f *fixture, p *pendingPause) { f.state.v.ObsAt = time.Time{} }},
		{"stale session", func(f *fixture, p *pendingPause) { f.state.v.ObsAt = f.clock.now().Add(-16 * time.Second) }},
		{"future job", func(f *fixture, p *pendingPause) { f.state.j.ObsAt = f.clock.now().Add(time.Second) }},
		{"future session", func(f *fixture, p *pendingPause) { f.state.v.ObsAt = f.clock.now().Add(time.Second) }},
		{"old job observation connection", func(f *fixture, p *pendingPause) { f.state.j.ObsGen++ }},
		{"old session observation connection", func(f *fixture, p *pendingPause) { f.state.v.ObsGen++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLayerFixture(t)
			f.hint("first_layer_complete")
			f.step()
			p := f.w.layerPending
			f.report("PAUSE", new(2))
			tc.invalidate(f, p)
			f.step()
			if layerStatus(f).Pause == "confirmed" {
				t.Fatal("invalid observation confirmed")
			}
			f.clock.advance(31 * time.Second)
			f.step()
			if layerStatus(f).Pause != "unconfirmed" || !f.hasEvent("platecheck_layer1_pause_unconfirmed") {
				t.Fatal("invalid confirmation did not expire")
			}
		})
	}
}

func TestFirstLayerPauseIdentityInvalidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*fixture)
	}{
		{"reconnect", func(f *fixture) { f.commands.reconnect() }},
		{"offline", func(f *fixture) { f.commands.offline = map[string]bool{"01S1": true} }},
		{"job replacement", func(f *fixture) { f.state.j.Generation++ }},
		{"revision", func(f *fixture) { f.state.j.Revision++ }},
		{"session", func(f *fixture) { f.state.v.SessionGen++ }},
		{"running epoch", func(f *fixture) { f.state.j.RunningEpoch++ }},
		{"session epoch", func(f *fixture) { f.state.v.Epoch++ }},
		{"pause resume before observation", func(f *fixture) { f.report("PAUSE", new(2)); f.report("RUNNING", new(2)) }},
		{"terminal", func(f *fixture) { f.report("FINISH", new(2)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLayerFixture(t)
			f.hint("first_layer_complete")
			f.step()
			tc.change(f)
			f.step()
			if layerStatus(f).Pause != "unconfirmed" || f.w.layerPending != nil || len(f.commands.pauses) != 1 {
				t.Fatalf("invalidation %+v", layerStatus(f))
			}
		})
	}
}

func TestFirstLayerPauseDeadlineNoReplay(t *testing.T) {
	for _, tc := range []struct {
		name         string
		reportAfter  time.Duration
		processAfter time.Duration
		want         string
	}{
		{"before deadline", 29 * time.Second, 0, "confirmed"},
		{"at deadline", 30 * time.Second, 0, "unconfirmed"},
		{"after deadline", 31 * time.Second, 0, "unconfirmed"},
		{"processed too late", 29 * time.Second, 2 * time.Second, "unconfirmed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLayerFixture(t)
			f.hint("first_layer_complete")
			f.step()
			// report advances the clock by one millisecond, so compensate for the exact boundary.
			f.clock.advance(tc.reportAfter - time.Millisecond)
			f.report("PAUSE", new(2))
			f.clock.advance(tc.processAfter)
			f.step()
			if layerStatus(f).Pause != tc.want {
				t.Fatalf("deadline %+v", layerStatus(f))
			}
			f.report("RUNNING", new(2))
			f.hint("first_layer_complete")
			f.step()
			if len(f.commands.pauses) != 1 || f.client.layerCalls != 1 {
				t.Fatal("deadline/resume replay")
			}
		})
	}
	f := newLayerFixture(t)
	f.hint("first_layer_complete")
	f.step()
	f.clock.advance(30 * time.Second)
	f.report("RUNNING", new(2))
	f.step()
	if layerStatus(f).Pause != "unconfirmed" {
		t.Fatal("running wait did not expire")
	}
	f.report("PAUSE", new(2))
	f.step()
	if layerStatus(f).Pause != "unconfirmed" {
		t.Fatal("late confirmation accepted")
	}
}

func TestFirstLayerConfirmationCanRecoverBeforeDeadline(t *testing.T) {
	f := newLayerFixture(t)
	f.hint("first_layer_complete")
	f.step()
	p := f.w.layerPending
	f.report("PAUSED", new(2))
	f.state.v.Obs = p.obs
	f.step()
	if layerStatus(f).Pause != "pending" {
		t.Fatal("old observation ended valid confirmation window")
	}
	f.report("PAUSED", new(2))
	f.step()
	if layerStatus(f).Pause != "confirmed" {
		t.Fatal("fresh observation did not confirm")
	}
}

type layerEvaluateFunc func(context.Context, [][]byte, string) (LayerResult, error)

func (f layerEvaluateFunc) Probe(context.Context) error { return nil }
func (f layerEvaluateFunc) Evaluate(context.Context, []byte, string) (Result, error) {
	return Result{}, &safeError{code: "canceled"}
}
func (f layerEvaluateFunc) EvaluateFirstLayer(ctx context.Context, frames [][]byte, model string) (LayerResult, error) {
	return f(ctx, frames, model)
}

func TestFirstLayerCloseCancelsCaptureAndEvaluation(t *testing.T) {
	for _, stage := range []string{"capture", "evaluation"} {
		t.Run(stage, func(t *testing.T) {
			f := newLayerFixture(t)
			entered := make(chan struct{})
			if stage == "capture" {
				f.s.frames = frameFunc(func(ctx context.Context, _ string, _ time.Time) (Frame, error) {
					close(entered)
					<-ctx.Done()
					return Frame{}, ctx.Err()
				})
			} else {
				f.s.client = layerEvaluateFunc(func(ctx context.Context, _ [][]byte, _ string) (LayerResult, error) {
					close(entered)
					<-ctx.Done()
					return LayerResult{}, ctx.Err()
				})
			}
			f.s.kPoll = time.Hour
			f.hint("first_layer_complete")
			f.s.Start()
			<-entered
			f.s.Close()
			if len(f.commands.pauses) != 0 || layerStatus(f).State != "error" {
				t.Fatal("close failed to cancel network phase")
			}
		})
	}
}

func TestFirstLayerSecondJobGetsIndependentCheck(t *testing.T) {
	f := newLayerFixture(t)
	f.client.layerResult = LayerResult{PAssessable: .9}
	f.hint("first_layer_complete")
	f.step()
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
		j.Generation++
		j.Revision++
		v.SessionGen++
		v.Epoch++
	})
	f.hint("first_layer_complete")
	f.step()
	if f.client.layerCalls != 2 || layerStatus(f).State != "passed" {
		t.Fatal("new job reused consumption marker")
	}
}

func TestFirstLayerCloseEndsPendingConfirmation(t *testing.T) {
	f := newLayerFixture(t)
	f.hint("first_layer_complete")
	f.step()
	f.s.kPoll = time.Hour
	f.s.Start()
	f.s.Close()
	if layerStatus(f).Pause != "unconfirmed" || f.w.layerPending != nil || len(f.commands.pauses) != 1 {
		t.Fatal("shutdown left an unbounded confirmation or replayed command")
	}
}
