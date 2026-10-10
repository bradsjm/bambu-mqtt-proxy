package platecheck

import (
	"context"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/telemetry"
)

func TestFirstLayerPauseConfirmationRequiresSameJobPause(t *testing.T) {
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

func TestFirstLayerConfirmationIsStateBased(t *testing.T) {
	// Stale stamps and generation counters never block confirmation:
	// any same-job PAUSE/PAUSED inside the deadline confirms, including
	// an HMS-driven pause the proxy did not cause.
	f := newLayerFixture(t)
	f.hint("first_layer_complete")
	f.step()
	f.report("PAUSE", new(2))
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
		v.ObsAt = time.Time{}
		v.ObsGen++
	})
	f.step()
	if layerStatus(f).Pause != "confirmed" || f.w.layerPending != nil || !f.hasEvent("platecheck_layer1_pause_confirmed") {
		t.Fatalf("stale stamps blocked confirmation: %+v", layerStatus(f))
	}
	// A terminal report inside the deadline is unconfirmed, not confirmed.
	f = newLayerFixture(t)
	f.hint("first_layer_complete")
	f.step()
	f.report("FINISH", new(2))
	f.step()
	if layerStatus(f).Pause != "unconfirmed" || f.w.layerPending != nil {
		t.Fatalf("terminal confirmed: %+v", layerStatus(f))
	}
}

func TestFirstLayerPauseIdentityInvalidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*fixture)
	}{
		{"job replacement", func(f *fixture) { f.state.j.Generation++ }},
		{"terminal", func(f *fixture) { f.report("FINISH", new(2)) }},
		{"ended idle", func(f *fixture) { f.report("IDLE", nil) }},
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
	// A reconnect or a pause/resume cycle that returns to RUNNING keeps
	// the wait open: only a same-job terminal report or a job
	// replacement ends it early.
	for _, tc := range []struct {
		name   string
		change func(*fixture)
	}{
		{"reconnect", func(f *fixture) { f.commands.reconnect() }},
		{"offline", func(f *fixture) { f.commands.offline = map[string]bool{"01S1": true} }},
		{"session", func(f *fixture) { f.state.v.SessionGen++ }},
		{"session epoch", func(f *fixture) { f.state.v.Epoch++ }},
		{"pause resume before confirmation", func(f *fixture) { f.report("PAUSE", new(2)); f.report("RUNNING", new(2)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLayerFixture(t)
			f.hint("first_layer_complete")
			f.step()
			tc.change(f)
			f.step()
			if layerStatus(f).Pause != "pending" || f.w.layerPending == nil || len(f.commands.pauses) != 1 {
				t.Fatalf("wait closed early: %+v", layerStatus(f))
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

// TestFirstLayerConfirmationRejectsReportProcessedAfterDeadline covers
// the post-read deadline check: the clock samples before the deadline,
// then advances past it between the first sample and the post-read
// sample while the state read already holds a same-job PAUSE report.
// The late pause must not confirm.
func TestFirstLayerConfirmationRejectsReportProcessedAfterDeadline(t *testing.T) {
	f := newLayerFixture(t)
	f.hint("first_layer_complete")
	f.step()
	if f.w.layerPending == nil {
		t.Fatal("no pending pause after the check")
	}
	f.report("PAUSE", new(2))
	f.clock.advance(30*time.Second - 2*time.Millisecond)
	f.s.now = func() time.Time {
		t := f.clock.now()
		f.clock.advance(2 * time.Millisecond)
		return t
	}
	f.step()
	if layerStatus(f).Pause != "unconfirmed" || f.hasEvent("platecheck_layer1_pause_confirmed") {
		t.Fatalf("report processed after the deadline confirmed: %+v", layerStatus(f))
	}
}

func TestFirstLayerConfirmationWaitSurvivesRunningReports(t *testing.T) {
	f := newLayerFixture(t)
	f.hint("first_layer_complete")
	f.step()
	// Intermediate RUNNING reports keep the wait open; the later PAUSE
	// still confirms inside the deadline.
	f.report("RUNNING", new(2))
	f.step()
	if layerStatus(f).Pause != "pending" {
		t.Fatal("RUNNING report ended valid confirmation window")
	}
	f.report("PAUSED", new(2))
	f.step()
	if layerStatus(f).Pause != "confirmed" {
		t.Fatal("later pause did not confirm")
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
