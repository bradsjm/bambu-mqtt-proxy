package platecheck

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

func newLayerFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t, "RUNNING")
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { v.LayerNum = new(2) })
	f.client.layerResult = LayerResult{PAssessable: .9, PTangled: .9}
	data := testJPEG(t)
	seq := uint64(0)
	f.s.frames = frameFunc(func(_ context.Context, _ string, after time.Time) (Frame, error) {
		f.clock.advance(after.Sub(f.clock.now()) + time.Millisecond)
		seq++
		return Frame{JPEG: data, Seq: seq, Captured: f.clock.now()}, nil
	})
	return f
}

func layerStatus(f *fixture) FirstLayerState {
	return f.s.moduleState("01S1").(ModuleState).FirstLayer
}

func TestFirstLayerIndependentConsumptionAndImmutableStatus(t *testing.T) {
	f := newLayerFixture(t)
	f.hint("state_initial")
	f.hint("first_layer_complete")
	f.hint("first_layer_complete")
	f.step()
	if f.client.layerCalls != 1 || len(f.commands.pauses) != 1 || layerStatus(f).Pause != "pending" || f.client.calls != 0 || !f.hasEvent("platecheck_layer1_pause_sent") {
		t.Fatalf("independent layer state %+v", layerStatus(f))
	}
	f.hint("first_layer_complete")
	f.step()
	if f.client.layerCalls != 1 || len(f.commands.pauses) != 1 {
		t.Fatal("duplicate replay")
	}
	v := f.s.moduleState("01S1").(ModuleState)
	*v.FirstLayer.PTangled = 0
	*v.FirstLayer.PAssessable = 0
	if *layerStatus(f).PTangled != .9 || *layerStatus(f).PAssessable != .9 {
		t.Fatal("nested status mutation escaped")
	}
	f.w.setStatus("clear", &Result{PClear: .9, POccupied: .1, PAssessable: .9})
	if layerStatus(f).Pause != "pending" {
		t.Fatal("startup update erased first-layer phase")
	}
	f.w.setLayerStatus("passed", "", &LayerResult{PAssessable: .9, PTangled: .1, PDetached: .2, PNozzleBlob: .3, PIncomplete: .4})
	v = f.s.moduleState("01S1").(ModuleState)
	for _, score := range []*float64{v.FirstLayer.PAssessable, v.FirstLayer.PTangled, v.FirstLayer.PDetached, v.FirstLayer.PNozzleBlob, v.FirstLayer.PIncomplete} {
		*score = 1
	}
	fresh := layerStatus(f)
	if *fresh.PAssessable != .9 || *fresh.PTangled != .1 || *fresh.PDetached != .2 || *fresh.PNozzleBlob != .3 || *fresh.PIncomplete != .4 || f.status() != "clear" {
		t.Fatal("immutable status or startup preservation failed")
	}
	if f.s.Display("01S1") == nil || len(f.s.Display("01S1").Panel.Rows) < 5 {
		t.Fatal("missing first-layer panel rows")
	}
}

func TestFirstLayerDisabledAndStartupStopSuppression(t *testing.T) {
	f := newLayerFixture(t)
	f.s.settings.FirstLayer.Enabled = new(false)
	f.hint("first_layer_complete")
	f.step()
	if f.client.layerCalls != 0 || len(f.commands.pauses) != 0 {
		t.Fatal("disabled phase evaluated")
	}
	f = newFixture(t, "PREPARE")
	f.commands.stopErr = errors.New("send failed")
	f.hint("print_preparing")
	f.step()
	if f.w.startupStopped != 1 {
		t.Fatal("failed startup dispatch did not retain job marker")
	}
	f.report("RUNNING", new(2))
	f.hint("first_layer_complete")
	f.w.pending = nil
	f.step()
	if f.client.layerCalls != 0 || len(f.commands.pauses) != 0 || layerStatus(f).State != "skipped" {
		t.Fatal("startup stop attempt did not suppress later phase")
	}
}

func TestFirstLayerFreshAdmissionAndBoundedHint(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*fixture)
	}{
		{"stale job", func(f *fixture) { f.state.j.ObsAt = f.clock.now().Add(-16 * time.Second) }},
		{"zero job", func(f *fixture) { f.state.j.ObsAt = time.Time{} }},
		{"zero session", func(f *fixture) { f.state.v.ObsAt = time.Time{} }},
		{"stale session", func(f *fixture) { f.state.v.ObsAt = f.clock.now().Add(-16 * time.Second) }},
		{"future job", func(f *fixture) { f.state.j.ObsAt = f.clock.now().Add(time.Second) }},
		{"future session", func(f *fixture) { f.state.v.ObsAt = f.clock.now().Add(time.Second) }},
		{"old job connection", func(f *fixture) { f.state.j.ObsGen = 2 }},
		{"old session connection", func(f *fixture) { f.state.v.ObsGen = 2 }},
		{"old session state connection", func(f *fixture) { f.state.v.StateGen = 2 }},
		{"offline", func(f *fixture) { f.commands.offline = map[string]bool{"01S1": true} }},
		{"layer one", func(f *fixture) { f.state.v.LayerNum = new(1) }},
		{"unknown layer", func(f *fixture) { f.state.v.LayerNum = nil }},
		{"paused", func(f *fixture) { f.report("PAUSE", new(2)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLayerFixture(t)
			tc.change(f)
			f.hint("first_layer_complete")
			f.step()
			if f.client.layerCalls != 0 || len(f.commands.pauses) != 0 {
				t.Fatal("invalid admission evaluated")
			}
		})
	}
	f := newLayerFixture(t)
	f.hint("first_layer_complete")
	f.clock.advance(26 * time.Second)
	f.report("RUNNING", new(2))
	f.step()
	if f.client.layerCalls != 0 {
		t.Fatal("queued hint lifetime extended")
	}
}

// firstLayerEvidence copies the current session's provenance exactly the way
// firstlayer stamps a real first_layer_complete event at the moment of
// sampling. Valid hint entries use it; deliberately invalid events must be
// hand-built so the rejection paths stay reachable.
func firstLayerEvidence(f *fixture) activity.Observation {
	j, _ := f.state.Job("01S1")
	v, _ := f.state.Session("01S1")
	return activity.Observation{SessionGen: v.SessionGen, Epoch: v.Epoch, StateGen: v.StateGen, ObsGen: v.ObsGen, ObsAt: v.ObsAt, JobGen: j.Generation, JobRevision: j.Revision, RunningEpoch: j.RunningEpoch}
}

// TestFirstLayerEventProvenanceValidation pins that the first-layer event
// carries originating session evidence: missing provenance is never repaired
// from current telemetry, and stale, future, foreign, or reconnected evidence
// is rejected before anything is queued, consumed, captured, uploaded, or
// paused.
func TestFirstLayerEventProvenanceValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		absent  bool
		prepare func(*fixture)
		mutate  func(*fixture, *activity.Observation)
	}{
		{"missing evidence", true, nil, nil},
		{"zero session generation", false, nil, func(_ *fixture, o *activity.Observation) { o.SessionGen = 0 }},
		{"zero origin", false, nil, func(_ *fixture, o *activity.Observation) { o.ObsAt = time.Time{} }},
		{"stale origin", false, nil, func(f *fixture, o *activity.Observation) { o.ObsAt = f.clock.now().Add(-16 * time.Second) }},
		{"future origin", false, nil, func(f *fixture, o *activity.Observation) { o.ObsAt = f.clock.now().Add(time.Second) }},
		{"foreign session", false, nil, func(_ *fixture, o *activity.Observation) { o.SessionGen = 99 }},
		{"foreign epoch", false, nil, func(_ *fixture, o *activity.Observation) { o.Epoch = 99 }},
		{"old connection observation", false, nil, func(_ *fixture, o *activity.Observation) { o.ObsGen = 99 }},
		{"old connection state", false, nil, func(_ *fixture, o *activity.Observation) { o.StateGen = 99 }},
		{"missing job generation", false, nil, func(_ *fixture, o *activity.Observation) { o.JobGen = 0 }},
		{"foreign job generation", false, nil, func(_ *fixture, o *activity.Observation) { o.JobGen = 99 }},
		{"foreign job revision", false, nil, func(_ *fixture, o *activity.Observation) { o.JobRevision = 99 }},
		{"foreign running epoch", false, nil, func(_ *fixture, o *activity.Observation) { o.RunningEpoch = 99 }},
		{"reconnect before delivery", false, func(f *fixture) { f.commands.reconnect() }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLayerFixture(t)
			captures := 0
			base := f.s.frames
			f.s.frames = frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
				captures++
				return base.Capture(ctx, serial, after)
			})
			var observation activity.Observation
			if !tc.absent {
				observation = firstLayerEvidence(f)
				if tc.mutate != nil {
					tc.mutate(f, &observation)
				}
			}
			if tc.prepare != nil {
				tc.prepare(f)
			}
			f.s.ObserveActivity("01S1", activity.Entry{Kind: "first_layer_complete", Time: f.clock.now(), Observation: observation})
			f.step()
			if f.w.layerHint != nil || f.w.layerProcessed != 0 || captures != 0 || f.client.layerCalls != 0 || len(f.commands.pauses) != 0 {
				t.Fatal("invalid provenance queued, consumed, captured, uploaded, or paused")
			}
			if layerStatus(f).State != "idle" || f.hasEvent("platecheck_layer1_skipped") || len(f.commands.lights) != 0 {
				t.Fatal("invalid provenance invented lifecycle")
			}
		})
	}
}

// TestFirstLayerStaleEventCannotBindSwitchedJob reproduces the reviewer race:
// session A is sampled at layer 2, telemetry switches to job/session B on the
// same upstream connection before the A event is recorded, and the delivered
// A event must bind and act on nothing.
func TestFirstLayerStaleEventCannotBindSwitchedJob(t *testing.T) {
	f := newLayerFixture(t)
	captures := 0
	base := f.s.frames
	f.s.frames = frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		captures++
		return base.Capture(ctx, serial, after)
	})
	// firstlayer samples session A evidence while job 1 still runs.
	observation := firstLayerEvidence(f)
	// Telemetry then switches to job/session B, already RUNNING at layer 2,
	// on the same upstream connection.
	f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) {
		j.Generation = 2
		j.Revision = 2
		j.ObsAt = f.clock.now()
		v.SessionGen = 2
		v.Epoch++
		v.ObsAt = f.clock.now()
	})
	f.s.ObserveActivity("01S1", activity.Entry{Kind: "first_layer_complete", Time: f.clock.now(), Observation: observation})
	f.step()
	if f.w.layerHint != nil || f.w.layerProcessed != 0 || captures != 0 || f.client.layerCalls != 0 || len(f.commands.pauses) != 0 || len(f.commands.lights) != 0 {
		t.Fatal("stale event bound, consumed, captured, uploaded, or paused the replaced session")
	}
	if layerStatus(f).State != "idle" || f.hasEvent("platecheck_layer1_skipped") {
		t.Fatal("stale event invented first-layer lifecycle")
	}
	// The current session keeps its own admission; the rejected event consumed nothing.
	f.hint("first_layer_complete")
	f.step()
	if f.client.layerCalls != 1 || len(f.commands.pauses) != 1 || layerStatus(f).Pause != "pending" {
		t.Fatal("current session lost its own admission")
	}
}

// TestFirstLayerDuplicateCannotRenewOrigin pins that only the first valid
// queued source event for a job counts: refreshed telemetry carries newer
// evidence, but the queued candidate keeps its originating time and the work
// budget still expires from it.
func TestFirstLayerDuplicateCannotRenewOrigin(t *testing.T) {
	f := newLayerFixture(t)
	f.hint("first_layer_complete")
	if f.w.layerHint == nil {
		t.Fatal("valid source event rejected")
	}
	original := f.w.layerHint.admitted
	f.report("RUNNING", new(2))
	f.hint("first_layer_complete")
	if !f.w.layerHint.admitted.Equal(original) {
		t.Fatal("duplicate hint renewed the originating evidence time")
	}
	f.clock.advance(26 * time.Second)
	f.report("RUNNING", new(2))
	f.step()
	if f.client.layerCalls != 0 || layerStatus(f).State != "skipped" || f.w.layerProcessed != 1 {
		t.Fatal("duplicate renewed the expired origin budget")
	}
}

func TestFirstLayerChangesAcrossNetworkBoundariesFailOpen(t *testing.T) {
	changes := []struct {
		name   string
		change func(*fixture)
	}{
		{"job", func(f *fixture) {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Generation++ })
		}},
		{"revision", func(f *fixture) { f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.Revision++ }) }},
		{"running epoch", func(f *fixture) {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { j.RunningEpoch++ })
		}},
		{"session", func(f *fixture) {
			f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { v.SessionGen++ })
		}},
		{"session epoch", func(f *fixture) { f.state.edit(func(j *telemetry.JobView, v *telemetry.SessionView) { v.Epoch++ }) }},
		{"reconnect", func(f *fixture) { f.commands.reconnect() }},
		{"pause resume", func(f *fixture) { f.report("PAUSE", new(2)); f.report("RUNNING", new(2)) }},
		{"stale job", func(f *fixture) { f.state.j.ObsAt = f.clock.now().Add(-16 * time.Second) }},
		{"stale session", func(f *fixture) { f.state.v.ObsAt = f.clock.now().Add(-16 * time.Second) }},
		{"budget", func(f *fixture) { f.clock.advance(26 * time.Second); f.report("RUNNING", new(2)) }},
	}
	for _, stage := range []string{"first capture", "second capture", "evaluation", "dispatch"} {
		for _, tc := range changes {
			t.Run(stage+"/"+tc.name, func(t *testing.T) {
				f := newLayerFixture(t)
				base := f.s.frames
				captures := 0
				f.s.frames = frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
					frame, err := base.Capture(ctx, serial, after)
					captures++
					if (stage == "first capture" && captures == 1) || (stage == "second capture" && captures == 2) {
						tc.change(f)
					}
					return frame, err
				})
				if stage == "evaluation" {
					f.client.layerHook = func() { tc.change(f) }
				}
				if stage == "dispatch" {
					f.client.layerHook = func() {
						reads := 0
						f.state.jobHook = func() {
							reads++
							// The first two reads authorize the returned result. Next reads belong to action revalidation.
							if reads == 3 {
								tc.change(f)
							}
						}
					}
				}
				f.hint("first_layer_complete")
				f.step()
				if len(f.commands.pauses) != 0 || layerStatus(f).State != "skipped" {
					t.Fatalf("changed evidence acted: %+v", layerStatus(f))
				}
				if (stage == "first capture" || stage == "second capture") && f.client.layerCalls != 0 {
					t.Fatal("changed frames uploaded")
				}
			})
		}
	}
}

func TestFirstLayerFrameBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*fixture, *Frame, time.Time, int)
	}{
		{"at first boundary", func(f *fixture, frame *Frame, after time.Time, n int) {
			if n == 1 {
				frame.Captured = after
			}
		}},
		{"at second boundary", func(f *fixture, frame *Frame, after time.Time, n int) {
			if n == 2 {
				frame.Captured = after
			}
		}},
		{"duplicate sequence", func(f *fixture, frame *Frame, after time.Time, n int) { frame.Seq = 1 }},
		{"future first", func(f *fixture, frame *Frame, after time.Time, n int) {
			if n == 1 {
				frame.Captured = f.clock.now().Add(time.Second)
			}
		}},
		{"zero first", func(f *fixture, frame *Frame, after time.Time, n int) {
			if n == 1 {
				frame.Captured = time.Time{}
			}
		}},
		{"zero second", func(f *fixture, frame *Frame, after time.Time, n int) {
			if n == 2 {
				frame.Captured = time.Time{}
			}
		}},
		{"future second", func(f *fixture, frame *Frame, after time.Time, n int) {
			if n == 2 {
				frame.Captured = f.clock.now().Add(time.Second)
			}
		}},
		{"stale first", func(f *fixture, frame *Frame, after time.Time, n int) {
			if n == 1 {
				f.clock.advance(31 * time.Second)
				f.report("RUNNING", new(2))
			}
		}},
		{"stale second", func(f *fixture, frame *Frame, after time.Time, n int) {
			if n == 2 {
				f.clock.advance(31 * time.Second)
				f.report("RUNNING", new(2))
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLayerFixture(t)
			base := f.s.frames
			n := 0
			f.s.frames = frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
				frame, err := base.Capture(ctx, serial, after)
				n++
				tc.mutate(f, &frame, after, n)
				return frame, err
			})
			f.hint("first_layer_complete")
			f.step()
			if f.client.layerCalls != 0 || len(f.commands.pauses) != 0 {
				t.Fatal("invalid frames uploaded or acted")
			}
		})
	}
}

func TestFirstLayerWarningPassErrorAndPauseFailureEvents(t *testing.T) {
	for _, tc := range []struct {
		name        string
		result      LayerResult
		err         error
		want, event string
		severity    string
	}{
		{"incomplete", LayerResult{PAssessable: .9, PIncomplete: 1}, nil, "warning", "platecheck_layer1_warning", activity.Warning},
		{"cutoff", LayerResult{PAssessable: .9, PTangled: .7}, nil, "warning", "platecheck_layer1_warning", activity.Warning},
		{"pass", LayerResult{PAssessable: .9}, nil, "passed", "platecheck_layer1_passed", activity.Info},
		{"inconclusive", LayerResult{PAssessable: .79, PDetached: 1}, nil, "inconclusive", "platecheck_layer1_skipped", activity.Info},
		{"invalid", LayerResult{PAssessable: math.NaN()}, nil, "error", "platecheck_layer1_skipped", activity.Info},
		{"provider error", LayerResult{}, errors.New("private failure"), "error", "platecheck_layer1_skipped", activity.Info},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLayerFixture(t)
			f.client.layerResult, f.client.err = tc.result, tc.err
			f.hint("first_layer_complete")
			f.step()
			if len(f.commands.pauses) != 0 || layerStatus(f).State != tc.want || !f.hasEvent(tc.event) {
				t.Fatalf("decision %+v", layerStatus(f))
			}
			for _, e := range f.activity.Recent("01S1") {
				if e.Kind == tc.event && e.Severity != tc.severity {
					t.Fatal("incorrect severity")
				}
			}
			for _, e := range f.activity.Recent("01S1") {
				if e.Kind == "platecheck_layer1_warning" && !strings.Contains(e.Message, "%") {
					t.Fatal("warning lacks defect score")
				}
				if e.Kind == "platecheck_layer1_warning" && tc.name == "incomplete" && !strings.Contains(e.Message, "incomplete layer (100.0%, warning only)") {
					t.Fatal("missing warning-only defect details")
				}
			}
		})
	}
	f := newLayerFixture(t)
	f.commands.pauseErr = errors.New("failed publish")
	f.hint("first_layer_complete")
	f.step()
	if layerStatus(f).Pause != "failed" || f.w.layerPending != nil || !f.hasEvent("platecheck_layer1_pause_failed") || f.hasEvent("platecheck_layer1_pause_sent") {
		t.Fatal("pause failure reported pending/success")
	}
	f.hint("first_layer_complete")
	f.step()
	if len(f.commands.pauses) != 1 {
		t.Fatal("failed send retried")
	}
}

func TestFirstLayerPauseMessagesNameValidatedDefects(t *testing.T) {
	f := newLayerFixture(t)
	f.client.layerResult = LayerResult{PAssessable: .9, PTangled: .9, PDetached: .8, PNozzleBlob: .7, PIncomplete: 1}
	f.hint("first_layer_complete")
	f.step()
	f.clock.advance(31 * time.Second)
	f.step()
	for _, kind := range []string{"platecheck_layer1_pause_sent", "platecheck_layer1_pause_unconfirmed"} {
		found := false
		for _, e := range f.activity.Recent("01S1") {
			if e.Kind != kind {
				continue
			}
			found = true
			if !strings.Contains(e.Message, "filament tangled (90.0%)") || !strings.Contains(e.Message, "part detached (80.0%)") || strings.Contains(e.Message, "nozzle blob") || strings.Contains(e.Message, "incomplete layer") {
				t.Fatal("pause message does not name exactly the triggering defects")
			}
		}
		if !found {
			t.Fatalf("missing %s", kind)
		}
	}
}

func TestFirstLayerTwoFrameTimingAndOriginalUploads(t *testing.T) {
	f := newLayerFixture(t)
	var boundaries []time.Time
	var frames []Frame
	base := f.s.frames
	f.s.frames = frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		boundaries = append(boundaries, after)
		frame, err := base.Capture(ctx, serial, after)
		frames = append(frames, frame)
		return frame, err
	})
	f.hint("first_layer_complete")
	f.step()
	if len(frames) != 2 || len(f.client.layerImages) != 2 || !boundaries[1].Equal(frames[0].Captured.Add(2*time.Second)) || !frames[1].Captured.After(boundaries[1]) || frames[1].Seq == frames[0].Seq {
		t.Fatal("second frame not strictly separated and unique")
	}
	for i, frame := range frames {
		if string(frame.JPEG) != string(f.client.layerImages[i]) {
			t.Fatal("original frame changed before upload")
		}
	}
	if f.w.layerHint != nil {
		t.Fatal("worker retained completed capture admission")
	}
}

func TestFirstLayerSourceBudgetIncludesQueueDelay(t *testing.T) {
	f := newLayerFixture(t)
	f.hint("first_layer_complete")
	f.clock.advance(10 * time.Second)
	f.report("RUNNING", new(2))
	captures := 0
	base := f.s.frames
	f.s.frames = frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		captures++
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("capture has no deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > 15*time.Second {
			t.Fatalf("queued source renewed capture budget: remaining=%s", remaining)
		}
		return base.Capture(ctx, serial, after)
	})
	f.step()
	if captures != 2 || f.client.layerCalls != 1 || len(f.commands.pauses) != 1 {
		t.Fatal("valid queued source did not complete within its remaining budget")
	}
}

func TestFirstLayerOriginFreshnessExpiresDuringSnapshot(t *testing.T) {
	f := newLayerFixture(t)
	observation := firstLayerEvidence(f)
	reads := 0
	f.state.jobHook = func() {
		reads++
		if reads == 2 {
			f.clock.advance(15 * time.Second)
			f.report("RUNNING", new(2))
		}
	}
	captures := 0
	base := f.s.frames
	f.s.frames = frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		captures++
		return base.Capture(ctx, serial, after)
	})
	f.s.ObserveActivity("01S1", activity.Entry{Kind: "first_layer_complete", Time: f.clock.now(), Observation: observation})
	f.step()
	if reads < 4 {
		t.Fatal("coherent snapshot retry was not exercised")
	}
	if f.w.layerHint != nil || f.w.layerProcessed != 0 || captures != 0 || f.client.layerCalls != 0 || len(f.commands.pauses) != 0 || len(f.commands.lights) != 0 {
		t.Fatal("expired source was repaired, consumed, captured, uploaded, or acted on")
	}
	if layerStatus(f).State != "idle" || f.hasEvent("platecheck_layer1_skipped") {
		t.Fatal("expired source invented a first-layer lifecycle")
	}
	f.state.jobHook = nil
	f.hint("first_layer_complete")
	f.step()
	if captures != 2 || f.client.layerCalls != 1 || len(f.commands.pauses) != 1 {
		t.Fatal("rejected source consumed the fresh current job")
	}
}

// newRealCacheFixture builds the first-layer worker against a real
// telemetry.Cache with the real clock: the cache stamps its own observation
// evidence, so both sides must run on the same time base for the 15 s
// freshness windows to mean real seconds.
func newRealCacheFixture(t *testing.T, state *telemetry.Cache) *fixture {
	t.Helper()
	client := &fakeDecision{layerResult: LayerResult{PAssessable: .9, PTangled: .9}}
	commands := &fakeCommands{gen: 1}
	data := testJPEG(t)
	seq := uint64(0)
	frames := frameFunc(func(_ context.Context, _ string, after time.Time) (Frame, error) {
		seq++
		// The camera waits for the requested boundary, exactly as a real
		// capture owner honors a "newer than" boundary.
		if d := time.Until(after); d > 0 {
			time.Sleep(d)
		}
		return Frame{JPEG: data, Seq: seq, Captured: time.Now(), Width: 8, Height: 6}, nil
	})
	printers := []config.Printer{{Serial: "01S1", Model: "P1S"}}
	s := New(printers, testSettings(), client, frames, state, commands, nil)
	s.now = time.Now
	events := activity.New(printers)
	s.SetActivity(events)
	return &fixture{s: s, w: s.workers["01S1"], commands: commands, client: client, activity: events}
}

// cacheEvidence stamps the originating Job+Session evidence of a real cache
// snapshot the way firstlayer stamps a real first_layer_complete event: one
// coherent sample of both projections at the moment of proof.
func cacheEvidence(c *telemetry.Cache, serial string) activity.Observation {
	j, _ := c.Job(serial)
	v, _ := c.Session(serial)
	return activity.Observation{SessionGen: v.SessionGen, Epoch: v.Epoch, StateGen: v.StateGen, ObsGen: v.ObsGen, ObsAt: v.ObsAt, JobGen: j.Generation, JobRevision: j.Revision, RunningEpoch: j.RunningEpoch}
}

// TestFirstLayerSameCookieStartTimeReplacementCannotBindNewJob reproduces the
// second critical review finding against a real telemetry.Cache: a changed
// positive gcode_start_time starts a new job generation while the identity
// cookie, session generation, session epoch, and upstream connection all stay
// unchanged, so session provenance alone cannot separate print A's event from
// print B. A's saved event must bind and act on nothing, and B's own fresh
// current evidence must still admit exactly once.
func TestFirstLayerSameCookieStartTimeReplacementCannotBindNewJob(t *testing.T) {
	c := telemetry.NewCache([]config.Printer{{Serial: "01S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	f := newRealCacheFixture(t, c)
	captures := 0
	base := f.s.frames
	f.s.frames = frameFunc(func(ctx context.Context, serial string, after time.Time) (Frame, error) {
		captures++
		return base.Capture(ctx, serial, after)
	})
	// Print A runs at layer 2 under identity 1-2-a.gcode.
	seq := uint64(0)
	report := func(started int64) {
		seq++
		payload := `{"print":{"gcode_state":"RUNNING","project_id":1,"task_id":2,"subtask_name":"a.gcode","gcode_start_time":` + strconv.FormatInt(started, 10) + `,"layer_num":2}}`
		c.ObserveReport("01S1", seq, 1, []byte(payload))
	}
	report(1000000000)
	sessionA, _ := c.Session("01S1")
	// firstlayer samples print A's originating evidence while print A runs.
	observation := cacheEvidence(c, "01S1")
	// Print B then overtakes the session on the same connection: same
	// identity, same session counters, same RUNNING layer-2 state, and the
	// only difference is a changed positive gcode_start_time.
	report(1760000000)
	jobB, _ := c.Job("01S1")
	sessionB, _ := c.Session("01S1")
	if jobB.Generation != observation.JobGen+1 || jobB.Revision != observation.JobRevision || jobB.RunningEpoch != observation.RunningEpoch {
		t.Fatalf("start-time replacement did not advance only the job generation: got %d/%d/%d, sampled %d/%d/%d", jobB.Generation, jobB.Revision, jobB.RunningEpoch, observation.JobGen, observation.JobRevision, observation.RunningEpoch)
	}
	if sessionB.Cookie != sessionA.Cookie || sessionB.SessionGen != sessionA.SessionGen || sessionB.Epoch != sessionA.Epoch {
		t.Fatalf("start-time replacement moved the session identity: %+v vs %+v", sessionB, sessionA)
	}
	if sessionB.SessionGen == 0 || observation.JobGen == 0 {
		t.Fatal("fixture premise broken")
	}
	// Deliver A's saved event: it must bind nothing and leave B unconsumed.
	f.s.ObserveActivity("01S1", activity.Entry{Kind: "first_layer_complete", Time: sessionA.ObsAt, Observation: observation})
	f.step()
	if f.w.layerHint != nil || f.w.layerProcessed != 0 || captures != 0 || f.client.layerCalls != 0 || len(f.commands.pauses) != 0 || len(f.commands.lights) != 0 {
		t.Fatal("A's stale event bound, consumed, captured, uploaded, lighted, or paused print B")
	}
	if layerStatus(f).State != "idle" || f.hasEvent("platecheck_layer1_skipped") {
		t.Fatal("A's stale event invented a first-layer lifecycle")
	}
	// B's own genuine current evidence may admit once, under the unchanged
	// session identity that could not carry A's event.
	f.s.ObserveActivity("01S1", activity.Entry{Kind: "first_layer_complete", Time: sessionB.ObsAt, Observation: cacheEvidence(c, "01S1")})
	f.step()
	if captures != 2 || f.client.layerCalls != 1 || len(f.commands.pauses) != 1 || layerStatus(f).Pause != "pending" || !f.hasEvent("platecheck_layer1_pause_sent") {
		t.Fatalf("print B lost or duplicated its own admission: captures=%d calls=%d pauses=%d pause=%+v", captures, f.client.layerCalls, len(f.commands.pauses), layerStatus(f))
	}
	if f.w.layerHint != nil || f.w.layerProcessed != jobB.Generation {
		t.Fatal("print B admission was not consumed exactly once")
	}
}
