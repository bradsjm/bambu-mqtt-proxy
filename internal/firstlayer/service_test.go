package firstlayer

import (
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// sourceState answers Print with the scripted views, so tests drive
// sampling without a telemetry cache. The same known flag gates the read.
type sourceState struct {
	view  telemetry.SessionView
	job   telemetry.JobView
	known bool
}

func (s *sourceState) Print(serial string) (telemetry.JobView, telemetry.SessionView, bool) {
	return s.job, s.view, s.known
}

// observableLog wraps the activity log with captured observer entries.
type observableLog struct {
	*activity.Log
	seen []activity.Entry
}

func newObservableLog() *observableLog {
	wrapped := &observableLog{Log: activity.New([]config.Printer{{Serial: "S1"}})}
	wrapped.SetObserver(func(serial string, e activity.Entry) {
		wrapped.seen = append(wrapped.seen, e)
	})
	return wrapped
}

// livePair returns one coherent Job+Session view pair for S1 at the given
// job generation and layer. The layer is stamped as reported by this job,
// so CurrentLayer exposes it. Identity fields change together when the
// generation changes.
func livePair(gen uint64, layer int) (telemetry.SessionView, telemetry.JobView) {
	session := telemetry.SessionView{
		Serial:       "S1",
		Active:       true,
		State:        "RUNNING",
		Cookie:       "1-2-job",
		Complete:     true,
		SessionGen:   gen,
		Epoch:        gen + 1,
		StateGen:     gen + 2,
		ObsGen:       gen + 3,
		ObsAt:        time.Unix(1700000000+int64(gen)*10, 0).UTC().Round(0),
		LayerNum:     &layer,
		LayerThisJob: true,
	}
	job := telemetry.JobView{
		Generation: gen,
		Active:     true,
		State:      "RUNNING",
	}
	return session, job
}

// TestSampleArmsOnFirstLayerThenRecordsOnce pins the arming contract: a
// sample at layer 1 arms without recording, the next sample above layer 1
// returns the completion evidence exactly once, and later samples during
// the same session return nothing.
func TestSampleArmsOnFirstLayerThenRecordsOnce(t *testing.T) {
	st := &sourceState{known: true}
	svc := New([]config.Printer{{Serial: "S1"}}, st, activity.New([]config.Printer{{Serial: "S1"}}))
	var session printSession

	st.view, st.job = livePair(7, 1)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample returned evidence")
	}
	if !session.armed {
		t.Fatal("arming sample did not set armed")
	}

	st.view, st.job = livePair(7, 2)
	obs, ok := svc.sample("S1", &session)
	if !ok {
		t.Fatal("armed session at layer 2 returned no evidence")
	}
	want := activity.Observation{JobGen: 7}
	if obs != want {
		t.Fatalf("completion evidence = %+v, want %+v", obs, want)
	}

	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("second sample at layer 2 returned evidence again")
	}
}

// TestSampleNoEventForLateAttachment pins that a session first seen already
// past layer 1 returns no evidence: reaching layer 2, not merely past it,
// proves the first layer, so a late-attached service must not fire.
func TestSampleNoEventForLateAttachment(t *testing.T) {
	st := &sourceState{known: true}
	svc := New([]config.Printer{{Serial: "S1"}}, st, activity.New([]config.Printer{{Serial: "S1"}}))
	var seen []activity.Entry
	svc.act.SetObserver(func(serial string, e activity.Entry) {
		seen = append(seen, e)
	})
	var session printSession

	st.view, st.job = livePair(9, 4)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("session attached late at layer 4 returned evidence")
	}
	st.view, st.job = livePair(9, 5)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("later layer 5 sample returned evidence")
	}
	if session.recorded {
		t.Fatal("arming never happened but recorded was set")
	}
	if len(seen) != 0 {
		t.Fatalf("observer saw %d entries, want 0", len(seen))
	}
}

// TestSampleSkipsEndedAndUnknownSessions pins that a session that already
// ended, and an unknown printer, return no completion evidence.
func TestSampleSkipsEndedAndUnknownSessions(t *testing.T) {
	st := &sourceState{known: true}
	svc := New([]config.Printer{{Serial: "S1"}}, st, activity.New([]config.Printer{{Serial: "S1"}}))
	var session printSession

	st.view, st.job = livePair(7, 1)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample returned evidence")
	}
	st.known = true
	st.view, st.job = telemetry.SessionView{Serial: "S1"}, telemetry.JobView{} // session ended: inactive
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("ended session returned evidence")
	}
	st.known = false
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("unknown serial returned evidence")
	}
}

// TestRecordKeepsOriginalEvidenceWhenTelemetryReplacesSession pins the
// source-event contract: the evidence sampled from the job that proved
// layer 2 reaches the activity log unchanged even when telemetry has
// already switched to a concurrent second job between the source read and
// the event handling. The new job's identity never replaces the captured
// evidence and no event was recorded for the replacement job.
func TestRecordKeepsOriginalEvidenceWhenTelemetryReplacesSession(t *testing.T) {
	log := newObservableLog()
	st := &sourceState{known: true}
	svc := New([]config.Printer{{Serial: "S1"}}, st, log.Log)
	var session printSession

	st.view, st.job = livePair(7, 1)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample returned evidence")
	}

	st.view, st.job = livePair(7, 2)
	obs, ok := svc.sample("S1", &session)
	if !ok {
		t.Fatal("armed session at layer 2 returned no evidence")
	}

	// Telemetry switches to job 9 between the source read and the event
	// handling, as a live poll loop can race across a report boundary.
	st.view, st.job = livePair(9, 2)
	svc.act.RecordObserved("S1", "first_layer_complete", activity.Info, "First layer complete", obs)

	if len(log.seen) != 1 {
		t.Fatalf("observer entries = %d, want 1", len(log.seen))
	}
	want := activity.Observation{JobGen: 7}
	if log.seen[0].Observation != want {
		t.Fatalf("observed observation = %+v, want %+v", log.seen[0].Observation, want)
	}
	if stored := log.Recent("S1")[0]; stored.Observation != want {
		t.Fatalf("stored observation = %+v, want %+v", stored.Observation, want)
	}

	// The replacement job was never rearmed, so sample keeps returning
	// no evidence for it.
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("replacement job returned evidence without arming")
	}
}

// TestSampleReplacementStartsUnarmed pins the job-following contract: a
// replacement job is adopted without inheriting the original job's arming,
// and its own layer-1 observation produces its own completion event.
func TestSampleReplacementStartsUnarmed(t *testing.T) {
	st := &sourceState{known: true}
	svc := New([]config.Printer{{Serial: "S1"}}, st, activity.New([]config.Printer{{Serial: "S1"}}))
	var session printSession

	// Arm the original job from a coherent read.
	st.view, st.job = livePair(7, 1)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample returned evidence")
	}
	if !session.armed {
		t.Fatal("arming sample did not set armed")
	}

	// The replacement arrives already past layer 1: it is adopted
	// unarmed, so no evidence may be combined across the boundary.
	st.view, st.job = livePair(9, 2)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("replacement seen past layer 1 returned evidence")
	}
	if session.jobGen != 9 {
		t.Fatalf("replacement not adopted: job = %d, want 9", session.jobGen)
	}
	if session.armed || session.recorded {
		t.Fatal("replacement inherited the original job's arming")
	}

	// Once the replacement is observed from layer 1 again it arms on its
	// own identity and later records its own event.
	st.view, st.job = livePair(9, 1)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample for the replacement returned evidence")
	}
	if !session.armed {
		t.Fatal("replacement layer-1 sample did not arm")
	}
	st.view, st.job = livePair(9, 2)
	obs9, ok := svc.sample("S1", &session)
	if !ok {
		t.Fatal("replacement layer-2 sample returned no evidence")
	}
	want9 := activity.Observation{JobGen: 9}
	if obs9 != want9 {
		t.Fatalf("replacement evidence = %+v, want %+v", obs9, want9)
	}
}
