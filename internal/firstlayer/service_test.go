package firstlayer

import (
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// sourceState answers Session and Job with the scripted views, so tests
// drive sampling without a telemetry cache. The same known flag gates both
// projections.
type sourceState struct {
	view  telemetry.SessionView
	job   telemetry.JobView
	known bool
}

func (s *sourceState) Session(serial string) (telemetry.SessionView, bool) {
	return s.view, s.known
}

func (s *sourceState) Job(serial string) (telemetry.JobView, bool) {
	return s.job, s.known
}

// scriptedSource answers successive Session and Job reads from scripted
// queues, so a test can interleave a projection replacement inside one
// sampling attempt. Reads pop in call order; an empty queue answers false.
type scriptedSource struct {
	jobs     []telemetry.JobView
	sessions []telemetry.SessionView
}

func (s *scriptedSource) Session(string) (telemetry.SessionView, bool) {
	if len(s.sessions) == 0 {
		return telemetry.SessionView{}, false
	}
	v := s.sessions[0]
	s.sessions = s.sessions[1:]
	return v, true
}

func (s *scriptedSource) Job(string) (telemetry.JobView, bool) {
	if len(s.jobs) == 0 {
		return telemetry.JobView{}, false
	}
	v := s.jobs[0]
	s.jobs = s.jobs[1:]
	return v, true
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
// pair generation and layer, with fixed past observation times. The job
// projection's observation time is deliberately offset from the session
// projection's: the two projections use separate clocks, so consumers must
// never compare their timestamps with each other. Identity fields change
// together when the generation changes.
func livePair(gen uint64, layer int) (telemetry.SessionView, telemetry.JobView) {
	session := telemetry.SessionView{
		Serial:     "S1",
		Active:     true,
		State:      "RUNNING",
		Cookie:     "1-2-job",
		Complete:   true,
		SessionGen: gen,
		Epoch:      gen + 1,
		StateGen:   gen + 2,
		ObsGen:     gen + 3,
		ObsAt:      time.Unix(1700000000+int64(gen)*10, 0).UTC().Round(0),
		LayerNum:   &layer,
	}
	job := telemetry.JobView{
		Generation:   gen,
		Revision:     gen + 4,
		RunningEpoch: gen + 5,
		Active:       true,
		State:        "RUNNING",
		StateGen:     gen + 2,
		ObsGen:       gen + 3,
		ObsAt:        time.Unix(1700000000+int64(gen)*10+3, 0).UTC().Round(0),
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
	sessionView, jobView := st.view, st.job
	obs, ok := svc.sample("S1", &session)
	if !ok {
		t.Fatal("armed session at layer 2 returned no evidence")
	}
	want := activity.Observation{JobGen: jobView.Generation, JobRevision: jobView.Revision,
		RunningEpoch: jobView.RunningEpoch, SessionGen: sessionView.SessionGen, Epoch: sessionView.Epoch,
		StateGen: sessionView.StateGen, ObsGen: sessionView.ObsGen, ObsAt: sessionView.ObsAt}
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
// source-event contract: the evidence sampled from the session view that
// proved layer 2 reaches the activity log unchanged even when telemetry has
// already switched to a concurrent second session between the source read
// and the event handling. The new session's identity never replaces the
// captured evidence and no event was recorded for the replacement session.
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
	sessionView, jobView := st.view, st.job
	obs, ok := svc.sample("S1", &session)
	if !ok {
		t.Fatal("armed session at layer 2 returned no evidence")
	}

	// Telemetry switches to session 9 between the source read and the event
	// handling, as a live poll loop can race across a report boundary.
	st.view, st.job = livePair(9, 2)
	svc.act.RecordObserved("S1", "first_layer_complete", activity.Info, "First layer complete", obs)

	if len(log.seen) != 1 {
		t.Fatalf("observer entries = %d, want 1", len(log.seen))
	}
	want := activity.Observation{JobGen: jobView.Generation, JobRevision: jobView.Revision,
		RunningEpoch: jobView.RunningEpoch, SessionGen: sessionView.SessionGen, Epoch: sessionView.Epoch,
		StateGen: sessionView.StateGen, ObsGen: sessionView.ObsGen, ObsAt: sessionView.ObsAt}
	if log.seen[0].Observation != want {
		t.Fatalf("observed observation = %+v, want %+v", log.seen[0].Observation, want)
	}
	if stored := log.Recent("S1")[0]; stored.Observation != want {
		t.Fatalf("stored observation = %+v, want %+v", stored.Observation, want)
	}

	// The replacement session was never rearmed, so sample keeps returning
	// no evidence for it.
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("replacement session returned evidence without arming")
	}
}

// pairReads queues one coherent sampling attempt (two Job reads bracketing
// two Session reads) for the given pair generation and layer.
func pairReads(src *scriptedSource, gen uint64, layer int) {
	session, job := livePair(gen, layer)
	src.jobs = append(src.jobs, job)
	src.sessions = append(src.sessions, session)
	src.jobs = append(src.jobs, job)
	src.sessions = append(src.sessions, session)
}

// TestSampleRetriesReplacementBetweenReads pins the bounded-read contract: a
// replacement that happens between the reads of one sampling attempt is
// rejected and retried instead of combining projections. The replacement's
// coherent pair is then adopted, it starts unarmed, and its own layer-1
// observation produces its own completion event.
func TestSampleRetriesReplacementBetweenReads(t *testing.T) {
	svc := New([]config.Printer{{Serial: "S1"}}, &scriptedSource{}, activity.New([]config.Printer{{Serial: "S1"}}))
	src := svc.state.(*scriptedSource)
	var session printSession

	// Arm the original pair from a coherent read.
	pairReads(src, 7, 1)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample returned evidence")
	}
	if !session.armed {
		t.Fatal("arming sample did not set armed")
	}

	// The first attempt reads the original pair and then the replacement,
	// so none of its reads may be combined: the sample must fall through to
	// the second attempt's read, which is coherently the replacement.
	session7, job7 := livePair(7, 2)
	session9, job9 := livePair(9, 2)
	src.jobs = append(src.jobs, job7)
	src.sessions = append(src.sessions, session7)
	src.jobs = append(src.jobs, job9)
	src.sessions = append(src.sessions, session9)
	pairReads(src, 9, 2)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("replacement seen mid-attempt past layer 1 returned evidence")
	}
	if session.jobGen != 9 || session.sessionGen != 9 {
		t.Fatalf("replacement not adopted: pair = %d/%d, want 9/9", session.jobGen, session.sessionGen)
	}
	if session.armed || session.recorded {
		t.Fatal("replacement inherited the original pair's arming")
	}

	// Once the replacement is observed from layer 1 again it arms on its
	// own identity and later records its own event.
	pairReads(src, 9, 1)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample for the replacement returned evidence")
	}
	if !session.armed {
		t.Fatal("replacement layer-1 sample did not arm")
	}
	pairReads(src, 9, 2)
	obs9, ok := svc.sample("S1", &session)
	if !ok {
		t.Fatal("replacement layer-2 sample returned no evidence")
	}
	want9 := activity.Observation{JobGen: 9, JobRevision: 13, RunningEpoch: 14,
		SessionGen: 9, Epoch: 10, StateGen: 11, ObsGen: 12, ObsAt: session9.ObsAt}
	if obs9 != want9 {
		t.Fatalf("replacement evidence = %+v, want %+v", obs9, want9)
	}
}

// TestSampleRejectsUnstableSourceWithoutReset pins that a source that never
// settles across all three bounded attempts is rejected wholesale, and that
// rejection leaves the tracked pair untouched: the original arming survives
// and a later coherent read completes the original pair's event.
func TestSampleRejectsUnstableSourceWithoutReset(t *testing.T) {
	svc := New([]config.Printer{{Serial: "S1"}}, &scriptedSource{}, activity.New([]config.Printer{{Serial: "S1"}}))
	src := svc.state.(*scriptedSource)
	var session printSession

	pairReads(src, 7, 1)
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample returned evidence")
	}
	if !session.armed {
		t.Fatal("arming sample did not set armed")
	}

	// Three attempts that each alternate between two pairs: every attempt
	// is incoherent, so the sample must return nothing.
	session7, job7 := livePair(7, 2)
	session8, job8 := livePair(8, 2)
	src.jobs = append(src.jobs, job7, job8, job8, job7, job7, job8)
	src.sessions = append(src.sessions, session7, session8, session8, session7, session7, session8)
	var seen []activity.Entry
	svc.act.SetObserver(func(serial string, e activity.Entry) {
		seen = append(seen, e)
	})
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("unstable source returned evidence")
	}
	if len(seen) != 0 {
		t.Fatalf("observer saw %d entries, want 0", len(seen))
	}
	if session.jobGen != 7 || session.sessionGen != 7 || !session.armed || session.recorded {
		t.Fatalf("rejection disturbed the tracked pair: %+v", session)
	}

	// A coherent read resumes the interrupted completion.
	pairReads(src, 7, 2)
	obs, ok := svc.sample("S1", &session)
	if !ok {
		t.Fatal("coherent resumption at layer 2 returned no evidence")
	}
	if !session.recorded {
		t.Fatal("coherent resumption did not record")
	}
	if obs.JobGen != 7 || obs.SessionGen != 7 {
		t.Fatalf("resumed evidence = %+v, want the original pair 7/7", obs)
	}
}
