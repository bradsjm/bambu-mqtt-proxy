package firstlayer

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// cacheReport builds one upstream report payload from key/value pairs.
func cacheReport(t *testing.T, kv map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"print": kv})
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	return raw
}

// TestCacheSourceJobGenerationAdvancesWithoutNewSession is a regression for
// the session-only provenance finding, run against a real telemetry.Cache:
// a changed positive gcode_start_time on the same connection and the same
// project/task/name cookie advances Job.Generation while SessionGen and
// Epoch stay unchanged. The scenario:
//
//  1. Job A starts at layer 1 and the service arms on the A pair (job 1,
//     session 1).
//  2. A reaches layer 2 and the service records A's completion evidence.
//  3. A new print starts without any session boundary: only
//     gcode_start_time changes, so the cookie, SessionGen and Epoch all
//     stay put while Job.Generation advances to 2.
//  4. Job B is first sampled with no layer evidence of its own: the
//     boundary report carries no layer_num, so the session view exposes
//     no layer at all, and B must not inherit A's arming — no evidence
//     for B yet.
//  5. B observes layer 1 and then layer 2, and gets its own event carrying
//     B's job evidence under the unchanged session identity.
//
// The stored A entry keeps A's JobGen even though job 2 is current, so the
// record pins the original job, not the replacement.
func TestCacheSourceJobGenerationAdvancesWithoutNewSession(t *testing.T) {
	cache := telemetry.NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	log := newObservableLog()
	svc := New([]config.Printer{{Serial: "S1"}}, cache, log.Log)
	var session printSession

	// Job A starts on the bed and reports its first layer.
	cache.ObserveReport("S1", 1, 5, cacheReport(t, map[string]any{
		"gcode_state": "RUNNING", "project_id": 1, "task_id": 2, "subtask_name": "a.3mf",
		"gcode_start_time": 1000.0, "layer_num": 1.0, "mc_percent": 5.0,
	}))
	jobA, _ := cache.Job("S1")
	sessionA, _ := cache.Session("S1")
	if jobA.Generation != 1 || jobA.Revision != 1 {
		t.Fatalf("job A after start = gen %d rev %d, want 1/1", jobA.Generation, jobA.Revision)
	}
	if !sessionA.Active || sessionA.SessionGen != 1 || sessionA.Epoch != 0 ||
		sessionA.Cookie != "1-2-a.3mf" || sessionA.LayerNum == nil || *sessionA.LayerNum != 1 {
		t.Fatalf("session A after start = %+v", sessionA)
	}
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample for job A returned evidence")
	}
	if !session.armed {
		t.Fatal("arming sample for job A did not set armed")
	}

	// Job A reaches layer 2: the service records with A's job evidence.
	cache.ObserveReport("S1", 2, 5, cacheReport(t, map[string]any{
		"layer_num": 2.0, "mc_percent": 6.0,
	}))
	obsA, ok := svc.sample("S1", &session)
	if !ok {
		t.Fatal("armed job A at layer 2 returned no evidence")
	}
	wantA := activity.Observation{JobGen: 1, JobRevision: 1, RunningEpoch: 0,
		SessionGen: 1, Epoch: 0, StateGen: 5, ObsGen: 5}
	// ObsAt is the observation time of the session view read that proved
	// layer 2, not of the earlier read above.
	sessionNow, _ := cache.Session("S1")
	wantA.ObsAt = sessionNow.ObsAt
	if obsA != wantA {
		t.Fatalf("job A evidence = %+v, want %+v", obsA, wantA)
	}
	log.RecordObserved("S1", "first_layer_complete", activity.Info, "First layer complete", obsA)

	// A new job arrives inside the same session: same connection, same
	// project/task/name cookie, only gcode_start_time changes.
	cache.ObserveReport("S1", 3, 5, cacheReport(t, map[string]any{
		"gcode_start_time": 2000.0, "mc_percent": 7.0,
	}))
	jobB, _ := cache.Job("S1")
	sessionB, _ := cache.Session("S1")
	if jobB.Generation != 2 {
		t.Fatalf("changed gcode_start_time did not advance job: gen=%d", jobB.Generation)
	}
	if jobB.Revision == 0 {
		t.Fatal("new job generation carries no revision")
	}
	if sessionB.SessionGen != 1 || sessionB.Epoch != 0 || sessionB.Cookie != "1-2-a.3mf" {
		t.Fatalf("session identity moved with the job start time: %+v", sessionB)
	}
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("job B already past layer 1 at first sight returned evidence")
	}
	if session.jobGen != 2 || session.sessionGen != 1 || session.armed || session.recorded {
		t.Fatalf("job B inherited or reset state wrongly: %+v", session)
	}

	// Job B is far past layer 1: still no evidence, and no second event.
	cache.ObserveReport("S1", 4, 5, cacheReport(t, map[string]any{
		"layer_num": 3.0, "mc_percent": 8.0,
	}))
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("unarmed job B at layer 3 returned evidence")
	}
	if len(log.seen) != 1 {
		t.Fatalf("observer entries = %d, want 1", len(log.seen))
	}

	// Job B observes layer 1 and then layer 2 under the unchanged session
	// identity and records its own event.
	cache.ObserveReport("S1", 5, 5, cacheReport(t, map[string]any{
		"layer_num": 1.0, "mc_percent": 9.0,
	}))
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample for job B returned evidence")
	}
	cache.ObserveReport("S1", 6, 5, cacheReport(t, map[string]any{
		"layer_num": 2.0, "mc_percent": 10.0,
	}))
	obsB, ok := svc.sample("S1", &session)
	if !ok {
		t.Fatal("armed job B at layer 2 returned no evidence")
	}
	sessionB, _ = cache.Session("S1")
	wantB := activity.Observation{JobGen: 2, JobRevision: 1, RunningEpoch: 0,
		SessionGen: 1, Epoch: 0, StateGen: 5, ObsGen: 5, ObsAt: sessionB.ObsAt}
	if obsB != wantB {
		t.Fatalf("job B evidence = %+v, want %+v", obsB, wantB)
	}
	log.RecordObserved("S1", "first_layer_complete", activity.Info, "First layer complete", obsB)

	// Exactly two events, and the first one still pins job A while job B is
	// the current job on the same session identity.
	if len(log.seen) != 2 {
		t.Fatalf("observer entries = %d, want 2", len(log.seen))
	}
	if log.seen[0].Observation != obsA {
		t.Fatalf("stored job A evidence = %+v, want %+v", log.seen[0].Observation, obsA)
	}
	if log.seen[1].Observation != obsB {
		t.Fatalf("stored job B evidence = %+v, want %+v", log.seen[1].Observation, obsB)
	}
}

// TestCacheSourceStickyLayerCannotArmReplacementJob is a regression for the
// layer-provenance finding, run against a real telemetry.Cache: the session
// view exposes LayerNum only when layer_num was reported inside the current
// job generation, so a same-cookie job replacement cannot arm from the
// previous job's sticky layer. The scenario:
//
//  1. Job A reports RUNNING at layer 1 (gcode_start_time 1000) and the
//     service arms on the A pair (job 1, session 1).
//  2. A same-cookie report changes gcode_start_time to 2000 and carries
//     mc_percent but NO layer_num: Job.Generation advances to 2 while
//     SessionGen and Epoch stay put. A's sticky layer 1 leaves the session
//     view (the display value stays put), so the sample resets to job 2
//     and remains UNARMED instead of arming from A's layer.
//  3. Job B reports RUNNING at layer 3 without ever observing layer 1.
//     Layer 3 is B's own evidence, reported inside B's generation, so the
//     session view exposes it again — but B was never armed, so no
//     completion event can be invented for it.
func TestCacheSourceStickyLayerCannotArmReplacementJob(t *testing.T) {
	cache := telemetry.NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	log := newObservableLog()
	svc := New([]config.Printer{{Serial: "S1"}}, cache, log.Log)
	var session printSession

	// Job A starts on the bed and reports its first layer.
	cache.ObserveReport("S1", 1, 5, cacheReport(t, map[string]any{
		"gcode_state": "RUNNING", "project_id": 1, "task_id": 2, "subtask_name": "a.3mf",
		"gcode_start_time": 1000.0, "layer_num": 1.0, "mc_percent": 5.0,
	}))
	jobA, _ := cache.Job("S1")
	sessionA, _ := cache.Session("S1")
	if jobA.Generation != 1 || jobA.Revision != 1 {
		t.Fatalf("job A after start = gen %d rev %d, want 1/1", jobA.Generation, jobA.Revision)
	}
	if !sessionA.Active || sessionA.SessionGen != 1 || sessionA.Epoch != 0 ||
		sessionA.Cookie != "1-2-a.3mf" || sessionA.LayerNum == nil || *sessionA.LayerNum != 1 {
		t.Fatalf("session A after start = %+v", sessionA)
	}
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample for job A returned evidence")
	}
	if !session.armed {
		t.Fatal("arming sample for job A did not set armed")
	}

	// The same-cookie boundary: only gcode_start_time changes, and the
	// report carries no layer_num.
	cache.ObserveReport("S1", 2, 5, cacheReport(t, map[string]any{
		"gcode_start_time": 2000.0, "mc_percent": 7.0,
	}))
	jobB, _ := cache.Job("S1")
	sessionB, _ := cache.Session("S1")
	if jobB.Generation != 2 || jobB.Revision != 1 {
		t.Fatalf("changed gcode_start_time did not advance the job: gen=%d rev=%d, want 2/1",
			jobB.Generation, jobB.Revision)
	}
	if sessionB.SessionGen != 1 || sessionB.Epoch != 0 || sessionB.Cookie != "1-2-a.3mf" {
		t.Fatalf("session identity moved with the job start time: %+v", sessionB)
	}
	if sessionB.LayerNum != nil {
		t.Fatalf("session view exposed job A's sticky layer %d across the job boundary, want nil",
			*sessionB.LayerNum)
	}
	if display, _ := cache.State("S1"); display.LayerNum == nil || *display.LayerNum != 1 {
		t.Fatalf("sticky display layer disturbed by the job boundary: %+v", display.LayerNum)
	}
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("job B sampled without its own layer evidence returned evidence")
	}
	if session.jobGen != 2 || session.sessionGen != 1 || session.armed || session.recorded {
		t.Fatalf("job B inherited or reset state wrongly: %+v", session)
	}

	// Job B reports RUNNING at layer 3 without ever observing layer 1.
	cache.ObserveReport("S1", 3, 5, cacheReport(t, map[string]any{
		"gcode_state": "RUNNING", "layer_num": 3.0, "mc_percent": 8.0,
	}))
	sessionB3, _ := cache.Session("S1")
	if sessionB3.LayerNum == nil || *sessionB3.LayerNum != 3 {
		t.Fatalf("job B's own layer 3 report stayed hidden: %+v", sessionB3.LayerNum)
	}
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("unarmed job B at layer 3 returned evidence")
	}
	if session.armed || session.recorded {
		t.Fatalf("layer 3 disturbed job B's unarmed state: %+v", session)
	}

	// No completion event: A was never past layer 1 when replaced, and B
	// was never armed, so no event may be invented from either side.
	if len(log.seen) != 0 {
		t.Fatalf("observer entries = %d, want 0", len(log.seen))
	}
}

// TestCacheSourceReplacementJobRereportsOwnLayerAndCompletes is the control
// for the layer-provenance fix: a same-cookie replacement that re-reports
// its own layer 1 inside its own generation arms on its own evidence and
// completes exactly once, under the unchanged session identity. The boundary
// here arrives as a metadata-only delta — a report whose print object
// carries only gcode_start_time, no real print state marker — which starts
// the new job generation without touching session identity or session
// freshness, and still hides the previous job's sticky layer.
func TestCacheSourceReplacementJobRereportsOwnLayerAndCompletes(t *testing.T) {
	cache := telemetry.NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	log := newObservableLog()
	svc := New([]config.Printer{{Serial: "S1"}}, cache, log.Log)
	var session printSession

	// Job A arms on its first layer.
	cache.ObserveReport("S1", 1, 5, cacheReport(t, map[string]any{
		"gcode_state": "RUNNING", "project_id": 1, "task_id": 2, "subtask_name": "a.3mf",
		"gcode_start_time": 1000.0, "layer_num": 1.0, "mc_percent": 5.0,
	}))
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample for job A returned evidence")
	}
	sessionA, _ := cache.Session("S1")

	// The metadata-only boundary: gcode_start_time alone.
	cache.ObserveReport("S1", 2, 5, cacheReport(t, map[string]any{
		"gcode_start_time": 2000.0,
	}))
	jobB, _ := cache.Job("S1")
	sessionB, _ := cache.Session("S1")
	if jobB.Generation != 2 || jobB.Revision != 1 {
		t.Fatalf("metadata-only boundary did not advance the job: gen=%d rev=%d, want 2/1",
			jobB.Generation, jobB.Revision)
	}
	if sessionB.SessionGen != 1 || sessionB.Epoch != 0 || sessionB.Cookie != "1-2-a.3mf" {
		t.Fatalf("session identity moved with the metadata-only boundary: %+v", sessionB)
	}
	if sessionB.Obs != sessionA.Obs || !sessionB.ObsAt.Equal(sessionA.ObsAt) {
		t.Fatalf("metadata-only boundary refreshed session freshness: %+v", sessionB)
	}
	if sessionB.LayerNum != nil {
		t.Fatalf("session view exposed job A's sticky layer %d across the metadata-only boundary, want nil",
			*sessionB.LayerNum)
	}
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("job B sampled without its own layer evidence returned evidence")
	}
	if session.jobGen != 2 || session.sessionGen != 1 || session.armed || session.recorded {
		t.Fatalf("job B inherited or reset state wrongly: %+v", session)
	}

	// Job B reports its own layer 1 inside its own generation and arms.
	cache.ObserveReport("S1", 3, 5, cacheReport(t, map[string]any{
		"layer_num": 1.0, "mc_percent": 9.0,
	}))
	sessionB1, _ := cache.Session("S1")
	if sessionB1.LayerNum == nil || *sessionB1.LayerNum != 1 {
		t.Fatalf("job B's own layer 1 report stayed hidden: %+v", sessionB1.LayerNum)
	}
	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("arming sample for job B returned evidence")
	}
	if !session.armed {
		t.Fatal("arming sample for job B did not set armed")
	}

	// Job B reaches layer 2 and records its own event exactly once.
	cache.ObserveReport("S1", 4, 5, cacheReport(t, map[string]any{
		"layer_num": 2.0, "mc_percent": 10.0,
	}))
	obsB, ok := svc.sample("S1", &session)
	if !ok {
		t.Fatal("armed job B at layer 2 returned no evidence")
	}
	sessionNow, _ := cache.Session("S1")
	wantB := activity.Observation{JobGen: 2, JobRevision: 1, RunningEpoch: 0,
		SessionGen: 1, Epoch: 0, StateGen: 5, ObsGen: 5, ObsAt: sessionNow.ObsAt}
	if obsB != wantB {
		t.Fatalf("job B evidence = %+v, want %+v", obsB, wantB)
	}
	log.RecordObserved("S1", "first_layer_complete", activity.Info, "First layer complete", obsB)

	if _, ok := svc.sample("S1", &session); ok {
		t.Fatal("second sample at layer 2 returned evidence again")
	}
	if len(log.seen) != 1 {
		t.Fatalf("observer entries = %d, want 1", len(log.seen))
	}
	if log.seen[0].Observation != obsB {
		t.Fatalf("stored job B evidence = %+v, want %+v", log.seen[0].Observation, obsB)
	}
}
