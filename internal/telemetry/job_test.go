package telemetry

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// jobClock is the injected preview clock. Tests move it explicitly and
// every preview merge reads it, so gap and settling boundaries stay
// deterministic. Detection timing stays on the real clock.
type jobClock struct{ now time.Time }

func newJobClock() *jobClock {
	return &jobClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *jobClock) Now() time.Time { return c.now }

func (c *jobClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// newJobTestCache returns a one-printer cache whose preview clock is clk.
func newJobTestCache(t *testing.T, clk *jobClock) *Cache {
	t.Helper()
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.now = clk.Now
	return c
}

// jobReport builds one upstream report payload from key/value pairs.
func jobReport(t *testing.T, kv map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"print": kv})
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	return raw
}

// TestJobGenerationSurvivesPauseResume covers the ordered lifecycle:
// PREPARE starts the generation, RUNNING starts settling, PAUSE and PAUSED
// reset settling with a RunningEpoch bump, and the next fresh RUNNING
// observation restarts the timer inside the same generation.
func TestJobGenerationSurvivesPauseResume(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	c.ObserveReport("S1", 1, 5, jobReport(t, map[string]any{
		"gcode_state": "PREPARE", "project_id": 1, "task_id": 2, "subtask_name": "a.3mf",
	}))
	v, ok := c.Job("S1")
	if !ok || !v.Active || v.State != "PREPARE" || v.Generation != 1 || v.Revision != 1 ||
		v.Name != "a.3mf" || v.ProjectID != "1" || v.TaskID != "2" ||
		v.StateGen != 5 || v.ObsGen != 5 || v.ObsAt.IsZero() || !v.RunningSince.IsZero() {
		t.Fatalf("after PREPARE: ok=%v view=%+v", ok, v)
	}

	clk.Advance(time.Second)
	c.ObserveReport("S1", 2, 5, jobReport(t, map[string]any{"gcode_state": "RUNNING"}))
	v, _ = c.Job("S1")
	if v.Generation != 1 || v.Revision != 1 || v.State != "RUNNING" ||
		!v.RunningSince.Equal(clk.now) || v.RunningEpoch != 0 {
		t.Fatalf("after RUNNING: gen=%d rev=%d state=%s since=%v epoch=%d",
			v.Generation, v.Revision, v.State, v.RunningSince, v.RunningEpoch)
	}

	// Fresh real reports continue settling without restarting the timer.
	clk.Advance(10 * time.Second)
	c.ObserveReport("S1", 3, 5, jobReport(t, map[string]any{"mc_percent": 50.0}))
	v, _ = c.Job("S1")
	if !v.RunningSince.Equal(clk.now.Add(-10 * time.Second)) {
		t.Fatalf("fresh real report restarted settling: since=%v", v.RunningSince)
	}

	// Leaving RUNNING resets settling and bumps the epoch.
	clk.Advance(time.Second)
	c.ObserveReport("S1", 4, 5, jobReport(t, map[string]any{"gcode_state": "PAUSE"}))
	v, _ = c.Job("S1")
	if v.Generation != 1 || !v.RunningSince.IsZero() || v.RunningEpoch != 1 {
		t.Fatalf("after PAUSE: gen=%d since=%v epoch=%d", v.Generation, v.RunningSince, v.RunningEpoch)
	}

	// The next fresh RUNNING observation restarts the timer.
	clk.Advance(time.Second)
	c.ObserveReport("S1", 5, 5, jobReport(t, map[string]any{"gcode_state": "RUNNING"}))
	v, _ = c.Job("S1")
	if v.Generation != 1 || !v.RunningSince.Equal(clk.now) || v.RunningEpoch != 1 {
		t.Fatalf("after resume: gen=%d since=%v epoch=%d", v.Generation, v.RunningSince, v.RunningEpoch)
	}

	// The PAUSED spelling is also a pause, not a new job.
	clk.Advance(time.Second)
	c.ObserveReport("S1", 6, 5, jobReport(t, map[string]any{"gcode_state": "PAUSED"}))
	v, _ = c.Job("S1")
	if v.Generation != 1 || !v.RunningSince.IsZero() || v.RunningEpoch != 2 {
		t.Fatalf("after PAUSED: gen=%d since=%v epoch=%d", v.Generation, v.RunningSince, v.RunningEpoch)
	}
}

// TestJobFirstObservationRules covers the entry rules: a first observation
// of an inactive state creates no fetchable job, while a first observed
// RUNNING is a genuine new generation whose settling starts immediately.
func TestJobFirstObservationRules(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	c.ObserveReport("S1", 1, 3, jobReport(t, map[string]any{
		"gcode_state": "IDLE", "subtask_name": "old.3mf",
	}))
	v, _ := c.Job("S1")
	if v.Active || v.Generation != 0 || v.Name != "" {
		t.Fatalf("IDLE first observation created a job: %+v", v)
	}

	c.ObserveReport("S1", 2, 3, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "benchy.3mf", "plate_idx": 2,
	}))
	v, _ = c.Job("S1")
	if !v.Active || v.Generation != 1 || v.State != "RUNNING" || v.Name != "benchy.3mf" ||
		v.PlateIndex == nil || *v.PlateIndex != 2 || v.Revision != 1 || !v.RunningSince.Equal(clk.now) {
		t.Fatalf("first RUNNING: %+v since=%v", v, v.RunningSince)
	}
}

// TestJobTerminalReportEndsWithoutMerging covers terminal precedence: the
// terminal report with an empty name, reset ids and a zero timestamp ends
// the job, retains every accepted field, and never starts a generation.
// Later metadata-only deltas are ignored while inactive, and re-entry
// starts a fresh generation without copying the sticky display filename.
func TestJobTerminalReportEndsWithoutMerging(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	c.ObserveReport("S1", 1, 4, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "project_id": 7, "task_id": 8,
		"subtask_name": "a.3mf", "gcode_start_time": 1000, "plate_idx": 3,
	}))
	v, _ := c.Job("S1")
	if v.Generation != 1 || v.Revision != 1 {
		t.Fatalf("initial: gen=%d rev=%d", v.Generation, v.Revision)
	}

	clk.Advance(time.Second)
	c.ObserveReport("S1", 2, 4, jobReport(t, map[string]any{
		"gcode_state": "FINISH", "subtask_name": "", "project_id": 0,
		"task_id": 0, "gcode_start_time": 0, "plate_idx": 0,
	}))
	v, _ = c.Job("S1")
	if v.Active {
		t.Fatal("FINISH must end the job")
	}
	if v.Generation != 1 || v.Revision != 1 || v.Name != "a.3mf" ||
		v.ProjectID != "7" || v.TaskID != "8" || v.StartedAt.IsZero() ||
		v.PlateIndex == nil || *v.PlateIndex != 3 || v.State != "FINISH" || v.RunningEpoch != 1 {
		t.Fatalf("terminal report must retain the last accepted result: %+v", v)
	}

	c.ObserveReport("S1", 3, 4, jobReport(t, map[string]any{"gcode_file": "x.3mf", "plate_idx": 9}))
	v, _ = c.Job("S1")
	if v.GCodeFile != "" || v.PlateIndex == nil || *v.PlateIndex != 3 || v.Revision != 1 {
		t.Fatalf("inactive metadata delta must be ignored entirely: %+v", v)
	}

	// A real identity-bearing report while inactive is equally inert for
	// the preview: no merge, no revision, and no settling restart from
	// the retained terminal state.
	c.ObserveReport("S1", 4, 4, jobReport(t, map[string]any{
		"subtask_name": "late.3mf", "project_id": 1,
	}))
	v, _ = c.Job("S1")
	if v.Name != "a.3mf" || v.Revision != 1 || v.Generation != 1 || !v.RunningSince.IsZero() {
		t.Fatalf("inactive real identity delta must stay inert: %+v", v)
	}

	c.ObserveReport("S1", 5, 4, jobReport(t, map[string]any{"gcode_state": "PREPARE"}))
	v, _ = c.Job("S1")
	if !v.Active || v.Generation != 2 || v.Revision != 0 || v.Name != "" ||
		v.ProjectID != "" || v.TaskID != "" || !v.StartedAt.IsZero() || v.PlateIndex != nil {
		t.Fatalf("re-entry must clear identity without the sticky display name: %+v", v)
	}
	// Detection's display filename followed the real report above; the
	// preview projection stayed on its ended generation.
	if st := c.Snapshot()[0]; st.Filename != "late.3mf" {
		t.Fatalf("display filename = %q, want the value from the real report", st.Filename)
	}
}

// TestJobStrongIdentityChangesStartGeneration covers the strong identity
// boundaries: a different valid task id or a different positive start time
// starts exactly one new generation per report, and identical reports
// change nothing.
func TestJobStrongIdentityChangesStartGeneration(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	observe := func(seq uint64, kv map[string]any) {
		c.ObserveReport("S1", seq, 2, jobReport(t, kv))
	}

	observe(1, map[string]any{"gcode_state": "RUNNING", "task_id": 10, "gcode_start_time": 5000})
	v, _ := c.Job("S1")
	if v.Generation != 1 || v.TaskID != "10" || v.Revision != 1 {
		t.Fatalf("initial: %+v", v)
	}

	observe(2, map[string]any{"gcode_state": "RUNNING", "task_id": 11, "gcode_start_time": 5000})
	v, _ = c.Job("S1")
	if v.Generation != 2 || v.TaskID != "11" || v.Revision != 1 {
		t.Fatalf("task change: %+v", v)
	}

	observe(3, map[string]any{"gcode_state": "RUNNING", "task_id": 11, "gcode_start_time": 6000})
	v, _ = c.Job("S1")
	if v.Generation != 3 || !v.StartedAt.Equal(time.Unix(6000, 0)) {
		t.Fatalf("start time change: gen=%d started=%v", v.Generation, v.StartedAt)
	}

	observe(4, map[string]any{"gcode_state": "RUNNING", "task_id": 11, "gcode_start_time": 6000})
	v, _ = c.Job("S1")
	if v.Generation != 3 || v.Revision != 1 {
		t.Fatalf("identical report must change nothing: gen=%d rev=%d", v.Generation, v.Revision)
	}
}

// TestJobStickyIdentityAcrossInvalidUpdates covers valid to invalid to
// same-valid sequences: absent ids, explicit empty strings and explicit
// nonpositive timestamps never start a generation, and the sticky strong
// identities survive them.
func TestJobStickyIdentityAcrossInvalidUpdates(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	observe := func(seq uint64, kv map[string]any) {
		c.ObserveReport("S1", seq, 2, jobReport(t, kv))
	}

	observe(1, map[string]any{
		"gcode_state": "RUNNING", "project_id": 5, "task_id": 6, "gcode_start_time": 100,
	})
	v, _ := c.Job("S1")
	if v.Generation != 1 || v.Revision != 1 {
		t.Fatalf("initial: gen=%d rev=%d", v.Generation, v.Revision)
	}

	// Absent identity fields: retained, no revision, no generation.
	observe(2, map[string]any{"gcode_state": "RUNNING"})
	v, _ = c.Job("S1")
	if v.ProjectID != "5" || v.TaskID != "6" || v.StartedAt.IsZero() ||
		v.Revision != 1 || v.Generation != 1 {
		t.Fatalf("absent fields must retain identity: %+v", v)
	}

	// The same valid values return: no new generation, no revision.
	observe(3, map[string]any{
		"gcode_state": "RUNNING", "project_id": 5, "task_id": 6, "gcode_start_time": 100,
	})
	v, _ = c.Job("S1")
	if v.Generation != 1 || v.Revision != 1 {
		t.Fatalf("same-valid must not grant anything: gen=%d rev=%d", v.Generation, v.Revision)
	}

	// An explicit empty project id clears the public field and moves the
	// revision; the sticky identity survives for boundary detection.
	observe(4, map[string]any{
		"gcode_state": "RUNNING", "project_id": "", "task_id": 6,
	})
	v, _ = c.Job("S1")
	if v.ProjectID != "" || v.TaskID != "6" || v.Revision != 2 || v.Generation != 1 {
		t.Fatalf("explicit empty id must clear only the public field: %+v", v)
	}

	// The same valid value returning restores the field without a
	// generation: valid, empty, valid never grants an attempt.
	observe(5, map[string]any{
		"gcode_state": "RUNNING", "project_id": 5, "task_id": 6,
	})
	v, _ = c.Job("S1")
	if v.Generation != 1 || v.Revision != 3 || v.ProjectID != "5" {
		t.Fatalf("same-valid return: gen=%d rev=%d project=%q",
			v.Generation, v.Revision, v.ProjectID)
	}

	// An explicit zero timestamp clears the merged field only.
	observe(6, map[string]any{"gcode_state": "RUNNING", "gcode_start_time": 0})
	v, _ = c.Job("S1")
	if !v.StartedAt.IsZero() || v.Revision != 4 || v.Generation != 1 {
		t.Fatalf("nonpositive timestamp must clear only the field: %+v", v)
	}

	// The sticky timestamp returning never grants a new generation.
	observe(7, map[string]any{"gcode_state": "RUNNING", "gcode_start_time": 100})
	v, _ = c.Job("S1")
	if v.Generation != 1 || !v.StartedAt.Equal(time.Unix(100, 0)) || v.Revision != 5 {
		t.Fatalf("sticky timestamp return: gen=%d started=%v rev=%d",
			v.Generation, v.StartedAt, v.Revision)
	}
}

// TestJobInvalidTypeValuesClearPreviewFields covers present but invalid
// values: every wrong-typed field clears its public value with a revision
// and a settling reset, while the private sticky identities keep the last
// valid values so the same valid values returning never start a
// generation.
func TestJobInvalidTypeValuesClearPreviewFields(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	c.ObserveReport("S1", 1, 1, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "project_id": 5, "task_id": 6,
		"subtask_name": "a.3mf", "gcode_file": "a.3mf", "plate_idx": 2,
		"gcode_start_time": 100,
	}))
	v, _ := c.Job("S1")
	if v.Name != "a.3mf" || v.GCodeFile != "a.3mf" || v.PlateIndex == nil ||
		*v.PlateIndex != 2 || v.StartedAt.IsZero() || v.Revision != 1 ||
		v.ProjectID != "5" || v.TaskID != "6" {
		t.Fatalf("initial: %+v", v)
	}

	c.ObserveReport("S1", 2, 1, jobReport(t, map[string]any{
		"subtask_name": 7, "gcode_file": true, "plate_idx": "later",
		"gcode_start_time": "soon", "project_id": false, "task_id": []any{1},
	}))
	v, _ = c.Job("S1")
	if v.Name != "" || v.GCodeFile != "" || v.PlateIndex != nil ||
		!v.StartedAt.IsZero() || v.ProjectID != "" || v.TaskID != "" {
		t.Fatalf("invalid values must clear their public fields: %+v", v)
	}
	if v.Generation != 1 || v.Revision != 2 {
		t.Fatalf("invalid clears must not start a generation: gen=%d rev=%d",
			v.Generation, v.Revision)
	}

	// The same valid values return without a generation: the sticky
	// identities bridged the invalid updates.
	c.ObserveReport("S1", 3, 1, jobReport(t, map[string]any{
		"project_id": 5, "task_id": 6, "subtask_name": "a.3mf",
		"plate_idx": 2, "gcode_start_time": 100,
	}))
	v, _ = c.Job("S1")
	if v.Generation != 1 || v.Revision != 3 || v.ProjectID != "5" || v.TaskID != "6" ||
		v.Name != "a.3mf" || v.GCodeFile != "" || v.PlateIndex == nil ||
		*v.PlateIndex != 2 || !v.StartedAt.Equal(time.Unix(100, 0)) {
		t.Fatalf("valid return: gen=%d rev=%d view=%+v", v.Generation, v.Revision, v)
	}
}

// TestJobMetadataOnlyDeltas covers metadata-only reports while active:
// they update preview fields and the revision, clear settling without
// restarting it, and never refresh ObsAt, ObsGen or detection freshness.
func TestJobMetadataOnlyDeltas(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	c.ObserveReport("S1", 1, 3, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "a.3mf",
	}))
	before, _ := c.Job("S1")
	sessionBefore, _ := c.Session("S1")

	clk.Advance(2 * time.Second)
	c.ObserveReport("S1", 2, 3, jobReport(t, map[string]any{"gcode_file": "part.3mf", "plate_idx": 1}))
	v, _ := c.Job("S1")
	if v.GCodeFile != "part.3mf" || v.PlateIndex == nil || *v.PlateIndex != 1 {
		t.Fatalf("metadata delta must merge fields: %+v", v)
	}
	if v.Revision != before.Revision+1 || v.Generation != before.Generation {
		t.Fatalf("metadata delta: gen=%d rev=%d", v.Generation, v.Revision)
	}
	if !v.ObsAt.Equal(before.ObsAt) || v.ObsGen != before.ObsGen {
		t.Fatalf("metadata delta refreshed observation evidence: at=%v gen=%d", v.ObsAt, v.ObsGen)
	}
	if !v.RunningSince.IsZero() {
		t.Fatal("metadata-only revision must clear settling without starting it")
	}
	sessionAfter, _ := c.Session("S1")
	if !sessionAfter.ObsAt.Equal(sessionBefore.ObsAt) || sessionAfter.Obs != sessionBefore.Obs {
		t.Fatal("metadata-only delta refreshed detection freshness")
	}

	// A fresh real RUNNING report on the same generation restarts settling.
	clk.Advance(time.Second)
	c.ObserveReport("S1", 3, 3, jobReport(t, map[string]any{"gcode_state": "RUNNING"}))
	v, _ = c.Job("S1")
	if !v.RunningSince.Equal(clk.now) {
		t.Fatalf("fresh RUNNING must restart settling: since=%v", v.RunningSince)
	}
}

// TestJobReconnectTemperatureStreamIsIneligible covers the reconnect rule:
// a stale generation RUNNING followed only by temperature reports on the
// new connection never settles; generation and revision stay put so the
// reconnect alone grants no new attempt. A fresh RUNNING report on the new
// connection starts its own full settling interval.
func TestJobReconnectTemperatureStreamIsIneligible(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	c.ObserveReport("S1", 1, 5, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "a.3mf", "task_id": 1,
	}))
	v, _ := c.Job("S1")
	if !v.RunningSince.Equal(clk.now) || v.RunningEpoch != 0 {
		t.Fatalf("initial settling: since=%v epoch=%d", v.RunningSince, v.RunningEpoch)
	}

	// More than 60 seconds of temperature-only reports on the new
	// connection. Temperature reports are real reports: they stamp the
	// observation evidence but never the state generation.
	for i := 0; i < 70; i++ {
		clk.Advance(time.Second)
		c.ObserveReport("S1", uint64(i+2), 6, jobReport(t, map[string]any{"nozzle_temper": 220.0}))
	}
	v, _ = c.Job("S1")
	if !v.RunningSince.IsZero() {
		t.Fatalf("stale generation RUNNING must not settle: since=%v", v.RunningSince)
	}
	if v.StateGen != 5 {
		t.Fatalf("temperature stream stamped the state generation: %d", v.StateGen)
	}
	if v.RunningEpoch != 1 {
		t.Fatalf("epoch = %d, want exactly one reset at the generation change", v.RunningEpoch)
	}
	if v.ObsGen != 6 {
		t.Fatalf("temperature reports must stamp the observation generation: %d", v.ObsGen)
	}
	if v.Generation != 1 || v.Revision != 1 {
		t.Fatalf("reconnect must not grant a new attempt: gen=%d rev=%d", v.Generation, v.Revision)
	}

	c.ObserveReport("S1", 72, 6, jobReport(t, map[string]any{"gcode_state": "RUNNING"}))
	v, _ = c.Job("S1")
	if v.Generation != 1 || v.Revision != 1 || !v.RunningSince.Equal(clk.now) {
		t.Fatalf("fresh RUNNING after reconnect: gen=%d rev=%d since=%v",
			v.Generation, v.Revision, v.RunningSince)
	}
}

// TestJobFreshnessGapBoundary pins the gap rule against the injected clock:
// a gap of exactly the window stays fresh, one second past it resets
// settling and the fresh report restarts the timer.
func TestJobFreshnessGapBoundary(t *testing.T) {
	if previewFreshnessGap != 15*time.Second {
		t.Fatalf("previewFreshnessGap = %v, must mirror printerview.FreshnessWindow", previewFreshnessGap)
	}
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	c.ObserveReport("S1", 1, 1, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "a.3mf",
	}))
	started := clk.now

	clk.Advance(15 * time.Second)
	c.ObserveReport("S1", 2, 1, jobReport(t, map[string]any{"mc_percent": 10.0}))
	v, _ := c.Job("S1")
	if v.RunningEpoch != 0 || !v.RunningSince.Equal(started) {
		t.Fatalf("exact window gap must stay fresh: epoch=%d since=%v", v.RunningEpoch, v.RunningSince)
	}

	clk.Advance(16 * time.Second)
	c.ObserveReport("S1", 3, 1, jobReport(t, map[string]any{"mc_percent": 11.0}))
	v, _ = c.Job("S1")
	if v.RunningEpoch != 1 {
		t.Fatalf("gap past the window must bump the epoch: %d", v.RunningEpoch)
	}
	if !v.RunningSince.Equal(clk.now) {
		t.Fatalf("fresh report after the gap must restart the timer: since=%v", v.RunningSince)
	}
}

// TestJobLocalPrintWithoutIDsAndEnrichment covers local prints: missing
// ids never prevent or start a job, and missing-to-known enrichment is a
// revision, not a new generation. The string "0" is a valid local id.
func TestJobLocalPrintWithoutIDsAndEnrichment(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	c.ObserveReport("S1", 1, 1, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "local.3mf",
	}))
	v, _ := c.Job("S1")
	if !v.Active || v.ProjectID != "" || v.TaskID != "" || v.Generation != 1 {
		t.Fatalf("local print without ids: %+v", v)
	}

	clk.Advance(time.Second)
	c.ObserveReport("S1", 2, 1, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "local.3mf",
	}))
	v, _ = c.Job("S1")
	if v.Generation != 1 || v.Revision != 1 {
		t.Fatalf("unchanged report: gen=%d rev=%d", v.Generation, v.Revision)
	}

	clk.Advance(time.Second)
	c.ObserveReport("S1", 3, 1, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "project_id": 0,
	}))
	v, _ = c.Job("S1")
	if v.Generation != 1 || v.Revision != 2 || v.ProjectID != "0" {
		t.Fatalf("enrichment: gen=%d rev=%d project=%q", v.Generation, v.Revision, v.ProjectID)
	}
}

// TestJobCoalescedPrepareAndIdentityChangeOnce covers the single-boundary
// rule: one report carrying both the preparation transition and a changed
// strong identity increments the generation exactly once and inherits no
// settling evidence.
func TestJobCoalescedPrepareAndIdentityChangeOnce(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	c.ObserveReport("S1", 1, 1, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "task_id": 1, "subtask_name": "a.3mf",
	}))
	v, _ := c.Job("S1")
	if v.Generation != 1 || !v.RunningSince.Equal(clk.now) {
		t.Fatalf("initial: gen=%d since=%v", v.Generation, v.RunningSince)
	}

	clk.Advance(time.Second)
	c.ObserveReport("S1", 2, 1, jobReport(t, map[string]any{
		"gcode_state": "PREPARE", "task_id": 2, "subtask_name": "b.3mf",
	}))
	v, _ = c.Job("S1")
	if v.Generation != 2 || v.TaskID != "2" || v.Name != "b.3mf" || v.Revision != 1 {
		t.Fatalf("coalesced boundary: %+v", v)
	}
	if !v.RunningSince.IsZero() || v.RunningEpoch != 1 {
		t.Fatalf("new generation must not inherit settling: since=%v epoch=%d",
			v.RunningSince, v.RunningEpoch)
	}
}

// TestJobPreparationTransitions covers the state graph: SLICING follows
// PREPARE and SLICING returns to PREPARE inside one generation, while
// RUNNING returning to PREPARE restarts the job.
func TestJobPreparationTransitions(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	observe := func(seq uint64, state string) {
		c.ObserveReport("S1", seq, 1, jobReport(t, map[string]any{
			"gcode_state": state, "task_id": 1,
		}))
	}

	observe(1, "PREPARE")
	clk.Advance(time.Second)
	observe(2, "SLICING")
	v, _ := c.Job("S1")
	if v.Generation != 1 || v.State != "SLICING" {
		t.Fatalf("PREPARE to SLICING: gen=%d state=%s", v.Generation, v.State)
	}

	clk.Advance(time.Second)
	observe(3, "PREPARE")
	v, _ = c.Job("S1")
	if v.Generation != 1 {
		t.Fatalf("SLICING to PREPARE must stay one generation: %d", v.Generation)
	}

	clk.Advance(time.Second)
	observe(4, "RUNNING")
	clk.Advance(time.Second)
	observe(5, "PREPARE")
	v, _ = c.Job("S1")
	if v.Generation != 2 || !v.RunningSince.IsZero() {
		t.Fatalf("RUNNING to PREPARE must restart the job: gen=%d since=%v",
			v.Generation, v.RunningSince)
	}
}

// TestJobCorrectionRevisesWithoutNewGeneration covers the attempt rule: a
// name or plate correction after settling invalidates the result and
// settling but never grants a second attempt, because the generation stays
// put while the revision moves.
func TestJobCorrectionRevisesWithoutNewGeneration(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)

	c.ObserveReport("S1", 1, 1, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "wrong.3mf", "plate_idx": 1,
	}))
	v, _ := c.Job("S1")
	if v.Generation != 1 || v.Revision != 1 || !v.RunningSince.Equal(clk.now) {
		t.Fatalf("initial: %+v", v)
	}
	gen, epoch := v.Generation, v.RunningEpoch

	// A real name correction mid-settling: revision moves, settling
	// restarts from this report, generation stays.
	clk.Advance(5 * time.Second)
	c.ObserveReport("S1", 2, 1, jobReport(t, map[string]any{"subtask_name": "right.3mf"}))
	v, _ = c.Job("S1")
	if v.Generation != gen || v.Revision != 2 || v.RunningEpoch != epoch+1 {
		t.Fatalf("name correction: gen=%d rev=%d epoch=%d", v.Generation, v.Revision, v.RunningEpoch)
	}
	if !v.RunningSince.Equal(clk.now) {
		t.Fatalf("corrected RUNNING report must restart settling: since=%v", v.RunningSince)
	}

	// A metadata-only file correction and an explicit plate zero clear
	// settling without starting it, still inside the same generation.
	clk.Advance(time.Second)
	c.ObserveReport("S1", 3, 1, jobReport(t, map[string]any{"gcode_file": "right.3mf", "plate_idx": 0}))
	v, _ = c.Job("S1")
	if v.Generation != gen || v.Revision != 3 || v.GCodeFile != "right.3mf" || v.PlateIndex != nil {
		t.Fatalf("file correction: %+v", v)
	}
	if !v.RunningSince.IsZero() || v.RunningEpoch != epoch+2 {
		t.Fatalf("metadata-only correction must clear settling only: since=%v epoch=%d",
			v.RunningSince, v.RunningEpoch)
	}
}

// TestJobViewCopyIsolation covers the accessor contract: unknown serials
// report false, and optional scalars are duplicated per call.
func TestJobViewCopyIsolation(t *testing.T) {
	clk := newJobClock()
	c := newJobTestCache(t, clk)
	if _, ok := c.Job("nope"); ok {
		t.Fatal("unknown serial must report false")
	}

	c.ObserveReport("S1", 1, 1, jobReport(t, map[string]any{
		"gcode_state": "RUNNING", "plate_idx": 4,
	}))
	v, _ := c.Job("S1")
	*v.PlateIndex = 99
	again, _ := c.Job("S1")
	if again.PlateIndex == nil || *again.PlateIndex != 4 {
		t.Fatal("Job must return copied optional scalars")
	}
}
