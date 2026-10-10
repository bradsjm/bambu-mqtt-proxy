package telemetry

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// newJobTestCache returns a one-printer cache for preview-job tests.
func newJobTestCache(t *testing.T) *Cache {
	t.Helper()
	return NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
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

// jobObserve merges one report on the given sequence, upstream generation,
// and payload keys.
func jobObserve(t *testing.T, c *Cache, seq, gen uint64, kv map[string]any) {
	t.Helper()
	c.ObserveReport("S1", seq, gen, jobReport(t, kv))
}

// TestJobLifecycleKeepsOneGenerationAcrossPauseResume covers the ordered
// lifecycle: PREPARE starts the generation, and the executing states
// RUNNING, PAUSE, and PAUSED all keep it while the state follows the
// last report.
func TestJobLifecycleKeepsOneGenerationAcrossPauseResume(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 5, map[string]any{
		"gcode_state": "PREPARE", "project_id": 1, "task_id": 2, "subtask_name": "a.3mf",
	})
	v, ok := c.Job("S1")
	if !ok || !v.Active || v.State != "PREPARE" || v.Generation != 1 ||
		v.Name != "a.3mf" || v.ProjectID != "1" || v.TaskID != "2" {
		t.Fatalf("after PREPARE: ok=%v view=%+v", ok, v)
	}

	for i, state := range []string{"RUNNING", "PAUSE", "RUNNING", "PAUSED", "RUNNING"} {
		jobObserve(t, c, uint64(i+2), 5, map[string]any{"gcode_state": state})
		v, _ = c.Job("S1")
		if v.Generation != 1 || !v.Active || v.State != state {
			t.Fatalf("after %s: gen=%d active=%v state=%s", state, v.Generation, v.Active, v.State)
		}
	}

	// PREPARE and SLICING move inside one generation while no executing
	// state has intervened.
	d := newJobTestCache(t)
	jobObserve(t, d, 1, 5, map[string]any{"gcode_state": "PREPARE"})
	for i, state := range []string{"SLICING", "PREPARE", "SLICING"} {
		jobObserve(t, d, uint64(i+2), 5, map[string]any{"gcode_state": state})
		v, _ = d.Job("S1")
		if v.Generation != 1 || !v.Active || v.State != state {
			t.Fatalf("after %s: gen=%d active=%v state=%s", state, v.Generation, v.Active, v.State)
		}
	}
}

// TestJobFirstObservationRules covers the entry rules: a first observation
// of an inactive state creates no job, while a first observed RUNNING is
// a genuine new generation.
func TestJobFirstObservationRules(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 3, map[string]any{
		"gcode_state": "IDLE", "subtask_name": "old.3mf",
	})
	v, _ := c.Job("S1")
	if v.Active || v.Generation != 0 {
		t.Fatalf("IDLE first observation created a job: %+v", v)
	}
	// The identity still merges as the last-reported value, without a job.
	if v.Name != "old.3mf" || v.State != "IDLE" {
		t.Fatalf("IDLE first observation did not merge: %+v", v)
	}

	jobObserve(t, c, 2, 3, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "benchy.3mf", "plate_idx": 2,
	})
	v, _ = c.Job("S1")
	if !v.Active || v.Generation != 1 || v.State != "RUNNING" || v.Name != "benchy.3mf" ||
		v.PlateIndex == nil || *v.PlateIndex != 2 {
		t.Fatalf("first RUNNING: %+v", v)
	}
}

// TestJobTerminalReportMergesAndEndsWithoutNewGeneration covers terminal
// precedence: the terminal report ends the job and its explicit values
// become the last-reported ones, but no new generation starts.
func TestJobTerminalReportMergesAndEndsWithoutNewGeneration(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 4, map[string]any{
		"gcode_state": "RUNNING", "project_id": 7, "task_id": 8,
		"subtask_name": "a.3mf", "gcode_start_time": 1000, "plate_idx": 3,
	})
	v, _ := c.Job("S1")
	if v.Generation != 1 || !v.Active {
		t.Fatalf("initial: %+v", v)
	}

	jobObserve(t, c, 2, 4, map[string]any{
		"gcode_state": "FINISH", "subtask_name": "", "project_id": 0,
		"task_id": 0, "gcode_start_time": 0, "plate_idx": 0,
	})
	v, _ = c.Job("S1")
	if v.Active {
		t.Fatal("FINISH must end the job")
	}
	if v.Generation != 1 || v.State != "FINISH" {
		t.Fatalf("terminal report started a generation: %+v", v)
	}
	// Explicit terminal values merge: the reset is the new last-reported
	// truth. Only the numeric id 0 survives as the valid id "0".
	if v.Name != "" || v.ProjectID != "0" || v.TaskID != "0" ||
		!v.StartedAt.IsZero() || v.PlateIndex != nil {
		t.Fatalf("terminal values did not merge: %+v", v)
	}
	// The sticky strong identities survive the terminal explicit values,
	// so the same valid values returning never start a generation.
	jobObserve(t, c, 3, 4, map[string]any{
		"gcode_state": "RUNNING", "project_id": 7, "task_id": 8,
		"subtask_name": "a.3mf", "gcode_start_time": 1000, "plate_idx": 3,
	})
	v, _ = c.Job("S1")
	if !v.Active || v.Generation != 2 {
		t.Fatalf("re-entry after the sticky terminal reset: %+v", v)
	}
}

// TestJobStrongIdentityChangesStartGeneration covers the strong identity
// boundaries: a different valid task id, a different positive start time,
// or a new non-empty name over a known one starts exactly one new
// generation per report, and identical reports change nothing.
func TestJobStrongIdentityChangesStartGeneration(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 2, map[string]any{
		"gcode_state": "RUNNING", "task_id": 10, "gcode_start_time": 5000, "subtask_name": "a.3mf",
	})
	v, _ := c.Job("S1")
	if v.Generation != 1 || v.TaskID != "10" {
		t.Fatalf("initial: %+v", v)
	}

	jobObserve(t, c, 2, 2, map[string]any{"gcode_state": "RUNNING", "task_id": 11, "gcode_start_time": 5000})
	v, _ = c.Job("S1")
	if v.Generation != 2 || v.TaskID != "11" {
		t.Fatalf("task change: %+v", v)
	}

	jobObserve(t, c, 3, 2, map[string]any{"gcode_state": "RUNNING", "task_id": 11, "gcode_start_time": 6000})
	v, _ = c.Job("S1")
	if v.Generation != 3 || !v.StartedAt.Equal(time.Unix(6000, 0)) {
		t.Fatalf("start time change: gen=%d started=%v", v.Generation, v.StartedAt)
	}

	jobObserve(t, c, 4, 2, map[string]any{"gcode_state": "RUNNING", "task_id": 11, "gcode_start_time": 6000, "subtask_name": "b.3mf"})
	v, _ = c.Job("S1")
	if v.Generation != 4 || v.Name != "b.3mf" {
		t.Fatalf("name change: %+v", v)
	}

	jobObserve(t, c, 5, 2, map[string]any{"gcode_state": "RUNNING", "task_id": 11, "gcode_start_time": 6000, "subtask_name": "b.3mf"})
	v, _ = c.Job("S1")
	if v.Generation != 4 {
		t.Fatalf("identical report must change nothing: gen=%d", v.Generation)
	}

	// A gcode_file change never starts a generation: it only merges.
	jobObserve(t, c, 6, 2, map[string]any{"gcode_file": "other.3mf"})
	v, _ = c.Job("S1")
	if v.Generation != 4 || v.GCodeFile != "other.3mf" {
		t.Fatalf("file change must merge without a boundary: %+v", v)
	}
}

// TestJobStickyIdentityAcrossInvalidUpdates covers valid to invalid to
// same-valid sequences: absent ids, explicit empty strings and explicit
// nonpositive timestamps never start a generation, and the sticky strong
// identities survive them.
func TestJobStickyIdentityAcrossInvalidUpdates(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 2, map[string]any{
		"gcode_state": "RUNNING", "project_id": 5, "task_id": 6, "gcode_start_time": 100,
	})
	v, _ := c.Job("S1")
	if v.Generation != 1 {
		t.Fatalf("initial: gen=%d", v.Generation)
	}

	// Absent identity fields: retained, no generation.
	jobObserve(t, c, 2, 2, map[string]any{"gcode_state": "RUNNING"})
	v, _ = c.Job("S1")
	if v.ProjectID != "5" || v.TaskID != "6" || v.StartedAt.IsZero() || v.Generation != 1 {
		t.Fatalf("absent fields must retain identity: %+v", v)
	}

	// The same valid values return: no new generation.
	jobObserve(t, c, 3, 2, map[string]any{
		"gcode_state": "RUNNING", "project_id": 5, "task_id": 6, "gcode_start_time": 100,
	})
	v, _ = c.Job("S1")
	if v.Generation != 1 {
		t.Fatalf("same-valid must not grant anything: gen=%d", v.Generation)
	}

	// An explicit empty project id clears the public field; the sticky
	// identity survives for boundary detection.
	jobObserve(t, c, 4, 2, map[string]any{
		"gcode_state": "RUNNING", "project_id": "", "task_id": 6,
	})
	v, _ = c.Job("S1")
	if v.ProjectID != "" || v.TaskID != "6" || v.Generation != 1 {
		t.Fatalf("explicit empty id must clear only the public field: %+v", v)
	}

	// The same valid value returning restores the field without a
	// generation: valid, empty, valid never grants an attempt.
	jobObserve(t, c, 5, 2, map[string]any{
		"gcode_state": "RUNNING", "project_id": 5, "task_id": 6,
	})
	v, _ = c.Job("S1")
	if v.Generation != 1 || v.ProjectID != "5" {
		t.Fatalf("same-valid return: gen=%d project=%q", v.Generation, v.ProjectID)
	}

	// An explicit zero timestamp clears the merged field only.
	jobObserve(t, c, 6, 2, map[string]any{"gcode_state": "RUNNING", "gcode_start_time": 0})
	v, _ = c.Job("S1")
	if !v.StartedAt.IsZero() || v.Generation != 1 {
		t.Fatalf("nonpositive timestamp must clear only the field: %+v", v)
	}

	// The sticky timestamp returning never grants a new generation.
	jobObserve(t, c, 7, 2, map[string]any{"gcode_state": "RUNNING", "gcode_start_time": 100})
	v, _ = c.Job("S1")
	if v.Generation != 1 || !v.StartedAt.Equal(time.Unix(100, 0)) {
		t.Fatalf("sticky timestamp return: gen=%d started=%v", v.Generation, v.StartedAt)
	}
}

// TestJobInvalidTypeValuesClearPreviewFields covers present but invalid
// values: every wrong-typed field clears its public value, while the
// private sticky identities keep the last valid values so the same valid
// values returning never start a generation.
func TestJobInvalidTypeValuesClearPreviewFields(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "project_id": 5, "task_id": 6,
		"subtask_name": "a.3mf", "gcode_file": "a.3mf", "plate_idx": 2,
		"gcode_start_time": 100,
	})
	v, _ := c.Job("S1")
	if v.Name != "a.3mf" || v.GCodeFile != "a.3mf" || v.PlateIndex == nil ||
		*v.PlateIndex != 2 || v.StartedAt.IsZero() ||
		v.ProjectID != "5" || v.TaskID != "6" {
		t.Fatalf("initial: %+v", v)
	}

	jobObserve(t, c, 2, 1, map[string]any{
		"subtask_name": 7, "gcode_file": true, "plate_idx": "later",
		"gcode_start_time": "soon", "project_id": false, "task_id": []any{1},
	})
	v, _ = c.Job("S1")
	if v.Name != "" || v.GCodeFile != "" || v.PlateIndex != nil ||
		!v.StartedAt.IsZero() || v.ProjectID != "" || v.TaskID != "" {
		t.Fatalf("invalid values must clear their public fields: %+v", v)
	}
	if v.Generation != 1 {
		t.Fatalf("invalid clears must not start a generation: gen=%d", v.Generation)
	}

	// The same valid values return without a generation: the sticky
	// identities bridged the invalid updates. gcode_file has no sticky
	// identity, so it stays cleared until the printer reports it.
	jobObserve(t, c, 3, 1, map[string]any{
		"project_id": 5, "task_id": 6, "subtask_name": "a.3mf",
		"plate_idx": 2, "gcode_start_time": 100,
	})
	v, _ = c.Job("S1")
	if v.Generation != 1 || v.ProjectID != "5" || v.TaskID != "6" ||
		v.Name != "a.3mf" || v.GCodeFile != "" || v.PlateIndex == nil ||
		*v.PlateIndex != 2 || !v.StartedAt.Equal(time.Unix(100, 0)) {
		t.Fatalf("valid return: gen=%d view=%+v", v.Generation, v)
	}
}

// TestJobMetadataOnlyDeltas covers metadata-only reports while active:
// they merge preview fields and start generations on strong identity
// changes, and never refresh session obs evidence.
func TestJobMetadataOnlyDeltas(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 3, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "a.3mf", "task_id": 10, "gcode_start_time": 5000,
	})
	before, _ := c.Job("S1")
	sessionBefore, _ := c.Session("S1")

	// gcode_file is not a strong identity, so the metadata delta merges
	// without a boundary.
	jobObserve(t, c, 2, 3, map[string]any{"gcode_file": "part.3mf", "plate_idx": 1})
	v, _ := c.Job("S1")
	if v.GCodeFile != "part.3mf" || v.PlateIndex == nil || *v.PlateIndex != 1 {
		t.Fatalf("metadata delta must merge fields: %+v", v)
	}
	if v.Generation != before.Generation {
		t.Fatalf("metadata delta: gen=%d", v.Generation)
	}
	sessionAfter, _ := c.Session("S1")
	if !sessionAfter.ObsAt.Equal(sessionBefore.ObsAt) || sessionAfter.Obs != sessionBefore.Obs {
		t.Fatal("metadata-only delta refreshed session obs evidence")
	}

	// A metadata-only change to a known strong identity (start time
	// carries no real-print marker) starts a generation without touching
	// the session counters.
	jobObserve(t, c, 3, 3, map[string]any{"gcode_start_time": 6000})
	v, _ = c.Job("S1")
	if v.Generation != before.Generation+1 || !v.StartedAt.Equal(time.Unix(6000, 0)) {
		t.Fatalf("metadata-only identity boundary: %+v", v)
	}
	sessionLater, _ := c.Session("S1")
	if sessionLater.Obs != sessionBefore.Obs || sessionLater.SessionGen != sessionBefore.SessionGen {
		t.Fatalf("metadata-only boundary moved session evidence: %+v -> %+v", sessionBefore, sessionLater)
	}
}

// TestJobReconnectTemperatureStreamKeepsJob covers the reconnect rule: a
// temperature stream on a new connection keeps the job, its generation,
// and its identity untouched. No state restatement is needed.
func TestJobReconnectTemperatureStreamKeepsJob(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 5, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "a.3mf", "task_id": 1,
	})
	v, _ := c.Job("S1")
	if !v.Active || v.Generation != 1 || v.State != "RUNNING" {
		t.Fatalf("initial: %+v", v)
	}

	// More than 60 seconds of temperature-only reports on the new
	// connection. The job keeps every last-reported value.
	for i := 0; i < 70; i++ {
		jobObserve(t, c, uint64(i+2), 6, map[string]any{"nozzle_temper": 220.0})
	}
	v, _ = c.Job("S1")
	if v.Generation != 1 || !v.Active || v.State != "RUNNING" ||
		v.Name != "a.3mf" || v.TaskID != "1" {
		t.Fatalf("reconnect stream must keep the job: %+v", v)
	}

	jobObserve(t, c, 72, 6, map[string]any{"gcode_state": "RUNNING"})
	v, _ = c.Job("S1")
	if v.Generation != 1 || !v.Active || v.State != "RUNNING" {
		t.Fatalf("state report after reconnect: %+v", v)
	}
}

// TestJobReportGapKeepsJob covers the report-gap rule: a long gap between
// real reports changes nothing. The job keeps its generation and identity.
func TestJobReportGapKeepsJob(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "a.3mf",
	})
	jobObserve(t, c, 2, 1, map[string]any{"mc_percent": 10.0})
	v, _ := c.Job("S1")
	if v.Generation != 1 || !v.Active || v.Name != "a.3mf" {
		t.Fatalf("gap must keep the job: %+v", v)
	}
}

// TestJobLocalPrintWithoutIDsAndEnrichment covers local prints: missing
// ids never prevent or start a job, and missing-to-known enrichment
// merges without a new generation. The string "0" is a valid local id.
func TestJobLocalPrintWithoutIDsAndEnrichment(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "local.3mf",
	})
	v, _ := c.Job("S1")
	if !v.Active || v.ProjectID != "" || v.TaskID != "" || v.Generation != 1 {
		t.Fatalf("local print without ids: %+v", v)
	}

	jobObserve(t, c, 2, 1, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "local.3mf",
	})
	v, _ = c.Job("S1")
	if v.Generation != 1 {
		t.Fatalf("unchanged report: gen=%d", v.Generation)
	}

	jobObserve(t, c, 3, 1, map[string]any{
		"gcode_state": "RUNNING", "project_id": 0,
	})
	v, _ = c.Job("S1")
	if v.Generation != 1 || v.ProjectID != "0" {
		t.Fatalf("enrichment: gen=%d project=%q", v.Generation, v.ProjectID)
	}
}

// TestJobCoalescedPrepareAndIdentityChangeOnce covers the single-boundary
// rule: one report carrying both the preparation transition and a changed
// strong identity increments the generation exactly once.
func TestJobCoalescedPrepareAndIdentityChangeOnce(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "task_id": 1, "subtask_name": "a.3mf",
	})
	v, _ := c.Job("S1")
	if v.Generation != 1 {
		t.Fatalf("initial: gen=%d", v.Generation)
	}

	jobObserve(t, c, 2, 1, map[string]any{
		"gcode_state": "PREPARE", "task_id": 2, "subtask_name": "b.3mf",
	})
	v, _ = c.Job("S1")
	if v.Generation != 2 || v.TaskID != "2" || v.Name != "b.3mf" {
		t.Fatalf("coalesced boundary: %+v", v)
	}
}

// TestJobPreparationTransitions covers the state graph: SLICING follows
// PREPARE and SLICING returns to PREPARE inside one generation, while
// RUNNING returning to PREPARE restarts the job.
func TestJobPreparationTransitions(t *testing.T) {
	c := newJobTestCache(t)

	observe := func(seq uint64, state string) {
		jobObserve(t, c, seq, 1, map[string]any{
			"gcode_state": state, "task_id": 1,
		})
	}

	observe(1, "PREPARE")
	observe(2, "SLICING")
	v, _ := c.Job("S1")
	if v.Generation != 1 || v.State != "SLICING" {
		t.Fatalf("PREPARE to SLICING: gen=%d state=%s", v.Generation, v.State)
	}

	observe(3, "PREPARE")
	v, _ = c.Job("S1")
	if v.Generation != 1 {
		t.Fatalf("SLICING to PREPARE must stay one generation: %d", v.Generation)
	}

	observe(4, "RUNNING")
	observe(5, "PREPARE")
	v, _ = c.Job("S1")
	if v.Generation != 2 || v.State != "PREPARE" {
		t.Fatalf("RUNNING to PREPARE must restart the job: gen=%d state=%s", v.Generation, v.State)
	}
}

// TestJobCorrectionMergesWithoutNewGeneration covers the identity rule: a
// name, file, or plate correction merges into the same generation. A new
// non-empty name over a known one is the one correction that starts a
// generation, exactly like any other strong identity change.
func TestJobCorrectionMergesWithoutNewGeneration(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "wrong.3mf", "plate_idx": 1,
	})
	v, _ := c.Job("S1")
	if v.Generation != 1 || v.Name != "wrong.3mf" {
		t.Fatalf("initial: %+v", v)
	}
	gen := v.Generation

	// A file correction and an explicit plate zero merge inside the same
	// generation: neither is a strong identity.
	jobObserve(t, c, 2, 1, map[string]any{"gcode_file": "right.3mf", "plate_idx": 0})
	v, _ = c.Job("S1")
	if v.Generation != gen || v.GCodeFile != "right.3mf" || v.PlateIndex != nil {
		t.Fatalf("file correction: %+v", v)
	}

	// Absent fields keep their values across the correction.
	jobObserve(t, c, 3, 1, map[string]any{"mc_percent": 10.0})
	v, _ = c.Job("S1")
	if v.Generation != gen || v.Name != "wrong.3mf" || v.GCodeFile != "right.3mf" {
		t.Fatalf("absent fields must survive corrections: %+v", v)
	}
}

// TestJobViewCopyIsolation covers the accessor contract: unknown serials
// report false, and optional scalars are duplicated per call.
func TestJobViewCopyIsolation(t *testing.T) {
	c := newJobTestCache(t)
	if _, ok := c.Job("nope"); ok {
		t.Fatal("unknown serial must report false")
	}

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "plate_idx": 4,
	})
	v, _ := c.Job("S1")
	*v.PlateIndex = 99
	again, _ := c.Job("S1")
	if again.PlateIndex == nil || *again.PlateIndex != 4 {
		t.Fatal("Job must return copied optional scalars")
	}
}

// TestStartObservedTracksOriginOfGeneration pins when the proxy saw a print
// begin: never for a first report that is already RUNNING, and for every
// later lifecycle or strong-identity boundary.
func TestStartObservedTracksOriginOfGeneration(t *testing.T) {
	c := newJobTestCache(t)
	jobObserve(t, c, 1, 1, map[string]any{"gcode_state": "RUNNING", "subtask_name": "a.3mf"})
	if v, _ := c.Job("S1"); v.Generation != 1 || v.StartObserved {
		t.Fatalf("first report RUNNING: %+v", v)
	}

	d := newJobTestCache(t)
	jobObserve(t, d, 1, 1, map[string]any{"gcode_state": "IDLE"})
	jobObserve(t, d, 2, 1, map[string]any{"gcode_state": "RUNNING", "subtask_name": "a.3mf", "project_id": 1})
	if v, _ := d.Job("S1"); v.Generation != 1 || !v.StartObserved {
		t.Fatalf("IDLE then RUNNING: %+v", v)
	}
	jobObserve(t, d, 3, 1, map[string]any{"project_id": 2})
	jobObserve(t, d, 4, 1, map[string]any{"project_id": 3})
	if v, _ := d.Job("S1"); v.Generation != 3 || !v.StartObserved {
		t.Fatalf("consecutive strong boundaries: %+v", v)
	}
}

// TestJobFailedFullReportThenRunningDeltaRetainsName covers the cancel/resend
// core: a FAILED full report with a name, then a RUNNING delta without
// one. The name survives, the generation advances by one, and the
// re-entry is a print this proxy saw begin.
func TestJobFailedFullReportThenRunningDeltaRetainsName(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "Sunlu_AMS_Heater",
		"gcode_file": "Sunlu_AMS_Heater.gcode.3mf", "layer_num": 0,
	})
	jobObserve(t, c, 2, 1, map[string]any{"gcode_state": "FAILED"})
	v, _ := c.Job("S1")
	if v.Active || v.Generation != 1 {
		t.Fatalf("FAILED must end job 1: %+v", v)
	}

	jobObserve(t, c, 3, 1, map[string]any{"gcode_state": "RUNNING"})
	v, _ = c.Job("S1")
	if !v.Active || v.Generation != 2 || v.Name != "Sunlu_AMS_Heater" ||
		v.GCodeFile != "Sunlu_AMS_Heater.gcode.3mf" || !v.StartObserved {
		t.Fatalf("RUNNING delta after FAILED: %+v", v)
	}
}

// TestJobRunningToFailedWithChangedNameMergesWithoutNewGeneration covers
// terminal precedence over identity: RUNNING to FAILED with a changed name
// ends the job, merges the new name, and starts no generation.
func TestJobRunningToFailedWithChangedNameMergesWithoutNewGeneration(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "A",
	})
	jobObserve(t, c, 2, 1, map[string]any{
		"gcode_state": "FAILED", "subtask_name": "B",
	})
	v, _ := c.Job("S1")
	if v.Active || v.Generation != 1 || v.Name != "B" || v.State != "FAILED" {
		t.Fatalf("terminal with changed name: %+v", v)
	}
}

// TestJobFailedLayerZeroThenRunningDeltaExposesZero covers the idle-layer
// rule: layer 0 reported while FAILED is decision evidence at 0 after
// the re-entry RUNNING delta.
func TestJobFailedLayerZeroThenRunningDeltaExposesZero(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "FAILED", "layer_num": 0,
	})
	jobObserve(t, c, 2, 1, map[string]any{"gcode_state": "RUNNING"})
	_, s, _ := c.Print("S1")
	if layer := s.CurrentLayer(); layer == nil || *layer != 0 {
		t.Fatalf("CurrentLayer after the re-entry delta = %v, want 0", layer)
	}
}

// TestJobIdleRestartWithoutLayerHidesPreviousPositiveLayer covers the
// cross-print layer rule: layer 300 in print A, then IDLE to RUNNING
// without a layer. The session view keeps the last reported 300, but
// CurrentLayer reports unknown for the new print.
func TestJobIdleRestartWithoutLayerHidesPreviousPositiveLayer(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "layer_num": 300,
	})
	jobObserve(t, c, 2, 1, map[string]any{"gcode_state": "IDLE"})
	jobObserve(t, c, 3, 1, map[string]any{"gcode_state": "RUNNING"})
	_, s, _ := c.Print("S1")
	if s.LayerNum == nil || *s.LayerNum != 300 {
		t.Fatalf("session view lost the last reported layer: %+v", s.LayerNum)
	}
	if layer := s.CurrentLayer(); layer != nil {
		t.Fatalf("CurrentLayer = %d, want nil for the new print", *layer)
	}
}

// TestJobReconnectDeltaWithoutLayerKeepsLayer covers the reconnect rule: a
// reconnect delta without a layer keeps the last reported layer value.
func TestJobReconnectDeltaWithoutLayerKeepsLayer(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "layer_num": 12,
	})
	jobObserve(t, c, 2, 2, map[string]any{"nozzle_temper": 220.0})
	_, s, _ := c.Print("S1")
	if s.LayerNum == nil || *s.LayerNum != 12 {
		t.Fatalf("reconnect delta lost the layer: %+v", s.LayerNum)
	}
	if layer := s.CurrentLayer(); layer == nil || *layer != 12 {
		t.Fatalf("CurrentLayer after the reconnect delta = %v, want 12", layer)
	}
}

// TestJobInvalidLayerClearsPublicField covers the invalid-layer rule: a
// present but unparseable layer_num clears the public field.
func TestJobInvalidLayerClearsPublicField(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "layer_num": 7,
	})
	jobObserve(t, c, 2, 1, map[string]any{"layer_num": "bad"})
	_, s, _ := c.Print("S1")
	if s.LayerNum != nil {
		t.Fatalf("invalid layer must clear the field: %+v", s.LayerNum)
	}
	if layer := s.CurrentLayer(); layer != nil {
		t.Fatalf("CurrentLayer after the invalid layer = %d, want nil", *layer)
	}
}

// TestJobExplicitEmptyNameClearsIdentityAndDisplay covers the empty-name
// rule: an explicit empty subtask_name clears both Job.Name and the
// display filename.
func TestJobExplicitEmptyNameClearsIdentityAndDisplay(t *testing.T) {
	c := newJobTestCache(t)

	jobObserve(t, c, 1, 1, map[string]any{
		"gcode_state": "RUNNING", "subtask_name": "a.3mf",
	})
	jobObserve(t, c, 2, 1, map[string]any{"subtask_name": ""})
	j, _, _ := c.Print("S1")
	st, _ := c.State("S1")
	if j.Name != "" || st.Filename != "" {
		t.Fatalf("explicit empty name: job=%q display=%q", j.Name, st.Filename)
	}
	if j.Generation != 1 {
		t.Fatalf("explicit empty name must not start a generation: gen=%d", j.Generation)
	}
}
