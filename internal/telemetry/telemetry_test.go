package telemetry

import (
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"reflect"
	"testing"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
)

func TestObserveReportRecordsPrintAndAlertTransitions(t *testing.T) {
	printers := []config.Printer{{Serial: "S1", Name: "Shop"}}
	c := NewCache(printers, slog.New(slog.NewTextHandler(io.Discard, nil)))
	log := activity.New(printers)
	c.SetActivity(log)
	report := func(seq uint64, payload string) {
		t.Helper()
		c.ObserveReport("S1", seq, 1, []byte(payload))
	}
	report(1, `{"print":{"gcode_state":"RUNNING","subtask_name":"benchy.3mf"}}`)
	report(2, `{"print":{"gcode_state":"PAUSE"}}`)
	report(3, `{"print":{"gcode_state":"RUNNING"}}`)
	report(4, `{"print":{"gcode_state":"FINISH"}}`)
	report(5, `{"print":{"print_error":50348044,"hms":[{"attr":50331904,"code":131079}]}}`)
	report(6, `{"print":{"print_error":0,"hms":[]}}`)

	events := log.Recent("S1")
	kinds := make([]string, len(events))
	for i, event := range events {
		kinds[len(events)-1-i] = event.Kind
	}
	want := []string{
		"state_initial", "print_paused", "print_resumed", "print_finished",
		"hms_alert", "print_error", "alerts_cleared",
	}
	if len(kinds) != len(want) {
		t.Fatalf("event kinds = %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("event kinds = %v, want %v", kinds, want)
		}
	}
}

func TestMergeErrorsAndHMS(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Garage"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Observe("S1", []byte(`{"print":{"print_error":50348044,"hms":[{"attr":50331904,"code":131079},{"attr":"not-a-number","code":1}]}}`))
	st := c.Snapshot()[0]
	if st.Name != "Garage" || st.obsAt.IsZero() {
		t.Fatalf("name/last report = %q/%v", st.Name, st.obsAt)
	}
	if st.PrintError != 50348044 {
		t.Fatalf("print_error = %d", st.PrintError)
	}
	if len(st.HMS) != 1 || st.HMS[0].ID() != "HMS_0300_0100_0002_0007" || st.HMS[0].Severity() != "serious" {
		t.Fatalf("hms = %+v", st.HMS)
	}

	// A report without the keys keeps them; explicit clears replace them.
	c.Observe("S1", []byte(`{"print":{"mc_percent":5}}`))
	if st := c.Snapshot()[0]; st.PrintError == 0 || len(st.HMS) != 1 {
		t.Fatalf("absent keys must not clear errors: %+v", st)
	}
	c.Observe("S1", []byte(`{"print":{"print_error":0,"hms":[]}}`))
	if st := c.Snapshot()[0]; st.PrintError != 0 || len(st.HMS) != 0 {
		t.Fatalf("explicit clear failed: %+v", st)
	}
}

func TestSessionEpochTracksStateBoundaries(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	report := func(state, project, task, sub string) []byte {
		return []byte(`{"print":{"gcode_state":"` + state + `","project_id":` + project +
			`,"task_id":` + task + `,"subtask_name":"` + sub + `","layer_num":7}}`)
	}
	c.ObserveReport("S1", 1, 5, report("RUNNING", "1", "2", "a.gcode"))
	c.ObserveReport("S1", 2, 5, report("PAUSE", "1", "2", "a.gcode"))
	v, _ := c.Session("S1")
	if !v.Active || v.State != "PAUSE" || v.Epoch != 1 {
		t.Fatalf("after PAUSE: active/state/epoch = %v/%q/%d", v.Active, v.State, v.Epoch)
	}
	c.ObserveReport("S1", 3, 5, report("FINISH", "1", "2", "a.gcode"))
	v, _ = c.Session("S1")
	// The ended print's identity is repopulated from the FINISH report and
	// stays until the next report changes it; only Active and the epoch
	// mark the boundary.
	if v.Active || v.Epoch != 2 || v.Cookie != "1-2-a.gcode" {
		t.Fatalf("after FINISH: active/epoch/cookie = %v/%d/%q", v.Active, v.Epoch, v.Cookie)
	}
	c.ObserveReport("S1", 4, 5, report("RUNNING", "9", "8", "b.gcode"))
	v, _ = c.Session("S1")
	// The restart bumps once by design: the state boundary opens the new
	// session, and the same report's new identity does not bump again.
	if !v.Active || v.Epoch != 3 || v.Cookie != "9-8-b.gcode" {
		t.Fatalf("after restart: active/epoch/cookie = %v/%d/%q", v.Active, v.Epoch, v.Cookie)
	}
}

func TestSessionIdentityChangeBumpsEpochWhileActive(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	// Coalesced FINISH-to-RUNNING: the state stays RUNNING but a new print
	// identity arrives. The epoch bump is the only session boundary.
	c.ObserveReport("S1", 1, 3, []byte(`{"print":{"gcode_state":"RUNNING","project_id":1,"task_id":2,"subtask_name":"a.gcode"}}`))
	c.ObserveReport("S1", 2, 3, []byte(`{"print":{"gcode_state":"RUNNING","project_id":9,"task_id":2,"subtask_name":"a.gcode"}}`))
	v, _ := c.Session("S1")
	if v.Epoch != 1 || v.Cookie != "9-2-a.gcode" {
		t.Fatalf("epoch/cookie = %d/%q, want the identity change to bump the epoch", v.Epoch, v.Cookie)
	}
}

func TestIncompleteIdentityStaysSticky(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.ObserveReport("S1", 1, 2, []byte(`{"print":{"gcode_state":"RUNNING","subtask_name":"a.gcode"}}`))
	v, _ := c.Session("S1")
	if v.Complete || v.Epoch != 0 {
		t.Fatalf("complete/epoch = %v/%d, want an incomplete identity without a bump", v.Complete, v.Epoch)
	}
	// A new part of an incomplete identity must not look like a new print.
	c.ObserveReport("S1", 2, 2, []byte(`{"print":{"gcode_state":"RUNNING","project_id":5}}`))
	v, _ = c.Session("S1")
	if v.Complete || v.Epoch != 0 {
		t.Fatalf("complete/epoch = %v/%d, want the sticky incomplete cookie", v.Complete, v.Epoch)
	}
	c.ObserveReport("S1", 3, 2, []byte(`{"print":{"gcode_state":"RUNNING","task_id":7}}`))
	v, _ = c.Session("S1")
	if !v.Complete || v.Epoch != 0 {
		t.Fatalf("complete/epoch = %v/%d, want completion without a bump", v.Complete, v.Epoch)
	}
	// Only a change after completion counts as a new print.
	c.ObserveReport("S1", 4, 2, []byte(`{"print":{"gcode_state":"RUNNING","task_id":8}}`))
	v, _ = c.Session("S1")
	if v.Epoch != 1 {
		t.Fatalf("epoch = %d, want the post-completion identity change to bump", v.Epoch)
	}
}

func TestObserveReportDropsStragglers(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.ObserveReport("S1", 10, 4, []byte(`{"print":{"gcode_state":"RUNNING"}}`))
	// An older report arriving after a newer one (paho dispatch order) must
	// never regress the merged state.
	c.ObserveReport("S1", 5, 4, []byte(`{"print":{"gcode_state":"FINISH"}}`))
	v, _ := c.Session("S1")
	if v.State != "RUNNING" || !v.Active || v.Epoch != 0 || v.Obs != 1 {
		t.Fatalf("state/active/epoch/obs = %q/%v/%d/%d, want the straggler dropped", v.State, v.Active, v.Epoch, v.Obs)
	}
}

func TestSessionLayerKeepsLastReportedValue(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	report := func(seq, gen uint64, payload string) {
		t.Helper()
		c.ObserveReport("S1", seq, gen, []byte(payload))
	}
	report(1, 4, `{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf","layer_num":7}}`)
	v, _ := c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 7 {
		t.Fatalf("layer = %v, want the reported layer 7", v.LayerNum)
	}
	if layer := v.CurrentLayer(); layer == nil || *layer != 7 {
		t.Fatalf("CurrentLayer = %v, want 7 inside the reporting job", layer)
	}

	// A mid-print report that omits layer_num preserves the layer.
	report(2, 4, `{"print":{"gcode_state":"RUNNING","mc_percent":42.5}}`)
	v, _ = c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 7 {
		t.Fatalf("layer = %v, want layer 7 preserved without a restamp", v.LayerNum)
	}

	// A new print on the same connection keeps the last reported value in
	// the view, while CurrentLayer hides the previous job's positive layer
	// from decisions about the new print.
	report(3, 4, `{"print":{"gcode_state":"FINISH"}}`)
	report(4, 4, `{"print":{"gcode_state":"RUNNING","task_id":12,"subtask_name":"b.3mf"}}`)
	v, _ = c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 7 {
		t.Fatalf("layer = %v, want the last reported 7 kept for the new print", v.LayerNum)
	}
	if v.LayerThisJob || v.CurrentLayer() != nil {
		t.Fatalf("previous job's layer stayed current for the new print: %+v", v)
	}

	// The new print's first layer report becomes decision evidence.
	report(5, 4, `{"print":{"gcode_state":"RUNNING","layer_num":1}}`)
	v, _ = c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 1 || !v.LayerThisJob {
		t.Fatalf("layer = %v thisJob=%v, want the new print's layer 1", v.LayerNum, v.LayerThisJob)
	}
	if layer := v.CurrentLayer(); layer == nil || *layer != 1 {
		t.Fatalf("CurrentLayer = %v, want 1", layer)
	}

	// A reconnect delta without a layer keeps the last reported value,
	// and it stays decision evidence: the job never changed.
	report(6, 5, `{"print":{"gcode_state":"RUNNING"}}`)
	v, _ = c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 1 {
		t.Fatalf("layer = %v, want layer 1 kept across the reconnect", v.LayerNum)
	}
	if layer := v.CurrentLayer(); layer == nil || *layer != 1 {
		t.Fatalf("CurrentLayer = %v, want 1 across the reconnect", layer)
	}
	report(7, 5, `{"print":{"gcode_state":"RUNNING","layer_num":2}}`)
	v, _ = c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 2 {
		t.Fatalf("layer = %v, want layer 2 re-reported on the new connection", v.LayerNum)
	}

	// A present but unparseable layer_num contradicts the last valid
	// layer: the public field is cleared, and so is the display value.
	report(8, 5, `{"print":{"gcode_state":"RUNNING","layer_num":"bad"}}`)
	v, _ = c.Session("S1")
	if v.LayerNum != nil || v.CurrentLayer() != nil {
		t.Fatalf("layer = %v, want nil after an invalid layer report", v.LayerNum)
	}
	st, _ := c.State("S1")
	if st.LayerNum != nil {
		t.Fatalf("display layer = %v, want nil after the invalid report", st.LayerNum)
	}

	// The next valid layer report restores the value.
	report(9, 5, `{"print":{"gcode_state":"RUNNING","layer_num":3}}`)
	v, _ = c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 3 {
		t.Fatalf("layer = %v, want layer 3 after the next valid report", v.LayerNum)
	}

	// The display state keeps the sticky last reported value throughout.
	st, _ = c.State("S1")
	if st.LayerNum == nil || *st.LayerNum != 3 {
		t.Fatalf("display layer = %v, want the sticky last report", st.LayerNum)
	}
}

// TestSessionLayerHidesPreviousJobFromCurrentLayer is a regression for the
// layer-provenance finding: a preview-job boundary that no session or
// connection evidence catches must hide an un-re-reported layer from
// CurrentLayer. A changed positive gcode_start_time on the same cookie,
// session and upstream connection starts a new print job at layer 1, so
// the previous job's layer must not stay decision evidence, while the
// session view keeps it as the last reported value.
func TestSessionLayerHidesPreviousJobFromCurrentLayer(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	report := func(seq uint64, payload string) {
		t.Helper()
		c.ObserveReport("S1", seq, 4, []byte(payload))
	}

	// Job A runs at layer 1 and its report is decision evidence.
	report(1, `{"print":{"gcode_state":"RUNNING","project_id":1,"task_id":2,"subtask_name":"a.3mf","gcode_start_time":1000,"layer_num":1}}`)
	v, _ := c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 1 {
		t.Fatalf("layer = %v, want job A's layer 1", v.LayerNum)
	}
	if layer := v.CurrentLayer(); layer == nil || *layer != 1 {
		t.Fatalf("CurrentLayer = %v, want job A's layer 1", layer)
	}

	// A real report restamps the same identity with a different positive
	// start time and omits layer_num: the job generation advances while the
	// cookie, session generation, epoch and upstream connection stay put,
	// and job B starts at layer 1, so job A's layer must leave
	// CurrentLayer while the view keeps it as the last reported value.
	report(2, `{"print":{"gcode_start_time":2000,"mc_percent":1}}`)
	jobB, _ := c.Job("S1")
	v, _ = c.Session("S1")
	if jobB.Generation != 2 {
		t.Fatalf("job generation = %d, want 2 after the changed start time", jobB.Generation)
	}
	if v.SessionGen != 1 || v.Epoch != 0 || v.Cookie != "1-2-a.3mf" {
		t.Fatalf("session evidence moved with the job boundary: %+v", v)
	}
	if v.LayerNum == nil || *v.LayerNum != 1 {
		t.Fatalf("layer = %v, want the last reported 1 kept", v.LayerNum)
	}
	if v.LayerThisJob || v.CurrentLayer() != nil {
		t.Fatalf("job A's layer stayed decision evidence for job B: %+v", v)
	}
	if st, _ := c.State("S1"); st.LayerNum == nil || *st.LayerNum != 1 {
		t.Fatalf("display layer = %v, want the sticky display value kept", st.LayerNum)
	}

	// Job B's own layer report restores the decision evidence.
	report(3, `{"print":{"gcode_state":"RUNNING","layer_num":3}}`)
	v, _ = c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 3 {
		t.Fatalf("layer = %v, want job B's layer 3", v.LayerNum)
	}
	if layer := v.CurrentLayer(); layer == nil || *layer != 3 {
		t.Fatalf("CurrentLayer = %v, want job B's layer 3", layer)
	}

	// A later report of the same job that omits layer_num keeps it current:
	// the same start time restarts nothing.
	report(4, `{"print":{"gcode_state":"RUNNING","gcode_start_time":2000,"mc_percent":42.5}}`)
	v, _ = c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 3 {
		t.Fatalf("layer = %v, want layer 3 preserved inside job B", v.LayerNum)
	}

	// A metadata-only delta that changes the start time starts job C
	// without touching the session counters: job B's layer must leave
	// CurrentLayer even though the session evidence still matches.
	before := v
	report(5, `{"print":{"gcode_start_time":3000}}`)
	v, _ = c.Session("S1")
	if v.LayerNum == nil || *v.LayerNum != 3 {
		t.Fatalf("layer = %v, want the last reported 3 kept", v.LayerNum)
	}
	if v.LayerThisJob || v.CurrentLayer() != nil {
		t.Fatalf("job B's layer stayed decision evidence for job C: %+v", v)
	}
	if v.Obs != before.Obs || v.SessionGen != before.SessionGen || v.ObsGen != before.ObsGen {
		t.Fatalf("metadata-only delta moved session evidence: %+v -> %+v", before, v)
	}
	if st, _ := c.State("S1"); st.LayerNum == nil || *st.LayerNum != 3 {
		t.Fatalf("display layer = %v, want the sticky display value kept", st.LayerNum)
	}
}

func TestSessionViewCarriesFreshnessEvidence(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.ObserveReport("S1", 1, 42, []byte(`{"print":{"gcode_state":"RUNNING","layer_num":12}}`))
	v, ok := c.Session("S1")
	if !ok {
		t.Fatal("session missing for a configured printer")
	}
	if v.ObsGen != 42 || v.Obs != 1 || v.ObsAt.IsZero() {
		t.Fatalf("obsGen/obs/obsAt = %d/%d/%v, want the report's generation and time", v.ObsGen, v.Obs, v.ObsAt)
	}
	if v.LayerNum == nil || *v.LayerNum != 12 {
		t.Fatalf("layer = %v, want 12", v.LayerNum)
	}
	if _, ok := c.Session("NOPE"); ok {
		t.Fatal("session must not exist for an unknown serial")
	}
}

func TestSessionGenerationTracksPrintsNotBoundaries(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	report := func(state string) []byte {
		return []byte(`{"print":{"gcode_state":"` + state + `","project_id":1,"task_id":2,"subtask_name":"a.gcode"}}`)
	}
	c.ObserveReport("S1", 1, 5, report("RUNNING"))
	if v, _ := c.Session("S1"); v.SessionGen != 1 || v.Epoch != 0 || !v.Active {
		t.Fatalf("session/epoch/active = %d/%d/%v, want the first print to open session 1", v.SessionGen, v.Epoch, v.Active)
	}
	// A pause and the resume are ordinary boundaries of the same print.
	c.ObserveReport("S1", 2, 5, report("PAUSE"))
	if v, _ := c.Session("S1"); v.SessionGen != 1 || v.Epoch != 1 {
		t.Fatalf("session/epoch = %d/%d, want the pause to bump only the epoch", v.SessionGen, v.Epoch)
	}
	c.ObserveReport("S1", 3, 5, report("RUNNING"))
	if v, _ := c.Session("S1"); v.SessionGen != 1 || v.Epoch != 2 {
		t.Fatalf("session/epoch = %d/%d, want the resume to bump only the epoch", v.SessionGen, v.Epoch)
	}
	// The print ends: the session closes without a new generation.
	c.ObserveReport("S1", 4, 5, report("FINISH"))
	if v, _ := c.Session("S1"); v.Active || v.SessionGen != 1 || v.Epoch != 3 {
		t.Fatalf("active/session/epoch = %v/%d/%d, want the finish to close session 1", v.Active, v.SessionGen, v.Epoch)
	}
	// The same file prints again: the FINISH-to-RUNNING boundary opens
	// session 2 even though the identity cookie is unchanged.
	c.ObserveReport("S1", 5, 5, report("RUNNING"))
	if v, _ := c.Session("S1"); !v.Active || v.SessionGen != 2 || v.Epoch != 4 {
		t.Fatalf("active/session/epoch = %v/%d/%d, want the restart to open session 2", v.Active, v.SessionGen, v.Epoch)
	}
}

func TestStateGenStampsOnlyStateReports(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.ObserveReport("S1", 1, 7, []byte(`{"print":{"gcode_state":"RUNNING","project_id":1,"task_id":2,"subtask_name":"a.gcode"}}`))
	if v, _ := c.Session("S1"); v.StateGen != 7 || v.ObsGen != 7 {
		t.Fatalf("stateGen/obsGen = %d/%d, want the state report to stamp both", v.StateGen, v.ObsGen)
	}
	// A temperature-only delta on a newer connection refreshes the merged
	// freshness but must not make the pre-reconnect RUNNING look current.
	c.ObserveReport("S1", 2, 8, []byte(`{"print":{"nozzle_temper":220,"bed_temper":55}}`))
	if v, _ := c.Session("S1"); v.ObsGen != 8 || v.StateGen != 7 || v.State != "RUNNING" || !v.Active {
		t.Fatalf("obsGen/stateGen/state = %d/%d/%q, want the delta to refresh obs only", v.ObsGen, v.StateGen, v.State)
	}
	// An explicit state report on the new connection restamps it.
	c.ObserveReport("S1", 3, 8, []byte(`{"print":{"gcode_state":"RUNNING"}}`))
	if v, _ := c.Session("S1"); v.StateGen != 8 || v.ObsGen != 8 {
		t.Fatalf("stateGen/obsGen = %d/%d, want the new state report to restamp", v.StateGen, v.ObsGen)
	}
}

func TestCommandAckDoesNotRefreshDetectionEvidence(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	wake := c.WatchReports("S1")
	c.ObserveReport("S1", 1, 4, []byte(`{"print":{"gcode_state":"RUNNING","project_id":1,"task_id":2,"subtask_name":"a.gcode"}}`))
	before, _ := c.Session("S1")
	<-wake
	// A pause command ACK (sequence_id/command/result) carries no print
	// state: it must not refresh the freshness or session evidence that
	// authorizes detection actions.
	c.ObserveReport("S1", 2, 4, []byte(`{"print":{"sequence_id":"7","command":"pause","result":"ok"}}`))
	after, _ := c.Session("S1")
	if after.Obs != before.Obs || !after.ObsAt.Equal(before.ObsAt) ||
		after.ObsGen != before.ObsGen || after.Epoch != before.Epoch ||
		after.SessionGen != before.SessionGen || after.State != before.State {
		t.Fatalf("ACK refreshed evidence: %+v -> %+v", before, after)
	}
	select {
	case <-wake:
		t.Fatal("an ACK must not wake detection")
	default:
	}
	// A remaining-time-only delta is real state: it refreshes the merge
	// evidence but must not claim an explicit current-generation state.
	c.ObserveReport("S1", 3, 5, []byte(`{"print":{"mc_remaining_time":42}}`))
	delta, _ := c.Session("S1")
	merged := c.Snapshot()[0]
	if delta.Obs == before.Obs || delta.ObsGen != 5 || delta.StateGen != 4 || merged.RemainMin != 42 {
		t.Fatalf("remaining-time delta merge = view %+v state %+v, want refreshed obs on gen 5 with stateGen 4", delta, merged)
	}
}

func TestWatchReportsWakesOncePerReport(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	wake := c.WatchReports("S1")
	c.ObserveReport("S1", 1, 1, []byte(`{"print":{"gcode_state":"RUNNING"}}`))
	select {
	case <-wake:
	default:
		t.Fatal("no wake token after a real report")
	}
	select {
	case <-wake:
		t.Fatal("more than one token for one report")
	default:
	}
	c.ObserveReport("S1", 2, 1, []byte(`{"print":{"gcode_state":"FINISH"}}`))
	select {
	case <-wake:
	default:
		t.Fatal("no wake token after the second report")
	}
}

// TestAMSMergePreservesOmittedFields pins the delta merge by unit and slot
// id: fields a report omits keep their merged values, and the selection
// persists until a new tray_now arrives.
func TestAMSMergePreservesOmittedFields(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Observe("S1", []byte(`{"print":{"ams":{"tray_now":"0","ams":[{"id":"0","humidity_raw":12,
		"tray":[{"id":"0","tray_info_idx":"GFL99","tray_type":"PLA","tray_color":"FFFF00FF","remain":64,"state":3}]}]}}}`))
	st := c.Snapshot()[0]
	if len(st.AMS) != 1 || st.AMS[0].ID != 0 {
		t.Fatalf("ams units = %+v, want unit 0", st.AMS)
	}
	if st.AMS[0].Humidity == nil || *st.AMS[0].Humidity != 12 {
		t.Fatalf("humidity = %v, want 12", st.AMS[0].Humidity)
	}
	slot := st.AMS[0].Slots[0]
	if slot.Loaded == nil || !*slot.Loaded || slot.Material != "PLA" || slot.Color != "FFFF00FF" ||
		slot.Remain == nil || *slot.Remain != 64 || !slot.Active {
		t.Fatalf("slot 0 = %+v, want loaded PLA with 64 percent selected", slot)
	}
	for i := 1; i < 4; i++ {
		if st.AMS[0].Slots[i].Loaded != nil {
			t.Fatalf("slot %d loaded = %v, want never-reported", i, *st.AMS[0].Slots[i].Loaded)
		}
	}

	// A color-only delta preserves material, remain, humidity, and the
	// selection from earlier reports.
	c.Observe("S1", []byte(`{"print":{"ams":{"ams":[{"id":"0","tray":[{"id":"0","tray_color":"00FF00FF"}]}]}}}`))
	st = c.Snapshot()[0]
	slot = st.AMS[0].Slots[0]
	if st.AMS[0].Humidity == nil || *st.AMS[0].Humidity != 12 || slot.Material != "PLA" ||
		slot.Color != "00FF00FF" || slot.Remain == nil || *slot.Remain != 64 || !slot.Active {
		t.Fatalf("delta merge lost preserved fields: %+v", slot)
	}

	// AMS-only deltas merge display state but must never refresh the
	// detection evidence the real-print gate guards.
	v, _ := c.Session("S1")
	if v.Obs != 0 || !v.ObsAt.IsZero() || v.Epoch != 0 || v.Active || v.ObsGen != 0 {
		t.Fatalf("AMS-only delta refreshed detection evidence: %+v", v)
	}
}

// TestAMSSlotClearingOnBitfield pins that only the state bitfield clears a
// loaded slot's filament data, and that a metadata-only refresh updates
// state without touching stored metadata.
func TestAMSSlotClearingOnBitfield(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Observe("S1", []byte(`{"print":{"ams":{"ams":[{"id":"0","tray":[{"id":"2",
		"tray_info_idx":"GFC99","tray_type":"PETG","tray_color":"FF8000FF","remain":40,"state":3}]}]}}}`))
	if slot := c.Snapshot()[0].AMS[0].Slots[2]; slot.Loaded == nil || !*slot.Loaded {
		t.Fatalf("slot = %+v, want loaded", slot)
	}
	// State 2 (metadata without a spool) is removal evidence.
	c.Observe("S1", []byte(`{"print":{"ams":{"ams":[{"id":"0","tray":[{"id":"2","state":2}]}]}}}`))
	slot := c.Snapshot()[0].AMS[0].Slots[2]
	if slot.Loaded == nil || *slot.Loaded || slot.Material != "" || slot.Color != "" || slot.Remain != nil {
		t.Fatalf("slot after removal = %+v, want cleared", slot)
	}
	// A metadata-only refresh carries state only; it loads the bitfield
	// slot again without resurrecting metadata.
	c.Observe("S1", []byte(`{"print":{"ams":{"ams":[{"id":"0","tray":[{"id":"2","state":3}]}]}}}`))
	slot = c.Snapshot()[0].AMS[0].Slots[2]
	if slot.Loaded == nil || !*slot.Loaded || slot.Material != "" || slot.Color != "" {
		t.Fatalf("slot after refresh = %+v, want loaded with metadata still cleared", slot)
	}
}

// TestAMSUnknownVersusZero pins the honest distinctions: a reported zero
// remain stays zero, bogus power-on humidity stays unknown, an explicit
// "Empty" material is not a spool, and a loaded slot without material is
// unknown, not empty.
func TestAMSUnknownVersusZero(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Observe("S1", []byte(`{"print":{"ams":{"ams":[{"id":"0","humidity_raw":0,
		"tray":[{"id":"0","tray_type":"PLA","remain":0,"state":3},
			{"id":"1","tray_type":"Empty"},
			{"id":"2","tray_info_idx":"GFB99","state":3}]}]}}}`))
	unit := c.Snapshot()[0].AMS[0]
	if unit.Humidity != nil {
		t.Fatalf("humidity = %v, want unknown for the bogus zero", unit.Humidity)
	}
	zero := unit.Slots[0]
	if zero.Remain == nil || *zero.Remain != 0 {
		t.Fatalf("remain = %v, want reported zero", zero.Remain)
	}
	if empty := unit.Slots[1]; empty.Loaded == nil || *empty.Loaded {
		t.Fatalf(`"Empty" material loaded = %v, want resolved empty`, empty.Loaded)
	}
	unknown := unit.Slots[2]
	if unknown.Loaded == nil || !*unknown.Loaded || unknown.Material != "" {
		t.Fatalf("loaded slot without material = %+v, want loaded and unknown material", unknown)
	}
}

// TestSelectionOnlyForSupportedEncodings pins the selection rules: legacy
// 255 selects nothing, 254 selects the external spool, values below 80 map
// to unit>>2 / slot&3, AMS HT targets and unknown unit ids select nothing
// and never create guessed units.
func TestSelectionOnlyForSupportedEncodings(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	units := `{"id":"0","tray":[{"id":"1","tray_type":"PLA","state":3}]},` +
		`{"id":"1","tray":[{"id":"1","tray_type":"PLA","state":3}]},` +
		`{"id":"128","tray":[{"id":"0","tray_type":"PLA","state":3}]}`
	observe := func(trayNow string) {
		t.Helper()
		c.Observe("S1", []byte(`{"print":{"ams":{"tray_now":"`+trayNow+`","ams":[`+units+`]}}}`))
	}
	observe("255")
	st := c.Snapshot()[0]
	if len(st.AMS) != 2 {
		t.Fatalf("units = %+v, want the conventional two (id 128 is AMS HT)", st.AMS)
	}
	if anyActive(st.AMS) || (st.ExtSpool != nil && st.ExtSpool.Active) {
		t.Fatal("tray_now 255 must select nothing")
	}
	c.Observe("S1", []byte(`{"print":{"ams":{"tray_now":"254","ams":[]},"vt_tray":{"id":"254","tray_type":"PETG","state":0}}}`))
	if st = c.Snapshot()[0]; st.ExtSpool == nil || !st.ExtSpool.Active || anyActive(st.AMS) {
		t.Fatalf("tray_now 254 selection wrong: ext %+v", st.ExtSpool)
	}
	observe("5")
	st = c.Snapshot()[0]
	if !st.AMS[1].Slots[1].Active || anyActiveExcept(st.AMS, 1, 1) {
		t.Fatalf("tray_now 5 must select unit 1 slot 1: %+v", st.AMS)
	}
	observe("80")
	st = c.Snapshot()[0]
	if anyActive(st.AMS) || (st.ExtSpool != nil && st.ExtSpool.Active) {
		t.Fatal("an AMS HT target must not mark a selection")
	}
}

// anyActive reports whether any merged slot is marked selected.
func anyActive(units []AMSUnit) bool {
	return anyActiveExcept(units, -1, -1)
}

// anyActiveExcept reports whether any slot other than unit/slot is marked.
func anyActiveExcept(units []AMSUnit, unitID, slotID int) bool {
	for _, u := range units {
		for _, s := range u.Slots {
			if s.Active && !(u.ID == unitID && s.ID == slotID) {
				return true
			}
		}
	}
	return false
}

// TestExternalSpoolLifecycle pins the single external spool: metadata-only
// occupancy with explicit removal evidence, and no remain estimate even
// when the hardware reports a placeholder.
func TestExternalSpoolLifecycle(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Observe("S1", []byte(`{"print":{"vt_tray":{"id":"254","tray_info_idx":"GFA01","tray_type":"ABS",
		"tray_color":"000000FF","remain":99}}}`))
	ext := c.Snapshot()[0].ExtSpool
	if ext == nil || ext.Loaded == nil || !*ext.Loaded || ext.Material != "ABS" || ext.Color != "000000FF" {
		t.Fatalf("external spool = %+v, want loaded ABS", ext)
	}
	if ext.Remain != nil {
		t.Fatalf("external spool remain = %v, want no estimate", ext.Remain)
	}
	// A metadata-only vt_tray delta is the explicit removal evidence.
	c.Observe("S1", []byte(`{"print":{"vt_tray":{"id":"254","state":0}}}`))
	ext = c.Snapshot()[0].ExtSpool
	if ext.Loaded == nil || *ext.Loaded || ext.Material != "" || ext.Color != "" {
		t.Fatalf("external spool after removal = %+v, want cleared", ext)
	}
	// Later reports without vt_tray keep it cleared.
	c.Observe("S1", []byte(`{"print":{"gcode_state":"RUNNING"}}`))
	if ext = c.Snapshot()[0].ExtSpool; ext.Loaded == nil || *ext.Loaded {
		t.Fatalf("external spool resurrected: %+v", ext)
	}
}

// TestStageMergeIsDisplayOnly pins that stg_cur merges into display state
// without refreshing detection evidence, and that the raw value including
// the idle sentinel is stored for the camera projection to map.
func TestStageMergeIsDisplayOnly(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	wake := c.WatchReports("S1")
	c.Observe("S1", []byte(`{"print":{"stg_cur":4}}`))
	st := c.Snapshot()[0]
	if st.Stage == nil || *st.Stage != 4 {
		t.Fatalf("stage = %v, want 4", st.Stage)
	}
	if v, _ := c.Session("S1"); v.Obs != 0 || !v.ObsAt.IsZero() {
		t.Fatalf("stage delta refreshed detection evidence: %+v", v)
	}
	select {
	case <-wake:
		t.Fatal("a stage-only delta must not wake detection")
	default:
	}
	c.Observe("S1", []byte(`{"print":{"gcode_state":"RUNNING"}}`))
	if st = c.Snapshot()[0]; st.Stage == nil || *st.Stage != 4 {
		t.Fatalf("stage lost after a real report: %v", st.Stage)
	}
	c.Observe("S1", []byte(`{"print":{"stg_cur":255}}`))
	if st = c.Snapshot()[0]; st.Stage == nil || *st.Stage != 255 {
		t.Fatalf("idle sentinel = %v, want the raw 255 kept for projection", st.Stage)
	}
}

// TestSnapshotImmutableAcrossMerges pins copy-on-write: a published
// snapshot keeps its nested filament values even after later merges change
// or clear them.
func TestSnapshotImmutableAcrossMerges(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Observe("S1", []byte(`{"print":{"ams":{"tray_now":"0","ams":[{"id":"0",
		"tray":[{"id":"0","tray_type":"PLA","tray_color":"FFFF00FF","remain":64,"state":3}]}]}}}`))
	snap := c.Snapshot()
	// The next report changes the color and the selection.
	c.Observe("S1", []byte(`{"print":{"ams":{"tray_now":"254","ams":[{"id":"0",
		"tray":[{"id":"0","tray_color":"00FF00FF"}]}]},"vt_tray":{"id":"254","tray_type":"ABS","state":0}}}`))
	fresh := c.Snapshot()
	old := snap[0].AMS[0].Slots[0]
	now := fresh[0].AMS[0].Slots[0]
	if old.Color != "FFFF00FF" || old.Remain == nil || *old.Remain != 64 || !old.Active {
		t.Fatalf("published snapshot mutated: %+v", old)
	}
	if now.Color != "00FF00FF" || now.Active {
		t.Fatalf("fresh snapshot missed the merge: %+v", now)
	}
	if fresh[0].ExtSpool == nil || !fresh[0].ExtSpool.Active {
		t.Fatalf("fresh selection missing: %+v", fresh[0].ExtSpool)
	}
}

func TestChamberLightIsDisplayOnlyAndRecordsChanges(t *testing.T) {
	printers := []config.Printer{{Serial: "S1", Name: "Shop"}}
	c := NewCache(printers, slog.New(slog.NewTextHandler(io.Discard, nil)))
	log := activity.New(printers)
	c.SetActivity(log)
	light := func(mode string) {
		c.Observe("S1", []byte(`{"print":{"lights_report":[{"node":"work_light","mode":"flashing"},{"node":"chamber_light","mode":"`+mode+`"}]}}`))
	}
	light("on")
	st, _ := c.State("S1")
	if st.ChamberLight != "on" {
		t.Fatalf("ChamberLight = %q, want on", st.ChamberLight)
	}
	if sv, _ := c.Session("S1"); sv.Obs != 0 || sv.ChamberLight != "on" {
		t.Fatalf("session = %+v, want Obs 0 and light on", sv)
	}
	if n := len(log.Recent("S1")); n != 0 {
		t.Fatalf("first observation recorded %d events", n)
	}
	light("off")
	light("bogus")
	light("on")
	entries := log.Recent("S1")
	if len(entries) != 2 || entries[0].Kind != "chamber_light_on" || entries[1].Kind != "chamber_light_off" {
		t.Fatalf("entries = %+v, want off then on", entries)
	}
	if st, _ := c.State("S1"); st.ChamberLight != "on" {
		t.Fatalf("ChamberLight = %q", st.ChamberLight)
	}
}

func TestExtrasParse(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Observe("S1", []byte(`{"print":{"cooling_fan_speed":"15","big_fan1_speed":"0","big_fan2_speed":"8","wifi_signal":"-61dBm","gcode_start_time":"1700000000","spd_mag":124,"nozzle_diameter":"0.4","nozzle_type":"hardened_steel","sdcard":true,"hw_switch_state":1,"ipcam":{"timelapse":"enable"},"xcam":{"first_layer_inspector":true,"spaghetti_detector":false}}}`))
	st, _ := c.State("S1")
	if st.Fans.Part == nil || *st.Fans.Part != 100 || st.Fans.Aux == nil || *st.Fans.Aux != 0 || *st.Fans.Chamber != 53 || st.Fans.Heatbreak != nil {
		t.Fatalf("fans = %+v", st.Fans)
	}
	if st.WifiDBm == nil || *st.WifiDBm != -61 {
		t.Fatalf("wifi = %v", st.WifiDBm)
	}
	if st.StartedAt.Unix() != 1700000000 {
		t.Fatalf("StartedAt = %v", st.StartedAt)
	}
	if *st.SpeedPercent != 124 || *st.NozzleDiameter != 0.4 || st.NozzleType != "hardened_steel" || !*st.SDCard || !*st.ExtSpoolSensor || !*st.Timelapse || !*st.FirstLayerInspection || *st.SpaghettiDetection {
		t.Fatalf("extras = %+v", st)
	}
	// Malformed values keep the previous value; zero start clears.
	c.Observe("S1", []byte(`{"print":{"wifi_signal":"weak","cooling_fan_speed":"x","sdcard":"yes","gcode_start_time":"0"}}`))
	st, _ = c.State("S1")
	if *st.WifiDBm != -61 || *st.Fans.Part != 100 || !*st.SDCard || !st.StartedAt.IsZero() {
		t.Fatalf("after malformed delta: %+v", st)
	}
	if sv, _ := c.Session("S1"); sv.Obs != 0 {
		t.Fatalf("extras refreshed detection freshness: Obs %d", sv.Obs)
	}
}

func TestAMSSubBrandPersistsAcrossMetadataOnlyDelta(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Observe("S1", []byte(`{"print":{"ams":{"ams":[{"id":"0","temp":"24.5","tray":[{"id":"0","state":3,"tray_type":"PLA","tray_sub_brands":"PLA Basic","nozzle_temp_min":"190","nozzle_temp_max":"230"}]}]}}}`))
	c.Observe("S1", []byte(`{"print":{"ams":{"ams":[{"id":"0","tray":[{"id":"0","state":3}]}]}}}`))
	st, _ := c.State("S1")
	slot := st.AMS[0].Slots[0]
	if slot.SubBrand != "PLA Basic" || slot.NozzleTempMin == nil || *slot.NozzleTempMin != 190 || *slot.NozzleTempMax != 230 {
		t.Fatalf("slot = %+v", slot)
	}
	if st.AMS[0].Temp == nil || *st.AMS[0].Temp != 24.5 {
		t.Fatalf("unit temp = %v", st.AMS[0].Temp)
	}
	c.Observe("S1", []byte(`{"print":{"ams":{"ams":[{"id":"0","tray":[{"id":"0","state":0}]}]}}}`))
	st, _ = c.State("S1")
	if slot := st.AMS[0].Slots[0]; slot.SubBrand != "" || slot.NozzleTempMin != nil {
		t.Fatalf("empty slot kept metadata: %+v", slot)
	}
}

func TestNumberFieldRejectsNonfinite(t *testing.T) {
	obj := map[string]any{
		"n_nan":   "NaN",
		"n_inf":   "Inf",
		"n_ninf":  "-Inf",
		"n_infty": "Infinity",
		"f_nan":   math.NaN(),
		"f_inf":   math.Inf(1),
		"f_ninf":  math.Inf(-1),
		"ok_num":  12.5,
		"ok_str":  "42",
	}
	for _, key := range []string{"n_nan", "n_inf", "n_ninf", "n_infty", "f_nan", "f_inf", "f_ninf"} {
		if v, ok := numberField(obj, key); ok {
			t.Fatalf("numberField(%q) = %v, want rejected", key, v)
		}
		if _, ok := intField(obj, key); ok {
			t.Fatalf("intField(%q) accepted, want rejected", key)
		}
	}
	if v, ok := numberField(obj, "ok_num"); !ok || v != 12.5 {
		t.Fatalf("numberField(ok_num) = %v/%v, want 12.5/true", v, ok)
	}
	if v, ok := intField(obj, "ok_str"); !ok || v != 42 {
		t.Fatalf("intField(ok_str) = %v/%v, want 42/true", v, ok)
	}
}

func TestNonfiniteFieldsLeavePriorValuesAndValidJSON(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Garage"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.ObserveReport("S1", 1, 1, []byte(`{"print":{"gcode_state":"RUNNING","nozzle_temper":220.5,"bed_temper":45,"mc_percent":33.5,"print_error":0}}`))

	// Nonfinite values (string and float encodings) are ignored; valid
	// siblings in the same report still merge.
	c.ObserveReport("S1", 2, 1, []byte(`{"print":{"gcode_state":"RUNNING","nozzle_temper":"NaN","bed_temper":50,"mc_percent":"Inf","spd_lvl":"-Infinity","layer_num":7}}`))
	st := c.Snapshot()[0]
	if st.NozzleTemp == nil || *st.NozzleTemp != 220.5 {
		t.Fatalf("nozzle after NaN report = %v, want prior 220.5", st.NozzleTemp)
	}
	if st.BedTemp == nil || *st.BedTemp != 50 {
		t.Fatalf("bed = %v, want 50 from the same report", st.BedTemp)
	}
	if st.Progress != 33.5 {
		t.Fatalf("progress = %v, want prior 33.5", st.Progress)
	}
	if st.LayerNum == nil || *st.LayerNum != 7 {
		t.Fatalf("layer = %v, want 7 from the same report", st.LayerNum)
	}

	// Fleet status JSON must stay serializable after nonfinite input.
	if _, err := json.Marshal(c.Snapshot()); err != nil {
		t.Fatalf("snapshot JSON: %v", err)
	}
}

// TestPrintMatchesJobAndSession pins that Print is Job plus Session.
func TestPrintMatchesJobAndSession(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.ObserveReport("S1", 1, 4, []byte(`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf","layer_num":2}}`))
	j, s, ok := c.Print("S1")
	wj, _ := c.Job("S1")
	ws, _ := c.Session("S1")
	if !ok || !reflect.DeepEqual(j, wj) || !reflect.DeepEqual(s, ws) {
		t.Fatalf("Print = %+v %+v %v; want %+v %+v", j, s, ok, wj, ws)
	}
	if _, _, ok := c.Print("NOPE"); ok {
		t.Fatal("unknown serial reported ok")
	}
}

// TestCurrentLayerRejectsEarlierJobsPositiveLayer pins CurrentLayer.
func TestCurrentLayerRejectsEarlierJobsPositiveLayer(t *testing.T) {
	n := func(v int) *int { return &v }
	cases := []struct {
		name string
		v    SessionView
		want *int
	}{
		{"nil", SessionView{}, nil},
		{"positive older job", SessionView{LayerNum: n(300)}, nil},
		{"positive this job", SessionView{LayerNum: n(3), LayerThisJob: true}, n(3)},
		{"zero older job", SessionView{LayerNum: n(0)}, n(0)},
	}
	for _, tc := range cases {
		got := tc.v.CurrentLayer()
		if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

// TestStartEventsCarryTheirJob pins that state_initial, print_started, and
// print_preparing carry the job generation that proved them, so consumers
// can reject entries recorded against a print that has since been
// replaced. A same-cookie start-time boundary and a same-cookie name-only
// boundary each emit exactly one print_started with the new JobGen.
func TestStartEventsCarryTheirJob(t *testing.T) {
	printers := []config.Printer{{Serial: "S1", Name: "Shop"}}
	c := NewCache(printers, slog.New(slog.NewTextHandler(io.Discard, nil)))
	log := activity.New(printers)
	c.SetActivity(log)
	seq := uint64(0)
	report := func(payload string) {
		t.Helper()
		seq++
		c.ObserveReport("S1", seq, 1, []byte(payload))
	}
	kinds := func() []activity.Entry {
		events := log.Recent("S1")
		out := make([]activity.Entry, len(events))
		for i, event := range events {
			out[len(events)-1-i] = event
		}
		return out
	}

	// The first report is already RUNNING: state_initial carries job 1.
	report(`{"print":{"gcode_state":"RUNNING","project_id":1,"task_id":2,"subtask_name":"a.3mf","gcode_start_time":1000}}`)
	got := kinds()
	if len(got) != 1 || got[0].Kind != "state_initial" || got[0].Observation.JobGen != 1 {
		t.Fatalf("first events = %+v, want one state_initial with JobGen 1", got)
	}

	// A same-cookie start-time-only boundary starts job 2 while the
	// session stays put: exactly one print_started with JobGen 2.
	report(`{"print":{"gcode_start_time":2000,"mc_percent":6}}`)
	got = kinds()
	if len(got) != 2 || got[1].Kind != "print_started" || got[1].Observation.JobGen != 2 {
		t.Fatalf("after start-time boundary = %+v, want print_started with JobGen 2", got)
	}

	// A same-cookie name-only boundary starts job 3: exactly one
	// print_started with JobGen 3, naming the new print.
	report(`{"print":{"subtask_name":"b.3mf","mc_percent":7}}`)
	got = kinds()
	if len(got) != 3 || got[2].Kind != "print_started" || got[2].Observation.JobGen != 3 {
		t.Fatalf("after name boundary = %+v, want print_started with JobGen 3", got)
	}

	// A PREPARE entry after FINISH emits print_preparing with the new JobGen.
	report(`{"print":{"gcode_state":"FINISH"}}`)
	report(`{"print":{"gcode_state":"PREPARE","project_id":1,"task_id":2,"subtask_name":"c.3mf"}}`)
	got = kinds()
	last := got[len(got)-1]
	j, _, _ := c.Print("S1")
	if last.Kind != "print_preparing" || last.Observation.JobGen != j.Generation || j.Generation != 4 {
		t.Fatalf("last = %+v job=%+v, want print_preparing with JobGen 4", last, j)
	}
}

// TestCollectActivityEventsCatalog pins every telemetry catalog row against
// the real hmscodes dataset: cancellation markers suppress their own
// print_error entry, dataset severities map info to Info and warning to
// Warning, and the cleared line names titles where the dataset knows them.
func TestCollectActivityEventsCatalog(t *testing.T) {
	type want struct {
		kind, severity, message, url string
	}
	cases := []struct {
		name    string
		reports []string
		last    []want
	}{
		{
			name: "state initial lowercases the state",
			reports: []string{
				`{"print":{"gcode_state":"IDLE","subtask_name":"a.3mf"}}`,
			},
			last: []want{{"state_initial", "info", "Printer idle · a.3mf", ""}},
		},
		{
			name: "failed without an error reports failure",
			reports: []string{
				`{"print":{"gcode_state":"IDLE"}}`,
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"gcode_state":"FAILED","print_error":0}}`,
			},
			last: []want{{"print_failed", "error", "Print failed · a.3mf", ""}},
		},
		{
			name: "failed with an unknown error reports the code",
			reports: []string{
				`{"print":{"gcode_state":"IDLE"}}`,
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"gcode_state":"FAILED","print_error":3735928559}}`,
			},
			last: []want{
				{"print_failed", "error", "Print failed · a.3mf", ""},
				{"print_error", "error", "Printer error DEAD-BEEF, no description available", "https://printara3d.com/tools/bambu-error-codes/?code=DEAD-BEEF"},
			},
		},
		{
			name: "failed with a known error carries title and link",
			reports: []string{
				`{"print":{"gcode_state":"IDLE"}}`,
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"gcode_state":"FAILED","print_error":50364425}}`,
			},
			last: []want{
				{"print_failed", "error", "Print failed · a.3mf", ""},
				{"print_error", "error", "Printer error: Heated Bed Temperature Malfunction (0300-8009)", "https://printara3d.com/tools/bambu-error-codes/hms-0300-8009/"},
			},
		},
		{
			name: "info-severity error notifies at info",
			reports: []string{
				`{"print":{"gcode_state":"IDLE"}}`,
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"gcode_state":"FAILED","print_error":50364417}}`,
			},
			last: []want{
				{"print_failed", "error", "Print failed · a.3mf", ""},
				{"print_error", "info", "Printer error: Print Paused by User (0300-8001)", "https://printara3d.com/tools/bambu-error-codes/hms-0300-8001/"},
			},
		},
		{
			name: "warning-severity error notifies at warning",
			reports: []string{
				`{"print":{"gcode_state":"IDLE"}}`,
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"gcode_state":"RUNNING","print_error":50364416}}`,
			},
			last: []want{
				{"print_error", "warning", "Printer error: Print Paused Without Reason (0300-8000)", "https://printara3d.com/tools/bambu-error-codes/hms-0300-8000/"},
			},
		},
		{
			name: "cancel marker records cancelled and suppresses its error",
			reports: []string{
				`{"print":{"gcode_state":"IDLE"}}`,
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"gcode_state":"FAILED","print_error":50348044}}`,
			},
			last: []want{{"print_cancelled", "warning", "Print cancelled · a.3mf", ""}},
		},
		{
			name: "cloud cancel marker records cancelled",
			reports: []string{
				`{"print":{"gcode_state":"IDLE"}}`,
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"gcode_state":"FAILED","print_error":83902478}}`,
			},
			last: []want{{"print_cancelled", "warning", "Print cancelled · a.3mf", ""}},
		},
		{
			name: "known hms alert carries title and link",
			reports: []string{
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"hms":[{"attr":117473296,"code":65543}]}}`,
			},
			last: []want{
				{"hms_alert", "error", "Printer alert: AMS Motor Overload (HMS_0700_8010_0001_0007)", "https://printara3d.com/tools/bambu-error-codes/hms-0700-8010/"},
			},
		},
		{
			name: "unknown hms alert keeps its lookup link",
			reports: []string{
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"hms":[{"attr":50331904,"code":65544}]}}`,
			},
			last: []want{
				{"hms_alert", "error", "Printer alert HMS_0300_0100_0001_0008, no description available", "https://printara3d.com/tools/bambu-error-codes/?code=0300-0100"},
			},
		},
		{
			name: "one clear names titles and ids",
			reports: []string{
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf","print_error":50364425,"hms":[{"attr":50331904,"code":65544}]}}`,
				`{"print":{"print_error":0,"hms":[]}}`,
			},
			last: []want{
				{"alerts_cleared", "info", "Printer alerts cleared: HMS_0300_0100_0001_0008, Heated Bed Temperature Malfunction", ""},
			},
		},
		{
			name: "suppressed cancel marker clears the announced error",
			reports: []string{
				`{"print":{"gcode_state":"IDLE"}}`,
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"gcode_state":"PAUSE","print_error":50364425}}`,
				`{"print":{"gcode_state":"FAILED","print_error":50348044}}`,
				`{"print":{"print_error":0}}`,
			},
			last: []want{
				{"alerts_cleared", "info", "Printer alerts cleared: Heated Bed Temperature Malfunction", ""},
			},
		},
		{
			name: "stopped keeps its sentence",
			reports: []string{
				`{"print":{"gcode_state":"IDLE"}}`,
				`{"print":{"gcode_state":"RUNNING","subtask_name":"a.3mf"}}`,
				`{"print":{"gcode_state":"IDLE"}}`,
			},
			last: []want{{"print_stopped", "warning", "Print stopped before finishing · a.3mf", ""}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			printers := []config.Printer{{Serial: "S1", Name: "Shop"}}
			c := NewCache(printers, slog.New(slog.NewTextHandler(io.Discard, nil)))
			log := activity.New(printers)
			c.SetActivity(log)
			before := 0
			for i, payload := range tc.reports {
				before = len(log.Recent("S1"))
				c.ObserveReport("S1", uint64(i+1), 1, []byte(payload))
			}
			got := log.Recent("S1")
			// Recent returns newest first: take the tail this last report added.
			added := got[:len(got)-before]
			if len(added) != len(tc.last) {
				t.Fatalf("last report events = %+v, want %d entries", added, len(tc.last))
			}
			for i, w := range tc.last {
				e := added[len(added)-1-i]
				if e.Kind != w.kind || e.Severity != w.severity || e.Message != w.message || e.URL != w.url {
					t.Fatalf("event %d = %+v, want kind=%q severity=%q message=%q url=%q", i, e, w.kind, w.severity, w.message, w.url)
				}
			}
		})
	}
}

// TestStagesThisJobTracksTheCurrentJob pins the per-job stage history: a
// report that carries both gcode_state and stg_cur records the stage on
// the job boundary it opens, a repeated stage keeps its first-seen slot,
// and a new generation resets the history so stages from the previous job
// never linger.
func TestStagesThisJobTracksTheCurrentJob(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	report := func(seq uint64, payload string) {
		t.Helper()
		c.ObserveReport("S1", seq, 1, []byte(payload))
	}
	stages := func() []int {
		t.Helper()
		v, _ := c.Session("S1")
		return v.StagesThisJob
	}
	// Job 1 reports a homing stage in the report that opens its own job
	// generation.
	report(1, `{"print":{"gcode_state":"IDLE"}}`)
	report(2, `{"print":{"gcode_state":"PREPARE","stg_cur":14,"subtask_name":"a.3mf"}}`)
	if got := stages(); !reflect.DeepEqual(got, []int{14}) {
		t.Fatalf("stages = %v, want [14] on the opening job", got)
	}
	// A stage-only delta appends in first-seen order; a repeat reorders
	// nothing.
	report(3, `{"print":{"stg_cur":1}}`)
	report(4, `{"print":{"stg_cur":14}}`)
	if got := stages(); !reflect.DeepEqual(got, []int{14, 1}) {
		t.Fatalf("stages = %v, want [14 1]", got)
	}
	// The next job starts without reporting a stage: the history resets,
	// and the previous job's stages do not linger.
	report(5, `{"print":{"gcode_state":"FINISH"}}`)
	report(6, `{"print":{"gcode_state":"RUNNING","subtask_name":"b.3mf"}}`)
	j, _ := c.Job("S1")
	if j.Generation != 2 {
		t.Fatalf("job generation = %d, want 2", j.Generation)
	}
	if got := stages(); len(got) != 0 {
		t.Fatalf("stages = %v, want the history reset on the new job", got)
	}
	// The new job's own stage report starts a fresh history.
	report(7, `{"print":{"stg_cur":13}}`)
	if got := stages(); !reflect.DeepEqual(got, []int{13}) {
		t.Fatalf("stages = %v, want [13] for the new job", got)
	}
}

// TestStagesThisJobExposesACopy pins that the stage history projection is a
// copy: mutating the reported slice cannot corrupt the cached history.
func TestStagesThisJobExposesACopy(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.ObserveReport("S1", 1, 1, []byte(`{"print":{"gcode_state":"IDLE"}}`))
	c.ObserveReport("S1", 2, 1, []byte(`{"print":{"gcode_state":"PREPARE","stg_cur":14}}`))
	v, _ := c.Session("S1")
	if len(v.StagesThisJob) != 1 {
		t.Fatalf("stages = %v, want [14]", v.StagesThisJob)
	}
	v.StagesThisJob[0] = 1
	u, _ := c.Session("S1")
	if !reflect.DeepEqual(u.StagesThisJob, []int{14}) {
		t.Fatalf("stages = %v, want the mutation to stay on the projection", u.StagesThisJob)
	}
}
