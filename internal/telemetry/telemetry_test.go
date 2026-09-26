package telemetry

import (
	"io"
	"log/slog"
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
		"hms_alert", "print_error", "hms_cleared", "print_error_cleared",
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
	if st.Name != "Garage" || st.LastReport.IsZero() {
		t.Fatalf("name/last report = %q/%v", st.Name, st.LastReport)
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
	// The restart bumps twice by design: the state boundary and the new
	// identity while a complete cookie was still set.
	if !v.Active || v.Epoch != 4 || v.Cookie != "9-8-b.gcode" {
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
	wake := c.WatchDetection("S1")
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

func TestWatchDetectionWakesOncePerReport(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	wake := c.WatchDetection("S1")
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
