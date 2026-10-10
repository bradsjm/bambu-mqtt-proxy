package platecheck

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// replayLine mirrors the telemetry replay fixture: one captured upstream
// report with its decoded payload.
type replayLine struct {
	TMS     int64           `json:"t_ms"`
	Payload json.RawMessage `json:"payload"`
}

// readCancelResendFixture loads the shared cancel/resend capture from the
// telemetry testdata directory.
func readCancelResendFixture(t *testing.T) []replayLine {
	t.Helper()
	return readReplayFixture(t, "p1s_cancel_resend.jsonl")
}

// readReplayFixture loads one JSONL capture from the telemetry testdata
// directory: one upstream report per line with its decoded payload.
func readReplayFixture(t *testing.T, name string) []replayLine {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "telemetry", "testdata", name))
	if err != nil {
		t.Fatalf("open fixture %s: %v", name, err)
	}
	defer f.Close()
	var lines []replayLine
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var line replayLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("decode fixture line: %v", err)
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return lines
}

// replayPrintStarted finds the print_started entry the cache recorded for
// the current job generation after feeding one fixture line.
func replayPrintStarted(t *testing.T, log *activity.Log, serial string, gen uint64) activity.Entry {
	t.Helper()
	for _, e := range log.Recent(serial) {
		if e.Kind == "print_started" && e.Observation.JobGen == gen {
			return e
		}
	}
	t.Fatalf("no recorded print_started entry for job generation %d", gen)
	return activity.Entry{}
}

// TestReplayCancelResendStartupAndFirstLayer feeds the cancel/resend
// capture through a real telemetry cache with online fakeCommands. The
// worker steps explicitly between lines with no timers. After line 2 the
// recorded print_started entry for generation 1 admits one startup check;
// after line 8 the generation 2 entry admits one more; after line 11 a
// first_layer_complete carrying Observation{JobGen: 2} admits one
// first-layer check.
func TestReplayCancelResendStartupAndFirstLayer(t *testing.T) {
	lines := readCancelResendFixture(t)
	if len(lines) != 11 {
		t.Fatalf("fixture lines = %d, want 11", len(lines))
	}
	printers := []config.Printer{{Serial: "01S1", Model: "P1S", Name: "Shop"}}
	cache := telemetry.NewCache(printers, slog.New(slog.NewTextHandler(io.Discard, nil)))
	telemetryLog := activity.New(printers)
	cache.SetActivity(telemetryLog)
	f := newRealCacheFixture(t, cache)
	if !f.commands.Online("01S1") {
		t.Fatal("real-cache fixture must start online")
	}
	// A clear plate keeps the startup checks from dispatching a stop, so
	// both job generations can complete without confirmation traffic.
	f.client.result = Result{PClear: .9, POccupied: .1, PAssessable: .9, Model: "clef"}
	startedAt := map[int]uint64{1: 1, 7: 2}
	for i, line := range lines {
		cache.Observe("01S1", line.Payload)
		if gen, ok := startedAt[i]; ok {
			f.s.ObserveActivity("01S1", replayPrintStarted(t, telemetryLog, "01S1", gen))
		}
		if i == 10 {
			j, _, ok := cache.Print("01S1")
			if !ok || j.Generation != 2 {
				t.Fatalf("after line 11: job = %+v, want generation 2", j)
			}
			f.s.ObserveActivity("01S1", activity.Entry{Kind: "first_layer_complete", Time: time.Now(), Observation: activity.Observation{JobGen: 2}})
		}
		f.step()
		switch i {
		case 1:
			if f.client.calls != 1 {
				t.Fatalf("after line 2: startup Evaluate calls = %d, want 1", f.client.calls)
			}
			if f.w.processed != 1 {
				t.Fatalf("after line 2: processed = %d, want 1", f.w.processed)
			}
		case 7:
			if f.client.calls != 2 {
				t.Fatalf("after line 8: startup Evaluate calls = %d, want 2 (one per job generation)", f.client.calls)
			}
			if f.w.processed != 2 {
				t.Fatalf("after line 8: processed = %d, want 2", f.w.processed)
			}
		case 10:
			if f.client.layerCalls != 1 {
				t.Fatalf("after line 11: first-layer EvaluateFirstLayer calls = %d, want 1", f.client.layerCalls)
			}
			if got := f.commands.pauses; got != 1 {
				t.Fatalf("after line 11: pause commands = %d, want 1", got)
			}
			if f.w.layerProcessed != 2 {
				t.Fatalf("after line 11: layerProcessed = %d, want 2", f.w.layerProcessed)
			}
		}
	}
	if f.commands.count() != 0 {
		t.Fatalf("stop commands = %d, want 0 for a clear plate", f.commands.count())
	}
	if f.status() != "clear" {
		t.Fatalf("startup status = %q, want clear", f.status())
	}
	if got := layerStatus(f); got.State != "pause" || got.Pause != "pending" {
		t.Fatalf("first-layer status = %+v, want state pause with pending pause", got)
	}
}

// TestReplayLivePrintChecksTwicePerJob feeds the live P1S capture through a
// real telemetry cache with online fakeCommands. The worker steps
// explicitly between lines with no timers. The job runs one pass 1 at
// PREPARE (line 6) and one pass 2 after stg_cur 14 (line 107): exactly two
// checks for the job, the second with the after-homing text.
func TestReplayLivePrintChecksTwicePerJob(t *testing.T) {
	serial := "01S00C351100139"
	lines := readReplayFixture(t, "live_01S00C351100139_20261010.jsonl")
	printers := []config.Printer{{Serial: serial, Model: "P1S", Name: "Shop"}}
	cache := telemetry.NewCache(printers, slog.New(slog.NewTextHandler(io.Discard, nil)))
	telemetryLog := activity.New(printers)
	cache.SetActivity(telemetryLog)
	// newRealCacheFixture wires serial 01S1; rebuild the same worker for
	// the capture's printer serial instead.
	client := &fakeDecision{result: Result{PClear: .9, POccupied: .1, PAssessable: .9, Model: "clef"}}
	commands := &fakeCommands{}
	data := testJPEG(t)
	seq := uint64(0)
	frames := frameFunc(func(_ context.Context, _ string, after time.Time) (Frame, error) {
		seq++
		if d := time.Until(after); d > 0 {
			time.Sleep(d)
		}
		return Frame{JPEG: data, Seq: seq, Captured: time.Now(), Width: 8, Height: 6}, nil
	})
	s := New(printers, testSettings(), client, frames, cache, commands, nil)
	s.now = time.Now
	events := activity.New(printers)
	s.SetActivity(events)
	w := s.workers[serial]
	step := func() { w.step(context.Background()) }
	if !commands.Online(serial) {
		t.Fatal("live replay must start online")
	}
	delivered := map[string]bool{}
	for i, line := range lines {
		cache.Observe(serial, line.Payload)
		// Deliver each start entry once, as the activity observer would.
		for _, e := range telemetryLog.Recent(serial) {
			if (e.Kind == "print_preparing" || e.Kind == "print_started") && !delivered[e.Kind] {
				delivered[e.Kind] = true
				s.ObserveActivity(serial, e)
			}
		}
		step()
		switch i {
		case 5:
			if client.calls != 1 {
				t.Fatalf("after line 6 (PREPARE): Evaluate calls = %d, want 1", client.calls)
			}
			if w.processed != 1 {
				t.Fatalf("after line 6: processed = %d, want 1", w.processed)
			}
		case 106:
			if client.calls != 2 {
				t.Fatalf("after line 107 (stg_cur 14): Evaluate calls = %d, want 2", client.calls)
			}
			if w.homingProcessed != 1 {
				t.Fatalf("after line 107: homingProcessed = %d, want 1", w.homingProcessed)
			}
		}
	}
	if client.calls != 2 {
		t.Fatalf("Evaluate calls = %d, want exactly 2 for the job", client.calls)
	}
	if commands.count() != 0 {
		t.Fatalf("stop commands = %d, want 0 for a clear plate", commands.count())
	}
	var got []string
	for _, e := range events.Recent(serial) {
		if e.Kind == "platecheck_passed" {
			got = append([]string{e.Message}, got...)
		}
	}
	want := []string{
		"Plate check passed, plate clear",
		"Plate check after homing passed, plate clear",
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("plate-check activity = %q, want %q", got, want)
	}
	if v := s.moduleState(serial).(ModuleState); !v.Homing || v.State != "clear" {
		t.Fatalf("status = %+v, want the clear after-homing pass", v)
	}
}

// TestHomingStageReplacedBeforeTheNextStepStillRunsPass2 pins the pass-2
// defect: pass 1's synchronous check blocks the worker while telemetry
// keeps merging stages, so a homing stage processed between polling ticks
// must still count. After pass 1 consumes the generation, stage 14 then
// stage 1 merge with no worker step between them; the next step runs pass
// 2 once from the job's stage history.
func TestHomingStageReplacedBeforeTheNextStepStillRunsPass2(t *testing.T) {
	serial := "01S1"
	printers := []config.Printer{{Serial: serial, Model: "P1S", Name: "Shop"}}
	cache := telemetry.NewCache(printers, slog.New(slog.NewTextHandler(io.Discard, nil)))
	telemetryLog := activity.New(printers)
	cache.SetActivity(telemetryLog)
	f := newRealCacheFixture(t, cache)
	f.client.result = Result{PClear: .9, POccupied: .1, PAssessable: .9, Model: "clef"}
	report := func(payload string) {
		t.Helper()
		cache.Observe(serial, []byte(payload))
	}
	deliverStarts := func() {
		t.Helper()
		for _, e := range telemetryLog.Recent(serial) {
			if e.Kind == "print_preparing" || e.Kind == "print_started" {
				f.s.ObserveActivity(serial, e)
			}
		}
	}
	report(`{"print":{"gcode_state":"IDLE"}}`)
	report(`{"print":{"gcode_state":"PREPARE","subtask_name":"benchy.3mf"}}`)
	deliverStarts()
	f.step()
	if f.client.calls != 1 || f.w.processed != 1 {
		t.Fatalf("after pass 1: calls = %d processed = %d, want 1 and 1", f.client.calls, f.w.processed)
	}
	// Both stages merge with no worker step between them: 14 is replaced
	// by 1 before the worker ever polls.
	report(`{"print":{"gcode_state":"RUNNING","layer_num":0}}`)
	report(`{"print":{"stg_cur":14}}`)
	report(`{"print":{"stg_cur":1}}`)
	if _, v, ok := cache.Print(serial); !ok || reportedHoming(v.StagesThisJob) != true {
		t.Fatalf("history = %+v, want the replaced homing stage kept", v.StagesThisJob)
	}
	f.step()
	if f.client.calls != 2 {
		t.Fatalf("Evaluate calls = %d, want 2 (pass 2 ran from the history)", f.client.calls)
	}
	if f.w.homingProcessed != 1 {
		t.Fatalf("homingProcessed = %d, want 1 after pass 2", f.w.homingProcessed)
	}
	var got []string
	for _, e := range f.activity.Recent(serial) {
		if e.Kind == "platecheck_passed" {
			got = append([]string{e.Message}, got...)
		}
	}
	want := []string{
		"Plate check passed, plate clear",
		"Plate check after homing passed, plate clear",
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("plate-check activity = %q, want %q", got, want)
	}
	// A later step runs nothing more for the generation.
	f.step()
	if f.client.calls != 2 {
		t.Fatalf("Evaluate calls = %d, want still 2", f.client.calls)
	}
}
