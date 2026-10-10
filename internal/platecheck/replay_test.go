package platecheck

import (
	"bufio"
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
// report with its connection generation and decoded payload.
type replayLine struct {
	TMS     int64           `json:"t_ms"`
	Gen     uint64          `json:"gen"`
	Payload json.RawMessage `json:"payload"`
}

// readCancelResendFixture loads the shared cancel/resend capture from the
// telemetry testdata directory.
func readCancelResendFixture(t *testing.T) []replayLine {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "telemetry", "testdata", "p1s_cancel_resend.jsonl"))
	if err != nil {
		t.Fatalf("open cancel/resend fixture: %v", err)
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
// capture through a real telemetry cache with a connected fakeCommands at
// generation 1. The worker steps explicitly between lines with no timers.
// After line 2 the recorded print_started entry for generation 1 admits
// one startup check; after line 8 the generation 2 entry admits one more;
// after line 11 a first_layer_complete carrying Observation{JobGen: 2}
// admits one first-layer check.
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
	if f.commands.gen != 1 || f.commands.Connected("01S1") != true {
		t.Fatal("real-cache fixture must start connected at generation 1")
	}
	// A clear plate keeps the startup checks from dispatching a stop, so
	// both job generations can complete without confirmation traffic.
	// The first-layer verdict pauses, which only arms a pending pause.
	f.client.result = Result{PClear: .9, POccupied: .1, PAssessable: .9, Model: "clef"}
	startedAt := map[int]uint64{1: 1, 7: 2}
	for i, line := range lines {
		cache.ObserveReport("01S1", uint64(i+1), line.Gen, line.Payload)
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
			if got := len(f.commands.pauses); got != 1 {
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
