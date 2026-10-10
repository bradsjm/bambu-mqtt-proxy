package jobpreview

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

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// replayLine mirrors the telemetry replay fixture: one captured printer
// report with its decoded payload.
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

// TestReplayCancelResendAttemptStartsAfterResend feeds the cancel/resend
// capture through a real telemetry cache. A blocked fake fetch keeps the
// gen-2 attempt in flight: an attempt starts on the first step after line
// 8, and stepping after the line-9 gap report does not cancel it.
func TestReplayCancelResendAttemptStartsAfterResend(t *testing.T) {
	lines := readCancelResendFixture(t)
	if len(lines) != 11 {
		t.Fatalf("fixture lines = %d, want 11", len(lines))
	}
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	printers := []config.Printer{{Serial: "S1", Address: "S1.invalid:8883", Username: "u", Password: "p"}}
	cache := telemetry.NewCache(printers, testLogger())
	svc := New(printers, cache, conn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.now = clk.Now

	started := make(chan struct{}, 4)
	cancelled := make(chan struct{}, 4)
	release := make(chan struct{})
	counts := &fetchCounter{}
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		select {
		case <-ctx.Done():
			cancelled <- struct{}{}
			return Result{}, ctx.Err()
		case <-release:
			return Result{PNG: pngBytes(t, 2, 2), Metadata: &Metadata{Source: sourcePrinter3mf, Title: strPtr("T")}}, nil
		}
	}
	defer svc.Close()

	// Feed the capture up to the resend without stepping the scheduler.
	for i := 0; i < 8; i++ {
		cache.Observe("S1", lines[i].Payload)
	}
	job, ok := cache.Job("S1")
	if !ok || job.Generation != 2 || !job.Active {
		t.Fatalf("after line 8: job = %+v, want active generation 2", job)
	}

	// The first step after the resend starts one attempt for gen 2.
	clk.Set(base)
	svc.step(base)
	await(t, started)
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d after the first step past line 8, want 1", got)
	}
	if rec, ok := attemptRecord(svc, "S1"); !ok || rec.gen != 2 || rec.count != 1 {
		t.Fatalf("attempt = %+v, %v; want gen 2 count 1", rec, ok)
	}
	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending || res.Preview.Reason != reasonDownloading {
		t.Fatalf("projection while in flight = %+v, want pending downloading", res.Preview)
	}

	// The line-9 gap report is a temperature-only delta: stepping after
	// it must not cancel the attempt.
	cache.Observe("S1", lines[8].Payload)
	clk.Set(base.Add(time.Second))
	svc.step(base.Add(time.Second))
	select {
	case <-cancelled:
		t.Fatal("line-9 gap report cancelled the in-flight attempt")
	case <-started:
		t.Fatal("gap step started a second attempt while one is in flight")
	default:
	}
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d after the gap step, want 1", got)
	}

	// Release the fetch and join the transfer: the attempt publishes.
	close(release)
	svc.wg.Wait()
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusReady {
		t.Fatalf("projection after release = %+v, want ready", res.Preview)
	}
}
