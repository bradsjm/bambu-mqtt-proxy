package firstlayer

import (
	"bufio"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

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

// TestReplayCancelResendArmsOnZeroThenCompletes feeds the cancel/resend
// capture through a real telemetry cache. Sampling after line 8 arms on
// the resent print's retained layer 0, and sampling after line 11 records
// completion for job generation 2.
func TestReplayCancelResendArmsOnZeroThenCompletes(t *testing.T) {
	lines := readCancelResendFixture(t)
	if len(lines) != 11 {
		t.Fatalf("fixture lines = %d, want 11", len(lines))
	}
	cache := telemetry.NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc := New([]config.Printer{{Serial: "S1"}}, cache, activity.New([]config.Printer{{Serial: "S1"}}))
	var session printSession

	for i, line := range lines {
		cache.ObserveReport("S1", uint64(i+1), line.Gen, line.Payload)
		switch i {
		case 7:
			if obs, ok := svc.sample("S1", &session); ok {
				t.Fatalf("sample after line 8 returned evidence %+v, want arming only", obs)
			}
			if session.jobGen != 2 {
				t.Fatalf("sample after line 8 adopted jobGen %d, want 2", session.jobGen)
			}
			if !session.armed {
				t.Fatal("sample after line 8 did not arm on layer 0")
			}
		case 10:
			obs, ok := svc.sample("S1", &session)
			if !ok {
				t.Fatal("sample after line 11 returned no evidence")
			}
			want := activity.Observation{JobGen: 2}
			if obs != want {
				t.Fatalf("sample after line 11 = %+v, want %+v", obs, want)
			}
		}
	}
}
