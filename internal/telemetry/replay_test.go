package telemetry

import (
	"bufio"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
)

// replayLine is one captured upstream report: t_ms is relative to the
// segment's first report, gen is the upstream connection generation, and
// payload is the decoded report JSON.
type replayLine struct {
	TMS     int64           `json:"t_ms"`
	Gen     uint64          `json:"gen"`
	Payload json.RawMessage `json:"payload"`
}

// readReplayFixture loads the named jsonl fixture from testdata.
func readReplayFixture(t *testing.T, name string) []replayLine {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
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
			t.Fatalf("decode fixture %s: %v", name, err)
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return lines
}

// replayOracle is the independent last-reported-value oracle: name tracks
// subtask_name (absent keeps, a string sets including empty, a non-string
// clears), and layer tracks layer_num (absent keeps, an integral number
// sets, anything else clears).
type replayOracle struct {
	name  string
	layer *int
}

// observe feeds one replay line's print object into the oracle.
func (o *replayOracle) observe(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var report struct {
		Print map[string]any `json:"print"`
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if v, ok := report.Print["subtask_name"]; ok {
		if s, ok := v.(string); ok {
			o.name = s
		} else {
			o.name = ""
		}
	}
	if v, ok := report.Print["layer_num"]; ok {
		switch n := v.(type) {
		case float64:
			if n == float64(int(n)) {
				layer := int(n)
				o.layer = &layer
			} else {
				o.layer = nil
			}
		default:
			o.layer = nil
		}
	}
}

// TestReplayKeepsLastReportedValues runs every testdata fixture through
// Cache.ObserveReport with a test clock at t_ms, and after each line
// asserts Job.Name and Session.LayerNum equal the oracle's values.
func TestReplayKeepsLastReportedValues(t *testing.T) {
	entries, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".jsonl" {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			lines := readReplayFixture(t, entry.Name())
			log := slog.New(slog.NewTextHandler(io.Discard, nil))
			c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, log)
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			c.now = func() time.Time { return now }
			var oracle replayOracle
			for i, line := range lines {
				now = now.Add(time.Duration(line.TMS) * time.Millisecond)
				if i > 0 {
					now = now.Add(-time.Duration(lines[i-1].TMS) * time.Millisecond)
				}
				c.ObserveReport("S1", uint64(i+1), line.Gen, line.Payload)
				oracle.observe(t, line.Payload)
				j, s, ok := c.Print("S1")
				if !ok {
					t.Fatal("Print missing for a configured printer")
				}
				if j.Name != oracle.name {
					t.Fatalf("line %d: Job.Name = %q, want oracle %q", i+1, j.Name, oracle.name)
				}
				if (s.LayerNum == nil) != (oracle.layer == nil) ||
					(s.LayerNum != nil && *s.LayerNum != *oracle.layer) {
					t.Fatalf("line %d: Session.LayerNum = %v, want oracle %v", i+1, s.LayerNum, oracle.layer)
				}
			}
		})
	}
}

// TestReplayCancelResend replays the cancel/resend capture and pins the
// job generations, start observation, retained identity, and the layer-0
// decision evidence after the resend.
func TestReplayCancelResend(t *testing.T) {
	lines := readReplayFixture(t, "p1s_cancel_resend.jsonl")
	if len(lines) != 11 {
		t.Fatalf("fixture lines = %d, want 11", len(lines))
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	printers := []config.Printer{{Serial: "S1", Name: "Shop"}}
	c := NewCache(printers, log)
	events := activity.New(printers)
	c.SetActivity(events)
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	jobs := make([]JobView, len(lines))
	sessions := make([]SessionView, len(lines))
	counts := make([]int, len(lines))
	for i, line := range lines {
		now = now.Add(time.Duration(line.TMS) * time.Millisecond)
		if i > 0 {
			now = now.Add(-time.Duration(lines[i-1].TMS) * time.Millisecond)
		}
		c.ObserveReport("S1", uint64(i+1), line.Gen, line.Payload)
		jobs[i], sessions[i], _ = c.Print("S1")
		counts[i] = len(events.Recent("S1"))
	}

	// Generation 1 after line 2, and 2 after the resend at line 8.
	if jobs[1].Generation != 1 || !jobs[1].Active {
		t.Fatalf("after line 2: %+v, want active generation 1", jobs[1])
	}
	if jobs[7].Generation != 2 || !jobs[7].Active {
		t.Fatalf("after line 8: %+v, want active generation 2", jobs[7])
	}
	// Both entries are prints this proxy saw begin: line 1 carried an
	// earlier IDLE, and the resend re-entered from FAILED.
	if !jobs[1].StartObserved || !jobs[7].StartObserved {
		t.Fatalf("StartObserved after lines 2/8 = %v/%v, want true/true", jobs[1].StartObserved, jobs[7].StartObserved)
	}
	// The resend carries no name of its own: the Sunlu identity survives.
	if jobs[7].Name != "Sunlu_AMS_Heater_Desiccant_Mount_V.2" ||
		jobs[7].GCodeFile != "Sunlu_AMS_Heater_Desiccant_Mount_V.2.gcode.3mf" {
		t.Fatalf("after line 8: name=%q file=%q, want the Sunlu values", jobs[7].Name, jobs[7].GCodeFile)
	}
	// Line 7's layer 0 is decision evidence at 0 after the resend.
	if layer := sessions[7].CurrentLayer(); layer == nil || *layer != 0 {
		t.Fatalf("CurrentLayer after line 8 = %v, want 0", layer)
	}
	// Lines 10 and 11 report layers 1 and 2 of the resent print.
	if layer := sessions[9].CurrentLayer(); layer == nil || *layer != 1 {
		t.Fatalf("CurrentLayer after line 10 = %v, want 1", layer)
	}
	if layer := sessions[10].CurrentLayer(); layer == nil || *layer != 2 {
		t.Fatalf("CurrentLayer after line 11 = %v, want 2", layer)
	}

	// Lines 4-6 carry the pause, the user cancel, and the alert clear.
	// Recent returns newest first, so slice each line's window by its
	// absolute offsets in the final list.
	total := counts[len(counts)-1]
	window := func(after, upto int) []activity.Entry {
		t.Helper()
		got := events.Recent("S1")
		added := got[total-counts[upto] : total-counts[after]]
		out := make([]activity.Entry, len(added))
		for i, e := range added {
			out[len(added)-1-i] = e
		}
		return out
	}
	line4 := window(2, 3)
	if len(line4) != 3 {
		t.Fatalf("line 4 events = %+v, want pause, hms alert, and error", line4)
	}
	if line4[0].Kind != "print_paused" || line4[0].Message != "Print paused · Sunlu_AMS_Heater_Desiccant_Mount_V.2" {
		t.Fatalf("line 4 pause = %+v", line4[0])
	}
	if line4[1].Kind != "hms_alert" || line4[1].Message != "Printer alert HMS_0300_0100_0001_0008, no description available" ||
		line4[1].URL != "https://printara3d.com/tools/bambu-error-codes/?code=0300-0100" {
		t.Fatalf("line 4 hms = %+v", line4[1])
	}
	if line4[2].Kind != "print_error" || line4[2].Message != "Printer error: Heated Bed Temperature Malfunction (0300-8009)" || line4[2].URL == "" {
		t.Fatalf("line 4 error = %+v", line4[2])
	}
	line5 := window(3, 4)
	if len(line5) != 1 || line5[0].Kind != "print_cancelled" ||
		line5[0].Message != "Print cancelled · Sunlu_AMS_Heater_Desiccant_Mount_V.2" {
		t.Fatalf("line 5 events = %+v, want only the cancelled line", line5)
	}
	line6 := window(4, 5)
	if len(line6) != 1 || line6[0].Kind != "alerts_cleared" ||
		line6[0].Message != "Printer alerts cleared: HMS_0300_0100_0001_0008, Heated Bed Temperature Malfunction" {
		t.Fatalf("line 6 events = %+v, want the single cleared line", line6)
	}
	for _, e := range events.Recent("S1") {
		if strings.Contains(e.Message, "0300_400C") || strings.Contains(e.Message, "0300-400C") {
			t.Fatalf("cancel marker leaked into activity: %+v", e)
		}
	}
}
