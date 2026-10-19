package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// reportLine builds one printer-report log line with the given timestamp,
// serial, generation, and already-quoted payload value.
func reportLine(ts, serial string, gen uint64, quotedPayload string) string {
	return "time=" + ts + " level=DEBUG msg=\"printer report received\" origin=printer action=report serial=" + serial +
		" topic=device/" + serial + "/report payload=" + quotedPayload +
		" retain=false dup=false connection_generation=" + strconv.FormatUint(gen, 10) + " sequence=1 delivery=received"
}

func TestParseSlogLine(t *testing.T) {
	cases := []struct {
		name  string
		line  string
		key   string
		want  string
		empty bool
	}{
		{
			name: "message with spaces",
			line: `time=2026-01-01T00:00:00Z level=INFO msg="mochi mqtt server started"`,
			key:  "msg",
			want: "mochi mqtt server started",
		},
		{
			name: "quoted filename",
			line: reportLine("2026-01-01T00:00:01Z", "S1", 1, `"{\"print\":{\"subtask_name\":\"a\\\"b.3mf\"}}"`),
			key:  "payload",
			want: `{"print":{"subtask_name":"a\"b.3mf"}}`,
		},
		{
			name: "backslash",
			line: reportLine("2026-01-01T00:00:01Z", "S1", 1, `"{\"print\":{\"subtask_name\":\"a\\\\b\"}}"`),
			key:  "payload",
			want: `{"print":{"subtask_name":"a\\b"}}`,
		},
		{
			name: "unicode passthrough",
			line: reportLine("2026-01-01T00:00:01Z", "S1", 1, `"{\"print\":{\"subtask_name\":\"你好🖨️\"}}"`),
			key:  "payload",
			want: `{"print":{"subtask_name":"你好🖨️"}}`,
		},
		{
			name: "hex and unicode escapes",
			line: reportLine("2026-01-01T00:00:01Z", "S1", 1, `"{\"print\":{\"a\":\"`+"\\x41\\u00e9"+`\"}}"`),
			key:  "payload",
			want: `{"print":{"a":"Aé"}}`,
		},
		{
			name: "bare payload",
			line: reportLine("2026-01-01T00:00:01Z", "S1", 1, `{}`),
			key:  "payload",
			want: `{}`,
		},
		{
			name:  "empty line",
			line:  ``,
			empty: true,
		},
		{
			name:  "token without equals is skipped",
			line:  `garbage`,
			empty: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseSlogLine(tc.line)
			if tc.empty {
				if len(got) != 0 {
					t.Fatalf("parseSlogLine(%q) = %v, want empty", tc.line, got)
				}
				return
			}
			if got[tc.key] != tc.want {
				t.Fatalf("parseSlogLine key %q = %q, want %q", tc.key, got[tc.key], tc.want)
			}
		})
	}
}

func TestRunWritesOneFilePerNonEmptySegment(t *testing.T) {
	lines := []string{
		`time=2026-01-01T00:00:00Z level=INFO msg="mochi mqtt server started"`,
		reportLine("2026-01-01T00:00:01Z", "S1", 1, `"{\"print\":{\"subtask_name\":\"a\\\"b.3mf\"}}"`),
		`time=2026-01-01T00:00:02Z level=INFO msg="upstream connected" serial=S1`,
		reportLine("2026-01-01T00:00:03Z", "S1X", 1, `"{\"print\":{}}"`),
		reportLine("2026-01-01T00:00:04Z", "S1", 1, `"{\"print\":{\"subtask_name\":\"a\\\\b\"}}"`),
		reportLine("2026-01-01T00:00:05Z", "S1", 1, `not-json`),
		`time=2026-01-01T00:00:06Z level=DEBUG msg="printer report received" serial=S1 payload={}`,
		`time=2026-01-01T00:00:07Z level=INFO msg="mochi mqtt server started"`,
		reportLine("2026-01-01T00:00:08Z", "S1", 2, `"{\"print\":{\"subtask_name\":\"你好🖨️\"}}"`),
		reportLine("2026-01-01T00:00:09Z", "S1", 2, `"{\"print\":{\"a\":\"`+"\\x41\\u00e9"+`\"}}"`),
		`time=2026-01-01T00:00:10Z level=INFO msg="mochi mqtt server started"`,
		`time=2026-01-01T00:00:11Z level=INFO msg="mochi mqtt server started"`,
	}
	dir := t.TempDir()
	prefix := filepath.Join(dir, "live_S1")
	n, err := run(strings.NewReader(strings.Join(lines, "\n")+"\n"), "S1", prefix)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if n != 2 {
		t.Fatalf("run wrote %d files, want 2", n)
	}
	file1, err := os.ReadFile(prefix + "_1.jsonl")
	if err != nil {
		t.Fatalf("read segment 1: %v", err)
	}
	want1 := "{\"t_ms\":0,\"gen\":1,\"payload\":{\"print\":{\"subtask_name\":\"a\\\"b.3mf\"}}}\n" +
		"{\"t_ms\":3000,\"gen\":1,\"payload\":{\"print\":{\"subtask_name\":\"a\\\\b\"}}}\n" +
		"{\"t_ms\":5000,\"gen\":0,\"payload\":{}}\n"
	if string(file1) != want1 {
		t.Fatalf("segment 1 = %q, want %q", file1, want1)
	}
	file2, err := os.ReadFile(prefix + "_2.jsonl")
	if err != nil {
		t.Fatalf("read segment 2: %v", err)
	}
	want2 := "{\"t_ms\":0,\"gen\":2,\"payload\":{\"print\":{\"subtask_name\":\"你好🖨️\"}}}\n" +
		"{\"t_ms\":1000,\"gen\":2,\"payload\":{\"print\":{\"a\":\"Aé\"}}}\n"
	if string(file2) != want2 {
		t.Fatalf("segment 2 = %q, want %q", file2, want2)
	}
	if _, err := os.Stat(prefix + "_3.jsonl"); !os.IsNotExist(err) {
		t.Fatalf("empty segment wrote a file, err = %v", err)
	}
}

// TestRunRoundTripsRealSlogLines emits report lines through the real slog text
// handler with the same attribute names pool.go uses, then checks the decoded
// payloads are byte-identical.
func TestRunRoundTripsRealSlogLines(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	times := []time.Time{base, base.Add(time.Second), base.Add(2 * time.Second), base.Add(3 * time.Second)}
	idx := 0
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.TimeValue(times[idx])
				idx++
			}
			return a
		},
	})
	log := slog.New(handler)
	payloads := []string{
		`{"print":{"subtask_name":"quote\"name.3mf","gcode_file":"x\\y.gcode.3mf"}}`,
		`{"print":{"subtask_name":"back\\slash"}}`,
		`{"print":{"subtask_name":"héllo🖨️"}}`,
	}
	log.Info("mochi mqtt server started")
	for i, p := range payloads {
		log.Debug("printer report received", "origin", "printer", "action", "report",
			"serial", "S1", "topic", "device/S1/report", "payload", p,
			"retain", false, "dup", false, "connection_generation", uint64(3),
			"sequence", i+1, "delivery", "received")
	}
	dir := t.TempDir()
	n, err := run(&buf, "S1", filepath.Join(dir, "live"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if n != 1 {
		t.Fatalf("run wrote %d files, want 1", n)
	}
	out, err := os.ReadFile(filepath.Join(dir, "live_1.jsonl"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
	if len(lines) != len(payloads) {
		t.Fatalf("got %d records, want %d", len(lines), len(payloads))
	}
	for i, line := range lines {
		var rec record
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if rec.TMs != int64(i*1000) {
			t.Fatalf("record %d t_ms = %d, want %d", i, rec.TMs, i*1000)
		}
		if rec.Gen != 3 {
			t.Fatalf("record %d gen = %d, want 3", i, rec.Gen)
		}
		if string(rec.Payload) != payloads[i] {
			t.Fatalf("record %d payload = %s, want %s", i, rec.Payload, payloads[i])
		}
	}
}

func TestRunNoMatchingReportsWritesNothing(t *testing.T) {
	in := reportLine("2026-01-01T00:00:01Z", "OTHER", 1, `"{\"print\":{}}"`) + "\n"
	dir := t.TempDir()
	n, err := run(strings.NewReader(in), "S1", filepath.Join(dir, "live"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if n != 0 {
		t.Fatalf("run wrote %d files, want 0", n)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("output dir has %d entries, want 0", len(entries))
	}
}
