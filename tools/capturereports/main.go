// Command capturereports extracts printer reports from proxy container logs.
//
// It reads slog text log lines on stdin, keeps the "printer report received"
// lines for one printer serial, splits them into segments at each
// "mochi mqtt server started" line, and writes one JSONL replay fixture per
// non-empty segment as <prefix>_<n>.jsonl.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// record is one replay fixture line: milliseconds since the segment's first
// report, the upstream connection generation, and the decoded report payload.
type record struct {
	TMs     int64           `json:"t_ms"`
	Gen     uint64          `json:"gen"`
	Payload json.RawMessage `json:"payload"`
}

// main parses flags and runs the capture from stdin to prefixed output files.
func main() {
	serial := flag.String("serial", "", "printer serial to keep (exact match)")
	out := flag.String("out", "", "output file prefix; writes <prefix>_<n>.jsonl")
	flag.Parse()
	if *serial == "" || *out == "" {
		flag.Usage()
		os.Exit(2)
	}
	n, err := run(os.Stdin, *serial, *out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "capturereports: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "capturereports: wrote %d file(s)\n", n)
}

// run reads slog text lines from in, keeps the printer report lines for
// serial, and writes one JSONL file per non-empty segment. It returns the
// number of files written.
func run(in io.Reader, serial, prefix string) (int, error) {
	if serial == "" {
		return 0, fmt.Errorf("serial is required")
	}
	if prefix == "" {
		return 0, fmt.Errorf("out prefix is required")
	}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	var (
		reports []record
		base    time.Time
		hasBase bool
		lastMs  int64
		files   int
	)
	flush := func() error {
		if len(reports) == 0 {
			return nil
		}
		files++
		if err := writeSegment(prefix, files, reports); err != nil {
			return err
		}
		reports = nil
		hasBase = false
		lastMs = 0
		return nil
	}
	for scanner.Scan() {
		attrs := parseSlogLine(scanner.Text())
		switch msg := attrs["msg"]; {
		case msg == "mochi mqtt server started":
			if err := flush(); err != nil {
				return files, err
			}
			reports = nil
			hasBase = false
			lastMs = 0
		case msg == "printer report received" && attrs["serial"] == serial:
			gen, _ := strconv.ParseUint(attrs["connection_generation"], 10, 64)
			payload := attrs["payload"]
			if payload == "" || !json.Valid([]byte(payload)) {
				continue
			}
			ms := lastMs
			if ts, err := time.Parse(time.RFC3339Nano, attrs["time"]); err == nil {
				if !hasBase {
					base = ts
					hasBase = true
				}
				ms = ts.Sub(base).Milliseconds()
				if ms < 0 {
					ms = 0
				}
				lastMs = ms
			}
			reports = append(reports, record{TMs: ms, Gen: gen, Payload: json.RawMessage(payload)})
		default:
			// Unrelated line; ignore it.
		}
	}
	if err := scanner.Err(); err != nil {
		return files, fmt.Errorf("read log: %w", err)
	}
	if err := flush(); err != nil {
		return files, err
	}
	return files, nil
}

// parseSlogLine scans one slog text handler line into its key/value pairs.
// Quoted values are decoded with strconv.Unquote; bare values are kept as is.
func parseSlogLine(line string) map[string]string {
	attrs := make(map[string]string)
	i, n := 0, len(line)
	for i < n {
		for i < n && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= n {
			break
		}
		ks := i
		for i < n && line[i] != '=' && line[i] != ' ' && line[i] != '\t' {
			i++
		}
		if i >= n || line[i] != '=' {
			for i < n && line[i] != ' ' && line[i] != '\t' {
				i++
			}
			continue
		}
		key := line[ks:i]
		i++
		if i < n && line[i] == '"' {
			qs := i
			i++
			for i < n {
				if line[i] == '\\' {
					i++
					if i < n {
						i++
					}
					continue
				}
				if line[i] == '"' {
					i++
					break
				}
				i++
			}
			raw := line[qs:i]
			if unq, err := strconv.Unquote(raw); err == nil {
				attrs[key] = unq
			} else if len(raw) >= 2 {
				attrs[key] = raw[1 : len(raw)-1]
			} else {
				attrs[key] = raw
			}
			continue
		}
		vs := i
		for i < n && line[i] != ' ' && line[i] != '\t' {
			i++
		}
		attrs[key] = line[vs:i]
	}
	return attrs
}

// writeSegment writes one segment's reports as JSON lines to <prefix>_<n>.jsonl.
func writeSegment(prefix string, n int, reports []record) error {
	if dir := filepath.Dir(prefix); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create output dir: %w", err)
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range reports {
		if err := enc.Encode(r); err != nil {
			return fmt.Errorf("encode report: %w", err)
		}
	}
	path := fmt.Sprintf("%s_%d.jsonl", prefix, n)
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
