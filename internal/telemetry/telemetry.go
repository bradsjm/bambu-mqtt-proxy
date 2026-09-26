// Package telemetry merges Bambu MQTT report deltas into a small display
// state per printer for the overlay and camera status endpoints. It reads
// upstream reports through the pool observer without changing forwarding.
package telemetry

import (
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// State is the display projection of one printer's merged reports. Nil
// numeric pointers mean "not reported yet"; a printer that never sends a
// field keeps that distinction, so the UI can show unknown values.
type State struct {
	Serial        string
	Model         string
	Connected     bool // last known MQTT upstream connectivity
	LastReport    time.Time
	PrintingState string  // gcode_state, e.g. RUNNING / IDLE / FINISH
	Filename      string  // subtask_name
	Progress      float64 // mc_percent
	RemainMin     float64 // mc_remaining_time, minutes
	LayerNum      *int
	TotalLayers   *int
	NozzleTemp    *float64
	NozzleTarget  *float64
	BedTemp       *float64
	BedTarget     *float64
	ChamberTemp   *float64
}

// Cache stores merged state for every configured printer.
type Cache struct {
	mu     sync.Mutex
	states map[string]*State
	log    *slog.Logger
}

// NewCache indexes the configured printers; serials without reports still
// appear so the overlay can list every printer.
func NewCache(printers []config.Printer, log *slog.Logger) *Cache {
	states := make(map[string]*State, len(printers))
	for _, p := range printers {
		states[p.Serial] = &State{Serial: p.Serial, Model: p.Model}
	}
	return &Cache{states: states, log: log}
}

// Observe is the pool observer hook: merge one raw report payload.
func (c *Cache) Observe(serial string, payload []byte) {
	c.mu.Lock()
	if st, ok := c.states[serial]; ok {
		mergeReport(st, payload)
		st.LastReport = time.Now()
	}
	c.mu.Unlock()
}

// SetConnected records upstream connectivity from the pool status.
func (c *Cache) SetConnected(statuses map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for serial, up := range statuses {
		if st, ok := c.states[serial]; ok {
			st.Connected = up
		}
	}
}

// Snapshot returns a copy of every printer's current display state,
// ordered by serial for stable rendering.
func (c *Cache) Snapshot() []State {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]State, 0, len(c.states))
	for _, st := range c.states {
		out = append(out, *st)
	}
	return out
}

// mergeReport applies one report payload onto st. Bambu reports nest under a
// type key ("print"); values change type between prints (number or string),
// so extraction goes through generic maps instead of a fixed struct.
func mergeReport(st *State, payload []byte) {
	var report map[string]json.RawMessage
	if err := json.Unmarshal(payload, &report); err != nil {
		return // not a JSON report (health probes, tests); ignore
	}
	raw, ok := report["print"]
	if !ok {
		return
	}
	var printObj map[string]any
	if err := json.Unmarshal(raw, &printObj); err != nil {
		return
	}
	if s, ok := stringField(printObj, "gcode_state"); ok {
		st.PrintingState = s
	}
	if s, ok := stringField(printObj, "subtask_name"); ok && s != "" {
		st.Filename = s
	}
	if v, ok := numberField(printObj, "mc_percent"); ok {
		st.Progress = v
	}
	if v, ok := numberField(printObj, "mc_remaining_time"); ok {
		st.RemainMin = v
	}
	if v, ok := intField(printObj, "layer_num"); ok {
		st.LayerNum = &v
	}
	if v, ok := intField(printObj, "total_layer_num"); ok {
		st.TotalLayers = &v
	}
	if v, ok := numberField(printObj, "nozzle_temper"); ok {
		st.NozzleTemp = &v
	}
	if v, ok := numberField(printObj, "nozzle_target_temper"); ok {
		st.NozzleTarget = &v
	}
	if v, ok := numberField(printObj, "bed_temper"); ok {
		st.BedTemp = &v
	}
	if v, ok := numberField(printObj, "bed_target_temper"); ok {
		st.BedTarget = &v
	}
	if v, ok := numberField(printObj, "chamber_temper"); ok {
		st.ChamberTemp = &v
	}
}

// lookup finds the first present key, case-insensitively: some printers
// spell report keys with different casing.
func lookup(obj map[string]any, keys ...string) (any, bool) {
	for _, k := range keys {
		if v, ok := obj[k]; ok {
			return v, true
		}
		for objKey, v := range obj {
			if equalFold(objKey, k) {
				return v, true
			}
		}
	}
	return nil, false
}

// equalFold is a tiny ASCII case-insensitive compare for report keys.
func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// stringField extracts a string value.
func stringField(obj map[string]any, key string) (string, bool) {
	v, ok := lookup(obj, key)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// numberField extracts a float from a JSON number or numeric string.
// Bambu flips between the two encodings across fields and firmwares.
func numberField(obj map[string]any, key string) (float64, bool) {
	v, ok := lookup(obj, key)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case string:
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// intField extracts an integer value.
func intField(obj map[string]any, key string) (int, bool) {
	f, ok := numberField(obj, key)
	if !ok {
		return 0, false
	}
	return int(f), true
}
