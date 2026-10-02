package telemetry

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"bambu-mqtt-proxy/internal/config"
)

// amsSensorCache builds a single-printer cache for AMS sensor tests.
func amsSensorCache(t *testing.T) *Cache {
	t.Helper()
	return NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// observeSensor merges one raw report into the test cache's printer.
func observeSensor(t *testing.T, c *Cache, payload string) {
	t.Helper()
	c.Observe("S1", []byte(payload))
}

// oneSensorRecord requires exactly one DEBUG diagnostic line in buf,
// returns it without the newline, and decodes it as a JSON object. The
// decode is the serialization proof: every attribute must survive as a
// JSON value, so numbers arrive as float64 and flags as bool.
func oneSensorRecord(t *testing.T, buf *bytes.Buffer) (string, map[string]any) {
	t.Helper()
	line := strings.TrimSuffix(buf.String(), "\n")
	buf.Reset()
	if line == "" || strings.Contains(line, "\n") {
		t.Fatalf("want exactly one diagnostic record, got %q", line)
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("diagnostic record is not JSON: %v (%q)", err, line)
	}
	return line, rec
}

// TestAMSTempRangeGate pins the AMS unit temperature ingestion: only finite
// readings inside the sensor's 0..100 Celsius range merge, the firmware's
// out-of-range sentinels (the observed 6503.6) never become displayed
// data, a rejected reading retains the prior valid one, and a unit that
// never reported a plausible value stays unknown.
func TestAMSTempRangeGate(t *testing.T) {
	c := amsSensorCache(t)

	// A first bogus reading must leave the field unknown, not zero.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","temp":6503.6}]}}}`)
	if got := c.Snapshot()[0].AMS[0].Temp; got != nil {
		t.Fatalf("temp = %v, want unknown for the bogus 6503.6", *got)
	}

	// A plausible reading merges.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","temp":23.5}]}}}`)
	if got := c.Snapshot()[0].AMS[0].Temp; got == nil || *got != 23.5 {
		t.Fatalf("temp = %v, want 23.5", c.Snapshot()[0].AMS[0].Temp)
	}

	// Out-of-range numbers, nonfinite string spellings, and unparsable
	// values all reject and retain the prior valid reading.
	for _, bogus := range []string{
		`"temp":6503.6`, `"temp":-0.1`, `"temp":100.01`, `"temp":-1`,
		`"temp":"6503.6"`, `"temp":"NaN"`, `"temp":"Infinity"`, `"temp":"-Inf"`,
		`"temp":"garbage"`,
	} {
		observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0",`+bogus+`}]}}}`)
		if got := c.Snapshot()[0].AMS[0].Temp; got == nil || *got != 23.5 {
			t.Fatalf("temp after %s = %v, want retained 23.5", bogus, c.Snapshot()[0].AMS[0].Temp)
		}
	}

	// The range is inclusive: both endpoints are valid readings, numbers
	// or numeric strings.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","temp":0}]}}}`)
	if got := c.Snapshot()[0].AMS[0].Temp; got == nil || *got != 0 {
		t.Fatalf("temp = %v, want accepted 0", c.Snapshot()[0].AMS[0].Temp)
	}
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","temp":"100"}]}}}`)
	if got := c.Snapshot()[0].AMS[0].Temp; got == nil || *got != 100 {
		t.Fatalf("temp = %v, want accepted 100", c.Snapshot()[0].AMS[0].Temp)
	}

	// A later plausible reading recovers the field after a rejection.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","temp":6503.6}]}}}`)
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","temp":24.25}]}}}`)
	if got := c.Snapshot()[0].AMS[0].Temp; got == nil || *got != 24.25 {
		t.Fatalf("temp = %v, want recovered 24.25", c.Snapshot()[0].AMS[0].Temp)
	}
}

// TestAMSHumiditySticky pins the humidity delta semantics the AMS report
// stream relies on: an invalid humidity_raw never replaces a retained valid
// reading, a never-valid unit stays unknown instead of showing a
// placeholder, and only a later valid reading updates the field.
func TestAMSHumiditySticky(t *testing.T) {
	c := amsSensorCache(t)

	// Bogus first readings (the power-on placeholder 0, out-of-range and
	// unparsable values) leave the field unknown, not zero.
	for _, bogus := range []string{
		`"humidity_raw":0`, `"humidity_raw":-5`, `"humidity_raw":101`, `"humidity_raw":"dry"`,
	} {
		observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0",`+bogus+`}]}}}`)
		if got := c.Snapshot()[0].AMS[0].Humidity; got != nil {
			t.Fatalf("humidity after %s = %v, want unknown while no valid reading arrived", bogus, *got)
		}
	}

	// A valid reading merges and survives invalid successors: the
	// firmware's print-time placeholders must not clear or remap it.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","humidity_raw":45}]}}}`)
	for _, bogus := range []string{
		`"humidity_raw":0`, `"humidity_raw":-5`, `"humidity_raw":101`, `"humidity_raw":""`,
	} {
		observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0",`+bogus+`}]}}}`)
		if got := c.Snapshot()[0].AMS[0].Humidity; got == nil || *got != 45 {
			t.Fatalf("humidity after %s = %v, want retained 45", bogus, c.Snapshot()[0].AMS[0].Humidity)
		}
	}

	// The accepted 1..100 range is inclusive at both ends.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","humidity_raw":1}]}}}`)
	if got := c.Snapshot()[0].AMS[0].Humidity; got == nil || *got != 1 {
		t.Fatalf("humidity = %v, want accepted 1", c.Snapshot()[0].AMS[0].Humidity)
	}
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","humidity_raw":100}]}}}`)
	if got := c.Snapshot()[0].AMS[0].Humidity; got == nil || *got != 100 {
		t.Fatalf("humidity = %v, want accepted 100", c.Snapshot()[0].AMS[0].Humidity)
	}

	// A later valid reading updates the retained value across an invalid
	// one in between.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","humidity_raw":0}]}}}`)
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","humidity_raw":50}]}}}`)
	if got := c.Snapshot()[0].AMS[0].Humidity; got == nil || *got != 50 {
		t.Fatalf("humidity = %v, want recovered 50", c.Snapshot()[0].AMS[0].Humidity)
	}
}

// TestAMSSensorFieldsDistinctPerUnit pins the sensor model's distinctions:
// temp and humidity are separate optional fields keyed by the reported unit
// id, a bogus value in one field or unit never remaps into the other, and a
// unit first seen under a new id starts with unknown sensors.
func TestAMSSensorFieldsDistinctPerUnit(t *testing.T) {
	c := amsSensorCache(t)
	observeSensor(t, c, `{"print":{"ams":{"ams":[
		{"id":"0","temp":25.5},
		{"id":"1","humidity_raw":30}]}}}`)

	st := c.Snapshot()[0]
	if len(st.AMS) != 2 {
		t.Fatalf("ams units = %+v, want units 0 and 1", st.AMS)
	}
	if u := st.AMS[0]; u.Temp == nil || *u.Temp != 25.5 || u.Humidity != nil {
		t.Fatalf("unit 0 = %+v, want temp 25.5 with unknown humidity", u)
	}
	if u := st.AMS[1]; u.Humidity == nil || *u.Humidity != 30 || u.Temp != nil {
		t.Fatalf("unit 1 = %+v, want humidity 30 with unknown temp", u)
	}

	// A bogus temp on unit 1 stays out of every field of every unit.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"1","temp":6503.6}]}}}`)
	st = c.Snapshot()[0]
	if u := st.AMS[0]; u.Temp == nil || *u.Temp != 25.5 || u.Humidity != nil {
		t.Fatalf("unit 0 changed: %+v", u)
	}
	if u := st.AMS[1]; u.Temp != nil || u.Humidity == nil || *u.Humidity != 30 {
		t.Fatalf("unit 1 = %+v, want unknown temp with retained humidity 30", u)
	}

	// A placeholder humidity reading does not become a zero or a temp, and
	// the retained value survives it.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"1","humidity_raw":0,"temp":6503.6}]}}}`)
	st = c.Snapshot()[0]
	if u := st.AMS[1]; u.Humidity == nil || *u.Humidity != 30 || u.Temp != nil {
		t.Fatalf("unit 1 = %+v, want humidity 30 retained and temp unknown", u)
	}

	// The same physical unit reporting under a new unit id is a fresh
	// entry: its sensors start unknown until it reports plausible values.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"2"}]}}}`)
	st = c.Snapshot()[0]
	if len(st.AMS) != 3 {
		t.Fatalf("ams units = %+v, want units 0, 1 and 2", st.AMS)
	}
	if u := st.AMS[2]; u.Temp != nil || u.Humidity != nil {
		t.Fatalf("unit 2 = %+v, want unknown sensors on first sight", u)
	}
}

// TestAMSSensorDebugDiagnostic pins the DEBUG sensor observation at the
// existing report merge: records emit only when a report unit carries a
// sensor field and only for unit ids mergeAMS accepts, every attribute
// serializes as a typed JSON number or flag, arbitrary report strings are
// omitted even when the sensor value is one, the out-of-range 6503.6 is
// logged as a raw number while the retained effective value survives, and
// the logger level gates emission with no other configuration.
func TestAMSSensorDebugDiagnostic(t *testing.T) {
	var buf bytes.Buffer
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}},
		slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// A valid humidity report: raw and effective numbers, no temp values.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","humidity_raw":45}]}}}`)
	_, rec := oneSensorRecord(t, &buf)
	if rec["serial"] != "S1" || rec["unit_id"] != float64(0) {
		t.Fatalf("record identity = %v/%v, want S1/0", rec["serial"], rec["unit_id"])
	}
	if rec["humidity_present"] != true || rec["humidity_valid"] != true ||
		rec["humidity_raw"] != float64(45) || rec["humidity"] != float64(45) {
		t.Fatalf("humidity attrs = %v", rec)
	}
	if rec["temp_present"] != false || rec["temp_valid"] != false {
		t.Fatalf("temp flags = %v/%v, want false", rec["temp_present"], rec["temp_valid"])
	}
	if _, ok := rec["temp_raw"]; ok {
		t.Fatal("temp_raw present without a temp field")
	}

	// A valid temp report: raw and effective numbers.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","temp":23.5}]}}}`)
	_, rec = oneSensorRecord(t, &buf)
	if rec["temp_present"] != true || rec["temp_valid"] != true ||
		rec["temp_raw"] != float64(23.5) || rec["temp"] != float64(23.5) {
		t.Fatalf("temp attrs = %v", rec)
	}

	// A hostile report: the raw string must not enter the record anywhere,
	// the unparsable humidity keeps its retained effective value, and the
	// bogus 6503.6 stays a raw number next to the retained 23.5.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0","temp":6503.6,
		"humidity_raw":"<script>alert(1)</script>"}]}}}`)
	line, rec := oneSensorRecord(t, &buf)
	if strings.Contains(line, "script") {
		t.Fatalf("raw report string leaked into the diagnostic: %q", line)
	}
	if rec["humidity_present"] != true || rec["humidity_valid"] != false {
		t.Fatalf("humidity flags = %v/%v, want present but invalid", rec["humidity_present"], rec["humidity_valid"])
	}
	if _, ok := rec["humidity_raw"]; ok {
		t.Fatal("the unparsable humidity_raw value must be omitted")
	}
	if rec["humidity"] != float64(45) {
		t.Fatalf("humidity = %v, want retained 45", rec["humidity"])
	}
	if rec["temp_valid"] != false || rec["temp_raw"] != float64(6503.6) {
		t.Fatalf("temp_raw = %v valid %v, want numeric 6503.6 invalid", rec["temp_raw"], rec["temp_valid"])
	}
	if rec["temp"] != float64(23.5) {
		t.Fatalf("temp = %v, want retained 23.5", rec["temp"])
	}

	// Reports without sensor fields and unit ids mergeAMS rejects stay
	// silent, and a default-level logger never emits.
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"0"}]}}}`)
	observeSensor(t, c, `{"print":{"ams":{"ams":[{"id":"200","temp":30}]}}}`)
	if line := buf.String(); line != "" {
		t.Fatalf("unexpected diagnostic records: %q", line)
	}
	var quiet bytes.Buffer
	q := NewCache([]config.Printer{{Serial: "S1", Name: "Shop"}}, slog.New(slog.NewJSONHandler(&quiet, nil)))
	observeSensor(t, q, `{"print":{"ams":{"ams":[{"id":"0","temp":30}]}}}`)
	if quiet.String() != "" {
		t.Fatalf("default-level logger emitted diagnostics: %q", quiet.String())
	}
}
