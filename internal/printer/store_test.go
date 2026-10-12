package printer

import (
	"bufio"
	"encoding/json"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// TestStoreMerge protects replacement, unrelated data, and published-document immutability.
func TestStoreMerge(t *testing.T) {
	s := NewStore([]config.Printer{{Serial: "S"}})
	s.Apply("S", []byte(`{"print":{"msg":1,"gcode_state":"RUNNING","nested":{"keep":1,"change":2},"array":[1,2]},"system":{"keep":true}}`))
	prev, _, _ := s.Get("S")
	before, _ := json.Marshal(prev)
	var readers sync.WaitGroup
	readers.Add(1)
	done := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		defer readers.Done()
		json.Marshal(prev)
		close(ready)
		for {
			select {
			case <-done:
				return
			default:
				raw, err := json.Marshal(prev)
				if err != nil || string(raw) != string(before) {
					t.Error("published document changed")
					return
				}
			}
		}
	}()
	<-ready
	cases := []struct {
		payload string
		want    string
	}{
		{`{"print":{"nested":{"change":3},"array":[4]}}`, `{"print":{"msg":1,"gcode_state":"RUNNING","nested":{"keep":1,"change":3},"array":[4]},"system":{"keep":true}}`},
		{`{"print":{"nested":{"change":null}}}`, `{"print":{"msg":1,"gcode_state":"RUNNING","nested":{"keep":1,"change":null},"array":[4]},"system":{"keep":true}}`},
		{`{"print":{"msg":"0","mc_percent":"4"}}`, `{"print":{"msg":"0","mc_percent":"4","gcode_state":"RUNNING","nested":{"keep":1,"change":null},"array":[4]},"system":{"keep":true}}`},
		{`{"print":{"msg":0,"gcode_state":"IDLE"}}`, `{"print":{"msg":0,"gcode_state":"IDLE"},"system":{"keep":true}}`},
		{`[]`, `{"print":{"msg":0,"gcode_state":"IDLE"},"system":{"keep":true}}`},
		{`{"system":{"print":{"keep":1}}}`, `{"print":{"msg":0,"gcode_state":"IDLE"},"system":{"keep":true,"print":{"keep":1}}}`},
		{`{"system":{"print":{"msg":0}}}`, `{"print":{"msg":0,"gcode_state":"IDLE"},"system":{"keep":true,"print":{"keep":1,"msg":0}}}`},
	}
	for _, c := range cases {
		s.Apply("S", []byte(c.payload))
		got, _, _ := s.Get("S")
		var want Doc
		json.Unmarshal([]byte(c.want), &want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("payload %s: got %#v want %#v", c.payload, got, want)
		}
	}
	close(done)
	readers.Wait()
	after, _ := json.Marshal(prev)
	if string(after) != string(before) {
		t.Fatal("published document mutated")
	}
}

// TestLiveReplay protects the final view contract against a captured P1S delta stream.
func TestLiveReplay(t *testing.T) {
	s := NewStore([]config.Printer{{Serial: "01S00C351100139", Name: "Shop", Model: "P1S"}})
	replayFixture(t, s, "01S00C351100139", "testdata/live_01S00C351100139_20261010.jsonl")
	s.SetOnline("01S00C351100139", true)
	v, _ := s.View("01S00C351100139")
	if v.PrintState == nil || *v.PrintState != "RUNNING" || !v.Printing || v.JobName == nil || *v.JobName != "Sunlu_AMS_Heater_Desiccant_Mount_V.2" || v.Progress == nil || *v.Progress != 4 || v.RemainingMinutes == nil || *v.RemainingMinutes != 567 || v.LayerNum == nil || *v.LayerNum != 2 || v.TotalLayers == nil || *v.TotalLayers != 500 || v.NozzleTemp == nil || *v.NozzleTemp != 270.0625 || v.BedTemp == nil || *v.BedTemp != 99.96875 || v.WifiSignalDBm == nil || *v.WifiSignalDBm != -70 || v.SpeedProfile == nil || *v.SpeedProfile != "standard" {
		t.Fatalf("final view = %+v", v)
	}
	if !v.Connected || !v.Freshness.Fresh || v.Freshness.LastReportAgeSeconds == nil || v.ChamberTemp != nil || len(v.AMS) != 1 || v.AMS[0].Temp == nil || *v.AMS[0].Temp != 31.0 || v.ExtSpool == nil || v.ExtSpool.Material != "PLA" {
		t.Fatalf("final sensors = %+v", v)
	}
}

// replayFixture applies every captured payload in the JSONL file at path to
// serial, in file order.
func replayFixture(t *testing.T, s *Store, serial, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 65536), 1<<20)
	for scan.Scan() {
		var line struct {
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(scan.Bytes(), &line); err != nil {
			t.Fatal(err)
		}
		s.Apply(serial, line.Payload)
	}
	if err := scan.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestAccessoryChamber protects native-sensor precedence and the accessory display window.
func TestAccessoryChamber(t *testing.T) {
	for _, c := range []struct {
		model string
		age   time.Duration
		want  bool
	}{{"P1S", time.Second, true}, {"P1S", 16 * time.Second, false}, {"X1C", time.Second, true}} {
		s := NewStore([]config.Printer{{Serial: "S", Model: c.model}})
		s.Apply("S", []byte(`{"print":{"chamber_temper":42}}`))
		s.SetChamberReader(func(string) (float64, time.Time, bool) { return 30, time.Now().Add(-c.age), true })
		v, _ := s.View("S")
		if (v.ChamberTemp != nil) != c.want {
			t.Fatalf("%s age %s chamber %v", c.model, c.age, v.ChamberTemp)
		}
		if c.model == "X1C" && *v.ChamberTemp != 42 {
			t.Fatal("accessory replaced native sensor")
		}
	}
}

// TestAMSExistBitsHex protects occupancy across AMS units: Bambu sends the mask in hex.
func TestAMSExistBitsHex(t *testing.T) {
	unit := func(id string) map[string]any { return map[string]any{"id": id, "tray": []any{}} }
	doc := Doc{"print": map[string]any{"ams": map[string]any{"tray_exist_bits": "11", "ams": []any{unit("0"), unit("1")}}}}
	units, _ := doc.AMS()
	var got []bool
	for _, u := range units {
		for _, s := range u.Slots {
			got = append(got, *s.Loaded)
		}
	}
	want := []bool{true, false, false, false, true, false, false, false}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded slots %v, want %v", got, want)
	}
}
