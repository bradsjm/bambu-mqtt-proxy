package printer

import (
	"reflect"
	"testing"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
)

// TestEventReplay checks exact report-order events across captured printer reports.
func TestEventReplay(t *testing.T) {
	cases := []struct {
		name  string
		kinds []string
	}{
		{"live_01S00C351100139_20261010.jsonl", []string{"state_initial", "print_preparing", "print_started", "first_layer_complete"}},
		{"p1s_edge_cases.jsonl", []string{"state_initial", "print_started", "first_layer_complete", "print_finished", "print_started"}},
		{"p1s_cancel_resend.jsonl", []string{"state_initial", "print_started", "print_paused", "hms_alert", "print_error", "print_cancelled", "alerts_cleared", "alerts_cleared", "print_started", "first_layer_complete"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewStore([]config.Printer{{Serial: "S1"}})
			log := activity.New([]config.Printer{{Serial: "S1"}})
			s.SetActivity(log)
			var got []string
			s.Subscribe(func(e Event) {
				// Activity must already include this report's last transition.
				if len(e.Kinds) > 0 && log.Recent(e.Serial)[0].Kind != e.Kinds[len(e.Kinds)-1] {
					t.Fatal("subscriber preceded activity recording")
				}
				got = append(got, e.Kinds...)
				s.Get(e.Serial)
			})
			replayFixture(t, s, "S1", "testdata/"+tc.name)
			if !reflect.DeepEqual(got, tc.kinds) {
				t.Fatalf("kinds = %v, want %v", got, tc.kinds)
			}
		})
	}
	t.Run("boot mid-print", func(t *testing.T) {
		s := NewStore([]config.Printer{{Serial: "S1"}})
		var kinds []string
		s.Subscribe(func(e Event) { kinds = append(kinds, e.Kinds...) })
		s.Apply("S1", []byte(`{"print":{"gcode_state":"RUNNING","layer_num":20,"print_error":123,"hms":[{"attr":50331904,"code":65544}]}}`))
		if !reflect.DeepEqual(kinds, []string{"state_initial"}) {
			t.Fatalf("boot kinds = %v", kinds)
		}
	})
	t.Run("connectivity", func(t *testing.T) {
		s := NewStore([]config.Printer{{Serial: "S1"}})
		log := activity.New([]config.Printer{{Serial: "S1"}})
		s.SetActivity(log)
		s.Apply("S1", []byte(`{"print":{"gcode_state":"RUNNING"}}`))
		var kinds []string
		s.Subscribe(func(e Event) {
			if !reflect.DeepEqual(e.Prev, e.Cur) || log.Recent(e.Serial)[0].Kind != e.Kinds[0] {
				t.Fatal("connectivity changed the document or preceded activity")
			}
			if e.Online != (e.Kinds[0] == "connected") {
				t.Fatal("connectivity does not match the event")
			}
			kinds = append(kinds, e.Kinds...)
		})
		s.SetOnline("S1", true)
		s.SetOnline("S1", true)
		s.SetOnline("S1", false)
		if !reflect.DeepEqual(kinds, []string{"connected", "disconnected"}) {
			t.Fatalf("connectivity kinds = %v", kinds)
		}
		doc, online, known := s.Get("S1")
		if state, _ := doc.GcodeState(); state != "RUNNING" || online || !known {
			t.Fatal("disconnect did not preserve the running document")
		}
	})
}
