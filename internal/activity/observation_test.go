package activity

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// TestRecordObservedCarriesEvidenceByValue pins the evidence contract: the
// observer and Recent see the exact Observation value recorded, and the
// evidence never serializes into the activity HTTP JSON.
func TestRecordObservedCarriesEvidenceByValue(t *testing.T) {
	log := New([]config.Printer{{Serial: "S1"}})
	var observed []Entry
	log.SetObserver(func(serial string, e Entry) {
		observed = append(observed, e)
	})

	at := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	want := Observation{JobGen: 4, JobRevision: 2, RunningEpoch: 5,
		SessionGen: 7, Epoch: 3, StateGen: 12, ObsGen: 12, ObsAt: at}
	log.RecordObserved("S1", "first_layer_complete", Info, "First layer complete", want)

	if len(observed) != 1 {
		t.Fatalf("observer calls = %d, want 1", len(observed))
	}
	if observed[0].Observation != want {
		t.Fatalf("observer observation = %+v, want %+v", observed[0].Observation, want)
	}
	stored := log.Recent("S1")[0]
	if stored.Observation != want {
		t.Fatalf("stored observation = %+v, want %+v", stored.Observation, want)
	}
	if observed[0].ID != stored.ID || !observed[0].Time.Equal(stored.Time) {
		t.Fatalf("observer entry %+v differs from stored %+v", observed[0], stored)
	}

	mux := http.NewServeMux()
	log.Register(mux)
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/activity/S1", nil))
	body := res.Body.String()
	for _, leak := range []string{"Observation", "SessionGen", "Epoch", "StateGen", "ObsGen", "ObsAt",
		"JobGen", "JobRevision", "RunningEpoch"} {
		if strings.Contains(body, leak) {
			t.Fatalf("GET /activity/S1 leaked %q: %s", leak, body)
		}
	}
	var payload struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	for k := range payload.Events[0] {
		keys[k] = true
	}
	wantKeys := map[string]bool{"id": true, "time": true, "kind": true, "severity": true, "message": true, "age_seconds": true}
	if len(keys) != len(wantKeys) {
		t.Fatalf("entry JSON keys = %v, want %v", keys, wantKeys)
	}
	for k := range wantKeys {
		if !keys[k] {
			t.Fatalf("entry JSON missing key %q: got %v", k, keys)
		}
	}
}

// TestRecordOmitsEvidence pins that the ordinary Record path stays
// unchanged: no evidence and identical ID/timestamp/kind handling.
func TestRecordOmitsEvidence(t *testing.T) {
	log := New([]config.Printer{{Serial: "S1"}})
	var observed []Entry
	log.SetObserver(func(serial string, e Entry) {
		observed = append(observed, e)
	})
	log.Record("S1", "print_finished", Info, "done")
	if observed[0].Observation != (Observation{}) {
		t.Fatalf("observer observation = %+v, want zero", observed[0].Observation)
	}
	stored := log.Recent("S1")[0]
	if stored.Observation != (Observation{}) {
		t.Fatalf("stored observation = %+v, want zero", stored.Observation)
	}
	if stored.Kind != "print_finished" || stored.Severity != Info || stored.ID == 0 {
		t.Fatalf("ordinary entry = %+v", stored)
	}
}

// TestRecordObservedNilAndUnknownIgnored pins that the nil-log guard and
// the unknown-serial guard behave exactly like Record's.
func TestRecordObservedNilAndUnknownIgnored(t *testing.T) {
	var nilLog *Log
	nilLog.RecordObserved("S1", "first_layer_complete", Info, "ignored", Observation{SessionGen: 1})
	if got := nilLog.Recent("S1"); got != nil {
		t.Fatalf("nil log returned events: %v", got)
	}

	var seen []Entry
	log := New([]config.Printer{{Serial: "S1"}})
	log.SetObserver(func(serial string, e Entry) {
		seen = append(seen, e)
	})
	log.RecordObserved("unknown", "first_layer_complete", Info, "ignored", Observation{SessionGen: 1})
	if len(seen) != 0 {
		t.Fatalf("observer called for unknown serial: %+v", seen)
	}
}
