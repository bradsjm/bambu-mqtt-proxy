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

func TestRecentKeepsNewestCapacityEntries(t *testing.T) {
	log := New([]config.Printer{{Serial: "S1"}})
	for range Capacity + 5 {
		log.Record("S1", "event", Info, "message")
	}
	events := log.Recent("S1")
	if len(events) != Capacity {
		t.Fatalf("got %d events, want %d", len(events), Capacity)
	}
	if events[0].ID != Capacity+5 || events[len(events)-1].ID != 6 {
		t.Fatalf("retained IDs = %d..%d, want %d..6", events[0].ID, events[len(events)-1].ID, Capacity+5)
	}
	if events[0].AgeSeconds < 0 || events[0].Time.Location() != time.UTC {
		t.Fatalf("timestamp/age = %v/%v", events[0].Time, events[0].AgeSeconds)
	}
}

func TestNilAndUnknownPrinterAreIgnored(t *testing.T) {
	var nilLog *Log
	nilLog.Record("S1", "event", Info, "message")
	if got := nilLog.Recent("S1"); got != nil {
		t.Fatalf("nil log returned events: %v", got)
	}
	log := New([]config.Printer{{Serial: "S1"}})
	log.Record("unknown", "event", Info, "message")
	if got := log.Recent("unknown"); got != nil {
		t.Fatalf("unknown printer returned events: %v", got)
	}
}

func TestRegisterActivityEndpoints(t *testing.T) {
	log := New([]config.Printer{{Serial: "S1"}, {Serial: "S2"}})
	log.Record("S1", "print_started", Info, "Print started")
	mux := http.NewServeMux()
	log.Register(mux)

	all := httptest.NewRecorder()
	mux.ServeHTTP(all, httptest.NewRequest(http.MethodGet, "/activity", nil))
	if all.Code != http.StatusOK || !strings.Contains(all.Header().Get("Cache-Control"), "no-store") {
		t.Fatalf("GET /activity: status=%d cache=%q", all.Code, all.Header().Get("Cache-Control"))
	}
	var payload struct {
		Printers map[string][]Entry `json:"printers"`
	}
	if err := json.Unmarshal(all.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Printers["S1"]) != 1 || payload.Printers["S2"] == nil {
		t.Fatalf("printer activity payload = %+v", payload.Printers)
	}

	one := httptest.NewRecorder()
	mux.ServeHTTP(one, httptest.NewRequest(http.MethodGet, "/activity/S1", nil))
	if one.Code != http.StatusOK || !strings.Contains(one.Body.String(), "print_started") {
		t.Fatalf("GET /activity/S1: status=%d body=%s", one.Code, one.Body.String())
	}
	missing := httptest.NewRecorder()
	mux.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/activity/NOPE", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("unknown serial status = %d, want 404", missing.Code)
	}
}
