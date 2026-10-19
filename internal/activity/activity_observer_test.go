package activity

import (
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// TestObserverReceivesRecordedEntries pins the observer contract: the
// callback sees the recorded serial and entry, unknown serials notify
// nobody, and a nil log or nil function is safe.
func TestObserverReceivesRecordedEntries(t *testing.T) {
	log := New([]config.Printer{{Serial: "S1"}})
	type call struct {
		serial string
		e      Entry
	}
	var calls []call
	log.SetObserver(func(serial string, e Entry) {
		calls = append(calls, call{serial, e})
	})

	log.Record("S1", "print_finished", Info, "done")
	log.Record("unknown", "printer_problem", Error, "ignored")
	if len(calls) != 1 || calls[0].serial != "S1" {
		t.Fatalf("observer calls = %+v, want one call for S1", calls)
	}
	stored := log.Recent("S1")[0]
	observed := calls[0].e
	if observed.ID == 0 || observed.ID != stored.ID || !observed.Time.Equal(stored.Time) ||
		observed.Kind != stored.Kind || observed.Severity != stored.Severity ||
		observed.Message != stored.Message || observed.URL != stored.URL {
		t.Fatalf("observer entry %+v differs from stored %+v", observed, stored)
	}

	// A nil function clears the observer.
	log.SetObserver(nil)
	log.Record("S1", "print_started", Info, "again")
	if len(calls) != 1 {
		t.Fatalf("cleared observer still called: %+v", calls)
	}

	// A nil log ignores registration.
	var nilLog *Log
	nilLog.SetObserver(func(string, Entry) {})
}

// TestObserverRecordingAgainDoesNotDeadlock pins that Record releases the
// log mutex before invoking the observer, so a callback that records
// another event cannot deadlock the log.
func TestObserverRecordingAgainDoesNotDeadlock(t *testing.T) {
	log := New([]config.Printer{{Serial: "S1"}})
	reentered := false
	done := make(chan struct{})
	log.SetObserver(func(serial string, e Entry) {
		if reentered {
			close(done)
			return
		}
		reentered = true
		log.Record(serial, "observer_echo", Info, "echo")
	})
	go func() {
		log.Record("S1", "print_finished", Info, "done")
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("observer re-recording deadlocked")
	}
}
