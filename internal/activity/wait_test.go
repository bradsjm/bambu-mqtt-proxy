package activity

import (
	"bambu-mqtt-proxy/internal/config"
	"context"
	"testing"
	"time"
)

// TestWaitReadsNewerEntriesAndCancels verifies event filtering, wakeup, and cancellation.
func TestWaitReadsNewerEntriesAndCancels(t *testing.T) {
	log := New([]config.Printer{{Serial: "S1"}, {Serial: "S2"}})
	log.Record("S1", "started", Info, "started")
	after := log.Recent("S1")[0].ID
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan []Entry, 1)
	go func() { result <- log.Wait(ctx, "S1", after) }()
	log.Record("S2", "other", Info, "other")
	log.Record("S1", "paused", Info, "paused")
	entries := <-result
	if len(entries) != 1 || entries[0].Kind != "paused" || entries[0].ID <= after {
		t.Fatalf("wait entries = %+v", entries)
	}
	// An already-cancelled context still reads available entries immediately.
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if got := log.Wait(cancelled, "S1", after); len(got) != 1 {
		t.Fatalf("immediate read = %+v", got)
	}
	if got := log.Wait(cancelled, "S1", entries[0].ID); len(got) != 0 {
		t.Fatalf("cancelled wait = %+v", got)
	}
	for _, serial := range []string{"S1", "unknown"} {
		if got := (*Log)(nil).Wait(cancelled, serial, 0); got != nil {
			t.Fatalf("nil wait = %+v", got)
		}
	}
	if got := log.Wait(ctx, "unknown", 0); got != nil {
		t.Fatalf("unknown wait = %+v", got)
	}
}
