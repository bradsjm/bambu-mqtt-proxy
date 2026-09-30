// Report ordering tests: per-transport callbacks are serialized in wire
// order, a blocked observer cannot reorder or drop later reports, and a late
// report from a retired transport never reaches the observer or downstream.
package upstream

import (
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// TestReportOrderingSurvivesBlockedObserver feeds RUNNING, FINISH, and a
// temperature-only report in wire order while the first observer blocks: the
// downstream injections keep wire order and the telemetry merge keeps both
// the FINISH state and the temperature fields.
func TestReportOrderingSurvivesBlockedObserver(t *testing.T) {
	fake := newFakePaho(true)
	c := newTestConn(fake, 7)
	inject := &recordingInject{}
	c.inject = inject

	printers := []config.Printer{{Serial: "S1", Name: "Shop"}}
	cache := telemetry.NewCache(printers, discardLogger())
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	c.observe = func(serial string, seq, gen uint64, payload []byte) {
		once.Do(func() {
			close(entered)
			<-release
		})
		cache.ObserveReport(serial, seq, gen, payload)
	}

	t1 := c.lockActive()
	done := make(chan struct{}, 3)
	go func() {
		c.onMessage(t1, fakeMessage{payload: []byte(`{"print":{"gcode_state":"RUNNING"}}`)})
		done <- struct{}{}
	}()
	<-entered
	// One feeder delivers the later reports back to back: wire order is
	// reportMu acquisition order, which onMessage serializes, so FINISH is
	// queued before the temperature report and neither is lost while the
	// first observer blocks.
	go func() {
		c.onMessage(t1, fakeMessage{payload: []byte(`{"print":{"gcode_state":"FINISH"}}`)})
		done <- struct{}{}
		c.onMessage(t1, fakeMessage{payload: []byte(`{"print":{"nozzle_temper":212.5}}`)})
		done <- struct{}{}
	}()

	// The blocked first observer holds both later reports; nothing is lost.
	select {
	case <-done:
		t.Fatal("later reports proceeded while the first observer was blocked")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("report callbacks did not complete")
		}
	}

	want := []string{
		"device/S1/report|" + `{"print":{"gcode_state":"RUNNING"}}`,
		"device/S1/report|" + `{"print":{"gcode_state":"FINISH"}}`,
		"device/S1/report|" + `{"print":{"nozzle_temper":212.5}}`,
	}
	got := inject.snapshot()
	if len(got) != len(want) {
		t.Fatalf("downstream order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("downstream order = %v, want %v", got, want)
		}
	}
	st, ok := cache.State("S1")
	if !ok {
		t.Fatal("telemetry state missing")
	}
	if st.PrintingState != "FINISH" {
		t.Fatalf("merged gcode_state = %q, want FINISH", st.PrintingState)
	}
	if st.NozzleTemp == nil || *st.NozzleTemp != 212.5 {
		t.Fatalf("merged nozzle temp = %v, want 212.5", st.NozzleTemp)
	}
}

// TestLateRetiredTransportReportRejected replaces the transport and then
// delivers a report from the old one: nothing is observed, merged, or
// injected downstream.
func TestLateRetiredTransportReportRejected(t *testing.T) {
	fake1 := newFakePaho(true)
	c := newTestConn(fake1, 7)
	inject := &recordingInject{}
	c.inject = inject
	printers := []config.Printer{{Serial: "S1", Name: "Shop"}}
	cache := telemetry.NewCache(printers, discardLogger())
	var observed int
	c.observe = func(serial string, seq, gen uint64, payload []byte) {
		observed++
		cache.ObserveReport(serial, seq, gen, payload)
	}

	t1 := c.lockActive()
	fake2 := newFakePaho(true)
	t2 := installTransport(c, fake2)

	c.onMessage(t1, fakeMessage{payload: []byte(`{"print":{"gcode_state":"RUNNING"}}`)})
	if observed != 0 {
		t.Fatalf("late retired-transport report observed %d times, want 0", observed)
	}
	if got := inject.snapshot(); len(got) != 0 {
		t.Fatalf("late report injected downstream: %v", got)
	}

	// The replacement's own report still flows.
	c.onMessage(t2, fakeMessage{payload: []byte(`{"print":{"gcode_state":"RUNNING"}}`)})
	if observed != 1 {
		t.Fatalf("replacement report observed %d times, want 1", observed)
	}
	st, ok := cache.State("S1")
	if !ok || st.PrintingState != "RUNNING" {
		t.Fatalf("replacement report not merged: %v/%v", st, ok)
	}
}
