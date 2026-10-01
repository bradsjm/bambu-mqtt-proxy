package upstream

import (
	"errors"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"bambu-mqtt-proxy/internal/config"
)

func TestConnectivityObserverReceivesConnectionLoss(t *testing.T) {
	var (
		gotSerial    string
		gotConnected bool
		gotErr       error
		hit          = make(chan struct{}, 2)
	)
	p := NewPool([]config.Printer{{Serial: "S1"}}, nopInject{}, config.Behavior{
		UpstreamConnectTimeoutSeconds: 1,
		UpstreamBackoffInitialSeconds: 1,
		UpstreamBackoffMaxSeconds:     1,
	}, discardLogger())
	p.SetConnectivityObserver(func(serial string, connected bool, err error) {
		gotSerial, gotConnected, gotErr = serial, connected, err
		hit <- struct{}{}
	})
	c := p.conn("S1")
	fake := newFakePaho(true)
	var t1 *transport
	c.newClient = func(tr *transport) mqtt.Client {
		t1 = tr
		return fake
	}
	c.subscribeAsync("device/S1/report")
	waitChannel(t, 2*time.Second, hit)
	if t1 == nil || t1.generation == 0 {
		t.Fatalf("transport installed = %v with generation %d, want an installed generation", t1 != nil, t1.generation)
	}
	wantErr := errors.New("lost")
	t1.signalLoss(wantErr)
	waitChannel(t, 2*time.Second, hit)
	if gotSerial != "S1" || gotConnected || !errors.Is(gotErr, wantErr) {
		t.Fatalf("observer got %q/%v/%v, want S1/false/lost", gotSerial, gotConnected, gotErr)
	}
	c.stop()
}

// waitChannel waits for one receive on ch.
func waitChannel(t *testing.T, timeout time.Duration, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("channel not signaled within %s", timeout)
	}
}

func TestNextBackoff(t *testing.T) {
	initial := time.Second
	max := 30 * time.Second
	cases := []struct {
		n  int
		lo time.Duration
		hi time.Duration
	}{
		{1, 800 * time.Millisecond, 1200 * time.Millisecond},
		{2, 1600 * time.Millisecond, 2400 * time.Millisecond},
		{3, 3200 * time.Millisecond, 4800 * time.Millisecond},
		{10, 24 * time.Second, 36 * time.Second},
		{100, 24 * time.Second, 36 * time.Second},
	}
	for _, c := range cases {
		for range 20 {
			got := nextBackoff(c.n, initial, max)
			if got < c.lo || got > c.hi {
				t.Fatalf("nextBackoff(%d) = %v, want in [%v, %v]", c.n, got, c.lo, c.hi)
			}
		}
	}
}

func TestReportInterestCountTransitions(t *testing.T) {
	// No active transport: recording works, completions reconcile to a
	// no-op, and the count is the only observable state.
	c := &Conn{spec: config.Printer{Serial: "A"}}
	c.recordSubscribe("device/A/report")
	c.recordSubscribe("device/+/report")
	c.recordSubscribe("device/A/report")
	if got := c.lockReportRefs(); got != 3 {
		t.Fatalf("count after adds = %d, want 3 (distinct filters share one report interest)", got)
	}

	c.recordUnsubscribe("device/A/report")
	c.recordUnsubscribe("device/+/report")
	if got := c.lockReportRefs(); got != 1 {
		t.Fatalf("count with remaining subscribers = %d, want 1", got)
	}
	c.recordUnsubscribe("device/A/report")
	if got := c.lockReportRefs(); got != 0 {
		t.Fatalf("count after final remove = %d, want 0", got)
	}
	// A remove past zero stays balanced at zero instead of going negative.
	c.recordUnsubscribe("device/A/report")
	if got := c.lockReportRefs(); got != 0 {
		t.Fatalf("count after remove past zero = %d, want 0", got)
	}
}

// TestRequestOnlyFilterRecordsNoInterest pins that request-only filters
// produce no report interest and no upstream work at all.
func TestRequestOnlyFilterRecordsNoInterest(t *testing.T) {
	c := &Conn{spec: config.Printer{Serial: "A"}}
	if done := c.recordSubscribe("device/A/request"); done == nil {
		t.Fatal("request-only subscribe must still return a completion")
	}
	done := c.recordUnsubscribe("device/A/request")
	done()
	if got := c.lockReportRefs(); got != 0 {
		t.Fatalf("report interest count = %d, want 0 for request-only filters", got)
	}
}
