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
	c.subscribeAsync("device/S1/report", 1)
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

func TestSubRefTransitions(t *testing.T) {
	s := &subRefs{refs: make(map[string]subRef)}

	if first := s.add("device/A/report", 1); !first {
		t.Fatal("first add should report first")
	}
	if first := s.add("device/A/report", 1); first {
		t.Fatal("second add should not report first")
	}
	if first := s.add("device/A/report", 0); first {
		t.Fatal("third add should not report first")
	}
	// QoS must only ever rise, never fall, across merged subscribers.
	if got := s.refs["device/A/report"].qos; got != 1 {
		t.Fatalf("qos after adds = %d, want 1", got)
	}

	if last := s.remove("device/A/report"); last {
		t.Fatal("remove with remaining subscribers should not report last")
	}
	if last := s.remove("device/A/report"); last {
		t.Fatal("second-to-last remove should not report last")
	}
	if last := s.remove("device/A/report"); !last {
		t.Fatal("final remove should report last")
	}
	if _, have := s.refs["device/A/report"]; have {
		t.Fatal("filter should be deleted at zero count")
	}
}

func TestRaiseQoS(t *testing.T) {
	c := &Conn{spec: config.Printer{Serial: "S1"}, subs: newSubRefs()}
	c.subs.add("device/S1/report", 0)

	// A repeated interest raises the stored maximum without adding a
	// refcount.
	c.raiseQoS("device/S1/report", 1)
	refs := c.subs.snapshot()
	if len(refs) != 1 || refs["device/S1/report"] != 1 {
		t.Fatalf("snapshot after raise = %v, want device/S1/report at qos 1", refs)
	}
	if got := c.subs.count("device/S1/report"); got != 1 {
		t.Fatalf("count after raise = %d, want unchanged 1", got)
	}

	// Lower or equal requests keep the stored maximum.
	c.raiseQoS("device/S1/report", 0)
	if got := c.subs.snapshot()["device/S1/report"]; got != 1 {
		t.Fatalf("qos after lower raise = %d, want 1", got)
	}

	// Request-only filters have no upstream interest to raise, and absent
	// filters stay absent.
	c.raiseQoS("device/S1/request", 1)
	c.raiseQoS("device/S2/report", 1)
	if got := c.subs.snapshot(); len(got) != 1 {
		t.Fatalf("snapshot after ignored raises = %v, want only device/S1/report", got)
	}
}
