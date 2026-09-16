package upstream

import (
	"testing"
	"time"
)

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
