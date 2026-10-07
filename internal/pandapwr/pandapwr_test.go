package pandapwr

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// TestPandaPwrPollSequence drives one store against an httptest device
// through the three behaviors the poll loop must survive: a busy failure
// that a fast retry clears, an outage that ages the reading out of the
// freshness window, and a shutdown that must not wait behind a stalled
// request. The test replaces the package delay and clock variables, so
// every bounded wait here stays far under a second of real time.
func TestPandaPwrPollSequence(t *testing.T) {
	prodDelay := pollDelay
	prodNow := timeNow
	defer func() { pollDelay, timeNow = prodDelay, prodNow }()

	// The production delay function draws each kind inside its contract
	// range: an initial 0-30 s, a 2-5 s retry, and a 25-35 s cadence.
	// Sampling before the swap keeps the constants honest without
	// sleeping on them.
	for i := 0; i < 100; i++ {
		if d := prodDelay(delayInitial); d < 0 || d >= 30*time.Second {
			t.Fatalf("initial delay %v outside [0s, 30s)", d)
		}
		if d := prodDelay(delayRetry); d < 2*time.Second || d >= 5*time.Second {
			t.Fatalf("retry delay %v outside [2s, 5s)", d)
		}
		if d := prodDelay(delayCadence); d < 25*time.Second || d >= 35*time.Second {
			t.Fatalf("cadence delay %v outside [25s, 35s)", d)
		}
	}

	// Clock seam: the store reads its observation and freshness times
	// through timeNow, so the test advances time instead of sleeping.
	clock := &fakeClock{now: time.Now()}
	timeNow = clock.Now

	// Delay seam: per-kind waits the test controls; zero makes the first
	// poll and the retry immediate, and a short cadence keeps the loop
	// live without spinning.
	var schedMu sync.Mutex
	sched := map[delayKind]time.Duration{
		delayInitial: 0,
		delayRetry:   0,
		delayCadence: 5 * time.Millisecond,
	}
	var kinds []delayKind
	pollDelay = func(k delayKind) time.Duration {
		schedMu.Lock()
		defer schedMu.Unlock()
		kinds = append(kinds, k)
		return sched[k]
	}
	setDelay := func(k delayKind, d time.Duration) {
		schedMu.Lock()
		sched[k] = d
		schedMu.Unlock()
	}
	kindsFrom := func(i int) []delayKind {
		schedMu.Lock()
		defer schedMu.Unlock()
		return append([]delayKind(nil), kinds[i:]...)
	}

	// Device: the first request answers 503 once; the test then flips
	// the mode between a measurement frame, a permanent 503, and a
	// stalled response.
	var reqs atomic.Int64
	var modeMu sync.Mutex
	const (
		modeOK = iota
		modeFail
		modeStall
	)
	mode := modeOK
	gate := make(chan struct{}) // closed once to release stalled handlers
	var releaseStall sync.Once
	release := func() { releaseStall.Do(func() { close(gate) }) }
	stallEntered := make(chan struct{}, 8)
	handlerDone := make(chan struct{}, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqs.Add(1)
		modeMu.Lock()
		m := mode
		modeMu.Unlock()
		switch m {
		case modeFail:
			w.WriteHeader(http.StatusServiceUnavailable)
			select {
			case handlerDone <- struct{}{}:
			default:
			}
			return
		case modeStall:
			select {
			case stallEntered <- struct{}{}:
			default:
			}
			<-gate
		}
		if n == 1 {
			// Busy failure: the device accepts one request at a time,
			// and a concurrent client just took its slot.
			w.WriteHeader(http.StatusServiceUnavailable)
			select {
			case handlerDone <- struct{}{}:
			default:
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"countdown_state":0,"auto_poweroff":"0","countdown":"0","voltage":229,"current":0,"power":46.6,"power_state":1,"usb_state":0,"ele":0}`))
		select {
		case handlerDone <- struct{}{}:
		default:
		}
	}))
	defer srv.Close()

	const serial = "TESTSERIAL01"
	s := New([]config.Printer{{Serial: serial,
		Settings: map[string]any{AddressKey: strings.TrimPrefix(srv.URL, "http://")}}}, logger())
	if s.moduleState("OTHERSERIAL") != nil {
		t.Fatal("unconfigured printer returned state")
	}
	s.Start()
	defer func() {
		s.Stop()
		release()
	}()

	// (a) A busy failure is followed by a fast retry that succeeds, and
	// the state becomes connected with the rounded reading.
	waitFor(t, 2*time.Second, func() bool {
		v := s.moduleState(serial)
		st, _ := v.(ModuleState)
		return v != nil && st.Link == LinkConnected && st.PowerW != nil && *st.PowerW == 47
	})
	waitFor(t, 2*time.Second, func() bool {
		ks := kindsFrom(0)
		return len(ks) >= 3 && ks[0] == delayInitial && ks[1] == delayRetry && ks[2] == delayCadence
	})
	data, err := json.Marshal(s.moduleState(serial))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"link":"connected","power_w":47}` {
		t.Fatalf("ModuleState JSON = %s", data)
	}
	d := s.Display(serial)
	if d == nil || d.Panel == nil || d.Panel.Title != "Panda PWR" ||
		len(d.Panel.Rows) != 2 ||
		d.Panel.Rows[0].Label != "Link" || d.Panel.Rows[0].Value != "Connected" || d.Panel.Rows[0].Tone != "calm" ||
		d.Panel.Rows[1].Label != "Power" || d.Panel.Rows[1].Value != "47 W" {
		t.Fatalf("fresh display = %+v", d)
	}

	// (b) After the reading ages past freshness and the polls fail, the
	// state becomes offline and power_w is absent. The mode flips
	// first; the clock advances only after a post-flip failure has been
	// recorded (the loop schedules a fast retry after one), so no
	// in-flight success can land on the moved clock and stay fresh.
	modeMu.Lock()
	mode = modeFail
	modeMu.Unlock()
	waitFor(t, 2*time.Second, func() bool {
		ks := kindsFrom(0)
		if len(ks) < 3 || ks[0] != delayInitial || ks[1] != delayRetry || ks[2] != delayCadence {
			return false
		}
		// The flip must produce at least one recorded post-(a) failure:
		// a second retry kind beyond the busy-failure retry. The tail is
		// volatile (a zero-length retry is followed at once by a
		// cadence), so counting is the stable signal.
		retries := 0
		for _, k := range ks {
			if k == delayRetry {
				retries++
			}
		}
		return retries > 1
	})
	clock.Advance(freshWindow + time.Minute)
	waitFor(t, 2*time.Second, func() bool {
		v := s.moduleState(serial)
		st, _ := v.(ModuleState)
		return v != nil && st.Link == LinkOffline && st.PowerW == nil
	})
	data, err = json.Marshal(s.moduleState(serial))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"link":"offline"}` {
		t.Fatalf("stale ModuleState JSON = %s", data)
	}
	d = s.Display(serial)
	if d == nil || d.Panel == nil || d.Panel.Title != "Panda PWR" ||
		len(d.Panel.Rows) != 1 ||
		d.Panel.Rows[0].Label != "Link" || d.Panel.Rows[0].Value != "Offline" || d.Panel.Rows[0].Tone != "warn" {
		t.Fatalf("stale display = %+v", d)
	}

	// (c) Stop returns quickly while a request is stalled on the server.
	setDelay(delayRetry, time.Millisecond)
	setDelay(delayCadence, time.Millisecond)
	modeMu.Lock()
	mode = modeStall
	modeMu.Unlock()
	waitFor(t, time.Second, func() bool {
		select {
		case <-stallEntered:
			return true
		default:
			return false
		}
	})
	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return while the request was stalled")
	}
	// Read hooks stay safe after Stop and never block on the device.
	if s.moduleState(serial) == nil {
		t.Fatal("state disappeared after Stop")
	}
	if s.Display(serial) == nil {
		t.Fatal("display disappeared after Stop")
	}
	// Release the stalled handler so the server can shut down cleanly.
	release()
	waitFor(t, time.Second, func() bool {
		select {
		case <-handlerDone:
			return true
		default:
			return false
		}
	})
}

// fakeClock is a mutex-guarded stand-in for timeNow.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// Now returns the fake time.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the fake time forward.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// waitFor runs f until it holds or the deadline passes, sleeping only
// milliseconds between tries.
func waitFor(t *testing.T, d time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a condition")
}

// logger returns a quiet process logger for the store.
func logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestAddressSettingValidate covers the address validation: a bare host or
// host:port form is accepted and every scheme form is rejected.
func TestAddressSettingValidate(t *testing.T) {
	accept := []string{"192.168.4.1", "10.0.0.5:8080", "panda-pwr.local"}
	for _, in := range accept {
		if err := AddressSetting.Validate(in); err != nil {
			t.Errorf("Validate(%q) rejected: %v", in, err)
		}
	}
	reject := []string{"http://192.168.4.1", "https://192.168.4.1/update_ele_data", "192.168.4.1/"}
	for _, in := range reject {
		if err := AddressSetting.Validate(in); err == nil {
			t.Errorf("Validate(%q) accepted, want rejection", in)
		}
	}
}

// TestNewBuildsHTTPHostTarget requires New to build the http:// base from a
// bare host name instead of trimming a stored URL.
func TestNewBuildsHTTPHostTarget(t *testing.T) {
	s := New([]config.Printer{{Serial: "S1",
		Settings: map[string]any{AddressKey: "192.168.4.1"}}}, logger())
	if len(s.targets) != 1 || s.targets[0].addr != "http://192.168.4.1" {
		t.Fatalf("targets = %+v, want S1 at http://192.168.4.1", s.targets)
	}
	if s.moduleState("S1") == nil {
		t.Fatal("configured printer returned no state")
	}
}
