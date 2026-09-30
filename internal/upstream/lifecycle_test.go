// Supervisor lifecycle tests: the supervisor owns every connect and loss
// transition, admits commands only on the installed transport, tears each
// attempt down before starting another, and never leaves unknown subscription
// state on a live connection.
package upstream

import (
	"errors"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"bambu-mqtt-proxy/internal/config"
)

// recordingInject records downstream publications in delivery order.
type recordingInject struct {
	mu    sync.Mutex
	items []string
}

func (r *recordingInject) PublishDownstream(topic string, payload []byte, _ byte) {
	r.mu.Lock()
	r.items = append(r.items, topic+"|"+string(payload))
	r.mu.Unlock()
}

func (r *recordingInject) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.items...)
}

// newSupervisedConn builds a conn whose supervisor installs fakes built by a
// test-controlled factory. tune, when set, configures each fake before use.
func newSupervisedConn(t *testing.T) (c *Conn, inject *recordingInject, latest func() *fakePaho, tune func(func(*fakePaho))) {
	t.Helper()
	inject = &recordingInject{}
	c = &Conn{
		spec:        config.Printer{Serial: "S1", Name: "Shop", Address: "S1.local:8883"},
		connectTO:   250 * time.Millisecond,
		backoffInit: 5 * time.Millisecond,
		backoffMax:  20 * time.Millisecond,
		warmup:      []string{`{"pushall":1}`},
		inject:      inject,
		log:         discardLogger(),
		stopCh:      make(chan struct{}),
		subs:        newSubRefs(),
		connCh:      make(chan struct{}),
	}
	var (
		mu      sync.Mutex
		clients []*fakePaho
		tuner   func(*fakePaho)
	)
	c.newClient = func(tr *transport) mqtt.Client {
		fake := newFakePaho(true)
		mu.Lock()
		fn := tuner
		mu.Unlock()
		if fn != nil {
			fn(fake)
		}
		mu.Lock()
		clients = append(clients, fake)
		mu.Unlock()
		return fake
	}
	latest = func() *fakePaho {
		mu.Lock()
		defer mu.Unlock()
		if len(clients) == 0 {
			return nil
		}
		return clients[len(clients)-1]
	}
	tune = func(fn func(*fakePaho)) {
		mu.Lock()
		tuner = fn
		mu.Unlock()
	}
	t.Cleanup(c.stop)
	return c, inject, latest, tune
}

// waitForCondition polls fn until true or the timeout elapses.
func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// TestAdmissionAfterReplacementRejectsOldGeneration pins the first race: a
// command admitted only after the transport was replaced must be rejected
// with no publish on either client.
func TestAdmissionAfterReplacementRejectsOldGeneration(t *testing.T) {
	fake1 := newFakePaho(true)
	c := newTestConn(fake1, 7)
	fake2 := newFakePaho(true)
	installTransport(c, fake2)

	if err := c.pause(7); err == nil {
		t.Fatal("pause validated on the replaced generation must fail")
	}
	if fake1.publishCount() != 0 || fake2.publishCount() != 0 {
		t.Fatalf("publishes on old/new = %d/%d, want 0/0", fake1.publishCount(), fake2.publishCount())
	}
	// The replacement's own generation is accepted.
	if err := c.pause(c.lockActive().generation); err != nil {
		t.Fatalf("pause on the replacement generation = %v, want success", err)
	}
	if fake2.publishCount() != 1 {
		t.Fatalf("replacement publishes = %d, want 1", fake2.publishCount())
	}
}

// TestAdmittedCommandAttemptsOnceOnCapturedClientOnly pins the second race:
// a command admitted before the replacement, blocked mid-publish, sends at
// most once on the captured old client and never on the replacement.
func TestAdmittedCommandAttemptsOnceOnCapturedClientOnly(t *testing.T) {
	fake1 := newFakePaho(true)
	c := newTestConn(fake1, 7)
	release := make(chan struct{})
	var started atomic.Bool
	fake1.onPublished = func() {
		started.Store(true)
		<-release
	}

	errCh := make(chan error, 1)
	go func() { errCh <- c.pause(7) }()
	waitForCondition(t, time.Second, func() bool { return started.Load() })

	fake2 := newFakePaho(true)
	installTransport(c, fake2)
	close(release)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("pause = %v, want local success (no post-send rejection)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pause did not complete")
	}
	if got := fake1.publishCount(); got != 1 {
		t.Fatalf("old client publishes = %d, want exactly one attempt", got)
	}
	if got := fake2.publishCount(); got != 0 {
		t.Fatalf("replacement publishes = %d, want zero", got)
	}
}

// TestStopDuringInitialConnectThenLateCompletion pins the shutdown race: stop
// during a pending Connect disconnects the in-flight client, ends the
// supervisor, and a Connect that completes late installs nothing.
func TestStopDuringInitialConnectThenLateCompletion(t *testing.T) {
	c, inject, latest, tune := newSupervisedConn(t)
	gate := make(chan struct{})
	tune(func(f *fakePaho) { f.connectGate = gate })

	c.subscribeAsync("device/S1/report", 1)
	var fake *fakePaho
	waitForCondition(t, time.Second, func() bool {
		fake = latest()
		return fake != nil && fake.connectCount() > 0
	})

	c.stop()
	if fake.disconnectCount() == 0 {
		t.Fatal("in-flight client was not disconnected on stop")
	}
	select {
	case <-c.supDone:
	default:
		t.Fatal("supervisor still running after stop")
	}

	// Complete the Connect only after the supervisor exited.
	close(gate)
	time.Sleep(50 * time.Millisecond)
	if c.lockActive() != nil {
		t.Fatal("late Connect installed a transport after stop")
	}
	if fake.subCount() != 0 {
		t.Fatalf("late install subscribed %d filters, want none", fake.subCount())
	}
	if len(inject.snapshot()) != 0 {
		t.Fatalf("downstream delivery after stop observed: %v", inject.snapshot())
	}
	if got := latest().connectCount(); got != 1 {
		t.Fatalf("client connect calls = %d, want exactly the one gated attempt", got)
	}
}

// TestCallsAfterPoolStopCreateNoClient pins pool-shutdown terminality: no
// entry point creates a client after Stop.
func TestCallsAfterPoolStopCreateNoClient(t *testing.T) {
	p := NewPool([]config.Printer{{Serial: "S1", Name: "Shop"}}, nopInject{}, config.Behavior{
		UpstreamConnectTimeoutSeconds: 1,
		UpstreamBackoffInitialSeconds: 1,
		UpstreamBackoffMaxSeconds:     1,
	}, discardLogger())
	var builds atomic.Int32
	{
		c := &Conn{
			spec:        config.Printer{Serial: "S1", Address: "S1.local:8883"},
			connectTO:   250 * time.Millisecond,
			backoffInit: 5 * time.Millisecond,
			backoffMax:  20 * time.Millisecond,
			inject:      &recordingInject{},
			log:         discardLogger(),
			stopCh:      make(chan struct{}),
			subs:        newSubRefs(),
			connCh:      make(chan struct{}),
		}
		c.newClient = func(tr *transport) mqtt.Client {
			builds.Add(1)
			return newFakePaho(true)
		}
		p.conns["S1"] = c
		c.subscribeAsync("device/S1/report", 1)
		waitForCondition(t, time.Second, func() bool { return c.lockActive() != nil })
	}

	p.Stop()
	before := builds.Load()

	p.Subscribe("S1", "device/S1/report", 1)
	p.SubscribeAsync("S1", "device/S1/report", 1)
	p.Unsubscribe("S1", "device/S1/report")
	p.RaiseQoS("S1", "device/S1/report", 1)
	p.PublishWithContext("S1", "device/S1/request", []byte(`{}`), 0, PublishContext{})
	p.EnsureConnected("S1", 10*time.Millisecond)
	if err := p.PausePrint("S1", 1); err == nil {
		t.Fatal("commands must fail after stop")
	}
	if builds.Load() != before {
		t.Fatalf("client builds after stop = %d, want unchanged %d", builds.Load(), before)
	}
	if p.conn("S1") != nil {
		t.Fatal("conn after shutdown must return nil")
	}
}

// TestReplacementWaitsForPreviousAttemptTeardown pins the attempt barrier: the
// replacement attempt starts only after the previous attempt's Disconnect
// completed.
func TestReplacementWaitsForPreviousAttemptTeardown(t *testing.T) {
	c, _, latest, _ := newSupervisedConn(t)
	c.subscribeAsync("device/S1/report", 1)
	var first *fakePaho
	waitForCondition(t, time.Second, func() bool {
		first = latest()
		return first != nil && c.lockActive() != nil
	})

	release := make(chan struct{})
	started := make(chan struct{})
	first.disconnectHook = func() {
		close(started)
		<-release
	}

	c.mu.Lock()
	t1 := c.active
	c.mu.Unlock()
	t1.signalLoss(errors.New("keepalive timeout"))

	<-started
	// While the first Disconnect is held, no replacement may exist or
	// connect.
	if second := latest(); second != first && second.connectCount() > 0 {
		t.Fatal("replacement connected while the previous Disconnect was still held")
	}
	close(release)
	waitForCondition(t, 2*time.Second, func() bool {
		a := c.lockActive()
		return a != nil && a.client != first
	})
}

// TestOpenSocketRegistersAndCancellationRejectsLateResults pins the socket
// barrier against a real loopback listener: a dialed socket is owned by the
// transport, and a dial losing the cancellation race is closed instead of
// registered.
func TestOpenSocketRegistersAndCancellationRejectsLateResults(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				buf := make([]byte, 64)
				for {
					if _, err := conn.Read(buf); err != nil {
						return
					}
				}
			}()
		}
	}()

	c := &Conn{spec: config.Printer{Serial: "S1"}, connectTO: 500 * time.Millisecond, log: discardLogger()}
	tr := newTestTransport(nil, 1)
	uri, err := url.Parse("tcp://" + l.Addr().String())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	conn, err := c.openSocket(tr, uri, mqtt.ClientOptions{ConnectTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("openSocket = %v, want success", err)
	}
	if sock := tr.takeSocket(); sock == nil {
		t.Fatal("socket was not registered on the transport")
	}
	conn.Close()

	// A canceled attempt never registers: cancel first, then dial.
	tr2 := newTestTransport(nil, 2)
	tr2.cancel()
	if _, err := c.openSocket(tr2, uri, mqtt.ClientOptions{ConnectTimeout: 500 * time.Millisecond}); err == nil {
		t.Fatal("openSocket after cancellation must fail")
	}
	if sock := tr2.takeSocket(); sock != nil {
		sock.Close()
		t.Fatal("canceled attempt registered a socket")
	}
}

// TestRetireClosesRegisteredSocketAndBumpsGeneration pins retirement:
// teardown closes the owned socket, clears the transport, and advances the
// generation so stale evidence goes stale.
func TestRetireClosesRegisteredSocketAndBumpsGeneration(t *testing.T) {
	fake := newFakePaho(true)
	c := newTestConn(fake, 3)
	t1 := c.lockActive()
	a, b := net.Pipe()
	defer b.Close()
	if err := t1.adoptSocket(a); err != nil {
		t.Fatalf("adoptSocket = %v", err)
	}

	c.retire(t1, false)

	if got := c.lockActive(); got != nil {
		t.Fatalf("active after retire = %v, want nil", got)
	}
	if gen := c.lockGeneration(); gen != 4 {
		t.Fatalf("generation after retire = %d, want 4", gen)
	}
	// The owned socket is closed: the far end observes the close instead of
	// blocking on a read.
	b.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err := b.Read(buf); err == nil {
		t.Fatal("pipe still readable; the retired socket was not closed")
	}
}

// TestSubscribeGrantFailuresForceReconnect pins the SUBACK contract: a
// missing grant, a 0x80 grant, a lower-QoS grant, a token error, and a token
// timeout each fail reconciliation, leave the upgrade unestablished on the
// failed transport, and drive a reconnect that restores the full desired set
// without deadlock. A QoS upgrade is the reconcile trigger because every
// downstream report filter merges onto the one report topic.
func TestSubscribeGrantFailuresForceReconnect(t *testing.T) {
	cases := []struct {
		name  string
		token *fakeToken
	}{
		{"missing grant", &fakeToken{grants: map[string]byte{}}},
		{"0x80 grant", &fakeToken{grants: map[string]byte{"device/S1/report": 0x80}}},
		{"lower qos grant", &fakeToken{grants: map[string]byte{"device/S1/report": 0}}},
		{"token error", &fakeToken{err: errors.New("not authorized")}},
		{"token timeout", &fakeToken{done: make(chan struct{})}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, _, latest, _ := newSupervisedConn(t)
			c.subscribeAsync("device/S1/report", 0)
			// Wait for the install's QoS 0 restore to be confirmed, so the
			// failing token below replaces only the upgrade's SUBACK.
			waitForCondition(t, time.Second, func() bool {
				_, on := c.lockOnWire()["device/S1/report"]
				return on
			})
			first := latest()
			firstGen := c.lockGeneration()
			t1 := c.lockActive()

			// The upgrade SUBSCRIBE issued by raiseQoS reconciliation on the
			// live transport fails with the case's token.
			first.setSubToken(tc.token)
			c.raiseQoS("device/S1/report", 1)

			// The failed transport is retired with the filter unestablished,
			// and the supervisor recovers on a fresh transport with the full
			// desired set.
			waitForCondition(t, 3*time.Second, func() bool {
				a := c.lockActive()
				return a != nil && a.generation > firstGen
			})
			// Two calls on the failed transport: the QoS 0 subscribe at
			// install and the failed QoS 1 upgrade.
			if got := first.subCount(); got != 2 {
				t.Fatalf("subscribe calls on failed transport = %d, want install plus the failed upgrade", got)
			}
			if q := c.transportOnWire(t1)["device/S1/report"]; q != 0 {
				t.Fatalf("upgrade recorded granted QoS %d on the failed transport, want the QoS 0 install only", q)
			}
			second := latest()
			if second == first {
				t.Fatal("no replacement client was built")
			}
			waitForCondition(t, 2*time.Second, func() bool {
				onWire := c.lockOnWire()
				return onWire["device/S1/report"] == 1
			})
			// The install warmup follows the restore reconcile, so poll for
			// it rather than checking immediately.
			waitForCondition(t, 2*time.Second, func() bool {
				return second.publishCount() > 0
			})
		})
	}
}

// TestUnsubscribeReconcilesWire removes the wire subscription when the last
// interest disappears and re-adds it when a new one arrives.
func TestUnsubscribeReconcilesWire(t *testing.T) {
	c, _, _, _ := newSupervisedConn(t)
	c.subscribeAsync("device/S1/report", 1)
	waitForCondition(t, time.Second, func() bool { return c.lockActive() != nil })

	waitForCondition(t, time.Second, func() bool {
		return c.lockOnWire()["device/S1/report"] == 1
	})
	// The install established the only reference; removing it unsubscribes
	// upstream, and a later subscribe re-establishes the wire subscription.
	c.unsubscribe("device/S1/report")
	waitForCondition(t, time.Second, func() bool {
		_, on := c.lockOnWire()["device/S1/report"]
		return !on
	})
	c.subscribe("device/S1/report", 1)
	waitForCondition(t, time.Second, func() bool {
		return c.lockOnWire()["device/S1/report"] == 1
	})
}

// TestPublishEngagesSupervisorAndDropsWhenOffline pins the publish-only
// contract: the first publish waits for the connection and forwards once;
// with the printer unreachable the publish is dropped, never queued.
func TestPublishEngagesSupervisorAndDropsWhenOffline(t *testing.T) {
	c, _, latest, tune := newSupervisedConn(t)
	gate := make(chan struct{})
	tune(func(f *fakePaho) { f.connectGate = gate })

	done := make(chan struct{})
	go func() {
		c.publish("device/S1/request", []byte(`{}`), 0, PublishContext{Origin: "internal", Action: "publish"})
		close(done)
	}()
	// The gated connect means the publish waits one connect budget (250ms).
	select {
	case <-done:
		t.Fatal("publish returned before the connect budget elapsed")
	case <-time.After(100 * time.Millisecond):
	}
	<-done
	if got := len(latest().publishSnapshot()); got != 0 {
		t.Fatalf("publishes while offline = %d, want zero (dropped, not queued)", got)
	}

	// Ungate the connect: a new publish reaches the printer exactly once.
	close(gate)
	waitForCondition(t, 2*time.Second, func() bool { return c.lockActive() != nil })
	c.publish("device/S1/request", []byte(`{"cmd":1}`), 0, PublishContext{Origin: "internal", Action: "publish"})
	// The install also warms up, so match the exact request payload instead
	// of the total count.
	waitForCondition(t, 2*time.Second, func() bool {
		return publishCountOf(latest(), `{"cmd":1}`) == 1
	})
	if got := publishCountOf(latest(), `{"cmd":1}`); got != 1 {
		t.Fatalf("request publishes = %d, want exactly one (never queued or replayed)", got)
	}
}

// TestSecondSubscriberRaisesWireQoS pins that a later subscriber whose QoS
// raises the merged maximum upgrades the live upstream subscription.
func TestSecondSubscriberRaisesWireQoS(t *testing.T) {
	c, _, _, _ := newSupervisedConn(t)
	c.subscribe("device/S1/report", 0)
	waitForCondition(t, time.Second, func() bool {
		q, on := c.lockOnWire()["device/S1/report"]
		return on && q == 0
	})
	c.subscribe("device/S1/report", 1)
	if q := c.lockOnWire()["device/S1/report"]; q != 1 {
		t.Fatalf("wire QoS after second subscriber = %d, want 1", q)
	}
}

// TestEnsureWaiterWakesOnReconnectAfterLoss pins that a waiter captured
// while a lost transport is still installed wakes on the next install
// instead of sleeping through its whole timeout.
func TestEnsureWaiterWakesOnReconnectAfterLoss(t *testing.T) {
	c, _, latest, _ := newSupervisedConn(t)
	c.connectTO = 2 * time.Second
	if !c.ensure(time.Second) {
		t.Fatal("initial connect failed")
	}
	old := c.lockActive()
	// Capture connCh while the lost transport is still active.
	c.mu.Lock()
	ch := c.connCh
	c.mu.Unlock()
	c.failTransport(old, errors.New("forced loss"))
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("waiter captured before retirement was not woken by the reconnect")
	}
	if latest() == nil || c.lockActive() == old {
		t.Fatal("expected a replacement transport")
	}
}

// TestRecordingDoesNotWaitBehindReconciliation pins that reference
// recording never blocks on a reconciliation stalled in token waits, so
// callers holding their own ownership lock stay responsive.
func TestRecordingDoesNotWaitBehindReconciliation(t *testing.T) {
	c := &Conn{spec: config.Printer{Serial: "S1"}, subs: newSubRefs()}
	c.subMu.Lock() // a reconciliation parked in a SUBACK wait
	defer c.subMu.Unlock()
	done := make(chan struct{})
	go func() {
		c.recordSubscribe("device/S1/report", 1)
		c.recordRaiseQoS("device/S1/report", 1)
		c.recordUnsubscribe("device/S1/report")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recording blocked behind the wire-reconciliation mutex")
	}
}
