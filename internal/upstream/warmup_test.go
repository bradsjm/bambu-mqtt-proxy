// Warmup-establishment barrier tests: one warmup batch per false→true report
// subscription establishment, genuinely late subscribers warm exactly once,
// and subscribers recorded while an establishment runs (SUBACK or warmup
// window) are covered by it instead of warming a second time. Transport
// identity keys late completions, so a subscriber recorded on a transport
// that is then replaced is covered by the replacement's restoration.
package upstream

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// warmupPayload is the single warmup command newSupervisedConn configures.
func warmupPayload() string { return `{"pushall":1}` }

// TestInterestAddRemoveConvergeToOneReportSubscription pins count/flag
// convergence: N downstream interests share one QoS 0 report subscription,
// the wire subscription tracks only the zero/nonzero boundary, and each
// establishment warms exactly once.
func TestInterestAddRemoveConvergeToOneReportSubscription(t *testing.T) {
	c, _, latest, _ := newSupervisedConn(t)
	if !c.ensure(time.Second) {
		t.Fatal("initial connect failed")
	}
	fake := latest()

	// Three interests (distinct downstream filters over one printer) become
	// one upstream subscription and one warmup batch.
	var settles []func()
	for _, f := range []string{"device/S1/report", "device/+/report", "device/#"} {
		settles = append(settles, c.recordSubscribe(f))
	}
	for _, settle := range settles {
		settle()
	}
	if got := c.lockReportRefs(); got != 3 {
		t.Fatalf("report interest count = %d, want 3", got)
	}
	waitForCondition(t, time.Second, func() bool { return c.lockSubscribed() })
	if got := fake.subCount(); got != 1 {
		t.Fatalf("upstream subscribes for 3 interests = %d, want 1", got)
	}
	for _, sub := range fake.subSnapshot() {
		if sub.topic != "device/S1/report" || sub.qos != 0 {
			t.Fatalf("upstream subscribe = %+v, want QoS 0 on device/S1/report", sub)
		}
	}
	if got := publishCountOf(fake, warmupPayload()); got != 1 {
		t.Fatalf("warmup publishes after first establishment = %d, want 1", got)
	}

	// Removing two of three interests leaves the subscription up: the wire
	// tracks the zero boundary, not each interest.
	c.recordUnsubscribe("device/S1/report")()
	c.recordUnsubscribe("device/+/report")()
	if got := c.lockReportRefs(); got != 1 {
		t.Fatalf("report interest count = %d, want 1", got)
	}
	if !c.lockSubscribed() {
		t.Fatal("subscription removed while one interest remains")
	}
	if got := fake.subCount(); got != 1 {
		t.Fatalf("upstream subscribes changed to %d, want 1", got)
	}
	if got := publishCountOf(fake, warmupPayload()); got != 1 {
		t.Fatalf("warmup publishes after partial removal = %d, want 1", got)
	}

	// The last removal unsubscribes the wire.
	c.recordUnsubscribe("device/#")()
	if got := c.lockReportRefs(); got != 0 {
		t.Fatalf("report interest count = %d, want 0", got)
	}
	waitForCondition(t, time.Second, func() bool { return !c.lockSubscribed() })

	// A fresh interest re-establishes: one more subscribe, one more warmup.
	c.recordSubscribe("device/S1/report")()
	waitForCondition(t, time.Second, func() bool { return c.lockSubscribed() })
	if got := fake.subCount(); got != 2 {
		t.Fatalf("upstream subscribes after re-establishment = %d, want 2", got)
	}
	if got := publishCountOf(fake, warmupPayload()); got != 2 {
		t.Fatalf("warmup publishes after re-establishment = %d, want 2 (one per establishment)", got)
	}
}

// TestReconnectRestoresInterestAndWarmsOnce pins reconnect restoration: a
// replacement transport re-subscribes the recorded interest once at QoS 0 and
// owns exactly one warmup batch, while a connection with no report interest
// subscribes and warms up nothing at install.
func TestReconnectRestoresInterestAndWarmsOnce(t *testing.T) {
	c, _, latest, _ := newSupervisedConn(t)
	c.subscribeAsync("device/S1/report")
	waitForCondition(t, time.Second, func() bool {
		return c.lockActive() != nil && c.lockSubscribed()
	})
	firstGen := c.lockGeneration()

	c.failTransport(c.lockActive(), errors.New("forced loss"))
	waitForCondition(t, 2*time.Second, func() bool {
		return c.lockGeneration() > firstGen+1 && c.lockSubscribed()
	})
	second := latest()
	if got := second.subCount(); got != 1 {
		t.Fatalf("upstream subscribes on replacement = %d, want 1", got)
	}
	for _, sub := range second.subSnapshot() {
		if sub.qos != 0 {
			t.Fatalf("restored subscribe requested QoS %d, want 0", sub.qos)
		}
	}
	if got := publishCountOf(second, warmupPayload()); got != 1 {
		t.Fatalf("warmup publishes on replacement = %d, want exactly one establishment batch", got)
	}

	// A publish-only connection records no interest: install neither
	// subscribes nor warms up.
	idle, _, idleLatest, _ := newSupervisedConn(t)
	if !idle.ensure(time.Second) {
		t.Fatal("idle connect failed")
	}
	waitForCondition(t, time.Second, func() bool { return idle.lockActive() != nil })
	if idle.lockSubscribed() {
		t.Fatal("publish-only connection subscribed the report topic")
	}
	if got := publishCountOf(idleLatest(), warmupPayload()); got != 0 {
		t.Fatalf("warmup publishes with no report interest = %d, want 0", got)
	}
}

// TestConcurrentInitialSubscribersWarmUpOnce pins the single startup warmup:
// subscribers recorded while the first establishment is in flight — here
// parked on the SUBACK — all land inside one establishment, produce one wire
// subscribe, and one warmup batch, and recording never waits behind the
// reconciliation that parks.
func TestConcurrentInitialSubscribersWarmUpOnce(t *testing.T) {
	c, _, latest, _ := newSupervisedConn(t)
	if !c.ensure(time.Second) {
		t.Fatal("initial connect failed")
	}
	fake := latest()

	// Park the establishment's SUBSCRIBE so every recorder lands before the
	// subscribed flag can be set.
	gateDone := make(chan struct{})
	fake.setSubToken(&fakeToken{done: gateDone})

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.recordSubscribe("device/S1/report")()
		}()
	}
	// Every complete but the first parks behind the establishment; recording
	// itself never blocks, so wait for all n records to land while the
	// establishment sits parked on the gate.
	waitForCondition(t, time.Second, func() bool { return c.lockReportRefs() == n })
	waitForCondition(t, time.Second, func() bool { return fake.subCount() == 1 })

	close(gateDone)
	wg.Wait()
	if got := c.lockReportRefs(); got != n {
		t.Fatalf("report interest count = %d, want %d", got, n)
	}
	waitForCondition(t, time.Second, func() bool { return c.lockSubscribed() })
	if got := fake.subCount(); got != 1 {
		t.Fatalf("upstream subscribes for %d concurrent initial subscribers = %d, want 1", n, got)
	}
	if got := publishCountOf(fake, warmupPayload()); got != 1 {
		t.Fatalf("warmup publishes = %d, want exactly one establishment batch", got)
	}
}

// TestGenuinelyLateSubscriberWarmsOnce pins the late-subscriber warmup: a
// subscriber joining an established report subscription requests exactly one
// full-state batch and no additional wire subscribe.
func TestGenuinelyLateSubscriberWarmsOnce(t *testing.T) {
	c, _, latest, _ := newSupervisedConn(t)
	c.subscribeAsync("device/S1/report")
	waitForCondition(t, time.Second, func() bool {
		return c.lockActive() != nil && c.lockSubscribed()
	})
	fake := latest()
	if got := publishCountOf(fake, warmupPayload()); got != 1 {
		t.Fatalf("establishment warmups = %d, want 1", got)
	}

	// Two genuinely late subscribers: each owns exactly one batch, and the
	// wire subscription is not touched again.
	for i := 2; i <= 3; i++ {
		c.recordSubscribe("device/S1/report")()
		if got := c.lockReportRefs(); got != i {
			t.Fatalf("report interest count = %d, want %d", got, i)
		}
		if got := fake.subCount(); got != 1 {
			t.Fatalf("upstream subscribes after late subscriber = %d, want 1", got)
		}
		if got := publishCountOf(fake, warmupPayload()); got != i {
			t.Fatalf("warmup publishes after %d subscribers = %d, want %d (one establishment + one per late subscriber)", i, got, i)
		}
	}
}

// TestIntentDuringEstablishmentIsCovered pins the SUBACK-window race: a
// subscriber recorded while the establishment's SUBACK is in flight is
// covered by it — one subscribe, one warmup — and its own completion
// converges without a second batch.
func TestIntentDuringEstablishmentIsCovered(t *testing.T) {
	c, _, latest, _ := newSupervisedConn(t)
	if !c.ensure(time.Second) {
		t.Fatal("initial connect failed")
	}
	fake := latest()

	gateDone := make(chan struct{})
	fake.setSubToken(&fakeToken{done: gateDone})
	established := make(chan struct{})
	go func() {
		c.recordSubscribe("device/S1/report")()
		close(established)
	}()
	waitForCondition(t, time.Second, func() bool { return fake.subCount() == 1 })

	// Recorded while the SUBACK is pending: not late, covered by the
	// establishment in flight.
	covered := c.recordSubscribe("device/S1/report")
	if got := c.lockReportRefs(); got != 2 {
		t.Fatalf("report interest count = %d, want 2", got)
	}
	coveredDone := make(chan struct{})
	go func() {
		covered()
		close(coveredDone)
	}()

	close(gateDone)
	<-established
	<-coveredDone
	if got := c.lockReportRefs(); got != 2 {
		t.Fatalf("report interest count = %d, want 2", got)
	}
	if got := fake.subCount(); got != 1 {
		t.Fatalf("upstream subscribes = %d, want 1", got)
	}
	if !c.lockSubscribed() {
		t.Fatal("report subscription not established")
	}
	if got := publishCountOf(fake, warmupPayload()); got != 1 {
		t.Fatalf("warmup publishes = %d, want exactly the establishment batch", got)
	}
}

// TestSubscriberDuringWarmupRequestsOwnBatch pins the coverage boundary at
// the SUBACK: the subscribed flag flips before the warmup batch is
// dispatched, so a full report that passes while the batch is still running
// does not cover a subscriber recorded after it — that subscriber is
// genuinely late and its completion sends its own batch with fresh state.
func TestSubscriberDuringWarmupRequestsOwnBatch(t *testing.T) {
	var publishes atomic.Int32
	c, inject, latest, _ := newSupervisedConn(t)
	// Two batch commands so the report can pass between them.
	c.warmup = []string{`{"pushall":1}`, `{"pushall":2}`}
	if !c.ensure(time.Second) {
		t.Fatal("initial connect failed")
	}
	fake := latest()

	// Block inside the second batch command; the first command's pushall
	// report passes downstream while the establishment batch is parked.
	started := make(chan struct{})
	release := make(chan struct{})
	fake.beforePublish = func() {
		if publishes.Add(1) == 2 {
			close(started)
			<-release
		}
	}
	established := make(chan struct{})
	go func() {
		c.recordSubscribe("device/S1/report")()
		close(established)
	}()
	<-started

	// A report the first command triggered is observed and injected while
	// the batch is parked: this full state has already passed downstream.
	c.observe = func(string, uint64, uint64, []byte) {}
	c.onMessage(c.lockActive(), fakeMessage{payload: []byte(`{"passed":true}`)})

	// Recorded after the report passed: late even though the establishment
	// batch is still in flight. Its completion waits out the establishment
	// (subMu) and then sends its own batch.
	late := c.recordSubscribe("device/S1/report")
	lateDone := make(chan struct{})
	go func() {
		late()
		close(lateDone)
	}()

	close(release)
	<-established
	<-lateDone

	if got := c.lockReportRefs(); got != 2 {
		t.Fatalf("report interest count = %d, want 2", got)
	}
	if got := fake.subCount(); got != 1 {
		t.Fatalf("upstream subscribes = %d, want 1", got)
	}
	if !c.lockSubscribed() {
		t.Fatal("report subscription not established")
	}
	// The establishment batch plus the late subscriber's own batch: each
	// command ran twice.
	if got := publishCountOf(fake, `{"pushall":1}`); got != 2 {
		t.Fatalf("pushall 1 publishes = %d, want 2 (establishment + late batch)", got)
	}
	if got := publishCountOf(fake, `{"pushall":2}`); got != 2 {
		t.Fatalf("pushall 2 publishes = %d, want 2 (establishment + late batch)", got)
	}
	// The missed report passed downstream exactly once, before the late
	// batch ran.
	if got := len(inject.snapshot()); got != 1 {
		t.Fatalf("downstream reports = %v, want exactly the one passed report", inject.snapshot())
	}
}

// TestStopDuringEstablishmentSubackSendsNoWarmup pins the shutdown barrier:
// Stop during a parked SUBACK admits no warmup command afterwards, even
// though the active transport still looks usable (Stop disconnects the
// connecting client, not the active one).
func TestStopDuringEstablishmentSubackSendsNoWarmup(t *testing.T) {
	c, _, latest, _ := newSupervisedConn(t)
	if !c.ensure(time.Second) {
		t.Fatal("initial connect failed")
	}
	fake := latest()
	gateDone := make(chan struct{})
	fake.setSubToken(&fakeToken{done: gateDone})

	established := make(chan struct{})
	go func() {
		c.recordSubscribe("device/S1/report")()
		close(established)
	}()
	waitForCondition(t, time.Second, func() bool { return fake.subCount() == 1 })

	c.stop()
	close(gateDone)
	<-established

	if got := fake.publishCount(); got != 0 {
		t.Fatalf("warmup publishes after stop = %d, want 0", got)
	}
}

// TestStopBetweenWarmupBatchCommands pins the per-command admission gate: an
// already-admitted batch command may finish, but Stop before the next
// admission leaves the rest of the batch unsent.
func TestStopBetweenWarmupBatchCommands(t *testing.T) {
	c, _, latest, _ := newSupervisedConn(t)
	c.warmup = []string{`{"pushall":1}`, `{"pushall":2}`}
	if !c.ensure(time.Second) {
		t.Fatal("initial connect failed")
	}
	fake := latest()

	// Hold the first command after its publish was admitted, then stop and
	// release: the remaining command must never be attempted.
	started := make(chan struct{})
	release := make(chan struct{})
	fake.onPublished = func() {
		close(started)
		<-release
	}

	established := make(chan struct{})
	go func() {
		c.recordSubscribe("device/S1/report")()
		close(established)
	}()
	<-started

	c.stop()
	close(release)
	<-established

	if got := fake.publishCount(); got != 1 {
		t.Fatalf("warmup publishes = %d, want exactly the admitted command", got)
	}
}

// TestLateCompletionAfterReplacementIsCoveredByRestore pins transport-identity
// keying: a late subscriber recorded on a transport that then dies is covered
// by the replacement's restoration establishment, and its completion adds no
// second batch.
func TestLateCompletionAfterReplacementIsCoveredByRestore(t *testing.T) {
	c, _, latest, _ := newSupervisedConn(t)
	c.subscribeAsync("device/S1/report")
	waitForCondition(t, time.Second, func() bool {
		return c.lockActive() != nil && c.lockSubscribed()
	})
	first := latest()
	if got := publishCountOf(first, warmupPayload()); got != 1 {
		t.Fatalf("establishment warmups = %d, want 1", got)
	}
	firstGen := c.lockGeneration()

	// Record the late subscriber against the live transport, then lose the
	// transport before the completion runs.
	late := c.recordSubscribe("device/S1/report")
	c.failTransport(c.lockActive(), errors.New("forced loss"))
	waitForCondition(t, 2*time.Second, func() bool {
		return c.lockGeneration() > firstGen+1 && c.lockSubscribed()
	})
	second := latest()
	if got := publishCountOf(second, warmupPayload()); got != 1 {
		t.Fatalf("restoration warmups = %d, want exactly one establishment batch", got)
	}

	// The late completion finds its transport replaced: plain convergence,
	// no second batch.
	late()
	if got := c.lockReportRefs(); got != 2 {
		t.Fatalf("report interest count = %d, want 2", got)
	}
	if got := second.subCount(); got != 1 {
		t.Fatalf("upstream subscribes on replacement = %d, want 1", got)
	}
	if got := publishCountOf(second, warmupPayload()); got != 1 {
		t.Fatalf("warmup publishes after late completion = %d, want 1 (restoration covered the subscriber)", got)
	}
}
