package broker

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/routing"
	"bambu-mqtt-proxy/internal/upstream"
)

// fakePool records the reference operations the bridge performs upstream.
type fakePool struct {
	mu  sync.Mutex
	ops []refOp
	// settled, when set, runs inside every upstream completion.
	settled func()
}

// refOp is one recorded upstream reference operation.
type refOp struct {
	kind   string // subscribe | unsubscribe | raise
	serial string
	filter string
	qos    byte
}

func (f *fakePool) EnsureConnected(string, time.Duration) bool { return true }

func (f *fakePool) record(op refOp) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, op)
	settled := f.settled
	return func() {
		if settled != nil {
			settled()
		}
	}
}

func (f *fakePool) RecordSubscribe(serial, filter string, qos byte) func() {
	return f.record(refOp{"subscribe", serial, filter, qos})
}

func (f *fakePool) RecordUnsubscribe(serial, filter string) func() {
	return f.record(refOp{"unsubscribe", serial, filter, 0})
}

func (f *fakePool) RecordRaiseQoS(serial, filter string, qos byte) func() {
	return f.record(refOp{"raise", serial, filter, qos})
}

func (f *fakePool) PublishWithContext(string, string, []byte, byte, upstream.PublishContext) {}

// count reports how many ops of a kind hit serial+filter.
func (f *fakePool) count(kind, serial, filter string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, op := range f.ops {
		if op.kind == kind && op.serial == serial && op.filter == filter {
			n++
		}
	}
	return n
}

// anyKind reports whether any op of a kind hit filter on any serial.
func (f *fakePool) anyKind(kind, filter string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, op := range f.ops {
		if op.kind == kind && op.filter == filter {
			return true
		}
	}
	return false
}

func newTestBridge() (*Bridge, *fakePool) {
	cfg := &config.Config{
		Auth:     config.Auth{Mode: config.AuthModeAcceptAll},
		Printers: []config.Printer{{Serial: "S1", Password: "pw"}, {Serial: "S2", Password: "pw"}},
		Behavior: config.Behavior{UpstreamConnectTimeoutSeconds: 1},
	}
	pool := &fakePool{}
	return newBridge(cfg, routing.NewTable([]string{"S1", "S2"}), pool, slog.New(slog.NewTextHandler(io.Discard, nil))), pool
}

// mkClient builds an unattached mochi client. subs seeds the session state
// the way mochi does when it inherits a persistent session.
func mkClient(id string, inline bool, subs map[string]byte) *mqtt.Client {
	srv := mqtt.New(&mqtt.Options{InlineClient: true})
	cl := srv.NewClient(nil, "test", id, inline)
	for filter, qos := range subs {
		cl.State.Subscriptions.Add(filter, packets.Subscription{Filter: filter, Qos: qos})
	}
	return cl
}

func subscribedPacket(filters ...string) packets.Packet {
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe}}
	for _, f := range filters {
		pk.Filters = append(pk.Filters, packets.Subscription{Filter: f, Qos: 1})
	}
	return pk
}

func unsubscribedPacket(filters ...string) packets.Packet {
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Unsubscribe}}
	for _, f := range filters {
		pk.Filters = append(pk.Filters, packets.Subscription{Filter: f, Qos: 0})
	}
	return pk
}

func TestOnWillSuppressesDownstreamPreservesInline(t *testing.T) {
	b, _ := newTestBridge()
	will := mqtt.Will{TopicName: "device/S1/request", Payload: []byte("PAUSE"), Qos: 1, Retain: true}

	got, err := b.OnWill(mkClient("downstream", false, nil), will)
	if err != nil {
		t.Fatalf("OnWill error: %v", err)
	}
	if got.TopicName != "" || got.Payload != nil || got.Qos != 0 || got.Retain {
		t.Fatalf("downstream will = %+v, want zero will", got)
	}

	back, err := b.OnWill(mkClient("inline", true, nil), will)
	if err != nil {
		t.Fatalf("inline OnWill error: %v", err)
	}
	if string(back.Payload) != "PAUSE" || back.TopicName != will.TopicName || !back.Retain {
		t.Fatalf("inline will = %+v, want preserved", back)
	}
}

func TestSubscribeMergeRefCountAndQoSRaise(t *testing.T) {
	b, pool := newTestBridge()
	filter := "device/S1/report"
	cl := mkClient("c1", false, nil)

	b.OnSubscribed(cl, subscribedPacket(filter), []byte{1})
	if got := pool.count("subscribe", "S1", filter); got != 1 {
		t.Fatalf("first subscribe merged %d times, want 1", got)
	}

	// Repeated SUBSCRIBE: refcount-neutral, QoS raise only.
	b.OnSubscribed(cl, subscribedPacket(filter), []byte{1})
	if got := pool.count("subscribe", "S1", filter); got != 1 {
		t.Fatalf("repeated subscribe merged %d times, want 1", got)
	}
	if got := pool.count("raise", "S1", filter); got != 1 {
		t.Fatalf("repeated subscribe raised QoS %d times, want 1", got)
	}

	// Explicit unsubscribe by the current owner releases once.
	b.OnUnsubscribed(cl, unsubscribedPacket(filter))
	if got := pool.count("unsubscribe", "S1", filter); got != 1 {
		t.Fatalf("explicit unsubscribe released %d times, want 1", got)
	}

	// A second release for a filter no longer held is a no-op.
	b.OnUnsubscribed(cl, unsubscribedPacket(filter))
	if got := pool.count("unsubscribe", "S1", filter); got != 1 {
		t.Fatalf("stale unsubscribe released %d times, want 1", got)
	}
	subs, raises := pool.count("subscribe", "S1", filter), pool.count("raise", "S1", filter)

	// Denied filters (0x80) never merge.
	b.OnSubscribed(cl, subscribedPacket(filter), []byte{0x80})
	if pool.count("subscribe", "S1", filter) != subs || pool.count("raise", "S1", filter) != raises {
		t.Fatal("denied filter merged upstream")
	}
}

func TestWildcardMergesEveryPrinter(t *testing.T) {
	b, pool := newTestBridge()
	filter := "device/+/report"
	cl := mkClient("c1", false, nil)

	b.OnSubscribed(cl, subscribedPacket(filter), []byte{1})
	if got := pool.count("subscribe", "S1", filter) + pool.count("subscribe", "S2", filter); got != 2 {
		t.Fatalf("wildcard merged %d printer refs, want 2", got)
	}
	b.OnUnsubscribed(cl, unsubscribedPacket(filter))
	if got := pool.count("unsubscribe", "S1", filter) + pool.count("unsubscribe", "S2", filter); got != 2 {
		t.Fatalf("wildcard released %d printer refs, want 2", got)
	}
}

func TestPersistentDisconnectKeepsInterestsAndExpiryReleasesOnce(t *testing.T) {
	b, pool := newTestBridge()
	filter := "device/S1/report"
	cl := mkClient("persist", false, nil)

	b.OnSubscribed(cl, subscribedPacket(filter), []byte{1})
	// Persistent disconnect: interests and owner are retained.
	b.OnDisconnect(cl, nil, false)
	if got := pool.count("unsubscribe", "S1", filter); got != 0 {
		t.Fatalf("persistent disconnect released %d refs, want 0", got)
	}

	// A delayed expiry for the retained session releases exactly once.
	b.OnClientExpired(cl)
	if got := pool.count("unsubscribe", "S1", filter); got != 1 {
		t.Fatalf("expiry released %d refs, want 1", got)
	}
	b.OnClientExpired(cl)
	if got := pool.count("unsubscribe", "S1", filter); got != 1 {
		t.Fatalf("second expiry released %d refs, want 1", got)
	}

	// An expiring disconnect releases everything once, immediately.
	cl2 := mkClient("volatile", false, nil)
	b.OnSubscribed(cl2, subscribedPacket(filter), []byte{1})
	b.OnDisconnect(cl2, nil, true)
	if got := pool.count("unsubscribe", "S1", filter); got != 2 {
		t.Fatalf("expiring disconnect released %d refs total, want 2", got)
	}
}

func TestSessionEstablishedReconcilesInheritedSet(t *testing.T) {
	b, pool := newTestBridge()
	f1, f2 := "device/S1/report", "device/S1/request"

	// First session holds two interests, then persists offline.
	old := mkClient("s", false, nil)
	b.OnSubscribed(old, subscribedPacket(f1, f2), []byte{1, 1})
	b.OnDisconnect(old, nil, false)

	// Resuming session inherits only f1: adopt without incrementing,
	// release the absent f2.
	resumed := mkClient("s", false, map[string]byte{f1: 1})
	b.OnSessionEstablished(resumed, packets.Packet{})
	if got := pool.count("subscribe", "S1", f1); got != 1 {
		t.Fatalf("adopted interest re-merged %d times, want 1", got)
	}
	if got := pool.count("unsubscribe", "S1", f2); got != 1 {
		t.Fatalf("absent interest released %d times, want 1", got)
	}
	if got := pool.count("raise", "S1", f1); got != 1 {
		t.Fatalf("adopted interest raised QoS %d times, want 1", got)
	}

	// Clean reconnect with an empty inherited set releases the rest.
	clean := mkClient("s", false, nil)
	b.OnSessionEstablished(clean, packets.Packet{})
	if got := pool.count("unsubscribe", "S1", f1); got != 1 {
		t.Fatalf("clean reconnect released f1 %d times, want 1", got)
	}
	if b.sessions["s"] != nil {
		t.Fatal("clean reconnect left a session entry behind")
	}

	// An inherited filter the bridge never merged is adopted once.
	mergedF2 := pool.count("subscribe", "S1", f2)
	heir := mkClient("s", false, map[string]byte{f2: 0})
	b.OnSessionEstablished(heir, packets.Packet{})
	if got := pool.count("subscribe", "S1", f2); got != mergedF2+1 {
		t.Fatalf("unmerged inherited filter added %d subscribes, want 1", got-mergedF2)
	}
}

func TestLateOldOwnerHooksKeepSuccessorInterests(t *testing.T) {
	b, pool := newTestBridge()
	filter := "device/S1/report"

	a := mkClient("dupe", false, nil)
	b.OnSubscribed(a, subscribedPacket(filter), []byte{1})

	// B takes over, inherits the interest, then persists offline.
	bSession := mkClient("dupe", false, map[string]byte{filter: 1})
	b.OnSessionEstablished(bSession, packets.Packet{})
	b.OnDisconnect(bSession, nil, false)
	if got := pool.count("subscribe", "S1", filter); got != 1 {
		t.Fatalf("takeover re-merged %d times, want 1", got)
	}

	// Late hooks from A must not release B's retained interests.
	b.OnDisconnect(a, nil, true)
	b.OnClientExpired(a)
	b.OnUnsubscribed(a, unsubscribedPacket(filter))
	if got := pool.count("unsubscribe", "S1", filter); got != 0 {
		t.Fatalf("late old-owner hooks released %d refs, want 0", got)
	}

	// The successor's explicit unsubscribe still releases exactly once.
	b.OnUnsubscribed(bSession, unsubscribedPacket(filter))
	if got := pool.count("unsubscribe", "S1", filter); got != 1 {
		t.Fatalf("successor unsubscribe released %d refs, want 1", got)
	}
}

func TestSessionEstablishedUnknownIDInheritsAdopts(t *testing.T) {
	b, pool := newTestBridge()
	filter := "device/S2/report"

	// No stored entry: an inherited filter is adopted with one merge; an
	// empty session creates no entry.
	b.OnSessionEstablished(mkClient("fresh", false, map[string]byte{filter: 1}), packets.Packet{})
	if got := pool.count("subscribe", "S2", filter); got != 1 {
		t.Fatalf("inherited filter subscribed %d times, want 1", got)
	}
	if b.sessions["fresh"] == nil {
		t.Fatal("inherited session stored no entry")
	}
	b.OnSessionEstablished(mkClient("empty", false, nil), packets.Packet{})
	if _, ok := b.sessions["empty"]; ok {
		t.Fatal("empty session created a map entry")
	}
}

// TestUpstreamCompletionsRunOutsideOwnershipLock pins that connection and
// wire waits never hold the fleet-wide ownership lock, so one offline
// printer cannot stall other clients' subscription hooks.
func TestUpstreamCompletionsRunOutsideOwnershipLock(t *testing.T) {
	b, pool := newTestBridge()
	var runs int
	pool.settled = func() {
		runs++
		if !b.mu.TryLock() {
			t.Error("upstream completion ran while the ownership lock was held")
			return
		}
		b.mu.Unlock()
	}
	filter := "device/S1/report"
	a := mkClient("a", false, nil)
	b.OnSubscribed(a, subscribedPacket(filter), []byte{1})
	b.OnSubscribed(a, subscribedPacket(filter), []byte{1})
	b.OnUnsubscribed(a, unsubscribedPacket(filter))
	persistent := mkClient("p", false, map[string]byte{filter: 1})
	b.OnSessionEstablished(persistent, packets.Packet{})
	b.OnDisconnect(persistent, nil, true)
	if runs != 5 {
		t.Fatalf("completions run = %d, want 5 (subscribe, raise, unsubscribe, inherit, expire)", runs)
	}
}
