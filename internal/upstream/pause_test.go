// Upstream pause-guard tests: the pause command may only reach the wire on
// exactly the validated connection generation, an admitted command attempts
// once on its captured client and never on a replacement, and reports are
// stamped with the generation of the transport they arrived on.
package upstream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"bambu-mqtt-proxy/internal/config"
)

// fakeClosedChan is the already-complete Done channel for tokens that finish
// immediately.
var fakeClosedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// fakeToken is a controllable paho token. done nil means already complete;
// a test-supplied channel finishes the token when closed (or never, for
// timeout cases). grants, when non-nil, backs the Subscribe result map.
type fakeToken struct {
	done   chan struct{}
	err    error
	grants map[string]byte
}

func (t *fakeToken) Done() <-chan struct{} {
	if t.done != nil {
		return t.done
	}
	return fakeClosedChan
}

func (t *fakeToken) Wait() bool {
	<-t.Done()
	return true
}

func (t *fakeToken) WaitTimeout(d time.Duration) bool {
	select {
	case <-t.Done():
		return true
	case <-time.After(d):
		return false
	}
}

func (t *fakeToken) Error() error { return t.err }

func (t *fakeToken) Result() map[string]byte { return t.grants }

type fakePublish struct {
	topic    string
	qos      byte
	retained bool
	payload  []byte
}

type fakeSub struct {
	topic   string
	qos     byte
	handler pahomqtt.MessageHandler
}

// fakePaho implements the paho client surface the pool uses. Connect may be
// gated on a channel, Subscribe grants may be overridden per call, and
// Publish may block in onPublished to hold a command mid-flight while the
// test replaces the transport.
type fakePaho struct {
	pahomqtt.Client
	mu sync.Mutex

	open         bool
	connectGate  chan struct{} // Connect token completes only when closed
	connectErr   error
	connectCalls int
	disconnects  int

	publishes []fakePublish
	// beforePublish, when set, runs before the publish is recorded; a test
	// may block here to hold a command between admission and the wire write.
	beforePublish func()
	// onPublished runs after the publish is recorded, before the token is
	// returned; a test may block here.
	onPublished func()
	failToken   bool

	subs     []fakeSub
	unsubs   []string
	subToken *fakeToken // overrides the default success token when non-nil
	// disconnectHook, when set, runs after the disconnect is recorded; a
	// test may block here to hold retirement mid-teardown.
	disconnectHook func()
}

func (c *fakePaho) IsConnectionOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.open
}

func (c *fakePaho) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.open
}

func (c *fakePaho) Connect() pahomqtt.Token {
	c.mu.Lock()
	c.connectCalls++
	gate := c.connectGate
	err := c.connectErr
	c.mu.Unlock()
	tok := &fakeToken{err: err}
	if gate != nil {
		tok.done = gate
	}
	return tok
}

func (c *fakePaho) Disconnect(quiesce uint) {
	c.mu.Lock()
	c.disconnects++
	hook := c.disconnectHook
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (c *fakePaho) Publish(topic string, qos byte, retained bool, payload any) pahomqtt.Token {
	c.mu.Lock()
	hook := c.beforePublish
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	c.mu.Lock()
	c.publishes = append(c.publishes, fakePublish{
		topic: topic, qos: qos, retained: retained, payload: payload.([]byte),
	})
	after := c.onPublished
	fail := c.failToken
	c.mu.Unlock()
	if after != nil {
		after()
	}
	if fail {
		return &fakeToken{err: errors.New("connection lost")}
	}
	return &fakeToken{}
}

func (c *fakePaho) Subscribe(topic string, qos byte, handler pahomqtt.MessageHandler) pahomqtt.Token {
	c.mu.Lock()
	c.subs = append(c.subs, fakeSub{topic: topic, qos: qos, handler: handler})
	override := c.subToken
	c.mu.Unlock()
	if override != nil {
		return override
	}
	return &fakeToken{grants: map[string]byte{topic: qos}}
}

func (c *fakePaho) Unsubscribe(topics ...string) pahomqtt.Token {
	c.mu.Lock()
	c.unsubs = append(c.unsubs, topics...)
	c.mu.Unlock()
	return &fakeToken{}
}

func (c *fakePaho) publishCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.publishes)
}

// publishSnapshot returns a copy of the recorded publishes.
func (c *fakePaho) publishSnapshot() []fakePublish {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]fakePublish(nil), c.publishes...)
}

// publishCountOf counts recorded publishes with the exact payload.
func publishCountOf(c *fakePaho, payload string) int {
	n := 0
	for _, pub := range c.publishSnapshot() {
		if string(pub.payload) == payload {
			n++
		}
	}
	return n
}

func (c *fakePaho) connectCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectCalls
}

func (c *fakePaho) disconnectCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.disconnects
}

func (c *fakePaho) subCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.subs)
}

// subSnapshot returns a copy of the recorded subscriptions.
func (c *fakePaho) subSnapshot() []fakeSub {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]fakeSub(nil), c.subs...)
}

// setSubToken installs a token override for the next Subscribe call.
func (c *fakePaho) setSubToken(tok *fakeToken) {
	c.mu.Lock()
	c.subToken = tok
	c.mu.Unlock()
}

func newFakePaho(open bool) *fakePaho { return &fakePaho{open: open} }

// newTestTransport builds an installed transport wrapping client.
func newTestTransport(client pahomqtt.Client, generation uint64) *transport {
	t := &transport{client: client, generation: generation, lost: make(chan struct{})}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	t.onWire = make(map[string]byte)
	return t
}

// installTransport replaces c's active transport the way the supervisor does:
// under the report mutex, with a fresh generation, before any handler exists.
func installTransport(c *Conn, client pahomqtt.Client) *transport {
	c.reportMu.Lock()
	c.mu.Lock()
	c.generation++
	t := newTestTransport(client, c.generation)
	c.active = t
	c.mu.Unlock()
	c.reportMu.Unlock()
	return t
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestConn builds a conn with an installed transport wrapping client.
// A nil client leaves the connection with no active transport.
func newTestConn(client pahomqtt.Client, generation uint64) *Conn {
	c := &Conn{
		spec:        config.Printer{Serial: "S1", Name: "Shop", Address: "S1.local:8883"},
		connectTO:   250 * time.Millisecond,
		backoffInit: time.Hour,
		backoffMax:  time.Hour,
		inject:      nopInject{},
		log:         discardLogger(),
		stopCh:      make(chan struct{}),
		subs:        newSubRefs(),
		connCh:      make(chan struct{}),
		generation:  generation,
	}
	if client != nil {
		c.active = newTestTransport(client, generation)
	}
	return c
}

type nopInject struct{}

func (nopInject) PublishDownstream(string, []byte, byte) {}

func TestPausePublishesExactGuardedCommand(t *testing.T) {
	fake := newFakePaho(true)
	c := newTestConn(fake, 7)

	if err := c.pause(7); err != nil {
		t.Fatalf("pause = %v, want success", err)
	}
	if len(fake.publishes) != 1 {
		t.Fatalf("publishes = %d, want exactly one attempt", len(fake.publishes))
	}
	pub := fake.publishes[0]
	if pub.topic != "device/S1/request" || pub.qos != 0 || pub.retained {
		t.Fatalf("publish = %+v, want QoS 0 unretained on device/S1/request", pub)
	}
	if string(pub.payload) != pausePayload() {
		t.Fatalf("payload = %s, want %s", pub.payload, pausePayload())
	}
}

func TestPauseRejectsGenerationMismatchWithoutPublishing(t *testing.T) {
	fake := newFakePaho(true)
	c := newTestConn(fake, 7)

	if err := c.pause(6); err == nil {
		t.Fatal("pause with a stale generation must fail")
	}
	if len(fake.publishes) != 0 {
		t.Fatalf("publishes = %d, want none on a generation mismatch", len(fake.publishes))
	}
}

func TestPauseRejectsDisconnectedAndNilTransport(t *testing.T) {
	c := newTestConn(newFakePaho(false), 7)
	if err := c.pause(7); err == nil {
		t.Fatal("pause while disconnected must fail")
	}
	c2 := newTestConn(nil, 7)
	if err := c2.pause(7); err == nil {
		t.Fatal("pause without an active transport must fail")
	}
}

func TestPauseRejectedAfterLossSignal(t *testing.T) {
	// A transport whose loss is signaled is rejected before the publish, even
	// though the generation and client look valid.
	fake := newFakePaho(true)
	c := newTestConn(fake, 7)
	c.active.signalLoss(errors.New("keepalive timeout"))
	if err := c.pause(7); err == nil {
		t.Fatal("pause on a loss-signaled transport must fail")
	}
	if len(fake.publishes) != 0 {
		t.Fatalf("publishes = %d, want none after loss signaling", len(fake.publishes))
	}
}

func TestPauseFailsOnTokenErrorWithoutRetry(t *testing.T) {
	fake := newFakePaho(true)
	fake.failToken = true
	c := newTestConn(fake, 5)
	if err := c.pause(5); err == nil {
		t.Fatal("pause must fail on a token error")
	}
	if len(fake.publishes) != 1 {
		t.Fatalf("publishes = %d, want no automatic retry", len(fake.publishes))
	}
}

func TestPoolPauseUnknownSerialAndGenerationZero(t *testing.T) {
	p := NewPool([]config.Printer{{Serial: "S1", Name: "Shop"}}, nil, config.Behavior{},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.PausePrint("UNKNOWN", 1); err == nil {
		t.Fatal("pause for an unknown serial must fail")
	}
	if gen := p.Generation("UNKNOWN"); gen != 0 {
		t.Fatalf("generation = %d, want 0 for an unknown serial", gen)
	}
}

func TestOnMessageStampsTransportGeneration(t *testing.T) {
	fake := newFakePaho(true)
	c := newTestConn(fake, 9)
	transportGen := c.active.generation
	var gotSeq, gotGen uint64
	var gotPayload []byte
	seen := make(chan struct{}, 1)
	c.observe = func(serial string, seq, gen uint64, payload []byte) {
		if serial != "S1" {
			t.Errorf("serial = %q, want S1", serial)
		}
		gotSeq, gotGen, gotPayload = seq, gen, payload
		seen <- struct{}{}
	}
	c.onMessage(c.active, fakeMessage{payload: []byte(`{"print":{}}`)})
	<-seen
	if gotGen != transportGen {
		t.Fatalf("stamped generation = %d, want the transport generation %d", gotGen, transportGen)
	}
	if gotSeq == 0 || string(gotPayload) != `{"print":{}}` {
		t.Fatalf("seq/payload = %d/%s", gotSeq, gotPayload)
	}
}

// fakeMessage is the paho message surface onMessage uses.
type fakeMessage struct{ payload []byte }

func (m fakeMessage) Duplicate() bool   { return false }
func (m fakeMessage) Qos() byte         { return 0 }
func (m fakeMessage) Retained() bool    { return false }
func (m fakeMessage) Topic() string     { return "device/S1/report" }
func (m fakeMessage) MessageID() uint16 { return 0 }
func (m fakeMessage) Payload() []byte   { return m.payload }
func (m fakeMessage) Ack()              {}

func TestControlCommandsPublishExactGuardedPayloads(t *testing.T) {
	cases := []struct {
		name string
		send func(p *Pool, gen uint64) error
		want string
	}{
		{"resume", func(p *Pool, gen uint64) error { return p.ResumePrint("S1", gen) },
			`{"print":{"sequence_id":"0","command":"resume"}}`},
		{"stop", func(p *Pool, gen uint64) error { return p.StopPrint("S1", gen) },
			`{"print":{"sequence_id":"0","command":"stop"}}`},
		{"light on", func(p *Pool, gen uint64) error { return p.SetChamberLight("S1", gen, true) },
			`{"system":{"sequence_id":"0","command":"ledctrl","led_node":"chamber_light","led_mode":"on","led_on_time":500,"led_off_time":500,"loop_times":0,"interval_time":0}}`},
		{"light off", func(p *Pool, gen uint64) error { return p.SetChamberLight("S1", gen, false) },
			`{"system":{"sequence_id":"0","command":"ledctrl","led_node":"chamber_light","led_mode":"off","led_on_time":500,"led_off_time":500,"loop_times":0,"interval_time":0}}`},
	}
	for _, tc := range cases {
		fake := newFakePaho(true)
		p := NewPool([]config.Printer{{Serial: "S1", Name: "Shop"}}, nil, config.Behavior{},
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		p.conns["S1"] = newTestConn(fake, 7)
		if err := tc.send(p, 6); err == nil {
			t.Fatalf("%s: stale generation accepted", tc.name)
		}
		if len(fake.publishes) != 0 {
			t.Fatalf("%s: published on a generation mismatch", tc.name)
		}
		if err := tc.send(p, 7); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(fake.publishes) != 1 {
			t.Fatalf("%s: publishes = %d, want 1", tc.name, len(fake.publishes))
		}
		pub := fake.publishes[0]
		if pub.topic != "device/S1/request" || pub.qos != 0 || pub.retained || string(pub.payload) != tc.want {
			t.Fatalf("%s: publish = %+v %s", tc.name, pub, pub.payload)
		}
	}
}

// lockActive returns the active transport under the lock, for assertions.
func (c *Conn) lockActive() *transport {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active
}

// lockGeneration returns the generation under the lock, for assertions.
func (c *Conn) lockGeneration() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

// lockOnWire returns a copy of the active transport's onWire map for
// assertions. Production writers guard onWire with subMu, so readers take it
// too (subMu before mu, the production order).
func (c *Conn) lockOnWire() map[string]byte {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	return copyOnWire(c.lockActive())
}

// transportOnWire returns a copy of one transport's onWire map under subMu.
func (c *Conn) transportOnWire(t *transport) map[string]byte {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	return copyOnWire(t)
}

func copyOnWire(t *transport) map[string]byte {
	if t == nil {
		return nil
	}
	out := make(map[string]byte, len(t.onWire))
	for f, q := range t.onWire {
		out[f] = q
	}
	return out
}
