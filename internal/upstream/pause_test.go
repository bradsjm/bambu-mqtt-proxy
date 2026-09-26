// Upstream pause-guard tests: the pause command may only reach the wire on
// exactly the validated connection generation, must be rejected when the
// connection changes during the publish, and reports must be stamped with
// the generation captured at handler entry.
package upstream

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"bambu-mqtt-proxy/internal/config"
)

// fakeToken is a completed paho publish token.
type fakeToken struct {
	ok  bool
	err error
}

func (t fakeToken) Wait() bool                     { return t.ok }
func (t fakeToken) WaitTimeout(time.Duration) bool { return t.ok }
func (t fakeToken) Done() <-chan struct{}          { return nil }
func (t fakeToken) Error() error                   { return t.err }

type fakePublish struct {
	topic    string
	qos      byte
	retained bool
	payload  []byte
}

// fakePaho implements the parts of the paho client the pause path touches.
// onPublished runs after the publish is recorded, simulating the world
// changing while the QoS 0 message was in flight.
type fakePaho struct {
	pahomqtt.Client
	mu          sync.Mutex
	open        bool
	failToken   bool
	publishes   []fakePublish
	onPublished func()
}

func (c *fakePaho) IsConnectionOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.open
}

func (c *fakePaho) Publish(topic string, qos byte, retained bool, payload any) pahomqtt.Token {
	c.mu.Lock()
	c.publishes = append(c.publishes, fakePublish{
		topic: topic, qos: qos, retained: retained, payload: payload.([]byte),
	})
	hook := c.onPublished
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failToken {
		return fakeToken{ok: true, err: errors.New("connection lost")}
	}
	return fakeToken{ok: true}
}

func newFakePaho(open bool) *fakePaho { return &fakePaho{open: open} }

func newTestConn(client pahomqtt.Client, generation uint64) *Conn {
	return &Conn{
		spec:       config.Printer{Serial: "S1", Name: "Shop", Address: "S1.local:8883"},
		client:     client,
		connectTO:  time.Second,
		generation: generation,
		inject:     nopInject{},
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
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

func TestPauseRejectsDisconnectedAndNilClient(t *testing.T) {
	c := newTestConn(newFakePaho(false), 7)
	if err := c.pause(7); err == nil {
		t.Fatal("pause while disconnected must fail")
	}
	c2 := newTestConn(nil, 7)
	if err := c2.pause(7); err == nil {
		t.Fatal("pause without a client must fail")
	}
}

func TestPauseRejectsWhenConnectionChangesDuringPublish(t *testing.T) {
	// Simulate paho silently dropping the QoS 0 message during an
	// auto-reconnect: the token reports success, but the connection was
	// replaced (generation bump) while the publish was in flight.
	fake := newFakePaho(true)
	c := newTestConn(fake, 7)
	fake.onPublished = func() {
		c.mu.Lock()
		c.generation = 8
		c.mu.Unlock()
	}
	if err := c.pause(7); err == nil {
		t.Fatal("pause must fail when the generation changed during the publish")
	}
	if len(fake.publishes) != 1 {
		t.Fatalf("publishes = %d, want one attempt and no replay", len(fake.publishes))
	}

	// A connection that simply dropped also rejects.
	fake2 := newFakePaho(true)
	c2 := newTestConn(fake2, 3)
	fake2.onPublished = func() {
		fake2.mu.Lock()
		fake2.open = false
		fake2.mu.Unlock()
	}
	if err := c2.pause(3); err == nil {
		t.Fatal("pause must fail when the connection dropped during the publish")
	}

	// A failed token is an error, and nothing is ever republished.
	fake3 := newFakePaho(true)
	c3 := newTestConn(fake3, 5)
	fake3.failToken = true
	if err := c3.pause(5); err == nil {
		t.Fatal("pause must fail on a token error")
	}
	if len(fake3.publishes) != 1 {
		t.Fatalf("publishes = %d, want no automatic retry", len(fake3.publishes))
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

func TestOnMessageStampsEntryGeneration(t *testing.T) {
	c := newTestConn(newFakePaho(true), 9)
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
	// The generation bumps after handler entry (a reconnect): the report
	// must keep the entry-time generation.
	c.mu.Lock()
	oldGen := c.generation
	c.mu.Unlock()
	c.onMessage(nil, fakeMessage{payload: []byte(`{"print":{}}`)})
	c.mu.Lock()
	c.generation = oldGen + 5
	c.mu.Unlock()

	<-seen
	if gotGen != oldGen {
		t.Fatalf("stamped generation = %d, want the entry-time %d", gotGen, oldGen)
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
