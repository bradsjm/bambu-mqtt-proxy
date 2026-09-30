// Package upstream manages one persistent MQTT connection per configured
// printer, with the retry lifecycle from DESIGN.md §5.1.
package upstream

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"bambu-mqtt-proxy/internal/config"
)

// Injector publishes upstream reports into the downstream broker, where the
// broker fans them out to subscribed clients.
type Injector interface {
	PublishDownstream(topic string, payload []byte, qos byte)
}

// ObserveFunc receives every upstream report before downstream forwarding.
// Seq is the per-connection delivery token assigned under the report mutex;
// reports from one transport are serialized in wire order, and a report from
// a retired transport is rejected before the observer runs. Gen is the
// generation of the transport the report arrived on, so a report can never
// be stamped with the generation of the connection that replaced it.
type ObserveFunc func(serial string, seq, gen uint64, payload []byte)

// ConnectivityObserver receives established and lost upstream connections.
type ConnectivityObserver func(serial string, connected bool, err error)

// PublishContext identifies the source and action for an upstream publish.
// SourcePacket is true when SourceQoS, SourceDup, and SourceRetain describe an
// incoming downstream MQTT PUBLISH packet.
type PublishContext struct {
	// Origin identifies the source of the publish.
	Origin string
	// Action identifies the operation that produced the publish.
	Action string
	// ClientID identifies the downstream client, when applicable.
	ClientID string
	// Reason records additional context for the publish, when applicable.
	Reason string
	// SourcePacket indicates that the source fields describe an incoming MQTT PUBLISH.
	SourcePacket bool
	// SourceQoS is the QoS of the incoming source packet.
	SourceQoS byte
	// SourceDup is the DUP flag of the incoming source packet.
	SourceDup bool
	// SourceRetain is the RETAIN flag of the incoming source packet.
	SourceRetain bool
}

// Pool owns one Conn per configured printer and merges downstream
// subscriptions onto each printer's single upstream connection.
type Pool struct {
	mu           sync.Mutex
	conns        map[string]*Conn
	specs        map[string]config.Printer
	behavior     config.Behavior
	inject       Injector
	observe      ObserveFunc
	connectivity ConnectivityObserver // observer copied to new connections
	log          *slog.Logger
	// closed makes shutdown terminal: no connection is created afterwards.
	closed bool
}

// NewPool creates a pool; connections are created lazily per printer on first
// use.
func NewPool(specs []config.Printer, inject Injector, behavior config.Behavior, log *slog.Logger) *Pool {
	p := &Pool{
		conns:    make(map[string]*Conn),
		specs:    make(map[string]config.Printer, len(specs)),
		behavior: behavior,
		inject:   inject,
		log:      log,
	}
	for _, spec := range specs {
		p.specs[spec.Serial] = spec
	}
	return p
}

// EnsureConnected reports whether the printer connection is (or becomes)
// established within timeout.
func (p *Pool) EnsureConnected(serial string, timeout time.Duration) bool {
	c, ok := p.existing(serial)
	if !ok {
		// No connection yet: engage one and wait within the budget.
		c = p.conn(serial)
		if c == nil {
			return false
		}
	}
	return c.ensure(timeout)
}

// Subscribe records downstream interest in filter on the printer's upstream
// connection, subscribing upstream when connected. Each new downstream
// interest on a connected printer also requests the configured warmup, so
// the newest subscriber converges to full state.
func (p *Pool) Subscribe(serial, filter string, qos byte) {
	p.RecordSubscribe(serial, filter, qos)()
}

// RecordSubscribe is Subscribe split in two: it records the reference at
// once without network waits, and the returned function completes the
// upstream work (connect, wire reconciliation, warmup). Callers that
// serialize reference ownership under their own lock record inside it and
// complete outside it. Completions reconcile the latest recorded state, so
// running them in any order converges.
func (p *Pool) RecordSubscribe(serial, filter string, qos byte) func() {
	if c := p.conn(serial); c != nil {
		return c.recordSubscribe(filter, qos)
	}
	return func() {}
}

// SubscribeAsync records downstream interest without waiting for upstream
// connectivity. It engages the connection supervisor and returns at once;
// the supervisor reconciles the merged filter set from the recorded refs
// once the printer answers. Use it for long-lived internal interests whose
// first delivery may wait for a printer that is offline at startup.
func (p *Pool) SubscribeAsync(serial, filter string, qos byte) {
	if c := p.conn(serial); c != nil {
		c.subscribeAsync(filter, qos)
	}
}

// Unsubscribe removes one downstream interest; the last removal unsubscribes
// upstream.
func (p *Pool) Unsubscribe(serial, filter string) {
	p.RecordUnsubscribe(serial, filter)()
}

// RecordUnsubscribe removes one reference at once; the returned function
// reconciles the wire. See RecordSubscribe.
func (p *Pool) RecordUnsubscribe(serial, filter string) func() {
	if c, ok := p.existing(serial); ok {
		return c.recordUnsubscribe(filter)
	}
	return func() {}
}

// RaiseQoS merges a repeated downstream interest's QoS into the stored
// maximum for an existing connection without changing refcounts; the change
// is reconciled onto the wire while connected, and reconnects restore the
// merged set at the highest stored request. Unknown connections create no
// interest.
func (p *Pool) RaiseQoS(serial, filter string, qos byte) {
	p.RecordRaiseQoS(serial, filter, qos)()
}

// RecordRaiseQoS merges the QoS at once; the returned function reconciles
// the wire. See RecordSubscribe.
func (p *Pool) RecordRaiseQoS(serial, filter string, qos byte) func() {
	if c, ok := p.existing(serial); ok {
		return c.recordRaiseQoS(filter, qos)
	}
	return func() {}
}

// Publish forwards a client request upstream. Fire-and-forget: when the
// upstream cannot be established within the connect budget the message is
// dropped and logged, matching the behavior of a dropped printer connection.
func (p *Pool) Publish(serial, topic string, payload []byte, qos byte) {
	p.PublishWithContext(serial, topic, payload, qos, PublishContext{Origin: "internal", Action: "publish"})
}

// PublishWithContext forwards a publish and records its source metadata in
// the upstream publish audit logs. The first publish on an idle connection
// engages the single-flight supervisor and waits one connect budget for the
// printer; a request that cannot be delivered within the budget is dropped
// without queueing or replay.
func (p *Pool) PublishWithContext(serial, topic string, payload []byte, qos byte, publishContext PublishContext) {
	if c := p.conn(serial); c != nil {
		c.publish(topic, payload, qos, publishContext)
	}
}

// Stop disconnects every upstream connection and ends all supervisors.
// Shutdown is terminal: later calls that would create a connection are
// no-ops.
func (p *Pool) Stop() {
	p.mu.Lock()
	p.closed = true
	conns := make([]*Conn, 0, len(p.conns))
	for _, c := range p.conns {
		conns = append(conns, c)
	}
	p.mu.Unlock()
	for _, c := range conns {
		c.stop()
	}
}

// Status reports per-serial upstream connectivity for the health endpoint.
// Every configured printer appears; serials never connected report false.
func (p *Pool) Status() map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]bool, len(p.specs))
	for serial := range p.specs {
		out[serial] = false
	}
	for serial, c := range p.conns {
		out[serial] = c.isConnected()
	}
	return out
}

// conn returns the connection for serial, creating it on first use. It
// returns nil after shutdown or for a serial outside the configured set;
// callers treat nil as "nothing to do".
func (p *Pool) conn(serial string) *Conn {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	spec, ok := p.specs[serial]
	if !ok {
		return nil
	}
	c, ok := p.conns[serial]
	if !ok {
		c = newConn(spec, p.behavior, p.inject, p.observe, p.connectivity, p.log)
		p.conns[serial] = c
		p.log.Info("upstream state created", "serial", serial, "address", c.spec.Address)
	}
	return c
}

// SetObserver registers a report observer invoked for every upstream report
// before downstream forwarding. Call it before serving traffic.
func (p *Pool) SetObserver(observe ObserveFunc) {
	p.mu.Lock()
	p.observe = observe
	p.mu.Unlock()
}

// SetConnectivityObserver registers an observer for successful upstream
// connections and connection losses. Call it before connections are created.
func (p *Pool) SetConnectivityObserver(observer ConnectivityObserver) {
	p.mu.Lock()
	p.connectivity = observer
	p.mu.Unlock()
}

// Generation returns the printer's current upstream connection generation.
// Every connect and every loss changes the value; 0 means no connection
// state exists yet.
func (p *Pool) Generation(serial string) uint64 {
	c, ok := p.existing(serial)
	if !ok {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

// PausePrint sends the pause command upstream. It fails unless the printer is
// connected on exactly the generation the caller validated against, so a
// decision made before a reconnect can never act on the new connection. QoS 0
// and the generation guard together make a replay across a reconnect
// impossible; callers own retry policy and the proxy never retries.
func (p *Pool) PausePrint(serial string, generation uint64) error {
	c, ok := p.existing(serial)
	if !ok {
		return fmt.Errorf("pause %s: no upstream connection", serial)
	}
	return c.pause(generation)
}

// SetSpeedProfile sends a guarded print-speed command. Profiles use Bambu's
// numeric values: 1 silent, 2 standard, 3 sport, and 4 ludicrous.
func (p *Pool) SetSpeedProfile(serial string, generation uint64, profile int) error {
	if profile < 1 || profile > 4 {
		return fmt.Errorf("speed profile %s: unsupported profile %d", serial, profile)
	}
	c, ok := p.existing(serial)
	if !ok {
		return fmt.Errorf("speed profile %s: no upstream connection", serial)
	}
	return c.command(generation, "speed profile", speedProfilePayload(profile))
}

// ResumePrint sends a guarded resume command.
func (p *Pool) ResumePrint(serial string, generation uint64) error {
	c, ok := p.existing(serial)
	if !ok {
		return fmt.Errorf("resume %s: no upstream connection", serial)
	}
	return c.command(generation, "resume", resumePayload())
}

// StopPrint sends a guarded stop command, cancelling the current print.
func (p *Pool) StopPrint(serial string, generation uint64) error {
	c, ok := p.existing(serial)
	if !ok {
		return fmt.Errorf("stop %s: no upstream connection", serial)
	}
	return c.command(generation, "stop", stopPayload())
}

// SetChamberLight sends a guarded chamber light on/off command.
func (p *Pool) SetChamberLight(serial string, generation uint64, on bool) error {
	c, ok := p.existing(serial)
	if !ok {
		return fmt.Errorf("chamber light %s: no upstream connection", serial)
	}
	return c.command(generation, "chamber light", chamberLightPayload(on))
}

// pausePayload is the Bambu pause command published to the request topic.
func pausePayload() string {
	return `{"print":{"sequence_id":"0","command":"pause"}}`
}

// resumePayload is the Bambu resume command.
func resumePayload() string {
	return `{"print":{"sequence_id":"0","command":"resume"}}`
}

// stopPayload is the Bambu stop command.
func stopPayload() string {
	return `{"print":{"sequence_id":"0","command":"stop"}}`
}

// chamberLightPayload returns Bambu's ledctrl command for the chamber light.
func chamberLightPayload(on bool) string {
	mode := "off"
	if on {
		mode = "on"
	}
	return `{"system":{"sequence_id":"0","command":"ledctrl","led_node":"chamber_light","led_mode":"` + mode +
		`","led_on_time":500,"led_off_time":500,"loop_times":0,"interval_time":0}}`
}

// speedProfilePayload returns Bambu's print_speed command. The parameter is
// a string on the wire, as required by the printer protocol.
func speedProfilePayload(profile int) string {
	return fmt.Sprintf(`{"print":{"sequence_id":"0","command":"print_speed","param":"%d"}}`, profile)
}

// pause publishes the pause command after validating the connection
// generation under the lock. An admitted command attempts once on the
// captured transport's client and never on a replacement, so a connection
// that changed before admission means the command is rejected, never
// republished. All rejection paths are errors.
func (c *Conn) pause(generation uint64) error {
	return c.command(generation, "pause", pausePayload())
}

// command publishes one guarded QoS 0 print command. The generation and
// connection checks are shared by pause and speed-profile control.
func (c *Conn) command(generation uint64, name, payload string) error {
	topic := c.requestTopic()
	ctx := PublishContext{Origin: "internal", Action: name}
	c.mu.Lock()
	t := c.active
	usable := !c.stopped && transportUsable(t)
	gen := c.generation
	var client mqtt.Client
	if usable {
		client = t.client
		gen = t.generation
	}
	c.mu.Unlock()
	if client == nil {
		attrs := publishLogAttrs(ctx, c.spec.Serial, topic, 0, false, gen, 0, "not_sent", "none", "upstream is not connected")
		c.log.Debug("printer command payload details", publishDebugLogAttrs(ctx, c.spec.Serial, topic, []byte(payload), 0, false, gen, 0, "not_sent", "none", "upstream is not connected")...)
		c.log.Warn("printer command not attempted", attrs...)
		return fmt.Errorf("%s %s: upstream is not connected", name, c.spec.Serial)
	}
	if gen != generation {
		attrs := publishLogAttrs(ctx, c.spec.Serial, topic, 0, false, gen, 0, "not_sent", "none", "connection generation changed")
		attrs = append(attrs, "expected_generation", generation)
		c.log.Debug("printer command payload details", publishDebugLogAttrs(ctx, c.spec.Serial, topic, []byte(payload), 0, false, gen, 0, "not_sent", "none", "connection generation changed")...)
		c.log.Warn("printer command not attempted", attrs...)
		return fmt.Errorf("%s %s: connection generation changed (have %d, want %d)",
			name, c.spec.Serial, gen, generation)
	}
	_, tok, completed := c.publishUpstream(client, gen, ctx, topic, []byte(payload), 0, false)
	if !completed {
		return fmt.Errorf("%s %s: publish timed out after %s", name, c.spec.Serial, c.connectTO)
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("%s %s: %w", name, c.spec.Serial, err)
	}
	// No post-send generation check: transport identity already guarantees
	// the command never moved to a replacement connection. A local QoS 0
	// success proves delivery was attempted, not that the printer executed.
	return nil
}

// existing returns an already-created connection or ok=false.
func (p *Pool) existing(serial string) (*Conn, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.conns[serial]
	return c, ok
}

// subRef tracks how many downstream clients share one merged upstream filter
// and the highest requested QoS.
type subRef struct {
	count int
	qos   byte
}

// subRefs tracks downstream interest counts and the highest requested QoS per
// merged upstream filter.
type subRefs struct {
	refs map[string]subRef
}

// newSubRefs creates an empty interest set.
func newSubRefs() *subRefs {
	return &subRefs{refs: make(map[string]subRef)}
}

// add records one interest; it reports whether this is the first for the filter.
func (s *subRefs) add(filter string, qos byte) bool {
	prev, have := s.refs[filter]
	next := subRef{count: prev.count + 1, qos: prev.qos}
	if !have {
		next = subRef{count: 1, qos: qos}
	} else if qos > next.qos {
		next.qos = qos
	}
	s.refs[filter] = next
	return !have
}

// remove drops one interest; it reports whether this was the last.
func (s *subRefs) remove(filter string) bool {
	prev, have := s.refs[filter]
	if !have {
		return false
	}
	prev.count--
	if prev.count <= 0 {
		delete(s.refs, filter)
		return true
	}
	s.refs[filter] = prev
	return false
}

// raise merges a higher requested QoS into an existing interest without
// changing its refcount; absent filters stay absent.
func (s *subRefs) raise(filter string, qos byte) {
	prev, have := s.refs[filter]
	if !have || qos <= prev.qos {
		return
	}
	s.refs[filter] = subRef{count: prev.count, qos: qos}
}

// snapshot returns a copy of the merged filter set for reconciliation.
func (s *subRefs) snapshot() map[string]byte {
	out := make(map[string]byte, len(s.refs))
	for f, r := range s.refs {
		out[f] = r.qos
	}
	return out
}

// count returns the number of downstream clients sharing one filter.
func (s *subRefs) count(filter string) int {
	return s.refs[filter].count
}

// transport owns every resource of one connection attempt: its Paho client,
// its dialed socket, and the loss signal the supervisor waits on. A transport
// is used by exactly one attempt and is never reused after it fails, times
// out, or is retired.
type transport struct {
	client mqtt.Client
	// generation is assigned at installation, before any handler or
	// subscription is registered, and never changes afterwards.
	generation uint64

	lost     chan struct{} // closed once when this transport loses the connection
	lostOnce sync.Once
	lostMu   sync.Mutex
	lostErr  error

	ctx    context.Context // the dial/handshake context for this attempt
	cancel context.CancelFunc

	sockMu sync.Mutex
	sock   net.Conn

	// onWire records the subscriptions live on this transport, with their
	// granted QoS. Guarded only by Conn.subMu; a replacement transport
	// starts empty.
	onWire map[string]byte

	// connectTok joins the outstanding Connect on teardown.
	connectTok mqtt.Token
}

// signalLoss records why this transport lost the connection and closes its
// loss channel once. Safe to call from the Paho loss callback: it never
// touches the connection generation and never waits on tokens.
func (t *transport) signalLoss(err error) {
	t.lostMu.Lock()
	if t.lostErr == nil {
		t.lostErr = err
	}
	t.lostMu.Unlock()
	t.lostOnce.Do(func() { close(t.lost) })
}

// lossErr returns the recorded loss cause, or nil when none was recorded.
func (t *transport) lossErr() error {
	t.lostMu.Lock()
	defer t.lostMu.Unlock()
	return t.lostErr
}

// adoptSocket registers conn as this transport's owned socket unless the
// attempt context is already canceled; a dial losing that race closes its
// result instead of registering it.
func (t *transport) adoptSocket(conn net.Conn) error {
	t.sockMu.Lock()
	defer t.sockMu.Unlock()
	if err := t.ctx.Err(); err != nil {
		conn.Close()
		return err
	}
	t.sock = conn
	return nil
}

// takeSocket removes and returns the registered socket, if any.
func (t *transport) takeSocket() net.Conn {
	t.sockMu.Lock()
	defer t.sockMu.Unlock()
	sock := t.sock
	t.sock = nil
	return sock
}

// joinConnect waits for the attempt's Connect token to settle, bounded so a
// wedged client cannot stall retirement or the next attempt.
func (t *transport) joinConnect(bound time.Duration) {
	if t.connectTok == nil {
		return
	}
	select {
	case <-t.connectTok.Done():
	case <-time.After(bound):
	}
}

// transportUsable reports whether t may carry traffic right now: it exists,
// has a live client, has not signaled loss, and the client connection is
// open. The caller pairs the result with other Conn state under c.mu.
func transportUsable(t *transport) bool {
	if t == nil || t.client == nil {
		return false
	}
	select {
	case <-t.lost:
		return false
	default:
	}
	return t.client.IsConnectionOpen()
}

// Conn is the single upstream MQTT connection for one printer. Its supervisor
// is the only owner of connect and loss transitions: each attempt builds a
// fresh, non-reconnecting Paho client and a dedicated transport, and the
// supervisor retires that transport before every reconnect, so at most one
// printer socket and client exist per connection at any time.
type Conn struct {
	spec        config.Printer
	keepalive   time.Duration
	connectTO   time.Duration
	backoffInit time.Duration
	backoffMax  time.Duration
	warmup      []string
	inject      Injector
	// observe sees every upstream report before downstream forwarding.
	// Telemetry registers it; nil on ordinary pools.
	observe      ObserveFunc
	connectivity ConnectivityObserver // observer called at connection boundaries
	log          *slog.Logger

	// newClient builds the Paho client for one transport. Production code
	// leaves it nil and the supervisor uses pahoClient; lifecycle tests
	// inject a deterministic fake.
	newClient func(*transport) mqtt.Client

	stopCh chan struct{}
	mu     sync.Mutex
	// desired records that a supervisor should run; stopped is terminal.
	desired bool
	stopped bool
	// connecting is the in-flight attempt's client, registered before
	// Connect so shutdown can always tear it down.
	connecting mqtt.Client
	// active is the installed transport; nil while disconnected.
	active *transport
	// supDone closes when the supervisor goroutine exits; nil until started.
	supDone chan struct{}
	// subs is the desired interest set, guarded by mu.
	subs      *subRefs
	backoffN  int
	nextRetry time.Time
	// connCh is closed (and replaced) on every successful install, so any
	// waiter captured while disconnected wakes on the next install.
	connCh chan struct{}
	// generation changes on every connect and every loss; the detection
	// engine's pause guard compares it to the generation a decision was
	// validated on. A transport's generation is assigned from it at install.
	generation uint64
	// reportSeq hands each observed report its serialized order token.
	reportSeq atomic.Uint64
	// publishSeq identifies publish attempts within this printer connection.
	publishSeq atomic.Uint64
	// reportMu serializes report observation and injection in wire order and
	// guards the short active-pointer transitions at install and retirement.
	// Lock order: reportMu, then mu. Never taken while holding mu or subMu.
	reportMu sync.Mutex
	// subMu serializes wire reconciliation and guards each transport's
	// onWire map. Intent (subs) is guarded by mu alone, so recording an
	// interest never waits behind a reconciliation's token waits.
	// Lock order: subMu, then mu. Never taken while holding mu or reportMu.
	subMu sync.Mutex
}

// newConn builds the connection state for one printer. observe may be nil.
func newConn(spec config.Printer, behavior config.Behavior, inject Injector,
	observe ObserveFunc, connectivity ConnectivityObserver, log *slog.Logger) *Conn {
	return &Conn{
		spec:         spec,
		keepalive:    time.Duration(behavior.UpstreamKeepaliveSeconds) * time.Second,
		connectTO:    time.Duration(behavior.UpstreamConnectTimeoutSeconds) * time.Second,
		backoffInit:  time.Duration(behavior.UpstreamBackoffInitialSeconds) * time.Second,
		backoffMax:   time.Duration(behavior.UpstreamBackoffMaxSeconds) * time.Second,
		warmup:       behavior.WarmupCommands,
		inject:       inject,
		observe:      observe,
		connectivity: connectivity,
		log:          log,
		stopCh:       make(chan struct{}),
		subs:         newSubRefs(),
		connCh:       make(chan struct{}),
	}
}

// ensure returns true when the connection is established within timeout.
// While the supervisor is in backoff-wait it refuses immediately when the
// scheduled retry falls outside the budget, per the design: hooks never wait
// on a scheduled future retry.
func (c *Conn) ensure(timeout time.Duration) bool {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return false
	}
	if transportUsable(c.active) {
		c.mu.Unlock()
		return true
	}
	if !c.desired {
		c.desired = true
		go c.supervise()
	}
	ch := c.connCh
	next := c.nextRetry
	c.mu.Unlock()

	if !next.IsZero() && next.After(time.Now().Add(timeout)) {
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
	case <-c.stopCh:
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.stopped && transportUsable(c.active)
}

// supervise runs the single-flight connection loop until the connection is
// stopped. Each iteration owns one attempt end to end: build a fresh client
// and transport, connect, install on success, and wait for that transport's
// loss or the stop signal. Every outage — refused connect, timeout, or loss —
// retires the attempt's resources and waits the capped, jittered backoff
// before trying again.
func (c *Conn) supervise() {
	c.mu.Lock()
	if c.stopped || !c.desired || c.supDone != nil {
		c.mu.Unlock()
		return
	}
	done := make(chan struct{})
	c.supDone = done
	c.mu.Unlock()
	defer close(done)

	for {
		if !c.attempt() {
			return
		}
		// An attempt ends in one of three ways: installed and then lost
		// (backoff and retry), failed or abandoned before installation
		// (backoff and retry), or stopped (return, handled inside attempt).
		c.mu.Lock()
		stopped := c.stopped
		d := time.Duration(0)
		if !stopped {
			c.backoffN++
			d = nextBackoff(c.backoffN, c.backoffInit, c.backoffMax)
			c.nextRetry = time.Now().Add(d)
		}
		c.mu.Unlock()
		if stopped {
			return
		}
		c.log.Info("upstream backoff scheduled",
			"serial", c.spec.Serial,
			"address", c.spec.Address,
			"state", "BACKOFF",
			"attempt", c.backoffN,
			"retry_in", d.String())
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
			timer.Stop()
		case <-c.stopCh:
			timer.Stop()
			return
		}
	}
}

// attempt runs one connect cycle. It reports false only when the connection
// was stopped and the supervisor should exit; a failed or lost attempt
// returns true after scheduling backoff.
func (c *Conn) attempt() bool {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return false
	}
	attempt := c.backoffN + 1
	c.mu.Unlock()
	c.log.Info("upstream connection attempt",
		"serial", c.spec.Serial,
		"address", c.spec.Address,
		"state", "CONNECTING",
		"attempt", attempt)

	t := &transport{lost: make(chan struct{})}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	if c.newClient != nil {
		t.client = c.newClient(t)
	} else {
		t.client = c.pahoClient(t)
	}

	// Register the in-flight client before Connect so shutdown can always
	// reach it, even while it is dialing.
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		c.abandonAttempt(t, nil)
		return false
	}
	c.connecting = t.client
	c.mu.Unlock()

	tok := t.client.Connect()
	t.connectTok = tok
	timer := time.NewTimer(c.connectTO + time.Second)
	var connectErr error
	timedOut := false
	select {
	case <-tok.Done():
		timer.Stop()
		connectErr = tok.Error()
	case <-timer.C:
		timedOut = true
		connectErr = fmt.Errorf("connect timed out after %s", c.connectTO)
	case <-c.stopCh:
		timer.Stop()
		// Recheck stopped after a late successful Connect: the client is
		// torn down without subscriptions or warmup, and no replacement
		// attempt is started.
		c.abandonAttempt(t, tok)
		return false
	}

	c.mu.Lock()
	stopped := c.stopped
	c.mu.Unlock()
	if stopped {
		c.abandonAttempt(t, tok)
		return false
	}
	if timedOut || connectErr != nil {
		c.abandonAttempt(t, tok)
		c.log.Info("upstream connect attempt failed",
			"serial", c.spec.Serial,
			"address", c.spec.Address,
			"attempt", attempt,
			"error", errString(connectErr))
		return true
	}

	// Success: install the transport. The generation is assigned before any
	// handler or subscription exists, so no report can carry a generation
	// the connection never advertised.
	c.reportMu.Lock()
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		c.reportMu.Unlock()
		c.abandonAttempt(t, tok)
		return false
	}
	c.generation++
	t.generation = c.generation
	c.active = t
	c.connecting = nil
	c.backoffN = 0
	c.nextRetry = time.Time{}
	reconnect := c.generation > 1
	close(c.connCh)
	c.connCh = make(chan struct{})
	c.mu.Unlock()
	c.reportMu.Unlock()

	c.log.Info("upstream connected",
		"serial", c.spec.Serial,
		"address", c.spec.Address,
		"attempt", attempt)
	if c.connectivity != nil {
		c.connectivity(c.spec.Serial, true, nil)
	}

	// Restore the complete desired subscription set on the fresh transport,
	// then warm up once so subscribers converge to full state.
	c.subMu.Lock()
	c.reconcileSubsLocked()
	c.subMu.Unlock()
	reason := "initial_connect"
	if reconnect {
		reason = "reconnect"
	}
	c.log.Info("upstream connection ready",
		"serial", c.spec.Serial,
		"connection_generation", t.generation,
		"reconnect", reconnect)
	c.sendWarmup(reason)

	// Own this transport until it loses the connection or the connection is
	// stopped.
	select {
	case <-t.lost:
		// Stop may have raced the loss; prefer the silent shutdown path.
		select {
		case <-c.stopCh:
			c.retire(t, false)
			return false
		default:
		}
		c.retire(t, true)
		return true
	case <-c.stopCh:
		c.retire(t, false)
		return false
	}
}

// abandonAttempt tears down an attempt that never installed: cancel the dial
// context, close any socket it registered, disconnect the client (a connecting
// client may not report IsConnected), and join the outstanding Connect token
// so the next attempt starts clean.
func (c *Conn) abandonAttempt(t *transport, tok mqtt.Token) {
	t.cancel()
	if sock := t.takeSocket(); sock != nil {
		sock.Close()
	}
	if t.client != nil {
		t.client.Disconnect(250)
	}
	t.joinConnect(c.connectTO)
	c.mu.Lock()
	if c.connecting == t.client {
		c.connecting = nil
	}
	c.mu.Unlock()
}

// retire tears down an installed transport. notify selects whether the loss
// reaches the connectivity observer and logs: outages report, shutdown stays
// silent. The transport is removed under the report mutex so an in-flight
// report callback either finishes first or is rejected, and the generation
// advances so stale evidence goes stale.
func (c *Conn) retire(t *transport, notify bool) {
	t.cancel()
	if sock := t.takeSocket(); sock != nil {
		sock.Close()
	}
	t.client.Disconnect(250)
	t.joinConnect(c.connectTO)

	c.reportMu.Lock()
	c.mu.Lock()
	generation := t.generation
	if c.active == t {
		c.active = nil
		c.generation++
		generation = c.generation
	}
	if c.connecting == t.client {
		c.connecting = nil
	}
	c.mu.Unlock()
	c.reportMu.Unlock()

	if !notify {
		return
	}
	err := t.lossErr()
	if err == nil {
		err = fmt.Errorf("connection lost")
	}
	if c.connectivity != nil {
		c.connectivity(c.spec.Serial, false, err)
	}
	c.log.Warn("upstream connection lost",
		"serial", c.spec.Serial,
		"state", "BACKOFF",
		"connection_generation", t.generation,
		"current_generation", generation,
		"backoff_max", c.backoffMax.String(),
		"error", errString(err))
}

// pahoClient builds the non-reconnecting Paho client for one transport. The
// loss callback only signals the transport; the supervisor owns every other
// transition. The custom open-connection function registers the dialed socket
// on the transport so retirement owns the printer socket, not just the client.
func (c *Conn) pahoClient(t *transport) mqtt.Client {
	scheme := "tcp"
	if c.spec.TLS {
		scheme = "ssl"
	}
	opts := mqtt.NewClientOptions().
		AddBroker(scheme + "://" + c.spec.Address).
		SetClientID("bmbpx-" + c.spec.Serial).
		SetUsername(c.spec.Username).
		SetPassword(c.spec.Password).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetOrderMatters(true).
		SetConnectTimeout(c.connectTO).
		SetStore(mqtt.NewMemoryStore()).
		SetKeepAlive(c.keepalive).
		SetPingTimeout(10 * time.Second).
		SetCustomOpenConnectionFn(func(uri *url.URL, options mqtt.ClientOptions) (net.Conn, error) {
			return c.openSocket(t, uri, options)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			t.signalLoss(err)
		})
	if c.spec.TLS {
		opts.SetTLSConfig(&tls.Config{InsecureSkipVerify: c.spec.InsecureSkipVerify})
	}
	return mqtt.NewClient(opts)
}

// openSocket dials the printer with the attempt context so cancellation beats
// any dial or handshake, and registers the result on the transport. TLS
// verification policy is preserved from the configured options.
func (c *Conn) openSocket(t *transport, uri *url.URL, options mqtt.ClientOptions) (net.Conn, error) {
	timeout := options.ConnectTimeout
	if timeout <= 0 {
		timeout = c.connectTO
	}
	dialer := &net.Dialer{Timeout: timeout}
	var conn net.Conn
	var err error
	switch uri.Scheme {
	case "ssl", "tls", "mqtts", "mqtt+ssl", "tcps":
		cfg := options.TLSConfig
		if cfg == nil {
			cfg = &tls.Config{}
		}
		cfg = cfg.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName = uri.Hostname()
		}
		conn, err = (&tls.Dialer{NetDialer: dialer, Config: cfg}).DialContext(t.ctx, "tcp", uri.Host)
	default:
		conn, err = dialer.DialContext(t.ctx, "tcp", uri.Host)
	}
	if err != nil {
		return nil, err
	}
	if derr := t.adoptSocket(conn); derr != nil {
		return nil, derr
	}
	return conn, nil
}

// failTransport records why a transport's subscription state can no longer be
// trusted and signals its loss once, so the supervisor retires it and
// reconnects to restore the complete desired set. A healthy-looking connection
// with unknown subscription state never survives.
func (c *Conn) failTransport(t *transport, err error) {
	c.log.Warn("upstream subscription state unknown; reconnecting",
		"serial", c.spec.Serial,
		"connection_generation", t.generation,
		"error", errString(err))
	t.signalLoss(err)
}

// onMessage forwards an upstream report into the downstream broker under the
// report mutex. The transport binding rejects reports from a retired
// transport before sequence assignment, observation, or injection, so an old
// connection's callback can never arrive after a replacement's report.
func (c *Conn) onMessage(t *transport, msg mqtt.Message) {
	c.reportMu.Lock()
	c.mu.Lock()
	current := c.active == t
	generation := t.generation
	c.mu.Unlock()
	if !current {
		c.reportMu.Unlock()
		return
	}
	seq := c.reportSeq.Add(1)
	if c.observe != nil {
		// Wire order is preserved: Paho dispatches synchronously
		// (SetOrderMatters) and this mutex serializes across transports, so
		// sequence tokens are monotonic in delivery order.
		c.observe(c.spec.Serial, seq, generation, msg.Payload())
	}
	c.log.Debug("printer report received", "origin", "printer", "action", "report", "serial", c.spec.Serial, "topic", msg.Topic(), "payload", string(msg.Payload()), "qos", msg.Qos(), "retain", msg.Retained(), "dup", msg.Duplicate(), "connection_generation", generation, "sequence", seq, "delivery", "received")
	qos := msg.Qos()
	if qos > 1 {
		qos = 1
	}
	c.inject.PublishDownstream(msg.Topic(), msg.Payload(), qos)
	c.reportMu.Unlock()
}

// reportHandler binds the report callback to one immutable transport.
func (c *Conn) reportHandler(t *transport) mqtt.MessageHandler {
	return func(_ mqtt.Client, msg mqtt.Message) {
		c.onMessage(t, msg)
	}
}

// reportFilter maps a downstream filter to this printer's exact report topic;
// request-only filters are not subscribed upstream.
func (c *Conn) reportFilter(filter string) string {
	if strings.HasSuffix(filter, "/request") {
		return ""
	}
	return fmt.Sprintf("device/%s/report", c.spec.Serial)
}

// subscribe records the filter and reconciles it upstream when connected. A
// new downstream interest on a connected printer also requests the configured
// warmup, so late subscribers converge to full state; one subscription
// operation produces one warmup.
func (c *Conn) subscribe(filter string, qos byte) {
	c.recordSubscribe(filter, qos)()
}

// recordSubscribe records the interest under mu and returns the upstream
// completion; see Pool.RecordSubscribe.
func (c *Conn) recordSubscribe(filter string, qos byte) func() {
	// Upstream subscriptions cover report-leaf filters only. Subscribing to
	// request filters upstream would make the printer broker echo proxied
	// requests back to the proxy and out to downstream subscribers.
	filter = c.reportFilter(filter)
	if filter == "" {
		return func() {}
	}
	if qos > 1 {
		qos = 1
	}
	c.mu.Lock()
	first := c.subs.add(filter, qos)
	c.mu.Unlock()
	return func() { c.completeSubscribe(first) }
}

// completeSubscribe connects, reconciles, and warms up after an interest
// was recorded; first reports whether it was the filter's first reference.
func (c *Conn) completeSubscribe(first bool) {
	if !first {
		// The report interest is already live; this late subscriber needs
		// full state, not another upstream subscription. Its QoS may have
		// raised the desired maximum, so reconcile the wire first.
		c.subMu.Lock()
		c.reconcileSubsLocked()
		c.subMu.Unlock()
		c.sendWarmup("subscriber_added")
		return
	}
	if !c.ensure(c.connectTO) {
		// Recorded; the supervisor reconciles the merged set at connect and
		// its restoration warmup covers this subscriber.
		return
	}
	c.subMu.Lock()
	c.reconcileSubsLocked()
	c.subMu.Unlock()
	// Report subscription is ready: request full state for this subscriber.
	c.sendWarmup("subscription_ready")
}

// subscribeAsync records the filter and engages the connection supervisor
// without waiting for it. The recorded interest survives printer outages: the
// supervisor reconciles the whole merged set once the printer answers, which
// is the same path a subscribe that lost the ensure race relies on.
func (c *Conn) subscribeAsync(filter string, qos byte) {
	filter = c.reportFilter(filter)
	if filter == "" {
		return
	}
	if qos > 1 {
		qos = 1
	}
	c.mu.Lock()
	first := c.subs.add(filter, qos)
	c.mu.Unlock()
	c.mu.Lock()
	alreadyDesired := c.desired || c.stopped
	if !alreadyDesired {
		c.desired = true
		go c.supervise()
	}
	usable := !c.stopped && transportUsable(c.active)
	c.mu.Unlock()
	if usable {
		// A transport may already be live (the supervisor is idle); push the
		// new interest onto it now instead of waiting for a reconnect.
		c.subMu.Lock()
		c.reconcileSubsLocked()
		c.subMu.Unlock()
	}
	if !first {
		return
	}
	c.log.Info("upstream interest recorded",
		"serial", c.spec.Serial, "filter", filter, "qos", qos, "mode", "async")
}

// unsubscribe removes one interest; the last removal unsubscribes upstream
// through reconciliation.
func (c *Conn) unsubscribe(filter string) {
	c.recordUnsubscribe(filter)()
}

// recordUnsubscribe removes the reference under mu and returns the wire
// reconciliation for a last removal.
func (c *Conn) recordUnsubscribe(filter string) func() {
	filter = c.reportFilter(filter)
	if filter == "" {
		return func() {}
	}
	c.mu.Lock()
	last := c.subs.remove(filter)
	c.mu.Unlock()
	if !last {
		return func() {}
	}
	return c.reconcileSubs
}

// raiseQoS merges a repeated interest's QoS into the stored maximum without
// changing refcounts, then reconciles: while connected a higher stored request
// upgrades the wire subscription; while offline the reconnect restores the
// merged set at the highest downstream request. Request-only filters have no
// upstream interest.
func (c *Conn) raiseQoS(filter string, qos byte) {
	c.recordRaiseQoS(filter, qos)()
}

// recordRaiseQoS merges the QoS under mu and returns the wire
// reconciliation.
func (c *Conn) recordRaiseQoS(filter string, qos byte) func() {
	filter = c.reportFilter(filter)
	if filter == "" {
		return func() {}
	}
	if qos > 1 {
		qos = 1
	}
	c.mu.Lock()
	c.subs.raise(filter, qos)
	c.mu.Unlock()
	return c.reconcileSubs
}

// reconcileSubs runs reconcileSubsLocked under subMu.
func (c *Conn) reconcileSubs() {
	c.subMu.Lock()
	c.reconcileSubsLocked()
	c.subMu.Unlock()
}

// reconcileSubsLocked converges the active transport's wire subscriptions to
// the desired interest set: subscribe missing filters and QoS increases,
// unsubscribe filters no longer desired, and record grants only after the
// broker confirms them. Caller holds c.subMu. Any failure — timeout, missing
// grant, 0x80, lower-QoS grant, or token error — retires the transport via a
// loss signal rather than leaving unknown subscription state behind.
func (c *Conn) reconcileSubsLocked() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	t := c.active
	desired := c.subs.snapshot()
	c.mu.Unlock()
	if !transportUsable(t) {
		// Nothing to reconcile onto; the supervisor restores the full set at
		// the next install.
		return
	}
	if t.onWire == nil {
		t.onWire = make(map[string]byte)
	}
	subscribed, removed := 0, 0
	for f, q := range desired {
		cur, have := t.onWire[f]
		if have && cur >= q {
			continue
		}
		tok := t.client.Subscribe(f, q, c.reportHandler(t))
		if !tok.WaitTimeout(c.connectTO) {
			c.failTransport(t, fmt.Errorf("subscribe %q timed out after %s", f, c.connectTO))
			return
		}
		granted, ok := subscribeGrant(tok, f, q)
		if !ok {
			c.failTransport(t, fmt.Errorf("subscribe %q not granted at QoS %d (granted 0x%02x)", f, q, granted))
			return
		}
		t.onWire[f] = granted
		subscribed++
	}
	for f := range t.onWire {
		if _, want := desired[f]; want {
			continue
		}
		tok := t.client.Unsubscribe(f)
		if !tok.WaitTimeout(c.connectTO) {
			c.failTransport(t, fmt.Errorf("unsubscribe %q timed out after %s", f, c.connectTO))
			return
		}
		if err := tok.Error(); err != nil {
			c.failTransport(t, fmt.Errorf("unsubscribe %q: %w", f, err))
			return
		}
		delete(t.onWire, f)
		removed++
	}
	if subscribed > 0 || removed > 0 {
		c.log.Info("upstream subscriptions restored",
			"serial", c.spec.Serial,
			"connection_generation", t.generation,
			"filters", len(desired),
			"restored", subscribed,
			"removed", removed)
	}
}

// subscribeGrant inspects a completed Subscribe token for the broker's grant
// of filter. Paho reports rejections through the per-filter result map, so
// the map is authoritative when the token provides one: a missing grant, a
// 0x80 code, or a grant below the request is a failure.
func subscribeGrant(tok mqtt.Token, filter string, requested byte) (byte, bool) {
	if err := tok.Error(); err != nil {
		return 0, false
	}
	if gr, ok := tok.(interface{ Result() map[string]byte }); ok {
		granted, have := gr.Result()[filter]
		if !have || granted >= 0x80 || granted < requested {
			return granted, false
		}
		return granted, true
	}
	// Tokens without a per-filter result report success only through the
	// token error, already checked above.
	return requested, true
}

// publish forwards a client request upstream, fire-and-forget. The first
// publish engages the supervisor and waits one connect budget; a request that
// cannot be delivered in time is logged and dropped, never queued or replayed.
func (c *Conn) publish(topic string, payload []byte, qos byte, publishContext PublishContext) {
	if qos > 1 {
		qos = 1
	}
	if !c.ensure(c.connectTO) {
		c.mu.Lock()
		generation := c.generation
		c.mu.Unlock()
		c.log.Warn("upstream publish not attempted", publishLogAttrs(publishContext, c.spec.Serial, topic, qos, false, generation, 0, "not_sent", "none", "upstream is unavailable")...)
		c.log.Debug("upstream publish payload details", publishDebugLogAttrs(publishContext, c.spec.Serial, topic, payload, qos, false, generation, 0, "not_sent", "none", "upstream is unavailable")...)
		return
	}
	c.mu.Lock()
	t := c.active
	usable := !c.stopped && transportUsable(t)
	generation := c.generation
	var client mqtt.Client
	if usable {
		client = t.client
		generation = t.generation
	}
	c.mu.Unlock()
	if client == nil {
		c.log.Warn("upstream publish not attempted", publishLogAttrs(publishContext, c.spec.Serial, topic, qos, false, generation, 0, "not_sent", "none", "upstream is unavailable")...)
		c.log.Debug("upstream publish payload details", publishDebugLogAttrs(publishContext, c.spec.Serial, topic, payload, qos, false, generation, 0, "not_sent", "none", "upstream is unavailable")...)
		return
	}
	c.publishUpstream(client, generation, publishContext, topic, payload, qos, false)
}

// publishUpstream logs the publish attempt before handing it to Paho, then
// sends only through the captured client. Its token completes locally for
// QoS 0, and after broker PUBACK for QoS 1.
func (c *Conn) publishUpstream(client mqtt.Client, generation uint64, publishContext PublishContext, topic string, payload []byte, qos byte, retain bool) (uint64, mqtt.Token, bool) {
	attemptID := c.publishSeq.Add(1)
	c.log.Debug("upstream publish attempt", publishDebugLogAttrs(publishContext, c.spec.Serial, topic, payload, qos, retain, generation, attemptID, "attempted", "not_yet_observed", "")...)
	tok := client.Publish(topic, qos, retain, payload)
	completed := tok.WaitTimeout(c.connectTO)
	if !completed {
		c.log.Warn("upstream publish completion wait timed out", publishLogAttrs(publishContext, c.spec.Serial, topic, qos, retain, generation, attemptID, "pending", "pending", "completion wait timed out")...)
		c.log.Debug("upstream publish timeout payload details", publishDebugLogAttrs(publishContext, c.spec.Serial, topic, payload, qos, retain, generation, attemptID, "pending", "pending", "completion wait timed out")...)
		return attemptID, tok, false
	}
	if err := tok.Error(); err != nil {
		c.log.Warn("upstream publish failed", publishLogAttrs(publishContext, c.spec.Serial, topic, qos, retain, generation, attemptID, "error", "unknown", errString(err))...)
		c.log.Debug("upstream publish failure payload details", publishDebugLogAttrs(publishContext, c.spec.Serial, topic, payload, qos, retain, generation, attemptID, "error", "unknown", errString(err))...)
		return attemptID, tok, true
	}
	completion := "local_qos0_complete"
	delivery := "local_only_no_broker_ack"
	if qos > 0 {
		completion = "broker_acknowledged"
		delivery = "mqtt_puback_received"
	}
	c.log.Debug("upstream publish complete", publishDebugLogAttrs(publishContext, c.spec.Serial, topic, payload, qos, retain, generation, attemptID, completion, delivery, "")...)
	return attemptID, tok, completed
}

// publishLogAttrs builds structured attributes for an upstream publish log.
func publishLogAttrs(publishContext PublishContext, serial, topic string, qos byte, retain bool, generation, attemptID uint64, completion, delivery, err string) []any {
	replayEligibility := "not_replayable_qos0"
	if qos > 0 {
		switch completion {
		case "not_sent":
			replayEligibility = "not_queued"
		case "broker_acknowledged":
			replayEligibility = "no_replay_pending_after_puback"
		case "pending":
			replayEligibility = "possible_paho_qos1_replay"
		case "error":
			replayEligibility = "unknown_after_publish_error"
		case "attempted":
			replayEligibility = "not_yet_observed"
		default:
			replayEligibility = "unknown"
		}
	}
	attrs := []any{
		"origin", publishContext.Origin,
		"action", publishContext.Action,
		"serial", serial,
		"topic", topic,
		"qos", qos,
		"retain", retain,
		"connection_generation", generation,
		"publish_attempt", attemptID,
		"completion", completion,
		"delivery_confirmation", delivery,
		"printer_execution", "unconfirmed",
		"replay_eligibility", replayEligibility,
		"wire_replay_observable", false,
	}
	if publishContext.ClientID != "" {
		attrs = append(attrs, "origin_id", publishContext.ClientID, "client", publishContext.ClientID)
	}
	if publishContext.Reason != "" {
		attrs = append(attrs, "reason", publishContext.Reason)
	}
	if publishContext.SourcePacket {
		attrs = append(attrs,
			"source_qos", publishContext.SourceQoS,
			"source_dup", publishContext.SourceDup,
			"source_retain", publishContext.SourceRetain,
		)
	}
	if err != "" {
		attrs = append(attrs, "error", err)
	}
	return attrs
}

// publishDebugLogAttrs adds payload details to upstream publish log attributes.
func publishDebugLogAttrs(publishContext PublishContext, serial, topic string, payload []byte, qos byte, retain bool, generation, attemptID uint64, completion, delivery, err string) []any {
	attrs := publishLogAttrs(publishContext, serial, topic, qos, retain, generation, attemptID, completion, delivery, err)
	attrs = append(attrs, "payload", string(payload))
	if !utf8.Valid(payload) {
		attrs = append(attrs, "payload_base64", base64.StdEncoding.EncodeToString(payload))
	}
	return attrs
}

// sendWarmup publishes the configured warmup commands through the active
// transport's client so the printer pushes its full state and every
// subscriber converges without client action. It is a no-op while the
// upstream is unavailable.
func (c *Conn) sendWarmup(reason string) {
	c.mu.Lock()
	t := c.active
	usable := !c.stopped && transportUsable(t)
	generation := c.generation
	var client mqtt.Client
	if usable {
		client = t.client
		generation = t.generation
	}
	c.mu.Unlock()
	if client == nil {
		c.log.Debug("upstream warmup skipped", "serial", c.spec.Serial, "reason", reason, "connection_generation", generation, "cause", "upstream_unavailable")
		return
	}
	warmupCompleted := 0
	for _, cmd := range c.warmup {
		ctx := PublishContext{Origin: "internal", Action: "warmup", Reason: reason}
		_, tok, completed := c.publishUpstream(client, generation, ctx, c.requestTopic(), []byte(cmd), 0, false)
		if !completed || tok.Error() != nil {
			continue
		}
		warmupCompleted++
	}
	if len(c.warmup) > 0 {
		c.log.Info("upstream warmup publish attempts complete", "serial", c.spec.Serial, "reason", reason, "connection_generation", generation, "commands", len(c.warmup), "completed", warmupCompleted, "failed", len(c.warmup)-warmupCompleted)
	}
}

// stop ends the supervisor and disconnects the upstream session. Repeated
// calls wait for the same completion, so callers never observe a connection
// that is still tearing down. A never-started supervisor has an
// already-complete shutdown path.
func (c *Conn) stop() {
	c.mu.Lock()
	if !c.stopped {
		c.stopped = true
		c.desired = false
		close(c.stopCh)
	}
	client := c.connecting
	done := c.supDone
	c.mu.Unlock()
	// An in-flight client is disconnected even though IsConnected is false:
	// only the supervisor knows whether it is mid-connect.
	if client != nil {
		client.Disconnect(250)
	}
	if done != nil {
		<-done
	}
}

// connectedLocked reports session state; caller must hold c.mu.
func (c *Conn) connectedLocked() bool {
	return !c.stopped && transportUsable(c.active)
}

// isConnected reports session state.
func (c *Conn) isConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectedLocked()
}

// requestTopic is the command topic for this printer.
func (c *Conn) requestTopic() string {
	return fmt.Sprintf("device/%s/request", c.spec.Serial)
}

// nextBackoff doubles the initial delay per consecutive failure, caps at max,
// and applies ±20% jitter.
func nextBackoff(n int, initial, max time.Duration) time.Duration {
	d := initial
	for i := 1; i < n && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	jittered := time.Duration(float64(d) * (1 + (rand.Float64()*0.4 - 0.2)))
	return jittered
}

// errString renders a nilable error for logs.
func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
