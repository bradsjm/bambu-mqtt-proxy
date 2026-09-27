// Package upstream manages one persistent MQTT connection per configured
// printer, with the retry lifecycle from DESIGN.md §5.1.
package upstream

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"bambu-mqtt-proxy/internal/config"
)

// Injector publishes upstream reports into the downstream broker, where the
// broker fans them out to subscribed clients.
type Injector interface {
	PublishDownstream(topic string, payload []byte, qos byte)
}

// ObserveFunc receives every upstream report. Seq is the per-connection
// delivery token assigned when the paho message handler was entered; paho
// runs handlers concurrently (SetOrderMatters is false), so the token lets
// observers order merges conservatively. Gen is the upstream connection
// generation captured under the connection lock at the same moment: a
// report delivered on a dying connection can never be stamped with the
// generation of the connection that replaced it.
type ObserveFunc func(serial string, seq, gen uint64, payload []byte)

// ConnectivityObserver receives established and lost upstream connections.
type ConnectivityObserver func(serial string, connected bool, err error)

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
		return p.conn(serial).ensure(timeout)
	}
	return c.ensure(timeout)
}

// Subscribe records downstream interest in filter on the printer's upstream
// connection, subscribing upstream when connected. Each new downstream
// interest on a connected printer also requests the configured warmup, so
// the newest subscriber converges to full state.
func (p *Pool) Subscribe(serial, filter string, qos byte) {
	p.conn(serial).subscribe(filter, qos)
}

// SubscribeAsync records downstream interest without waiting for upstream
// connectivity. It engages the connection supervisor and returns at once;
// onConnect resubscribes the merged filter set from the recorded refs once
// the printer answers. Use it for long-lived internal interests whose first
// delivery may wait for a printer that is offline at startup.
func (p *Pool) SubscribeAsync(serial, filter string, qos byte) {
	p.conn(serial).subscribeAsync(filter, qos)
}

// Unsubscribe removes one downstream interest; the last removal unsubscribes
// upstream.
func (p *Pool) Unsubscribe(serial, filter string) {
	if c, ok := p.existing(serial); ok {
		c.unsubscribe(filter)
	}
}

// RaiseQoS merges a repeated downstream interest's QoS into the stored
// maximum for an existing connection without changing refcounts or
// re-issuing the upstream subscription; reconnects restore the merged set at
// the highest stored request. Unknown connections create no interest.
func (p *Pool) RaiseQoS(serial, filter string, qos byte) {
	if c, ok := p.existing(serial); ok {
		c.raiseQoS(filter, qos)
	}
}

// Publish forwards a client request upstream. Fire-and-forget: when the
// upstream is unavailable the message is dropped and logged, matching the
// behavior of a dropped printer connection.
func (p *Pool) Publish(serial, topic string, payload []byte, qos byte) {
	if c, ok := p.existing(serial); ok {
		c.publish(topic, payload, qos)
		return
	}
	// Never published before: engage the connection lazily but do not block
	// the publish path waiting for it.
	p.conn(serial)
}

// Stop disconnects every upstream connection and ends all supervisors.
func (p *Pool) Stop() {
	p.mu.Lock()
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

// conn returns the connection for serial, creating it on first use.
func (p *Pool) conn(serial string) *Conn {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.conns[serial]
	if !ok {
		c = newConn(p.specs[serial], p.behavior, p.inject, p.observe, p.connectivity, p.log)
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

// PausePrint sends the pause command upstream. It fails unless the printer
// is connected on exactly the generation the caller validated against, so a
// decision made before a reconnect can never act on the new connection.
// QoS 0 and the generation guard together make a replay across a reconnect
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

// pausePayload is the Bambu pause command published to the request topic.
func pausePayload() string {
	return `{"print":{"sequence_id":"0","command":"pause"}}`
}

// speedProfilePayload returns Bambu's print_speed command. The parameter is
// a string on the wire, as required by the printer protocol.
func speedProfilePayload(profile int) string {
	return fmt.Sprintf(`{"print":{"sequence_id":"0","command":"print_speed","param":"%d"}}`, profile)
}

// pause publishes the pause command after re-checking the connection
// generation under the lock, and re-checks the connection again after the
// publish: paho silently drops QoS 0 messages while reconnecting, so a
// connection that changed during the publish means the command may never
// have reached the printer. All rejection paths are errors; nothing is ever
// republished.
func (c *Conn) pause(generation uint64) error {
	return c.command(generation, "pause", pausePayload())
}

// command publishes one guarded QoS 0 print command. The generation and
// connection checks are shared by pause and speed-profile control.
func (c *Conn) command(generation uint64, name, payload string) error {
	c.mu.Lock()
	gen, client, connected := c.generation, c.client, c.connectedLocked()
	c.mu.Unlock()
	if client == nil || !connected {
		return fmt.Errorf("%s %s: upstream is not connected", name, c.spec.Serial)
	}
	if gen != generation {
		return fmt.Errorf("%s %s: connection generation changed (have %d, want %d)",
			name, c.spec.Serial, gen, generation)
	}
	tok := client.Publish(c.requestTopic(), 0, false, []byte(payload))
	if !tok.WaitTimeout(c.connectTO) {
		return fmt.Errorf("%s %s: publish timed out after %s", name, c.spec.Serial, c.connectTO)
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("%s %s: %w", name, c.spec.Serial, err)
	}
	c.mu.Lock()
	genNow, clientNow, connectedNow := c.generation, c.client, c.connectedLocked()
	c.mu.Unlock()
	if clientNow != client || !connectedNow || genNow != gen {
		return fmt.Errorf("%s %s: connection changed during publish", name, c.spec.Serial)
	}
	c.log.Info("printer command published", "serial", c.spec.Serial, "command", name, "generation", gen)
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

// snapshot returns a copy of the merged filter set for resubscription.
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

// Conn is the single upstream MQTT connection for one printer. The supervisor
// retries the initial connect with capped exponential backoff; after the first
// success paho AutoReconnect owns outage recovery (bounded by
// MaxReconnectInterval = backoff max).
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

	stopCh    chan struct{}
	mu        sync.Mutex
	client    mqtt.Client
	subs      *subRefs
	desired   bool
	stopped   bool
	backoffN  int
	nextRetry time.Time
	connCh    chan struct{} // closed (and replaced) on every successful connect
	// generation changes on every connect and loss; the detection engine's
	// pause guard compares it to the generation a decision was validated on.
	generation uint64
	// lastFailureAt records the latest paho failure for reconnect delay logging.
	lastFailureAt time.Time
	// reportSeq hands each observed report its handler-entry order token.
	reportSeq atomic.Uint64
}

// bumpGeneration advances the connection generation under the lock. Every
// connect and loss calls it, so any transition changes the value the
// detection engine validated a decision against.
func (c *Conn) bumpGeneration() {
	c.mu.Lock()
	c.generation++
	c.mu.Unlock()
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
// While the supervisor is in backoff-wait it refuses immediately, per the
// design: subscribe hooks never wait on a scheduled future retry.
func (c *Conn) ensure(timeout time.Duration) bool {
	c.mu.Lock()
	if c.connectedLocked() {
		c.mu.Unlock()
		return true
	}
	if c.stopped {
		c.mu.Unlock()
		return false
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
		return c.isConnected()
	case <-timer.C:
		return c.isConnected()
	case <-c.stopCh:
		return false
	}
}

// supervise retries the initial connect with capped exponential backoff until
// the first success, then hands outage recovery to paho AutoReconnect.
func (c *Conn) supervise() {
	for {
		c.mu.Lock()
		if c.stopped || !c.desired {
			c.mu.Unlock()
			return
		}
		if c.client == nil {
			c.client = c.newClientLocked()
		}
		client := c.client
		attempt := c.backoffN + 1
		c.mu.Unlock()

		tok := client.Connect()
		done := tok.WaitTimeout(c.connectTO + time.Second)
		var connErr error
		if !done {
			connErr = fmt.Errorf("connect timed out after %s", c.connectTO)
		} else {
			connErr = tok.Error()
		}
		ok := done && connErr == nil

		c.mu.Lock()
		if ok {
			c.backoffN = 0
			c.nextRetry = time.Time{}
			c.lastFailureAt = time.Time{}
			close(c.connCh)
			c.connCh = make(chan struct{})
			c.mu.Unlock()
			c.log.Info("upstream connected", "serial", c.spec.Serial, "address", c.spec.Address, "attempt", attempt)
			return
		}
		c.backoffN++
		d := nextBackoff(c.backoffN, c.backoffInit, c.backoffMax)
		c.nextRetry = time.Now().Add(d)
		c.mu.Unlock()
		c.log.Info("upstream backoff scheduled",
			"serial", c.spec.Serial,
			"address", c.spec.Address,
			"state", "BACKOFF",
			"attempt", attempt,
			"retry_in", d.String(),
			"error", errString(connErr))

		select {
		case <-time.After(d):
		case <-c.stopCh:
			return
		}
	}
}

// onConnect is the paho OnConnect callback: re-issue the merged subscription
// set and send the warmup commands so clients converge to full state.
func (c *Conn) onConnect(_ mqtt.Client) {
	c.bumpGeneration()
	if c.connectivity != nil {
		c.connectivity(c.spec.Serial, true, nil)
	}
	c.mu.Lock()
	subs := c.subs.snapshot()
	client := c.client
	c.mu.Unlock()

	restored := 0
	pending := 0
	for f, q := range subs {
		tok := client.Subscribe(f, q, c.onMessage)
		if !tok.WaitTimeout(c.connectTO) {
			// Fire-and-forget: the command was handed to the connection; a
			// slow broker ack may trail the wait budget.
			pending++
			continue
		}
		if err := tok.Error(); err != nil {
			c.log.Warn("upstream resubscribe failed", "serial", c.spec.Serial, "filter", f, "error", errString(tok.Error()))
			continue
		}
		restored++
	}
	if len(subs) > 0 {
		c.log.Info("upstream subscriptions restored",
			"serial", c.spec.Serial,
			"filters", len(subs),
			"restored", restored,
			"pending", pending)
	}
	c.sendWarmup()
}

// sendWarmup publishes the configured warmup commands so the printer pushes
// its full state and every subscriber converges without client action. It is
// a no-op while the upstream is disconnected: onConnect warms up on every
// successful (re)connect, which covers interests recorded during an outage.
func (c *Conn) sendWarmup() {
	c.mu.Lock()
	client, connected := c.client, c.connectedLocked()
	c.mu.Unlock()
	if client == nil || !connected {
		return
	}
	warmupSent := 0
	for _, cmd := range c.warmup {
		tok := client.Publish(c.requestTopic(), 0, false, []byte(cmd))
		if !tok.WaitTimeout(c.connectTO) || tok.Error() != nil {
			c.log.Warn("upstream warmup failed", "serial", c.spec.Serial, "error", errString(tok.Error()))
			continue
		}
		warmupSent++
	}
	if len(c.warmup) > 0 {
		c.log.Info("upstream warmup complete", "serial", c.spec.Serial, "commands", len(c.warmup), "sent", warmupSent)
	}
}

// onLost is the paho ConnectionLost callback.
func (c *Conn) onLost(_ mqtt.Client, err error) {
	c.bumpGeneration()
	if c.connectivity != nil {
		c.connectivity(c.spec.Serial, false, err)
	}
	c.mu.Lock()
	c.lastFailureAt = time.Now()
	c.mu.Unlock()
	c.log.Warn("upstream connection lost",
		"serial", c.spec.Serial,
		"state", "RECONNECTING",
		"backoff_max", c.backoffMax.String(),
		"error", errString(err))
}

// onConnectionNotification logs paho connection attempts and failures,
// including the measured delay between automatic reconnect attempts.
func (c *Conn) onConnectionNotification(_ mqtt.Client, notification mqtt.ConnectionNotification) {
	switch n := notification.(type) {
	case mqtt.ConnectionNotificationConnecting:
		state := "CONNECTING"
		retryAfter := ""
		if n.IsReconnect {
			state = "RECONNECTING"
			c.mu.Lock()
			if !c.lastFailureAt.IsZero() {
				retryAfter = time.Since(c.lastFailureAt).Round(time.Millisecond).String()
			}
			c.mu.Unlock()
		}
		c.log.Info("upstream connection attempt",
			"serial", c.spec.Serial,
			"address", c.spec.Address,
			"state", state,
			"attempt", n.Attempt+1,
			"retry_after", retryAfter)
	case mqtt.ConnectionNotificationFailed:
		c.mu.Lock()
		c.lastFailureAt = time.Now()
		c.mu.Unlock()
		c.log.Info("upstream connection attempt failed",
			"serial", c.spec.Serial,
			"address", c.spec.Address,
			"state", "BACKOFF",
			"error", errString(n.Reason))
	}
}

// onMessage forwards an upstream report into the downstream broker; the broker
// fans it out to every subscribed downstream client.
func (c *Conn) onMessage(_ mqtt.Client, msg mqtt.Message) {
	if c.observe != nil {
		// Assign the ordering token and capture the connection generation
		// at handler entry, before any waiting: entry order approximates
		// wire order under paho's concurrent dispatch, observers drop
		// stragglers from older reports, and the entry-time generation
		// proves the report belongs to the connection it arrived on.
		c.mu.Lock()
		seq := c.reportSeq.Add(1)
		gen := c.generation
		c.mu.Unlock()
		c.observe(c.spec.Serial, seq, gen, msg.Payload())
	}
	qos := msg.Qos()
	if qos > 1 {
		qos = 1
	}
	c.inject.PublishDownstream(msg.Topic(), msg.Payload(), qos)
}

// reportFilter maps a downstream filter to this printer's exact report topic;
// request-only filters are not subscribed upstream.
func (c *Conn) reportFilter(filter string) string {
	if strings.HasSuffix(filter, "/request") {
		return ""
	}
	return fmt.Sprintf("device/%s/report", c.spec.Serial)
}

// subscribe records the filter and subscribes upstream when connected. A new
// downstream interest on a connected printer also requests the configured
// warmup, so late subscribers converge to full state. At a fresh connect the
// restore pass and this path can both issue the SUBSCRIBE and a warmup; the
// printer treats both as idempotent.
func (c *Conn) subscribe(filter string, qos byte) {
	// Upstream subscriptions cover report-leaf filters only. Subscribing to
	// request filters upstream would make the printer broker echo proxied
	// requests back to the proxy and out to downstream subscribers.
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

	if !first {
		// The interest is already merged upstream, but the new subscriber
		// still needs full state. The first-interest path or onConnect
		// warms again once the subscription is ready, covering subscribers
		// registered meanwhile.
		c.sendWarmup()
		return
	}
	if !c.ensure(c.connectTO) {
		// Recorded; paho resubscribes from the merged set in onConnect once
		// the printer returns, and onConnect sends the warmup.
		return
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()

	tok := client.Subscribe(filter, qos, c.onMessage)
	if tok.WaitTimeout(c.connectTO) && tok.Error() == nil {
		c.mu.Lock()
		refs := c.subs.count(filter)
		c.mu.Unlock()
		c.log.Info("upstream subscribed", "serial", c.spec.Serial, "filter", filter, "refs", refs, "qos", qos)
		// Report subscription is ready: request full state for this
		// subscriber. At a fresh connect this can duplicate onConnect's own
		// warmup; repeated pushall is an idempotent state snapshot.
		c.sendWarmup()
		return
	}
	// Recorded; onConnect resubscribes after the next recovery.
	c.log.Warn("upstream subscribe failed", "serial", c.spec.Serial, "filter", filter, "error", errString(tok.Error()))
}

// subscribeAsync records the filter and engages the connection supervisor
// without waiting for it. The recorded interest survives printer outages:
// onConnect resubscribes the whole merged set when the printer returns,
// which is the same path a subscribe that lost the ensure race relies on.
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
	alreadyDesired := c.desired || c.stopped
	if !alreadyDesired {
		c.desired = true
		go c.supervise()
	}
	c.mu.Unlock()
	if !first {
		return
	}
	c.log.Info("upstream interest recorded",
		"serial", c.spec.Serial, "filter", filter, "qos", qos, "mode", "async")
}

// unsubscribe removes one interest; the last removal unsubscribes upstream.
func (c *Conn) unsubscribe(filter string) {
	filter = c.reportFilter(filter)
	if filter == "" {
		return
	}
	c.mu.Lock()
	last := c.subs.remove(filter)
	client, connected := c.client, c.connectedLocked()
	c.mu.Unlock()

	if last && connected {
		tok := client.Unsubscribe(filter)
		if tok.WaitTimeout(c.connectTO) && tok.Error() == nil {
			c.log.Info("upstream unsubscribed", "serial", c.spec.Serial, "filter", filter)
			return
		}
		c.log.Warn("upstream unsubscribe failed", "serial", c.spec.Serial, "filter", filter, "error", errString(tok.Error()))
	}
}

// raiseQoS merges a repeated interest's QoS into the stored maximum without
// changing refcounts, so a reconnect restores the merged set at the highest
// downstream request. Request-only filters have no upstream interest.
func (c *Conn) raiseQoS(filter string, qos byte) {
	filter = c.reportFilter(filter)
	if filter == "" {
		return
	}
	if qos > 1 {
		qos = 1
	}
	c.mu.Lock()
	c.subs.raise(filter, qos)
	c.mu.Unlock()
}

// publish forwards a client request upstream, fire-and-forget.
func (c *Conn) publish(topic string, payload []byte, qos byte) {
	c.mu.Lock()
	client, connected := c.client, c.connectedLocked()
	c.mu.Unlock()
	if client == nil || !connected {
		c.log.Warn("dropping request; upstream unavailable", "serial", c.spec.Serial, "topic", topic)
		return
	}
	if qos > 1 {
		qos = 1
	}
	tok := client.Publish(topic, qos, false, payload)
	if !tok.WaitTimeout(c.connectTO) || tok.Error() != nil {
		c.log.Warn("upstream publish failed", "serial", c.spec.Serial, "topic", topic, "error", errString(tok.Error()))
	}
}

// stop ends the supervisor and disconnects the upstream session.
func (c *Conn) stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	c.desired = false
	client := c.client
	close(c.stopCh)
	c.mu.Unlock()
	if client != nil && client.IsConnected() {
		client.Disconnect(250)
	}
}

// connectedLocked reports session state; caller must hold c.mu.
func (c *Conn) connectedLocked() bool {
	// IsConnectionOpen, not IsConnected: paho reports IsConnected=true while
	// auto-reconnecting, which would wrongly pass availability gates.
	return c.client != nil && c.client.IsConnectionOpen()
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

// newClientLocked builds the paho client; caller must hold c.mu.
func (c *Conn) newClientLocked() mqtt.Client {
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
		SetAutoReconnect(true).
		SetMaxReconnectInterval(c.backoffMax).
		SetConnectTimeout(c.connectTO).
		SetKeepAlive(c.keepalive).
		SetPingTimeout(10 * time.Second).
		SetOrderMatters(false).
		SetOnConnectHandler(c.onConnect).
		SetConnectionNotificationHandler(c.onConnectionNotification).
		SetConnectionLostHandler(c.onLost)
	if c.spec.TLS {
		opts.SetTLSConfig(&tls.Config{InsecureSkipVerify: c.spec.InsecureSkipVerify})
	}
	return mqtt.NewClient(opts)
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
