// Package integration runs the proxy end-to-end against fake Bambu printers
// built on mochi, asserting the routing and parity contract from DESIGN.md §12.
package integration_test

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"bambu-mqtt-proxy/internal/broker"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/health"
	"bambu-mqtt-proxy/internal/httpsrv"
	"bambu-mqtt-proxy/internal/routing"
	"bambu-mqtt-proxy/internal/tlsutil"
	"bambu-mqtt-proxy/internal/upstream"
)

const (
	accessCode = "12345678"
	serial1    = "01P00A000000001"
	serial2    = "01P00A000000002"
)

// recorder is a fake-printer hook that records publishes to request topics
// and the proxy's upstream subscription lifecycle.
type recorder struct {
	mochi.HookBase
	mu        sync.Mutex
	seen      map[string]recItem
	subs      map[string]int
	unsubs    map[string]int
	subEvents []subEvent
	onWarmup  func() // optional full-state response to a warmup pushall
}

// subEvent records one upstream SUBSCRIBE with its requested QoS.
type subEvent struct {
	filter string
	qos    byte
}

// recItem is one recorded request payload.
type recItem struct {
	count  int
	retain bool
}

// ID returns the hook ID.
func (r *recorder) ID() string { return "recorder" }

// Provides declares the implemented hook points.
func (r *recorder) Provides(k byte) bool {
	return bytes.Contains([]byte{mochi.OnPublish, mochi.OnSubscribed, mochi.OnUnsubscribed}, []byte{k})
}

// OnPublish records request-topic publishes and passes them through. A warmup
// pushall additionally triggers the responder, modeling a printer that pushes
// full state only on request.
func (r *recorder) OnPublish(_ *mochi.Client, pk packets.Packet) (packets.Packet, error) {
	if strings.HasSuffix(pk.TopicName, "/request") {
		r.mu.Lock()
		key := pk.TopicName + "|" + string(pk.Payload)
		item := r.seen[key]
		item.count++
		item.retain = pk.FixedHeader.Retain
		r.seen[key] = item
		respond := r.onWarmup
		r.mu.Unlock()
		if string(pk.Payload) == config.DefaultWarmupPushall && respond != nil {
			go respond()
		}
	}
	return pk, nil
}

// OnSubscribed counts the proxy's upstream subscriptions per filter.
func (r *recorder) OnSubscribed(_ *mochi.Client, pk packets.Packet, _ []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range pk.Filters {
		r.subs[f.Filter]++
		r.subEvents = append(r.subEvents, subEvent{filter: f.Filter, qos: f.Qos})
	}
}

// OnUnsubscribed counts the proxy's upstream unsubscribe removals per filter.
func (r *recorder) OnUnsubscribed(_ *mochi.Client, pk packets.Packet) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range pk.Filters {
		r.unsubs[f.Filter]++
	}
}

// count returns how many times topic+payload was recorded.
func (r *recorder) count(topic, payload string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[topic+"|"+payload].count
}

// retainFlag reports the retain bit recorded for topic+payload.
func (r *recorder) retainFlag(topic, payload string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[topic+"|"+payload].retain
}

// subCount reports how often the proxy subscribed filter upstream.
func (r *recorder) subCount(filter string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.subs[filter]
}

// unsubCount reports how often the proxy removed filter upstream.
func (r *recorder) unsubCount(filter string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.unsubs[filter]
}

// lastSubQos reports the QoS of the most recent upstream subscribe for
// filter; ok is false when the proxy never subscribed it.
func (r *recorder) lastSubQos(filter string) (byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.subEvents) - 1; i >= 0; i-- {
		if r.subEvents[i].filter == filter {
			return r.subEvents[i].qos, true
		}
	}
	return 0, false
}

// setWarmupResponder installs the callback fired for every warmup pushall.
func (r *recorder) setWarmupResponder(fn func()) {
	r.mu.Lock()
	r.onWarmup = fn
	r.mu.Unlock()
}

// fakePrinter is a stand-in Bambu printer: a mochi broker with TLS on a fixed
// port that records requests and can publish reports.
type fakePrinter struct {
	srv      *mochi.Server
	rec      *recorder
	port     int
	stopOnce sync.Once
}

// newFakePrinter starts a fake printer listening on port with TLS.
func newFakePrinter(t *testing.T, port int) *fakePrinter {
	t.Helper()
	srv := mochi.New(&mochi.Options{InlineClient: true})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("printer auth hook: %v", err)
	}
	rec := &recorder{
		seen:   make(map[string]recItem),
		subs:   make(map[string]int),
		unsubs: make(map[string]int),
	}
	if err := srv.AddHook(rec, nil); err != nil {
		t.Fatalf("printer recorder hook: %v", err)
	}
	dir := t.TempDir()
	cert, err := tlsutil.Ensure(filepath.Join(dir, "crt"), filepath.Join(dir, "key"))
	if err != nil {
		t.Fatalf("printer cert: %v", err)
	}
	lc := listeners.Config{
		Type:    "tcp",
		ID:      "tcp",
		Address: fmt.Sprintf("127.0.0.1:%d", port),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		},
	}
	if err := srv.AddListener(listeners.NewTCP(lc)); err != nil {
		t.Fatalf("printer listener: %v", err)
	}
	go func() { _ = srv.Serve() }()
	fp := &fakePrinter{srv: srv, rec: rec, port: port}
	t.Cleanup(fp.stop)
	return fp
}

// stop closes the printer broker exactly once (mochi Close is not
// idempotent).
func (f *fakePrinter) stop() {
	f.stopOnce.Do(func() { _ = f.srv.Close() })
}

// respondToWarmup makes the fake printer answer each warmup pushall with a
// full-state report on topic, the way a P1 pushes full state only on pushall.
func (f *fakePrinter) respondToWarmup(topic, payload string) {
	f.rec.setWarmupResponder(func() {
		_ = f.srv.Publish(topic, []byte(payload), false, 1)
	})
}

// publish pushes a report as if from the printer.
func (f *fakePrinter) publish(t *testing.T, topic, payload string) {
	t.Helper()
	if err := f.srv.Publish(topic, []byte(payload), false, 1); err != nil {
		t.Fatalf("printer publish: %v", err)
	}
}

// printerSpec builds the config entry for a fake printer.
func printerSpec(port int, serial string) config.Printer {
	return config.Printer{
		Serial:             serial,
		Address:            fmt.Sprintf("127.0.0.1:%d", port),
		TLS:                true,
		InsecureSkipVerify: true,
		Username:           "bblp",
		Password:           accessCode,
	}
}

// proxy is a running proxy instance for tests.
type proxy struct {
	srv        *broker.Server
	pool       *upstream.Pool
	port       int
	healthPort int
}

// startProxy builds and serves the proxy in-process with fast test timing.
func startProxy(t *testing.T, printers []config.Printer) *proxy {
	t.Helper()
	port := freePort(t)
	dir := t.TempDir()
	cfg := &config.Config{
		Listen: []config.Listener{{
			Port:     port,
			TLS:      true,
			CertFile: filepath.Join(dir, "proxy.crt"),
			KeyFile:  filepath.Join(dir, "proxy.key"),
		}},
		Auth:     config.Auth{Mode: config.AuthModePrinter},
		Printers: printers,
	}
	cfg.ApplyDefaults()
	cfg.Behavior.UpstreamConnectTimeoutSeconds = 2
	cfg.Behavior.UpstreamBackoffInitialSeconds = 1
	cfg.Behavior.UpstreamBackoffMaxSeconds = 2
	if err := cfg.Validate(); err != nil {
		t.Fatalf("proxy config: %v", err)
	}
	healthPort := freePort(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	serials := make([]string, 0, len(printers))
	for _, p := range printers {
		serials = append(serials, p.Serial)
	}
	table := routing.NewTable(serials)
	inject := broker.NewInjector(logger)
	pool := upstream.NewPool(printers, inject, cfg.Behavior, logger)
	srv, err := broker.New(cfg, table, pool, inject, logger)
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	go func() { _ = srv.Serve() }()
	httpSrv := httpsrv.New(healthPort, logger)
	health.Routes(httpSrv.Mux(), pool, nil)
	if err := httpSrv.Start(); err != nil {
		t.Fatalf("http server: %v", err)
	}
	t.Cleanup(func() {
		httpSrv.Stop()
		pool.Stop()
		_ = srv.Close()
	})
	return &proxy{srv: srv, pool: pool, port: port, healthPort: healthPort}
}

// testClient is a downstream MQTT client plus captured messages.
type testClient struct {
	cl   mqtt.Client
	box  *msgBox
	lost chan struct{}
}

// msgBox captures received topic|payload strings.
type msgBox struct {
	mu   sync.Mutex
	seen map[string]int
}

// record stores one message.
func (b *msgBox) record(topic string, payload []byte) {
	b.mu.Lock()
	b.seen[topic+"|"+string(payload)]++
	b.mu.Unlock()
}

// has reports whether topic|payload was received.
func (b *msgBox) has(topic, payload string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.seen[topic+"|"+payload] > 0
}

// connect opens a TLS client to the proxy; user/pass default to printer auth.
func connect(t *testing.T, p *proxy, id, user, pass string, autoReconnect bool) *testClient {
	t.Helper()
	box := &msgBox{seen: make(map[string]int)}
	lost := make(chan struct{}, 1)
	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("ssl://127.0.0.1:%d", p.port)).
		SetClientID(id).
		SetUsername(user).
		SetPassword(pass).
		SetCleanSession(true).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: true}).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(autoReconnect).
		SetOrderMatters(false).
		SetConnectionLostHandler(func(mqtt.Client, error) {
			select {
			case lost <- struct{}{}:
			default:
			}
		}).
		SetDefaultPublishHandler(func(_ mqtt.Client, m mqtt.Message) {
			box.record(m.Topic(), m.Payload())
		})
	cl := mqtt.NewClient(opts)
	tok := cl.Connect()
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("client %s connect: %v", id, tok.Error())
	}
	t.Cleanup(func() { cl.Disconnect(100) })
	return &testClient{cl: cl, box: box, lost: lost}
}

// subscribe subscribes and returns the granted code (0x80 means denied).
func (c *testClient) subscribe(t *testing.T, filter string, qos byte) byte {
	t.Helper()
	tok := c.cl.Subscribe(filter, qos, nil)
	if !tok.WaitTimeout(10 * time.Second) {
		t.Fatalf("subscribe %s timed out", filter)
	}
	res, ok := tok.(*mqtt.SubscribeToken)
	if !ok {
		t.Fatalf("subscribe %s: unexpected token type", filter)
	}
	return res.Result()[filter]
}

// publish sends a message from the client.
func (c *testClient) publish(t *testing.T, topic, payload string, qos byte, retain bool) {
	t.Helper()
	tok := c.cl.Publish(topic, qos, retain, []byte(payload))
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish %s: %v", topic, tok.Error())
	}
}

// waitFor polls fn until true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// freePort reserves an ephemeral port for the test.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// warmupPayload is the pushall command the proxy sends upstream.
func warmupPayload() string { return config.DefaultWarmupPushall }

// requestTopic builds the command topic for a serial.
func requestTopic(serial string) string { return fmt.Sprintf("device/%s/request", serial) }

// reportTopic builds the report topic for a serial.
func reportTopic(serial string) string { return fmt.Sprintf("device/%s/report", serial) }

func TestAuthWrongPasswordRejected(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})

	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("ssl://127.0.0.1:%d", p.port)).
		SetClientID("bad").
		SetUsername("bblp").
		SetPassword("00000000").
		SetCleanSession(true).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: true}).
		SetConnectTimeout(5 * time.Second)
	cl := mqtt.NewClient(opts)
	tok := cl.Connect()
	_ = tok.WaitTimeout(5 * time.Second)
	if tok.Error() == nil {
		t.Fatal("connect with wrong access code should be refused")
	}
}

func TestSubscribeUnknownSerialDenied(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	c := connect(t, p, "c1", "bblp", accessCode, true)

	if got := c.subscribe(t, "device/00UNKNOWN/report", 1); got != 0x80 {
		t.Fatalf("unknown serial granted code %#x, want 0x80", got)
	}
}

func TestMergedSubscriptionWarmupAndFanout(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	cA := connect(t, p, "cA", "bblp", accessCode, true)
	cB := connect(t, p, "cB", "bblp", accessCode, true)

	if got := cA.subscribe(t, reportTopic(serial1), 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}
	if got := cB.subscribe(t, reportTopic(serial1), 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}

	// The upstream connect warms at least once, and the second subscriber
	// joining an already-connected printer warms again. At a fresh connect
	// the restore pass and the first interest can both subscribe and warm;
	// those wire duplicates are idempotent, so only lower bounds hold: both
	// interests share one merged upstream subscription.
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.count(requestTopic(serial1), warmupPayload()) >= 2
	})
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.subCount(reportTopic(serial1)) >= 1
	})

	p1.publish(t, reportTopic(serial1), "state-v1")
	waitFor(t, 5*time.Second, func() bool {
		return cA.box.has(reportTopic(serial1), "state-v1") && cB.box.has(reportTopic(serial1), "state-v1")
	})
}

// TestRepeatedSubscribeDoesNotLeakRefcount pins refcount neutrality: a
// repeated SUBSCRIBE for a held filter adds no upstream interest and
// re-requests no warmup, and the client's disconnect removes the single
// interest instead of leaking a refcount.
func TestRepeatedSubscribeDoesNotLeakRefcount(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	c := connect(t, p, "c1", "bblp", accessCode, true)
	filter := reportTopic(serial1)

	if got := c.subscribe(t, filter, 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}
	// Let the connect-time subscribe and warmup burst settle, then record
	// the baseline (a fresh connect may subscribe and warm idempotently
	// more than once).
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.subCount(filter) >= 1 &&
			p1.rec.count(requestTopic(serial1), warmupPayload()) >= 1
	})
	time.Sleep(300 * time.Millisecond)
	baselineSubs := p1.rec.subCount(filter)
	baselineWarmups := p1.rec.count(requestTopic(serial1), warmupPayload())

	// The same client subscribes to the same filter again.
	if got := c.subscribe(t, filter, 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}
	time.Sleep(500 * time.Millisecond) // duplicate-merge window
	if got := p1.rec.subCount(filter); got != baselineSubs {
		t.Fatalf("repeated subscribe changed upstream subscriptions to %d, baseline %d", got, baselineSubs)
	}
	if got := p1.rec.count(requestTopic(serial1), warmupPayload()); got != baselineWarmups {
		t.Fatalf("repeated subscribe changed warmups to %d, baseline %d", got, baselineWarmups)
	}

	// Disconnect releases the interest: the proxy unsubscribes upstream.
	c.cl.Disconnect(100)
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.unsubCount(filter) == 1
	})
}

// TestPublishOnlyReachesPrinterOnceWithNoReportInterest pins the publish-only
// path: with no downstream report interest the first request engages the
// supervisor, reaches the printer exactly once, and no upstream subscription
// is ever created.
func TestPublishOnlyReachesPrinterOnceWithNoReportInterest(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	c := connect(t, p, "c1", "bblp", accessCode, true)

	payload := `{"print":{"sequence_id":"1","command":"pause"}}`
	c.publish(t, requestTopic(serial1), payload, 1, false)
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.count(requestTopic(serial1), payload) == 1
	})
	time.Sleep(300 * time.Millisecond) // no duplicate delivery window
	if got := p1.rec.count(requestTopic(serial1), payload); got != 1 {
		t.Fatalf("printer received %d copies of the publish-only request, want exactly 1", got)
	}
	if got := p1.rec.subCount(reportTopic(serial1)); got != 0 {
		t.Fatalf("upstream report subscriptions with no report interest = %d, want 0", got)
	}
}

// TestOfflineRequestNotReplayed pins the drop-don't-queue contract: a request
// published while the printer is unreachable is dropped and never reaches the
// printer, not at the first connection and not after an outage recovery.
func TestOfflineRequestNotReplayed(t *testing.T) {
	port := freePort(t) // nothing listens yet: the printer is offline
	p := startProxy(t, []config.Printer{printerSpec(port, serial1)})
	c := connect(t, p, "c1", "bblp", accessCode, true)

	offline := `{"print":{"sequence_id":"2","command":"resume"}}`
	c.publish(t, requestTopic(serial1), offline, 1, false)

	p1 := newFakePrinter(t, port)
	online := `{"print":{"sequence_id":"3","command":"resume"}}`
	c.publish(t, requestTopic(serial1), online, 1, false)
	waitFor(t, 10*time.Second, func() bool {
		return p1.rec.count(requestTopic(serial1), online) == 1
	})
	time.Sleep(300 * time.Millisecond) // replay window after the connection is live
	if got := p1.rec.count(requestTopic(serial1), offline); got != 0 {
		t.Fatalf("offline request was replayed %d times after the first connect, want 0", got)
	}

	// The same contract across an outage: a request published while the
	// printer is down is dropped, and recovery does not replay it.
	p1.stop()
	offline2 := `{"print":{"sequence_id":"4","command":"resume"}}`
	c.publish(t, requestTopic(serial1), offline2, 1, false)
	p2 := newFakePrinter(t, port)
	online2 := `{"print":{"sequence_id":"5","command":"resume"}}`
	c.publish(t, requestTopic(serial1), online2, 1, false)
	waitFor(t, 15*time.Second, func() bool {
		return p2.rec.count(requestTopic(serial1), online2) == 1
	})
	time.Sleep(300 * time.Millisecond)
	if got := p2.rec.count(requestTopic(serial1), offline2); got != 0 {
		t.Fatalf("outage request was replayed %d times after recovery, want 0", got)
	}
}

// TestSubscribeUpgradeQoS0ToQoS1 pins the upgrade contract: the first QoS 0
// report interest emits one upstream SUBSCRIBE, the same client re-subscribing
// at QoS 1 emits exactly one upgrade SUBSCRIBE without another reference or
// warmup, reports still flow, and the single reference unsubscribes once.
func TestSubscribeUpgradeQoS0ToQoS1(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	c := connect(t, p, "c1", "bblp", accessCode, true)
	filter := reportTopic(serial1)

	if got := c.subscribe(t, filter, 0); got != 0 {
		t.Fatalf("granted qos %#x, want 0", got)
	}
	waitFor(t, 5*time.Second, func() bool {
		qos, ok := p1.rec.lastSubQos(filter)
		return p1.rec.subCount(filter) == 1 && ok && qos == 0
	})
	// Let the connect-time restore warmup burst settle (the restore pass and
	// the first interest can both warm idempotently), then take the baseline.
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.count(requestTopic(serial1), warmupPayload()) >= 1
	})
	time.Sleep(300 * time.Millisecond)
	baselineWarmups := p1.rec.count(requestTopic(serial1), warmupPayload())

	// The same client raises its own interest to QoS 1: one upgrade
	// SUBSCRIBE, no new reference, no warmup.
	if got := c.subscribe(t, filter, 1); got != 1 {
		t.Fatalf("granted qos %#x, want 1", got)
	}
	waitFor(t, 5*time.Second, func() bool {
		qos, ok := p1.rec.lastSubQos(filter)
		return p1.rec.subCount(filter) == 2 && ok && qos == 1
	})
	time.Sleep(300 * time.Millisecond) // duplicate-subscribe window
	if got := p1.rec.subCount(filter); got != 2 {
		t.Fatalf("upstream subscribes settled at %d, want 2 (initial plus upgrade)", got)
	}
	if got := p1.rec.count(requestTopic(serial1), warmupPayload()); got != baselineWarmups {
		t.Fatalf("upgrade changed warmups to %d, baseline %d", got, baselineWarmups)
	}

	p1.publish(t, filter, "after-upgrade")
	waitFor(t, 5*time.Second, func() bool {
		return c.box.has(filter, "after-upgrade")
	})

	c.cl.Disconnect(100)
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.unsubCount(filter) == 1
	})
}

// TestLateSubscriberReceivesFullState models P1 pushall semantics: the fake
// printer answers each warmup with the full-state report. A subscriber that
// joins late on an already-connected printer must trigger its own warmup and
// receive full state.
func TestLateSubscriberReceivesFullState(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p1.respondToWarmup(reportTopic(serial1), "full-state-v1")
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})

	cA := connect(t, p, "cA", "bblp", accessCode, true)
	if got := cA.subscribe(t, reportTopic(serial1), 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}
	waitFor(t, 5*time.Second, func() bool {
		return cA.box.has(reportTopic(serial1), "full-state-v1")
	})
	baselineWarmups := p1.rec.count(requestTopic(serial1), warmupPayload())

	// The late joiner missed the first full state: its new interest on the
	// connected printer requests warmup once the report subscription is
	// ready, and the printer's full-state response reaches it.
	cB := connect(t, p, "cB", "bblp", accessCode, true)
	if got := cB.subscribe(t, reportTopic(serial1), 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}
	waitFor(t, 5*time.Second, func() bool {
		return cB.box.has(reportTopic(serial1), "full-state-v1")
	})
	// The late joiner's own interest requested the warmup that delivered
	// its full state.
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.count(requestTopic(serial1), warmupPayload()) > baselineWarmups
	})
}

func TestRequestForwardedOnceAndNeverFannedOut(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	cA := connect(t, p, "cA", "bblp", accessCode, true)
	cB := connect(t, p, "cB", "bblp", accessCode, true)

	if got := cA.subscribe(t, reportTopic(serial1), 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}
	if got := cB.subscribe(t, requestTopic(serial1), 1); got >= 0x80 {
		t.Fatalf("subscribe to request topic denied: %#x", got)
	}

	payload := `{"print":{"sequence_id":"7","command":"pause"}}`
	cA.publish(t, requestTopic(serial1), payload, 1, false)

	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.count(requestTopic(serial1), payload) == 1
	})
	time.Sleep(500 * time.Millisecond) // no duplicate delivery window
	if got := p1.rec.count(requestTopic(serial1), payload); got != 1 {
		t.Fatalf("printer received %d copies of request, want exactly 1", got)
	}
	if cB.box.has(requestTopic(serial1), payload) {
		t.Fatal("request fanned out to another downstream client; must not")
	}
}

func TestRetainStripped(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	c := connect(t, p, "c1", "bblp", accessCode, true)
	if got := c.subscribe(t, reportTopic(serial1), 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}

	payload := `{"retained":"probe"}`
	c.publish(t, requestTopic(serial1), payload, 1, true)
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.count(requestTopic(serial1), payload) == 1
	})
	if p1.rec.retainFlag(requestTopic(serial1), payload) {
		t.Fatal("retain bit reached the printer; must be stripped")
	}
}

func TestWildcardFansBothPrinters(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p2 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1), printerSpec(p2.port, serial2)})
	c := connect(t, p, "c1", "bblp", accessCode, true)

	if got := c.subscribe(t, "device/+/report", 1); got >= 0x80 {
		t.Fatalf("wildcard subscribe denied: %#x", got)
	}
	p1.publish(t, reportTopic(serial1), "r1")
	p2.publish(t, reportTopic(serial2), "r2")
	waitFor(t, 5*time.Second, func() bool {
		return c.box.has(reportTopic(serial1), "r1") && c.box.has(reportTopic(serial2), "r2")
	})
}

func TestPublishToReportDenied(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	c := connect(t, p, "c1", "bblp", accessCode, true)

	payload := `{"fake":"report"}`
	tok := c.cl.Publish(reportTopic(serial1), 1, false, []byte(payload))
	_ = tok.WaitTimeout(5 * time.Second)

	select {
	case <-c.lost:
	case <-time.After(5 * time.Second):
		t.Fatal("client publishing to /report was not disconnected")
	}
	if p1.rec.count(reportTopic(serial1), payload) != 0 {
		t.Fatal("fake report reached the fake printer downstream capture")
	}
}

func TestSameIDTakeover(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	c1 := connect(t, p, "dupe", "bblp", accessCode, false)
	if got := c1.subscribe(t, reportTopic(serial1), 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}

	c2 := connect(t, p, "dupe", "bblp", accessCode, false)
	if got := c2.subscribe(t, reportTopic(serial1), 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}

	select {
	case <-c1.lost:
	case <-time.After(5 * time.Second):
		t.Fatal("old session with same client ID was not taken over")
	}
	p1.publish(t, reportTopic(serial1), "after-takeover")
	waitFor(t, 5*time.Second, func() bool {
		return c2.box.has(reportTopic(serial1), "after-takeover")
	})
}

func TestOutageLifecycle(t *testing.T) {
	port := freePort(t)
	p1 := newFakePrinter(t, port)
	p := startProxy(t, []config.Printer{printerSpec(port, serial1)})
	c := connect(t, p, "c1", "bblp", accessCode, true)

	if got := c.subscribe(t, reportTopic(serial1), 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}
	// The connect warms at least once; the restore pass and the first
	// interest can both warm idempotently, so only the lower bound holds.
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.count(requestTopic(serial1), warmupPayload()) >= 1
	})
	p1.publish(t, reportTopic(serial1), "before-outage")
	waitFor(t, 5*time.Second, func() bool {
		return c.box.has(reportTopic(serial1), "before-outage")
	})

	// Printer goes offline. Wildcard subscribes must be refused (gate), the
	// exact subscriber stays registered locally.
	p1.stop()
	time.Sleep(300 * time.Millisecond)

	guest := connect(t, p, "guest", "bblp", accessCode, true)
	if got := guest.subscribe(t, "device/+/report", 1); got != 0x80 {
		t.Fatalf("wildcard subscribe during outage granted %#x, want 0x80", got)
	}

	// Printer returns on the same port: proxy reconnects, resubscribes the
	// merged set, sends warmup — no client action.
	p2 := newFakePrinter(t, port)
	waitFor(t, 10*time.Second, func() bool {
		return p2.rec.count(requestTopic(serial1), warmupPayload()) >= 1
	})
	p2.publish(t, reportTopic(serial1), "after-recovery")
	waitFor(t, 5*time.Second, func() bool {
		return c.box.has(reportTopic(serial1), "after-recovery")
	})
}

func TestHealthEndpoints(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})

	var live struct {
		Status    string          `json:"status"`
		Upstreams map[string]bool `json:"upstreams"`
	}
	decode := func(path string, out any) int {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", p.healthPort, path))
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		if out != nil {
			_ = json.NewDecoder(resp.Body).Decode(out)
		}
		return resp.StatusCode
	}

	// Before any subscriber: process healthy, upstream not yet engaged.
	if code := decode("/livez", nil); code != 200 {
		t.Fatalf("/livez = %d, want 200", code)
	}
	if code := decode("/status", &live); code != 200 || live.Upstreams[serial1] {
		t.Fatalf("/status before subscribe = %d %+v, want 200 with unengaged upstream", code, live)
	}

	c := connect(t, p, "c1", "bblp", accessCode, true)
	if got := c.subscribe(t, reportTopic(serial1), 1); got > 1 {
		t.Fatalf("granted qos %#x, want <= 1", got)
	}
	p1.publish(t, reportTopic(serial1), "health-probe")
	waitFor(t, 5*time.Second, func() bool {
		return c.box.has(reportTopic(serial1), "health-probe")
	})
	waitFor(t, 5*time.Second, func() bool {
		live.Upstreams = nil
		return decode("/status", &live) == 200 && live.Upstreams[serial1]
	})
}
