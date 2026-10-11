// Package integration runs the proxy end-to-end against fake Bambu printers
// built on mochi, asserting the routing and parity contract of the proxy.
package integration_test

import (
	"bytes"
	"context"
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
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"bambu-mqtt-proxy/internal/broker"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/health"
	"bambu-mqtt-proxy/internal/httpsrv"
	"bambu-mqtt-proxy/internal/printer"
	"bambu-mqtt-proxy/internal/routing"
	"bambu-mqtt-proxy/internal/tlsutil"
)

const (
	accessCode = "12345678"
	serial1    = "01P00A000000001"
	serial2    = "01P00A000000002"
)

// recorder captures the fake printer's MQTT traffic and subscription grants.
type recorder struct {
	mochi.HookBase
	mu        sync.Mutex
	seen      map[string]recItem
	subs      map[string]int
	subEvents []subEvent
	refuse    bool
	connects  int
}

// subEvent records one upstream SUBSCRIBE with its requested QoS.
type subEvent struct {
	filter string
	qos    byte
}

// recItem records the count and MQTT flags of a request payload.
type recItem struct {
	count  int
	retain bool
	qos    byte
}

// ID returns the hook ID.
func (r *recorder) ID() string { return "recorder" }

// Provides declares the fake printer's authentication, ACL, and recording hooks.
func (r *recorder) Provides(k byte) bool {
	return bytes.Contains([]byte{mochi.OnConnectAuthenticate, mochi.OnConnect, mochi.OnACLCheck, mochi.OnPublish, mochi.OnSubscribed}, []byte{k})
}

// OnConnectAuthenticate accepts the proxy's fake-printer connection.
func (r *recorder) OnConnectAuthenticate(_ *mochi.Client, _ packets.Packet) bool { return true }

// OnConnect records accepted connection attempts.
func (r *recorder) OnConnect(_ *mochi.Client, _ packets.Packet) error {
	r.mu.Lock()
	r.connects++
	r.mu.Unlock()
	signalObservation()
	return nil
}

// OnACLCheck can refuse report subscriptions without closing the connection.
func (r *recorder) OnACLCheck(_ *mochi.Client, _ string, write bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return write || !r.refuse
}

// OnPublish records the byte-exact request and its flags.
func (r *recorder) OnPublish(_ *mochi.Client, pk packets.Packet) (packets.Packet, error) {
	if strings.HasSuffix(pk.TopicName, "/request") {
		r.mu.Lock()
		key := pk.TopicName + "|" + string(pk.Payload)
		item := r.seen[key]
		item.count++
		item.retain, item.qos = pk.FixedHeader.Retain, pk.FixedHeader.Qos
		r.seen[key] = item
		r.mu.Unlock()
		signalObservation()
	}
	return pk, nil
}

// OnSubscribed records every attempt, including a refused subscription.
func (r *recorder) OnSubscribed(_ *mochi.Client, pk packets.Packet, _ []byte) {
	r.mu.Lock()
	for _, f := range pk.Filters {
		r.subs[f.Filter]++
		r.subEvents = append(r.subEvents, subEvent{filter: f.Filter, qos: f.Qos})
	}
	r.mu.Unlock()
	signalObservation()
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

// fakePrinter is a stand-in Bambu printer: a mochi broker with TLS on a fixed
// port that records requests and can publish reports.
type fakePrinter struct {
	srv      *mochi.Server
	rec      *recorder
	port     int
	stopOnce sync.Once
}

// newFakePrinter starts a fake printer listening on port with TLS.
func newFakePrinter(t *testing.T, port int, refuse ...bool) *fakePrinter {
	t.Helper()
	srv := mochi.New(&mochi.Options{InlineClient: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	// The real printer's ESP32 broker supports nothing above QoS 0: SUBACK
	// grants 0 and inbound publishes are downgraded to 0.
	srv.Options.Capabilities.MaximumQos = 0
	rec := &recorder{seen: make(map[string]recItem), subs: make(map[string]int), refuse: len(refuse) > 0 && refuse[0]}
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
	if err := srv.Serve(); err != nil {
		t.Fatalf("printer serve: %v", err)
	}
	fp := &fakePrinter{srv: srv, rec: rec, port: port}
	t.Cleanup(fp.stop)
	return fp
}

// stop closes the printer broker exactly once (mochi Close is not
// idempotent).
func (f *fakePrinter) stop() {
	f.stopOnce.Do(func() { _ = f.srv.Close() })
}

// publish pushes a report as if from the printer.
func (f *fakePrinter) publish(t *testing.T, topic, payload string) {
	t.Helper()
	if err := f.srv.Publish(topic, []byte(payload), false, 0); err != nil {
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
	srv   *broker.Server
	links *printer.Links
	// state holds reports merged after downstream forwarding.
	state      *printer.Store
	port       int
	logs       *testLogs
	healthPort int
}

// startProxy builds and serves the proxy with always-on printer links.
func startProxy(t *testing.T, printers []config.Printer) *proxy {
	return startProxyAuth(t, config.AuthModePrinter, printers)
}

// startProxyAuth serves a proxy with the selected authentication policy.
func startProxyAuth(t *testing.T, mode string, printers []config.Printer) *proxy {
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
		Auth:     config.Auth{Mode: mode},
		Printers: printers,
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("proxy config: %v", err)
	}
	healthPort := freePort(t)

	logs := &testLogs{seen: make(map[string]int), online: make(map[string][]bool)}
	logger := slog.New(logs)
	serials := make([]string, 0, len(printers))
	aliases := make(map[string]string, len(printers))
	for _, p := range printers {
		serials = append(serials, p.Serial)
		if p.Alias != "" {
			aliases[p.Serial] = p.Alias
		}
	}
	table := routing.NewTable(serials, aliases)
	inject := broker.NewInjector(logger)
	state := printer.NewStore(printers)
	links := printer.New(printers, logger, func(serial string, topic string, payload []byte) {
		inject.PublishDownstream(topic, payload)
		state.Apply(serial, payload)
	}, func(serial string, online bool) {
		state.SetOnline(serial, online)
		logs.mu.Lock()
		logs.online[serial] = append(logs.online[serial], online)
		logs.mu.Unlock()
		signalObservation()
	})
	srv, err := broker.New(cfg, table, links, inject, logger)
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	if err := srv.Serve(); err != nil {
		t.Fatalf("proxy serve: %v", err)
	}
	links.Start()
	httpSrv := httpsrv.New(healthPort, logger)
	health.Routes(httpSrv.Mux(), links, nil)
	if err := httpSrv.Start(); err != nil {
		t.Fatalf("http server: %v", err)
	}
	t.Cleanup(func() {
		httpSrv.Stop()
		links.Close()
		_ = srv.Close()
	})
	return &proxy{srv: srv, links: links, state: state, port: port, healthPort: healthPort, logs: logs}
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
	signalObservation()
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
		SetOrderMatters(true).
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

// observations wakes bounded waits whenever MQTT traffic or link status changes.
var observations = struct {
	sync.Mutex
	changed chan struct{}
}{changed: make(chan struct{})}

// signalObservation broadcasts a newly observed network event.
func signalObservation() {
	observations.Lock()
	close(observations.changed)
	observations.changed = make(chan struct{})
	observations.Unlock()
}

// waitFor waits for observable events until fn is true or the deadline expires.
func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		observations.Lock()
		changed := observations.changed
		observations.Unlock()
		if fn() {
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("condition not met within %s", timeout)
		}
	}
}

// testLogs records lifecycle messages as bounded-wait observations.
type testLogs struct {
	mu     sync.Mutex
	seen   map[string]int
	online map[string][]bool
}

// Enabled enables diagnostic lifecycle observations.
func (*testLogs) Enabled(context.Context, slog.Level) bool { return true }

// Handle records the message without storing credentials or packet contents.
func (l *testLogs) Handle(_ context.Context, record slog.Record) error {
	l.mu.Lock()
	l.seen[record.Message]++
	l.mu.Unlock()
	signalObservation()
	return nil
}

// WithAttrs keeps message-only observations for child loggers.
func (l *testLogs) WithAttrs([]slog.Attr) slog.Handler { return l }

// WithGroup keeps message-only observations for grouped loggers.
func (l *testLogs) WithGroup(string) slog.Handler { return l }

// count reports the observed count of a lifecycle message.
func (l *testLogs) count(message string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seen[message]
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
func warmupPayload() string { return `{"pushing":{"sequence_id":"0","command":"pushall"}}` }

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

func TestRequestForwardedOnceAndNeverFannedOut(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	cA := connect(t, p, "cA", "bblp", accessCode, true)
	cB := connect(t, p, "cB", "bblp", accessCode, true)

	if got := cA.subscribe(t, reportTopic(serial1), 1); got != 0 {
		t.Fatalf("granted qos %#x, want 0 (broker caps at the printer's QoS 0)", got)
	}
	if got := cB.subscribe(t, requestTopic(serial1), 1); got >= 0x80 {
		t.Fatalf("subscribe to request topic granted %#x, want 0 (broker caps at the printer's QoS 0)", got)
	}
	if got := cB.subscribe(t, reportTopic(serial1), 0); got != 0 {
		t.Fatalf("report barrier subscription: %#x", got)
	}

	waitFor(t, 5*time.Second, func() bool { return p.links.Online(serial1) })
	payload := `{"print":{"sequence_id":"7","command":"pause"}}`
	cA.publish(t, requestTopic(serial1), payload, 0, false)

	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.count(requestTopic(serial1), payload) == 1
	})
	// A subsequent report is a downstream delivery barrier.
	p1.publish(t, reportTopic(serial1), "after-request")
	waitFor(t, 5*time.Second, func() bool {
		return cA.box.has(reportTopic(serial1), "after-request") && cB.box.has(reportTopic(serial1), "after-request")
	})
	if cB.box.has(requestTopic(serial1), payload) {
		t.Fatal("request fanned out to another downstream client; must not")
	}
}

func TestRetainStripped(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	c := connect(t, p, "c1", "bblp", accessCode, true)
	if got := c.subscribe(t, reportTopic(serial1), 1); got != 0 {
		t.Fatalf("granted qos %#x, want 0 (broker caps at the printer's QoS 0)", got)
	}
	waitFor(t, 5*time.Second, func() bool { return p.links.Online(serial1) })

	payload := `{"retained":"probe"}`
	c.publish(t, requestTopic(serial1), payload, 0, true)
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
		t.Fatalf("wildcard subscribe granted %#x, want 0 (broker caps at the printer's QoS 0)", got)
	}
	waitFor(t, 5*time.Second, func() bool { return p.links.Online(serial1) && p.links.Online(serial2) })
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
	if got := c1.subscribe(t, reportTopic(serial1), 1); got != 0 {
		t.Fatalf("granted qos %#x, want 0 (broker caps at the printer's QoS 0)", got)
	}

	c2 := connect(t, p, "dupe", "bblp", accessCode, false)
	if got := c2.subscribe(t, reportTopic(serial1), 1); got != 0 {
		t.Fatalf("granted qos %#x, want 0 (broker caps at the printer's QoS 0)", got)
	}

	select {
	case <-c1.lost:
	case <-time.After(5 * time.Second):
		t.Fatal("old session with same client ID was not taken over")
	}
	waitFor(t, 5*time.Second, func() bool { return p.links.Online(serial1) })
	p1.publish(t, reportTopic(serial1), "after-takeover")
	waitFor(t, 5*time.Second, func() bool {
		return c2.box.has(reportTopic(serial1), "after-takeover")
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
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(fmt.Sprintf("http://127.0.0.1:%d%s", p.healthPort, path))
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		if out != nil {
			_ = json.NewDecoder(resp.Body).Decode(out)
		}
		return resp.StatusCode
	}

	// Health remains independent of printer availability.
	if code := decode("/livez", nil); code != 200 {
		t.Fatalf("/livez = %d, want 200", code)
	}
	waitFor(t, 5*time.Second, func() bool { return p.links.Online(serial1) })

	if code := decode("/status", &live); code != 200 || !live.Upstreams[serial1] {
		t.Fatalf("/status = %d %+v, want online printer", code, live)
	}
}
