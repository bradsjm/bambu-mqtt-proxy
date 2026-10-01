package integration_test

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"bambu-mqtt-proxy/internal/broker"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/routing"
	"bambu-mqtt-proxy/internal/upstream"
)

// startProxyAuth serves the proxy in-process with the given auth mode. It is
// the mode-parameterized variant of startProxy without the health endpoints,
// which these tests do not exercise.
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
	cfg.Behavior.UpstreamConnectTimeoutSeconds = 2
	cfg.Behavior.UpstreamBackoffInitialSeconds = 1
	cfg.Behavior.UpstreamBackoffMaxSeconds = 2
	if err := cfg.Validate(); err != nil {
		t.Fatalf("proxy config: %v", err)
	}

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
	t.Cleanup(func() {
		pool.Stop()
		_ = srv.Close()
	})
	return &proxy{srv: srv, pool: pool, port: port}
}

// connectResume opens a persistent-session (CleanSession=false) client so a
// reconnect resumes its session. Auto-reconnect is off: tests reconnect with
// fresh client objects.
func connectResume(t *testing.T, p *proxy, id string) *testClient {
	t.Helper()
	box := &msgBox{seen: make(map[string]int)}
	lost := make(chan struct{}, 1)
	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("ssl://127.0.0.1:%d", p.port)).
		SetClientID(id).
		SetUsername("bblp").
		SetPassword(accessCode).
		SetCleanSession(false).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: true}).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false).
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

// mqttString encodes one MQTT string field (big-endian length prefix).
func mqttString(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return b
}

// remainLen encodes an MQTT variable-length remaining-length field.
func remainLen(n int) []byte {
	var out []byte
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			return out
		}
	}
}

// rawWillConnect completes a raw MQTT 3.1.1 CONNECT with a will over TLS and
// returns the connection. Closing the returned connection without DISCONNECT
// makes the broker fire the will, which paho cannot express.
func rawWillConnect(t *testing.T, p *proxy, id, willTopic, willPayload string, willRetain bool) net.Conn {
	t.Helper()
	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", p.port), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("will client %s dial: %v", id, err)
	}
	t.Cleanup(func() { conn.Close() })

	// clean session, QoS 0 will, username, password
	flags := byte(0x02 | 0x04 | 0x80 | 0x40)
	if willRetain {
		flags |= 0x20
	}
	payload := bytes.Join([][]byte{
		mqttString(id),
		mqttString(willTopic),
		mqttString(willPayload),
		mqttString("bblp"),
		mqttString(accessCode),
	}, nil)
	vh := bytes.Join([][]byte{
		mqttString("MQTT"),
		{0x04, flags, 0x00, 0x3C}, // protocol level 4, keepalive 60s
		payload,
	}, nil)
	pkt := append([]byte{0x10}, remainLen(len(vh))...)
	pkt = append(pkt, vh...)

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(pkt); err != nil {
		t.Fatalf("will client %s connect write: %v", id, err)
	}
	ack := make([]byte, 4)
	if _, err := io.ReadFull(conn, ack); err != nil {
		t.Fatalf("will client %s connack: %v", id, err)
	}
	if ack[0] != 0x20 || ack[3] != 0x00 {
		t.Fatalf("will client %s connack = % x, want acceptance", id, ack)
	}
	conn.SetDeadline(time.Time{})
	return conn
}

// TestWillNeverFansOutOrRetains pins the will policy end to end: in both
// auth modes, report- and request-topic wills (retained or not) reach no
// subscriber and nothing is retained, while real printer reports still flow.
func TestWillNeverFansOutOrRetains(t *testing.T) {
	for _, mode := range []string{config.AuthModePrinter, config.AuthModeAcceptAll} {
		t.Run(mode, func(t *testing.T) {
			p1 := newFakePrinter(t, freePort(t))
			p := startProxyAuth(t, mode, []config.Printer{printerSpec(p1.port, serial1)})
			sub := connect(t, p, "sub-"+mode, "bblp", accessCode, false)
			if got := sub.subscribe(t, reportTopic(serial1), 1); got != 0 {
				t.Fatalf("granted qos %#x, want 0 (broker caps at the printer's QoS 0)", got)
			}
			waitFor(t, 5*time.Second, func() bool {
				return p1.rec.subCount(reportTopic(serial1)) >= 1
			})
			// Mochi publishes wills straight to subscribers, bypassing
			// OnPublish, so a leaked request will surfaces here.
			reqSub := connect(t, p, "reqsub-"+mode, "bblp", accessCode, false)
			if got := reqSub.subscribe(t, requestTopic(serial1), 1); got != 0 {
				t.Fatalf("request subscribe granted %#x, want 0 (broker caps at the printer's QoS 0)", got)
			}

			// Retained report-topic will.
			retained := rawWillConnect(t, p, "willer-r-"+mode, reportTopic(serial1), "WILL-RETAINED", true)
			time.Sleep(200 * time.Millisecond)
			retained.Close() // ungraceful: fires the will
			// Request-topic will: a leak would surface as a command.
			req := rawWillConnect(t, p, "willer-q-"+mode, requestTopic(serial1), "WILL-REQ", false)
			time.Sleep(200 * time.Millisecond)
			req.Close()

			time.Sleep(700 * time.Millisecond)
			if sub.box.has(reportTopic(serial1), "WILL-RETAINED") {
				t.Fatal("downstream will reached a subscriber")
			}
			if reqSub.box.has(requestTopic(serial1), "WILL-REQ") {
				t.Fatal("request-topic will reached a request subscriber")
			}
			// Nor may it be forwarded to the printer as a command.
			if n := p1.rec.count(requestTopic(serial1), "WILL-REQ"); n != 0 {
				t.Fatalf("request-topic will reached the printer %d times", n)
			}

			// A later subscriber receives no retained will.
			late := connect(t, p, "late-"+mode, "bblp", accessCode, false)
			if got := late.subscribe(t, reportTopic(serial1), 1); got != 0 {
				t.Fatalf("granted qos %#x, want 0 (broker caps at the printer's QoS 0)", got)
			}
			time.Sleep(300 * time.Millisecond)
			if late.box.has(reportTopic(serial1), "WILL-RETAINED") {
				t.Fatal("retained will was stored and delivered to a later subscriber")
			}

			// A real printer report still reaches subscribers.
			p1.publish(t, reportTopic(serial1), "real-"+mode)
			waitFor(t, 5*time.Second, func() bool {
				return late.box.has(reportTopic(serial1), "real-"+mode)
			})
		})
	}
}

// TestPersistentSessionKeepsInterestAcrossResume pins session resumption: a
// CleanSession=false disconnect keeps the merged upstream interest, and the
// resumed session receives live reports without a new upstream SUBSCRIBE.
// Reports published while the session is offline are dropped, not queued:
// every delivery is QoS 0, like the printer's own broker.
func TestPersistentSessionKeepsInterestAcrossResume(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	filter := reportTopic(serial1)

	c1 := connectResume(t, p, "persist")
	if got := c1.subscribe(t, filter, 1); got != 0 {
		t.Fatalf("granted qos %#x, want 0 (broker caps at the printer's QoS 0)", got)
	}
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.subCount(filter) >= 1
	})
	time.Sleep(300 * time.Millisecond) // let the merge settle
	baseline := p1.rec.subCount(filter)

	// A live report arrives before the disconnect.
	p1.publish(t, filter, "before-offline")
	waitFor(t, 5*time.Second, func() bool {
		return c1.box.has(filter, "before-offline")
	})

	// Graceful persistent disconnect: the interest and owner are retained.
	c1.cl.Disconnect(100)
	time.Sleep(200 * time.Millisecond)

	// While offline, the printer publishes: QoS 0 delivery has no session
	// queue, so the proxy drops the report instead of replaying it.
	p1.publish(t, filter, "dropped-offline")

	// Resuming reconnect: SessionPresent resume and no upstream re-subscribe.
	c2 := connectResume(t, p, "persist")
	time.Sleep(300 * time.Millisecond) // replay window
	if c2.box.has(filter, "dropped-offline") {
		t.Fatal("offline report was queued and replayed; QoS 0 delivery must drop it")
	}
	if got := p1.rec.subCount(filter); got != baseline {
		t.Fatalf("resume changed upstream subscribes: %d -> %d", baseline, got)
	}
	p1.publish(t, filter, "after-resume")
	waitFor(t, 5*time.Second, func() bool {
		return c2.box.has(filter, "after-resume")
	})
}

// TestSameIDTakeoverKeepsSingleReference pins takeover reference counting: a
// persistent takeover adopts the inherited interest without re-subscribing,
// and the successor's explicit unsubscribe releases exactly once.
func TestSameIDTakeoverKeepsSingleReference(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	filter := reportTopic(serial1)

	c1 := connectResume(t, p, "dupe")
	if got := c1.subscribe(t, filter, 1); got != 0 {
		t.Fatalf("granted qos %#x, want 0 (broker caps at the printer's QoS 0)", got)
	}
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.subCount(filter) >= 1
	})
	time.Sleep(300 * time.Millisecond)
	baseline := p1.rec.subCount(filter)

	c2 := connectResume(t, p, "dupe")
	select {
	case <-c1.lost:
	case <-time.After(5 * time.Second):
		t.Fatal("old session with the same client ID was not taken over")
	}
	time.Sleep(500 * time.Millisecond)
	if got := p1.rec.subCount(filter); got != baseline {
		t.Fatalf("takeover changed upstream subscribes: %d -> %d", baseline, got)
	}

	// Exactly one reference: the successor's unsubscribe releases once, and
	// nothing further.
	c2.cl.Unsubscribe(filter)
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.unsubCount(filter) == 1
	})
	time.Sleep(300 * time.Millisecond)
	if got := p1.rec.unsubCount(filter); got != 1 {
		t.Fatalf("takeover session released %d times, want 1", got)
	}
}

// TestCleanReconnectReleasesOnce pins the clean-takeover path: the old
// session's interests are released exactly once despite the racing
// unsubscribe and disconnect hooks, and resubscribing adds exactly one.
func TestCleanReconnectReleasesOnce(t *testing.T) {
	p1 := newFakePrinter(t, freePort(t))
	p := startProxy(t, []config.Printer{printerSpec(p1.port, serial1)})
	filter := reportTopic(serial1)

	c1 := connect(t, p, "cr", "bblp", accessCode, false)
	if got := c1.subscribe(t, filter, 1); got != 0 {
		t.Fatalf("granted qos %#x, want 0 (broker caps at the printer's QoS 0)", got)
	}
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.subCount(filter) >= 1
	})
	time.Sleep(300 * time.Millisecond)
	baseline := p1.rec.subCount(filter)

	// Clean takeover of the same ID: the old interests are released once.
	c2 := connect(t, p, "cr", "bblp", accessCode, false)
	select {
	case <-c1.lost:
	case <-time.After(5 * time.Second):
		t.Fatal("old clean session was not taken over")
	}
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.unsubCount(filter) == 1
	})
	time.Sleep(300 * time.Millisecond)
	if got := p1.rec.unsubCount(filter); got != 1 {
		t.Fatalf("clean takeover released %d times, want 1", got)
	}
	if got := p1.rec.subCount(filter); got != baseline {
		t.Fatalf("clean takeover changed upstream subscribes: %d -> %d", baseline, got)
	}

	// The fresh session subscribes again: exactly one new reference.
	if got := c2.subscribe(t, filter, 1); got != 0 {
		t.Fatalf("granted qos %#x, want 0 (broker caps at the printer's QoS 0)", got)
	}
	waitFor(t, 5*time.Second, func() bool {
		return p1.rec.subCount(filter) == baseline+1
	})
	time.Sleep(300 * time.Millisecond)
	if got := p1.rec.subCount(filter); got != baseline+1 {
		t.Fatalf("resubscribe settled at %d refs, want %d", got, baseline+1)
	}
}
