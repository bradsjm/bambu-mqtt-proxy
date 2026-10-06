// One-shot connection probes behind the /config test endpoints. They are
// deliberately separate from the long-lived upstream pool: each probe owns
// its whole lifecycle, connects at most once, and reports only connectivity
// or authentication.
package configui

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"

	"bambu-mqtt-proxy/internal/config"
)

// testConnectTimeout bounds one printer test: dial, MQTT handshake, and the
// whole request. It is deliberately independent of the configured upstream
// connect timeout, so a draft test behaves the same wherever the page is.
const testConnectTimeout = 10 * time.Second

// probePrinterConn performs exactly one MQTT CONNECT to p and nothing else:
// no subscriptions, no publishes, no warmup commands, no retry. The client
// id is unique per attempt, and the endpoint's configured-destination
// rejection (exact configured serial or address) keeps the test away from
// configured printers; brokers that cap total connections may still see
// the one short-lived test connection as a fourth or first client. The
// dialed socket is captured and closed on every path — success, refusal,
// timeout, and cancellation — so a stalled handshake can never leak a
// connection. TLS settings are preserved from the submitted draft,
// including the certificate-check skip.
func probePrinterConn(ctx context.Context, p config.Printer) error {
	scheme := "tcp"
	if p.TLS {
		scheme = "ssl"
	}
	var (
		sockMu  sync.Mutex
		sock    net.Conn
		stopped bool
	)
	client := paho.NewClient(paho.NewClientOptions().
		AddBroker(scheme + "://" + p.Address).
		SetClientID(testClientID()).
		SetUsername(p.Username).
		SetPassword(p.Password).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		// Pin MQTT 3.1.1: without an explicit version paho retries a
		// refused CONNACK with 3.1, which would mean a second CONNECT and
		// socket. One version, one CONNECT, one socket.
		SetProtocolVersion(4).
		SetOrderMatters(true).
		SetConnectTimeout(testConnectTimeout).
		// A real keepalive, because some brokers reject CONNECT with
		// keepalive 0; the socket closes before the first ping is due.
		SetKeepAlive(30 * time.Second).
		SetStore(paho.NewMemoryStore()).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: p.InsecureSkipVerify}).
		SetCustomOpenConnectionFn(func(uri *url.URL, options paho.ClientOptions) (net.Conn, error) {
			conn, err := dialPrinter(ctx, uri, options)
			sockMu.Lock()
			// Cleanup ran while this dial was in flight: the freshly
			// dialed socket must not outlive the probe.
			if stopped {
				sockMu.Unlock()
				if conn != nil {
					_ = conn.Close()
				}
				if cerr := ctx.Err(); cerr != nil {
					return nil, cerr
				}
				return nil, errors.New("probe connection already cleaned up")
			}
			if err == nil {
				sock = conn
			}
			sockMu.Unlock()
			return conn, err
		}))
	// Guaranteed teardown: close the captured socket, which kills any
	// handshake paho still considers in flight, then disconnect the client
	// (a harmless no-op when the CONNECT was never accepted). The stopped
	// flag makes a dial that finishes after this point close its own
	// socket, closing the registration race.
	defer func() {
		sockMu.Lock()
		stopped = true
		conn := sock
		sockMu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		client.Disconnect(0)
	}()

	tok := client.Connect()
	select {
	case <-tok.Done():
		if err := tok.Error(); err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("the test did not complete within %s", testConnectTimeout)
	}
}

// dialPrinter dials the printer with the probe context so cancellation
// beats any dial or handshake. TLS verification policy is preserved from
// the submitted options.
func dialPrinter(ctx context.Context, uri *url.URL, options paho.ClientOptions) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: options.ConnectTimeout}
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
		return (&tls.Dialer{NetDialer: dialer, Config: cfg}).DialContext(ctx, "tcp", uri.Host)
	default:
		return dialer.DialContext(ctx, "tcp", uri.Host)
	}
}

// testClientID returns a unique client id for one test connect. Bambu
// brokers evict an existing connection with the same id, so the test must
// never collide with a configured printer's live connection.
func testClientID() string {
	var b [4]byte
	_, _ = rand.Read(b[:]) // documented never to fail
	return "bmbpx-test-" + hex.EncodeToString(b[:])
}

// classifyProbeError converts a failed MQTT CONNECT into the sanitized
// message the endpoint returns. CONNACK credential refusals become
// authentication failures; everything else — refused dial, timeout, dropped
// handshake — stays a connectivity failure. The messages name only the
// submitted address. Relaying the underlying error is safe by inspection:
// dial and TLS handshake errors carry only the dialed host and port — the
// address the submitter already supplied — and never the access code,
// which travels only inside the MQTT CONNECT payload.
func classifyProbeError(err error, address string) string {
	if errors.Is(err, packets.ErrorRefusedBadUsernameOrPassword) ||
		errors.Is(err, packets.ErrorRefusedNotAuthorised) {
		return fmt.Sprintf("The printer at %s rejected the username or access code.", address)
	}
	return fmt.Sprintf("Could not connect to %s: %s. Check the address, TLS setting, and access code.", address, err)
}
