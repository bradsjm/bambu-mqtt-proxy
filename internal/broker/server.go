package broker

import (
	"crypto/tls"
	"fmt"
	"log/slog"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/routing"
	"bambu-mqtt-proxy/internal/tlsutil"
	"bambu-mqtt-proxy/internal/upstream"
)

// Injector forwards upstream reports into the downstream broker. The mqtt
// server is attached after construction to break the pool/server dependency
// cycle.
type Injector struct {
	srv *mqtt.Server
	log *slog.Logger
}

// NewInjector creates an unattached injector.
func NewInjector(log *slog.Logger) *Injector {
	return &Injector{log: log}
}

// PublishDownstream injects one upstream report into the broker; the broker
// fans it out to every subscribed client. Retain is never set: Bambu reports
// are live pushes and must not survive reconnects.
func (i *Injector) PublishDownstream(topic string, payload []byte) {
	if i.srv == nil {
		return
	}
	if err := i.srv.Publish(topic, payload, false, 0); err != nil {
		i.log.Warn("downstream inject failed", "topic", topic, "error", err)
	}
}

// Server wraps the downstream mochi broker with the bridge hook and TLS
// listeners.
type Server struct {
	srv *mqtt.Server
	log *slog.Logger
}

// New assembles the downstream broker: mochi server, bridge hook, and one
// listener per configured port (TLS with a generated or loaded self-signed
// certificate where enabled).
func New(cfg *config.Config, table *routing.Table, pool *upstream.Pool, inject *Injector, log *slog.Logger) (*Server, error) {
	srv := mqtt.New(&mqtt.Options{InlineClient: true, Logger: log})
	// The printer's broker speaks QoS 0 only; so does the proxy. Mochi
	// grants QoS 0 on every SUBACK and downgrades every PUBLISH to QoS 0.
	srv.Options.Capabilities.MaximumQos = 0

	// fail closes the partially constructed server exactly once and returns
	// its error: a construction failure must release every bound port, and
	// no Server escapes New on this path, so nothing else can close it again.
	fail := func(err error) (*Server, error) {
		_ = srv.Close()
		return nil, err
	}

	if err := srv.AddHook(newBridge(cfg, table, pool, log), nil); err != nil {
		return fail(fmt.Errorf("add bridge hook: %w", err))
	}

	s := &Server{srv: srv, log: log}
	for i, ln := range cfg.Listen {
		lc := listeners.Config{
			Type:    "tcp",
			ID:      fmt.Sprintf("tcp-%d-%d", ln.Port, i),
			Address: fmt.Sprintf(":%d", ln.Port),
		}
		if ln.TLS {
			cert, err := tlsutil.Ensure(ln.CertFile, ln.KeyFile)
			if err != nil {
				return fail(fmt.Errorf("listener %d: %w", i, err))
			}
			lc.TLSConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			}
		}
		if err := srv.AddListener(listeners.NewTCP(lc)); err != nil {
			return fail(fmt.Errorf("add listener %d: %w", i, err))
		}
	}
	// Every listener is bound. Publishing the server into the injector is
	// the last step, so a failed construction never leaves a live target.
	inject.srv = srv
	return s, nil
}

// Serve blocks serving the downstream broker until Close.
func (s *Server) Serve() error {
	return s.srv.Serve()
}

// Close stops the broker and its listeners.
func (s *Server) Close() error {
	return s.srv.Close()
}
