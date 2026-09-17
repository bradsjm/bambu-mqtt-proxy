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
func (i *Injector) PublishDownstream(topic string, payload []byte, qos byte) {
	if i.srv == nil {
		return
	}
	if err := i.srv.Publish(topic, payload, false, qos); err != nil {
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
	srv.Options.Capabilities.MaximumQos = cfg.Behavior.QoSMax

	if err := srv.AddHook(newBridge(cfg, table, pool, log), nil); err != nil {
		return nil, fmt.Errorf("add bridge hook: %w", err)
	}
	inject.srv = srv

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
				return nil, fmt.Errorf("listener %d: %w", i, err)
			}
			lc.TLSConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			}
		}
		if err := srv.AddListener(listeners.NewTCP(lc)); err != nil {
			return nil, fmt.Errorf("add listener %d: %w", i, err)
		}
	}
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
