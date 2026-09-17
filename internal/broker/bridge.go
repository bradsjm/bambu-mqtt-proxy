// Package broker assembles the downstream mochi broker with TLS listeners and
// the serial-routing bridge hooks.
package broker

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/routing"
	"bambu-mqtt-proxy/internal/upstream"
)

// Bridge is the mochi hook coupling the downstream broker to the upstream
// pool: it authenticates clients, enforces the topic ACL, gates subscriptions
// on upstream availability, merges subscriptions upstream, and forwards
// requests. It never rewrites payloads.
type Bridge struct {
	mqtt.HookBase
	pool        *upstream.Pool
	table       *routing.Table
	authMode    string
	accessCodes map[string]struct{}
	gateTimeout time.Duration
	log         *slog.Logger

	mu      sync.Mutex
	clients map[*mqtt.Client]map[string]struct{} // client pointer -> granted filters
}

// newBridge builds the routing bridge hook.
func newBridge(cfg *config.Config, table *routing.Table, pool *upstream.Pool, log *slog.Logger) *Bridge {
	codes := make(map[string]struct{}, len(cfg.Printers))
	for _, p := range cfg.Printers {
		codes[p.Password] = struct{}{}
	}
	return &Bridge{
		pool:        pool,
		table:       table,
		authMode:    cfg.Auth.Mode,
		accessCodes: codes,
		gateTimeout: time.Duration(cfg.Behavior.UpstreamConnectTimeoutSeconds) * time.Second,
		log:         log,
		clients:     make(map[*mqtt.Client]map[string]struct{}),
	}
}

// ID returns the hook ID.
func (b *Bridge) ID() string {
	return "bambu-bridge"
}

// Provides lists the hook points implemented here.
func (b *Bridge) Provides(k byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnectAuthenticate,
		mqtt.OnACLCheck,
		mqtt.OnConnect,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribed,
		mqtt.OnDisconnect,
		mqtt.OnPublish,
	}, []byte{k})
}

// OnConnectAuthenticate enforces the downstream auth mode: printer mode
// requires username bblp plus a password matching any configured access code.
func (b *Bridge) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	if b.authMode == config.AuthModeAcceptAll {
		return true
	}
	if string(pk.Connect.Username) != "bblp" {
		return false
	}
	_, ok := b.accessCodes[string(pk.Connect.Password)]
	return ok
}

// OnConnect logs downstream client connections for state visibility.
func (b *Bridge) OnConnect(cl *mqtt.Client, _ packets.Packet) error {
	if cl.Net.Inline {
		return nil
	}
	b.log.Info("client connected", "client", cl.ID, "remote", cl.Net.Remote)
	return nil
}

// OnACLCheck enforces the topic contract. Writes must be
// device/{knownSerial}/request. Reads must be a device report/request filter
// over configured printers. Subscribe filters containing wildcards additionally
// gate on upstream availability (SUBACK 0x80 while a targeted printer is
// unreachable); exact-serial subscribes are accepted during outages and
// backfilled by the reconnect warmup. The same read path also runs per
// delivery, where it is a fast grammar check.
func (b *Bridge) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	if write {
		return b.table.AllowedPublish(topic)
	}
	if !b.table.AllowedSubscribe(topic) {
		return false
	}
	if !strings.ContainsAny(topic, "+#") {
		return true
	}
	deadline := time.Now().Add(b.gateTimeout)
	for _, serial := range b.table.PrintersFor(topic) {
		if !b.pool.EnsureConnected(serial, time.Until(deadline)) {
			return false
		}
	}
	return true
}

// OnSubscribed records the client's granted filters and merges the
// subscription upstream (refcounted per printer and filter).
func (b *Bridge) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	if cl.Net.Inline {
		return
	}
	for i, sub := range pk.Filters {
		if i >= len(reasonCodes) || reasonCodes[i] >= 0x80 {
			continue
		}
		printers := b.table.PrintersFor(sub.Filter)
		b.recordFilter(cl, sub.Filter, true)
		for _, serial := range printers {
			b.pool.Subscribe(serial, sub.Filter, sub.Qos)
		}
		b.log.Info("client subscribed",
			"client", cl.ID,
			"filter", sub.Filter,
			"printers", strings.Join(printers, ","),
			"qos", sub.Qos)
	}
}

// OnUnsubscribed removes the client's interest in the filters.
func (b *Bridge) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	if cl.Net.Inline {
		return
	}
	for _, sub := range pk.Filters {
		if b.recordFilter(cl, sub.Filter, false) {
			printers := b.table.PrintersFor(sub.Filter)
			for _, serial := range printers {
				b.pool.Unsubscribe(serial, sub.Filter)
			}
			b.log.Info("client unsubscribed", "client", cl.ID, "filter", sub.Filter, "printers", strings.Join(printers, ","))
		}
	}
}

// OnDisconnect reconciles refcounts for the dropped client, covering
// ungraceful disconnects that never send UNSUBSCRIBE.
func (b *Bridge) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	if cl.Net.Inline {
		return
	}
	b.mu.Lock()
	filters, ok := b.clients[cl]
	delete(b.clients, cl)
	b.mu.Unlock()
	b.log.Info("client disconnected", "client", cl.ID, "reason", cleanErr(err), "filters", len(filters))
	if !ok {
		return
	}
	for filter := range filters {
		for _, serial := range b.table.PrintersFor(filter) {
			b.pool.Unsubscribe(serial, filter)
		}
	}
}

// OnPublish forwards a client request to the owning printer's upstream
// connection and suppresses local fan-out via CodeSuccessIgnore, which keeps
// the QoS flow (PUBACK) intact so clients never retransmit into the proxy.
// The inline-client guard is essential: injected upstream reports re-enter
// this hook, and forwarding them again would loop.
func (b *Bridge) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline {
		return pk, nil
	}
	serials := b.table.PrintersFor(pk.TopicName)
	if len(serials) == 0 {
		return pk, packets.CodeSuccessIgnore
	}
	b.log.Debug("request forwarded", "client", cl.ID, "serial", serials[0], "bytes", len(pk.Payload))
	b.pool.Publish(serials[0], pk.TopicName, pk.Payload, pk.FixedHeader.Qos)
	return pk, packets.CodeSuccessIgnore
}

// cleanErr renders a disconnect reason without a nil error string.
func cleanErr(err error) string {
	if err == nil {
		return "clean"
	}
	return err.Error()
}

// recordFilter adds or removes filter in the client's granted set; it reports
// whether the set changed for the caller's refcount action.
func (b *Bridge) recordFilter(cl *mqtt.Client, filter string, add bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	set, ok := b.clients[cl]
	if !ok {
		set = make(map[string]struct{})
		b.clients[cl] = set
	}
	if add {
		_, have := set[filter]
		set[filter] = struct{}{}
		return !have
	}
	_, have := set[filter]
	delete(set, filter)
	if len(set) == 0 {
		delete(b.clients, cl)
	}
	return have
}
