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

// poolHooks is the upstream surface the bridge needs. *upstream.Pool
// satisfies it; tests substitute a recording fake.
type poolHooks interface {
	EnsureConnected(serial string, timeout time.Duration) bool
	RecordSubscribe(serial, filter string) func()
	RecordUnsubscribe(serial, filter string) func()
	PublishWithContext(serial, topic string, payload []byte, publishContext upstream.PublishContext)
}

// sessionInterest records one client ID's merged subscriptions. The owner is
// the last connected client with that ID; a persistent disconnect leaves the
// entry in place so a resuming session keeps its upstream interests without
// re-subscribing. Every release is guarded by owner identity, so late hooks
// from a replaced connection can never release a successor's interests.
type sessionInterest struct {
	owner   *mqtt.Client
	filters map[string]struct{}
}

// Bridge is the mochi hook coupling the downstream broker to the upstream
// pool: it authenticates clients, enforces the topic ACL, suppresses printer
// wills, gates subscriptions on upstream availability, merges subscriptions
// upstream, and forwards requests. It never rewrites payloads.
type Bridge struct {
	mqtt.HookBase
	pool        poolHooks
	table       *routing.Table
	authMode    string
	accessCodes map[string]struct{}
	gateTimeout time.Duration
	log         *slog.Logger

	mu       sync.Mutex
	sessions map[string]*sessionInterest // client ID -> owned filters
}

// newBridge builds the routing bridge hook. *upstream.Pool satisfies
// poolHooks.
func newBridge(cfg *config.Config, table *routing.Table, pool poolHooks, log *slog.Logger) *Bridge {
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
		sessions:    make(map[string]*sessionInterest),
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
		mqtt.OnSessionEstablished,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribed,
		mqtt.OnDisconnect,
		mqtt.OnClientExpired,
		mqtt.OnWill,
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

// OnWill suppresses every downstream client's last-will message: a printer
// will must not fan out to subscribers or become retained, including
// request-topic wills that would otherwise surface as commands. Inline
// clients (upstream report injection) keep their wills. It never returns an
// error: mochi keeps the original will when a hook errors.
func (b *Bridge) OnWill(cl *mqtt.Client, will mqtt.Will) (mqtt.Will, error) {
	if cl.Net.Inline {
		return will, nil
	}
	b.log.Info("will suppressed", "client", cl.ID, "topic", will.TopicName)
	return mqtt.Will{}, nil
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

// OnSessionEstablished reconciles the client ID's retained interests with the
// subscriptions mochi has just inherited into the new session. Inherited
// filters keep their upstream references without incrementing them; inherited
// filters the bridge never merged are adopted; retained filters absent from
// the inherited set (a clean reconnect inherits nothing) are released once.
func (b *Bridge) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	if cl.Net.Inline {
		return
	}
	inherited := cl.State.Subscriptions.GetAll()
	settle := new([]func())
	defer b.settle(settle)
	b.mu.Lock()
	defer b.mu.Unlock()
	// A connection taken over while this hook waited must not reclaim
	// ownership from its successor.
	if cl.IsTakenOver() {
		return
	}
	entry := b.sessions[cl.ID]
	if entry == nil {
		if len(inherited) == 0 {
			return // empty session: no entry until it subscribes
		}
		entry = &sessionInterest{owner: cl, filters: make(map[string]struct{}, len(inherited))}
		b.sessions[cl.ID] = entry
	}
	entry.owner = cl // establishment always replaces a retained owner
	for filter := range inherited {
		if _, held := entry.filters[filter]; held {
			continue // adopted: the upstream reference already exists
		}
		entry.filters[filter] = struct{}{}
		printers := b.table.PrintersFor(filter)
		for _, serial := range printers {
			*settle = append(*settle, b.pool.RecordSubscribe(serial, filter))
		}
		b.log.Info("session inherited subscription",
			"client", cl.ID, "filter", filter,
			"printers", strings.Join(printers, ","))
	}
	var absent []string
	for filter := range entry.filters {
		if _, ok := inherited[filter]; !ok {
			absent = append(absent, filter)
		}
	}
	for _, filter := range absent {
		b.releaseLocked(cl, filter, settle)
	}
}

// OnSubscribed records the client's granted filters and merges each new
// interest upstream (refcounted per printer and filter). A filter the client
// ID already holds must not merge again: upstream interests count one per
// client filter, so repeated SUBSCRIBEs stay refcount-neutral.
func (b *Bridge) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	// A connection already taken over must not reclaim ownership from its
	// successor; the successor adopts any inherited filters itself.
	if cl.Net.Inline {
		return
	}
	settle := new([]func())
	defer b.settle(settle)
	b.mu.Lock()
	defer b.mu.Unlock()
	// Checked under the lock: a takeover that completed while this hook
	// waited must not hand ownership back to the replaced connection.
	if cl.IsTakenOver() {
		return
	}
	entry := b.ownedLocked(cl)
	for i, sub := range pk.Filters {
		if i >= len(reasonCodes) || reasonCodes[i] >= 0x80 {
			continue
		}
		if _, held := entry.filters[sub.Filter]; held {
			continue
		}
		entry.filters[sub.Filter] = struct{}{}
		printers := b.table.PrintersFor(sub.Filter)
		for _, serial := range printers {
			*settle = append(*settle, b.pool.RecordSubscribe(serial, sub.Filter))
		}
		b.log.Info("client subscribed",
			"client", cl.ID,
			"filter", sub.Filter,
			"printers", strings.Join(printers, ","))
	}
}

// OnUnsubscribed releases one explicitly unsubscribed filter for the current
// owner. Takeover cleanup runs in mochi with stale ownership and must not
// release the successor's interests.
func (b *Bridge) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	if cl.Net.Inline || cl.IsTakenOver() {
		return
	}
	settle := new([]func())
	defer b.settle(settle)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, sub := range pk.Filters {
		b.releaseLocked(cl, sub.Filter, settle)
	}
}

// OnDisconnect reconciles ownership for the dropped client. A session that
// survives (expire=false) keeps its retained owner and interests so a
// resuming client inherits them without re-merging; an expiring session
// releases everything once. Owner identity guards both paths: late hooks
// from a replaced connection never release the successor's interests.
func (b *Bridge) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	if cl.Net.Inline {
		return
	}
	b.log.Info("client disconnected", "client", cl.ID, "reason", cleanErr(err), "expire", expire)
	if !expire {
		return
	}
	settle := new([]func())
	defer b.settle(settle)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.releaseSessionLocked(cl, settle)
}

// OnClientExpired releases the interests of a session mochi just expired.
// Owner identity guards against a delayed expiry callback for a replaced
// connection releasing a successor's interests under the same client ID.
func (b *Bridge) OnClientExpired(cl *mqtt.Client) {
	if cl.Net.Inline {
		return
	}
	settle := new([]func())
	defer b.settle(settle)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.releaseSessionLocked(cl, settle)
}

// cleanErr renders a disconnect reason without a nil error string.
func cleanErr(err error) string {
	if err == nil {
		return "clean"
	}
	return err.Error()
}

// OnPublish forwards a client request to the owning printer's upstream
// connection and suppresses local fan-out via CodeSuccessIgnore.
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
	b.pool.PublishWithContext(serials[0], b.table.Upstream(pk.TopicName), pk.Payload, upstream.PublishContext{
		Origin:       "client",
		Action:       "request",
		ClientID:     cl.ID,
		SourcePacket: true,
		SourceDup:    pk.FixedHeader.Dup,
		SourceRetain: pk.FixedHeader.Retain,
	})
	return pk, packets.CodeSuccessIgnore
}

// settle runs the upstream completions recorded under b.mu, after the
// deferred unlock: references change atomically with ownership, while
// connection and wire waits never hold the fleet-wide ownership lock.
// Completions reconcile the latest recorded state, so their order is free.
func (b *Bridge) settle(fns *[]func()) {
	for _, fn := range *fns {
		fn()
	}
}

// ownedLocked returns the session entry owned by cl, replacing a retained
// owner for the same client ID and creating the entry when absent. Called
// with b.mu held.
func (b *Bridge) ownedLocked(cl *mqtt.Client) *sessionInterest {
	entry := b.sessions[cl.ID]
	if entry == nil {
		entry = &sessionInterest{owner: cl, filters: make(map[string]struct{})}
		b.sessions[cl.ID] = entry
		return entry
	}
	entry.owner = cl
	return entry
}

// releaseLocked removes filter from the entry cl owns and drops the merged
// upstream interest once. It is a no-op when cl is not the retained owner or
// the filter is not held. Called with b.mu held; pool calls never reenter it.
func (b *Bridge) releaseLocked(cl *mqtt.Client, filter string, settle *[]func()) {
	entry := b.sessions[cl.ID]
	if entry == nil || entry.owner != cl {
		return
	}
	if _, held := entry.filters[filter]; !held {
		return
	}
	delete(entry.filters, filter)
	if len(entry.filters) == 0 {
		delete(b.sessions, cl.ID)
	}
	printers := b.table.PrintersFor(filter)
	for _, serial := range printers {
		*settle = append(*settle, b.pool.RecordUnsubscribe(serial, filter))
	}
	b.log.Info("client unsubscribed",
		"client", cl.ID, "filter", filter, "printers", strings.Join(printers, ","))
}

// releaseSessionLocked releases every filter of the entry cl owns and removes
// the entry. It is a no-op when cl is not the retained owner. Called with
// b.mu held.
func (b *Bridge) releaseSessionLocked(cl *mqtt.Client, settle *[]func()) {
	entry := b.sessions[cl.ID]
	if entry == nil || entry.owner != cl {
		return
	}
	filters := make([]string, 0, len(entry.filters))
	for filter := range entry.filters {
		filters = append(filters, filter)
	}
	for _, filter := range filters {
		b.releaseLocked(cl, filter, settle)
	}
}
