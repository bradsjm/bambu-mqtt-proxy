// Package broker assembles the downstream mochi broker with TLS listeners and
// the serial-routing bridge hooks.
package broker

import (
	"bytes"
	"errors"
	"log/slog"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
	"bambu-mqtt-proxy/internal/routing"
)

// Sender forwards a byte-exact command to a configured printer.
type Sender interface {
	// Send attempts one command without waiting for an offline printer.
	Send(serial string, payload []byte) error
}

// Bridge authenticates clients, enforces topic ACLs, and forwards commands.
type Bridge struct {
	// HookBase supplies the unused mochi hook methods.
	mqtt.HookBase
	// links sends requests to configured printers.
	links Sender
	// table resolves downstream serials and aliases.
	table *routing.Table
	// authMode selects configured printer credentials or unrestricted auth.
	authMode string
	// accessCodes contains the configured printer passwords.
	accessCodes map[string]struct{}
	// log records client connections and dropped commands.
	log *slog.Logger
}

// newBridge builds the downstream routing and authentication hook.
func newBridge(cfg *config.Config, table *routing.Table, links Sender, log *slog.Logger) *Bridge {
	codes := make(map[string]struct{}, len(cfg.Printers))
	for _, p := range cfg.Printers {
		codes[p.Password] = struct{}{}
	}
	return &Bridge{
		links: links, table: table, authMode: cfg.Auth.Mode,
		accessCodes: codes, log: log,
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

// OnACLCheck permits configured report/request subscriptions and request publishes.
func (b *Bridge) OnACLCheck(_ *mqtt.Client, topic string, write bool) bool {
	if write {
		_, ok := b.table.RequestSerial(topic)
		return ok
	}
	return b.table.AllowedSubscribe(topic)
}

// OnPublish forwards a client request to the owning printer's upstream
// connection and suppresses local fan-out via CodeSuccessIgnore.
// The inline-client guard is essential: injected upstream reports re-enter
// this hook, and forwarding them again would loop.
func (b *Bridge) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline {
		return pk, nil
	}
	serial, ok := b.table.RequestSerial(pk.TopicName)
	if !ok {
		return pk, packets.CodeSuccessIgnore
	}
	if err := b.links.Send(serial, pk.Payload); err != nil {
		if errors.Is(err, printer.ErrOffline) {
			b.log.Debug("printer request dropped while offline", "serial", serial, "client", cl.ID)
		} else {
			b.log.Warn("printer request failed", "serial", serial, "client", cl.ID, "error", err)
		}
	}
	return pk, packets.CodeSuccessIgnore
}
