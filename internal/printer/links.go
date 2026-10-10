package printer

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// ErrOffline means the printer is not connected and subscribed.
var ErrOffline = errors.New("printer is offline")

// Links owns the always-on links for all configured printers.
type Links struct {
	// links indexes the immutable set of configured printer links.
	links map[string]*link
	// mu serializes startup and shutdown.
	mu sync.Mutex
	// started prevents duplicate initial connection attempts.
	started bool
	// closed makes shutdown terminal.
	closed bool
}

// New builds links without dialing or waiting for printers.
// Report callbacks must not block or publish upstream.
func New(printers []config.Printer, log *slog.Logger, onReport func(serial, topic string, payload []byte), onOnline func(serial string, online bool)) *Links {
	ls := &Links{links: make(map[string]*link, len(printers))}
	for _, spec := range printers {
		ls.links[spec.Serial] = newLink(spec, log, onReport, onOnline)
	}
	return ls
}

// Start dials every printer concurrently and lets Paho retry offline printers.
func (ls *Links) Start() {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.started || ls.closed {
		return
	}
	ls.started = true
	for _, l := range ls.links {
		l.client.Connect()
	}
}

// Send attempts one QoS 0 command without waiting for a connection.
// Its publish wait is bounded to five seconds; commands are never retained.
func (ls *Links) Send(serial string, payload []byte) error {
	l := ls.links[serial]
	if l == nil || !l.isOnline() {
		return ErrOffline
	}
	deadline := time.Now().Add(5 * time.Second)
	token := l.client.Publish("device/"+serial+"/request", 0, false, payload)
	if !token.WaitTimeout(time.Until(deadline)) {
		return fmt.Errorf("send %s: publish timed out", serial)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("send %s: %w", serial, err)
	}
	return nil
}

// Online reports whether a configured printer is connected and subscribed.
func (ls *Links) Online(serial string) bool {
	l := ls.links[serial]
	return l != nil && l.isOnline()
}

// Status includes connected-and-subscribed status for every configured serial.
func (ls *Links) Status() map[string]bool {
	out := make(map[string]bool, len(ls.links))
	for serial, l := range ls.links {
		out[serial] = l.isOnline()
	}
	return out
}

// Close stops initial retries, reconnects, and all printer sockets.
func (ls *Links) Close() {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.closed {
		return
	}
	ls.closed = true
	for _, l := range ls.links {
		l.close()
	}
}
