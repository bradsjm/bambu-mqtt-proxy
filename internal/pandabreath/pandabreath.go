// Package pandabreath observes Panda Breath accessory chamber sensors over a read-only WebSocket connection.
package pandabreath

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"bambu-mqtt-proxy/internal/config"
)

// Connection tuning.
const (
	// dialTimeout bounds one WebSocket handshake attempt.
	// dialTimeout is readTimeout closes and redials a connection that has delivered no frame for this long; a healthy device pushes far more often.
	dialTimeout = 5 * time.Second
	// readTimeout is maxFrameBytes bounds one incoming frame.
	readTimeout = 30 * time.Second
	// maxFrameBytes is backoffInitial and backoffMax bound the reconnect delay: capped exponential growth with jitter, reset after a connection that delivered at least one reading.
	maxFrameBytes  = 64 << 10
	backoffInitial = 1 * time.Second
	backoffMax     = 30 * time.Second
)

// target is one observed device.
type target struct {
	// serial is the configured printer routing key.
	serial string
	// addr is the WebSocket URL built from the configured host.
	addr string
}

// reading is one accepted warehouse_temper observation.
type reading struct {
	// temp is chamber temperature in degrees Celsius.
	temp float64
	// at is arrival time of the frame that carried it.
	at time.Time
}

// Store holds the latest accessory reading per printer and runs one read-only WebSocket connection per configured Panda Breath address.
type Store struct {
	// log is process logger; frames never reach it.
	log *slog.Logger
	// targets is printers configured with a Panda Breath address.
	targets []target

	// mu is guards readings, history, and connected.
	mu sync.Mutex
	// readings is latest accepted reading per serial.
	readings map[string]reading
	// history is spaced recent readings per serial, oldest first, for the trend.
	history map[string][]reading
	// connected is whether the device WebSocket is open, per serial.
	connected map[string]bool
}

// New returns a store for the printers configured with a Panda Breath address.
func New(printers []config.Printer, log *slog.Logger) *Store {
	s := &Store{log: log, readings: make(map[string]reading),
		history: make(map[string][]reading), connected: make(map[string]bool)}
	for _, p := range printers {
		if addr := p.Setting(AddressKey); addr != "" {
			host, err := config.HostOnly(addr)
			// if is validate rejects this value before New runs.
			if err != nil {
				continue
			}
			s.targets = append(s.targets, target{serial: p.Serial, addr: "ws://" + host + "/ws"})
		}
	}
	return s
}

// Run observes each configured device until cancellation and waits for every connection.
func (s *Store) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, t := range s.targets {
		wg.Go(func() { s.observe(ctx, t) })
	}
	wg.Wait()
	return nil
}

// Chamber returns the latest accepted reading for serial and the time it arrived. ok is false before the first accepted frame.
func (s *Store) Chamber(serial string) (temp float64, at time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.readings[serial]
	if !ok {
		return 0, time.Time{}, false
	}
	return r.temp, r.at, true
}

// observe keeps one device connection alive until ctx is done: dial, read until error or silence, wait out a bounded backoff, repeat.
func (s *Store) observe(ctx context.Context, t target) {
	backoff := backoffInitial
	for {
		if ctx.Err() != nil {
			return
		}
		gotReading, reason := s.connect(ctx, t)
		if ctx.Err() != nil {
			return
		}
		if gotReading {
			backoff = backoffInitial
		}
		delay := backoff/2 + time.Duration(rand.Float64()*float64(backoff/2))
		s.log.Warn("panda breath connection lost; reconnecting",
			"serial", t.serial, "retry_delay_ms", delay.Milliseconds(), "reason", reason)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, backoffMax)
	}
}

// connect dials the device WebSocket and reads until an error, a read timeout, or ctx cancellation, which closes the connection immediately.
func (s *Store) connect(ctx context.Context, t target) (bool, string) {
	dialer := websocket.Dialer{HandshakeTimeout: dialTimeout}
	conn, _, err := dialer.DialContext(ctx, t.addr, nil)
	if err != nil {
		return false, failReason(err, "handshake")
	}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer func() {
		stopClose()
		conn.Close()
	}()
	// Discard the response: its headers belong to the device, not the log.
	s.log.Info("panda breath connected", "serial", t.serial)
	s.setConnected(t.serial, true)
	defer s.setConnected(t.serial, false)
	conn.SetReadLimit(maxFrameBytes)
	gotReading := false
	for {
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return gotReading, failReason(err, "read")
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			return gotReading, failReason(err, "read")
		}
		if temp, ok := warehouseTemper(data); ok {
			gotReading = true
			r := reading{temp: temp, at: time.Now()}
			s.mu.Lock()
			s.readings[t.serial] = r
			s.history[t.serial] = keepSample(s.history[t.serial], r)
			s.mu.Unlock()
		}
	}
}

// failReason classifies a connection failure into a bounded, payload-free reason class for the log.
func failReason(err error, phase string) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	var ce *websocket.CloseError
	if errors.As(err, &ce) || errors.Is(err, websocket.ErrCloseSent) {
		return "closed"
	}
	return phase
}

// warehouseTemper extracts the one reading this observer accepts: a JSON frame with settings.warehouse_temper as a JSON number.
func warehouseTemper(data []byte) (float64, bool) {
	var frame struct {
		// Settings is a nil pointer means the field was missing or null; a string or object fails the decode.
		Settings struct {
			WarehouseTemper *float64 `json:"warehouse_temper"`
		} `json:"settings"`
	}
	if err := json.Unmarshal(data, &frame); err != nil {
		return 0, false
	}
	if frame.Settings.WarehouseTemper == nil {
		return 0, false
	}
	return *frame.Settings.WarehouseTemper, true
}
