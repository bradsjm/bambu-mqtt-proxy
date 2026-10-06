// Package pandabreath observes Panda Breath accessory chamber sensors over
// a read-only WebSocket connection. A printer configured with an AddressKey
// setting (config.Printer.Settings) gains a chamber temperature display
// through the shared projection even when its model has no physical chamber
// sensor.
//
// Flow: the store runs one outbound WebSocket connection per configured
// device. The device pushes JSON frames; the observer accepts exactly the
// settings.warehouse_temper numeric reading and records it with the arrival
// time in a small per-serial map. Every other frame shape or field is
// ignored, frames are never logged, and the connection never sends an
// application message. Readings carry their observation time, so a device
// that goes quiet stops projecting instead of freezing a stale value, and a
// connection with no traffic is closed and redialed on a bounded backoff.
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

// Connection tuning. Healthy devices push settings continuously, so these
// bounds only shape failure recovery; they are fixed because the accessory
// link has no upstream pool to inherit behavior from.
const (
	// dialTimeout bounds one WebSocket handshake attempt.
	dialTimeout = 5 * time.Second
	// readTimeout closes and redials a connection that has delivered no
	// frame for this long; a healthy device pushes far more often. It also
	// detects half-open TCP connections that would otherwise idle forever.
	readTimeout = 30 * time.Second
	// maxFrameBytes bounds one incoming frame. Settings frames are tiny;
	// anything larger is not a Panda Breath frame.
	maxFrameBytes = 64 << 10
	// backoffInitial and backoffMax bound the reconnect delay: capped
	// exponential growth with jitter, reset after a connection that
	// delivered at least one reading.
	backoffInitial = 1 * time.Second
	backoffMax     = 30 * time.Second
)

// target is one observed device.
type target struct {
	serial string // routing key shared with the telemetry cache
	addr   string // validated ws:// or wss:// URL from the printer config
}

// reading is one accepted warehouse_temper observation.
type reading struct {
	temp float64   // chamber temperature in degrees Celsius
	at   time.Time // arrival time of the frame that carried it
}

// Store holds the latest accessory reading per printer and runs one
// read-only WebSocket connection per configured Panda Breath address. The
// zero value is not usable; construct with New.
type Store struct {
	log     *slog.Logger // process logger; frames never reach it
	targets []target     // printers configured with a Panda Breath address

	mu        sync.Mutex           // guards readings, history, and connected
	readings  map[string]reading   // latest accepted reading per serial
	history   map[string][]reading // spaced recent readings per serial, oldest first, for the trend
	connected map[string]bool      // whether the device WebSocket is open, per serial

	cancel context.CancelFunc // set by Start; nil before it and after Stop
	done   chan struct{}      // closed when the observers have returned
}

// New returns a store for the printers configured with a Panda Breath
// address. Printers without an address get no connection; a store with no
// configured device starts nothing.
func New(printers []config.Printer, log *slog.Logger) *Store {
	s := &Store{log: log, readings: make(map[string]reading),
		history: make(map[string][]reading), connected: make(map[string]bool)}
	for _, p := range printers {
		if addr := p.Setting(AddressKey); addr != "" {
			s.targets = append(s.targets, target{serial: p.Serial, addr: addr})
		}
	}
	return s
}

// Start launches one observer per configured device. It returns
// immediately; with no configured device it does nothing. Stop must not be
// called concurrently with Start or itself: one caller (the service
// wiring) owns the lifecycle.
func (s *Store) Start() {
	if len(s.targets) == 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		s.log.Info("panda breath observer started", "devices", len(s.targets))
		var wg sync.WaitGroup
		for _, t := range s.targets {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.observe(ctx, t)
			}()
		}
		wg.Wait()
	}()
}

// Stop ends every observer and waits for its goroutine, closing any open
// connection immediately instead of waiting out its read deadline. It is
// safe to call repeatedly and before Start.
func (s *Store) Stop() {
	if s.cancel == nil {
		return
	}
	s.cancel()
	<-s.done
	s.cancel, s.done = nil, nil
}

// ChamberReading returns the latest accepted reading for serial and the
// time it arrived. ok is false before the first accepted frame. Freshness
// is a projection decision: callers compare at against their own clock so
// the staleness semantic matches the rest of the projection.
func (s *Store) ChamberReading(serial string) (temp float64, at time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.readings[serial]
	if !ok {
		return 0, time.Time{}, false
	}
	return r.temp, r.at, true
}

// observe keeps one device connection alive until ctx is done: dial, read
// until error or silence, wait out a bounded backoff, repeat. The backoff
// resets once a connection proves itself with a reading, so a healthy link
// recovers instantly while an absent device waits out the cap. Failures
// are logged as bounded reason classes only: raw errors from this
// boundary can carry the peer's close-reason text or the configured URL.
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

// connect dials the device WebSocket and reads until an error, a read
// timeout, or ctx cancellation, which closes the connection immediately.
// It reports whether the connection delivered at least one accepted
// reading and a bounded failure reason for the log; the raw error never
// leaves this function. It never writes an application message: the only
// traffic is the device's own frames plus the protocol-level pong the read
// loop answers with.
func (s *Store) connect(ctx context.Context, t target) (bool, string) {
	// stopHandshakeClose stops the cancellation-driven close of the TCP
	// connection; nil until the TCP connection exists. It is read and
	// written only on the goroutine that called DialContext. The defer is
	// the failure-path cleanup: by the time it runs after a successful
	// handshake, the callback was already stopped below.
	var stopHandshakeClose func() bool
	defer func() {
		if stopHandshakeClose != nil {
			stopHandshakeClose()
		}
	}()
	dialer := websocket.Dialer{
		HandshakeTimeout: dialTimeout,
		// Gorilla honors ctx only while dialing TCP; the TLS handshake,
		// request write, and upgrade-response read run on the raw
		// connection without a ctx-driven close, so a stalled device that
		// accepted the TCP connection would hold Stop until the handshake
		// timeout. Closing the connection the moment it exists, and
		// stopping that callback when the handshake returned, bounds
		// cancellation by the dial. Verified TLS for wss:// is untouched:
		// the custom dial only wraps the TCP dial.
		NetDialContext: func(dialCtx context.Context, network, addr string) (net.Conn, error) {
			nc, err := (&net.Dialer{}).DialContext(dialCtx, network, addr)
			if err != nil {
				return nil, err
			}
			stopHandshakeClose = context.AfterFunc(ctx, func() { _ = nc.Close() })
			return nc, nil
		},
	}
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, _, err := dialer.DialContext(dialCtx, t.addr, nil)
	if err != nil {
		return false, failReason(err, "handshake")
	}
	// Success: install the read-loop close before stopping the handshake
	// close, so a cancellation landing during the handover still finds a
	// registered closer. From here on the read loop owns the connection.
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	if stopHandshakeClose != nil {
		stopHandshakeClose()
	}
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

// failReason classifies a connection failure into a bounded, payload-free
// reason class for the log. Raw errors stay out of the output: a websocket
// close error carries the peer's close-reason text, and transport errors
// can echo the configured URL, including userinfo it may hold. phase names
// the step that failed ("handshake" or "read") for everything the classes
// above do not cover.
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

// warehouseTemper extracts the one reading this observer accepts: a JSON
// frame with settings.warehouse_temper as a JSON number. Malformed frames,
// missing or null values, and nonnumeric encodings all fail the typed
// decode, and JSON cannot carry nonfinite numbers, so nothing else can
// pass. Every other field is ignored, and frame contents never reach the
// log.
func warehouseTemper(data []byte) (float64, bool) {
	var frame struct {
		Settings struct {
			// A nil pointer means the field was missing or null; a
			// string or object fails the decode.
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
