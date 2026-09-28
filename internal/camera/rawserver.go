// The raw camera server exposes the Bambu chamber image protocol to LAN
// camera clients on the printer camera port: TLS, one exact 80-byte
// authentication payload, then a stream of frames replayed byte-for-byte
// from the shared captures — the printer's raw 16-byte frame header followed
// by its JPEG payload. Sessions are printer-compatible, so there is no
// heartbeat and no application-level reply: unmatched, malformed, and
// unsupported clients are closed silently and acquire no capture.
//
// The server is enabled exactly when the camera feature is, independently
// of the HTTP port.
package camera

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"bambu-mqtt-proxy/internal/tlsutil"
)

// Raw session tuning. Fixed production budgets; variables so focused tests
// can shorten them without touching production constants.
var (
	// rawAuthBudget is the absolute bound shared by the TLS handshake and
	// the exact 80-byte authentication read; the deadline is cleared once a
	// session authenticates.
	rawAuthBudget = 10 * time.Second
	// rawCacheMaxAge is how fresh a cached frame must be to feed a new
	// session without waiting; it mirrors the snapshot freshness rule.
	rawCacheMaxAge = 5 * time.Second
	// rawFrameBudget is the total wait for each usable frame, the cached
	// frame included: one window per frame the session owes, and stale
	// results inside the window never extend it. The raw protocol has no
	// heartbeat, so a camera that produces nothing within the budget ends
	// the session.
	rawFrameBudget = 15 * time.Second
	// rawWriteBudget bounds each header+JPEG write so a client that stops
	// reading cannot pin a session forever.
	rawWriteBudget = 15 * time.Second
)

// RawServer serves the chamber image camera protocol to LAN clients on the
// printer camera port. Every authenticated session streams one manager
// capture; the printer allows one camera connection, so the sharing is the
// point.
type RawServer struct {
	manager *Manager
	log     *slog.Logger
	// tlsConfig wraps every accepted connection. Start builds it from the
	// ephemeral certificate; tests inject a loopback listener with their
	// own certificate.
	tlsConfig *tls.Config

	mu       sync.Mutex
	listener net.Listener
	sockets  map[net.Conn]struct{}
	wg       sync.WaitGroup
	closed   bool
	// connectionID uniquely identifies accepted raw-camera sessions in logs.
	connectionID atomic.Uint64
}

// NewRawServer builds the raw camera server for one manager's captures.
func NewRawServer(manager *Manager, log *slog.Logger) *RawServer {
	return &RawServer{
		manager: manager,
		log:     log,
		sockets: make(map[net.Conn]struct{}),
	}
}

// Start binds the printer camera port with an ephemeral self-signed
// certificate — camera clients skip verification exactly as they do against
// the printer — and serves sessions until Close. Startup errors return
// synchronously; serving continues in the background. The application
// wiring calls Start once per server.
func (s *RawServer) Start() error {
	cert, err := tlsutil.Ensure("", "")
	if err != nil {
		return fmt.Errorf("camera raw server certificate: %w", err)
	}
	s.tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	ln, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(Port)))
	if err != nil {
		return fmt.Errorf("camera raw server listen on port %d: %w", Port, err)
	}
	s.serve(ln)
	return nil
}

// serve accepts sessions on ln. It is split from Start so tests can inject
// a loopback listener instead of the fixed printer port.
func (s *RawServer) serve(ln net.Listener) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = ln.Close()
		return
	}
	s.listener = ln
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		s.acceptLoop(ln)
	}()
}

// acceptLoop accepts clients until the listener closes. Each accepted socket
// is registered and counted before its worker launches, so Close can never
// miss a session that is still being set up.
func (s *RawServer) acceptLoop(ln net.Listener) {
	for {
		raw, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if !closed {
				s.log.Error("camera raw listener stopped",
					"address", ln.Addr().String(), "error", err)
			}
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = raw.Close()
			continue
		}
		s.sockets[raw] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(raw)
	}
}

// Close stops the listener, closes every tracked socket, and joins all
// session workers. Sockets close before the join so handshake, read, and
// write I/O blocked on them unwinds immediately.
func (s *RawServer) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	ln := s.listener
	s.listener = nil
	sockets := make([]net.Conn, 0, len(s.sockets))
	for c := range s.sockets {
		sockets = append(sockets, c)
	}
	s.sockets = make(map[net.Conn]struct{})
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	for _, c := range sockets {
		_ = c.Close()
	}
	s.wg.Wait()
}

// handle runs one raw session: the TLS handshake and the exact 80-byte
// authentication payload under one absolute deadline, then a frame stream
// from the shared capture until the client goes away, misbehaves, or no
// usable frame arrives within the budget.
func (s *RawServer) handle(raw net.Conn) {
	defer s.wg.Done()
	defer s.forget(raw)
	defer raw.Close()
	log := s.log.With("connection_id", s.connectionID.Add(1), "remote", raw.RemoteAddr().String())

	// The handshake and the authentication read share one absolute
	// deadline. A client that is not fully authenticated within it closes
	// with no reply and no capture acquisition.
	if err := raw.SetDeadline(time.Now().Add(rawAuthBudget)); err != nil {
		log.Debug("camera raw authentication deadline failed", "error", err)
		return
	}
	conn := tls.Server(raw, s.tlsConfig)
	if err := conn.Handshake(); err != nil {
		log.Debug("camera raw TLS handshake failed", "error", err)
		return
	}
	var payload [authPayloadLen]byte
	if _, err := io.ReadFull(conn, payload[:]); err != nil {
		log.Debug("camera raw authentication read failed", "error", err)
		return
	}
	if !validAuthHeader(payload[:]) {
		// Wrong magic or command: close silently, like the printer does,
		// and never touch a capture.
		log.Info("camera raw authentication refused", "reason", "invalid header")
		return
	}
	printer, ok := s.manager.matchCameraCredentials(payload[16:48], payload[48:80])
	if !ok {
		// Unknown credentials or an ineligible printer: close silently,
		// like the printer does, and never touch a capture.
		log.Info("camera raw authentication refused", "reason", "unknown credentials or ineligible printer")
		return
	}
	// Authenticated: clear the shared deadline. From here the session lives
	// until client EOF, a protocol error, or a frame budget.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		log.Debug("camera raw session deadline could not be cleared", "serial", printer.Serial, "error", err)
		return
	}
	if _, st := s.manager.Acquire(printer.Serial); st != StatusOK {
		log.Info("camera raw session unavailable", "serial", printer.Serial, "status", st)
		return
	}
	log.Info("camera raw client connected", "serial", printer.Serial)
	// One persistent reference per raw session, released when it ends.
	// Manager.Wait balances its own temporary references internally.
	defer s.manager.Release(printer.Serial)
	defer log.Info("camera raw client disconnected", "serial", printer.Serial)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// EOF watcher: a camera client sends nothing after the authentication
	// payload, so the first read result — EOF, reset, or unexpected bytes —
	// ends the session. Cancelling the context unblocks frame waits
	// promptly; closing the socket also unblocks pending writes.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		var probe [1]byte
		if n, _ := conn.Read(probe[:]); n > 0 {
			log.Info("camera raw client sent unexpected data; ending session")
		}
		_ = raw.Close()
	}()

	// Each usable frame the session owes has one total budget. A cached
	// frame fresher than the cache age counts as delivered immediately; a
	// stale cache is skipped at its observed sequence; and a stale frame
	// returned inside the window is skipped too — it never extends the
	// budget, so no usable frame can take longer than one budget.
	budgetStart := time.Now()
	var lastSeq uint64
	if cached := s.manager.Latest(printer.Serial); cached != nil {
		lastSeq = cached.Seq
		if time.Since(cached.Captured) <= rawCacheMaxAge {
			if !s.sendFrameWithLogger(conn, cached, log) {
				return
			}
			budgetStart = time.Now()
		}
	}
	for ctx.Err() == nil {
		remaining := rawFrameBudget - time.Since(budgetStart)
		if remaining <= 0 {
			// The camera produced nothing usable within the budget; the
			// raw protocol has no heartbeat for the client to watch, so
			// the session ends.
			return
		}
		frame := s.manager.Wait(printer.Serial, ctx, lastSeq, remaining)
		if frame == nil {
			return
		}
		lastSeq = frame.Seq
		if time.Since(frame.Captured) > rawCacheMaxAge {
			// Stale frame: skip at its observed sequence inside the same
			// window and keep waiting for a fresh one.
			continue
		}
		if !s.sendFrameWithLogger(conn, frame, log) {
			return
		}
		budgetStart = time.Now()
	}
}

// sendFrame writes one frame byte-for-byte — the raw 16-byte header captured
// from the printer, then its JPEG payload — under one absolute write
// deadline. A frame without its raw header is a bug: it is never
// synthesized, and the session ends instead.
func (s *RawServer) sendFrame(conn net.Conn, f *Frame) bool {
	return s.sendFrameWithLogger(conn, f, s.log)
}

// sendFrameWithLogger writes a frame and logs failures with the session context.
func (s *RawServer) sendFrameWithLogger(conn net.Conn, f *Frame, log *slog.Logger) bool {
	if len(f.Header) != frameHeaderLen {
		log.Error("camera raw frame lost its raw header; ending session", "seq", f.Seq)
		return false
	}
	if err := conn.SetWriteDeadline(time.Now().Add(rawWriteBudget)); err != nil {
		log.Debug("camera raw client write deadline failed", "seq", f.Seq, "error", err)
		return false
	}
	var err error
	if _, err = conn.Write(f.Header); err == nil {
		_, err = conn.Write(f.JPEG)
	}
	if err != nil {
		log.Info("camera raw client write failed; ending session", "seq", f.Seq, "error", err)
		return false
	}
	return true
}

// forget removes a session's socket from the shutdown registry.
func (s *RawServer) forget(raw net.Conn) {
	s.mu.Lock()
	delete(s.sockets, raw)
	s.mu.Unlock()
}
