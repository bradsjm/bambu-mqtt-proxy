// Package httpsrv owns the single HTTP listener that serves the health,
// camera, and camera wall endpoints on one port.
package httpsrv

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server owns the shared HTTP listener. Register routes on Mux before Start;
// Start binds synchronously so a bind failure is a startup error.
type Server struct {
	srv *http.Server
	mux *http.ServeMux
	log *slog.Logger
	// cancel ends the service root context. Request contexts derive from
	// it, so canceling ends long-lived handlers before Shutdown waits.
	cancel context.CancelFunc
}

// New builds the server bound to :port with the shared route mux.
func New(port int, log *slog.Logger) *Server {
	mux := http.NewServeMux()
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		mux:    mux,
		log:    log,
		cancel: cancel,
		srv: &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			BaseContext: func(net.Listener) context.Context {
				return ctx
			},
		},
	}
}

// Mux returns the shared route mux. Register routes before Start.
func (s *Server) Mux() *http.ServeMux { return s.mux }

// Start binds the listener synchronously and serves in a background
// goroutine. An error return means the port could not be served.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.srv.Addr, err)
	}
	go func() {
		if err := s.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.log.Error("http server stopped unexpectedly", "address", ln.Addr().String(), "error", err)
		}
	}()
	return nil
}

// Stop ends the HTTP service. It cancels the root context first so SSE,
// MJPEG, and other streaming handlers observe cancellation immediately,
// then shuts the listener down gracefully. A handler that outlives the
// shutdown budget is closed hard, and any unexpected close error is
// reported. Stop is safe on repeated calls and when Start never ran or
// failed.
func (s *Server) Stop() {
	s.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		if cerr := s.srv.Close(); cerr != nil {
			s.log.Error("http server close returned an error", "error", cerr)
		}
	}
}
