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
}

// New builds the server bound to :port with the shared route mux.
func New(port int, log *slog.Logger) *Server {
	mux := http.NewServeMux()
	return &Server{
		mux: mux,
		log: log,
		srv: &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
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

// Stop shuts the HTTP server down gracefully.
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(ctx)
}
