// Package health serves the HTTP health endpoints for process supervision.
package health

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// StatusSource reports per-printer upstream connectivity.
type StatusSource interface {
	// Status returns serial -> connected for every configured printer.
	Status() map[string]bool
}

// Server serves /livez and /readyz (always ok once serving — upstream state
// must not fail readiness, clients stay connected while printers recover) and
// /status, which reports per-printer upstream connectivity as JSON.
type Server struct {
	srv    *http.Server
	source StatusSource
	log    *slog.Logger
}

// New builds the health server bound to :port.
func New(port int, source StatusSource, log *slog.Logger) *Server {
	s := &Server{source: source, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", s.ok)
	mux.HandleFunc("/readyz", s.ok)
	mux.HandleFunc("/status", s.status)
	s.srv = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Start serves in a background goroutine; listener errors are logged.
func (s *Server) Start() {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("health server", "error", err)
		}
	}()
}

// Stop shuts the health server down gracefully.
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(ctx)
}

// ok answers 200 for liveness and readiness probes.
func (s *Server) ok(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// status answers with upstream connectivity JSON.
func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":    "ok",
		"upstreams": s.source.Status(),
	})
}
