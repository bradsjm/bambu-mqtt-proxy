// Package health registers the process health endpoints on the shared HTTP
// server: /livez and /readyz (always ok once serving — upstream state must
// not fail readiness, clients stay connected while printers recover) and
// /status, which reports per-printer upstream connectivity as JSON.
package health

import (
	"encoding/json"
	"net/http"
)

// StatusSource reports per-printer upstream connectivity.
type StatusSource interface {
	// Status returns serial -> connected for every configured printer.
	Status() map[string]bool
}

// Routes registers /livez, /readyz and /status on the shared mux.
func Routes(mux *http.ServeMux, source StatusSource) {
	mux.HandleFunc("GET /livez", ok)
	mux.HandleFunc("GET /readyz", ok)
	mux.HandleFunc("GET /status", status(source))
}

// ok answers 200 for liveness and readiness probes.
func ok(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// status answers with upstream connectivity JSON.
func status(source StatusSource) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":    "ok",
			"upstreams": source.Status(),
		})
	}
}
