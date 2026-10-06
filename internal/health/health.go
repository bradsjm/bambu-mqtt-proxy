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
// Each sections entry adds a top-level /status member; nil keeps the core payload.
// A section name that collides with a core member is rejected before mounting.
func Routes(mux *http.ServeMux, source StatusSource, sections map[string]func() any) {
	for name := range sections {
		if name == "status" || name == "upstreams" {
			panic("health section collides with core member: " + name)
		}
	}
	mux.HandleFunc("GET /livez", ok)
	mux.HandleFunc("GET /readyz", ok)
	mux.HandleFunc("GET /status", status(source, sections))
}

// ok answers 200 for liveness and readiness probes.
func ok(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// status answers with upstream connectivity JSON.
func status(source StatusSource, sections map[string]func() any) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		out := map[string]any{
			"status":    "ok",
			"upstreams": source.Status(),
		}
		for name, value := range sections {
			if value != nil {
				out[name] = value()
			}
		}
		_ = json.NewEncoder(w).Encode(out)
	}
}
