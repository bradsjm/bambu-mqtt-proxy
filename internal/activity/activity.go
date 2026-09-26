// Package activity keeps a bounded, in-memory log of notable events per
// printer — print starts, pauses, finishes, failures, HMS alerts,
// connectivity changes, and AI detection findings — so the camera wall and
// the /activity endpoint can show what happened recently. Nothing is
// persisted: the log starts empty on every proxy start.
package activity

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Capacity is the number of entries kept per printer; once full, each new
// entry overwrites the oldest one.
const Capacity = 50

// Severity values for Entry.Severity.
const (
	Info    = "info"
	Warning = "warning"
	Error   = "error"
)

// Entry is one recorded event. AgeSeconds is computed when the entry is read,
// so clients can show relative times without trusting their own clock.
type Entry struct {
	ID         uint64    `json:"id"`          // monotonically increases across all printers
	Time       time.Time `json:"time"`        // UTC event timestamp
	Kind       string    `json:"kind"`        // stable event code
	Severity   string    `json:"severity"`    // info, warning, or error
	Message    string    `json:"message"`     // display-ready event text
	AgeSeconds float64   `json:"age_seconds"` // event age when read
}

// Log holds one ring buffer per configured printer. A nil *Log is valid and
// records nothing, so producers need no feature checks.
type Log struct {
	mu     sync.Mutex       // guards IDs and printer rings
	nextID uint64           // ID assigned to the next event
	rings  map[string]*ring // configured printer event buffers
	now    func() time.Time // event clock; replaceable by package tests
}

// ring is a fixed-capacity circular buffer; next is the slot the following
// write overwrites.
type ring struct {
	buf  []Entry // events in circular-buffer order
	next int     // next overwrite slot after the buffer fills
}

// New builds a log for the configured printers; events for other serials are
// ignored.
func New(printers []config.Printer) *Log {
	rings := make(map[string]*ring, len(printers))
	for _, p := range printers {
		rings[p.Serial] = &ring{buf: make([]Entry, 0, Capacity)}
	}
	return &Log{rings: rings, now: time.Now}
}

// Record appends one event for serial.
func (l *Log) Record(serial, kind, severity, message string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.rings[serial]
	if !ok {
		return
	}
	l.nextID++
	e := Entry{
		ID:       l.nextID,
		Time:     l.now().UTC().Round(0),
		Kind:     kind,
		Severity: severity,
		Message:  message,
	}
	if len(r.buf) < Capacity {
		r.buf = append(r.buf, e)
		return
	}
	r.buf[r.next] = e
	r.next = (r.next + 1) % len(r.buf)
}

// Recent returns serial's entries newest first, with ages filled in. Unknown
// serials and a nil log return nil.
func (l *Log) Recent(serial string) []Entry {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.rings[serial]
	if !ok || len(r.buf) == 0 {
		return nil
	}
	now := l.now()
	out := make([]Entry, 0, len(r.buf))
	for i := range len(r.buf) {
		// Walk backwards from the newest slot, the one just before next.
		e := r.buf[(r.next-1-i+len(r.buf))%len(r.buf)]
		e.AgeSeconds = now.Sub(e.Time).Seconds()
		out = append(out, e)
	}
	return out
}

// Register mounts GET /activity (every printer) and GET /activity/{serial}
// on the shared mux.
func (l *Log) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /activity", func(w http.ResponseWriter, _ *http.Request) {
		out := make(map[string][]Entry, len(l.rings))
		for serial := range l.rings {
			out[serial] = nonNil(l.Recent(serial))
		}
		writeJSON(w, http.StatusOK, map[string]any{"printers": out})
	})
	mux.HandleFunc("GET /activity/{serial}", func(w http.ResponseWriter, req *http.Request) {
		serial := req.PathValue("serial")
		if _, ok := l.rings[serial]; !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown printer serial"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"serial": serial, "events": nonNil(l.Recent(serial))})
	})
}

// nonNil keeps empty logs serializing as [] instead of null.
func nonNil(events []Entry) []Entry {
	if events == nil {
		return []Entry{}
	}
	return events
}

// writeJSON answers with an uncached JSON body.
func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
