// Package activity keeps a bounded, in-memory log of notable events per
// printer — print starts, pauses, finishes, failures, HMS alerts,
// connectivity changes, and AI detection findings — so the camera wall and
// the /activity endpoint can show what happened recently. Nothing is
// persisted: the log starts empty on every proxy start.
package activity

import (
	"context"
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

// Entry is one recorded event. AgeSeconds is computed when the entry is
// read, so clients can show relative times without trusting their own
// clock.
type Entry struct {
	// ID increases across the log and starts from the process clock.
	ID uint64 `json:"id"`
	// Time contains the UTC event timestamp.
	Time time.Time `json:"time"`
	// Kind identifies the event.
	Kind string `json:"kind"`
	// Severity classifies the event as info, warning, or error.
	Severity string `json:"severity"`
	// Message contains display-ready event text.
	Message string `json:"message"`
	// AgeSeconds contains the event age at read time.
	AgeSeconds float64 `json:"age_seconds"`
	// URL optionally links to an event reference.
	URL string `json:"url,omitempty"`

	// Observation is the source-event evidence for generators that must pin
	// an entry to the original job that caused it: JobGen identifies the
	// preview-job generation at event time. It is internal evidence only
	// and never serialized to HTTP clients.
	Observation Observation `json:"-"`
}

// Observation is the source-event evidence attached to entries whose
// meaning depends on the print that caused them. It carries the job
// generation copied from the exact coherent job+session snapshot that
// proved the event, so consumers can reject entries recorded against
// telemetry that has since switched to another print.
type Observation struct {
	// JobGen identifies the job generation at event time.
	JobGen uint64
}

// Log holds one ring buffer per configured printer. A nil *Log is valid and
// records nothing, so producers need no feature checks.
type Log struct {
	// mu guards IDs, rings, and the observer.
	mu sync.Mutex
	// nextID contains the last assigned event ID.
	nextID uint64
	// rings contains the configured printer buffers.
	rings map[string]*ring
	// now supplies the event clock.
	now func() time.Time
	// observer receives newly recorded entries without the lock.
	observer func(string, Entry)
	// changed wakes all waiters when an entry is recorded.
	changed chan struct{}
}

// ring is a fixed-capacity circular buffer; next is the slot the following
// write overwrites.
type ring struct {
	// buf holds entries in circular-buffer order.
	buf []Entry
	// next selects the next overwrite slot after the buffer fills.
	next int
}

// New builds a configured printer log with IDs seeded from the current microsecond.
// Events for other serials are ignored.
func New(printers []config.Printer) *Log {
	rings := make(map[string]*ring, len(printers))
	for _, p := range printers {
		rings[p.Serial] = &ring{buf: make([]Entry, 0, Capacity)}
	}
	return &Log{rings: rings, now: time.Now, nextID: uint64(time.Now().UnixMicro()), changed: make(chan struct{})}
}

// Record appends one event for serial. Producers that must pin the event
// to the print session that caused it use RecordObserved instead.
func (l *Log) Record(serial, kind, severity, message string) {
	l.RecordObserved(serial, kind, severity, message, Observation{})
}

// RecordObserved appends one event for serial with the source-session
// evidence Observation. It owns the shared append logic: timestamp and ID
// generation, ring overwrite, and the unlocked observer callback.
func (l *Log) RecordObserved(serial, kind, severity, message string, observation Observation) {
	l.record(serial, kind, severity, message, "", observation)
}

// RecordLink appends one event for serial that carries a reference URL, such
// as an error-code lookup page. It records no source-session evidence.
func (l *Log) RecordLink(serial, kind, severity, message, url string) {
	l.record(serial, kind, severity, message, url, Observation{})
}

// record is the shared append path behind RecordObserved and RecordLink.
func (l *Log) record(serial, kind, severity, message, url string, observation Observation) {
	if l == nil {
		return
	}
	l.mu.Lock()
	r, ok := l.rings[serial]
	if !ok {
		l.mu.Unlock()
		return
	}
	l.nextID++
	e := Entry{
		ID:          l.nextID,
		Time:        l.now().UTC().Round(0),
		Kind:        kind,
		Severity:    severity,
		Message:     message,
		URL:         url,
		Observation: observation,
	}
	if len(r.buf) < Capacity {
		r.buf = append(r.buf, e)
	} else {
		r.buf[r.next] = e
		r.next = (r.next + 1) % len(r.buf)
	}
	close(l.changed)
	l.changed = make(chan struct{})
	observer := l.observer
	l.mu.Unlock()
	// Report processing calls this method, so the observer runs unlocked
	// and must not block.
	if observer != nil {
		observer(serial, e)
	}
}

// SetObserver registers a non-blocking callback for newly recorded entries.
// A nil function clears the observer; a nil log ignores the call.
func (l *Log) SetObserver(fn func(serial string, e Entry)) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.observer = fn
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

// Wait returns newer entries immediately or waits until an entry arrives or ctx ends.
// Unknown serials and a nil log return nil.
func (l *Log) Wait(ctx context.Context, serial string, afterID uint64) []Entry {
	if l == nil {
		return nil
	}
	for {
		l.mu.Lock()
		r, ok := l.rings[serial]
		if !ok {
			l.mu.Unlock()
			return nil
		}
		var entries []Entry
		now := l.now()
		for i := range len(r.buf) {
			e := r.buf[(r.next-1-i+len(r.buf))%len(r.buf)]
			if e.ID > afterID {
				e.AgeSeconds = now.Sub(e.Time).Seconds()
				entries = append(entries, e)
			}
		}
		wake := l.changed
		l.mu.Unlock()
		if len(entries) != 0 {
			return entries
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return nil
		}
	}
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
