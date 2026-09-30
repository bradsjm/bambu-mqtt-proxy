// HTTP handlers for the camera package: a single JPEG snapshot and a
// multipart MJPEG live stream per printer serial, both served without
// authentication per the proxy's LAN trust model.
package camera

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// streamHeartbeat bounds each blocking wait so a dead client is noticed and
// a live client keeps receiving bytes even at the printer's low frame rate.
const streamHeartbeat = 15 * time.Second

// boundary is the MJPEG multipart boundary.
const boundary = "frame"

// Register mounts the camera routes on the shared mux.
func Register(mux *http.ServeMux, m *Manager) {
	mux.HandleFunc("GET /camera/{serial}/snapshot", m.handleSnapshot)
	mux.HandleFunc("GET /camera/{serial}/stream", m.handleStream)
}

// Status returns the capture state for one serial in the /camera/status
// payload shape: supported models report the newest frame age.
func (m *Manager) Status(serial string) any {
	m.mu.Lock()
	c, ok := m.captures[serial]
	m.mu.Unlock()
	if !ok {
		return map[string]any{"supported": false}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	supported := m.WebSupported(serial)
	state := map[string]any{"supported": supported, "connected": c.cancel != nil}
	if c.frame != nil {
		state["frame_age_seconds"] = time.Since(c.frame.Captured).Seconds()
		state["frame_seq"] = c.frame.Seq
	}
	return state
}

// noStore marks every camera response uncacheable.
func noStore(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
}

// handleSnapshot writes one JPEG frame for the serial in the path.
func (m *Manager) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	serial := r.PathValue("serial")
	noStore(w)
	frame, st := m.WebSnapshot(serial, func(c *capture) (*Frame, bool) {
		return c.snapshot(r.Context())
	})
	switch st {
	case StatusUnknownSerial:
		http.Error(w, "unknown printer serial", http.StatusNotFound)
	case StatusUnsupportedModel:
		http.Error(w, "printer model does not support a web camera", http.StatusUnprocessableEntity)
	case StatusUnavailable:
		http.Error(w, "camera unavailable", http.StatusServiceUnavailable)
	default:
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("Content-Length", strconv.Itoa(len(frame.JPEG)))
		_, _ = w.Write(frame.JPEG)
	}
}

// handleStream writes an MJPEG multipart stream for the serial in the path.
// Response headers commit as soon as the serial is eligible; later camera
// failures can only end the stream, never change the status code.
func (m *Manager) handleStream(w http.ResponseWriter, r *http.Request) {
	serial := r.PathValue("serial")
	noStore(w)
	if _, st := m.AcquireWeb(serial); st != StatusOK {
		switch st {
		case StatusUnknownSerial:
			http.Error(w, "unknown printer serial", http.StatusNotFound)
		case StatusUnsupportedModel:
			http.Error(w, "printer model does not support a web camera", http.StatusUnprocessableEntity)
		default:
			http.Error(w, "camera unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	// Release balances Acquire above. The loop's extra acquires are paired
	// with its own releases, so interest ends when this handler returns.
	defer m.Release(serial)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "multipart/x-mixed-replace; boundary="+boundary)
	w.WriteHeader(http.StatusOK)

	var lastSeq uint64
	for {
		if r.Context().Err() != nil {
			return
		}
		frame := m.Wait(serial, r.Context(), lastSeq, streamHeartbeat)
		if frame == nil {
			// No new frame within the heartbeat: retry while the client
			// stays connected so a printer outage self-heals. A closed
			// manager or a canceled request ends the stream instead.
			if m.isClosed() || r.Context().Err() != nil {
				return
			}
			continue
		}
		lastSeq = frame.Seq
		// Opportunistic write deadline: unsupported recorders (tests) and
		// exotic ResponseWriters skip it; the heartbeat loop still bounds
		// stalled clients through read-side disconnect detection.
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(streamHeartbeat))
		// The boundary must precede EVERY part: browsers delimit frames on
		// it, and a single leading preamble leaves them unable to find the
		// second frame.
		if _, err := fmt.Fprintf(w,
			"--%s\r\nContent-Type: image/jpeg\r\nContent-Length: %d\r\n\r\n",
			boundary, len(frame.JPEG)); err != nil {
			return
		}
		if _, err := w.Write(frame.JPEG); err != nil {
			return
		}
		if _, err := fmt.Fprint(w, "\r\n"); err != nil {
			return
		}
		flusher.Flush()
	}
}
