// The manager maps printer serials to their capture and enforces camera
// eligibility before any connection attempt.
package camera

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Status explains why a camera request cannot be served.
type Status int

// Request outcomes. Handlers map them to HTTP status codes.
const (
	StatusOK Status = iota
	StatusUnknownSerial
	StatusUnsupportedModel
	StatusUnavailable
)

// Manager owns one capture per configured printer serial.
type Manager struct {
	mu       sync.Mutex
	captures map[string]*capture
	bySerial map[string]config.Printer
	// cameraEndpoint resolves a printer's camera address. Production code
	// uses cameraAddress; tests inject loopback fakes so they never fight
	// over the fixed camera port.
	cameraEndpoint func(config.Printer) (string, error)
	log            *slog.Logger
}

// NewManager indexes the configured printers. Only P1 and A1 series models
// ever get a capture; other serials are permanently unsupported.
func NewManager(printers []config.Printer, log *slog.Logger) *Manager {
	bySerial := make(map[string]config.Printer, len(printers))
	for _, p := range printers {
		bySerial[p.Serial] = p
	}
	return &Manager{
		captures:       make(map[string]*capture),
		bySerial:       bySerial,
		cameraEndpoint: cameraAddress,
		log:            log,
	}
}

// supported reports whether the serial names a configured camera-capable
// printer. The explicit model wins when present; otherwise the model is
// inferred from the serial prefix (01P/01S/030/039). Unknown serials and
// non-chamber-image printers are refused before any camera socket opens.
func (m *Manager) supported(serial string) (config.Printer, bool) {
	p, ok := m.bySerial[serial]
	if !ok || !config.CameraEligible(p.Model, p.Serial) {
		return config.Printer{}, false
	}
	return p, true
}

// get returns the existing capture or creates it lazily. Callers have
// already checked support; creation is reserved for eligible printers.
func (m *Manager) get(serial string) *capture {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.captures[serial]; ok {
		return c
	}
	c := newCapture(m.bySerial[serial], m.log, m.cameraEndpoint)
	m.captures[serial] = c
	return c
}

// matchCameraCredentials resolves a raw camera session's NUL-padded username
// and access-code fields to the configured eligible printer they belong to.
// Access codes are printer-generated and unique, so an exact field match
// identifies one printer; ineligible models never match even when their
// credentials happen to coincide. Credential uniqueness and format are the
// configuration's concern; nothing is validated or rewritten here.
func (m *Manager) matchCameraCredentials(username, accessCode []byte) (config.Printer, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.bySerial {
		if !config.CameraEligible(p.Model, p.Serial) {
			continue
		}
		if nulPaddedEqual(username, p.Username) && nulPaddedEqual(accessCode, p.Password) {
			return p, true
		}
	}
	return config.Printer{}, false
}

// nulPaddedEqual reports whether a fixed-length authentication field is
// exactly value followed by NUL padding, the shape this proxy itself sends
// upstream in authPayload.
func nulPaddedEqual(field []byte, value string) bool {
	if len(value) > len(field) {
		return false
	}
	if string(field[:len(value)]) != value {
		return false
	}
	for _, b := range field[len(value):] {
		if b != 0 {
			return false
		}
	}
	return true
}

// Snapshot returns the freshest frame for serial. It reports the failure
// reason so handlers can answer 404, 422, or 503 without opening camera
// sockets for ineligible printers.
func (m *Manager) Snapshot(serial string, wait func(*capture) (*Frame, bool)) (*Frame, Status) {
	p, ok := m.supported(serial)
	if !ok {
		if _, known := m.bySerial[serial]; known {
			return nil, StatusUnsupportedModel
		}
		return nil, StatusUnknownSerial
	}
	_ = p
	c := m.get(serial)
	frame, okFrame := wait(c)
	if !okFrame || frame == nil {
		return nil, StatusUnavailable
	}
	return frame, StatusOK
}

// Acquire starts (or joins) the shared capture for serial and returns the
// frame notification channel. It reports StatusOK only for eligible serials;
// callers own the matching Release.
func (m *Manager) Acquire(serial string) (chan struct{}, Status) {
	if _, ok := m.supported(serial); !ok {
		if _, known := m.bySerial[serial]; known {
			return nil, StatusUnsupportedModel
		}
		return nil, StatusUnknownSerial
	}
	c := m.get(serial)
	return c.acquire(), StatusOK
}

// Release drops one consumer interest previously taken with Acquire.
func (m *Manager) Release(serial string) {
	m.mu.Lock()
	c, ok := m.captures[serial]
	m.mu.Unlock()
	if ok {
		c.release()
	}
}

// Latest returns the newest frame for serial without waiting, for callers
// that render a fallback instead of blocking (the camera wall).
func (m *Manager) Latest(serial string) *Frame {
	m.mu.Lock()
	c, ok := m.captures[serial]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return c.latest()
}

// Wait blocks until the capture publishes a frame with a sequence greater
// than after, the context ends, or the timeout expires. It is the shared
// wait path for HTTP streams, detection, and raw camera sessions; each call
// takes a temporary capture reference that the call itself balances.
func (m *Manager) Wait(serial string, ctx context.Context, after uint64, timeout time.Duration) *Frame {
	m.mu.Lock()
	c, ok := m.captures[serial]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return c.wait(ctx, after, timeout)
}

// Close shuts every capture down. Streams end and waiters wake.
func (m *Manager) Close() {
	m.mu.Lock()
	captures := make([]*capture, 0, len(m.captures))
	for _, c := range m.captures {
		captures = append(captures, c)
	}
	m.mu.Unlock()
	for _, c := range captures {
		c.close()
	}
}
