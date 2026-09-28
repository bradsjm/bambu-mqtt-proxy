// The manager maps printer serials to their capture and enforces camera
// eligibility before any connection attempt.
package camera

import (
	"context"
	"log/slog"
	"os/exec"
	"strings"
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
	// ffmpegPath is the resolved executable for RTSPS web captures.
	ffmpegPath string
	// webEnabled allows RTSPS models only for managers serving web routes.
	webEnabled bool
	// cameraEndpoint resolves a printer's camera address. Production code
	// uses cameraAddress; tests inject loopback fakes so they never fight
	// over the fixed camera port.
	cameraEndpoint func(config.Printer) (string, error)
	log            *slog.Logger
}

// NewManager indexes configured printers without enabling RTSPS web capture.
// The legacy camera protocol remains limited to P1 and A1 models.
func NewManager(printers []config.Printer, log *slog.Logger) *Manager {
	return newManager(printers, log, false)
}

// NewWebManager indexes configured printers and resolves FFmpeg once when
// the web camera routes are enabled.
func NewWebManager(printers []config.Printer, log *slog.Logger) *Manager {
	return newManager(printers, log, true)
}

// newManager indexes printers and optionally resolves the RTSPS capture tool.
func newManager(printers []config.Printer, log *slog.Logger, webEnabled bool) *Manager {
	bySerial := make(map[string]config.Printer, len(printers))
	needsFFmpeg := false
	for _, p := range printers {
		bySerial[p.Serial] = p
		if requiresFFmpeg(p.Model, p.Serial) {
			needsFFmpeg = true
		}
	}
	m := &Manager{
		captures:       make(map[string]*capture),
		bySerial:       bySerial,
		cameraEndpoint: cameraAddress,
		log:            log,
		webEnabled:     webEnabled,
	}
	if webEnabled && needsFFmpeg {
		if path, err := exec.LookPath("ffmpeg"); err == nil {
			m.ffmpegPath = path
		} else {
			log.Warn("RTSPS web cameras unavailable: FFmpeg executable was not found; install FFmpeg (the Docker image includes it) and restart",
				"executable", "ffmpeg")
		}
	}
	return m
}

// webSupported reports whether the configured model can provide a web camera
// image. It is deliberately separate from CameraEligible, which gates raw,
// Gadget, and MCP camera behavior.
func (m *Manager) webSupported(serial string) (config.Printer, bool) {
	if !m.webEnabled {
		return m.supported(serial)
	}
	p, ok := m.bySerial[serial]
	return p, ok && webCameraSupported(p.Model, p.Serial)
}

// WebSupported reports web-camera capability without starting a capture.
func (m *Manager) WebSupported(serial string) bool {
	_, ok := m.webSupported(serial)
	return ok
}

// WebUnavailableReason returns the stable display reason for a web camera
// that is model-capable but cannot run because FFmpeg is unavailable.
func (m *Manager) WebUnavailableReason(serial string) string {
	p, ok := m.webSupported(serial)
	if m.webEnabled && ok && requiresFFmpeg(p.Model, p.Serial) && m.ffmpegPath == "" {
		return "RTSPS camera unavailable: FFmpeg is not installed"
	}
	return ""
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
	p := m.bySerial[serial]
	var c *capture
	if m.webEnabled && requiresFFmpeg(p.Model, p.Serial) {
		c = newRTSPCapture(p, m.log, m.ffmpegPath)
	} else {
		c = newCapture(p, m.log, m.cameraEndpoint)
	}
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

// WebSnapshot returns a camera frame for a model supported by the web wall.
func (m *Manager) WebSnapshot(serial string, wait func(*capture) (*Frame, bool)) (*Frame, Status) {
	if _, ok := m.webSupported(serial); !ok {
		if _, known := m.bySerial[serial]; known {
			return nil, StatusUnsupportedModel
		}
		return nil, StatusUnknownSerial
	}
	if m.WebUnavailableReason(serial) != "" {
		return nil, StatusUnavailable
	}
	frame, ok := wait(m.get(serial))
	if !ok || frame == nil {
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

// AcquireWeb starts or joins a web camera capture. RTSPS captures are
// unavailable without FFmpeg and are never started by raw/Gadget/MCP callers.
func (m *Manager) AcquireWeb(serial string) (chan struct{}, Status) {
	if _, ok := m.webSupported(serial); !ok {
		if _, known := m.bySerial[serial]; known {
			return nil, StatusUnsupportedModel
		}
		return nil, StatusUnknownSerial
	}
	if m.WebUnavailableReason(serial) != "" {
		return nil, StatusUnavailable
	}
	return m.get(serial).acquire(), StatusOK
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

// webCameraModel uses explicit model configuration first, then known legacy
// P1/A1 inference and verified RTSPS serial prefixes.
func webCameraModel(model, serial string) string {
	if strings.TrimSpace(model) != "" {
		return config.NormalizeModel(model)
	}
	if inferred := config.ModelFromSerial(serial); inferred != "" {
		return inferred
	}
	s := strings.ToUpper(strings.TrimSpace(serial))
	for _, mapping := range rtspSerialPrefixes {
		if strings.HasPrefix(s, mapping.prefix) {
			return mapping.model
		}
	}
	return ""
}

// rtspSerialPrefixes maps verified RTSPS printer serial prefixes to models.
var rtspSerialPrefixes = []struct {
	prefix string
	model  string
}{
	{"00M", "X1C"},
	{"00W", "X1"},
	{"03W", "X1E"},
	{"22E", "P2S"},
	{"093", "H2S"},
	{"094", "H2D"},
}

// webCameraSupported reports image-capture support for web camera routes.
func webCameraSupported(model, serial string) bool {
	switch webCameraModel(model, serial) {
	case "P1P", "P1S", "A1", "A1MINI", "X1", "X1C", "X1E", "P2S", "H2S", "H2D":
		return true
	default:
		return false
	}
}

// requiresFFmpeg reports whether web capture needs the external FFmpeg tool.
func requiresFFmpeg(model, serial string) bool {
	switch webCameraModel(model, serial) {
	case "X1", "X1C", "X1E", "P2S", "H2S", "H2D":
		return true
	default:
		return false
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
