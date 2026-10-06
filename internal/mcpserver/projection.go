package mcpserver

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/printerview"
)

// Printer projection. These types double as the tools' typed output schema:
// the SDK derives JSON Schema from them, so every field the wire can carry
// is declared here. Nulls mean "the printer has not reported this", never a
// guessed 0.

// Stable operational error codes. Input validation failures (bad enum,
// out-of-range numbers) are rejected by the input schema before a handler
// runs and never surface as these codes.
const (
	errCameraDisabled     = "camera_disabled"
	errCameraUnsupported  = "camera_unsupported"
	errCameraUnavailable  = "camera_unavailable"
	errStaleImage         = "stale_image"
	errInvalidCursor      = "invalid_cursor"
	errTooManyWaits       = "too_many_waits"
	errSubscriptionsLimit = "subscriptions_limit"
)

// PrinterState is the typed projection of one printer: the shared printer
// view (the same fields /camera/status serves) plus MCP feature states.
type PrinterState struct {
	printerview.View
	// Camera reports the core camera feature state.
	Camera string `json:"camera"`
	// Modules contains non-nil state from running modules, keyed by name.
	Modules map[string]any `json:"modules,omitempty"`
	// Activity lists recent printer events, newest first.
	Activity []activity.Entry `json:"activity"`
	// Controls lists the control actions currently available.
	Controls []string `json:"controls"`
}

// Camera states reported in PrinterState.Camera.
const (
	cameraDisabled    = "disabled"
	cameraUnsupported = "unsupported"
	cameraOffline     = "offline"
	cameraReady       = "ready"
)

// WatchEvent summarizes one notification-relevant change. Kinds are stable;
// events are the coalesced delta of the most recent change, and the server
// keeps no per-client history.
type WatchEvent struct {
	Kind string `json:"kind"`
	// Module names the module for module_changed events.
	Module string `json:"module,omitempty"`
	// Percent is set only for progress_milestone events.
	Percent *float64 `json:"percent,omitempty"`
	// Detail is a short human-readable note; not a stable field.
	Detail string `json:"detail,omitempty"`
}

// Attention event kinds. Progress mode additionally emits progress_milestone.
const (
	kindPrintStarted    = "print_started"
	kindJobChanged      = "job_changed"
	kindPrintPaused     = "print_paused"
	kindPrintFinished   = "print_finished"
	kindPrintFailed     = "print_failed"
	kindConnectLost     = "connectivity_lost"
	kindConnectRestored = "connectivity_restored"
	kindReportsStale    = "reports_stale"
	kindReportsFresh    = "reports_fresh"
	kindModuleChanged   = "module_changed"
	kindProgressStep    = "progress_milestone"
	kindHMSAlert        = "hms_alert"
	kindPrintError      = "print_error"
	kindPrintStopped    = "print_stopped"
)

// revision is the opaque wake counter pair behind watch_printer and every
// returned revision token. Clients treat the token as opaque. The epoch is
// drawn once per process, so tokens issued by a previous process never
// decode against the current one: a restart always forces a resync. Any
// token the server cannot decode, or one whose counters no longer match the
// current per-printer counters, forces a snapshot with resync_required. The
// server claims no lossless history.
type revision struct {
	attention uint64
	progress  uint64
}

// encodeRevision renders the opaque token form
// 1.<epoch36>.<attention36>.<progress36>.
func encodeRevision(epoch uint32, r revision) string {
	return "1." + strconv.FormatUint(uint64(epoch), 36) + "." +
		strconv.FormatUint(r.attention, 36) + "." + strconv.FormatUint(r.progress, 36)
}

// decodeRevision parses a token produced by encodeRevision. An epoch of 0
// is never issued, so it can never decode.
func decodeRevision(s string) (epoch uint32, r revision, ok bool) {
	parts := strings.Split(s, ".")
	if len(parts) != 4 || parts[0] != "1" {
		return 0, revision{}, false
	}
	epoch64, errEpoch := strconv.ParseUint(parts[1], 36, 32)
	att, errAtt := strconv.ParseUint(parts[2], 36, 64)
	prog, errProg := strconv.ParseUint(parts[3], 36, 64)
	if errEpoch != nil || errAtt != nil || errProg != nil || epoch64 == 0 {
		return 0, revision{}, false
	}
	return uint32(epoch64), revision{attention: att, progress: prog}, true
}

// stateURI is the resource URI for one printer's typed projection.
func stateURI(serial string) string {
	return "bambu://printers/" + serial + "/state"
}

// serialFromStateURI reverses stateURI for exact resource URIs.
func serialFromStateURI(uri string) (string, bool) {
	const prefix, suffix = "bambu://printers/", "/state"
	if !strings.HasPrefix(uri, prefix) || !strings.HasSuffix(uri, suffix) {
		return "", false
	}
	serial := uri[len(prefix) : len(uri)-len(suffix)]
	if serial == "" || strings.ContainsAny(serial, "/?#") {
		return "", false
	}
	return serial, true
}

// encodeCursor wraps a serial in the opaque pagination token.
func encodeCursor(serial string) string {
	return base64.RawURLEncoding.EncodeToString([]byte("s:" + serial))
}

// decodeCursor unwraps a pagination token into the serial to resume after.
func decodeCursor(cursor string) (string, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || !strings.HasPrefix(string(raw), "s:") {
		return "", false
	}
	return strings.TrimPrefix(string(raw), "s:"), true
}

// buildPrinterState merges core and module views into the wire projection.
// now is injected so tests can freeze time.
func (s *Server) buildPrinterState(serial string, now time.Time) (PrinterState, bool) {
	info, known := s.printers[serial]
	if !known {
		return PrinterState{}, false
	}
	st, _ := s.state.State(serial)
	st.Serial, st.Name, st.Model = info.serial, info.name, info.model
	sv, _ := s.state.Session(serial)
	out := PrinterState{
		View:     printerview.Build(st, sv, s.connStatus(serial), s.generation(serial), now),
		Camera:   s.cameraState(serial),
		Activity: []activity.Entry{},
		Controls: []string{},
	}
	for _, m := range s.modules {
		if m.State == nil {
			continue
		}
		if value := m.State(serial); value != nil {
			if out.Modules == nil {
				out.Modules = make(map[string]any)
			}
			out.Modules[m.Name] = value
		}
	}
	if s.control != nil {
		out.Controls = s.control.Available(serial)
	}
	if s.activity != nil {
		if recent := s.activity.Recent(serial); recent != nil {
			out.Activity = recent
		}
	}
	return out, true
}
