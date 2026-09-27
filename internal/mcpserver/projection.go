package mcpserver

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/detection"
)

// Printer projection. These types double as the tools' typed output schema:
// the SDK derives JSON Schema from them, so every field the wire can carry
// is declared here. Nulls mean "the printer has not reported this", never a
// guessed 0.

// ToolError is a stable, machine-readable operational outcome. Codes are a
// closed set; message text is human context and may change.
type ToolError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Stable operational error codes. Input validation failures (bad enum,
// out-of-range numbers) are rejected by the input schema before a handler
// runs and never surface as these codes.
const (
	errUnknownSerial      = "unknown_serial"
	errCameraDisabled     = "camera_disabled"
	errCameraUnsupported  = "camera_unsupported"
	errCameraUnavailable  = "camera_unavailable"
	errStaleImage         = "stale_image"
	errInvalidCursor      = "invalid_cursor"
	errTooManyWaits       = "too_many_waits"
	errSubscriptionsLimit = "subscriptions_limit"
)

// Freshness describes real-report evidence for one printer. A report only
// counts when it carried print state; command ACKs never refresh it.
type Freshness struct {
	// Fresh is true when a real report arrived within the freshness window
	// on the upstream connection that is still current.
	Fresh bool `json:"fresh"`
	// LastReportAgeSeconds is the age of the last real report; null when no
	// real report was ever observed.
	LastReportAgeSeconds *float64 `json:"last_report_age_seconds"`
	// UpstreamGeneration identifies the connection the last real report
	// arrived on; null when none was ever observed. It changes on every
	// reconnect, so a pre-reconnect observation never looks current.
	UpstreamGeneration *uint64 `json:"upstream_generation"`
}

// DetectionView is the projection of the optional OctoEverywhere detection
// worker. The disabled state is explicit, never omitted, and the per-print
// user override surfaces as its own permission state.
type DetectionView struct {
	// State mirrors the detection display state, or "disabled" when the
	// feature is not configured.
	State string `json:"state"`
	// Reason explains blocked, starting, or user-disabled states; empty
	// otherwise.
	Reason string `json:"reason,omitempty"`
	// Quality is the reported detection quality 0-100, when reported.
	Quality *int `json:"quality,omitempty"`
	// Enabled reports permission, not activity: false while the user's
	// per-print override is in force, and also when the feature is not
	// configured at all.
	Enabled bool `json:"enabled"`
	// DisabledUntil explains the override's lifetime and is set only while
	// Enabled is false because of a user override.
	DisabledUntil string `json:"disabled_until,omitempty"`
	// SessionID is the opaque token of the active print session. It is
	// informational here: the MCP endpoint itself stays read-only, and
	// disabling goes through the proxy's HTTP endpoint.
	SessionID string `json:"session_id,omitempty"`
	// PauseState mirrors the pause lifecycle: none, pending, confirmed, or
	// unconfirmed. It is preserved while a user override is active.
	PauseState string `json:"pause_state"`
	// Suspended reports a Gadget account-level suspension.
	Suspended bool `json:"suspended,omitempty"`
	// SuspendedReason carries the suspension message when suspended.
	SuspendedReason string `json:"suspended_reason,omitempty"`
}

// PrinterState is the typed projection of one printer.
type PrinterState struct {
	Serial    string `json:"serial"`
	Name      string `json:"name"`
	Model     string `json:"model"`
	Connected bool   `json:"connected"`
	// PrintState is the merged gcode_state; null when never reported.
	PrintState *string `json:"print_state"`
	Printing   bool    `json:"printing"`
	// JobName is the printer's subtask name; null when never reported.
	JobName *string `json:"job_name"`
	// Progress is mc_percent with source presence: null means never
	// reported, even though the merged scalar would read 0.
	Progress *float64 `json:"progress"`
	// RemainingMinutes is mc_remaining_time under the same presence rule.
	RemainingMinutes *float64 `json:"remaining_minutes"`
	LayerNum         *int     `json:"layer_num"`
	TotalLayers      *int     `json:"total_layers"`
	NozzleTemp       *float64 `json:"nozzle_temp"`
	NozzleTarget     *float64 `json:"nozzle_target"`
	BedTemp          *float64 `json:"bed_temp"`
	BedTarget        *float64 `json:"bed_target"`
	ChamberTemp      *float64 `json:"chamber_temp"`
	// PrintError is the raw print_error code; 0 means the printer reported
	// no error, null means no real report was ever observed.
	PrintError *int           `json:"print_error"`
	HMS        []HMSAlertView `json:"hms"`
	// Camera and Detection are explicit feature states; they never disappear
	// when a feature is switched off.
	Camera    string        `json:"camera"`
	Detection DetectionView `json:"detection"`
	Freshness Freshness     `json:"freshness"`
}

// HMSAlertView is one Bambu HMS alert in published ID form.
type HMSAlertView struct {
	ID       string `json:"id"`
	Severity string `json:"severity"`
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
	kindDetectionChange = "detection_health_changed"
	kindProgressStep    = "progress_milestone"
)

// freshnessWindow bounds how long since the last real report a printer is
// still called fresh. It matches the detection engine's reportFreshMax so
// every consumer shares one staleness semantic.
const freshnessWindow = 15 * time.Second

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

// detectionKey renders the change-sensitive fingerprint of a detection
// status: continuously aging fields are stripped via StableKey, so the key
// only moves on real health changes. An absent status collapses to "".
func detectionKey(v any) string {
	st, ok := v.(*detection.Status)
	if !ok || st == nil {
		return ""
	}
	return fmt.Sprint(st.StableKey())
}

// buildPrinterState merges telemetry, connectivity, camera, and detection
// views into the wire projection. now is injected so tests can freeze time.
func (s *Server) buildPrinterState(serial string, now time.Time) (PrinterState, bool) {
	info, known := s.printers[serial]
	if !known {
		return PrinterState{}, false
	}
	out := PrinterState{
		Serial:    info.serial,
		Name:      info.name,
		Model:     info.model,
		Connected: s.connStatus(serial),
		Camera:    s.cameraState(serial),
		Detection: s.detectionView(serial),
	}
	st, haveState := s.state.State(serial)
	if haveState {
		out.PrintState = nullableString(st.PrintingState)
		out.JobName = nullableString(st.Filename)
		out.TotalLayers = st.TotalLayers
		out.NozzleTemp = st.NozzleTemp
		out.NozzleTarget = st.NozzleTarget
		out.BedTemp = st.BedTemp
		out.BedTarget = st.BedTarget
		out.ChamberTemp = st.ChamberTemp
		for _, a := range st.HMS {
			out.HMS = append(out.HMS, HMSAlertView{ID: a.ID(), Severity: a.Severity()})
		}
	}
	sv, haveSession := s.state.Session(serial)
	if haveSession {
		out.Printing = sv.Active
		out.Progress = sv.Progress
		out.RemainingMinutes = sv.RemainingMin
		out.LayerNum = sv.LayerNum
		if sv.Obs > 0 {
			out.PrintError = &st.PrintError
		}
	}
	if !sv.ObsAt.IsZero() {
		age := now.Sub(sv.ObsAt).Seconds()
		out.Freshness.LastReportAgeSeconds = &age
		gen := sv.ObsGen
		out.Freshness.UpstreamGeneration = &gen
		out.Freshness.Fresh = age <= freshnessWindow.Seconds() &&
			s.generation(serial) == sv.ObsGen
	}
	return out, true
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
