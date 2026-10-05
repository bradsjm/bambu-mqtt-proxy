// The camera wall: /camera/status JSON, the /camera/events server-sent
// event stream of the same payload, and the embedded /camwall page. The page
// composes camera images and telemetry in the browser; raw JPEG and MJPEG
// responses never carry status data.
package camera

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/control"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/jobpreview"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/printerview"
	"bambu-mqtt-proxy/internal/telemetry"
)

//go:embed camwall.html
var camwallHTML []byte

// Bookmark and home-screen icons for the wall. The page also inlines an SVG
// favicon; these raster forms cover /favicon.ico probes and iOS, which
// ignores SVG touch icons.
var (
	//go:embed favicon.ico
	faviconICO []byte
	//go:embed apple-touch-icon.png
	appleTouchIconPNG []byte
)

// eventsInterval is how often /camera/events checks for status changes,
// coalescing bursts of report deltas into at most one event per interval.
// eventsKeepalive bounds how long an unchanged status goes unsent, so
// clients refresh relative ages and notice a dead proxy.
const (
	eventsInterval  = time.Second
	eventsKeepalive = 10 * time.Second
)

// detectionPutMaxBody bounds the toggle and control request bodies: a few
// small fields.
const detectionPutMaxBody = 4 << 10

// Tile is one printer's entry in /camera/status: the shared printer view
// plus camera-wall-only state. It carries only display state: never
// serial-derived credentials or raw configuration.
type Tile struct {
	printerview.View
	CameraOK     bool   `json:"camera_supported"`
	CameraReason string `json:"camera_reason,omitempty"`
	// Detection is the optional OctoEverywhere detection status object,
	// omitted when the feature is not configured.
	Detection any              `json:"detection,omitempty"`
	Activity  []activity.Entry `json:"activity,omitempty"` // recent events, newest first
	// Controls lists the printer controls currently available.
	Controls []string `json:"controls"`
	FrameAge float64  `json:"frame_age_seconds,omitempty"`
	FrameSeq uint64   `json:"frame_seq,omitempty"`
	// JobPreview is the shared archived preview of the current print and
	// JobMetadata carries its accepted archive facts; both come from the
	// job preview service and are present whenever the feature runs, with
	// status disabled when it does not. These are display-only archived
	// values: they never feed printerview's live projection and never
	// become alerts.
	JobPreview  jobpreview.View      `json:"job_preview"`
	JobMetadata *jobpreview.Metadata `json:"job_metadata"`
	// Modules contains optional display contributions keyed by module name.
	Modules map[string]module.Display `json:"modules,omitempty"`
}

// statusPayload is the shared /camera/status and /camera/events body.
type statusPayload struct {
	Printers []Tile `json:"printers"`
	// DetectionSuspended and DetectionMessage appear only while the
	// OctoEverywhere account-level suspension is active.
	DetectionSuspended bool   `json:"detection_suspended,omitempty"`
	DetectionMessage   string `json:"detection_message,omitempty"`
}

// NewStatusRenderer pairs the camera manager with the telemetry cache.
type StatusRenderer struct {
	cameras   *Manager
	state     *telemetry.Cache
	status    connectivitySource
	detection detectionSource
	control   detectionControl
	printer   controlService      // optional printer controls
	activity  *activity.Log       // optional recent-event source
	previews  *jobpreview.Service // optional archived print preview
	modules   []module.Module     // optional module display hooks
}

// NewStatusRenderer builds the /camera/status payload renderer.
func NewStatusRenderer(cameras *Manager, state *telemetry.Cache, status connectivitySource) *StatusRenderer {
	return &StatusRenderer{cameras: cameras, state: state, status: status}
}

// detectionSource is the narrow, optional detection contract the renderer
// consumes: one status object per serial plus the account suspension state.
type detectionSource interface {
	DetectionStatus(serial string) any
	AccountSuspended() (suspended bool, message string)
}

// detectionControl is the optional write contract behind PUT /detection/
// {serial}: apply the per-print override and return the authoritative
// state. It is satisfied by *detection.Engine and stays nil when detection
// is not configured, which keeps the write endpoint absent.
type detectionControl interface {
	SetDetectionEnabled(serial string, enabled bool, sessionID string) (any, error)
}

// controlService is the allow-listed printer control backend behind POST
// /control/{serial}. It is satisfied by *control.Service.
type controlService interface {
	Do(serial, origin string, req control.Request) (control.Result, error)
	Available(serial string) []string
}

// SetControl attaches the printer control service.
func (r *StatusRenderer) SetControl(c controlService) {
	r.printer = c
}

// SetDetection attaches the optional detection engine after construction.
func (r *StatusRenderer) SetDetection(d detectionSource) {
	r.detection = d
}

// SetDetectionControl attaches the optional per-print AI toggle backend.
func (r *StatusRenderer) SetDetectionControl(c detectionControl) {
	r.control = c
}

// SetActivity attaches the recent activity log shown in printer tiles.
func (r *StatusRenderer) SetActivity(log *activity.Log) {
	r.activity = log
}

// SetJobPreview attaches the shared job preview service; nil keeps the
// disabled projection in every tile.
func (r *StatusRenderer) SetJobPreview(s *jobpreview.Service) {
	r.previews = s
}

// SetModules attaches optional module display hooks before serving requests.
func (r *StatusRenderer) SetModules(mods []module.Module) {
	r.modules = mods
}

// connectivitySource reports upstream MQTT connectivity per serial, so the
// camera wall can distinguish an idle printer from a disconnected one.
type connectivitySource interface {
	Status() map[string]bool
	Generation(serial string) uint64
}

// Tiles merges telemetry and camera state for every configured printer,
// ordered by serial.
func (r *StatusRenderer) Tiles() []Tile {
	connected := r.status.Status()
	r.state.SetConnected(connected)
	states := r.state.Snapshot()
	tiles := make([]Tile, 0, len(states))
	for _, st := range states {
		sv, _ := r.state.Session(st.Serial)
		t := Tile{
			View:         printerview.Build(st, sv, st.Connected, r.status.Generation(st.Serial), time.Now()),
			CameraOK:     r.cameras.WebSupported(st.Serial),
			CameraReason: r.cameras.WebUnavailableReason(st.Serial),
			Controls:     []string{},
		}
		if r.printer != nil {
			t.Controls = r.printer.Available(st.Serial)
		}
		if f := r.cameras.Latest(st.Serial); f != nil {
			t.FrameAge = time.Since(f.Captured).Seconds()
			t.FrameSeq = f.Seq
		}
		if r.detection != nil {
			t.Detection = r.detection.DetectionStatus(st.Serial)
		}
		preview := jobpreview.Disabled()
		if r.previews != nil {
			// Cache-only read: Lookup never performs network work and the
			// false flag keeps the PNG bytes out of the status payload.
			if res, ok := r.previews.Lookup(st.Serial, false); ok {
				preview = res
			}
		}
		t.JobPreview = preview.Preview
		t.JobMetadata = preview.Metadata
		t.Activity = r.activity.Recent(st.Serial)
		for _, mod := range r.modules {
			if mod.Display == nil {
				continue
			}
			if display := mod.Display(st.Serial); display != nil {
				if t.Modules == nil {
					t.Modules = make(map[string]module.Display)
				}
				t.Modules[mod.Name] = *display
			}
		}
		tiles = append(tiles, t)
	}
	sort.Slice(tiles, func(i, j int) bool { return tiles[i].Serial < tiles[j].Serial })
	return tiles
}

// payload assembles the shared status body, including the account-level
// detection suspension when active.
func (r *StatusRenderer) payload(tiles []Tile) statusPayload {
	p := statusPayload{Printers: tiles}
	if r.detection != nil {
		p.DetectionSuspended, p.DetectionMessage = r.detection.AccountSuspended()
	}
	return p
}

// changeKey serializes tiles without their continuously aging fields, so
// only real state changes trigger an event between keepalives.
func changeKey(tiles []Tile) ([]byte, error) {
	stable := make([]Tile, len(tiles))
	for i, t := range tiles {
		t.Freshness.LastReportAgeSeconds, t.FrameAge, t.FrameSeq = nil, 0, 0
		t.Activity = append([]activity.Entry(nil), t.Activity...)
		for j := range t.Activity {
			t.Activity[j].AgeSeconds = 0
		}
		// Detection ages (age_seconds, next_check_seconds) are continuously
		// changing; the detection object carries a stable projection.
		if d, ok := t.Detection.(interface{ StableKey() any }); ok {
			t.Detection = d.StableKey()
		}
		stable[i] = t
	}
	return json.Marshal(stable)
}

// handleEvents streams the status payload as server-sent events: once on
// connect, then whenever display state changes, and at least every
// eventsKeepalive. It ends when the client disconnects.
func (r *StatusRenderer) handleEvents(w http.ResponseWriter, req *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	noStore(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprint(w, "retry: 3000\n\n"); err != nil {
		return
	}
	flusher.Flush()

	ticker := time.NewTicker(eventsInterval)
	defer ticker.Stop()
	var lastKey []byte
	var lastSent time.Time
	for {
		tiles := r.Tiles()
		key, err := changeKey(tiles)
		if err == nil && (!bytes.Equal(key, lastKey) || time.Since(lastSent) >= eventsKeepalive) {
			payload, err := json.Marshal(r.payload(tiles))
			if err != nil {
				return
			}
			// Opportunistic write deadline, as for MJPEG streams: a stalled
			// client cannot pin this handler forever.
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(2 * eventsKeepalive))
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
			lastKey, lastSent = key, time.Now()
		}
		select {
		case <-req.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

// RegisterStatus mounts /camera/status, /camera/events, /camwall and the
// wall's icons on the shared mux.
func (r *StatusRenderer) RegisterStatus(mux *http.ServeMux) {
	mux.HandleFunc("GET /camera/status", func(w http.ResponseWriter, _ *http.Request) {
		noStore(w)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.payload(r.Tiles()))
	})
	mux.HandleFunc("GET /camera/events", r.handleEvents)
	// The per-print AI toggle and the printer controls are the wall's write
	// endpoints; they are protected against cross-origin browser writes like
	// /config/api.
	protection := http.NewCrossOriginProtection()
	mux.Handle("PUT /detection/{serial}", protection.Handler(http.HandlerFunc(r.handleDetectionPut)))
	mux.Handle("POST /control/{serial}", protection.Handler(http.HandlerFunc(r.handleControl)))
	mux.HandleFunc("GET /camwall", func(w http.ResponseWriter, _ *http.Request) {
		noStore(w)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(camwallHTML)
	})
	mux.HandleFunc("GET /favicon.ico", staticAsset("image/x-icon", faviconICO))
	mux.HandleFunc("GET /apple-touch-icon.png", staticAsset("image/png", appleTouchIconPNG))
}

// staticAsset serves an embedded, immutable-per-build asset.
func staticAsset(contentType string, body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "public, max-age=86400")
		_, _ = w.Write(body)
	}
}

// handleDetectionPut applies the per-print AI detection toggle: PUT JSON
// {"enabled":bool,"session_id":string}. Disabling requires the opaque
// token of the current print; a stale token or no active print answers
// 409. Every success returns the authoritative detection state.
func (r *StatusRenderer) handleDetectionPut(w http.ResponseWriter, req *http.Request) {
	noStore(w)
	if r.control == nil {
		writeJSONError(w, http.StatusNotFound, "detection is not configured")
		return
	}
	var in struct {
		Enabled   *bool  `json:"enabled"`
		SessionID string `json:"session_id"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, req.Body, detectionPutMaxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil || in.Enabled == nil {
		writeJSONError(w, http.StatusBadRequest, "body must be a JSON object with an enabled boolean")
		return
	}
	st, err := r.control.SetDetectionEnabled(req.PathValue("serial"), *in.Enabled, in.SessionID)
	if err != nil {
		var code int
		switch {
		case errors.Is(err, detection.ErrUnknownPrinter):
			code = http.StatusNotFound
		case errors.Is(err, detection.ErrNoPrintSession),
			errors.Is(err, detection.ErrStaleSession),
			errors.Is(err, detection.ErrDetectionUnavailable):
			code = http.StatusConflict
		default:
			code = http.StatusInternalServerError
		}
		writeJSONError(w, code, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

// handleControl sends one allow-listed printer control: POST JSON
// {"action":"light|pause|resume|speed|stop","on":bool,"profile":string}.
// There is no confirmation step; the result reports that the command was
// sent, and the printer's next report confirms the effect.
func (r *StatusRenderer) handleControl(w http.ResponseWriter, req *http.Request) {
	noStore(w)
	if r.printer == nil {
		writeJSONError(w, http.StatusNotFound, "printer controls are not configured")
		return
	}
	var in control.Request
	dec := json.NewDecoder(http.MaxBytesReader(w, req.Body, detectionPutMaxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		writeJSONError(w, http.StatusBadRequest, "body must be a JSON object with an action")
		return
	}
	res, err := r.printer.Do(req.PathValue("serial"), "camera wall", in)
	if err != nil {
		var code int
		switch {
		case errors.Is(err, control.ErrUnknownPrinter):
			code = http.StatusNotFound
		case errors.Is(err, control.ErrInvalidRequest):
			code = http.StatusBadRequest
		case errors.Is(err, control.ErrNotAvailable):
			code = http.StatusConflict
		case errors.Is(err, control.ErrNotConnected):
			code = http.StatusServiceUnavailable
		case errors.Is(err, control.ErrSendFailed):
			code = http.StatusBadGateway
		default:
			code = http.StatusInternalServerError
		}
		writeJSONError(w, code, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

// writeJSONError renders one operational error as a JSON object.
func writeJSONError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
