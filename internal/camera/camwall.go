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
	"reflect"
	"sort"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/control"
	"bambu-mqtt-proxy/internal/jsonobj"
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

// controlMaxBody bounds printer control request bodies.
const controlMaxBody = 4 << 10

// Tile is one printer's entry in /camera/status: the shared printer view
// plus camera-wall-only state. It carries only display state: never
// serial-derived credentials or raw configuration.
type Tile struct {
	printerview.View
	CameraOK     bool             `json:"camera_supported"`
	CameraReason string           `json:"camera_reason,omitempty"`
	Activity     []activity.Entry `json:"activity,omitempty"` // recent events, newest first
	// Controls lists the printer controls currently available.
	Controls []string `json:"controls"`
	FrameAge float64  `json:"frame_age_seconds,omitempty"`
	FrameSeq uint64   `json:"frame_seq,omitempty"`
	// Modules contains optional display contributions keyed by module name.
	Modules map[string]module.Display `json:"modules,omitempty"`
	extra   map[string]any
}

// statusPayload is the shared /camera/status and /camera/events body.
type statusPayload struct {
	Printers []Tile `json:"printers"`
	extra    map[string]any
}

// MarshalJSON adds module-owned tile values to the core tile object.
func (t Tile) MarshalJSON() ([]byte, error) {
	type coreTile Tile
	obj, err := json.Marshal(coreTile(t))
	if err != nil {
		return nil, err
	}
	return jsonobj.Append(obj, t.extra)
}

// MarshalJSON adds module-owned fleet members to the core payload.
func (p statusPayload) MarshalJSON() ([]byte, error) {
	type corePayload statusPayload
	obj, err := json.Marshal(corePayload(p))
	if err != nil {
		return nil, err
	}
	return jsonobj.Append(obj, p.extra)
}

// tileCoreKeys includes core tile members even when their zero values are omitted.
var tileCoreKeys = coreJSONKeys(reflect.TypeFor[Tile]())

// coreJSONKeys collects JSON field names, including promoted embedded fields.
func coreJSONKeys(t reflect.Type) map[string]bool {
	keys := make(map[string]bool)
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if field.Anonymous && name == "" {
			for key := range coreJSONKeys(field.Type) {
				keys[key] = true
			}
			continue
		}
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		keys[name] = true
	}
	return keys
}

// NewStatusRenderer pairs the camera manager with the telemetry cache.
type StatusRenderer struct {
	cameras  *Manager
	state    *telemetry.Cache
	status   connectivitySource
	printer  controlService  // optional printer controls
	activity *activity.Log   // optional recent-event source
	modules  []module.Module // optional module display hooks
}

// NewStatusRenderer builds the /camera/status payload renderer.
func NewStatusRenderer(cameras *Manager, state *telemetry.Cache, status connectivitySource) *StatusRenderer {
	return &StatusRenderer{cameras: cameras, state: state, status: status}
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

// SetActivity attaches the recent activity log shown in printer tiles.
func (r *StatusRenderer) SetActivity(log *activity.Log) {
	r.activity = log
}

// SetModules attaches module hooks and rejects names that collide with core tile keys.
func (r *StatusRenderer) SetModules(mods []module.Module) error {
	for _, mod := range mods {
		if mod.State != nil {
			if tileCoreKeys[mod.Name] {
				return fmt.Errorf("module %q collides with a core tile member", mod.Name)
			}
		}
	}
	r.modules = mods
	return nil
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
		t.Activity = r.activity.Recent(st.Serial)
		for _, mod := range r.modules {
			if mod.State != nil {
				if value := mod.State(st.Serial); value != nil {
					if t.extra == nil {
						t.extra = make(map[string]any)
					}
					t.extra[mod.Name] = value
				}
			}
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

// payload assembles the shared status body with module fleet values.
func (r *StatusRenderer) payload(tiles []Tile) statusPayload {
	p := statusPayload{Printers: tiles}
	for _, mod := range r.modules {
		if mod.FleetValues == nil {
			continue
		}
		for key, value := range mod.FleetValues() {
			if key != mod.Name && !strings.HasPrefix(key, mod.Name+"_") {
				continue
			}
			if key == "printers" {
				continue
			}
			if p.extra == nil {
				p.extra = make(map[string]any)
			}
			p.extra[key] = value
		}
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
		t.extra = make(map[string]any, len(t.extra))
		for key, value := range tiles[i].extra {
			if stable, ok := value.(interface{ StableKey() any }); ok {
				value = stable.StableKey()
			}
			t.extra[key] = value
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
	// Printer controls are protected against cross-origin browser writes.
	protection := http.NewCrossOriginProtection()
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
	dec := json.NewDecoder(http.MaxBytesReader(w, req.Body, controlMaxBody))
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
