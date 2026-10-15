// The camera wall: /camera/status JSON and the embedded /camwall page. The page
// composes camera images and telemetry in the browser; raw JPEG and MJPEG
// responses never carry status data.
package camera

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/control"
	"bambu-mqtt-proxy/internal/jsonobj"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/printer"
)

//go:embed camwall.html
var camwallHTML []byte

// camwallGzip is the embedded wall precompressed with best compression.
var camwallGzip []byte

// camwallETag is the weak validator shared by the identity and gzip wall
// encodings. A weak tag may cover both content-codings because they carry the
// same page, and Vary: Accept-Encoding keeps caches from mixing the variants.
var camwallETag string

// init compresses the embedded wall and derives its validator once.
func init() {
	sum := sha256.Sum256(camwallHTML)
	camwallETag = `W/"` + hex.EncodeToString(sum[:]) + `"`
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		panic("camera: wall gzip level: " + err.Error())
	}
	if _, err := w.Write(camwallHTML); err != nil {
		panic("camera: wall gzip write: " + err.Error())
	}
	if err := w.Close(); err != nil {
		panic("camera: wall gzip close: " + err.Error())
	}
	camwallGzip = buf.Bytes()
}

// Bookmark and home-screen icons for the wall. The page also inlines an SVG
// favicon; these raster forms cover /favicon.ico probes and iOS, which
// ignores SVG touch icons.
var (
	//go:embed favicon.ico
	faviconICO []byte
	//go:embed apple-touch-icon.png
	appleTouchIconPNG []byte
)

// controlMaxBody bounds printer control request bodies.
const controlMaxBody = 4 << 10

// tileActivityLimit bounds the activity entries embedded in one status tile.
// The full log stays available at GET /activity/{serial}.
const tileActivityLimit = 20

// Tile is one printer's entry in /camera/status: the shared printer view
// plus camera-wall-only state. It carries only display state: never
// serial-derived credentials or raw configuration.
type Tile struct {
	printer.View
	CameraOK     bool             `json:"camera_supported"`
	CameraReason string           `json:"camera_reason,omitempty"`
	Activity     []activity.Entry `json:"activity,omitempty"` // recent events, newest first
	// ActivityTotal counts every entry the log holds for this printer.
	ActivityTotal int `json:"activity_total,omitempty"`
	// Controls lists the printer controls currently available.
	Controls []string `json:"controls"`
	FrameAge float64  `json:"frame_age_seconds,omitempty"`
	FrameSeq uint64   `json:"frame_seq,omitempty"`
	// Modules contains optional display contributions keyed by module name.
	Modules map[string]module.Display `json:"modules,omitempty"`
	extra   map[string]any
}

// statusPayload is the /camera/status body.
type statusPayload struct {
	Printers []Tile `json:"printers"`
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

// StatusRenderer combines printer documents with camera and module display state.
type StatusRenderer struct {
	// cameras supplies camera capabilities and the latest frames.
	cameras *Manager
	// state supplies the printer-authoritative display views.
	state *printer.Store
	// printer supplies optional user controls.
	printer controlService
	// activity supplies recent printer events.
	activity *activity.Log
	// modules supplies optional module display hooks.
	modules []module.Module
}

// NewStatusRenderer builds the /camera/status payload renderer.
func NewStatusRenderer(cameras *Manager, state *printer.Store) *StatusRenderer {
	return &StatusRenderer{cameras: cameras, state: state}
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

// Tiles combines printer views with camera and module state, ordered by serial.
func (r *StatusRenderer) Tiles() []Tile {
	states := r.state.Views()
	tiles := make([]Tile, 0, len(states))
	for _, st := range states {
		t := Tile{
			View:         st,
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
		recent := r.activity.Recent(st.Serial)
		t.ActivityTotal = len(recent)
		t.Activity = recent
		if len(t.Activity) > tileActivityLimit {
			t.Activity = t.Activity[:tileActivityLimit]
		}
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

// payload assembles the shared status body.
func (r *StatusRenderer) payload(tiles []Tile) statusPayload {
	return statusPayload{Printers: tiles}
}

// RegisterStatus mounts /camera/status, /camwall and the
// wall's icons on the shared mux.
func (r *StatusRenderer) RegisterStatus(mux *http.ServeMux) {
	mux.HandleFunc("GET /camera/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.payload(r.Tiles()))
	})
	// Printer controls are protected against cross-origin browser writes.
	protection := http.NewCrossOriginProtection()
	mux.Handle("POST /control/{serial}", protection.Handler(http.HandlerFunc(r.handleControl)))
	mux.HandleFunc("GET /camwall", serveWall)
	mux.HandleFunc("GET /favicon.ico", staticAsset("image/x-icon", faviconICO))
	mux.HandleFunc("GET /apple-touch-icon.png", staticAsset("image/png", appleTouchIconPNG))
}

// serveWall answers the embedded wall, gzipped when the client accepts it.
// http.ServeContent handles the weak ETag comparison, 304, HEAD, and
// Content-Length.
func serveWall(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("ETag", camwallETag)
	w.Header().Set("Vary", "Accept-Encoding")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	body := camwallHTML
	if acceptsGzip(r) {
		w.Header().Set("Content-Encoding", "gzip")
		body = camwallGzip
	}
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(body))
}

// acceptsGzip reports whether the request accepts gzip with nonzero weight.
// An explicit gzip entry overrides a "*" wildcard.
func acceptsGzip(r *http.Request) bool {
	// gzipWeight and anyWeight hold the listed weights; -1 means not listed.
	gzipWeight, anyWeight := -1.0, -1.0
	for _, field := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		name, params, _ := strings.Cut(field, ";")
		// weight defaults to 1 when the entry has no valid q parameter.
		weight := 1.0
		if _, value, ok := strings.Cut(params, "="); ok {
			if q, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
				weight = q
			}
		}
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "gzip":
			gzipWeight = weight
		case "*":
			anyWeight = weight
		}
	}
	if gzipWeight < 0 {
		gzipWeight = anyWeight
	}
	return gzipWeight > 0
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
