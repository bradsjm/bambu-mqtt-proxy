// The camera wall: /camera/status JSON plus the embedded /overlay page.
// The page composes camera images and telemetry in the browser; raw JPEG
// and MJPEG responses never carry overlay data.
package camera

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

//go:embed overlay.html
var overlayHTML []byte

// Tile is one printer's entry in /camera/status. It carries only display
// state: never serial-derived credentials or raw configuration.
type Tile struct {
	Serial       string   `json:"serial"`
	Model        string   `json:"model"`
	CameraOK     bool     `json:"camera_supported"`
	Connected    bool     `json:"connected"`
	State        string   `json:"state,omitempty"`
	Filename     string   `json:"filename,omitempty"`
	Progress     float64  `json:"progress,omitempty"`
	RemainMin    float64  `json:"remain_min,omitempty"`
	LayerNum     *int     `json:"layer_num,omitempty"`
	TotalLayers  *int     `json:"total_layers,omitempty"`
	NozzleTemp   *float64 `json:"nozzle_temp,omitempty"`
	NozzleTarget *float64 `json:"nozzle_target,omitempty"`
	BedTemp      *float64 `json:"bed_temp,omitempty"`
	BedTarget    *float64 `json:"bed_target,omitempty"`
	ChamberTemp  *float64 `json:"chamber_temp,omitempty"`
	FrameAge     float64  `json:"frame_age_seconds,omitempty"`
	FrameSeq     uint64   `json:"frame_seq,omitempty"`
}

// NewStatusRenderer pairs the camera manager with the telemetry cache.
type StatusRenderer struct {
	cameras *Manager
	state   *telemetry.Cache
	status  connectivitySource
}

// NewStatusRenderer builds the /camera/status payload renderer.
func NewStatusRenderer(cameras *Manager, state *telemetry.Cache, status connectivitySource) *StatusRenderer {
	return &StatusRenderer{cameras: cameras, state: state, status: status}
}

// connectivitySource reports upstream MQTT connectivity per serial, so the
// overlay can distinguish an idle printer from a disconnected one.
type connectivitySource interface {
	Status() map[string]bool
}

// Tiles merges telemetry and camera state for every configured printer.
func (r *StatusRenderer) Tiles() []Tile {
	connected := r.status.Status()
	r.state.SetConnected(connected)
	states := r.state.Snapshot()
	tiles := make([]Tile, 0, len(states))
	for _, st := range states {
		// Display model falls back to the serial-prefix inference so tiles
		// from configs without model fields still show a useful label.
		model := st.Model
		if strings.TrimSpace(model) == "" {
			model = config.ModelFromSerial(st.Serial)
		}
		chamberTemp := st.ChamberTemp
		if !config.ChamberTemperatureSupported(st.Model, st.Serial) {
			chamberTemp = nil
		}
		t := Tile{
			Serial:       st.Serial,
			Model:        model,
			CameraOK:     config.CameraEligible(st.Model, st.Serial),
			Connected:    st.Connected,
			State:        st.PrintingState,
			Filename:     st.Filename,
			Progress:     st.Progress,
			RemainMin:    st.RemainMin,
			LayerNum:     st.LayerNum,
			TotalLayers:  st.TotalLayers,
			NozzleTemp:   st.NozzleTemp,
			NozzleTarget: st.NozzleTarget,
			BedTemp:      st.BedTemp,
			BedTarget:    st.BedTarget,
			ChamberTemp:  chamberTemp,
		}
		if f := r.cameras.Latest(st.Serial); f != nil {
			t.FrameAge = time.Since(f.Captured).Seconds()
			t.FrameSeq = f.Seq
		}
		tiles = append(tiles, t)
	}
	return tiles
}

// RegisterStatus mounts /camera/status, /overlay and /camwall on the shared mux.
func (r *StatusRenderer) RegisterStatus(mux *http.ServeMux) {
	mux.HandleFunc("GET /camera/status", func(w http.ResponseWriter, _ *http.Request) {
		noStore(w)
		w.Header().Set("Content-Type", "application/json")
		list := r.Tiles()
		sort.Slice(list, func(i, j int) bool { return list[i].Serial < list[j].Serial })
		_ = json.NewEncoder(w).Encode(map[string]any{"printers": list})
	})
	wall := func(w http.ResponseWriter, _ *http.Request) {
		noStore(w)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(overlayHTML)
	}
	mux.HandleFunc("GET /overlay", wall)
	mux.HandleFunc("GET /camwall", wall)
}
