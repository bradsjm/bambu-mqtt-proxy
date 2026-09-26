// The camera wall: /camera/status JSON, the /camera/events server-sent
// event stream of the same payload, and the embedded /camwall page. The page
// composes camera images and telemetry in the browser; raw JPEG and MJPEG
// responses never carry status data.
package camera

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

//go:embed camwall.html
var camwallHTML []byte

// eventsInterval is how often /camera/events checks for status changes,
// coalescing bursts of report deltas into at most one event per interval.
// eventsKeepalive bounds how long an unchanged status goes unsent, so
// clients refresh relative ages and notice a dead proxy.
const (
	eventsInterval  = time.Second
	eventsKeepalive = 10 * time.Second
)

// Tile is one printer's entry in /camera/status. It carries only display
// state: never serial-derived credentials or raw configuration.
type Tile struct {
	Serial       string   `json:"serial"`
	Name         string   `json:"name,omitempty"`
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
	PrintError   string   `json:"print_error,omitempty"`
	HMS          []HMS    `json:"hms,omitempty"`
	// Detection is the optional OctoEverywhere detection status object,
	// omitted when the feature is not configured.
	Detection any      `json:"detection,omitempty"`
	ReportAge *float64 `json:"report_age_seconds,omitempty"`
	FrameAge  float64  `json:"frame_age_seconds,omitempty"`
	FrameSeq  uint64   `json:"frame_seq,omitempty"`
}

// HMS is one Health Management System alert in display form.
type HMS struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
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

// SetDetection attaches the optional detection engine after construction.
func (r *StatusRenderer) SetDetection(d detectionSource) {
	r.detection = d
}

// connectivitySource reports upstream MQTT connectivity per serial, so the
// camera wall can distinguish an idle printer from a disconnected one.
type connectivitySource interface {
	Status() map[string]bool
}

// Tiles merges telemetry and camera state for every configured printer,
// ordered by serial.
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
			Name:         st.Name,
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
		if st.PrintError != 0 {
			t.PrintError = fmt.Sprintf("%04X_%04X", uint32(st.PrintError)>>16, uint32(st.PrintError)&0xFFFF)
		}
		for _, a := range st.HMS {
			t.HMS = append(t.HMS, HMS{Code: a.ID(), Severity: a.Severity()})
		}
		if !st.LastReport.IsZero() {
			age := time.Since(st.LastReport).Seconds()
			t.ReportAge = &age
		}
		if f := r.cameras.Latest(st.Serial); f != nil {
			t.FrameAge = time.Since(f.Captured).Seconds()
			t.FrameSeq = f.Seq
		}
		if r.detection != nil {
			t.Detection = r.detection.DetectionStatus(st.Serial)
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
		t.ReportAge, t.FrameAge, t.FrameSeq = nil, 0, 0
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

// RegisterStatus mounts /camera/status, /camera/events and /camwall on the
// shared mux.
func (r *StatusRenderer) RegisterStatus(mux *http.ServeMux) {
	mux.HandleFunc("GET /camera/status", func(w http.ResponseWriter, _ *http.Request) {
		noStore(w)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.payload(r.Tiles()))
	})
	mux.HandleFunc("GET /camera/events", r.handleEvents)
	mux.HandleFunc("GET /camwall", func(w http.ResponseWriter, _ *http.Request) {
		noStore(w)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(camwallHTML)
	})
}
