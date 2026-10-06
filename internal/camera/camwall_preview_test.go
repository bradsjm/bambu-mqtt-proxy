// Job preview projection tests for the camera wall: the module's State
// hook feeds the jobpreview module state object into /camera/status JSON,
// the SSE change key, and the disabled projection before any archive is
// accepted. The tests never dial FTPS: the fixture printers are
// unreachable and Lookup performs no network work.
package camera

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/jobpreview"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/telemetry"
)

// previewPrinters returns the single configured printer used by these tests.
func previewPrinters() []config.Printer {
	return []config.Printer{{
		Serial: "01S00C351100139", Model: "P1S",
		Address: "127.0.0.1:8883", Username: "bblp", Password: "code",
	}}
}

// newPreviewService builds the preview service over the fixture printer
// and attaches its module to the wall renderer.
func newPreviewService(t *testing.T, state *telemetry.Cache) *jobpreview.Service {
	t.Helper()
	printers := previewPrinters()
	service := jobpreview.New(printers, state, testConnectivity{}, discardLogger())
	t.Cleanup(service.Close)
	return service
}

// newPreviewRenderer builds a wall renderer over the fixture printer with
// the preview module attached when withModule is true.
func newPreviewRenderer(t *testing.T, withModule bool) (*StatusRenderer, *telemetry.Cache, *jobpreview.Service) {
	t.Helper()
	printers := previewPrinters()
	state := telemetry.NewCache(printers, discardLogger())
	renderer := NewStatusRenderer(NewManager(printers, discardLogger()), state, testConnectivity{})
	var service *jobpreview.Service
	if withModule {
		service = newPreviewService(t, state)
		if err := renderer.SetModules([]module.Module{service.Module()}); err != nil {
			t.Fatalf("SetModules: %v", err)
		}
	}
	return renderer, state, service
}

// statusJSON fetches /camera/status from the renderer's mux and returns
// the raw body.
func statusJSON(t *testing.T, r *StatusRenderer) []byte {
	t.Helper()
	mux := http.NewServeMux()
	r.RegisterStatus(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/camera/status", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET /camera/status = %d, want 200", w.Code)
	}
	return w.Body.Bytes()
}

// statusTiles decodes the /camera/status payload into printer tiles.
func statusTiles(t *testing.T, r *StatusRenderer) []Tile {
	t.Helper()
	var payload struct {
		Printers []Tile `json:"printers"`
	}
	if err := json.Unmarshal(statusJSON(t, r), &payload); err != nil {
		t.Fatalf("decode /camera/status: %v", err)
	}
	if len(payload.Printers) != 1 {
		t.Fatalf("camera status tiles = %+v", payload.Printers)
	}
	return payload.Printers
}

// observeReport merges one upstream report into the cache the way the pool
// observer does, on transport generation 7.
func observeReport(t *testing.T, state *telemetry.Cache, seq uint64, payload string) {
	t.Helper()
	state.ObserveReport("01S00C351100139", seq, 7, []byte(payload))
}

// TestTilesNoPreviewModuleWithoutModuleState requires the nil-module path:
// tiles carry no jobpreview member at all, so the wall shows no archive
// section while the job preview module is off.
func TestTilesNoPreviewModuleWithoutModuleState(t *testing.T) {
	renderer, _, _ := newPreviewRenderer(t, false)

	if _, ok := renderer.Tiles()[0].extra["jobpreview"]; ok {
		t.Fatal("tile must carry no jobpreview member when the module is off")
	}
	if bytes.Contains(statusJSON(t, renderer), []byte(`"jobpreview"`)) {
		t.Fatal("status payload must carry no jobpreview member when the module is off")
	}
}

// TestPreviewStateUnavailableBeforeArchive wires a real preview service
// over a telemetry cache and requires the module state to show the
// unavailable projection before anything is observed or archived: no job
// name, not current, and no metadata. The disabled projection is the
// module-off state, asserted by TestTilesNoPreviewModuleWithoutModuleState.
func TestPreviewStateUnavailableBeforeArchive(t *testing.T) {
	renderer, _, _ := newPreviewRenderer(t, true)

	tile := renderer.Tiles()[0]
	raw, err := json.Marshal(tile.extra["jobpreview"])
	if err != nil {
		t.Fatalf("marshal module state: %v", err)
	}
	var st jobpreview.ModuleState
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("decode module state: %v", err)
	}
	if st.JobPreview.Status != jobpreview.StatusUnavailable || st.JobPreview.Current ||
		st.JobPreview.JobName != "" || st.JobPreview.RetrievedAt != nil || st.JobPreview.ImageURL != nil {
		t.Fatalf("view = %+v, want status %q with null optional fields", st.JobPreview, jobpreview.StatusUnavailable)
	}
	if st.JobMetadata != nil {
		t.Fatalf("job_metadata = %+v, want nil before retrieval", st.JobMetadata)
	}
}

// TestPreviewStateFollowsLiveProjection requires the module state to follow
// the live projection: pending while a job is active, then unavailable
// once it ends, with the job name and Current flag always mirroring the
// current view.
func TestPreviewStateFollowsLiveProjection(t *testing.T) {
	renderer, state, _ := newPreviewRenderer(t, true)

	decode := func() jobpreview.ModuleState {
		t.Helper()
		raw, err := json.Marshal(renderer.Tiles()[0].extra["jobpreview"])
		if err != nil {
			t.Fatalf("marshal module state: %v", err)
		}
		var st jobpreview.ModuleState
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatalf("decode module state: %v", err)
		}
		return st
	}

	observeReport(t, state, 1, `{"print":{"gcode_state":"RUNNING","subtask_name":"benchy.3mf"}}`)
	st := decode()
	if st.JobPreview.Status != jobpreview.StatusPending || !st.JobPreview.Current || st.JobPreview.JobName != "benchy.3mf" {
		t.Fatalf("active view = %+v, want pending current benchy.3mf", st.JobPreview)
	}
	if st.JobPreview.RetrievedAt != nil || st.JobPreview.ImageURL != nil || st.JobMetadata != nil {
		t.Fatalf("unsettled job must carry no retrieved fields: %+v / %+v", st.JobPreview, st.JobMetadata)
	}

	// The job ends without any attempt: the projection switches to
	// unavailable and stops claiming current.
	observeReport(t, state, 2, `{"print":{"gcode_state":"FINISH"}}`)
	st = decode()
	if st.JobPreview.Status != jobpreview.StatusUnavailable || st.JobPreview.Current {
		t.Fatalf("finished view = %+v, want unavailable and not current", st.JobPreview)
	}
}

// TestStatusPayloadCarriesPreviewModuleState requires the JSON paths on the
// real /camera/status body: printers[0].modules.jobpreview.job_preview and
// .job_metadata, with job_metadata null before an archive is accepted.
func TestStatusPayloadCarriesPreviewModuleState(t *testing.T) {
	renderer, state, _ := newPreviewRenderer(t, true)
	observeReport(t, state, 1, `{"print":{"gcode_state":"RUNNING","subtask_name":"benchy.3mf"}}`)

	var doc struct {
		Printers []struct {
			JobPreview jobpreview.ModuleState `json:"jobpreview"`
		} `json:"printers"`
	}
	if err := json.Unmarshal(statusJSON(t, renderer), &doc); err != nil {
		t.Fatalf("decode /camera/status: %v", err)
	}
	if len(doc.Printers) != 1 {
		t.Fatalf("printers = %d, want 1", len(doc.Printers))
	}
	st := doc.Printers[0].JobPreview
	if st.JobPreview.Status != jobpreview.StatusPending || !st.JobPreview.Current {
		t.Fatalf("job_preview = %+v, want a pending current projection", st.JobPreview)
	}
	if st.JobMetadata != nil {
		t.Fatalf("job_metadata = %+v, want null before retrieval", st.JobMetadata)
	}
}

// TestChangeKeyIncludesPreviewModuleState requires module state changes to
// trigger SSE events: the change key serializes the module state, so a
// status difference must change the key while identical state keeps it
// stable. handleEvents serves the same tiles to SSE, so both surfaces
// always carry the same preview state.
func TestChangeKeyIncludesPreviewModuleState(t *testing.T) {
	base := Tile{extra: map[string]any{"jobpreview": jobpreview.ModuleState{}}}
	base.extra["jobpreview"] = jobpreview.ModuleState{
		JobPreview: jobpreview.View{Status: jobpreview.StatusUnavailable},
	}
	ready := Tile{extra: map[string]any{"jobpreview": jobpreview.ModuleState{
		JobPreview: jobpreview.View{Status: jobpreview.StatusReady, JobName: "benchy.3mf", Current: true},
	}}}

	unavailable, err := changeKey([]Tile{base})
	if err != nil {
		t.Fatalf("changeKey: %v", err)
	}
	a, err := changeKey([]Tile{ready})
	if err != nil {
		t.Fatalf("changeKey: %v", err)
	}
	if bytes.Equal(unavailable, a) {
		t.Fatal("a preview status change must change the SSE change key")
	}
	again, err := changeKey([]Tile{ready})
	if err != nil || !bytes.Equal(a, again) {
		t.Fatalf("identical preview state must keep the key stable: %v", err)
	}
}
