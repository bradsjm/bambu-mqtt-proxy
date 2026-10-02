// Job preview projection tests for the camera wall: the shared service's
// cache-only lookup feeds Tile.JobPreview/JobMetadata into /camera/status
// JSON, the SSE change key, and the disabled projection when no service
// exists. The tests never dial FTPS: the fixture printers are unreachable
// and Lookup performs no network work.
package camera

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/jobpreview"
	"bambu-mqtt-proxy/internal/printerview"
	"bambu-mqtt-proxy/internal/telemetry"
)

// previewPrinters returns the single configured printer used by these tests.
func previewPrinters() []config.Printer {
	return []config.Printer{{
		Serial: "01S00C351100139", Model: "P1S",
		Address: "127.0.0.1:8883", Username: "bblp", Password: "code",
	}}
}

// newPreviewRenderer builds a wall renderer over the fixture printer, with
// the preview service attached only when withService is true.
func newPreviewRenderer(t *testing.T, withService bool) (*StatusRenderer, *telemetry.Cache) {
	t.Helper()
	printers := previewPrinters()
	state := telemetry.NewCache(printers, discardLogger())
	renderer := NewStatusRenderer(NewManager(printers, discardLogger()), state, testConnectivity{})
	if withService {
		service := jobpreview.New(printers, state, testConnectivity{}, discardLogger())
		t.Cleanup(service.Close)
		renderer.SetJobPreview(service)
	}
	return renderer, state
}

// statusTiles fetches /camera/status from the renderer's mux and returns
// the decoded printer tiles.
func statusTiles(t *testing.T, r *StatusRenderer) []Tile {
	t.Helper()
	mux := http.NewServeMux()
	r.RegisterStatus(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/camera/status", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET /camera/status = %d, want 200", w.Code)
	}
	var payload struct {
		Printers []Tile `json:"printers"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
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

// TestTilesJobPreviewDisabledWithoutService exercises the nil-service path:
// every tile must carry the shared disabled projection, with no metadata
// and no image fields, so the wall can render a stable feature-off state.
func TestTilesJobPreviewDisabledWithoutService(t *testing.T) {
	renderer, _ := newPreviewRenderer(t, false)

	tile := renderer.Tiles()[0]
	want := jobpreview.Disabled().Preview
	if tile.JobPreview != want {
		t.Fatalf("JobPreview = %+v, want %+v", tile.JobPreview, want)
	}
	if tile.JobPreview.Status != jobpreview.StatusDisabled || tile.JobPreview.Current ||
		tile.JobPreview.JobName != "" || tile.JobPreview.RetrievedAt != nil || tile.JobPreview.ImageURL != nil {
		t.Fatalf("disabled view = %+v, want status %q with null optional fields", tile.JobPreview, jobpreview.StatusDisabled)
	}
	if tile.JobMetadata != nil {
		t.Fatalf("JobMetadata = %+v, want nil when the service is absent", tile.JobMetadata)
	}
}

// TestTilesJobPreviewServiceProjection wires a real preview service over a
// telemetry cache and requires the tile to follow the live projection:
// pending while a job is active, then unavailable once it ends, with the
// job name and Current flag always mirroring the current view.
func TestTilesJobPreviewServiceProjection(t *testing.T) {
	renderer, state := newPreviewRenderer(t, true)

	observeReport(t, state, 1, `{"print":{"gcode_state":"RUNNING","subtask_name":"benchy.3mf"}}`)

	tile := renderer.Tiles()[0]
	v := tile.JobPreview
	if v.Status != jobpreview.StatusPending || !v.Current || v.JobName != "benchy.3mf" {
		t.Fatalf("active view = %+v, want pending current benchy.3mf", v)
	}
	if v.RetrievedAt != nil || v.ImageURL != nil || tile.JobMetadata != nil {
		t.Fatalf("unsettled job must carry no retrieved fields: %+v / %+v", v, tile.JobMetadata)
	}

	// The job ends without any attempt: the projection switches to
	// unavailable and stops claiming current.
	observeReport(t, state, 2, `{"print":{"gcode_state":"FINISH"}}`)
	v = renderer.Tiles()[0].JobPreview
	if v.Status != jobpreview.StatusUnavailable || v.Current {
		t.Fatalf("finished view = %+v, want unavailable and not current", v)
	}
}

// TestStatusPayloadCarriesJobPreviewFields requires the additive wire
// fields on the real /camera/status JSON: job_preview always present with
// its status string and job_metadata null when no archive was accepted.
func TestStatusPayloadCarriesJobPreviewFields(t *testing.T) {
	renderer, state := newPreviewRenderer(t, true)
	observeReport(t, state, 1, `{"print":{"gcode_state":"RUNNING","subtask_name":"benchy.3mf"}}`)

	var doc struct {
		Printers []struct {
			JobPreview  jobpreview.View      `json:"job_preview"`
			JobMetadata *jobpreview.Metadata `json:"job_metadata"`
		} `json:"printers"`
	}
	mux := http.NewServeMux()
	renderer.RegisterStatus(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/camera/status", nil))
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode /camera/status: %v", err)
	}
	if len(doc.Printers) != 1 {
		t.Fatalf("printers = %d, want 1", len(doc.Printers))
	}
	got := doc.Printers[0]
	if got.JobPreview.Status != jobpreview.StatusPending || !got.JobPreview.Current {
		t.Fatalf("job_preview = %+v, want a pending current projection", got.JobPreview)
	}
	if got.JobMetadata != nil {
		t.Fatalf("job_metadata = %+v, want null before retrieval", got.JobMetadata)
	}
}

// TestChangeKeyIncludesJobPreview requires preview state changes to trigger
// SSE events: the change key serializes JobPreview and JobMetadata, so a
// status difference must change the key while identical state keeps it
// stable. handleEvents serves the same tiles to SSE, so both surfaces
// always carry the same preview state.
func TestChangeKeyIncludesJobPreview(t *testing.T) {
	base := Tile{View: printerview.View{Serial: "S1"}}
	unavailable := base
	unavailable.JobPreview = jobpreview.View{Status: jobpreview.StatusUnavailable}
	ready := base
	ready.JobPreview = jobpreview.View{Status: jobpreview.StatusReady, JobName: "benchy.3mf", Current: true}

	a, err := changeKey([]Tile{unavailable})
	if err != nil {
		t.Fatalf("changeKey: %v", err)
	}
	b, err := changeKey([]Tile{ready})
	if err != nil {
		t.Fatalf("changeKey: %v", err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("a preview status change must change the SSE change key")
	}
	again, err := changeKey([]Tile{ready})
	if err != nil || !bytes.Equal(b, again) {
		t.Fatalf("identical preview state must keep the key stable: %v", err)
	}
}
