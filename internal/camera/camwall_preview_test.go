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
	"bambu-mqtt-proxy/internal/printer"
)

// previewPrinters returns the single configured printer used by wall tests.
func previewPrinters() []config.Printer {
	return []config.Printer{{Serial: "01S00C351100139", Model: "P1S", Address: "127.0.0.1:8883", Username: "bblp", Password: "code"}}
}

// statusJSON fetches /camera/status from the renderer's mux.
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
	if err := json.Unmarshal(statusJSON(t, r), &payload); err != nil || len(payload.Printers) != 1 {
		t.Fatalf("decode /camera/status: %v %+v", err, payload.Printers)
	}
	return payload.Printers
}

// TestPreviewModuleProjection checks real camera status with and without a preview module.
func TestPreviewModuleProjection(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		printers := []config.Printer{{Serial: "01S00C351100139", Model: "P1S"}}
		state := printer.NewStore(printers)
		renderer := NewStatusRenderer(NewManager(printers, discardLogger()), state)
		if enabled {
			service := jobpreview.New(printers, state, discardLogger())
			if err := renderer.SetModules([]module.Module{service.Module()}); err != nil {
				t.Fatal(err)
			}
		}
		mux := http.NewServeMux()
		renderer.RegisterStatus(mux)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/camera/status", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d", w.Code)
		}
		if bytes.Contains(w.Body.Bytes(), []byte(`"jobpreview"`)) != enabled {
			t.Fatalf("module enabled=%v: %s", enabled, w.Body.String())
		}
		if enabled {
			var body struct {
				Printers []struct {
					Preview jobpreview.ModuleState `json:"jobpreview"`
				} `json:"printers"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if len(body.Printers) != 1 || body.Printers[0].Preview.JobPreview.Status != jobpreview.StatusUnavailable {
				t.Fatalf("preview = %+v", body)
			}
		}
	}
}
