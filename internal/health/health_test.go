package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeSource struct {
	status map[string]bool
}

func (f fakeSource) Status() map[string]bool { return f.status }

// get serves one GET through mux without a network listener.
func get(mux *http.ServeMux, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	Routes(mux, fakeSource{status: map[string]bool{"S1": true, "S2": false}}, nil)

	for _, path := range []string{"/livez", "/readyz"} {
		rec := get(mux, path)
		if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
			t.Fatalf("GET %s = %d %q, want 200 ok", path, rec.Code, rec.Body.String())
		}
	}

	var decoded struct {
		Status    string          `json:"status"`
		Upstreams map[string]bool `json:"upstreams"`
	}
	if err := json.Unmarshal(get(mux, "/status").Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode /status: %v", err)
	}
	if decoded.Status != "ok" || !decoded.Upstreams["S1"] || decoded.Upstreams["S2"] {
		t.Fatalf("/status = %+v", decoded)
	}
}

func TestStatusDetectionContract(t *testing.T) {
	mk := func(sections map[string]func() any) *http.ServeMux {
		mux := http.NewServeMux()
		Routes(mux, fakeSource{status: map[string]bool{"S1": true}}, sections)
		return mux
	}

	// Without detection configured, the payload must not carry the field at
	// all: the no-key behavior stays byte-compatible with the old shape.
	var raw map[string]any
	if err := json.Unmarshal(get(mk(nil), "/status").Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, present := raw["detection"]; present {
		t.Fatalf("detection field present without a source: %v", raw)
	}

	// With detection configured, every serial's status object is served.
	with := mk(map[string]func() any{
		"detection": func() any {
			return map[string]any{"S1": map[string]string{
				"state": "monitoring", "pause_state": "none",
			}}
		},
	})
	var decoded struct {
		Detection map[string]struct {
			State      string `json:"state"`
			PauseState string `json:"pause_state"`
		} `json:"detection"`
	}
	if err := json.Unmarshal(get(with, "/status").Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	d := decoded.Detection["S1"]
	if d.State != "monitoring" || d.PauseState != "none" {
		t.Fatalf("detection[S1] = %+v, want the UI contract fields", d)
	}
}
