package health

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

type fakeSource struct {
	status map[string]bool
}

func (f fakeSource) Status() map[string]bool { return f.status }

func TestEndpoints(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	mux := http.NewServeMux()
	Routes(mux, fakeSource{status: map[string]bool{"S1": true, "S2": false}}, nil)
	srv := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", port), Handler: mux}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	client := &http.Client{Timeout: 2 * time.Second}
	waitFor(t, 5*time.Second, func() bool {
		resp, err := client.Get(liveURL(port, "/livez"))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	for _, path := range []string{"/livez", "/readyz"} {
		resp, err := client.Get(liveURL(port, path))
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
			t.Fatalf("GET %s = %d %q, want 200 ok", path, resp.StatusCode, body)
		}
	}

	resp, err := client.Get(liveURL(port, "/status"))
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer resp.Body.Close()
	var decoded struct {
		Status    string          `json:"status"`
		Upstreams map[string]bool `json:"upstreams"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode /status: %v", err)
	}
	if decoded.Status != "ok" || !decoded.Upstreams["S1"] || decoded.Upstreams["S2"] {
		t.Fatalf("/status = %+v", decoded)
	}
}

func liveURL(port int, path string) string {
	return "http://127.0.0.1:" + strconv.Itoa(port) + path
}

func TestStatusDetectionContract(t *testing.T) {
	mk := func(sections map[string]func() any) *httptest.Server {
		mux := http.NewServeMux()
		Routes(mux, fakeSource{status: map[string]bool{"S1": true}}, sections)
		return httptest.NewServer(mux)
	}

	// Without detection configured, the payload must not carry the field at
	// all: the no-key behavior stays byte-compatible with the old shape.
	without := mk(nil)
	defer without.Close()
	resp, err := http.Get(without.URL + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
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
	defer with.Close()
	resp, err = http.Get(with.URL + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	var decoded struct {
		Detection map[string]struct {
			State      string `json:"state"`
			PauseState string `json:"pause_state"`
		} `json:"detection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	d := decoded.Detection["S1"]
	if d.State != "monitoring" || d.PauseState != "none" {
		t.Fatalf("detection[S1] = %+v, want the UI contract fields", d)
	}
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
