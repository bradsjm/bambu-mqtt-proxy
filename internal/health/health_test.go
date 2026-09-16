package health

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
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

	base := "http://127.0.0.1:8080"
	_ = base
	s := New(port, fakeSource{status: map[string]bool{"S1": true, "S2": false}}, discardLogger())
	s.Start()
	t.Cleanup(s.Stop)

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

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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
