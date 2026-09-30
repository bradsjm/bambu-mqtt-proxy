// Real configuration reload with streaming clients: an SSE camera-events
// request and an MJPEG camera-stream request held open across a /config
// save must both end when the HTTP service restarts, and the new listener
// must serve the new printer list.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/configui"
)

// TestReloadEndsOpenStreamsAndServesNewList drives the real serveOnce
// reload loop: save a new printer list through the PUT endpoint while an
// SSE and an MJPEG response are open, then require both old responses to
// end and the restarted listener to serve the new printer list.
func TestReloadEndsOpenStreamsAndServesNewList(t *testing.T) {
	const (
		oldSerial = "01S00CRELOADOLD1"
		newSerial = "01S00CRELOADNEW2"
	)
	httpPort := freePort(t)
	listenPort := freePort(t)
	cfgPath := filepath.Join(t.TempDir(), "bambu-mqtt-proxy.yaml")
	initial := fmt.Sprintf(`listen:
  - port: %d
    tls: false
auth:
  mode: printer
printers:
  - serial: "%s"
    model: "P1S"
    address: "127.0.0.1:1"
    username: "bblp"
    password: "00008888"
camera:
  enabled: true
http:
  port: %d
log:
  level: error
`, listenPort, oldSerial, httpPort)
	if err := os.WriteFile(cfgPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// serveOnce runs in-process, so inherited BMBPX_* overrides would
	// replace the loopback fixture; unset them for this test only.
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "BMBPX_") {
			t.Setenv(name, "")
			os.Unsetenv(name)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	serving := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-serving:
			if err != nil {
				t.Fatalf("serve loop ended with an error: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("serve loop did not stop after cancellation")
		}
	})
	store := configui.NewStore(cfgPath)
	go func() {
		for {
			next, err := serveOnce(ctx, cfgPath, "error", store)
			if err != nil || next == nil {
				serving <- err
				return
			}
		}
	}()

	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	waitFor(t, 15*time.Second, func() bool { return probeOK(base + "/livez") })

	// SSE: the connect event proves the response is open; a drain goroutine
	// reports when the old response ends.
	sseCtx, sseCancel := context.WithCancel(context.Background())
	defer sseCancel()
	sseReq, err := http.NewRequestWithContext(sseCtx, http.MethodGet, base+"/camera/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	sseResp, err := http.DefaultClient.Do(sseReq)
	if err != nil {
		t.Fatalf("open SSE stream: %v", err)
	}
	defer sseResp.Body.Close()
	reader := bufio.NewReader(sseResp.Body)
	// The stream opens with a retry hint, then the connect event.
	for {
		line, rerr := reader.ReadString('\n')
		if rerr != nil {
			t.Fatalf("read SSE stream: %v", rerr)
		}
		if strings.HasPrefix(line, "data: ") {
			break
		}
	}
	sseEnded := make(chan error, 1)
	go func() {
		_, cerr := io.Copy(io.Discard, reader)
		sseEnded <- cerr
	}()

	// MJPEG: the handler commits its headers and then blocks in the frame
	// wait, so the response stays in flight without writing bytes.
	mjpegEnded := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			base+"/camera/"+oldSerial+"/stream", nil)
		if err != nil {
			mjpegEnded <- err
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			mjpegEnded <- err
			return
		}
		defer resp.Body.Close()
		_, err = io.Copy(io.Discard, resp.Body)
		mjpegEnded <- err
	}()
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-mjpegEnded:
		t.Fatalf("MJPEG request ended before the reload; the old stream was not held open: %v", err)
	default:
	}
	if !probeOK(base + "/livez") {
		t.Fatal("old listener stopped serving while the streams were open")
	}

	// Save a new printer list through the real PUT endpoint.
	var view struct {
		Config configui.View `json:"config"`
	}
	getResp, err := http.Get(base + "/config/api")
	if err != nil {
		t.Fatalf("GET /config/api: %v", err)
	}
	if err := json.NewDecoder(getResp.Body).Decode(&view); err != nil {
		t.Fatalf("decode /config/api: %v", err)
	}
	getResp.Body.Close()
	view.Config.Printers = []configui.PrinterView{{
		Serial:     newSerial,
		Model:      "P1S",
		Address:    "127.0.0.1:1",
		Username:   "bblp",
		AccessCode: "00001111",
	}}
	body, err := json.Marshal(view.Config)
	if err != nil {
		t.Fatal(err)
	}
	putReq, err := http.NewRequest(http.MethodPut, base+"/config/api", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	putReq.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT /config/api: %v", err)
	}
	putBody, _ := io.ReadAll(putResp.Body)
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT /config/api = %d, body: %s", putResp.StatusCode, putBody)
	}

	// Both old responses must end once the HTTP service stops for the restart.
	select {
	case <-sseEnded:
	case <-time.After(10 * time.Second):
		t.Fatal("SSE response stayed open across the reload")
	}
	select {
	case <-mjpegEnded:
	case <-time.After(10 * time.Second):
		t.Fatal("MJPEG response stayed open across the reload")
	}

	// With the old listener gone, the next /livez answer is the new server.
	waitFor(t, 15*time.Second, func() bool { return probeOK(base + "/livez") })
	var status struct {
		Upstreams map[string]bool `json:"upstreams"`
	}
	decodeStatus(t, base, &status)
	if connected, ok := status.Upstreams[newSerial]; !ok || connected {
		t.Fatalf("upstreams = %v, want the new printer %s present", status.Upstreams, newSerial)
	}
	if _, ok := status.Upstreams[oldSerial]; ok {
		t.Fatalf("upstreams = %v, want the old printer %s gone", status.Upstreams, oldSerial)
	}
	camResp, err := http.Get(base + "/camera/status")
	if err != nil {
		t.Fatalf("GET /camera/status: %v", err)
	}
	var camStatus struct {
		Printers []struct {
			Serial string `json:"serial"`
		} `json:"printers"`
	}
	if err := json.NewDecoder(camResp.Body).Decode(&camStatus); err != nil {
		t.Fatalf("decode /camera/status: %v", err)
	}
	camResp.Body.Close()
	if len(camStatus.Printers) != 1 || camStatus.Printers[0].Serial != newSerial {
		t.Fatalf("camera status tiles = %+v, want only %s", camStatus.Printers, newSerial)
	}
}
