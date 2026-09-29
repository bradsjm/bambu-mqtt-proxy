package camera

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/control"
	"bambu-mqtt-proxy/internal/printerview"
	"bambu-mqtt-proxy/internal/telemetry"
)

// testConnectivity is an empty upstream-status source for route tests.
type testConnectivity struct{}

// Status returns no connected printers for route tests.
func (testConnectivity) Status() map[string]bool  { return map[string]bool{} }
func (testConnectivity) Generation(string) uint64 { return 0 }

// discardLogger keeps test output quiet.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// cameraSpec builds a supported printer whose MQTT address is addr. Tests
// pair it with newFakeManager so the capture dials the fake's ephemeral
// address instead of the derived camera port.
func cameraSpec(serial, addr string) config.Printer {
	host, port, _ := net.SplitHostPort(addr)
	return config.Printer{
		Serial:             serial,
		Model:              "P1S",
		Address:            net.JoinHostPort(host, port),
		Username:           "bblp",
		Password:           "code",
		TLS:                true,
		InsecureSkipVerify: true,
	}
}

// TestSnapshotAndStreamLifecycle drives one fake camera through snapshot and
// stream paths and asserts one shared upstream session per printer.
func TestSnapshotAndStreamLifecycle(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, cameraSpec("S1", "127.0.0.1:8883"))

	// Snapshot before any frame: bounded wait must fail with 503 shape.
	frame, st := m.Snapshot("S1", func(c *capture) (*Frame, bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		return c.snapshot(ctx)
	})
	if st != StatusUnavailable || frame != nil {
		t.Fatalf("empty camera snapshot = %v, %v; want unavailable", frame, st)
	}

	// Publish two frames; the second supersedes the first.
	fc.frames <- jpeg(64)
	fc.frames <- jpeg(96)
	var got *Frame
	waitUntil(t, 5*time.Second, func() bool {
		got = m.Latest("S1")
		return got != nil && got.Seq >= 2
	})
	if !isJPEG(got.JPEG) || len(got.JPEG) != 96 {
		t.Fatalf("latest frame = %v", got)
	}

	// Fresh frame answers snapshot immediately from the shared buffer.
	frame, st = m.Snapshot("S1", func(c *capture) (*Frame, bool) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return c.snapshot(ctx)
	})
	if st != StatusOK || frame != got {
		t.Fatalf("fresh snapshot = %v, %v", frame, st)
	}

	// Concurrent waiters each observe the next published frame.
	var wg sync.WaitGroup
	results := make([]*Frame, 4)
	for i := range results {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			results[slot] = m.Wait("S1", ctx, got.Seq, 5*time.Second)
		}(i)
	}
	time.Sleep(100 * time.Millisecond) // let all waiters subscribe
	fc.frames <- jpeg(32)
	wg.Wait()
	for i, f := range results {
		if f == nil || f.Seq != got.Seq+1 {
			t.Fatalf("waiter %d = %v, want seq %d", i, f, got.Seq+1)
		}
	}
}

// TestUnsupportedModelNeverDials asserts eligibility refusal without any
// camera connection: unknown serial 404s, unsupported models 422.
func TestUnsupportedModelNeverDials(t *testing.T) {
	// A printer with a deliberately dead camera address still must not be
	// dialed for an unsupported model or unknown serial.
	dead := cameraSpec("DEAD", "127.0.0.1:1")
	dead.Model = "X1C" // RTSP family: unsupported by this implementation
	m := NewManager([]config.Printer{dead}, discardLogger())
	t.Cleanup(m.Close)

	if _, st := m.Snapshot("UNKNOWN", func(c *capture) (*Frame, bool) { return nil, false }); st != StatusUnknownSerial {
		t.Fatalf("unknown serial status = %v", st)
	}
	if _, st := m.Snapshot("DEAD", func(c *capture) (*Frame, bool) { return nil, false }); st != StatusUnsupportedModel {
		t.Fatalf("unsupported model status = %v", st)
	}
	m.mu.Lock()
	_, dialed := m.captures["DEAD"]
	m.mu.Unlock()
	if dialed {
		t.Fatal("unsupported model must not create a capture")
	}
}

// TestSerialPrefixInferenceGates asserts the eligibility path for configs
// without model fields: a P1S-prefix serial gets a capture, an X1C-prefix
// serial is refused without ever creating one.
func TestSerialPrefixInferenceGates(t *testing.T) {
	m := NewManager([]config.Printer{
		{Serial: "01S00C351100139", Address: "127.0.0.1:1", Username: "bblp", Password: "x"},
		{Serial: "00M09A123456789", Address: "127.0.0.1:1", Username: "bblp", Password: "x"},
	}, discardLogger())
	t.Cleanup(m.Close)

	if _, st := m.Snapshot("01S00C351100139", func(c *capture) (*Frame, bool) {
		return c.latest(), c.latest() != nil
	}); st != StatusUnavailable {
		t.Fatalf("model-less P1S serial = %v, want eligible (unavailable, not refused)", st)
	}
	m.mu.Lock()
	_, p1sCapture := m.captures["01S00C351100139"]
	_, x1cCapture := m.captures["00M09A123456789"]
	m.mu.Unlock()
	if !p1sCapture {
		t.Fatal("eligible serial must have a capture created")
	}
	if x1cCapture {
		t.Fatal("X1C-prefix serial must not create a capture")
	}
	if _, st := m.Snapshot("00M09A123456789", func(c *capture) (*Frame, bool) { return nil, false }); st != StatusUnsupportedModel {
		t.Fatalf("X1C-prefix serial = %v, want unsupported", st)
	}
}

// TestHandlerStatusCodes asserts the HTTP contract for snapshot.
func TestHandlerStatusCodes(t *testing.T) {
	fc := newFakeCamera(t)
	printers := []config.Printer{
		cameraSpec("S1", "127.0.0.1:8883"),
		{Serial: "P1PONLY", Model: "a1 mini", Address: "127.0.0.1:8883", Username: "bblp", Password: "x"},
		{Serial: "XSERIES", Model: "X1C", Address: "127.0.0.1:1", Username: "bblp", Password: "x"},
	}
	m := newFakeManager(t, fc, printers...)

	get := func(serial string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/camera/"+serial+"/snapshot", nil)
		req.SetPathValue("serial", serial)
		w := httptest.NewRecorder()
		m.handleSnapshot(w, req)
		return w
	}
	if code := get("NOPE").Code; code != 404 {
		t.Fatalf("unknown serial = %d, want 404", code)
	}
	if code := get("XSERIES").Code; code != 422 {
		t.Fatalf("unsupported model = %d, want 422", code)
	}

	// A1MINI normalization is supported; it will dial and fail (dead addr)
	// but must be refused as eligible-model-only after the 15s bound — so
	// assert the request completes with 503 quickly via short context path:
	// here we assert eligibility created a capture, not the timeout itself.
	if _, st := m.Snapshot("P1PONLY", func(c *capture) (*Frame, bool) {
		return c.latest(), c.latest() != nil
	}); st != StatusUnavailable {
		t.Fatalf("eligible model without frame = %v, want unavailable", st)
	}

	// Full happy path through the HTTP handler with a live fake camera.
	// Warm the capture through a handler-equivalent acquire, as a real
	// viewer would; Latest() itself never starts a capture.
	m.Acquire("S1")
	fc.frames <- jpeg(48)
	waitUntil(t, 5*time.Second, func() bool { return m.Latest("S1") != nil })
	m.Release("S1")
	w := get("S1")
	if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), "image/jpeg") {
		t.Fatalf("snapshot = %d %s", w.Code, w.Header().Get("Content-Type"))
	}
	if !isJPEG(w.Body.Bytes()) {
		t.Fatal("snapshot body is not JPEG")
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("snapshot must be no-store")
	}
}

// TestStreamWritesMultipart asserts stream framing end to end.
func TestStreamWritesMultipart(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, cameraSpec("S1", "127.0.0.1:8883"))

	m.Acquire("S1")
	fc.frames <- jpeg(40)
	waitUntil(t, 5*time.Second, func() bool { return m.Latest("S1") != nil })

	// A streaming recorder that implements Flusher.
	rec := newStreamingRecorder()
	req := httptest.NewRequest("GET", "/camera/S1/stream", nil)
	req.SetPathValue("serial", "S1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.handleStream(rec, req)
	}()
	time.Sleep(100 * time.Millisecond) // let the handler reach Wait
	fc.frames <- jpeg(56)
	var body string
	waitUntil(t, 5*time.Second, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		body = rec.buf.String()
		return strings.Count(body, "image/jpeg") >= 2
	})
	// Re-read the accumulated body under the recorder lock: the closure's
	// copy predates the second part when the condition fired mid-write.
	rec.mu.Lock()
	body = rec.buf.String()
	rec.mu.Unlock()
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "multipart/x-mixed-replace") {
		t.Fatalf("stream content type = %q", got)
	}
	if !strings.Contains(body, "--frame") {
		t.Fatalf("stream boundary missing in body: %q", body[:min(80, len(body))])
	}
	if got, want := strings.Count(body, "--frame\r\n"), strings.Count(body, "Content-Type: image/jpeg"); got != want {
		t.Fatalf("multipart has %d boundaries for %d JPEG parts; every part must be boundary-prefixed", got, want)
	}
	// Client hangup ends the handler promptly: mark the recorder closed and
	// publish one more frame so the handler's next write fails immediately
	// rather than idling in the frame wait.
	rec.closed = true
	fc.frames <- jpeg(72)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream handler did not return after client hangup")
	}
}

// TestCamWallRoutesServeEmbeddedWall asserts that /camwall serves the
// embedded wall, /overlay is gone, and /camera/status carries display state.
func TestCamWallRoutesServeEmbeddedWall(t *testing.T) {
	printers := []config.Printer{
		{
			Serial:   "01S00C351100139",
			Name:     "Garage",
			Model:    "P1S",
			Address:  "127.0.0.1:8883",
			Username: "bblp",
			Password: "secret-test-only",
		},
		{
			Serial:   "00M09A123456789",
			Model:    "X1C",
			Address:  "127.0.0.1:8884",
			Username: "bblp",
			Password: "another-secret",
		},
	}
	manager := NewManager(printers, discardLogger())
	state := telemetry.NewCache(printers, discardLogger())
	activities := activity.New(printers)
	activities.Record("01S00C351100139", "print_started", activity.Info, "Print started")
	state.Observe("01S00C351100139", []byte(`{"print":{"chamber_temper":5.0,"print_error":50348044,"hms":[{"attr":50331904,"code":65543},{"attr":117473296,"code":65543}]}}`))
	state.Observe("00M09A123456789", []byte(`{"print":{"chamber_temper":24.0}}`))
	renderer := NewStatusRenderer(manager, state, testConnectivity{})
	renderer.SetActivity(activities)
	mux := http.NewServeMux()
	renderer.RegisterStatus(mux)

	wallReq := httptest.NewRequest(http.MethodGet, "/camwall", nil)
	wallResp := httptest.NewRecorder()
	mux.ServeHTTP(wallResp, wallReq)
	if wallResp.Code != http.StatusOK {
		t.Fatalf("GET /camwall = %d, want 200", wallResp.Code)
	}
	if wallResp.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("GET /camwall Cache-Control = %q", wallResp.Header().Get("Cache-Control"))
	}
	if !strings.HasPrefix(wallResp.Header().Get("Content-Type"), "text/html") {
		t.Errorf("GET /camwall Content-Type = %q", wallResp.Header().Get("Content-Type"))
	}
	if !strings.Contains(wallResp.Body.String(), "/camera/events") {
		t.Fatal("embedded wall does not subscribe to camera status events")
	}
	for path, want := range map[string]string{"/favicon.ico": "image/x-icon", "/apple-touch-icon.png": "image/png"} {
		if !strings.Contains(wallResp.Body.String(), `href="`+path+`"`) {
			t.Errorf("embedded wall does not link %s", path)
		}
		iconResp := httptest.NewRecorder()
		mux.ServeHTTP(iconResp, httptest.NewRequest(http.MethodGet, path, nil))
		if iconResp.Code != http.StatusOK || iconResp.Header().Get("Content-Type") != want || iconResp.Body.Len() == 0 {
			t.Errorf("GET %s = %d %q (%d bytes), want 200 %q", path, iconResp.Code, iconResp.Header().Get("Content-Type"), iconResp.Body.Len(), want)
		}
	}

	overlayResp := httptest.NewRecorder()
	mux.ServeHTTP(overlayResp, httptest.NewRequest(http.MethodGet, "/overlay", nil))
	if overlayResp.Code != http.StatusNotFound {
		t.Fatalf("GET /overlay = %d, want 404 (endpoint removed)", overlayResp.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/camera/status", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var payload struct {
		Printers []Tile `json:"printers"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode /camera/status: %v", err)
	}
	if len(payload.Printers) != 2 {
		t.Fatalf("camera status tiles = %+v", payload.Printers)
	}
	tiles := make(map[string]Tile, len(payload.Printers))
	for _, tile := range payload.Printers {
		tiles[tile.Serial] = tile
	}
	p1s, ok := tiles["01S00C351100139"]
	if !ok || !p1s.CameraOK || p1s.ChamberTemp != nil {
		t.Fatalf("P1S tile must be camera-supported and omit chamber temp: %+v", p1s)
	}
	if p1s.Name != "Garage" || p1s.PrintError == nil || p1s.PrintError.ID != "0300_400C" || p1s.Freshness.LastReportAgeSeconds == nil || len(p1s.HMS) != 2 {
		t.Fatalf("P1S tile name/errors/report age wrong: %+v", p1s)
	}
	if pe := p1s.PrintError; pe.Text != "Print Cancelled" || pe.Severity != "info" || pe.Fix == "" ||
		pe.URL != "https://printara3d.com/tools/bambu-error-codes/hms-0300-400c/" {
		t.Fatalf("P1S print error description wrong: %+v", p1s)
	}
	// The unknown print-module alert keeps its code and gains only the
	// reference link; the AMS alert carries the dataset description.
	if p1s.HMS[0] != (printerview.Alert{ID: "HMS_0300_0100_0001_0007", Severity: "fatal",
		URL: "https://printara3d.com/tools/bambu-error-codes/?code=0300-0100"}) {
		t.Fatalf("unknown HMS alert must keep code and reference link: %+v", p1s.HMS[0])
	}
	hms := p1s.HMS[1]
	if hms.ID != "HMS_0700_8010_0001_0007" || hms.Severity != "fatal" || hms.Text != "AMS Motor Overload" ||
		hms.Fix == "" || hms.URL != "https://printara3d.com/tools/bambu-error-codes/hms-0700-8010/" {
		t.Fatalf("known HMS alert must carry description: %+v", p1s.HMS[1])
	}
	if len(p1s.Activity) != 1 || p1s.Activity[0].Kind != "print_started" {
		t.Fatalf("P1S tile activity = %+v", p1s.Activity)
	}
	x1c, ok := tiles["00M09A123456789"]
	if !ok || x1c.CameraOK || x1c.ChamberTemp == nil || *x1c.ChamberTemp != 24 {
		t.Fatalf("X1C tile must retain its valid chamber temp: %+v", x1c)
	}
	if strings.Contains(w.Body.String(), "secret-test-only") || strings.Contains(w.Body.String(), "another-secret") || strings.Contains(w.Body.String(), "127.0.0.1") {
		t.Fatal("Cam Wall status leaked credentials or printer address")
	}
}

func TestChangeKeyIgnoresActivityAge(t *testing.T) {
	first := []Tile{{View: printerview.View{Serial: "S1"}, Activity: []activity.Entry{{ID: 1, AgeSeconds: 2}}}}
	second := []Tile{{View: printerview.View{Serial: "S1"}, Activity: []activity.Entry{{ID: 1, AgeSeconds: 8}}}}
	a, err := changeKey(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := changeKey(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("activity aging changed the SSE key: %s != %s", a, b)
	}
	if first[0].Activity[0].AgeSeconds != 2 || second[0].Activity[0].AgeSeconds != 8 {
		t.Fatal("changeKey mutated its input activity ages")
	}
}

// TestCameraEventsStreamsChanges reads the SSE stream: one event on
// connect, then another after a report changes display state.
func TestCameraEventsStreamsChanges(t *testing.T) {
	printers := []config.Printer{{Serial: "01S00C351100139", Address: "127.0.0.1:8883", Password: "secret-test-only"}}
	state := telemetry.NewCache(printers, discardLogger())
	renderer := NewStatusRenderer(NewManager(printers, discardLogger()), state, testConnectivity{})
	mux := http.NewServeMux()
	renderer.RegisterStatus(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/camera/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /camera/events: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}

	lines := bufio.NewScanner(resp.Body)
	nextEvent := func() statusPayload {
		t.Helper()
		for lines.Scan() {
			data, ok := strings.CutPrefix(lines.Text(), "data: ")
			if !ok {
				continue
			}
			var p statusPayload
			if err := json.Unmarshal([]byte(data), &p); err != nil {
				t.Fatalf("decode event %q: %v", data, err)
			}
			return p
		}
		t.Fatalf("stream ended before an event: %v", lines.Err())
		return statusPayload{}
	}

	first := nextEvent()
	if len(first.Printers) != 1 || first.Printers[0].PrintState != nil {
		t.Fatalf("initial event = %+v", first)
	}
	state.Observe("01S00C351100139", []byte(`{"print":{"gcode_state":"RUNNING"}}`))
	if second := nextEvent(); second.Printers[0].PrintState == nil || *second.Printers[0].PrintState != "RUNNING" {
		t.Fatalf("change event = %+v", second)
	}
}

// TestCameraStatusProjectsFilamentAndStage pins the optional wire contract
// on /camera/status: known stages map with the idle sentinel absent, AMS
// units sort by id with stable slots, occupancy stays honest, the active
// selection carries only supported encodings, and the external spool is
// its own object.
func TestCameraStatusProjectsFilamentAndStage(t *testing.T) {
	printers := []config.Printer{{Serial: "01S00C351100139", Address: "127.0.0.1:8883", Password: "secret-test-only"}}
	state := telemetry.NewCache(printers, discardLogger())
	renderer := NewStatusRenderer(NewManager(printers, discardLogger()), state, testConnectivity{})
	state.Observe("01S00C351100139", []byte(`{"print":{"stg_cur":4,"ams":{"tray_now":"0","ams":[
		{"id":"1","tray":[{"id":"3","tray_type":"PETG","tray_color":"FF8000FF","remain":25,"state":3}]},
		{"id":"0","humidity_raw":12,"tray":[{"id":"0","tray_type":"PLA","tray_color":"FFFF00FF","remain":64,"state":3}]}]},
		"vt_tray":{"id":"254","tray_type":"ABS","tray_color":"000000FF"}}}`))
	mux := http.NewServeMux()
	renderer.RegisterStatus(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	getTile := func() Tile {
		t.Helper()
		resp, err := http.Get(srv.URL + "/camera/status")
		if err != nil {
			t.Fatalf("GET /camera/status: %v", err)
		}
		defer resp.Body.Close()
		var payload statusPayload
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			t.Fatalf("decode /camera/status: %v", err)
		}
		if len(payload.Printers) != 1 {
			t.Fatalf("printers = %+v", payload.Printers)
		}
		return payload.Printers[0]
	}

	tile := getTile()
	if tile.Stage == nil || *tile.Stage != "changing_filament" {
		t.Fatalf("stage = %v, want the mapped stg_cur 4", tile.Stage)
	}
	if len(tile.AMS) != 2 || tile.AMS[0].ID != 0 || tile.AMS[1].ID != 1 {
		t.Fatalf("ams units = %+v, want ids 0 then 1", tile.AMS)
	}
	unit := tile.AMS[0]
	if unit.Humidity == nil || *unit.Humidity != 12 {
		t.Fatalf("humidity = %v, want 12", unit.Humidity)
	}
	slot := unit.Slots[0]
	if slot.Loaded == nil || !*slot.Loaded || slot.Material != "PLA" || slot.Color != "FFFF00FF" ||
		slot.Remain == nil || *slot.Remain != 64 || !slot.Active {
		t.Fatalf("selected slot = %+v, want active PLA 64%%", slot)
	}
	other := tile.AMS[1].Slots[3]
	if other.Loaded == nil || !*other.Loaded || other.Material != "PETG" || *other.Remain != 25 || other.Active {
		t.Fatalf("unselected slot = %+v, want loaded PETG without selection", other)
	}
	if tile.ExtSpool == nil || tile.ExtSpool.Loaded == nil || !*tile.ExtSpool.Loaded ||
		tile.ExtSpool.Material != "ABS" || tile.ExtSpool.Active {
		t.Fatalf("external spool = %+v, want loaded and unselected", tile.ExtSpool)
	}

	// The wire shape itself: optional keys carry the documented names and
	// stay absent when there is nothing to say.
	var raw struct {
		Printers []map[string]any `json:"printers"`
	}
	resp, err := http.Get(srv.URL + "/camera/status")
	if err != nil {
		t.Fatalf("GET /camera/status: %v", err)
	}
	err = json.NewDecoder(resp.Body).Decode(&raw)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("decode raw status: %v", err)
	}
	wire := raw.Printers[0]
	units, _ := wire["ams"].([]any)
	unitWire, _ := units[0].(map[string]any)
	for _, key := range []string{"id", "humidity", "slots"} {
		if _, ok := unitWire[key]; !ok {
			t.Fatalf("ams unit missing %q: %v", key, unitWire)
		}
	}
	slots, _ := unitWire["slots"].([]any)
	slotWire, _ := slots[0].(map[string]any)
	for _, key := range []string{"id", "loaded", "active", "material", "color", "remain"} {
		if _, ok := slotWire[key]; !ok {
			t.Fatalf("slot missing %q: %v", key, slotWire)
		}
	}
	extWire, ok := wire["ext_spool"].(map[string]any)
	if !ok || extWire["material"] != "ABS" {
		t.Fatalf("ext_spool = %v, want the loaded spool object", wire["ext_spool"])
	}

	// The idle sentinel and unknown stage ids leave stage null, so
	// consumers keep the printer state.
	state.Observe("01S00C351100139", []byte(`{"print":{"stg_cur":255}}`))
	if tile = getTile(); tile.Stage != nil {
		t.Fatalf("idle sentinel stage = %q, want null", *tile.Stage)
	}
	state.Observe("01S00C351100139", []byte(`{"print":{"stg_cur":99}}`))
	if tile = getTile(); tile.Stage != nil {
		t.Fatalf("unknown stage = %q, want null", *tile.Stage)
	}
}

// streamingRecorder is an httptest.ResponseRecorder with Flush and a
// controllable client-gone state.
type streamingRecorder struct {
	*httptest.ResponseRecorder
	mu     sync.Mutex
	buf    *strings.Builder
	closed bool
}

func newStreamingRecorder() *streamingRecorder {
	b := &strings.Builder{}
	r := httptest.NewRecorder()
	return &streamingRecorder{ResponseRecorder: r, buf: b}
}

func (s *streamingRecorder) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, fmt.Errorf("client gone")
	}
	return s.buf.Write(p)
}

func (s *streamingRecorder) Flush() {}

// TestAuthPayloadShape pins the 80-byte protocol format.
func TestAuthPayloadShape(t *testing.T) {
	p := authPayload("bblp", "12345678")
	if len(p) != 80 {
		t.Fatalf("payload = %d bytes, want 80", len(p))
	}
	if binary.LittleEndian.Uint32(p[0:4]) != 0x40 || binary.LittleEndian.Uint32(p[4:8]) != 0x3000 {
		t.Fatal("magic or command wrong")
	}
	if string(p[16:20]) != "bblp" || string(p[48:56]) != "12345678" {
		t.Fatalf("credential fields wrong: %q %q", p[16:48], p[48:80])
	}
}

// TestFrameValidation rejects truncated or oversized frames.
func TestFrameValidation(t *testing.T) {
	var header [16]byte
	binary.LittleEndian.PutUint32(header[0:4], 1<<24)
	if _, err := readFrame(bufio.NewReader(strings.NewReader(string(header[:])))); err == nil {
		t.Fatal("oversized frame must fail")
	}
	binary.LittleEndian.PutUint32(header[0:4], 4)
	r := bufio.NewReader(strings.NewReader(string(header[:]) + "\xff\xd8\xff\xd9"))
	f, err := readFrame(r)
	if err != nil || !isJPEG(f.JPEG) {
		t.Fatalf("minimal jpeg = %v, %v", f, err)
	}
	if len(f.Header) != frameHeaderLen || string(f.Header) != string(header[:]) {
		t.Fatal("readFrame must preserve the printer's raw header bytes")
	}
}

// fakeCommander accepts every command for a connected S1.
type fakeCommander struct{ sent []string }

func (f *fakeCommander) Status() map[string]bool  { return map[string]bool{"S1": true} }
func (f *fakeCommander) Generation(string) uint64 { return 1 }
func (f *fakeCommander) PausePrint(string, uint64) error {
	f.sent = append(f.sent, "pause")
	return nil
}
func (f *fakeCommander) ResumePrint(string, uint64) error {
	f.sent = append(f.sent, "resume")
	return nil
}
func (f *fakeCommander) StopPrint(string, uint64) error {
	f.sent = append(f.sent, "stop")
	return nil
}
func (f *fakeCommander) SetSpeedProfile(string, uint64, int) error {
	f.sent = append(f.sent, "speed")
	return nil
}
func (f *fakeCommander) SetChamberLight(string, uint64, bool) error {
	f.sent = append(f.sent, "light")
	return nil
}

func TestControlEndpoint(t *testing.T) {
	printers := []config.Printer{{Serial: "S1", Name: "Shop"}}
	state := telemetry.NewCache(printers, discardLogger())
	cmd := &fakeCommander{}
	renderer := NewStatusRenderer(NewManager(printers, discardLogger()), state, testConnectivity{})
	renderer.SetControl(control.New(cmd, state, nil))
	mux := http.NewServeMux()
	renderer.RegisterStatus(mux)
	post := func(body string, header map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/control/S1", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range header {
			req.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w
	}
	if w := post(`{"action":"stop"}`, nil); w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"serial":"S1","action":"stop","sent":true}` {
		t.Fatalf("stop = %d %s", w.Code, w.Body)
	}
	if w := post(`{"action":"heat"}`, nil); w.Code != 400 {
		t.Fatalf("heat = %d", w.Code)
	}
	if w := post(`{"action":"stop","temp":200}`, nil); w.Code != 400 {
		t.Fatalf("unknown field = %d", w.Code)
	}
	if w := post(`{"action":"pause"}`, nil); w.Code != 409 {
		t.Fatalf("pause on idle = %d", w.Code)
	}
	if w := post(`{"action":"stop"}`, map[string]string{"Sec-Fetch-Site": "cross-site", "Origin": "https://evil.example"}); w.Code != 403 {
		t.Fatalf("cross-site = %d", w.Code)
	}
	if len(cmd.sent) != 1 {
		t.Fatalf("sent = %v, want only the first stop", cmd.sent)
	}
}
