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
	"bambu-mqtt-proxy/internal/telemetry"
)

// testConnectivity is an empty upstream-status source for route tests.
type testConnectivity struct{}

// Status returns no connected printers for route tests.
func (testConnectivity) Status() map[string]bool { return map[string]bool{} }

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
	state.Observe("01S00C351100139", []byte(`{"print":{"chamber_temper":5.0,"print_error":50348044,"hms":[{"attr":50331904,"code":65543}]}}`))
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
	if p1s.Name != "Garage" || p1s.PrintError != "0300_400C" || p1s.ReportAge == nil ||
		len(p1s.HMS) != 1 || p1s.HMS[0] != (HMS{Code: "HMS_0300_0100_0001_0007", Severity: "fatal"}) {
		t.Fatalf("P1S tile name/errors/report age wrong: %+v", p1s)
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
	first := []Tile{{Serial: "S1", Activity: []activity.Entry{{ID: 1, AgeSeconds: 2}}}}
	second := []Tile{{Serial: "S1", Activity: []activity.Entry{{ID: 1, AgeSeconds: 8}}}}
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
	if len(first.Printers) != 1 || first.Printers[0].State != "" {
		t.Fatalf("initial event = %+v", first)
	}
	state.Observe("01S00C351100139", []byte(`{"print":{"gcode_state":"RUNNING"}}`))
	if second := nextEvent(); second.Printers[0].State != "RUNNING" {
		t.Fatalf("change event = %+v", second)
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
