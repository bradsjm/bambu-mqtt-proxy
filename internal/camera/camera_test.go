package camera

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// discardLogger keeps test output quiet.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// cameraSpec builds a supported printer whose MQTT address is addr; the
// capture derives the camera endpoint 127.0.0.1:6000 from that host.
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
	m := NewManager([]config.Printer{cameraSpec("S1", "127.0.0.1:8883")}, discardLogger())
	t.Cleanup(m.Close)

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
// without model fields: a P1S-prefix serial gets a capture (the gateway
// dialed 127.0.0.1:6000 and failed fast), an X1C-prefix serial is refused
// without ever creating one.
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
	m := NewManager(printers, discardLogger())
	t.Cleanup(m.Close)

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
	m := NewManager([]config.Printer{cameraSpec("S1", "127.0.0.1:8883")}, discardLogger())
	t.Cleanup(m.Close)

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
	if err != nil || !isJPEG(f) {
		t.Fatalf("minimal jpeg = %v, %v", f, err)
	}
}
