package camera

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// newTestRawServer injects a loopback listener with a throwaway certificate
// so raw server tests never fight the production server over the fixed
// camera port 6000.
func newTestRawServer(t *testing.T, m *Manager) (*RawServer, string) {
	t.Helper()
	cert, err := selfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	srv := NewRawServer(m, discardLogger())
	srv.tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	srv.serve(ln)
	return srv, ln.Addr().String()
}

// rawDial opens a TLS camera client session against addr and authenticates
// with username bblp and the given access code.
func rawDial(t *testing.T, addr, accessCode string) net.Conn {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("camera client dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(authPayload("bblp", accessCode)); err != nil {
		t.Fatalf("camera client auth write: %v", err)
	}
	return conn
}

// readRawFrame reads one raw frame: the 16-byte header, then the payload.
func readRawFrame(r *bufio.Reader) ([]byte, []byte, error) {
	var header [frameHeaderLen]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, nil, err
	}
	length := binary.LittleEndian.Uint32(header[0:4])
	if length == 0 || length > maxPayloadLen {
		return header[:], nil, io.ErrUnexpectedEOF
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return header[:], nil, err
	}
	return header[:], payload, nil
}

// rawSessionCount reports the server's live session count.
func rawSessionCount(srv *RawServer) int {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return len(srv.sockets)
}

// captureConsumers reports one capture's live consumer count.
func captureConsumers(m *Manager, serial string) int {
	m.mu.Lock()
	c, ok := m.captures[serial]
	m.mu.Unlock()
	if !ok {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumers
}

// paddedField builds a NUL-padded 32-byte authentication field.
func paddedField(value string) []byte {
	field := make([]byte, 32)
	copy(field, value)
	return field
}

// fullCode returns a full-width 32-byte access code: the field fills
// exactly, leaving no room for NUL padding.
func fullCode() string {
	return string(bytes.Repeat([]byte("c"), 32))
}

// rawPrinterSpec builds a supported printer with its own access code.
func rawPrinterSpec(serial, accessCode, model string) config.Printer {
	return config.Printer{
		Serial:             serial,
		Model:              model,
		Address:            "127.0.0.1:8883",
		Username:           "bblp",
		Password:           accessCode,
		TLS:                true,
		InsecureSkipVerify: true,
	}
}

// TestRawAuthCredentialMatch pins the credential matcher: an exact
// NUL-padded username and access code pair resolves to the eligible printer
// only; wrong fields, trailing garbage, and ineligible printers never match.
func TestRawAuthCredentialMatch(t *testing.T) {
	m := NewManager([]config.Printer{
		rawPrinterSpec("01S00C00000000A", "code", "P1S"),
		// A full-width access code leaves no room for NUL padding.
		rawPrinterSpec("01P00A00000000C", string(bytes.Repeat([]byte("c"), 32)), "P1P"),
		// Same credentials as the eligible printer but an ineligible model:
		// even a deliberate collision must never route a raw session here.
		rawPrinterSpec("XSERIES", "code", "X1C"),
	}, discardLogger())

	p, ok := m.matchCameraCredentials(paddedField("bblp"), paddedField("code"))
	if !ok || p.Serial != "01S00C00000000A" {
		t.Fatalf("valid credentials = %v, %v; want the eligible printer", p, ok)
	}
	// A 32-byte code with no NUL padding is still an exact match.
	if p, ok := m.matchCameraCredentials(paddedField("bblp"), []byte(fullCode())); !ok || p.Serial != "01P00A00000000C" {
		t.Fatal("full-width unpadded code must match")
	}
	for name, tc := range map[string]struct {
		username string
		code     string
		garbage  bool
	}{
		"wrong code":      {"bblp", "wrong", false},
		"wrong username":  {"bltp", "code", false},
		"case mismatch":   {"BBLP", "code", false},
		"field garbage":   {"bblp", "code", true},
		"unknown printer": {"bblp", "no-such-code", false},
	} {
		username := paddedField(tc.username)
		if tc.garbage {
			username[5] = 0x01
		}
		if _, ok := m.matchCameraCredentials(username, paddedField(tc.code)); ok {
			t.Fatalf("%s must not match", name)
		}
	}
}

// TestRawServerRoutesDistinctCodes routes two concurrent clients with two
// distinct access codes to their own printers' captures: each client
// receives only its own printer's frames, and each capture sees one session.
func TestRawServerRoutesDistinctCodes(t *testing.T) {
	fcA, fcB := newFakeCamera(t), newFakeCamera(t)
	specA := rawPrinterSpec("01S00C00000000A", "code-aaa", "P1S")
	specB := rawPrinterSpec("01P00A00000000B", "code-bbb", "P1P")
	m := NewManager([]config.Printer{specA, specB}, discardLogger())
	t.Cleanup(m.Close)
	m.cameraEndpoint = func(p config.Printer) (string, error) {
		if p.Serial == specA.Serial {
			return fcA.addr, nil
		}
		return fcB.addr, nil
	}
	_, addr := newTestRawServer(t, m)
	ca := rawDial(t, addr, "code-aaa")
	cb := rawDial(t, addr, "code-bbb")
	payloadA, payloadB := jpeg(64), jpeg(96)
	fcA.frames <- payloadA
	fcB.frames <- payloadB

	_ = ca.SetReadDeadline(time.Now().Add(5 * time.Second))
	_ = cb.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, gotA, err := readRawFrame(bufio.NewReader(ca))
	if err != nil {
		t.Fatalf("client A read: %v", err)
	}
	_, gotB, err := readRawFrame(bufio.NewReader(cb))
	if err != nil {
		t.Fatalf("client B read: %v", err)
	}
	if !bytes.Equal(gotA, payloadA) || !bytes.Equal(gotB, payloadB) {
		t.Fatalf("routing crossed streams: A got %d bytes, B got %d bytes", len(gotA), len(gotB))
	}
	if n := fcA.sessions.Load(); n != 1 {
		t.Fatalf("fake camera A served %d sessions, want 1", n)
	}
	if n := fcB.sessions.Load(); n != 1 {
		t.Fatalf("fake camera B served %d sessions, want 1", n)
	}
}

// TestRawServerRejectsBadAuth closes unmatched, malformed, and unsupported
// clients with no application reply and no capture acquisition.
func TestRawServerRejectsBadAuth(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc,
		rawPrinterSpec("01S00C00000000A", "right-code", "P1S"),
		rawPrinterSpec("XSERIES", "x-code", "X1C"), // ineligible model
	)
	srv, addr := newTestRawServer(t, m)

	cases := map[string][]byte{}
	mutate := func(name string, f func(p []byte)) {
		p := authPayload("bblp", "right-code")
		f(p)
		cases[name] = p
	}
	mutate("wrong code", func(p []byte) { copy(p[48:80], "wrong-code") })
	mutate("wrong username", func(p []byte) { copy(p[16:48], "bltp") })
	mutate("bad magic", func(p []byte) { p[0] = 0x41 })
	mutate("bad command", func(p []byte) { p[4] = 0x01 })
	mutate("garbage in username field", func(p []byte) { p[20] = 0x01 })
	cases["code of ineligible printer"] = authPayload("bblp", "x-code")
	cases["empty fields"] = make([]byte, authPayloadLen)

	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()
			if _, err := conn.Write(payload); err != nil {
				t.Fatalf("auth write: %v", err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			// The server must close without sending a single application
			// byte; any read result with content is a protocol violation.
			buf := make([]byte, 64)
			n, rerr := conn.Read(buf)
			if n != 0 {
				t.Fatalf("refused session received %d application bytes", n)
			}
			if rerr == nil {
				t.Fatal("refused session stayed open")
			}
		})
	}
	// No client acquired a capture: nothing dialed the fake camera and the
	// manager holds no captures for the matched or unsupported printers.
	waitUntil(t, 2*time.Second, func() bool { return rawSessionCount(srv) == 0 })
	m.mu.Lock()
	count := len(m.captures)
	m.mu.Unlock()
	if count != 0 {
		t.Fatalf("bad auth created %d captures, want 0", count)
	}
}

// TestRawServerAcceptsFragmentedAuth proves the exact 80-byte read tolerates
// a client that trickles the authentication payload in small chunks.
func TestRawServerAcceptsFragmentedAuth(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, rawPrinterSpec("01S00C00000000A", "code", "P1S"))
	_, addr := newTestRawServer(t, m)

	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	payload := authPayload("bblp", "code")
	for start := 0; start < authPayloadLen; start += 16 {
		if _, err := conn.Write(payload[start : start+16]); err != nil {
			t.Fatalf("chunk write: %v", err)
		}
		time.Sleep(30 * time.Millisecond)
	}
	frame := jpeg(48)
	fc.frames <- frame
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, got, err := readRawFrame(bufio.NewReader(conn))
	if err != nil || !bytes.Equal(got, frame) {
		t.Fatalf("fragmented client = %d bytes, %v", len(got), err)
	}
}

// TestRawServerPreservesHeaderBytes proves frames are replayed byte-for-byte:
// the raw 16-byte header, including non-canonical marker bytes, arrives
// untouched ahead of the untouched JPEG payload.
func TestRawServerPreservesHeaderBytes(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, rawPrinterSpec("01S00C00000000A", "code", "P1S"))
	_, addr := newTestRawServer(t, m)
	conn := rawDial(t, addr, "code")
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	sequence := []rawTestFrame{
		{header: frameHeaderWithMarkers(0x40, 1234), payload: jpeg(1234)},
		{header: frameHeaderWithMarkers(0xFF, 4), payload: jpeg(4)},
	}
	for _, f := range sequence {
		fc.custom <- f
		gotHeader, gotPayload, err := readRawFrame(reader)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(gotHeader, f.header) || !bytes.Equal(gotPayload, f.payload) {
			t.Fatalf("frame bytes changed: header % X, payload %d bytes", gotHeader, len(gotPayload))
		}
	}
	// The shared cache keeps the raw header for later sessions too.
	if cached := m.Latest("01S00C00000000A"); cached == nil || !bytes.Equal(cached.Header, sequence[1].header) {
		t.Fatalf("cached frame lost its raw header: %v", cached)
	}
}

// frameHeaderWithMarkers builds a plausible printer frame header: the JPEG
// length in the first uint32, marker bytes in the rest — bytes the raw
// server must never synthesize or strip.
func frameHeaderWithMarkers(marker byte, length int) []byte {
	header := make([]byte, frameHeaderLen)
	binary.LittleEndian.PutUint32(header[0:4], uint32(length))
	for i := 4; i < frameHeaderLen; i++ {
		header[i] = marker + byte(i)
	}
	return header
}

// TestRawServerAuthDeadline proves the shared handshake and authentication
// budget drops stalled clients without application bytes, and that a
// healthy authenticated session keeps streaming far beyond the shortened
// budget because the deadline is cleared on success.
func TestRawServerAuthDeadline(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, rawPrinterSpec("01S00C00000000A", "code", "P1S"))

	// Restore after server cleanup; see TestRawServerSkipsStaleCache.
	oldBudget := rawAuthBudget
	rawAuthBudget = 500 * time.Millisecond
	t.Cleanup(func() { rawAuthBudget = oldBudget })
	_, addr := newTestRawServer(t, m)

	// A client that never starts the TLS handshake…
	stalledHandshake, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("handshake dial: %v", err)
	}
	defer stalledHandshake.Close()
	// …and a client that finishes the handshake but stalls before the
	// exact 80-byte payload both belong to the same budget.
	stalledAuth, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("auth dial: %v", err)
	}
	defer stalledAuth.Close()

	// The healthy session authenticates well inside the budget and is
	// still served a frame long after the budget has passed.
	live := rawDial(t, addr, "code")
	_ = live.SetReadDeadline(time.Now().Add(5 * time.Second))
	time.Sleep(1200 * time.Millisecond)
	frame := jpeg(64)
	fc.frames <- frame
	_, payload, err := readRawFrame(bufio.NewReader(live))
	if err != nil || !bytes.Equal(payload, frame) {
		t.Fatalf("healthy session beyond the auth budget = %d bytes, %v", len(payload), err)
	}

	for _, stalled := range []struct {
		name string
		conn net.Conn
	}{
		{"stalled handshake", stalledHandshake},
		{"stalled auth", stalledAuth},
	} {
		_ = stalled.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		buf := make([]byte, 64)
		n, rerr := stalled.conn.Read(buf)
		if n != 0 {
			t.Fatalf("%s received %d application bytes", stalled.name, n)
		}
		if rerr == nil {
			t.Fatalf("%s survived the auth budget", stalled.name)
		}
	}
}

// TestRawServerSharesCapture proves concurrent clients with the same access
// code share one camera connection: the fake serves exactly one session and
// both clients receive the newest frame.
func TestRawServerSharesCapture(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, rawPrinterSpec("01S00C00000000A", "code", "P1S"))
	_, addr := newTestRawServer(t, m)

	ca := rawDial(t, addr, "code")
	cb := rawDial(t, addr, "code")
	time.Sleep(150 * time.Millisecond) // let both sessions reach their waits
	final := jpeg(96)
	fc.frames <- jpeg(32)
	fc.frames <- jpeg(64)
	fc.frames <- final

	readToFinal := func(conn net.Conn) {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(conn)
		for range 10 {
			_, payload, err := readRawFrame(reader)
			if err != nil {
				t.Fatalf("shared client read: %v", err)
			}
			if bytes.Equal(payload, final) {
				return
			}
		}
		t.Fatal("shared client never received the newest frame")
	}
	readToFinal(ca)
	readToFinal(cb)
	if n := fc.sessions.Load(); n != 1 {
		t.Fatalf("two clients produced %d camera sessions, want exactly 1", n)
	}
	// Both sessions hold their persistent references for their lifetime;
	// Manager.Wait adds transient references on top, so only the floor of
	// two is deterministic while the clients stay connected.
	if consumers := captureConsumers(m, "01S00C00000000A"); consumers < 2 {
		t.Fatalf("capture consumers = %d, want at least 2", consumers)
	}
}

// TestRawServerSkipsStaleCache proves a new session never replays history:
// a cache older than the freshness window is skipped at its observed
// sequence, and only newer frames flow.
func TestRawServerSkipsStaleCache(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, rawPrinterSpec("01S00C00000000A", "code", "P1S"))

	// Register the restore before the server helper: cleanup runs LIFO, so
	// the server closes fully before the budgets are restored and no live
	// session races the write.
	oldBudget := rawCacheMaxAge
	rawCacheMaxAge = 300 * time.Millisecond
	t.Cleanup(func() { rawCacheMaxAge = oldBudget })
	_, addr := newTestRawServer(t, m)

	// Prime the cache over a warm capture: Acquire starts the loop, the
	// published frame lands, and the balanced Release leaves it cached.
	m.Acquire("01S00C00000000A")
	stale := jpeg(32)
	fc.frames <- stale
	waitUntil(t, 5*time.Second, func() bool { return m.Latest("01S00C00000000A") != nil })
	m.Release("01S00C00000000A")
	time.Sleep(400 * time.Millisecond) // the cached frame is now stale

	conn := rawDial(t, addr, "code")
	_ = conn.SetReadDeadline(time.Now().Add(600 * time.Millisecond))
	_, payload, err := readRawFrame(bufio.NewReader(conn))
	if err == nil {
		t.Fatalf("stale cache was replayed: %d bytes", len(payload))
	}
	if nerr, ok := err.(net.Error); !ok || !nerr.Timeout() {
		t.Fatalf("session should still be alive waiting for a fresh frame, got %v", err)
	}

	fresh := jpeg(48)
	fc.frames <- fresh
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, payload, err = readRawFrame(bufio.NewReader(conn))
	if err != nil || !bytes.Equal(payload, fresh) {
		t.Fatalf("fresh frame after stale cache = %d bytes, %v", len(payload), err)
	}
}

// TestRawServerNoFrameEndsSession proves the frame budget closes a session
// that never receives a usable frame, releasing its capture reference.
func TestRawServerNoFrameEndsSession(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, rawPrinterSpec("01S00C00000000A", "code", "P1S"))

	// Restore after server cleanup; see TestRawServerSkipsStaleCache.
	oldBudget := rawFrameBudget
	rawFrameBudget = 500 * time.Millisecond
	t.Cleanup(func() { rawFrameBudget = oldBudget })
	srv, addr := newTestRawServer(t, m)

	conn := rawDial(t, addr, "code")
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := readRawFrame(bufio.NewReader(conn)); err == nil {
		t.Fatal("session without frames must end")
	}
	waitUntil(t, 3*time.Second, func() bool { return rawSessionCount(srv) == 0 })
	if consumers := captureConsumers(m, "01S00C00000000A"); consumers != 0 {
		t.Fatalf("ended session kept %d consumers", consumers)
	}
}

// TestRawServerClientEOFCancelsPromptly proves a client disconnect cancels
// the frame wait immediately instead of running out the frame budget.
func TestRawServerClientEOFCancelsPromptly(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, rawPrinterSpec("01S00C00000000A", "code", "P1S"))

	// Restore after server cleanup; see TestRawServerSkipsStaleCache.
	oldBudget := rawFrameBudget
	rawFrameBudget = 30 * time.Second // the budget must never get the chance
	t.Cleanup(func() { rawFrameBudget = oldBudget })
	srv, addr := newTestRawServer(t, m)

	conn := rawDial(t, addr, "code")
	time.Sleep(150 * time.Millisecond) // the session blocks in its frame wait
	_ = conn.Close()
	waitUntil(t, 3*time.Second, func() bool { return rawSessionCount(srv) == 0 })
	if consumers := captureConsumers(m, "01S00C00000000A"); consumers != 0 {
		t.Fatalf("client EOF kept %d consumers", consumers)
	}
}

// TestRawServerSendFrameWriteDeadline proves the per-frame write budget
// fails a send whose peer never reads. The boundary is a net.Pipe: it has
// no kernel buffer, so the write can only return through the deadline —
// latest-frame skipping upstream cannot mask it, and no timing window
// remains. Session teardown on a failed send and reference release on EOF
// and shutdown stay covered by the EOF, no-frame, and shutdown tests.
func TestRawServerSendFrameWriteDeadline(t *testing.T) {
	oldBudget := rawWriteBudget
	rawWriteBudget = 300 * time.Millisecond
	t.Cleanup(func() { rawWriteBudget = oldBudget })

	srv := NewRawServer(NewManager(nil, discardLogger()), discardLogger())
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() { _ = server.Close() })

	payload := jpeg(1024)
	frame := &Frame{
		Header:   frameHeaderWithMarkers(0x40, len(payload)),
		JPEG:     payload,
		Seq:      1,
		Captured: time.Now(),
	}

	start := time.Now()
	if ok := srv.sendFrameWithLogger(server, frame, srv.log); ok {
		t.Fatal("sendFrameWithLogger must fail when the peer never reads")
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Fatalf("write failed after %s; the write deadline did not fire", elapsed)
	}
}

// TestRawServerShutdownJoinsWorkers proves Close tears down every phase of
// every session — streaming, mid-handshake, and waiting — without waiting
// for any budget, and that shutdown stays idempotent.
func TestRawServerShutdownJoinsWorkers(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, rawPrinterSpec("01S00C00000000A", "code", "P1S"))
	srv, addr := newTestRawServer(t, m)

	streaming := rawDial(t, addr, "code")
	handshake, err := net.Dial("tcp", addr) // plain TCP: stuck in handshake
	if err != nil {
		t.Fatalf("handshake dial: %v", err)
	}
	defer handshake.Close()
	waiting := rawDial(t, addr, "code")

	fc.frames <- jpeg(48)
	_ = streaming.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := readRawFrame(bufio.NewReader(streaming)); err != nil {
		t.Fatalf("streaming client read: %v", err)
	}

	start := time.Now()
	srv.Close()
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Close waited %s; shutdown must not wait for budgets", elapsed)
	}
	if n := rawSessionCount(srv); n != 0 {
		t.Fatalf("Close left %d sessions", n)
	}
	srv.Close() // idempotent

	if _, _, err := readRawFrame(bufio.NewReader(streaming)); err == nil {
		t.Fatal("streaming client must observe the closed session")
	}
	if _, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		t.Fatal("listener must be closed after Close")
	}
	_ = waiting.Close()
}

// TestRawServerStartServesPrinterPort drives the production Start path: the
// ephemeral certificate, the fixed camera port, and a full frame round trip.
func TestRawServerStartServesPrinterPort(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, rawPrinterSpec("01S00C00000000A", "code", "P1S"))
	srv := NewRawServer(m, discardLogger())
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(srv.Close)

	conn := rawDial(t, "127.0.0.1:6000", "code")
	frame := jpeg(56)
	fc.frames <- frame
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, payload, err := readRawFrame(bufio.NewReader(conn))
	if err != nil || !bytes.Equal(payload, frame) {
		t.Fatalf("production port round trip = %d bytes, %v", len(payload), err)
	}
}
