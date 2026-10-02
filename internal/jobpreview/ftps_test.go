// The transport tests run fetch against a minimal test-only implicit
// TLS FTP server. They verify the bounded-protocol contract end to end:
// exactly one control session and at most one RETR per attempt, SIZE-only
// probing where only a definite file-missing 550 advances, a hostile PASV
// address that cannot redirect the data connection, data-channel session
// resumption enforced through tls.ConnectionState.DidResume, the byte cap
// holding even when SIZE lies, terminal outcomes for auth rejection,
// rejected data TLS, ambiguous 550 replies and unsupported SIZE, a
// control-response flood stopped by the cumulative read cap, cancellation
// of stalled greetings, data TLS handshakes and transfers, registration
// racing the cancellation sweep under barrier control, temporary-file
// staging failure, the attempt deadline, and the absence of temporary
// files on every path, with accepted partial parse results propagating
// through the transport unchanged.
package jobpreview

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"image"
	"image/png"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// ftpsShortenTiming replaces the fixed production timing seams with test
// values: a three-second attempt deadline backstop, a 750 millisecond
// control bound, and a 64 KiB archive cap so cap enforcement stays cheap.
func ftpsShortenTiming(t *testing.T) {
	t.Helper()
	prevAttempt, prevControl, prevMax := ftpsAttemptTimeout, ftpsControlBound, ftpsArchiveMax
	ftpsAttemptTimeout = 3 * time.Second
	ftpsControlBound = 750 * time.Millisecond
	ftpsArchiveMax = 64 << 10
	t.Cleanup(func() {
		ftpsAttemptTimeout, ftpsControlBound, ftpsArchiveMax = prevAttempt, prevControl, prevMax
	})
}

// ftpsPointControlAt repoints the control-address hook at the fake
// server's ephemeral listener. Production always derives port 990.
func ftpsPointControlAt(t *testing.T, addr string) {
	t.Helper()
	prev := resolveControlAddr
	resolveControlAddr = func(string) string { return addr }
	t.Cleanup(func() { resolveControlAddr = prev })
}

func ftpsPrinter(addr string) config.Printer {
	return config.Printer{
		Serial:             "01P00AFTPS00001",
		Address:            addr,
		Username:           "bblp",
		Password:           "12345678",
		TLS:                true,
		InsecureSkipVerify: true,
	}
}

func ftpsJob(name, gcodeFile string) telemetry.JobView {
	return telemetry.JobView{
		Generation:   3,
		Revision:     1,
		RunningEpoch: 1,
		Active:       true,
		State:        "RUNNING",
		Name:         name,
		GCodeFile:    gcodeFile,
	}
}

// ftpsPlateJob returns a job carrying an explicit selected plate index.
func ftpsPlateJob(name, gcodeFile string, plate int) telemetry.JobView {
	job := ftpsJob(name, gcodeFile)
	job.PlateIndex = &plate
	return job
}

func ftpsFetchCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}

// ftpsNoTempFiles asserts the transport left no temporary archive behind.
func ftpsNoTempFiles(t *testing.T) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "bmbpx-preview-*"))
	if err != nil {
		t.Fatalf("glob temporary files: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary archive files left behind: %v", matches)
	}
}

// ftpsRequireCategory asserts one fixed transport category.
func ftpsRequireCategory(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("fetch error = nil, want category %q", want)
	}
	var categorized interface{ category() string }
	if !errors.As(err, &categorized) {
		t.Fatalf("fetch error %v carries no fixed category", err)
	}
	if got := categorized.category(); got != want {
		t.Fatalf("category = %q, want %q", got, want)
	}
}

// fakeFTPSCert generates the throwaway self-signed server certificate.
func fakeFTPSCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "bambulab.com"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

// ftpsTest3MF builds the minimal accepted archive fixture: one root model
// metadata value. Plate sidecars are absent and the job reports no plate
// evidence, so the parsed result carries the project title only.
func ftpsTest3MF(t *testing.T, title string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("3D/3dmodel.model")
	if err != nil {
		t.Fatalf("zip create: %v", err)
	}
	doc := `<?xml version="1.0" encoding="UTF-8"?>` +
		`<model unit="millimeter" xmlns="http://schemas.microsoft.com/3dmanufacturing/core/2015/02">` +
		`<metadata name="Title">` + title + `</metadata>` +
		`<resources></resources><build></build></model>`
	if _, err := io.WriteString(w, doc); err != nil {
		t.Fatalf("zip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

// ftpsTestPNG encodes a small valid RGBA plate render.
func ftpsTestPNG(t *testing.T, width, height int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, width, height))); err != nil {
		t.Fatalf("png encode: %v", err)
	}
	return buf.Bytes()
}

// ftpsTestPlate3MF builds an accepted archive fixture with a real
// selected plate: a project title, a plate 1 gcode entry for selection,
// and a valid plate 1 render. The absent sidecar entries contribute no
// fields and no failures.
func ftpsTestPlate3MF(t *testing.T, title string, render []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	add := func(name string, data []byte) {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("zip create %s: %v", name, err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatalf("zip write %s: %v", name, err)
		}
	}
	doc := `<?xml version="1.0" encoding="UTF-8"?>` +
		`<model unit="millimeter" xmlns="http://schemas.microsoft.com/3dmanufacturing/core/2015/02">` +
		`<metadata name="Title">` + title + `</metadata>` +
		`<resources></resources><build></build></model>`
	add("3D/3dmodel.model", []byte(doc))
	add("Metadata/plate_1.gcode", []byte("; plate 1\n"))
	add("Metadata/plate_1.png", render)
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

// ftpsTestPartial3MF builds an archive whose slice_info entry is
// malformed while the model metadata and the plate render are valid, so
// the transport must propagate parseArchive's accepted partial result
// instead of discarding it with the failed entry.
func ftpsTestPartial3MF(t *testing.T, title string, render []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	add := func(name string, data []byte) {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("zip create %s: %v", name, err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatalf("zip write %s: %v", name, err)
		}
	}
	doc := `<?xml version="1.0" encoding="UTF-8"?>` +
		`<model unit="millimeter" xmlns="http://schemas.microsoft.com/3dmanufacturing/core/2015/02">` +
		`<metadata name="Title">` + title + `</metadata>` +
		`<resources></resources><build></build></model>`
	add("3D/3dmodel.model", []byte(doc))
	// Root-level trailing content after the closed config element: the
	// shared sidecar parser rejects the whole entry deterministically.
	add("Metadata/slice_info.config", []byte("<config></config>trailing"))
	add("Metadata/plate_1.png", render)
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

// fakeFTPSConf holds the per-scenario behavior knobs of the fake server.
type fakeFTPSConf struct {
	user, pass    string
	greetingDelay time.Duration     // stall before the 220 greeting
	authFail      bool              // reject PASS with 530
	rejectEPSV    bool              // reply 500 to EPSV, forcing the PASV fallback
	pasvHost      string            // hostile dotted quad announced by PASV
	failDataTLS   bool              // accept the data socket, then drop it
	stallDataTLS  bool              // accept the data socket, never start its TLS handshake
	stallTLS      bool              // accept TCP, never start the TLS handshake
	floodSizeText bool              // answer SIZE with far more than cap bytes of 550 text
	announcedSize int64             // SIZE reply override (lied SIZE); <=0 derives len(file)
	file          []byte            // archive bytes served by RETR
	extraData     int               // bytes streamed beyond file (lied stream)
	stallAfter    int               // stall mid-stream after this many bytes
	serve         map[string]bool   // remote paths that exist
	sizeReplies   map[string]string // exact-path SIZE replies, full reply without CRLF
}

// fakeFTPS is the minimal test-only implicit-TLS FTP server. It speaks
// exactly the command surface the client needs and records every command,
// so unexpected verbs, sessions, or retries fail the tests.
type fakeFTPS struct {
	t    *testing.T
	conf fakeFTPSConf

	tlsConf    *tls.Config
	ctrl       net.Listener
	data       net.Listener
	done       chan struct{}
	wg         sync.WaitGroup
	ctrlClosed chan struct{}
	closedOnce sync.Once
	closeState sync.Once

	mu           sync.Mutex
	ctrlSessions int
	dataSessions int
	dataAccepted int
	dataResumed  []bool
	cmds         []string
	retrs        []string
	sizes        []string
}

func newFakeFTPS(t *testing.T, conf fakeFTPSConf) *fakeFTPS {
	t.Helper()
	cert, err := fakeFTPSCert()
	if err != nil {
		t.Fatalf("fake ftps certificate: %v", err)
	}
	ctrl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake ftps control listen: %v", err)
	}
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake ftps data listen: %v", err)
	}
	s := &fakeFTPS{
		t:          t,
		conf:       conf,
		tlsConf:    &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		ctrl:       ctrl,
		data:       data,
		done:       make(chan struct{}),
		ctrlClosed: make(chan struct{}),
	}
	s.wg.Add(1)
	go s.serve()
	t.Cleanup(s.close)
	return s
}

func (s *fakeFTPS) close() {
	s.closeState.Do(func() {
		close(s.done)
		_ = s.ctrl.Close()
		_ = s.data.Close()
	})
	exited := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(exited)
	}()
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		s.t.Error("fake ftps goroutines did not exit")
	}
}

func (s *fakeFTPS) port() int {
	return s.data.Addr().(*net.TCPAddr).Port
}

func (s *fakeFTPS) serve() {
	defer s.wg.Done()
	for {
		c, err := s.ctrl.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go s.control(c)
	}
}

func writeFTPSLine(w io.Writer, line string) {
	_, _ = io.WriteString(w, line+"\r\n") // a gone client is noticed by the read loop
}

func (s *fakeFTPS) control(raw net.Conn) {
	defer s.wg.Done()
	defer s.closedOnce.Do(func() { close(s.ctrlClosed) })
	tc := tls.Server(raw, s.tlsConf)
	_ = tc.SetDeadline(time.Now().Add(30 * time.Second))
	if s.conf.stallTLS {
		// Accept the TCP connection but never start the TLS handshake:
		// the client's control bound must fire.
		select {
		case <-time.After(30 * time.Second):
		case <-s.done:
		}
		_ = tc.Close()
		return
	}
	if err := tc.Handshake(); err != nil {
		_ = tc.Close()
		return
	}
	s.mu.Lock()
	s.ctrlSessions++
	s.mu.Unlock()
	defer tc.Close()
	if s.conf.greetingDelay > 0 {
		select {
		case <-time.After(s.conf.greetingDelay):
		case <-s.done:
			return
		}
	}
	writeFTPSLine(tc, "220 bambu fake ftps ready")
	br := bufio.NewReader(tc)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return // client closed or cancelled; the socket is the client's
		}
		if s.command(tc, strings.TrimRight(line, "\r\n")) {
			return
		}
	}
}

// command handles one recorded control command and reports whether the
// session ends.
func (s *fakeFTPS) command(tc *tls.Conn, line string) bool {
	verb, arg := line, ""
	if i := strings.IndexByte(line, ' '); i >= 0 {
		verb, arg = line[:i], line[i+1:]
	}
	s.mu.Lock()
	s.cmds = append(s.cmds, line)
	s.mu.Unlock()
	switch strings.ToUpper(verb) {
	case "USER":
		writeFTPSLine(tc, "331 password required")
	case "PASS":
		if s.conf.authFail {
			writeFTPSLine(tc, "530 login incorrect")
			return false
		}
		writeFTPSLine(tc, "230 logged in")
	case "FEAT":
		feats := "211-Features:\r\n SIZE\r\n PBSZ\r\n PROT\r\n"
		if !s.conf.rejectEPSV {
			feats += " EPSV\r\n"
		}
		writeFTPSLine(tc, feats+"211 End")
	case "TYPE", "PBSZ", "PROT":
		writeFTPSLine(tc, "200 ok")
	case "SIZE":
		s.mu.Lock()
		s.sizes = append(s.sizes, arg)
		s.mu.Unlock()
		switch {
		case s.conf.floodSizeText:
			// Multi-line 550 far beyond the client's cumulative control
			// read cap: the client must bail out on its budget.
			flood := strings.Repeat("550-more than any client should read\r\n", 9000)
			writeFTPSLine(tc, flood+"550 end")
		default:
			if reply, ok := s.conf.sizeReplies[arg]; ok {
				writeFTPSLine(tc, reply)
				break
			}
			if s.conf.serve[arg] {
				size := s.conf.announcedSize
				if size <= 0 {
					size = int64(len(s.conf.file))
				}
				writeFTPSLine(tc, fmt.Sprintf("213 %d", size))
				break
			}
			writeFTPSLine(tc, "550 No such file")
		}
	case "EPSV":
		if s.conf.rejectEPSV {
			writeFTPSLine(tc, "500 EPSV not supported")
			break
		}
		writeFTPSLine(tc, fmt.Sprintf("229 Entering Extended Passive Mode (|||%d|)", s.port()))
	case "PASV":
		host := "127,0,0,1"
		if s.conf.pasvHost != "" {
			host = strings.ReplaceAll(s.conf.pasvHost, ".", ",")
		}
		p := s.port()
		writeFTPSLine(tc, fmt.Sprintf("227 Entering Passive Mode (%s,%d,%d)", host, p/256, p%256))
	case "RETR":
		s.mu.Lock()
		s.retrs = append(s.retrs, arg)
		s.mu.Unlock()
		writeFTPSLine(tc, "150 opening data connection")
		s.transfer()
		writeFTPSLine(tc, "226 transfer complete")
	case "QUIT":
		writeFTPSLine(tc, "221 bye")
		return true
	default:
		writeFTPSLine(tc, "502 not implemented")
	}
	return false
}

// transfer accepts the passive data connection once per RETR, counts the
// raw data connection and the completed data session separately, and
// enforces session resumption: a data handshake that did not resume the
// control session is a transport configuration bug and fails the test.
func (s *fakeFTPS) transfer() {
	c, err := s.data.Accept()
	if err != nil {
		return
	}
	defer c.Close()
	s.mu.Lock()
	s.dataAccepted++
	s.mu.Unlock()
	if s.conf.failDataTLS {
		return // accept then drop: the client's lazy data handshake fails
	}
	if s.conf.stallDataTLS {
		// Accept the TCP connection but never start the data TLS
		// handshake: the client's lazy handshake must stall until the
		// attempt is cancelled or its deadline fires.
		select {
		case <-time.After(30 * time.Second):
		case <-s.done:
		}
		return
	}
	dt := tls.Server(c, s.tlsConf)
	_ = dt.SetDeadline(time.Now().Add(30 * time.Second))
	if err := dt.Handshake(); err != nil {
		return
	}
	s.mu.Lock()
	s.dataSessions++
	s.dataResumed = append(s.dataResumed, dt.ConnectionState().DidResume)
	s.mu.Unlock()
	payload := s.conf.file
	if s.conf.extraData > 0 {
		payload = append(append([]byte{}, payload...), bytes.Repeat([]byte{0x5a}, s.conf.extraData)...)
	}
	if s.conf.stallAfter > 0 && s.conf.stallAfter < len(payload) {
		if _, err := dt.Write(payload[:s.conf.stallAfter]); err != nil {
			return
		}
		select {
		case <-time.After(30 * time.Second):
		case <-s.done:
		}
		return // never complete: the client must cancel or time out
	}
	if len(payload) > 0 {
		_, _ = dt.Write(payload)
	}
}

// snapshot returns the recorded protocol observations.
func (s *fakeFTPS) snapshot() (ctrl, data int, resumed []bool, cmds, retrs, sizes []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctrlSessions, s.dataSessions, append([]bool(nil), s.dataResumed...),
		append([]string(nil), s.cmds...), append([]string(nil), s.retrs...),
		append([]string(nil), s.sizes...)
}

// assertClean enforces the bounded-protocol invariants: only negotiated
// verbs, one control session, at most one data session, at most one
// RETR, and at most maxSizes SIZE probes.
func (s *fakeFTPS) assertClean(t *testing.T, maxSizes int) {
	t.Helper()
	ctrl, data, _, cmds, retrs, sizes := s.snapshot()
	if ctrl != 1 {
		t.Errorf("control sessions = %d, want 1 (no reconnect)", ctrl)
	}
	if data > 1 {
		t.Errorf("data sessions = %d, want at most 1", data)
	}
	if len(retrs) > 1 {
		t.Errorf("RETR count = %d, want at most 1", len(retrs))
	}
	if len(sizes) > maxSizes {
		t.Errorf("SIZE probes = %d, want at most %d", len(sizes), maxSizes)
	}
	allowed := map[string]bool{
		"USER": true, "PASS": true, "FEAT": true, "TYPE": true, "PBSZ": true,
		"PROT": true, "SIZE": true, "EPSV": true, "PASV": true,
		"RETR": true, "QUIT": true,
	}
	for _, cmd := range cmds {
		v := cmd
		if i := strings.IndexByte(cmd, ' '); i >= 0 {
			v = cmd[:i]
		}
		if !allowed[strings.ToUpper(v)] {
			t.Errorf("unexpected command %q (no LIST, REST, STOR, DELE, or unadvertised extras)", cmd)
		}
	}
}

func (s *fakeFTPS) waitCtrl(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ctrl, _, _, _, _, _ := s.snapshot(); ctrl == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("control session not established in time")
}

func (s *fakeFTPS) waitData(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, data, _, _, _, _ := s.snapshot(); data == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("data session not established in time")
}

// dataAcceptedCount reports how many raw data connections the server
// accepted, independent of handshake completion.
func (s *fakeFTPS) dataAcceptedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dataAccepted
}

// waitDataAccepted waits until the server has accepted the raw data
// connection, even when its TLS handshake never completes.
func (s *fakeFTPS) waitDataAccepted(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if s.dataAcceptedCount() >= 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("data connection not accepted in time")
}

// waitCtrlClosed waits for the server to observe the end of the control
// session.
func (s *fakeFTPS) waitCtrlClosed(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-s.ctrlClosed:
	case <-time.After(d):
		t.Fatal("control session still open after deadline")
	}
}

func TestResolveControlAddrDefaultPort(t *testing.T) {
	if got := resolveControlAddr("192.168.1.42"); got != "192.168.1.42:990" {
		t.Fatalf("resolveControlAddr = %q, want port 990", got)
	}
	if got := resolveControlAddr("2001:db8::1"); got != "[2001:db8::1]:990" {
		t.Fatalf("resolveControlAddr ipv6 = %q, want [2001:db8::1]:990", got)
	}
}

func TestTransferCandidates(t *testing.T) {
	cases := []struct {
		name string
		job  telemetry.JobView
		want []string
	}{
		{"gcode first, then name variants",
			telemetry.JobView{Name: "Box", GCodeFile: "/cache/deep/rover.gcode.3mf"},
			[]string{"rover.gcode.3mf", "Box.3mf", "Box.gcode.3mf"}},
		{"dedupe keeps first occurrence",
			telemetry.JobView{Name: "Box", GCodeFile: "/cache/Box.3mf"},
			[]string{"Box.3mf", "Box.gcode.3mf"}},
		{"name already ends in 3mf",
			telemetry.JobView{Name: "Model.3MF"},
			[]string{"Model.3MF"}},
		{"slash name drops only name candidates",
			telemetry.JobView{Name: "a/b", GCodeFile: "/cache/ok.3mf"},
			[]string{"ok.3mf"}},
		{"dot name rejected",
			telemetry.JobView{Name: "..", GCodeFile: "/cache/ok.3mf"},
			[]string{"ok.3mf"}},
		{"control characters rejected everywhere",
			telemetry.JobView{Name: "Box\x01", GCodeFile: "ok\x00.3mf"},
			nil},
		{"traversal segment rejects gcode",
			telemetry.JobView{GCodeFile: "a/../b.3mf"},
			nil},
		{"dot segment rejects gcode",
			telemetry.JobView{GCodeFile: "a/./b.3mf"},
			nil},
		{"non-3mf gcode falls back to name",
			telemetry.JobView{Name: "Box", GCodeFile: "/cache/x.gcode"},
			[]string{"Box.3mf", "Box.gcode.3mf"}},
		{"overlong name dropped",
			telemetry.JobView{Name: strings.Repeat("n", 300), GCodeFile: "/cache/ok.3mf"},
			[]string{"ok.3mf"}},
		{"spaces and unicode stay valid",
			telemetry.JobView{Name: "Zusammenbau Anleitung \u00fcber"},
			[]string{"Zusammenbau Anleitung \u00fcber.3mf", "Zusammenbau Anleitung \u00fcber.gcode.3mf"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := transferCandidates(tc.job)
			if len(got) != len(tc.want) {
				t.Fatalf("candidates = %q, want %q", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("candidates = %q, want %q", got, tc.want)
				}
			}
		})
	}
}

func TestFtpsDialerRegisterAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := newFtpsDialer(ctx, time.Now().Add(time.Minute), &tls.Config{})
	c1, c2 := net.Pipe()
	defer c2.Close()
	if err := d.register(c1); err == nil {
		t.Fatal("register on cancelled attempt succeeded")
	}
	if _, err := c2.Read(make([]byte, 1)); err == nil {
		t.Fatal("rejected socket was not closed")
	}
	if _, err := d.dial("tcp", "127.0.0.1:1"); err == nil {
		t.Fatal("dial on cancelled attempt succeeded")
	}
}

func TestFtpsDialerClosesSocketsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newFtpsDialer(ctx, time.Now().Add(time.Minute), &tls.Config{})
	c1, c2 := net.Pipe()
	defer c2.Close()
	if err := d.register(c1); err != nil {
		t.Fatalf("register: %v", err)
	}
	cancel()
	if _, err := c2.Read(make([]byte, 1)); err == nil {
		t.Fatal("registered socket was not closed on cancellation")
	}
}

func TestFtpsDialerClosesSocketsOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newFtpsDialer(ctx, time.Now().Add(time.Minute), &tls.Config{})
	c1, c2 := net.Pipe()
	defer c2.Close()
	if err := d.register(c1); err != nil {
		t.Fatalf("register: %v", err)
	}
	d.close() // the successful-return cleanup path: sweep and watcher join
	if _, err := c2.Read(make([]byte, 1)); err == nil {
		t.Fatal("registered socket was not closed on attempt end")
	}
	if err := d.register(c1); err == nil {
		t.Fatal("register after close succeeded")
	}
}

// ftpsGatedConn wraps one pipe end and parks SetDeadline on a gate, so a
// test can freeze the dialer's sweeper or registrant at a deterministic
// point inside the socket registry.
type ftpsGatedConn struct {
	net.Conn
	enteredOnce sync.Once
	entered     chan struct{} // closed when SetDeadline parks
	release     chan struct{} // closed to unpark SetDeadline
}

func newFtpsGatedConn() (*ftpsGatedConn, net.Conn) {
	c1, c2 := net.Pipe()
	gated := &ftpsGatedConn{
		Conn:    c1,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	return gated, c2
}

func (c *ftpsGatedConn) SetDeadline(t time.Time) error {
	c.enteredOnce.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.SetDeadline(t)
}

// TestFtpsDialerRegisterVersusSweepRace drives both interleavings of a
// socket registration racing the cancellation sweep behind gates. In
// every order the attempt must end with no socket left open: a
// registration arriving during the sweep is rejected and closed, and the
// sweep cannot overtake a registration that is still closing its
// rejected socket under the registry lock.
func TestFtpsDialerRegisterVersusSweepRace(t *testing.T) {
	t.Run("rejects registration while the sweep is closing sockets", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		d := newFtpsDialer(ctx, time.Now().Add(time.Minute), &tls.Config{})
		gated, far1 := newFtpsGatedConn()
		defer far1.Close()
		if err := d.register(gated); err != nil {
			t.Fatalf("register: %v", err)
		}
		cancel() // the sweep parks inside its first socket close
		<-gated.entered
		plain1, plain2 := net.Pipe()
		defer plain2.Close()
		if err := d.register(plain1); err == nil {
			t.Fatal("registration during the sweep succeeded")
		}
		if _, err := plain2.Read(make([]byte, 1)); err == nil {
			t.Fatal("socket registered during the sweep was not closed")
		}
		close(gated.release) // let the sweep finish closing the first socket
		d.close()            // joins the watcher only after the sweep returns
		if _, err := far1.Read(make([]byte, 1)); err == nil {
			t.Fatal("swept socket was not closed")
		}
	})

	t.Run("sweep waits for the in-flight registration close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		d := newFtpsDialer(ctx, time.Now().Add(time.Minute), &tls.Config{})
		cancel() // empty sweep: the watcher is joined before any registration
		d.close()
		gated, far := newFtpsGatedConn()
		defer far.Close()
		regErr := make(chan error, 1)
		go func() { regErr <- d.register(gated) }()
		<-gated.entered // the rejected registration parks closing its socket, holding the registry lock
		swept := make(chan struct{})
		go func() { d.sweep(); close(swept) }()
		select {
		case <-swept:
			t.Fatal("sweep overtook the in-flight registration close")
		case <-time.After(250 * time.Millisecond):
			// Deadlock-detection bound only: the rejection owns the
			// registry until its socket is closed.
		}
		close(gated.release)
		if err := <-regErr; err == nil {
			t.Fatal("registration on the ended attempt succeeded")
		}
		select {
		case <-swept:
		case <-time.After(5 * time.Second):
			t.Fatal("sweep did not complete after the registration close")
		}
		if _, err := far.Read(make([]byte, 1)); err == nil {
			t.Fatal("rejected socket was not closed")
		}
	})
}

func TestFetch3MFOverbound550Terminal(t *testing.T) {
	ftpsShortenTiming(t)
	// Missing wording up front, denial hidden past the classification
	// bound: the over-bound reply must terminate the attempt, never
	// advance to another candidate.
	text := "550 no such file " + strings.Repeat("x", 300) + " permission denied"
	srv := newFakeFTPS(t, fakeFTPSConf{
		user: "bblp", pass: "12345678",
		sizeReplies: map[string]string{"/cache/BoxTower.gcode.3mf": text},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTransport)
	_, _, _, _, retrs, sizes := srv.snapshot()
	if len(sizes) != 1 || len(retrs) != 0 {
		t.Fatalf("SIZE = %q, RETR = %q, want terminal after the over-bound reply", sizes, retrs)
	}
	srv.assertClean(t, 1)
}

func TestFetch3MFDeadlineCappedAtPolicy(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:          "bblp",
		pass:          "12345678",
		greetingDelay: 10 * time.Second,
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	// A caller deadline far beyond the policy must not extend the attempt.
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	start := time.Now()
	_, err := fetch(ctx, ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTimeout)
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("attempt ran %v, want the policy cap to fire", elapsed)
	}
	ftpsNoTempFiles(t)
}

func TestFetch3MFHandshakeStallControlBound(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:     "bblp",
		pass:     "12345678",
		stallTLS: true,
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	start := time.Now()
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTimeout)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("control establishment took %v, want the control bound to fire", elapsed)
	}
	ftpsNoTempFiles(t)
}

func TestFetch3MFSuccessResumesSession(t *testing.T) {
	ftpsShortenTiming(t)
	render := ftpsTestPNG(t, 2, 2)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:  "bblp",
		pass:  "12345678",
		file:  ftpsTestPlate3MF(t, "Casing Preview", render),
		serve: map[string]bool{"/cache/BoxTower.gcode.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	res, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsPlateJob("", "/cache/BoxTower.gcode.3mf", 1))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if res.Metadata == nil || res.Metadata.Title == nil || *res.Metadata.Title != "Casing Preview" {
		t.Fatalf("metadata = %+v, want accepted title", res.Metadata)
	}
	if res.Preview.Status != StatusReady {
		t.Fatalf("status = %q, want ready with a validated plate image", res.Preview.Status)
	}
	if !bytes.Equal(res.PNG, render) {
		t.Fatal("served PNG differs from the archived plate render")
	}
	cfg, err := png.DecodeConfig(bytes.NewReader(res.PNG))
	if err != nil {
		t.Fatalf("png decode config: %v", err)
	}
	if cfg.Width != 2 || cfg.Height != 2 {
		t.Fatalf("png = %dx%d, want 2x2", cfg.Width, cfg.Height)
	}
	srv.assertClean(t, 1)
	// The library's Quit is fire-and-forget: it writes QUIT and closes
	// the control connection without reading the 221 reply, so the server
	// may not have recorded the command yet when fetch returns. The fake
	// handler exits only after recording QUIT, so wait for the session
	// end before asserting on the recording.
	srv.waitCtrlClosed(t, 5*time.Second)
	_, _, resumed, cmds, retrs, sizes := srv.snapshot()
	if len(sizes) != 1 || sizes[0] != "/cache/BoxTower.gcode.3mf" {
		t.Fatalf("SIZE probes = %q, want exactly the first cache candidate", sizes)
	}
	if len(retrs) != 1 || retrs[0] != "/cache/BoxTower.gcode.3mf" {
		t.Fatalf("RETR = %q, want one download", retrs)
	}
	if len(resumed) != 1 || !resumed[0] {
		t.Fatalf("data DidResume = %v, want [true] (shared per-attempt cache and ServerName)", resumed)
	}
	quit := false
	for _, cmd := range cmds {
		if cmd == "QUIT" {
			quit = true
		}
	}
	if !quit {
		t.Fatal("session ended without QUIT")
	}
	ftpsNoTempFiles(t)
}

func TestFetch3MFProbeOrderRootFallback(t *testing.T) {
	ftpsShortenTiming(t)
	render := ftpsTestPNG(t, 2, 2)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:  "bblp",
		pass:  "12345678",
		file:  ftpsTestPlate3MF(t, "Gusset", render),
		serve: map[string]bool{"/Gusset.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	res, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsPlateJob("Gusset", "", 1))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if res.Metadata == nil || res.Metadata.Title == nil || *res.Metadata.Title != "Gusset" {
		t.Fatalf("metadata = %+v", res.Metadata)
	}
	if res.Preview.Status != StatusReady || !bytes.Equal(res.PNG, render) {
		t.Fatalf("status = %q, png %d bytes, want ready with the archived render",
			res.Preview.Status, len(res.PNG))
	}
	_, _, _, _, retrs, sizes := srv.snapshot()
	wantSizes := []string{"/cache/Gusset.3mf", "/cache/Gusset.gcode.3mf", "/Gusset.3mf"}
	if len(sizes) != len(wantSizes) {
		t.Fatalf("SIZE probes = %q, want %q", sizes, wantSizes)
	}
	for i := range wantSizes {
		if sizes[i] != wantSizes[i] {
			t.Fatalf("SIZE probes = %q, want %q", sizes, wantSizes)
		}
	}
	if len(retrs) != 1 || retrs[0] != "/Gusset.3mf" {
		t.Fatalf("RETR = %q, want the root fallback", retrs)
	}
	srv.assertClean(t, 3)
	ftpsNoTempFiles(t)
}

func TestFetch3MFAllMissing(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{user: "bblp", pass: "12345678"})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catNotFound)
	_, data, _, _, retrs, sizes := srv.snapshot()
	if len(sizes) != 2 {
		t.Fatalf("SIZE probes = %q, want cache then root", sizes)
	}
	if data != 0 || len(retrs) != 0 {
		t.Fatalf("data sessions = %d, RETR = %q, want none", data, retrs)
	}
	srv.assertClean(t, 2)
	ftpsNoTempFiles(t)
}

func TestFetch3MFDenied550Terminal(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user: "bblp", pass: "12345678",
		sizeReplies: map[string]string{"/cache/BoxTower.gcode.3mf": "550 Permission denied"},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTransport)
	_, _, _, _, retrs, sizes := srv.snapshot()
	if len(sizes) != 1 {
		t.Fatalf("SIZE probes = %q, want terminal after the denied reply", sizes)
	}
	if len(retrs) != 0 {
		t.Fatalf("RETR = %q, want none", retrs)
	}
	srv.assertClean(t, 1)
}

func TestFetch3MFAmbiguous550Terminal(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user: "bblp", pass: "12345678",
		sizeReplies: map[string]string{"/cache/BoxTower.gcode.3mf": "550 printer state uncertain"},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTransport)
	_, _, _, _, _, sizes := srv.snapshot()
	if len(sizes) != 1 {
		t.Fatalf("SIZE probes = %q, want terminal after the ambiguous reply", sizes)
	}
	srv.assertClean(t, 1)
}

func TestFetch3MFUnsupportedSizeTerminal(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user: "bblp", pass: "12345678",
		sizeReplies: map[string]string{"/cache/BoxTower.gcode.3mf": "502 SIZE not implemented"},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTransport)
	_, _, _, _, retrs, _ := srv.snapshot()
	if len(retrs) != 0 {
		t.Fatalf("RETR = %q, want no speculative download", retrs)
	}
	srv.assertClean(t, 1)
}

func TestFetch3MFFloodBounded(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:          "bblp",
		pass:          "12345678",
		floodSizeText: true,
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	start := time.Now()
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTransport)
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("flooded reply took %v, want an immediate budget abort", elapsed)
	}
	_, _, _, _, retrs, _ := srv.snapshot()
	if len(retrs) != 0 {
		t.Fatalf("RETR = %q, want none", retrs)
	}
	srv.assertClean(t, 1)
}

func TestFetch3MFAuthRejectedNoReconnect(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{user: "bblp", pass: "12345678", authFail: true})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTransport)
	ctrl, data, _, _, retrs, _ := srv.snapshot()
	if ctrl != 1 {
		t.Fatalf("control sessions = %d, want exactly one (no reconnect after auth rejection)", ctrl)
	}
	if data != 0 || len(retrs) != 0 {
		t.Fatalf("data sessions = %d, RETR = %q, want none", data, retrs)
	}
	srv.waitCtrlClosed(t, 5*time.Second)
	ftpsNoTempFiles(t)
}

func TestFetch3MFHostilePASVPinned(t *testing.T) {
	ftpsShortenTiming(t)
	render := ftpsTestPNG(t, 2, 2)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:       "bblp",
		pass:       "12345678",
		rejectEPSV: true,
		pasvHost:   "203.0.113.9", // TEST-NET-3: unroutable, must be ignored
		file:       ftpsTestPlate3MF(t, "Bracket", render),
		serve:      map[string]bool{"/cache/Bracket.gcode.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	res, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsPlateJob("", "/cache/Bracket.gcode.3mf", 1))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if res.Metadata == nil || res.Metadata.Title == nil {
		t.Fatalf("metadata = %+v", res.Metadata)
	}
	if res.Preview.Status != StatusReady || !bytes.Equal(res.PNG, render) {
		t.Fatalf("status = %q, png %d bytes, want ready with the archived render",
			res.Preview.Status, len(res.PNG))
	}
	_, data, resumed, cmds, retrs, _ := srv.snapshot()
	if data != 1 {
		t.Fatalf("data sessions = %d, want the pinned peer session", data)
	}
	if len(resumed) != 1 || !resumed[0] {
		t.Fatalf("data DidResume = %v, want [true]", resumed)
	}
	if len(retrs) != 1 {
		t.Fatalf("RETR = %q, want one", retrs)
	}
	epsv, pasv := 0, 0
	for _, cmd := range cmds {
		switch {
		case strings.HasPrefix(cmd, "EPSV"):
			epsv++
		case strings.HasPrefix(cmd, "PASV"):
			pasv++
		}
	}
	if epsv != 1 || pasv != 1 {
		t.Fatalf("EPSV = %d, PASV = %d, want exactly one negotiation fallback", epsv, pasv)
	}
	srv.assertClean(t, 1)
	ftpsNoTempFiles(t)
}

func TestFetch3MFDataTLSRejected(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:        "bblp",
		pass:        "12345678",
		failDataTLS: true,
		serve:       map[string]bool{"/cache/BoxTower.gcode.3mf": true},
		file:        ftpsTest3MF(t, "Broken data"),
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTransport)
	ctrl, data, _, _, retrs, _ := srv.snapshot()
	if ctrl != 1 {
		t.Fatalf("control sessions = %d, want no reconnect after data TLS failure", ctrl)
	}
	if data != 0 {
		t.Fatalf("data sessions = %d, want none (handshake never completed)", data)
	}
	if len(retrs) != 1 {
		t.Fatalf("RETR = %q, want exactly one attempt", retrs)
	}
	srv.assertClean(t, 1)
	ftpsNoTempFiles(t)
}

func TestFetch3MFLiedSizeCapEnforced(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:          "bblp",
		pass:          "12345678",
		announcedSize: 100,
		file:          bytes.Repeat([]byte{0x42}, 100),
		extraData:     200_000,
		serve:         map[string]bool{"/cache/BoxTower.gcode.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catOversized)
	_, _, _, _, retrs, sizes := srv.snapshot()
	if len(retrs) != 1 || len(sizes) != 1 {
		t.Fatalf("RETR = %q, SIZE = %q, want a single aborted transfer", retrs, sizes)
	}
	srv.assertClean(t, 1)
	ftpsNoTempFiles(t)
}

func TestFetch3MFAnnouncedOversizeNoRetr(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:          "bblp",
		pass:          "12345678",
		announcedSize: int64(33) << 20,
		file:          ftpsTest3MF(t, "Huge"),
		serve:         map[string]bool{"/cache/BoxTower.gcode.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catOversized)
	_, data, _, _, retrs, _ := srv.snapshot()
	if len(retrs) != 0 || data != 0 {
		t.Fatalf("RETR = %q, data sessions = %d, want rejection before any transfer", retrs, data)
	}
	srv.assertClean(t, 1)
	ftpsNoTempFiles(t)
}

func TestFetch3MFSizeMismatch(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:          "bblp",
		pass:          "12345678",
		announcedSize: 100,
		file:          bytes.Repeat([]byte{0x42}, 40),
		serve:         map[string]bool{"/cache/BoxTower.gcode.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTransport)
	srv.assertClean(t, 1)
	ftpsNoTempFiles(t)
}

func TestFetch3MFGreetingStallCancel(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:          "bblp",
		pass:          "12345678",
		greetingDelay: 10 * time.Second,
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		res Result
		err error
	}
	out := make(chan outcome, 1)
	go func() {
		res, err := fetch(ctx, ftpsPrinter(srv.ctrl.Addr().String()),
			ftpsJob("", "/cache/BoxTower.gcode.3mf"))
		out <- outcome{res, err}
	}()
	srv.waitCtrl(t, 5*time.Second) // the client is parked reading the stalled greeting
	cancel()
	select {
	case o := <-out:
		ftpsRequireCategory(t, o.err, catCancelled)
	case <-time.After(10 * time.Second):
		t.Fatal("fetch did not return after cancellation")
	}
	ctrl, _, _, _, retrs, _ := srv.snapshot()
	if ctrl != 1 || len(retrs) != 0 {
		t.Fatalf("control sessions = %d, RETR = %q, want one stalled session and no transfer", ctrl, retrs)
	}
	ftpsNoTempFiles(t)
}

func TestFetch3MFDataStallCancel(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:       "bblp",
		pass:       "12345678",
		file:       bytes.Repeat([]byte{0x42}, 200),
		stallAfter: 10,
		serve:      map[string]bool{"/cache/BoxTower.gcode.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		res Result
		err error
	}
	out := make(chan outcome, 1)
	go func() {
		res, err := fetch(ctx, ftpsPrinter(srv.ctrl.Addr().String()),
			ftpsJob("", "/cache/BoxTower.gcode.3mf"))
		out <- outcome{res, err}
	}()
	srv.waitData(t, 5*time.Second) // the client is parked inside the stalled transfer
	cancel()
	select {
	case o := <-out:
		ftpsRequireCategory(t, o.err, catCancelled)
	case <-time.After(10 * time.Second):
		t.Fatal("fetch did not return after cancellation")
	}
	ctrl, data, _, _, retrs, _ := srv.snapshot()
	if ctrl != 1 || data != 1 || len(retrs) != 1 {
		t.Fatalf("control = %d, data = %d, RETR = %q, want a single stalled attempt", ctrl, data, retrs)
	}
	ftpsNoTempFiles(t)
}

func TestFetch3MFAttemptTimeout(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:       "bblp",
		pass:       "12345678",
		file:       bytes.Repeat([]byte{0x42}, 200),
		stallAfter: 10,
		serve:      map[string]bool{"/cache/BoxTower.gcode.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	start := time.Now()
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTimeout)
	if elapsed := time.Since(start); elapsed < 2*time.Second {
		t.Fatalf("attempt returned after %v, want the attempt deadline to fire", elapsed)
	}
	_, _, _, _, retrs, _ := srv.snapshot()
	if len(retrs) != 1 {
		t.Fatalf("RETR = %q, want the one timed-out transfer", retrs)
	}
	ftpsNoTempFiles(t)
}

func TestFetch3MFNoCandidatesNoDial(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{user: "bblp", pass: "12345678"})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	cases := []struct {
		name, jobName, gcode string
	}{
		{"empty identity", "", ""},
		{"slash in name", "a/b", ""},
		{"traversal gcode", "", "../etc/passwd.3mf"},
		{"control characters", "Bo\x01x", "ok\x07.3mf"},
		{"gcode without 3mf", "", "/cache/x.gcode"},
		{"dot name", ".", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
				ftpsJob(tc.jobName, tc.gcode))
			ftpsRequireCategory(t, err, catNotFound)
		})
	}
	ctrl, data, _, _, retrs, sizes := srv.snapshot()
	if ctrl != 0 || data != 0 || len(retrs) != 0 || len(sizes) != 0 {
		t.Fatalf("control = %d, data = %d, RETR = %q, SIZE = %q, want no dial at all",
			ctrl, data, retrs, sizes)
	}
}

// TestFetch3MFDataTLSStallCancel stalls the data TLS handshake on its
// own: the server accepts the raw data socket but never answers the
// ClientHello, separately from the control-bound stall and the dropped
// data connection. Cancellation must sweep the registered raw socket and
// cancel the attempt without a retry or a leftover temporary file.
func TestFetch3MFDataTLSStallCancel(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:         "bblp",
		pass:         "12345678",
		file:         bytes.Repeat([]byte{0x42}, 200),
		stallDataTLS: true,
		serve:        map[string]bool{"/cache/BoxTower.gcode.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		res Result
		err error
	}
	out := make(chan outcome, 1)
	go func() {
		res, err := fetch(ctx, ftpsPrinter(srv.ctrl.Addr().String()),
			ftpsJob("", "/cache/BoxTower.gcode.3mf"))
		out <- outcome{res, err}
	}()
	srv.waitDataAccepted(t, 5*time.Second) // the lazy data TLS handshake is stalled server-side
	cancel()
	select {
	case o := <-out:
		ftpsRequireCategory(t, o.err, catCancelled)
	case <-time.After(10 * time.Second):
		t.Fatal("fetch did not return after cancellation")
	}
	ctrl, data, _, _, retrs, _ := srv.snapshot()
	if ctrl != 1 || len(retrs) != 1 {
		t.Fatalf("control = %d, RETR = %q, want a single attempt stalled before the data handshake",
			ctrl, retrs)
	}
	if accepted := srv.dataAcceptedCount(); accepted != 1 {
		t.Fatalf("data accepts = %d, want the one stalled connection", accepted)
	}
	if data != 0 {
		t.Fatalf("data sessions = %d, want none (the handshake never completed)", data)
	}
	ftpsNoTempFiles(t)
}

// TestFetch3MFTempStagingFailure isolates os.CreateTemp through the
// temp-directory environment: the RETR has already begun when staging
// fails, so the attempt must end in the transport category with the data
// response closed and no temporary file left in the real temp directory.
func TestFetch3MFTempStagingFailure(t *testing.T) {
	ftpsShortenTiming(t)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:  "bblp",
		pass:  "12345678",
		file:  bytes.Repeat([]byte{0x42}, 200), // small enough for kernel buffers, no stall
		serve: map[string]bool{"/cache/BoxTower.gcode.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	realTmp := os.TempDir() // captured before the environment isolation below
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "absent"))
	_, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsJob("", "/cache/BoxTower.gcode.3mf"))
	ftpsRequireCategory(t, err, catTransport)
	// Staging fails after the single RETR and after the raw data socket
	// was dialled, but before any data read starts the lazy TLS
	// handshake, so the completed-session count is scheduler-dependent
	// and stays unasserted. The bounded poll covers the kernel accept
	// racing fetch's return.
	srv.waitDataAccepted(t, 5*time.Second)
	_, _, _, _, retrs, sizes := srv.snapshot()
	if len(retrs) != 1 || len(sizes) != 1 {
		t.Fatalf("RETR = %q, SIZE = %q, want the failure after the transfer began",
			retrs, sizes)
	}
	matches, gerr := filepath.Glob(filepath.Join(realTmp, "bmbpx-preview-*"))
	if gerr != nil {
		t.Fatalf("glob temporary files: %v", gerr)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary archive files left behind: %v", matches)
	}
	srv.assertClean(t, 1)
}

// TestFetch3MFPartialParsePropagates downloads an archive whose
// slice_info entry is malformed while the model metadata and the plate
// render are valid: fetch must return parseArchive's accepted partial
// result with the categorized failure, so a valid image stays ready and
// no field of the failed entry is invented.
func TestFetch3MFPartialParsePropagates(t *testing.T) {
	ftpsShortenTiming(t)
	render := ftpsTestPNG(t, 2, 2)
	srv := newFakeFTPS(t, fakeFTPSConf{
		user:  "bblp",
		pass:  "12345678",
		file:  ftpsTestPartial3MF(t, "Partial Keep", render),
		serve: map[string]bool{"/cache/BoxTower.gcode.3mf": true},
	})
	ftpsPointControlAt(t, srv.ctrl.Addr().String())
	res, err := fetch(ftpsFetchCtx(t), ftpsPrinter(srv.ctrl.Addr().String()),
		ftpsPlateJob("", "/cache/BoxTower.gcode.3mf", 1))
	if err == nil {
		t.Fatal("fetch error = nil, want the failed slice_info entry reported")
	}
	if got := errorCategory(err); got != catArchive {
		t.Fatalf("category = %q, want %q (the malformed slice_info entry is reported)", got, catArchive)
	}
	if res.PNG == nil || !bytes.Equal(res.PNG, render) {
		t.Fatalf("png = %d bytes, want the accepted render to survive the failed entry", len(res.PNG))
	}
	if res.Preview.Status != StatusReady {
		t.Fatalf("status = %q, want ready from the surviving image", res.Preview.Status)
	}
	md := res.Metadata
	if md == nil || md.Title == nil || *md.Title != "Partial Keep" {
		t.Fatalf("metadata = %+v, want the accepted model title", md)
	}
	if md.Source != sourcePrinter3mf {
		t.Fatalf("source = %q, want %q", md.Source, sourcePrinter3mf)
	}
	if md.Plate != nil || md.Process != nil || md.ObjectCount != nil {
		t.Fatalf("plate/process/object_count = %+v/%+v/%+v, want nil: no entry supplying them parsed",
			md.Plate, md.Process, md.ObjectCount)
	}
	if md.Materials == nil || len(md.Materials) != 0 ||
		md.Objects == nil || len(md.Objects) != 0 ||
		md.Warnings == nil || len(md.Warnings) != 0 {
		t.Fatalf("arrays = %v/%v/%v, want non-nil and empty", md.Materials, md.Objects, md.Warnings)
	}
	_, _, _, _, retrs, _ := srv.snapshot()
	if len(retrs) != 1 {
		t.Fatalf("RETR = %q, want the single download", retrs)
	}
	srv.assertClean(t, 1)
	ftpsNoTempFiles(t)
}
