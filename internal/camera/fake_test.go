package camera

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"
)

// fakeCamera is a minimal P1/A1 camera stand-in: TLS semantics, an 80-byte
// auth payload, then length-prefixed JPEG frames (no login reply).
type fakeCamera struct {
	ln      net.Listener
	frames  chan []byte
	authErr chan error
}

// errBadMagic reports a malformed auth payload in tests.
var errBadMagic = errors.New("bad auth magic")

// newFakeCamera starts a TLS listener emulating the chamber image protocol.
func newFakeCamera(t *testing.T) *fakeCamera {
	t.Helper()
	cert, err := selfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	// The production capture always derives camera port 6000 from the
	// printer host, so the fake binds the real port. Tests run sequentially
	// (no t.Parallel) and release the listener via t.Cleanup, so rebinding
	// per test is safe.
	ln, err := tls.Listen("tcp", "127.0.0.1:6000", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	if err != nil {
		t.Fatal(err)
	}
	fc := &fakeCamera{ln: ln, frames: make(chan []byte, 8), authErr: make(chan error, 8)}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go fc.handle(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return fc
}

// handle serves one camera session.
func (fc *fakeCamera) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	var header [16]byte
	if _, err := readFull(r, header[:]); err != nil {
		fc.authErr <- err
		return
	}
	if binary.LittleEndian.Uint32(header[0:4]) != 0x40 {
		fc.authErr <- errBadMagic
		return
	}
	auth := make([]byte, authPayloadLen-16)
	if _, err := readFull(r, auth); err != nil {
		fc.authErr <- err
		return
	}
	for frame := range fc.frames {
		hdr := make([]byte, 16)
		binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(frame)))
		if _, err := conn.Write(append(hdr, frame...)); err != nil {
			return
		}
	}
}

// serveError reports why the fake camera dropped a session, for tests.
func (fc *fakeCamera) serveError() <-chan error {
	return fc.authErr
}

// jpeg builds a minimal valid JPEG (SOI ... EOI).
func jpeg(n int) []byte {
	b := make([]byte, n)
	b[0], b[1] = 0xFF, 0xD8
	b[n-2], b[n-1] = 0xFF, 0xD9
	_, _ = rand.Read(b[2 : n-2])
	return b
}

// readFull blocks until the buffer is full.
func readFull(r *bufio.Reader, p []byte) (int, error) {
	total := 0
	for total < len(p) {
		n, err := r.Read(p[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// waitUntil polls fn until true or the deadline passes.
func waitUntil(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// selfSignedCert generates a throwaway certificate for the fake camera.
func selfSignedCert() (tls.Certificate, error) {
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
