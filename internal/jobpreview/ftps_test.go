package jobpreview

import (
	"bambu-mqtt-proxy/internal/config"
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
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeFTPSCert creates a throwaway TLS certificate.
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

// TestFetchResumesSession verifies protected data TLS reuses the control session.
func TestFetchResumesSession(t *testing.T) {
	fixture, image := archiveFixture(t)
	cert, err := fakeFTPSCert()
	if err != nil {
		t.Fatal(err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12}
	ctrl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ctrl.Close()
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	done := make(chan error, 1)
	resumed := make(chan bool, 1)
	go func() {
		raw, err := ctrl.Accept()
		if err != nil {
			done <- err
			return
		}
		tc := tls.Server(raw, cfg)
		defer tc.Close()
		tc.SetDeadline(time.Now().Add(5 * time.Second))
		reply := func(line string) { fmt.Fprintf(tc, "%s\r\n", line) }
		reply("220 ready")
		scanner := bufio.NewScanner(tc)
		for scanner.Scan() {
			verb, arg, _ := strings.Cut(scanner.Text(), " ")
			switch verb {
			case "USER":
				reply("331 password")
			case "PASS":
				reply("230 logged in")
			case "TYPE", "PBSZ":
				reply("200 ok")
			case "FEAT":
				reply("211 no features")
			case "PROT":
				if arg != "P" {
					done <- fmt.Errorf("unprotected data")
					return
				}
				reply("200 protected")
			case "SIZE":
				reply(fmt.Sprintf("213 %d", len(fixture)))
			case "EPSV":
				reply(fmt.Sprintf("229 Entering Extended Passive Mode (|||%d|)", data.Addr().(*net.TCPAddr).Port))
			case "RETR":
				reply("150 data")
				raw, err := data.Accept()
				if err != nil {
					done <- err
					return
				}
				dc := tls.Server(raw, cfg)
				dc.SetDeadline(time.Now().Add(5 * time.Second))
				if err := dc.Handshake(); err != nil {
					dc.Close()
					done <- err
					return
				}
				resumed <- dc.ConnectionState().DidResume
				_, err = io.Copy(dc, bytes.NewReader(fixture))
				dc.Close()
				if err != nil {
					done <- err
					return
				}
				reply("226 complete")
			case "QUIT":
				done <- nil
				return
			default:
				reply("200 ok")
			}
		}
		done <- scanner.Err()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := fetchAddress(ctx, ctrl.Addr().String(), config.Printer{Username: "bblp", Password: "12345678", InsecureSkipVerify: true}, jobKey{Name: "fixture", Plate: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.PNG, image) {
		t.Fatal("thumbnail differs")
	}
	select {
	case ok := <-resumed:
		if !ok {
			t.Fatal("data TLS did not resume control session")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
