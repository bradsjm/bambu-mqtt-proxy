// Package tlsutil loads or creates the proxy TLS certificate.
package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Ensure returns the TLS certificate for certPath/keyPath. When either path
// is empty the certificate is generated in memory only. When both files
// exist they are loaded; when neither exists a self-signed ECDSA P-256
// certificate (CN bambulab.com, valid ten years) is generated and persisted
// for stable restarts. Existing files are never rewritten: a pair with
// exactly one file present, or a pair that fails to load, is an error
// naming the paths.
func Ensure(certPath, keyPath string) (tls.Certificate, error) {
	if certPath == "" || keyPath == "" {
		return generate()
	}
	if certPath == keyPath {
		return tls.Certificate{}, fmt.Errorf("cert and key paths must differ: %s", certPath)
	}

	certExists, err := statExists(certPath)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyExists, err := statExists(keyPath)
	if err != nil {
		return tls.Certificate{}, err
	}
	switch {
	case certExists && keyExists:
		return tls.LoadX509KeyPair(certPath, keyPath)
	case certExists || keyExists:
		return tls.Certificate{}, fmt.Errorf(
			"incomplete certificate pair: %s and %s must both exist or both be absent", certPath, keyPath)
	}

	cert, certPEM, keyPEM, err := generatePEM()
	if err != nil {
		return tls.Certificate{}, err
	}
	if err := writeNewPair(certPath, keyPath, certPEM, keyPEM); err != nil {
		return tls.Certificate{}, err
	}
	return cert, nil
}

// statExists reports whether path exists, distinguishing a missing file
// from any other stat failure.
func statExists(path string) (bool, error) {
	_, err := os.Stat(path)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	default:
		return false, fmt.Errorf("stat %s: %w", path, err)
	}
}

// generate builds an in-memory certificate without touching the filesystem.
func generate() (tls.Certificate, error) {
	cert, _, _, err := generatePEM()
	return cert, err
}

// generatePEM creates the self-signed key/certificate and returns the
// parsed leaf together with its PEM encodings. The pair is validated here
// so a write path can never persist an unusable certificate.
func generatePEM() (tls.Certificate, []byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, nil, fmt.Errorf("generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 63))
	if err != nil {
		return tls.Certificate{}, nil, nil, fmt.Errorf("generate serial: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "bambulab.com"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "bambulab.com"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, nil, fmt.Errorf("create certificate: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, nil, nil, fmt.Errorf("marshal key: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, nil, fmt.Errorf("parse generated pair: %w", err)
	}
	return cert, certPEM, keyPEM, nil
}

// writeNewPair creates certPath and keyPath for a freshly generated pair.
// Both files are opened with O_CREATE|O_EXCL before anything is written, so
// a file created concurrently (or an existing one) fails the call instead of
// being overwritten. Only files created by this call are removed on failure.
func writeNewPair(certPath, keyPath string, certPEM, keyPEM []byte) error {
	if err := os.MkdirAll(filepath.Dir(certPath), 0o755); err != nil {
		return fmt.Errorf("create cert dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o755); err != nil {
		return fmt.Errorf("create key dir: %w", err)
	}

	certFile, err := os.OpenFile(certPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create cert %s: %w", certPath, err)
	}

	keyFile, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		_ = certFile.Close()
		_ = os.Remove(certPath)
		return fmt.Errorf("create key %s: %w", keyPath, err)
	}

	// Both files were created by this invocation. Every failure from here
	// removes exactly this freshly created pair.
	cleanup := func() {
		_ = certFile.Close()
		_ = keyFile.Close()
		_ = os.Remove(certPath)
		_ = os.Remove(keyPath)
	}

	if _, err := certFile.Write(certPEM); err != nil {
		cleanup()
		return fmt.Errorf("write cert %s: %w", certPath, err)
	}
	if err := certFile.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync cert %s: %w", certPath, err)
	}
	if err := certFile.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close cert %s: %w", certPath, err)
	}

	if _, err := keyFile.Write(keyPEM); err != nil {
		cleanup()
		return fmt.Errorf("write key %s: %w", keyPath, err)
	}
	if err := keyFile.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync key %s: %w", keyPath, err)
	}
	if err := keyFile.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close key %s: %w", keyPath, err)
	}
	return nil
}
