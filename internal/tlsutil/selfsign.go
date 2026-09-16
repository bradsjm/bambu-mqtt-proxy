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
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Ensure returns the TLS certificate for certPath/keyPath. When both files
// exist they are loaded; otherwise a self-signed ECDSA P-256 certificate
// (CN bambulab.com, valid ten years) is generated and, when paths are given,
// persisted for stable restarts.
func Ensure(certPath, keyPath string) (tls.Certificate, error) {
	if certPath != "" && keyPath != "" {
		if _, err := os.Stat(certPath); err == nil {
			if _, err2 := os.Stat(keyPath); err2 == nil {
				return tls.LoadX509KeyPair(certPath, keyPath)
			}
		}
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 63))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate serial: %w", err)
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
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal key: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	if certPath != "" && keyPath != "" {
		if err := os.MkdirAll(filepath.Dir(certPath), 0o755); err != nil {
			return tls.Certificate{}, fmt.Errorf("create cert dir: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(keyPath), 0o755); err != nil {
			return tls.Certificate{}, fmt.Errorf("create key dir: %w", err)
		}
		if err := os.WriteFile(certPath, certPEM, 0o644); err != nil {
			return tls.Certificate{}, fmt.Errorf("write cert: %w", err)
		}
		if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
			return tls.Certificate{}, fmt.Errorf("write key: %w", err)
		}
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse generated pair: %w", err)
	}
	return cert, nil
}
