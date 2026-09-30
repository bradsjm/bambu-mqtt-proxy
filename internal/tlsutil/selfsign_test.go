package tlsutil

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pairFiles returns distinct cert/key paths inside a fresh directory.
func pairFiles(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
}

func TestEnsureCreatesReusablePairWithPermissions(t *testing.T) {
	certPath, keyPath := pairFiles(t)

	first, err := Ensure(certPath, keyPath)
	if err != nil {
		t.Fatalf("first Ensure: %v", err)
	}

	certInfo, err := os.Stat(certPath)
	if err != nil {
		t.Fatalf("cert file: %v", err)
	}
	if certInfo.Mode().Perm() != 0o644 {
		t.Fatalf("cert mode = %v, want 0644", certInfo.Mode().Perm())
	}
	keyInfo, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("key file: %v", err)
	}
	if keyInfo.Mode().Perm() != 0o600 {
		t.Fatalf("key mode = %v, want 0600", keyInfo.Mode().Perm())
	}

	// A restart must reuse the persisted pair instead of regenerating it.
	second, err := Ensure(certPath, keyPath)
	if err != nil {
		t.Fatalf("second Ensure: %v", err)
	}
	if !bytes.Equal(first.Certificate[0], second.Certificate[0]) {
		t.Fatal("second Ensure returned a different certificate; the pair was rewritten")
	}
}

func TestEnsureEmptyPathsGenerateInMemory(t *testing.T) {
	cert, err := Ensure("", "")
	if err != nil {
		t.Fatalf("Ensure with empty paths: %v", err)
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("in-memory Ensure returned no certificate")
	}
}

func TestEnsureHalfPairFailsWithoutChanges(t *testing.T) {
	t.Run("cert only", func(t *testing.T) {
		certPath, keyPath := pairFiles(t)
		otherKey := filepath.Join(filepath.Dir(certPath), "other.key")
		if _, err := Ensure(certPath, otherKey); err != nil {
			t.Fatalf("seed cert: %v", err)
		}
		if err := os.Remove(otherKey); err != nil {
			t.Fatal(err)
		}
		assertHalfPairRejected(t, certPath, keyPath)
	})
	t.Run("key only", func(t *testing.T) {
		certPath, keyPath := pairFiles(t)
		otherCert := filepath.Join(filepath.Dir(keyPath), "other.crt")
		if _, err := Ensure(otherCert, keyPath); err != nil {
			t.Fatalf("seed key: %v", err)
		}
		if err := os.Remove(otherCert); err != nil {
			t.Fatal(err)
		}
		assertHalfPairRejected(t, certPath, keyPath)
	})
}

// assertHalfPairRejected requires Ensure to reject a half pair: the error
// names the incomplete pair, the existing half keeps its bytes, and the
// missing half is still missing.
func assertHalfPairRejected(t *testing.T, certPath, keyPath string) {
	t.Helper()
	existing := firstExisting(t, certPath, keyPath)
	before, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}

	_, err = Ensure(certPath, keyPath)
	if err == nil {
		t.Fatal("half pair accepted")
	}
	if !strings.Contains(err.Error(), "incomplete certificate pair") {
		t.Fatalf("error = %v, want an incomplete certificate pair message", err)
	}
	if !strings.Contains(err.Error(), certPath) || !strings.Contains(err.Error(), keyPath) {
		t.Fatalf("error %v must name both paths %s and %s", err, certPath, keyPath)
	}
	after, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("existing half was modified")
	}
	missing := certPath
	if _, err := os.Stat(certPath); err == nil {
		missing = keyPath
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("missing half %s was created", missing)
	}
}

// firstExisting returns whichever of the two paths exists.
func firstExisting(t *testing.T, a, b string) string {
	t.Helper()
	for _, p := range []string{a, b} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	t.Fatal("neither file exists")
	return ""
}

func TestEnsureInvalidExistingPairReturnsLoadError(t *testing.T) {
	certPath, keyPath := pairFiles(t)
	certBody := []byte("not a certificate\n")
	keyBody := []byte("not a key\n")
	if err := os.WriteFile(certPath, certBody, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyBody, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Ensure(certPath, keyPath); err == nil {
		t.Fatal("invalid existing pair accepted")
	}
	for path, want := range map[string][]byte{certPath: certBody, keyPath: keyBody} {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s was modified: %q", path, got)
		}
	}
}

func TestEnsureIdenticalPathsFailWithoutChanges(t *testing.T) {
	dir := t.TempDir()
	same := filepath.Join(dir, "pair.pem")
	body := []byte("existing bytes\n")
	if err := os.WriteFile(same, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Ensure(same, same); err == nil {
		t.Fatal("identical cert/key paths accepted")
	}
	got, err := os.ReadFile(same)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("existing file was modified: %q", got)
	}

	absent := filepath.Join(dir, "absent.pem")
	if _, err := Ensure(absent, absent); err == nil {
		t.Fatal("identical absent paths accepted")
	}
	if _, err := os.Stat(absent); !os.IsNotExist(err) {
		t.Fatal("identical absent paths created a file")
	}
}

// TestWriteNewPairLosesExclusiveCreateRace simulates another writer
// creating the key inside the Ensure window: after the stats see nothing,
// the key file appears. The call must fail, keep the key bytes, and remove
// the cert file it created.
func TestWriteNewPairLosesExclusiveCreateRace(t *testing.T) {
	certPath, keyPath := pairFiles(t)
	keyBody := []byte("concurrent key\n")
	if err := os.WriteFile(keyPath, keyBody, 0o600); err != nil {
		t.Fatal(err)
	}

	err := writeNewPair(certPath, keyPath, []byte("cert"), []byte("key"))
	if err == nil {
		t.Fatal("exclusive-create conflict accepted")
	}
	if _, err := os.Stat(certPath); !os.IsNotExist(err) {
		t.Fatal("cert file created by the failed call was left behind")
	}
	got, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, keyBody) {
		t.Fatalf("concurrently created key was modified: %q", got)
	}
}

// TestWriteNewPairLosesExclusiveCreateRaceOnCert checks the mirror race on
// the certificate path.
func TestWriteNewPairLosesExclusiveCreateRaceOnCert(t *testing.T) {
	certPath, keyPath := pairFiles(t)
	certBody := []byte("concurrent cert\n")
	if err := os.WriteFile(certPath, certBody, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := writeNewPair(certPath, keyPath, []byte("cert"), []byte("key")); err == nil {
		t.Fatal("exclusive-create conflict accepted")
	}
	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Fatal("key file created by the failed call was left behind")
	}
	got, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, certBody) {
		t.Fatalf("concurrently created cert was modified: %q", got)
	}
}
