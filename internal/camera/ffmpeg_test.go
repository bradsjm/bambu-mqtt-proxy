// RTSPS capture coverage for the FFmpeg backend: stderr is discarded, so
// credentials FFmpeg reflects (raw and URL-escaped) can never reach logs at
// any level while frames keep flowing.
package camera

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// secretWriter is a concurrent io.Writer collecting log output.
type secretWriter struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (w *secretWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *secretWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// TestRTSPSSecretsNeverReachLogs runs the RTSPS backend against a fake
// FFmpeg that writes password markers (raw and URL-escaped, as FFmpeg and
// the RTSPS URL would render them) to stderr and then serves one valid
// frame. The markers must never appear in debug output, and the frame must
// still be published.
func TestRTSPSSecretsNeverReachLogs(t *testing.T) {
	const (
		rawMarker     = "BMBPX-RAW-PASSWORD-MARKER"
		escapedMarker = "BMBPX-ESCAPED-PASSWORD-MARKER"
	)
	// Octal escapes keep the JPEG bytes portable across /bin/sh printf
	// implementations: FF D8 FF D9 is a minimal valid JPEG.
	script := fmt.Sprintf("#!/bin/sh\n"+
		"echo %s >&2\n"+
		"echo %s >&2\n"+
		"printf -- '--ffmpeg\\r\\nContent-Type: image/jpeg\\r\\n\\r\\n'\n"+
		"printf '\\377\\330\\377\\331'\n"+
		"printf '\\r\\n--ffmpeg--\\r\\n'\n"+
		"sleep 30\n", rawMarker, escapedMarker)
	path := filepath.Join(t.TempDir(), "ffmpeg")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	logs := &secretWriter{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	m := NewWebManager([]config.Printer{{
		Serial:   "00M09AFFMPEG001",
		Model:    "X1C",
		Address:  "127.0.0.1:1",
		Username: "bblp",
		Password: "p@ss:word",
	}}, logger)
	m.ffmpegPath = path
	t.Cleanup(m.Close)

	if _, st := m.AcquireWeb("00M09AFFMPEG001"); st != StatusOK {
		t.Fatalf("AcquireWeb = %v, want ok", st)
	}
	waitUntil(t, 5*time.Second, func() bool { return m.Latest("00M09AFFMPEG001") != nil })

	out := logs.String()
	if strings.Contains(out, rawMarker) || strings.Contains(out, escapedMarker) {
		t.Fatalf("FFmpeg stderr markers leaked into logs:\n%s", out)
	}
	if strings.Contains(out, "p@ss:word") {
		t.Fatalf("printer password leaked into logs:\n%s", out)
	}
}
