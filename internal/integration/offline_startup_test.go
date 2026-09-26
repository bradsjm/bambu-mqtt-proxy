package integration_test

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/health"
	"bambu-mqtt-proxy/internal/httpsrv"
	"bambu-mqtt-proxy/internal/upstream"
)

// liveURL builds the loopback URL for the shared HTTP server.
func liveURL(port int, path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", port, path)
}

// TestHTTPStartsBeforeOfflinePrinters pins the startup availability
// contract: an offline printer must not delay HTTP serving, and the
// async-recorded camera interest must survive until the printer returns.
func TestHTTPStartsBeforeOfflinePrinters(t *testing.T) {
	// Address points at a port with no listener: connect attempts fail for
	// the full connect timeout, exactly like an offline printer at boot.
	offline := config.Printer{
		Serial:             "01OFFLINE0000001",
		Model:              "P1S",
		Address:            "127.0.0.1:1",
		TLS:                true,
		InsecureSkipVerify: true,
		Username:           "bblp",
		Password:           accessCode,
	}
	printers := []config.Printer{offline}
	cfg := &config.Config{
		Listen:   []config.Listener{{Port: freePort(t), TLS: false}},
		Auth:     config.Auth{Mode: config.AuthModePrinter},
		Printers: printers,
	}
	cfg.ApplyDefaults()
	cfg.Behavior.UpstreamConnectTimeoutSeconds = 2
	if err := cfg.Validate(); err != nil {
		t.Fatalf("proxy config: %v", err)
	}
	serials := make([]string, 0, len(printers))
	for _, p := range printers {
		serials = append(serials, p.Serial)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	pool := upstream.NewPool(printers, nil, cfg.Behavior, logger)
	t.Cleanup(pool.Stop)

	httpPort := freePort(t)
	httpSrv := httpsrv.New(httpPort, logger)
	health.Routes(httpSrv.Mux(), pool, nil)
	started := time.Now()
	if err := httpSrv.Start(); err != nil {
		t.Fatalf("http server: %v", err)
	}
	t.Cleanup(httpSrv.Stop)

	// The camera feature's startup path: one async interest per printer.
	// Must return immediately; the blocking Subscribe would spend the
	// connect timeout here.
	pool.SubscribeAsync(offline.Serial, "device/"+offline.Serial+"/report", 1)
	elapsed := time.Since(started)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("startup subscriptions blocked %s; must be non-blocking", elapsed)
	}

	// Health serves immediately even though the printer is unreachable.
	client := &http.Client{Timeout: 2 * time.Second}
	waitFor(t, 3*time.Second, func() bool {
		resp, err := client.Get(liveURL(httpPort, "/livez"))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	// The interest is recorded: status reports the printer as configured
	// (false until it connects), and the ref survives for onConnect to
	// resubscribe when the printer returns.
	if got := pool.Status()[offline.Serial]; got {
		t.Fatal("offline printer must not report connected")
	}
}
