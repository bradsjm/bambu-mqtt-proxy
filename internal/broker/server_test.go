package broker

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/routing"
	"bambu-mqtt-proxy/internal/upstream"
)

// freePort reserves an ephemeral port and releases it for the test bind.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func newTestConfig(t *testing.T, ports ...int) *config.Config {
	t.Helper()
	cfg := &config.Config{
		Listen: []config.Listener{
			{Port: ports[0], TLS: false},
			{Port: ports[0], TLS: false}, // same port again: the second bind must fail
		},
		Printers: []config.Printer{{
			Serial: "01P00A0000001", Model: "P1S", Address: "127.0.0.1:1",
			Username: "bblp", Password: "00008888",
		}},
	}
	cfg.ApplyDefaults()
	return cfg
}

func TestNewListenerFailureReleasesPortAndInjector(t *testing.T) {
	port := freePort(t)
	cfg := newTestConfig(t, port)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	inject := NewInjector(logger)
	pool := upstream.NewPool(cfg.Printers, inject, cfg.Behavior, logger)
	t.Cleanup(pool.Stop)
	table := routing.NewTable([]string{cfg.Printers[0].Serial})

	srv, err := New(cfg, table, pool, inject, logger)
	if err == nil {
		_ = srv.Close()
		t.Fatal("duplicate listener accepted")
	}
	if inject.srv != nil {
		t.Fatal("failed construction left the injector attached")
	}

	// The first listener's port must be bindable again in the exact form
	// broker.New uses.
	probe, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		t.Fatalf("port still held after failed New: %v", err)
	}
	_ = probe.Close()

	// A retry with only the valid listener binds it and attaches the
	// injector only after success.
	cfg.Listen = cfg.Listen[:1]
	srv2, err := New(cfg, table, pool, inject, logger)
	if err != nil {
		t.Fatalf("retry after failed New: %v", err)
	}
	if inject.srv == nil {
		t.Fatal("successful construction did not attach the injector")
	}
	if err := srv2.Serve(); err != nil {
		t.Fatalf("serve retry: %v", err)
	}
	if err := srv2.Close(); err != nil {
		t.Fatalf("close retry: %v", err)
	}
}

func TestCloseOnServingServerKeepsWorking(t *testing.T) {
	port := freePort(t)
	cfg := newTestConfig(t, port)
	cfg.Listen = cfg.Listen[:1]
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	inject := NewInjector(logger)
	pool := upstream.NewPool(cfg.Printers, inject, cfg.Behavior, logger)
	t.Cleanup(pool.Stop)

	srv, err := New(cfg, routing.NewTable([]string{cfg.Printers[0].Serial}), pool, inject, logger)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := srv.Serve(); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	// The normal teardown path from main must keep working once.
	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
