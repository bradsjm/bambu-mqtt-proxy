package broker

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
	"bambu-mqtt-proxy/internal/routing"
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
	links := printer.New(cfg.Printers, logger, nil, nil)
	t.Cleanup(links.Close)
	table := routing.NewTable([]string{cfg.Printers[0].Serial}, nil)
	report := fmt.Sprintf("device/%s/report", cfg.Printers[0].Serial)

	srv, err := New(cfg, table, links, inject, logger)
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
	// injector after success: a downstream subscriber receives a report
	// published through it.
	cfg.Listen = cfg.Listen[:1]
	srv2, err := New(cfg, table, links, inject, logger)
	if err != nil {
		t.Fatalf("retry after failed New: %v", err)
	}
	if inject.srv == nil {
		t.Fatal("successful construction did not attach the injector")
	}
	if err := srv2.Serve(); err != nil {
		t.Fatalf("serve retry: %v", err)
	}
	received := make(chan string, 1)
	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", port)).
		SetClientID("injector-test").
		SetUsername(cfg.Printers[0].Username).
		SetPassword(cfg.Printers[0].Password).
		SetCleanSession(true).
		SetConnectTimeout(5 * time.Second).
		SetDefaultPublishHandler(func(_ mqtt.Client, m mqtt.Message) {
			received <- string(m.Payload())
		})
	client := mqtt.NewClient(opts)
	connect := client.Connect()
	if !connect.WaitTimeout(5*time.Second) || connect.Error() != nil {
		t.Fatalf("subscriber connect: %v", connect.Error())
	}
	t.Cleanup(func() { client.Disconnect(100) })
	sub := client.Subscribe(report, 0, nil)
	if !sub.WaitTimeout(5*time.Second) || sub.Error() != nil {
		t.Fatalf("subscribe: %v", sub.Error())
	}
	const payload = `{"print":{"gcode_state":"RUNNING"}}`
	inject.PublishDownstream(report, []byte(payload))
	select {
	case got := <-received:
		if got != payload {
			t.Fatalf("injected payload = %q, want %q", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("successful construction did not attach the injector")
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
	links := printer.New(cfg.Printers, logger, nil, nil)
	t.Cleanup(links.Close)

	srv, err := New(cfg, routing.NewTable([]string{cfg.Printers[0].Serial}, nil), links, inject, logger)
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
