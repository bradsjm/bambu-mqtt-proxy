package pandabreath

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"bambu-mqtt-proxy/internal/config"
)

// TestAddressSettingValidate covers the address validation: host names and host:port pairs are accepted and every scheme form is rejected..
func TestAddressSettingValidate(t *testing.T) {
	accept := []string{"panda-breath-blue.iot", "10.0.0.5:8080"}
	for _, in := range accept {
		if err := AddressSetting.Validate(in); err != nil {
			t.Errorf("Validate(%q) rejected: %v", in, err)
		}
	}
	reject := []string{"ws://panda/ws", "wss://panda/ws"}
	for _, in := range reject {
		if err := AddressSetting.Validate(in); err == nil {
			t.Errorf("Validate(%q) accepted, want rejection", in)
		}
	}
}

// TestNewBuildsWebSocketTarget requires New to build ws://<host>/ws from a
// bare host name. A fake WebSocket server observes the dial target.
func TestNewBuildsWebSocketTarget(t *testing.T) {
	type dial struct{ host, path string }
	dials := make(chan dial, 1)
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		select {
		case dials <- dial{host: r.Host, path: r.URL.Path}:
		default:
		}
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	printers := []config.Printer{
		{Serial: "S1", Settings: map[string]any{AddressKey: addr}},
		{Serial: "S2"},
	}
	s := New(printers, slog.New(slog.DiscardHandler))
	if s.Display("S2") != nil {
		t.Fatal("unconfigured printer returned a display")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case got := <-dials:
		if got.host != addr || got.path != "/ws" {
			t.Fatalf("dial = %+v, want host %q at /ws", got, addr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no WebSocket dial arrived")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Panda Breath Run did not return after cancel")
	}
}
