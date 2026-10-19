package pandapwr

import (
	"bambu-mqtt-proxy/internal/config"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// logger returns a quiet logger.
func logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestAddressSettingValidate covers the address validation: a bare host or
// host:port form is accepted and every scheme form is rejected.
func TestAddressSettingValidate(t *testing.T) {
	accept := []string{"192.168.4.1", "10.0.0.5:8080", "panda-pwr.local"}
	for _, in := range accept {
		if err := AddressSetting.Validate(in); err != nil {
			t.Errorf("Validate(%q) rejected: %v", in, err)
		}
	}
	reject := []string{"http://192.168.4.1", "https://192.168.4.1/update_ele_data", "192.168.4.1/"}
	for _, in := range reject {
		if err := AddressSetting.Validate(in); err == nil {
			t.Errorf("Validate(%q) accepted, want rejection", in)
		}
	}
}

// TestNewBuildsHTTPHostTarget requires New to build the http:// base from a
// bare host name instead of trimming a stored URL. The target is observed
// through a real poll request.
func TestNewBuildsHTTPHostTarget(t *testing.T) {
	type poll struct{ host, path string }
	polls := make(chan poll, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case polls <- poll{host: r.Host, path: r.URL.Path}:
		default:
		}
		_, _ = w.Write([]byte(`{"power":12}`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	s := New([]config.Printer{{Serial: "S1",
		Settings: map[string]any{AddressKey: addr}}}, logger())
	state, ok := s.Module().State("S1").(ModuleState)
	if !ok || state.Link != LinkOffline {
		t.Fatalf("configured state = %+v, want offline", s.Module().State("S1"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case got := <-polls:
		if got.host != addr || got.path != "/update_ele_data" {
			t.Fatalf("poll = %+v, want host %q at /update_ele_data", got, addr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no poll request arrived")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Panda Power Run did not return after cancel")
	}
}
