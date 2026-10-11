package pandapwr

import (
	"bambu-mqtt-proxy/internal/config"
	"io"
	"log/slog"
	"testing"
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
// bare host name instead of trimming a stored URL.
func TestNewBuildsHTTPHostTarget(t *testing.T) {
	s := New([]config.Printer{{Serial: "S1",
		Settings: map[string]any{AddressKey: "192.168.4.1"}}}, logger())
	if len(s.targets) != 1 || s.targets[0].addr != "http://192.168.4.1" {
		t.Fatalf("targets = %+v, want S1 at http://192.168.4.1", s.targets)
	}
	if s.moduleState("S1") == nil {
		t.Fatal("configured printer returned no state")
	}
}
