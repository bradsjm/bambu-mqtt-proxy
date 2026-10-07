package pandabreath

import (
	"log/slog"
	"testing"

	"bambu-mqtt-proxy/internal/config"
)

// TestAddressSettingValidate covers the address validation: host names and
// host:port pairs are accepted and every scheme form is rejected.
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
// bare host name, so the observer dials the device path directly. Missing
// or invalid addresses must produce no target; an invalid address never
// reaches New because Validate rejects it, but the helper stays constructed.
func TestNewBuildsWebSocketTarget(t *testing.T) {
	printers := []config.Printer{
		{Serial: "S1", Settings: map[string]any{AddressKey: "panda.local"}},
		{Serial: "S2"},
	}
	s := New(printers, slog.New(slog.DiscardHandler))
	if len(s.targets) != 1 {
		t.Fatalf("targets = %d, want 1", len(s.targets))
	}
	if s.targets[0].serial != "S1" || s.targets[0].addr != "ws://panda.local/ws" {
		t.Fatalf("target = %+v, want S1 at ws://panda.local/ws", s.targets[0])
	}
	if s.Display("S2") != nil {
		t.Fatal("unconfigured printer returned a display")
	}
}
