package safety

import (
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/notification"
	"bambu-mqtt-proxy/internal/pandabreath"
	"bambu-mqtt-proxy/internal/printer"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// chamberProbe controls accessory presence and its retained value.
type chamberProbe struct {
	// value is the current chamber temperature.
	value float64
	// known controls reading presence.
	known bool
}

// Chamber returns the test accessory reading.
func (c *chamberProbe) Chamber(string) (float64, time.Time, bool) {
	return c.value, time.Time{}, c.known
}

// stopCounter counts fleet-wide stop requests.
type stopCounter struct {
	// count is the number of stop requests.
	count int
}

// Stop records a stop request.
func (c *stopCounter) Stop(string) error { c.count++; return nil }

// TestEpisodeLatch protects one trip per episode and rearming only on in-limit readings.
func TestEpisodeLatch(t *testing.T) {
	printers := []config.Printer{{Serial: "S1", Model: "P1S", Settings: map[string]any{pandabreath.AddressKey: "breath.local"}}, {Serial: "S2"}}
	state := printer.NewStore(printers)
	state.Apply("S1", []byte(`{"print":{"nozzle_temper":200,"bed_temper":60}}`))
	chamber := &chamberProbe{value: 81, known: true}
	cmd := &stopCounter{}
	s := New(printers, state, chamber, cmd, nil, notification.Settings{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.send = func(context.Context, notification.Settings, string, string) error { return nil }
	for _, step := range []struct {
		name          string
		online, known bool
		value         float64
		stops         int
	}{
		{"trip", true, true, 81, 2},
		{"no retrip", true, true, 81, 2},
		{"device loss", true, false, 0, 2},
		{"printer offline", false, true, 20, 2},
		{"in limit", true, true, 20, 2},
		{"next episode", true, true, 81, 4},
	} {
		state.SetOnline("S1", step.online)
		chamber.known, chamber.value = step.known, step.value
		s.check(context.Background(), printers[0])
		s.responses.Wait()
		if cmd.count != step.stops {
			t.Fatalf("%s: stops=%d, want %d", step.name, cmd.count, step.stops)
		}
	}
}
