package camera

import (
	"bytes"
	"encoding/json"
	"testing"

	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/telemetry"
)

// agingModuleValue represents a module value with continuously aging fields.
type agingModuleValue struct {
	State string  `json:"state"`
	Age   float64 `json:"age_seconds"`
}

// StableKey returns only the fields that should trigger an events update.
func (v agingModuleValue) StableKey() any { return v.State }

// TestSetModulesRejectsStateKeyCollision verifies that a module whose
// State hook serves a payload member colliding with a core tile key is
// rejected at wiring time.
func TestSetModulesRejectsStateKeyCollision(t *testing.T) {
	printers := previewPrinters()
	renderer := NewStatusRenderer(NewManager(printers, discardLogger()), telemetry.NewCache(printers, discardLogger()), testConnectivity{})
	if err := renderer.SetModules([]module.Module{{Name: "camera_reason", State: func(string) any { return "x" }}}); err == nil {
		t.Fatal("SetModules must reject a module name colliding with a core tile member")
	}
}

// TestModuleStateStableKey verifies that aging alone does not trigger events.
func TestModuleStateStableKey(t *testing.T) {
	tile := Tile{extra: map[string]any{"sample": agingModuleValue{State: "monitoring", Age: 1}}}
	before, err := changeKey([]Tile{tile})
	if err != nil {
		t.Fatal(err)
	}
	tile.extra["sample"] = agingModuleValue{State: "monitoring", Age: 2}
	after, err := changeKey([]Tile{tile})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("aging changed events key: %s -> %s", before, after)
	}
	wire, err := json.Marshal(tile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(wire, []byte(`"age_seconds":2`)) {
		t.Fatalf("changeKey replaced the live module value: %s", wire)
	}
	tile.extra["sample"] = agingModuleValue{State: "paused", Age: 2}
	changed, err := changeKey([]Tile{tile})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(after, changed) {
		t.Fatal("state change did not change events key")
	}
}
