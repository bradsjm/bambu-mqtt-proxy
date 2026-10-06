package camera

import (
	"bytes"
	"encoding/json"
	"testing"
)

// agingModuleValue represents a module value with continuously aging fields.
type agingModuleValue struct {
	State string  `json:"state"`
	Age   float64 `json:"age_seconds"`
}

// StableKey returns only the fields that should trigger an events update.
func (v agingModuleValue) StableKey() any { return v.State }

// TestModuleTileValueStableKey verifies that aging alone does not trigger events.
func TestModuleTileValueStableKey(t *testing.T) {
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
