package camera

import (
	"bytes"
	"testing"

	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/printer"
)

// TestSetModulesRejectsStateKeyCollision verifies that a module whose
// State hook serves a payload member colliding with a core tile key is
// rejected at wiring time.
func TestSetModulesRejectsStateKeyCollision(t *testing.T) {
	printers := previewPrinters()
	renderer := NewStatusRenderer(NewManager(printers, discardLogger()), printer.NewStore(printers))
	if err := renderer.SetModules([]module.Module{{Name: "camera_reason", State: func(string) any { return "x" }}}); err == nil {
		t.Fatal("SetModules must reject a module name colliding with a core tile member")
	}
}

// TestModuleDisplayRowSectionNoteRoundTrip verifies that Row Section and
// Note survive /camera/status JSON: a module Display with sectioned rows
// decodes back with identical fields.
func TestModuleDisplayRowSectionNoteRoundTrip(t *testing.T) {
	printers := previewPrinters()
	renderer := NewStatusRenderer(NewManager(printers, discardLogger()), printer.NewStore(printers))
	want := module.Display{Panel: &module.Panel{Title: "Print checks", Level: 3, Rows: []module.Row{
		{Label: "Plate", Value: "Not checked", Note: "Proxy started after this print began", Section: "Before printing"},
		{Label: "Rule", Value: "Stops the print if the plate looks occupied (over 50%)", Section: "Before printing"},
		{Label: "First layer", Value: "Pause sent", Tone: "error", Note: "Waiting for the printer to confirm", Section: "After layer 1"},
	}}}
	if err := renderer.SetModules([]module.Module{{Name: "sample", Display: func(string) *module.Display { return &want }}}); err != nil {
		t.Fatalf("SetModules: %v", err)
	}
	tiles := statusTiles(t, renderer)
	got, ok := tiles[0].Modules["sample"]
	if !ok || got.Panel == nil {
		t.Fatalf("modules = %+v, want sample display", tiles[0].Modules)
	}
	if got.Panel.Title != "Print checks" || got.Panel.Level != 3 || len(got.Panel.Rows) != len(want.Panel.Rows) {
		t.Fatalf("panel = %+v, want %+v", got.Panel, want.Panel)
	}
	for i, row := range want.Panel.Rows {
		if got.Panel.Rows[i] != row {
			t.Fatalf("row %d = %+v, want %+v", i, got.Panel.Rows[i], row)
		}
	}
	raw := statusJSON(t, renderer)
	for _, member := range []string{`"section":"Before printing"`, `"section":"After layer 1"`, `"note":"Proxy started after this print began"`, `"note":"Waiting for the printer to confirm"`} {
		if !bytes.Contains(raw, []byte(member)) {
			t.Fatalf("status JSON missing %s: %s", member, raw)
		}
	}
}
