package jobpreview

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"bambu-mqtt-proxy/internal/telemetry"
)

// ---- fixtures ----

type fixtureEntry struct {
	name   string
	data   string
	method uint16
}

func writeArchive(t *testing.T, entries ...fixtureEntry) string {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for _, e := range entries {
		h := &zip.FileHeader{Name: e.name, Method: e.method}
		fw, err := w.CreateHeader(h)
		if err != nil {
			t.Fatalf("create %s: %v", e.name, err)
		}
		if _, err := fw.Write([]byte(e.data)); err != nil {
			t.Fatalf("write %s: %v", e.name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}
	path := filepath.Join(t.TempDir(), "job.3mf")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	return path
}

func replaceEntry(entries []fixtureEntry, name, data string) []fixtureEntry {
	for i := range entries {
		if entries[i].name == name {
			entries[i].data = data
			return entries
		}
	}
	return append(entries, fixtureEntry{name: name, data: data})
}

func dropEntry(entries []fixtureEntry, name string) []fixtureEntry {
	out := entries[:0]
	for _, e := range entries {
		if e.name != name {
			out = append(out, e)
		}
	}
	return out
}

func pngBytes(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := range h {
		for x := range w {
			img.Set(x, y, color.RGBA{R: uint8(x * 16), G: uint8(y * 16), B: 0x40, A: 0xff})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	return buf.Bytes()
}

func intPtr(n int) *int { return &n }

func jobView(plate *int, gcode string) telemetry.JobView {
	return telemetry.JobView{Name: "job name", GCodeFile: gcode, PlateIndex: plate, Active: true}
}

// escapeXML escapes fixture text so it can be embedded raw in a fixture
// XML document.
func escapeXML(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(s)
}

func modelFixture(fields map[string]string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<model unit="millimeter" xml:lang="en-US" xmlns="http://schemas.microsoft.com/3dmanufacturing/core/2015/02">` + "\n")
	for _, k := range []string{"Title", "Description", "Designer", "License"} {
		v, ok := fields[k]
		if !ok {
			continue
		}
		b.WriteString(`  <metadata name="` + k + `">` + escapeXML(v) + `</metadata>` + "\n")
	}
	b.WriteString("  <resources>\n    <object id=\"1\" type=\"model\">\n      <mesh><vertices><vertex x=\"0\" y=\"0\" z=\"0\"/></vertices></mesh>\n    </object>\n  </resources>\n  <build/>\n</model>\n")
	return b.String()
}

const modelSettingsFixture = `<?xml version="1.0" encoding="UTF-8"?>
<config>
  <plate>
    <metadata key="plater_id" value="1"/>
    <metadata key="plater_name" value="Untitled"/>
  </plate>
  <plate>
    <metadata key="plater_id" value="2"/>
    <metadata key="plater_name" value="Plate Two"/>
  </plate>
</config>
`

const sliceInfoFixture = `<?xml version="1.0" encoding="UTF-8"?>
<config>
  <plate>
    <metadata key="index" value="1"/>
    <metadata key="prediction" value="600"/>
    <metadata key="weight" value="4.5"/>
    <filament id="1" type="PLA" color="#ff0000ff" used_g="3" used_m="1"/>
    <filament id="2" type="PLA" color="#00ff00ff" used_g="1" used_m="0.4"/>
    <filament id="3" type="PLA" color="#0000ffff" used_g="0.5" used_m="0.2"/>
    <object name="calibration_cube.stl" skipped="false"/>
  </plate>
  <plate>
    <metadata key="index" value="2"/>
    <metadata key="prediction" value="3600"/>
    <metadata key="weight" value="12.5"/>
    <metadata key="first_layer_time" value="150"/>
    <metadata key="support_used" value="true"/>
    <filament id="7" type="PETG" color="#23d1aaff" used_g="4.30" used_m="1.65"/>
    <filament id="8" type="PLA" color="#ffffff" used_g="8.20" used_m="2.74"/>
    <object name="left_brick.stl" skipped="false"/>
    <object name="left_brick.stl" skipped="false"/>
    <object name="right_brick.3mf" skipped="true"/>
    <object name="gear.obj" skipped="false"/>
    <warning msg="bed_temperature_too_high_than_filament" error_code="1000C010"/>
    <warning msg="spiral_mode_recommended" error_code=""/>
  </plate>
</config>
`

const plate1JSONFixture = `{"bed_type": "cool_plate"}`
const plate2JSONFixture = `{"idx": 2, "name": "Plate Two", "bed_type": "textured_plate"}`

// projectSettingsFixture carries four project filament rows that must
// never surface as materials.
const projectSettingsFixture = `{"from": "system", "layer_height": "0.2", "sparse_infill_density": "15%",` +
	` "filament_type": ["PLA", "PLA", "PLA", "PLA"],` +
	` "filament_colour": ["#ffffff", "#000000", "#00ff00", "#ff00ff"]}`

func baselineEntries(t *testing.T) []fixtureEntry {
	return []fixtureEntry{
		{name: modelEntry, data: modelFixture(map[string]string{
			"Title":       "Bracket",
			"Description": `<p>Plate &amp; frame</p>`,
			"Designer":    "ACME",
			"License":     "CC-BY-4.0",
		})},
		{name: modelSettingsEntry, data: modelSettingsFixture},
		{name: sliceInfoEntry, data: sliceInfoFixture},
		{name: "Metadata/plate_1.json", data: plate1JSONFixture},
		{name: "Metadata/plate_2.json", data: plate2JSONFixture},
		{name: projectSettingsEntry, data: projectSettingsFixture},
		{name: "Metadata/plate_1.gcode", data: "PLATE1-GCODE-NAME-ONLY"},
		{name: "Metadata/plate_2.gcode", data: "PLATE2-GCODE-NAME-ONLY"},
		{name: "Metadata/plate_1.png", data: string(pngBytes(t, 4, 4))},
		{name: "Metadata/plate_2.png", data: string(pngBytes(t, 3, 2))},
	}
}

// ---- assertion helpers ----

func wantCategory(t *testing.T, err error, cat string) {
	t.Helper()
	if err == nil {
		if cat == "" {
			return
		}
		t.Fatalf("parse error nil, want category %q", cat)
	}
	if got := errorCategory(err); got != cat {
		t.Fatalf("parse error category = %q (%v), want %q", got, err, cat)
	}
}

func wantStr(t *testing.T, name string, got *string, want string) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s = nil, want %q", name, want)
	}
	if *got != want {
		t.Fatalf("%s = %q, want %q", name, *got, want)
	}
}

func wantFloat(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s = nil, want %v", name, want)
	}
	if *got != want {
		t.Fatalf("%s = %v, want %v", name, *got, want)
	}
}

func wantBool(t *testing.T, name string, got *bool, want bool) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s = nil, want %v", name, want)
	}
	if *got != want {
		t.Fatalf("%s = %v, want %v", name, *got, want)
	}
}

func wantNil(t *testing.T, name string, got any) {
	t.Helper()
	v := reflect.ValueOf(got)
	switch v.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Interface:
		if !v.IsNil() {
			t.Fatalf("%s = %v, want nil", name, got)
		}
	default:
		t.Fatalf("%s: unsupported kind %s", name, v.Kind())
	}
}

func wantInt(t *testing.T, name string, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %d, want %d", name, got, want)
	}
}

func wantIntPtr(t *testing.T, name string, got *int, want int) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s = nil, want %d", name, want)
	}
	if *got != want {
		t.Fatalf("%s = %d, want %d", name, *got, want)
	}
}

func wantRuneCount(t *testing.T, name, got string, want int) {
	t.Helper()
	if n := len([]rune(got)); n != want {
		t.Fatalf("%s = %d runes, want %d", name, n, want)
	}
}

// wantPlateTwo asserts every accepted plate-2 fact of the baseline archive.
func wantPlateTwo(t *testing.T, res Result) {
	t.Helper()
	md := res.Metadata
	if md == nil {
		t.Fatal("metadata nil")
	}
	if md.Source != sourcePrinter3mf {
		t.Fatalf("source = %q", md.Source)
	}
	wantStr(t, "title", md.Title, "Bracket")
	wantStr(t, "description", md.Description, "Plate & frame")
	wantStr(t, "designer", md.Designer, "ACME")
	wantStr(t, "license", md.License, "CC-BY-4.0")
	if md.Plate == nil {
		t.Fatal("plate nil")
	}
	wantInt(t, "plate.index", md.Plate.Index, 2)
	wantStr(t, "plate.name", md.Plate.Name, "Plate Two")
	wantStr(t, "plate.bed_type", md.Plate.BedType, "textured_plate")
	wantFloat(t, "plate.estimated_seconds", md.Plate.EstimatedSeconds, 3600)
	wantFloat(t, "plate.weight_g", md.Plate.WeightG, 12.5)
	wantFloat(t, "plate.first_layer_seconds", md.Plate.FirstLayerSeconds, 150)
	wantBool(t, "plate.supports", md.Plate.Supports, true)
	wantInt(t, "materials", len(md.Materials), 2)
	m0 := md.Materials[0]
	if m0.ID != "7" {
		t.Fatalf("material[0].id = %q", m0.ID)
	}
	wantStr(t, "material[0].type", m0.Type, "PETG")
	wantStr(t, "material[0].color", m0.Color, "#23D1AAFF")
	wantFloat(t, "material[0].used_g", m0.UsedG, 4.3)
	wantFloat(t, "material[0].used_m", m0.UsedM, 1.65)
	m1 := md.Materials[1]
	if m1.ID != "8" {
		t.Fatalf("material[1].id = %q", m1.ID)
	}
	wantStr(t, "material[1].type", m1.Type, "PLA")
	wantStr(t, "material[1].color", m1.Color, "#FFFFFF")
	wantFloat(t, "material[1].used_g", m1.UsedG, 8.2)
	wantFloat(t, "material[1].used_m", m1.UsedM, 2.74)
	wantInt(t, "objects", len(md.Objects), 2)
	wantStr(t, "objects[0].name", &md.Objects[0].Name, "left_brick.stl")
	wantInt(t, "objects[0].count", md.Objects[0].Count, 2)
	wantStr(t, "objects[1].name", &md.Objects[1].Name, "gear.obj")
	wantInt(t, "objects[1].count", md.Objects[1].Count, 1)
	wantIntPtr(t, "object_count", md.ObjectCount, 3)
	wantInt(t, "warnings", len(md.Warnings), 2)
	if md.Warnings[0].Code != "1000C010" {
		t.Fatalf("warning[0].code = %q", md.Warnings[0].Code)
	}
	wantStr(t, "warning[0].message", &md.Warnings[0].Message, "Sliced bed temperature exceeds the filament recommendation")
	if md.Warnings[1].Code != "spiral_mode_recommended" {
		t.Fatalf("warning[1].code = %q", md.Warnings[1].Code)
	}
	wantStr(t, "warning[1].message", &md.Warnings[1].Message, "spiral mode recommended")
	if md.Process == nil {
		t.Fatal("process nil")
	}
	wantFloat(t, "process.layer_height_mm", md.Process.LayerHeightMM, 0.2)
	wantFloat(t, "process.infill_percent", md.Process.InfillPercent, 15)
	if md.Truncated {
		t.Fatal("truncated = true, want false")
	}
}

// ---- plate resolution ----

func TestParseArchiveSelectsReportedPlate(t *testing.T) {
	path := writeArchive(t, baselineEntries(t)...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, "")
	if res.Preview.Status != StatusReady {
		t.Fatalf("status = %q, want ready", res.Preview.Status)
	}
	if !bytes.Equal(res.PNG, pngBytes(t, 3, 2)) {
		t.Fatalf("png is not the plate 2 render")
	}
	if res.Preview.JobName != "job name" || !res.Preview.Current {
		t.Fatalf("draft view = %+v", res.Preview)
	}
	wantNil(t, "retrieved_at", res.Preview.RetrievedAt)
	wantNil(t, "image_url", res.Preview.ImageURL)
	wantPlateTwo(t, res)
}

func TestParseArchiveSelectsPlateOne(t *testing.T) {
	path := writeArchive(t, baselineEntries(t)...)
	res, err := parseArchive(path, jobView(intPtr(1), ""))
	wantCategory(t, err, "")
	if res.Preview.Status != StatusReady {
		t.Fatalf("status = %q, want ready", res.Preview.Status)
	}
	md := res.Metadata
	if md == nil || md.Plate == nil {
		t.Fatal("plate metadata missing")
	}
	wantInt(t, "plate.index", md.Plate.Index, 1)
	wantStr(t, "plate.name", md.Plate.Name, "Untitled")
	wantStr(t, "plate.bed_type", md.Plate.BedType, "cool_plate")
	wantFloat(t, "plate.estimated_seconds", md.Plate.EstimatedSeconds, 600)
	wantFloat(t, "plate.weight_g", md.Plate.WeightG, 4.5)
	wantNil(t, "plate.first_layer_seconds", md.Plate.FirstLayerSeconds)
	wantNil(t, "plate.supports", md.Plate.Supports)
	wantInt(t, "materials", len(md.Materials), 3)
	wantInt(t, "warnings", len(md.Warnings), 0)
	wantInt(t, "objects", len(md.Objects), 1)
	wantIntPtr(t, "object_count", md.ObjectCount, 1)
	if md.Truncated {
		t.Fatal("truncated = true, want false")
	}
}

func TestParseArchiveResolvesPlateFromGCodeName(t *testing.T) {
	entries := []fixtureEntry{
		{name: modelEntry, data: modelFixture(map[string]string{"Title": "Rocks"})},
		{name: sliceInfoEntry, data: `<config><plate>` +
			`<metadata key="index" value="3"/><metadata key="prediction" value="42"/>` +
			`<filament id="9" type="PLA" color="#123456" used_g="1.5" used_m="0.5"/>` +
			`</plate></config>`},
		{name: "Metadata/plate_3.json", data: `{"bed_type": "engineering_plate"}`},
		{name: "Metadata/plate_3.gcode", data: "\x00\x01not-really-deflate"},
		{name: "Metadata/plate_3.png", data: string(pngBytes(t, 2, 2))},
	}
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(nil, "rocks_v3_plate_3.GCODE.3mf"))
	wantCategory(t, err, "")
	if res.Preview.Status != StatusReady {
		t.Fatalf("status = %q, want ready", res.Preview.Status)
	}
	md := res.Metadata
	if md == nil || md.Plate == nil {
		t.Fatal("plate metadata missing")
	}
	wantInt(t, "plate.index", md.Plate.Index, 3)
	wantStr(t, "plate.bed_type", md.Plate.BedType, "engineering_plate")
	wantFloat(t, "plate.estimated_seconds", md.Plate.EstimatedSeconds, 42)
	wantInt(t, "materials", len(md.Materials), 1)
	if md.Materials[0].ID != "9" {
		t.Fatalf("material id = %q, want 9", md.Materials[0].ID)
	}
}

func TestParseArchiveResolvesSingleArchivePlate(t *testing.T) {
	entries := []fixtureEntry{
		{name: modelEntry, data: modelFixture(map[string]string{"Title": "Rocks"})},
		{name: sliceInfoEntry, data: `<config><plate>` +
			`<metadata key="index" value="3"/><metadata key="prediction" value="42"/>` +
			`</plate></config>`},
		{name: "Metadata/plate_3.json", data: `{"bed_type": "engineering_plate"}`},
		{name: "Metadata/plate_3.gcode", data: "name-only"},
		{name: "Metadata/plate_3.png", data: string(pngBytes(t, 2, 2))},
	}
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(nil, ""))
	wantCategory(t, err, "")
	if res.Metadata == nil || res.Metadata.Plate == nil || res.Metadata.Plate.Index != 3 {
		t.Fatalf("plate = %+v, want index 3", res.Metadata)
	}
}

func TestParseArchiveAmbiguousConflictingSources(t *testing.T) {
	path := writeArchive(t, baselineEntries(t)...)
	res, err := parseArchive(path, jobView(intPtr(1), "job_plate_2.gcode.3mf"))
	wantCategory(t, err, catAmbiguousPlate)
	md := res.Metadata
	if md == nil {
		t.Fatal("descriptive metadata missing")
	}
	wantStr(t, "title", md.Title, "Bracket")
	wantNil(t, "plate", md.Plate)
	wantNil(t, "object_count", md.ObjectCount)
	wantNil(t, "process", md.Process)
	wantInt(t, "materials", len(md.Materials), 0)
	wantInt(t, "objects", len(md.Objects), 0)
	wantInt(t, "warnings", len(md.Warnings), 0)
	if res.PNG != nil {
		t.Fatal("png present without a selected plate")
	}
	if res.Preview.Status != StatusUnavailable {
		t.Fatalf("status = %q, want unavailable", res.Preview.Status)
	}
}

func TestParseArchiveAmbiguousArchivePlates(t *testing.T) {
	t.Run("multiple archive plates", func(t *testing.T) {
		path := writeArchive(t, baselineEntries(t)...)
		res, err := parseArchive(path, jobView(nil, ""))
		wantCategory(t, err, catAmbiguousPlate)
		if res.Metadata == nil || res.Metadata.Title == nil {
			t.Fatal("project title missing")
		}
		if res.Metadata.Plate != nil || res.PNG != nil {
			t.Fatal("plate or image resolved despite ambiguity")
		}
	})
	t.Run("no plate evidence", func(t *testing.T) {
		entries := dropEntry(baselineEntries(t), "Metadata/plate_1.gcode")
		entries = dropEntry(entries, "Metadata/plate_2.gcode")
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(nil, "model.gcode.3mf"))
		wantCategory(t, err, catAmbiguousPlate)
		if res.Metadata == nil || res.Metadata.Title == nil {
			t.Fatal("project title missing")
		}
	})
}

// ---- entry allowlist and gcode safety ----

func TestParseArchiveNeverOpensPlateGcode(t *testing.T) {
	entries := replaceEntry(baselineEntries(t), "Metadata/plate_2.gcode", "\x00\xff\xfe garbage not deflate")
	entries = replaceEntry(entries, "Metadata/plate_1.gcode", "\x01\x02\x03 also garbage")
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, "")
	if res.Preview.Status != StatusReady || res.PNG == nil {
		t.Fatalf("status = %q, png = %d bytes", res.Preview.Status, len(res.PNG))
	}
	wantPlateTwo(t, res)
}

func TestParseArchiveWrongCaseEntriesIgnored(t *testing.T) {
	entries := []fixtureEntry{
		{name: "3d/3dmodel.model", data: modelFixture(map[string]string{"Title": "WrongCase"})},
		{name: "metadata/slice_info.config", data: sliceInfoFixture},
		{name: "Metadata/PLATE_2.JSON", data: plate2JSONFixture},
		{name: "metadata/plate_2.png", data: string(pngBytes(t, 2, 2))},
		{name: "Metadata/plate_2.gcode", data: "\x00 garbage"},
	}
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, catImageMissing)
	if res.Metadata != nil {
		t.Fatalf("metadata = %+v, want nil when nothing was accepted", res.Metadata)
	}
	if res.PNG != nil || res.Preview.Status != StatusUnavailable {
		t.Fatalf("status = %q, png = %d bytes", res.Preview.Status, len(res.PNG))
	}
}

// ---- descriptive text ----

func TestParseArchiveNoDescription(t *testing.T) {
	entries := replaceEntry(baselineEntries(t), modelEntry,
		modelFixture(map[string]string{"Title": "Only"}))
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, "")
	md := res.Metadata
	if md == nil {
		t.Fatal("metadata nil")
	}
	wantStr(t, "title", md.Title, "Only")
	wantNil(t, "description", md.Description)
	wantNil(t, "designer", md.Designer)
	wantNil(t, "license", md.License)
	wantStr(t, "plate.name", md.Plate.Name, "Plate Two")
	wantIntPtr(t, "object_count", md.ObjectCount, 3)
	if md.Truncated {
		t.Fatal("truncated = true, want false")
	}
}

func TestParseArchiveHTMLDescription(t *testing.T) {
	raw := `<p>Hello <script>alert(1)</script>world<br>line2 &amp;amp; more</p><style>x{a:1}</style>End`
	entries := replaceEntry(baselineEntries(t), modelEntry,
		modelFixture(map[string]string{"Description": raw}))
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, "")
	wantStr(t, "description", res.Metadata.Description, "Hello world\nline2 & more\nEnd")
}

func TestParseArchiveDoubleEscapedDescription(t *testing.T) {
	entries := replaceEntry(baselineEntries(t), modelEntry,
		modelFixture(map[string]string{"Description": "&amp;lt;p&amp;gt;Deep&amp;lt;/p&amp;gt;"}))
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, "")
	wantStr(t, "description", res.Metadata.Description, "Deep")
}

// ---- failure isolation ----

func TestParseArchiveMissingPNGKeepsMetadata(t *testing.T) {
	entries := dropEntry(baselineEntries(t), "Metadata/plate_2.png")
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, catImageMissing)
	if res.PNG != nil {
		t.Fatal("png present")
	}
	if res.Preview.Status != StatusUnavailable {
		t.Fatalf("status = %q, want unavailable", res.Preview.Status)
	}
	wantPlateTwo(t, res)
}

func TestParseArchiveMalformedModelKeepsImage(t *testing.T) {
	entries := replaceEntry(baselineEntries(t), modelEntry,
		`<model><metadata name="Title">truncated`)
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, catArchive)
	if res.PNG == nil || res.Preview.Status != StatusReady {
		t.Fatalf("status = %q, png = %d bytes; image must survive", res.Preview.Status, len(res.PNG))
	}
	md := res.Metadata
	if md == nil {
		t.Fatal("metadata nil")
	}
	wantNil(t, "title", md.Title)
	wantNil(t, "description", md.Description)
	wantFloat(t, "plate.weight_g", md.Plate.WeightG, 12.5)
}

func TestParseArchiveAbsentSlicePlate(t *testing.T) {
	onlyPlateOne := `<config><plate>
  <metadata key="index" value="1"/>
  <metadata key="prediction" value="600"/>
  <metadata key="weight" value="4.5"/>
  <filament id="1" type="PLA" color="#ff0000ff" used_g="3" used_m="1"/>
  <object name="cube.stl" skipped="false"/>
</plate></config>`
	t.Run("metadata null without any accepted field", func(t *testing.T) {
		entries := []fixtureEntry{
			{name: sliceInfoEntry, data: onlyPlateOne},
			{name: "Metadata/plate_1.gcode", data: "name-only"},
		}
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catImageMissing)
		if res.Metadata != nil {
			t.Fatalf("metadata = %+v, want nil when no field was accepted", res.Metadata)
		}
		if res.PNG != nil || res.Preview.Status != StatusUnavailable {
			t.Fatalf("status = %q, png = %d bytes", res.Preview.Status, len(res.PNG))
		}
	})
	t.Run("other accepted entries survive", func(t *testing.T) {
		entries := baselineEntries(t)
		entries = replaceEntry(entries, sliceInfoEntry, onlyPlateOne)
		entries = dropEntry(entries, "Metadata/plate_2.json")
		entries = dropEntry(entries, "Metadata/plate_2.png")
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catImageMissing)
		md := res.Metadata
		if md == nil {
			t.Fatal("metadata nil, want descriptive fields")
		}
		wantStr(t, "title", md.Title, "Bracket")
		if md.Plate == nil {
			t.Fatal("plate nil, want the model_settings name")
		}
		wantInt(t, "plate.index", md.Plate.Index, 2)
		wantStr(t, "plate.name", md.Plate.Name, "Plate Two")
		wantNil(t, "plate.estimated_seconds", md.Plate.EstimatedSeconds)
		wantNil(t, "plate.weight_g", md.Plate.WeightG)
		wantNil(t, "object_count", md.ObjectCount)
		if md.Process == nil {
			t.Fatal("process nil, want surviving project settings")
		}
		wantFloat(t, "process.layer_height_mm", md.Process.LayerHeightMM, 0.2)
		wantFloat(t, "process.infill_percent", md.Process.InfillPercent, 15)
		wantInt(t, "materials", len(md.Materials), 0)
		wantInt(t, "objects", len(md.Objects), 0)
		wantInt(t, "warnings", len(md.Warnings), 0)
		if md.Truncated {
			t.Fatal("truncated = true, want false")
		}
	})
}

func TestParseArchiveBadRoots(t *testing.T) {
	t.Run("model root", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), modelEntry, `<foo><bar/></foo>`)
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catArchive)
		wantNil(t, "title", res.Metadata.Title)
	})
	t.Run("config root", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), sliceInfoEntry, `<boxes><box/></boxes>`)
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catArchive)
		wantNil(t, "object_count", res.Metadata.ObjectCount)
	})
	t.Run("empty model", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), modelEntry, "")
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catArchive)
		wantNil(t, "title", res.Metadata.Title)
	})
}

func TestParseArchiveTrailingContent(t *testing.T) {
	t.Run("model stray element", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), modelEntry,
			modelFixture(map[string]string{"Title": "Bracket"})+"<stray/>")
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catArchive)
		wantNil(t, "title", res.Metadata.Title)
	})
	t.Run("model trailing text", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), modelEntry,
			modelFixture(map[string]string{"Title": "Bracket"})+"junk")
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catArchive)
		wantNil(t, "title", res.Metadata.Title)
	})
	t.Run("config stray element", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), sliceInfoEntry, sliceInfoFixture+"<stray/>")
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catArchive)
		wantNil(t, "object_count", res.Metadata.ObjectCount)
	})
	t.Run("plate json trailing", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), "Metadata/plate_2.json",
			`{"bed_type": "textured_plate"} tail`)
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catArchive)
		wantNil(t, "plate.bed_type", res.Metadata.Plate.BedType)
		wantStr(t, "plate.name", res.Metadata.Plate.Name, "Plate Two")
	})
}

func TestParseArchiveDuplicateEntries(t *testing.T) {
	t.Run("slice info", func(t *testing.T) {
		entries := append(baselineEntries(t),
			fixtureEntry{name: sliceInfoEntry, data: `<config><plate><metadata key="index" value="2"/></plate></config>`})
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
		md := res.Metadata
		wantNil(t, "object_count", md.ObjectCount)
		wantInt(t, "materials", len(md.Materials), 0)
		wantStr(t, "title", md.Title, "Bracket")
		wantStr(t, "plate.name", md.Plate.Name, "Plate Two")
		wantStr(t, "plate.bed_type", md.Plate.BedType, "textured_plate")
		if res.Preview.Status != StatusReady {
			t.Fatalf("status = %q, want ready", res.Preview.Status)
		}
	})
	t.Run("plate png", func(t *testing.T) {
		entries := append(baselineEntries(t),
			fixtureEntry{name: "Metadata/plate_2.png", data: string(pngBytes(t, 5, 5))})
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catImageMissing)
		if res.PNG != nil {
			t.Fatal("png accepted from duplicated entry")
		}
		wantPlateTwo(t, res)
	})
}

func TestParseArchiveCRCError(t *testing.T) {
	marker := "SLICE_CRC_MARKER{" + strings.Repeat("m", 60) + "}"
	badSlice := `<config><!-- ` + marker + ` --><plate><metadata key="index" value="2"/><metadata key="prediction" value="5"/></plate></config>`
	entries := replaceEntry(baselineEntries(t), sliceInfoEntry, badSlice)
	for i := range entries {
		if entries[i].name == sliceInfoEntry {
			entries[i].method = zip.Store
		}
	}
	path := writeArchive(t, entries...)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	at := bytes.Index(raw, []byte(marker))
	if at < 0 {
		t.Fatal("marker not found in archive")
	}
	raw[at+30] ^= 0x20 // stays valid XML text: proves the failure is the CRC
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, catArchive)
	wantNil(t, "object_count", res.Metadata.ObjectCount)
	wantStr(t, "title", res.Metadata.Title, "Bracket")
}

func TestParseArchiveOversizeEntryRejected(t *testing.T) {
	big := "<config><!--" + strings.Repeat("x", int(entryCapBytes)+100) + "--></config>"
	entries := replaceEntry(baselineEntries(t), sliceInfoEntry, big)
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, catOversized)
	wantNil(t, "object_count", res.Metadata.ObjectCount)
	wantStr(t, "title", res.Metadata.Title, "Bracket")
	wantStr(t, "plate.name", res.Metadata.Plate.Name, "Plate Two")
}

func TestParseArchiveXMLDepthCap(t *testing.T) {
	t.Run("depth 64 accepted", func(t *testing.T) {
		doc := "<config>" + strings.Repeat("<a>", 63) + strings.Repeat("</a>", 63) + "</config>"
		entries := replaceEntry(baselineEntries(t), sliceInfoEntry, doc)
		path := writeArchive(t, entries...)
		_, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
	})
	t.Run("depth 65 rejected", func(t *testing.T) {
		doc := "<config>" + strings.Repeat("<a>", 64) + strings.Repeat("</a>", 64) + "</config>"
		entries := replaceEntry(baselineEntries(t), sliceInfoEntry, doc)
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catArchive)
		wantNil(t, "object_count", res.Metadata.ObjectCount)
		wantStr(t, "title", res.Metadata.Title, "Bracket")
	})
}

func TestParseArchiveJunkArchive(t *testing.T) {
	path := writeArchive(t, baselineEntries(t)...)
	if err := os.WriteFile(path, []byte("definitely not a zip file"), 0o600); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, catArchive)
	if res.Metadata != nil || res.PNG != nil || res.Preview.Status != "" {
		t.Fatalf("result = %+v, want zero value", res)
	}
}

// ---- namespace prefixes ----

func TestParseArchiveNamespacePrefixes(t *testing.T) {
	entries := replaceEntry(baselineEntries(t), modelEntry,
		`<?xml version="1.0"?>
<a:model xmlns:a="http://schemas.microsoft.com/3dmanufacturing/core/2015/02">
  <a:metadata name="Title">Prefixed</a:metadata>
  <a:resources/><a:build/>
</a:model>`)
	entries = replaceEntry(entries, modelSettingsEntry,
		`<?xml version="1.0"?>
<b:config xmlns:b="urn:test">
  <b:plate><b:metadata key="plater_id" value="2"/><b:metadata key="plater_name" value="Prefixed"/></b:plate>
</b:config>`)
	entries = replaceEntry(entries, sliceInfoEntry,
		`<?xml version="1.0"?>
<p:config xmlns:p="urn:test">
  <p:plate>
    <p:metadata key="index" value="2"/>
    <p:metadata key="prediction" value="77"/>
    <p:filament id="4" type="PLA" color="#abcdef" used_g="2" used_m="0.7"/>
    <p:object name="thing.stl" skipped="false"/>
  </p:plate>
</p:config>`)
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, "")
	md := res.Metadata
	wantStr(t, "title", md.Title, "Prefixed")
	wantStr(t, "plate.name", md.Plate.Name, "Prefixed")
	wantFloat(t, "plate.estimated_seconds", md.Plate.EstimatedSeconds, 77)
	wantInt(t, "materials", len(md.Materials), 1)
	if md.Materials[0].ID != "4" {
		t.Fatalf("material id = %q, want 4", md.Materials[0].ID)
	}
	wantIntPtr(t, "object_count", md.ObjectCount, 1)
}

// ---- numeric validation ----

func TestParseArchiveNumericValidation(t *testing.T) {
	entries := replaceEntry(baselineEntries(t), sliceInfoEntry, `<config><plate>
  <metadata key="index" value="2"/>
  <metadata key="prediction" value="-5"/>
  <metadata key="weight" value="inf"/>
  <metadata key="first_layer_time" value="0"/>
  <metadata key="support_used" value="banana"/>
  <filament id="7" type="PETG" color="red" used_g="-1" used_m="nan"/>
  <filament id="8" type="" color="" used_g="8" used_m="1e400"/>
</plate></config>`)
	entries = replaceEntry(entries, projectSettingsEntry,
		`{"layer_height": "0", "sparse_infill_density": "150%"}`)
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, "")
	md := res.Metadata
	if md == nil {
		t.Fatal("metadata nil")
	}
	wantNil(t, "plate.estimated_seconds", md.Plate.EstimatedSeconds)
	wantNil(t, "plate.weight_g", md.Plate.WeightG)
	wantNil(t, "plate.first_layer_seconds", md.Plate.FirstLayerSeconds)
	wantNil(t, "plate.supports", md.Plate.Supports)
	wantNil(t, "process", md.Process)
	wantInt(t, "materials", len(md.Materials), 2)
	wantStr(t, "material[0].type", md.Materials[0].Type, "PETG")
	wantNil(t, "material[0].color", md.Materials[0].Color)
	wantNil(t, "material[0].used_g", md.Materials[0].UsedG)
	wantNil(t, "material[0].used_m", md.Materials[0].UsedM)
	wantFloat(t, "material[1].used_g", md.Materials[1].UsedG, 8)
	wantNil(t, "material[1].used_m", md.Materials[1].UsedM)
	wantNil(t, "plate.weight_g", md.Plate.WeightG)
}

func TestParseArchiveProjectSettingsJSON(t *testing.T) {
	t.Run("json numeric layer", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), projectSettingsEntry,
			`{"layer_height": 0.08, "sparse_infill_density": "15%"}`)
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
		wantFloat(t, "layer_height_mm", res.Metadata.Process.LayerHeightMM, 0.08)
		wantFloat(t, "infill_percent", res.Metadata.Process.InfillPercent, 15)
	})
	t.Run("xml content rejected", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), projectSettingsEntry,
			`<config><metadata key="layer_height" value="0.08"/><metadata key="sparse_infill_density" value="15%"/></config>`)
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catArchive)
		wantNil(t, "process", res.Metadata.Process)
		wantStr(t, "title", res.Metadata.Title, "Bracket")
		wantFloat(t, "plate.weight_g", res.Metadata.Plate.WeightG, 12.5)
	})
	t.Run("infill bounds", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), projectSettingsEntry,
			`{"sparse_infill_density": "0%"}`)
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
		if res.Metadata.Process == nil {
			t.Fatal("process nil")
		}
		wantFloat(t, "infill_percent", res.Metadata.Process.InfillPercent, 0)
		entries = replaceEntry(baselineEntries(t), projectSettingsEntry,
			`{"sparse_infill_density": "-5%"}`)
		path = writeArchive(t, entries...)
		res, err = parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
		wantNil(t, "process", res.Metadata.Process)
	})
}

func TestParseArchiveProjectFilamentsNotExposed(t *testing.T) {
	// The project settings carry four filament rows and plate 1 carries
	// three; only the selected plate's two rows may surface.
	path := writeArchive(t, baselineEntries(t)...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, "")
	wantInt(t, "materials", len(res.Metadata.Materials), 2)
}

func TestParseArchiveGroupedSkippedObjects(t *testing.T) {
	entries := replaceEntry(baselineEntries(t), sliceInfoEntry, `<config><plate>
  <metadata key="index" value="2"/>
  <object name="a.stl" skipped="false"/>
  <object name="a.stl" skipped="0"/>
  <object name="b.stl" skipped="true"/>
  <object name="b.stl" skipped="false"/>
  <object name="c.obj" skipped=""/>
</plate></config>`)
	path := writeArchive(t, entries...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, "")
	md := res.Metadata
	wantIntPtr(t, "object_count", md.ObjectCount, 4)
	wantInt(t, "groups", len(md.Objects), 3)
	wantStr(t, "objects[0].name", &md.Objects[0].Name, "a.stl")
	wantInt(t, "objects[0].count", md.Objects[0].Count, 2)
	wantStr(t, "objects[1].name", &md.Objects[1].Name, "b.stl")
	wantInt(t, "objects[1].count", md.Objects[1].Count, 1)
	wantStr(t, "objects[2].name", &md.Objects[2].Name, "c.obj")
	wantInt(t, "objects[2].count", md.Objects[2].Count, 1)
}

func TestParseArchiveHostileUnknownPlateKeys(t *testing.T) {
	t.Run("unknown keys dropped, accepted siblings intact", func(t *testing.T) {
		var b strings.Builder
		b.WriteString(`<config><plate>`)
		for i := range 12000 {
			fmt.Fprintf(&b, `<metadata key="probe_%d" value="x"/>`, i)
		}
		b.WriteString(`<metadata key="index" value="2"/>` +
			`<metadata key="prediction" value="42"/>` +
			`<metadata key="weight" value="7"/>` +
			`<metadata key="first_layer_time" value="0"/>` +
			`<metadata key="support_used" value="true"/>` +
			`<filament id="1" type="PLA" used_g="1"/>` +
			`</plate></config>`)
		entries := replaceEntry(baselineEntries(t), sliceInfoEntry, b.String())
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
		md := res.Metadata
		if md == nil {
			t.Fatal("metadata nil")
		}
		wantFloat(t, "plate.estimated_seconds", md.Plate.EstimatedSeconds, 42)
		wantFloat(t, "plate.weight_g", md.Plate.WeightG, 7)
		wantNil(t, "plate.first_layer_seconds", md.Plate.FirstLayerSeconds)
		wantBool(t, "plate.supports", md.Plate.Supports, true)
		wantInt(t, "materials", len(md.Materials), 1)
		wantIntPtr(t, "object_count", md.ObjectCount, 0)
		if md.Truncated {
			t.Fatal("truncated = true, want false")
		}
	})
	t.Run("first known value wins", func(t *testing.T) {
		doc := `<config><plate>` +
			`<metadata key="probe" value="x"/>` +
			`<metadata key="index" value="2"/>` +
			`<metadata key="prediction" value="1"/>` +
			`<metadata key="prediction" value="2"/>` +
			`</plate></config>`
		entries := replaceEntry(baselineEntries(t), sliceInfoEntry, doc)
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
		wantFloat(t, "plate.estimated_seconds", res.Metadata.Plate.EstimatedSeconds, 1)
	})
}

// ---- truncation boundaries ----

func sliceFixture(index, filaments, objects, warnings int) string {
	var b strings.Builder
	b.WriteString(`<config><plate><metadata key="index" value="` + fmt.Sprint(index) + `"/>`)
	for i := range filaments {
		fmt.Fprintf(&b, `<filament id="%d" type="PLA" color="#ffffff" used_g="1" used_m="0.3"/>`, i+1)
	}
	for i := range objects {
		fmt.Fprintf(&b, `<object name="obj%03d.stl" skipped="false"/>`, i)
	}
	for i := range warnings {
		fmt.Fprintf(&b, `<warning msg="warn_code_%d" error_code=""/>`, i)
	}
	b.WriteString(`</plate></config>`)
	return b.String()
}

func TestParseArchiveTruncationBoundaries(t *testing.T) {
	t.Run("title at cap", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), modelEntry,
			modelFixture(map[string]string{"Title": strings.Repeat("x", capTextRunes)}))
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
		wantRuneCount(t, "title", *res.Metadata.Title, capTextRunes)
		if res.Metadata.Truncated {
			t.Fatal("truncated at exactly the cap")
		}
	})
	t.Run("title above cap", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), modelEntry,
			modelFixture(map[string]string{"Title": strings.Repeat("x", capTextRunes+1)}))
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
		wantRuneCount(t, "title", *res.Metadata.Title, capTextRunes)
		if !res.Metadata.Truncated {
			t.Fatal("truncation not reported")
		}
	})
	t.Run("title multibyte runes", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), modelEntry,
			modelFixture(map[string]string{"Title": strings.Repeat("🙂", capTextRunes+1)}))
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
		wantRuneCount(t, "title", *res.Metadata.Title, capTextRunes)
		if !res.Metadata.Truncated {
			t.Fatal("truncation not reported")
		}
	})
	t.Run("description at and above cap", func(t *testing.T) {
		for _, n := range []int{capDescriptionRunes, capDescriptionRunes + 1} {
			entries := replaceEntry(baselineEntries(t), modelEntry,
				modelFixture(map[string]string{"Description": strings.Repeat("w", n)}))
			path := writeArchive(t, entries...)
			res, err := parseArchive(path, jobView(intPtr(2), ""))
			wantCategory(t, err, "")
			wantRuneCount(t, "description", *res.Metadata.Description, capDescriptionRunes)
			if got := res.Metadata.Truncated; got != (n > capDescriptionRunes) {
				t.Fatalf("n=%d truncated = %v", n, got)
			}
		}
	})
	t.Run("materials at and above cap", func(t *testing.T) {
		for _, n := range []int{maxMaterials, maxMaterials + 1} {
			entries := replaceEntry(baselineEntries(t), sliceInfoEntry, sliceFixture(2, n, 1, 0))
			path := writeArchive(t, entries...)
			res, err := parseArchive(path, jobView(intPtr(2), ""))
			wantCategory(t, err, "")
			wantInt(t, "materials", len(res.Metadata.Materials), maxMaterials)
			if got := res.Metadata.Truncated; got != (n > maxMaterials) {
				t.Fatalf("n=%d truncated = %v", n, got)
			}
		}
	})
	t.Run("object groups at and above cap", func(t *testing.T) {
		for _, n := range []int{maxObjectGroups, maxObjectGroups + 1} {
			entries := replaceEntry(baselineEntries(t), sliceInfoEntry, sliceFixture(2, 0, n, 0))
			path := writeArchive(t, entries...)
			res, err := parseArchive(path, jobView(intPtr(2), ""))
			wantCategory(t, err, "")
			wantInt(t, "groups", len(res.Metadata.Objects), maxObjectGroups)
			wantIntPtr(t, "object_count", res.Metadata.ObjectCount, n)
			if got := res.Metadata.Truncated; got != (n > maxObjectGroups) {
				t.Fatalf("n=%d truncated = %v", n, got)
			}
		}
	})
	t.Run("warnings at and above cap", func(t *testing.T) {
		for _, n := range []int{maxWarnings, maxWarnings + 1} {
			entries := replaceEntry(baselineEntries(t), sliceInfoEntry, sliceFixture(2, 0, 0, n))
			path := writeArchive(t, entries...)
			res, err := parseArchive(path, jobView(intPtr(2), ""))
			wantCategory(t, err, "")
			wantInt(t, "warnings", len(res.Metadata.Warnings), maxWarnings)
			wantStr(t, "warnings[0].message", &res.Metadata.Warnings[0].Message, "warn code 0")
			if got := res.Metadata.Truncated; got != (n > maxWarnings) {
				t.Fatalf("n=%d truncated = %v", n, got)
			}
		}
	})
	t.Run("material id at and above cap", func(t *testing.T) {
		for _, n := range []int{capIDRunes, capIDRunes + 1} {
			doc := `<config><plate><metadata key="index" value="2"/><filament id="` +
				strings.Repeat("🙂", n) + `" type="PLA" used_g="1"/></plate></config>`
			entries := replaceEntry(baselineEntries(t), sliceInfoEntry, doc)
			path := writeArchive(t, entries...)
			res, err := parseArchive(path, jobView(intPtr(2), ""))
			wantCategory(t, err, "")
			wantInt(t, "materials", len(res.Metadata.Materials), 1)
			wantRuneCount(t, "material id", res.Metadata.Materials[0].ID, capIDRunes)
			if got := res.Metadata.Truncated; got != (n > capIDRunes) {
				t.Fatalf("n=%d truncated = %v", n, got)
			}
		}
	})
	t.Run("warning code at and above cap", func(t *testing.T) {
		for _, n := range []int{capIDRunes, capIDRunes + 1} {
			doc := `<config><plate><metadata key="index" value="2"/>` +
				`<warning msg="spiral_mode_recommended" error_code="` + strings.Repeat("😊", n) +
				`"/></plate></config>`
			entries := replaceEntry(baselineEntries(t), sliceInfoEntry, doc)
			path := writeArchive(t, entries...)
			res, err := parseArchive(path, jobView(intPtr(2), ""))
			wantCategory(t, err, "")
			wantInt(t, "warnings", len(res.Metadata.Warnings), 1)
			wantRuneCount(t, "warning code", res.Metadata.Warnings[0].Code, capIDRunes)
			wantStr(t, "warnings[0].message", &res.Metadata.Warnings[0].Message, "spiral mode recommended")
			if got := res.Metadata.Truncated; got != (n > capIDRunes) {
				t.Fatalf("n=%d truncated = %v", n, got)
			}
		}
	})
}

// ---- PNG validation ----

func TestParseArchivePNGDimensionCap(t *testing.T) {
	t.Run("above cap rejected", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), "Metadata/plate_2.png", string(pngBytes(t, maxPixelDim+1, 2)))
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catOversized)
		if res.PNG != nil {
			t.Fatal("oversized png accepted")
		}
		wantFloat(t, "plate.weight_g", res.Metadata.Plate.WeightG, 12.5)
	})
	t.Run("at cap accepted", func(t *testing.T) {
		entries := replaceEntry(baselineEntries(t), "Metadata/plate_2.png", string(pngBytes(t, maxPixelDim, 2)))
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, "")
		if res.PNG == nil || res.Preview.Status != StatusReady {
			t.Fatalf("status = %q, png = %d bytes", res.Preview.Status, len(res.PNG))
		}
	})
	t.Run("corrupt png rejected", func(t *testing.T) {
		bad := append([]byte(nil), pngBytes(t, 3, 2)...)
		bad[len(bad)/2] ^= 0xff
		entries := replaceEntry(baselineEntries(t), "Metadata/plate_2.png", string(bad))
		path := writeArchive(t, entries...)
		res, err := parseArchive(path, jobView(intPtr(2), ""))
		wantCategory(t, err, catArchive)
		if res.PNG != nil {
			t.Fatal("corrupt png accepted")
		}
		wantFloat(t, "plate.weight_g", res.Metadata.Plate.WeightG, 12.5)
	})
}

// ---- read caps and budget ----

func TestReadZipEntryCapsAndBudget(t *testing.T) {
	path := writeArchive(t, fixtureEntry{name: "e.txt", data: "hello world", method: zip.Store})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	f := zr.File[0]
	// Declared sizes beyond the cap skip decompression entirely; hostile
	// lied sizes are covered by the streaming allowance, which a standard
	// writer cannot produce.
	if _, err := readZipEntry(f, 5, nil); errorCategory(err) != catOversized {
		t.Fatalf("small cap error = %v", err)
	}
	data, err := readZipEntry(f, 11, nil)
	if err != nil || string(data) != "hello world" {
		t.Fatalf("exact cap: data = %q, err = %v", data, err)
	}
	budget := int64(3)
	if _, err := readZipEntry(f, 100, &budget); errorCategory(err) != catOversized {
		t.Fatalf("exhausted budget error = %v", err)
	}
	if budget != -1 {
		t.Fatalf("budget = %d, want -1", budget)
	}
	budget = 50
	data, err = readZipEntry(f, 100, &budget)
	if err != nil || string(data) != "hello world" || budget != 39 {
		t.Fatalf("budgeted read: data = %q, err = %v, budget = %d", data, err, budget)
	}
}

// ---- wire shape ----

func TestDisabledResult(t *testing.T) {
	d := Disabled()
	if d.Preview.Status != StatusDisabled || d.Preview.JobName != "" || d.Preview.Current {
		t.Fatalf("disabled view = %+v", d.Preview)
	}
	wantNil(t, "retrieved_at", d.Preview.RetrievedAt)
	wantNil(t, "image_url", d.Preview.ImageURL)
	wantNil(t, "metadata", d.Metadata)
	wantNil(t, "png", d.PNG)
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"job_preview":{"status":"disabled","job_name":"","current":false,"retrieved_at":null,"image_url":null},"job_metadata":null}`
	if string(raw) != want {
		t.Fatalf("disabled json = %s", raw)
	}
}

func TestResultJSONShape(t *testing.T) {
	path := writeArchive(t, baselineEntries(t)...)
	res, err := parseArchive(path, jobView(intPtr(2), ""))
	wantCategory(t, err, "")
	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytes.Contains(raw, []byte(`"png"`)) {
		t.Fatal("png leaked into json")
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	preview := doc["job_preview"].(map[string]any)
	if preview["status"] != StatusReady || preview["retrieved_at"] != nil {
		t.Fatalf("job_preview = %v", preview)
	}
	md := doc["job_metadata"].(map[string]any)
	if md["source"] != sourcePrinter3mf {
		t.Fatalf("source = %v", md["source"])
	}
	if mats, ok := md["materials"].([]any); !ok || len(mats) != 2 {
		t.Fatalf("materials = %v, want non-nil array of 2", md["materials"])
	}
	if objs, ok := md["objects"].([]any); !ok || len(objs) != 2 {
		t.Fatalf("objects = %v, want non-nil array of 2", md["objects"])
	}
	if warns, ok := md["warnings"].([]any); !ok || len(warns) != 2 {
		t.Fatalf("warnings = %v, want non-nil array of 2", md["warnings"])
	}
	if md["object_count"] != float64(3) {
		t.Fatalf("object_count = %v", md["object_count"])
	}
	if md["truncated"] != false {
		t.Fatalf("truncated = %v", md["truncated"])
	}
}

// ---- helper units ----

func TestArchivePlateGcodeMatching(t *testing.T) {
	for _, tc := range []struct {
		name  string
		want  int
		match bool
	}{
		{"Metadata/plate_2.gcode", 2, true},
		{"Metadata/plate_12.gcode", 12, true},
		{"Metadata/plate_0.gcode", 0, false},
		{"Metadata/plate_02.gcode", 0, false},
		{"Metadata/plate_+2.gcode", 0, false},
		{"Metadata/plate_2.gcode.bak", 0, false},
		{"metadata/plate_2.gcode", 0, false},
		{"Metadata/plate_2.png", 0, false},
		{"Metadata/plate_.gcode", 0, false},
	} {
		got, ok := archivePlateGcode(tc.name)
		if ok != tc.match || got != tc.want {
			t.Fatalf("%q = (%d, %v), want (%d, %v)", tc.name, got, ok, tc.want, tc.match)
		}
	}
}

func TestPlateFromGCodeFileMatching(t *testing.T) {
	for _, tc := range []struct {
		name string
		want int
	}{
		{"a_plate_12.gcode.3mf", 12},
		{"PLATE_3.GCODE", 3},
		{"job.gcode", 0},
		{"plate_x.gcode", 0},
		{"plate_99999999999999999999.gcode", 0},
		{"", 0},
	} {
		if got := plateFromGCodeFile(tc.name); got != tc.want {
			t.Fatalf("%q = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestTextAndNumberHelpers(t *testing.T) {
	if got, ok := parseNumericString(" 12.5 "); !ok || got != 12.5 {
		t.Fatalf("parseNumericString = %v, %v", got, ok)
	}
	for _, s := range []string{"inf", "-inf", "nan", "1e400", "", "abc"} {
		if _, ok := parseNumericString(s); ok {
			t.Fatalf("parseNumericString(%q) accepted", s)
		}
	}
	if got, ok := parsePercent("15%"); !ok || got != 15 {
		t.Fatalf("parsePercent = %v, %v", got, ok)
	}
	if got, ok := parsePercent(" 7 "); !ok || got != 7 {
		t.Fatalf("parsePercent = %v, %v", got, ok)
	}
	if _, ok := parsePercent("abc%"); ok {
		t.Fatal("parsePercent accepted junk")
	}
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"23d1aa", "#23D1AA"},
		{"#23d1aaff", "#23D1AAFF"},
		{"#FFFFFF", "#FFFFFF"},
		{"red", ""},
		{"#12345", ""},
		{"#1234567", ""},
		{"", ""},
	} {
		got := normalizeColor(tc.in)
		if tc.want == "" {
			if got != nil {
				t.Fatalf("normalizeColor(%q) = %v, want nil", tc.in, *got)
			}
			continue
		}
		if got == nil || *got != tc.want {
			t.Fatalf("normalizeColor(%q) = %v, want %q", tc.in, got, tc.want)
		}
	}
	if s := collapseSpace("  a\t\tb  \n c "); s != "a b c" {
		t.Fatalf("collapseSpace = %q", s)
	}
	if s, trunc := capRunes("hello", 10); trunc || s != "hello" {
		t.Fatalf("capRunes = %q, %v", s, trunc)
	}
}
