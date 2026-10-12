package jobpreview

import (
	"archive/zip"
	"bytes"
	"context"
	"image"
	"image/png"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
	"bambu-mqtt-proxy/internal/testutil/mcptest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// archiveFixture builds a complete 3MF fixture with Bambu sidecars and a PNG.
func archiveFixture(t *testing.T) ([]byte, []byte) {
	t.Helper()
	var imageData, archive bytes.Buffer
	if err := png.Encode(&imageData, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(&archive)
	entries := map[string]string{
		"3D/3dmodel.model":                 `<model xmlns="http://schemas.microsoft.com/3dmanufacturing/core/2015/02"><metadata name="Title">Fixture</metadata><metadata name="Description">&lt;p&gt;Plain &lt;b&gt;text&lt;/b&gt;&lt;/p&gt;</metadata><metadata name="Designer">Designer</metadata><metadata name="License">CC-BY</metadata><resources/><build/></model>`,
		"Metadata/model_settings.config":   `<config><plate><metadata key="plater_id" value="1"/><metadata key="plater_name" value="Main plate"/></plate></config>`,
		"Metadata/slice_info.config":       `<config><plate><metadata key="index" value="1"/><metadata key="prediction" value="600"/><metadata key="weight" value="12.5"/><metadata key="first_layer_time" value="30"/><metadata key="support_used" value="true"/><filament id="1" type="PLA" color="#FF0000" used_g="12.5" used_m="4"/><object name="Box"/><object name="Box"/><object name="Skip" skipped="true"/><warning msg="bed_temperature_too_high_than_filament" error_code="W1"/></plate></config>`,
		"Metadata/plate_1.json":            `{"bed_type":"Textured PEI"}`,
		"Metadata/project_settings.config": `{"layer_height":"0.2", "sparse_infill_density":"15%"}`,
		"Metadata/plate_1.gcode":           "; plate 1\n",
		"Metadata/plate_1.png":             imageData.String(),
	}
	for name, data := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(w, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes(), imageData.Bytes()
}

// TestArchiveWallFields checks the thumbnail and every wall metadata group.
func TestArchiveWallFields(t *testing.T) {
	fixture, image := archiveFixture(t)
	for _, k := range []jobKey{{Plate: 1}, {File: "Metadata/plate_1.gcode"}, {}} {
		result, err := parseArchive(fixture, k)
		if err != nil {
			t.Fatal(err)
		}
		md := result.Metadata
		if !bytes.Equal(result.PNG, image) || md.Plate.Index != 1 || *md.Plate.Name != "Main plate" || *md.Plate.BedType != "Textured PEI" || *md.Plate.EstimatedSeconds != 600 || *md.Plate.WeightG != 12.5 || *md.Plate.FirstLayerSeconds != 30 || !*md.Plate.Supports || *md.ObjectCount != 2 {
			t.Fatalf("plate facts = %+v", md)
		}
		if *md.Title != "Fixture" || *md.Description != "Plain text" || *md.Designer != "Designer" || *md.License != "CC-BY" || len(md.Materials) != 1 || *md.Materials[0].Type != "PLA" || *md.Materials[0].Color != "#FF0000" || *md.Materials[0].UsedG != 12.5 || len(md.Objects) != 1 || md.Objects[0] != (Object{Name: "Box", Count: 2}) || len(md.Warnings) != 1 || md.Warnings[0].Code != "W1" || *md.Process.LayerHeightMM != .2 || *md.Process.InfillPercent != 15 {
			t.Fatalf("wall metadata = %+v", md)
		}
	}
}

// testState supplies documents and forwards events without network commands.
type testState struct {
	// mu guards the current document and callback.
	mu sync.Mutex
	// doc contains the printer report.
	doc printer.Doc
	// subscriber receives report events.
	subscriber func(printer.Event)
}

// Get returns one authoritative document.
func (s *testState) Get(string) (printer.Doc, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doc, true, true
}

// Subscribe stores the service callback.
func (s *testState) Subscribe(fn func(printer.Event)) { s.subscriber = fn }

// report replaces the test document and emits its event.
func (s *testState) report(d printer.Doc, kinds ...string) {
	s.mu.Lock()
	prev := s.doc
	s.doc = d
	s.mu.Unlock()
	s.subscriber(printer.Event{Serial: "S1", Prev: prev, Cur: d, Online: true, Kinds: kinds, NewJob: printer.NewJob(prev, d)})
}

// jobDoc creates a printer report with an archive key.
func jobDoc(name string) printer.Doc {
	return printer.Doc{"print": map[string]any{"gcode_state": "RUNNING", "subtask_name": name, "plate_idx": float64(1)}}
}

// TestKeyChangeAndMCP checks boot-mid-print fetching, past entries, and the real MCP boundary.
func TestKeyChangeAndMCP(t *testing.T) {
	state := &testState{}
	svc := New([]config.Printer{{Serial: "S1"}}, state, slog.New(slog.NewTextHandler(io.Discard, nil)))
	fixture, image := archiveFixture(t)
	fetched := make(chan struct{}, 1)
	svc.fetch = func(ctx context.Context, _ config.Printer, k jobKey) (Result, error) {
		if k.Name != "first" {
			<-ctx.Done()
			return Result{}, ctx.Err()
		}
		result, err := parseArchive(fixture, k)
		fetched <- struct{}{}
		return result, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	state.report(jobDoc("first"), "state_initial")
	select {
	case <-fetched:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Cache publication is the observable completion condition.
	for {
		result, _ := svc.Lookup("S1", true)
		if result.Preview.Status == StatusReady {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
			runtime.Gosched()
		}
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "preview-test", Version: "1"}, nil)
	svc.Module().MCP(server)
	cs := mcptest.Connect(t, ctx, server)
	for _, serial := range []string{"S1", "unknown"} {
		result, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "get_job_preview", Arguments: map[string]any{"serial": serial}})
		if err != nil {
			t.Fatal(err)
		}
		out := mcptest.Decode[JobPreviewOut](t, result)
		if serial == "unknown" {
			if !result.IsError || out.Error == nil {
				t.Fatal("unknown serial accepted")
			}
			continue
		}
		if out.JobPreview.Status != StatusReady || !out.JobPreview.Current || out.JobMetadata == nil {
			t.Fatalf("MCP output = %+v", out)
		}
		found := false
		for _, content := range result.Content {
			if c, ok := content.(*mcp.ImageContent); ok {
				found = bytes.Equal(c.Data, image)
			}
		}
		if !found {
			t.Fatal("MCP thumbnail missing")
		}
	}
	state.report(jobDoc("second"))
	result, _ := svc.Lookup("S1", true)
	if result.Preview.Current || result.Preview.JobName != "first" || !bytes.Equal(result.PNG, image) {
		t.Fatalf("past entry = %+v", result)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
