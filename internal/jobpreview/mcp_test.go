package jobpreview

// Wire-level tests for the get_job_preview module tool. They drive a real
// mcp.Server through the SDK in-memory transports, so the SDK validates the
// input and output schemas and decodes the structured content. Published
// preview entries come from the service's own finish path with the same
// fixture helpers the scheduler tests use.

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/module"
)

// connectPreviewMCP connects an SDK client to a server that carries only
// the preview service's get_job_preview tool. The service is not started;
// the tool is a pure cache read and never needs the scheduler.
func connectPreviewMCP(t *testing.T, svc *Service) *mcp.ClientSession {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	svc.registerMCP(srv)
	ctx := context.Background()
	sTransport, cTransport := mcp.NewInMemoryTransports()
	serverSession, err := srv.Connect(ctx, sTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { serverSession.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	clientSession, err := client.Connect(ctx, cTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { clientSession.Close() })
	return clientSession
}

// callPreview runs the tool and returns the SDK-decoded result.
func callPreview(t *testing.T, client *mcp.ClientSession, serial string) *mcp.CallToolResult {
	t.Helper()
	res, err := client.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "get_job_preview", Arguments: map[string]any{"serial": serial},
	})
	if err != nil {
		t.Fatalf("call get_job_preview: %v", err)
	}
	return res
}

// previewOut is the decode target for the tool's structured content.
type previewOut struct {
	Serial      string            `json:"serial"`
	JobPreview  View              `json:"job_preview"`
	JobMetadata *Metadata         `json:"job_metadata"`
	Error       *module.ToolError `json:"error"`
}

// decodePreview reads the structured output of a preview tool result.
func decodePreview(t *testing.T, res *mcp.CallToolResult) previewOut {
	t.Helper()
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatalf("marshal structured: %v", err)
	}
	var out previewOut
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal structured: %v", err)
	}
	return out
}

func TestGetJobPreviewUnknownSerial(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, newFakeConn("S1"), rd, clk, "S1")
	client := connectPreviewMCP(t, svc)

	res := callPreview(t, client, "NOPE")
	if !res.IsError {
		t.Fatalf("unknown serial not flagged error: %+v", res)
	}
	out := decodePreview(t, res)
	if out.Error == nil || out.Error.Code != module.CodeUnknownSerial ||
		out.Error.Message != "serial is not configured: NOPE" {
		t.Fatalf("unknown serial output = %+v", out)
	}
	if out.Serial != "NOPE" || out.JobPreview.Status != StatusDisabled || out.JobMetadata != nil {
		t.Fatalf("unknown serial fields = %+v", out)
	}
}

func TestGetJobPreviewNotReady(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, newFakeConn("S1"), rd, clk, "S1")
	client := connectPreviewMCP(t, svc)

	res := callPreview(t, client, "S1")
	if res.IsError {
		t.Fatalf("not-ready flagged error: %+v", res)
	}
	out := decodePreview(t, res)
	if out.Error != nil {
		t.Fatalf("not-ready carried error: %+v", out.Error)
	}
	want := "S1: job preview " + out.JobPreview.Status
	var text string
	for _, c := range res.Content {
		if tc, isText := c.(*mcp.TextContent); isText {
			text = tc.Text
		}
	}
	if text != want {
		t.Fatalf("not-ready text = %q, want %q", text, want)
	}
	for _, c := range res.Content {
		if _, isImage := c.(*mcp.ImageContent); isImage {
			t.Fatalf("not-ready result carried image content: %+v", res.Content)
		}
	}
}

func TestGetJobPreviewReady(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, newFakeConn("S1"), rd, clk, "S1")
	adm := &admission{
		printer:  config.Printer{Serial: "S1", Address: "S1.invalid:8883", Username: "u", Password: "p"},
		job:      settledJob(base, 7),
		gen:      7,
		rev:      1,
		epoch:    2,
		upstream: 4,
	}
	reserveSlot(svc)
	png := pngBytes(t, 2, 2)
	title := "Title"
	svc.finish(adm, Result{PNG: png, Metadata: &Metadata{Source: sourcePrinter3mf, Title: &title}}, nil)
	if res, ok := svc.Lookup("S1", false); !ok || res.Preview.Status != StatusReady {
		t.Fatalf("fixture did not publish a ready entry: %+v", res)
	}
	client := connectPreviewMCP(t, svc)

	res := callPreview(t, client, "S1")
	if res.IsError {
		t.Fatalf("ready flagged error: %+v", res)
	}
	out := decodePreview(t, res)
	if out.Error != nil || out.Serial != "S1" ||
		out.JobPreview.Status != StatusReady || out.JobPreview.JobName != "Benchy" {
		t.Fatalf("ready structured = %+v", out)
	}
	if out.JobMetadata == nil || out.JobMetadata.Source != sourcePrinter3mf ||
		out.JobMetadata.Title == nil || *out.JobMetadata.Title != "Title" {
		t.Fatalf("ready metadata = %+v", out.JobMetadata)
	}
	// The text content comes first, then the PNG image content with the
	// exact published bytes.
	if len(res.Content) < 2 {
		t.Fatalf("ready content = %+v", res.Content)
	}
	textC, isText := res.Content[0].(*mcp.TextContent)
	if !isText {
		t.Fatalf("first content is not text: %+v", res.Content[0])
	}
	for _, want := range []string{"sliced plate render", "not a camera photograph", "untrusted"} {
		if !bytes.Contains([]byte(textC.Text), []byte(want)) {
			t.Fatalf("ready text %q lacks %q", textC.Text, want)
		}
	}
	image, isImage := res.Content[1].(*mcp.ImageContent)
	if !isImage {
		t.Fatalf("second content is not image: %+v", res.Content[1])
	}
	if !bytes.Equal(image.Data, png) || image.MIMEType != "image/png" {
		t.Fatalf("image content = %d bytes %s", len(image.Data), image.MIMEType)
	}

	// A repeat call is a pure cache read: identical payload.
	again := callPreview(t, client, "S1")
	raw1, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal result 1: %v", err)
	}
	raw2, err := json.Marshal(again)
	if err != nil {
		t.Fatalf("marshal result 2: %v", err)
	}
	if !bytes.Equal(raw1, raw2) {
		t.Fatalf("repeat call differed:\n%s\n%s", raw1, raw2)
	}
}
