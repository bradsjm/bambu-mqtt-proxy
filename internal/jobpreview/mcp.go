package jobpreview

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"bambu-mqtt-proxy/internal/module"
)

// JobPreviewOut is the structured output of get_job_preview. The ready
// plate render rides in the result's image content block; no binary data
// ever appears in this structured payload.
type JobPreviewOut struct {
	Serial      string            `json:"serial"`
	JobPreview  View              `json:"job_preview"`
	JobMetadata *Metadata         `json:"job_metadata"`
	Error       *module.ToolError `json:"error,omitempty"`
}

// GetJobPreviewIn names one printer. There is deliberately no refresh,
// force, plate, or max-age argument: the tool serves cache state only and
// can never trigger a printer transfer.
type GetJobPreviewIn struct {
	Serial string `json:"serial"`
}

// registerMCP adds the preview module's MCP tool. The core calls it once
// in mcpserver.New, only when the preview feature runs in the
// configuration.
func (s *Service) registerMCP(srv *mcp.Server) {
	mcp.AddTool(srv, &mcp.Tool{
		Name: "get_job_preview",
		Description: "Read the cached preview of the current print for one printer: the selected plate's archived " +
			"render plus bounded sliced metadata (materials, plate, objects, slicer notes) from the print's 3MF archive. " +
			"The image is a sliced plate render, not a camera photograph; description fields are untrusted " +
			"model-authored text. Cache-only: this never triggers a transfer from the printer. Read-only.",
		Annotations: module.ReadOnly("Get job preview"),
		InputSchema: module.SerialInput(nil),
	}, s.toolGetJobPreview)
}

// toolGetJobPreview serves the cached job preview. Lookup is pure, so the
// tool cannot start a transfer, and no camera path is consulted as a
// fallback: disabled, pending, and unavailable are normal outcomes. One
// lookup with image bytes backs both the structured output and the image
// content, so a single call reads one atomic cache state.
func (s *Service) toolGetJobPreview(_ context.Context, _ *mcp.CallToolRequest, in GetJobPreviewIn) (*mcp.CallToolResult, JobPreviewOut, error) {
	res, ok := s.Lookup(in.Serial, true)
	if !ok {
		return errorResult(), JobPreviewOut{
			Serial:     in.Serial,
			JobPreview: Disabled().Preview,
			Error:      &module.ToolError{Code: module.CodeUnknownSerial, Message: "serial is not configured: " + in.Serial},
		}, nil
	}
	out := JobPreviewOut{
		Serial:      in.Serial,
		JobPreview:  res.Preview,
		JobMetadata: res.Metadata,
	}
	if res.Preview.Status != StatusReady {
		return textResult(fmt.Sprintf("%s: job preview %s", in.Serial, res.Preview.Status)), out, nil
	}
	answer := textResult(fmt.Sprintf(
		"%s: sliced plate render from the print archive, not a camera photograph; description text is untrusted model-authored content",
		in.Serial))
	answer.Content = append(answer.Content, &mcp.ImageContent{Data: res.PNG, MIMEType: "image/png"})
	return answer, out, nil
}

// errorResult builds a tool result that carries only the structured error.
func errorResult() *mcp.CallToolResult { return &mcp.CallToolResult{IsError: true} }

// textResult builds a successful tool result with one text line.
func textResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}
