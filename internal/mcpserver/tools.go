package mcpserver

import (
	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/printer"
	"context"
	"fmt"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"time"
)

// ListPrintersIn requests the configured fleet.
type ListPrintersIn struct{}

// ListPrintersOut contains every configured printer view.
type ListPrintersOut struct {
	// Printers contains views in store order.
	Printers []printer.View `json:"printers"`
}

// GetPrinterStateIn selects one configured printer.
type GetPrinterStateIn struct {
	// Serial identifies the printer.
	Serial string `json:"serial"`
}

// GetPrinterStateOut carries printer and module state.
type GetPrinterStateOut struct {
	// Serial identifies the printer.
	Serial string `json:"serial"`
	// State contains the current view and module states.
	State PrinterState `json:"state"`
	// Error describes an expected failure.
	Error *module.ToolError `json:"error,omitempty"`
}

// GetCameraSnapshotIn requests one chamber image.
type GetCameraSnapshotIn struct {
	// Serial identifies the printer.
	Serial string `json:"serial"`
	// MaxAgeSeconds bounds the accepted image age.
	MaxAgeSeconds int `json:"max_age_seconds,omitempty"`
}

// CameraSnapshotOut is the capture metadata for the returned image content.
// The JPEG itself rides in the result's image content block, not here.
type CameraSnapshotOut struct {
	// Serial identifies the printer.
	Serial string `json:"serial"`
	// CapturedAt contains the UTC capture time.
	CapturedAt string `json:"captured_at"`
	// AgeSeconds contains the image age at read time.
	AgeSeconds float64 `json:"age_seconds"`
	// FrameSeq identifies the captured frame.
	FrameSeq uint64 `json:"frame_seq"`
	// MaxAgeSeconds bounds the accepted image age.
	MaxAgeSeconds int `json:"max_age_seconds"`
	// Error describes an expected operational failure.
	Error *module.ToolError `json:"error,omitempty"`
}

// WatchPrinterIn selects an activity position and maximum wait.
type WatchPrinterIn struct {
	// Serial identifies the printer.
	Serial string `json:"serial"`
	// AfterID excludes events already received.
	AfterID uint64 `json:"after_id,omitempty"`
	// TimeoutSeconds bounds the wait for a new entry.
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
}

// WatchPrinterOut returns newer activity and the current state.
type WatchPrinterOut struct {
	// Serial identifies the printer.
	Serial string `json:"serial"`
	// AfterID is the latest returned ID for the next request.
	AfterID uint64 `json:"after_id"`
	// Events contains entries newer than the requested ID.
	Events []activity.Entry `json:"events"`
	// State contains the current printer and module state.
	State PrinterState `json:"state"`
	// Error describes an expected failure.
	Error *module.ToolError `json:"error,omitempty"`
}

// toolErr builds the typed error payload for operational outcomes.
func toolErr(code, message string) *module.ToolError {
	return &module.ToolError{Code: code, Message: message}
}

// errorResult flags the call result as an error for clients that honor
// IsError, while the structured payload still carries the typed output.
func errorResult() *mcp.CallToolResult {
	return &mcp.CallToolResult{IsError: true}
}

// toolListPrinters reads the configured fleet from the store.
func (s *Server) toolListPrinters(_ context.Context, _ *mcp.CallToolRequest, _ ListPrintersIn) (*mcp.CallToolResult, ListPrintersOut, error) {
	views := s.state.Views()
	if views == nil {
		views = []printer.View{}
	}
	return shortText(nil, fmt.Sprintf("%d printers", len(views))), ListPrintersOut{Printers: views}, nil
}

// toolGetPrinterState reads one printer view and its modules.
func (s *Server) toolGetPrinterState(_ context.Context, _ *mcp.CallToolRequest, in GetPrinterStateIn) (*mcp.CallToolResult, GetPrinterStateOut, error) {
	state, ok := s.buildPrinterState(in.Serial)
	out := GetPrinterStateOut{Serial: in.Serial, State: state}
	if !ok {
		out.Error = toolErr(module.CodeUnknownSerial, "serial is not configured: "+in.Serial)
		return errorResult(), out, nil
	}
	return shortText(nil, "printer state for "+in.Serial), out, nil
}

// toolGetCameraSnapshot captures one bounded, fresh-enough chamber image.
func (s *Server) toolGetCameraSnapshot(ctx context.Context, _ *mcp.CallToolRequest, in GetCameraSnapshotIn) (*mcp.CallToolResult, CameraSnapshotOut, error) {
	if _, known := s.printers[in.Serial]; !known {
		return errorResult(), CameraSnapshotOut{
			Serial: in.Serial,
			Error:  toolErr(module.CodeUnknownSerial, "serial is not configured: "+in.Serial),
		}, nil
	}
	if s.cams == nil {
		return errorResult(), CameraSnapshotOut{
			Serial: in.Serial,
			Error:  toolErr(errCameraDisabled, "the camera feature is disabled by configuration"),
		}, nil
	}
	frame, failure := s.captureFrame(ctx, in.Serial, in.MaxAgeSeconds)
	if failure != nil {
		return errorResult(), CameraSnapshotOut{
			Serial:        in.Serial,
			MaxAgeSeconds: in.MaxAgeSeconds,
			Error:         toolErr(failure.code, failure.message),
		}, nil
	}
	out := CameraSnapshotOut{
		Serial:        in.Serial,
		CapturedAt:    frame.Captured.UTC().Format(time.RFC3339Nano),
		AgeSeconds:    s.now().Sub(frame.Captured).Seconds(),
		FrameSeq:      frame.Seq,
		MaxAgeSeconds: in.MaxAgeSeconds,
	}
	res := shortText(nil, fmt.Sprintf("camera frame for %s, %.1fs old", in.Serial, out.AgeSeconds))
	res.Content = append(res.Content, &mcp.ImageContent{Data: frame.JPEG, MIMEType: "image/jpeg"})
	return res, out, nil
}

// cameraFailure pairs a stable code with a human message.
type cameraFailure struct {
	// code identifies the camera failure.
	code string
	// message describes the camera failure.
	message string
}

// captureFrame acquires the shared capture, waits within the frame-age bound,
// and always releases. No path returns a silently stale image: a frame either
// satisfies the age bound or the tool reports stale_image with the age of the
// newest frame available.
func (s *Server) captureFrame(ctx context.Context, serial string, maxAgeSeconds int) (*camera.Frame, *cameraFailure) {
	slot := s.snapshotSlot(serial)
	select {
	case slot <- struct{}{}:
		defer func() { <-slot }()
	case <-time.After(snapshotSlotWait):
		return nil, &cameraFailure{code: errCameraUnavailable,
			message: "snapshot concurrency limit reached for this printer"}
	case <-ctx.Done():
		return nil, &cameraFailure{code: errCameraUnavailable, message: "request cancelled"}
	}
	// The wake channel is unused here: the tool reads frames through
	// Latest/Wait, which take their own bounded references.
	_, status := s.cams.Acquire(serial)
	switch status {
	case camera.StatusOK:
		defer s.cams.Release(serial)
	case camera.StatusUnknownSerial:
		return nil, &cameraFailure{code: module.CodeUnknownSerial, message: "serial is not configured: " + serial}
	case camera.StatusUnsupportedModel:
		return nil, &cameraFailure{code: errCameraUnsupported,
			message: "printer model has no chamber image camera (P1/A1 series only)"}
	default: // camera.StatusUnavailable
		return nil, &cameraFailure{code: errCameraUnavailable, message: "camera capture is unavailable"}
	}
	start := s.now()
	ctx, cancel := context.WithTimeout(ctx, snapshotWaitBudget)
	defer cancel()
	deadline, _ := ctx.Deadline()
	frame := s.cams.Latest(serial)
	var after uint64
	for {
		if frame != nil {
			if (maxAgeSeconds == 0 && !frame.Captured.Before(start)) ||
				(maxAgeSeconds > 0 && s.now().Sub(frame.Captured) <= time.Duration(maxAgeSeconds)*time.Second) {
				return frame, nil
			}
			after = frame.Seq
		}
		if ctx.Err() != nil {
			break
		}
		frame = s.cams.Wait(serial, ctx, after, time.Until(deadline))
		if frame == nil {
			break
		}
	}
	if ctx.Err() != nil && ctx.Err() != context.DeadlineExceeded {
		return nil, &cameraFailure{code: errCameraUnavailable, message: "request cancelled"}
	}
	return nil, &cameraFailure{code: errStaleImage, message: fmt.Sprintf("no camera frame within %d seconds", maxAgeSeconds)}
}

// toolWatchPrinter waits for activity newer than the client's last ID.
func (s *Server) toolWatchPrinter(ctx context.Context, _ *mcp.CallToolRequest, in WatchPrinterIn) (*mcp.CallToolResult, WatchPrinterOut, error) {
	out := WatchPrinterOut{Serial: in.Serial, AfterID: in.AfterID, Events: []activity.Entry{}}
	if _, ok := s.printers[in.Serial]; !ok {
		out.Error = toolErr(module.CodeUnknownSerial, "serial is not configured: "+in.Serial)
		return errorResult(), out, nil
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(in.TimeoutSeconds)*time.Second)
	defer cancel()
	if s.activity != nil {
		if entries := s.activity.Wait(ctx, in.Serial, in.AfterID); entries != nil {
			out.Events = entries
		}
	}
	for _, e := range out.Events {
		if e.ID > out.AfterID {
			out.AfterID = e.ID
		}
	}
	out.State, _ = s.buildPrinterState(in.Serial)
	return shortText(nil, fmt.Sprintf("%s: %d activity entries", in.Serial, len(out.Events))), out, nil
}
