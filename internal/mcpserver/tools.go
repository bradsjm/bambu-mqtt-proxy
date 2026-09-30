package mcpserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"bambu-mqtt-proxy/internal/camera"
)

// Tool input types. The SDK validates inputs and applies schema defaults
// before the handler runs, so an omitted optional field arrives with the
// documented default and an explicit 0 stays a 0.

// ListPrintersIn selects one page of the printer list.
type ListPrintersIn struct {
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

// PrinterSummary is one row of list_printers.
type PrinterSummary struct {
	Serial           string        `json:"serial"`
	Name             string        `json:"name"`
	Model            string        `json:"model"`
	Connected        bool          `json:"connected"`
	PrintState       *string       `json:"print_state"`
	Printing         bool          `json:"printing"`
	Progress         *float64      `json:"progress"`
	RemainingMinutes *float64      `json:"remaining_minutes"`
	AlertCount       int           `json:"alert_count"`
	ChamberLight     *string       `json:"chamber_light"`
	Fresh            bool          `json:"fresh"`
	Detection        DetectionView `json:"detection"`
	Revision         string        `json:"revision"`
}

// ListPrintersOut is one page; NextCursor is empty after the last page.
type ListPrintersOut struct {
	Printers   []PrinterSummary `json:"printers"`
	NextCursor string           `json:"next_cursor,omitempty"`
	Error      *ToolError       `json:"error,omitempty"`
}

// GetPrinterStateIn names one printer.
type GetPrinterStateIn struct {
	Serial string `json:"serial"`
}

// GetPrinterStateOut carries the typed projection and its revision token.
type GetPrinterStateOut struct {
	Serial   string       `json:"serial"`
	Revision string       `json:"revision"`
	State    PrinterState `json:"state"`
	Error    *ToolError   `json:"error,omitempty"`
}

// GetCameraSnapshotIn requests one chamber image.
type GetCameraSnapshotIn struct {
	Serial        string `json:"serial"`
	MaxAgeSeconds int    `json:"max_age_seconds,omitempty"`
}

// CameraSnapshotOut is the capture metadata for the returned image content.
// The JPEG itself rides in the result's image content block, not here.
type CameraSnapshotOut struct {
	Serial        string     `json:"serial"`
	CapturedAt    string     `json:"captured_at"`
	AgeSeconds    float64    `json:"age_seconds"`
	FrameSeq      uint64     `json:"frame_seq"`
	MaxAgeSeconds int        `json:"max_age_seconds"`
	Error         *ToolError `json:"error,omitempty"`
}

// WatchPrinterIn long-polls one printer.
type WatchPrinterIn struct {
	Serial         string `json:"serial"`
	AfterRevision  string `json:"after_revision,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
	Mode           string `json:"mode,omitempty"`
}

// WatchPrinterOut answers one long poll: the current snapshot, the revision
// to replay next time, the change flag, and coalesced change events.
type WatchPrinterOut struct {
	Serial         string       `json:"serial"`
	Mode           string       `json:"mode"`
	Revision       string       `json:"revision"`
	Changed        bool         `json:"changed"`
	ResyncRequired bool         `json:"resync_required"`
	Events         []WatchEvent `json:"events,omitempty"`
	State          PrinterState `json:"state"`
	Error          *ToolError   `json:"error,omitempty"`
}

// toolErr builds the typed error payload for operational outcomes.
func toolErr(code, message string) *ToolError {
	return &ToolError{Code: code, Message: message}
}

// errorResult flags the call result as an error for clients that honor
// IsError, while the structured payload still carries the typed output.
func errorResult() *mcp.CallToolResult {
	return &mcp.CallToolResult{IsError: true}
}

// toolListPrinters pages the configured printers by serial.
func (s *Server) toolListPrinters(_ context.Context, _ *mcp.CallToolRequest, in ListPrintersIn) (*mcp.CallToolResult, ListPrintersOut, error) {
	limit := in.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	after := ""
	last := ""
	if in.Cursor != "" {
		serial, ok := decodeCursor(in.Cursor)
		if !ok {
			return errorResult(), ListPrintersOut{
				Error: toolErr(errInvalidCursor, "cursor is not a valid page token"),
			}, nil
		}
		after = serial
	}
	now := s.now()
	out := ListPrintersOut{Printers: make([]PrinterSummary, 0, limit)}
	for _, serial := range s.serials {
		if serial <= after {
			continue
		}
		if len(out.Printers) == limit {
			// The cursor resumes strictly after the last row this page
			// served, so the next page starts at the current serial.
			out.NextCursor = encodeCursor(last)
			break
		}
		out.Printers = append(out.Printers, s.summarize(serial, now))
		last = serial
	}
	return shortText(nil, fmt.Sprintf("%d printers", len(out.Printers))), out, nil
}

// summarize builds one list row with a fresh revision token.
func (s *Server) summarize(serial string, now time.Time) PrinterSummary {
	// The revision is captured before any source read. A token issued after
	// the state reads could describe a change the row does not show, and the
	// client's next watch would then match the counter and never learn of
	// the change. Captured first, the token can only be stale, and staleness
	// forces a resync.
	rev := s.sampler.revision(serial)
	st, _ := s.buildPrinterState(serial, now)
	summary := PrinterSummary{
		Serial:           serial,
		Name:             st.Name,
		Model:            st.Model,
		Connected:        st.Connected,
		PrintState:       st.PrintState,
		Printing:         st.Printing,
		Progress:         st.Progress,
		RemainingMinutes: st.RemainingMinutes,
		AlertCount:       st.ActiveAlerts(),
		ChamberLight:     st.ChamberLight,
		Fresh:            st.Freshness.Fresh,
		Detection:        st.Detection,
	}
	summary.Revision = s.token(rev)
	return summary
}

// toolGetPrinterState serves the typed projection.
func (s *Server) toolGetPrinterState(_ context.Context, _ *mcp.CallToolRequest, in GetPrinterStateIn) (*mcp.CallToolResult, GetPrinterStateOut, error) {
	// Revision first, state second: see summarize.
	rev := s.sampler.revision(in.Serial)
	state, known := s.buildPrinterState(in.Serial, s.now())
	if !known {
		return errorResult(), GetPrinterStateOut{
			Serial: in.Serial,
			Error:  toolErr(errUnknownSerial, "serial is not configured: "+in.Serial),
		}, nil
	}
	out := GetPrinterStateOut{
		Serial:   in.Serial,
		Revision: s.token(rev),
		State:    state,
	}
	return shortText(nil, fmt.Sprintf("%s: %s", in.Serial, stateLine(state))), out, nil
}

// stateLine renders the one-line text summary. Parts whose value is null
// are omitted.
func stateLine(st PrinterState) string {
	state := "unknown"
	if st.PrintState != nil {
		state = *st.PrintState
	}
	if st.Progress != nil {
		state += fmt.Sprintf(" %.0f%%", *st.Progress)
		if st.RemainingMinutes != nil {
			state += fmt.Sprintf(" (%.0f min left)", *st.RemainingMinutes)
		}
	}
	parts := []string{state}
	if st.JobName != nil {
		parts = append(parts, *st.JobName)
	}
	parts = append(parts, fmt.Sprintf("%d alerts", st.ActiveAlerts()))
	if st.ChamberLight != nil {
		parts = append(parts, "light "+*st.ChamberLight)
	}
	conn := "disconnected"
	if st.Connected {
		conn = "connected"
	}
	parts = append(parts, "upstream "+conn, "camera "+st.Camera, fmt.Sprintf("fresh %v", st.Freshness.Fresh))
	if len(st.Controls) > 0 {
		parts = append(parts, "controls "+strings.Join(st.Controls, ","))
	}
	return strings.Join(parts, " · ")
}

// toolGetCameraSnapshot captures one bounded, fresh-enough chamber image.
func (s *Server) toolGetCameraSnapshot(ctx context.Context, _ *mcp.CallToolRequest, in GetCameraSnapshotIn) (*mcp.CallToolResult, CameraSnapshotOut, error) {
	if _, known := s.printers[in.Serial]; !known {
		return errorResult(), CameraSnapshotOut{
			Serial: in.Serial,
			Error:  toolErr(errUnknownSerial, "serial is not configured: "+in.Serial),
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
	code    string
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
		return nil, &cameraFailure{code: errUnknownSerial, message: "serial is not configured: " + serial}
	case camera.StatusUnsupportedModel:
		return nil, &cameraFailure{code: errCameraUnsupported,
			message: "printer model has no chamber image camera (P1/A1 series only)"}
	default: // camera.StatusUnavailable
		return nil, &cameraFailure{code: errCameraUnavailable, message: "camera capture is unavailable"}
	}
	maxAge := time.Duration(maxAgeSeconds) * time.Second
	start := s.now()
	acceptable := func(f *camera.Frame) bool {
		if maxAge == 0 {
			// Age bound zero: only a frame captured at or after the request
			// qualifies.
			return !f.Captured.Before(start)
		}
		return s.now().Sub(f.Captured) <= maxAge
	}
	latest := s.cams.Latest(serial)
	if latest != nil && acceptable(latest) {
		return latest, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, &cameraFailure{code: errCameraUnavailable, message: "request cancelled"}
	}
	var after uint64
	ageNote := ""
	if latest != nil {
		after = latest.Seq
		ageNote = fmt.Sprintf("; newest frame is %.1fs old", s.now().Sub(latest.Captured).Seconds())
	}
	deadline := start.Add(snapshotWaitBudget)
	for {
		wait := snapshotWaitBudget
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		if wait <= 0 {
			break
		}
		f := s.cams.Wait(serial, ctx, after, wait)
		if f == nil {
			// Wait gives up on a closed capture, a quiet capture, or a done
			// context; only the last one is the caller's cancellation.
			if err := ctx.Err(); err != nil {
				return nil, &cameraFailure{code: errCameraUnavailable, message: "request cancelled"}
			}
			break // capture closed or no newer frame in time
		}
		if acceptable(f) {
			return f, nil
		}
		after = f.Seq
	}
	return nil, &cameraFailure{code: errStaleImage,
		message: fmt.Sprintf("no camera frame within %d seconds%s", maxAgeSeconds, ageNote)}
}

// toolWatchPrinter long-polls one printer for notification-relevant changes.
func (s *Server) toolWatchPrinter(ctx context.Context, _ *mcp.CallToolRequest, in WatchPrinterIn) (*mcp.CallToolResult, WatchPrinterOut, error) {
	if _, known := s.printers[in.Serial]; !known {
		return errorResult(), WatchPrinterOut{
			Serial: in.Serial, Mode: "attention",
			Error: toolErr(errUnknownSerial, "serial is not configured: "+in.Serial),
		}, nil
	}
	mode := in.Mode
	if mode == "" {
		mode = "attention"
	}
	// The schema default fills an omitted timeout with defaultWatchSeconds,
	// so an explicit 0 here means "return immediately".
	timeout := in.TimeoutSeconds
	if timeout < 0 {
		timeout = 0
	}
	if timeout > maxWatchSeconds {
		timeout = maxWatchSeconds
	}

	select {
	case s.waits <- struct{}{}:
		defer func() { <-s.waits }()
	default:
		return errorResult(), WatchPrinterOut{
			Serial: in.Serial, Mode: mode,
			Error: toolErr(errTooManyWaits,
				fmt.Sprintf("at most %d concurrent watches are served", maxWaits)),
		}, nil
	}

	cur, wake := s.sampler.watch(in.Serial, mode)

	answer := func(changed, resync bool) (*mcp.CallToolResult, WatchPrinterOut, error) {
		// Revision first, state second: see summarize. On the wake path the
		// counter has already moved before answer runs, so the captured
		// revision always describes a state at least as new as the one read
		// below, and a change landing mid-read stays detectably stale.
		rev := s.sampler.revision(in.Serial)
		state, known := s.buildPrinterState(in.Serial, s.now())
		if !known {
			return errorResult(), WatchPrinterOut{
				Serial: in.Serial, Mode: mode,
				Error: toolErr(errUnknownSerial, "serial is not configured: "+in.Serial),
			}, nil
		}
		out := WatchPrinterOut{
			Serial:         in.Serial,
			Mode:           mode,
			Revision:       s.token(rev),
			Changed:        changed,
			ResyncRequired: resync,
			State:          state,
		}
		if changed {
			for _, ev := range s.sampler.lastEvents(in.Serial) {
				// Attention mode never reports progress steps, even when
				// the same tick bumped both counters.
				if mode == "attention" && ev.Kind == kindProgressStep {
					continue
				}
				out.Events = append(out.Events, ev)
			}
		}
		return shortText(nil, fmt.Sprintf("%s: changed=%v revision=%s",
			in.Serial, changed, out.Revision)), out, nil
	}

	// An empty after_revision is the initial call: return the current
	// snapshot and let the client replay the revision on the next poll.
	if in.AfterRevision == "" {
		return answer(false, false)
	}
	afterEpoch, after, ok := decodeRevision(in.AfterRevision)
	if !ok || afterEpoch != s.epoch {
		// Unknown token: another server, a restart, or corruption. No
		// lossless history is claimed; the client resyncs from a snapshot.
		return answer(false, true)
	}
	// An attention change expires the token in both modes, because
	// progress mode is a superset of attention. Progress mode additionally
	// expires on progress-only movement, which attention mode ignores.
	expired := after.attention != cur.attention ||
		(mode == "progress" && after.progress != cur.progress)
	if expired {
		return answer(false, true)
	}
	if timeout == 0 {
		return answer(false, false)
	}
	parkCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	select {
	case <-wake:
		return answer(true, false)
	case <-parkCtx.Done():
		return answer(false, false)
	}
}
