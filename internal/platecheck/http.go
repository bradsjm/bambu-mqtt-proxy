package platecheck

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// snapshotCaptureTimeout bounds diagnostic capture independently of automatic checks.
const snapshotCaptureTimeout = 5 * time.Second

// snapshotImage carries the exact evaluated JPEG and its validated dimensions.
type snapshotImage struct {
	// Type fixes the response media type.
	Type string `json:"type"`
	// Data is base64 of the exact bytes uploaded to the provider.
	Data string `json:"data"`
	// Width is the JPEG width.
	Width int `json:"width"`
	// Height is the JPEG height.
	Height int `json:"height"`
}

// snapshotRow is one printer's independent diagnostic result.
type snapshotRow struct {
	// Serial is the configured printer routing identity.
	Serial string `json:"serial"`
	// Name is the configured display name.
	Name string `json:"name"`
	// Model is the configured or inferred printer model.
	Model string `json:"model"`
	// Decision is the server-computed policy outcome.
	Decision string `json:"decision"`
	// PClear is absent as a null value when evaluation failed.
	PClear *float64 `json:"p_clear"`
	// POccupied is absent as a null value when evaluation failed.
	POccupied *float64 `json:"p_occupied"`
	// PAssessable is absent as a null value when evaluation failed.
	PAssessable *float64 `json:"p_assessable"`
	// CapturedAt is the exact capture timestamp, or null after failed capture.
	CapturedAt *time.Time `json:"captured_at"`
	// FrameSequence identifies the shared capture frame.
	FrameSequence uint64 `json:"frame_sequence"`
	// ImageBytes is the original JPEG byte count.
	ImageBytes int `json:"image_bytes"`
	// Image remains present after an inference error, but not a capture error.
	Image *snapshotImage `json:"image"`
	// ErrorCode is a fixed safe category.
	ErrorCode string `json:"error_code"`
	// Error is a fixed safe sentence.
	Error string `json:"error"`
}

// snapshotPrinter identifies one configured printer and its initial diagnostic status.
type snapshotPrinter struct {
	// Serial is the configured printer routing identity.
	Serial string `json:"serial"`
	// Name is the configured display name.
	Name string `json:"name"`
	// Model is the configured or inferred printer model.
	Model string `json:"model"`
	// Status is checking, offline, or unsupported.
	Status string `json:"status"`
}

// snapshotStart begins the dry-run stream in configured printer order.
type snapshotStart struct {
	// Type identifies the start event.
	Type string `json:"type"`
	// DryRun confirms this path sends no printer commands.
	DryRun bool `json:"dry_run"`
	// Model is the submitted effective model.
	Model string `json:"model"`
	// Cutoff is the normalized submitted cutoff.
	Cutoff float64 `json:"cutoff"`
	// AssessableMin is the fixed local quality gate.
	AssessableMin float64 `json:"assessable_min"`
	// Printers preserves configured order.
	Printers []snapshotPrinter `json:"printers"`
}

// snapshotResult carries one completed or skipped printer result.
type snapshotResult struct {
	// Type identifies the result event.
	Type string `json:"type"`
	// Printer is the independent diagnostic result.
	Printer snapshotRow `json:"printer"`
}

// snapshotDone terminates the diagnostic stream.
type snapshotDone struct {
	// Type identifies the done event.
	Type string `json:"type"`
	// Outcome distinguishes completed from canceled runs.
	Outcome string `json:"outcome"`
}

// supportedCamera mirrors the current web-camera model set without speculative support.
func supportedCamera(p config.Printer) bool {
	switch config.DisplayModel(p.Model, p.Serial) {
	case "P1P", "P1S", "A1", "A1MINI", "X1", "X1C", "X1E", "P2S", "H2S", "H2D":
		return true
	default:
		return false
	}
}

// handleSnapshots evaluates current configured cameras without touching automatic state or controls.
func (s *Service) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
	if err != nil || len(raw) == 0 {
		writeJSONError(w, 400, "The plate-check settings could not be read.")
		return
	}
	in, err := decodeSettingsView(raw)
	if err != nil {
		writeJSONError(w, 400, err.Error())
		return
	}
	settings, err := resolveTestSettings(in, s.settings)
	if err != nil {
		writeJSONError(w, 422, err.Error())
		return
	}
	started := s.now()
	operation := operations.Add(1)
	ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), operationKey{}, operation), snapshotCaptureTimeout+requestTimeout)
	defer cancel()
	log := s.log.With("operation", operation)
	log.InfoContext(ctx, "Plate-check snapshot test started", "model", settings.Model, "cutoff", settings.StopConfidence, "printers", len(s.printers))
	start := snapshotStart{Type: "start", DryRun: true, Model: settings.Model, Cutoff: settings.StopConfidence, AssessableMin: assessableMin, Printers: make([]snapshotPrinter, 0, len(s.printers))}
	online := make([]config.Printer, 0, len(s.printers))
	skippedRows := make([]snapshotRow, 0, len(s.printers))
	for _, p := range s.printers {
		printer := snapshotPrinter{Serial: p.Serial, Name: p.Name, Model: config.DisplayModel(p.Model, p.Serial), Status: "checking"}
		code := ""
		if !supportedCamera(p) {
			printer.Status, code = "unsupported", "unsupported_camera"
		} else if !s.commands.Connected(p.Serial) {
			printer.Status, code = "offline", "printer_offline"
		}
		start.Printers = append(start.Printers, printer)
		if code != "" {
			skippedRows = append(skippedRows, snapshotRow{Serial: p.Serial, Name: p.Name, Model: printer.Model, Decision: "skipped", ErrorCode: code, Error: fixedSentence(code)})
		} else {
			online = append(online, p)
		}
	}
	rows := make(chan snapshotRow, len(s.printers))
	var wg sync.WaitGroup
	if len(online) > 0 {
		client := s.newClient(settings, log.With("component", "vision_client"))
		for _, p := range online {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rows <- s.evaluateSnapshot(ctx, client, p, started, settings.StopConfidence)
			}()
		}
	}
	go func() {
		wg.Wait()
		close(rows)
	}()
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	writable := true
	emit := func(event any) {
		if !writable {
			return
		}
		if err := enc.Encode(event); err != nil {
			writable = false
			cancel()
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
	emit(start)
	checked, failed, skipped, stops := 0, 0, 0, 0
	emitRow := func(row snapshotRow) {
		switch row.Decision {
		case "error":
			failed++
		case "skipped":
			skipped++
		default:
			checked++
			if row.Decision == "would_stop" {
				stops++
			}
		}
		attrs := []any{"serial", row.Serial, "decision", row.Decision, "cutoff", settings.StopConfidence, "error_code", row.ErrorCode}
		if row.PClear != nil {
			attrs = append(attrs, "p_clear", *row.PClear, "p_occupied", *row.POccupied, "p_assessable", *row.PAssessable)
		}
		log.InfoContext(ctx, "Plate-check snapshot evaluated", attrs...)
		if log.Enabled(ctx, slog.LevelDebug) {
			width, height := 0, 0
			if row.Image != nil {
				width, height = row.Image.Width, row.Image.Height
			}
			log.DebugContext(ctx, "Plate-check snapshot metadata", "serial", row.Serial, "printer_model", row.Model, "model", settings.Model, "frame_sequence", row.FrameSequence, "captured_at", row.CapturedAt, "width", width, "height", height, "image_bytes", row.ImageBytes, "error_code", row.ErrorCode)
		}
		emit(snapshotResult{Type: "result", Printer: row})
	}
	for _, row := range skippedRows {
		emitRow(row)
	}
	for row := range rows {
		emitRow(row)
	}
	outcome := "completed"
	if ctx.Err() != nil {
		outcome = "canceled"
	}
	emit(snapshotDone{Type: "done", Outcome: outcome})
	log.InfoContext(ctx, "Plate-check snapshot test ended", "outcome", outcome, "skipped", skipped, "checked", checked, "failed", failed, "would_stop", stops, "duration_ms", s.now().Sub(started).Milliseconds())
}

// evaluateSnapshot captures and evaluates one online printer and returns its result row.
func (s *Service) evaluateSnapshot(ctx context.Context, client DecisionClient, p config.Printer, started time.Time, cutoff float64) snapshotRow {
	row := snapshotRow{Serial: p.Serial, Name: p.Name, Model: config.DisplayModel(p.Model, p.Serial), Decision: "error"}
	captureCtx, captureCancel := context.WithTimeout(ctx, snapshotCaptureTimeout)
	frame, captureErr := s.frames.Capture(captureCtx, p.Serial, started)
	if captureErr != nil && captureCtx.Err() == context.DeadlineExceeded {
		captureErr = &safeError{code: "capture_timeout"}
	}
	captureCancel()
	if captureErr == nil && !frame.Captured.After(started) {
		captureErr = &safeError{code: "capture_timeout"}
	}
	if captureErr == nil {
		width, height, imageErr := imageDimensions(frame.JPEG)
		if imageErr != nil {
			captureErr = imageErr
		} else {
			row.CapturedAt = new(frame.Captured.UTC())
			row.FrameSequence = frame.Seq
			row.ImageBytes = len(frame.JPEG)
			row.Image = &snapshotImage{Type: "image/jpeg", Data: base64.StdEncoding.EncodeToString(frame.JPEG), Width: width, Height: height}
		}
	}
	if captureErr != nil {
		row.ErrorCode = errorCategory(captureErr)
		row.Error = fixedSentence(row.ErrorCode)
	} else if ctx.Err() != nil {
		row.ErrorCode = errorCategory(ctx.Err())
		row.Error = fixedSentence(row.ErrorCode)
	} else {
		evaluationCtx, evaluationCancel := context.WithTimeout(ctx, requestTimeout)
		result, evaluationErr := client.Evaluate(evaluationCtx, frame.JPEG, row.Model)
		evaluationCancel()
		if evaluationErr == nil && !validResult(result) {
			evaluationErr = &safeError{code: "bad_response"}
		}
		if evaluationErr != nil {
			row.ErrorCode = errorCategory(evaluationErr)
			row.Error = fixedSentence(row.ErrorCode)
		} else {
			row.Decision = classify(result, cutoff)
			row.PClear = new(result.PClear)
			row.POccupied = new(result.POccupied)
			row.PAssessable = new(result.PAssessable)
		}
	}
	return row
}

// writeJSONError renders one safe operational error as uncached JSON.
func writeJSONError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
