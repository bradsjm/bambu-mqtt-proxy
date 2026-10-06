package platecheck

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// snapshotImage carries the exact evaluated JPEG and its validated dimensions.
type snapshotImage struct {
	// Type fixes the response media type.
	Type string `json:"type"`
	// Data is base64 of the exact bytes uploaded to Clef.
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

// snapshotResponse is the single-response dry-run contract.
type snapshotResponse struct {
	// DryRun confirms this path sends no printer commands.
	DryRun bool `json:"dry_run"`
	// Model is the submitted effective model.
	Model string `json:"model"`
	// Cutoff is the normalized submitted cutoff.
	Cutoff float64 `json:"cutoff"`
	// AssessableMin is the fixed local quality gate.
	AssessableMin float64 `json:"assessable_min"`
	// Cameras is available or none.
	Cameras string `json:"cameras"`
	// Printers preserves configured order.
	Printers []snapshotRow `json:"printers"`
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
	ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), operationKey{}, operation), 25*time.Second*time.Duration(len(s.printers)))
	defer cancel()
	log := s.log.With("operation", operation)
	log.InfoContext(ctx, "Plate-check snapshot test started", "model", settings.Model, "cutoff", settings.StopConfidence, "printers", len(s.printers))
	out := snapshotResponse{DryRun: true, Model: settings.Model, Cutoff: settings.StopConfidence, AssessableMin: assessableMin, Cameras: "none", Printers: make([]snapshotRow, 0, len(s.printers))}
	for _, p := range s.printers {
		if supportedCamera(p) {
			out.Cameras = "available"
			break
		}
	}
	checked, failed, stops := 0, 0, 0
	if out.Cameras == "available" {
		client := s.newClient(settings, log.With("component", "clef_client"))
		for _, p := range s.printers {
			if ctx.Err() != nil {
				break
			}
			row := snapshotRow{Serial: p.Serial, Name: p.Name, Model: config.DisplayModel(p.Model, p.Serial), Decision: "error"}
			var frame Frame
			var captureErr error
			if !supportedCamera(p) {
				captureErr = &safeError{code: "unsupported_camera"}
			} else {
				captureCtx, captureCancel := context.WithTimeout(ctx, captureTimeout)
				frame, captureErr = s.frames.Capture(captureCtx, p.Serial, started)
				if captureErr != nil && captureCtx.Err() == context.DeadlineExceeded {
					captureErr = &safeError{code: "capture_timeout"}
				}
				captureCancel()
			}
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
					row.Decision = classify(result, settings.StopConfidence)
					row.PClear = new(result.PClear)
					row.POccupied = new(result.POccupied)
					row.PAssessable = new(result.PAssessable)
					checked++
					if row.Decision == "would_stop" {
						stops++
					}
				}
			}
			if row.Decision == "error" {
				failed++
			}
			attrs := []any{"serial", p.Serial, "decision", row.Decision, "cutoff", settings.StopConfidence, "error_code", row.ErrorCode}
			if row.PClear != nil {
				attrs = append(attrs, "p_clear", *row.PClear, "p_occupied", *row.POccupied, "p_assessable", *row.PAssessable)
			}
			log.InfoContext(ctx, "Plate-check snapshot evaluated", attrs...)
			if log.Enabled(ctx, slog.LevelDebug) {
				width, height := 0, 0
				if row.Image != nil {
					width, height = row.Image.Width, row.Image.Height
				}
				log.DebugContext(ctx, "Plate-check snapshot metadata", "serial", p.Serial, "printer_model", row.Model, "model", settings.Model, "frame_sequence", row.FrameSequence, "captured_at", row.CapturedAt, "width", width, "height", height, "image_bytes", row.ImageBytes, "error_code", row.ErrorCode)
			}
			out.Printers = append(out.Printers, row)
		}
	}
	outcome := "completed"
	if ctx.Err() != nil {
		outcome = "canceled"
	}
	log.InfoContext(ctx, "Plate-check snapshot test ended", "outcome", outcome, "cameras", out.Cameras, "checked", checked, "failed", failed, "would_stop", stops, "duration_ms", s.now().Sub(started).Milliseconds())
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// writeJSONError renders one safe operational error as uncached JSON.
func writeJSONError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
