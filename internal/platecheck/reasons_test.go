package platecheck

import (
	"strings"
	"testing"
)

// TestReasonCatalogCoversEveryCode pins the activity and panel text for
// every skip and failure code in both states. An unknown code is a
// programming error: reasonClause falls back to a generic clause, and any
// code used in the package that this table lacks fails the coverage leg.
func TestReasonCatalogCoversEveryCode(t *testing.T) {
	clauses := map[string]string{
		"print_past_start":         "printing had already started",
		"layer_not_reported":       "printer did not report its layer",
		"first_layer_not_complete": "printer reports the first layer is not complete",
		"print_ended":              "print ended",
		"print_paused":             "print paused",
		"printer_offline":          "printer offline",
		"start_not_observed":       "proxy started after this print began",
		"startup_stopped":          "plate check stopped this print",
		"camera_disabled":          "camera is disabled in configuration",
		"check_timeout":            "check took too long",
		"capture_timeout":          "no new camera image arrived",
		"camera_unavailable":       "camera unavailable",
		"unsupported_camera":       "printer camera not supported",
		"invalid_image":            "camera image was not valid",
		"image_too_large":          "camera image too large",
		"auth_rejected":            "AI provider rejected the API key",
		"endpoint_unreachable":     "AI provider unreachable",
		"timeout":                  "AI provider timed out",
		"rate_limited":             "AI provider rate-limited the request",
		"bad_response":             "AI provider returned an invalid answer",
		"model_error":              "AI provider could not evaluate the image",
		"canceled":                 "proxy stopping",
	}
	for code, clause := range clauses {
		if got := reasonClause(code); got != clause {
			t.Errorf("reasonClause(%q) = %q, want %q", code, got, clause)
		}
	}
	if got := reasonClause("no_such_code"); got != "check could not run" {
		t.Errorf("reasonClause(unknown) = %q, want the generic clause", got)
	}

	sentences := map[string]struct{ plate, layer string }{
		"print_ended":  {"Plate check cancelled, print ended", "First-layer check cancelled, print ended"},
		"print_paused": {"Plate check skipped, print paused", "First-layer check skipped, print paused"},
		"canceled":     {"Plate check cancelled, proxy stopping", "First-layer check cancelled, proxy stopping"},
		"startup_stopped": {
			"Plate check skipped, plate check stopped this print",
			"First-layer check skipped, plate check stopped this print",
		},
		"printer_offline": {
			"Plate check skipped, printer offline, no stop sent",
			"First-layer check skipped, printer offline, no pause sent",
		},
	}
	for code := range clauses {
		want, special := sentences[code]
		plateSkipped, layerSkipped := activitySentence("Plate check", "skipped", code), activitySentence("First-layer check", "skipped", code)
		plateError, layerError := activitySentence("Plate check", "error", code), activitySentence("First-layer check", "error", code)
		if special {
			if plateSkipped != want.plate || plateError != want.plate {
				t.Errorf("activitySentence(plate, %q) = %q/%q, want %q", code, plateSkipped, plateError, want.plate)
			}
			if layerSkipped != want.layer || layerError != want.layer {
				t.Errorf("activitySentence(layer, %q) = %q/%q, want %q", code, layerSkipped, layerError, want.layer)
			}
			continue
		}
		if want := "Plate check skipped, " + clauses[code] + ", continuing print"; plateSkipped != want {
			t.Errorf("activitySentence(plate skipped, %q) = %q, want %q", code, plateSkipped, want)
		}
		if want := "Plate check failed, " + clauses[code] + ", continuing print"; plateError != want {
			t.Errorf("activitySentence(plate error, %q) = %q, want %q", code, plateError, want)
		}
		if want := "First-layer check skipped, " + clauses[code] + ", continuing print"; layerSkipped != want {
			t.Errorf("activitySentence(layer skipped, %q) = %q, want %q", code, layerSkipped, want)
		}
		if want := "First-layer check failed, " + clauses[code] + ", continuing print"; layerError != want {
			t.Errorf("activitySentence(layer error, %q) = %q, want %q", code, layerError, want)
		}
	}

	notes := map[string]struct{ plate, layer string }{
		"print_ended":        {"Print ended", "Print ended"},
		"print_paused":       {"Print paused", "Print paused"},
		"canceled":           {"Proxy stopping", "Proxy stopping"},
		"printer_offline":    {"Printer offline; no stop sent", "Printer offline; no pause sent"},
		"start_not_observed": {"Proxy started after this print began", "Proxy started after this print began"},
		"startup_stopped":    {"Plate check stopped this print", "Plate check stopped this print"},
		"camera_disabled":    {"Camera is disabled in configuration", "Camera is disabled in configuration"},
	}
	for code := range clauses {
		plate, layer := reasonNote("Plate check", code), reasonNote("First-layer check", code)
		if want, special := notes[code]; special {
			if plate != want.plate || layer != want.layer {
				t.Errorf("reasonNote(%q) = %q/%q, want %q/%q", code, plate, layer, want.plate, want.layer)
			}
			continue
		}
		clause := clauses[code]
		want := string(clause[0]-('a'-'A')) + clause[1:] + "; printing continued"
		if clause[0] >= 'A' && clause[0] <= 'Z' {
			want = clause + "; printing continued"
		}
		if plate != want || layer != want {
			t.Errorf("reasonNote(%q) = %q/%q, want %q", code, plate, layer, want)
		}
	}

	// Every code the package can record must appear in the table above.
	// The list is the union of authorize/layerEvidence reasons, skip
	// sites, errorCategory outputs, and the fixed outcome codes.
	for _, code := range []string{
		"print_past_start", "layer_not_reported", "first_layer_not_complete",
		"print_ended", "print_paused", "printer_offline", "start_not_observed",
		"startup_stopped", "camera_disabled", "check_timeout", "capture_timeout",
		"camera_unavailable", "unsupported_camera", "invalid_image",
		"image_too_large", "auth_rejected", "endpoint_unreachable", "timeout",
		"rate_limited", "bad_response", "model_error", "canceled",
	} {
		if _, ok := clauses[code]; !ok {
			t.Errorf("code %q used by the package is missing from the catalog table", code)
		}
	}
	for _, code := range []string{"auth_rejected", "endpoint_unreachable", "timeout", "rate_limited", "bad_response", "model_error", "invalid_image", "image_too_large", "canceled", "printer_offline", "unsupported_camera", "camera_unavailable", "capture_timeout"} {
		if !strings.Contains(fixedSentence(code), " ") {
			t.Errorf("fixedSentence(%q) lost its /config sentence", code)
		}
	}
	// The second startup pass names its check "Plate check after homing"
	// and, like pass 1, sends a stop rather than a pause.
	for code := range clauses {
		plate, homing := activitySentence("Plate check", "skipped", code), activitySentence("Plate check after homing", "skipped", code)
		if want := "Plate check after homing" + plate[len("Plate check"):]; homing != want {
			t.Errorf("activitySentence(homing, %q) = %q, want %q", code, homing, want)
		}
		if plateNote, homingNote := reasonNote("Plate check", code), reasonNote("Plate check after homing", code); homingNote != plateNote {
			t.Errorf("reasonNote(homing, %q) = %q, want the pass 1 note %q", code, homingNote, plateNote)
		}
	}
	if got := activitySentence("Plate check after homing", "skipped", "printer_offline"); got != "Plate check after homing skipped, printer offline, no stop sent" {
		t.Errorf("homing printer_offline = %q", got)
	}
	if got := reasonNote("Plate check after homing", "printer_offline"); got != "Printer offline; no stop sent" {
		t.Errorf("homing printer_offline note = %q", got)
	}
}
