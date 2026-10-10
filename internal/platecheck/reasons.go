package platecheck

// reasonClause maps an internal skip or failure code to its plain-English
// clause. An unknown code is a programming error: the table test fails for
// any code used in the package that this table lacks, and the sentence
// falls back to a generic clause.
func reasonClause(code string) string {
	switch code {
	case "print_past_start":
		return "printing had already started"
	case "layer_not_reported":
		return "printer did not report its layer"
	case "first_layer_not_complete":
		return "printer reports the first layer is not complete"
	case "print_ended":
		return "print ended"
	case "print_paused":
		return "print paused"
	case "printer_offline":
		return "printer offline"
	case "start_not_observed":
		return "proxy started after this print began"
	case "startup_stopped":
		return "plate check stopped this print"
	case "camera_disabled":
		return "camera is disabled in configuration"
	case "check_timeout":
		return "check took too long"
	case "capture_timeout":
		return "no new camera image arrived"
	case "camera_unavailable":
		return "camera unavailable"
	case "unsupported_camera":
		return "printer camera not supported"
	case "invalid_image":
		return "camera image was not valid"
	case "image_too_large":
		return "camera image too large"
	case "auth_rejected":
		return "AI provider rejected the API key"
	case "endpoint_unreachable":
		return "AI provider unreachable"
	case "timeout":
		return "AI provider timed out"
	case "rate_limited":
		return "AI provider rate-limited the request"
	case "bad_response":
		return "AI provider returned an invalid answer"
	case "model_error":
		return "AI provider could not evaluate the image"
	case "canceled":
		return "proxy stopping"
	default:
		return "check could not run"
	}
}

// activitySentence renders the activity-log sentence for a skipped or failed
// check. Check is "Plate check" or "First-layer check", and state is
// "skipped" or "error".
func activitySentence(check, state, code string) string {
	switch code {
	case "print_ended":
		return check + " cancelled, print ended"
	case "print_paused":
		return check + " skipped, print paused"
	case "canceled":
		return check + " cancelled, proxy stopping"
	case "startup_stopped":
		return check + " skipped, plate check stopped this print"
	case "printer_offline":
		if check == "Plate check" {
			return check + " skipped, printer offline, no stop sent"
		}
		return check + " skipped, printer offline, no pause sent"
	}
	if state == "error" {
		return check + " failed, " + reasonClause(code) + ", continuing print"
	}
	return check + " skipped, " + reasonClause(code) + ", continuing print"
}

// reasonNote renders the camera-wall note for a skipped or failed check.
// Check is "Plate check" or "First-layer check".
func reasonNote(check, code string) string {
	switch code {
	case "print_ended":
		return "Print ended"
	case "print_paused":
		return "Print paused"
	case "canceled":
		return "Proxy stopping"
	case "printer_offline":
		if check == "Plate check" {
			return "Printer offline; no stop sent"
		}
		return "Printer offline; no pause sent"
	case "start_not_observed":
		return "Proxy started after this print began"
	case "startup_stopped":
		return "Plate check stopped this print"
	case "camera_disabled":
		return "Camera is disabled in configuration"
	default:
		clause := reasonClause(code)
		if c := clause[0]; c >= 'a' && c <= 'z' {
			clause = string(c-('a'-'A')) + clause[1:]
		}
		return clause + "; printing continued"
	}
}
