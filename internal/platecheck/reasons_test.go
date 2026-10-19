package platecheck

import "testing"

// TestReasonCatalogCoversEveryCode checks that every code the package can
// record has a specific clause and that an unknown code gets the generic
// fallback instead of a real reason.
func TestReasonCatalogCoversEveryCode(t *testing.T) {
	// codes is the union of authorize/layerEvidence reasons, skip sites,
	// errorCategory outputs, and the fixed outcome codes.
	codes := []string{
		"print_past_start", "layer_not_reported", "first_layer_not_complete",
		"print_ended", "print_paused", "printer_offline", "start_not_observed",
		"startup_stopped", "camera_disabled", "check_timeout", "capture_timeout",
		"camera_unavailable", "unsupported_camera", "invalid_image",
		"image_too_large", "image_too_small", "auth_rejected", "endpoint_unreachable", "timeout",
		"rate_limited", "bad_response", "model_error", "canceled",
	}
	// Unknown codes share one fallback, and no recordable code may use it;
	// otherwise a missing or duplicated catalog case becomes a real reason.
	fallback := reasonClause("no_such_code")
	if fallback == "" {
		t.Fatal("unknown code has no fallback clause")
	}
	if got := reasonClause("another_no_such_code"); got != fallback {
		t.Fatalf("unknown codes do not share the fallback: %q != %q", got, fallback)
	}
	seen := make(map[string]string, len(codes))
	for _, code := range codes {
		clause := reasonClause(code)
		if clause == "" || clause == fallback {
			t.Errorf("reasonClause(%q) = %q, want a specific clause that is not the generic fallback", code, clause)
			continue
		}
		if prev, ok := seen[clause]; ok {
			t.Errorf("reasonClause(%q) and reasonClause(%q) both return %q", prev, code, clause)
		}
		seen[clause] = code
	}
}
