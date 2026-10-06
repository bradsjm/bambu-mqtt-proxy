package detection

import (
	"encoding/json"
	"errors"
	"net/http"
)

// detectionPutMaxBody bounds the per-print toggle request body.
const detectionPutMaxBody = 4 << 10

// handlePut applies the per-print AI detection toggle.
// Disabling requires the current print token; stale tokens or no print answer 409.
func (e *Engine) handlePut(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	var in struct {
		Enabled   *bool  `json:"enabled"`
		SessionID string `json:"session_id"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, req.Body, detectionPutMaxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil || in.Enabled == nil {
		writeJSONError(w, http.StatusBadRequest, "body must be a JSON object with an enabled boolean")
		return
	}
	st, err := e.SetDetectionEnabled(req.PathValue("serial"), *in.Enabled, in.SessionID)
	if err != nil {
		var code int
		switch {
		case errors.Is(err, ErrUnknownPrinter):
			code = http.StatusNotFound
		case errors.Is(err, ErrNoPrintSession), errors.Is(err, ErrStaleSession), errors.Is(err, ErrDetectionUnavailable):
			code = http.StatusConflict
		default:
			code = http.StatusInternalServerError
		}
		writeJSONError(w, code, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

// writeJSONError renders one operational error as a JSON object.
func writeJSONError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
