// HTTP cache handlers for the job preview: one route serving the accepted.
package jobpreview

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
)

// previewRoute is the versioned image path shared with stampedResult:.
const previewRoute = "GET /camera/{serial}/preview"

// handlePreview serves the cached PNG for one serial and version. The.
func (s *Service) handlePreview(w http.ResponseWriter, r *http.Request) {
	serial := r.PathValue("serial")
	res, ok := s.Lookup(serial, true)
	if !ok || res.Preview.Status != StatusReady || len(res.PNG) == 0 {
		http.NotFound(w, r)
		return
	}
	sum := sha256.Sum256(res.PNG)
	if r.URL.Query().Get("v") != hex.EncodeToString(sum[:]) {
		http.NotFound(w, r)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "image/png")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(res.PNG)
}
