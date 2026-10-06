// HTTP cache handlers for the job preview: one route serving the accepted
// plate PNG from memory. Every read is cache-only — a handler never dials
// the printer, retries a transfer, or triggers any scheduler work.
package jobpreview

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
)

// previewRoute is the versioned image path shared with stampedResult:
// GET /camera/{serial}/preview?v=<sha256-hex of the PNG bytes>.
const previewRoute = "GET /camera/{serial}/preview"

// handlePreview serves the cached PNG for one serial and version. The
// version must equal the SHA-256 of the currently cached image: unknown
// serials, unavailable images, and stale versions all return 404 with no
// network work and no fallback to an older image.
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
