// HTTP route tests for the cached preview PNG. Every response must come
// from the cache alone: the fetch function is wired to fail the test on
// invocation, so any 404 path that triggered network work would be caught.
package jobpreview

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/telemetry"
)

// publishReady publishes a ready entry through the production path:
// admission marking plus finish, so route tests exercise the real stamps.
func publishReady(t *testing.T, svc *Service, serial string, job telemetry.JobView, png []byte, md *Metadata) {
	t.Helper()
	adm := &admission{
		printer:  config.Printer{Serial: serial},
		job:      job,
		gen:      job.Generation,
		rev:      job.Revision,
		epoch:    job.RunningEpoch,
		upstream: 4,
	}
	svc.begin(serial, adm)
	svc.finish(adm, Result{PNG: png, Metadata: md}, nil)
}

func servePreview(t *testing.T, mux *http.ServeMux, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func mountRoute(t *testing.T, mux *http.ServeMux, svc *Service) {
	t.Helper()
	if err := module.Check([]module.Module{svc.Module()}); err != nil {
		t.Fatalf("module Check: %v", err)
	}
	module.Mount(mux, []module.Module{svc.Module()})
}

func TestPreviewRouteServesVersionedPNG(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := settledJob(base, 7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		return Result{}, nil
	}

	png := pngBytes(t, 3, 4)
	md := &Metadata{Source: sourcePrinter3mf, Title: strPtr("Title")}
	publishReady(t, svc, "S1", job, png, md)
	sum := sha256.Sum256(png)
	v := hex.EncodeToString(sum[:])

	mux := http.NewServeMux()
	mountRoute(t, mux, svc)

	// The published image URL matches the route exactly.
	res, ok := svc.Lookup("S1", false)
	if !ok || res.Preview.ImageURL == nil || *res.Preview.ImageURL != previewURLPrefix+"S1/preview?v="+v {
		t.Fatalf("image_url = %v, want versioned preview URL", res.Preview.ImageURL)
	}

	rec := servePreview(t, mux, http.MethodGet, "/camera/S1/preview?v="+v)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	h := rec.Header()
	if got := h.Get("Content-Type"); got != "image/png" {
		t.Fatalf("content-type = %q, want image/png", got)
	}
	if got := h.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("x-content-type-options = %q, want nosniff", got)
	}
	if got := h.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("cache-control = %q, want no-store", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), png) {
		t.Fatal("served bytes differ from the cached PNG")
	}

	// Misses: wrong version, missing version, unknown serial.
	for _, tc := range []struct {
		name, target string
	}{
		{"wrong version", "/camera/S1/preview?v=deadbeef"},
		{"missing version", "/camera/S1/preview"},
		{"empty version", "/camera/S1/preview?v="},
		{"unknown serial", "/camera/OTHER/preview?v=" + v},
	} {
		if rec := servePreview(t, mux, http.MethodGet, tc.target); rec.Code != http.StatusNotFound {
			t.Errorf("%s: code = %d, want 404", tc.name, rec.Code)
		}
	}
	// Method guard: only GET is routed.
	if rec := servePreview(t, mux, http.MethodPost, "/camera/S1/preview?v="+v); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST code = %d, want 405", rec.Code)
	}
	if counts.count() != 0 {
		t.Fatalf("calls = %d, want 0: reads must never perform network work", counts.count())
	}
}

func TestPreviewRouteStaleVersionAfterJobSwitch(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job7 := settledJob(base, 7)
	rd := newJobReader("S1", job7)
	svc := newTestService(t, conn, rd, clk, "S1")

	pngA := pngBytes(t, 3, 4)
	publishReady(t, svc, "S1", job7, pngA, nil)
	sumA := sha256.Sum256(pngA)
	vA := hex.EncodeToString(sumA[:])

	// A new generation settles and its attempt returns a different image.
	pngB := pngBytes(t, 5, 2)
	started := make(chan struct{}, 4)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		started <- struct{}{}
		return Result{PNG: pngB}, nil
	}
	j8 := settledJob(base, 8)
	rd.set("S1", j8)
	svc.step(base)
	await(t, started)
	svc.wg.Wait()
	sumB := sha256.Sum256(pngB)
	vB := hex.EncodeToString(sumB[:])

	mux := http.NewServeMux()
	mountRoute(t, mux, svc)
	if rec := servePreview(t, mux, http.MethodGet, "/camera/S1/preview?v="+vA); rec.Code != http.StatusNotFound {
		t.Fatalf("old version code = %d, want 404 after job switch", rec.Code)
	}
	rec := servePreview(t, mux, http.MethodGet, "/camera/S1/preview?v="+vB)
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), pngB) {
		t.Fatalf("new version = %d, want 200 with the new PNG", rec.Code)
	}
}

func TestPreviewRouteMetadataOnlyHasNoImage(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := settledJob(base, 7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		return Result{}, nil
	}

	adm := &admission{
		printer: config.Printer{Serial: "S1"}, job: job, gen: 7, rev: 1, epoch: 2, upstream: 4,
	}
	svc.begin("S1", adm)
	svc.finish(adm,
		Result{Metadata: &Metadata{Source: sourcePrinter3mf, Title: strPtr("Title")}},
		archiveErr(catImageMissing, "plate image missing"))

	mux := http.NewServeMux()
	mountRoute(t, mux, svc)
	rec := servePreview(t, mux, http.MethodGet, "/camera/S1/preview?v=anything")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("metadata-only code = %d, want 404", rec.Code)
	}
	if counts.count() != 0 {
		t.Fatalf("calls = %d, want 0", counts.count())
	}
}
