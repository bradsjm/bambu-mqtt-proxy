// Scheduler and cache tests. Timing is driven by step(now) with injected
// clocks and job readers; fetch calls synchronize through channels and the
// service WaitGroup, so no test sleeps for settle timing. Freshness stays
// meaningful: jobs built on the real telemetry cache use real report
// timestamps and real step times.
package jobpreview

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/textproto"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printerview"
	"bambu-mqtt-proxy/internal/telemetry"
	"bambu-mqtt-proxy/internal/upstream"
)

// testClock is the injected scheduler clock.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Set moves the clock to an absolute instant. Every step(at) must keep the
// injected clock aligned with at: the pre-dial and publish gates re-read
// s.now(), and a stale clock would legitimately cancel the attempt.
func (c *testClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

// fakeConn is an injectable Connectivity source. Generations default to 4,
// matching settledJob's StateGen/ObsGen so fixtures stay eligible.
type fakeConn struct {
	mu     sync.Mutex
	status map[string]bool
	gens   map[string]uint64
}

func newFakeConn(serials ...string) *fakeConn {
	c := &fakeConn{status: make(map[string]bool), gens: make(map[string]uint64)}
	for _, s := range serials {
		c.status[s] = true
		c.gens[s] = 4
	}
	return c
}

func (c *fakeConn) Status() map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]bool, len(c.status))
	for k, v := range c.status {
		out[k] = v
	}
	return out
}

func (c *fakeConn) Generation(serial string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gens[serial]
}

func (c *fakeConn) setConnected(serial string, up bool) {
	c.mu.Lock()
	c.status[serial] = up
	c.mu.Unlock()
}

func (c *fakeConn) bump(serial string) {
	c.mu.Lock()
	c.gens[serial]++
	c.mu.Unlock()
}

// jobReader stands in for the telemetry cache in scheduler tests. A
// scripted seq serves views one readJob call at a time before falling back
// to the map, which pins Lookup's double sampling.
type jobReader struct {
	mu   sync.Mutex
	jobs map[string]telemetry.JobView
	seq  []telemetry.JobView
}

func newJobReader(serial string, job telemetry.JobView) *jobReader {
	return &jobReader{jobs: map[string]telemetry.JobView{serial: job}}
}

func (r *jobReader) Job(serial string) (telemetry.JobView, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.seq) > 0 {
		j := r.seq[0]
		r.seq = r.seq[1:]
		return j, true
	}
	j, ok := r.jobs[serial]
	return j, ok
}

func (r *jobReader) set(serial string, job telemetry.JobView) {
	r.mu.Lock()
	r.jobs[serial] = job
	r.mu.Unlock()
}

// fetchCounter records fetch invocations.
type fetchCounter struct {
	mu    sync.Mutex
	calls []string
}

func (f *fetchCounter) add(serial string) {
	f.mu.Lock()
	f.calls = append(f.calls, serial)
	f.mu.Unlock()
}

func (f *fetchCounter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fetchCounter) serials() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// testError carries a fixed category like the transport's failures.
type testError struct{ cat string }

func (e *testError) Error() string    { return "test transfer failure" }
func (e *testError) category() string { return e.cat }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// await receives one event or fails after a generous watchdog. The watchdog
// only bounds deadlocks; it is never part of the timing under test.
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scheduler event")
	}
}

// settledJob returns a RUNNING view settled well past the delay, freshly
// observed relative to now, on upstream generation 4.
func settledJob(now time.Time, gen uint64) telemetry.JobView {
	return telemetry.JobView{
		Generation:   gen,
		Revision:     1,
		RunningEpoch: 2,
		Active:       true,
		State:        "RUNNING",
		Name:         "Benchy",
		GCodeFile:    "benchy_plate_3.3mf",
		ProjectID:    "0",
		TaskID:       "12345",
		PlateIndex:   intPtr(3),
		StateGen:     4,
		ObsGen:       4,
		ObsAt:        now.Add(-2 * time.Second),
		RunningSince: now.Add(-90 * time.Second),
	}
}

// newTestService builds a service over injected readers, clock and
// connectivity. The real telemetry cache is still constructed so the
// production wiring shape is exercised; readJob is replaced per test.
func newTestService(t *testing.T, conn Connectivity, rd *jobReader, clk *testClock, serials ...string) *Service {
	t.Helper()
	printers := make([]config.Printer, len(serials))
	for i, s := range serials {
		printers[i] = config.Printer{Serial: s, Address: s + ".invalid:8883", Username: "u", Password: "p"}
	}
	cache := telemetry.NewCache(slices.Clone(printers), testLogger())
	svc := New(printers, cache, conn, testLogger())
	svc.readJob = rd.Job
	svc.now = clk.Now
	return svc
}

// reserveSlot mirrors step's reservation of the sole transfer slot for
// tests that drive begin or transfer directly; begin refuses to consume
// an attempt without it.
func reserveSlot(svc *Service) {
	svc.mu.Lock()
	svc.worker = true
	svc.mu.Unlock()
}

func TestUpstreamPoolSatisfiesConnectivity(t *testing.T) {
	var c Connectivity = (*upstream.Pool)(nil)
	_ = c
}

func TestEligibleBoundaries(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	mk := func(settle, obsAge time.Duration) telemetry.JobView {
		j := settledJob(base, 7)
		j.RunningSince = base.Add(-settle)
		j.ObsAt = base.Add(-obsAge)
		return j
	}
	cases := []struct {
		name      string
		job       telemetry.JobView
		connected bool
		upstream  uint64
		want      bool
	}{
		{"settled exactly at delay, fresh exactly at window", mk(settleDelay, printerview.FreshnessWindow), true, 4, true},
		{"one second short of settle", mk(settleDelay-time.Second, time.Second), true, 4, false},
		{"zero running since", func() telemetry.JobView { j := mk(time.Hour, time.Second); j.RunningSince = time.Time{}; return j }(), true, 4, false},
		{"obs one nanosecond past window", mk(time.Hour, printerview.FreshnessWindow+time.Nanosecond), true, 4, false},
		{"zero obs", func() telemetry.JobView { j := mk(time.Hour, time.Second); j.ObsAt = time.Time{}; return j }(), true, 4, false},
		{"disconnected", mk(time.Hour, time.Second), false, 4, false},
		{"state gen behind upstream", mk(time.Hour, time.Second), true, 5, false},
		{"obs gen behind upstream", func() telemetry.JobView { j := mk(time.Hour, time.Second); j.ObsGen = 3; return j }(), true, 4, false},
		{"paused state", func() telemetry.JobView { j := mk(time.Hour, time.Second); j.State = "PAUSE"; return j }(), true, 4, false},
		{"inactive", func() telemetry.JobView { j := mk(time.Hour, time.Second); j.Active = false; return j }(), true, 4, false},
		{"name only", func() telemetry.JobView { j := mk(time.Hour, time.Second); j.GCodeFile = ""; return j }(), true, 4, true},
		{"gcode basename only", func() telemetry.JobView { j := mk(time.Hour, time.Second); j.Name = ""; return j }(), true, 4, true},
		{"gcode with directories", func() telemetry.JobView {
			j := mk(time.Hour, time.Second)
			j.Name = ""
			j.GCodeFile = "/cache/job.3mf"
			return j
		}(), true, 4, true},
		{"uppercase extension", func() telemetry.JobView {
			j := mk(time.Hour, time.Second)
			j.Name = ""
			j.GCodeFile = "JOB.3MF"
			return j
		}(), true, 4, true},
		{"no identity at all", func() telemetry.JobView { j := mk(time.Hour, time.Second); j.Name = ""; j.GCodeFile = ""; return j }(), true, 4, false},
		{"gcode not 3mf", func() telemetry.JobView {
			j := mk(time.Hour, time.Second)
			j.Name = ""
			j.GCodeFile = "job.gcode"
			return j
		}(), true, 4, false},
		{"gcode traversal segment", func() telemetry.JobView {
			j := mk(time.Hour, time.Second)
			j.Name = ""
			j.GCodeFile = "a/../b.3mf"
			return j
		}(), true, 4, false},
		{"gcode dot segment", func() telemetry.JobView {
			j := mk(time.Hour, time.Second)
			j.Name = ""
			j.GCodeFile = "./b.3mf"
			return j
		}(), true, 4, false},
		{"gcode control character", func() telemetry.JobView {
			j := mk(time.Hour, time.Second)
			j.Name = ""
			j.GCodeFile = "job\x01.3mf"
			return j
		}(), true, 4, false},
		{"only separators", func() telemetry.JobView { j := mk(time.Hour, time.Second); j.Name = ""; j.GCodeFile = `/\`; return j }(), true, 4, false},
	}
	for _, tc := range cases {
		if got := eligible(tc.job, tc.connected, tc.upstream, base); got != tc.want {
			t.Errorf("%s: eligible = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestStepSettleSweep pins the settle boundary through the scheduler: no
// fetch through 59 seconds of fresh RUNNING, exactly one at 60, and no
// second attempt for the consumed generation afterwards.
func TestStepSettleSweep(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	runStart := base
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")

	counts := &fetchCounter{}
	started := make(chan struct{}, 8)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		return Result{}, &testError{cat: catTransport}
	}

	// Fresh reports keep arriving beside every step, so the 15s freshness
	// window holds while the 60s settle window runs out.
	stepAt := func(offset time.Duration) {
		at := runStart.Add(offset)
		j := settledJob(at, 7)
		j.RunningSince = runStart
		rd.set("S1", j)
		clk.Set(at)
		svc.step(at)
	}
	stepAt(59 * time.Second)
	select {
	case <-started:
		t.Fatal("fetch admitted before 60 seconds of settled RUNNING")
	default:
	}
	if counts.count() != 0 {
		t.Fatalf("calls = %d, want 0 before settle", counts.count())
	}

	stepAt(60 * time.Second)
	await(t, started)
	svc.wg.Wait() // join the transfer goroutine: outcome settled, slot released
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d, want exactly 1 at settle", got)
	}

	stepAt(61 * time.Second)
	stepAt(62 * time.Second)
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d after consumed generation, want 1", got)
	}
}

func TestFinishBeforeSettleZeroCalls(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := settledJob(base, 7)
	job.RunningSince = base.Add(-30 * time.Second) // settling, not settled
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	started := make(chan struct{}, 4)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		return Result{}, nil
	}
	for i := range 5 {
		at := base.Add(time.Duration(i) * time.Second)
		j := job
		j.ObsAt = at.Add(-time.Second)
		rd.set("S1", j)
		svc.step(at)
	}
	select {
	case <-started:
		t.Fatal("fetch admitted while still settling")
	default:
	}
	if counts.count() != 0 {
		t.Fatalf("calls = %d, want 0", counts.count())
	}
}

// TestSortedAdmissionNeverOverlaps proves the sole transfer slot: with S1
// blocked in fetch, further steps never start S2; only after S1 completes
// does the next step admit S2, in sorted serial order.
func TestSortedAdmissionNeverOverlaps(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S2", "S1") // inserted out of order on purpose
	rd := &jobReader{jobs: map[string]telemetry.JobView{
		"S1": settledJob(base, 7),
		"S2": settledJob(base, 9),
	}}
	svc := newTestService(t, conn, rd, clk, "S2", "S1")

	counts := &fetchCounter{}
	s1Started := make(chan struct{}, 4)
	s2Started := make(chan struct{}, 4)
	release := make(chan struct{})
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		if p.Serial == "S1" {
			s1Started <- struct{}{}
			<-release
			return Result{}, errors.New("released")
		}
		s2Started <- struct{}{}
		return Result{}, &testError{cat: catTransport}
	}

	svc.step(base)
	await(t, s1Started)
	// While S1 is in flight, every tick is refused: the sole slot is busy.
	for i := range 5 {
		svc.step(base.Add(time.Duration(i+1) * time.Second))
	}
	if got := counts.serials(); len(got) != 1 || got[0] != "S1" {
		t.Fatalf("serials = %v, want only [S1] while first transfer in flight", got)
	}
	close(release)
	svc.wg.Wait()
	svc.step(base.Add(10 * time.Second))
	await(t, s2Started)
	svc.wg.Wait()
	if got := counts.serials(); !slices.Equal(got, []string{"S1", "S2"}) {
		t.Fatalf("serials = %v, want [S1 S2] in sorted admission order", got)
	}
}

// TestFailureThenReconnectAndRevisionNeverRetry pins attempt consumption:
// a failed attempt survives reconnects and revisions; only a new
// generation grants exactly one more attempt.
func TestFailureThenReconnectAndRevisionNeverRetry(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")

	counts := &fetchCounter{}
	started := make(chan struct{}, 8)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		return Result{}, &testError{cat: catNotFound}
	}
	upstreamGen := func() uint64 { return conn.Generation("S1") }

	// Step with a fresh settled view of the given generation, aligned to
	// the current upstream generation.
	stepSettled := func(gen uint64) {
		at := clk.Now().Add(time.Second)
		j := settledJob(at, gen)
		j.StateGen = upstreamGen()
		j.ObsGen = upstreamGen()
		rd.set("S1", j)
		clk.Advance(time.Second)
		svc.step(at)
		await(t, started)
		svc.wg.Wait()
	}

	stepSettled(7)
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d after first attempt, want 1", got)
	}

	// Reconnect: upstream generation moves and the job reports fresh state
	// on the new connection, same preview generation. No second attempt.
	conn.bump("S1")
	at := clk.Now().Add(time.Second)
	j := settledJob(at, 7)
	j.StateGen = upstreamGen()
	j.ObsGen = upstreamGen()
	rd.set("S1", j)
	clk.Advance(time.Second)
	svc.step(at)
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d after reconnect, want 1", got)
	}

	// Revision bump inside the same generation: no second attempt.
	at = clk.Now().Add(time.Second)
	j = settledJob(at, 7)
	j.Revision = 2
	j.StateGen = upstreamGen()
	j.ObsGen = upstreamGen()
	rd.set("S1", j)
	clk.Advance(time.Second)
	svc.step(at)
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d after revision, want 1", got)
	}

	// New generation: exactly one more attempt, then never again.
	stepSettled(8)
	if got := counts.count(); got != 2 {
		t.Fatalf("calls = %d after new generation, want 2", got)
	}
	at = clk.Now().Add(time.Second)
	j = settledJob(at, 8)
	j.StateGen = upstreamGen()
	j.ObsGen = upstreamGen()
	rd.set("S1", j)
	clk.Advance(time.Second)
	svc.step(at)
	if got := counts.count(); got != 2 {
		t.Fatalf("calls = %d after consumed new generation, want 2", got)
	}
}

// TestFinishPublishesOutcomes drives finish synchronously and pins the
// published view for ready, metadata-only, pure-failure and discarded
// outcomes.
func TestFinishPublishesOutcomes(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	serial := "S1"

	newFixture := func(t *testing.T) (*Service, *fakeConn, *jobReader, *admission) {
		clk := &testClock{now: base}
		conn := newFakeConn(serial)
		job := settledJob(base, 7)
		rd := newJobReader(serial, job)
		svc := newTestService(t, conn, rd, clk, serial)
		adm := &admission{
			printer:  config.Printer{Serial: serial},
			job:      job,
			gen:      7,
			rev:      1,
			epoch:    2,
			upstream: 4,
		}
		reserveSlot(svc)
		return svc, conn, rd, adm
	}

	t.Run("ready result", func(t *testing.T) {
		svc, _, _, adm := newFixture(t)
		png := pngBytes(t, 2, 2)
		md := &Metadata{Source: sourcePrinter3mf, Title: strPtr("Title")}
		svc.finish(adm, Result{PNG: png, Metadata: md}, nil)
		res, ok := svc.Lookup(serial, true)
		if !ok || res.Preview.Status != StatusReady {
			t.Fatalf("Lookup = %+v, %v; want ready", res.Preview, ok)
		}
		sum := sha256.Sum256(png)
		wantURL := previewURLPrefix + serial + "/preview?v=" + hex.EncodeToString(sum[:])
		if res.Preview.ImageURL == nil || *res.Preview.ImageURL != wantURL {
			t.Fatalf("image_url = %v, want %s", res.Preview.ImageURL, wantURL)
		}
		if res.Preview.RetrievedAt == nil {
			t.Fatal("retrieved_at = nil, want stamped")
		}
		if _, err := time.Parse(time.RFC3339Nano, *res.Preview.RetrievedAt); err != nil {
			t.Fatalf("retrieved_at %q does not parse: %v", *res.Preview.RetrievedAt, err)
		}
		if res.Preview.JobName != "Benchy" || !res.Preview.Current {
			t.Fatalf("live projection = %+v, want live name and current", res.Preview)
		}
		if !slices.Equal(res.PNG, png) {
			t.Fatal("PNG bytes differ from published bytes")
		}
		withoutImage, _ := svc.Lookup(serial, false)
		if withoutImage.PNG != nil {
			t.Fatal("includeImage=false must not copy PNG bytes")
		}
		if withoutImage.Preview.ImageURL == nil {
			t.Fatal("image_url must survive includeImage=false")
		}
	})

	t.Run("metadata-only partial result", func(t *testing.T) {
		svc, _, _, adm := newFixture(t)
		md := &Metadata{Source: sourcePrinter3mf, Title: strPtr("Title")}
		svc.finish(adm, Result{Metadata: md}, archiveErr(catImageMissing, "plate image missing"))
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusUnavailable {
			t.Fatalf("status = %s, want unavailable", res.Preview.Status)
		}
		if res.Metadata == nil || res.Metadata.Title == nil || *res.Metadata.Title != "Title" {
			t.Fatalf("metadata = %+v, want accepted metadata", res.Metadata)
		}
		if res.Preview.RetrievedAt == nil {
			t.Fatal("retrieved_at = nil, want stamped for accepted metadata")
		}
		if res.Preview.ImageURL != nil || res.PNG != nil {
			t.Fatal("metadata-only result must not carry an image")
		}
	})

	t.Run("pure failure", func(t *testing.T) {
		svc, _, _, adm := newFixture(t)
		svc.finish(adm, Result{}, errors.New("no such file"))
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusUnavailable {
			t.Fatalf("status = %s, want unavailable", res.Preview.Status)
		}
		if res.Preview.RetrievedAt != nil || res.Metadata != nil || res.PNG != nil {
			t.Fatalf("failed attempt published payload: %+v", res)
		}
	})

	t.Run("discard after revision change", func(t *testing.T) {
		svc, _, rd, adm := newFixture(t)
		svc.begin(serial, adm)
		j := settledJob(base, 7)
		j.Revision = 2
		rd.set(serial, j)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusUnavailable || res.PNG != nil || res.Preview.ImageURL != nil {
			t.Fatalf("stale result served as %+v, want terminal unavailable", res.Preview)
		}
	})

	t.Run("discard after running epoch change", func(t *testing.T) {
		svc, _, rd, adm := newFixture(t)
		svc.begin(serial, adm)
		j := settledJob(base, 7)
		j.RunningEpoch = 3 // pause/resume restarted the settling interval
		rd.set(serial, j)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusUnavailable || res.PNG != nil {
			t.Fatalf("result landed across a running epoch change: %+v", res.Preview)
		}
	})

	t.Run("discard after reconnect", func(t *testing.T) {
		svc, conn, rd, adm := newFixture(t)
		svc.begin(serial, adm)
		conn.bump(serial) // the printer reconnected: the pool generation moves
		j := settledJob(base, 7)
		j.StateGen = 5
		j.ObsGen = 5
		rd.set(serial, j)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusUnavailable || res.PNG != nil {
			t.Fatalf("result landed across a reconnect: %+v", res.Preview)
		}
	})

	t.Run("discard after disconnect", func(t *testing.T) {
		svc, conn, _, adm := newFixture(t)
		svc.begin(serial, adm)
		conn.setConnected(serial, false) // the printer dropped before publish
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusUnavailable || res.PNG != nil {
			t.Fatalf("result landed across a disconnect: %+v", res.Preview)
		}
	})

	t.Run("discard after observation goes stale", func(t *testing.T) {
		svc, _, rd, adm := newFixture(t)
		svc.begin(serial, adm)
		j := settledJob(base, 7)
		j.ObsAt = base.Add(-printerview.FreshnessWindow - time.Second) // report gap
		rd.set(serial, j)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusUnavailable || res.PNG != nil {
			t.Fatalf("result landed across a stale observation: %+v", res.Preview)
		}
	})
}

// TestFinishLogRecordShape drives finish across the outcome classes and
// pins the one-debug-record contract through slog's JSON serializer:
// fixed string outcome and phase fields, a published bool separating a
// served result from a fetched-but-discarded one, a timestamp on every
// record, and no wrapped error text — FTP reply text or credentials —
// anywhere in the output.
func TestFinishLogRecordShape(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	serial := "S1"
	const msg = "job preview attempt finished"

	newFixture := func(t *testing.T) (*Service, *jobReader, *admission, *bytes.Buffer) {
		t.Helper()
		var buf bytes.Buffer
		// Debug level is test-only; the production logger keeps its
		// configured level and the record under test is a Debug event.
		// The wall-clock time is pinned to a fixed value: its random digits
		// could contain a leak marker such as "550" and fail by chance.
		log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if len(groups) == 0 && a.Key == slog.TimeKey {
					return slog.Time(slog.TimeKey, time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC))
				}
				return a
			},
		}))
		clk := &testClock{now: base}
		conn := newFakeConn(serial)
		job := settledJob(base, 7)
		rd := newJobReader(serial, job)
		printers := []config.Printer{{
			Serial: serial, Address: serial + ".invalid:8883", Username: "u", Password: "p",
		}}
		cache := telemetry.NewCache(slices.Clone(printers), testLogger())
		svc := New(printers, cache, conn, log)
		svc.readJob = rd.Job
		svc.now = clk.Now
		adm := &admission{
			printer:  printers[0],
			job:      job,
			gen:      7,
			rev:      1,
			epoch:    2,
			upstream: 4,
		}
		reserveSlot(svc)
		return svc, rd, adm, &buf
	}

	// lastRecord parses the newest serialized record and checks the
	// record shape every outcome must share.
	lastRecord := func(t *testing.T, buf *bytes.Buffer) map[string]any {
		t.Helper()
		lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
		if len(lines) == 0 || lines[len(lines)-1] == "" {
			t.Fatal("no log record serialized")
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(lines[len(lines)-1]), &rec); err != nil {
			t.Fatalf("record is not one JSON object: %v", err)
		}
		if rec["msg"] != msg {
			t.Errorf("msg = %v, want %q", rec["msg"], msg)
		}
		if rec["level"] != "DEBUG" {
			t.Errorf("level = %v, want DEBUG", rec["level"])
		}
		if rec["serial"] != serial {
			t.Errorf("serial = %v, want %q", rec["serial"], serial)
		}
		when, ok := rec["time"].(string)
		if !ok {
			t.Fatalf("time = %v, want an RFC3339 string", rec["time"])
		}
		if _, err := time.Parse(time.RFC3339Nano, when); err != nil {
			t.Errorf("time %q does not parse: %v", when, err)
		}
		return rec
	}

	expect := func(t *testing.T, rec map[string]any, outcome, phase string, published bool) {
		t.Helper()
		if rec["outcome"] != outcome {
			t.Errorf("outcome = %v, want %q", rec["outcome"], outcome)
		}
		if rec["phase"] != phase {
			t.Errorf("phase = %v, want %q", rec["phase"], phase)
		}
		got, ok := rec["published"].(bool)
		if !ok {
			t.Fatalf("published = %v, want a bool", rec["published"])
		}
		if got != published {
			t.Errorf("published = %v, want %v", got, published)
		}
	}

	t.Run("ready is published with no failure phase", func(t *testing.T) {
		svc, _, adm, buf := newFixture(t)
		svc.begin(serial, adm)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2),
			Metadata: &Metadata{Source: sourcePrinter3mf, Title: strPtr("T")}}, nil)
		expect(t, lastRecord(t, buf), catReady, phaseNone, true)
	})

	t.Run("fetched ready but discarded is not published", func(t *testing.T) {
		svc, rd, adm, buf := newFixture(t)
		svc.begin(serial, adm)
		j := settledJob(base, 7)
		j.Revision = 2
		rd.set(serial, j)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
		// outcome stays ready — the fetch itself succeeded — while
		// published separates it from a served result.
		expect(t, lastRecord(t, buf), catReady, phaseNone, false)
	})

	t.Run("login failure keeps credential text out", func(t *testing.T) {
		svc, _, adm, buf := newFixture(t)
		svc.begin(serial, adm)
		// The injected secret carries a newline: the serializer must
		// escape it into one parseable record rather than forge a line.
		secret := "hunter2-secret\nmarker"
		err := classifyAttempt(context.Background(),
			fmt.Errorf("530 login incorrect for user u pass %s", secret), phaseLogin)
		svc.finish(adm, Result{}, err)
		// The gate still holds, so the terminal unavailable entry is
		// published; only the identity of the failure is confidential.
		expect(t, lastRecord(t, buf), catTransport, phaseLogin, true)
		for _, leak := range []string{secret, "530 login incorrect"} {
			if strings.Contains(buf.String(), leak) {
				t.Errorf("log leaked %q", leak)
			}
		}
	})

	t.Run("size failure keeps reply text out", func(t *testing.T) {
		svc, _, adm, buf := newFixture(t)
		svc.begin(serial, adm)
		err := ftpsErrAt(phaseSize, catTransport,
			&textproto.Error{Code: 550, Msg: "Permission denied secret-marker-2"})
		svc.finish(adm, Result{}, err)
		expect(t, lastRecord(t, buf), catTransport, phaseSize, true)
		for _, leak := range []string{"secret-marker-2", "Permission denied", "550"} {
			if strings.Contains(buf.String(), leak) {
				t.Errorf("log leaked %q", leak)
			}
		}
	})

	t.Run("accepted partial archive stays published as archive phase", func(t *testing.T) {
		svc, _, adm, buf := newFixture(t)
		svc.begin(serial, adm)
		svc.finish(adm, Result{Metadata: &Metadata{Source: sourcePrinter3mf, Title: strPtr("T")}},
			archiveErr(catImageMissing, "plate image absent"))
		expect(t, lastRecord(t, buf), catImageMissing, phaseArchive, true)
	})

	t.Run("cancelled after the gate broke names no phase", func(t *testing.T) {
		svc, rd, adm, buf := newFixture(t)
		svc.begin(serial, adm)
		j := settledJob(base, 7)
		j.Active = false
		j.State = "FINISH"
		rd.set(serial, j)
		svc.finish(adm, Result{}, context.Canceled)
		expect(t, lastRecord(t, buf), catCancelled, phaseNone, false)
	})
}

func TestRetainedResultAfterTerminal(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := settledJob(base, 7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	png := pngBytes(t, 2, 2)
	adm := &admission{
		printer: config.Printer{Serial: "S1"}, job: job, gen: 7, rev: 1, epoch: 2, upstream: 4,
	}
	reserveSlot(svc)
	svc.begin("S1", adm)
	svc.finish(adm, Result{PNG: png}, nil)

	// The same generation finishes; the result stays, current flips off.
	done := job
	done.Active = false
	done.State = "FINISH"
	done.ObsAt = base.Add(-time.Second)
	rd.set("S1", done)

	res, ok := svc.Lookup("S1", true)
	if !ok || res.Preview.Status != StatusReady {
		t.Fatalf("Lookup = %+v, %v; want retained ready result", res.Preview, ok)
	}
	if res.Preview.Current {
		t.Fatal("current = true after terminal report, want false")
	}
	if res.Preview.JobName != "Benchy" {
		t.Fatalf("job_name = %q, want retained identity", res.Preview.JobName)
	}
	if res.Preview.ImageURL == nil || !slices.Equal(res.PNG, png) {
		t.Fatal("retained payload missing image")
	}
}

func TestRevisionAndPreparationInvalidateImmediately(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := settledJob(base, 7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	started := make(chan struct{}, 4)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		return Result{PNG: pngBytes(t, 2, 2)}, nil
	}
	adm := &admission{
		printer: config.Printer{Serial: "S1"}, job: job, gen: 7, rev: 1, epoch: 2, upstream: 4,
	}
	reserveSlot(svc)
	svc.begin("S1", adm)
	svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)

	// Revision bump: the old payload must never be served again, and no
	// second attempt is granted within the generation.
	j := job
	j.Revision = 2
	rd.set("S1", j)
	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusUnavailable || res.PNG != nil || res.Preview.ImageURL != nil {
		t.Fatalf("consumed generation after revision = %+v, want unavailable without payload", res.Preview)
	}
	svc.step(base.Add(time.Second))
	select {
	case <-started:
		t.Fatal("revision granted a second attempt")
	default:
	}

	// New preparation: new generation, pending, still no attempt while
	// the printer is not RUNNING.
	j = job
	j.Generation = 8
	j.Revision = 1
	j.State = "PREPARE"
	j.RunningSince = time.Time{}
	j.ObsAt = base.Add(-time.Second)
	rd.set("S1", j)
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending || !res.Preview.Current {
		t.Fatalf("new preparation projection = %+v, want pending and current", res.Preview)
	}
	svc.step(base.Add(2 * time.Second))
	select {
	case <-started:
		t.Fatal("PREPARE granted an attempt")
	default:
	}
	if counts.count() != 0 {
		t.Fatalf("calls = %d, want 0", counts.count())
	}
}

func TestLookupBoundaryDoubleSample(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := settledJob(base, 7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	png := pngBytes(t, 2, 2)
	adm := &admission{
		printer: config.Printer{Serial: "S1"}, job: job, gen: 7, rev: 1, epoch: 2, upstream: 4,
	}
	reserveSlot(svc)
	svc.begin("S1", adm)
	svc.finish(adm, Result{PNG: png}, nil)

	next := settledJob(base, 8) // generation boundary between the two samples
	rd.seq = []telemetry.JobView{job, next}
	res, ok := svc.Lookup("S1", true)
	if !ok {
		t.Fatal("Lookup lost configured serial")
	}
	if res.Preview.Status != StatusPending || res.PNG != nil || res.Preview.ImageURL != nil {
		t.Fatalf("boundary lookup served old payload: %+v", res.Preview)
	}
	if res.Preview.JobName != next.Name {
		t.Fatalf("job_name = %q, want new generation name", res.Preview.JobName)
	}

	// Same generation twice: the payload is served.
	rd.seq = []telemetry.JobView{job, job}
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusReady || !slices.Equal(res.PNG, png) {
		t.Fatalf("stable lookup = %+v, want ready payload", res.Preview)
	}
}

func TestLookupUnconfiguredAndUnobserved(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")

	if _, ok := svc.Lookup("OTHER", true); ok {
		t.Fatal("unknown serial must report unconfigured")
	}
	// A configured serial whose cache has no state yet projects unavailable.
	svc.readJob = func(string) (telemetry.JobView, bool) { return telemetry.JobView{}, false }
	res, ok := svc.Lookup("S1", true)
	if !ok || res.Preview.Status != StatusUnavailable || res.Preview.Current {
		t.Fatalf("unobserved projection = %+v, %v; want unavailable", res.Preview, ok)
	}

	// A nil state cache degrades to the same unavailable projection.
	nilSvc := New([]config.Printer{{Serial: "S1"}}, nil, conn, testLogger())
	res, ok = nilSvc.Lookup("S1", false)
	if !ok || res.Preview.Status != StatusUnavailable {
		t.Fatalf("nil-state projection = %+v, %v; want unavailable", res.Preview, ok)
	}
}

func TestConcurrentLookupsSingleAttempt(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		<-release
		return Result{PNG: pngBytes(t, 2, 2)}, nil
	}

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			svc.Lookup("S1", i%2 == 0)
		}(i)
	}
	at := base.Add(60 * time.Second)
	j := settledJob(at, 7)
	j.RunningSince = base
	rd.set("S1", j)
	clk.Set(at)
	svc.step(at)
	await(t, started)
	wg.Wait() // readers done
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d during concurrent reads, want 1", got)
	}
	close(release)
	svc.wg.Wait()

	// Published result serves every reader with consistent copies.
	var payloadWG sync.WaitGroup
	for i := range 100 {
		payloadWG.Add(1)
		go func(i int) {
			defer payloadWG.Done()
			res, ok := svc.Lookup("S1", i%2 == 0)
			if !ok || res.Preview.Status != StatusReady {
				t.Errorf("Lookup = %+v, %v; want ready", res.Preview, ok)
			}
			if i%2 == 0 && res.PNG == nil {
				t.Error("includeImage=true returned no PNG")
			}
			if i%2 == 1 && res.PNG != nil {
				t.Error("includeImage=false returned PNG bytes")
			}
		}(i)
	}
	payloadWG.Wait()
}

func TestCloseCancelsInFlightFetch(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	started := make(chan struct{}, 4)
	allowReturn := make(chan struct{})
	fetchDone := make(chan struct{})
	var ferr error
	var orderMu sync.Mutex
	var order []string
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		<-ctx.Done()
		ferr = ctx.Err()
		<-allowReturn
		orderMu.Lock()
		order = append(order, "fetch")
		orderMu.Unlock()
		close(fetchDone)
		return Result{}, ferr
	}
	at := base.Add(60 * time.Second)
	j := settledJob(at, 7)
	j.RunningSince = base
	rd.set("S1", j)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	// Two concurrent Close callers: both must stay pending until the
	// in-flight fetch has returned and been joined.
	firstClosed := make(chan struct{})
	go func() {
		svc.Close()
		orderMu.Lock()
		order = append(order, "close1")
		orderMu.Unlock()
		close(firstClosed)
	}()
	secondClosed := make(chan struct{})
	go func() {
		svc.Close()
		orderMu.Lock()
		order = append(order, "close2")
		orderMu.Unlock()
		close(secondClosed)
	}()
	select {
	case <-firstClosed:
		t.Fatal("first Close completed without joining the in-flight fetch")
	case <-secondClosed:
		t.Fatal("repeated Close completed without joining the in-flight fetch")
	default:
	}

	close(allowReturn)
	await(t, fetchDone)
	await(t, firstClosed)
	await(t, secondClosed)
	orderMu.Lock()
	got := slices.Clone(order)
	orderMu.Unlock()
	// Both Close callers wait for the join; whichever wins the stopMu
	// race returns first, so only the causal order is pinned: the fetch
	// returns and is joined before either Close caller completes.
	if len(got) != 3 || got[0] != "fetch" {
		t.Fatalf("completion order = %v, want the fetch joined before both Close callers", got)
	}
	tail := slices.Clone(got[1:])
	slices.Sort(tail)
	if !slices.Equal(tail, []string{"close1", "close2"}) {
		t.Fatalf("completion order = %v, want exactly one completion per Close caller", got)
	}
	if !errors.Is(ferr, context.Canceled) {
		t.Fatalf("fetch error = %v, want context.Canceled", ferr)
	}
	if counts.count() != 1 {
		t.Fatalf("calls = %d, want 1", counts.count())
	}
	// After Close, no further reservations and no payload from a
	// cancelled attempt.
	svc.step(base.Add(61 * time.Second))
	if counts.count() != 1 {
		t.Fatalf("calls = %d after close, want 1", counts.count())
	}
	res, ok := svc.Lookup("S1", true)
	if !ok || res.Preview.Status != StatusUnavailable {
		t.Fatalf("Lookup after cancelled attempt = %+v; want terminal unavailable", res.Preview)
	}
}

// TestSchedulerCancelsReconnectedUpstream pins the captured-upstream
// equality in the strict gate: a printer that reconnects and reports fresh
// RUNNING on the new connection invalidates the attempt admitted on the
// old one, even with the job otherwise unchanged.
func TestSchedulerCancelsReconnectedUpstream(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	started := make(chan struct{}, 4)
	done := make(chan struct{})
	var ferr error
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		<-ctx.Done()
		ferr = ctx.Err()
		close(done)
		return Result{}, ferr
	}
	at := base.Add(60 * time.Second)
	j := settledJob(at, 7)
	j.RunningSince = base
	rd.set("S1", j)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	// Reconnect: the pool generation moves and the printer reports fresh
	// RUNNING on the new connection with the same preview generation,
	// revision and running epoch. Only the captured-upstream equality
	// catches this.
	conn.bump("S1")
	reconnected := settledJob(at, 7)
	reconnected.StateGen = 5
	reconnected.ObsGen = 5
	reconnected.ObsAt = at.Add(-time.Second)
	rd.set("S1", reconnected)
	clk.Set(at.Add(time.Second))
	svc.step(at.Add(time.Second))
	await(t, done)
	svc.wg.Wait()

	if !errors.Is(ferr, context.Canceled) {
		t.Fatalf("fetch error = %v, want context.Canceled", ferr)
	}
	if counts.count() != 1 {
		t.Fatalf("calls = %d, want 1", counts.count())
	}
	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusUnavailable || res.PNG != nil {
		t.Fatalf("reconnected attempt left %+v, want unavailable without payload", res.Preview)
	}
}

func TestStartCloseIdempotent(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		return Result{}, &testError{cat: catTransport}
	}
	svc.Start()
	svc.Start() // second Start is a no-op
	svc.Close()
	svc.Close() // second Close is a no-op
	if svc.started {
		t.Fatal("started after Close")
	}
	svc.Start() // a closed service never restarts
	if svc.started {
		t.Fatal("Start after Close started the scheduler")
	}
}

func TestMetadataDeepCopyIsolation(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := settledJob(base, 7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	originalPNG := pngBytes(t, 2, 2)
	md := &Metadata{
		Source:      sourcePrinter3mf,
		Title:       strPtr("Title"),
		Description: strPtr("Desc"),
		Plate:       &Plate{Index: 3, Name: strPtr("Plate")},
		Materials:   []Material{{ID: "1", Type: strPtr("PLA")}},
		Objects:     []Object{{Name: "a", Count: 2}},
		Warnings:    []SlicerWarning{{Code: "c", Message: "m"}},
		Process:     &Process{LayerHeightMM: fPtr(0.2)},
	}
	adm := &admission{
		printer: config.Printer{Serial: "S1"}, job: job, gen: 7, rev: 1, epoch: 2, upstream: 4,
	}
	reserveSlot(svc)
	svc.begin("S1", adm)
	svc.finish(adm, Result{Metadata: md, PNG: originalPNG}, nil)

	first, _ := svc.Lookup("S1", true)
	*first.Metadata.Title = "mutated"
	*first.Metadata.Plate.Name = "mutated"
	*first.Metadata.Materials[0].Type = "mutated"
	first.Metadata.Objects[0].Count = 99
	*first.Metadata.Process.LayerHeightMM = 9
	first.Metadata.Warnings[0].Message = "mutated"
	*first.Preview.ImageURL = "mutated"
	first.PNG[0] ^= 0xff

	second, _ := svc.Lookup("S1", true)
	if *second.Metadata.Title != "Title" ||
		*second.Metadata.Plate.Name != "Plate" ||
		*second.Metadata.Materials[0].Type != "PLA" ||
		second.Metadata.Objects[0].Count != 2 ||
		*second.Metadata.Process.LayerHeightMM != 0.2 ||
		second.Metadata.Warnings[0].Message != "m" {
		t.Fatalf("caller mutation reached cached metadata: %+v", second.Metadata)
	}
	if *second.Preview.ImageURL == "mutated" {
		t.Fatal("caller mutation reached cached image URL")
	}
	if !slices.Equal(second.PNG, originalPNG) {
		t.Fatal("caller mutation reached cached PNG bytes")
	}
	// The original fetch result must not alias the cache either.
	if *md.Title != "Title" {
		t.Fatal("fetch result metadata aliased cache")
	}
}

func strPtr(s string) *string { return &s }
func fPtr(f float64) *float64 { return &f }

func TestOutcomeCategories(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"success", nil, catReady},
		{"cancelled", context.Canceled, catCancelled},
		{"deadline", context.DeadlineExceeded, catTimeout},
		{"wrapped cancel", fmt.Errorf("transfer: %w", context.Canceled), catCancelled},
		{"archive category", archiveErr(catOversized, "test"), catOversized},
		{"archive image missing", archiveErr(catImageMissing, "test"), catImageMissing},
		{"transport category", &testError{cat: catNotFound}, catNotFound},
		{"wrapped transport category", fmt.Errorf("dial: %w", &testError{cat: catTransport}), catTransport},
		{"unknown", errors.New("mystery"), catTransport},
	}
	for _, tc := range cases {
		if got := outcomeCategory(tc.err); got != tc.want {
			t.Errorf("%s: outcomeCategory = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestRealCacheSchedulerFlow drives the service against the real telemetry
// cache with real report timestamps and real step times: pending while
// settling, no fetch attempts, and the live projection tracking the job.
func TestRealCacheSchedulerFlow(t *testing.T) {
	printers := []config.Printer{{Serial: "S1", Address: "s1.invalid:8883"}}
	cache := telemetry.NewCache(printers, testLogger())
	conn := newFakeConn("S1")
	conn.gens["S1"] = 1
	svc := New(printers, cache, conn, testLogger())
	counts := &fetchCounter{}
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		return Result{}, errors.New("must not be called")
	}
	t.Cleanup(svc.Close)

	report := func(kv map[string]any) []byte {
		raw, err := json.Marshal(map[string]any{"print": kv})
		if err != nil {
			t.Fatalf("marshal report: %v", err)
		}
		return raw
	}

	cache.ObserveReport("S1", 1, 1, report(map[string]any{
		"gcode_state": "PREPARE", "subtask_name": "Job One",
	}))
	res, ok := svc.Lookup("S1", false)
	if !ok || res.Preview.Status != StatusPending || !res.Preview.Current || res.Preview.JobName != "Job One" {
		t.Fatalf("projection = %+v, %v; want pending current job", res.Preview, ok)
	}

	cache.ObserveReport("S1", 2, 1, report(map[string]any{"gcode_state": "RUNNING"}))
	svc.step(time.Now()) // real clock: RUNNING began moments ago, not settled
	if counts.count() != 0 {
		t.Fatalf("calls = %d, want 0 while settling", counts.count())
	}

	// Metadata-only deltas never refresh the observation and never fetch.
	cache.ObserveReport("S1", 3, 1, report(map[string]any{"gcode_file": "other.3mf"}))
	res, _ = svc.Lookup("S1", false)
	if res.Preview.Status != StatusPending {
		t.Fatalf("status after metadata delta = %s, want pending", res.Preview.Status)
	}

	cache.ObserveReport("S1", 4, 1, report(map[string]any{"gcode_state": "FINISH"}))
	res, _ = svc.Lookup("S1", false)
	if res.Preview.Status != StatusUnavailable || res.Preview.Current {
		t.Fatalf("projection after finish = %+v, want unavailable", res.Preview)
	}
	if counts.count() != 0 {
		t.Fatalf("calls = %d, want 0 across the whole flow", counts.count())
	}
}

// TestSchedulerCancelsStaleTransfer proves the scheduler owns in-flight
// invalidation: a tick that finds the job out of RUNNING cancels the
// transfer, the result is discarded, and the attempt stays consumed.
func TestSchedulerCancelsStaleTransfer(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	started := make(chan struct{}, 4)
	done := make(chan struct{})
	var ferr error
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		<-ctx.Done()
		ferr = ctx.Err()
		close(done)
		return Result{}, ferr
	}
	at := base.Add(60 * time.Second)
	j := settledJob(at, 7)
	j.RunningSince = base
	rd.set("S1", j)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	// The job leaves RUNNING: pause moves the running epoch, exactly as
	// the telemetry merge does.
	paused := settledJob(at, 7)
	paused.State = "PAUSE"
	paused.RunningEpoch = 3
	paused.ObsAt = at.Add(-time.Second)
	rd.set("S1", paused)
	clk.Set(at.Add(time.Second))
	svc.step(at.Add(time.Second)) // this tick invalidates and cancels
	await(t, done)
	svc.wg.Wait()

	if !errors.Is(ferr, context.Canceled) {
		t.Fatalf("fetch error = %v, want context.Canceled", ferr)
	}
	if counts.count() != 1 {
		t.Fatalf("calls = %d, want 1", counts.count())
	}
	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusUnavailable || res.PNG != nil {
		t.Fatalf("cancelled attempt left %+v, want unavailable without payload", res.Preview)
	}
}

// TestTransferCancelBeforeDial pins the pre-dial recheck: a state change
// between admission and the fetch call must fail the recheck and consume
// the attempt without any dial.
func TestTransferCancelBeforeDial(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := settledJob(base, 7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	adm := &admission{
		printer: config.Printer{Serial: "S1"}, job: job, gen: 7, rev: 1, epoch: 2, upstream: 4,
	}
	if !svc.transferValid(adm, base) {
		t.Fatal("fresh admission must pass the pre-dial recheck")
	}
	j := job
	j.Revision = 2
	rd.set("S1", j)
	if svc.transferValid(adm, base) {
		t.Fatal("revision change must fail the pre-dial recheck")
	}

	counts := &fetchCounter{}
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		return Result{PNG: pngBytes(t, 2, 2)}, nil
	}
	done := make(chan struct{})
	reserveSlot(svc)
	svc.begin("S1", adm)
	svc.wg.Add(1) // step registers the slot before launching; mirror it here
	go func() {
		svc.transfer(adm)
		close(done)
	}()
	await(t, done)
	if counts.count() != 0 {
		t.Fatalf("calls = %d, want 0: fetch must not dial after a failed preflight", counts.count())
	}
	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusUnavailable || res.PNG != nil {
		t.Fatalf("cancelled attempt left %+v, want unavailable without payload", res.Preview)
	}
}

// TestPublishDiscardedAfterTerminalDuringTransfer: the publish gate is the
// same strict check as the transfer gate, with no late-landing exception —
// a success completing after the same generation finished is discarded.
// (Retention after FINISH applies to already published entries, which
// TestRetainedResultAfterTerminal covers.)
func TestPublishDiscardedAfterTerminalDuringTransfer(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")
	png := pngBytes(t, 2, 2)
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		started <- struct{}{}
		<-release
		return Result{PNG: png}, nil
	}
	at := base.Add(60 * time.Second)
	j := settledJob(at, 7)
	j.RunningSince = base
	rd.set("S1", j)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	// The generation finishes while the transfer is still in flight; no
	// tick runs, so the outcome lands once the fetch returns.
	fin := settledJob(at, 7)
	fin.Active = false
	fin.State = "FINISH"
	fin.RunningEpoch = 3 // the terminal report leaves RUNNING
	fin.ObsAt = at.Add(-time.Second)
	rd.set("S1", fin)
	close(release)
	svc.wg.Wait()

	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusUnavailable || res.PNG != nil || res.Preview.ImageURL != nil {
		t.Fatalf("late success after terminal = %+v, want discarded unavailable", res.Preview)
	}
}

// TestPublishDiscardedAfterPauseDuringTransfer: a pause while transferring
// discards the result; the generation reads unavailable, never pending.
func TestPublishDiscardedAfterPauseDuringTransfer(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		started <- struct{}{}
		<-release
		return Result{PNG: pngBytes(t, 2, 2)}, nil
	}
	at := base.Add(60 * time.Second)
	j := settledJob(at, 7)
	j.RunningSince = base
	rd.set("S1", j)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	paused := settledJob(at, 7)
	paused.State = "PAUSE"
	paused.RunningEpoch = 3
	paused.ObsAt = at.Add(-time.Second)
	rd.set("S1", paused)
	close(release)
	svc.wg.Wait()

	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusUnavailable || res.PNG != nil || res.Preview.ImageURL != nil {
		t.Fatalf("paused transfer outcome = %+v, want discarded unavailable", res.Preview)
	}
}

// TestInFlightAndConsumedStatuses pins the pending/unavailable lifecycle:
// pending only while the admitted generation's transfer can still produce
// a result, unavailable once its attempt is consumed, and pending again
// for a fresh generation.
func TestInFlightAndConsumedStatuses(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", settledJob(base, 7))
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		<-release
		return Result{}, &testError{cat: catTransport}
	}

	// Settling, not yet attempted: pending.
	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending {
		t.Fatalf("settling status = %s, want pending", res.Preview.Status)
	}
	at := base.Add(60 * time.Second)
	j := settledJob(at, 7)
	j.RunningSince = base
	rd.set("S1", j)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	// The attempt is registered for scheduler cancellation while in
	// flight, and its registration is gone before the slot reopens.
	if svc.active == nil || svc.active.adm.gen != 7 {
		t.Fatal("in-flight transfer not registered for cancellation")
	}

	// Transfer in progress for this generation: pending.
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending {
		t.Fatalf("in-flight status = %s, want pending", res.Preview.Status)
	}
	close(release)
	svc.wg.Wait()
	if svc.active != nil {
		t.Fatal("active transfer outlived the worker slot")
	}

	// Attempt consumed without a published result: terminal unavailable.
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusUnavailable {
		t.Fatalf("consumed status = %s, want unavailable", res.Preview.Status)
	}
	if counts.count() != 1 {
		t.Fatalf("calls = %d, want 1", counts.count())
	}

	// A new generation has not spent its attempt: pending again.
	next := settledJob(base.Add(61*time.Second), 8)
	rd.set("S1", next)
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending {
		t.Fatalf("new generation status = %s, want pending", res.Preview.Status)
	}
}

// TestBeginReservationRules pins begin's guards: the allowance is consumed
// only by a caller holding the reserved sole transfer slot, only once per
// generation, and never on a closed service.
func TestBeginReservationRules(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	conn := newFakeConn("S1")
	job := settledJob(base, 7)
	newSvc := func() *Service {
		return newTestService(t, conn, newJobReader("S1", job), &testClock{now: base}, "S1")
	}
	adm := func() *admission {
		return &admission{printer: config.Printer{Serial: "S1"}, job: job, gen: 7, rev: 1, epoch: 2, upstream: 4}
	}

	// Without the reserved sole slot, begin consumes nothing.
	unreserved := newSvc()
	if unreserved.begin("S1", adm()) {
		t.Fatal("begin without the reserved sole slot must fail")
	}
	if unreserved.attemptedGeneration("S1", 7) {
		t.Fatal("a refused reservation consumed the attempt")
	}

	// With the slot reserved, exactly one begin per generation succeeds.
	svc := newSvc()
	reserveSlot(svc)
	if !svc.begin("S1", adm()) {
		t.Fatal("begin with the reserved sole slot must succeed")
	}
	if svc.begin("S1", adm()) {
		t.Fatal("begin must refuse an already consumed generation")
	}
	svc.mu.Lock()
	inflight := svc.inflight["S1"]
	svc.mu.Unlock()
	if inflight == nil || inflight.gen != 7 {
		t.Fatal("the first admission must stay registered as in flight")
	}

	// A closed service refuses and consumes nothing.
	closedSvc := newSvc()
	closedSvc.mu.Lock()
	closedSvc.closed = true
	closedSvc.worker = true // reservation held when Close landed
	closedSvc.mu.Unlock()
	if closedSvc.begin("S1", adm()) {
		t.Fatal("begin on a closed service must fail")
	}
	if closedSvc.attemptedGeneration("S1", 7) {
		t.Fatal("a closed-service refusal consumed the attempt")
	}
}

// TestPreflightBarrierBetweenReservationAndAdmission pins the window
// between reserving the sole slot and consuming the attempt: a pause or a
// stale observation in the preflight re-read releases the reservation
// without consuming the allowance, a revision is absorbed into the
// re-read snapshot, and a closed service admits nothing.
func TestPreflightBarrierBetweenReservationAndAdmission(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	at := base.Add(60 * time.Second)
	settled := func(obsAt time.Time, rev uint64) telemetry.JobView {
		j := settledJob(at, 7)
		j.RunningSince = base
		j.ObsAt = obsAt
		j.Revision = rev
		return j
	}

	t.Run("pause in preflight releases without consuming", func(t *testing.T) {
		clk := &testClock{now: base}
		conn := newFakeConn("S1")
		rd := newJobReader("S1", settledJob(base, 7))
		svc := newTestService(t, conn, rd, clk, "S1")
		counts := &fetchCounter{}
		started := make(chan struct{}, 4)
		svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
			counts.add(p.Serial)
			started <- struct{}{}
			return Result{}, &testError{cat: catTransport}
		}
		// The first read passes eligibility; the preflight re-read sees
		// the pause that landed inside the reservation window.
		paused := settled(at.Add(-time.Second), 1)
		paused.State = "PAUSE"
		paused.RunningEpoch = 3
		rd.seq = []telemetry.JobView{settled(at.Add(-time.Second), 1), paused}
		clk.Set(at)
		svc.step(at)
		if counts.count() != 0 {
			t.Fatalf("calls = %d, want 0: the pause must stop the dial", counts.count())
		}
		if svc.attemptedGeneration("S1", 7) {
			t.Fatal("a failed preflight consumed the generation's attempt")
		}
		// The allowance is intact: the settled generation admits next tick.
		rd.set("S1", settled(at.Add(-time.Second), 1))
		clk.Set(at.Add(time.Second))
		svc.step(at.Add(time.Second))
		await(t, started)
		svc.wg.Wait()
		if counts.count() != 1 {
			t.Fatalf("calls = %d, want exactly 1 after the barrier cleared", counts.count())
		}
	})

	t.Run("stale observation in preflight releases without consuming", func(t *testing.T) {
		clk := &testClock{now: base}
		conn := newFakeConn("S1")
		rd := newJobReader("S1", settledJob(base, 7))
		svc := newTestService(t, conn, rd, clk, "S1")
		counts := &fetchCounter{}
		svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
			counts.add(p.Serial)
			return Result{}, &testError{cat: catTransport}
		}
		// The observation aged past the freshness window between the two
		// reads: a report gap, not a state change.
		stale := settled(at.Add(-printerview.FreshnessWindow-time.Second), 1)
		rd.seq = []telemetry.JobView{settled(at.Add(-time.Second), 1), stale}
		clk.Set(at)
		svc.step(at)
		if counts.count() != 0 {
			t.Fatalf("calls = %d, want 0: the stale observation must stop the dial", counts.count())
		}
		if svc.attemptedGeneration("S1", 7) {
			t.Fatal("a failed preflight consumed the generation's attempt")
		}
	})

	t.Run("revision in the window is absorbed into the re-read", func(t *testing.T) {
		clk := &testClock{now: base}
		conn := newFakeConn("S1")
		rd := newJobReader("S1", settledJob(base, 7))
		svc := newTestService(t, conn, rd, clk, "S1")
		counts := &fetchCounter{}
		started := make(chan struct{}, 4)
		var gotRev uint64
		svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
			counts.add(p.Serial)
			gotRev = j.Revision
			started <- struct{}{}
			return Result{PNG: pngBytes(t, 2, 2)}, nil
		}
		// The revision lands inside the window: the re-read snapshot
		// carries it, so the single attempt proceeds against revision 2.
		rd.seq = []telemetry.JobView{settled(at.Add(-time.Second), 1), settled(at.Add(-time.Second), 2)}
		rd.set("S1", settled(at.Add(-time.Second), 2))
		clk.Set(at)
		svc.step(at)
		await(t, started)
		svc.wg.Wait()
		if gotRev != 2 || counts.count() != 1 {
			t.Fatalf("fetch revision = %d, calls = %d; want the re-read revision 2 in one attempt", gotRev, counts.count())
		}
		res, _ := svc.Lookup("S1", true)
		if res.Preview.Status != StatusReady {
			t.Fatalf("status = %s, want the result published for revision 2", res.Preview.Status)
		}
	})

	t.Run("closed service admits nothing", func(t *testing.T) {
		clk := &testClock{now: base}
		conn := newFakeConn("S1")
		rd := newJobReader("S1", settled(at.Add(-time.Second), 1))
		svc := newTestService(t, conn, rd, clk, "S1")
		svc.mu.Lock()
		svc.closed = true
		svc.worker = true // reservation held when Close landed
		svc.mu.Unlock()
		if adm := svc.admit(at); adm != nil {
			t.Fatal("admission on a closed service must fail")
		}
		if svc.attemptedGeneration("S1", 7) {
			t.Fatal("a closed-service refusal consumed the attempt")
		}
	})
}

// TestSchedulerCancelsOnDisconnectOrStaleObservation proves the strict
// gate beyond state changes: a pure disconnect and a pure telemetry-gap
// staleness — RUNNING unchanged, same running epoch — each cancel the
// in-flight attempt and discard its outcome.
func TestSchedulerCancelsOnDisconnectOrStaleObservation(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		invalidate func(t *testing.T, conn *fakeConn, rd *jobReader, at time.Time)
	}{
		{"disconnect", func(t *testing.T, conn *fakeConn, rd *jobReader, at time.Time) {
			conn.setConnected("S1", false)
		}},
		{"stale observation", func(t *testing.T, conn *fakeConn, rd *jobReader, at time.Time) {
			stale := settledJob(at, 7)
			stale.RunningSince = base
			stale.ObsAt = at.Add(-printerview.FreshnessWindow - time.Second)
			rd.set("S1", stale)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &testClock{now: base}
			conn := newFakeConn("S1")
			rd := newJobReader("S1", settledJob(base, 7))
			svc := newTestService(t, conn, rd, clk, "S1")
			counts := &fetchCounter{}
			started := make(chan struct{}, 4)
			done := make(chan struct{})
			var ferr error
			svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
				counts.add(p.Serial)
				started <- struct{}{}
				<-ctx.Done()
				ferr = ctx.Err()
				close(done)
				return Result{}, ferr
			}
			at := base.Add(60 * time.Second)
			j := settledJob(at, 7)
			j.RunningSince = base
			rd.set("S1", j)
			clk.Set(at)
			svc.step(at)
			await(t, started)

			tc.invalidate(t, conn, rd, at)
			clk.Set(at.Add(time.Second))
			svc.step(at.Add(time.Second)) // this tick invalidates and cancels
			await(t, done)
			svc.wg.Wait()

			if !errors.Is(ferr, context.Canceled) {
				t.Fatalf("fetch error = %v, want context.Canceled", ferr)
			}
			if counts.count() != 1 {
				t.Fatalf("calls = %d, want 1", counts.count())
			}
			res, _ := svc.Lookup("S1", true)
			if res.Preview.Status != StatusUnavailable || res.PNG != nil {
				t.Fatalf("cancelled attempt left %+v, want unavailable without payload", res.Preview)
			}
		})
	}
}
