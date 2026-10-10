// Scheduler and cache tests. Timing is driven by step(now) with injected
// clocks and job readers; fetch calls synchronize through channels and the
// service WaitGroup, so no test sleeps for retry timing.
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

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
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

// fakeConn is an injectable Connectivity source. Printers start connected.
type fakeConn struct {
	mu     sync.Mutex
	status map[string]bool
}

func newFakeConn(serials ...string) *fakeConn {
	c := &fakeConn{status: make(map[string]bool)}
	for _, s := range serials {
		c.status[s] = true
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

func (c *fakeConn) setConnected(serial string, up bool) {
	c.mu.Lock()
	c.status[serial] = up
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

// runningJob returns an active RUNNING view admitted on its first tick.
func runningJob(gen uint64) telemetry.JobView {
	return telemetry.JobView{
		Generation: gen,
		Active:     true,
		State:      "RUNNING",
		Name:       "a.3mf",
		GCodeFile:  "a_plate_1.3mf",
		PlateIndex: intPtr(1),
	}
}

// testAdmission builds one admission for serial from the job snapshot.
func testAdmission(serial string, job telemetry.JobView) *admission {
	return &admission{
		printer: config.Printer{Serial: serial},
		job:     job,
		gen:     job.Generation,
	}
}

// beginAttempt reserves the slot and records one attempt for the job.
func beginAttempt(t *testing.T, svc *Service, serial string, job telemetry.JobView) *admission {
	t.Helper()
	reserveSlot(svc)
	adm := testAdmission(serial, job)
	if !svc.begin(serial, adm) {
		t.Fatal("begin refused a fresh attempt")
	}
	return adm
}

// attemptRecord copies the serial's attempt record for assertions.
func attemptRecord(svc *Service, serial string) (attempt, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	rec := svc.attempts[serial]
	if rec == nil {
		return attempt{}, false
	}
	return *rec, true
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
	mk := func() telemetry.JobView { return runningJob(7) }
	cases := []struct {
		name      string
		job       telemetry.JobView
		connected bool
		want      bool
	}{
		{"running connected", mk(), true, true},
		{"pause state", func() telemetry.JobView { j := mk(); j.State = "PAUSE"; return j }(), true, true},
		{"paused state", func() telemetry.JobView { j := mk(); j.State = "PAUSED"; return j }(), true, true},
		{"prepare state", func() telemetry.JobView { j := mk(); j.State = "PREPARE"; return j }(), true, false},
		{"slicing state", func() telemetry.JobView { j := mk(); j.State = "SLICING"; return j }(), true, false},
		{"disconnected", mk(), false, false},
		{"inactive", func() telemetry.JobView { j := mk(); j.Active = false; return j }(), true, false},
		{"name only", func() telemetry.JobView { j := mk(); j.GCodeFile = ""; return j }(), true, true},
		{"gcode basename only", func() telemetry.JobView { j := mk(); j.Name = ""; return j }(), true, true},
		{"gcode with directories", func() telemetry.JobView {
			j := mk()
			j.Name = ""
			j.GCodeFile = "/cache/job.3mf"
			return j
		}(), true, true},
		{"uppercase extension", func() telemetry.JobView {
			j := mk()
			j.Name = ""
			j.GCodeFile = "JOB.3MF"
			return j
		}(), true, true},
		{"no identity at all", func() telemetry.JobView { j := mk(); j.Name = ""; j.GCodeFile = ""; return j }(), true, false},
		{"gcode not 3mf", func() telemetry.JobView {
			j := mk()
			j.Name = ""
			j.GCodeFile = "job.gcode"
			return j
		}(), true, false},
		{"gcode traversal segment", func() telemetry.JobView {
			j := mk()
			j.Name = ""
			j.GCodeFile = "a/../b.3mf"
			return j
		}(), true, false},
		{"gcode dot segment", func() telemetry.JobView {
			j := mk()
			j.Name = ""
			j.GCodeFile = "./b.3mf"
			return j
		}(), true, false},
		{"gcode control character", func() telemetry.JobView {
			j := mk()
			j.Name = ""
			j.GCodeFile = "job\x01.3mf"
			return j
		}(), true, false},
		{"only separators", func() telemetry.JobView { j := mk(); j.Name = ""; j.GCodeFile = `/\`; return j }(), true, false},
	}
	for _, tc := range cases {
		if got := eligible(tc.job, tc.connected); got != tc.want {
			t.Errorf("%s: eligible = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestStepAdmitsOnFirstRunningTick pins immediate admission: the first
// RUNNING tick starts one attempt, and the generation's retry backoff
// holds further ticks until the retry time.
func TestStepAdmitsOnFirstRunningTick(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", runningJob(7))
	svc := newTestService(t, conn, rd, clk, "S1")

	counts := &fetchCounter{}
	started := make(chan struct{}, 8)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		return Result{}, &testError{cat: catTransport}
	}

	clk.Set(base)
	svc.step(base)
	await(t, started)
	svc.wg.Wait() // join the transfer goroutine: outcome settled, slot released
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d, want exactly 1 on the first RUNNING tick", got)
	}

	// The transient failure backs off: ticks before the retry time start
	// nothing, and the reason names the wait.
	svc.step(base.Add(time.Second))
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d during backoff, want 1", got)
	}
	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending || res.Preview.Reason != reasonRetryWait {
		t.Fatalf("projection = %+v, want pending retry_wait", res.Preview)
	}
}

// TestIneligibleStatesNeverAdmit pins the eligibility floor through the
// scheduler: preparing, idle, and disconnected jobs never start a fetch.
func TestIneligibleStatesNeverAdmit(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		name      string
		job       telemetry.JobView
		connected bool
	}{
		{"prepare", func() telemetry.JobView { j := runningJob(7); j.State = "PREPARE"; return j }(), true},
		{"slicing", func() telemetry.JobView { j := runningJob(7); j.State = "SLICING"; return j }(), true},
		{"inactive", func() telemetry.JobView { j := runningJob(7); j.Active = false; return j }(), true},
		{"disconnected", runningJob(7), false},
		{"no identity", func() telemetry.JobView { j := runningJob(7); j.Name = ""; j.GCodeFile = ""; return j }(), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &testClock{now: base}
			conn := newFakeConn("S1")
			conn.setConnected("S1", tc.connected)
			rd := newJobReader("S1", tc.job)
			svc := newTestService(t, conn, rd, clk, "S1")
			counts := &fetchCounter{}
			started := make(chan struct{}, 4)
			svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
				counts.add(p.Serial)
				started <- struct{}{}
				return Result{}, nil
			}
			for i := range 5 {
				svc.step(base.Add(time.Duration(i) * time.Second))
			}
			select {
			case <-started:
				t.Fatal("fetch admitted for an ineligible job")
			default:
			}
			if counts.count() != 0 {
				t.Fatalf("calls = %d, want 0", counts.count())
			}
		})
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
		"S1": runningJob(7),
		"S2": runningJob(9),
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

// TestTransientFailuresRetryThenFinal pins the retry schedule: a transport
// failure retries at +30 s and +60 s, the third failure marks the
// generation final, and exactly one job_preview_unavailable entry is
// recorded. A new generation starts a fresh budget.
func TestTransientFailuresRetryThenFinal(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", runningJob(7))
	svc := newTestService(t, conn, rd, clk, "S1")
	log := activity.New([]config.Printer{{Serial: "S1"}})
	svc.SetActivity(log)

	counts := &fetchCounter{}
	started := make(chan struct{}, 8)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		return Result{}, &testError{cat: catTransport}
	}
	stepAndJoin := func(at time.Time) {
		clk.Set(at)
		svc.step(at)
		await(t, started)
		svc.wg.Wait()
	}

	stepAndJoin(base)
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d after first attempt, want 1", got)
	}
	if rec, ok := attemptRecord(svc, "S1"); !ok || rec.count != 1 || rec.final || !rec.nextAt.Equal(base.Add(30*time.Second)) {
		t.Fatalf("record = %+v, %v; want count 1 backing off to +30s", rec, ok)
	}

	// Before the retry time: no attempt.
	clk.Set(base.Add(29 * time.Second))
	svc.step(base.Add(29 * time.Second))
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d before retry time, want 1", got)
	}

	stepAndJoin(base.Add(30 * time.Second))
	if got := counts.count(); got != 2 {
		t.Fatalf("calls = %d after +30s retry, want 2", got)
	}
	if rec, _ := attemptRecord(svc, "S1"); rec.count != 2 || !rec.nextAt.Equal(base.Add(90*time.Second)) {
		t.Fatalf("record = %+v, want count 2 backing off to +90s", rec)
	}

	stepAndJoin(base.Add(90 * time.Second))
	if got := counts.count(); got != 3 {
		t.Fatalf("calls = %d after +60s retry, want 3", got)
	}
	rec, _ := attemptRecord(svc, "S1")
	if !rec.final || rec.reason != catTransport {
		t.Fatalf("record = %+v, want final transport", rec)
	}
	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusUnavailable || res.Preview.Reason != catTransport {
		t.Fatalf("projection = %+v, want unavailable transport", res.Preview)
	}
	events := log.Recent("S1")
	if len(events) != 1 || events[0].Kind != "job_preview_unavailable" || events[0].Severity != activity.Info {
		t.Fatalf("activity = %+v, want one info job_preview_unavailable", events)
	}
	wantMsg := "Print details unavailable, " + strings.ToLower(reasonText(catTransport)[:1]) + reasonText(catTransport)[1:]
	if events[0].Message != wantMsg {
		t.Fatalf("message = %q, want %q", events[0].Message, wantMsg)
	}

	// Final: no further attempt for this generation.
	clk.Set(base.Add(10 * time.Minute))
	svc.step(base.Add(10 * time.Minute))
	if got := counts.count(); got != 3 {
		t.Fatalf("calls = %d after final, want 3", got)
	}

	// A new job before the next tick ignores the old final: fresh budget.
	rd.set("S1", runningJob(8))
	stepAndJoin(base.Add(11 * time.Minute))
	if got := counts.count(); got != 4 {
		t.Fatalf("calls = %d after new generation, want 4", got)
	}
	if rec, _ := attemptRecord(svc, "S1"); rec.gen != 8 || rec.count != 1 || rec.final {
		t.Fatalf("record = %+v, want fresh count 1 for gen 8", rec)
	}
}

// TestFinishPublishesOutcomes drives finish synchronously and pins the
// published view for ready, partial, failed, and selector-moved outcomes.
func TestFinishPublishesOutcomes(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	serial := "S1"

	newFixture := func(t *testing.T) (*Service, *fakeConn, *jobReader, telemetry.JobView, *admission) {
		clk := &testClock{now: base}
		conn := newFakeConn(serial)
		job := runningJob(7)
		rd := newJobReader(serial, job)
		svc := newTestService(t, conn, rd, clk, serial)
		adm := beginAttempt(t, svc, serial, job)
		return svc, conn, rd, job, adm
	}

	t.Run("ready result", func(t *testing.T) {
		svc, _, _, job, adm := newFixture(t)
		png := pngBytes(t, 2, 2)
		md := &Metadata{Source: sourcePrinter3mf, Title: strPtr("Title")}
		svc.finish(adm, Result{PNG: png, Metadata: md}, nil)
		res, ok := svc.Lookup(serial, true)
		if !ok || res.Preview.Status != StatusReady {
			t.Fatalf("Lookup = %+v, %v; want ready", res.Preview, ok)
		}
		if res.Preview.Reason != "" {
			t.Fatalf("reason = %q, want empty for a clean ready", res.Preview.Reason)
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
		if res.Preview.JobName != job.Name || !res.Preview.Current {
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

	t.Run("png plus metadata error publishes ready with reason", func(t *testing.T) {
		svc, _, _, _, adm := newFixture(t)
		png := pngBytes(t, 2, 2)
		svc.finish(adm, Result{PNG: png}, archiveErr(catArchive, "slice info bad"))
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusReady || res.Preview.Reason != catArchive {
			t.Fatalf("view = %+v, want ready with reason archive", res.Preview)
		}
		if res.Preview.ImageURL == nil || res.PNG == nil {
			t.Fatal("partial ready result lost its image")
		}
	})

	t.Run("metadata-only with image missing publishes unavailable with reason", func(t *testing.T) {
		svc, _, _, _, adm := newFixture(t)
		md := &Metadata{Source: sourcePrinter3mf, Title: strPtr("Title")}
		svc.finish(adm, Result{Metadata: md}, archiveErr(catImageMissing, "plate image missing"))
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusUnavailable || res.Preview.Reason != catImageMissing {
			t.Fatalf("view = %+v, want unavailable with reason image_missing", res.Preview)
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

	t.Run("archive failure without data is final after one attempt", func(t *testing.T) {
		svc, _, _, _, adm := newFixture(t)
		log := activity.New([]config.Printer{{Serial: serial}})
		svc.SetActivity(log)
		svc.finish(adm, Result{}, archiveErr(catArchive, "open archive"))
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusUnavailable || res.Preview.Reason != catArchive {
			t.Fatalf("view = %+v, want unavailable archive", res.Preview)
		}
		if rec, _ := attemptRecord(svc, serial); !rec.final || rec.count != 1 {
			t.Fatalf("record = %+v, want final count 1", rec)
		}
		if events := log.Recent(serial); len(events) != 1 || events[0].Kind != "job_preview_unavailable" {
			t.Fatalf("activity = %+v, want one job_preview_unavailable", events)
		}
	})

	t.Run("transient failure schedules a retry", func(t *testing.T) {
		svc, _, _, _, adm := newFixture(t)
		svc.finish(adm, Result{}, &testError{cat: catTransport})
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusPending || res.Preview.Reason != reasonRetryWait {
			t.Fatalf("view = %+v, want pending retry_wait", res.Preview)
		}
		if res.Preview.RetrievedAt != nil || res.Metadata != nil || res.PNG != nil {
			t.Fatalf("failed attempt published payload: %+v", res)
		}
	})

	t.Run("pause and disconnect do not discard", func(t *testing.T) {
		svc, conn, rd, job, adm := newFixture(t)
		paused := job
		paused.State = "PAUSE"
		rd.set(serial, paused)
		conn.setConnected(serial, false)
		png := pngBytes(t, 2, 2)
		svc.finish(adm, Result{PNG: png}, nil)
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusReady || !slices.Equal(res.PNG, png) {
			t.Fatalf("view = %+v, want the ready result published across pause and disconnect", res.Preview)
		}
	})

	t.Run("accepted result with moved selectors retries immediately", func(t *testing.T) {
		svc, _, rd, job, adm := newFixture(t)
		moved := job
		moved.GCodeFile = "other_plate_2.3mf"
		moved.PlateIndex = intPtr(2)
		rd.set(serial, moved)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusPending || res.PNG != nil {
			t.Fatalf("view = %+v, want pending without payload", res.Preview)
		}
		rec, _ := attemptRecord(svc, serial)
		if !rec.nextAt.Equal(base) {
			t.Fatalf("nextAt = %v, want immediate retry at %v", rec.nextAt, base)
		}
	})

	t.Run("stale generation outcome records nothing", func(t *testing.T) {
		svc, _, rd, _, adm := newFixture(t)
		// The replacement generation starts its own attempt first.
		next := runningJob(8)
		rd.set(serial, next)
		release := func() {
			svc.mu.Lock()
			svc.worker = false
			svc.mu.Unlock()
		}
		release()
		reserveSlot(svc)
		nextAdm := testAdmission(serial, next)
		if !svc.begin(serial, nextAdm) {
			t.Fatal("begin refused the replacement generation")
		}
		var buf bytes.Buffer
		svc.log = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
		if buf.Len() != 0 {
			t.Fatalf("stale outcome logged: %s", buf.String())
		}
		if rec, _ := attemptRecord(svc, serial); rec.gen != 8 || rec.count != 1 {
			t.Fatalf("record = %+v, want the replacement generation untouched", rec)
		}
		res, _ := svc.Lookup(serial, true)
		if res.Preview.Status != StatusPending || res.PNG != nil {
			t.Fatalf("view = %+v, want pending without stale payload", res.Preview)
		}
	})
}

// TestFinishLogRecordShape drives finish across the outcome classes and
// pins the one-Info-record contract through slog's JSON serializer: fixed
// string outcome and phase fields, a published bool, the attempt budget
// count, retry_in_s only when a retry is scheduled, a timestamp on every
// record, and no wrapped error text — FTP reply text or credentials —
// anywhere in the output.
func TestFinishLogRecordShape(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	serial := "S1"
	const msg = "job preview attempt finished"

	newFixture := func(t *testing.T) (*Service, *jobReader, *admission, *bytes.Buffer) {
		t.Helper()
		var buf bytes.Buffer
		// The wall-clock time is pinned to a fixed value: its random digits
		// could contain a leak marker such as "550" and fail by chance.
		log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelInfo,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if len(groups) == 0 && a.Key == slog.TimeKey {
					return slog.Time(slog.TimeKey, time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC))
				}
				return a
			},
		}))
		clk := &testClock{now: base}
		conn := newFakeConn(serial)
		job := runningJob(7)
		rd := newJobReader(serial, job)
		printers := []config.Printer{{
			Serial: serial, Address: serial + ".invalid:8883", Username: "u", Password: "p",
		}}
		cache := telemetry.NewCache(slices.Clone(printers), testLogger())
		svc := New(printers, cache, conn, log)
		svc.readJob = rd.Job
		svc.now = clk.Now
		reserveSlot(svc)
		adm := testAdmission(serial, job)
		if !svc.begin(serial, adm) {
			t.Fatal("begin refused a fresh attempt")
		}
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
		if rec["level"] != "INFO" {
			t.Errorf("level = %v, want INFO", rec["level"])
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

	expect := func(t *testing.T, rec map[string]any, outcome, phase string, published bool, attempt float64, retryIn float64) {
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
		if rec["attempt"] != attempt {
			t.Errorf("attempt = %v, want %v", rec["attempt"], attempt)
		}
		gotRetry, hasRetry := rec["retry_in_s"]
		if retryIn == 0 {
			if hasRetry {
				t.Errorf("retry_in_s = %v, want absent", gotRetry)
			}
			return
		}
		if gotRetry != retryIn {
			t.Errorf("retry_in_s = %v, want %v", gotRetry, retryIn)
		}
	}

	t.Run("ready is published with no failure phase", func(t *testing.T) {
		svc, _, adm, buf := newFixture(t)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2),
			Metadata: &Metadata{Source: sourcePrinter3mf, Title: strPtr("T")}}, nil)
		expect(t, lastRecord(t, buf), catReady, phaseNone, true, 1, 0)
	})

	t.Run("accepted result with moved selectors is not published", func(t *testing.T) {
		svc, rd, adm, buf := newFixture(t)
		moved := runningJob(7)
		moved.GCodeFile = "other.3mf"
		rd.set(serial, moved)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
		// outcome stays ready — the fetch itself succeeded — while
		// published separates it from a served result.
		expect(t, lastRecord(t, buf), catReady, phaseNone, false, 1, 0)
	})

	t.Run("transient failure schedules a retry", func(t *testing.T) {
		svc, _, adm, buf := newFixture(t)
		svc.finish(adm, Result{}, &testError{cat: catTransport})
		expect(t, lastRecord(t, buf), catTransport, phaseNone, false, 1, 30)
	})

	t.Run("login failure keeps credential text out", func(t *testing.T) {
		svc, _, adm, buf := newFixture(t)
		// The injected secret carries a newline: the serializer must
		// escape it into one parseable record rather than forge a line.
		secret := "hunter2-secret\nmarker"
		err := classifyAttempt(context.Background(),
			fmt.Errorf("530 login incorrect for user u pass %s", secret), phaseLogin)
		svc.finish(adm, Result{}, err)
		expect(t, lastRecord(t, buf), catTransport, phaseLogin, false, 1, 30)
		for _, leak := range []string{secret, "530 login incorrect"} {
			if strings.Contains(buf.String(), leak) {
				t.Errorf("log leaked %q", leak)
			}
		}
	})

	t.Run("size failure keeps reply text out", func(t *testing.T) {
		svc, _, adm, buf := newFixture(t)
		err := ftpsErrAt(phaseSize, catTransport,
			&textproto.Error{Code: 550, Msg: "Permission denied secret-marker-2"})
		svc.finish(adm, Result{}, err)
		expect(t, lastRecord(t, buf), catTransport, phaseSize, false, 1, 30)
		for _, leak := range []string{"secret-marker-2", "Permission denied", "550"} {
			if strings.Contains(buf.String(), leak) {
				t.Errorf("log leaked %q", leak)
			}
		}
	})

	t.Run("accepted partial archive stays published as archive phase", func(t *testing.T) {
		svc, _, adm, buf := newFixture(t)
		svc.finish(adm, Result{Metadata: &Metadata{Source: sourcePrinter3mf, Title: strPtr("T")}},
			archiveErr(catImageMissing, "plate image absent"))
		expect(t, lastRecord(t, buf), catImageMissing, phaseArchive, true, 1, 0)
	})

	t.Run("cancelled refunds the budget and names no phase", func(t *testing.T) {
		svc, _, adm, buf := newFixture(t)
		svc.finish(adm, Result{}, context.Canceled)
		expect(t, lastRecord(t, buf), catCancelled, phaseNone, false, 0, 0)
		if rec, _ := attemptRecord(svc, serial); rec.count != 0 {
			t.Fatalf("record = %+v, want refunded count 0", rec)
		}
	})
}

func TestRetainedResultAfterTerminal(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := runningJob(7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	png := pngBytes(t, 2, 2)
	adm := beginAttempt(t, svc, "S1", job)
	svc.finish(adm, Result{PNG: png}, nil)

	// The same generation finishes; the result stays, current flips off.
	done := job
	done.Active = false
	done.State = "FINISH"
	rd.set("S1", done)

	res, ok := svc.Lookup("S1", true)
	if !ok || res.Preview.Status != StatusReady {
		t.Fatalf("Lookup = %+v, %v; want retained ready result", res.Preview, ok)
	}
	if res.Preview.Current {
		t.Fatal("current = true after terminal report, want false")
	}
	if res.Preview.JobName != job.Name {
		t.Fatalf("job_name = %q, want retained identity", res.Preview.JobName)
	}
	if res.Preview.ImageURL == nil || !slices.Equal(res.PNG, png) {
		t.Fatal("retained payload missing image")
	}
}

// TestPlateSelectorChangeInvalidatesAndReadmits pins selector ownership: a
// plate selector change after publication invalidates the entry, and the
// next tick re-admits within the same generation's budget.
func TestPlateSelectorChangeInvalidatesAndReadmits(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := runningJob(7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	started := make(chan struct{}, 4)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		return Result{PNG: pngBytes(t, 2, 2)}, nil
	}
	adm := beginAttempt(t, svc, "S1", job)
	svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
	if res, _ := svc.Lookup("S1", true); res.Preview.Status != StatusReady {
		t.Fatalf("setup did not publish: %+v", res.Preview)
	}

	// The printer later reports a different plate selector: the entry no
	// longer serves, and the next tick re-admits within budget.
	moved := job
	moved.PlateIndex = intPtr(2)
	rd.set("S1", moved)
	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending || res.PNG != nil || res.Preview.ImageURL != nil {
		t.Fatalf("moved selectors served stale payload: %+v", res.Preview)
	}
	svc.mu.Lock()
	svc.worker = false // beginAttempt reserved the slot; free it for step
	svc.mu.Unlock()
	clk.Set(base.Add(time.Second))
	svc.step(base.Add(time.Second))
	await(t, started)
	svc.wg.Wait()
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d, want 1 re-admission within budget", got)
	}
	if rec, _ := attemptRecord(svc, "S1"); rec.gen != 7 || rec.count != 2 {
		t.Fatalf("record = %+v, want gen 7 count 2", rec)
	}
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusReady {
		t.Fatalf("re-admitted result = %+v, want ready for the new selectors", res.Preview)
	}

	// New preparation: new generation, pending, still no attempt while
	// the printer is not executing.
	prep := job
	prep.Generation = 8
	prep.State = "PREPARE"
	rd.set("S1", prep)
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending || !res.Preview.Current || res.Preview.Reason != reasonWaitingForPrint {
		t.Fatalf("new preparation projection = %+v, want pending waiting_for_print", res.Preview)
	}
	clk.Set(base.Add(2 * time.Second))
	svc.step(base.Add(2 * time.Second))
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d, want 1: PREPARE must not admit", got)
	}
}

func TestLookupBoundaryDoubleSample(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := runningJob(7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	png := pngBytes(t, 2, 2)
	adm := beginAttempt(t, svc, "S1", job)
	svc.finish(adm, Result{PNG: png}, nil)

	next := runningJob(8) // generation boundary between the two samples
	next.Name = "b.3mf"
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
	rd := newJobReader("S1", runningJob(7))
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
	rd := newJobReader("S1", runningJob(7))
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
	at := base.Add(time.Second)
	rd.set("S1", runningJob(7))
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
	rd := newJobReader("S1", runningJob(7))
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
	at := base.Add(time.Second)
	rd.set("S1", runningJob(7))
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
	// cancelled attempt; the projection names the shutdown.
	svc.step(base.Add(61 * time.Second))
	if counts.count() != 1 {
		t.Fatalf("calls = %d after close, want 1", counts.count())
	}
	res, ok := svc.Lookup("S1", true)
	if !ok || res.Preview.Status != StatusUnavailable || res.Preview.Reason != reasonProxyStopping {
		t.Fatalf("Lookup after cancelled attempt = %+v; want unavailable proxy_stopping", res.Preview)
	}
}

// TestSchedulerSurvivesDisconnectPinsGenerationCancel proves the transfer
// gate: a connectivity drop mid-transfer never cancels, while a new job
// generation cancels the attempt and refunds its budget.
func TestSchedulerSurvivesDisconnectPinsGenerationCancel(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", runningJob(7))
	svc := newTestService(t, conn, rd, clk, "S1")
	started := make(chan struct{}, 4)
	done := make(chan struct{})
	var ferr error
	release := make(chan struct{})
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		started <- struct{}{}
		select {
		case <-ctx.Done():
			ferr = ctx.Err()
		case <-release:
		}
		close(done)
		return Result{}, ferr
	}
	at := base.Add(time.Second)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	// The printer drops mid-transfer: the attempt keeps running.
	conn.setConnected("S1", false)
	clk.Set(at.Add(time.Second))
	svc.step(at.Add(time.Second))
	select {
	case <-done:
		t.Fatal("connectivity drop cancelled the transfer")
	default:
	}

	// A new job generation cancels it and refunds the consumed budget.
	rd.set("S1", runningJob(8))
	clk.Set(at.Add(2 * time.Second))
	svc.step(at.Add(2 * time.Second))
	await(t, done)
	svc.wg.Wait()
	if !errors.Is(ferr, context.Canceled) {
		t.Fatalf("fetch error = %v, want context.Canceled", ferr)
	}
	if rec, _ := attemptRecord(svc, "S1"); rec.gen != 7 || rec.count != 0 {
		t.Fatalf("record = %+v, want gen 7 refunded to count 0", rec)
	}
}

func TestStartCloseIdempotent(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", runningJob(7))
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
	job := runningJob(7)
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
	adm := beginAttempt(t, svc, "S1", job)
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
// cache with real report timestamps and real step times: waiting while
// preparing, one attempt on the first RUNNING tick, the attempt surviving
// a metadata delta, and the live projection tracking the job.
func TestRealCacheSchedulerFlow(t *testing.T) {
	printers := []config.Printer{{Serial: "S1", Address: "s1.invalid:8883"}}
	cache := telemetry.NewCache(printers, testLogger())
	conn := newFakeConn("S1")
	svc := New(printers, cache, conn, testLogger())
	counts := &fetchCounter{}
	started := make(chan struct{}, 4)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		return Result{}, &testError{cat: catTransport}
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
	if res.Preview.Reason != reasonWaitingForPrint {
		t.Fatalf("reason = %q, want waiting_for_print", res.Preview.Reason)
	}

	cache.ObserveReport("S1", 2, 1, report(map[string]any{"gcode_state": "RUNNING"}))
	svc.step(time.Now())
	await(t, started)
	svc.wg.Wait()
	if counts.count() != 1 {
		t.Fatalf("calls = %d, want 1 on the first RUNNING tick", counts.count())
	}

	// A metadata-only delta never cancels and never re-admits before the
	// retry time: the projection names the backoff.
	cache.ObserveReport("S1", 3, 1, report(map[string]any{"gcode_file": "other.3mf"}))
	res, _ = svc.Lookup("S1", false)
	if res.Preview.Status != StatusPending || res.Preview.Reason != reasonRetryWait {
		t.Fatalf("view after metadata delta = %+v, want pending retry_wait", res.Preview)
	}

	cache.ObserveReport("S1", 4, 1, report(map[string]any{"gcode_state": "FINISH"}))
	res, _ = svc.Lookup("S1", false)
	if res.Preview.Status != StatusUnavailable || res.Preview.Current {
		t.Fatalf("projection after finish = %+v, want unavailable", res.Preview)
	}
}

// TestTransferSurvivesPause proves pause never invalidates: a tick that
// finds the job paused leaves the transfer running, and its result still
// publishes.
func TestTransferSurvivesPause(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", runningJob(7))
	svc := newTestService(t, conn, rd, clk, "S1")
	png := pngBytes(t, 2, 2)
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		started <- struct{}{}
		<-release
		return Result{PNG: png}, nil
	}
	at := base.Add(time.Second)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	paused := runningJob(7)
	paused.State = "PAUSE"
	rd.set("S1", paused)
	clk.Set(at.Add(time.Second))
	svc.step(at.Add(time.Second)) // the transfer slot is busy; nothing cancels
	close(release)
	svc.wg.Wait()

	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusReady || !slices.Equal(res.PNG, png) {
		t.Fatalf("view = %+v, want the ready result published across the pause", res.Preview)
	}
}

// TestTransferCancelBeforeDial pins the pre-dial recheck: a new job
// generation between admission and the fetch call must fail the recheck,
// refund the budget, and skip the dial. A pause in the same window passes.
func TestTransferCancelBeforeDial(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := runningJob(7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	adm := testAdmission("S1", job)
	if !svc.transferValid(adm, base) {
		t.Fatal("fresh admission must pass the pre-dial recheck")
	}
	paused := job
	paused.State = "PAUSE"
	rd.set("S1", paused)
	if !svc.transferValid(adm, base) {
		t.Fatal("pause must pass the pre-dial recheck")
	}
	rd.set("S1", runningJob(8))
	if svc.transferValid(adm, base) {
		t.Fatal("generation change must fail the pre-dial recheck")
	}

	counts := &fetchCounter{}
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		return Result{PNG: pngBytes(t, 2, 2)}, nil
	}
	done := make(chan struct{})
	reserveSlot(svc)
	if !svc.begin("S1", adm) {
		t.Fatal("begin refused a fresh attempt")
	}
	svc.wg.Add(1) // step registers the slot before launching; mirror it here
	go func() {
		svc.transfer(adm)
		close(done)
	}()
	await(t, done)
	if counts.count() != 0 {
		t.Fatalf("calls = %d, want 0: fetch must not dial after a failed preflight", counts.count())
	}
	// The cancelled pre-dial attempt refunds its budget: the replacement
	// generation starts fresh, and the stale admission recorded nothing.
	if rec, _ := attemptRecord(svc, "S1"); rec.gen != 7 || rec.count != 0 {
		t.Fatalf("record = %+v, want gen 7 refunded to count 0", rec)
	}
}

// TestPublishAfterFinishDuringTransfer: the selectors still match when the
// same generation finished mid-transfer, so the late success still
// publishes and is retained with current flipped off.
func TestPublishAfterFinishDuringTransfer(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", runningJob(7))
	svc := newTestService(t, conn, rd, clk, "S1")
	png := pngBytes(t, 2, 2)
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		started <- struct{}{}
		<-release
		return Result{PNG: png}, nil
	}
	at := base.Add(time.Second)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	// The generation finishes while the transfer is still in flight; no
	// tick runs, so the outcome lands once the fetch returns.
	fin := runningJob(7)
	fin.Active = false
	fin.State = "FINISH"
	rd.set("S1", fin)
	close(release)
	svc.wg.Wait()

	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusReady || !slices.Equal(res.PNG, png) || res.Preview.Current {
		t.Fatalf("late success after finish = %+v, want retained ready not current", res.Preview)
	}
}

// TestPublishAfterPauseDuringTransfer: a pause while transferring never
// discards the result; the generation reads ready.
func TestPublishAfterPauseDuringTransfer(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", runningJob(7))
	svc := newTestService(t, conn, rd, clk, "S1")
	png := pngBytes(t, 2, 2)
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		started <- struct{}{}
		<-release
		return Result{PNG: png}, nil
	}
	at := base.Add(time.Second)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	paused := runningJob(7)
	paused.State = "PAUSE"
	rd.set("S1", paused)
	close(release)
	svc.wg.Wait()

	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusReady || !slices.Equal(res.PNG, png) {
		t.Fatalf("paused transfer outcome = %+v, want ready", res.Preview)
	}
}

// TestInFlightAndRetryStatuses pins the pending lifecycle: downloading
// while the transfer is in flight, retry_wait after a transient failure,
// and a fresh downloading projection for a new generation.
func TestInFlightAndRetryStatuses(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", runningJob(7))
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

	// Not yet attempted: pending, downloading.
	res, _ := svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending || res.Preview.Reason != reasonDownloading {
		t.Fatalf("fresh view = %+v, want pending downloading", res.Preview)
	}
	at := base.Add(time.Second)
	clk.Set(at)
	svc.step(at)
	await(t, started)

	// The attempt is registered for scheduler cancellation while in
	// flight, and its registration is gone before the slot reopens.
	if svc.active == nil || svc.active.adm.gen != 7 {
		t.Fatal("in-flight transfer not registered for cancellation")
	}

	// Transfer in progress for this generation: pending, downloading.
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending || res.Preview.Reason != reasonDownloading {
		t.Fatalf("in-flight view = %+v, want pending downloading", res.Preview)
	}
	close(release)
	svc.wg.Wait()
	if svc.active != nil {
		t.Fatal("active transfer outlived the worker slot")
	}

	// Transient failure: pending, retry_wait.
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending || res.Preview.Reason != reasonRetryWait {
		t.Fatalf("retry view = %+v, want pending retry_wait", res.Preview)
	}
	if counts.count() != 1 {
		t.Fatalf("calls = %d, want 1", counts.count())
	}

	// A new generation ignores the old record: pending, downloading.
	rd.set("S1", runningJob(8))
	res, _ = svc.Lookup("S1", true)
	if res.Preview.Status != StatusPending || res.Preview.Reason != reasonDownloading {
		t.Fatalf("new generation view = %+v, want pending downloading", res.Preview)
	}
}

// TestBeginReservationRules pins begin's guards: the budget is consumed
// only by a caller holding the reserved sole transfer slot, up to
// maxAttempts per generation outside backoff, and never on a closed
// service. A new generation replaces the old record.
func TestBeginReservationRules(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	conn := newFakeConn("S1")
	job := runningJob(7)
	newSvc := func() *Service {
		return newTestService(t, conn, newJobReader("S1", job), &testClock{now: base}, "S1")
	}
	adm := func() *admission { return testAdmission("S1", job) }

	// Without the reserved sole slot, begin consumes nothing.
	unreserved := newSvc()
	if unreserved.begin("S1", adm()) {
		t.Fatal("begin without the reserved sole slot must fail")
	}
	if _, ok := attemptRecord(unreserved, "S1"); ok {
		t.Fatal("a refused reservation consumed the budget")
	}

	// With the slot reserved, begins consume the budget up to the cap.
	svc := newSvc()
	reserveSlot(svc)
	for i := 1; i <= maxAttempts; i++ {
		if !svc.begin("S1", adm()) {
			t.Fatalf("begin %d with the reserved sole slot must succeed", i)
		}
	}
	if svc.begin("S1", adm()) {
		t.Fatal("begin past maxAttempts must fail")
	}
	if rec, _ := attemptRecord(svc, "S1"); rec.count != maxAttempts {
		t.Fatalf("record = %+v, want count %d", rec, maxAttempts)
	}
	svc.mu.Lock()
	inflight := svc.inflight["S1"]
	svc.mu.Unlock()
	if inflight == nil || inflight.gen != 7 {
		t.Fatal("the latest admission must stay registered as in flight")
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
	if _, ok := attemptRecord(closedSvc, "S1"); ok {
		t.Fatal("a closed-service refusal consumed the budget")
	}

	// A new generation replaces the spent record with a fresh budget.
	next := runningJob(8)
	nextAdm := testAdmission("S1", next)
	if !svc.begin("S1", nextAdm) {
		t.Fatal("begin for a new generation must succeed")
	}
	if rec, _ := attemptRecord(svc, "S1"); rec.gen != 8 || rec.count != 1 {
		t.Fatalf("record = %+v, want fresh gen 8 count 1", rec)
	}
}

// TestPreflightBarrierBetweenReservationAndAdmission pins the window
// between reserving the sole slot and consuming the budget: a pause in
// the preflight re-read still proceeds, a generation change or a
// disconnect in the window releases the reservation without consuming
// the budget, and a closed service admits nothing.
func TestPreflightBarrierBetweenReservationAndAdmission(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	at := base.Add(time.Second)

	t.Run("pause in preflight still proceeds", func(t *testing.T) {
		clk := &testClock{now: base}
		conn := newFakeConn("S1")
		rd := newJobReader("S1", runningJob(7))
		svc := newTestService(t, conn, rd, clk, "S1")
		counts := &fetchCounter{}
		started := make(chan struct{}, 4)
		svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
			counts.add(p.Serial)
			started <- struct{}{}
			return Result{}, &testError{cat: catTransport}
		}
		// The first read passes eligibility; the preflight re-read sees
		// the pause that landed inside the reservation window. Pause is
		// eligible, so the attempt proceeds.
		paused := runningJob(7)
		paused.State = "PAUSE"
		rd.seq = []telemetry.JobView{runningJob(7), paused}
		clk.Set(at)
		svc.step(at)
		await(t, started)
		svc.wg.Wait()
		if counts.count() != 1 {
			t.Fatalf("calls = %d, want 1: the pause must not stop the dial", counts.count())
		}
	})

	t.Run("generation change in preflight releases without consuming", func(t *testing.T) {
		clk := &testClock{now: base}
		conn := newFakeConn("S1")
		rd := newJobReader("S1", runningJob(7))
		svc := newTestService(t, conn, rd, clk, "S1")
		counts := &fetchCounter{}
		started := make(chan struct{}, 4)
		svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
			counts.add(p.Serial)
			started <- struct{}{}
			return Result{}, &testError{cat: catTransport}
		}
		// The preflight re-read sees the new generation that started
		// inside the reservation window: the reservation is for gen 7
		// but the live job is gen 8, so the slot releases without
		// consuming either generation's budget.
		rd.seq = []telemetry.JobView{runningJob(7), runningJob(8), runningJob(8)}
		rd.set("S1", runningJob(8))
		clk.Set(at)
		svc.step(at)
		select {
		case <-started:
			t.Fatal("fetch admitted across a generation change in preflight")
		default:
		}
		if counts.count() != 0 {
			t.Fatalf("calls = %d, want 0", counts.count())
		}
		if _, ok := attemptRecord(svc, "S1"); ok {
			t.Fatal("a failed preflight consumed the budget")
		}
		// Both budgets are intact: the new generation admits next tick.
		clk.Set(at.Add(time.Second))
		svc.step(at.Add(time.Second))
		await(t, started)
		svc.wg.Wait()
		if counts.count() != 1 {
			t.Fatalf("calls = %d, want exactly 1 after the barrier cleared", counts.count())
		}
	})

	t.Run("disconnect in preflight releases without consuming", func(t *testing.T) {
		clk := &testClock{now: base}
		conn := newFakeConn("S1")
		rd := newJobReader("S1", runningJob(7))
		svc := newTestService(t, conn, rd, clk, "S1")
		counts := &fetchCounter{}
		svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
			counts.add(p.Serial)
			return Result{}, &testError{cat: catTransport}
		}
		// The printer drops between the first read and the preflight
		// re-read: status is sampled fresh, so the disconnect stops the
		// dial without consuming the budget.
		conn.setConnected("S1", false)
		clk.Set(at)
		svc.step(at)
		if counts.count() != 0 {
			t.Fatalf("calls = %d, want 0: the disconnect must stop the dial", counts.count())
		}
		if _, ok := attemptRecord(svc, "S1"); ok {
			t.Fatal("a failed preflight consumed the budget")
		}
	})

	t.Run("closed service admits nothing", func(t *testing.T) {
		clk := &testClock{now: base}
		conn := newFakeConn("S1")
		rd := newJobReader("S1", runningJob(7))
		svc := newTestService(t, conn, rd, clk, "S1")
		svc.mu.Lock()
		svc.closed = true
		svc.worker = true // reservation held when Close landed
		svc.mu.Unlock()
		if adm := svc.admit(at); adm != nil {
			t.Fatal("admission on a closed service must fail")
		}
		if _, ok := attemptRecord(svc, "S1"); ok {
			t.Fatal("a closed-service refusal consumed the budget")
		}
	})
}

// TestLookupReasonTable covers one case per liveResult row: each wait and
// failure projects its exact status and reason.
func TestLookupReasonTable(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	newSvc := func(job telemetry.JobView, connected bool) (*Service, *jobReader, *testClock) {
		clk := &testClock{now: base}
		conn := newFakeConn("S1")
		conn.setConnected("S1", connected)
		rd := newJobReader("S1", job)
		return newTestService(t, conn, rd, clk, "S1"), rd, clk
	}

	t.Run("valid entry serves its reason", func(t *testing.T) {
		job := runningJob(7)
		svc, _, _ := newSvc(job, true)
		adm := beginAttempt(t, svc, "S1", job)
		md := &Metadata{Source: sourcePrinter3mf, Title: strPtr("T")}
		svc.finish(adm, Result{Metadata: md}, archiveErr(catImageMissing, "plate image missing"))
		res, _ := svc.Lookup("S1", false)
		if res.Preview.Status != StatusUnavailable || res.Preview.Reason != catImageMissing {
			t.Fatalf("view = %+v, want unavailable image_missing", res.Preview)
		}
	})

	t.Run("closed yields proxy_stopping", func(t *testing.T) {
		svc, _, _ := newSvc(runningJob(7), true)
		svc.mu.Lock()
		svc.closed = true
		svc.mu.Unlock()
		res, _ := svc.Lookup("S1", false)
		if res.Preview.Status != StatusUnavailable || res.Preview.Reason != reasonProxyStopping {
			t.Fatalf("view = %+v, want unavailable proxy_stopping", res.Preview)
		}
	})

	t.Run("inactive has no reason", func(t *testing.T) {
		job := runningJob(7)
		job.Active = false
		svc, _, _ := newSvc(job, true)
		res, _ := svc.Lookup("S1", false)
		if res.Preview.Status != StatusUnavailable || res.Preview.Reason != "" {
			t.Fatalf("view = %+v, want unavailable without reason", res.Preview)
		}
	})

	t.Run("in flight reads downloading", func(t *testing.T) {
		job := runningJob(7)
		svc, _, _ := newSvc(job, true)
		beginAttempt(t, svc, "S1", job)
		res, _ := svc.Lookup("S1", false)
		if res.Preview.Status != StatusPending || res.Preview.Reason != reasonDownloading {
			t.Fatalf("view = %+v, want pending downloading", res.Preview)
		}
	})

	t.Run("final reads its failure reason", func(t *testing.T) {
		job := runningJob(7)
		svc, _, _ := newSvc(job, true)
		adm := beginAttempt(t, svc, "S1", job)
		svc.finish(adm, Result{}, archiveErr(catOversized, "archive entry count"))
		res, _ := svc.Lookup("S1", false)
		if res.Preview.Status != StatusUnavailable || res.Preview.Reason != catOversized {
			t.Fatalf("view = %+v, want unavailable oversized", res.Preview)
		}
	})

	t.Run("backed off reads retry_wait", func(t *testing.T) {
		job := runningJob(7)
		svc, _, _ := newSvc(job, true)
		adm := beginAttempt(t, svc, "S1", job)
		svc.finish(adm, Result{}, &testError{cat: catTimeout})
		res, _ := svc.Lookup("S1", false)
		if res.Preview.Status != StatusPending || res.Preview.Reason != reasonRetryWait {
			t.Fatalf("view = %+v, want pending retry_wait", res.Preview)
		}
	})

	t.Run("disconnected reads printer_offline", func(t *testing.T) {
		svc, _, _ := newSvc(runningJob(7), false)
		res, _ := svc.Lookup("S1", false)
		if res.Preview.Status != StatusPending || res.Preview.Reason != reasonPrinterOffline {
			t.Fatalf("view = %+v, want pending printer_offline", res.Preview)
		}
	})

	t.Run("preparing reads waiting_for_print", func(t *testing.T) {
		for _, state := range []string{"PREPARE", "SLICING"} {
			job := runningJob(7)
			job.State = state
			svc, _, _ := newSvc(job, true)
			res, _ := svc.Lookup("S1", false)
			if res.Preview.Status != StatusPending || res.Preview.Reason != reasonWaitingForPrint {
				t.Fatalf("%s: view = %+v, want pending waiting_for_print", state, res.Preview)
			}
		}
	})

	t.Run("no identity reads waiting_for_file_name", func(t *testing.T) {
		job := runningJob(7)
		job.Name = ""
		job.GCodeFile = ""
		svc, _, _ := newSvc(job, true)
		res, _ := svc.Lookup("S1", false)
		if res.Preview.Status != StatusPending || res.Preview.Reason != reasonWaitingForFileName {
			t.Fatalf("view = %+v, want pending waiting_for_file_name", res.Preview)
		}
	})

	t.Run("busy slot reads queued", func(t *testing.T) {
		svc, _, _ := newSvc(runningJob(7), true)
		svc.mu.Lock()
		svc.worker = true // another printer holds the sole slot
		svc.mu.Unlock()
		res, _ := svc.Lookup("S1", false)
		if res.Preview.Status != StatusPending || res.Preview.Reason != reasonQueued {
			t.Fatalf("view = %+v, want pending queued", res.Preview)
		}
	})

	t.Run("fresh job reads downloading", func(t *testing.T) {
		svc, _, _ := newSvc(runningJob(7), true)
		res, _ := svc.Lookup("S1", false)
		if res.Preview.Status != StatusPending || res.Preview.Reason != reasonDownloading {
			t.Fatalf("view = %+v, want pending downloading", res.Preview)
		}
	})
}

// TestCloseDuringRetryWaitStopsScheduling pins shutdown inside backoff: the
// projection names the shutdown and no further attempt starts.
func TestCloseDuringRetryWaitStopsScheduling(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	rd := newJobReader("S1", runningJob(7))
	svc := newTestService(t, conn, rd, clk, "S1")
	counts := &fetchCounter{}
	started := make(chan struct{}, 4)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		counts.add(p.Serial)
		started <- struct{}{}
		return Result{}, &testError{cat: catTransport}
	}
	clk.Set(base)
	svc.step(base)
	await(t, started)
	svc.wg.Wait()
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
	svc.Close()
	res, _ := svc.Lookup("S1", false)
	if res.Preview.Status != StatusUnavailable || res.Preview.Reason != reasonProxyStopping {
		t.Fatalf("view = %+v, want unavailable proxy_stopping", res.Preview)
	}
	clk.Set(base.Add(time.Hour))
	svc.step(base.Add(time.Hour))
	if got := counts.count(); got != 1 {
		t.Fatalf("calls = %d after close, want 1", got)
	}
}

// TestDisplayPanels pins the module Display hook: nil for disabled, clean
// ready, and reason-free views; a Print details panel naming the exact
// reason otherwise, info while pending and warn when unavailable.
func TestDisplayPanels(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	newSvc := func(job telemetry.JobView) (*Service, *jobReader) {
		clk := &testClock{now: base}
		rd := newJobReader("S1", job)
		return newTestService(t, newFakeConn("S1"), rd, clk, "S1"), rd
	}

	svcUnconfigured, _ := newSvc(runningJob(7))
	if d := svcUnconfigured.Display("OTHER"); d != nil {
		t.Fatalf("Display(OTHER) = %+v, want nil", d)
	}

	t.Run("idle printer is nil", func(t *testing.T) {
		job := runningJob(7)
		job.Active = false
		svc, _ := newSvc(job)
		if d := svc.Display("S1"); d != nil {
			t.Fatalf("Display = %+v, want nil for a reason-free view", d)
		}
	})

	t.Run("clean ready is nil", func(t *testing.T) {
		job := runningJob(7)
		svc, _ := newSvc(job)
		adm := beginAttempt(t, svc, "S1", job)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
		if d := svc.Display("S1"); d != nil {
			t.Fatalf("Display = %+v, want nil for a clean ready", d)
		}
	})

	t.Run("ready with reason shows the panel", func(t *testing.T) {
		job := runningJob(7)
		svc, _ := newSvc(job)
		adm := beginAttempt(t, svc, "S1", job)
		svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, archiveErr(catArchive, "slice info bad"))
		d := svc.Display("S1")
		if d == nil || d.Panel == nil || d.Panel.Title != "Print details" {
			t.Fatalf("Display = %+v, want a Print details panel", d)
		}
		if len(d.Panel.Rows) != 1 || d.Panel.Rows[0].Value != reasonText(catArchive) || d.Panel.Rows[0].Tone != "warn" {
			t.Fatalf("rows = %+v, want the archive warn text", d.Panel.Rows)
		}
	})

	t.Run("retry wait shows info text", func(t *testing.T) {
		job := runningJob(7)
		svc, _ := newSvc(job)
		adm := beginAttempt(t, svc, "S1", job)
		svc.finish(adm, Result{}, &testError{cat: catTimeout})
		d := svc.Display("S1")
		if d == nil || d.Panel == nil {
			t.Fatalf("Display = %+v, want a panel", d)
		}
		row := d.Panel.Rows[0]
		if row.Label != "Status" || row.Value != reasonText(reasonRetryWait) || row.Tone != "info" {
			t.Fatalf("row = %+v, want the retry_wait info text", row)
		}
	})

	t.Run("final failure shows warn text", func(t *testing.T) {
		job := runningJob(7)
		svc, _ := newSvc(job)
		adm := beginAttempt(t, svc, "S1", job)
		svc.finish(adm, Result{}, archiveErr(catOversized, "archive entry count"))
		d := svc.Display("S1")
		if d == nil || d.Panel == nil {
			t.Fatalf("Display = %+v, want a panel", d)
		}
		row := d.Panel.Rows[0]
		if row.Label != "Status" || row.Value != reasonText(catOversized) || row.Tone != "warn" {
			t.Fatalf("row = %+v, want the oversized warn text", row)
		}
	})

	t.Run("reason text covers the catalog", func(t *testing.T) {
		cases := map[string]string{
			reasonWaitingForPrint:    "Waiting for the print to start",
			reasonWaitingForFileName: "Waiting for the printer to report the file name",
			reasonPrinterOffline:     "Waiting for the printer to reconnect",
			reasonQueued:             "Waiting for another print file download to finish",
			reasonDownloading:        "Downloading the print file from the printer",
			reasonRetryWait:          "Download failed, retrying shortly",
			reasonProxyStopping:      "Proxy stopping",
			reasonSelectionChanged:   "Printer changed the selected plate or file",
			catNotFound:              "Print file not found on the printer",
			catTimeout:               "Print file download stalled",
			catTransport:             "Could not read the print file from the printer",
			catArchive:               "Print file could not be read",
			catOversized:             "Print file exceeds the size limit",
			catAmbiguousPlate:        "Could not tell which plate is printing",
			catImageMissing:          "Print file has no preview image",
		}
		for reason, want := range cases {
			if got := reasonText(reason); got != want {
				t.Errorf("reasonText(%q) = %q, want %q", reason, got, want)
			}
		}
	})
}

// TestLookupSelectorChangeBetweenReadsServesLive pins the Lookup double
// sample: when plate_idx or gcode_file changes between the two reads, the
// cached entry from the first read must not be served with the second
// read's job name.
func TestLookupSelectorChangeBetweenReadsServesLive(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	clk := &testClock{now: base}
	conn := newFakeConn("S1")
	job := runningJob(7)
	rd := newJobReader("S1", job)
	svc := newTestService(t, conn, rd, clk, "S1")
	png := pngBytes(t, 2, 2)
	adm := beginAttempt(t, svc, "S1", job)
	svc.finish(adm, Result{PNG: png}, nil)

	cases := []struct {
		name   string
		mutate func(telemetry.JobView) telemetry.JobView
	}{
		{"plate index", func(j telemetry.JobView) telemetry.JobView { j.PlateIndex = intPtr(2); return j }},
		{"gcode file", func(j telemetry.JobView) telemetry.JobView { j.GCodeFile = "other_plate_2.3mf"; return j }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			moved := tc.mutate(job)
			moved.Name = "moved.3mf"
			rd.seq = []telemetry.JobView{job, moved}
			res, ok := svc.Lookup("S1", true)
			if !ok {
				t.Fatal("Lookup lost configured serial")
			}
			if res.Preview.Status == StatusReady || res.PNG != nil || res.Preview.ImageURL != nil {
				t.Fatalf("selector-moved lookup served old payload: %+v", res.Preview)
			}
			if res.Preview.JobName != moved.Name {
				t.Fatalf("job_name = %q, want %q", res.Preview.JobName, moved.Name)
			}
		})
	}
}

// TestFinishAcceptedSelectorsMovedExhaustedFinalizes pins the spent-budget
// end of an accepted transfer: two transient failures, then an accepted
// third attempt whose selectors moved, ends the generation as unavailable
// with selection_changed and one activity entry.
func TestFinishAcceptedSelectorsMovedExhaustedFinalizes(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	serial := "S1"
	clk := &testClock{now: base}
	conn := newFakeConn(serial)
	job := runningJob(7)
	rd := newJobReader(serial, job)
	svc := newTestService(t, conn, rd, clk, serial)
	log := activity.New([]config.Printer{{Serial: serial}})
	svc.SetActivity(log)

	releaseSlot := func() {
		svc.mu.Lock()
		svc.worker = false
		svc.mu.Unlock()
	}
	for range 2 {
		adm := beginAttempt(t, svc, serial, job)
		svc.finish(adm, Result{}, &testError{cat: catTransport})
		releaseSlot()
		clk.Advance(retryBase * time.Duration(2))
	}
	adm := beginAttempt(t, svc, serial, job)
	moved := job
	moved.PlateIndex = intPtr(2)
	moved.GCodeFile = "other_plate_2.3mf"
	rd.set(serial, moved)
	svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)

	rec, _ := attemptRecord(svc, serial)
	if !rec.final || rec.reason != reasonSelectionChanged || rec.count != maxAttempts {
		t.Fatalf("record = %+v, want final selection_changed count %d", rec, maxAttempts)
	}
	res, _ := svc.Lookup(serial, true)
	if res.Preview.Status != StatusUnavailable || res.Preview.Reason != reasonSelectionChanged {
		t.Fatalf("view = %+v, want unavailable selection_changed", res.Preview)
	}
	if res.PNG != nil || res.Preview.ImageURL != nil {
		t.Fatalf("finalized lookup served payload: %+v", res.Preview)
	}
	events := log.Recent(serial)
	if len(events) != 1 || events[0].Kind != "job_preview_unavailable" || events[0].Severity != activity.Info {
		t.Fatalf("activity = %+v, want one info job_preview_unavailable", events)
	}
	wantMsg := "Print details unavailable, " + strings.ToLower(reasonText(reasonSelectionChanged)[:1]) + reasonText(reasonSelectionChanged)[1:]
	if events[0].Message != wantMsg {
		t.Fatalf("message = %q, want %q", events[0].Message, wantMsg)
	}
}

// TestSpentPublishedEntryInvalidatedFinalizesAfterStep pins the late
// selector change: a published third-attempt entry invalidated later by a
// plate change becomes unavailable with selection_changed after the next
// step, with one activity entry.
func TestSpentPublishedEntryInvalidatedFinalizesAfterStep(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	serial := "S1"
	clk := &testClock{now: base}
	conn := newFakeConn(serial)
	job := runningJob(7)
	rd := newJobReader(serial, job)
	svc := newTestService(t, conn, rd, clk, serial)
	log := activity.New([]config.Printer{{Serial: serial}})
	svc.SetActivity(log)

	releaseSlot := func() {
		svc.mu.Lock()
		svc.worker = false
		svc.mu.Unlock()
	}
	for range 2 {
		adm := beginAttempt(t, svc, serial, job)
		svc.finish(adm, Result{}, &testError{cat: catTransport})
		releaseSlot()
		clk.Advance(retryBase * time.Duration(2))
	}
	adm := beginAttempt(t, svc, serial, job)
	svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
	releaseSlot()
	if res, _ := svc.Lookup(serial, true); res.Preview.Status != StatusReady {
		t.Fatalf("setup did not publish: %+v", res.Preview)
	}

	moved := job
	moved.PlateIndex = intPtr(2)
	rd.set(serial, moved)
	at := clk.Now().Add(time.Second)
	clk.Set(at)
	svc.step(at)
	svc.wg.Wait()

	rec, _ := attemptRecord(svc, serial)
	if !rec.final || rec.reason != reasonSelectionChanged {
		t.Fatalf("record = %+v, want final selection_changed", rec)
	}
	res, _ := svc.Lookup(serial, true)
	if res.Preview.Status != StatusUnavailable || res.Preview.Reason != reasonSelectionChanged {
		t.Fatalf("view = %+v, want unavailable selection_changed", res.Preview)
	}
	events := log.Recent(serial)
	if len(events) != 1 || events[0].Kind != "job_preview_unavailable" {
		t.Fatalf("activity = %+v, want one job_preview_unavailable", events)
	}
	// A further step records nothing more: the final entry is once only.
	at = at.Add(time.Second)
	clk.Set(at)
	svc.step(at)
	svc.wg.Wait()
	if events := log.Recent(serial); len(events) != 1 {
		t.Fatalf("activity = %+v, want exactly one entry", events)
	}
}

// TestFinishAcceptedDuringJobChangeDiscardsWithoutFinalizing pins the
// stale-generation finish: two transient failures, then an accepted third
// attempt whose job moves to a new generation before finish. No
// replacement attempt installs first, so the record still belongs to the
// old admission. finish discards the result, refunds the budget like a
// cancellation, and records nothing; the new generation admits fresh on
// the next step.
func TestFinishAcceptedDuringJobChangeDiscardsWithoutFinalizing(t *testing.T) {
	base := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	serial := "S1"
	clk := &testClock{now: base}
	conn := newFakeConn(serial)
	job := runningJob(7)
	rd := newJobReader(serial, job)
	svc := newTestService(t, conn, rd, clk, serial)
	log := activity.New([]config.Printer{{Serial: serial}})
	svc.SetActivity(log)

	releaseSlot := func() {
		svc.mu.Lock()
		svc.worker = false
		svc.mu.Unlock()
	}
	for range 2 {
		adm := beginAttempt(t, svc, serial, job)
		svc.finish(adm, Result{}, &testError{cat: catTransport})
		releaseSlot()
		clk.Advance(retryBase * time.Duration(2))
	}
	adm := beginAttempt(t, svc, serial, job)
	// The job moves to a new generation mid-transfer, with no
	// replacement attempt record installed first: the worker slot stays
	// held until finish settles.
	rd.set(serial, runningJob(8))
	svc.finish(adm, Result{PNG: pngBytes(t, 2, 2)}, nil)
	releaseSlot()

	if rec, _ := attemptRecord(svc, serial); rec.gen != 7 || rec.final || rec.count != 2 {
		t.Fatalf("record = %+v, want gen 7 refunded to count 2 without final", rec)
	}
	if events := log.Recent(serial); len(events) != 0 {
		t.Fatalf("activity = %+v, want no job_preview_unavailable for the obsolete attempt", events)
	}
	if res, _ := svc.Lookup(serial, true); res.PNG != nil || res.Preview.ImageURL != nil {
		t.Fatalf("lookup served stale payload: %+v", res.Preview)
	}

	// The new generation admits on the next step with a fresh budget.
	started := make(chan struct{}, 1)
	svc.fetch = func(ctx context.Context, p config.Printer, j telemetry.JobView) (Result, error) {
		started <- struct{}{}
		return Result{}, &testError{cat: catTransport}
	}
	at := clk.Now().Add(time.Second)
	clk.Set(at)
	svc.step(at)
	await(t, started)
	svc.wg.Wait()
	if rec, _ := attemptRecord(svc, serial); rec.gen != 8 || rec.count != 1 || rec.final {
		t.Fatalf("record = %+v, want fresh count 1 for gen 8", rec)
	}
}
