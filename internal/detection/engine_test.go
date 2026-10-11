package detection

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
)

// etClient scripts Gadget responses and counts requests.
type etClient struct {
	mu              sync.Mutex
	calls, contexts int
	process         func(int) (Result, error)
}

func (c *etClient) CreateContext(context.Context) (Session, error) {
	c.mu.Lock()
	c.contexts++
	c.mu.Unlock()
	return Session{ContextID: "context", ProcessURL: "primary", FallbackURL: "fallback"}, nil
}
func (c *etClient) Process(context.Context, string, []byte) (Result, error) {
	c.mu.Lock()
	c.calls++
	n, fn := c.calls, c.process
	c.mu.Unlock()
	if fn != nil {
		return fn(n)
	}
	return Result{Intervals: IntervalSec{1, 20}, PrintQuality: 8}, nil
}
func (c *etClient) counts() (int, int) { c.mu.Lock(); defer c.mu.Unlock(); return c.calls, c.contexts }

// etFrames supplies distinct immutable test frames.
type etFrames struct{ seq uint64 }

func (*etFrames) Acquire(string) bool { return true }
func (*etFrames) Release(string)      {}
func (f *etFrames) WaitFrame(string, context.Context, uint64, time.Duration) (Frame, bool) {
	f.seq++
	return Frame{JPEG: []byte("jpeg"), Seq: f.seq, Captured: time.Now()}, true
}

// etPauser counts commands without a speed API.
type etPauser struct {
	mu             sync.Mutex
	pauses, lights int
}

func (p *etPauser) Pause(string) error { p.mu.Lock(); defer p.mu.Unlock(); p.pauses++; return nil }
func (p *etPauser) Light(string, bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lights++
	return nil
}
func (*etPauser) Online(string) bool { return true }
func (p *etPauser) count() int       { p.mu.Lock(); defer p.mu.Unlock(); return p.pauses }

// etWorld uses the production document store and activity log.
type etWorld struct {
	engine   *Engine
	store    *printer.Store
	client   *etClient
	control  *etPauser
	activity *activity.Log
}

func newEtWorld(t *testing.T) *etWorld {
	t.Helper()
	printers := []config.Printer{{Serial: "S1", Model: "P1S"}}
	w := &etWorld{store: printer.NewStore(printers), client: &etClient{}, control: &etPauser{}, activity: activity.New(printers)}
	w.engine = New(printers, w.client, &etFrames{}, w.store, w.control, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.engine.SetActivity(w.activity)
	w.store.SetOnline("S1", true)
	return w
}
func (w *etWorld) report(state string, layer int) {
	w.store.Apply("S1", []byte(fmt.Sprintf(`{"print":{"gcode_state":%q,"layer_num":%d}}`, state, layer)))
}
func (w *etWorld) startSession(_ uint64, layer int) { w.report("RUNNING", layer) }
func (w *etWorld) status() *Status                  { return w.engine.DetectionStatus("S1").(*Status) }
func (w *etWorld) run(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = w.engine.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
}
func hasActivityKind(entries []activity.Entry, kind string) bool {
	for _, e := range entries {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

func TestFailureFlagPausesOnceAndUsesRecommendedInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newEtWorld(t)
		w.client.process = func(int) (Result, error) {
			return Result{Intervals: IntervalSec{1, 20}, PrintQuality: 2, PauseSuggested: true, FasterInspectionSuggested: true}, nil
		}
		w.startSession(1, 3)
		w.run(t)
		synctest.Wait()
		if w.control.count() != 1 {
			t.Fatal("failure did not pause once")
		}
		time.Sleep(19 * time.Second)
		synctest.Wait()
		if calls, _ := w.client.counts(); calls != 1 {
			t.Fatalf("early check: %d", calls)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if calls, contexts := w.client.counts(); calls != 2 || contexts != 1 || w.control.count() != 1 {
			t.Fatalf("calls=%d contexts=%d pauses=%d", calls, contexts, w.control.count())
		}
		w.report("PAUSE", 3)
		synctest.Wait()
		if w.status().PauseState != PauseConfirmed || !hasActivityKind(w.activity.Recent("S1"), "ai_pause_confirmed") {
			t.Fatal("pause report did not confirm outcome")
		}
		w.report("RUNNING", 4)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if w.control.count() != 1 {
			t.Fatal("resume rearmed pause latch")
		}
	})
}

func TestStaleResultDroppedAfterNewPrint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newEtWorld(t)
		release := make(chan struct{})
		w.client.process = func(n int) (Result, error) {
			if n == 1 {
				<-release
				return Result{Intervals: IntervalSec{1, 20}, PrintQuality: 1, PauseSuggested: true}, nil
			}
			return Result{Intervals: IntervalSec{1, 20}, PrintQuality: 9}, nil
		}
		w.startSession(1, 3)
		w.run(t)
		synctest.Wait()
		old := w.status().SessionID
		w.report("FINISH", 3)
		w.report("PREPARE", 0)
		w.report("RUNNING", 0)
		close(release)
		synctest.Wait()
		if w.control.count() != 0 || w.status().Quality != 9 || w.status().SessionID == old {
			t.Fatalf("stale result affected new print: %+v", w.status())
		}
		if _, contexts := w.client.counts(); contexts != 2 {
			t.Fatalf("contexts=%d", contexts)
		}
	})
}

func TestBootMidPrintToggleHTTP(t *testing.T) {
	for _, state := range []string{"RUNNING", "PAUSE", "PAUSED"} {
		t.Run(state, func(t *testing.T) {
			w := newEtWorld(t)
			w.report(state, 8)
			token := w.status().SessionID
			if token != "1" {
				t.Fatalf("boot counter=%q", token)
			}
			mux := http.NewServeMux()
			mux.Handle("PUT /detection/{serial}", http.HandlerFunc(w.engine.handlePut))
			for _, tc := range []struct {
				body string
				code int
			}{{`{"enabled":false,"session_id":"old"}`, 409}, {`{"enabled":false,"session_id":"` + token + `"}`, 200}, {`{"enabled":true}`, 200}} {
				rr := httptest.NewRecorder()
				mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/detection/S1", strings.NewReader(tc.body)))
				if rr.Code != tc.code {
					t.Fatalf("PUT=%d %s", rr.Code, rr.Body.String())
				}
			}
			if _, err := w.engine.SetDetectionEnabled("S1", false, token); err != nil {
				t.Fatal(err)
			}
			w.report("FINISH", 8)
			w.report("RUNNING", 0)
			if !w.status().Enabled || w.status().SessionID == token {
				t.Fatal("new print did not reset toggle")
			}
		})
	}
}

func TestPauseOutcomeFromReports(t *testing.T) {
	for _, tc := range []struct {
		state string
		layer int
		want  string
	}{{"RUNNING", 3, PausePending}, {"RUNNING", 4, PauseUnconfirmed}, {"PAUSE", 3, PauseConfirmed}, {"PAUSED", 3, PauseConfirmed}, {"FINISH", 3, PauseConfirmed}} {
		t.Run(tc.state+fmt.Sprint(tc.layer), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w := newEtWorld(t)
				w.client.process = func(int) (Result, error) {
					return Result{Intervals: IntervalSec{1, 20}, PrintQuality: 2, PauseSuggested: true}, nil
				}
				w.startSession(1, 3)
				w.run(t)
				synctest.Wait()
				w.report(tc.state, tc.layer)
				synctest.Wait()
				if got := w.status().PauseState; got != tc.want {
					t.Fatalf("outcome=%s want=%s", got, tc.want)
				}
			})
		})
	}
}

// TestInterruptionRecoversOnlyAfterConsecutiveSuccesses pins one
// interruption per failure run: a single success between failures is not a
// recovery, and recovery needs recoverAfter successes in a row.
func TestInterruptionRecoversOnlyAfterConsecutiveSuccesses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newEtWorld(t)
		w.client.process = func(n int) (Result, error) {
			if n == 1 || n == 3 {
				return Result{}, &APIError{Type: errTypeBackendThrottle, Status: 429, Host: "gadget-ric2.octoeverywhere.com"}
			}
			return Result{Intervals: IntervalSec{1, 20}, PrintQuality: 8}, nil
		}
		count := func(kind string) int {
			n := 0
			for _, e := range w.activity.Recent("S1") {
				if e.Kind == kind {
					n++
				}
			}
			return n
		}
		w.startSession(1, 3)
		w.run(t)
		synctest.Wait()
		if w.control.count() != 0 || count("ai_degraded") != 1 || !strings.Contains(w.status().Message, "gadget-ric2.octoeverywhere.com is overloaded") {
			t.Fatalf("first failure: degraded=%d message=%q", count("ai_degraded"), w.status().Message)
		}
		time.Sleep(30 * time.Second) // check 2 succeeds
		synctest.Wait()
		time.Sleep(20 * time.Second) // check 3 fails again
		synctest.Wait()
		if count("ai_recovered") != 0 || count("ai_degraded") != 1 || !strings.Contains(w.status().Message, "2 failed checks") {
			t.Fatalf("flapping: recovered=%d degraded=%d message=%q", count("ai_recovered"), count("ai_degraded"), w.status().Message)
		}
		for range recoverAfter - 1 { // checks 4 and 5 succeed
			time.Sleep(30 * time.Second)
			synctest.Wait()
		}
		if count("ai_recovered") != 0 {
			t.Fatal("recovered before enough consecutive successes")
		}
		time.Sleep(20 * time.Second) // check 6 is the third success in a row
		synctest.Wait()
		if count("ai_recovered") != 1 {
			t.Fatalf("not recovered after %d successes: %+v", recoverAfter, w.activity.Recent("S1"))
		}
	})
}
