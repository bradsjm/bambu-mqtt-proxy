package mcpserver

// Unit tests for the mcpserver package. Wire-level acceptance tests live in
// acceptance_test.go; this file drives handlers and the sampler directly so
// revision, change, and lifecycle invariants are checked deterministically.
//
// The sampler primes its baseline when the server is constructed. Tests
// therefore establish their own reference revision after construction and
// assert on deltas, never on absolute counter values.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/telemetry"
)

// fakeState is a StateSource over mutable maps.
type fakeState struct {
	mu       sync.Mutex
	states   map[string]telemetry.State
	sessions map[string]telemetry.SessionView
	// onState, when set, runs after each State read returns, outside the
	// mutex. Tests arm it to inject a change and a sampler step in the
	// middle of a read path; it must not touch fakeState itself, because
	// setSession from the hook would wait on the lock the caller just
	// released. Reads and writes of the field stay on the test goroutine.
	onState func(serial string)
}

func newFakeState(serials ...string) *fakeState {
	f := &fakeState{
		states:   make(map[string]telemetry.State),
		sessions: make(map[string]telemetry.SessionView),
	}
	for _, s := range serials {
		f.states[s] = telemetry.State{Serial: s}
	}
	return f
}

func (f *fakeState) State(serial string) (telemetry.State, bool) {
	f.mu.Lock()
	st, ok := f.states[serial]
	f.mu.Unlock()
	if f.onState != nil {
		f.onState(serial)
	}
	return st, ok
}

func (f *fakeState) Session(serial string) (telemetry.SessionView, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sv, ok := f.sessions[serial]
	return sv, ok
}

func (f *fakeState) Snapshot() []telemetry.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]telemetry.State, 0, len(f.states))
	for _, st := range f.states {
		out = append(out, st)
	}
	return out
}

func (f *fakeState) setSession(serial string, sv telemetry.SessionView) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions[serial] = sv
}

// fakeConn is a StatusSource over a mutable map.
type fakeConn struct {
	mu sync.Mutex
	st map[string]bool
}

func (f *fakeConn) Status() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]bool, len(f.st))
	for k, v := range f.st {
		out[k] = v
	}
	return out
}

func (f *fakeConn) set(serial string, connected bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.st[serial] = connected
}

// fakeGens is a GenerationSource over a mutable map.
type fakeGens struct {
	mu sync.Mutex
	g  map[string]uint64
}

func (f *fakeGens) Generation(serial string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.g[serial]
}

func (f *fakeGens) set(serial string, gen uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.g[serial] = gen
}

// fakeCams is a SnapshotSource with balanced Acquire/Release accounting.
type fakeCams struct {
	mu       sync.Mutex
	acquires int
	releases int
	frames   map[string]*camera.Frame
	waits    map[string]*camera.Frame
	statuses map[string]camera.Status
}

func newFakeCams() *fakeCams {
	return &fakeCams{
		frames:   make(map[string]*camera.Frame),
		waits:    make(map[string]*camera.Frame),
		statuses: make(map[string]camera.Status),
	}
}

func (f *fakeCams) Acquire(serial string) (chan struct{}, camera.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.statuses[serial]
	if !ok {
		st = camera.StatusOK
	}
	if st != camera.StatusOK {
		// The real Manager registers no consumer interest for a refused
		// serial, so refused attempts stay out of the acquire/release
		// balance that Release must close.
		return nil, st
	}
	f.acquires++
	return make(chan struct{}, 1), st
}

func (f *fakeCams) Release(serial string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
}

func (f *fakeCams) Latest(serial string) *camera.Frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frames[serial]
}

func (f *fakeCams) Wait(serial string, _ context.Context, after uint64, _ time.Duration) *camera.Frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	fm, ok := f.waits[serial]
	if !ok || fm == nil || fm.Seq <= after {
		return nil
	}
	return fm
}

func (f *fakeCams) balanced(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.acquires != f.releases {
		t.Fatalf("camera acquire/release imbalance: %d acquires, %d releases", f.acquires, f.releases)
	}
}

// fakeDet is a DetectionSource with a mutable status pointer.
type fakeDet struct {
	mu   sync.Mutex
	st   *detection.Status
	susp bool
	why  string
}

func (f *fakeDet) DetectionStatus(string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.st
}

func (f *fakeDet) AccountSuspended() (bool, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.susp, f.why
}

// fixture bundles one server with mutable fakes and a fixed clock.
type fixture struct {
	srv   *Server
	state *fakeState
	conn  *fakeConn
	gens  *fakeGens
	cams  *fakeCams
	det   *fakeDet
	now   time.Time
}

// newFixture builds a server over three printers: P001/P002 camera-capable
// (model P1S), P003 not (model X1C, refused by the camera source). The
// sampler is NOT started; tests call step explicitly.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	now := time.Now()
	st := newFakeState("P001", "P002", "P003")
	cams := newFakeCams()
	cams.statuses["P003"] = camera.StatusUnsupportedModel
	f := &fixture{
		state: st,
		conn:  &fakeConn{st: map[string]bool{}},
		gens:  &fakeGens{g: map[string]uint64{}},
		cams:  cams,
		det:   &fakeDet{},
		now:   now,
	}
	f.srv = New(Deps{
		Printers: []config.Printer{
			{Serial: "P001", Name: "Alpha", Model: "P1S", Address: "secret-a", Password: "pw-a"},
			{Serial: "P002", Name: "Beta", Model: "P1S", Address: "secret-b", Password: "pw-b"},
			{Serial: "P003", Name: "Gamma", Model: "X1C", Address: "secret-c", Password: "pw-c"},
		},
		State:        st,
		Connectivity: f.conn,
		Generations:  f.gens,
		Cameras:      cams,
		Detector:     f.det,
	})
	f.srv.now = func() time.Time { return now }
	return f
}

// step runs one sampler pass at the fixture clock.
func (f *fixture) step() {
	f.srv.sampler.step()
}

// printingSession returns an active RUNNING session with a fresh report.
func printingSession(serial string, now time.Time, gen uint64, progress float64) telemetry.SessionView {
	p := progress
	return telemetry.SessionView{
		Serial:     serial,
		Active:     true,
		State:      "RUNNING",
		SessionGen: gen,
		Obs:        3,
		ObsAt:      now,
		ObsGen:     1,
		Progress:   &p,
	}
}

// callWatch invokes the watch handler synchronously.
func callWatch(t *testing.T, s *Server, in WatchPrinterIn) (WatchPrinterOut, error) {
	t.Helper()
	_, out, err := s.toolWatchPrinter(context.Background(), nil, in)
	return out, err
}

func TestRevisionTokenRoundTrip(t *testing.T) {
	r := revision{attention: 3, progress: 11}
	const epoch = uint32(123456)
	tok := encodeRevision(epoch, r)
	gotEpoch, got, ok := decodeRevision(tok)
	if !ok || gotEpoch != epoch || got != r {
		t.Fatalf("decode(%q) = (%d, %v, %v)", tok, gotEpoch, got, ok)
	}
	if strings.Count(tok, ".") != 3 {
		t.Fatalf("token %q does not have the 4-part epoch form", tok)
	}
	if _, _, ok := decodeRevision(encodeRevision(epoch+1, r)); !ok {
		t.Fatal("valid foreign-epoch token failed to decode")
	}
	for _, bad := range []string{
		// Wrong shape, epoch zero, junk, a counter with a sign, and an
		// epoch past 32 bits: base36 letters like "x" are legal digits, so
		// only genuinely malformed tokens belong here.
		"", "1.1.5", "1.0.1.5", "0.1.2.3", "abc.1.2.3", "1.1.-2.3",
		"1.zzzzzzzzz.2.3",
	} {
		if _, _, ok := decodeRevision(bad); ok {
			t.Errorf("decodeRevision(%q) accepted an invalid token", bad)
		}
	}
}

func TestSerialFromStateURI(t *testing.T) {
	if serial, ok := serialFromStateURI("bambu://printers/P001/state"); !ok || serial != "P001" {
		t.Fatalf("serialFromStateURI good = (%q, %v)", serial, ok)
	}
	for _, bad := range []string{
		"bambu://printers//state", "bambu://printers/P001", "bambu://printers/P001/state/x",
		"file:///etc/passwd", "bambu://printers/P001/state#frag", "bambu://printers/a/b/state",
	} {
		if _, ok := serialFromStateURI(bad); ok {
			t.Errorf("serialFromStateURI(%q) accepted", bad)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	tok := encodeCursor("P002")
	got, ok := decodeCursor(tok)
	if !ok || got != "P002" {
		t.Fatalf("decodeCursor = (%q, %v)", got, ok)
	}
	if _, ok := decodeCursor("not-a-cursor"); ok {
		t.Fatal("decodeCursor accepted garbage")
	}
}

func TestAttentionEventTransitions(t *testing.T) {
	base := sample{state: "RUNNING", sessionGen: 1, active: true, connected: true, fresh: true, milestone: 4}
	set := func(mutate func(*sample)) sample {
		c := base
		mutate(&c)
		return c
	}
	cases := []struct {
		name   string
		prev   func(*sample)
		cur    sample
		want   []string
		silent bool
	}{
		{"no change", nil, base, nil, true},
		{"pause", nil, set(func(c *sample) { c.state = "PAUSE" }), []string{kindPrintPaused}, false},
		// PAUSE and PAUSED are two spellings of one paused state, as in
		// telemetry: entering the pair emits once; moving between the
		// aliases is ordinary value churn and stays silent.
		{"paused from running", nil, set(func(c *sample) { c.state = "PAUSED" }), []string{kindPrintPaused}, false},
		{"paused follows pause", func(p *sample) { p.state = "PAUSE" },
			set(func(c *sample) { c.state = "PAUSED" }), nil, true},
		{"fail", nil, set(func(c *sample) { c.state = "FAILED" }), []string{kindPrintFailed}, false},
		{"finish", nil, set(func(c *sample) { c.state = "FINISH" }), []string{kindPrintFinished}, false},
		{"job change during session", nil, set(func(c *sample) { c.sessionGen = 2 }), []string{kindJobChanged}, false},
		{"new print from idle", func(p *sample) { p.active = false },
			set(func(c *sample) { c.sessionGen = 2 }), []string{kindPrintStarted}, false},
		{"connectivity lost", nil, set(func(c *sample) { c.connected = false }), []string{kindConnectLost}, false},
		{"connectivity restored", func(p *sample) { p.connected = false },
			set(func(c *sample) { c.connected = true }), []string{kindConnectRestored}, false},
		{"reports stale", nil, set(func(c *sample) { c.fresh = false }), []string{kindReportsStale}, false},
		{"reports fresh again", func(p *sample) { p.fresh = false },
			set(func(c *sample) { c.fresh = true }), []string{kindReportsFresh}, false},
		{"detection health", nil, set(func(c *sample) { c.detKey = "changed" }), []string{kindDetectionChange}, false},
		{"state churn without a notable state", nil, set(func(c *sample) { c.state = "SLICING" }), nil, true},
	}
	for _, tc := range cases {
		prev := base
		if tc.prev != nil {
			tc.prev(&prev)
		}
		events := attentionEvents(prev, tc.cur)
		if tc.silent {
			if len(events) != 0 {
				t.Errorf("%s: got %v, want none", tc.name, events)
			}
			continue
		}
		if len(events) != len(tc.want) {
			t.Errorf("%s: events %v, want %d kinds", tc.name, events, len(tc.want))
			continue
		}
		for i, want := range tc.want {
			if events[i].Kind != want {
				t.Errorf("%s: event %d = %q, want %q", tc.name, i, events[i].Kind, want)
			}
		}
	}
}

func TestSamplerProgressMilestones(t *testing.T) {
	f := newFixture(t)
	f.state.setSession("P001", printingSession("P001", f.now, 7, 42))
	f.step()
	base := f.srv.sampler.revision("P001")

	// 44% stays inside the 40-bucket: no counter moves.
	f.state.setSession("P001", printingSession("P001", f.now, 7, 44))
	f.step()
	if rev := f.srv.sampler.revision("P001"); rev != base {
		t.Fatalf("in-bucket progress moved counters: %+v from %+v", rev, base)
	}

	// 47% enters the 45-bucket: progress counter only.
	f.state.setSession("P001", printingSession("P001", f.now, 7, 47))
	f.step()
	rev := f.srv.sampler.revision("P001")
	if rev.progress != base.progress+1 || rev.attention != base.attention {
		t.Fatalf("milestone bump = %+v, base %+v", rev, base)
	}
	events := f.srv.sampler.lastEvents("P001")
	if len(events) != 1 || events[0].Kind != kindProgressStep || events[0].Percent == nil || *events[0].Percent != 45 {
		t.Fatalf("milestone events = %+v", events)
	}
}

func TestSamplerPerModeWakes(t *testing.T) {
	f := newFixture(t)
	f.conn.set("P001", true)
	f.state.setSession("P001", printingSession("P001", f.now, 7, 42))
	f.step()

	// Attention change wakes attention watchers only.
	_, wakeAtt := f.srv.sampler.watch("P001", "attention")
	_, wakeProg := f.srv.sampler.watch("P001", "progress")
	f.conn.set("P001", false)
	f.step()
	select {
	case <-wakeAtt:
	default:
		t.Fatal("attention watcher not woken by connectivity loss")
	}
	select {
	case <-wakeProg:
		t.Fatal("progress watcher woken by attention-only change")
	default:
	}

	// Progress milestone wakes progress watchers only.
	_, wakeAtt = f.srv.sampler.watch("P001", "attention")
	_, wakeProg = f.srv.sampler.watch("P001", "progress")
	f.state.setSession("P001", printingSession("P001", f.now, 7, 51))
	f.step()
	select {
	case <-wakeProg:
	default:
		t.Fatal("progress watcher not woken by milestone")
	}
	select {
	case <-wakeAtt:
		t.Fatal("attention watcher woken by progress-only change")
	default:
	}
}

func TestSamplerRevisionsAreFleetStable(t *testing.T) {
	f := newFixture(t)
	f.state.setSession("P001", printingSession("P001", f.now, 7, 10))
	f.step()
	before := f.srv.sampler.revision("P001")

	// Parking and leaving must not reset counters: earlier tokens stay
	// comparable against the live revision.
	if _, wake := f.srv.sampler.watch("P001", "attention"); wake == nil {
		t.Fatal("watch returned nil channel")
	}
	if got := f.srv.sampler.revision("P001"); got != before {
		t.Fatalf("parking moved the revision: %+v != %+v", got, before)
	}

	// A change with nobody parked still moves the counter, so the earlier
	// token is now detectably stale instead of silently matching.
	f.state.setSession("P001", printingSession("P001", f.now, 8, 10))
	f.step()
	after := f.srv.sampler.revision("P001")
	if after.attention != before.attention+1 {
		t.Fatalf("unwatched change not counted: %+v from %+v", after, before)
	}
}

func TestWatchTokenEpochMismatchResyncs(t *testing.T) {
	fA := newFixture(t)
	fB := newFixture(t)
	if fA.srv.epoch == fB.srv.epoch {
		t.Fatal("distinct servers drew the same epoch")
	}
	token := fA.srv.token(fA.srv.sampler.revision("P001"))
	res, err := callWatch(t, fB.srv, WatchPrinterIn{
		Serial: "P001", AfterRevision: token, TimeoutSeconds: 0,
	})
	if err != nil {
		t.Fatalf("watch handler error: %v", err)
	}
	if res.Error != nil {
		t.Fatalf("unexpected tool error: %+v", res.Error)
	}
	if !res.ResyncRequired {
		t.Fatalf("foreign-epoch token did not force resync: %+v", res)
	}
	// The issuing server accepts its own token.
	res, err = callWatch(t, fA.srv, WatchPrinterIn{
		Serial: "P001", AfterRevision: token, TimeoutSeconds: 0,
	})
	if err != nil {
		t.Fatalf("watch handler error: %v", err)
	}
	if res.Error != nil || res.ResyncRequired {
		t.Fatalf("own token rejected: %+v resync=%v", res.Error, res.ResyncRequired)
	}
}

func TestWatchPrinterResyncOnStaleOrGarbageToken(t *testing.T) {
	f := newFixture(t)
	f.state.setSession("P001", printingSession("P001", f.now, 7, 10))
	f.step()
	token := f.srv.token(f.srv.sampler.revision("P001"))

	f.state.setSession("P001", printingSession("P001", f.now, 8, 10))
	f.step()
	res, err := callWatch(t, f.srv, WatchPrinterIn{
		Serial: "P001", AfterRevision: token, TimeoutSeconds: 0,
	})
	if err != nil {
		t.Fatalf("watch handler error: %v", err)
	}
	if !res.ResyncRequired || res.Changed {
		t.Fatalf("stale token: changed=%v resync=%v", res.Changed, res.ResyncRequired)
	}
	if res.Revision == token {
		t.Fatal("resync answer reused the stale token")
	}

	res, err = callWatch(t, f.srv, WatchPrinterIn{
		Serial: "P001", AfterRevision: "garbage", TimeoutSeconds: 0,
	})
	if err != nil {
		t.Fatalf("watch handler error: %v", err)
	}
	if !res.ResyncRequired {
		t.Fatalf("garbage token: resync=%v", res.ResyncRequired)
	}
}

func TestWatchPrinterChangedFlow(t *testing.T) {
	f := newFixture(t)
	f.state.setSession("P001", printingSession("P001", f.now, 7, 10))
	f.step()

	// The mutation repeats so that a park landing after one bump is still
	// overtaken by the next; a resync answer (change landed before the
	// park) is retried with a fresh token.
	for attempt := 0; attempt < 20; attempt++ {
		token := f.srv.token(f.srv.sampler.revision("P001"))
		stop := make(chan struct{})
		done := make(chan WatchPrinterOut, 1)
		go func() {
			defer close(stop)
			_, out, err := f.srv.toolWatchPrinter(context.Background(), nil, WatchPrinterIn{
				Serial: "P001", AfterRevision: token, TimeoutSeconds: 2,
			})
			if err != nil {
				t.Errorf("watch handler error: %v", err)
				return
			}
			done <- out
		}()
		go func() {
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			flip := false
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					flip = !flip
					f.conn.set("P001", flip)
					f.step()
				}
			}
		}()
		var res WatchPrinterOut
		select {
		case res = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("watch did not return")
		}
		<-stop
		time.Sleep(10 * time.Millisecond) // let the mutator observe stop
		if res.Changed {
			found := false
			for _, e := range res.Events {
				if e.Kind == kindConnectLost || e.Kind == kindConnectRestored {
					found = true
				}
			}
			if !found {
				t.Fatalf("connectivity event missing: %+v", res.Events)
			}
			return
		}
		if !res.ResyncRequired {
			t.Fatalf("no change and no resync: %+v", res)
		}
	}
	t.Fatal("changed flow never observed in 20 attempts")
}

func TestWatchPrinterUnknownSerial(t *testing.T) {
	f := newFixture(t)
	res, err := callWatch(t, f.srv, WatchPrinterIn{Serial: "NOPE", TimeoutSeconds: 0})
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if res.Error == nil || res.Error.Code != errUnknownSerial {
		t.Fatalf("unknown serial: %+v", res.Error)
	}
}

func TestWatchTooManyWaits(t *testing.T) {
	f := newFixture(t)
	for i := 0; i < maxWaits; i++ {
		f.srv.waits <- struct{}{}
	}
	res, err := callWatch(t, f.srv, WatchPrinterIn{Serial: "P001", TimeoutSeconds: 0})
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if res.Error == nil || res.Error.Code != errTooManyWaits {
		t.Fatalf("waits ceiling: %+v", res.Error)
	}
}

func TestSubscriptionAccountingAndLimits(t *testing.T) {
	f := newFixture(t)
	uri := stateURI("P001")
	sub := func(u string) error {
		return f.srv.handleSubscribe(context.Background(),
			&mcp.SubscribeRequest{Params: &mcp.SubscribeParams{URI: u}})
	}
	unsub := func(u string) {
		_ = f.srv.handleUnsubscribe(context.Background(),
			&mcp.UnsubscribeRequest{Params: &mcp.UnsubscribeParams{URI: u}})
	}
	if err := sub("file:///etc"); err == nil {
		t.Fatal("foreign URI accepted")
	}
	if err := sub(stateURI("NOPE")); err == nil {
		t.Fatal("unknown serial accepted")
	}
	for i := 0; i < maxSubscriptions; i++ {
		if err := sub(uri); err != nil {
			t.Fatalf("subscription %d rejected: %v", i+1, err)
		}
	}
	if err := sub(uri); err == nil {
		t.Fatal("subscription beyond the ceiling accepted")
	}
	if !f.srv.subscribed("P001") {
		t.Fatal("subscribed() false with active subscribers")
	}
	for i := 0; i < maxSubscriptions; i++ {
		unsub(uri)
	}
	if f.srv.subscribed("P001") {
		t.Fatal("subscribed() true after all unsubscribes")
	}
	// The ceiling is released: a new subscription fits again.
	if err := sub(uri); err != nil {
		t.Fatalf("ceiling not released: %v", err)
	}
}

func TestSubscriptionOnlyInterestDrivesNotifications(t *testing.T) {
	f := newFixture(t)
	f.conn.set("P001", true)
	f.state.setSession("P001", printingSession("P001", f.now, 7, 10))
	f.step()
	if err := f.srv.handleSubscribe(context.Background(),
		&mcp.SubscribeRequest{Params: &mcp.SubscribeParams{URI: stateURI("P001")}}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := f.srv.handleSubscribe(context.Background(),
		&mcp.SubscribeRequest{Params: &mcp.SubscribeParams{URI: stateURI("P002")}}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// One shared sampler pass notifies exactly the serials with a change
	// and a subscriber, with no parked watcher anywhere.
	f.conn.set("P001", false)
	f.step()
	queued := map[string]int{}
	for drained := false; !drained; {
		select {
		case serial := <-f.srv.sampler.notfCh:
			queued[serial]++
		default:
			drained = true
		}
	}
	if len(queued) != 1 || queued["P001"] != 1 {
		t.Fatalf("notification queue = %v, want exactly P001 once", queued)
	}
}

func TestCameraSnapshotLifecycle(t *testing.T) {
	f := newFixture(t)
	frame := &camera.Frame{JPEG: []byte{0xFF, 0xD8, 0xFF}, Seq: 9, Captured: f.now}
	f.cams.frames["P001"] = frame
	f.cams.waits["P001"] = frame

	res, out, err := f.srv.toolGetCameraSnapshot(context.Background(), nil,
		GetCameraSnapshotIn{Serial: "P001", MaxAgeSeconds: 30})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if res.IsError || out.Error != nil {
		t.Fatalf("fresh frame rejected: isError=%v err=%+v", res.IsError, out.Error)
	}
	if out.FrameSeq != 9 || out.AgeSeconds < 0 {
		t.Fatalf("metadata: %+v", out)
	}
	sawImage := false
	for _, c := range res.Content {
		if img, ok := c.(*mcp.ImageContent); ok && img.MIMEType == "image/jpeg" {
			sawImage = true
		}
	}
	if !sawImage {
		t.Fatalf("no jpeg content: %+v", res.Content)
	}
	f.cams.balanced(t)

	// No frame material at all: explicit stale outcome, still balanced.
	res, out, _ = f.srv.toolGetCameraSnapshot(context.Background(), nil,
		GetCameraSnapshotIn{Serial: "P002", MaxAgeSeconds: 5})
	if !res.IsError || out.Error == nil || out.Error.Code != errStaleImage {
		t.Fatalf("stale outcome: isError=%v err=%+v", res.IsError, out.Error)
	}
	f.cams.balanced(t)

	// Unknown serial and disabled feature stay explicit.
	_, out, _ = f.srv.toolGetCameraSnapshot(context.Background(), nil,
		GetCameraSnapshotIn{Serial: "NOPE"})
	if out.Error == nil || out.Error.Code != errUnknownSerial {
		t.Fatalf("unknown serial: %+v", out.Error)
	}
	f.cams.balanced(t)

	fOff := newFixture(t)
	fOff.srv.cams = nil
	_, out, _ = fOff.srv.toolGetCameraSnapshot(context.Background(), nil,
		GetCameraSnapshotIn{Serial: "P001"})
	if out.Error == nil || out.Error.Code != errCameraDisabled {
		t.Fatalf("disabled camera: %+v", out.Error)
	}

	// An unsupported model is refused before any capture work.
	_, out, _ = f.srv.toolGetCameraSnapshot(context.Background(), nil,
		GetCameraSnapshotIn{Serial: "P003", MaxAgeSeconds: 5})
	if out.Error == nil || out.Error.Code != errCameraUnsupported {
		t.Fatalf("unsupported model: %+v", out.Error)
	}
	f.cams.balanced(t)
}

func TestCameraSnapshotCancelledRequest(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, out, err := f.srv.toolGetCameraSnapshot(ctx, nil,
		GetCameraSnapshotIn{Serial: "P001", MaxAgeSeconds: 30})
	if err != nil {
		t.Fatalf("handler returned transport error: %v", err)
	}
	if out.Error == nil || out.Error.Code != errCameraUnavailable {
		t.Fatalf("cancelled snapshot: %+v", out.Error)
	}
	f.cams.balanced(t)
}

func TestCameraSnapshotSlotLimit(t *testing.T) {
	f := newFixture(t)
	slot := f.srv.snapshotSlot("P001")
	for i := 0; i < snapshotWaitersPerPrinter; i++ {
		slot <- struct{}{}
	}
	_, out, err := f.srv.toolGetCameraSnapshot(context.Background(), nil,
		GetCameraSnapshotIn{Serial: "P001", MaxAgeSeconds: 30})
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if out.Error == nil || out.Error.Code != errCameraUnavailable {
		t.Fatalf("slot ceiling: %+v", out.Error)
	}
	for i := 0; i < snapshotWaitersPerPrinter; i++ {
		<-slot
	}
	f.cams.balanced(t)
}

func TestPrinterStateFreshnessAcrossReconnect(t *testing.T) {
	f := newFixture(t)
	f.gens.set("P001", 1)
	f.conn.set("P001", true)
	f.state.setSession("P001", printingSession("P001", f.now, 7, 10))
	f.step() // baseline: a fresh report on generation 1

	st, ok := f.srv.buildPrinterState("P001", f.now)
	if !ok || !st.Freshness.Fresh {
		t.Fatalf("fresh report: fresh=%v ok=%v", st.Freshness.Fresh, ok)
	}

	// An upstream reconnect bumps the generation; the observation predates
	// it, so the evidence is stale even though its age is small.
	f.gens.set("P001", 2)
	st, _ = f.srv.buildPrinterState("P001", f.now)
	if st.Freshness.Fresh {
		t.Fatalf("pre-reconnect report counted fresh: %+v", st.Freshness)
	}
	if st.Freshness.LastReportAgeSeconds == nil || st.Freshness.UpstreamGeneration == nil {
		t.Fatalf("stale evidence lost its fields: %+v", st.Freshness)
	}

	// The sampler sees the same flip and reports it once.
	f.step()
	events := f.srv.sampler.lastEvents("P001")
	if len(events) != 1 || events[0].Kind != kindReportsStale {
		t.Fatalf("reconnect events = %+v", events)
	}
}

func TestCameraStateProjection(t *testing.T) {
	f := newFixture(t)
	if got := f.srv.cameraState("P003"); got != cameraUnsupported {
		t.Fatalf("X1C camera state = %q", got)
	}
	if got := f.srv.cameraState("P001"); got != cameraOffline {
		t.Fatalf("frameless camera state = %q", got)
	}
	f.cams.frames["P001"] = &camera.Frame{Captured: f.now, Seq: 1}
	if got := f.srv.cameraState("P001"); got != cameraReady {
		t.Fatalf("framed camera state = %q", got)
	}
	f.srv.cams = nil
	if got := f.srv.cameraState("P001"); got != cameraDisabled {
		t.Fatalf("feature-off camera state = %q", got)
	}
}

func TestListPrintersPaging(t *testing.T) {
	f := newFixture(t)
	_, page1, err := f.srv.toolListPrinters(context.Background(), nil, ListPrintersIn{Limit: 2})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1.Printers) != 2 || page1.NextCursor == "" {
		t.Fatalf("page 1 = %+v", page1)
	}
	if page1.Printers[0].Serial != "P001" || page1.Printers[1].Serial != "P002" {
		t.Fatalf("page 1 order = %+v", page1.Printers)
	}
	_, page2, err := f.srv.toolListPrinters(context.Background(), nil,
		ListPrintersIn{Limit: 2, Cursor: page1.NextCursor})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2.Printers) != 1 || page2.Printers[0].Serial != "P003" || page2.NextCursor != "" {
		t.Fatalf("page 2 = %+v", page2)
	}
	_, bad, _ := f.srv.toolListPrinters(context.Background(), nil, ListPrintersIn{Cursor: "junk"})
	if bad.Error == nil || bad.Error.Code != errInvalidCursor {
		t.Fatalf("bad cursor: %+v", bad.Error)
	}
}

func TestPrinterStateUnknownSerial(t *testing.T) {
	f := newFixture(t)
	res, out, err := f.srv.toolGetPrinterState(context.Background(), nil, GetPrinterStateIn{Serial: "NOPE"})
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !res.IsError || out.Error == nil || out.Error.Code != errUnknownSerial {
		t.Fatalf("unknown serial: isError=%v err=%+v", res.IsError, out.Error)
	}
}

func TestResourceReaderUnknownSerial(t *testing.T) {
	f := newFixture(t)
	_, err := f.srv.readStateResource(context.Background(),
		&mcp.ReadResourceRequest{Params: &mcp.ReadResourceParams{URI: stateURI("NOPE")}})
	if err == nil {
		t.Fatal("unknown serial accepted by the resource reader")
	}
}

func TestCredentialStripping(t *testing.T) {
	f := newFixture(t)
	for _, info := range f.srv.printers {
		if rendered := fmt.Sprintf("%+v", info); strings.Contains(rendered, "secret") ||
			strings.Contains(rendered, "pw-") {
			t.Fatalf("credentials leaked into inventory: %s", rendered)
		}
	}
	raw := fmt.Sprintf("%+v", f.srv)
	if strings.Contains(raw, "secret-a") || strings.Contains(raw, "pw-a") {
		t.Fatal("server state carries credentials")
	}
}

func TestDetectionDisabledProjection(t *testing.T) {
	f := newFixture(t)
	f.srv.det = nil
	view := f.srv.detectionView("P001")
	if view.State != "disabled" {
		t.Fatalf("disabled detection = %+v", view)
	}
}

// A change landing between the revision capture and the state read must
// leave the returned token stale. Replaying it then forces resync_required:
// the change is re-served by a fresh snapshot instead of being silently
// consumed by a token that already names the newer counter. The injection
// is deterministic: the fake state source runs the change and a sampler
// step inside the State read, and every read path under test must capture
// its revision before that read.
func TestRevisionTokenCannotConsumeMidReadChange(t *testing.T) {
	arm := func(t *testing.T, f *fixture) {
		t.Helper()
		f.state.onState = func(serial string) {
			if serial != "P001" {
				return
			}
			f.state.onState = nil
			f.conn.set("P001", false)
			f.step()
		}
		t.Cleanup(func() { f.state.onState = nil })
	}
	// resync replays the token through watch_printer in immediate mode and
	// demands the forced resync: a stale token must never match.
	resync := func(t *testing.T, f *fixture, token string) {
		t.Helper()
		res, err := callWatch(t, f.srv, WatchPrinterIn{Serial: "P001", AfterRevision: token, TimeoutSeconds: 0})
		if err != nil {
			t.Fatalf("watch replay: %v", err)
		}
		if res.Error != nil {
			t.Fatalf("watch replay error: %+v", res.Error)
		}
		if !res.ResyncRequired || res.Changed {
			t.Fatalf("replayed token consumed the mid-read change: changed=%v resync=%v",
				res.Changed, res.ResyncRequired)
		}
	}
	seed := func(t *testing.T) *fixture {
		t.Helper()
		f := newFixture(t)
		f.conn.set("P001", true)
		f.state.setSession("P001", printingSession("P001", f.now, 7, 10))
		f.step()
		return f
	}

	t.Run("get_printer_state", func(t *testing.T) {
		f := seed(t)
		arm(t, f)
		_, out, err := f.srv.toolGetPrinterState(context.Background(), nil, GetPrinterStateIn{Serial: "P001"})
		if err != nil {
			t.Fatalf("get state: %v", err)
		}
		if out.Error != nil {
			t.Fatalf("get state error: %+v", out.Error)
		}
		resync(t, f, out.Revision)
	})

	t.Run("list_printers", func(t *testing.T) {
		f := seed(t)
		arm(t, f)
		_, page, err := f.srv.toolListPrinters(context.Background(), nil, ListPrintersIn{Limit: maxListLimit})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if page.Error != nil {
			t.Fatalf("list error: %+v", page.Error)
		}
		for _, row := range page.Printers {
			if row.Serial != "P001" {
				continue
			}
			resync(t, f, row.Revision)
			return
		}
		t.Fatal("P001 row missing from the page")
	})

	t.Run("watch_printer initial answer", func(t *testing.T) {
		f := seed(t)
		arm(t, f)
		out, err := callWatch(t, f.srv, WatchPrinterIn{Serial: "P001", TimeoutSeconds: 0})
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		if out.Error != nil {
			t.Fatalf("watch error: %+v", out.Error)
		}
		resync(t, f, out.Revision)
	})

	t.Run("state resource read", func(t *testing.T) {
		f := seed(t)
		arm(t, f)
		rr, err := f.srv.readStateResource(context.Background(),
			&mcp.ReadResourceRequest{Params: &mcp.ReadResourceParams{URI: stateURI("P001")}})
		if err != nil {
			t.Fatalf("read resource: %v", err)
		}
		var payload struct {
			Revision string `json:"revision"`
		}
		if err := json.Unmarshal([]byte(rr.Contents[0].Text), &payload); err != nil {
			t.Fatalf("decode resource payload: %v", err)
		}
		resync(t, f, payload.Revision)
	})
}
