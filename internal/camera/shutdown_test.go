// Shutdown and stalled-read coverage: Manager.Close is terminal, capture
// close wakes every waiter exactly once, native reads carry a bounded
// per-frame budget, and cancellation interrupts a pending read by closing
// the connection.
package camera

import (
	"context"
	"encoding/binary"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// shortenReadTimeout swaps the native per-frame read budget for d and
// restores the production value when the test ends.
func shortenReadTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := frameReadTimeout
	frameReadTimeout = d
	t.Cleanup(func() { frameReadTimeout = old })
}

// TestNativeStallOnHeaderTimesOutAndReconnects authenticates one session,
// lets it go silent at the frame header, and requires the per-frame budget
// to end the session so the capture loop opens a second one.
func TestNativeStallOnHeaderTimesOutAndReconnects(t *testing.T) {
	shortenReadTimeout(t, 200*time.Millisecond)
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, cameraSpec("S1", "127.0.0.1:8883"))
	if _, st := m.Acquire("S1"); st != StatusOK {
		t.Fatalf("acquire = %v, want ok", st)
	}
	defer m.Release("S1")

	// The session authenticates and then receives nothing: the header read
	// must time out and the loop must dial a replacement session.
	waitUntil(t, 5*time.Second, func() bool { return fc.sessions.Load() >= 2 })

	// The recovered capture still captures. Frames drip instead of a single
	// send: every timed-out session leaves one stale fake handle that can
	// consume a frame before noticing the closed connection, so a burst is
	// required for a live session to win one.
	dripStop := make(chan struct{})
	defer close(dripStop)
	go func() {
		for {
			select {
			case <-dripStop:
				return
			default:
			}
			select {
			case fc.frames <- jpeg(48):
			default:
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	waitUntil(t, 10*time.Second, func() bool { return m.Latest("S1") != nil })
}

// TestNativeStallMidPayloadTimesOut sends a header that promises more
// payload than follows; the read stalls mid-payload and the same per-frame
// budget must end the session and reconnect.
func TestNativeStallMidPayloadTimesOut(t *testing.T) {
	shortenReadTimeout(t, 200*time.Millisecond)
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, cameraSpec("S1", "127.0.0.1:8883"))
	if _, st := m.Acquire("S1"); st != StatusOK {
		t.Fatalf("acquire = %v, want ok", st)
	}
	defer m.Release("S1")

	header := make([]byte, frameHeaderLen)
	binary.LittleEndian.PutUint32(header[0:4], 64)
	fc.custom <- rawTestFrame{header: header, payload: []byte{0xFF, 0xD8, 0xFF, 0xD9}}

	waitUntil(t, 5*time.Second, func() bool { return fc.sessions.Load() >= 2 })
}

// TestCloseInterruptsPendingNativeRead pins the cancellation contract: with
// a long per-frame budget, only closing the connection can unblock the
// pending read, so Close must finish promptly and no replacement transport
// may appear afterwards.
func TestCloseInterruptsPendingNativeRead(t *testing.T) {
	shortenReadTimeout(t, time.Minute)
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, cameraSpec("S1", "127.0.0.1:8883"))
	if _, st := m.Acquire("S1"); st != StatusOK {
		t.Fatalf("acquire = %v, want ok", st)
	}
	fc.frames <- jpeg(32)
	waitUntil(t, 5*time.Second, func() bool { return m.Latest("S1") != nil })

	done := make(chan struct{})
	go func() {
		m.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not interrupt a pending camera read")
	}
	before := fc.sessions.Load()
	time.Sleep(150 * time.Millisecond)
	if fc.sessions.Load() != before {
		t.Fatal("capture dialed a new camera session after close")
	}
}

// TestCloseRacesAcquirePublishAndWaiters hammers Acquire, publish, and
// Wait against Close: nothing may panic, waiters must stop and return nil
// after close, and every post-close accessor must refuse work without
// invoking the snapshot callback.
func TestCloseRacesAcquirePublishAndWaiters(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, cameraSpec("S1", "127.0.0.1:8883"))
	if _, st := m.Acquire("S1"); st != StatusOK {
		t.Fatalf("acquire = %v, want ok", st)
	}
	m.mu.Lock()
	c := m.captures["S1"]
	m.mu.Unlock()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = m.Wait("S1", ctx, 0, 20*time.Millisecond)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.publish(&Frame{JPEG: jpeg(64)})
			time.Sleep(time.Millisecond)
		}
	}()
	acquirerDone := make(chan struct{})
	go func() {
		defer close(acquirerDone)
		for range 200 {
			if _, st := m.Acquire("S1"); st != StatusOK {
				return
			}
			m.Release("S1")
		}
	}()

	time.Sleep(20 * time.Millisecond)
	m.Close()
	close(stop)
	wg.Wait()
	<-acquirerDone

	// Terminal state: every accessor refuses work after Close.
	if _, st := m.Acquire("S1"); st != StatusUnavailable {
		t.Fatalf("Acquire after close = %v, want unavailable", st)
	}
	if _, st := m.AcquireWeb("S1"); st != StatusUnavailable {
		t.Fatalf("AcquireWeb after close = %v, want unavailable", st)
	}
	invoked := false
	wait := func(*capture) (*Frame, bool) {
		invoked = true
		return nil, false
	}
	if _, st := m.Snapshot("S1", wait); st != StatusUnavailable || invoked {
		t.Fatalf("Snapshot after close = %v, callback invoked = %v", st, invoked)
	}
	if _, st := m.WebSnapshot("S1", wait); st != StatusUnavailable || invoked {
		t.Fatalf("WebSnapshot after close = %v, callback invoked = %v", st, invoked)
	}
	if f := m.Latest("S1"); f != nil {
		t.Fatalf("Latest after close = %v, want nil", f)
	}
	started := time.Now()
	if f := m.Wait("S1", context.Background(), 0, 30*time.Second); f != nil || time.Since(started) > 2*time.Second {
		t.Fatalf("Wait after close = %v in %s, want nil immediately", f, time.Since(started))
	}

	// Post-close publishes are discarded: the notification channel must
	// stay closed so future waiters wake immediately.
	c.publish(&Frame{JPEG: jpeg(8)})
	select {
	case <-c.notify:
	default:
		t.Fatal("notify channel must stay closed after close")
	}

	// Repeated Close completes instead of racing a second teardown.
	done := make(chan struct{})
	go func() {
		m.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("repeated Close did not return")
	}
}

// TestStreamHandlerEndsWhenManagerCloses drives handleStream with a live
// fake camera and closes the manager mid-stream: the handler must return
// promptly instead of waiting out the heartbeat.
func TestStreamHandlerEndsWhenManagerCloses(t *testing.T) {
	fc := newFakeCamera(t)
	m := newFakeManager(t, fc, cameraSpec("S1", "127.0.0.1:8883"))
	if _, st := m.Acquire("S1"); st != StatusOK {
		t.Fatalf("acquire = %v, want ok", st)
	}
	fc.frames <- jpeg(40)
	waitUntil(t, 5*time.Second, func() bool { return m.Latest("S1") != nil })

	rec := newStreamingRecorder()
	req := httptest.NewRequest("GET", "/camera/S1/stream", nil)
	req.SetPathValue("serial", "S1")
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.handleStream(rec, req)
	}()
	// One delivered part proves the handler reached its frame loop.
	waitUntil(t, 5*time.Second, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return strings.Count(rec.buf.String(), "image/jpeg") >= 1
	})

	m.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stream handler did not end after the manager closed")
	}
}
