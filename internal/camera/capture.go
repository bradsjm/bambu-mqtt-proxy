// Capture management for the camera package. One capture per supported
// printer shares a single camera connection across every concurrent snapshot
// and stream consumer; printers allow only one camera connection, and slow
// HTTP clients must never throttle frame capture.
package camera

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Capture tuning. Fixed internal defaults keep the configuration surface
// small; they are not printer guarantees.
const (
	idleStopAfter    = 5 * time.Second // stop capture with no consumers
	backoffInitial   = 500 * time.Millisecond
	backoffMax       = 5 * time.Second
	snapshotMaxAge   = 5 * time.Second  // accept a shared frame younger than this
	initialFrameWait = 15 * time.Second // bound a snapshot's wait for the first frame
)

// Frame is an immutable captured JPEG. Buffers are never reused, so a frame
// handed to an HTTP handler stays valid while the capture loop moves on.
type Frame struct {
	JPEG     []byte
	Seq      uint64
	Captured time.Time
}

// capture owns the camera connection for one printer serial.
type capture struct {
	spec config.Printer
	log  *slog.Logger

	mu        sync.Mutex
	consumers int
	notify    chan struct{} // closed and replaced on every published frame
	closed    bool
	frame     *Frame
	cancel    context.CancelFunc
	done      chan struct{} // closed when the capture loop exits
	backoffN  int
}

// newCapture builds the idle capture for one printer serial.
func newCapture(spec config.Printer, log *slog.Logger) *capture {
	return &capture{
		spec:   spec,
		log:    log,
		notify: make(chan struct{}),
		done:   make(chan struct{}),
		frame:  nil,
		cancel: nil,
	}
}

// acquire registers a consumer and starts the capture loop when this is the
// first consumer or the idle-stop has not yet fired. It returns the current
// notification channel; see wait.
func (c *capture) acquire() chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return c.notify
	}
	c.consumers++
	if c.cancel == nil {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		c.cancel = cancel
		c.done = done
		go c.run(ctx, cancel, done)
	}
	return c.notify
}

// release drops one consumer. When the last consumer leaves, an idle-stop
// timer ends the capture loop unless someone acquires first.
func (c *capture) release() {
	c.mu.Lock()
	if c.consumers > 0 {
		c.consumers--
	}
	last := c.consumers == 0 && c.cancel != nil && !c.closed
	serial := c.spec.Serial
	c.mu.Unlock()
	if last {
		time.AfterFunc(idleStopAfter, func() { c.stopIdle(serial) })
	}
}

// stopIdle ends the capture loop when no consumer re-acquired in time.
func (c *capture) stopIdle(serial string) {
	c.mu.Lock()
	if c.closed || c.consumers > 0 || c.cancel == nil {
		c.mu.Unlock()
		return
	}
	cancel, done := c.cancel, c.done
	c.cancel = nil
	c.mu.Unlock()
	cancel()
	<-done
	c.log.Info("camera capture idle-stopped", "serial", serial)
}

// latest returns the newest frame, or nil before the first frame.
func (c *capture) latest() *Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.frame
}

// snapshot returns a sufficiently fresh frame. A stale-or-missing frame
// starts a bounded wait for the next published one; the returned ok flag is
// false when no fresh frame arrives in time or the capture is closed.
func (c *capture) snapshot(ctx context.Context) (*Frame, bool) {
	if f := c.latest(); f != nil && time.Since(f.Captured) <= snapshotMaxAge {
		return f, true
	}
	notify := c.acquire()
	defer c.release()
	c.mu.Lock()
	notify = c.notify
	c.mu.Unlock()
	if f := c.latest(); f != nil && time.Since(f.Captured) <= snapshotMaxAge {
		return f, true
	}
	timer := time.NewTimer(initialFrameWait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-timer.C:
			if f := c.latest(); f != nil && time.Since(f.Captured) <= snapshotMaxAge {
				return f, true
			}
			return nil, false
		case <-notify:
			c.mu.Lock()
			f := c.frame
			notify = c.notify
			c.mu.Unlock()
			if f != nil && time.Since(f.Captured) <= snapshotMaxAge {
				return f, true
			}
		}
	}
}

// wait blocks until the next frame after seq, the context ends, the capture
// closes, or the timeout expires. The frame is nil when nothing arrived.
func (c *capture) wait(ctx context.Context, after uint64, timeout time.Duration) *Frame {
	c.acquire()
	defer c.release()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		c.mu.Lock()
		f := c.frame
		ch := c.notify
		c.mu.Unlock()
		if f != nil && f.Seq > after {
			return f
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ch:
		case <-timer.C:
			return nil
		}
	}
}

// close shuts the capture down permanently.
func (c *capture) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	cancel, done := c.cancel, c.done
	c.cancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

// run is the capture loop: dial, authenticate, then read frames until the
// context is cancelled. Connection loss retries with capped backoff.
func (c *capture) run(ctx context.Context, cancel context.CancelFunc, done chan struct{}) {
	defer cancel()
	defer close(done)
	address, err := cameraAddress(c.spec)
	if err != nil {
		c.log.Error("camera capture cannot start", "serial", c.spec.Serial, "error", err)
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		if c.streamOnce(ctx, address) {
			return
		}
		// Lost connection: backoff before reconnecting. The wait re-reads
		// consumer state under the lock so shutdown races resolve safely.
		d := c.backoffDuration()
		c.log.Info("camera reconnect scheduled",
			"serial", c.spec.Serial, "address", address, "retry_in", d.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}

// streamOnce runs one connected session. It reports whether the loop is
// finished (context cancelled); connection loss returns false.
func (c *capture) streamOnce(ctx context.Context, address string) bool {
	conn, err := dialTLS(ctx, address)
	if err != nil {
		c.log.Info("camera connect failed",
			"serial", c.spec.Serial, "address", address, "error", err)
		return false
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	if err := authenticate(conn, reader, c.spec.Username, c.spec.Password); err != nil {
		c.log.Warn("camera authentication failed",
			"serial", c.spec.Serial, "address", address, "error", err)
		return false
	}
	c.log.Info("camera connected", "serial", c.spec.Serial, "address", address)
	c.resetBackoff()

	readCtx, stopRead := context.WithCancel(ctx)
	defer stopRead()
	go func() {
		<-readCtx.Done()
		_ = conn.SetDeadline(time.Unix(1, 0)) // unblock the pending read
	}()
	_ = conn.SetDeadline(time.Time{})

	for {
		if readCtx.Err() != nil {
			return true
		}
		frame, err := readFrame(reader)
		if err != nil {
			c.mu.Lock()
			stopped := c.cancel == nil
			c.mu.Unlock()
			if stopped || readCtx.Err() != nil {
				return true
			}
			c.log.Info("camera stream ended",
				"serial", c.spec.Serial, "error", err)
			return false
		}
		c.publish(frame)
	}
}

// publish stores the newest frame and wakes every waiter. Frames are large;
// a new buffer per frame keeps handler-held bytes valid without copies.
func (c *capture) publish(jpeg []byte) {
	c.mu.Lock()
	seq := uint64(0)
	if c.frame != nil {
		seq = c.frame.Seq
	}
	f := &Frame{JPEG: jpeg, Seq: seq + 1, Captured: time.Now()}
	c.frame = f
	old := c.notify
	c.notify = make(chan struct{})
	c.mu.Unlock()
	close(old)
}

// backoffDuration returns the next reconnect delay with jitter.
func (c *capture) backoffDuration() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := backoffInitial << c.backoffN
	if d > backoffMax || d <= 0 {
		d = backoffMax
	}
	c.backoffN++
	jitter := time.Duration(rand.Int64N(int64(d) / 5))
	return d + jitter
}

// resetBackoff clears the reconnect backoff after a successful session.
func (c *capture) resetBackoff() {
	c.mu.Lock()
	c.backoffN = 0
	c.mu.Unlock()
}

// String renders a frame for logs.
func (f *Frame) String() string {
	if f == nil {
		return "<nil>"
	}
	return fmt.Sprintf("frame seq=%d bytes=%d age=%s",
		f.Seq, len(f.JPEG), time.Since(f.Captured).Round(time.Millisecond))
}
