// Capture management for the camera package. One capture per supported
// printer shares a single camera connection across every concurrent snapshot
// and stream consumer; printers allow only one camera connection, and slow
// HTTP clients must never throttle frame capture.
package camera

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"mime/multipart"
	"net"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Capture tuning. Fixed internal defaults keep the configuration surface
// small; they are not printer guarantees.
const (
	idleStopAfter    = 2 * time.Minute // idle grace after the last consumer release
	backoffInitial   = 500 * time.Millisecond
	backoffMax       = 5 * time.Second
	snapshotMaxAge   = 5 * time.Second  // accept a shared frame younger than this
	initialFrameWait = 15 * time.Second // bound a snapshot's wait for the first frame
	rtspsReadTimeout = 15 * time.Second // kill FFmpeg if a frame stalls
)

// frameReadTimeout bounds each complete native frame read (header and
// payload): a stalled camera ends the session, and the capture loop
// reconnects after its backoff. A variable so focused tests can shorten
// the wait without touching the production budget.
var frameReadTimeout = 15 * time.Second

// Frame is an immutable captured JPEG. Buffers are never reused, so a frame
// handed to an HTTP handler stays valid while the capture loop moves on.
// Header holds the exact 16 raw header bytes the printer sent; the raw
// camera server replays them verbatim instead of synthesizing one.
type Frame struct {
	JPEG     []byte
	Header   []byte
	Seq      uint64
	Captured time.Time
}

// capture owns the camera connection for one printer serial.
type capture struct {
	spec config.Printer
	log  *slog.Logger
	// ffmpegPath selects the RTSPS process backend when non-empty.
	ffmpegPath string
	// endpoint resolves the printer's camera address. Production code uses
	// cameraAddress; tests point captures at loopback fakes without
	// changing printer configuration.
	endpoint func(config.Printer) (string, error)

	mu        sync.Mutex
	consumers int
	notify    chan struct{} // closed and replaced on every published frame
	closed    bool
	stopping  bool          // idle-stopped and waiting for the loop to exit
	retired   chan struct{} // closed when a retirement fully completes
	idle      *time.Timer   // armed while the last consumer is gone
	idleGen   uint64        // bumped on every arm; stale callbacks abort
	frame     *Frame
	cancel    context.CancelFunc
	done      chan struct{} // closed when the capture loop exits
	backoffN  int
	// connectionID increases for each native or FFmpeg camera connection attempt.
	connectionID atomic.Uint64
}

func newRTSPCapture(spec config.Printer, log *slog.Logger, ffmpegPath string) *capture {
	return &capture{
		spec:       spec,
		log:        log,
		ffmpegPath: ffmpegPath,
		notify:     make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// newCapture builds the idle capture for one printer serial. The endpoint
// function resolves where to dial; see capture.endpoint.
func newCapture(spec config.Printer, log *slog.Logger, endpoint func(config.Printer) (string, error)) *capture {
	return &capture{
		spec:     spec,
		log:      log,
		endpoint: endpoint,
		notify:   make(chan struct{}),
		done:     make(chan struct{}),
		frame:    nil,
		cancel:   nil,
	}
}

// acquire registers a consumer and starts the capture loop when this is the
// first consumer, restarts it after an idle stop, and otherwise joins the
// live loop. It returns the current notification channel; see wait.
func (c *capture) acquire() chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return c.notify
	}
	if c.idle != nil {
		c.idle.Stop()
		c.idle = nil
	}
	// A retiring capture is idle-stopped with its loop still exiting. The
	// printer allows one camera connection, so wait for the retirement to
	// complete — retired closes only after the loop exited and the flag
	// cleared — before starting a new one, then re-check: shutdown or a
	// concurrent acquire may have won the race meanwhile.
	for c.cancel == nil && c.stopping {
		retired := c.retired
		c.mu.Unlock()
		<-retired
		c.mu.Lock()
	}
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
	defer c.mu.Unlock()
	if c.consumers > 0 {
		c.consumers--
	}
	// Arming a fresh timer on every last release measures the grace from
	// the most recent release; acquire stops the pending one. At most one
	// timer can be pending here: consumers reach zero only through this
	// arm, and acquire cancels it before counting a new consumer.
	if c.consumers == 0 && c.cancel != nil && !c.closed {
		c.idleGen++
		gen := c.idleGen
		c.idle = time.AfterFunc(idleStopAfter, func() { c.stopIdle(gen) })
	}
}

// stopIdle ends the capture loop when no consumer re-acquired in time.
// The generation rejects a callback delayed past its own grace: a later
// release has armed a newer timer by then. The stopping flag and retired
// channel stay until the loop has fully exited, so a concurrent acquire
// waits instead of dialing a second camera connection.
func (c *capture) stopIdle(gen uint64) {
	c.mu.Lock()
	if gen != c.idleGen || c.closed || c.consumers > 0 || c.cancel == nil {
		c.mu.Unlock()
		return
	}
	cancel, done := c.cancel, c.done
	c.cancel = nil
	c.stopping = true
	c.retired = make(chan struct{})
	c.idle = nil
	c.mu.Unlock()
	cancel()
	<-done
	c.mu.Lock()
	c.stopping = false
	retired := c.retired
	c.mu.Unlock()
	close(retired)
	c.log.Info("camera capture idle-stopped", "serial", c.spec.Serial)
}

// latest returns the newest frame, or nil before the first frame.
func (c *capture) latest() *Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	return c.frame
}

// snapshot returns a sufficiently fresh frame. A stale-or-missing frame
// starts a bounded wait for the next published one; the returned ok flag is
// false when no fresh frame arrives in time or the capture is closed.
func (c *capture) snapshot(ctx context.Context) (*Frame, bool) {
	// Acquiring before the fresh check counts every caller as a consumer,
	// so repeated cached-frame answers keep resetting the idle-stop timer
	// and hold the transport open.
	notify := c.acquire()
	defer c.release()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, false
	}
	notify = c.notify
	f := c.frame
	c.mu.Unlock()
	if f != nil && time.Since(f.Captured) <= snapshotMaxAge {
		return f, true
	}
	timer := time.NewTimer(initialFrameWait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-timer.C:
			c.mu.Lock()
			closed, f := c.closed, c.frame
			c.mu.Unlock()
			if !closed && f != nil && time.Since(f.Captured) <= snapshotMaxAge {
				return f, true
			}
			return nil, false
		case <-notify:
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return nil, false
			}
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
		if c.closed {
			// close woke this wait through the notify channel; return
			// immediately instead of serving a stale frame or spinning.
			c.mu.Unlock()
			return nil
		}
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

// close shuts the capture down permanently. It wakes every waiter by
// closing the notification channel once, cancels any pending idle stop,
// then cancels the capture loop and waits for a running or retiring loop
// to exit.
func (c *capture) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.idle != nil {
		c.idle.Stop()
		c.idle = nil
	}
	// Wake every waiter before the transport goes away. publish discards
	// post-close frames, so this channel stays closed forever after.
	close(c.notify)
	cancel, done, stopping := c.cancel, c.done, c.stopping
	c.cancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if cancel != nil || stopping {
		<-done
	}
}

// run is the capture loop: dial, authenticate, then read frames until the
// context is cancelled. Connection loss retries with capped backoff.
func (c *capture) run(ctx context.Context, cancel context.CancelFunc, done chan struct{}) {
	defer cancel()
	defer close(done)
	address := ""
	if c.ffmpegPath == "" {
		var err error
		address, err = c.endpoint(c.spec)
		if err != nil {
			c.log.Error("camera capture cannot start", "serial", c.spec.Serial, "error", err)
			return
		}
	}
	for {
		if ctx.Err() != nil {
			return
		}
		connectionID := c.connectionID.Add(1)
		if c.streamOnce(ctx, address, connectionID) {
			return
		}
		// Lost connection: backoff before reconnecting. The wait re-reads
		// consumer state under the lock so shutdown races resolve safely.
		d := c.backoffDuration()
		if c.ffmpegPath == "" {
			c.log.Info("camera reconnect scheduled",
				"serial", c.spec.Serial, "connection_id", connectionID,
				"address", address, "retry_in", d.String())
		} else {
			c.log.Info("RTSPS capture reconnect scheduled",
				"serial", c.spec.Serial, "connection_id", connectionID,
				"retry_in", d.String())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}

// streamOnce runs one connected session. It reports whether the loop is
// finished (context cancelled); connection loss returns false.
func (c *capture) streamOnce(ctx context.Context, address string, connectionID uint64) bool {
	if c.ffmpegPath != "" {
		return c.streamRTSPSOnce(ctx, connectionID)
	}
	log := c.log.With("serial", c.spec.Serial, "connection_id", connectionID)
	conn, err := dialTLS(ctx, address)
	if err != nil {
		log.Info("camera connect failed", "address", address, "error", err)
		return false
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	if err := authenticate(conn, reader, c.spec.Username, c.spec.Password); err != nil {
		log.Warn("camera authentication failed", "address", address, "error", err)
		return false
	}
	log.Info("camera connected", "address", address)
	c.resetBackoff()

	readCtx, stopRead := context.WithCancel(ctx)
	defer stopRead()
	// Cancellation closes the connection instead of writing deadlines: a
	// pending read always unblocks, and a concurrent per-frame deadline
	// reset cannot defeat it.
	go func() {
		<-readCtx.Done()
		_ = conn.Close()
	}()

	for {
		if readCtx.Err() != nil {
			return true
		}
		// One bounded budget per complete frame read: a stalled header or
		// payload ends this session, and run reconnects after its backoff.
		_ = conn.SetReadDeadline(time.Now().Add(frameReadTimeout))
		frame, err := readFrame(reader)
		if err != nil {
			if readCtx.Err() != nil {
				return true
			}
			c.mu.Lock()
			stopped := c.cancel == nil
			c.mu.Unlock()
			if stopped {
				return true
			}
			log.Info("camera stream ended", "error", err)
			return false
		}
		c.publish(frame)
	}
}

// streamRTSPSOnce runs one FFmpeg RTSPS session and publishes bounded JPEG
// parts. A true result means the capture context ended; every other exit is
// retried by run after its shared backoff.
func (c *capture) streamRTSPSOnce(ctx context.Context, connectionID uint64) bool {
	log := c.log.With("serial", c.spec.Serial, "connection_id", connectionID)
	streamURL, err := rtspsURL(c.spec)
	if err != nil {
		log.Warn("RTSPS camera address is invalid", "error", err)
		return false
	}
	cmd := exec.CommandContext(ctx, c.ffmpegPath,
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-rtsp_transport", "tcp", "-i", streamURL,
		"-an", "-sn", "-dn", "-vf", "fps=2", "-c:v", "mjpeg", "-q:v", "5",
		"-f", "mpjpeg", "-boundary_tag", "ffmpeg", "pipe:1",
	)
	// FFmpeg diagnostics are discarded: FFmpeg reflects the stream URL,
	// and the URL carries the printer's access code. The structured logs
	// below describe every failure this path reports.
	cmd.Stderr = io.Discard
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Warn("cannot create RTSPS camera output pipe", "error", err)
		return false
	}
	if err := cmd.Start(); err != nil {
		log.Warn("FFmpeg RTSPS camera process could not start", "error", err)
		return false
	}
	// Every started process is killed and waited on this path, including
	// parser errors and normal EOF. stderr is bounded before it reaches logs.
	waited := false
	stop := func() {
		if waited {
			return
		}
		_ = cmd.Process.Kill()
		_ = stdout.Close()
		_ = cmd.Wait()
		waited = true
	}
	defer stop()

	reader := multipart.NewReader(stdout, "ffmpeg")
	for {
		stalled := make(chan struct{}, 1)
		readDone := new(atomic.Bool)
		timer := time.AfterFunc(rtspsReadTimeout, func() {
			if !readDone.CompareAndSwap(false, true) {
				return
			}
			select {
			case stalled <- struct{}{}:
			default:
			}
			_ = cmd.Process.Kill()
		})
		part, readErr := reader.NextPart()
		var jpeg []byte
		if readErr == nil {
			jpeg, readErr = io.ReadAll(io.LimitReader(part, maxPayloadLen+1))
		}
		timedOut := !readDone.CompareAndSwap(false, true)
		timer.Stop()
		if timedOut {
			<-stalled
			stop()
			log.Warn("RTSPS camera frame read stalled", "error", "frame read timeout")
			return false
		}
		if readErr != nil {
			if ctx.Err() == nil {
				stop()
				log.Info("RTSPS camera stream ended", "error", readErr)
			}
			return ctx.Err() != nil
		}
		if len(jpeg) == 0 || len(jpeg) > maxPayloadLen || !isJPEG(jpeg) {
			stop()
			log.Warn("RTSPS camera returned an invalid JPEG frame", "bytes", len(jpeg))
			return false
		}
		_ = part.Close()
		c.resetBackoff()
		c.publish(&Frame{JPEG: jpeg})
	}
}

// rtspsURL builds the printer RTSPS endpoint from its configured host and
// credentials. Structured URL construction escapes credentials and
// net.JoinHostPort preserves IPv6 host syntax.
func rtspsURL(printer config.Printer) (string, error) {
	address, err := rtspsAddress(printer)
	if err != nil {
		return "", err
	}
	u := url.URL{
		Scheme: "rtsps",
		Host:   address,
		Path:   "/streaming/live/1",
		User:   url.UserPassword(printer.Username, printer.Password),
	}
	return u.String(), nil
}

func rtspsAddress(printer config.Printer) (string, error) {
	host, _, err := net.SplitHostPort(printer.Address)
	if err != nil || strings.TrimSpace(host) == "" {
		return "", fmt.Errorf("invalid printer address")
	}
	return net.JoinHostPort(strings.TrimSpace(host), "322"), nil
}

// publish stamps the frame with its sequence and capture time, stores it as
// the newest frame, and wakes every waiter. Frames are large; a new buffer
// per frame keeps handler-held bytes valid without copies.
func (c *capture) publish(f *Frame) {
	c.mu.Lock()
	if c.closed {
		// Replacing the closed notification channel would strand future
		// waiters on an open channel; discard post-close frames instead.
		c.mu.Unlock()
		return
	}
	seq := uint64(0)
	if c.frame != nil {
		seq = c.frame.Seq
	}
	f.Seq = seq + 1
	f.Captured = time.Now()
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
