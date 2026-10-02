// The FTPS transport performs the single bounded archive download behind
// one admitted preview attempt. It is deliberately retry-free: the
// scheduler admits one attempt per job generation and any outcome here
// consumes that allowance. All timing and size policy is fixed plan
// policy; the fake-server tests in this package shorten the timing seams
// temporarily instead of sleeping, and production code never writes them.
//
// Protocol posture, verified against the studied jlaffaye/ftp v0.2.4
// source:
//   - DialWithTLS plus DialWithDialFunc. The dial hook owns every raw
//     socket, TLS-wraps control and data connections with one shared
//     per-attempt config, handshakes only the control connection eagerly
//     inside the five-second bound, and leaves the data handshake lazy
//     until the first RETR read.
//   - DialWithContext has no effect while the dial hook is set, so all
//     cancellation lives in the hook's cancellation-safe socket registry.
//   - The library pins passive data connections to the resolved control
//     peer by default (TrustPASVIP is never enabled), so a hostile PASV
//     address cannot redirect the data socket.
//   - textproto responses are unbounded inside the library, so the
//     plaintext side of the completed control TLS connection enforces a
//     cumulative per-session read cap.
//   - DialWithShutTimeout is never set: it can move a deadline past the
//     attempt deadline, and no deadline here is ever extended.
package jobpreview

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/textproto"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jlaffaye/ftp"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// Fixed transport policy.
const (
	// ftpsControlReadCap bounds the cumulative bytes read from one FTPS
	// control connection. The library's textproto reader is unbounded,
	// so this per-session cap is the only per-response bound.
	ftpsControlReadCap = 256 << 10
	// ftpsClassifyBytes caps how much 550 reply text the classifier
	// inspects. Reply text is used for classification only and is never
	// logged or returned.
	ftpsClassifyBytes = 256
	// ftpsNameMaxBytes caps one candidate basename in UTF-8 bytes,
	// matching common filesystem name limits.
	ftpsNameMaxBytes = 255
)

// Transport timing and size seams. The values below are the plan's fixed
// policy: a 60-second attempt deadline, a five-second TCP plus control
// TLS bound, and the 32 MiB archive cap. Only this package's fake-server
// tests replace them, and always restore them.
var (
	ftpsAttemptTimeout = 60 * time.Second
	ftpsControlBound   = 5 * time.Second
	ftpsArchiveMax     = int64(32) << 20
)

// resolveControlAddr selects the implicit-TLS control address for one
// printer host. Production always uses port 990; only tests replace this
// hook to select an ephemeral control listener.
var resolveControlAddr = func(host string) string {
	return net.JoinHostPort(host, "990")
}

// ftpsProbeDirs are the remote directories probed in order. The printer
// stages downloads under /cache; the root is the fallback for other
// layouts. /data/Metadata/plate_N.gcode files are never treated as
// remote archives.
var ftpsProbeDirs = [...]string{"/cache", "/"}

// Fixed internal causes for transport failures. They carry no server
// reply text, filename, credential, or filesystem detail, and are never
// rendered to users.
var (
	errFtpsNoCandidate  = errors.New("no transferable archive basename")
	errFtpsAddress      = errors.New("printer address has no host")
	errFtpsControlCap   = errors.New("control response budget exhausted")
	errFtpsSizeMismatch = errors.New("downloaded size differs from the SIZE reply")
	errFtpsEmptyMatch   = errors.New("matched archive is empty")
	errFtpsAttemptEnded = errors.New("attempt already ended")
	// errBare550 is the exact ProtocolError Go's textproto produces for a
	// bare "550" status line with no space and no message. The live
	// diagnostic recorded the printers' empty 550 replies without their
	// exact wire spacing, so the SIZE classifier accepts this exact value
	// as the same definite file-missing answer; no other protocol error
	// text is ever matched.
	errBare550 = textproto.ProtocolError(`short response: "550"`)
)

// ftpsError is a fixed-category transfer failure tagged with the fixed
// protocol phase that produced it. The scheduler logs the category and
// phase once per attempt; the wrapped cause is for diagnosis only and
// never reaches users or logs.
type ftpsError struct {
	outcome string
	step    string
	err     error
}

func (e *ftpsError) Error() string { return "jobpreview: ftps " + e.outcome }

func (e *ftpsError) Unwrap() error { return e.err }

func (e *ftpsError) category() string { return e.outcome }

func (e *ftpsError) phase() string { return e.step }

func ftpsErr(category string, err error) error {
	return &ftpsError{outcome: category, err: err}
}

// ftpsErrAt tags a transport failure with the protocol phase it surfaced
// in. phase is always one of the fixed phase identifiers.
func ftpsErrAt(phase, category string, err error) error {
	return &ftpsError{outcome: category, step: phase, err: err}
}

// fetch is the production transfer behind the service's fetch field:
// probe the candidate basenames in /cache then / with SIZE, download the
// first match with one RETR into a capped temporary file, and parse the
// archive locally. The host comes from the configured MQTT address; the
// implicit-TLS control port is always 990; credentials come only from the
// printer configuration. The attempt is always capped at the fixed
// 60-second policy; a shorter caller deadline wins and is never
// extended. There is no retry, no REST or range read, no directory
// listing, no second archive, no TCP6000 fallback, and no reconnect.
// parseArchive's accepted partial results pass through unchanged, so a
// valid plate image stays ready even when a metadata entry failed.
// Failures carry their fixed protocol phase — connect, tls, login, size,
// retr, download — for the scheduler's single outcome record.
func fetch(ctx context.Context, printer config.Printer, job telemetry.JobView) (Result, error) {
	candidates := transferCandidates(job)
	if len(candidates) == 0 {
		return Result{}, ftpsErr(catNotFound, errFtpsNoCandidate)
	}
	host, _, err := net.SplitHostPort(printer.Address)
	if err != nil || host == "" {
		return Result{}, ftpsErr(catTransport, errFtpsAddress)
	}
	// The attempt stays capped at the fixed 60-second policy even when
	// the caller's context carries a longer deadline; a shorter caller
	// deadline still wins and is never extended.
	deadline := time.Now().Add(ftpsAttemptTimeout)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	attemptCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	// One config per attempt. The explicit ServerName makes control
	// (port 990) and data (passive port) session cache keys equal, so
	// the lazy data handshake resumes the control session; the cache is
	// fresh and never shared across printers. InsecureSkipVerify follows
	// the existing camera convention for self-signed printer
	// certificates.
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         printer.Serial,
		InsecureSkipVerify: true,
		ClientSessionCache: tls.NewLRUClientSessionCache(4),
	}
	d := newFtpsDialer(attemptCtx, deadline, tlsCfg)
	// Registered before the Quit defer below so the attempt ends with a
	// full socket sweep and the watcher joined, even on success.
	defer d.close()
	conn, err := ftp.Dial(resolveControlAddr(host),
		ftp.DialWithTLS(tlsCfg),
		ftp.DialWithDialFunc(d.dial))
	if err != nil {
		return Result{}, classifyAttempt(attemptCtx, err, d.lastPhase())
	}
	defer func() { _ = conn.Quit() }()
	if err := conn.Login(printer.Username, printer.Password); err != nil {
		return Result{}, classifyAttempt(attemptCtx, err, phaseLogin)
	}
	remote, size, err := probeArchive(conn, candidates)
	if err != nil {
		return Result{}, err
	}
	return downloadArchive(attemptCtx, conn, remote, size, job)
}

// transferCandidates derives the at most three distinct archive basenames
// probed on the printer, in plan order: the safe reported gcode file
// basename when it ends in .3mf, then the reported name with .3mf, or
// with .gcode.3mf appended otherwise. Invalid values are omitted; no
// valid candidate ends the attempt before any dial.
func transferCandidates(job telemetry.JobView) []string {
	var out []string
	add := func(name string) {
		if name == "" || len(out) == 3 ||
			len(name) > ftpsNameMaxBytes || !utf8.ValidString(name) {
			return
		}
		for _, existing := range out {
			if existing == name {
				return
			}
		}
		out = append(out, name)
	}
	if base, ok := gcodeFileBasename(job.GCodeFile); ok {
		add(base)
	}
	switch {
	case !safeJobName(job.Name):
	case has3MFSuffix(job.Name):
		add(job.Name)
	default:
		add(job.Name + ".3mf")
		add(job.Name + ".gcode.3mf")
	}
	return out
}

// has3MFSuffix reports a case-insensitive .3mf suffix.
func has3MFSuffix(name string) bool {
	return len(name) >= 4 && strings.EqualFold(name[len(name)-4:], ".3mf")
}

// safeJobName reports whether a reported subtask name may become a remote
// basename: bare display names only, no path separators, no dot path
// segments, no control characters. Spaces and non-ASCII letters stay
// valid.
func safeJobName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, `/\`) {
		return false
	}
	return !containsControl(name)
}

// containsControl reports any C0 control rune or DEL, including NUL,
// CR, and LF.
func containsControl(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

// gcodeFileBasename extracts the archive basename from a reported gcode
// file path. Control characters or a dot path segment reject the whole
// value instead of being trimmed away, and the basename must end in
// .3mf to count as an archive candidate.
func gcodeFileBasename(gcodeFile string) (string, bool) {
	if gcodeFile == "" || containsControl(gcodeFile) {
		return "", false
	}
	segments := strings.FieldsFunc(gcodeFile, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	for _, segment := range segments {
		if segment == "." || segment == ".." {
			return "", false
		}
	}
	if len(segments) == 0 {
		return "", false
	}
	base := segments[len(segments)-1]
	return base, has3MFSuffix(base)
}

// probeArchive asks SIZE for every candidate under /cache and then /,
// stopping at the first definite hit. Only a definite file-missing 550 —
// the studied printers' bare reply or worded missing-file text — advances
// to the next candidate; unsupported SIZE, permission wording, and every
// ambiguous reply end the attempt. A matched size must be positive and
// within the archive cap; an invalid match is terminal and never falls
// through to an older alternative.
func probeArchive(conn *ftp.ServerConn, candidates []string) (string, int64, error) {
	for _, dir := range ftpsProbeDirs {
		for _, name := range candidates {
			remote := dir + "/" + name
			if dir == "/" {
				remote = "/" + name
			}
			size, perr := conn.FileSize(remote)
			if perr != nil {
				// A nil result is one definite file-missing reply:
				// advance to the next candidate.
				if ferr := classifySize(perr); ferr != nil {
					return "", 0, ferr
				}
				continue
			}
			if size <= 0 {
				return "", 0, ftpsErrAt(phaseSize, catNotFound, errFtpsEmptyMatch)
			}
			if size > ftpsArchiveMax {
				return "", 0, ftpsErrAt(phaseSize, catOversized, nil)
			}
			return remote, size, nil
		}
	}
	return "", 0, ftpsErrAt(phaseSize, catNotFound, nil)
}

// classifySize interprets one failed SIZE probe over its full bounded
// reply text and returns nil only for a definite file-missing 550 that
// may advance to the next candidate. The studied printers answer a
// missing file with a 550 carrying no message, which this client sees in
// one of two forms; both are the same definite miss: a parsed 550 whose
// message is truly empty or whitespace-only, and the exact bare-"550"
// short-response ProtocolError emitted when the wire line carried no
// space at all. Worded missing-file replies stay misses too. A reply
// longer than the classification bound is rejected outright, so denial
// wording cannot hide past the inspected portion and read as missing. A
// 550 with permission wording, a 550 with any other nonempty text, and
// every other reply — including every other protocol error — are
// terminal. Reply text is inspected for classification only and is never
// logged or returned.
func classifySize(err error) error {
	var reply *textproto.Error
	if !errors.As(err, &reply) {
		// Exact-constant match only: any other short response, and any
		// other protocol error, stays terminal.
		if errors.Is(err, errBare550) {
			return nil
		}
		return ftpsErrAt(phaseSize, catTransport, err)
	}
	if reply.Code != ftp.StatusFileUnavailable {
		return ftpsErrAt(phaseSize, catTransport, err)
	}
	if len(reply.Msg) > ftpsClassifyBytes {
		return ftpsErrAt(phaseSize, catTransport, err)
	}
	if strings.TrimSpace(reply.Msg) == "" {
		// This printer family's definite file-missing answer: a bare
		// 550 with nothing after the code.
		return nil
	}
	low := strings.ToLower(reply.Msg)
	for _, denied := range [...]string{"denied", "permission", "not allowed"} {
		if strings.Contains(low, denied) {
			return ftpsErrAt(phaseSize, catTransport, err)
		}
	}
	for _, absent := range [...]string{"no such file", "not found", "could not get file size"} {
		if strings.Contains(low, absent) {
			return nil
		}
	}
	return ftpsErrAt(phaseSize, catTransport, err)
}

// dataStreamPhase names the failure phase of a data-channel read or
// close error: an identifiable data TLS handshake failure — a rejected
// alert or a non-TLS record header on a plaintext data port — reports
// tls, while a dropped or reset stream during the lazy handshake is
// indistinguishable from a mid-transfer drop and reports download.
func dataStreamPhase(err error) string {
	var rh tls.RecordHeaderError
	var alert tls.AlertError
	if errors.As(err, &rh) || errors.As(err, &alert) {
		return phaseTLS
	}
	return phaseDownload
}

// downloadArchive issues the single RETR and streams the response into a
// temporary file under the archive cap, even when the SIZE reply lied.
// The byte count must match SIZE exactly before the archive is parsed.
// The temporary file is removed on every completion, error, and
// cancellation path; only bounded metadata and the validated PNG ever
// stay in memory.
func downloadArchive(ctx context.Context, conn *ftp.ServerConn, remote string, size int64, job telemetry.JobView) (Result, error) {
	resp, err := conn.Retr(remote)
	if err != nil {
		return Result{}, classifyAttempt(ctx, err, phaseRetr)
	}
	tmp, err := os.CreateTemp("", "bmbpx-preview-*.3mf")
	if err != nil {
		_ = resp.Close()
		return Result{}, ftpsErrAt(phaseDownload, catTransport, err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	written, copyErr := io.Copy(tmp, io.LimitReader(resp, ftpsArchiveMax+1))
	shutErr := resp.Close()
	closeErr := tmp.Close()
	switch {
	case copyErr != nil:
		return Result{}, classifyAttempt(ctx, copyErr, dataStreamPhase(copyErr))
	case written > ftpsArchiveMax:
		return Result{}, ftpsErrAt(phaseDownload, catOversized, nil)
	case written != size:
		return Result{}, ftpsErrAt(phaseDownload, catTransport, errFtpsSizeMismatch)
	case shutErr != nil:
		return Result{}, classifyAttempt(ctx, shutErr, dataStreamPhase(shutErr))
	case closeErr != nil:
		return Result{}, ftpsErrAt(phaseDownload, catTransport, closeErr)
	}
	return parseArchive(tmpPath, job)
}

// classifyAttempt maps one attempt failure onto the fixed categories and
// stamps the protocol phase it surfaced in. Cancellation of the caller's
// context and the attempt deadline are distinct outcomes; socket-level
// timeouts land on the timeout category.
func classifyAttempt(ctx context.Context, err error, phase string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled {
		return ftpsErrAt(phase, catCancelled, err)
	}
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded ||
		errors.Is(err, os.ErrDeadlineExceeded) || isNetTimeout(err) {
		return ftpsErrAt(phase, catTimeout, err)
	}
	return ftpsErrAt(phase, catTransport, err)
}

// isNetTimeout reports network timeouts, including expired socket
// deadlines that surface as os.ErrDeadlineExceeded.
func isNetTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// ftpsDialer owns every raw socket of one attempt. Sockets are registered
// cancellation-safely: registration rejects and closes a socket when the
// attempt already ended, so no orphan connection can outlive the attempt.
// Every deadline is absolute and never later than the attempt deadline.
type ftpsDialer struct {
	ctx      context.Context
	deadline time.Time
	tlsCfg   *tls.Config

	mu        sync.Mutex
	firstDial bool
	phase     string
	sockets   map[net.Conn]struct{}
	closed    bool

	done      chan struct{}
	closeOnce sync.Once
	watchDone chan struct{}
}

func newFtpsDialer(ctx context.Context, deadline time.Time, tlsCfg *tls.Config) *ftpsDialer {
	d := &ftpsDialer{
		ctx:       ctx,
		deadline:  deadline,
		tlsCfg:    tlsCfg,
		firstDial: true,
		phase:     phaseNone,
		sockets:   make(map[net.Conn]struct{}),
		done:      make(chan struct{}),
		watchDone: make(chan struct{}),
	}
	go d.watch()
	return d
}

// markPhase records the protocol phase the dialer is about to run, so a
// dial-path failure is attributed to TCP establishment or to the control
// TLS handshake.
func (d *ftpsDialer) markPhase(phase string) {
	d.mu.Lock()
	d.phase = phase
	d.mu.Unlock()
}

// lastPhase reports the most recent protocol phase the dialer entered.
// A control-session failure after the handshake reports connect again:
// greeting and protocol establishment belong to the connection, not to
// TLS.
func (d *ftpsDialer) lastPhase() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.phase
}

// watch sweeps every registered socket when the attempt context ends or
// close is called, so blocked TLS handshakes, reply reads, and data
// copies all fail fast.
func (d *ftpsDialer) watch() {
	defer close(d.watchDone)
	select {
	case <-d.ctx.Done():
	case <-d.done:
	}
	d.sweep()
}

// sweep stamps every registered socket with an immediate deadline and
// closes it, then marks the attempt ended so later dials and
// registrations are rejected.
func (d *ftpsDialer) sweep() {
	now := time.Now()
	d.mu.Lock()
	d.closed = true
	sockets := make([]net.Conn, 0, len(d.sockets))
	for c := range d.sockets {
		sockets = append(sockets, c)
	}
	d.mu.Unlock()
	for _, c := range sockets {
		_ = c.SetDeadline(now)
		_ = c.Close()
	}
}

// close terminates the attempt without waiting for the caller's context:
// every socket is closed and the watcher goroutine is joined. Safe to
// call multiple times.
func (d *ftpsDialer) close() {
	d.closeOnce.Do(func() { close(d.done) })
	<-d.watchDone
}

// register adds one dialled raw socket. When the attempt already ended or
// its context is done, the socket is closed immediately under the socket
// mutex, so a cancellation racing the sweep cannot register a new socket.
func (d *ftpsDialer) register(c net.Conn) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ctx.Err(); d.closed || err != nil {
		if err == nil {
			err = errFtpsAttemptEnded
		}
		now := time.Now()
		_ = c.SetDeadline(now)
		_ = c.Close()
		return err
	}
	d.sockets[c] = struct{}{}
	return nil
}

// dial is the DialWithDialFunc hook. The first call of a session is
// always the control connection: its TCP dial and TLS handshake share the
// five-second bound, after which the fixed attempt deadline is restored
// before the hook returns. Later calls are passive data connections: TLS
// is wrapped but the handshake stays lazy until the first RETR read,
// bounded by the attempt deadline and by cancellation.
func (d *ftpsDialer) dial(network, address string) (net.Conn, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, errFtpsAttemptEnded
	}
	first := d.firstDial
	d.firstDial = false
	d.phase = phaseConnect
	d.mu.Unlock()
	if err := d.ctx.Err(); err != nil {
		return nil, err
	}

	bound := d.deadline
	dialCtx := d.ctx
	if first {
		if local := time.Now().Add(ftpsControlBound); local.Before(bound) {
			bound = local
		}
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithDeadline(d.ctx, bound)
		defer cancel()
	}
	raw, err := (&net.Dialer{}).DialContext(dialCtx, network, address)
	if err != nil {
		return nil, err
	}
	if err := d.register(raw); err != nil {
		return nil, err
	}
	if !first {
		// Data socket: lazy TLS handshake on the first RETR read,
		// bounded by the absolute attempt deadline and cancellation.
		_ = raw.SetDeadline(d.deadline)
		return tls.Client(raw, d.tlsCfg), nil
	}
	// Control socket: bound TCP plus TLS establishment by the
	// five-second budget, then restore the fixed attempt deadline for
	// the rest of the session (greeting, replies, QUIT).
	_ = raw.SetDeadline(bound)
	tlsConn := tls.Client(raw, d.tlsCfg)
	d.markPhase(phaseTLS)
	if err := tlsConn.HandshakeContext(dialCtx); err != nil {
		return nil, err
	}
	d.markPhase(phaseConnect)
	_ = raw.SetDeadline(d.deadline)
	// The cumulative response budget wraps the completed TLS
	// connection, so it counts plaintext protocol responses, not
	// ciphertext. RemoteAddr delegates through to the raw socket and
	// stays a *net.TCPAddr for the library's Dial assertion.
	return &ftpsControlConn{Conn: tlsConn, budget: ftpsControlReadCap}, nil
}

// ftpsControlConn bounds the cumulative plaintext bytes read from the
// control session: the library's textproto reader is unbounded, so the
// completed TLS connection is wrapped once more with this per-session
// cap. LocalAddr, RemoteAddr, and every other method delegate unchanged,
// keeping RemoteAddr a *net.TCPAddr as the library's Dial requires.
type ftpsControlConn struct {
	net.Conn
	budget int64
}

func (c *ftpsControlConn) Read(p []byte) (int, error) {
	if c.budget <= 0 {
		return 0, errFtpsControlCap
	}
	if int64(len(p)) > c.budget {
		p = p[:c.budget]
	}
	n, err := c.Conn.Read(p)
	c.budget -= int64(n)
	return n, err
}
