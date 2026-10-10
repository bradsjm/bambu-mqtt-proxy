// Package jobpreview shares one best-effort preview of the current print:
// the selected plate's archived render plus bounded sliced metadata,
// retrieved directly from the printer's FTPS surface with bounded retries
// per job generation. Scheduler, transfer and handler wiring live in their
// own files; this file defines the shared data contracts. Every string in
// Metadata and every archive-derived View field originates in the
// printer's 3MF archive and is untrusted: it is bounded,
// whitespace-normalized plain text — never HTML, Markdown, or links — and
// every consumer must render it as text.
package jobpreview

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

// View.Status values. Exactly these strings appear on the wire.
const (
	StatusDisabled    = "disabled"
	StatusPending     = "pending"
	StatusReady       = "ready"
	StatusUnavailable = "unavailable"
)

// View is the display projection of one printer's job preview. Status
// pending covers waiting, queued, and in-flight transfers; ready requires
// a validated PNG; unavailable covers unknown or idle printers without a
// job, unusable archive data, and terminal failed attempts. RetrievedAt is
// stamped by the retrieval caller in UTC RFC3339Nano once an archive has
// been accepted (including metadata-only results) and is null before that;
// ImageURL is non-null only while the PNG is ready. Reason is the fixed
// machine identifier for the current status: the wait cause while pending,
// the failure category when the archive or transfer failed, and empty for
// a clean ready or an idle printer.
type View struct {
	Status      string  `json:"status"`
	JobName     string  `json:"job_name"`
	Current     bool    `json:"current"`
	RetrievedAt *string `json:"retrieved_at"`
	ImageURL    *string `json:"image_url"`
	Reason      string  `json:"reason,omitempty"`
}

// Result is one atomic preview read: the view, the optional accepted
// archive metadata, and the raw validated PNG. Read view, metadata and
// image from one entry; they are always published together so a lookup can
// never cross a job boundary. PNG is json:"-" and never appears in status
// JSON, SSE, or structured MCP output. Consumers receive copies; they can
// never modify cached service state.
type Result struct {
	Preview  View      `json:"job_preview"`
	Metadata *Metadata `json:"job_metadata"`
	PNG      []byte    `json:"-"`
}

// Disabled returns the shared projection used when the preview feature is
// switched off or no service exists: status disabled, no job name, not
// current, and null for every other field.
func Disabled() Result {
	return Result{Preview: View{Status: StatusDisabled}}
}

// Metadata holds the accepted archive facts for the selected plate. Every
// value is untrusted archive text or a validated number: strings are
// capped by rune count, arrays are bounded, nullable fields are nil when
// the archive did not supply a valid value, and Truncated marks that an
// otherwise accepted value reached an output cap and content was omitted.
// Materials carry archive filament IDs, never AMS slot IDs. Warnings are
// archived slicer advisories, never current printer faults. Process holds
// global sliced defaults, not per-object settings.
type Metadata struct {
	Source      string          `json:"source"`
	Title       *string         `json:"title"`
	Description *string         `json:"description"`
	Designer    *string         `json:"designer"`
	License     *string         `json:"license"`
	Plate       *Plate          `json:"plate"`
	Materials   []Material      `json:"materials"`
	Objects     []Object        `json:"objects"`
	ObjectCount *int            `json:"object_count"`
	Warnings    []SlicerWarning `json:"warnings"`
	Process     *Process        `json:"process"`
	Truncated   bool            `json:"truncated"`
}

// Plate is the selected plate: the archive plate index plus the values
// sourced from the plate sidecars. Index is present whenever a plate was
// selected; the remaining fields are nil when their entry did not supply a
// valid value.
type Plate struct {
	Index             int      `json:"index"`
	Name              *string  `json:"name"`
	BedType           *string  `json:"bed_type"`
	EstimatedSeconds  *float64 `json:"estimated_seconds"`
	WeightG           *float64 `json:"weight_g"`
	FirstLayerSeconds *float64 `json:"first_layer_seconds"`
	Supports          *bool    `json:"supports"`
}

// Material is one archive filament row: its archive filament ID and the
// sliced type, color, and usage when valid. Color is normalized to
// #RRGGBB or #RRGGBBAA and never implies the installed spool.
type Material struct {
	ID    string   `json:"id"`
	Type  *string  `json:"type"`
	Color *string  `json:"color"`
	UsedG *float64 `json:"used_g"`
	UsedM *float64 `json:"used_m"`
}

// Object is one grouped non-skipped archive object: the original name and
// how many instances the plate slices.
type Object struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// SlicerWarning is one archived slicer advisory. Code is the bounded
// archive identifier and Message is the fixed human text for known codes
// or the identifier with underscores converted to spaces. These are notes
// from slicing time, not live printer faults, and carry no live severity.
type SlicerWarning struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Process holds the global sliced defaults read from project settings.
// They are sliced profile values, not proof that every object uses them.
type Process struct {
	LayerHeightMM *float64 `json:"layer_height_mm"`
	InfillPercent *float64 `json:"infill_percent"`
}

// Fixed outcome categories. Every attempt logs exactly one of these, and
// the transport reuses them for its own failures; archive parse failures
// carry the parser's categories instead. The failure categories double as
// the final View.Reason values.
const (
	catReady     = "ready"
	catNotFound  = "not_found"
	catTimeout   = "timeout"
	catTransport = "transport"
	catCancelled = "cancelled"
)

// Fixed failure phases for the one Info record per attempt. The phase
// names the protocol step where the attempt stopped, independently of the
// outcome category: none covers a success and any failure before a
// protocol step started. These are fixed identifiers, never reply text.
const (
	phaseNone     = "none"
	phaseConnect  = "connect"
	phaseTLS      = "tls"
	phaseLogin    = "login"
	phaseSize     = "size"
	phaseRetr     = "retr"
	phaseDownload = "download"
	phaseArchive  = "archive"
)

// Scheduler policy. attemptTimeout bounds the whole transfer; maxAttempts
// bounds the tries per job generation; retryBase scales the linear
// backoff between retries. These are fixed internal policy, not settings.
const (
	attemptTimeout = 10 * time.Minute
	maxAttempts    = 3
	retryBase      = 30 * time.Second

	previewURLPrefix = "/camera/"
)

// Wait reasons for View.Reason while the preview is pending, plus the
// terminal selection reason when the printer moves selectors after the
// retry budget is spent. The archive failure categories in archive.go
// complete the pending and unavailable reasons from the plan's job preview
// catalog.
const (
	reasonWaitingForPrint    = "waiting_for_print"
	reasonWaitingForFileName = "waiting_for_file_name"
	reasonPrinterOffline     = "printer_offline"
	reasonQueued             = "queued"
	reasonDownloading        = "downloading"
	reasonRetryWait          = "retry_wait"
	reasonProxyStopping      = "proxy_stopping"
	reasonSelectionChanged   = "selection_changed"
)

// Connectivity is the printer link liveness surface the scheduler polls.
// The printer Links satisfy it structurally; jobpreview never imports that
// package from production code.
type Connectivity interface {
	Status() map[string]bool
}

// categorizer is implemented by transfer failures so the scheduler can log
// one fixed category per attempt. Errors never carry reply text,
// filenames, or credentials.
type categorizer interface {
	category() string
}

// phaser is implemented by errors carrying the fixed protocol phase that
// produced them, so the scheduler record can name where the attempt
// stopped without exposing reply text or filenames.
type phaser interface {
	phase() string
}

// entry is one published preview for one serial. gen pins the entry to
// the job generation it was retrieved for; plate and gcodeFile pin it to
// the archive selectors of the admitting snapshot. A different live
// generation or selector refuses to serve it.
type entry struct {
	gen       uint64
	plate     *int
	gcodeFile string
	result    Result
}

// admission is one started attempt: the chosen printer, the job snapshot
// it was admitted with, and the generation that may still publish.
type admission struct {
	printer config.Printer
	job     telemetry.JobView
	gen     uint64
}

// attempt is the per-printer retry bookkeeping for one job generation. A
// record whose gen differs from the live generation counts as absent and
// is replaced. Guarded by Service.mu.
type attempt struct {
	gen    uint64
	count  int
	nextAt time.Time
	final  bool
	reason string
}

// activeTransfer is the one in-flight attempt; the scheduler holds its
// cancel so a tick can stop it the moment a new job generation starts.
type activeTransfer struct {
	adm    *admission
	cancel context.CancelFunc
}

// Service schedules bounded archive retrievals for the current print,
// with up to maxAttempts tries per job generation, and shares the
// accepted result with every consumer. One global transfer slot exists;
// lookups are pure cache reads. fetch, readJob and now are seams for
// deterministic tests and must be replaced before Start or while the
// scheduler goroutine is stopped.
type Service struct {
	printers []config.Printer // sorted by serial
	bySerial map[string]config.Printer
	state    *telemetry.Cache
	conn     Connectivity
	log      *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	// fetch performs the bounded FTPS retrieval and archive parse. The
	// production implementation is fetch in ftps.go.
	fetch func(ctx context.Context, printer config.Printer, job telemetry.JobView) (Result, error)
	// readJob returns the current preview-job projection; production uses
	// the telemetry cache.
	readJob func(serial string) (telemetry.JobView, bool)
	// now is the scheduler clock.
	now func() time.Time

	activity *activity.Log // optional final-failure sink; nil records nothing

	mu       sync.Mutex
	entries  map[string]*entry
	attempts map[string]*attempt // serial -> retry bookkeeping for one generation
	inflight map[string]*admission
	active   *activeTransfer // sole in-flight transfer, cancellable by step
	worker   bool            // sole transfer slot
	started  bool
	closed   bool
	wg       sync.WaitGroup
	stopMu   sync.Mutex // serializes Close callers until the join completes
}

// New builds the preview service for the configured printers. The state
// cache and connectivity source are shared with the rest of the proxy;
// neither the constructor nor any lookup performs network work.
func New(printers []config.Printer, state *telemetry.Cache, connectivity Connectivity, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	sorted := slices.Clone(printers)
	slices.SortFunc(sorted, func(a, b config.Printer) int {
		return strings.Compare(a.Serial, b.Serial)
	})
	bySerial := make(map[string]config.Printer, len(sorted))
	for _, p := range sorted {
		bySerial[p.Serial] = p
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		printers: sorted,
		bySerial: bySerial,
		state:    state,
		conn:     connectivity,
		log:      log,
		ctx:      ctx,
		cancel:   cancel,
		fetch:    fetch,
		now:      time.Now,
		entries:  make(map[string]*entry),
		attempts: make(map[string]*attempt),
		inflight: make(map[string]*admission),
	}
	if state != nil {
		s.readJob = state.Job
	} else {
		s.readJob = func(string) (telemetry.JobView, bool) { return telemetry.JobView{}, false }
	}
	return s
}

// Start launches the one-second scheduler ticker. It is idempotent, and a
// closed service never starts.
func (s *Service) Start() {
	s.mu.Lock()
	if s.started || s.closed {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.wg.Add(1)
	s.mu.Unlock()
	go s.loop()
}

// loop ticks once per second and drives the scheduler until Close.
func (s *Service) loop() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.step(s.now())
		}
	}
}

// Close stops the scheduler and cancels any in-flight transfer, then joins
// the goroutines outside the service mutex. Concurrent and repeated calls
// serialize on stopMu: every caller returns only after the transfer is
// joined, never early.
func (s *Service) Close() {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.started = false // the ticker goroutine is joined; closed refuses restart
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
}

// SetActivity attaches the optional activity log that receives one
// job_preview_unavailable entry per terminal generation failure. Call it
// before Start; a nil log records nothing.
func (s *Service) SetActivity(log *activity.Log) {
	s.mu.Lock()
	s.activity = log
	s.mu.Unlock()
}

// step runs one scheduler tick. It first cancels the in-flight attempt if
// its job generation ended — the scheduler owns invalidation, so
// transfers carry no ticker of their own. With the sole transfer slot
// free it then picks the first eligible printer in sorted serial order and
// transfers in a fresh goroutine; a busy or closed service returns
// immediately. now is injected by tests; production passes the wall clock.
func (s *Service) step(now time.Time) {
	s.mu.Lock()
	active := s.active
	s.mu.Unlock()
	if active != nil && !s.transferValid(active.adm, now) {
		active.cancel()
	}

	s.mu.Lock()
	if s.worker || s.closed {
		s.mu.Unlock()
		return
	}
	// Reserve the sole slot without touching attempt bookkeeping. The
	// WaitGroup slot is registered in the same critical section so Close
	// can never wait on an unregistered goroutine.
	s.worker = true
	s.wg.Add(1)
	s.mu.Unlock()

	adm := s.admit(now)
	if adm == nil {
		s.releaseWorker()
		s.wg.Done()
		return
	}
	go s.transfer(adm)
}

// admit chooses the first eligible printer in sorted serial order and
// records one attempt against its retry budget. It performs no network
// work and never holds the service mutex across telemetry or pool reads.
// Eligibility is evaluated fresh, then re-read in full immediately before
// the attempt is marked, so a job change inside the reservation window
// releases the slot without consuming the budget.
func (s *Service) admit(now time.Time) *admission {
	status := s.conn.Status()
	for _, p := range s.printers {
		job, ok := s.readJob(p.Serial)
		if !ok || s.skipped(p.Serial, job, now) {
			s.finalizeExhaustedSelection(p.Serial, job, ok)
			continue
		}
		if !eligible(job, status[p.Serial]) {
			continue
		}
		// Sole slot reserved: re-read everything before consuming one
		// attempt. A failed preflight releases the reservation without
		// touching the budget.
		wantGen := job.Generation
		status = s.conn.Status()
		job, ok = s.readJob(p.Serial)
		if !ok || job.Generation != wantGen || s.skipped(p.Serial, job, now) || !eligible(job, status[p.Serial]) {
			s.finalizeExhaustedSelection(p.Serial, job, ok)
			continue
		}
		candidate := &admission{
			printer: p,
			job:     job,
			gen:     job.Generation,
		}
		if !s.begin(p.Serial, candidate) {
			return nil
		}
		return candidate
	}
	return nil
}

// skipped reports whether the printer must not start an attempt now: its
// retry record for the live generation is final, backing off, or spent,
// or a valid entry is already published for the live selectors.
func (s *Service) skipped(serial string, job telemetry.JobView, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.attempts[serial]
	if rec != nil && rec.gen == job.Generation {
		if rec.final || rec.count >= maxAttempts || now.Before(rec.nextAt) {
			return true
		}
	}
	return entryValid(s.entries[serial], job)
}

// finalizeExhaustedSelection marks a spent generation final when its
// published entry no longer matches the live selectors and no attempt is
// in flight: the budget cannot serve the new plate or file, so the wall
// names the selection change instead of waiting forever. It records one
// job_preview_unavailable entry outside the service mutex.
func (s *Service) finalizeExhaustedSelection(serial string, job telemetry.JobView, ok bool) {
	if !ok {
		return
	}
	s.mu.Lock()
	rec := s.attempts[serial]
	if rec == nil || rec.gen != job.Generation || rec.final || rec.count < maxAttempts {
		s.mu.Unlock()
		return
	}
	if s.inflight[serial] != nil || entryValid(s.entries[serial], job) {
		s.mu.Unlock()
		return
	}
	rec.final = true
	rec.reason = reasonSelectionChanged
	activityLog := s.activity
	s.mu.Unlock()
	if activityLog != nil {
		t := reasonText(reasonSelectionChanged)
		activityLog.Record(serial, "job_preview_unavailable", activity.Info,
			"Print details unavailable, "+strings.ToLower(t[:1])+t[1:])
	}
}

// entryValid reports whether the published entry serves the live job: the
// generation and the archive selectors — gcode file and plate index —
// all match.
func entryValid(e *entry, job telemetry.JobView) bool {
	if e == nil || e.gen != job.Generation || e.gcodeFile != job.GCodeFile {
		return false
	}
	if e.plate == nil || job.PlateIndex == nil {
		return e.plate == nil && job.PlateIndex == nil
	}
	return *e.plate == *job.PlateIndex
}

// selectorsEqual reports whether the live job still carries the selectors
// of the admitting snapshot.
func selectorsEqual(live, admitted telemetry.JobView) bool {
	if live.Generation != admitted.Generation || live.GCodeFile != admitted.GCodeFile {
		return false
	}
	if live.PlateIndex == nil || admitted.PlateIndex == nil {
		return live.PlateIndex == nil && admitted.PlateIndex == nil
	}
	return *live.PlateIndex == *admitted.PlateIndex
}

// transfer runs the admitted attempt under the shared attempt deadline. It
// re-verifies the job generation immediately before the dial — a new job
// in that window cancels the attempt and refunds its budget — and
// registers its cancel so scheduler ticks can stop it. The outcome is
// recorded once more before publishing, and the sole slot is released
// only afterwards.
func (s *Service) transfer(adm *admission) {
	defer s.wg.Done()
	ctx, cancel := context.WithTimeout(s.ctx, attemptTimeout)
	defer cancel()

	s.mu.Lock()
	s.active = &activeTransfer{adm: adm, cancel: cancel}
	s.mu.Unlock()

	// settle tears the attempt down in a fixed order: first drop the
	// cancellable registration so no later tick can touch a finished
	// attempt, then publish, and only then free the sole slot — the next
	// admission can never start while the outcome is still settling.
	settle := func(res Result, err error) {
		s.mu.Lock()
		if s.active != nil && s.active.adm == adm {
			s.active = nil
		}
		s.mu.Unlock()
		s.finish(adm, res, err)
		s.releaseWorker()
	}

	if !s.transferValid(adm, s.now()) {
		settle(Result{}, context.Canceled)
		return
	}

	res, err := s.fetch(ctx, adm.printer, adm.job)
	settle(res, err)
}

// transferValid reports whether an admitted attempt may keep running (or
// may start dialling): the live job is still the admitted generation.
// Disconnects, pauses, report gaps, and field enrichment never cancel;
// only a new generation or Close cancels.
func (s *Service) transferValid(adm *admission, now time.Time) bool {
	_ = now
	job, ok := s.readJob(adm.printer.Serial)
	return ok && job.Generation == adm.gen
}

// finish records one attempt outcome: it publishes an accepted archive
// result against unchanged selectors, schedules a retry for a transient
// failure, or marks the generation final and records one
// job_preview_unavailable activity entry. Bookkeeping mutates only while
// the attempt record still belongs to the admitted generation; an outcome
// for a replaced record records nothing at all. When the live job has
// moved to another generation but the record still belongs to the
// admitted one, the result is discarded and its budget refunded, like a
// cancellation. The activity entry is recorded outside the service mutex.
func (s *Service) finish(adm *admission, res Result, err error) {
	serial := adm.printer.Serial
	now := s.now()
	outcome := outcomeCategory(err)
	accepted := res.Metadata != nil || len(res.PNG) > 0
	live, ok := s.readJob(serial)
	stale := !ok || live.Generation != adm.gen
	selectorsOK := !stale && selectorsEqual(live, adm.job)

	s.mu.Lock()
	rec := s.attempts[serial]
	if rec == nil || rec.gen != adm.gen {
		s.mu.Unlock()
		return
	}
	if s.inflight[serial] == adm {
		delete(s.inflight, serial)
	}
	var stamped Result
	published := false
	retryIn := time.Duration(0)
	finalReason := ""
	switch {
	case s.closed:
		// No publish and no schedule.
	case outcome == catCancelled:
		// A generation change or Close cancelled the attempt: the
		// budget it consumed is refunded and nothing is recorded.
		rec.count--
	case stale:
		// The job moved on while the attempt was in flight, but the
		// replacement has not started yet, so the record still
		// belongs to the old admission. The result belongs to no
		// live job: refund the budget like a cancellation and record
		// nothing.
		rec.count--
	case accepted && selectorsOK:
		stamped = stampedResult(serial, res, now)
		if err != nil {
			stamped.Preview.Reason = outcome
		}
		s.entries[serial] = &entry{
			gen:       adm.gen,
			plate:     clonePlate(adm.job.PlateIndex),
			gcodeFile: adm.job.GCodeFile,
			result:    stamped,
		}
		published = true
	case accepted:
		// The archive parsed but the plate or file selectors moved
		// within the same generation: retry immediately on the next
		// tick with the new selectors. When the budget is already
		// spent no retry can start, so the generation ends with the
		// selection change named.
		if rec.count >= maxAttempts {
			rec.final = true
			rec.reason = reasonSelectionChanged
			finalReason = reasonSelectionChanged
		} else {
			rec.nextAt = now
			retryIn = 0
		}
	default:
		if (outcome == catNotFound || outcome == catTimeout || outcome == catTransport) && rec.count < maxAttempts {
			rec.nextAt = now.Add(retryBase * time.Duration(rec.count))
			rec.reason = reasonRetryWait
			retryIn = retryBase * time.Duration(rec.count)
		} else {
			rec.final = true
			rec.reason = outcome
			finalReason = outcome
		}
	}
	count := rec.count
	activityLog := s.activity
	s.mu.Unlock()

	if finalReason != "" && activityLog != nil {
		t := reasonText(finalReason)
		activityLog.Record(serial, "job_preview_unavailable", activity.Info,
			"Print details unavailable, "+strings.ToLower(t[:1])+t[1:])
	}
	// Exactly one fixed-field Info record per attempt: outcome is the
	// fetched result's category, phase names the protocol step that
	// stopped a failed attempt, published separates a served result from
	// a recorded-but-discarded one, and attempt is the generation's
	// consumed budget after this outcome. No reply text, filename,
	// credential, or error text ever joins the record.
	args := []any{
		"serial", serial,
		"outcome", outcome,
		"phase", outcomePhase(err),
		"published", published,
		"attempt", count,
	}
	if retryIn > 0 {
		args = append(args, "retry_in_s", retryIn.Seconds())
	}
	s.log.Info("job preview attempt finished", args...)
}

// eligible reports whether one preview attempt may start for this job
// snapshot: an active, connected, executing job with a usable identity.
// Cloud project and task IDs are deliberately not required; local prints
// often have none.
func eligible(job telemetry.JobView, connected bool) bool {
	if !job.Active {
		return false
	}
	switch job.State {
	case "RUNNING", "PAUSE", "PAUSED":
	default:
		return false
	}
	if !connected {
		return false
	}
	return job.Name != "" || validArchiveBasename(job.GCodeFile)
}

// reasonText renders one View.Reason as the plain-English wall text. An
// unknown reason is a programming error and reads as a generic failure.
func reasonText(reason string) string {
	switch reason {
	case reasonWaitingForPrint:
		return "Waiting for the print to start"
	case reasonWaitingForFileName:
		return "Waiting for the printer to report the file name"
	case reasonPrinterOffline:
		return "Waiting for the printer to reconnect"
	case reasonQueued:
		return "Waiting for another print file download to finish"
	case reasonDownloading:
		return "Downloading the print file from the printer"
	case reasonRetryWait:
		return "Download failed, retrying shortly"
	case reasonProxyStopping:
		return "Proxy stopping"
	case reasonSelectionChanged:
		return "Printer changed the selected plate or file"
	case catNotFound:
		return "Print file not found on the printer"
	case catTimeout:
		return "Print file download stalled"
	case catTransport:
		return "Could not read the print file from the printer"
	case catArchive:
		return "Print file could not be read"
	case catOversized:
		return "Print file exceeds the size limit"
	case catAmbiguousPlate:
		return "Could not tell which plate is printing"
	case catImageMissing:
		return "Print file has no preview image"
	}
	return "Print details unavailable"
}

// clonePlate duplicates one optional plate selector so cached entries
// share nothing with the admitting snapshot.
func clonePlate(p *int) *int {
	if p == nil {
		return nil
	}
	c := *p
	return &c
}

// validArchiveBasename reports whether a reported gcode file can name a
// printable archive: no control characters, no dot path segments, and a
// nonempty basename ending in .3mf. Candidate derivation is the
// transport's job; the scheduler only gates on obvious validity.
func validArchiveBasename(file string) bool {
	if file == "" {
		return false
	}
	for _, r := range file {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	segs := strings.FieldsFunc(file, func(r rune) bool { return r == '/' || r == '\\' })
	if len(segs) == 0 {
		return false
	}
	for _, seg := range segs {
		if seg == "." || seg == ".." {
			return false
		}
	}
	base := segs[len(segs)-1]
	dot := strings.LastIndexByte(base, '.')
	return dot >= 0 && strings.EqualFold(base[dot:], ".3mf")
}

// Lookup returns the cached preview for one serial and whether the serial
// is configured. It never performs network work: pending and unavailable
// are live projections of the current job view, and payloads come only
// from published entries pinned to the sampled generation and selectors.
// The job view is sampled before and after the entry copy, so a job
// boundary or selector change during the read serves the live projection
// without the stale payload. includeImage=false never copies PNG bytes;
// includeImage=true returns an independent copy.
func (s *Service) Lookup(serial string, includeImage bool) (Result, bool) {
	if _, configured := s.bySerial[serial]; !configured {
		return Result{}, false
	}
	job1, ok := s.readJob(serial)
	if !ok {
		return Result{Preview: View{Status: StatusUnavailable}}, true
	}
	var res Result
	have := false
	s.mu.Lock()
	if e := s.entries[serial]; entryValid(e, job1) {
		res = copyResult(e.result, includeImage)
		have = true
	}
	inflight := s.inflight[serial]
	var rec *attempt
	if a := s.attempts[serial]; a != nil && a.gen == job1.Generation {
		cp := *a
		rec = &cp
	}
	closed := s.closed
	worker := s.worker
	s.mu.Unlock()
	connected := s.conn.Status()[serial]

	job2, ok2 := s.readJob(serial)
	if !ok2 || job2.Generation != job1.Generation || !selectorsEqual(job2, job1) {
		cur := job1
		if ok2 {
			cur = job2
		}
		return s.liveResult(cur, inflight, rec, connected, closed, worker), true
	}
	if !have {
		return s.liveResult(job2, inflight, rec, connected, closed, worker), true
	}
	// Current and job name always reflect the live view, even for results
	// retained after the job finished.
	res.Preview.JobName = job2.Name
	res.Preview.Current = job2.Active
	return res, true
}

// liveResult projects a job view without cached data. The reason table
// names the exact wait or failure behind every non-ready status; only an
// attempt record for the live generation is considered.
func (s *Service) liveResult(job telemetry.JobView, inflight *admission, rec *attempt, connected, closed, worker bool) Result {
	v := View{JobName: job.Name, Current: job.Active, Status: StatusPending}
	switch {
	case closed:
		v.Status = StatusUnavailable
		v.Reason = reasonProxyStopping
	case !job.Active:
		v.Status = StatusUnavailable
	case inflight != nil && inflight.gen == job.Generation:
		v.Reason = reasonDownloading
	case rec != nil && rec.final:
		v.Status = StatusUnavailable
		v.Reason = rec.reason
	case rec != nil && rec.count > 0:
		v.Reason = reasonRetryWait
	case !connected:
		v.Reason = reasonPrinterOffline
	case job.State == "PREPARE" || job.State == "SLICING":
		v.Reason = reasonWaitingForPrint
	case job.Name == "" && !validArchiveBasename(job.GCodeFile):
		v.Reason = reasonWaitingForFileName
	case worker && (inflight == nil || inflight.gen != job.Generation):
		v.Reason = reasonQueued
	default:
		v.Reason = reasonDownloading
	}
	return Result{Preview: v}
}

// stampedResult copies a fetch outcome into a publishable result: status
// ready only for a validated PNG, retrieved_at for any accepted archive
// data (including metadata-only results), and the versioned image URL only
// alongside ready. Metadata and PNG are copied so the fetch result never
// aliases cache state.
func stampedResult(serial string, res Result, now time.Time) Result {
	out := Result{
		Preview:  View{Status: StatusUnavailable},
		Metadata: copyMetadata(res.Metadata),
		PNG:      bytes.Clone(res.PNG),
	}
	if len(out.PNG) > 0 {
		out.Preview.Status = StatusReady
		sum := sha256.Sum256(out.PNG)
		url := previewURLPrefix + serial + "/preview?v=" + hex.EncodeToString(sum[:])
		out.Preview.ImageURL = &url
	}
	if out.Metadata != nil || len(out.PNG) > 0 {
		ts := now.UTC().Format(time.RFC3339Nano)
		out.Preview.RetrievedAt = &ts
	}
	return out
}

// copyResult duplicates a cached entry result for one caller. Callers can
// never reach cached service state through the returned value.
func copyResult(r Result, includeImage bool) Result {
	out := Result{
		Preview: View{
			Status:      r.Preview.Status,
			JobName:     r.Preview.JobName,
			Current:     r.Preview.Current,
			RetrievedAt: clonePtr(r.Preview.RetrievedAt),
			ImageURL:    clonePtr(r.Preview.ImageURL),
			Reason:      r.Preview.Reason,
		},
		Metadata: copyMetadata(r.Metadata),
	}
	if includeImage {
		out.PNG = bytes.Clone(r.PNG)
	}
	return out
}

// copyMetadata deep-copies one metadata value, including every pointer and
// slice a caller could reach.
func copyMetadata(m *Metadata) *Metadata {
	if m == nil {
		return nil
	}
	out := &Metadata{
		Source:      m.Source,
		Title:       clonePtr(m.Title),
		Description: clonePtr(m.Description),
		Designer:    clonePtr(m.Designer),
		License:     clonePtr(m.License),
		Plate:       copyPlate(m.Plate),
		Process:     copyProcess(m.Process),
		ObjectCount: clonePtr(m.ObjectCount),
		Truncated:   m.Truncated,
		Materials:   make([]Material, len(m.Materials)),
		Objects:     slices.Clone(m.Objects),
		Warnings:    slices.Clone(m.Warnings),
	}
	for i, mat := range m.Materials {
		out.Materials[i] = Material{
			ID:    mat.ID,
			Type:  clonePtr(mat.Type),
			Color: clonePtr(mat.Color),
			UsedG: clonePtr(mat.UsedG),
			UsedM: clonePtr(mat.UsedM),
		}
	}
	return out
}

// copyPlate deep-copies one optional plate value, including every pointer
// field a caller could reach.
func copyPlate(p *Plate) *Plate {
	if p == nil {
		return nil
	}
	return &Plate{
		Index:             p.Index,
		Name:              clonePtr(p.Name),
		BedType:           clonePtr(p.BedType),
		EstimatedSeconds:  clonePtr(p.EstimatedSeconds),
		WeightG:           clonePtr(p.WeightG),
		FirstLayerSeconds: clonePtr(p.FirstLayerSeconds),
		Supports:          clonePtr(p.Supports),
	}
}

// copyProcess deep-copies one optional process value, including every
// pointer field a caller could reach.
func copyProcess(p *Process) *Process {
	if p == nil {
		return nil
	}
	return &Process{
		LayerHeightMM: clonePtr(p.LayerHeightMM),
		InfillPercent: clonePtr(p.InfillPercent),
	}
}

// clonePtr duplicates one optional scalar so callers share nothing.
func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	c := *p
	return &c
}

// begin records one attempt against the generation's retry budget and
// registers the in-flight admission that Lookup projects as pending. A
// record for another generation counts as absent and is replaced. It
// refuses — leaving the budget untouched — when the service was closed
// between reservation and admission, when the caller no longer holds the
// reserved sole transfer slot, or when this generation's budget is final,
// backing off, or spent.
func (s *Service) begin(serial string, adm *admission) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.worker {
		return false
	}
	rec := s.attempts[serial]
	if rec == nil || rec.gen != adm.gen {
		rec = &attempt{gen: adm.gen}
		s.attempts[serial] = rec
	}
	now := s.now()
	if rec.final || rec.count >= maxAttempts || now.Before(rec.nextAt) {
		return false
	}
	rec.count++
	rec.nextAt = time.Time{}
	s.inflight[serial] = adm
	return true
}

// releaseWorker frees the sole transfer slot.
func (s *Service) releaseWorker() {
	s.mu.Lock()
	s.worker = false
	s.mu.Unlock()
}

// outcomeCategory maps one attempt outcome to its fixed log category.
func outcomeCategory(err error) string {
	if err == nil {
		return catReady
	}
	if errors.Is(err, context.Canceled) {
		return catCancelled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return catTimeout
	}
	if c := errorCategory(err); c != "" {
		return c
	}
	var c categorizer
	if errors.As(err, &c) {
		return c.category()
	}
	return catTransport
}

// outcomePhase maps one attempt outcome to its fixed failure phase:
// archive parse failures report the archive phase, transport errors
// report the phase stamped at their failure site, and a success or a
// failure before any protocol step reports none.
func outcomePhase(err error) string {
	if err == nil {
		return phaseNone
	}
	if errorCategory(err) != "" {
		return phaseArchive
	}
	var p phaser
	if errors.As(err, &p) && p.phase() != "" {
		return p.phase()
	}
	return phaseNone
}
