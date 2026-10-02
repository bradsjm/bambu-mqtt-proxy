// Package jobpreview shares one best-effort preview of the current print:
// the selected plate's archived render plus bounded sliced metadata,
// retrieved once per settled job generation directly from the printer's
// FTPS surface. Scheduler, transfer and handler wiring live in their own
// files; this file defines the shared data contracts. Every string in
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

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printerview"
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
// pending covers settling, waiting for transfer capacity, and an in-flight
// transfer; ready requires a validated PNG; unavailable covers unknown or
// idle printers without a job and terminal failed attempts. RetrievedAt is
// stamped by the retrieval caller in UTC RFC3339Nano once an archive has
// been accepted (including metadata-only results) and is null before that;
// ImageURL is non-null only while the PNG is ready.
type View struct {
	Status      string  `json:"status"`
	JobName     string  `json:"job_name"`
	Current     bool    `json:"current"`
	RetrievedAt *string `json:"retrieved_at"`
	ImageURL    *string `json:"image_url"`
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
// carry the parser's categories instead.
const (
	catReady     = "ready"
	catNotFound  = "not_found"
	catTimeout   = "timeout"
	catTransport = "transport"
	catCancelled = "cancelled"
)

// Scheduler policy. settleDelay is the continuous fresh RUNNING interval
// one job generation must show before its single attempt; freshnessWindow
// bounds the age of the last real report at admission and publish;
// attemptTimeout bounds the whole transfer. These are fixed internal
// policy, not settings.
const (
	settleDelay     = 60 * time.Second
	freshnessWindow = printerview.FreshnessWindow
	attemptTimeout  = 60 * time.Second

	runningState = "RUNNING"

	previewURLPrefix = "/camera/"
)

// Connectivity is the upstream readiness surface the scheduler polls.
// The upstream Pool satisfies it structurally; jobpreview never imports
// that package from production code.
type Connectivity interface {
	Status() map[string]bool
	Generation(serial string) uint64
}

// categorizer is implemented by transfer failures so the scheduler can log
// one fixed category per attempt. Errors never carry reply text,
// filenames, or credentials.
type categorizer interface {
	category() string
}

// entry is one published preview for one serial. gen and rev pin the entry
// to the job generation and revision it was retrieved for; a different
// live generation or revision refuses to serve it.
type entry struct {
	gen    uint64
	rev    uint64
	result Result
}

// admission is one consumed attempt: the chosen printer, the job snapshot
// it was admitted with, and the identity tuple re-validated before the
// result may publish.
type admission struct {
	printer  config.Printer
	job      telemetry.JobView
	gen      uint64
	rev      uint64
	epoch    uint64
	upstream uint64
}

// activeTransfer is the one in-flight attempt; the scheduler holds its
// cancel so a tick can stop it the moment the admission tuple breaks.
type activeTransfer struct {
	adm    *admission
	cancel context.CancelFunc
}

// Service schedules at most one bounded archive retrieval per preview job
// generation and shares the accepted result with every consumer. One
// global transfer slot exists; lookups are pure cache reads. fetch, readJob
// and now are seams for deterministic tests and must be replaced before
// Start or while the scheduler goroutine is stopped.
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

	mu        sync.Mutex
	entries   map[string]*entry
	attempted map[string]uint64 // serial -> generation whose attempt was consumed
	inflight  map[string]*admission
	active    *activeTransfer // sole in-flight transfer, cancellable by step
	worker    bool            // sole transfer slot
	started   bool
	closed    bool
	wg        sync.WaitGroup
	stopMu    sync.Mutex // serializes Close callers until the join completes
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
		printers:  sorted,
		bySerial:  bySerial,
		state:     state,
		conn:      connectivity,
		log:       log,
		ctx:       ctx,
		cancel:    cancel,
		fetch:     fetch,
		now:       time.Now,
		entries:   make(map[string]*entry),
		attempted: make(map[string]uint64),
		inflight:  make(map[string]*admission),
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

// step runs one scheduler tick. It first cancels the in-flight attempt if
// its admission tuple stopped holding — the scheduler owns invalidation,
// so transfers carry no ticker of their own. With the sole transfer slot
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
	// Reserve the sole slot without setting attempted. The WaitGroup slot
	// is registered in the same critical section so Close can never wait
	// on an unregistered goroutine.
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
// consumes its generation's single attempt. It performs no network work
// and never holds the service mutex across telemetry or pool reads.
// Eligibility is evaluated fresh, then re-read in full immediately before
// the attempt is marked.
func (s *Service) admit(now time.Time) *admission {
	status := s.conn.Status()
	for _, p := range s.printers {
		job, ok := s.readJob(p.Serial)
		if !ok || s.attemptedGeneration(p.Serial, job.Generation) {
			continue
		}
		if !eligible(job, status[p.Serial], s.conn.Generation(p.Serial), now) {
			continue
		}
		// Sole slot reserved: re-read everything before consuming the
		// attempt. A failed preflight releases the reservation without
		// consuming it.
		status = s.conn.Status()
		job, ok = s.readJob(p.Serial)
		upstream := s.conn.Generation(p.Serial)
		if !ok || !eligible(job, status[p.Serial], upstream, now) {
			continue
		}
		candidate := &admission{
			printer:  p,
			job:      job,
			gen:      job.Generation,
			rev:      job.Revision,
			epoch:    job.RunningEpoch,
			upstream: upstream,
		}
		if !s.begin(p.Serial, candidate) {
			return nil
		}
		return candidate
	}
	return nil
}

// transfer runs the admitted attempt under the shared attempt deadline. It
// re-verifies full eligibility immediately before the dial — a state
// change in that window is a consumed, cancelled attempt — and registers
// its cancel so scheduler ticks can stop it. The outcome is validated once
// more before publishing, and the sole slot is released only afterwards.
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
// may start dialling): the admission tuple still matches live state, the
// captured upstream generation still equals the live one, and the job is
// still a settled, fresh, connected RUNNING. Any pause, reconnect,
// revision, new generation or telemetry gap invalidates it.
func (s *Service) transferValid(adm *admission, now time.Time) bool {
	job, ok := s.readJob(adm.printer.Serial)
	upstream := s.conn.Generation(adm.printer.Serial)
	return ok &&
		job.Generation == adm.gen &&
		job.Revision == adm.rev &&
		job.RunningEpoch == adm.epoch &&
		upstream == adm.upstream &&
		eligible(job, s.conn.Status()[adm.printer.Serial], upstream, now)
}

// finish publishes the stamped result only when the same strict gate that
// guards the transfer still holds — full admission tuple plus running,
// fresh and connected state — and the service is not closed, checked under
// the publication mutex. Retention after FINISH/FAILED/IDLE applies to
// already published entries only; a transfer completing after its
// generation ended is discarded. Every outcome consumes the generation's
// allowance; discards leave no cache entry and no retry.
func (s *Service) finish(adm *admission, res Result, err error) {
	now := s.now()
	publish := s.transferValid(adm, now)
	var stamped Result
	if publish {
		stamped = stampedResult(adm.printer.Serial, res, now)
	}
	s.mu.Lock()
	delete(s.inflight, adm.printer.Serial)
	if publish && !s.closed {
		s.entries[adm.printer.Serial] = &entry{gen: adm.gen, rev: adm.rev, result: stamped}
	}
	s.mu.Unlock()
	s.log.Debug("job preview attempt finished",
		"serial", adm.printer.Serial, "outcome", outcomeCategory(err))
}

// eligible reports whether one preview attempt may start for this job
// snapshot: a running, connected, fresh job settled for at least
// settleDelay with a usable identity. Cloud project and task IDs are
// deliberately not required; local prints often have none.
func eligible(job telemetry.JobView, connected bool, upstreamGen uint64, now time.Time) bool {
	if !job.Active || job.State != runningState {
		return false
	}
	if job.RunningSince.IsZero() || now.Sub(job.RunningSince) < settleDelay {
		return false
	}
	if !connected {
		return false
	}
	if job.StateGen != upstreamGen || job.ObsGen != upstreamGen {
		return false
	}
	if job.ObsAt.IsZero() || now.Sub(job.ObsAt) > freshnessWindow {
		return false
	}
	return job.Name != "" || validArchiveBasename(job.GCodeFile)
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
// from published entries pinned to the sampled generation and revision.
// The job view is sampled before and after the entry copy, so a job
// boundary during the read serves the live projection without the stale
// payload. includeImage=false never copies PNG bytes; includeImage=true
// returns an independent copy.
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
	if e := s.entries[serial]; e != nil && e.gen == job1.Generation && e.rev == job1.Revision {
		res = copyResult(e.result, includeImage)
		have = true
	}
	inflight := s.inflight[serial]
	spent := s.attempted[serial]
	s.mu.Unlock()

	job2, ok2 := s.readJob(serial)
	if !ok2 || job2.Generation != job1.Generation || job2.Revision != job1.Revision {
		cur := job1
		if ok2 {
			cur = job2
		}
		return liveResult(cur, inflight, spent), true
	}
	if !have {
		return liveResult(job2, inflight, spent), true
	}
	// Current and job name always reflect the live view, even for results
	// retained after the job finished.
	res.Preview.JobName = job2.Name
	res.Preview.Current = job2.Active
	return res, true
}

// liveResult projects a job view without cached data. Pending covers a
// settling job, one waiting for the sole transfer slot, and a transfer in
// progress for the same generation. Unavailable covers an ended job and a
// generation whose single attempt was already consumed without a
// published result — it can never return to pending.
func liveResult(job telemetry.JobView, inflight *admission, spent uint64) Result {
	v := View{JobName: job.Name, Current: job.Active, Status: StatusPending}
	switch {
	case !job.Active:
		v.Status = StatusUnavailable
	case inflight != nil && inflight.gen == job.Generation:
		// The generation's transfer is reserved or in flight.
	case spent == job.Generation:
		v.Status = StatusUnavailable
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

// attemptedGeneration reports whether this generation's attempt is spent.
func (s *Service) attemptedGeneration(serial string, gen uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempted[serial] == gen
}

// begin consumes the generation's allowance and records the in-flight
// admission that Lookup projects as pending. It refuses — leaving the
// allowance untouched — when the service was closed between reservation
// and admission, when the caller no longer holds the reserved sole
// transfer slot, or when this generation's attempt was already consumed.
func (s *Service) begin(serial string, adm *admission) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.worker || s.attempted[serial] == adm.gen {
		return false
	}
	s.attempted[serial] = adm.gen
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
