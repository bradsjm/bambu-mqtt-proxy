// Package jobpreview retrieves archived thumbnails and slice facts from printers.
package jobpreview

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
)

// Preview statuses describe retrieval progress.
const (
	StatusDisabled    = "disabled"
	StatusPending     = "pending"
	StatusReady       = "ready"
	StatusUnavailable = "unavailable"
)

// View holds archived preview display data.
type View struct {
	// Status names the retrieval outcome.
	Status string `json:"status"`
	// JobName names the archived print.
	JobName string `json:"job_name"`
	// Current reports whether the archive selectors match the current document.
	Current bool `json:"current"`
	// RetrievedAt contains the UTC retrieval timestamp.
	RetrievedAt *string `json:"retrieved_at"`
	// ImageURL contains the versioned thumbnail route.
	ImageURL *string `json:"image_url"`
}

// Result holds archived preview display data.
type Result struct {
	// Preview contains thumbnail retrieval state.
	Preview View `json:"job_preview"`
	// Metadata contains archived slice facts.
	Metadata *Metadata `json:"job_metadata"`
	// PNG contains the archived image bytes.
	PNG []byte `json:"-"`
	// ArchiveBytes is the downloaded 3MF size, kept for diagnostics.
	ArchiveBytes int `json:"-"`
}

// Disabled returns the disabled preview projection.
func Disabled() Result {
	return Result{Preview: View{Status: StatusDisabled}}
}

// Metadata holds archived preview display data.
type Metadata struct {
	// Source identifies the archive source.
	Source string `json:"source"`
	// Title contains the model title.
	Title *string `json:"title"`
	// Description contains plain model-authored text.
	Description *string `json:"description"`
	// Designer names the model designer.
	Designer *string `json:"designer"`
	// License contains the model license.
	License *string `json:"license"`
	// Plate contains the selected plate facts.
	Plate *Plate `json:"plate"`
	// Materials contains sliced filament usage.
	Materials []Material `json:"materials"`
	// Objects contains grouped sliced objects.
	Objects []Object `json:"objects"`
	// ObjectCount counts non-skipped sliced instances.
	ObjectCount *int `json:"object_count"`
	// Warnings contains archived slicer advisories.
	Warnings []SlicerWarning `json:"warnings"`
	// Process contains global sliced settings.
	Process *Process `json:"process"`
}

// Plate holds archived preview display data.
type Plate struct {
	// Index identifies the selected archive plate.
	Index int `json:"index"`
	// Name contains the archived name.
	Name *string `json:"name"`
	// BedType names the sliced build surface.
	BedType *string `json:"bed_type"`
	// EstimatedSeconds contains the estimated print duration.
	EstimatedSeconds *float64 `json:"estimated_seconds"`
	// WeightG contains the sliced plate weight in grams.
	WeightG *float64 `json:"weight_g"`
	// FirstLayerSeconds contains the first-layer duration.
	FirstLayerSeconds *float64 `json:"first_layer_seconds"`
	// Supports reports sliced support usage.
	Supports *bool `json:"supports"`
}

// Material holds archived preview display data.
type Material struct {
	// ID identifies an archive filament, not an AMS slot.
	ID string `json:"id"`
	// Type names the sliced filament type.
	Type *string `json:"type"`
	// Color contains the sliced filament color.
	Color *string `json:"color"`
	// UsedG contains material usage in grams.
	UsedG *float64 `json:"used_g"`
	// UsedM contains material usage in meters.
	UsedM *float64 `json:"used_m"`
}

// Object holds archived preview display data.
type Object struct {
	// Name contains the archived name.
	Name string `json:"name"`
	// Count counts grouped instances.
	Count int `json:"count"`
}

// SlicerWarning holds archived preview display data.
type SlicerWarning struct {
	// Code identifies the slicer advisory.
	Code string `json:"code"`
	// Message contains readable advisory text.
	Message string `json:"message"`
}

// Process holds archived preview display data.
type Process struct {
	// LayerHeightMM contains the sliced layer height.
	LayerHeightMM *float64 `json:"layer_height_mm"`
	// InfillPercent contains the sliced infill percentage.
	InfillPercent *float64 `json:"infill_percent"`
}

// State supplies authoritative printer documents and ordered events.
type State interface {
	// Get reads the current document and connectivity.
	Get(string) (printer.Doc, bool, bool)
	// Subscribe registers a non-blocking report callback.
	Subscribe(func(printer.Event))
}

// jobKey identifies an archive selection.
type jobKey struct {
	// Name contains subtask_name.
	Name string
	// File contains gcode_file.
	File string
	// Plate contains plate_idx.
	Plate int
}

// key reads the archive selectors from a document.
func key(d printer.Doc) jobKey {
	name, _ := d.JobName()
	file, _ := d.GcodeFile()
	plate, _ := d.PlateIndex()
	return jobKey{name, file, plate}
}

// archive returns the selectors that identify the downloaded file. A .3mf
// gcode_file names the archive alone: the printer rewrites subtask_name
// characters after the project_file echo without changing the file.
func (k jobKey) archive() jobKey {
	if strings.HasSuffix(strings.ToLower(k.File), ".3mf") {
		k.Name = ""
	}
	return k
}

// active reports whether a preview can be fetched for the document.
func active(d printer.Doc) bool {
	state, _ := d.GcodeState()
	return state == "PREPARE" || state == "RUNNING" || state == "PAUSE" || state == "PAUSED"
}

// entry holds the last accepted immutable result.
type entry struct {
	// key identifies the archived job.
	key jobKey
	// result contains the thumbnail and facts.
	result Result
}

// Service owns one cancellable fetch worker per printer.
type Service struct {
	// printers contains configured printer credentials.
	printers map[string]config.Printer
	// state supplies authoritative documents.
	state State
	// log records queue loss and retrieval failures.
	log *slog.Logger
	// activity records exhausted retrievals.
	activity *activity.Log
	// events hands report callbacks to Run.
	events chan printer.Event
	// fetch retrieves one archive.
	fetch func(context.Context, config.Printer, jobKey) (Result, error)
	// mu guards entries and pending keys.
	mu sync.Mutex
	// entries holds the last accepted results.
	entries map[string]entry
	// pending holds ongoing retrieval selections.
	pending map[string]jobKey
}

// New constructs a cache-only preview service and subscribes to events.
func New(printers []config.Printer, state State, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	s := &Service{printers: make(map[string]config.Printer), state: state, log: log.With("module", "jobpreview"), events: make(chan printer.Event, 64), fetch: fetch, entries: make(map[string]entry), pending: make(map[string]jobKey)}
	for _, p := range printers {
		s.printers[p.Serial] = p
	}
	state.Subscribe(func(e printer.Event) {
		if _, ok := s.printers[e.Serial]; !ok {
			return
		}
		select {
		case s.events <- e:
		default:
			s.log.Warn("jobpreview event queue full", "serial", e.Serial)
		}
	})
	return s
}

// SetActivity supplies the optional activity sink before Run starts.
func (s *Service) SetActivity(log *activity.Log) { s.activity = log }

// Run dispatches fetch workers and joins them on shutdown.
func (s *Service) Run(ctx context.Context) error {
	cancels := make(map[string]context.CancelFunc)
	keys := make(map[string]jobKey)
	var wg sync.WaitGroup
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
		wg.Wait()
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-s.events:
			k := key(e.Cur)
			if old, ok := keys[e.Serial]; ok && old.archive() != k.archive() {
				cancels[e.Serial]()
				delete(keys, e.Serial)
			}
			// A job begins at its first prepare or, without one, at RUNNING.
			prevState, _ := e.Prev.GcodeState()
			newJob := (slices.Contains(e.Kinds, "print_preparing") || slices.Contains(e.Kinds, "print_started")) && prevState != "PREPARE" && prevState != "SLICING"
			trigger := newJob || (slices.Contains(e.Kinds, "state_initial") && active(e.Cur)) || (key(e.Prev).archive() != k.archive() && active(e.Cur))
			if !trigger {
				continue
			}
			s.mu.Lock()
			ongoing, fetching := s.pending[e.Serial]
			s.mu.Unlock()
			if fetching && ongoing.archive() == k.archive() {
				continue
			}
			workerCtx, cancel := context.WithCancel(ctx)
			cancels[e.Serial], keys[e.Serial] = cancel, k
			s.mu.Lock()
			s.pending[e.Serial] = k
			s.mu.Unlock()
			wg.Add(1)
			go func(serial string, k jobKey) { defer wg.Done(); s.retrieve(workerCtx, serial, k) }(e.Serial, k)
		}
	}
}

// retrieve retries an archive three times with a fixed interval.
func (s *Service) retrieve(ctx context.Context, serial string, k jobKey) {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		started := time.Now()
		var result Result
		result, err = s.fetch(attemptCtx, s.printers[serial], k)
		cancel()
		if ctx.Err() != nil {
			return
		}
		d, _, _ := s.state.Get(serial)
		if key(d).archive() != k.archive() {
			return
		}
		if err == nil {
			plate := 0
			if result.Metadata != nil && result.Metadata.Plate != nil {
				plate = result.Metadata.Plate.Index
			}
			s.log.Debug("job preview retrieved", "serial", serial, "attempt", attempt, "file", k.File, "plate", plate, "archive_bytes", result.ArchiveBytes, "duration_ms", time.Since(started).Milliseconds())
			at := time.Now().UTC().Format(time.RFC3339Nano)
			result.Preview = View{Status: StatusReady, JobName: k.Name, Current: true, RetrievedAt: &at}
			if len(result.PNG) > 0 {
				url := fmt.Sprintf("/camera/%s/preview?v=%x", serial, sha256.Sum256(result.PNG))
				result.Preview.ImageURL = &url
			}
			s.mu.Lock()
			s.entries[serial] = entry{k, result}
			delete(s.pending, serial)
			s.mu.Unlock()
			return
		}
		s.log.Debug("job preview attempt failed", "serial", serial, "attempt", attempt, "file", k.File, "error", err)
		if attempt < 3 {
			timer := time.NewTimer(30 * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
	s.mu.Lock()
	delete(s.pending, serial)
	s.mu.Unlock()
	s.log.Warn("job preview unavailable", "serial", serial, "attempts", 3, "error", err)
	s.activity.Record(serial, "job_preview_unavailable", activity.Warning, "Print details unavailable after three attempts")
}

// Lookup reads the last accepted entry and marks a changed selection as past.
func (s *Service) Lookup(serial string, image bool) (Result, bool) {
	if _, ok := s.printers[serial]; !ok {
		return Result{}, false
	}
	d, _, _ := s.state.Get(serial)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[serial]; ok {
		result := e.result
		result.Preview.Current = e.key.archive() == key(d).archive()
		if !image {
			result.PNG = nil
		}
		return result, true
	}
	status := StatusUnavailable
	if k, ok := s.pending[serial]; ok && k.archive() == key(d).archive() {
		status = StatusPending
	}
	return Result{Preview: View{Status: status}}, true
}
