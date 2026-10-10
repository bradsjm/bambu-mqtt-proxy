// Package telemetry merges Bambu MQTT report deltas into a small display
// state per printer for the camera wall and camera status endpoints. It reads
// upstream reports through the pool observer without changing forwarding.
package telemetry

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/hmscodes"
)

// State is the display projection of one printer's merged reports. Nil
// numeric pointers mean "not reported yet"; a printer that never sends a
// field keeps that distinction, so the UI can show unknown values.
type State struct {
	Serial        string
	Name          string
	Model         string
	Connected     bool    // last known MQTT upstream connectivity
	PrintingState string  // gcode_state, e.g. RUNNING / IDLE / FINISH
	Filename      string  // subtask_name
	Progress      float64 // mc_percent
	RemainMin     float64 // mc_remaining_time, minutes
	// progressSeen and remainSeen record that the printer actually reported
	// the scalar at least once. The merged values carry no unknown marker of
	// their own, so consumers that must distinguish a reported 0 from
	// "never reported" read these flags (see SessionView).
	progressSeen bool
	remainSeen   bool
	LayerNum     *int
	TotalLayers  *int
	NozzleTemp   *float64
	NozzleTarget *float64
	BedTemp      *float64
	BedTarget    *float64
	ChamberTemp  *float64
	PrintError   int // print_error; 0 means no error
	// announcedError is the last print_error value announced in the
	// activity log. A cancel marker suppressed on a FAILED transition
	// never replaces it, so the later clear names the error the user
	// actually saw.
	announcedError int
	HMS            []HMSAlert // current Health Management System alerts
	// Stage is the raw stg_cur stage id; nil means never reported. The
	// camera projection maps known ids to names and leaves idle sentinels
	// and unknown ids unmapped, so the printer state stays the headline.
	Stage *int
	// AMS and ExtSpool are the display-only filament projection: the
	// conventional four-slot AMS units sorted by unit id, and the single
	// external spool (the report's vt_tray object, legacy id 254). They are
	// merged before the real-print report gate and read by nothing but the
	// camera status endpoints, so they never touch session bookkeeping or
	// action freshness. Nested values follow copy-on-write: merges
	// replace slices and slot values wholesale instead of editing them in
	// place, so a published State or Snapshot never changes under a reader.
	AMS      []AMSUnit
	ExtSpool *AMSSlot

	// Display-only extras merged before the real-print gate (see
	// mergeExtras). None of them refresh action freshness.
	ChamberLight         string    // on|off|flashing; empty = never reported
	StartedAt            time.Time // gcode_start_time; zero = unknown
	SpeedPercent         *int      // spd_mag
	Fans                 Fans
	NozzleDiameter       *float64
	NozzleType           string
	WifiDBm              *int
	SDCard               *bool
	ExtSpoolSensor       *bool // hw_switch_state: filament present at the external spool sensor
	Timelapse            *bool
	FirstLayerInspection *bool
	SpaghettiDetection   *bool
	// AccessoryChamberTemp and AccessoryChamberAt hold the accessory chamber
	// reading from a module, stamped onto State copies at read time by the
	// optional chamber source. They are display-only: mergeReport never
	// writes them, and they never refresh the report sequence, freshness,
	// activity, or session state, nor the native ChamberTemp. Nil means
	// nothing was observed; consumers decide freshness from
	// AccessoryChamberAt against their own clock.
	AccessoryChamberTemp *float64
	AccessoryChamberAt   time.Time

	// Print session bookkeeping. Not display state: these fields track the
	// current print session for every consumer of Session. Guarded by the Cache mutex like every other field.
	projectID        string // raw project_id; "0" is a valid local print
	taskID           string // raw task_id
	cookie           string // projectID-taskID-subtaskName identity cookie
	completeIdentity bool   // every identity part has been reported
	sessionActive    bool   // a print session is in progress
	sessionGen       uint64 // bumps only when a genuinely new print session starts
	epoch            uint64 // bumps at every state boundary or identity change
	stateGen         uint64 // upstream generation of the last gcode_state report
	layerJobGen      uint64 // Job generation of the last layer_num report.
	// stagesThisJobGen is the job generation the stage history belongs to.
	// stagesThisJob holds the distinct valid stg_cur values merged while
	// stagesThisJobGen is the current job generation, in first-seen order.
	// Both live on the merge path after trackJob, never in mergeDisplay,
	// so they cannot predate a job boundary in the same payload.
	stagesThisJobGen uint64
	stagesThisJob    []int
	speedProfile     *int      // last reported Bambu spd_lvl (1..4)
	speedObs         uint64    // increments for each spd_lvl report
	trayNow          *int      // raw tray_now selection value; nil = not reported
	obs              uint64    // real print reports merged (ACKs excluded)
	obsGen           uint64    // upstream generation of the last report
	obsAt            time.Time // time of the last real report
	lastSeq          uint64    // paho delivery order token of the last merge
	// job is the private preview-job projection (job.go): preview
	// generations, identity revisions and RUNNING settling evidence for the
	// optional job preview service. It never feeds display values, and its
	// only effect on session bookkeeping is the generation that guards
	// layer_num provenance: a preview-job boundary (for example a changed
	// positive gcode_start_time on the same cookie) invalidates an
	// un-re-reported layer without touching the session generation.
	job jobTrack
}

// Fans holds fan speeds in percent; nil means never reported.
type Fans struct {
	Part, Aux, Chamber, Heatbreak *int
}

// HMSAlert is one Bambu Health Management System entry from a report's
// hms list. Reports replace the list wholesale, so a merged State never
// mutates a published slice in place.
type HMSAlert struct {
	Attr uint32
	Code uint32
}

// ID formats the alert in Bambu's published HMS code form,
// e.g. HMS_0300_0100_0001_0007.
func (a HMSAlert) ID() string {
	return fmt.Sprintf("HMS_%04X_%04X_%04X_%04X", a.Attr>>16, a.Attr&0xFFFF, a.Code>>16, a.Code&0xFFFF)
}

// Severity decodes the severity level carried in the high half of Code.
func (a HMSAlert) Severity() string {
	switch a.Code >> 16 {
	case 1:
		return "fatal"
	case 2:
		return "serious"
	case 3:
		return "common"
	case 4:
		return "info"
	default:
		return "unknown"
	}
}

// Cache stores merged state for every configured printer.
type Cache struct {
	mu          sync.Mutex
	activityMu  sync.Mutex // keeps activity events in report-merge order
	states      map[string]*State
	watch       map[string]chan struct{}
	seq         atomic.Uint64
	log         *slog.Logger
	activity    *activity.Log                                             // optional recent-event log
	chamberRead func(serial string) (temp float64, at time.Time, ok bool) // optional accessory chamber reading from a module
	// now is the preview-job timing clock. Production uses time.Now;
	// package-local preview-job tests replace it to drive gap and settling
	// boundaries deterministically. Session timing stays on time.Now.
	now func() time.Time
}

// NewCache indexes the configured printers; serials without reports still
// appear so the camera wall can list every printer.
func NewCache(printers []config.Printer, log *slog.Logger) *Cache {
	states := make(map[string]*State, len(printers))
	for _, p := range printers {
		states[p.Serial] = &State{Serial: p.Serial, Name: p.Name, Model: p.Model}
	}
	return &Cache{states: states, watch: make(map[string]chan struct{}), log: log, now: time.Now}
}

// SetActivity attaches the optional in-memory event log. Call before observing
// reports.
func (c *Cache) SetActivity(log *activity.Log) {
	c.activity = log
}

// SetChamberReader attaches an accessory chamber reading from a module.
// It is observed outside the MQTT report stream and returns the latest
// reading and its observation time. A false ok means no reading was accepted.
// Consumers decide freshness from the observation time. Call during wiring,
// before the first Snapshot or State read. The reader must never call back
// into the Cache: the accessors hold the cache mutex while stamping.
func (c *Cache) SetChamberReader(read func(serial string) (temp float64, at time.Time, ok bool)) {
	c.chamberRead = read
}

// stampAccessory copies the accessory chamber reading from a module onto a State copy.
// The caller holds c.mu; the source keeps its own synchronization and never
// calls back into the Cache.
func (c *Cache) stampAccessory(s *State) {
	if c.chamberRead == nil {
		return
	}
	if temp, at, ok := c.chamberRead(s.Serial); ok {
		s.AccessoryChamberTemp, s.AccessoryChamberAt = &temp, at
	}
}

// obsSeqBase starts the cache-assigned sequence space far above the pool's
// per-connection counters so legacy Observe calls can never collide with
// pool-ordered reports.
const obsSeqBase = 1 << 62

// Observe is the pool observer hook: merge one raw report payload.
func (c *Cache) Observe(serial string, payload []byte) {
	c.ObserveReport(serial, 0, 0, payload)
}

// ObserveReport merges one upstream report with session bookkeeping. Seq
// is the per-connection paho delivery token assigned when the message
// handler was entered (0 lets the cache assign one); paho dispatches
// concurrently, so an older report that arrives after a newer one is
// dropped instead of regressing the merged state. Gen is the upstream
// connection generation the report arrived on, read before the merge so
// cached state is never newer than the connection it came from.
func (c *Cache) ObserveReport(serial string, seq, gen uint64, payload []byte) {
	if seq == 0 {
		seq = obsSeqBase + c.seq.Add(1)
	}
	var events []activityRecord
	var real bool
	c.mu.Lock()
	if st, ok := c.states[serial]; ok {
		if seq < st.lastSeq {
			// Wire ordering limitation: paho handler entry order approximates
			// but cannot prove wire order. Dropping the straggler keeps the
			// merged session state conservative; boundary epochs invalidate
			// any action window that spanned the overlap.
			c.mu.Unlock()
			return
		}
		st.lastSeq = seq
		before := activitySnapshot{state: st.PrintingState, sessionGen: st.sessionGen, jobGen: st.job.generation, printError: st.PrintError, hms: append([]HMSAlert(nil), st.HMS...), chamberLight: st.ChamberLight}
		real = mergeReport(st, gen, payload, c.now(), c.log)
		if real || st.job.generation != before.jobGen {
			events = collectActivityEvents(before, st)
		} else {
			events = lightEvents(before, st)
		}
		if len(events) > 0 {
			// Reserve event-recording order while report merges are still
			// serialized. Record after releasing c.mu to avoid nested locks.
			c.activityMu.Lock()
		}
	}
	c.mu.Unlock()
	for _, event := range events {
		if event.url != "" {
			c.activity.RecordLink(serial, event.kind, event.severity, event.message, event.url)
		} else {
			c.activity.RecordObserved(serial, event.kind, event.severity, event.message, event.obs)
		}
	}
	if len(events) > 0 {
		c.activityMu.Unlock()
	}
	if real {
		c.notify(serial)
	}
}

// activitySnapshot is the state needed to identify notable report changes.
type activitySnapshot struct {
	state        string
	sessionGen   uint64
	jobGen       uint64
	printError   int
	hms          []HMSAlert
	chamberLight string
}

// activityRecord is an event collected while Cache.mu is held and recorded
// after it is released.
type activityRecord struct {
	kind, severity, message string
	url                     string
	obs                     activity.Observation
}

// lightEvents reports a chamber light change. The first observation records
// nothing: it is not a change the user made.
func lightEvents(before activitySnapshot, st *State) []activityRecord {
	if before.chamberLight == "" || st.ChamberLight == before.chamberLight {
		return nil
	}
	switch st.ChamberLight {
	case "on":
		return []activityRecord{{kind: "chamber_light_on", severity: activity.Info, message: "Chamber light on"}}
	case "off":
		return []activityRecord{{kind: "chamber_light_off", severity: activity.Info, message: "Chamber light off"}}
	}
	return nil
}

// collectActivityEvents derives user-facing events from one merged report.
// It runs under the cache mutex and may update the activity-evidence
// bookkeeping on st (announcedError) alongside the merged display state.
func collectActivityEvents(before activitySnapshot, st *State) []activityRecord {
	var events []activityRecord
	add := func(kind, severity, message string) {
		events = append(events, activityRecord{kind: kind, severity: severity, message: message})
	}
	addLink := func(kind, severity, message, url string) {
		events = append(events, activityRecord{kind: kind, severity: severity, message: message, url: url})
	}
	filename := ""
	if st.Filename != "" {
		filename = " · " + st.Filename
	}
	// jobStarted marks a job-generation boundary that this report crossed
	// into an active job, including a metadata-only report that carried the
	// identity change without print state.
	jobStarted := st.job.generation > before.jobGen && st.job.active
	startObs := activity.Observation{JobGen: st.job.generation}
	if st.PrintingState != "" && before.state == "" {
		events = append(events, activityRecord{kind: "state_initial", severity: activity.Info, message: "Printer " + strings.ToLower(st.PrintingState) + filename, obs: startObs})
	} else if st.sessionGen > before.sessionGen || (jobStarted && previewStateExec(st.PrintingState)) {
		events = append(events, activityRecord{kind: "print_started", severity: activity.Info, message: "Print started" + filename, obs: startObs})
	} else if jobStarted && (st.PrintingState == "PREPARE" || st.PrintingState == "SLICING") {
		events = append(events, activityRecord{kind: "print_preparing", severity: activity.Info, message: "Print preparing" + filename, obs: startObs})
	} else if st.PrintingState != before.state {
		switch st.PrintingState {
		case "PREPARE", "SLICING":
			add("print_preparing", activity.Info, "Print preparing"+filename)
		case "PAUSE", "PAUSED":
			if before.state != "PAUSE" && before.state != "PAUSED" {
				add("print_paused", activity.Warning, "Print paused"+filename)
			}
		case "RUNNING":
			if before.state == "PAUSE" || before.state == "PAUSED" {
				add("print_resumed", activity.Info, "Print resumed"+filename)
			}
		case "FINISH":
			add("print_finished", activity.Info, "Print finished"+filename)
		case "FAILED":
			if isCancelCode(st.PrintError) {
				add("print_cancelled", activity.Warning, "Print cancelled"+filename)
			} else {
				add("print_failed", activity.Error, "Print failed"+filename)
			}
		case "IDLE":
			if isControlState(before.state) {
				add("print_stopped", activity.Warning, "Print stopped before finishing"+filename)
			}
		}
	}

	oldHMS := make(map[string]HMSAlert, len(before.hms))
	for _, alert := range before.hms {
		oldHMS[alert.ID()] = alert
	}
	newHMS := make(map[string]HMSAlert, len(st.HMS))
	for _, alert := range st.HMS {
		id := alert.ID()
		newHMS[id] = alert
		if _, existed := oldHMS[id]; existed {
			continue
		}
		severity := activity.Info
		switch alert.Severity() {
		case "fatal", "serious":
			severity = activity.Error
		case "common":
			severity = activity.Warning
		}
		info := hmscodes.HMS(alert.Attr)
		if info.Title != "" {
			addLink("hms_alert", severity, "Printer alert: "+info.Title+" ("+id+")", info.URL)
		} else {
			addLink("hms_alert", severity, "Printer alert "+id+", no description available", info.URL)
		}
	}
	var cleared []string
	for _, alert := range before.hms {
		id := alert.ID()
		if _, active := newHMS[id]; !active {
			if title := hmscodes.HMS(alert.Attr).Title; title != "" {
				cleared = append(cleared, title)
			} else {
				cleared = append(cleared, id)
			}
		}
	}
	// A FAILED transition carrying a user-cancel marker suppresses only
	// the print_error entry for that marker; the marker never becomes the
	// announced error, so the later clear names the error the user saw.
	suppressedCancel := st.PrintingState == "FAILED" && before.state != "FAILED" && isCancelCode(st.PrintError)
	if before.printError != st.PrintError {
		if st.PrintError == 0 {
			if st.announcedError != 0 {
				cleared = append(cleared, clearedErrorItem(st.announcedError))
				st.announcedError = 0
			}
		} else if !suppressedCancel {
			code := uint32(st.PrintError)
			info := hmscodes.PrintError(code)
			text := fmt.Sprintf("%04X-%04X", code>>16, code&0xFFFF)
			severity := activity.Error
			switch info.Severity {
			case "info":
				severity = activity.Info
			case "warning":
				severity = activity.Warning
			}
			if info.Title != "" {
				addLink("print_error", severity, "Printer error: "+info.Title+" ("+text+")", info.URL)
			} else {
				addLink("print_error", severity, "Printer error "+text+", no description available", info.URL)
			}
			st.announcedError = st.PrintError
		}
	}
	if len(cleared) > 0 {
		add("alerts_cleared", activity.Info, "Printer alerts cleared: "+strings.Join(cleared, ", "))
	}
	events = append(events, lightEvents(before, st)...)
	return events
}

// isCancelCode reports whether a print_error value is one of the printer's
// user-cancel markers: a FAILED transition carrying one records
// print_cancelled instead of print_failed.
func isCancelCode(code int) bool {
	switch uint32(code) {
	case 0x0300400C, 0x0500400E:
		return true
	}
	return false
}

// clearedErrorItem renders the alerts-cleared item for a print error that
// is leaving: the dataset title when known, else "error XXXX-XXXX".
func clearedErrorItem(code int) string {
	if title := hmscodes.PrintError(uint32(code)).Title; title != "" {
		return title
	}
	return "error " + fmt.Sprintf("%04X-%04X", uint32(code)>>16, uint32(code)&0xFFFF)
}

// notify wakes report watchers, coalescing bursts into one token.
func (c *Cache) notify(serial string) {
	c.mu.Lock()
	ch := c.watch[serial]
	c.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// WatchReports returns a level-triggered wake channel for one serial:
// every merged real report leaves at most one pending token. The channel
// has a single consumer: a second caller shares the same channel and splits
// the tokens with the first.
func (c *Cache) WatchReports(serial string) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.watch[serial]
	if !ok {
		ch = make(chan struct{}, 1)
		c.watch[serial] = ch
	}
	return ch
}

// SessionView is the shared print-session projection of one printer's
// report state, used by several consumers. It carries the session identity
// and freshness evidence they need; it never carries credentials.
type SessionView struct {
	Serial   string
	Active   bool   // a print session is in progress
	State    string // merged gcode_state
	Cookie   string // project_id-task_id-subtask_name identity cookie
	Complete bool   // every identity part has been reported
	// SessionGen bumps only when a genuinely new print session starts (a
	// control state entered from outside a session, or a new print identity
	// while a session is active). Epoch keeps bumping at every state
	// boundary, including ordinary PAUSE-to-RUNNING, so in-flight results
	// and pauses are invalidated without resetting the session context.
	SessionGen uint64
	Epoch      uint64 // bumps at every state boundary or identity change
	StateGen   uint64 // upstream generation of the last gcode_state report
	// SpeedProfile is nil until a supported profile (1..4) is reported.
	SpeedProfile *int
	SpeedObs     uint64
	Obs          uint64 // real print reports merged
	ObsAt        time.Time
	ObsGen       uint64 // upstream generation of the last report
	// Progress and RemainingMin carry the merged scalar with presence: nil
	// means the printer never reported the field, so consumers can keep the
	// value unknown instead of showing a meaningless 0.
	Progress     *float64
	RemainingMin *float64
	// LayerNum is the last reported layer; use CurrentLayer for decisions
	// about the current print. It is a copy of the sticky display value
	// and is exposed in any state, whenever it is non-nil.
	LayerNum *int
	// LayerThisJob reports whether LayerNum was reported during the
	// current preview-job generation.
	LayerThisJob bool
	// StagesThisJob holds the distinct stages the printer reported during
	// the current job, in first-seen order. It is a copy of the sticky
	// per-job history, so a stage replaced by a later one still counts as
	// reported for this job.
	StagesThisJob []int
	ChamberLight  string
	StartedAt     time.Time
}

// Session returns the shared print-session projection for one serial.
func (c *Cache) Session(serial string) (SessionView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[serial]
	if !ok {
		return SessionView{}, false
	}
	return st.sessionView(), true
}

// Print returns the preview-job and session projections for one serial
// from a single locked read, so both describe the same instant. An unknown
// serial returns zero values and false.
func (c *Cache) Print(serial string) (JobView, SessionView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[serial]
	if !ok {
		return JobView{}, SessionView{}, false
	}
	return st.job.view(), st.sessionView(), true
}

// sessionView builds the session projection. Callers hold the Cache mutex.
func (st *State) sessionView() SessionView {
	view := SessionView{
		Serial:     st.Serial,
		Active:     st.sessionActive,
		State:      st.PrintingState,
		Cookie:     st.cookie,
		Complete:   st.completeIdentity,
		SessionGen: st.sessionGen,
		Epoch:      st.epoch,
		StateGen:   st.stateGen,
		SpeedObs:   st.speedObs,
		Obs:        st.obs,
		ObsAt:      st.obsAt,
		ObsGen:     st.obsGen,
	}
	if st.speedProfile != nil {
		v := *st.speedProfile
		view.SpeedProfile = &v
	}
	if st.progressSeen {
		v := st.Progress
		view.Progress = &v
	}
	if st.remainSeen {
		v := st.RemainMin
		view.RemainingMin = &v
	}
	// LayerNum is the last reported layer, exposed in any state whenever
	// it is non-nil. CurrentLayer decides whether it belongs to the
	// current print.
	if st.LayerNum != nil {
		v := *st.LayerNum
		view.LayerNum = &v
	}
	view.LayerThisJob = st.layerJobGen == st.job.generation
	// StagesThisJob copies the sticky per-job history in first-seen order,
	// but only when it belongs to the current job: a job boundary without
	// a new stage report leaves the old history behind, exactly as
	// CurrentLayer treats a positive layer from an earlier print as
	// unknown.
	if st.stagesThisJobGen == st.job.generation && len(st.stagesThisJob) > 0 {
		view.StagesThisJob = append([]int(nil), st.stagesThisJob...)
	}
	view.ChamberLight = st.ChamberLight
	view.StartedAt = st.StartedAt
	return view
}

// SetConnected records upstream connectivity from the pool status.
func (c *Cache) SetConnected(statuses map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for serial, up := range statuses {
		if st, ok := c.states[serial]; ok {
			st.Connected = up
		}
	}
}

// Snapshot returns a copy of every printer's current display state,
// ordered by serial for stable rendering.
func (c *Cache) Snapshot() []State {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]State, 0, len(c.states))
	for _, st := range c.states {
		s := *st
		c.stampAccessory(&s)
		out = append(out, s)
	}
	return out
}

// State returns a copy of one printer's display state. The second result is
// false for serials outside the configured set.
func (c *Cache) State(serial string) (State, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[serial]
	if !ok {
		return State{}, false
	}
	s := *st
	c.stampAccessory(&s)
	return s, true
}

// mergeReport applies one report payload onto st and reports whether the
// payload carried real print state. Bambu reports nest under a type key
// ("print"); values change type between prints (number or string), so
// extraction goes through generic maps instead of a fixed struct. now is
// kept for callers that stamp report time; session timing is untouched.
func mergeReport(st *State, gen uint64, payload []byte, now time.Time, log *slog.Logger) bool {
	var report map[string]json.RawMessage
	if err := json.Unmarshal(payload, &report); err != nil {
		return false // not a JSON report (health probes, tests); ignore
	}
	raw, ok := report["print"]
	if !ok {
		return false
	}
	var printObj map[string]any
	if err := json.Unmarshal(raw, &printObj); err != nil {
		return false
	}
	// Display-only extras merge before the real-print gate: stage and AMS
	// deltas arrive in payloads whose print object the gate must keep
	// rejecting as real print state. Merging first changes neither the marker
	// list nor the gate's freshness decisions.
	mergeDisplay(st, printObj, log)
	real := isRealPrintReport(printObj)
	// Preview-job tracking runs for real and metadata-only reports alike:
	// the projection keeps the last value the printer reported for every
	// identity field, and metadata-only deltas never refresh session
	// obs evidence.
	st.trackJob(printObj)
	// Stage history records after trackJob so a same-payload job boundary
	// dates the stage to the new job. mergeDisplay already merged the raw
	// stage value before the gate. Recording stays outside trackSession:
	// stg_cur is not a print-state marker, so stage-only deltas must
	// never refresh session obs evidence.
	if v, ok := intField(printObj, "stg_cur"); ok {
		st.recordStage(v)
	}
	if !real {
		// Command ACK objects (sequence_id/command/result) and other control
		// payloads carry no print state. Ignoring them entirely keeps them
		// from refreshing the action freshness consumers rely
		// on; only genuine print state reports may authorize actions.
		return false
	}
	st.trackSession(printObj, gen)
	if v, ok := numberField(printObj, "mc_percent"); ok {
		st.Progress = v
		st.progressSeen = true
	}
	if v, ok := numberField(printObj, "mc_remaining_time"); ok {
		st.RemainMin = v
		st.remainSeen = true
	}
	if _, present := lookup(printObj, "layer_num"); present {
		if v, ok := intField(printObj, "layer_num"); ok {
			st.LayerNum = &v
			// trackJob has already run for this report, so the
			// job generation names the print job the layer
			// belongs to. A report that omits layer_num
			// restamps nothing: the last reported layer is
			// kept across sessions, connections, and jobs,
			// and CurrentLayer decides whether it belongs to
			// the current print.
			st.layerJobGen = st.job.generation
		} else {
			// Present but invalid: the printer contradicted
			// the last valid layer, so the public field is
			// cleared. Zero means no job generation has
			// reported a layer.
			st.LayerNum = nil
			st.layerJobGen = 0
		}
	}
	if _, present := lookup(printObj, "spd_lvl"); present {
		// A reported but unsupported value invalidates an older known
		// profile. Consumers must not act on stale evidence after an
		// unknown or malformed profile report.
		st.speedProfile = nil
		if v, ok := intField(printObj, "spd_lvl"); ok && v >= 1 && v <= 4 {
			st.speedProfile = &v
		}
		st.speedObs++
	}
	if v, ok := intField(printObj, "total_layer_num"); ok {
		st.TotalLayers = &v
	}
	if v, ok := numberField(printObj, "nozzle_temper"); ok {
		st.NozzleTemp = &v
	}
	if v, ok := numberField(printObj, "nozzle_target_temper"); ok {
		st.NozzleTarget = &v
	}
	if v, ok := numberField(printObj, "bed_temper"); ok {
		st.BedTemp = &v
	}
	if v, ok := numberField(printObj, "bed_target_temper"); ok {
		st.BedTarget = &v
	}
	if v, ok := numberField(printObj, "chamber_temper"); ok {
		st.ChamberTemp = &v
	}
	if v, ok := intField(printObj, "print_error"); ok {
		st.PrintError = v
	}
	if v, ok := lookup(printObj, "hms"); ok {
		if list, ok := v.([]any); ok {
			alerts := make([]HMSAlert, 0, len(list))
			for _, item := range list {
				obj, ok := item.(map[string]any)
				if !ok {
					continue
				}
				attr, okAttr := numberField(obj, "attr")
				code, okCode := numberField(obj, "code")
				if okAttr && okCode {
					alerts = append(alerts, HMSAlert{Attr: uint32(attr), Code: uint32(code)})
				}
			}
			st.HMS = alerts
		}
	}
	return true
}

// maxStagesThisJob bounds the per-job stage history. History past the bound
// is dropped: the printer reports a small stage vocabulary, and staleness
// cannot hide a homing stage already recorded.
const maxStagesThisJob = 64

// recordStage appends one valid stg_cur value to the current job's stage
// history. A generation change resets the history first, so stages from a
// previous job never linger; a repeated value keeps its first-seen slot.
// Callers hold the Cache mutex.
func (st *State) recordStage(stage int) {
	if st.stagesThisJobGen != st.job.generation {
		st.stagesThisJobGen = st.job.generation
		st.stagesThisJob = nil
	}
	for _, seen := range st.stagesThisJob {
		if seen == stage {
			return
		}
	}
	if len(st.stagesThisJob) >= maxStagesThisJob {
		return
	}
	st.stagesThisJob = append(st.stagesThisJob, stage)
}

// printStateMarkers are the report keys that mark a "print" object as real
// print state. Any presence of one makes the payload a state report.
var printStateMarkers = []string{
	"gcode_state", "mc_percent", "subtask_name", "project_id", "task_id",
	"layer_num", "total_layer_num", "mc_remaining_time", "print_error", "hms",
	"spd_lvl",
	"nozzle_temper", "nozzle_target_temper", "bed_temper", "bed_target_temper",
	"chamber_temper",
}

// isRealPrintReport reports whether the print object carries print state.
func isRealPrintReport(obj map[string]any) bool {
	for _, k := range printStateMarkers {
		if _, ok := lookup(obj, k); ok {
			return true
		}
	}
	return false
}

// isControlState reports whether gcode_state keeps a print session alive.
func isControlState(s string) bool {
	return s == "RUNNING" || s == "PAUSE" || s == "PAUSED"
}

// isRunningState reports whether gcode_state means the printer is executing.
func isRunningState(s string) bool {
	return s == "RUNNING"
}

// idField extracts project_id/task_id. Bambu reports them as number or
// string; "0" is a valid local-print project id, not a missing value.
func idField(obj map[string]any, key string) (string, bool) {
	v, ok := lookup(obj, key)
	if !ok {
		return "", false
	}
	switch n := v.(type) {
	case string:
		if n == "" {
			return "", false
		}
		return n, true
	case float64:
		return strconv.FormatInt(int64(n), 10), true
	}
	return "", false
}

// trackSession maintains print session bookkeeping: boundaries are
// captured BEFORE the coalesced display merge so short state transitions
// are never lost, and identity follows the OctoEverywhere Bambu model
// (cookie project_id-task_id-subtask_name; "0" is a valid local id; missing
// parts leave a sticky incomplete cookie until the print ends).
//
// stateGen is stamped only when the report carries gcode_state, so a
// temperature-only delta on a new connection can never make a pre-reconnect
// RUNNING look current-generation. sessionGen bumps only at real print
// session starts; epoch keeps marking every boundary for in-flight checks.
func (st *State) trackSession(printObj map[string]any, gen uint64) {
	started := false
	if s, ok := stringField(printObj, "gcode_state"); ok {
		st.stateGen = gen
		prev := st.PrintingState
		switch {
		case prev != "" && s != prev:
			st.epoch++
			if isControlState(s) {
				st.sessionActive = true
				if !isControlState(prev) {
					// A control state entered from outside a session
					// (idle, finish, failed) is a genuinely new print.
					st.sessionGen++
					started = true
				}
			} else {
				st.resetSession()
			}
		case prev == "" && isControlState(s):
			st.sessionActive = true
			st.sessionGen++
			started = true
		}
		st.PrintingState = s
	}
	if _, present := lookup(printObj, "subtask_name"); present {
		// An absent key keeps the value. A present string sets it,
		// even when empty; a non-string clears it.
		s, ok := stringField(printObj, "subtask_name")
		if ok {
			st.Filename = s
		} else {
			st.Filename = ""
		}
	}
	if _, present := lookup(printObj, "project_id"); present {
		// An absent key keeps the value; a present but invalid id
		// clears it.
		if v, ok := idField(printObj, "project_id"); ok {
			st.projectID = v
		} else {
			st.projectID = ""
		}
	}
	if _, present := lookup(printObj, "task_id"); present {
		if v, ok := idField(printObj, "task_id"); ok {
			st.taskID = v
		} else {
			st.taskID = ""
		}
	}
	cookie := st.projectID + "-" + st.taskID + "-" + st.Filename
	if !started && st.cookie != "" && cookie != st.cookie && st.completeIdentity && st.sessionActive {
		// A genuinely new print identity while a session is active starts a
		// new session; incomplete cookies stay sticky until the print ends.
		st.epoch++
		st.sessionGen++
	}
	st.cookie = cookie
	st.completeIdentity = st.projectID != "" && st.taskID != "" && st.Filename != ""
	st.obs++
	st.obsGen = gen
	st.obsAt = time.Now()
}

// resetSession closes the print session after a print ends. Identity and
// display fields keep the last values the printer reported: only the
// session-active flag marks the boundary.
func (st *State) resetSession() {
	st.sessionActive = false
}

// lookup finds the first present key, case-insensitively: some printers
// spell report keys with different casing.
func lookup(obj map[string]any, keys ...string) (any, bool) {
	for _, k := range keys {
		if v, ok := obj[k]; ok {
			return v, true
		}
		for objKey, v := range obj {
			if equalFold(objKey, k) {
				return v, true
			}
		}
	}
	return nil, false
}

// equalFold is a tiny ASCII case-insensitive compare for report keys.
func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// stringField extracts a string value.
func stringField(obj map[string]any, key string) (string, bool) {
	v, ok := lookup(obj, key)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// numberField extracts a float from a JSON number or numeric string.
// Bambu flips between the two encodings across fields and firmwares.
// Nonfinite values (NaN, Inf, and spellings like "Infinity") are rejected:
// they cannot enter merged state or break status JSON serialization.
func numberField(obj map[string]any, key string) (float64, bool) {
	v, ok := lookup(obj, key)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return 0, false
		}
		return n, true
	case string:
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
				return 0, false
			}
			return parsed, true
		}
	}
	return 0, false
}

// intField extracts an integer value.
func intField(obj map[string]any, key string) (int, bool) {
	f, ok := numberField(obj, key)
	if !ok {
		return 0, false
	}
	return int(f), true
}

// CurrentLayer returns the layer to use for decisions about the current
// print, or nil when it is unknown. A reported layer of 0 is valid from any
// job generation: an idle printer reports 0. A positive layer reported
// before the current job began belongs to an earlier print and is unknown.
func (v SessionView) CurrentLayer() *int {
	if v.LayerNum == nil {
		return nil
	}
	if *v.LayerNum == 0 || v.LayerThisJob {
		n := *v.LayerNum
		return &n
	}
	return nil
}
