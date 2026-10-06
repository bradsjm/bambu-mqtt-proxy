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
	PrintError   int        // print_error; 0 means no error
	HMS          []HMSAlert // current Health Management System alerts
	// Stage is the raw stg_cur stage id; nil means never reported. The
	// camera projection maps known ids to names and leaves idle sentinels
	// and unknown ids unmapped, so the printer state stays the headline.
	Stage *int
	// AMS and ExtSpool are the display-only filament projection: the
	// conventional four-slot AMS units sorted by unit id, and the single
	// external spool (the report's vt_tray object, legacy id 254). They are
	// merged before the real-print report gate and read by nothing but the
	// camera status endpoints, so they never touch session bookkeeping or
	// detection freshness. Nested values follow copy-on-write: merges
	// replace slices and slot values wholesale instead of editing them in
	// place, so a published State or Snapshot never changes under a reader.
	AMS      []AMSUnit
	ExtSpool *AMSSlot

	// Display-only extras merged before the real-print gate (see
	// mergeExtras). None of them refresh detection freshness.
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
	// activity, or detection state, nor the native ChamberTemp. Nil means
	// nothing was observed; consumers decide freshness from
	// AccessoryChamberAt against their own clock.
	AccessoryChamberTemp *float64
	AccessoryChamberAt   time.Time

	// Detection session bookkeeping. Not display state: these fields track
	// the current print session for the optional OctoEverywhere detection
	// worker. Guarded by the Cache mutex like every other field.
	projectID        string    // raw project_id; "0" is a valid local print
	taskID           string    // raw task_id
	cookie           string    // projectID-taskID-subtaskName identity cookie
	completeIdentity bool      // every identity part has been reported
	sessionActive    bool      // a print session is in progress
	sessionGen       uint64    // bumps only when a genuinely new print session starts
	epoch            uint64    // bumps at every state boundary or identity change
	stateGen         uint64    // upstream generation of the last gcode_state report
	layerSessionGen  uint64    // print session in which layer_num was last reported
	layerGen         uint64    // upstream generation of the last layer_num report
	speedProfile     *int      // last reported Bambu spd_lvl (1..4)
	speedGen         uint64    // upstream generation of speedProfile
	speedSessionGen  uint64    // print session in which speedProfile was reported
	speedObs         uint64    // increments for each spd_lvl report
	speedAt          time.Time // time speedProfile was observed
	trayNow          *int      // raw tray_now selection value; nil = not reported
	obs              uint64    // real print reports merged (ACKs excluded)
	obsGen           uint64    // upstream connection generation of the last report
	obsAt            time.Time // time of the last real report
	lastSeq          uint64    // paho delivery order token of the last merge
	// job is the private preview-job projection (job.go). It shares the
	// report stream but never feeds detection or display values: it
	// tracks preview generations, identity revisions and RUNNING settling
	// evidence for the optional job preview service.
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
	// boundaries deterministically. Detection timing stays on time.Now.
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

// ObserveReport merges one upstream report with detection bookkeeping. Seq
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
		before := activitySnapshot{state: st.PrintingState, sessionGen: st.sessionGen, printError: st.PrintError, hms: append([]HMSAlert(nil), st.HMS...), chamberLight: st.ChamberLight}
		real = mergeReport(st, gen, payload, c.now(), c.log)
		if real {
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
		c.activity.Record(serial, event.kind, event.severity, event.message)
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
	printError   int
	hms          []HMSAlert
	chamberLight string
}

// activityRecord is an event collected while Cache.mu is held and recorded
// after it is released.
type activityRecord struct {
	kind, severity, message string
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
func collectActivityEvents(before activitySnapshot, st *State) []activityRecord {
	var events []activityRecord
	add := func(kind, severity, message string) {
		events = append(events, activityRecord{kind: kind, severity: severity, message: message})
	}
	filename := ""
	if st.Filename != "" {
		filename = " · " + st.Filename
	}
	if st.PrintingState != "" && before.state == "" {
		add("state_initial", activity.Info, "Print first observedstate "+strings.ToLower(st.PrintingState)+filename)
	} else if st.sessionGen > before.sessionGen {
		add("print_started", activity.Info, "Print started"+filename)
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
			add("print_failed", activity.Error, "Print failed or was cancelled"+filename)
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
		add("hms_alert", severity, "HMS alert "+id+" ("+alert.Severity()+")")
	}
	for _, alert := range before.hms {
		id := alert.ID()
		if _, active := newHMS[id]; !active {
			add("hms_cleared", activity.Info, "HMS alert "+id+" cleared")
		}
	}
	if before.printError != st.PrintError {
		if st.PrintError == 0 {
			add("print_error_cleared", activity.Info, "Printer error cleared")
		} else {
			code := fmt.Sprintf("%04X_%04X", uint32(st.PrintError)>>16, uint32(st.PrintError)&0xFFFF)
			add("print_error", activity.Error, "Printer error "+code)
		}
	}
	events = append(events, lightEvents(before, st)...)
	return events
}

// notify wakes detection watchers, coalescing bursts into one token.
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

// WatchDetection returns a level-triggered wake channel for one serial:
// every merged real report leaves at most one pending token.
func (c *Cache) WatchDetection(serial string) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.watch[serial]
	if !ok {
		ch = make(chan struct{}, 1)
		c.watch[serial] = ch
	}
	return ch
}

// SessionView is the detection-facing projection of one printer's report
// state. It carries the session identity and freshness evidence the
// detection worker needs; it never carries credentials.
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
	// Its observation evidence prevents use across print sessions or reconnects.
	SpeedProfile    *int
	SpeedGen        uint64
	SpeedSessionGen uint64
	SpeedObs        uint64
	SpeedAt         time.Time
	Obs             uint64 // real print reports merged
	ObsAt           time.Time
	ObsGen          uint64 // upstream generation of the last report
	// Progress and RemainingMin carry the merged scalar with presence: nil
	// means the printer never reported the field, so consumers can keep the
	// value unknown instead of showing a meaningless 0.
	Progress     *float64
	RemainingMin *float64
	LayerNum     *int
	ChamberLight string
	StartedAt    time.Time
}

// Session returns the detection view for one serial.
func (c *Cache) Session(serial string) (SessionView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[serial]
	if !ok {
		return SessionView{}, false
	}
	view := SessionView{
		Serial:          st.Serial,
		Active:          st.sessionActive,
		State:           st.PrintingState,
		Cookie:          st.cookie,
		Complete:        st.completeIdentity,
		SessionGen:      st.sessionGen,
		Epoch:           st.epoch,
		StateGen:        st.stateGen,
		SpeedGen:        st.speedGen,
		SpeedSessionGen: st.speedSessionGen,
		SpeedObs:        st.speedObs,
		SpeedAt:         st.speedAt,
		Obs:             st.obs,
		ObsAt:           st.obsAt,
		ObsGen:          st.obsGen,
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
	// LayerNum is the current print's layer only: the display State keeps
	// the last reported value for the camera wall, but the detection view
	// exposes it only when it was reported in the current print session on
	// the current connection, so a previous job's sticky layer can never
	// authorize a new print and a reconnected printer must re-report it.
	if st.LayerNum != nil && st.sessionGen != 0 &&
		st.layerSessionGen == st.sessionGen && st.layerGen == st.obsGen {
		v := *st.LayerNum
		view.LayerNum = &v
	}
	view.ChamberLight = st.ChamberLight
	view.StartedAt = st.StartedAt
	return view, true
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
// the preview clock time, taken once per report for the preview-job
// projection; detection timing is untouched.
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
	// rejecting for detection. Merging first changes neither the marker
	// list nor the gate's freshness decisions.
	mergeDisplay(st, printObj, log)
	real := isRealPrintReport(printObj)
	// Preview-job tracking runs for real and metadata-only reports alike:
	// the projection keeps its own identity and freshness evidence, and
	// metadata-only deltas never refresh detection's.
	st.trackJob(printObj, gen, now, real)
	if !real {
		// Command ACK objects (sequence_id/command/result) and other control
		// payloads carry no print state. Ignoring them entirely keeps them
		// from refreshing the action freshness the detection worker relies
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
			// Stamp the layer's evidence so Session can tell the current
			// print's layer from the sticky display value, following the
			// speedGen/speedSessionGen pattern. A report that omits
			// layer_num restamps nothing: on one connection the
			// generations cannot move, so the last reported layer stays
			// current until a reconnect or a new print session.
			st.layerSessionGen = st.sessionGen
			st.layerGen = gen
		} else {
			// Present but invalid: the printer contradicted the last valid
			// layer. Keep the sticky display value, but drop the evidence
			// (session 0 matches no real session) so an unparseable report
			// cannot leave the previous layer authorizing indefinitely.
			st.layerSessionGen = 0
			st.layerGen = 0
		}
	}
	if _, present := lookup(printObj, "spd_lvl"); present {
		// A reported but unsupported value invalidates an older known
		// profile. Detection must not act on stale evidence after an
		// unknown or malformed profile report.
		st.speedProfile = nil
		if v, ok := intField(printObj, "spd_lvl"); ok && v >= 1 && v <= 4 {
			st.speedProfile = &v
		}
		st.speedGen = gen
		st.speedSessionGen = st.sessionGen
		st.speedObs++
		st.speedAt = time.Now()
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

// trackSession maintains detection session bookkeeping: boundaries are
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
				}
			} else {
				st.resetSession()
			}
		case prev == "" && isControlState(s):
			st.sessionActive = true
			st.sessionGen++
		}
		st.PrintingState = s
	}
	if v, ok := idField(printObj, "project_id"); ok {
		st.projectID = v
	}
	if v, ok := idField(printObj, "task_id"); ok {
		st.taskID = v
	}
	if s, ok := stringField(printObj, "subtask_name"); ok && s != "" {
		st.Filename = s
	}
	cookie := st.projectID + "-" + st.taskID + "-" + st.Filename
	if st.cookie != "" && cookie != st.cookie && st.completeIdentity && st.sessionActive {
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

// resetSession clears session identity after a print ends. The display
// filename is kept for the camera wall.
func (st *State) resetSession() {
	st.sessionActive = false
	st.cookie = ""
	st.projectID = ""
	st.taskID = ""
	st.completeIdentity = false
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
