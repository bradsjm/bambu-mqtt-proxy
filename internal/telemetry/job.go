// Preview-job projection: a second, independent view of the current print
// beside the detection session. Detection (trackSession) answers "which
// print is this and is the evidence fresh enough to act"; this projection
// answers "which generation of a printable job is on the bed and has
// RUNNING been continuously fresh long enough to attempt one bounded
// archive retrieval". It starts at PREPARE/SLICING where detection starts
// at RUNNING/PAUSE, tracks identity corrections as revisions, and never
// feeds back into detection state or display values.

package telemetry

import "time"

// previewFreshnessGap bounds how long since the last real report a RUNNING
// observation stays fresh. It mirrors printerview.FreshnessWindow, which
// this package must not import; the preview gap test pins the value.
const previewFreshnessGap = 15 * time.Second

// JobView is the preview-job projection of one printer: which generation of
// a printable job is current, its identity fields, and the RUNNING settling
// evidence a preview consumer needs before its single attempt. It is
// independent of SessionView.
type JobView struct {
	// Generation increments at every preview-job boundary: the first
	// observation of a preview-active state, entry from an inactive
	// state, an executing-to-preparation transition, or a strong identity
	// change inside an active generation. One generation grants at most
	// one preview attempt.
	Generation uint64
	// Revision increments when any identifying field changes inside a
	// generation. A revision difference invalidates cached preview data
	// and settling evidence without granting a second attempt.
	Revision uint64
	// RunningEpoch increments whenever RUNNING settling evidence is
	// invalidated: leaving RUNNING, an identity revision, an upstream
	// connection generation change, or a report gap longer than the
	// freshness window.
	RunningEpoch uint64
	// Active reports whether a preview-active job is current. An ended
	// job keeps its generation and identity as last-job data with Active
	// false.
	Active bool
	// State is the merged gcode_state of the current generation.
	State string
	// Name is the merged subtask_name.
	Name string
	// GCodeFile is the merged gcode_file report value.
	GCodeFile string
	// ProjectID and TaskID are the merged strong identity ids. "0" is a
	// valid local-print id, not a missing value.
	ProjectID string
	TaskID    string
	// PlateIndex is the merged plate_idx; nil means not reported.
	PlateIndex *int
	// StartedAt is the merged gcode_start_time; zero means not reported.
	StartedAt time.Time
	// StateGen is the upstream connection generation of the last report
	// that carried gcode_state. Zero means this generation has seen no
	// state report of its own.
	StateGen uint64
	// ObsGen is the upstream connection generation of the last real print
	// report and ObsAt its merge time. Metadata-only deltas never refresh
	// them.
	ObsGen uint64
	ObsAt  time.Time
	// RunningSince is the start of the current continuous fresh RUNNING
	// interval, or zero while settling evidence is invalid.
	RunningSince time.Time
}

// previewStateActive reports whether gcode_state keeps a preview job
// active. PREPARE and SLICING join detection's control states: a preview
// generation must exist before the printer starts executing.
func previewStateActive(s string) bool {
	switch s {
	case "PREPARE", "SLICING", "RUNNING", "PAUSE", "PAUSED":
		return true
	}
	return false
}

// previewStateExec reports whether gcode_state is an executing state.
// Returning to PREPARE or SLICING from one of these starts a new preview
// generation (a re-slice or restart); SLICING to PREPARE does not.
func previewStateExec(s string) bool {
	return s == "RUNNING" || s == "PAUSE" || s == "PAUSED"
}

// jobTrack is the private preview-job state behind one State. Guarded by
// the Cache mutex like every other State field.
type jobTrack struct {
	generation   uint64
	revision     uint64
	runningEpoch uint64
	active       bool
	state        string
	name         string
	gcodeFile    string
	projectID    string
	taskID       string
	plateIndex   *int
	startedAt    time.Time
	stateGen     uint64
	obsGen       uint64
	obsAt        time.Time
	runningSince time.Time
	// lastProjectID, lastTaskID and lastStarted are the sticky strong
	// identities. They survive invalid or absent updates so a
	// valid-empty-same-valid sequence cannot look like a change.
	lastProjectID string
	lastTaskID    string
	lastStarted   int64
	knownProject  bool
	knownTask     bool
	knownStarted  bool
}

// view returns a coherent copy with duplicated optional scalars, so a
// caller can never mutate cached state. Callers hold the Cache mutex.
func (j *jobTrack) view() JobView {
	v := JobView{
		Generation:   j.generation,
		Revision:     j.revision,
		RunningEpoch: j.runningEpoch,
		Active:       j.active,
		State:        j.state,
		Name:         j.name,
		GCodeFile:    j.gcodeFile,
		ProjectID:    j.projectID,
		TaskID:       j.taskID,
		StartedAt:    j.startedAt,
		StateGen:     j.stateGen,
		ObsGen:       j.obsGen,
		ObsAt:        j.obsAt,
		RunningSince: j.runningSince,
	}
	if j.plateIndex != nil {
		p := *j.plateIndex
		v.PlateIndex = &p
	}
	return v
}

// clear resets every preview-job field for a new generation, including the
// sticky identities: the new job starts with no history. The previous
// display filename is deliberately not copied here and never read from
// State.Filename. Generation, RunningEpoch and the observation stamps stay:
// they are monotonic transport evidence, not job metadata.
func (j *jobTrack) clear() {
	j.revision = 0
	j.state = ""
	j.name = ""
	j.gcodeFile = ""
	j.projectID = ""
	j.taskID = ""
	j.plateIndex = nil
	j.startedAt = time.Time{}
	j.stateGen = 0
	j.runningSince = time.Time{}
	j.lastProjectID = ""
	j.lastTaskID = ""
	j.lastStarted = 0
	j.knownProject = false
	j.knownTask = false
	j.knownStarted = false
}

// resetSettling invalidates the RUNNING settling evidence: a consumer must
// observe a fresh, continuous RUNNING interval again before acting.
func (j *jobTrack) resetSettling() {
	j.runningSince = time.Time{}
	j.runningEpoch++
}

// Job returns the preview-job projection for one serial. The second result
// is false for serials outside the configured set. The view is a coherent
// copy taken under the Cache mutex: optional scalars are duplicated, so
// callers cannot mutate cached state or observe a torn read across a
// generation boundary.
func (c *Cache) Job(serial string) (JobView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[serial]
	if !ok {
		return JobView{}, false
	}
	return st.job.view(), true
}

// trackJob maintains the preview-job projection for one parsed report map.
// It runs for real print reports and metadata-only deltas alike; only
// metadata-only deltas are ignored while no job is active. now is the
// preview clock time and real marks a report that counts as print state
// evidence. Detection bookkeeping (trackSession) is never touched, and
// metadata-only deltas never refresh its freshness.
//
// Each report is evaluated once: lifecycle and strong-identity boundaries
// decide a single generation increment, the clear happens once, then the
// fields merge once. Terminal states take precedence over every identity
// decision and merge nothing.
//
// Field keys are checked for presence separately from validity: a key
// that is absent keeps the previous merged value, while a key that is
// present but invalid or empty clears its merged public field (a revision
// that also resets settling). The private sticky identities keep their
// last valid values across invalid updates, so the same valid value
// returning never starts a generation.
func (st *State) trackJob(printObj map[string]any, gen uint64, now time.Time, real bool) {
	j := &st.job

	state, hasState := stringField(printObj, "gcode_state")
	name, hasName := stringField(printObj, "subtask_name")
	file, hasFile := stringField(printObj, "gcode_file")
	plate, hasPlate := intField(printObj, "plate_idx")
	startedF, hasStarted := numberField(printObj, "gcode_start_time")
	project, hasProject := idField(printObj, "project_id")
	task, hasTask := idField(printObj, "task_id")
	_, namePresent := lookup(printObj, "subtask_name")
	_, filePresent := lookup(printObj, "gcode_file")
	_, platePresent := lookup(printObj, "plate_idx")
	_, startedPresent := lookup(printObj, "gcode_start_time")
	_, projectPresent := lookup(printObj, "project_id")
	_, taskPresent := lookup(printObj, "task_id")

	// A metadata-only delta while no job is active can neither reopen the
	// job nor disturb the retained last-job data.
	if !j.active && !real {
		return
	}

	// Freshness boundaries between real reports: an upstream connection
	// generation change or a gap longer than the freshness window
	// invalidates RUNNING settling evidence. obsAt is read before this
	// report can restamp it.
	freshnessReset := real && !j.obsAt.IsZero() &&
		(gen != j.obsGen || now.Sub(j.obsAt) > previewFreshnessGap)

	// settleReset collects every settling invalidation this report
	// triggers; the epoch bumps at most once per report.
	settleReset := false

	wasRunning := j.active && j.state == "RUNNING"
	// leftRunning marks a merged transition out of RUNNING, including a
	// pause and a preparation transition: every one of them ends the
	// settled interval.
	leftRunning := wasRunning && hasState && state != "RUNNING"

	// Lifecycle boundaries, evaluated once per report.
	terminal := false
	newGen := false
	if hasState {
		switch {
		case !j.active:
			// First observation of a preview-active state, or entry
			// from an inactive state: a new job generation. A first
			// observation of FINISH/FAILED/IDLE creates no job.
			newGen = previewStateActive(state)
		case !previewStateActive(state):
			// Terminal precedence: end the generation without
			// merging the terminal report's reset identity or name
			// values, and without starting a new generation.
			terminal = true
		case previewStateExec(j.state) && !previewStateExec(state):
			// RUNNING/PAUSE/PAUSED to PREPARE/SLICING: a re-slice or
			// restart is a new job; SLICING to PREPARE is not.
			newGen = true
		}
	}

	if terminal {
		settleReset = leftRunning || freshnessReset
		if settleReset {
			j.resetSettling()
		}
		j.active = false
		if hasState {
			j.state = state
			j.stateGen = gen
		}
		if real {
			j.obsGen = gen
			j.obsAt = now
		}
		return
	}

	// Strong identity boundaries inside an active generation: a known
	// valid project_id or task_id changing to a different valid value, or
	// a known positive gcode_start_time changing to a different positive
	// value, starts a new generation. Missing-to-known enrichment does
	// not. Sticky values compare before the clear below resets them.
	if j.active {
		if hasProject && j.knownProject && project != j.lastProjectID {
			newGen = true
		}
		if hasTask && j.knownTask && task != j.lastTaskID {
			newGen = true
		}
		if hasStarted && startedF > 0 && j.knownStarted && int64(startedF) != j.lastStarted {
			newGen = true
		}
	}

	if newGen {
		j.generation++
		j.active = true
		j.clear()
	}
	if leftRunning {
		// Leaving RUNNING ends the previous settling interval with
		// it, whichever state replaced it.
		settleReset = true
	}

	// Merge identity fields once. Absent values keep the previous field;
	// explicit empty or invalid values clear the public field. The
	// private sticky identities are only ever replaced by valid values.
	changed := false
	if j.active {
		if namePresent {
			if hasName && name != j.name {
				j.name = name
				changed = true
			} else if !hasName && j.name != "" {
				// Present but not a string: clear the field.
				j.name = ""
				changed = true
			}
		}
		if filePresent {
			if hasFile && file != j.gcodeFile {
				j.gcodeFile = file
				changed = true
			} else if !hasFile && j.gcodeFile != "" {
				j.gcodeFile = ""
				changed = true
			}
		}
		if platePresent {
			switch {
			case hasPlate && plate > 0:
				if j.plateIndex == nil || *j.plateIndex != plate {
					p := plate
					j.plateIndex = &p
					changed = true
				}
			case j.plateIndex != nil:
				// Explicit nonpositive or non-numeric index
				// clears the field.
				j.plateIndex = nil
				changed = true
			}
		}
		if startedPresent {
			switch {
			case hasStarted && startedF > 0:
				at := time.Unix(int64(startedF), 0)
				if !j.startedAt.Equal(at) {
					j.startedAt = at
					changed = true
				}
				j.lastStarted = int64(startedF)
				j.knownStarted = true
			case !j.startedAt.IsZero():
				// Explicit nonpositive or non-numeric timestamp
				// clears the merged field; the sticky
				// last-known value is retained.
				j.startedAt = time.Time{}
				changed = true
			}
		}
		if projectPresent {
			if hasProject {
				if project != j.projectID {
					j.projectID = project
					changed = true
				}
				j.lastProjectID = project
				j.knownProject = true
			} else if j.projectID != "" {
				// Present but invalid or empty: clear the public
				// field; the sticky last-known identity is
				// retained so the same valid value returning
				// never starts a generation.
				j.projectID = ""
				changed = true
			}
		}
		if taskPresent {
			if hasTask {
				if task != j.taskID {
					j.taskID = task
					changed = true
				}
				j.lastTaskID = task
				j.knownTask = true
			} else if j.taskID != "" {
				j.taskID = ""
				changed = true
			}
		}
		if changed {
			j.revision++
			// An identity revision inside the generation
			// invalidates settling, and a metadata-only revision
			// never restarts it. A generation-start merge only
			// fills the cleared job: the generation increment is
			// that job's invalidation marker.
			if !newGen {
				settleReset = true
			}
		}
	}

	// Observation stamps. StateGen tracks only reports carrying
	// gcode_state; ObsAt and ObsGen track only real print reports.
	if hasState {
		j.state = state
		j.stateGen = gen
	}
	if real && freshnessReset {
		settleReset = true
	}
	if settleReset {
		j.resetSettling()
	}
	if real {
		j.obsGen = gen
		j.obsAt = now
		// Start or restart the RUNNING settling timer only on fresh
		// evidence: a real report, merged state RUNNING, and a state
		// stamp from this report's own transport generation. A
		// temperature stream on a new connection must never continue a
		// pre-reconnect timer, and a fresh RUNNING report after a
		// reconnect requires its own full settling interval.
		if j.state == "RUNNING" && j.stateGen == gen && j.runningSince.IsZero() {
			j.runningSince = now
		}
	}
}
