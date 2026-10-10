// Preview-job projection: a second, independent view of the current print
// beside the detection session. It answers "which generation of a
// printable job is on the bed". A field keeps the last value the printer
// reported until the printer reports a different value: an absent key
// keeps the value, while an explicit empty or invalid key clears the
// public field. A new print starts only from a printer-reported state
// transition or a changed identity.

package telemetry

import "time"

// JobView is the preview-job projection of one printer: which generation of
// a printable job is current and the last values the printer reported for
// its identity fields. It is independent of SessionView.
type JobView struct {
	// Generation increments at every preview-job boundary: entry from an
	// inactive state into a preview-active one, a re-preparation
	// transition, or a strong identity change inside an active job. One
	// generation grants at most one preview attempt.
	Generation uint64
	// Active reports whether a preview-active job is current. An ended
	// job keeps its generation and identity as last-job data with Active
	// false.
	Active bool
	// State is the last reported gcode_state.
	State string
	// Name is the last reported subtask_name.
	Name string
	// GCodeFile is the last reported gcode_file value.
	GCodeFile string
	// ProjectID and TaskID are the last reported strong identity ids.
	// "0" is a valid local-print id, not a missing value.
	ProjectID string
	TaskID    string
	// PlateIndex is the last reported plate_idx; nil means not reported.
	PlateIndex *int
	// StartedAt is the last reported gcode_start_time; zero means not reported.
	StartedAt time.Time
	// StartObserved reports whether this proxy saw the print begin: the
	// generation started from a printer-reported state transition after
	// an earlier gcode_state was already known, or from an identity
	// change inside an active job. It is false when the first state this
	// proxy ever saw for the printer was already active.
	StartObserved bool
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
	generation uint64
	active     bool
	state      string
	name       string
	gcodeFile  string
	projectID  string
	taskID     string
	plateIndex *int
	startedAt  time.Time
	// startObserved mirrors JobView.StartObserved for the current
	// generation. stateKnown becomes true when any report carrying
	// gcode_state has merged and is never reset.
	startObserved bool
	stateKnown    bool
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
		Generation:    j.generation,
		Active:        j.active,
		State:         j.state,
		Name:          j.name,
		GCodeFile:     j.gcodeFile,
		ProjectID:     j.projectID,
		TaskID:        j.taskID,
		StartedAt:     j.startedAt,
		StartObserved: j.startObserved,
	}
	if j.plateIndex != nil {
		p := *j.plateIndex
		v.PlateIndex = &p
	}
	return v
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
// It runs for real print reports and metadata-only deltas alike: every
// report recomputes the boundary decisions from the pre-merge job state
// and then merges every identity field it carries, so a field keeps the
// last value the printer reported until the printer reports a different
// one. A report increments the generation at most once. Detection
// bookkeeping (trackSession) is never touched.
//
// Lifecycle boundaries apply only when gcode_state is present: terminal
// precedence ends the job without starting a generation, entry into a
// preview-active state starts one, and re-preparation starts one. Strong
// identity boundaries apply only when the job is still active and no
// lifecycle boundary fired: a known valid project or task id changing to
// a different valid value, a known positive start time changing to a
// different positive value, or a new non-empty subtask name over an
// already known one. A gcode_file change never starts a generation.
//
// Field keys are checked for presence separately from validity: an absent
// key keeps the previous merged value, while a key that is present but
// invalid or empty clears its merged public field. The private sticky
// identities keep their last valid values across invalid updates, so the
// same valid value returning never starts a generation.
func (st *State) trackJob(printObj map[string]any) {
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

	prevKnown := j.stateKnown
	newGen := false
	if hasState {
		j.stateKnown = true
		switch {
		case j.active && !previewStateActive(state):
			// Terminal precedence: end the generation without
			// starting a new one. The report's identity values
			// still merge below, carrying a terminal reset as
			// the new last-reported values.
			j.active = false
		case !j.active && previewStateActive(state):
			// Entry from an inactive state into a
			// preview-active one is a new print.
			newGen = true
		case j.active && previewStateExec(j.state) && previewStateActive(state) && !previewStateExec(state):
			// RUNNING/PAUSE/PAUSED back to PREPARE/SLICING: a
			// re-slice or restart is a new print; SLICING to
			// PREPARE is not.
			newGen = true
		}
	}

	// Strong identity boundaries inside an active job. Sticky values
	// compare before the merge below updates them.
	lifecycleGen := newGen
	if j.active && !newGen {
		switch {
		case hasProject && j.knownProject && project != j.lastProjectID:
			newGen = true
		case hasTask && j.knownTask && task != j.lastTaskID:
			newGen = true
		case hasStarted && startedF > 0 && j.knownStarted && int64(startedF) != j.lastStarted:
			newGen = true
		case hasName && name != "" && j.name != "" && name != j.name:
			newGen = true
		}
	}

	if newGen {
		j.generation++
		j.active = true
		// A lifecycle entry is a print this proxy saw begin only when
		// an earlier gcode_state was already known. An identity
		// boundary inside an active job always carries its own start
		// evidence. No field is cleared: the last-reported values
		// stay until the printer reports different ones.
		if lifecycleGen {
			j.startObserved = prevKnown
		} else {
			j.startObserved = true
		}
	}

	// Merge every present identity field, in terminal and metadata-only
	// reports too. Absent values keep the previous field; explicit empty
	// or invalid values clear the public field. The private sticky
	// identities are only ever replaced by valid values.
	if namePresent {
		if hasName {
			j.name = name
		} else {
			// Present but not a string: clear the field.
			j.name = ""
		}
	}
	if filePresent {
		if hasFile {
			j.gcodeFile = file
		} else {
			j.gcodeFile = ""
		}
	}
	if platePresent {
		switch {
		case hasPlate && plate > 0:
			p := plate
			j.plateIndex = &p
		default:
			// Explicit nonpositive or non-numeric index
			// clears the field.
			j.plateIndex = nil
		}
	}
	if startedPresent {
		switch {
		case hasStarted && startedF > 0:
			j.startedAt = time.Unix(int64(startedF), 0)
			j.lastStarted = int64(startedF)
			j.knownStarted = true
		default:
			// Explicit nonpositive or non-numeric timestamp
			// clears the merged field; the sticky
			// last-known value is retained.
			j.startedAt = time.Time{}
		}
	}
	if projectPresent {
		if hasProject {
			j.projectID = project
			j.lastProjectID = project
			j.knownProject = true
		} else {
			// Present but invalid or empty: clear the public
			// field; the sticky last-known identity is
			// retained so the same valid value returning
			// never starts a generation.
			j.projectID = ""
		}
	}
	if taskPresent {
		if hasTask {
			j.taskID = task
			j.lastTaskID = task
			j.knownTask = true
		} else {
			j.taskID = ""
		}
	}
	if hasState {
		j.state = state
	}
}
