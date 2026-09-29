// Package control is the single allow-listed path from the camera wall and
// the MCP endpoint to printer commands: chamber light, pause, resume, speed
// profile, and stop. No gcode, temperature, or heater command exists here.
package control

import (
	"errors"
	"fmt"
	"slices"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/telemetry"
)

// Control actions.
const (
	ActionLight  = "light"
	ActionPause  = "pause"
	ActionResume = "resume"
	ActionSpeed  = "speed"
	ActionStop   = "stop"
)

// Request is one control command.
type Request struct {
	Action  string `json:"action"`
	On      *bool  `json:"on,omitempty"`
	Profile string `json:"profile,omitempty"`
}

// Result reports a sent command.
type Result struct {
	Serial string `json:"serial"`
	Action string `json:"action"`
	Sent   bool   `json:"sent"`
}

// Errors returned by Do; send failures wrap ErrSendFailed.
var (
	ErrUnknownPrinter = errors.New("unknown printer")
	ErrInvalidRequest = errors.New("invalid control request")
	ErrNotAvailable   = errors.New("control not available in the current printer state")
	ErrNotConnected   = errors.New("printer is not connected")
	ErrSendFailed     = errors.New("command could not be sent")
)

// Commander publishes guarded printer commands. It is satisfied by
// *upstream.Pool.
type Commander interface {
	Status() map[string]bool
	Generation(serial string) uint64
	PausePrint(serial string, generation uint64) error
	ResumePrint(serial string, generation uint64) error
	StopPrint(serial string, generation uint64) error
	SetSpeedProfile(serial string, generation uint64, profile int) error
	SetChamberLight(serial string, generation uint64, on bool) error
}

// StateSource reads merged printer state. It is satisfied by
// *telemetry.Cache.
type StateSource interface {
	State(serial string) (telemetry.State, bool)
}

// speedProfiles maps profile names to Bambu spd_lvl values.
var speedProfiles = map[string]int{"silent": 1, "standard": 2, "sport": 3, "ludicrous": 4}

// Service validates and sends control requests.
type Service struct {
	cmd   Commander
	state StateSource
	log   *activity.Log
}

// New builds the control service. A nil log records nothing.
func New(cmd Commander, state StateSource, log *activity.Log) *Service {
	return &Service{cmd: cmd, state: state, log: log}
}

// Available lists the actions allowed right now, in a fixed order. A
// disconnected or unknown printer has none. Stop is always available while
// connected: it is the emergency stop, and stale state must never block it.
func (s *Service) Available(serial string) []string {
	st, ok := s.state.State(serial)
	if !ok || !s.cmd.Status()[serial] {
		return []string{}
	}
	out := []string{ActionLight}
	switch st.PrintingState {
	case "RUNNING":
		out = append(out, ActionPause, ActionSpeed)
	case "PAUSE", "PAUSED":
		out = append(out, ActionResume, ActionSpeed)
	}
	return append(out, ActionStop)
}

// Do validates and sends one control request. origin names the surface in
// the activity log ("camera wall" or "MCP").
func (s *Service) Do(serial, origin string, req Request) (Result, error) {
	res := Result{Serial: serial, Action: req.Action}
	if _, ok := s.state.State(serial); !ok {
		return res, ErrUnknownPrinter
	}
	profile := 0
	switch req.Action {
	case ActionLight:
		if req.On == nil {
			return res, fmt.Errorf("%w: light requires on", ErrInvalidRequest)
		}
	case ActionSpeed:
		var ok bool
		if profile, ok = speedProfiles[req.Profile]; !ok {
			return res, fmt.Errorf("%w: profile must be silent, standard, sport, or ludicrous", ErrInvalidRequest)
		}
	case ActionPause, ActionResume, ActionStop:
	default:
		return res, fmt.Errorf("%w: unknown action %q", ErrInvalidRequest, req.Action)
	}
	if !s.cmd.Status()[serial] {
		return res, ErrNotConnected
	}
	if !slices.Contains(s.Available(serial), req.Action) {
		return res, ErrNotAvailable
	}
	gen := s.cmd.Generation(serial)
	var err error
	var message string
	switch req.Action {
	case ActionLight:
		err = s.cmd.SetChamberLight(serial, gen, *req.On)
		word := "off"
		if *req.On {
			word = "on"
		}
		message = "Chamber light " + word + " sent from " + origin
	case ActionPause:
		err = s.cmd.PausePrint(serial, gen)
		message = "Pause sent from " + origin
	case ActionResume:
		err = s.cmd.ResumePrint(serial, gen)
		message = "Resume sent from " + origin
	case ActionSpeed:
		err = s.cmd.SetSpeedProfile(serial, gen, profile)
		message = "Speed set to " + req.Profile + " from " + origin
	case ActionStop:
		err = s.cmd.StopPrint(serial, gen)
		message = "Emergency stop sent from " + origin
	}
	if err != nil {
		return res, fmt.Errorf("%w: %v", ErrSendFailed, err)
	}
	severity := activity.Info
	if req.Action == ActionStop {
		severity = activity.Warning
	}
	s.log.Record(serial, "control_"+req.Action, severity, message)
	res.Sent = true
	return res, nil
}
