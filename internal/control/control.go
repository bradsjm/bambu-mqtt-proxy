// Package control is the single allow-listed path from the camera wall and
// the MCP endpoint to printer commands: chamber light, pause, resume, speed
// profile, and stop. No gcode, temperature, or heater command exists here.
package control

import (
	"errors"
	"fmt"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/printer"
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
	ErrNotConnected   = errors.New("printer is not connected")
	ErrSendFailed     = errors.New("command could not be sent")
)

// Commander sends printer commands. It is satisfied by *printer.Links.
type Commander interface {
	Online(serial string) bool
	Pause(serial string) error
	Resume(serial string) error
	Stop(serial string) error
	Speed(serial string, level int) error
	Light(serial string, on bool) error
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

// Available lists the actions the printer's last reported state suggests
// right now, in a fixed order. It is a UI hint only: Do never consults
// it, so stale state can never block a command. A disconnected or unknown
// printer has none. Stop is always listed while connected: it is the
// emergency stop.
func (s *Service) Available(serial string) []string {
	st, ok := s.state.State(serial)
	if !ok || !s.cmd.Online(serial) {
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
	if !s.cmd.Online(serial) {
		return res, ErrNotConnected
	}
	var err error
	var message string
	switch req.Action {
	case ActionLight:
		err = s.cmd.Light(serial, *req.On)
		word := "off"
		if *req.On {
			word = "on"
		}
		message = "Chamber light " + word + " sent from " + origin
	case ActionPause:
		err = s.cmd.Pause(serial)
		message = "Pause sent from " + origin
	case ActionResume:
		err = s.cmd.Resume(serial)
		message = "Resume sent from " + origin
	case ActionSpeed:
		err = s.cmd.Speed(serial, profile)
		message = "Speed set to " + req.Profile + " from " + origin
	case ActionStop:
		err = s.cmd.Stop(serial)
		message = "Emergency stop sent from " + origin
	}
	if err != nil {
		if errors.Is(err, printer.ErrOffline) {
			return res, ErrNotConnected
		}
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
