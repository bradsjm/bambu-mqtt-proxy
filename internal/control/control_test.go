package control

import (
	"errors"
	"slices"
	"testing"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/printer"
)

// fakeCmd is a Commander over a mutable connected flag.
type fakeCmd struct {
	connected bool
	sent      []string
	err       error
}

func (f *fakeCmd) Online(string) bool          { return f.connected }
func (f *fakeCmd) record(name string) error    { f.sent = append(f.sent, name); return f.err }
func (f *fakeCmd) Pause(string) error          { return f.record("pause") }
func (f *fakeCmd) Resume(string) error         { return f.record("resume") }
func (f *fakeCmd) Stop(string) error           { return f.record("stop") }
func (f *fakeCmd) Speed(_ string, p int) error { return f.record("speed" + string(rune('0'+p))) }
func (f *fakeCmd) Light(_ string, on bool) error {
	if on {
		return f.record("light on")
	}
	return f.record("light off")
}

// newService builds a service over a cache holding one S1 state.
func newService(t *testing.T, state string, connected bool) (*Service, *fakeCmd, *activity.Log) {
	t.Helper()
	printers := []config.Printer{{Serial: "S1", Name: "Shop"}}
	cache := printer.NewStore(printers)
	if state != "" {
		cache.Apply("S1", []byte(`{"print":{"gcode_state":"`+state+`"}}`))
	}
	cmd := &fakeCmd{connected: connected}
	log := activity.New(printers)
	return New(cmd, cache, log), cmd, log
}

// TestActionsSendRegardlessOfState pins that Do never refuses on cached
// state: pause, resume, and speed all send, and Available stays a hint.
func TestActionsSendRegardlessOfState(t *testing.T) {
	s, cmd, _ := newService(t, "IDLE", true)
	for _, tc := range []struct {
		action  string
		profile string
		sent    string
	}{
		{ActionPause, "", "pause"},
		{ActionResume, "", "resume"},
		{ActionSpeed, "sport", "speed3"},
	} {
		res, err := s.Do("S1", "MCP", Request{Action: tc.action, Profile: tc.profile})
		if err != nil || !res.Sent {
			t.Fatalf("%s = %+v %v, want sent", tc.action, res, err)
		}
	}
	if !slices.Equal(cmd.sent, []string{"pause", "resume", "speed3"}) {
		t.Fatalf("sent %v", cmd.sent)
	}
	if got := s.Available("S1"); !slices.Equal(got, []string{"light", "stop"}) {
		t.Fatalf("hint = %v, want the idle hint", got)
	}
}

// TestOfflineReturnsNotConnected pins that both an offline link and an
// offline send race return ErrNotConnected.
func TestOfflineReturnsNotConnected(t *testing.T) {
	s, cmd, _ := newService(t, "RUNNING", false)
	for _, action := range []string{ActionStop, ActionPause} {
		if _, err := s.Do("S1", "MCP", Request{Action: action}); !errors.Is(err, ErrNotConnected) {
			t.Fatalf("%s err = %v, want ErrNotConnected", action, err)
		}
	}
	if len(cmd.sent) != 0 {
		t.Fatalf("sent %v", cmd.sent)
	}
	cmd.connected = true
	cmd.err = printer.ErrOffline
	if _, err := s.Do("S1", "MCP", Request{Action: ActionStop}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("offline race err = %v, want ErrNotConnected", err)
	}
}

// TestInvalidRequests pins request validation: bad profiles, unknown
// actions, missing light state, and unknown printers fail before sending.
func TestInvalidRequests(t *testing.T) {
	s, cmd, _ := newService(t, "RUNNING", true)
	off := false
	cases := map[string]struct {
		serial string
		req    Request
	}{
		"turbo profile":    {"S1", Request{Action: ActionSpeed, Profile: "turbo"}},
		"unknown action":   {"S1", Request{Action: "heat"}},
		"light without on": {"S1", Request{Action: ActionLight}},
		"unknown printer":  {"NOPE", Request{Action: ActionStop}},
	}
	for name, tc := range cases {
		res, err := s.Do(tc.serial, "MCP", tc.req)
		if !errors.Is(err, ErrInvalidRequest) && !errors.Is(err, ErrUnknownPrinter) {
			t.Fatalf("%s = %+v %v, want a validation error", name, res, err)
		}
	}
	if res, err := s.Do("S1", "MCP", Request{Action: ActionLight, On: &off}); err != nil || !res.Sent {
		t.Fatalf("light off = %+v %v, want sent", res, err)
	}
	cmd.err = errors.New("boom")
	if _, err := s.Do("S1", "MCP", Request{Action: ActionPause}); !errors.Is(err, ErrSendFailed) {
		t.Fatalf("send failure err = %v", err)
	}
	if len(cmd.sent) != 2 {
		t.Fatalf("sent %v", cmd.sent)
	}
}

// TestStopSendsAndRecordsActivity pins the stop path and its Warning entry.
func TestStopSendsAndRecordsActivity(t *testing.T) {
	s, cmd, log := newService(t, "IDLE", true)
	res, err := s.Do("S1", "camera wall", Request{Action: ActionStop})
	if err != nil || !res.Sent || !slices.Equal(cmd.sent, []string{"stop"}) {
		t.Fatalf("stop = %+v %v, sent %v", res, err, cmd.sent)
	}
	if e := log.Recent("S1"); len(e) != 1 || e[0].Kind != "control_stop" || e[0].Severity != activity.Warning ||
		e[0].Message != "Emergency stop sent from camera wall" {
		t.Fatalf("activity = %+v", e)
	}
}

// TestAvailable pins the hint matrix and the disconnected empty hint.
func TestAvailable(t *testing.T) {
	for state, want := range map[string][]string{
		"RUNNING": {"light", "pause", "speed", "stop"},
		"PAUSE":   {"light", "resume", "speed", "stop"},
		"IDLE":    {"light", "stop"},
	} {
		s, _, _ := newService(t, state, true)
		if got := s.Available("S1"); !slices.Equal(got, want) {
			t.Fatalf("%s: %v, want %v", state, got, want)
		}
	}
	s, _, _ := newService(t, "RUNNING", false)
	if got := s.Available("S1"); got == nil || len(got) != 0 {
		t.Fatalf("disconnected: %v", got)
	}
}
