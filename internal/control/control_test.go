package control

import (
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

type fakeCmd struct {
	connected bool
	sent      []string
	err       error
}

func (f *fakeCmd) Status() map[string]bool          { return map[string]bool{"S1": f.connected} }
func (f *fakeCmd) Generation(string) uint64         { return 4 }
func (f *fakeCmd) record(name string) error         { f.sent = append(f.sent, name); return f.err }
func (f *fakeCmd) PausePrint(string, uint64) error  { return f.record("pause") }
func (f *fakeCmd) ResumePrint(string, uint64) error { return f.record("resume") }
func (f *fakeCmd) StopPrint(string, uint64) error   { return f.record("stop") }
func (f *fakeCmd) SetSpeedProfile(_ string, _ uint64, p int) error {
	return f.record("speed" + string(rune('0'+p)))
}
func (f *fakeCmd) SetChamberLight(_ string, _ uint64, on bool) error {
	if on {
		return f.record("light on")
	}
	return f.record("light off")
}

func newService(t *testing.T, state string, connected bool) (*Service, *fakeCmd, *activity.Log) {
	t.Helper()
	printers := []config.Printer{{Serial: "S1", Name: "Shop"}}
	cache := telemetry.NewCache(printers, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if state != "" {
		cache.Observe("S1", []byte(`{"print":{"gcode_state":"`+state+`"}}`))
	}
	cmd := &fakeCmd{connected: connected}
	log := activity.New(printers)
	return New(cmd, cache, log), cmd, log
}

func TestStopOnIdleSends(t *testing.T) {
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

func TestPauseOnIdleNotAvailable(t *testing.T) {
	s, cmd, _ := newService(t, "IDLE", true)
	if _, err := s.Do("S1", "MCP", Request{Action: ActionPause}); !errors.Is(err, ErrNotAvailable) {
		t.Fatalf("err = %v, want ErrNotAvailable", err)
	}
	if len(cmd.sent) != 0 {
		t.Fatalf("sent %v", cmd.sent)
	}
}

func TestDisconnectedAndInvalid(t *testing.T) {
	s, cmd, _ := newService(t, "RUNNING", false)
	for _, action := range []string{ActionStop, ActionPause} {
		if _, err := s.Do("S1", "MCP", Request{Action: action}); !errors.Is(err, ErrNotConnected) {
			t.Fatalf("%s err = %v, want ErrNotConnected", action, err)
		}
	}
	cmd.connected = true
	if _, err := s.Do("S1", "MCP", Request{Action: ActionSpeed, Profile: "turbo"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("turbo err = %v", err)
	}
	if _, err := s.Do("S1", "MCP", Request{Action: "heat"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("heat err = %v", err)
	}
	if _, err := s.Do("S1", "MCP", Request{Action: ActionLight}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("light without on err = %v", err)
	}
	if _, err := s.Do("NOPE", "MCP", Request{Action: ActionStop}); !errors.Is(err, ErrUnknownPrinter) {
		t.Fatalf("unknown err = %v", err)
	}
	if _, err := s.Do("S1", "MCP", Request{Action: ActionSpeed, Profile: "sport"}); err != nil || cmd.sent[0] != "speed3" {
		t.Fatalf("speed = %v %v", err, cmd.sent)
	}
	cmd.err = errors.New("boom")
	if _, err := s.Do("S1", "MCP", Request{Action: ActionPause}); !errors.Is(err, ErrSendFailed) {
		t.Fatalf("send failure err = %v", err)
	}
	if len(cmd.sent) != 2 {
		t.Fatalf("sent %v", cmd.sent)
	}
}

func TestLightOffSendsAndRecordsActivity(t *testing.T) {
	s, cmd, log := newService(t, "RUNNING", true)
	off := false
	if _, err := s.Do("S1", "MCP", Request{Action: ActionLight, On: &off}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cmd.sent, []string{"light off"}) {
		t.Fatalf("sent %v", cmd.sent)
	}
	if got := log.Recent("S1"); len(got) != 1 || got[0].Kind != "control_light" ||
		got[0].Message != "Chamber light off sent from MCP" {
		t.Fatalf("activity = %+v", got)
	}
}

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
