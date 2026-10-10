// Package safety is a best-effort second line of defense behind the
// printer's own thermal protection. When a printer's nozzle, bed, or
// chamber temperature reaches its safety limit, the module stops every
// printer, switches the impacted printer's Panda PWR off when one is
// configured, and sends one Pushover emergency notification.
package safety

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/notification"
	"bambu-mqtt-proxy/internal/pandapwr"
	"bambu-mqtt-proxy/internal/telemetry"

	"gopkg.in/yaml.v3"
)

// Settings is the YAML safety section.
type Settings struct {
	// Enabled turns the over-temperature stop on.
	Enabled bool `yaml:"enabled"`
}

// SettingsOf decodes the safety section without changing the configuration.
func SettingsOf(c *config.Config) (Settings, error) {
	v, err := c.Section("safety")
	if err != nil {
		return Settings{}, err
	}
	return *v.(*Settings), nil
}

// ConfigSection owns the safety YAML section. The /config page has no
// safety form, so a page save keeps the stored section unchanged.
var ConfigSection = config.Section{
	Key: "safety",
	New: func() any { return &Settings{} },
	Validate: func(c *config.Config) error {
		s, err := SettingsOf(c)
		if err != nil || !s.Enabled {
			return err
		}
		n, err := notification.SettingsOf(c)
		if err != nil {
			return err
		}
		if strings.TrimSpace(n.Pushover.AppToken) == "" || strings.TrimSpace(n.Pushover.UserKey) == "" {
			return errors.New("safety: enabled safety requires the Pushover app token and user key in the notifications section")
		}
		return nil
	},
	Save: func(_ json.RawMessage, stored, out *config.Config) error {
		if stored == nil {
			return nil
		}
		if node, ok := stored.Sections["safety"]; ok {
			if out.Sections == nil {
				out.Sections = make(map[string]yaml.Node)
			}
			out.Sections["safety"] = node
		}
		return nil
	},
}

// limits are the trip temperatures of one printer model in °C.
type limits struct {
	nozzle, bed, chamber float64
}

// modelLimits maps config.DisplayModel names to trip temperatures: the
// model's rated maximum plus a margin, so normal printing never trips.
var modelLimits = map[string]limits{
	"A1MINI": {320, 95, 80},
	"A1":     {320, 115, 80},
	"P1P":    {320, 115, 80},
	"P1S":    {320, 115, 80},
	"X1":     {320, 125, 80},
	"X1C":    {320, 125, 80},
	"P2S":    {320, 125, 80},
	"X1E":    {340, 135, 80},
	"H2D":    {370, 135, 80},
	"H2DPRO": {370, 135, 80},
	"H2S":    {370, 135, 80},
	"H2C":    {370, 135, 80},
	"X2D":    {370, 135, 80},
}

// defaultLimits apply to unknown models with the most permissive model limits.
var defaultLimits = limits{370, 135, 80}

// StateSource reads merged printer state. *telemetry.Cache satisfies it.
type StateSource interface {
	State(serial string) (telemetry.State, bool)
}

// Commander sends stop commands and reports printer liveness. *printer.Links satisfies it.
type Commander interface {
	// Online reports whether the printer is connected and subscribed.
	Online(serial string) bool
	// Stop sends one stop command.
	Stop(serial string) error
}

// Service checks every printer's temperatures once per second and responds
// once per over-limit episode.
type Service struct {
	printers []config.Printer
	state    StateSource
	cmd      Commander
	act      *activity.Log
	notify   notification.Settings
	log      *slog.Logger
	// send delivers the emergency notification; tests replace it.
	send func(ctx context.Context, cfg notification.Settings, title, body string) error
	// powerOff switches a Panda PWR off; tests replace it.
	powerOff func(ctx context.Context, address string) error

	// tripped holds printers already handled in the current over-limit
	// episode. Only the loop goroutine uses it.
	tripped map[string]bool
	stop    chan struct{}
	done    chan struct{}
	// responses tracks running responses so Stop waits for them.
	responses sync.WaitGroup
}

// New builds the safety service for the configured printers.
func New(printers []config.Printer, state StateSource, cmd Commander, act *activity.Log, notify notification.Settings, log *slog.Logger) *Service {
	return &Service{
		printers: printers,
		state:    state,
		cmd:      cmd,
		act:      act,
		notify:   notify,
		log:      log,
		send:     notification.SendEmergency,
		powerOff: pandapwr.PowerOff,
		tripped:  make(map[string]bool),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Module declares the service lifecycle.
func (s *Service) Module() module.Module {
	return module.Module{Name: "safety", Start: s.Start, Stop: s.Stop}
}

// Start launches the check loop. The core calls it once.
func (s *Service) Start() {
	s.log.Info("safety monitor started", "printers", len(s.printers))
	go s.loop()
}

// Stop ends the check loop and waits for running responses to finish.
func (s *Service) Stop() {
	close(s.stop)
	<-s.done
	s.responses.Wait()
}

// loop checks every printer once per second, about the printer report rate.
func (s *Service) loop() {
	defer close(s.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			for _, p := range s.printers {
				s.check(p)
			}
		}
	}
}

// check compares one printer's cached temperatures with its limits while
// the printer is online. The latch clears only when an online printer's
// readings are all below the limits, so a cached over-limit value trips
// only if the printer is still connected and no check has seen it yet.
func (s *Service) check(p config.Printer) {
	if !s.cmd.Online(p.Serial) {
		return
	}
	st, ok := s.state.State(p.Serial)
	if !ok {
		return
	}
	probe, value, limit, over := overLimit(st)
	if !over {
		if s.tripped[p.Serial] {
			s.log.Info("safety re-armed", "serial", p.Serial)
		}
		delete(s.tripped, p.Serial)
		return
	}
	if s.tripped[p.Serial] {
		return
	}
	s.tripped[p.Serial] = true
	s.responses.Add(1)
	go func() {
		defer s.responses.Done()
		s.respond(p, probe, value, limit)
	}()
}

// overLimit returns the first temperature at or above its limit. The
// chamber reading is the native sensor on models that have one, otherwise
// the latest accessory reading. A stale accessory reading keeps its last
// value, so losing the device never re-arms a tripped printer.
func overLimit(st telemetry.State) (probe string, value, limit float64, over bool) {
	model := config.DisplayModel(st.Model, st.Serial)
	lim, ok := modelLimits[model]
	if !ok {
		lim = defaultLimits
	}
	chamber := st.AccessoryChamberTemp
	if config.ChamberTemperatureSupported(model, st.Serial) {
		chamber = st.ChamberTemp
	}
	for _, r := range []struct {
		probe string
		value *float64
		limit float64
	}{
		{"Nozzle", st.NozzleTemp, lim.nozzle},
		{"Bed", st.BedTemp, lim.bed},
		{"Chamber", chamber, lim.chamber},
	} {
		if r.value != nil && *r.value >= r.limit {
			return r.probe, *r.value, r.limit, true
		}
	}
	return "", 0, 0, false
}

// respond stops every printer, switches the impacted printer's Panda PWR
// off when configured, and sends one emergency notification that lists
// the outcome. Each step is one attempt.
func (s *Service) respond(p config.Printer, probe string, value, limit float64) {
	name := p.Name
	if name == "" {
		name = p.Serial
	}
	reason := fmt.Sprintf("%s temperature %.0f°C reached the %.0f°C safety limit.", probe, value, limit)
	s.log.Error("safety limit reached: stopping all printers", "serial", p.Serial, "probe", probe, "value", value, "limit", limit)
	s.act.Record(p.Serial, "safety_overtemp", activity.Error, reason+" Stopping all printers.")

	lines := []string{reason}
	for _, q := range s.printers {
		if err := s.cmd.Stop(q.Serial); err != nil {
			s.log.Error("safety stop failed", "serial", q.Serial, "error", err)
			lines = append(lines, "Stop NOT sent to "+q.Serial+": "+err.Error())
			continue
		}
		s.log.Warn("safety stop sent", "serial", q.Serial)
	}
	if len(lines) == 1 {
		lines = append(lines, "Stop sent to all printers.")
	}

	if addr := p.Setting(pandapwr.AddressKey); addr != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := s.powerOff(ctx, addr)
		cancel()
		if err != nil {
			s.log.Error("safety power off failed", "serial", p.Serial, "error", err)
			lines = append(lines, "Panda PWR power off FAILED: "+err.Error())
		} else {
			s.log.Warn("safety power off sent", "serial", p.Serial)
			s.act.Record(p.Serial, "safety_power_off", activity.Warning, "Panda PWR power off sent")
			lines = append(lines, "Panda PWR power off sent.")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.send(ctx, s.notify, "FIRE RISK: "+name, strings.Join(lines, "\n")); err != nil {
		s.log.Error("safety emergency notification failed", "serial", p.Serial, "error", err)
		return
	}
	s.log.Warn("safety emergency notification sent", "serial", p.Serial)
}
