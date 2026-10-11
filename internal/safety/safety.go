// Package safety is a best-effort second line of defense behind the printer's own thermal protection.
package safety

import (
	"cmp"
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
	"bambu-mqtt-proxy/internal/pandabreath"
	"bambu-mqtt-proxy/internal/pandapwr"
	"bambu-mqtt-proxy/internal/printer"
)

// Settings is the YAML safety section.
type Settings struct {
	// Enabled turns the over-temperature stop on.
	Enabled bool `yaml:"enabled" json:"enabled"`
}

// SettingsOf decodes the safety section without changing the configuration.
func SettingsOf(c *config.Config) (Settings, error) {
	v, err := c.Section("safety")
	if err != nil {
		return Settings{}, err
	}
	return *v.(*Settings), nil
}

// ConfigSection owns the safety YAML section and its /config switch.
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
	View: func(file *config.Config) any {
		s, _ := SettingsOf(file)
		return s
	},
	Save: func(raw json.RawMessage, stored, out *config.Config) error {
		if len(raw) == 0 {
			// A page without the member keeps the stored switch.
			if stored != nil {
				if node, ok := stored.Sections["safety"]; ok {
					out.SetSection("safety", node)
				}
			}
			return nil
		}
		var in Settings
		if err := json.Unmarshal(raw, &in); err != nil {
			return fmt.Errorf("The safety settings could not be read: %w", err)
		}
		out.SetSection("safety", in)
		return nil
	},
}

// limits are the trip temperatures of one printer model in °C.
type limits [3]float64

// modelLimits maps config.DisplayModel names to trip temperatures: the model's rated maximum plus a margin, so normal printing never trips.
var modelLimits = map[string]limits{
	"A1MINI": {320, 95, 80}, "A1": {320, 115, 80}, "P1P": {320, 115, 80}, "P1S": {320, 115, 80},
	"X1": {320, 125, 80}, "X1C": {320, 125, 80}, "P2S": {320, 125, 80}, "X1E": {340, 135, 80},
}

// defaultLimits apply to unknown models with the most permissive model limits.
var defaultLimits = limits{370, 135, 80}

// StateSource reads the core printer document and connectivity.
type StateSource interface {
	Get(serial string) (printer.Doc, bool, bool)
}

// ChamberSource reads the retained accessory chamber reading.
type ChamberSource interface {
	Chamber(string) (float64, time.Time, bool)
}

// Commander sends stop commands.
type Commander interface {
	// Stop sends one stop command.
	Stop(serial string) error
}

// Service checks every printer's temperatures once per second and responds once per over-limit episode.
type Service struct {
	// printers lists the configured fleet.
	printers []config.Printer
	// chamber reads accessory temperatures.
	chamber ChamberSource
	// state reads printer documents.
	state StateSource
	// cmd sends fleet stop commands.
	cmd Commander
	// act records safety actions.
	act *activity.Log
	// notify holds emergency credentials.
	notify notification.Settings
	// log reports safety outcomes.
	log *slog.Logger
	// send delivers the emergency notification; tests replace it.
	send func(ctx context.Context, cfg notification.Settings, title, body string) error
	// powerOff switches a Panda PWR off; tests replace it.
	powerOff func(ctx context.Context, address string) error
	// tripped holds printers already handled in the current over-limit episode.
	tripped map[string]bool
	// responses joins emergency work at shutdown.
	responses sync.WaitGroup
}

// New builds the safety service for the configured printers.
func New(printers []config.Printer, state StateSource, chamber ChamberSource, cmd Commander, act *activity.Log, notify notification.Settings, log *slog.Logger) *Service {
	return &Service{printers: printers, state: state, chamber: chamber, cmd: cmd, act: act,
		notify: notify, log: log, send: notification.SendEmergency, powerOff: pandapwr.PowerOff, tripped: make(map[string]bool)}
}

// Module declares the safety monitor lifecycle.
func (s *Service) Module() module.Module { return module.Module{Name: "safety", Run: s.Run} }

// Run checks temperatures once per second and waits for responses at shutdown.
func (s *Service) Run(ctx context.Context) error {
	defer s.responses.Wait()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for _, p := range s.printers {
				s.check(ctx, p)
			}
		}
	}
}

// check compares one printer's cached temperatures with its limits while the printer is online.
func (s *Service) check(ctx context.Context, p config.Printer) {
	st, online, ok := s.state.Get(p.Serial)
	if !ok || !online {
		return
	}
	temps, _ := st.Temps()
	model := config.DisplayModel(p.Model, p.Serial)
	if !config.ChamberTemperatureSupported(model, p.Serial) {
		temps.Chamber = nil
		if s.chamber != nil {
			if value, _, ok := s.chamber.Chamber(p.Serial); ok {
				temps.Chamber = &value
			}
		}
	}
	lim, ok := modelLimits[model]
	if !ok {
		lim = defaultLimits
	}
	probe, value, limit, over := overLimit(temps, lim)
	if !over {
		if temps.Nozzle == nil || temps.Bed == nil || (p.Setting(pandabreath.AddressKey) != "" || config.ChamberTemperatureSupported(model, p.Serial)) && temps.Chamber == nil {
			return
		}
		delete(s.tripped, p.Serial)
		return
	}
	if s.tripped[p.Serial] {
		return
	}
	s.tripped[p.Serial] = true
	s.responses.Go(func() { s.respond(ctx, p, probe, value, limit) })
}

// overLimit returns the first temperature at or above its limit.
func overLimit(temps printer.Temps, lim limits) (probe string, value, limit float64, over bool) {
	for i, value := range []*float64{temps.Nozzle, temps.Bed, temps.Chamber} {
		if value != nil && *value >= lim[i] {
			return []string{"Nozzle", "Bed", "Chamber"}[i], *value, lim[i], true
		}
	}
	return "", 0, 0, false
}

// respond stops every printer, switches the impacted printer's Panda PWR off when configured, and sends one emergency notification that lists the outcome.
func (s *Service) respond(parent context.Context, p config.Printer, probe string, value, limit float64) {
	name := cmp.Or(p.Name, p.Serial)
	reason := fmt.Sprintf("%s temperature %.0f°C reached the %.0f°C safety limit.", probe, value, limit)
	s.log.Error("safety limit reached: stopping all printers", "serial", p.Serial, "probe", probe, "value", value, "limit", limit)
	s.act.Record(p.Serial, "safety_overtemp", activity.Error, reason+" Stopping all printers.")
	lines := []string{reason}
	for _, q := range s.printers {
		if err := s.cmd.Stop(q.Serial); err != nil {
			s.log.Error("safety stop failed", "serial", q.Serial, "error", err)
			lines = append(lines, "Stop NOT sent to "+q.Serial+": "+err.Error())
		}
	}
	if len(lines) == 1 {
		lines = append(lines, "Stop sent to all printers.")
	}
	if addr := p.Setting(pandapwr.AddressKey); addr != "" {
		ctx, cancel := context.WithTimeout(parent, 10*time.Second)
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
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	if err := s.send(ctx, s.notify, "FIRE RISK: "+name, strings.Join(lines, "\n")); err != nil {
		s.log.Error("safety emergency notification failed", "serial", p.Serial, "error", err)
	}
}
