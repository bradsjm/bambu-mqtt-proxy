package pandapwr

import (
	"fmt"
	"math"

	"bambu-mqtt-proxy/internal/module"
)

// Link states of ModuleState.Link.
const (
	// LinkConnected means a fresh reading exists.
	LinkConnected = "connected"
	// LinkOffline means no fresh reading exists: the device is busy for
	// longer than the freshness window, unreachable, or never answered.
	LinkOffline = "offline"
)

// ModuleState is one printer's Panda PWR state. The core serves it as the
// camera wall tile member and as MCP state.modules.pandapwr: in
// get_printer_state, in the bambu://printers/{serial}/state resource, and
// as module_changed in watch_printer.
type ModuleState struct {
	// Link is LinkConnected or LinkOffline.
	Link string `json:"link"`
	// PowerW is the device's power draw in watts, rounded to a whole
	// number and present only while the reading is fresh.
	PowerW *float64 `json:"power_w,omitempty"`
}

// StableKey returns the link state only. Watts steps are value churn: they
// must not wake MCP watch_printer attention watchers.
func (v ModuleState) StableKey() any { return v.Link }

// Module declares the store's lifecycle and the camera wall panel and
// per-printer state hooks.
func (s *Store) Module() module.Module {
	return module.Module{
		Name:    "pandapwr",
		Start:   s.Start,
		Stop:    s.Stop,
		Display: s.Display,
		State:   s.moduleState,
	}
}

// moduleState returns the ModuleState for one printer, or nil when the
// printer has no configured device.
func (s *Store) moduleState(serial string) any {
	v, ok := s.snapshot(serial)
	if !ok {
		return nil
	}
	return v
}

// snapshot computes one printer's ModuleState under the store lock. ok is
// false when the printer has no configured device. Watts are rounded to a
// whole number and excluded from StableKey, so only a link change counts as
// a state change.
func (s *Store) snapshot(serial string) (v ModuleState, ok bool) {
	if !s.configured(serial) {
		return ModuleState{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, have := s.readings[serial]
	if !have || timeNow().Sub(r.at) > freshWindow {
		return ModuleState{Link: LinkOffline}, true
	}
	w := math.Round(r.power)
	return ModuleState{Link: LinkConnected, PowerW: &w}, true
}

// Display returns the Panda PWR panel for one printer, or nil when the
// printer has no configured device. The panel shows the link state and,
// while the reading is fresh, the power draw in whole watts.
func (s *Store) Display(serial string) *module.Display {
	v, ok := s.snapshot(serial)
	if !ok {
		return nil
	}
	link := module.Row{Label: "Link", Value: "Offline", Tone: "warn"}
	if v.Link == LinkConnected {
		link.Value, link.Tone = "Connected", "calm"
	}
	d := &module.Display{Panel: &module.Panel{Title: "Panda PWR", Rows: []module.Row{link}}}
	if v.PowerW == nil {
		return d
	}
	d.Panel.Rows = append(d.Panel.Rows, module.Row{Label: "Power", Value: fmt.Sprintf("%.0f W", *v.PowerW)})
	return d
}

// configured reports whether serial has a Panda PWR address. targets is
// fixed at construction, so no lock is needed.
func (s *Store) configured(serial string) bool {
	for _, t := range s.targets {
		if t.serial == serial {
			return true
		}
	}
	return false
}
