package pandabreath

import (
	"fmt"
	"math"
	"time"

	"bambu-mqtt-proxy/internal/module"
)

// Display tuning.
const (
	// sampleEvery is the minimum spacing between kept trend samples.
	sampleEvery = 10 * time.Second
	// trendWindow is the longest span of samples kept behind the trend.
	trendWindow = 3 * time.Minute
	// trendMinSpan is the shortest sample span that yields a trend.
	trendMinSpan = 2 * time.Minute
	// trendMinDelta is the smallest temperature change, in degrees, that counts as warming or cooling.
	trendMinDelta = 2.0
)

// Link states of ModuleState.Link.
const (
	// LinkConnected means the device link is open and its reading is fresh.
	LinkConnected = "connected"
	// LinkNoReadings means the device link is open but no fresh reading has arrived.
	LinkNoReadings = "no_readings"
	// LinkOffline means the device link is closed and no fresh reading exists.
	LinkOffline = "offline"
)

// Trend directions of ModuleState.Trend.
const (
	// TrendWarming means the chamber rose by at least trendMinDelta.
	TrendWarming = "warming"
	// TrendCooling means the chamber fell by at least trendMinDelta.
	TrendCooling = "cooling"
	// TrendSteady means the chamber changed by less than trendMinDelta.
	TrendSteady = "steady"
)

// ModuleState is one printer's Panda Breath state.
type ModuleState struct {
	// Link is LinkConnected, LinkNoReadings, or LinkOffline.
	Link string `json:"link"`
	// ChamberC is the device's own chamber reading in degrees Celsius, present only while the reading is fresh.
	ChamberC *float64 `json:"chamber_c,omitempty"`
	// Trend is TrendWarming, TrendCooling, or TrendSteady.
	Trend string `json:"trend,omitempty"`
	// RateCPerMin is the signed rate of change in degrees Celsius per minute, rounded to half a degree with a 0.5 floor.
	RateCPerMin *float64 `json:"rate_c_per_min,omitempty"`
}

// Module declares the accessory store's lifecycle, chamber reading hook, camera wall display, and per-printer state.
func (s *Store) Module() module.Module {
	return module.Module{
		Name:    "pandabreath",
		Run:     s.Run,
		Display: s.Display,
		State:   s.moduleState,
	}
}

// moduleState returns the ModuleState for one printer, or nil when the printer has no configured device.
func (s *Store) moduleState(serial string) any {
	v, ok := s.snapshot(serial)
	if !ok {
		return nil
	}
	return v
}

// snapshot computes one printer's ModuleState under the store lock. ok is false when the printer has no configured device.
func (s *Store) snapshot(serial string) (v ModuleState, ok bool) {
	if !s.configured(serial) {
		return ModuleState{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, have := s.readings[serial]
	fresh := have && time.Since(r.at) <= 15*time.Second
	switch {
	case fresh:
		v.Link = LinkConnected
	case s.connected[serial]:
		v.Link = LinkNoReadings
	default:
		v.Link = LinkOffline
	}
	if !fresh {
		return v, true
	}
	temp := r.temp
	v.ChamberC = &temp
	delta, span, have := trend(s.history[serial], r)
	if !have {
		return v, true
	}
	rate := max(0.5, math.Round(math.Abs(delta)/span.Minutes()*2)/2)
	switch {
	case delta >= trendMinDelta:
		v.Trend, v.RateCPerMin = TrendWarming, &rate
	case delta <= -trendMinDelta:
		rate = -rate
		v.Trend, v.RateCPerMin = TrendCooling, &rate
	default:
		v.Trend = TrendSteady
	}
	return v, true
}

// Display returns the Panda Breath panel and warming badge for one printer, or nil when the printer has no configured device or nothing to show.
func (s *Store) Display(serial string) *module.Display {
	v, ok := s.snapshot(serial)
	if !ok {
		return nil
	}
	// Link shows only when something is wrong; a healthy link adds nothing, and
	// the chamber reading already appears in the printer's temperature chips.
	var rows []module.Row
	switch v.Link {
	case LinkConnected:
	case LinkNoReadings:
		rows = append(rows, module.Row{Label: "Link", Value: "Connected · no readings", Tone: "warn"})
	default:
		rows = append(rows, module.Row{Label: "Link", Value: "Offline", Tone: "warn"})
	}
	d := &module.Display{}
	// Warming is stated once, by the overlay badge; the panel keeps the other trends.
	if v.ChamberC != nil && v.Trend != "" {
		switch v.Trend {
		case TrendWarming:
			rate := *v.RateCPerMin
			d.Overlay = []module.Badge{{Text: "Chamber warming", Tone: "calm",
				Title: fmt.Sprintf("Panda Breath: chamber rising %.1f °C per minute", rate)}}
		case TrendCooling:
			rows = append(rows, module.Row{Label: "Trend", Value: fmt.Sprintf("Cooling · −%.1f °C/min", -*v.RateCPerMin)})
		default:
			rows = append(rows, module.Row{Label: "Trend", Value: "Steady"})
		}
	}
	if len(rows) > 0 {
		d.Panel = &module.Panel{Title: "Panda Breath", Rows: rows}
	}
	if d.Panel == nil && len(d.Overlay) == 0 {
		return nil
	}
	return d
}

// configured reports whether serial has a Panda Breath address. targets is fixed at construction, so no lock is needed.
func (s *Store) configured(serial string) bool {
	for _, t := range s.targets {
		if t.serial == serial {
			return true
		}
	}
	return false
}

// setConnected records whether the device WebSocket for serial is open.
func (s *Store) setConnected(serial string, open bool) {
	s.mu.Lock()
	s.connected[serial] = open
	s.mu.Unlock()
}

// keepSample drops samples older than trendWindow relative to r, then adds r when the newest kept sample is at least sampleEvery old.
func keepSample(h []reading, r reading) []reading {
	i := 0
	for i < len(h) && r.at.Sub(h[i].at) > trendWindow {
		i++
	}
	h = h[i:]
	if n := len(h); n > 0 && r.at.Sub(h[n-1].at) < sampleEvery {
		return h
	}
	return append(h, r)
}

// trend returns the temperature change from the oldest kept sample to the
// latest reading and the time between them. ok is false until the samples
// span at least trendMinSpan.
func trend(h []reading, latest reading) (delta float64, span time.Duration, ok bool) {
	if len(h) == 0 {
		return 0, 0, false
	}
	span = latest.at.Sub(h[0].at)
	if span < trendMinSpan {
		return 0, 0, false
	}
	return latest.temp - h[0].temp, span, true
}
