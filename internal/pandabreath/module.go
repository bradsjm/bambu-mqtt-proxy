package pandabreath

import (
	"fmt"
	"math"
	"time"

	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/printerview"
)

// Display tuning. The device reports whole degrees about once a second, so
// the trend compares spaced samples over a few minutes and needs a rise or
// fall of at least two degrees: one-degree sensor flicker never reads as a
// trend.
const (
	// sampleEvery is the minimum spacing between kept trend samples.
	sampleEvery = 10 * time.Second
	// trendWindow is the longest span of samples kept behind the trend.
	trendWindow = 3 * time.Minute
	// trendMinSpan is the shortest sample span that yields a trend.
	trendMinSpan = 2 * time.Minute
	// trendMinDelta is the smallest temperature change, in degrees, that
	// counts as warming or cooling.
	trendMinDelta = 2.0
)

// Link states of ModuleState.Link.
const (
	// LinkConnected means the device link is open and its reading is fresh.
	LinkConnected = "connected"
	// LinkNoReadings means the device link is open but no fresh reading
	// has arrived.
	LinkNoReadings = "no_readings"
	// LinkOffline means the device link is closed and no fresh reading
	// exists.
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

// ModuleState is one printer's Panda Breath state. The core serves it as
// the camera wall tile member and as MCP state.modules.pandabreath.
type ModuleState struct {
	// Link is LinkConnected, LinkNoReadings, or LinkOffline.
	Link string `json:"link"`
	// ChamberC is the device's own chamber reading in degrees Celsius,
	// present only while the reading is fresh.
	ChamberC *float64 `json:"chamber_c,omitempty"`
	// Trend is TrendWarming, TrendCooling, or TrendSteady. It is empty
	// until the kept samples span trendMinSpan, and while no fresh reading
	// exists.
	Trend string `json:"trend,omitempty"`
	// RateCPerMin is the signed rate of change in degrees Celsius per
	// minute, rounded to half a degree with a 0.5 floor. It is present
	// only while Trend is TrendWarming or TrendCooling.
	RateCPerMin *float64 `json:"rate_c_per_min,omitempty"`
}

// StableKey returns the link and the trend only. Chamber temperature and
// rate steps are value churn: they must not wake MCP watch_printer
// attention watchers.
func (v ModuleState) StableKey() any { return [2]string{v.Link, v.Trend} }

// Module declares the accessory store's lifecycle, chamber reading hook,
// camera wall display, and per-printer state.
func (s *Store) Module() module.Module {
	return module.Module{
		Name:           "pandabreath",
		Start:          s.Start,
		Stop:           s.Stop,
		ChamberReading: s.ChamberReading,
		Display:        s.Display,
		State:          s.moduleState,
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
// false when the printer has no configured device. Values change only with
// real changes: the rate is rounded to half a degree per minute.
func (s *Store) snapshot(serial string) (v ModuleState, ok bool) {
	if !s.configured(serial) {
		return ModuleState{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, have := s.readings[serial]
	fresh := have && time.Since(r.at) <= printerview.FreshnessWindow
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

// Display returns the Panda Breath panel for one printer, or nil when the
// printer has no configured device. The panel shows the link state, the
// device's own chamber reading while fresh, and the temperature trend once
// enough samples exist. A "Chamber warming" overlay badge appears only
// while the chamber is warming.
func (s *Store) Display(serial string) *module.Display {
	v, ok := s.snapshot(serial)
	if !ok {
		return nil
	}
	link := module.Row{Label: "Link"}
	switch v.Link {
	case LinkConnected:
		link.Value, link.Tone = "Connected", "calm"
	case LinkNoReadings:
		link.Value, link.Tone = "Connected · no readings", "warn"
	default:
		link.Value, link.Tone = "Offline", "warn"
	}
	d := &module.Display{Panel: &module.Panel{Title: "Panda Breath", Rows: []module.Row{link}}}
	if v.ChamberC == nil {
		return d
	}
	d.Panel.Rows = append(d.Panel.Rows, module.Row{Label: "Chamber", Value: fmt.Sprintf("%.0f °C", *v.ChamberC)})
	if v.Trend == "" {
		return d
	}
	row := module.Row{Label: "Trend", Value: "Steady"}
	switch v.Trend {
	case TrendWarming:
		rate := *v.RateCPerMin
		row.Value, row.Tone = fmt.Sprintf("Warming · +%.1f °C/min", rate), "calm"
		d.Overlay = []module.Badge{{Text: "Chamber warming", Tone: "calm",
			Title: fmt.Sprintf("Panda Breath: chamber rising %.1f °C per minute", rate)}}
	case TrendCooling:
		row.Value = fmt.Sprintf("Cooling · −%.1f °C/min", -*v.RateCPerMin)
	}
	d.Panel.Rows = append(d.Panel.Rows, row)
	return d
}

// configured reports whether serial has a Panda Breath address. targets is
// fixed at construction, so no lock is needed.
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

// keepSample drops samples older than trendWindow relative to r, then adds
// r when the newest kept sample is at least sampleEvery old. Trimming on
// every reading keeps the trend baseline inside the window.
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
