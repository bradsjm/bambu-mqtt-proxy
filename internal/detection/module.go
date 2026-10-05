package detection

import "bambu-mqtt-proxy/internal/module"

// Module declares the detection engine's lifecycle and report interest.
// Call after SetBlocked and before Start.
func (e *Engine) Module() module.Module {
	return module.Module{
		Name:         "detection",
		Start:        e.Start,
		Stop:         e.Close,
		NeedsReports: e.blocked == "",
	}
}
