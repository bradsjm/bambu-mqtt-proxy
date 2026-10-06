package detection

import (
	"net/http"

	"bambu-mqtt-proxy/internal/module"
)

// Module declares the detection engine's lifecycle and report interest.
// Call after SetBlocked and before Start.
func (e *Engine) Module() module.Module {
	m := module.Module{
		Name:         "detection",
		Start:        e.Start,
		Stop:         e.Close,
		NeedsReports: e.blocked == "",
		TileValue:    e.DetectionStatus,
		FleetValues:  e.fleetValues,
		StatusValue:  func() any { return e.DetectionMap() },
	}
	if e.blocked == "" {
		m.Routes = []module.Route{{
			Pattern: "PUT /detection/{serial}",
			Handler: http.HandlerFunc(e.handlePut),
		}}
	}
	return m
}

// fleetValues returns account suspension members only while suspension is active.
func (e *Engine) fleetValues() map[string]any {
	suspended, message := e.AccountSuspended()
	if !suspended {
		return nil
	}
	values := map[string]any{"detection_suspended": true}
	if message != "" {
		values["detection_message"] = message
	}
	return values
}
