package detection

import (
	"bambu-mqtt-proxy/internal/module"
	"net/http"
)

// Module declares detection lifecycle, state, and command routes.
func (e *Engine) Module() module.Module {
	m := module.Module{Name: "detection", Run: e.Run, State: e.DetectionStatus, StatusValue: func() any { return e.DetectionMap() }}
	if e.blocked == "" {
		m.MCP = e.registerMCP
		m.Routes = []module.Route{{Pattern: "PUT /detection/{serial}", Handler: http.HandlerFunc(e.handlePut)}}
	}
	return m
}
