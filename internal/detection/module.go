package detection

import (
	"bambu-mqtt-proxy/internal/module"
	"net/http"
)

// Module declares detection lifecycle, state, and command routes.
func (e *Engine) Module() module.Module {
	m := module.Module{Name: "detection", Run: e.Run, State: e.DetectionStatus, Display: e.Display, StatusValue: func() any { return e.DetectionMap() }}
	if e.blocked == "" {
		m.MCP = e.registerMCP
		m.Routes = []module.Route{{Pattern: "PUT /detection/{serial}", Handler: http.HandlerFunc(e.handlePut)}}
	}
	return m
}

// Display contributes an operator warning when detection needs attention.
func (e *Engine) Display(serial string) *module.Display {
	st, ok := e.DetectionStatus(serial).(*Status)
	if !ok || st.Message == "" {
		return nil
	}
	return &module.Display{Panel: &module.Panel{Title: "AI inspection", Rows: []module.Row{{Label: st.State, Value: st.Message}}}}
}
