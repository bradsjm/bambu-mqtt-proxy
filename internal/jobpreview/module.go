package jobpreview

import (
	"bambu-mqtt-proxy/internal/module"
	"net/http"
)

// ModuleState carries the wall and MCP archived job members.
type ModuleState struct {
	// JobPreview contains thumbnail retrieval state.
	JobPreview View `json:"job_preview"`
	// JobMetadata contains sliced job facts.
	JobMetadata *Metadata `json:"job_metadata"`
}

// moduleState returns cache-only wall state.
func (s *Service) moduleState(serial string) any {
	result, ok := s.Lookup(serial, false)
	if !ok {
		result = Disabled()
	}
	return ModuleState{result.Preview, result.Metadata}
}

// Display returns the retrieval status panel when details are unavailable.
func (s *Service) Display(serial string) *module.Display {
	result, ok := s.Lookup(serial, false)
	if !ok || result.Preview.Status == StatusReady || result.Preview.Status == StatusDisabled {
		return nil
	}
	// Without an active print there is nothing to retrieve, so only pending shows.
	if result.Preview.Status != StatusPending {
		if d, _, _ := s.state.Get(serial); !active(d) {
			return nil
		}
	}
	value, tone := "Print details unavailable", "warn"
	if result.Preview.Status == StatusPending {
		value, tone = "Downloading print details", "info"
	}
	return &module.Display{Panel: &module.Panel{Title: "Print details", Level: 3, Rows: []module.Row{{Label: "Status", Value: value, Tone: tone}}}}
}

// Module returns preview lifecycle, wall, HTTP, and MCP hooks.
func (s *Service) Module() module.Module {
	return module.Module{Name: "jobpreview", Run: s.Run, State: s.moduleState, Display: s.Display,
		Routes: []module.Route{{Pattern: previewRoute, Handler: http.HandlerFunc(s.handlePreview)}}, MCP: s.registerMCP}
}
