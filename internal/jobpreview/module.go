package jobpreview

import (
	"net/http"

	"bambu-mqtt-proxy/internal/module"
)

// ModuleState is the per-printer module value for the camera wall tile
// member and the MCP printer state member under the module name
// "jobpreview". It carries the shared archived preview of the current
// print and its accepted archive facts; only display state: never
// serial-derived credentials or raw configuration.
type ModuleState struct {
	JobPreview  View      `json:"job_preview"`
	JobMetadata *Metadata `json:"job_metadata"`
}

// moduleState returns one wall and MCP state value for a printer: a cache-only
// lookup, falling back to the shared disabled projection when the printer
// has no accepted preview. It never performs network work, so it stays
// safe to call while the service is stopped.
func (s *Service) moduleState(serial string) any {
	res, ok := s.Lookup(serial, false)
	if !ok {
		res = Disabled()
	}
	return ModuleState{JobPreview: res.Preview, JobMetadata: res.Metadata}
}

// Module declares the job preview service's module hooks: the per-printer
// state for the camera wall and MCP, the versioned preview image route, and
// the get_job_preview tool.
func (s *Service) Module() module.Module {
	return module.Module{
		Name:  "jobpreview",
		Start: s.Start,
		Stop:  s.Close,
		State: s.moduleState,
		Routes: []module.Route{{
			Pattern: previewRoute,
			Handler: http.HandlerFunc(s.handlePreview),
		}},
		MCP: s.registerMCP,
	}
}
