package jobpreview

import "bambu-mqtt-proxy/internal/module"

// Module declares the job preview service's lifecycle.
func (s *Service) Module() module.Module {
	return module.Module{
		Name:  "jobpreview",
		Start: s.Start,
		Stop:  s.Close,
	}
}
