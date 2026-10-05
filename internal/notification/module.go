package notification

import "bambu-mqtt-proxy/internal/module"

// Module declares the notification service's lifecycle and activity observer.
func (s *Service) Module() module.Module {
	return module.Module{
		Name:            "notification",
		Start:           s.Start,
		Stop:            s.Close,
		NeedsReports:    true,
		ObserveActivity: s.Observe,
	}
}
