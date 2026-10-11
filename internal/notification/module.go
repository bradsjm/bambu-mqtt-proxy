package notification

import "bambu-mqtt-proxy/internal/module"

// Module declares the notification worker lifecycle.
func (s *Service) Module() module.Module {
	return module.Module{Name: "notification", Run: s.Run}
}
