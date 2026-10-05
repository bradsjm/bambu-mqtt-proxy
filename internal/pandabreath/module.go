package pandabreath

import "bambu-mqtt-proxy/internal/module"

// Module declares the accessory store's lifecycle and chamber reading hook.
func (s *Store) Module() module.Module {
	return module.Module{
		Name:           "pandabreath",
		Start:          s.Start,
		Stop:           s.Stop,
		ChamberReading: s.ChamberReading,
	}
}
