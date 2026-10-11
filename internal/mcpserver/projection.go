package mcpserver

import (
	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/printer"
)

// Operational error codes describe camera failures.
const (
	errCameraDisabled    = "camera_disabled"
	errCameraUnsupported = "camera_unsupported"
	errCameraUnavailable = "camera_unavailable"
	errStaleImage        = "stale_image"
)

// PrinterState combines the printer view with optional feature state.
type PrinterState struct {
	printer.View
	// Camera reports the camera feature state.
	Camera string `json:"camera"`
	// Modules contains state from each running module.
	Modules map[string]any `json:"modules,omitempty"`
	// Activity contains recent events, newest first.
	Activity []activity.Entry `json:"activity"`
	// Controls contains actions available for display.
	Controls []string `json:"controls"`
}

// Camera feature states describe observable image availability.
const (
	cameraDisabled    = "disabled"
	cameraUnsupported = "unsupported"
	cameraOffline     = "offline"
	cameraReady       = "ready"
)

// buildPrinterState reads the store and each module state hook.
func (s *Server) buildPrinterState(serial string) (PrinterState, bool) {
	view, ok := s.state.View(serial)
	if !ok {
		return PrinterState{}, false
	}
	out := PrinterState{View: view, Camera: s.cameraState(serial), Activity: []activity.Entry{}, Controls: []string{}}
	for _, m := range s.modules {
		if m.State != nil {
			if value := m.State(serial); value != nil {
				if out.Modules == nil {
					out.Modules = make(map[string]any)
				}
				out.Modules[m.Name] = value
			}
		}
	}
	if s.control != nil {
		out.Controls = s.control.Available(serial)
	}
	if s.activity != nil {
		if recent := s.activity.Recent(serial); recent != nil {
			out.Activity = recent
		}
	}
	return out, true
}
