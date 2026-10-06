// Package module defines how optional, non-core capabilities plug into the
// proxy. A module is a package that owns one capability (AI failure
// detection, an accessory sensor, notifications, job previews) and declares
// its hooks with one Module value. The service wiring constructs each
// enabled module with the core services it needs, then hands the
// declarations to the core, which owns lifecycle order, the shared report
// interest, activity fan-out, display placement, and the members and
// routes a module serves under its own name.
//
// Core services a module may take at construction, by narrow interface:
//   - merged printer state and print sessions (*telemetry.Cache),
//   - camera frames (*camera.Manager; nil while cameras are disabled),
//   - user-style printer commands through the allow-listed control service
//     (*control.Service), or generation-guarded commands
//     (*upstream.Pool) for autonomous actions taken on observed evidence,
//   - the recent activity log (*activity.Log) for recording events.
//
// Modules never publish raw MQTT and never change forwarded payloads.
package module

import (
	"time"

	"bambu-mqtt-proxy/internal/activity"
)

// Module declares one module's hooks. Name is required; every other field
// is optional and nil or false means the module does not use that hook.
type Module struct {
	// Name is the stable module key, also used as the key of the module's
	// entry in the camera wall tile "modules" object.
	Name string
	// Start launches the module's background work. The core calls it once,
	// after the MQTT broker serves and the shutdown path is installed.
	Start func()
	// Stop ends the module's background work and waits for it. The core
	// calls it once during shutdown, after the HTTP and MCP consumers stop
	// and before the cameras and upstream connections the module reads.
	// Read hooks below must stay safe to call after Stop.
	Stop func()
	// NeedsReports asks the core to hold one report interest per printer
	// while the proxy runs, so telemetry stays live without MQTT clients.
	NeedsReports bool
	// ObserveActivity receives every newly recorded activity entry. It runs
	// on report processing paths and must not block.
	ObserveActivity func(serial string, e activity.Entry)
	// ChamberReading supplies an accessory chamber temperature for printers
	// whose model has no usable chamber sensor. The core uses the first
	// module that sets it and applies the shared freshness window.
	ChamberReading func(serial string) (temp float64, at time.Time, ok bool)
	// Display returns the module's contribution to one printer's camera
	// wall tile, or nil for nothing. It must be cheap and non-blocking.
	Display func(serial string) *Display
	// TileValue returns the module's own value for one printer's camera wall
	// tile. The core serves it as the tile member named Name and omits the
	// member for nil. A value with a StableKey() any method is compared
	// through that method when the core decides whether to send an events
	// update, so continuously aging fields may stay in the value. It must be
	// safe for concurrent use and must not block.
	TileValue func(serial string) any
	// FleetValues returns top-level members of the /camera/status and
	// /camera/events body. Every key equals Name or starts with Name+"_".
	// nil or empty adds nothing.
	FleetValues func() map[string]any
	// StatusValue returns the module's /status member, served under Name.
	StatusValue func() any
	// Routes are HTTP handlers on the shared listener, mounted only when the
	// listener runs. Mount protects every non-GET/HEAD route against
	// cross-origin browser writes.
	Routes []Route
}

// Display is one module's contribution to one printer tile. Values must be
// stable between real changes: the camera wall sends an update whenever the
// serialized display changes, so continuously aging values (ages,
// countdowns) must be rounded to a coarse step or omitted.
type Display struct {
	// Overlay holds short badges drawn over the camera image.
	Overlay []Badge `json:"overlay,omitempty"`
	// Panel is an optional titled section in the tile details.
	Panel *Panel `json:"panel,omitempty"`
}

// Badge is one short glass pill drawn over the camera image.
type Badge struct {
	// Text is the visible pill text; keep it to a few words.
	Text string `json:"text"`
	// Tone colors the pill: "calm", "info", "warn", "error", or empty for
	// neutral.
	Tone string `json:"tone,omitempty"`
	// Title is the tooltip and accessible description.
	Title string `json:"title,omitempty"`
}

// Panel is one titled section of label and value rows in the tile details.
type Panel struct {
	// Title names the section.
	Title string `json:"title"`
	// Level is the lowest tile detail level that shows the panel: 1 compact,
	// 2 vitals, 3 full. Zero means 3.
	Level int `json:"level,omitempty"`
	// Rows are shown in order.
	Rows []Row `json:"rows"`
}

// Row is one label and value pair in a panel.
type Row struct {
	// Label names the value.
	Label string `json:"label"`
	// Value is the display-ready value, including its unit.
	Value string `json:"value"`
	// Tone colors the value with the Badge tone vocabulary.
	Tone string `json:"tone,omitempty"`
}
