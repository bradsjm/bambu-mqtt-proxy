// Package module defines optional capabilities and their core-owned lifecycle.
package module

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Module declares one optional capability's hooks.
type Module struct {
	// Name is the stable key used by the wall, status, and MCP.
	Name string
	// Run performs background work until cancellation and returns after cleanup.
	Run func(ctx context.Context) error
	// State returns non-blocking per-printer state.
	State func(serial string) any
	// Display returns non-blocking camera wall badges and panels.
	Display func(serial string) *Display
	// StatusValue returns the module's status member.
	StatusValue func() any
	// Routes contains handlers mounted on the shared listener.
	Routes []Route
	// MCP registers tools with unique names on the shared server.
	MCP func(s *mcp.Server)
}

// Display is one module's contribution to one printer tile. The wall polls
// /camera/status, so values may change on every poll.
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
	// Section is a subheading rendered when it differs from the previous row's.
	Section string `json:"section,omitempty"`
	// Note is a muted line under the value.
	Note string `json:"note,omitempty"`
}
