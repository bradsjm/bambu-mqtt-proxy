// Package mcpserver exposes printer observation and control tools through MCP.
package mcpserver

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/printer"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Fixed bounds limit request bodies, camera waits, and activity waits.
const (
	defaultWatchSeconds       = 25
	maxWatchSeconds           = 30
	snapshotWaitBudget        = 15 * time.Second
	snapshotSlotWait          = 2 * time.Second
	snapshotWaitersPerPrinter = 8
	bodyLimit                 = 64 << 10
)

// StateSource provides the merged printer views.
type StateSource interface {
	View(string) (printer.View, bool)
	Views() []printer.View
}

// SnapshotSource provides bounded chamber camera access.
type SnapshotSource interface {
	Acquire(string) (chan struct{}, camera.Status)
	Release(string)
	Latest(string) *camera.Frame
	Wait(string, context.Context, uint64, time.Duration) *camera.Frame
}

// ActivitySource provides recent events and a cancellable wait for new entries.
type ActivitySource interface {
	Recent(string) []activity.Entry
	Wait(context.Context, string, uint64) []activity.Entry
}

// printerInfo contains only the identity needed for camera eligibility.
type printerInfo struct {
	// model is the configured printer model.
	model string
}

// Deps supplies the printer store and optional feature services.
type Deps struct {
	// Printers defines the configured serials.
	Printers []config.Printer
	// State reads the merged printer store.
	State StateSource
	// Cameras provides snapshots when enabled.
	Cameras SnapshotSource
	// Modules supplies state hooks and extra tools.
	Modules []module.Module
	// Activity supplies the event log.
	Activity ActivitySource
	// Control sends printer commands when enabled.
	Control ControlService
	// Log receives HTTP transport diagnostics.
	Log *slog.Logger
}

// Server owns the MCP tools and HTTP endpoint.
type Server struct {
	// printers defines known serials and camera models.
	printers map[string]printerInfo
	// state supplies merged printer views.
	state StateSource
	// cams supplies camera frames.
	cams SnapshotSource
	// modules supplies optional state and tool hooks.
	modules []module.Module
	// activity supplies event history.
	activity ActivitySource
	// control sends allow-listed commands.
	control ControlService
	// now provides the camera age clock.
	now func() time.Time
	// srv registers tools and validates their outputs.
	srv *mcp.Server
	// handler serves Streamable HTTP.
	handler *mcp.StreamableHTTPHandler
	// rootCtx ends in-flight requests at shutdown.
	rootCtx context.Context
	// rootCancel cancels all in-flight requests.
	rootCancel context.CancelFunc
	// snapMu guards the snapshot slots.
	snapMu sync.Mutex
	// snapSlots bounds camera waits per printer.
	snapSlots map[string]chan struct{}
}

// New registers core tools and the running modules' tools.
func New(deps Deps) *Server {
	log := deps.Log
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{printers: make(map[string]printerInfo), state: deps.State, cams: deps.Cameras,
		modules: append([]module.Module(nil), deps.Modules...), activity: deps.Activity, control: deps.Control,
		now: time.Now, rootCtx: ctx, rootCancel: cancel, snapSlots: make(map[string]chan struct{})}
	for _, p := range deps.Printers {
		s.printers[p.Serial] = printerInfo{model: p.Model}
	}
	s.srv = mcp.NewServer(&mcp.Implementation{Name: "bambu-mqtt-proxy", Version: "1.0.0"}, nil)
	s.registerTools()
	if s.control != nil {
		s.registerControlTools()
	}
	for _, m := range s.modules {
		if m.MCP != nil {
			m.MCP(s.srv)
		}
	}
	s.handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s.srv },
		&mcp.StreamableHTTPOptions{Stateless: true, Logger: log, MaxRequestBodyBytes: bodyLimit, PropagateRequestCancellation: true})
	return s
}

// Register mounts the endpoint with cross-origin protection.
func (s *Server) Register(mux *http.ServeMux) {
	mux.Handle("/mcp", http.NewCrossOriginProtection().Handler(http.HandlerFunc(s.serveMCP)))
}

// serveMCP links request cancellation to server shutdown.
func (s *Server) serveMCP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.rootCtx, cancel)
	defer stop()
	s.handler.ServeHTTP(w, r.WithContext(ctx))
}

// Close cancels in-flight HTTP requests.
func (s *Server) Close() { s.rootCancel() }

// registerTools adds the four observation tools. Each carries read-only,
// non-destructive, idempotent annotations and a hand-written input schema so
// enums, ranges, and defaults are exact on the wire.
func (s *Server) registerTools() {

	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "list_printers",
		Description: "List configured printers with a one-line status each. Read-only.",
		Annotations: module.ReadOnly("List printers"),
		InputSchema: &jsonschema.Schema{Type: "object", AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}}},
	}, s.toolListPrinters)

	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "get_printer_state",
		Description: "Read the current printer view and running module states. Read-only.",
		Annotations: module.ReadOnly("Get printer state"),
		InputSchema: module.SerialInput(nil),
	}, s.toolGetPrinterState)

	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "get_camera_snapshot",
		Description: "Capture one chamber camera JPEG for a P1/A1-series printer. Returns image content plus capture metadata; never returns a frame older than max_age_seconds. Read-only.",
		Annotations: module.ReadOnly("Get camera snapshot"),
		InputSchema: module.SerialInput(map[string]*jsonschema.Schema{
			"max_age_seconds": {Type: "integer", Default: []byte("5"), Minimum: new(float64(0)), Maximum: new(float64(60))},
		}),
	}, s.toolGetCameraSnapshot)

	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "watch_printer",
		Description: "Wait for activity entries newer than after_id, then return them with the current printer state. Read-only.",
		Annotations: module.ReadOnly("Watch printer"),
		InputSchema: module.SerialInput(map[string]*jsonschema.Schema{
			"after_id":        {Type: "integer", Minimum: new(float64(0))},
			"timeout_seconds": &jsonschema.Schema{Type: "integer", Default: []byte("25"), Minimum: new(float64(0)), Maximum: new(float64(maxWatchSeconds))},
		}),
	}, s.toolWatchPrinter)
}

// cameraState describes camera availability without starting capture.
func (s *Server) cameraState(serial string) string {
	if s.cams == nil {
		return cameraDisabled
	}
	if !config.CameraEligible(s.printers[serial].model, serial) {
		return cameraUnsupported
	}
	if s.cams.Latest(serial) != nil {
		return cameraReady
	}
	return cameraOffline
}

// snapshotSlot returns the shared camera wait limit for one printer.
func (s *Server) snapshotSlot(serial string) chan struct{} {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	if s.snapSlots[serial] == nil {
		s.snapSlots[serial] = make(chan struct{}, snapshotWaitersPerPrinter)
	}
	return s.snapSlots[serial]
}

// shortText prepends a one-line text content to the tool result. Structured
// output remains the contract; the text is for chat-first clients.
func shortText(res *mcp.CallToolResult, text string) *mcp.CallToolResult {
	if res == nil {
		res = &mcp.CallToolResult{}
	}
	res.Content = append(res.Content, &mcp.TextContent{Text: text})
	return res
}
