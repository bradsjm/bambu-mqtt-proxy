// Package mcpserver exposes a small Model Context Protocol (MCP) endpoint on
// the shared HTTP listener. It serves four read-only observation tools, a
// per-printer state resource with subscriptions, and the allow-listed
// printer control tools (pause, resume, stop, chamber light, speed profile,
// AI monitoring). There are no gcode, heater, or temperature commands, no
// arbitrary URLs, and no credential or raw payload output. The endpoint speaks protocol 2026-07-28 (SEP-2575) over
// Streamable HTTP in stateless mode, which is the only mode the official Go
// SDK supports for that protocol.
package mcpserver

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/telemetry"
)

// Operational bounds. They are deliberately fixed: the endpoint is an
// unauthenticated read surface on a LAN proxy, so every wait, body, list, and
// fan-out must have a ceiling that does not depend on operator tuning.
const (
	// defaultListLimit and maxListLimit bound list_printers paging.
	defaultListLimit = 20
	maxListLimit     = 100
	// protocolVersionCurrent is the only protocol version the endpoint
	// speaks: 2026-07-28 (SEP-2575), which carries subscriptions over
	// subscriptions/listen streams instead of resources/subscribe.
	protocolVersionCurrent = "2026-07-28"
	// defaultWatchSeconds and maxWatchSeconds bound one watch_printer park.
	defaultWatchSeconds = 25
	maxWatchSeconds     = 30
	// maxWaits caps watch_printer long-park handlers process-wide.
	maxWaits = 32
	// maxSubscriptions caps active resource subscriptions process-wide.
	maxSubscriptions = 32
	// snapshotWaitBudget bounds the wait for an acceptable camera frame.
	snapshotWaitBudget = 15 * time.Second
	// snapshotSlotWait bounds how long a call waits for a per-printer
	// snapshot slot before reporting the printer busy.
	snapshotSlotWait = 2 * time.Second
	// snapshotWaitersPerPrinter caps concurrent snapshot waits per serial so
	// one printer cannot accumulate unbounded goroutines.
	snapshotWaitersPerPrinter = 8
	// samplerInterval paces the single shared sampler.
	samplerInterval = time.Second
	// bodyLimit caps the JSON-RPC request body read from any client.
	bodyLimit = 64 << 10
)

// StateSource is the narrow read-only telemetry projection the server reads.
// It is satisfied by *telemetry.Cache.
type StateSource interface {
	State(serial string) (telemetry.State, bool)
	Session(serial string) (telemetry.SessionView, bool)
	Snapshot() []telemetry.State
}

// StatusSource reports per-printer upstream MQTT connectivity. It is
// satisfied by *upstream.Pool.
type StatusSource interface {
	Status() map[string]bool
}

// GenerationSource reports the current upstream connection generation per
// serial, which turns cached real-report evidence stale after a reconnect.
// It is satisfied by *upstream.Pool.
type GenerationSource interface {
	Generation(serial string) uint64
}

// SnapshotSource is the camera read surface the server uses. It mirrors the
// *camera.Manager read methods exactly: Acquire/Release balance on every
// path through the snapshot tool, and Latest is side-effect free (it never
// creates a capture).
type SnapshotSource interface {
	Acquire(serial string) (chan struct{}, camera.Status)
	Release(serial string)
	Latest(serial string) *camera.Frame
	Wait(serial string, ctx context.Context, after uint64, timeout time.Duration) *camera.Frame
}

// DetectionSource is the optional detection read surface. It is satisfied by
// *detection.Engine; nil means the feature is off and is projected as an
// explicit disabled state.
type DetectionSource interface {
	DetectionStatus(serial string) any
	AccountSuspended() (bool, string)
}

// ActivitySource is the recent printer event log. It is satisfied by
// *activity.Log.
type ActivitySource interface {
	Recent(serial string) []activity.Entry
}

// printerInfo is the narrow inventory record the endpoint is allowed to
// know: identity and display fields only. New copies these out of the
// configured printers, so credentials, addresses, and TLS settings from
// config.Printer never reach this package's state.
type printerInfo struct {
	serial string
	name   string
	model  string
}

// Deps wires the read dependencies. Printers defines the serial universe;
// New strips it to printerInfo, so the server never learns credentials,
// addresses, or endpoints. Cameras is a *camera.Manager when the camera
// feature is on; Detector is a *detection.Engine when detection is on. Both
// must stay nil when their feature is off.
type Deps struct {
	Printers     []config.Printer
	State        StateSource
	Connectivity StatusSource
	Generations  GenerationSource
	Cameras      SnapshotSource // nil when the camera feature is disabled
	Detector     DetectionSource
	Activity     ActivitySource // nil yields empty activity lists
	// Control enables the printer control tools; nil registers none.
	Control ControlService
	// DetectorControl backs set_ai_monitoring; nil when detection is off.
	DetectorControl DetectorControl
	Log             *slog.Logger
}

// Server owns the MCP endpoint. Construct with New, register the handler on
// the shared mux with Register, and stop background work with Close.
type Server struct {
	printers map[string]printerInfo
	serials  []string // sorted; the fixed list universe
	state    StateSource
	conn     StatusSource
	gens     GenerationSource
	cams     SnapshotSource
	det      DetectionSource
	activity ActivitySource
	control  ControlService
	detCtl   DetectorControl
	log      *slog.Logger
	now      func() time.Time

	srv     *mcp.Server
	handler *mcp.StreamableHTTPHandler
	sampler *sampler
	// epoch is drawn once per process and rides in every revision token,
	// so tokens from a previous process never match and always resync.
	epoch      uint32
	rootCtx    context.Context
	rootCancel context.CancelFunc
	closeMu    sync.Mutex
	closed     bool

	// waits is the process-wide long-park semaphore for watch_printer.
	waits chan struct{}

	snapMu    sync.Mutex
	snapSlots map[string]chan struct{} // per-printer snapshot semaphores

	subMu       sync.Mutex
	subs        int
	subsByState map[string]int // exact state URIs with active subscribers
}

// New builds the server and starts the shared sampler goroutine. Deps.State,
// Deps.Connectivity, and Deps.Log are required; Cameras and Detector stay
// nil when their feature is disabled.
func New(deps Deps) *Server {
	log := deps.Log
	if log == nil {
		log = slog.Default()
	}
	serials := make([]string, 0, len(deps.Printers))
	printers := make(map[string]printerInfo, len(deps.Printers))
	for _, p := range deps.Printers {
		printers[p.Serial] = printerInfo{serial: p.Serial, name: p.Name, model: p.Model}
		serials = append(serials, p.Serial)
	}
	sort.Strings(serials)
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		printers:    printers,
		serials:     serials,
		state:       deps.State,
		conn:        deps.Connectivity,
		gens:        deps.Generations,
		cams:        deps.Cameras,
		det:         deps.Detector,
		activity:    deps.Activity,
		control:     deps.Control,
		detCtl:      deps.DetectorControl,
		log:         log,
		epoch:       newEpoch(),
		rootCtx:     ctx,
		rootCancel:  cancel,
		now:         time.Now,
		waits:       make(chan struct{}, maxWaits),
		snapSlots:   make(map[string]chan struct{}),
		subsByState: make(map[string]int),
	}
	s.srv = mcp.NewServer(&mcp.Implementation{Name: "bambu-mqtt-proxy", Version: "1.0.0"},
		&mcp.ServerOptions{
			Capabilities: &mcp.ServerCapabilities{
				// Logging is not offered: the endpoint reports state through
				// tools and the resource only.
				Logging:   nil,
				Tools:     &mcp.ToolCapabilities{ListChanged: false},
				Resources: &mcp.ResourceCapabilities{ListChanged: false, Subscribe: true},
			},
			SubscribeHandler:   s.handleSubscribe,
			UnsubscribeHandler: s.handleUnsubscribe,
			// Serve the current protocol only. Legacy resources/subscribe
			// must never reach the subscription ceiling: stateless mode runs
			// every POST on an ephemeral session, and the SDK releases such
			// sessions without invoking UnsubscribeHandler, so a legacy
			// subscribe would permanently consume one of the process-wide
			// slots. The restriction makes the SDK reject every legacy request
			// that carries the protocol header before any handler runs;
			// handleSubscribe refuses the header-less remainder.
			SupportedProtocolVersions: []string{protocolVersionCurrent},
		})
	s.registerTools()
	if s.control != nil {
		s.registerControlTools()
	}
	s.registerResource()
	s.sampler = newSampler(s)
	s.handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s.srv },
		&mcp.StreamableHTTPOptions{
			// Stateless is required for protocol 2026-07-28 and its
			// subscriptions/listen streams; stateful servers reject that
			// protocol version outright.
			Stateless: true,
			Logger:    log,
			// Bounded bodies: the endpoint accepts small JSON-RPC documents
			// only, never uploads.
			MaxRequestBodyBytes: bodyLimit,
			// Tie in-flight handler contexts to the HTTP request so client
			// disconnects release watch parks and camera waits immediately.
			PropagateRequestCancellation: true,
		})
	return s
}

// newEpoch draws a nonzero random process epoch; a crypto failure falls
// back to the clock, which is still unique across restarts for resync
// purposes.
func newEpoch() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err == nil {
		if e := binary.LittleEndian.Uint32(b[:]); e != 0 {
			return e
		}
	}
	e := uint32(time.Now().UnixNano())
	if e == 0 {
		e = 1
	}
	return e
}

// Start launches the shared sampler goroutine. Call once before serving;
// Close stops it.
func (s *Server) Start() {
	s.sampler.Start()
}

// Register mounts the endpoint on the shared mux at /mcp. The handler is
// wrapped with the SDK-recommended cross-origin protection middleware; the
// streamable handler's own localhost protection (DNS-rebinding host checks)
// stays enabled. Non-browser MCP clients send no Origin header and pass.
func (s *Server) Register(mux *http.ServeMux) {
	protection := http.NewCrossOriginProtection()
	// All methods reach the wrapper: the streamable handler answers
	// GET/DELETE with 405 in stateless mode, which is the spec-correct
	// response.
	mux.Handle("/mcp", protection.Handler(http.HandlerFunc(s.serveMCP)))
}

// serveMCP runs one Streamable HTTP request under a context that is a child
// of the server root context, so Close unblocks every in-flight watch park
// and listen stream immediately, before the process tears down the camera
// manager and the upstream pool.
func (s *Server) serveMCP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.rootCtx, cancel)
	defer stop()
	s.handler.ServeHTTP(w, r.WithContext(ctx))
}

// Close cancels the root context, which unwinds every in-flight request and
// listen stream, then stops the sampler and its notification dispatcher.
// Call it before the camera manager and the upstream pool shut down.
func (s *Server) Close() {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.rootCancel()
	if s.sampler != nil {
		s.sampler.stop()
	}
}

// registerTools adds the four observation tools. Each carries read-only,
// non-destructive, idempotent annotations and a hand-written input schema so
// enums, ranges, and defaults are exact on the wire.
func (s *Server) registerTools() {
	readOnly := func(title string) *mcp.ToolAnnotations {
		no := false
		return &mcp.ToolAnnotations{
			Title:           title,
			ReadOnlyHint:    true,
			DestructiveHint: &no,
			IdempotentHint:  true,
			OpenWorldHint:   &no,
		}
	}

	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "list_printers",
		Description: "List configured printers with a one-line status each. Read-only; results page by serial.",
		Annotations: readOnly("List printers"),
		InputSchema: objSchema(map[string]*jsonschema.Schema{
			"limit":  intProp("Maximum printers per page.", defaultListLimit, 1, maxListLimit),
			"cursor": strProp("Opaque cursor from a previous page's next_cursor."),
		}),
	}, s.toolListPrinters)

	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "get_printer_state",
		Description: "Read the typed live projection for one printer: print state, temperatures, job, freshness, camera and detection health. Read-only.",
		Annotations: readOnly("Get printer state"),
		InputSchema: objSchema(map[string]*jsonschema.Schema{
			"serial": strRequiredProp("Printer serial number."),
		}, "serial"),
	}, s.toolGetPrinterState)

	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "get_camera_snapshot",
		Description: "Capture one chamber camera JPEG for a P1/A1-series printer. Returns image content plus capture metadata; never returns a frame older than max_age_seconds. Read-only.",
		Annotations: readOnly("Get camera snapshot"),
		InputSchema: objSchema(map[string]*jsonschema.Schema{
			"serial": strRequiredProp("Printer serial number."),
			"max_age_seconds": intProp(
				"Maximum acceptable age of the returned frame in seconds.",
				5, 0, 60),
		}, "serial"),
	}, s.toolGetCameraSnapshot)

	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "watch_printer",
		Description: "Long-poll one printer for state changes. mode=attention fires on pause/fail/finish/stop/job changes, new HMS alerts, printer errors, and connectivity/freshness/detection-health changes; mode=progress additionally fires every 5 percentage points of print progress. Age and countdown churn is suppressed. Read-only.",
		Annotations: readOnly("Watch printer"),
		InputSchema: objSchema(map[string]*jsonschema.Schema{
			"serial": strRequiredProp("Printer serial number."),
			"after_revision": strProp(
				"Revision token from a previous read or watch. Unknown or expired tokens return a full snapshot with resync_required instead of waiting."),
			"timeout_seconds": intProp(
				"How long to wait for a change before returning unchanged.",
				defaultWatchSeconds, 0, maxWatchSeconds),
			"mode": enumProp("Which changes wake the poll.", []any{"attention", "progress"}, "attention"),
		}, "serial"),
	}, s.toolWatchPrinter)
}

// registerResource adds bambu://printers/{serial}/state as a template backed
// by the same typed projection the tools serve.
func (s *Server) registerResource() {
	s.srv.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: "bambu://printers/{serial}/state",
		Name:        "printer-state",
		Title:       "Printer state",
		Description: "Typed live projection of one printer; identical structure to get_printer_state's state output.",
		MIMEType:    "application/json",
	}, s.readStateResource)
}

// objSchema builds an object input schema with additionalProperties denied.
func objSchema(props map[string]*jsonschema.Schema, required ...string) *jsonschema.Schema {
	return &jsonschema.Schema{
		Type:                 "object",
		Properties:           props,
		Required:             required,
		AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}},
	}
}

// strProp declares an optional string property.
func strProp(description string) *jsonschema.Schema {
	return &jsonschema.Schema{Type: "string", Description: description}
}

// strRequiredProp declares a non-empty string property.
func strRequiredProp(description string) *jsonschema.Schema {
	minLen := 1
	return &jsonschema.Schema{Type: "string", Description: description, MinLength: &minLen}
}

// intProp declares an integer property with a schema default so clients that
// omit the field get the documented value; the SDK applies defaults and
// validates bounds before the handler runs.
func intProp(description string, def, min, max int) *jsonschema.Schema {
	mn, mx := float64(min), float64(max)
	return &jsonschema.Schema{
		Type:        "integer",
		Description: description,
		Default:     mustJSON(def),
		Minimum:     &mn,
		Maximum:     &mx,
	}
}

// enumProp declares a string enum property with a default.
func enumProp(description string, values []any, def string) *jsonschema.Schema {
	return &jsonschema.Schema{
		Type:        "string",
		Description: description,
		Enum:        values,
		Default:     mustJSON(def),
	}
}

// mustJSON marshals a schema default or panics; all defaults are literals.
func mustJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("mcpserver: bad schema default: %v", err))
	}
	return raw
}

// connStatus reads upstream connectivity; a missing serial maps to false.
func (s *Server) connStatus(serial string) bool {
	if s.conn == nil {
		return false
	}
	return s.conn.Status()[serial]
}

// generation reads the current upstream connection generation; 0 when the
// pool has not connected the printer (or in tests without a source).
func (s *Server) generation(serial string) uint64 {
	if s.gens == nil {
		return 0
	}
	return s.gens.Generation(serial)
}

// cameraState names the explicit camera feature state for one printer.
func (s *Server) cameraState(serial string) string {
	if s.cams == nil {
		return cameraDisabled
	}
	info, known := s.printers[serial]
	if !known || !config.CameraEligible(info.model, serial) {
		return cameraUnsupported
	}
	// Latest is side-effect free: a frame on file is the one observable
	// signal that the chamber stream has delivered image material.
	if s.cams.Latest(serial) != nil {
		return cameraReady
	}
	return cameraOffline
}

// detectionView projects the optional detection worker.
func (s *Server) detectionView(serial string) DetectionView {
	if s.det == nil {
		return DetectionView{State: "disabled", PauseState: detection.PauseNone}
	}
	suspended, reason := s.det.AccountSuspended()
	view := DetectionView{Suspended: suspended, SuspendedReason: reason, PauseState: detection.PauseNone}
	if st, ok := s.det.DetectionStatus(serial).(*detection.Status); ok && st != nil {
		view.State = st.State
		view.Reason = st.Reason
		view.Enabled = st.Enabled
		view.DisabledUntil = st.DisabledUntil
		view.SessionID = st.SessionID
		view.PauseState = st.PauseState
		if st.Quality != 0 {
			q := st.Quality
			view.Quality = &q
		}
	}
	return view
}

// snapshotSlots returns the per-printer semaphore, creating it on first use.
func (s *Server) snapshotSlot(serial string) chan struct{} {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	ch, ok := s.snapSlots[serial]
	if !ok {
		ch = make(chan struct{}, snapshotWaitersPerPrinter)
		s.snapSlots[serial] = ch
	}
	return ch
}

// handleSubscribe validates and counts a resource subscription. The SDK
// tracks the registry itself; these callbacks are the enforcement point for
// the current-protocol rule, the subscription ceiling, and the known-serial
// rule.
func (s *Server) handleSubscribe(_ context.Context, req *mcp.SubscribeRequest) error {
	// Current protocol only. The SDK keeps routing resources/subscribe for
	// old-protocol sessions, and a subscribe on a stateless ephemeral
	// session leaks its count: the SDK releases that session without
	// invoking UnsubscribeHandler. The transport header gate rejects legacy
	// requests before any handler; this refuses the header-less remainder,
	// whose session state never names the current protocol, plus hybrid
	// sessions whose initialize state is nil because the request carried a
	// current protocol header but no client _meta triple. A nil Session
	// only occurs when unit tests call the handler directly.
	if req.Session != nil {
		ip := req.Session.InitializeParams()
		if ip == nil || ip.ProtocolVersion != protocolVersionCurrent {
			return &jsonrpc.Error{Code: mcp.CodeUnsupportedProtocolVersion,
				Message: fmt.Sprintf("resource subscriptions require protocol %s", protocolVersionCurrent)}
		}
	}
	serial, ok := serialFromStateURI(req.Params.URI)
	if !ok {
		return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams,
			Message: "unknown resource URI; subscribe to bambu://printers/{serial}/state"}
	}
	if _, known := s.printers[serial]; !known {
		return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams,
			Message: errUnknownSerial + ": " + serial}
	}
	s.subMu.Lock()
	defer s.subMu.Unlock()
	if s.subs >= maxSubscriptions {
		return &jsonrpc.Error{Code: jsonrpc.CodeInvalidRequest,
			Message: fmt.Sprintf("%s: at most %d subscriptions are served", errSubscriptionsLimit, maxSubscriptions)}
	}
	s.subs++
	s.subsByState[req.Params.URI]++
	return nil
}

// handleUnsubscribe releases one subscription count. The SDK invokes it on
// explicit unsubscribe and when a listen stream unwinds.
func (s *Server) handleUnsubscribe(_ context.Context, req *mcp.UnsubscribeRequest) error {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	if n := s.subsByState[req.Params.URI]; n > 0 {
		s.subsByState[req.Params.URI] = n - 1
		s.subs--
		if n == 1 {
			delete(s.subsByState, req.Params.URI)
		}
	}
	return nil
}

// token renders the revision as the process-scoped opaque token.
func (s *Server) token(r revision) string {
	return encodeRevision(s.epoch, r)
}

// subscribed reports whether any resource subscriber watches the serial.
func (s *Server) subscribed(serial string) bool {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	return s.subsByState[stateURI(serial)] > 0
}

// readStateResource serves the typed projection as JSON for
// bambu://printers/{serial}/state.
func (s *Server) readStateResource(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	serial, ok := serialFromStateURI(req.Params.URI)
	if !ok {
		return nil, fmt.Errorf("unknown resource URI %q", req.Params.URI)
	}
	// Revision first, state second: see summarize in tools.go. A token
	// captured after the read could outdate the state it accompanies and
	// hide a concurrent change from the client's next watch.
	rev := s.sampler.revision(serial)
	state, known := s.buildPrinterState(serial, s.now())
	if !known {
		return nil, fmt.Errorf("%s: %s", errUnknownSerial, serial)
	}
	payload := struct {
		Serial   string       `json:"serial"`
		Revision string       `json:"revision"`
		State    PrinterState `json:"state"`
	}{Serial: serial, Revision: s.token(rev), State: state}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode state: %w", err)
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      req.Params.URI,
			MIMEType: "application/json",
			Text:     string(raw),
		}},
	}, nil
}

// toolErrorResult renders an operational outcome as a typed, error-flagged
// tool result. The structured payload carries the stable code; the short
// text echoes it for clients that read content only.
func toolErrorResult(code, message string) (*mcp.CallToolResult, any) {
	out := struct {
		Error ToolError `json:"error"`
	}{Error: ToolError{Code: code, Message: message}}
	return &mcp.CallToolResult{IsError: true}, out
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
