package mcpserver

// Wire-level acceptance tests. They drive the endpoint exactly the way a
// current-protocol (2026-07-28, SEP-2575) Streamable HTTP client does: raw
// JSON-RPC over POST with SSE responses, tools/list discovery, tools/call,
// resources/read, and subscriptions/listen streams with their
// subscriptions/acknowledged and notifications/resources/updated messages.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/camera"
)

const (
	protocolHeader         = "MCP-Protocol-Version"
	protocolCurrent        = "2026-07-28"
	methodHeader           = "Mcp-Method"
	nameHeader             = "Mcp-Name"
	metaProtocolVersionKey = "io.modelcontextprotocol/protocolVersion"
	metaClientInfoKey      = "io.modelcontextprotocol/clientInfo"
	metaClientCapsKey      = "io.modelcontextprotocol/clientCapabilities"
	subscriptionMetaKey    = "io.modelcontextprotocol/subscriptionId"
	ackMethod              = "notifications/subscriptions/acknowledged"
	updatedMethod          = "notifications/resources/updated"
)

// clientMeta is the _meta triple protocol 2026-07-28 defines for calls
// (SEP-2575): the negotiated protocol version, the client implementation,
// and the client capabilities. Every call must carry all of it.
func clientMeta() map[string]any {
	return map[string]any{
		metaProtocolVersionKey: protocolCurrent,
		metaClientInfoKey:      map[string]any{"name": "acceptance", "version": "0"},
		metaClientCapsKey:      map[string]any{},
	}
}

// wireFixture is a fixture served over a real HTTP test server.
type wireFixture struct {
	*fixture
	ts *httptest.Server
}

func newWireFixture(t *testing.T) *wireFixture {
	t.Helper()
	f := newFixture(t)
	mux := http.NewServeMux()
	f.srv.Register(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(func() {
		f.srv.Close()
		ts.Close()
	})
	return &wireFixture{fixture: f, ts: ts}
}

// startDispatch runs the notification dispatcher without the sampling loop,
// so tests keep deterministic control over when samples are taken.
func (w *wireFixture) startDispatch() {
	go w.srv.sampler.dispatch()
}

// activeSubs reports the process-wide subscription count.
func (w *wireFixture) activeSubs() int {
	w.srv.subMu.Lock()
	defer w.srv.subMu.Unlock()
	return w.srv.subs
}

// sseMessage is one decoded JSON-RPC message from an SSE stream.
type sseMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// rpcRequest builds a JSON-RPC request body. Protocol 2026-07-28 requires
// every call to carry the client _meta triple, matching the
// MCP-Protocol-Version header, so it is injected here.
func rpcRequest(id int, method string, params any) string {
	meta := clientMeta()
	switch p := params.(type) {
	case nil:
		params = map[string]any{"_meta": clientMeta()}
	case map[string]any:
		// Unknown keys ride along; the standard triple fills the rest,
		// exactly how the SDK client merges caller metadata.
		if user, ok := p["_meta"].(map[string]any); ok {
			for k, v := range user {
				meta[k] = v
			}
		}
		withMeta := make(map[string]any, len(p)+1)
		for k, v := range p {
			withMeta[k] = v
		}
		withMeta["_meta"] = meta
		params = withMeta
	}
	raw, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": method, "params": params,
	})
	return string(raw)
}

// wireHeaders returns the transport headers protocol 2026-07-28 requires on
// a POST: Mcp-Method always names the method, and the methods that target a
// named artifact (tools/call, resources/read, prompts/get) also carry
// Mcp-Name with the tool name or resource URI.
func wireHeaders(method string, params any) map[string]string {
	headers := map[string]string{methodHeader: method}
	m, ok := params.(map[string]any)
	if !ok {
		return headers
	}
	var key string
	switch method {
	case "tools/call", "prompts/get":
		key = "name"
	case "resources/read":
		key = "uri"
	default:
		return headers
	}
	if name, ok := m[key].(string); ok && name != "" {
		headers[nameHeader] = name
	}
	return headers
}

// post sends one JSON-RPC request and returns the response message with the
// matching id. The server answers every POST with an SSE stream because
// JSONResponse is left at its production default.
func (w *wireFixture) post(t *testing.T, id int, method string, params any) sseMessage {
	t.Helper()
	msgs, err := w.postRaw(t, rpcRequest(id, method, params), wireHeaders(method, params))
	if err != nil {
		t.Fatalf("POST %s: %v", method, err)
	}
	for _, m := range msgs {
		if m.ID != nil && string(m.ID) == fmt.Sprint(id) {
			return m
		}
	}
	t.Fatalf("POST %s: no response for id %d in %d messages", method, id, len(msgs))
	return sseMessage{}
}

// postRejected asserts that a method the surface must not serve is refused:
// either at the transport (non-200 with a JSON-RPC error body, the shape the
// current protocol uses for unknown methods) or as an in-stream JSON-RPC
// error. Either shape proves the method produced no result.
func (w *wireFixture) postRejected(t *testing.T, id int, method string, params any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, w.ts.URL+"/mcp",
		strings.NewReader(rpcRequest(id, method, params)))
	if err != nil {
		t.Fatalf("%s request: %v", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(protocolHeader, protocolCurrent)
	for k, v := range wireHeaders(method, params) {
		req.Header.Set(k, v)
	}
	resp, err := w.ts.Client().Do(req)
	if err != nil {
		t.Fatalf("%s POST: %v", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var rejected sseMessage
		if err := json.NewDecoder(resp.Body).Decode(&rejected); err != nil || rejected.Error == nil {
			t.Fatalf("%s: transport status %d without a JSON-RPC error", method, resp.StatusCode)
		}
		return
	}
	var res sseMessage
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var m sseMessage
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &m); err != nil {
			t.Fatalf("%s: bad SSE data %q", method, line)
		}
		if m.ID != nil && string(m.ID) == fmt.Sprint(id) {
			res = m
		}
	}
	if res.Error == nil {
		t.Fatalf("write-style method %s accepted: %s", method, res.Result)
	}
}

// postRaw posts a raw body and returns every JSON-RPC message in the SSE
// stream. Extra headers are merged over the defaults.
func (w *wireFixture) postRaw(t *testing.T, body string, extra map[string]string) ([]sseMessage, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, w.ts.URL+"/mcp", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(protocolHeader, protocolCurrent)
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	resp, err := w.ts.Client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var msgs []sseMessage
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var m sseMessage
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &m); err != nil {
			return nil, fmt.Errorf("bad SSE data %q: %w", line, err)
		}
		msgs = append(msgs, m)
	}
	return msgs, sc.Err()
}

// listenStream is one open subscriptions/listen stream.
type listenStream struct {
	events chan sseMessage
	cancel context.CancelFunc
	done   chan struct{}
}

// openListen starts a subscriptions/listen stream, consumes the
// subscriptions/acknowledged message, and returns the stream with its
// subscription id.
func (w *wireFixture) openListen(t *testing.T, id int, uris []string) (listenStream, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.ts.URL+"/mcp",
		strings.NewReader(rpcRequest(id, "subscriptions/listen", map[string]any{
			"notifications": map[string]any{"resourceSubscriptions": uris},
		})))
	if err != nil {
		cancel()
		t.Fatalf("listen request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(protocolHeader, protocolCurrent)
	for k, v := range wireHeaders("subscriptions/listen", nil) {
		req.Header.Set(k, v)
	}
	resp, err := w.ts.Client().Do(req)
	if err != nil {
		cancel()
		t.Fatalf("listen POST: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		resp.Body.Close()
		t.Fatalf("listen status %d", resp.StatusCode)
	}
	ls := listenStream{events: make(chan sseMessage, 16), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(ls.done)
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			var m sseMessage
			if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &m); err != nil {
				return
			}
			select {
			case ls.events <- m:
			default:
				return
			}
		}
	}()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case m := <-ls.events:
			if m.Method != ackMethod {
				continue
			}
			var p struct {
				Meta map[string]any `json:"_meta"`
			}
			if err := json.Unmarshal(m.Params, &p); err != nil || p.Meta == nil {
				t.Fatalf("ack without _meta: %s", m.Params)
			}
			raw, ok := p.Meta[subscriptionMetaKey]
			if !ok || raw == nil {
				t.Fatalf("ack without subscription id: %s", m.Params)
			}
			return ls, fmt.Sprint(raw)
		case <-deadline:
			t.Fatal("no subscription acknowledgment within 5s")
		}
	}
}

// expectUpdate waits for one notifications/resources/updated on the stream
// and asserts it belongs to the given subscription.
func (w *wireFixture) expectUpdate(t *testing.T, ls listenStream, wantID, wantURI string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case m := <-ls.events:
			if m.Method != updatedMethod {
				continue
			}
			var p struct {
				URI  string         `json:"uri"`
				Meta map[string]any `json:"_meta"`
			}
			if err := json.Unmarshal(m.Params, &p); err != nil {
				t.Fatalf("bad update params: %v", err)
			}
			if got := fmt.Sprint(p.Meta[subscriptionMetaKey]); got != wantID {
				t.Fatalf("update subscription id = %q, want %q", got, wantID)
			}
			if p.URI != wantURI {
				t.Fatalf("update uri = %q, want %q", p.URI, wantURI)
			}
			return
		case <-deadline:
			t.Fatal("no resource-updated notification within 5s")
		}
	}
}

func TestMCPDiscovery(t *testing.T) {
	w := newWireFixture(t)

	// Protocol 2026-07-28 removes the initialize handshake: a session
	// initializes from the _meta triple, and discovery is server/discover.
	// The legacy method must be refused, not silently served.
	w.postRejected(t, 1, "initialize", map[string]any{
		"protocolVersion": protocolCurrent,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "acceptance", "version": "0"},
	})

	disc := w.post(t, 2, "server/discover", map[string]any{})
	if disc.Error != nil {
		t.Fatalf("discover error: %+v", disc.Error)
	}
	var dr struct {
		SupportedVersions []string `json:"supportedVersions"`
		Capabilities      struct {
			Resources struct {
				Subscribe bool `json:"subscribe"`
			} `json:"resources"`
		} `json:"capabilities"`
	}
	if err := json.Unmarshal(disc.Result, &dr); err != nil {
		t.Fatalf("bad discover result: %v", err)
	}
	if !slices.Contains(dr.SupportedVersions, protocolCurrent) {
		t.Fatalf("supported versions %v do not include %q", dr.SupportedVersions, protocolCurrent)
	}
	if !dr.Capabilities.Resources.Subscribe {
		t.Fatal("resource subscription capability not advertised")
	}

	tl := w.post(t, 3, "tools/list", map[string]any{})
	if tl.Error != nil {
		t.Fatalf("tools/list error: %+v", tl.Error)
	}
	var tools struct {
		Tools []struct {
			Name        string `json:"name"`
			InputSchema struct {
				Type       string `json:"type"`
				Properties map[string]struct {
					Type string `json:"type"`
				} `json:"properties"`
			} `json:"inputSchema"`
			Annotations *struct {
				ReadOnlyHint    bool  `json:"readOnlyHint"`
				DestructiveHint *bool `json:"destructiveHint"`
			} `json:"annotations"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(tl.Result, &tools); err != nil {
		t.Fatalf("bad tools/list result: %v", err)
	}
	readOnly := []string{"list_printers", "get_printer_state", "get_camera_snapshot", "watch_printer"}
	want := append(slices.Clone(readOnly), "pause_print", "resume_print", "stop_print",
		"set_chamber_light", "set_speed_profile", "set_ai_monitoring")
	if len(tools.Tools) != len(want) {
		t.Fatalf("tools/list returned %d tools, want exactly %d", len(tools.Tools), len(want))
	}
	seen := map[string]bool{}
	for _, tool := range tools.Tools {
		if seen[tool.Name] {
			t.Fatalf("duplicate tool %q", tool.Name)
		}
		seen[tool.Name] = true
		if tool.Annotations == nil || tool.Annotations.ReadOnlyHint != slices.Contains(readOnly, tool.Name) {
			t.Fatalf("tool %q readOnlyHint wrong: %+v", tool.Name, tool.Annotations)
		}
		destructive := tool.Annotations.DestructiveHint != nil && *tool.Annotations.DestructiveHint
		if destructive != (tool.Name == "stop_print") {
			t.Fatalf("tool %q destructiveHint = %v", tool.Name, destructive)
		}
		if tool.InputSchema.Type != "object" {
			t.Fatalf("tool %q schema type %q", tool.Name, tool.InputSchema.Type)
		}
	}
	for _, name := range want {
		if !seen[name] {
			t.Fatalf("tool %q missing from discovery", name)
		}
	}

	rl := w.post(t, 4, "resources/templates/list", map[string]any{})
	if rl.Error != nil {
		t.Fatalf("templates error: %+v", rl.Error)
	}
	var tpls struct {
		Templates []struct {
			URITemplate string `json:"uriTemplate"`
		} `json:"resourceTemplates"`
	}
	if err := json.Unmarshal(rl.Result, &tpls); err != nil {
		t.Fatalf("bad templates result: %v", err)
	}
	if len(tpls.Templates) != 1 || tpls.Templates[0].URITemplate != "bambu://printers/{serial}/state" {
		t.Fatalf("templates = %+v", tpls.Templates)
	}
}

func TestMCPToolCalls(t *testing.T) {
	w := newWireFixture(t)
	w.conn.set("P001", true)
	w.state.setSession("P001", printingSession("P001", w.now, 7, 30))
	w.step()

	call := func(id int, name string, args map[string]any, meta map[string]any) sseMessage {
		t.Helper()
		params := map[string]any{"name": name, "arguments": args}
		if meta != nil {
			params["_meta"] = meta
		}
		return w.post(t, id, "tools/call", params)
	}

	// list_printers; unknown client metadata rides along harmlessly.
	res := call(10, "list_printers", map[string]any{"limit": 2},
		map[string]any{"client.example/unknown": "ignored"})
	if res.Error != nil {
		t.Fatalf("list_printers error: %+v", res.Error)
	}
	var listOut struct {
		IsError           bool `json:"isError"`
		StructuredContent struct {
			Printers []struct {
				Serial   string `json:"serial"`
				Name     string `json:"name"`
				Revision string `json:"revision"`
			} `json:"printers"`
			Error *struct {
				Code string `json:"code"`
			} `json:"error"`
		} `json:"structuredContent"`
	}
	if err := json.Unmarshal(res.Result, &listOut); err != nil {
		t.Fatalf("bad list result: %v", err)
	}
	if listOut.IsError || listOut.StructuredContent.Error != nil ||
		len(listOut.StructuredContent.Printers) != 2 {
		t.Fatalf("list_printers = %s", res.Result)
	}
	if listOut.StructuredContent.Printers[0].Serial != "P001" {
		t.Fatalf("first printer = %+v", listOut.StructuredContent.Printers[0])
	}
	if listOut.StructuredContent.Printers[0].Revision == "" {
		t.Fatal("list row without revision token")
	}

	// get_printer_state: typed projection with freshness and camera state.
	res = call(11, "get_printer_state", map[string]any{"serial": "P001"}, nil)
	if res.Error != nil {
		t.Fatalf("get_printer_state error: %+v", res.Error)
	}
	var stateOut struct {
		StructuredContent struct {
			Serial   string `json:"serial"`
			Revision string `json:"revision"`
			State    struct {
				Name      string `json:"name"`
				Connected bool   `json:"connected"`
				Camera    string `json:"camera"`
				Freshness struct {
					Fresh                bool     `json:"fresh"`
					LastReportAgeSeconds *float64 `json:"last_report_age_seconds"`
				} `json:"freshness"`
				Progress *float64 `json:"progress"`
			} `json:"state"`
		} `json:"structuredContent"`
	}
	if err := json.Unmarshal(res.Result, &stateOut); err != nil {
		t.Fatalf("bad state result: %v", err)
	}
	sc := stateOut.StructuredContent
	if sc.Serial != "P001" || sc.State.Name != "Alpha" || !sc.State.Connected {
		t.Fatalf("state = %s", res.Result)
	}
	if sc.State.Freshness.Fresh {
		t.Fatal("state without a real report counted fresh")
	}
	if sc.State.Progress == nil || *sc.State.Progress != 30 {
		t.Fatalf("progress presence broken: %s", res.Result)
	}
	if sc.Revision == "" {
		t.Fatal("state without revision token")
	}

	// Unknown serial is a typed tool error, not a transport failure.
	assertToolError(t, call(12, "get_printer_state", map[string]any{"serial": "NOPE"}, nil), errUnknownSerial)

	// Unknown tool and unknown method are refused with JSON-RPC errors: the
	// surface has no write path at all.
	w.postRejected(t, 13, "tools/call", map[string]any{
		"name": "no_such_tool", "arguments": map[string]any{},
	})
	// Under the current protocol an unknown method is refused at the
	// transport with a JSON-RPC error; the surface has no write path at all.
	w.postRejected(t, 14, "printers/pause", map[string]any{"serial": "P001"})

	// The state resource mirrors the tool output.
	rd := w.post(t, 15, "resources/read", map[string]any{"uri": stateURI("P001")})
	if rd.Error != nil {
		t.Fatalf("resources/read error: %+v", rd.Error)
	}
	var read struct {
		Contents []struct {
			URI      string `json:"uri"`
			MIMEType string `json:"mimeType"`
			Text     string `json:"text"`
		} `json:"contents"`
	}
	if err := json.Unmarshal(rd.Result, &read); err != nil {
		t.Fatalf("bad read result: %v", err)
	}
	if len(read.Contents) != 1 || read.Contents[0].MIMEType != "application/json" {
		t.Fatalf("contents = %+v", read.Contents)
	}
	var payload struct {
		Serial   string `json:"serial"`
		Revision string `json:"revision"`
	}
	if err := json.Unmarshal([]byte(read.Contents[0].Text), &payload); err != nil {
		t.Fatalf("bad resource JSON: %v", err)
	}
	if payload.Serial != "P001" || payload.Revision == "" {
		t.Fatalf("resource payload = %+v", payload)
	}

	// Unknown serial resources fail at the protocol layer.
	rd = w.post(t, 16, "resources/read", map[string]any{"uri": stateURI("NOPE")})
	if rd.Error == nil {
		t.Fatal("unknown serial resource accepted")
	}
}

// assertToolError asserts a typed, error-flagged tool result with the given
// stable code.
func assertToolError(t *testing.T, msg sseMessage, code string) {
	t.Helper()
	var res struct {
		IsError           bool `json:"isError"`
		StructuredContent struct {
			Error *struct {
				Code string `json:"code"`
			} `json:"error"`
		} `json:"structuredContent"`
	}
	if err := json.Unmarshal(msg.Result, &res); err != nil {
		t.Fatalf("bad tool result: %v", err)
	}
	if !res.IsError || res.StructuredContent.Error == nil ||
		res.StructuredContent.Error.Code != code {
		t.Fatalf("tool error %q not reported: %s", code, msg.Result)
	}
}

func TestMCPWatchToolWire(t *testing.T) {
	w := newWireFixture(t)
	w.conn.set("P001", true)
	w.state.setSession("P001", printingSession("P001", w.now, 7, 10))
	w.step()

	watch := func(id int, args map[string]any) map[string]any {
		t.Helper()
		res := w.post(t, id, "tools/call", map[string]any{
			"name": "watch_printer", "arguments": args,
		})
		if res.Error != nil {
			t.Fatalf("watch error: %+v", res.Error)
		}
		var out struct {
			StructuredContent map[string]any `json:"structuredContent"`
		}
		if err := json.Unmarshal(res.Result, &out); err != nil {
			t.Fatalf("bad watch result: %v", err)
		}
		return out.StructuredContent
	}
	flag := func(sc map[string]any, key string) bool {
		b, _ := sc[key].(bool)
		return b
	}

	// Initial call: snapshot plus a replayable revision.
	first := watch(20, map[string]any{"serial": "P001"})
	if flag(first, "changed") || flag(first, "resync_required") {
		t.Fatalf("initial watch = %+v", first)
	}
	token, _ := first["revision"].(string)
	if token == "" {
		t.Fatal("initial watch without revision")
	}

	// Zero-timeout poll on a quiet printer: unchanged.
	quiet := watch(21, map[string]any{"serial": "P001", "after_revision": token, "timeout_seconds": 0})
	if flag(quiet, "changed") || flag(quiet, "resync_required") {
		t.Fatalf("quiet poll = %+v", quiet)
	}

	// Stale token after a change: explicit resync with a fresh snapshot.
	w.conn.set("P001", false)
	w.step()
	stale := watch(22, map[string]any{"serial": "P001", "after_revision": token, "timeout_seconds": 0})
	if flag(stale, "changed") || !flag(stale, "resync_required") {
		t.Fatalf("stale token poll = %+v", stale)
	}
	fresh, _ := stale["revision"].(string)
	if fresh == "" || fresh == token {
		t.Fatalf("resync revision = %q", fresh)
	}

	// Reconnect the upstream and let the report become fresh again; the
	// resulting attention bump invalidates `fresh`, so re-baseline.
	w.conn.set("P001", true)
	w.gens.set("P001", 1)
	w.step()
	rebas := watch(23, map[string]any{"serial": "P001", "after_revision": fresh, "timeout_seconds": 0})
	if !flag(rebas, "resync_required") {
		t.Fatalf("expected resync after reconnect bump: %+v", rebas)
	}
	token2, _ := rebas["revision"].(string)
	steady := watch(24, map[string]any{"serial": "P001", "after_revision": token2, "timeout_seconds": 0})
	if flag(steady, "changed") || flag(steady, "resync_required") {
		t.Fatalf("baseline poll = %+v", steady)
	}

	// A second reconnect (generation bump over a still-recent report) must
	// surface as reports_stale. The poll parks first and the change lands
	// while it is in flight, which is the changed flow a long poll exists
	// for; a bump that lands before the park is a resync, not a change.
	token3, _ := steady["revision"].(string)
	type posted struct {
		msgs []sseMessage
		err  error
	}
	watchParams := map[string]any{
		"name": "watch_printer",
		"arguments": map[string]any{
			"serial": "P001", "after_revision": token3, "timeout_seconds": 3,
		},
	}
	resCh := make(chan posted, 1)
	go func() {
		msgs, err := w.postRaw(t, rpcRequest(25, "tools/call", watchParams),
			wireHeaders("tools/call", watchParams))
		resCh <- posted{msgs: msgs, err: err}
	}()
	if !waitWatchParked() {
		t.Fatal("freshness-loss watch never parked")
	}
	w.gens.set("P001", 2)
	w.step()
	var res posted
	select {
	case res = <-resCh:
	case <-time.After(5 * time.Second):
		t.Fatal("freshness-loss watch did not return")
	}
	if res.err != nil {
		t.Fatalf("freshness-loss watch POST: %v", res.err)
	}
	var lost sseMessage
	for _, m := range res.msgs {
		if m.ID != nil && string(m.ID) == "25" {
			lost = m
		}
	}
	if lost.Result == nil {
		t.Fatalf("no freshness-loss response: %+v", res.msgs)
	}
	var lostOut struct {
		StructuredContent map[string]any `json:"structuredContent"`
	}
	if err := json.Unmarshal(lost.Result, &lostOut); err != nil {
		t.Fatalf("bad freshness-loss result: %v", err)
	}
	if !flag(lostOut.StructuredContent, "changed") {
		t.Fatalf("freshness loss poll = %+v", lostOut.StructuredContent)
	}
	events, _ := lostOut.StructuredContent["events"].([]any)
	found := false
	for _, e := range events {
		if m, ok := e.(map[string]any); ok && m["kind"] == kindReportsStale {
			found = true
		}
	}
	if !found {
		t.Fatalf("reports_stale missing: %+v", events)
	}
}

func TestMCPWatchChangedFlowWire(t *testing.T) {
	w := newWireFixture(t)
	w.conn.set("P001", true)
	w.state.setSession("P001", printingSession("P001", w.now, 7, 10))
	w.step()

	for attempt := 0; attempt < 10; attempt++ {
		token := w.srv.token(w.srv.sampler.revision("P001"))
		stop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			flip := false
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					flip = !flip
					w.conn.set("P001", flip)
					w.step()
				}
			}
		}()
		callParams := map[string]any{
			"name": "watch_printer",
			"arguments": map[string]any{
				"serial": "P001", "after_revision": token, "timeout_seconds": 3,
			},
		}
		msgs, err := w.postRaw(t, rpcRequest(30, "tools/call", callParams),
			wireHeaders("tools/call", callParams))
		close(stop)
		if err != nil {
			t.Fatalf("watch POST: %v", err)
		}
		var res sseMessage
		for _, m := range msgs {
			if m.ID != nil && string(m.ID) == "30" {
				res = m
			}
		}
		if res.Result == nil {
			t.Fatalf("no watch response: %+v", msgs)
		}
		var out struct {
			StructuredContent map[string]any `json:"structuredContent"`
		}
		if err := json.Unmarshal(res.Result, &out); err != nil {
			t.Fatalf("bad watch result: %v", err)
		}
		if changed, _ := out.StructuredContent["changed"].(bool); changed {
			events, _ := out.StructuredContent["events"].([]any)
			found := false
			for _, e := range events {
				if m, ok := e.(map[string]any); ok &&
					(m["kind"] == kindConnectLost || m["kind"] == kindConnectRestored) {
					found = true
				}
			}
			if !found {
				t.Fatalf("connectivity event missing: %+v", events)
			}
			return
		}
		if resync, _ := out.StructuredContent["resync_required"].(bool); !resync {
			t.Fatalf("neither changed nor resync: %+v", out.StructuredContent)
		}
	}
	t.Fatal("changed flow never observed over the wire")
}

func TestMCPWatchProgressSupersetWire(t *testing.T) {
	w := newWireFixture(t)
	w.conn.set("P001", true)
	w.state.setSession("P001", printingSession("P001", w.now, 7, 10))
	w.step()

	watch := func(id int, args map[string]any) map[string]any {
		t.Helper()
		res := w.post(t, id, "tools/call", map[string]any{
			"name": "watch_printer", "arguments": args,
		})
		if res.Error != nil {
			t.Fatalf("watch error: %+v", res.Error)
		}
		var out struct {
			StructuredContent map[string]any `json:"structuredContent"`
		}
		if err := json.Unmarshal(res.Result, &out); err != nil {
			t.Fatalf("bad watch result: %v", err)
		}
		return out.StructuredContent
	}
	flag := func(sc map[string]any, key string) bool {
		b, _ := sc[key].(bool)
		return b
	}

	// Initial progress-mode call yields the replayable token.
	first := watch(40, map[string]any{"serial": "P001", "mode": "progress"})
	if flag(first, "changed") || flag(first, "resync_required") {
		t.Fatalf("initial progress watch = %+v", first)
	}
	token, _ := first["revision"].(string)
	if token == "" {
		t.Fatal("initial progress watch without revision")
	}

	// A parked progress watch must wake on an attention change: progress
	// mode is a superset of attention on the wire, not mode-exclusive.
	type posted struct {
		msgs []sseMessage
		err  error
	}
	params := map[string]any{
		"name": "watch_printer",
		"arguments": map[string]any{
			"serial": "P001", "after_revision": token,
			"timeout_seconds": 3, "mode": "progress",
		},
	}
	resCh := make(chan posted, 1)
	go func() {
		msgs, err := w.postRaw(t, rpcRequest(41, "tools/call", params),
			wireHeaders("tools/call", params))
		resCh <- posted{msgs: msgs, err: err}
	}()
	if !waitWatchParked() {
		t.Fatal("progress watch never parked")
	}
	w.conn.set("P001", false)
	w.step()
	var res posted
	select {
	case res = <-resCh:
	case <-time.After(5 * time.Second):
		t.Fatal("progress watch did not return")
	}
	if res.err != nil {
		t.Fatalf("progress watch POST: %v", res.err)
	}
	var msg sseMessage
	for _, m := range res.msgs {
		if m.ID != nil && string(m.ID) == "41" {
			msg = m
		}
	}
	if msg.Result == nil {
		t.Fatalf("no progress watch response: %+v", res.msgs)
	}
	var out struct {
		StructuredContent map[string]any `json:"structuredContent"`
	}
	if err := json.Unmarshal(msg.Result, &out); err != nil {
		t.Fatalf("bad progress watch result: %v", err)
	}
	if !flag(out.StructuredContent, "changed") {
		t.Fatalf("attention change did not wake progress watch: %+v", out.StructuredContent)
	}
	events, _ := out.StructuredContent["events"].([]any)
	found := false
	for _, e := range events {
		if m, ok := e.(map[string]any); ok && m["kind"] == kindConnectLost {
			found = true
		}
	}
	if !found {
		t.Fatalf("connectivity event missing: %+v", events)
	}
}

func TestMCPSubscriptionLifecycle(t *testing.T) {
	w := newWireFixture(t)
	w.startDispatch()
	w.conn.set("P001", true)
	w.state.setSession("P001", printingSession("P001", w.now, 7, 10))
	w.step()

	ls, subID := w.openListen(t, 900, []string{stateURI("P001")})

	// A change fans the update out to the stream with the subscription id.
	w.conn.set("P001", false)
	w.step()
	w.expectUpdate(t, ls, subID, stateURI("P001"))
	if w.activeSubs() != 1 {
		t.Fatalf("subs = %d, want 1", w.activeSubs())
	}

	// Cancelling the stream releases the subscription. The SDK notices the
	// dead stream on its next write, so keep producing notifications until
	// the unwind lands.
	ls.cancel()
	<-ls.done
	deadline := time.Now().Add(10 * time.Second)
	flip := false
	for w.activeSubs() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("subscription never released; subs=%d", w.activeSubs())
		}
		flip = !flip
		w.conn.set("P001", flip)
		w.step()
		time.Sleep(20 * time.Millisecond)
	}

	// The ceiling is freed: a new stream subscribes cleanly and gets a
	// fresh acknowledgment id.
	ls2, id2 := w.openListen(t, 901, []string{stateURI("P001")})
	if id2 == subID {
		t.Fatal("reconnection reused the old subscription id")
	}
	ls2.cancel()
	<-ls2.done
}

func TestMCPSubscriptionOnlyClient(t *testing.T) {
	w := newWireFixture(t)
	w.startDispatch()
	w.conn.set("P001", true)
	w.state.setSession("P001", printingSession("P001", w.now, 7, 10))
	w.step()

	// A client that only holds a subscription never parks a watch, yet the
	// shared sampler still observes and notifies.
	ls, subID := w.openListen(t, 910, []string{stateURI("P001")})
	w.conn.set("P001", false)
	w.step()
	w.expectUpdate(t, ls, subID, stateURI("P001"))
	ls.cancel()
	<-ls.done

	// Unknown serials never subscribe: the listen request itself fails.
	w.postRejected(t, 911, "subscriptions/listen", map[string]any{
		"notifications": map[string]any{
			"resourceSubscriptions": []string{stateURI("NOPE")},
		},
	})
	if w.activeSubs() != 0 {
		t.Fatalf("rejected listen left a count: %d", w.activeSubs())
	}
}

func TestMCPCameraSnapshotWire(t *testing.T) {
	w := newWireFixture(t)
	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x01}
	frame := &camera.Frame{JPEG: jpeg, Seq: 42, Captured: w.now}
	w.cams.frames["P001"] = frame
	w.cams.waits["P001"] = frame

	res := w.post(t, 40, "tools/call", map[string]any{
		"name":      "get_camera_snapshot",
		"arguments": map[string]any{"serial": "P001", "max_age_seconds": 30},
	})
	if res.Error != nil {
		t.Fatalf("snapshot error: %+v", res.Error)
	}
	var out struct {
		Content []struct {
			Type     string `json:"type"`
			MIMEType string `json:"mimeType"`
			Data     string `json:"data"`
		} `json:"content"`
		StructuredContent struct {
			Serial     string  `json:"serial"`
			FrameSeq   uint64  `json:"frame_seq"`
			AgeSeconds float64 `json:"age_seconds"`
		} `json:"structuredContent"`
	}
	if err := json.Unmarshal(res.Result, &out); err != nil {
		t.Fatalf("bad snapshot result: %v", err)
	}
	if out.StructuredContent.FrameSeq != 42 || out.StructuredContent.Serial != "P001" {
		t.Fatalf("metadata = %s", res.Result)
	}
	sawJPEG := false
	for _, c := range out.Content {
		if c.Type == "image" && c.MIMEType == "image/jpeg" {
			raw, err := base64.StdEncoding.DecodeString(c.Data)
			if err != nil || !bytes.Equal(raw, jpeg) {
				t.Fatalf("image payload mismatch: %v", err)
			}
			sawJPEG = true
		}
	}
	if !sawJPEG {
		t.Fatalf("no jpeg block: %+v", out.Content)
	}
	w.cams.balanced(t)

	// An impossible age bound reports stale explicitly instead of serving
	// the old frame silently.
	w.srv.now = func() time.Time { return w.now.Add(10 * time.Second) }
	assertToolError(t, w.post(t, 41, "tools/call", map[string]any{
		"name":      "get_camera_snapshot",
		"arguments": map[string]any{"serial": "P001", "max_age_seconds": 0},
	}), errStaleImage)
	w.cams.balanced(t)
}

func TestMCPTransportHardening(t *testing.T) {
	w := newWireFixture(t)

	do := func(body string, extra map[string]string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, w.ts.URL+"/mcp", strings.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set(protocolHeader, protocolCurrent)
		for k, v := range extra {
			req.Header.Set(k, v)
		}
		resp, err := w.ts.Client().Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	// Oversized bodies are rejected before any processing.
	if got := do(strings.Repeat("x", 65<<10), nil); got != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body status = %d, want 413", got)
	}

	// Browser cross-origin requests are refused.
	if got := do(rpcRequest(1, "tools/list", map[string]any{}),
		map[string]string{"Origin": "https://evil.example"}); got != http.StatusForbidden {
		t.Fatalf("cross-origin status = %d, want 403", got)
	}

	// Stateless mode: GET is not a transport.
	resp, err := http.Get(w.ts.URL + "/mcp")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", resp.StatusCode)
	}
}

func TestMCPCloseUnblocksListeners(t *testing.T) {
	w := newWireFixture(t)
	w.startDispatch()
	ls, _ := w.openListen(t, 920, []string{stateURI("P001")})
	if w.activeSubs() != 1 {
		t.Fatalf("subs = %d, want 1", w.activeSubs())
	}

	// Close cancels the root context; the listen stream must unwind and
	// release its subscription without any further notification traffic.
	w.srv.Close()
	select {
	case <-ls.done:
	case <-time.After(5 * time.Second):
		t.Fatal("listen stream outlived Close")
	}
	deadline := time.Now().Add(5 * time.Second)
	for w.activeSubs() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("subscription leaked past Close: %d", w.activeSubs())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A pre-2026-07-28 client must never reach the subscription registry. The
// SDK keeps routing legacy resources/subscribe for old-protocol sessions,
// and a subscribe on a stateless ephemeral session would leak its count
// forever: the SDK releases such sessions without invoking
// UnsubscribeHandler. Every legacy spelling is therefore refused, the count
// stays at zero, and the current protocol's listen stream keeps working end
// to end.
func TestMCPSubscribeLegacyProtocolRefused(t *testing.T) {
	w := newWireFixture(t)
	w.startDispatch()
	w.conn.set("P001", true)
	w.state.setSession("P001", printingSession("P001", w.now, 7, 10))
	w.step()

	// A raw legacy request: no _meta triple, no Mcp-Method header, and a
	// legacy or absent MCP-Protocol-Version header.
	legacySub := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":900,"method":"resources/subscribe","params":{"uri":%q}}`,
		stateURI("P001"))
	post := func(version, method string) []sseMessage {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, w.ts.URL+"/mcp", strings.NewReader(legacySub))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if version != "" {
			req.Header.Set(protocolHeader, version)
		}
		if method != "" {
			req.Header.Set(methodHeader, method)
		}
		resp, err := w.ts.Client().Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			// Refused at the transport with a plain HTTP error status.
			return nil
		}
		var msgs []sseMessage
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			var m sseMessage
			if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &m); err != nil {
				t.Fatalf("bad SSE data %q: %v", line, err)
			}
			msgs = append(msgs, m)
		}
		return msgs
	}

	for _, tc := range []struct{ name, version, method string }{
		{"legacy header 2025-06-18", "2025-06-18", ""},
		{"legacy header 2025-11-25", "2025-11-25", ""},
		{"missing protocol header", "", ""},
		// A current protocol header with the matching Mcp-Method passes
		// the transport gate, but no _meta triple leaves the session with
		// nil initialize state. The gate must refuse it, not dereference it.
		{"current header without _meta", protocolCurrent, "resources/subscribe"},
	} {
		msgs := post(tc.version, tc.method)
		if len(msgs) != 0 {
			refused := false
			for _, m := range msgs {
				if m.Result != nil {
					t.Fatalf("%s: subscribe succeeded: %s", tc.name, m.Result)
				}
				if m.Error != nil {
					refused = true
				}
			}
			if !refused {
				t.Fatalf("%s: accepted without a JSON-RPC error: %+v", tc.name, msgs)
			}
		}
		if got := w.activeSubs(); got != 0 {
			t.Fatalf("%s: subscription count = %d, want 0", tc.name, got)
		}
	}

	// The refusals spare nothing for current clients: a live protocol
	// subscription still takes its slot, receives its fan-out, and releases.
	ls, subID := w.openListen(t, 901, []string{stateURI("P001")})
	if w.activeSubs() != 1 {
		t.Fatalf("subs = %d, want 1", w.activeSubs())
	}
	w.conn.set("P001", false)
	w.step()
	w.expectUpdate(t, ls, subID, stateURI("P001"))
	ls.cancel()
	<-ls.done
	deadline := time.Now().Add(5 * time.Second)
	for w.activeSubs() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("subscription leaked: %d", w.activeSubs())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMCPStateSchemaFlattensView pins the embedded printer view: the shared
// fields must appear at the top level of state in the output schema.
func TestMCPStateSchemaFlattensView(t *testing.T) {
	w := newWireFixture(t)
	tl := w.post(t, 1, "tools/list", map[string]any{})
	var tools struct {
		Tools []struct {
			Name         string `json:"name"`
			OutputSchema struct {
				Properties map[string]struct {
					Properties map[string]json.RawMessage `json:"properties"`
				} `json:"properties"`
			} `json:"outputSchema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(tl.Result, &tools); err != nil {
		t.Fatalf("bad tools/list result: %v", err)
	}
	for _, tool := range tools.Tools {
		if tool.Name != "get_printer_state" {
			continue
		}
		props := tool.OutputSchema.Properties["state"].Properties
		for _, key := range []string{"print_state", "chamber_light", "freshness", "controls", "activity", "camera"} {
			if _, ok := props[key]; !ok {
				t.Fatalf("state schema lacks %q: %v", key, props)
			}
		}
		return
	}
	t.Fatal("get_printer_state missing")
}

func TestMCPControlTools(t *testing.T) {
	w := newWireFixture(t)
	w.conn.set("P001", true)
	call := func(id int, name string, args map[string]any) sseMessage {
		t.Helper()
		return w.post(t, id, "tools/call", map[string]any{"name": name, "arguments": args})
	}
	res := call(1, "stop_print", map[string]any{"serial": "P001"})
	var out struct {
		IsError           bool `json:"isError"`
		StructuredContent struct {
			Serial string `json:"serial"`
			Action string `json:"action"`
			Sent   bool   `json:"sent"`
			State  *struct {
				Controls []string `json:"controls"`
			} `json:"state"`
		} `json:"structuredContent"`
	}
	if err := json.Unmarshal(res.Result, &out); err != nil {
		t.Fatalf("bad stop result: %v", err)
	}
	sc := out.StructuredContent
	if out.IsError || !sc.Sent || sc.Action != "stop" || sc.State == nil || !slices.Contains(sc.State.Controls, "stop") {
		t.Fatalf("stop_print = %s", res.Result)
	}
	if got := w.cmd.sentList(); !slices.Equal(got, []string{"stop"}) {
		t.Fatalf("sent = %v", got)
	}
	assertToolError(t, call(2, "set_chamber_light", map[string]any{"serial": "P002", "on": true}), errUpstreamUnavailable)
	assertToolError(t, call(3, "pause_print", map[string]any{"serial": "P001"}), errInvalidState)
	assertToolError(t, call(4, "stop_print", map[string]any{"serial": "NOPE"}), errUnknownSerial)
	assertToolError(t, call(5, "set_ai_monitoring", map[string]any{"serial": "P001", "enabled": false}), errDetectionDisabled)
	if bad := call(6, "set_speed_profile", map[string]any{"serial": "P001", "profile": "turbo"}); bad.Error == nil {
		var r struct {
			IsError bool `json:"isError"`
		}
		if json.Unmarshal(bad.Result, &r); !r.IsError {
			t.Fatalf("invalid profile accepted: %s", bad.Result)
		}
	}
	if got := w.cmd.sentList(); len(got) != 1 {
		t.Fatalf("sent = %v, want only the stop", got)
	}
}

// waitWatchParked reports whether a watch_printer handler became blocked in
// its park select within two seconds. Parking happens only after the handler
// captured its revision and wake channel, so a change applied afterwards is
// guaranteed to wake it; the admission-slot count is taken earlier and is
// not that barrier.
func waitWatchParked() bool {
	buf := make([]byte, 1<<20)
	for i := 0; i < 2000; i++ {
		n := runtime.Stack(buf, true)
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(g, "[select") && strings.Contains(g, ").toolWatchPrinter(") {
				return true
			}
		}
		time.Sleep(time.Millisecond)
	}
	return false
}
