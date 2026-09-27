# bambu-mqtt-proxy — Design

## 1. Purpose

Many Bambu integrations (Bambu Studio, Home Assistant, xtouch, OctoPrint plugins) each open their own MQTT connection to a printer. The printer tolerates few connections, and each client must know every printer address. This proxy removes both problems:

- Clients connect to ONE MQTT endpoint.
- The proxy holds ONE MQTT connection per configured printer.
- The proxy routes traffic by the serial number in the topic path (`device/{serial}/...`).

### Goals

1. Accept downstream MQTT clients on port 8883 with TLS and a self-signed certificate.
2. Pose as a printer: MQTT behavior, authentication, and TLS match the printer's embedded broker, so existing apps (Bambu Studio, Home Assistant, xtouch, OctoPrint plugins) work without changes.
3. Hold exactly one upstream MQTT connection per configured printer endpoint.
4. Route publishes and subscriptions bidirectionally by serial number.
5. Stay transparent: forward payloads byte-for-byte. No JSON parsing.
6. Ship as one small static Go binary with a YAML config file.
7. Serve live camera snapshots, MJPEG streams, and a browser camera wall over one shared HTTP port for P1/A1-series printers, and expose the camera also through a printer-compatible raw TLS endpoint on camera port 6000.

### Non-goals

- RTSP proxy (X1-class camera, port 322), FTPS proxy (port 990), file transfer.
- Cloud (Bambu Cloud) bridging or cloud authentication.
- Payload transformation, filtering, or command signing.
- Message persistence across proxy restarts; retained-message storage.
- MQTT v5-only features; clusters; horizontal scaling.

## 2. Research summary

### 2.1 Go MQTT broker libraries

| Library | Verdict | Evidence |
|---|---|---|
| **mochi-mqtt v2** (`github.com/mochi-mqtt/server/v2`) | **Selected** — embeddable broker, MQTT v5 + v3.1.1 hybrid, TLS listeners, stackable hooks (`OnConnectAuthenticate`, `OnSubscribe`, `OnUnsubscribe`, `OnPublish`, `OnDisconnect`), inline client (`server.Publish`/`Subscribe`), MIT, active project, passes Paho interop tests | https://github.com/mochi-mqtt/server |
| volantmq and similar | Stale or inactive; no advantage over mochi | — |
| Hand-rolled MQTT 3.1.1 server on `net` | Rejected: re-implements varint encoding, QoS flows, keepalive; mochi removes that risk | — |

### 2.2 Go MQTT client libraries (upstream)

| Library | Verdict | Evidence |
|---|---|---|
| **paho.mqtt.golang** (`github.com/eclipse/paho.mqtt.golang`) | **Selected** — MQTT 3.1/3.1.1, `SetAutoReconnect`, `SetResumeSubs`, TLS via `tls.Config` (works with `InsecureSkipVerify`), mature | https://github.com/eclipse/paho.mqtt.golang |
| paho.golang (`github.com/eclipse/paho.golang`) | Rejected: MQTT v5 only; printer broker is v3.1.1 | https://github.com/eclipse/paho.golang |

### 2.3 Bambu P1S MQTT protocol (LAN)

- Printer runs an MQTT broker at `mqtt://{PRINTER_IP}:8883`, MQTT over TLS, self-signed certificate; all community clients skip certificate verification.
- Username `bblp`, password = the printer's LAN access code (Settings → Network → LAN Mode).
- LAN Mode must be ON. On 2025+ "authorization-control" firmware, `print.*` commands must be RSA-SHA256 signed by the client; the proxy does not touch payloads, so signing stays a client concern.
- Topics: `device/{serial}/report` (printer → clients), `device/{serial}/request` (clients → printer).
- Handshake on subscribe: clients publish `{"pushing":{"sequence_id":"0","command":"pushall"}}` to get full state. P1 series sends only deltas afterwards; a fresh subscriber MUST trigger `pushall` to see full state.
- Commands (`get_version`, `pushall`, `project_file`, `pause`, `gcode_line`, `ledctrl`, ...) are JSON on `/request`; responses and state arrive as JSON on `/report`.
- Observed broker behavior (community-verified; not formally documented by Bambu): MQTT 3.1.1, clean session, keepalive 30-60 s, QoS 0 for routine and 1 for critical traffic, unique client IDs per connection. Concurrent-connection limits are real and model-dependent: about 4 on P1/X1, ONE on A1 series. Without Developer Mode the broker accepts connections but silently drops write commands. The TLS certificate is Bambu-issued with CN = printer serial; all community clients disable verification.
- Stock firmware exposes NO plain-text MQTT port 1883. The listener-side port 8883 is the real printer endpoint. The proxy therefore makes the upstream transport configurable (see §5).
- Sources: https://github.com/Doridian/OpenBambuAPI/blob/main/mqtt.md , https://github.com/sksat/bambu-rs/blob/main/docs/protocol.md , https://github.com/karaktaka/pandaproxy (ports/auth table), https://contentnation.net/en/grumpydevelop/bl-knowledge (keepalive/QoS observations), https://deepwiki.com/synman/bambu-printer-manager/9-troubleshooting-and-faq (model connection limits).

### 2.4 Existing proxy projects (prior art)

| Project | Pattern | Gap vs this design |
|---|---|---|
| https://github.com/disconn3ct/bambu-proxy | Mosquitto relay: many clients → one printer, single bridge connection upstream | Not Go; one printer per relay; no serial routing; config-only |
| https://github.com/karaktaka/pandaproxy | Python TLS-terminating TCP proxy per printer (camera/MQTT/FTP); one upstream per client | No upstream multiplexing; one printer per instance; archived on GitHub |
| https://github.com/slynn1324/bambu-bridge | Node.js printer → central broker republish | Not transparent for Bambu Studio clients; inactive |
| https://github.com/ggogel/Bambu-MQTT-Scraper | Python 1:1 scraper → Home Assistant broker | Changes topic shape; 1 printer |
| https://github.com/tribixbite/beambam | Python full client/daemon with many surfaces | Not an MQTT endpoint multiplexer |

Gap: no existing project combines ONE MQTT endpoint + serial-based routing to MULTIPLE printers + ONE upstream connection per printer. The disconn3ct Mosquitto relay validates the core pattern (a single upstream connection serves many clients against a Bambu printer broker, with SSL verification disabled downstream).

## 3. Architecture

```
 Bambu Studio   HA integration   xtouch   ...        (≈10 TLS clients)
      │               │             │
      └───────────────┼─────────────┘
                      ▼
        ┌──────────────────────────────┐
        │  bambu-mqtt-proxy            │
        │  TLS :8883 (self-signed)     │  ← mochi-mqtt v2 broker + routing hooks
        │                              │
        │  serial → upstream table     │
        │  HTTP :8080 health + camera  │
        │  TLS :6000 raw camera        │
        └───┬──────────────┬───────────┘
            │ 1 conn       │ 1 conn        (paho.mqtt.golang)
            ▼              ▼
     P1S 192.168.1.42  P1S 192.168.1.43
       :8883 MQTTS       :1883 MQTT (configurable)
```

Four parts, one process:

1. **Downstream broker** — mochi-mqtt v2 with a TLS listener on 1883. Mochi owns all MQTT protocol mechanics for clients (CONNECT/CONNACK, keepalive per client, SUBACK/PUBACK, session takeover).
2. **Upstream pool** — one paho.mqtt.golang client per configured printer, created lazily, reconnected automatically. A routing table maps serial → upstream connection and tracks merged subscriptions.
3. **HTTP service** — one `net/http` listener (`http.port`, default 8080) for the health endpoints and, when cameras are enabled, the camera snapshot/stream endpoints and the embedded camera wall.
4. **Raw camera endpoint** — a TLS listener on printer camera port 6000 serving the chamber-image protocol to camera clients; the access code a client authenticates with selects the printer (§6). It starts whenever cameras are enabled, independent of the HTTP port.

A custom mochi hook is the only coupling between the broker and the pool. The hook rewrites where packets go; it does not rewrite packets. The telemetry cache observes reports through the pool without changing forwarding.

## 4. Downstream endpoint (printer-compatible)

- Listener: TCP + TLS on `:8883` (default) — the port every Bambu app already targets. Certificate: self-signed, ECDSA P-256, 10-year validity, generated on first start and persisted to `cert_file`/`key_file` paths. Apps skip certificate verification exactly as they do against the printer; the file paths also let an operator load a specific cert for tools that pin.
- Additional listeners are optional config entries (for example a plain or TLS `:1883` for custom dashboards).
- Authentication, default mode `printer`: CONNECT must carry username `bblp` and a password matching ANY configured printer access code; otherwise the proxy refuses with CONNACK `bad username or password` (v3.1.1 code 4 — mochi's standard denial; apps treat code 4 and 5 identically: refused). Mode `accept_all` remains available for unusual clients. Enforced in `OnConnectAuthenticate`.
- Access control: SUBSCRIBE is allowed for `device/{configured serial}/report`, `device/{configured serial}/request`, and serial-level wildcards over configured printers. PUBLISH is allowed ONLY to `device/{configured serial}/request`. This blocks clients from injecting fake reports into other clients — no legitimate Bambu app publishes to `/report`. Enforced via mochi's `OnACLCheck`.
- Capacity: about 10 clients is trivial for mochi (benchmarks show thousands of msg/s at 10+ clients). No hard client cap in v1.

### 4.1 Protocol parity with the printer's embedded broker

The listener MUST behave at the same protocol level as the broker in the printer firmware, including observed quirks. Behaviors marked "observed" come from community reverse engineering, not official documentation. Hook semantics are verified against mochi-mqtt v2 source (`server.go`: `processPublish`, `processSubscribe`, `publishToSubscribers`).

| Behavior | Printer (observed) | Proxy |
|---|---|---|
| Protocol version | MQTT 3.1.1; apps use no v5 features | Serves v3.1.1; v5 clients tolerated but unused |
| QoS | 0 and 1 only | Capped at 1 on every hop via mochi `Capabilities.MaximumQos = 1`; SUBACK grants max 1 |
| Keepalive | Apps use 30-60 s | Per client, enforced by mochi |
| Client ID takeover | Same-ID reconnect kicks the old session | Same (mochi default) |
| Clean session | Apps use true | Accepted; sessions are not persisted across proxy restart (same as a printer reboot) |
| Retained messages | Not used; reports are live pushes | Client publishes forward with the retain bit stripped; nothing is retained broker-side |
| Last Will | Unused by Bambu apps | Supported by mochi, no special handling |
| Connection limit | ~4 concurrent (P1/X1), 1 on A1 series | Intentionally raised — removing this limit is the proxy's purpose |
| Auth failure | Connection refused | CONNACK code 4 (bad username or password) in `printer` mode |
| Unknown serial subscription | n/a (printer serves one serial) | SUBACK 0x80 for that filter only; connection stays open (v3-clamped, verified in mochi source) |
| Publish to `/report` or unknown topics | Printer broker is permissive | QoS 0 dropped silently; QoS ≥ 1 client disconnected (mochi ACL behavior) — protects other apps from injected state |
| TLS | Bambu-issued cert, CN = serial; clients skip verify | Self-signed leaf; clients skip verify; pinning possible via config |

## 5. Upstream connections

Per configured printer:

- `address` + `tls` + `insecure_skip_verify` select the transport. Defaults support the user requirement (plain `1883`, no certificate verification) AND the stock printer reality (`8883`, TLS, skip verify). Config examples ship with a working 8883/TLS entry; a plain-1883 entry is valid for non-standard endpoints (custom firmware, fronting relay).
- Credentials: `username` (default `bblp`) + `password` (LAN access code).
- ONE `paho.mqtt.golang` client per printer, client ID `bmbpx-<serial>` (random suffix appended on connect rejection due to duplicate ID).
- `SetAutoReconnect(true)`, `SetMaxReconnectInterval(30s)`, `CleanSession(true)`, keepalive 30 s, ping timeout 10 s. `SetResumeSubs` is NOT used — CleanSession discards broker state anyway; the pool re-issues the merged subscription set explicitly in `OnConnect` (see §5.1). On every successful (re)connect, the pool also sends the warmup command.
- Publishes are serialized through paho's thread-safe `Publish`; QoS capped at 1.

### 5.1 Connection lifecycle and retry policy

Printers are switched off, rebooted (1-2 minutes), and drop Wi-Fi. The upstream pool treats each printer as a desired-state machine; all delays are per printer and singleflighted. Verified paho behavior: mid-session loss triggers paho's internal reconnect loop starting at 1 s with exponential growth, capped by `MaxReconnectInterval` (default 10 MINUTES — the pool sets it to 30 s). Initial `Connect()` with `ConnectRetry(false)` fails once and returns; the blocking `ConnectRetry(true)` fixed-interval mode is not used because hooks must stay bounded.

| State | Meaning | Behavior |
|---|---|---|
| IDLE | No downstream subscriber has ever targeted this printer | No connection attempt. Lazy connect on first use. |
| CONNECTING | Sync attempt in flight (≤ `upstream_connect_timeout_seconds`, default 5 s) | Subscribe hooks wait up to the timeout; then refuse with SUBACK 0x80. |
| BACKOFF | Wanted, last attempt failed; retry scheduled at `min(initial × 2^n, max) ± 20% jitter` (default 1 s → 30 s cap) | Subscribe hooks refuse immediately (no waiting on a future retry). Supervisor retries until CONNECTED. |
| CONNECTED | paho session up | From here paho's AutoReconnect owns outages (1 s → 30 s cap). `OnConnect` fires per success: resubscribe merged set, then send warmup `pushall`. |

Policy details:

- **Printer switched off at subscribe time**: first `EnsureConnected` fails within the 5 s bound → SUBACK 0x80, client retries later. The supervisor keeps background retrying at capped backoff — one TCP attempt per ~30 s worst case, no tight loop, negligible load.
- **Printer reboot mid-session**: TCP drops, paho reconnect loop runs; a rebooting printer is picked up within ≤30 s of its broker returning. Reconnect triggers resubscribe + warmup `pushall`, so downstream apps converge to full state automatically — no client action needed.
- **Printer power-cut (no TCP RST/FIN)**: keepalive 30 s + ping timeout 10 s detect the dead peer; connection loss flows into the same reconnect path. Reports during the outage are lost; warmup restores full state.
- **Recovery is automatic and self-verifying**: SUBSCRIBE failures during an outage leave no half state; when the connection returns, the merged subscription set (kept by refcount) is re-issued wholesale.
- **Resubscribe failure after connect** (e.g. access code changed on the printer): logged at WARN; the next reconnect cycle retries. Backoff state transitions are logged at INFO so outages are visible in logs.

## 6. Camera and camera wall (P1/A1 series)

P1P, P1S, A1, and A1 MINI expose a camera through the Bambu chamber image protocol: TLS on printer port 6000. The client sends an 80-byte authentication payload (magic 0x40, command 0x3000, username `bblp`, LAN access code) and the printer answers with length-prefixed JPEG frames — a 16-byte header whose first little-endian uint32 is the payload length, then the JPEG. There is no login reply; the first frame follows the auth payload directly. Verified against a real P1S (1280×720 JPEG) and cross-checked against the bambuddy reference implementation.

The printer allows exactly ONE camera connection. The proxy enforces this with one capture owner per printer serial:

- Capture starts on demand (first snapshot or stream consumer) and stops after a 5 s idle grace period.
- Every consumer reads the latest immutable frame; slow clients skip frames and never block capture.
- Connection loss reconnects with capped backoff (0.5 s → 5 s) while consumers remain.

Besides the HTTP surface, the proxy runs a printer-compatible raw camera endpoint: a TLS listener on port 6000, the printer's camera port. A camera client connects, sends the 80-byte `bblp` + access-code authentication payload, and receives the native length-prefixed JPEG frame flow unchanged. No topic serial travels on this path, so the access code is the routing key: the proxy assumes printer-generated codes are unique and does not reject duplicates, but two printers sharing one access code are indistinguishable to this endpoint (MQTT serial routing is unaffected). The listener serves exactly when the camera feature is enabled — independent of `http.port`, which can stay 0 — shares the one-capture-owner rule with the HTTP consumers above, and closes before the capture manager on shutdown. Startup fails fast with a wrapped `raw camera endpoint` error when the port or its ephemeral certificate (`tlsutil.Ensure("", "")`) cannot be prepared; the deferred teardown then stops every already-started service.

HTTP surface (all on the shared `http.port` listener, unauthenticated by design):

| Endpoint | Behavior |
|---|---|
| `GET /camera/{serial}/snapshot` | Freshest frame younger than 5 s from the shared buffer; otherwise waits up to 15 s for a new frame; 503 if none arrives |
| `GET /camera/{serial}/stream` | Multipart MJPEG; complete parts flushed per frame; survives printer outages while the client stays connected |
| `GET /camera/status` | Display state for every printer (name, state, stage, filename, progress, layers, temperatures, print error, HMS alerts, AMS filament slots with the external spool, report and frame age). No credentials, no addresses |
| `GET /camera/events` | Server-sent events carrying the `/camera/status` payload: on connect, when display state changes (checked every 1 s), and at least every 10 s |
| `GET /activity` | Recent events for every configured printer, newest first |
| `GET /activity/{serial}` | Recent events for one configured printer; 404 for an unknown serial |
| `GET /camwall` | Multi-printer camera wall dashboard; composes camera images with telemetry in the browser |
| `GET /favicon.ico`, `GET /apple-touch-icon.png` | Embedded raster icons for the wall (browser probes, bookmarks, iOS home screen); the page itself inlines an SVG favicon |
| `POST /mcp` | Read-only Model Context Protocol endpoint (on by default; `BMBPX_MCP_ENABLED=false` removes it): the `list_printers`, `get_printer_state`, `get_camera_snapshot`, and `watch_printer` tools plus the subscribable `bambu://printers/{serial}/state` resource, over MCP 2026-07-28 Streamable HTTP |

The camera wall is one self-contained embedded HTML page (inline CSS, JS, and
SVG icons, including an inline SVG favicon; no external assets beyond the
raster bookmark icons served by the same binary). It receives status through `/camera/events`
(EventSource, reopened if silent for 30 s or closed while the tab is hidden) and gives the live
budget (default 4, configurable 1–16) to the focused camera first, then active
prints (RUNNING or PAUSE) and busy printers with stale reports in wall order. Other visible eligible printers get
snapshots; off-screen tiles, disconnected printers, and hidden browser tabs
hold no camera connection. Snapshot refresh defaults to 8 s and is
configurable from 2–60 s. Tiles carry icon and color state, fill the window
width, cycle through three detail levels when their details panel is clicked,
and reorder by drag or keyboard. Fleet chips filter the wall by state or
"needs attention". Print errors (`print_error`, formatted `XXXX_XXXX`) and HMS
alerts (`HMS_AAAA_BBBB_CCCC_DDDD`, severity from the code's high half) stay
visible at every detail level. A busy printer silent for 120 s shows as stale.
Kiosk mode or a setting requests a Screen Wake Lock where the origin allows it.
Order, filters, detail levels, and settings persist
only in the browser's local storage; the proxy holds no wall state. There is
no token-authenticated kiosk mode: the proxy has no user/session system, and
these camera routes are intentionally open on the configured HTTP interface.

Eligibility is checked before any camera socket is opened, with no operator configuration required: the model is inferred from the serial prefix (01P→P1P, 01S→P1S, 030→A1 MINI, 039→A1; 01P/01S verified against live printers), and an explicit `model` field overrides the inference when present. Unknown serial → 404; non-chamber-image printers (X1-class RTSP, unknown prefixes) → 422. Camera capture never affects MQTT proxying. A disabled camera feature (`BMBPX_CAMERA_ENABLED=false`) removes the routes, stops capture workers, and skips the camera wall's per-printer report subscriptions, which are otherwise held asynchronously so HTTP starts without waiting for printers.

The MCP endpoint (`internal/mcpserver`, official `github.com/modelcontextprotocol/go-sdk`, protocol 2026-07-28, stateless Streamable HTTP) serves exactly four read-only tools and one resource; there are no control methods, no sampling, no MCP Tasks, and no path from a tool call to an MQTT publish or a Gadget upload. `watch_printer` long-parks up to 30 s on a single shared one-second sampler that diffs a notification-relevant fingerprint per printer (state transitions, job changes, connectivity, real-report freshness, detection health; progress mode adds 5-point milestones) — it never consumes the detection engine's `WatchDetection` channel. Revision tokens are epoch-stamped counters captured before state reads, so a change landing mid-read leaves the token stale and forces `resync_required` instead of being silently consumed. Resource subscriptions ride `subscriptions/listen`; legacy-protocol subscribe requests are refused before touching the bounded 32-slot ceiling. Waits (32), subscriptions (32), bodies (64 KiB), and per-printer snapshot slots are capped; camera Acquire/Release balance on every path; the endpoint never outputs credentials, addresses, or raw MQTT payloads. `http.port: 0` keeps the endpoint off without failing validation.

Telemetry for `/camera/status` comes from a delta-merging cache that observes upstream reports through the pool observer hook. Merges apply only fields present in each report; P1 `pushall` warmup supplies the initial full state. The display-only AMS, external-spool, and stage fields merge before the real-print report gate and never touch detection freshness. The cache never feeds back into MQTT forwarding.

The in-memory activity log keeps the 50 newest events per configured printer
and clears on restart. It records print state changes, changes to HMS alerts
and `print_error`, upstream connect/loss events, and notable AI detection
transitions. `/camera/status` and `/camera/events` include each printer's
activity entries; the camera wall shows them at the Full detail level. The
separate `/activity` endpoints work whenever HTTP is enabled, even when the
camera feature is disabled. Report-derived events require observed reports;
the proxy does not add report subscriptions solely to populate activity.
Each entry has an increasing ID, UTC timestamp, stable event kind, severity,
display message, and server-computed age in seconds. Event ages do not cause
camera SSE updates by themselves.

Chamber temperature is model-filtered before `/camera/status` is serialized.
P1P/P1S and A1/A1 MINI do not have this sensor, and their reported
`chamber_temper` value is meaningless; the field is omitted for those models.
Only known chamber-sensor models expose it. Unknown model capability defaults
to hidden rather than displaying a possibly invalid reading.

### 6.1 Gadget AI failure detection (optional, key-only)

An optional integration with OctoEverywhere's Gadget AI failure detection
(developer docs: https://docs.octoeverywhere.com/ai-failure-detection-apis/developer-docs/overview/)
adds automatic print-failure monitoring on top of the camera pipeline. The
feature has exactly one configuration input: the `BMBPX_OCTOEVERYWHERE_API_KEY`
environment variable. There is no YAML field, no threshold settings, and no
quota bookkeeping — the proxy stays stateless and the key-only surface keeps
the opt-in unambiguous. An unset key means the feature is completely off: no
context creation, no uploads, no automatic pauses.

Setting the key is the operator's documented consent for two things: camera
snapshots leave the LAN for OctoEverywhere's servers, and the proxy may pause
a print autonomously when the service recommends it. The provider states that
images submitted through the public Gadget API are not used for AI model
training.

- **Trigger and pacing.** While telemetry shows an active print session with
  `gcode_state` RUNNING on a camera-eligible printer, a single worker per
  printer (max one request in flight) POSTs the latest captured JPEG to the
  context's `ProcessRequestUrl` as `multipart/form-data` field `image`
  (filename `print.jpg`) with the `X-API-Key` header. Frames above the API's
  6 MiB cap are never downscaled: the inspection is skipped and retried after
  backoff (P1/A1 chamber frames sit far below the cap in practice). The
  inspection timer fires on schedule regardless of report churn; each
  inspection fetches a frame captured within the last 5 s and unique to
  that inspection (sequence strictly greater than the previous one),
  waiting up to 15 s for one. The first inspection of a fresh session fires
  immediately. Normal monitoring uses
  `max(NextProcessIntervalSec.Minimum, NextProcessIntervalSec.Recommended)`.
  Intensive monitoring uses `NextProcessIntervalSec.Minimum`, which is
  currently often 5 s but is not a fixed floor. The initial monitoring period
  starts at the first active print observation and lasts at least 2 minutes.
  Layers 1–3 keep
  it active regardless of elapsed time; if the layer is unknown, the timed
  window alone applies. A PAUSE/PAUSED→RUNNING observation starts another
  2-minute window. Monitoring recovery starts one only after monitoring was
  unavailable. `FasterInspectionSuggested`, `WarningSuggested`,
  `PauseSuggested`, or quality ≤5 activates risk-intensive monitoring. It
  ends only after the timed and layer conditions end and three consecutive
  successful results, each with quality ≥6 and
  `FasterInspectionSuggested`, `WarningSuggested`, and `PauseSuggested` false,
  span at least 30 s. These are heuristics, not a guarantee of failure detection.
  Missing, failed, stale, and discarded analyses do not count as clear
  results. The context retains timing across discarded analyses, detection
  off/on toggles, and cadence changes. It also retains valid same-context API
  intervals from a discarded analysis. Before a context's first successful
  analysis, a retry waits at least 20 s or the latest provider interval,
  whichever is longer. After success, a retry waits at least
  `max(Minimum, Recommended)`, including in intensive mode. Exponential
  backoff has a 10-minute cap unless that provider interval is longer. The provider's
  pricing and free-allowance math (180 calls per print-hour) assumes the
  20-second cadence; faster intervals consume the allowance proportionally
  faster (about 125 print hours at 5 s).
- **Context lifecycle (session freshness).** Sessions derive from telemetry
  via the print identity cookie `project_id-task_id-subtask_name` (ids
  normalized to strings; `0` is a valid local-print value; an incomplete
  cookie — missing parts — is sticky for the session rather than abandoning
  the context). One Gadget context covers one print session: a genuinely new
  identity, a print end plus a new print, or a coalesced FINISH→RUNNING
  transition while RUNNING resets to a new context via the Create Context
  API (`{}` body, default confidence levels 3; the call is free), which
  returns `ContextId`, `ProcessRequestUrl`, and
  `FallbackProcessRequestUrl` — held in memory only. A pause decision is
  action-fresh only while telemetry is fresh (≤ 15 s), carries `gcode_state`
  on the current upstream connection generation, still says RUNNING, and
  the analyzed frame was captured at most 30 s before the decision — a
  temperature-only delta on a new connection can never make a
  pre-reconnect RUNNING look current. Telemetry keeps three counters: the
  session generation identifies the print and resets the context whenever
  it changes (a control state entered from outside a session, a new
  identity while a session is active, or a coalesced FINISH→RUNNING with no
  idle gap); the epoch marks every state boundary and invalidates in-flight
  results; the observation generation anchors the connection. The provider
  expires contexts after 14 days; a proxy restart always starts fresh
  contexts.
  Detection keeps results and policy timing in memory only. A restart clears
  them, and quality/age readings start empty with each new context.
- **Actions on results.** Per-printer detection state, `PrintQuality` (1–10),
  and `WarningSuggested` surface in status only (see Status below).
  `PauseSuggested = true` triggers a pause only if, at decision time,
  telemetry still shows RUNNING on fresh, current-generation reports, the
  session epoch is unchanged since the inspected frame, and the frame is
  still recent — an inspected result authorizes a pause for at most 30 s
  after the inspection returns. The proxy then sends exactly one `{"print":{"sequence_id":
  "0","command":"pause"}}` to `device/{serial}/request` on the exact upstream
  connection generation the decision was validated against (§7.2), subject to
  the same firmware constraints as any client (2025+ signed `print.*`
  firmware may reject unsigned commands). It then waits up to 30 s for a
  fresh PAUSE report; RUNNING reports during that window do not cut the
  wait short — a printer may react late. The state becomes `unconfirmed`
  only when the 30 s pass without a PAUSE report, the guarded publish
  fails, or the in-flight suggestion fails re-validation. Pauses are never
  retried or replayed, prints are never resumed automatically, and pausing
  re-arms only after a later clear result (no warning and no pause
  suggestion). The attempt is visible as `pause_state` `pending` inside the
  window and `confirmed` or `unconfirmed` afterwards (see Status below).
  `Score` (raw 0–100) is ignored.
- **Error taxonomy.** Temporary errors — network errors and timeouts, HTTP
  5xx, `OE_INTERNAL_ERROR`, `OE_BACKEND_THROTTLED`, `OE_CONTEXT_RATE_LIMITED`,
  and `OE_IMAGE_DECODE_FAILED` — retry with exponential backoff while the
  last good result stays visible as a sticky fallback with growing age and
  the printer's state shows `degraded` with reason `api_retrying` (or
  `camera_unavailable` when capture fails). Backoff has a 10-minute cap unless
  max(Minimum, Recommended) is longer than 10 minutes; rate limiting
  can raise the delay, and success resets backoff. Before the first successful
  analysis in a context, a retry waits at least 20 s. After success, a retry
  waits at least max(Minimum, Recommended), including in intensive mode. `OE_BAD_ARGS`
  and `OE_ARGS_PARSE_FAILED` block further detection for that printer until
  process restart; neither error is retried. On transport failure,
  `OE_INTERNAL_ERROR`, or an unknown error with a 5xx status, the worker
  switches to `FallbackProcessRequestUrl` and keeps it for that context's
  lifetime, per the provider's retry guidance; throttling, invalid-request,
  and account errors never switch URLs. Account errors suspend ALL detection
  account-wide until process
  restart — `OE_INVALID_API_KEY`,
  `OE_API_KEY_DISABLED`, `OE_API_KEY_BLOCKED_PAYMENT_FAILED`,
  `OE_API_KEY_IP_RESTRICTED` (never retried, never switched to the fallback
  URL), and `OE_FREE_USAGE_LIMIT_REACHED` (the proxy tracks no billing
  periods, so it cannot know when a free allowance resets) — releasing camera
  holds and keeping the last per-printer status visible. Logs and status show
  only safe, structured local reasons, not provider error text, response
  bodies, or raw URLs. Gadget failures never affect MQTT proxying, camera
  serving, or a print in progress.
- **Gates.** Detection requires the API key and the camera feature
  (`camera.enabled`, default on). With the key set but cameras explicitly
  disabled nothing runs — zero API or camera activity, plus a startup
  warning — and every printer's `detection` object is still served with
  state `blocked` (reason `camera_disabled`), so the configuration is
  visible rather than silent. With no key the proxy behaves exactly as
  before, byte-for-byte, down to which MQTT report interests are
  subscribed. The HTTP listener is not required: with `http.port: 0`,
  detection, inspection, and pausing all run; only the status/SSE endpoints
  are absent.
- **No quota tracking.** The proxy keeps no counters and renders no usage
  forecasts. The provider enforces the allowance server-side: 90,000 free
  inspection calls per monthly billing period (≈ 500 print hours at the
  pricing-assumption 20 s cadence, ≈ 125 at 5 s), shared across the whole
  account, with **Free Usage Only** enabled by default so inspections stop
  at the allowance instead of billing.
- **Status.** On `/camera/status` and the `/camera/events` SSE feed
  (identical payload), every printer tile carries a `detection` object
  (omitted entirely when the feature is off): `state` — `starting`, `idle`,
  `monitoring`, `attention`, `paused`, `degraded`, `unsupported`, or
  `blocked` — and `pause_state` (`none`, `pending`, `confirmed`, or
  `unconfirmed`) are always present, and the omitempty fields `quality`,
  `warning`, `age_seconds` (seconds since the last result; relative age
  only, no absolute timestamp), `next_check_seconds`,
  `last_inspected_layer`, `faster_inspection`, `camera_lost`,
  `using_fallback`, `suspended`, `reason` (a short machine code such as
  `camera_disabled`, `camera_unsupported`, `awaiting_reports`,
  `awaiting_fresh_telemetry`, `camera_unavailable`, `api_retrying`,
  `pause_unconfirmed`, or `pause_command_failed`), and `message` appear when
  set. `paused` is the print state, while `pause_state` tracks a Gadget
  pause attempt: `pending` inside the 30 s confirmation window, `confirmed`
  once the printer's PAUSE report lands, `unconfirmed` when the window
  expires, the publish fails, or the print resumes without confirmation.
  `last_inspected_layer` is the layer observed when the inspected frame was
  captured (the snapshot is taken at inspection start), not after upload.
  `degraded` marks a temporary failure with the sticky last result (reason
  `camera_unavailable` or `api_retrying`); `unsupported` marks non-camera
  printer models. While an account-level suspension is active, the object
  carries `"suspended": true`, and on `/camera/status` and `/camera/events`
  the payload adds top-level `detection_suspended` and `detection_message`.
  The health `/status` payload gains a top-level `detection` map keyed by
  serial, holding the same complete per-printer object for every configured
  printer; the field is present only when the API key is configured —
  omitted entirely, no empty map, when the feature is off, so the no-key
  `/status` payload stays byte-compatible — and suspension shows only
  inside each serial's object. There is no separate endpoint.
- **Security.** The key exists only in the environment: never logged, never
  exposed in any JSON endpoint, never written to disk, and never present in
  YAML examples. Uploads go only to the two URLs returned by Create Context
  over TLS; both URLs are validated at creation (HTTPS, vendor host,
  no userinfo, fragment, or non-default port), redirects are disabled, every
  request is bounded by a 15 s timeout and a 64 KiB response cap, responses
  must satisfy the documented contract (quality in 1–10, required warning
  and pause booleans, positive intervals retained as provider timing floors), and error
  details are sanitized — no provider text, raw URLs, or response bodies
  reach logs or status.
## 7. Routing model (core)

Topic grammar: `device/{serial}/report` and `device/{serial}/request`. The serial is the second level. Any other topic shape is denied/dropped and logged. This routing model covers MQTT only: the raw camera endpoint (§6) carries no topics and routes by the access code instead.

### 7.1 Subscribe path (downstream → upstream)

```
client SUBSCRIBE device/{sn}/report
  → OnACLCheck hook:
      grammar check:  filter must be device/{serial-or-wildcard}/report|request
        off-grammar or unknown sn → deny → mochi sends SUBACK 0x80 for that
                                    filter only (v3-clamped), connection stays open
        ok                        → accept, then OnSubscribed hook:
              exact serial   → upstream.EnsureConnected(sn) (singleflight, ≤5 s)
              serial wildcard (device/+... or device/#) → EnsureConnected on ALL printers
              subref[printer][filter]++ (first → paho Subscribe with the filter AS-IS)
```

- No filter expansion: each printer connection serves ONE serial, so a wildcard filter is forwarded to the printer broker as-is (`+` matches its serial); mochi's topic trie handles fan-out on the downstream side.
- Subscription merging: N downstream clients on the same filter produce ONE upstream subscription per printer (refcount N). The single upstream connection is the point of the design.
- Upstream subscriptions cover REPORT-leaf filters only (`device/{sn}/report`, `device/+/report`). Request filters are accepted downstream but never subscribed upstream: the printer broker would echo proxied requests back to the proxy and out to other clients (verified in integration testing).
- Availability gating (refuse with SUBACK 0x80) applies to wildcard subscribe filters and is bounded by the connect timeout; exact-serial subscribes during a full outage are accepted locally and backfilled by the reconnect warmup — no client retry needed.
- The connect path inside the hook is bounded (config `upstream_connect_timeout_seconds`, default 5) and singleflighted per printer: concurrent subscribers share one in-flight attempt; while the supervisor sits in BACKOFF the hook refuses immediately instead of waiting.

### 7.2 Publish path (downstream → upstream)

```
client PUBLISH device/{sn}/request
  → OnPublish hook:
      sn unknown or off-grammar → deny (mochi ACL: QoS 0 dropped, QoS ≥ 1 disconnects)
      known sn                  → upstream.Publish(same topic, same payload, QoS≤1, retain stripped)
                                  → return packets.CodeSuccessIgnore
```

`CodeSuccessIgnore` (verified in mochi source) suppresses LOCAL fan-out — requests never reach other downstream clients — while the normal QoS flow completes: the publishing client gets its PUBACK. This matters: denying the packet instead would withhold PUBACK, the client would retransmit, and the printer would receive DUPLICATE commands.

- Payload bytes pass through untouched (signing-compatible); only the retain bit is stripped.
- The upstream publish is fire-and-forget: PUBACK is issued after the bytes are handed to paho, not after printer acknowledgment. If the printer is unreachable at that moment, the command is lost and the app retries at application level — the same behavior an app sees when a printer drops mid-command today.

### 7.3 Report path (upstream → downstream)

```
printer PUBLISH device/{sn}/report
  → paho MessageHandler on the sn's upstream connection
      → server.Publish(topic, payload, retain=false, qos=1)   (mochi inline client)
      → mochi fans out to every subscribed downstream client
```

Mochi owns per-client QoS delivery and PUBACK handling. Retain is never set: Bambu reports are ephemeral state, and stale snapshots must not survive reconnects.

Serial-level wildcard subscribers (`device/+/report`) receive reports from ALL printers — inherent to the single-endpoint design. Clients demux by the serial in the topic, which every Bambu app already parses.

### 7.4 Unsubscribe / disconnect reconciliation

- `OnUnsubscribe`: decrement refcount; at zero, upstream unsubscribe.
- `OnDisconnect`: drop that client's filter set (tracked per client id at subscribe time), recompute all refcounts, upstream-unsubscribe any filter that reached zero. This covers ungraceful TCP drops, which never send UNSUBSCRIBE.

### 7.5 Warmup (state correctness for P1)

P1 printers push deltas only after the first `pushall`. The proxy sends `{"pushing":{"sequence_id":"0","command":"pushall"}}` to `/request` (a) when the first downstream client subscribes to a serial, and (b) after each upstream reconnect. Configurable (`warmup_commands`, default `["pushall"]`). Every subscriber — including late joiners — then receives full state.

## 8. QoS, keepalive, ordering

| Concern | Decision |
|---|---|
| QoS | Cap all hops at QoS 1. Bambu uses 0/1; QoS 2 adds state for no benefit. |
| Downstream keepalive | Per client, owned by mochi. |
| Upstream keepalive | 30 s, owned by paho. |
| Ordering | Per serial, upstream publish and report delivery are FIFO (single paho connection). Cross-serial ordering is not guaranteed and not needed. |
| Message loss windows | Upstream disconnect: reports in flight are lost; next warmup `pushall` restores full state. Accepted for v1. |
| Duplicate suppression | Exactly ONE upstream publish per downstream publish; the broker hop completes the QoS flow so clients never retransmit into the proxy. |

## 9. Configuration

Single YAML file. Secrets sit next to the binary in a homelab deployment; no env expansion in v1.

```yaml
listen:
  - port: 8883              # default: TLS, self-signed cert (generated if missing)
    tls: true
    cert_file: ./certs/proxy.crt
    key_file: ./certs/proxy.key
  # - port: 1883            # optional extra listener for custom dashboards
  #   tls: false

auth:
  mode: printer             # require username bblp + a configured access code; or: accept_all

printers:
  - serial: "01P00A123456789"
    # name: "Garage P1S"      # optional; camera wall label, never used for routing
    # model: "P1S"            # optional; camera support is inferred from the serial prefix
    address: "192.168.1.42:8883"
    tls: true
    insecure_skip_verify: true
    username: "bblp"
    password: "12345678"
  - serial: "01P00A987654321"
    address: "192.168.1.43:1883"   # non-standard endpoint: plain MQTT, no TLS
    tls: false
    username: "bblp"
    password: "87654321"

behavior:
  qos_max: 1
  warmup_commands: ["pushall"]
  upstream_keepalive_seconds: 30
  upstream_connect_timeout_seconds: 5
  upstream_backoff_initial_seconds: 1   # doubles per failure, ±20% jitter
  upstream_backoff_max_seconds: 30      # also set as paho MaxReconnectInterval

log:
  level: info               # debug shows per-packet routing decisions
```

HTTP and camera configuration (shared listener; see §6):

```yaml
http:
  port: 8080                # 0 disables the HTTP server entirely
camera:
  enabled: true             # false removes camera routes and the camera wall
mcp:
  enabled: true             # default; false removes the read-only MCP endpoint at /mcp
```

Validation at startup: unique serials, resolvable addresses, TLS flag consistency, HTTP port range. Startup fails fast on invalid config.

The config file contains printer access codes in plain text. Deploy with restrictive file permissions (e.g. chmod 600) and never log passwords; connection logs redact credentials.

## 10. Package layout and dependencies

```
cmd/bambu-mqtt-proxy/main.go   flags, config load, wiring, graceful shutdown
internal/config/               schema, defaults, validation
internal/broker/server.go      mochi server, TLS listeners, cert bootstrap
internal/broker/bridge.go      routing hooks + per-client filter tracking + refcounts
internal/upstream/pool.go      serial → connection table, lazy connect, reconnect/resubscribe
internal/upstream/conn.go      paho client wrapper (connect, publish, merged subscribe)
internal/routing/topic.go      serial extraction, wildcard expansion, filter↔serial sets
internal/telemetry/            delta-merging display-state cache fed by upstream reports
internal/camera/               P1/A1 chamber-image capture, raw TLS camera endpoint, snapshot/stream/camera wall handlers
internal/mcpserver/            read-only MCP endpoint (tools, state resource, subscriptions, shared sampler)
internal/httpsrv/              shared health + camera HTTP listener
internal/tlsutil/              self-signed certificate generation and persistence
```

Dependencies: `github.com/mochi-mqtt/server/v2`, `github.com/eclipse/paho.mqtt.golang`, `gopkg.in/yaml.v3`, plus stdlib. Estimated ~1,200 LOC; static binary ≈ 12 MB; cross-compiles with plain `go build` for linux/darwin/arm64/amd64.

## 11. Failure modes and limitations

| Failure | Behavior | Consequence |
|---|---|---|
| Printer offline at startup | HTTP starts immediately; camera wall report interests are recorded asynchronously and restored on reconnect | No startup delay proportional to printer count |
| Printer offline at subscribe time | SUBACK 0x80 for that filter; supervisor keeps background retrying at capped backoff | Client retries when printer returns; no proxy-side queue; no tight retry loop |
| Printer reboot (1-2 min) | paho reconnect (≤30 s between attempts), resubscribe merged set, warmup pushall | Reports during the gap are lost; state converges automatically after pushall |
| Printer power-cut without TCP close | keepalive 30 s + ping timeout 10 s detect the dead peer | Same reconnect path as reboot |
| Printer IP changed (DHCP) | Connect fails indefinitely at capped rate | WARN logs; operator updates config |
| Downstream client crash | OnDisconnect reconciles refcounts; empty filters unsubscribe upstream | None |
| Duplicate serial in config | Startup validation error | Operator fixes config |
| Two clients send conflicting commands concurrently | Both go upstream; both responses broadcast to all report subscribers | Clients must tolerate each other's responses (sequence_id is per client). Same limitation as a shared Mosquitto bridge. |
| Printer requires signed `print.*` (2025+ firmware) | Proxy forwards signed payloads untouched | Signing is the client's responsibility; LAN Mode + Developer Mode as required by the printer settings |
| Proxy restart | Stateless; clients reconnect, subscriptions re-established, warmup re-sent | Brief gap, then full state via pushall |
| A1 series printer (single-client limit) | Proxy holds one upstream connection regardless of model | The printer's limit stops affecting apps entirely |
| Camera requested for unknown serial or non-P1/A1 model | 404 / 422 before any camera socket opens | Supported models: P1P, P1S, A1, A1MINI; X1-class RTSP cameras are not implemented |
| Camera connection drops mid-stream | Capture reconnects with 0.5–5 s backoff while consumers remain; streams resume | Brief frame gap; slow clients skip frames instead of stalling capture |
| Raw camera port 6000 busy at startup (second proxy instance, another service) or its certificate cannot be created | Startup fails fast with a wrapped `raw camera endpoint` error after the deferred teardown stops every already-started service | Operator frees the port or sets `camera.enabled: false`; no half-started services remain |
| Unauthenticated HTTP exposure | Camera, camera wall, status endpoints, and the `/mcp` endpoint (default on) have no login | Anyone who can reach `http.port` sees cameras, telemetry, and MCP camera snapshots/state; bind accordingly (state responses carry no credentials); set `BMBPX_MCP_ENABLED=false` to remove the MCP surface |
| Bambu Studio "add printer by IP" | Studio probes camera/FTP ports in addition to MQTT; probe failure can block discovery | MQTT control and status work; camera/FTP passthrough is future work |
| Tools that pin the printer TLS certificate | The self-signed proxy cert fails pinning | Load a custom cert/key via config, or disable pinning (clients must already skip verify against the real printer) |
| Downstream QoS 1 command while printer offline | PUBACK was already issued; command does not reach the printer | App-level retry, identical to a direct-connection drop |
| Malicious/buggy client publishes to `/report` | Denied by ACL | QoS 0 silently dropped; QoS ≥ 1 client disconnected; other clients never see injected state |
| Gadget API unreachable or transiently failing (optional integration) | Inspection retries use exponential backoff (20 s initial, 10 min cap, jittered), with the latest provider interval as a floor; the last good result stays visible; the context switches to the fallback URL on connection/server failure | Detection lags; printing, MQTT proxying, and camera serving are unaffected |
| Gadget bad-arguments error (`OE_BAD_ARGS`, `OE_ARGS_PARSE_FAILED`) | Detection for that printer is blocked until proxy restart | Inspection and automatic pauses stop for that printer; other printers are unaffected |
| Gadget account error: invalid/disabled key, billing failure, IP restriction, or free allowance exhausted | Detection suspends account-wide until proxy restart; logs use safe structured reasons | Warnings and automatic pauses stop; everything else is unaffected |
| Gadget-triggered pause rejected or lost (printer offline, signed firmware) | One pause command per suggestion (§7.2); up to 30 s wait for a confirming PAUSE report; no retry, no auto-resume | Pause failure is logged; the print continues; pausing re-arms after a later clear result |

## 12. Alternatives considered

1. **Mosquitto relay per printer** (disconn3ct pattern): proven, but N printers = N broker endpoints and configs; not Go; no single endpoint. Rejected against requirement (c).
2. **Transparent TCP/TLS proxy per printer** (pandaproxy pattern): no MQTT-level merge; one upstream per client defeats requirement (a) and cannot route by serial.
3. **Scraper/republish bridges** (bambu-bridge, Bambu-MQTT-Scraper): change topic semantics; not transparent for Bambu Studio.
4. **Hand-rolled MQTT server**: removes mochi dependency but re-implements protocol edge cases (varint lengths, QoS flows, keepalive) with real correctness risk for zero user-visible gain.
5. **mochi for both directions**: impossible — mochi is a server only; the printer side needs a client library (paho).

## 13. Verification plan

1. **Unit**: filter resolution tables (exact serial → one printer, serial wildcard → all printers, off-grammar/unknown → deny); refcount transitions (first subscribe → upstream sub, last unsubscribe → upstream unsub, disconnect reconciliation); deny paths (unknown serial, off-grammar topic).
2. **Integration** (fake printer): run a mochi broker with `bblp` auth + TLS as a stand-in printer; drive 3 fake clients; assert merged upstream subscriptions, report fan-out to exactly the subscribed clients, SUBACK 0x80 on unknown serial, warmup pushall on first subscribe and after forced upstream reconnect. Assert printer-parity behavior: CONNACK code 4 on wrong access code, QoS cap on SUBACK grants, retain stripping, same-ID session takeover. Assert routing correctness: exactly ONE upstream publish per downstream QoS 1 publish (no retransmit duplicates), `device/+/report` fans out from all printers, publishes to `/report` are denied, requests never fan out to other downstream clients. Assert availability lifecycle: SUBACK 0x80 while the printer is stopped, background retries follow the configured backoff schedule (no tight loop), and the proxy reconnects, resubscribes, and sends warmup pushall when the printer returns — with no client action.
3. **Camera unit/integration** (fake TLS camera on :6000): auth payload shape (80 bytes, 0x40/0x3000, `bblp` at 16, code at 48); frame header parsing, length bounds, JPEG SOI/EOI validation; eligibility gates (unknown serial → 404, unsupported/missing model → 422, no socket opened); snapshot freshness and shared-buffer reuse; MJPEG framing and slow-client isolation; concurrent consumers sharing one upstream session; HTTP startup while printers are offline (non-blocking interests).
4. **Smoke (manual, performed)**: real P1S in LAN Mode at `10.10.20.141` — MQTT upstream connected on first attempt with `pushall` warmup; snapshot returned a valid 1280×720 JPEG (~87 KB); 4-second stream sample contained 3 complete multipart JPEG parts; 404/422 gates answered without opening a camera socket; overlay served. This smoke run caught and fixed a real defect: the camera endpoint derived from the MQTT port instead of always using 6000.
5. **Smoke (manual, remaining)**: Bambu Studio and Home Assistant connected through the proxy on `:8883` with the printer's access code, observing `pushall` warmup and delta flow in debug logs.
6. **Gadget integration (optional feature)**: designed coverage — unit and integration tests against a fake Gadget HTTP server for create-context URL validation, response contract enforcement (quality bounds, required flags, and positive interval validation), error taxonomy (per-printer bad-arguments blocking, account-wide suspension, transient backoff, and fallback-URL switch), local 6 MiB frame rejection, policy timing and interval retention across discarded analysis and detection toggles, backoff reset on success, and status-object shape. No real-printer smoke and no live-API call back the detection feature, and no real printer report fixtures are available; hardware behavior is unverified.
7. **Cmd wiring** (subprocess, real `run()`): cameras enabled with `BMBPX_HTTP_PORT=0` must answer a TLS handshake on 127.0.0.1:6000 and exit cleanly on SIGTERM; `BMBPX_CAMERA_ENABLED=false` must serve health while port 6000 is held, proving it never binds; a pre-held port 6000 must fail startup with the wrapped `raw camera endpoint` error.
8. **MCP unit/wire acceptance** (`internal/mcpserver`): tool discovery and typed calls against the SDK client and raw HTTP; subscription acknowledge/update/cancel over `subscriptions/listen`; legacy-protocol subscribe refusal (2025-06-18, 2025-11-25, headerless, and current-header-without-`_meta` shapes) with zero slot usage; revision-before-state ordering with deterministic mid-read injection; camera acquire/release balance; freshness ACK exclusion; cancellation releasing waits and camera interests; body/origin/limit rejections.

## 14. Future work (explicitly out of scope for v1)

- X1-class RTSP camera support (rtsps :322) and FTPS (990) TCP passthrough, completing Bambu Studio's add-by-IP probe path.
- Per-client serial ACL binding (a client may only touch the serial matching its access code).
- Optional Prometheus metrics endpoint.
- On-disk subscription persistence across proxy restarts.

## 15. Container deployment

The image is multi-arch (`linux/amd64`, `linux/arm64`) and stateless by default: the self-signed certificate is generated in memory per start (clients skip verification, as they do against printers), and no volumes are required. Operators who want a stable certificate mount a writable directory and set `BMBPX_CERT_FILE`/`BMBPX_KEY_FILE`.

### 14.1 Configuration sources

Configuration comes from a YAML file, environment variables, or both; environment overrides replace file values per field. With no file, the proxy runs from environment alone.

| Variable | Default | Meaning |
|---|---|---|
| `BMBPX_PRINTERS` | — | Semicolon-separated printer entries: `serial=SN,address=host:port,password=code[,name=label][,username=bblp][,tls=true][,insecure_skip_verify=true]`. The optional `name` is a display label for the camera wall; MQTT routing always uses the serial, while the raw camera endpoint routes by access code (§6). |
| `BMBPX_LISTEN_PORT` | `8883` | Downstream MQTT port |
| `BMBPX_LISTEN_TLS` | `true` | TLS on the downstream listener |
| `BMBPX_CERT_FILE` / `BMBPX_KEY_FILE` | empty | Empty = ephemeral in-memory self-signed certificate |
| `BMBPX_AUTH_MODE` | `printer` | `printer` or `accept_all` |
| `BMBPX_LOG_LEVEL` | `info` | `debug` logs routing decisions |
| `BMBPX_HTTP_PORT` | `8080` | Shared health + camera HTTP port (0 off) |
| `BMBPX_CAMERA_ENABLED` | `true` | Camera endpoints, camera wall, and the raw camera listener on port 6000 |
| `BMBPX_MCP_ENABLED` | `true` | Read-only MCP endpoint at `/mcp` (off when `http.port` is 0) |
| `BMBPX_OCTOEVERYWHERE_API_KEY` | empty | Empty = off; set to an OctoEverywhere Gadget API key to enable AI failure detection (consents to external snapshot upload and automatic pause; see §6.1) |

Behavior tuning (`behavior:` in YAML) has no environment surface — its defaults match the design.

### 14.2 Health monitoring

The shared HTTP server (config `http.port`, env `BMBPX_HTTP_PORT`) serves:

- `/livez`, `/readyz` — `200 ok` once the process serves. Upstream connectivity deliberately does NOT affect readiness: clients stay connected while printers recover, and readiness must not flap with printer power states.
- `/status` — JSON `{"status":"ok","upstreams":{"<serial>":true|false}}` for dashboards and scrape jobs.

The image runs as a non-root user and ships a `HEALTHCHECK` probing `/livez` (interval 30 s, timeout 3 s, start period 5 s).

### 14.3 Build and run

```sh
docker buildx build --platform linux/amd64,linux/arm64 -t bambu-mqtt-proxy .

# env-only (stateless):
docker run -d -p 8883:8883 -p 6000:6000 -p 8080:8080 \
  -e BMBPX_PRINTERS='serial=SN,address=printer.lan:8883,tls=true,password=CODE' \
  bambu-mqtt-proxy

# config file:
docker run -d -p 8883:8883 -p 6000:6000 -p 8080:8080 \
  -v /path/to/bambu-mqtt-proxy.yaml:/config/bambu-mqtt-proxy.yaml:ro \
  bambu-mqtt-proxy
```
