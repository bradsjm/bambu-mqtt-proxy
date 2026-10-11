# bambu-mqtt-proxy — Design

## 1. Purpose

Many Bambu integrations (Bambu Studio, Home Assistant, xtouch, OctoPrint plugins) each open their own MQTT connection to a printer. The printer tolerates few connections, and each client must know every printer address. This proxy removes both problems:

- Clients connect to ONE MQTT endpoint.
- The proxy holds ONE MQTT connection per configured printer.
- The proxy routes traffic by the serial number in the topic path (`device/{serial}/...`).

### Goals

1. Accept downstream MQTT clients on port 8883 with TLS and a self-signed certificate.
2. Pose as a printer: MQTT behavior, authentication, and TLS match the printer's embedded broker, so existing apps work without changes.
3. Hold exactly one always-on upstream MQTT connection per configured printer.
4. Route publishes and subscriptions bidirectionally by serial number.
5. Stay transparent for clients: forward client payloads byte-for-byte. The proxy parses printer reports only to display them and sends only its own fixed commands.
6. Ship as one small static Go binary with a YAML config file.
7. Serve live camera snapshots, MJPEG streams, and a browser camera wall over one shared HTTP port for P1/A1-series printers (native chamber protocol) and X1/P2S/H2-series printers (RTSPS converted server-side with FFmpeg), and expose the P1/A1 camera also through a printer-compatible raw TLS endpoint on camera port 6000.
8. Make every view scannable at a glance: spend visual space on the most useful information for that view first, and represent state with icons, color, and visual elements (progress bars, gauges, swatches, badges) instead of long text wherever they carry the same meaning.

### Non-goals

- Raw RTSP/RTSPS TCP passthrough (printer port 322), FTPS proxy (port 990), file transfer. (Web-wall RTSPS *consumption* for X1/P2S/H2-series is in scope; see §6.)
- Cloud (Bambu Cloud) bridging or cloud authentication.
- Payload transformation, filtering, or command signing.
- Message persistence across proxy restarts; retained-message storage.
- MQTT v5-only features; clusters; horizontal scaling.

## 2. Research summary

### 2.1 Go MQTT broker libraries

| Library | Verdict | Evidence |
|---|---|---|
| **mochi-mqtt v2** (`github.com/mochi-mqtt/server/v2`) | **Selected** — embeddable broker, MQTT v5 + v3.1.1 hybrid, TLS listeners, stackable hooks (`OnConnectAuthenticate`, `OnACLCheck`, `OnPublish`, `OnConnect`, `OnWill`), inline client (`server.Publish`), MIT, active project, passes Paho interop tests | https://github.com/mochi-mqtt/server |
| volantmq and similar | Stale or inactive; no advantage over mochi | — |
| Hand-rolled MQTT 3.1.1 server on `net` | Rejected: re-implements varint encoding, QoS flows, keepalive; mochi removes that risk | — |

### 2.2 Go MQTT client libraries (upstream)

| Library | Verdict | Evidence |
|---|---|---|
| **paho.mqtt.golang** (`github.com/eclipse/paho.mqtt.golang`) | **Selected** — MQTT 3.1/3.1.1, `SetAutoReconnect`, `SetConnectRetry`, TLS via `tls.Config` (works with `InsecureSkipVerify`), mature | https://github.com/eclipse/paho.mqtt.golang |
| paho.golang (`github.com/eclipse/paho.golang`) | Rejected: MQTT v5 only; printer broker is v3.1.1 | https://github.com/eclipse/paho.golang |

### 2.3 Bambu P1S MQTT protocol (LAN)

- Printer runs an MQTT broker at `mqtt://{PRINTER_IP}:8883`, MQTT over TLS, self-signed certificate; all community clients skip certificate verification.
- Username `bblp`, password = the printer's LAN access code (Settings → Network → LAN Mode).
- LAN Mode must be ON. On 2025+ "authorization-control" firmware, `print.*` commands must be RSA-SHA256 signed by the client; forwarding stays byte-exact, so signing stays a client concern.
- Topics: `device/{serial}/report` (printer → clients), `device/{serial}/request` (clients → printer).
- Handshake on subscribe: clients publish `{"pushing":{"sequence_id":"0","command":"pushall"}}` to get full state. P1 series sends only deltas afterwards; a fresh subscriber MUST trigger `pushall` to see full state.
- Commands (`get_version`, `pushall`, `project_file`, `pause`, `gcode_line`, `ledctrl`, ...) are JSON on `/request`; responses and state arrive as JSON on `/report`.
- Observed broker behavior (community-verified; not formally documented by Bambu): MQTT 3.1.1, clean session, keepalive 30-60 s, QoS 0 only (the ESP32 broker supports nothing higher), unique client IDs per connection. Concurrent-connection limits are real and model-dependent: about 4 on P1/X1, ONE on A1 series. Without Developer Mode the broker accepts connections but silently drops write commands. The TLS certificate is Bambu-issued with CN = printer serial; all community clients disable verification.
- Stock firmware exposes NO plain-text MQTT port 1883. `tls` is per printer in the config because non-standard endpoints (custom firmware, fronting relay) use plain MQTT.
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
       :8883 MQTTS       :1883 MQTT (non-standard)
```

Four parts, one process:

1. **Downstream broker** — mochi-mqtt v2 with TLS listeners. Mochi owns all MQTT protocol mechanics for clients (CONNECT/CONNACK, keepalive per client, SUBACK, session takeover).
2. **Upstream links** — one always-on paho.mqtt.golang client per configured printer, dialed at boot, with automatic reconnect.
3. **HTTP service** — one `net/http` listener (`http.port`, default 8080) for health, `/config`, the camera endpoints, the camera wall, and MCP.
4. **Raw camera endpoint** — a TLS listener on printer camera port 6000 serving the chamber-image protocol to camera clients; the access code a client authenticates with selects the printer (§6). It starts whenever cameras are enabled.

A custom mochi hook is the only coupling between the broker and the links. The hook routes packets; it does not rewrite them. The document store observes reports without changing forwarding.

### 3.1 Modules

The core is the MQTT proxy path (§4–§8), the printer store, cameras, the activity log, the allow-listed control service, the shared HTTP listener and health, the camera wall and `/config` page shells, and the MCP endpoint with its core tools. Every other capability is a module: one package that owns the capability and declares its hooks with one `module.Module` value (`internal/module`). The core holds no capability-specific fields, imports, or branches; a module serves everything under its own name.

`serve` constructs each enabled module with only the narrow core services it needs: the printer store and its subscriptions, the camera frames, `control.Service` for user-style commands, `printer.Links` command helpers for autonomous actions, and the activity log. It collects the declarations into one explicit list, and `module.Check` rejects empty or duplicate names and route patterns without an HTTP method. Modules are compile-time: no plugin loading, no registry of constructed modules.

| Hook | What the core does with it |
|---|---|
| `Run` | Runs in list order after the links dial, in its own goroutine with its own cancel; cancels and joins in reverse order at shutdown |
| `State` | Serves one per-printer value as the wall tile member `<name>` and as MCP `state.modules.<name>` |
| `Display` | Draws badges over the camera image or a titled panel in the tile details, under the tile's `modules` key |
| `StatusValue` | Adds the `/status` member `<name>` |
| `Routes` | Mounts HTTP handlers on the shared listener; `module.Mount` adds cross-origin protection to every write route |
| `MCP` | Calls the hook once while building the MCP server; the module registers its tools with `mcp.AddTool` and the helpers in `internal/module/mcp.go` |

Read hooks and tool handlers run on HTTP, worker, and report goroutines at once. They can also run after shutdown starts, so they must be safe for concurrent use, non-blocking, free of network work, and safe after `Run` returns. `State` and `Display` return nil when the module has nothing to show for a printer. Modules never publish raw MQTT and never change forwarded payloads.

Module configuration has two generic forms, both defined in the module package and registered in `cmd/bambu-mqtt-proxy/settings.go` at program initialization, because the `/config` store outlives `serve` and setup mode runs with no modules:

- A per-printer option is a `config.PrinterSetting` (key, page label, hint, validation). It stays at the printer level in YAML (for example `panda_breath:`) and as a field in the `/config` printer dialog. Unknown printer keys still load and are ignored, whatever their YAML shape.
- A top-level section is a `config.Section`. It owns the section's YAML shape, effective validation, its `/config` API member and secret handling, and an optional `POST /config/{key}/test` action.

MCP follows the same ownership. A module's per-printer data reaches agents only through `State`; `internal/mcpserver` has no module types. A module registers a tool only when it can serve it, so tool presence follows the running configuration; a configuration save rebuilds the server. Module tools check the serial first, return expected failures as typed output with an `error` member and `IsError`, and use unique names, because the SDK replaces a tool with the same name silently.

| Module | Runs when | Hooks | Surfaces |
|---|---|---|---|
| `notification` | `notifications.enabled` (a `config.Section` owned by the module) | `Run` | Pushover messages (§9), `POST /config/notifications/test` |
| `jobpreview` | Cameras or MCP on | `Run`, `State`, `Display`, `Routes`, `MCP` | Tile and `state.modules.jobpreview`, `get_job_preview`, `GET /camera/{serial}/preview` |
| `pandabreath` | Always constructed; serves printers with `panda_breath` set | `Run`, `State`, `Display` | Wall panel and badge, accessory chamber reading, `state.modules.pandabreath` |
| `pandapwr` | Always constructed; polls printers with `panda_pwr` set | `Run`, `State`, `Display` | Wall panel, `state.modules.pandapwr`; no MCP tool |
| `detection` | Key set or explicit enable (§6.1) | `Run`, `State`, `Display`, `StatusValue`, and `Routes` + `MCP` while unblocked | Tile `detection`, `/status` `detection`, `PUT /detection/{serial}`, `state.modules.detection`, `set_ai_monitoring` |
| `platecheck` | Endpoint and key set, or cameras on (diagnostics) (§6.2) | `Run`, `State`, `Display`, `StatusValue`, and `Routes` while unblocked | Tile `platecheck` and panel (startup and first-layer), `/status` `platecheck`, `POST /platecheck/snapshots` (startup only), `state.modules.platecheck` |
| `safety` | `safety.enabled` (a `config.Section` owned by the module; requires the `notifications` Pushover credentials) | `Run` | The `safety_overtemp` and `safety_power_off` activity entries, one `stop` to every printer, one Panda PWR power-off, one Pushover emergency alert |

Two kinds of module-specific markup remain in the embedded pages, because the generic hooks cannot express them: the interactive detection controls in `camwall.html` and the section forms in `config.html`. The notification module's summaries for other modules' activity kinds (`ai_*`, `platecheck_*`, `first_layer_complete`) are accepted module-to-module knowledge. The safety module calls `notification.SendEmergency` and `pandapwr.PowerOff` directly; that is accepted module-to-module knowledge too.

Safety is a best-effort backup to the printer firmware's thermal protection. Once per second it compares the cached nozzle, bed, and chamber readings of each online printer with a per-model limit table (the rated maximum plus a margin). The latch holds through disconnects and clears only when a connected printer's readings are all below the limits and fully reported, so a value cached before a disconnect trips only if the printer reported it over the limit. The chamber is the native sensor, or the latest accessory reading on models without one; a stale accessory value keeps its last reading, so losing the device never re-arms a tripped printer. On a trip it records `safety_overtemp` and sends one `stop` to every printer, one Panda PWR power-off for the tripped printer when configured, and one Pushover emergency alert listing the outcome.

Panda Breath shows a wall panel with the device link (connected, connected with no readings, or offline), the device's own chamber reading while fresh (15 s), and a trend over up to three minutes of spaced samples; a "Chamber warming" badge appears on the camera image only while the chamber rises by at least 2 °C over at least two minutes. Everything derives from the live device stream: the device sends its settings only once per connection, so the module shows no settings and opens no extra connections.

Panda PWR polls a BigTreeTech smart plug over HTTP for the printer's power draw: one GET per printer per 30 s tick, a reading staying fresh for 90 s. A wall panel shows the device link (connected while a reading is fresh, otherwise offline) and, while fresh, the power in whole watts; `State` serves `link` and, while fresh, `power_w`.

Detection keeps its own `detection:` section, the enable-requires-key rule, and the key test in `internal/detection` as a `config.Section`. While blocked (cameras off) it serves its blocked state but registers neither `PUT /detection/{serial}` nor `set_ai_monitoring`.

## 4. Downstream endpoint (printer-compatible)

- Listener: one entry per `listen` item, default TCP + TLS on `:8883` — the port every Bambu app already targets. Certificate: self-signed, ECDSA P-256, 10-year validity; generated on first start and persisted to `cert_file`/`key_file` paths when both are set, otherwise generated in memory. Apps skip certificate verification exactly as they do against the printer; the file paths also let an operator load a specific cert for tools that pin.
- Authentication, default mode `printer`: CONNECT must carry username `bblp` and a password matching ANY configured printer access code; otherwise the proxy refuses with CONNACK "bad username or password" (v3.1.1 code 5 — mochi's standard denial). Mode `accept_all` remains available for unusual clients. Enforced in `OnConnectAuthenticate`.
- Access control: SUBSCRIBE is allowed for `device/{configured serial}/report`, `device/{configured serial}/request`, and serial-level wildcards over configured printers. PUBLISH is allowed ONLY to `device/{configured serial}/request`. This blocks clients from injecting fake reports into other clients — no legitimate Bambu app publishes to `/report`. Enforced via mochi's `OnACLCheck`: an off-grammar or unknown-serial filter gets SUBACK 0x80, and a denied QoS 0 publish is dropped silently (mochi's behavior).
- Aliases: each printer has one downstream ID — its `alias` when set, otherwise its `serial`. The bridge rewrites the request topic's ID to the real serial upstream, and the injector rewrites the report topic's serial to the downstream ID; payloads pass through unchanged. While an alias is set, the real serial is refused downstream. Wildcard subscribers receive each report once, on the downstream-ID topic.
- Capacity: the expected handful of clients is trivial for mochi. No hard client cap.

### 4.1 Protocol parity with the printer's embedded broker

The listener MUST behave at the same protocol level as the broker in the printer firmware, including observed quirks. Behaviors marked "observed" come from community reverse engineering, not official documentation. Hook semantics are verified against mochi-mqtt v2 source (`server.go`: `processPublish`, `processSubscribe`, `publishToSubscribers`).

| Behavior | Printer (observed) | Proxy |
|---|---|---|
| Protocol version | MQTT 3.1.1; apps use no v5 features | Serves v3.1.1; v5 clients tolerated but unused |
| QoS | 0 only | QoS 0 on every hop: mochi `Capabilities.MaximumQos = 0` grants 0 on every SUBACK and downgrades every PUBLISH; upstream subscribes and publishes at 0 |
| Keepalive | Apps use 30-60 s | Per client, enforced by mochi |
| Client ID takeover | Same-ID reconnect kicks the old session | Same (mochi default) |
| Clean session | Apps use true | Accepted; sessions are not persisted across proxy restart (same as a printer reboot) |
| Retained messages | Not used; reports are live pushes | Client publishes forward with the retain bit stripped; nothing is retained broker-side |
| Last Will | Unused by Bambu apps | Suppressed: every downstream will is dropped |
| Connection limit | ~4 concurrent (P1/X1), 1 on A1 series | Intentionally raised — removing this limit is the proxy's purpose |
| Auth failure | Connection refused | CONNACK code 5 (bad username or password) in `printer` mode |
| Unknown serial subscription | n/a (printer serves one serial) | SUBACK 0x80 for that filter only; connection stays open |
| Publish to `/report` or unknown topics | Printer broker is permissive | Dropped silently (mochi ACL behavior) — protects other apps from injected state |
| TLS | Bambu-issued cert, CN = serial; clients skip verify | Self-signed leaf; clients skip verify; pinning possible via config |

## 5. Upstream connections

Per configured printer:

- `address` + `tls` + `insecure_skip_verify` select the endpoint. Defaults support the stock printer reality (`8883`, TLS, skip verify); a plain-1883 entry is valid for non-standard endpoints (custom firmware, fronting relay).
- Credentials: `username` (default `bblp`) + `password` (LAN access code).
- ONE always-on `paho.mqtt.golang` client per printer, built before serve and dialed at boot without waiting, client ID `bmbpx-<serial>`.
- `SetAutoReconnect(true)`, `SetConnectRetry(true)` with a 10 s retry interval, `SetMaxReconnectInterval(30s)`, `CleanSession(true)`, keepalive 30 s, ping timeout 10 s, connect timeout 10 s.
- On every connect, the link subscribes to `device/{serial}/report` at QoS 0, then publishes one `pushall` to `/request`. The warmup is attempted exactly once per connect and never retried.
- A refused report subscription (SUBACK 0x80) keeps the printer offline and retries SUBSCRIBE every 10 s on the same connection. Online means connected AND subscribed.
- Publishes are serialized through paho's thread-safe `Publish`, always at QoS 0, never retained.
- `Send` attempts one command without waiting: it returns `ErrOffline` when the printer is not connected and subscribed. The publish wait is bounded to 5 s; commands are never queued or replayed.

### 5.1 Connection lifecycle

Printers are switched off, rebooted (1-2 minutes), and drop Wi-Fi. The store keeps each printer's document across disconnects, so a reconnect never loses state the proxy already holds.

- **Printer offline at boot**: paho retries the connect every 10 s until the printer answers. HTTP serves immediately.
- **Printer reboot mid-session**: TCP drops, paho reconnects (≤30 s between attempts), resubscribes, and sends one `pushall`, so downstream apps converge to full state automatically — no client action needed.
- **Printer power-cut (no TCP RST/FIN)**: keepalive 30 s + ping timeout 10 s detect the dead peer; connection loss flows into the same reconnect path. Reports during the outage are lost; the connect `pushall` restores full state.
- **Downstream command while printer offline**: `Send` returns `ErrOffline` and the bridge drops the command at once, logging at debug. The app retries at application level — the same behavior an app sees when a printer drops mid-command today.
- **Subscriptions during an outage**: SUBSCRIBE needs no printer; a wildcard SUBSCRIBE succeeds with a printer offline, and the reconnect warmup backfills reports.

## 6. Camera and camera wall (P1/A1 native; X1/P2S/H2-series RTSPS)

P1P, P1S, A1, and A1 MINI expose a camera through the Bambu chamber image protocol: TLS on printer port 6000. The client sends an 80-byte authentication payload (magic 0x40, command 0x3000, username `bblp`, LAN access code) and the printer answers with length-prefixed JPEG frames — a 16-byte header whose first little-endian uint32 is the payload length, then the JPEG. There is no login reply; the first frame follows the auth payload directly. Verified against a real P1S (1280×720 JPEG) and cross-checked against the bambuddy reference implementation.

The printer allows exactly ONE camera connection. The proxy enforces this with one capture owner per printer serial:

- Capture starts on demand (first snapshot or stream consumer) and stops after a 2-minute idle grace period.
- Every consumer reads the latest immutable frame; slow clients skip frames and never block capture.
- Connection loss reconnects with capped backoff (0.5 s → 5 s) while consumers remain.

X1, X1C, X1E, P2S, H2S, and H2D printers expose the camera as RTSPS on port 322 (`rtsps://<printer>:322/streaming/live/1`, basic auth `bblp` + LAN access code; serial prefixes 00W/00M/03W/22E/093/094 per community serial tables). The web wall consumes these through one supervised FFmpeg process per active printer: RTSP over TCP, decode, 2 fps MJPEG (`-f mpjpeg`, boundary `ffmpeg`) on stdout. The standard-library multipart reader enforces the frame contract (1 B–10 MiB, JPEG SOI/EOI) and a 15 s per-part watchdog kills a stalled child. Every started child is killed and reaped on all exit paths, and stderr is discarded so the credential-bearing URL can never reach logs. H2-series firmware may need Settings → LAN Mode Liveview enabled before port 322 answers. FFmpeg is bundled in the Docker image; without it, RTSPS models answer 503 with the tile reason.

Besides the HTTP surface, the proxy runs a printer-compatible raw camera endpoint: a TLS listener on port 6000, the printer's camera port. A camera client connects, sends the 80-byte `bblp` + access-code authentication payload, and receives the native length-prefixed JPEG frame flow unchanged. No topic serial travels on this path, so the access code is the routing key: the proxy assumes printer-generated codes are unique and does not reject duplicates, but two printers sharing one access code are indistinguishable to this endpoint (MQTT serial routing is unaffected). The listener serves exactly when the camera feature is enabled, shares the one-capture-owner rule with the HTTP consumers, and serves only native (P1/A1) models: an access code that also matches an RTSPS printer never routes there.

HTTP surface (all on the shared `http.port` listener, unauthenticated by design):

| Endpoint | Behavior |
|---|---|
| `GET /camera/{serial}/snapshot` | Freshest frame younger than 5 s from the shared buffer; otherwise waits up to 15 s for a new frame; 503 if none arrives |
| `GET /camera/{serial}/stream` | Multipart MJPEG; complete parts flushed per frame; survives printer outages while the client stays connected |
| `GET /camera/status` | Display state for every printer (name, print state, stage, job name, progress, remaining minutes, layers, temperatures, fans, print error, HMS alerts, AMS filament slots with the external spool, available controls, freshness, frame age). No credentials, no addresses |
| `GET /activity` | Recent events for every configured printer, newest first |
| `GET /activity/{serial}` | Recent events for one configured printer; 404 for an unknown serial |
| `GET /camwall` | Multi-printer camera wall dashboard; composes camera images with telemetry in the browser |
| `POST /control/{serial}` | One allow-listed printer command as JSON (`light`, `pause`, `resume`, `speed`, `stop`); 404 unknown printer, 400 invalid request, 503 printer offline, 502 send failure |
| `GET /favicon.ico`, `GET /apple-touch-icon.png` | Embedded raster icons for the wall (browser probes, bookmarks, iOS home screen); the page itself inlines an SVG favicon |
| `POST /mcp` | Model Context Protocol endpoint (on by default; `mcp.enabled: false` removes it): the read-only `list_printers`, `get_printer_state`, `get_camera_snapshot`, and `watch_printer` tools, the `pause_print`, `resume_print`, `stop_print`, `set_chamber_light`, and `set_speed_profile` control tools, and the `set_ai_monitoring` and `get_job_preview` module tools served when their modules run, over stateless Streamable HTTP |
| `GET /livez`, `GET /readyz` | `200 ok` once the process serves. Upstream connectivity deliberately does NOT affect readiness |
| `GET /status` | JSON `status`/`upstreams` for dashboards and scrape jobs, plus one member per module with `StatusValue` |
| `GET /config`, `GET /config/api`, `PUT /config/api` | Browser configuration page and its JSON API (§9); also `POST /config/printers/test` and one `POST /config/{section}/test` per section with a test action |

The camera wall is one self-contained embedded HTML page (inline CSS, JS, and SVG icons, including an inline SVG favicon; no external assets beyond the raster bookmark icons served by the same binary). It polls `/camera/status` once a second while the page is visible, with at most one request in flight; a failed poll marks the proxy offline, and the next success restores it. A hidden page releases every camera connection and stops polling. The wall gives the live budget (default 4, configurable 1–16) to the focused camera first, then active prints (RUNNING or PAUSE) in wall order, with current stream holders keeping their slots so streams do not restart. Other visible eligible printers get snapshots; off-screen tiles, disconnected printers, and hidden browser tabs hold no camera connection. Snapshot refresh defaults to 8 s and is configurable from 2–60 s. Tiles fill the window width, cycle through three detail levels (compact, vitals, full) when their details panel is clicked, and reorder by drag or keyboard. Fleet chips filter the wall by state or "needs attention". Print errors (`print_error`, formatted `XXXX_XXXX`) and HMS alerts (`HMS_AAAA_BBBB_CCCC_DDDD`, severity from the code's high half) stay visible at every detail level. The reported state shows as reported; offline comes from connectivity only. Kiosk mode or a setting requests a Screen Wake Lock where the origin allows it. Order, filters, detail levels, and settings persist only in the browser's local storage; the proxy holds no wall state. There is no token-authenticated kiosk mode: the proxy has no user/session system, and these camera routes are intentionally open on the configured HTTP interface.

Eligibility is checked before any camera socket is opened, with no operator configuration required: the model is inferred from the serial prefix (01P→P1P, 01S→P1S, 030→A1 MINI, 039→A1, 00W→X1, 00M→X1C, 03W→X1E, 22E→P2S, 093→H2S, 094→H2D; 01P/01S verified against live printers, the rest against community serial tables), and an explicit `model` field overrides the inference when present. Unknown serial → 404; models outside both transports → 422. Camera capture never affects MQTT proxying. A disabled camera feature (`camera.enabled: false`) removes the routes and stops capture workers.

The MCP endpoint (`internal/mcpserver`, official `github.com/modelcontextprotocol/go-sdk`, stateless Streamable HTTP) serves four read-only tools and five control tools, with module tools added by the modules in the running configuration: the read-only `list_printers`, `get_printer_state`, `get_camera_snapshot` (P1/A1-series only), and `watch_printer`, control tools `pause_print`, `resume_print`, `stop_print` (destructive emergency stop with no confirmation), `set_chamber_light`, and `set_speed_profile`, plus the module tools `set_ai_monitoring` (detection) and `get_job_preview` (job preview). `watch_printer` returns activity entries with `ID > after_id`, waiting up to `timeout_seconds`, then returns them with the current printer state. Activity IDs start from the process clock, so `after_id` survives a config-save restart. There are no resources, subscriptions, revisions, or cursors. Expected failures return typed output with an `error` member and `IsError`.

Telemetry for `/camera/status` comes from the printer store (§7.3): one merged document per printer, projected to a shared `View` for the wall and MCP. A field keeps the last value the printer reported until the printer reports a different value; the document survives disconnects. The freshly connected `pushall` supplies the initial full state. `View` carries the reported state plus connectivity (`freshness.fresh` is the link state) and a display-only last-report age that gates nothing. The store never feeds back into MQTT forwarding.

The in-memory activity log keeps the 50 newest events per configured printer and clears on restart. It records print transitions, changes to HMS alerts and `print_error` (with the same `hmscodes` descriptions the live alert row shows, and a `url` lookup link on alert and error entries), upstream connect/loss events, control commands with their origin, and module activity entries. A FAILED report carrying a user-cancel code (`0x0300400C`, `0x0500400E`) records `print_cancelled` instead of `print_failed`; one report records one combined `alerts_cleared` entry. `/camera/status` includes each printer's activity entries; the camera wall shows them at the Full detail level with a `Look up` link for entries carrying a URL. The separate `/activity` endpoints work whenever HTTP is enabled, even when the camera feature is disabled. Each entry has an increasing ID, UTC timestamp, stable event kind, severity, display message, optionally a URL, and a server-computed age in seconds read at read time.

Chamber temperature is model-filtered before `/camera/status` is serialized. P1P/P1S and A1/A1 MINI do not have this sensor, and their reported `chamber_temper` value is meaningless; the field is omitted for those models unless a Panda Breath reading is fresh (15 s). Only known chamber-sensor models expose the printer's value. Unknown model capability defaults to hidden rather than displaying a possibly invalid reading.

### 6.1 Gadget AI failure detection (optional)

An optional integration with OctoEverywhere's Gadget AI failure detection (developer docs: https://docs.octoeverywhere.com/ai-failure-detection-apis/developer-docs/overview/) adds automatic print-failure monitoring on top of the camera pipeline. The feature is configured by the `detection` section: `enabled` switches it and `api_key` stores the Gadget API key. A nil `enabled` keeps the historical key-based default: detection runs exactly when a key is configured, and an explicit `enabled: false` disables it even when a key exists. Enabling without any key fails validation on the effective configuration. The stored key never leaves the server: the configuration API reports only whether a key exists, a blank value on save keeps the stored key, and the config file is written with owner-only permissions (0600). Detection that is off — no key and no explicit enable — is completely off: no context creation, no uploads, no automatic pauses.

Setting the key is the operator's documented consent for two things: camera snapshots leave the LAN for OctoEverywhere's servers, and the proxy may pause a print autonomously when the service recommends it. The provider states that images submitted through the public Gadget API are not used for AI model training.

- **Trigger and pacing.** While the document shows `gcode_state` RUNNING on an eligible printer with a live connection, a single worker per printer (max one request in flight) POSTs the latest captured JPEG to the context's `ProcessRequestUrl` as `multipart/form-data` field `image` (filename `print.jpg`) with the `X-API-Key` header. Frames above the API's 6 MiB cap are rejected locally without a request. Each inspection fetches a frame newer than the previous inspection (strictly greater sequence), waiting up to 15 s for one. The next inspection waits Gadget's `Recommended` interval; the worker keeps no local cadence rules.
- **Print counter.** A module counter bumps on `Event.NewJob` (the first PREPARE/SLICING of a job, or RUNNING without one), and on `state_initial` when the document is PREPARE, SLICING, RUNNING, PAUSE, or PAUSED. A new counter value clears the previous print's results, so they no longer show during PREPARE, and resets the context (via the Create Context API, `{}` body, held in memory only), the pause latch, and the user toggle. Results with a stale counter are discarded.
- **Actions on results.** Per-printer detection state, `PrintQuality` (1–10), and `WarningSuggested` surface in status only (see Status below). `PauseSuggested = true` triggers a pause only if, at decision time, the connection is live and the document still shows RUNNING on the current counter: the proxy then sends exactly one pause. Pauses are never retried or replayed, prints are never resumed automatically, and pausing re-arms only on the next print. `Score` (raw 0–100) is ignored. Before the first inspection of a print, the worker turns the chamber light on once when it is off (`ai_light_on`).
- **Outcomes from reports.** Autonomous outcomes resolve from reports with no clocks: confirmed when a report shows PAUSE, PAUSED, or the print ended; unconfirmed when a later report shows RUNNING at a higher `layer_num` than at send. The attempt is visible as `pause_state` `pending` until a report resolves it, then `confirmed` or `unconfirmed`.
- **Error taxonomy.** Temporary errors — network errors and timeouts, HTTP 5xx, `OE_INTERNAL_ERROR`, `OE_BACKEND_THROTTLED`, `OE_CONTEXT_RATE_LIMITED`, and `OE_IMAGE_DECODE_FAILED` — open one interruption: the first failure records `ai_degraded` with the plain-language cause, every failure logs a WARN with the cause, the underlying error, the failure count, and the retry delay, and the wall shows the cause, the failure count, the interruption length, and the next try. Retries follow the provider's recommended interval (30 s fallback). A success between failures does not end the interruption; `ai_recovered` is recorded only after 3 consecutive successful checks, with the failure count, the duration, and the last cause. `OE_BAD_ARGS` and `OE_ARGS_PARSE_FAILED` block further detection for that printer until process restart. On `OE_INTERNAL_ERROR` or another HTTP 5xx the worker switches to `FallbackProcessRequestUrl` (recorded as `ai_backup_server`) and keeps it for that context's lifetime; throttling, invalid-request, and account errors never switch URLs. Account errors suspend ALL detection account-wide until process restart — `OE_INVALID_API_KEY`, `OE_API_KEY_DISABLED`, `OE_API_KEY_BLOCKED_PAYMENT_FAILED`, `OE_API_KEY_IP_RESTRICTED` (never retried, never switched to the fallback URL), and `OE_FREE_USAGE_LIMIT_REACHED` (the proxy tracks no billing periods, so it cannot know when a free allowance resets) — releasing camera holds and keeping the last per-printer status visible. Logs carry the underlying error, the HTTP status, the provider's error type, and a bounded response body; the API key is never logged. Status and activity show the cause in plain words. Gadget failures never affect MQTT proxying, camera serving, or a print in progress.
- **No quota tracking.** The proxy keeps no counters and renders no usage forecasts. The provider enforces the allowance server-side.
- **Status.** On `/camera/status` every printer tile carries a `detection` object (omitted entirely when the feature is off): `state` — `starting`, `idle`, `monitoring`, `attention`, `paused`, `degraded`, `unsupported`, `blocked`, or `disabled` — and `pause_state` (`none`, `pending`, `confirmed`, or `unconfirmed`) are always present, and the omitempty fields `quality`, `warning`, `age_seconds`, `next_check_seconds`, `last_inspected_layer`, `faster_inspection`, `camera_lost`, `using_fallback`, `suspended`, `reason`, and `message` appear when set. The health `/status` payload gains a top-level `detection` map keyed by serial, holding the same complete per-printer object for every configured printer; the field is present only when the feature runs. There is no separate endpoint.
- **Security.** The key is a write-only secret on the `/config` page: never returned by the API, never logged, and stored only in the configuration file, which is written with owner-only permissions (0600). Uploads go only to the two URLs returned by Create Context over TLS; both URLs are validated at creation (HTTPS, vendor host, no userinfo, fragment, or non-default port), redirects are disabled, and every request is bounded by a 15 s timeout.

### 6.2 Build-plate check (optional)

The plate check uploads camera frames to a vision provider when a print starts and sends `stop` only when the plate looks occupied. Every error lets the print continue (fail-open): skips and failures are cataloged plain-English reasons (`platecheck_skipped`, `platecheck_failed`) from one reason catalog shared by the status, the activity entries, and the wall panel.

- **Config.** The `platecheck` section holds `enabled`, `provider` (`cloudflare` or `custom`), `account_id`, `endpoint`, `api_key`, `model`, `stop_confidence`, `min_clarity` (default 0.70, shared by both phases), and the nested `first_layer` object with `enabled` and `pause_confidence`. Without an explicit `enabled`, the check runs when the provider target and key are set; `first_layer.enabled: false` disables the first-layer phase alone, and that phase is otherwise on whenever the check is on. `stop_confidence` and `pause_confidence` accept whole percentages from 0.50 through 0.99 and compare strictly; the defaults are 0.50 and 0.70. Both phases share the provider, credential, and model. Cloudflare builds its URL from the account ID; custom uses `endpoint`. A key is bound to its destination, so a saved key never moves to a new account or endpoint.
- **Startup decision.** Two fixed questions return numbers in [0,1]. Image clarity (`p_assessable`) below `min_clarity` is unclear (wire state `inconclusive`, shown to users as "Image unclear"), and the occupied score is not used. A stop needs `p_clear < (100 - N)/100` for cutoff `N` percent, so exact equality does not stop. All errors are fail-open and use fixed categories; provider bodies, URLs, and image bytes never enter logs or errors.
- **First-layer decision.** The phase consumes each `first_layer_complete` event. It captures two fresh original JPEGs at least 2 s apart and uploads both in one request that answers five questions: view assessable, filament tangled, part detached, nozzle blob at millimeter scale, and layer incomplete. Image clarity below `min_clarity` makes the image unclear and skips the phase; a missing camera and every error are fail-open. A pause needs the gate and any pause-capable probability strictly above the cutoff; an incomplete layer above 0.5 warns only. A chamber light that reports off turns on once before capture.
- **Admission.** Pass 1 runs at `print_preparing` or `print_started` when the printer is online, the print is PREPARE/SLICING/RUNNING, and the layer is 0 or unreported-running. PREPARE and SLICING still report the previous job's `layer_num` (only RUNNING resets it), so the layer is ignored until the printer leaves them. `state_initial` on an active job records a `start_not_observed` skip with no check. Pass 2 runs once more after the toolhead homes: a reported `stg_cur` of 13 or 14 sets a per-print homing flag, and the second pass runs when the flag is set, pass 1 is done, no stop was sent, and the print is RUNNING at layer 0; above layer 0 outside PREPARE/SLICING it skips with `print_past_start`. `Event.NewJob` resets the startup and first-layer findings and any unresolved stop or pause to idle.
- **Outcomes from reports.** A stop sent in PREPARE is sent once more at PREPARE→RUNNING. Otherwise outcomes resolve like the Gadget pause: a stop is confirmed when a report shows PAUSE, PAUSED, IDLE, FINISH, or FAILED, and unconfirmed when a later report shows RUNNING at a higher `layer_num` than at send; the first-layer pause resolves the same way over PAUSE/PAUSED. There are no confirmation clocks.
- **Diagnostics.** `POST /platecheck/snapshots` skips printers whose upstream connection is down, captures and scores every online camera printer in parallel using the submitted settings, and streams NDJSON events (`start`, one `result` per printer, `done`) with the exact uploaded JPEGs. It touches no commands, activity, or worker state, and it covers the startup check only: the first-layer phase has no dry-run route. The `/config` page calls `POST /config/platecheck/test` (key check, text only) first.
- **Limits.** Chamber cameras view the plate from one corner at low resolution, so a tangle, blob, or detached part can be hidden behind the toolhead or the part itself, and small features sit near the resolution limit. The first-layer defect thresholds are unverified model calibration.

### 6.3 Job preview

Job preview (`internal/jobpreview`) downloads the running print's 3MF from the printer over FTPS (implicit TLS to port 990, `jlaffaye/ftp`, 64 MiB read cap) and serves its plate render and metadata. A retrieval triggers once per job on `Event.NewJob`, on `state_initial` with an active document, or when the archive selectors change on an active document. A `.3mf` `gcode_file` identifies the archive alone, because the printer rewrites `subtask_name` characters after the `project_file` echo; otherwise `subtask_name`, `gcode_file`, and `plate_idx` all count; each attempt runs under a 10-minute budget with up to 3 tries 30 s apart, and three failures record `job_preview_unavailable` and log the last error (each attempt logs its outcome at debug; a success logs the plate, archive size, and duration). The plate comes from `plate_idx`, then a `plate_N` in `gcode_file`, then the single `Metadata/plate_N.gcode` the archive carries (P1 firmware sends no `plate_idx`, and a sliced 3MF keeps every plate's thumbnails), then the archive's only plate. The wall shows the plate thumbnail plus slice facts: plate (index, name, bed type, estimated time, weight, supports, first-layer time, object count), materials (type, color, weight), objects (name, count), process, description, license, and warnings. The cached PNG is served at `GET /camera/{serial}/preview`, and `get_job_preview` reads the cache without ever starting a transfer. Model-authored description text is untrusted. The service exists only while cameras or MCP are on, so with neither consumer no FTPS socket can ever open.

## 7. Routing model (core)

Topic grammar: `device/{id}/report` and `device/{id}/request`, where `{id}` is the printer's downstream ID (its alias when set, otherwise its serial). The ID is the second level. Any other topic shape is denied/dropped. This routing model covers MQTT only: the raw camera endpoint (§6) carries no topics and routes by the access code instead.

### 7.1 Subscribe path

```
client SUBSCRIBE device/{id}/report
  → OnACLCheck hook:
      off-grammar or unknown id → deny → mochi sends SUBACK 0x80 for that
                                   filter only, connection stays open
      ok                        → accept; mochi owns the subscription
```

SUBSCRIBE needs no printer: links dial at boot and stay connected on their own, so a wildcard subscribe succeeds with a printer offline. There is no per-subscriber warmup: clients send their own `pushall` (§2.3), and the proxy's connect `pushall` keeps its own document full.

### 7.2 Publish path (downstream → upstream)

```
client PUBLISH device/{id}/request
  → OnPublish hook:
      id unknown or off-grammar → deny (mochi ACL: dropped)
      known id                  → links.Send(real serial, same payload)
                                  → return packets.CodeSuccessIgnore
```

`CodeSuccessIgnore` (verified in mochi source) suppresses LOCAL fan-out — requests never reach other downstream clients. The inline-client guard is essential: injected upstream reports re-enter this hook, and forwarding them again would loop.

- Payload bytes pass through untouched (signing-compatible); only the retain bit is stripped.
- The upstream publish is fire-and-forget QoS 0, like a direct connection. If the printer is offline at that moment, the command is dropped and the app retries at application level — the same behavior an app sees when a printer drops mid-command today.

### 7.3 Report path (upstream → downstream, store, activity, subscribers)

```
printer PUBLISH device/{serial}/report
  → paho MessageHandler on the serial's link
      → inject.PublishDownstream(topic, payload)   (mochi inline client,
           alias rewrite, retain=false, qos=0) → mochi fans out to subscribers
      → store.Apply(serial, payload):
           merge → Transitions → activity record → subscribers in wire order
```

Retain is never set: Bambu reports are ephemeral state, and stale snapshots must not survive reconnects.

Serial-level wildcard subscribers (`device/+/report`) receive reports from ALL printers — inherent to the single-endpoint design. Clients demux by the serial in the topic, which every Bambu app already parses.

- **Merge.** One copy-on-write `Doc` per printer, kept across disconnects. A top-level `print` object with `print.msg == 0` is a full report and replaces `print`; every other object deep-merges, and scalars, arrays, and null replace. Non-object payloads drop without an event. Evidence: the live P1 capture (`internal/printer/testdata/live_01S00C351100139_20261010.jsonl`): lines 1 and 5 are `msg:0` full reports with 63 `print` keys; the 627 `push_status` deltas carry 3–14 keys; line 2 is a 26-key `project_file` command and line 4 is an `info` report without `print`. The unit list under `ams` and the `hms` list arrive complete in every report that carries them — never as per-slot or per-alert deltas — so array replacement never loses a slot or an alert.
- **Accessors.** Views read through typed accessors (`GcodeState`, `JobName`, `Progress`, `Layer`, `Temps`, `Stage`, `SpeedLevel`, `Fans`, `ChamberLight`, `PrintError`, `HMS`, `AMS`, …), so one parser serves the wall and MCP. Documents are immutable after publication; readers must not mutate them.
- **Transitions.** `Transitions(prev, cur)` is the only place print events are derived, and `NewJob(prev, cur)` is the only new-print rule: the store sets `Event.NewJob` on the first PREPARE/SLICING of a job, or on RUNNING with no PREPARE/SLICING before it (never on a resume or `state_initial`), and modules read that field instead of deriving their own. The first state ever seen yields only `state_initial`; later reports yield `print_preparing` (→PREPARE/SLICING), `print_started` (→RUNNING, except from pause), `print_resumed` (pause→RUNNING), `print_paused` (→PAUSE/PAUSED, except pause→pause), `print_finished` (→FINISH), `print_failed` or `print_cancelled` (→FAILED), `print_stopped` (RUNNING/pause→IDLE), `hms_alert` per new alert ID, `print_error` on a new non-cancel error, one combined `alerts_cleared`, `chamber_light_on`/`chamber_light_off` (silent until the previous document reports a light value), and `first_layer_complete` (layer ≤1 to ≥2 while RUNNING/PAUSE/PAUSED). A print already running at boot is only `state_initial`. Connectivity changes record `connected`/`disconnected` without touching the document.
- **Order.** `Apply` merges, derives transitions, records each transition in the activity log, then calls subscribers in wire order under one dispatch mutex, so a subscriber always sees the recorded activity. Subscribers run in paho's ordered handler: a subscriber MUST NOT block and MUST NOT send printer commands. Modules that need commands queue the event and act from their own `Run` goroutine.
- **Control.** `control.Service` is the only allow-listed path from the wall and MCP to commands: `Pause`, `Resume`, `Stop`, `Light`, `Speed` call `Links`. It never refuses on cached state — `Available` is a UI hint only — and records `control_<action>` with its origin ("camera wall" or "MCP"). Speed maps silent/standard/sport/ludicrous to Bambu `spd_lvl` 1–4. No heater or temperature command exists.

## 8. QoS, keepalive, ordering

| Concern | Decision |
|---|---|
| QoS | QoS 0 on every hop, matching the printer's broker. |
| Downstream keepalive | Per client, owned by mochi. |
| Upstream keepalive | 30 s, owned by paho. |
| Ordering | Per serial, upstream publish and report delivery are FIFO (single paho connection, `OrderMatters`). Cross-serial ordering is not guaranteed and not needed. |
| Message loss windows | Upstream disconnect: reports in flight are lost; the connect `pushall` restores full state. |
| Duplicate suppression | Exactly ONE upstream publish per downstream publish. |

## 9. Configuration

Single YAML file. Secrets sit next to the binary in a homelab deployment; no env expansion.

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
    # alias: "01P00A000000001" # optional downstream serial: clients must use it and the real serial is refused; uppercase letters and digits, unique across serials and aliases
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
```

HTTP and camera configuration (shared listener; see §6):

```yaml
http:
  port: 8080                # 0 means unset: the proxy uses 8080 and logs a warning; HTTP is always on
camera:
  enabled: true             # false removes camera routes and the camera wall
mcp:
  enabled: true             # default; false removes the MCP endpoint at /mcp
notifications:              # optional print alerts to one Pushover account
  enabled: false
  provider: pushover        # only pushover today
  pushover:
    app_token: ""           # write-only through /config; blank keeps the stored value
    user_key: ""
```

Validation at startup: one listener minimum, listener port range, cert/key pairing, known auth mode, unique non-empty serials, required address and access code per printer, string-typed module settings, valid unique aliases, HTTP port range, plus module effective checks (for example enabled detection needs a key, enabled safety needs Pushover credentials). Unknown keys load and are ignored. A removed `behavior` section in an old file is ignored.

The config file contains printer access codes in plain text. Deploy with restrictive file permissions (the proxy writes it 0600) and never log passwords; connection logs redact credentials.

Every setting is editable on `/config`, including a `safety.enabled` switch. A save validates, test-binds a changed `http.port` and rejects the save when the port cannot bind, writes the file atomically with mode 0600 to a temp file plus rename (a single-file bind mount breaks the rename, so compose mounts a config directory), responds `{saved:true}`, and then shuts down normally and re-execs the binary (`syscall.Exec` of the running executable with the same args and environment). A startup failure after re-exec serves only health and `/config` with the error shown on the page. `/config` treats both Pushover credentials as write-only secrets like printer access codes: the API reports only whether a value is stored, and a blank submitted value keeps the stored one. The `/config/printers/test` endpoint performs exactly one MQTT CONNECT with the submitted draft settings — no subscribe, no publish, no save — and refuses drafts whose serial or address is already configured.

Notifications are owned by the YAML file and the `/config` page. The notifier observes the activity log directly with one table, one 16-slot queue, and one worker: it batches recorded activity for 2 seconds, sends at most one message per printer per batch (summaries, file name, active alert lines), and attaches a camera snapshot when the camera answers with a fresh frame. A lone `print_paused` batch is silent unless the final state carries an active alert. Delivery makes one attempt with no retry or persistence; a full queue or failed send logs a warning and drops the event. The `POST /config/notifications/test` endpoint sends one message from the submitted section merged with stored credentials, forced enabled; it never saves or reloads.

## 10. Package layout and dependencies

```
cmd/bambu-mqtt-proxy/main.go   flags, config load, wiring, graceful shutdown, re-exec
internal/config/               schema, defaults, validation, section registry
internal/broker/server.go      mochi server, TLS listeners, report injector
internal/broker/bridge.go      routing hooks: auth, ACL, command forwarding
internal/printer/link.go       one always-on paho link: subscribe, warmup, reconnect
internal/printer/links.go      serial → link table, Send with ErrOffline, commands
internal/printer/doc.go        merged document, merge rule, accessors
internal/printer/store.go      per-printer store, ordering, subscriber dispatch
internal/printer/events.go     Transitions: the only print-event derivation
internal/printer/view.go       shared wall/MCP JSON projection
internal/routing/topic.go      serial and alias extraction, ACL grammar, topic rewrite
internal/activity/             in-memory log, waits, /activity endpoints
internal/control/              allow-listed Do/Available service
internal/camera/               P1/A1 chamber-image capture, X1/P2S/H2-series RTSPS capture via FFmpeg, raw TLS camera endpoint, snapshot/stream/camera wall handlers
internal/mcpserver/            MCP endpoint (core tools over stateless Streamable HTTP)
internal/module/               module contract, route mounting, shared MCP tool helpers
internal/notification/         module: Pushover sender + batched best-effort notifier over recorded activity
internal/jobpreview/           module: cached 3MF plate render and metadata, get_job_preview
internal/pandabreath/          module: Panda Breath accessory link, chamber reading, and trend
internal/pandapwr/             module: Panda PWR smart plug power draw over HTTP
internal/detection/            module: Gadget AI failure detection, its config section, set_ai_monitoring
internal/platecheck/           module: build-plate and first-layer checks, snapshot diagnostics
internal/safety/               module: over-temperature stop, Panda PWR power-off, Pushover emergency alert
internal/httpsrv/              shared health + camera HTTP listener
internal/health/               /livez, /readyz, /status
internal/configui/             /config page, API, save, printer probe
internal/hmscodes/             printer error and HMS alert descriptions
internal/tlsutil/              self-signed certificate generation and persistence
```

Dependencies: `github.com/mochi-mqtt/server/v2`, `github.com/eclipse/paho.mqtt.golang`, `github.com/modelcontextprotocol/go-sdk`, `github.com/jlaffaye/ftp`, `github.com/gorilla/websocket`, `gopkg.in/yaml.v3`, plus stdlib.

## 11. Failure modes and limitations

| Failure | Behavior | Consequence |
|---|---|---|
| Printer offline at startup | HTTP starts immediately; paho retries the connect every 10 s | No startup delay proportional to printer count |
| Printer offline at subscribe time | SUBSCRIBE succeeds; reports arrive when the printer returns | No client retry needed |
| Printer reboot (1-2 min) | paho reconnect (≤30 s between attempts), resubscribe, one `pushall` | Reports during the gap are lost; state converges automatically after pushall |
| Printer power-cut without TCP close | keepalive 30 s + ping timeout 10 s detect the dead peer | Same reconnect path as reboot |
| Refused report subscription (SUBACK 0x80) | Printer stays offline; SUBSCRIBE retried every 10 s on the same connection; no `pushall` is sent | Commands fail fast with `ErrOffline` until the broker grants the subscription |
| Printer IP changed (DHCP) | Connect fails indefinitely at capped rate | WARN logs; operator updates config |
| Downstream client crash | mochi drops the session; nothing upstream to reconcile | None |
| Duplicate serial in config | Startup validation error | Operator fixes config |
| Two clients send conflicting commands concurrently | Both go upstream; both responses broadcast to all report subscribers | Clients must tolerate each other's responses (sequence_id is per client). Same limitation as a shared Mosquitto bridge. |
| Printer requires signed `print.*` (2025+ firmware) | Proxy forwards signed payloads untouched | Signing is the client's responsibility; LAN Mode + Developer Mode as required by the printer settings |
| Proxy restart | Stateless; clients reconnect, links redial, warmup re-sent | Brief gap, then full state via pushall |
| A1 series printer (single-client limit) | Proxy holds one upstream connection regardless of model | The printer's limit stops affecting apps entirely |
| Camera requested for unknown serial or unsupported model | 404 / 422 before any camera socket opens | Supported models: P1P/P1S/A1/A1MINI (native), X1/X1C/X1E/P2S/H2S/H2D (RTSPS web wall); everything else is refused |
| FFmpeg missing for an RTSPS model | RTSPS capture stays disabled; snapshot/stream answer 503 and the tile shows the reason; one startup warning explains the dependency | P1/A1 capture and every other service are unaffected; install FFmpeg (bundled in the Docker image) and restart |
| Camera connection drops mid-stream | Capture reconnects with 0.5–5 s backoff while consumers remain; streams resume | Brief frame gap; slow clients skip frames instead of stalling capture |
| Raw camera port 6000 busy at startup (second proxy instance, another service) or its certificate cannot be created | Startup fails fast with a wrapped `raw camera endpoint` error after the deferred teardown stops every already-started service | Operator frees the port or sets `camera.enabled: false`; no half-started services remain |
| Unauthenticated HTTP exposure | Camera, camera wall, status endpoints, the camera wall `POST /control/{serial}` endpoint, and the `/mcp` endpoint (default on) have no login | Anyone who can reach `http.port` sees cameras and telemetry, and can pause, resume, change speed, toggle the chamber light, and **emergency-stop** prints through the camera wall control endpoint or the MCP control tools, without login. Heater and temperature commands are never exposed. Bind accordingly (state responses carry no credentials); set `mcp.enabled: false` or `camera.enabled: false` to remove part of the surface |
| Bambu Studio "add printer by IP" | Studio probes camera/FTP ports in addition to MQTT; probe failure can block discovery | MQTT control and status work; camera/FTP passthrough is future work |
| Tools that pin the printer TLS certificate | The self-signed proxy cert fails pinning | Load a custom cert/key via config, or disable pinning (clients must already skip verify against the real printer) |
| Downstream command while printer offline | `Send` returns `ErrOffline`; the command is dropped at once | App-level retry, identical to a direct-connection drop |
| Malicious/buggy client publishes to `/report` | Denied by ACL | Silently dropped; other clients never see injected state |
| Gadget API unreachable or transiently failing (optional integration) | One interruption: `ai_degraded` with the cause on the first failure, a WARN per failure, retries after the provider's recommended interval (30 s fallback); `ai_recovered` only after 3 consecutive successes | Detection lags; printing, MQTT proxying, and camera serving are unaffected |
| Gadget bad-arguments error (`OE_BAD_ARGS`, `OE_ARGS_PARSE_FAILED`) | Detection for that printer is blocked until proxy restart | Inspection and automatic pauses stop for that printer; other printers are unaffected |
| Gadget account error: invalid/disabled key, billing failure, IP restriction, or free allowance exhausted | Detection suspends account-wide until proxy restart; the activity entry names the cause and the fix; the ERROR log carries the full provider error | Warnings and automatic pauses stop; everything else is unaffected |
| Gadget-triggered pause rejected or lost (printer offline, signed firmware) | One pause command per print; outcome resolves from later reports (PAUSE/PAUSED/ended vs RUNNING at a higher layer); no retry, no auto-resume | Pause failure reaches the activity log; the print continues; pausing re-arms on the next print |
| Plate-check stop rejected or lost (printer offline, signed firmware) | One stop, sent once more at PREPARE→RUNNING; outcome resolves from later reports; no retry | The print continues; `platecheck_stop_unconfirmed` reaches the activity log and, with notifications on, Pushover |
| First-layer image unclear (camera unavailable, dark or blocked image, clarity below `min_clarity`) | The image is unclear and the phase is skipped for that job | The print continues; `platecheck_layer1_skipped` stays in the activity log |
| Notification queue full (activity burst) | The observer drops the new event and logs `notification dropped` | That event never notifies; forwarding and printing are unaffected |
| Pushover unreachable, slow, or rejecting | One send attempt per message, no retry; the failure is logged with a credential-safe error | The message is lost; every other service is unaffected |
| Camera unavailable when a notification sends | The message goes out text-only | The alert still arrives without the image |
| Over-temperature trip (safety module) | One `stop` per printer, one Panda PWR power-off, one Pushover emergency alert; no retry or confirmation; failures are logged and listed in the alert | A disconnected printer misses the stop; a printer that stops reporting cannot trip |
| Config save rejected (invalid settings or unbindable HTTP port) | 422 with the reason; nothing is written | Operator fixes the settings; the running proxy is untouched |
| Startup failure after re-exec | Only health and `/config` serve, with the error shown on the page | Operator fixes the settings on the page; a save restarts the full proxy |

## 12. Alternatives considered

1. **Mosquitto relay per printer** (disconn3ct pattern): proven, but N printers = N broker endpoints and configs; not Go; no single endpoint. Rejected against requirement (c).
2. **Transparent TCP/TLS proxy per printer** (pandaproxy pattern): no MQTT-level merge; one upstream per client defeats requirement (a) and cannot route by serial.
3. **Scraper/republish bridges** (bambu-bridge, Bambu-MQTT-Scraper): change topic semantics; not transparent for Bambu Studio.
4. **Hand-rolled MQTT server**: removes mochi dependency but re-implements protocol edge cases (varint lengths, QoS flows, keepalive) with real correctness risk for zero user-visible gain.
5. **mochi for both directions**: impossible — mochi is a server only; the printer side needs a client library (paho).

## 13. Verification plan

Tests run across real boundaries:

1. **Forwarding, auth, ACL**: wrong access code refused; unknown-serial subscribe denied (0x80); requests forwarded exactly once and never fanned out; retain stripped; QoS 0 granted; same-ID takeover; wildcard fans out from all printers; publishes to `/report` denied; wills suppressed; alias routing byte-identical on both hops (`internal/integration/integration_test.go`, `alias_test.go`, `internal/routing/topic_test.go`).
2. **Link**: offline at boot then connects; one SUBSCRIBE and one `pushall` per connect; a 0x80 SUBACK keeps the printer offline and retries on the open connection; `Send` offline returns `ErrOffline`; a wildcard SUBSCRIBE succeeds with one printer offline; command helpers emit the exact QoS 0 wire payloads (`internal/integration/link_test.go`).
3. **Merge**: replacement plus unrelated-data preservation plus published-document immutability; live P1 replay asserts the final view; accessory chamber precedence and window; connect/disconnect events preserve the document (`internal/printer/store_test.go`, `internal/printer/events_test.go`, `internal/jsonobj/jsonobj_test.go`).
4. **Transitions**: fixture replays assert final values and the exact order of event kinds across the live P1S capture, the edge cases, and the cancel/resend, including boot-mid-print `state_initial` (`internal/printer/events_test.go`).
5. **Camera**: one shared upstream session; unsupported models never dialed; serial-prefix and explicit-model gating; 404/422/503 status codes; snapshot freshness and shared-buffer reuse; MJPEG framing and slow-client isolation; live-status and stall/reconnect/shutdown paths; raw server auth, routing, fragmented sessions, deadlines, and shutdown; RTSPS secrets never reaching logs; wall-tile projection of filament, stage, preview, and module display; single-file binary route (`internal/camera/*_test.go`).
6. **Wall status**: `/camera/status` carries the fields the wall reads (tile shape, HMS/error projection, chamber filtering, controls, activity); model, eligibility, chamber, and port-zero config contracts; job-preview tile and MCP boundaries; route enablement wiring (`internal/camera/camwall_preview_test.go`, `internal/config/config_camera_test.go`, `internal/jobpreview/service_test.go`, `cmd/bambu-mqtt-proxy/preview_wiring_test.go`).
7. **Config**: defaults; access codes never returned and kept blank; invalid input never written; unbindable port rejected; rename failure keeps old bytes; atomic replace with mode 0600; env overrides; alias trim and duplicate rejection; safety switch; MCP default-on; detection/platecheck/notification section round-trips, redaction, key preservation, and test actions that never save (`internal/configui/*_test.go`, `internal/config/*_test.go`, `internal/notification/config_test.go`, `internal/platecheck/config_test.go`).
8. **Save and startup**: save restarts the process with the new config; startup failure serves the config page with the error; logger fallback; no-printer setup mode (`cmd/bambu-mqtt-proxy/main_test.go`). Listener lifecycle, HTTP server stop, and TLS certificate create/reuse/permissions/rotation (`internal/broker/server_test.go`, `internal/httpsrv/httpsrv_test.go`, `internal/tlsutil/selfsign_test.go`).
9. **Activity**: capacity and ordering; unknown printers ignored; entry serialization with link and age; wait filtering, wakeup, and cancel; observer delivery without deadlock (`internal/activity/*_test.go`).
10. **MCP**: all core tool outputs and typed errors against the store/module boundary; `watch_printer` across a restart; module tool registration and boundaries through a real `mcp.Server` (`internal/mcpserver/mcpserver_test.go`, `internal/detection/mcp_test.go`, `internal/jobpreview/service_test.go`, `cmd/bambu-mqtt-proxy/preview_wiring_test.go`).
11. **Control**: actions send regardless of cached state; offline maps to `ErrNotConnected`; invalid requests rejected; stop sends and records activity; available-hint matrix (`internal/control/control_test.go`).
12. **Modules**: platecheck stops once on an occupied plate with outcomes from reports, PREPARE→RUNNING stop resend, homing pass 2 once-or-skip, boot-mid-print silence, first-layer pause outcomes, snapshot diagnostics, provider wire contract and safe errors, reason catalog coverage; detection pauses once on Gadget's recommended interval, drops stale results, keeps the toggle working after a boot mid-print, resolves pause outcomes from reports, and recovers fail-open; safety trips once per episode, re-arms only on in-limit readings, and never re-arms on device loss; notification delivery with summary, file, alert, and JPEG, silent lone pauses, and non-blocking full-queue drops; jobpreview archive wall fields and FTPS session reuse; Panda address validation; HMS/error code lookups (`internal/platecheck/*_test.go`, `internal/detection/*_test.go`, `internal/safety/safety_test.go`, `internal/notification/*_test.go`, `internal/jobpreview/*_test.go`, `internal/pandabreath/setting_test.go`, `internal/pandapwr/pandapwr_test.go`, `internal/hmscodes/hmscodes_test.go`).
13. **Smoke (manual, performed)**: real P1S in LAN Mode at `10.10.20.141` — MQTT upstream connected on first attempt with `pushall` warmup; snapshot returned a valid 1280×720 JPEG (~87 KB); 4-second stream sample contained 3 complete multipart JPEG parts; 404/422 gates answered without opening a camera socket. This smoke run caught and fixed a real defect: the camera endpoint derived from the MQTT port instead of always using 6000.
14. **Smoke (manual, remaining)**: Bambu Studio and Home Assistant connected through the proxy on `:8883` with the printer's access code, observing `pushall` warmup and delta flow in debug logs.

## 14. Future work (explicitly out of scope)

- Raw RTSPS (X1-class, port 322) and FTPS (990) TCP passthrough, completing Bambu Studio's add-by-IP probe path; server-side web-wall RTSPS cameras are implemented (§6).
- Per-client serial ACL binding (a client may only touch the serial matching its access code).
- Optional Prometheus metrics endpoint.
- On-disk subscription persistence across proxy restarts.

## 15. Container deployment

The image is multi-arch (`linux/amd64`, `linux/arm64`) and stateless by default: the self-signed certificate is generated in memory per start when the cert paths are empty (clients skip verification, as they do against printers). Operators who want a stable certificate point `cert_file`/`key_file` at a writable mounted directory. The image runs as a non-root user, bundles FFmpeg for RTSPS cameras, and ships a `HEALTHCHECK` probing `/livez` (interval 30 s, timeout 3 s, start period 5 s).

### 15.1 Configuration sources

Configuration comes from a YAML file. Only two environment variables override it: `BMBPX_HTTP_PORT` replaces `http.port`, and `BMBPX_LOG_LEVEL` replaces `log.level`. With no file, the proxy serves health plus the `/config` page, which creates the file on save.

| Variable | Default | Meaning |
|---|---|---|
| `BMBPX_LOG_LEVEL` | *(unset)* | A non-empty value overrides `log.level` (`debug` logs routing decisions, raw printer reports, every activity entry, and each plate-check, first-layer, AI detection, and job-preview decision with its scores, timings, and outcome) |
| `BMBPX_HTTP_PORT` | *(unset)* | A value from 0 to 65535 overrides `http.port`; 0 means unset (8080) |

### 15.2 Health monitoring

The shared HTTP server (config `http.port`, env `BMBPX_HTTP_PORT`) serves:

- `/livez`, `/readyz` — `200 ok` once the process serves. Upstream connectivity deliberately does NOT affect readiness: clients stay connected while printers recover, and readiness must not flap with printer power states.
- `/status` — JSON `{"status":"ok","upstreams":{"<serial>":true|false}}` plus one member per module with `StatusValue`, for dashboards and scrape jobs.

### 15.3 Build and run

```sh
docker buildx build --platform linux/amd64,linux/arm64 -t bambu-mqtt-proxy .

# configuration file:
docker run -d -p 8883:8883 -p 6000:6000 -p 8080:8080 \
  -v /path/to/config:/config \
  bambu-mqtt-proxy

# no file yet: the proxy serves only health plus the /config page until a save creates
# /config/bambu-mqtt-proxy.yaml in the mounted directory
docker run -d -p 8883:8883 -p 6000:6000 -p 8080:8080 -v config:/config \
  bambu-mqtt-proxy
```
