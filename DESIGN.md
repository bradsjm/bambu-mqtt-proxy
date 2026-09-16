# bambu-mqtt-proxy — Design

Status: proposed. This document is the implementation contract for a single Go binary.

## 1. Purpose

Many Bambu integrations (Bambu Studio, Home Assistant, xtouch, OctoPrint plugins) each open their own MQTT connection to a printer. The printer tolerates few connections, and each client must know every printer address. This proxy removes both problems:

- Clients connect to ONE MQTT endpoint.
- The proxy holds ONE MQTT connection per configured printer.
- The proxy routes traffic by the serial number in the topic path (`device/{serial}/...`).

### Goals

1. Accept about 10 (configurable, soft) downstream MQTT clients on port 8883 with TLS and a self-signed certificate.
2. Pose as a printer: MQTT behavior, authentication, and TLS match the printer's embedded broker, so existing apps (Bambu Studio, Home Assistant, xtouch, OctoPrint plugins) work without changes.
3. Hold exactly one upstream MQTT connection per configured printer endpoint.
4. Route publishes and subscriptions bidirectionally by serial number.
5. Stay transparent: forward payloads byte-for-byte. No JSON parsing.
6. Ship as one small static Go binary with a YAML config file.

### Non-goals

- Camera proxy (port 6000 / 322), FTPS proxy (port 990), file transfer.
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
        └───┬──────────────┬───────────┘
            │ 1 conn       │ 1 conn        (paho.mqtt.golang)
            ▼              ▼
     P1S 192.168.1.42  P1S 192.168.1.43
       :8883 MQTTS       :1883 MQTT (configurable)
```

Two halves, one process:

1. **Downstream broker** — mochi-mqtt v2 with a TLS listener on 1883. Mochi owns all MQTT protocol mechanics for clients (CONNECT/CONNACK, keepalive per client, SUBACK/PUBACK, session takeover).
2. **Upstream pool** — one paho.mqtt.golang client per configured printer, created lazily, reconnected automatically. A routing table maps serial → upstream connection and tracks merged subscriptions.

A custom mochi hook is the only coupling between the halves. The hook rewrites where packets go; it does not rewrite packets.

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

## 6. Routing model (core)

Topic grammar: `device/{serial}/report` and `device/{serial}/request`. The serial is the second level. Any other topic shape is denied/dropped and logged.

### 6.1 Subscribe path (downstream → upstream)

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

### 6.2 Publish path (downstream → upstream)

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

### 6.3 Report path (upstream → downstream)

```
printer PUBLISH device/{sn}/report
  → paho MessageHandler on the sn's upstream connection
      → server.Publish(topic, payload, retain=false, qos=1)   (mochi inline client)
      → mochi fans out to every subscribed downstream client
```

Mochi owns per-client QoS delivery and PUBACK handling. Retain is never set: Bambu reports are ephemeral state, and stale snapshots must not survive reconnects.

Serial-level wildcard subscribers (`device/+/report`) receive reports from ALL printers — inherent to the single-endpoint design. Clients demux by the serial in the topic, which every Bambu app already parses.

### 6.4 Unsubscribe / disconnect reconciliation

- `OnUnsubscribe`: decrement refcount; at zero, upstream unsubscribe.
- `OnDisconnect`: drop that client's filter set (tracked per client id at subscribe time), recompute all refcounts, upstream-unsubscribe any filter that reached zero. This covers ungraceful TCP drops, which never send UNSUBSCRIBE.

### 6.5 Warmup (state correctness for P1)

P1 printers push deltas only after the first `pushall`. The proxy sends `{"pushing":{"sequence_id":"0","command":"pushall"}}` to `/request` (a) when the first downstream client subscribes to a serial, and (b) after each upstream reconnect. Configurable (`warmup_commands`, default `["pushall"]`). Every subscriber — including late joiners — then receives full state.

## 7. QoS, keepalive, ordering

| Concern | Decision |
|---|---|
| QoS | Cap all hops at QoS 1. Bambu uses 0/1; QoS 2 adds state for no benefit. |
| Downstream keepalive | Per client, owned by mochi. |
| Upstream keepalive | 30 s, owned by paho. |
| Ordering | Per serial, upstream publish and report delivery are FIFO (single paho connection). Cross-serial ordering is not guaranteed and not needed. |
| Message loss windows | Upstream disconnect: reports in flight are lost; next warmup `pushall` restores full state. Accepted for v1. |
| Duplicate suppression | Exactly ONE upstream publish per downstream publish; the broker hop completes the QoS flow so clients never retransmit into the proxy. |

## 8. Configuration

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

Validation at startup: unique serials, resolvable addresses, TLS flag consistency. Startup fails fast on invalid config.

The config file contains printer access codes in plain text. Deploy with restrictive file permissions (e.g. chmod 600) and never log passwords; connection logs redact credentials.

## 9. Package layout and dependencies

```
cmd/bambu-mqtt-proxy/main.go   flags, config load, wiring, graceful shutdown
internal/config/               schema, defaults, validation
internal/broker/server.go      mochi server, TLS listeners, cert bootstrap
internal/broker/bridge.go      routing hooks + per-client filter tracking + refcounts
internal/upstream/pool.go      serial → connection table, lazy connect, reconnect/resubscribe
internal/upstream/conn.go      paho client wrapper (connect, publish, merged subscribe)
internal/routing/topic.go      serial extraction, wildcard expansion, filter↔serial sets
internal/tlsutil/              self-signed certificate generation and persistence
```

Dependencies: `github.com/mochi-mqtt/server/v2`, `github.com/eclipse/paho.mqtt.golang`, `gopkg.in/yaml.v3`, plus stdlib. Estimated ~1,200 LOC; static binary ≈ 12 MB; cross-compiles with plain `go build` for linux/darwin/arm64/amd64.

## 10. Failure modes and limitations

| Failure | Behavior | Consequence |
|---|---|---|
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
| Bambu Studio "add printer by IP" | Studio probes camera/FTP ports in addition to MQTT; probe failure can block discovery | MQTT control and status work; camera/FTP passthrough is future work |
| Tools that pin the printer TLS certificate | The self-signed proxy cert fails pinning | Load a custom cert/key via config, or disable pinning (clients must already skip verify against the real printer) |
| Downstream QoS 1 command while printer offline | PUBACK was already issued; command does not reach the printer | App-level retry, identical to a direct-connection drop |
| Malicious/buggy client publishes to `/report` | Denied by ACL | QoS 0 silently dropped; QoS ≥ 1 client disconnected; other clients never see injected state |

## 11. Alternatives considered

1. **Mosquitto relay per printer** (disconn3ct pattern): proven, but N printers = N broker endpoints and configs; not Go; no single endpoint. Rejected against requirement (c).
2. **Transparent TCP/TLS proxy per printer** (pandaproxy pattern): no MQTT-level merge; one upstream per client defeats requirement (a) and cannot route by serial.
3. **Scraper/republish bridges** (bambu-bridge, Bambu-MQTT-Scraper): change topic semantics; not transparent for Bambu Studio.
4. **Hand-rolled MQTT server**: removes mochi dependency but re-implements protocol edge cases (varint lengths, QoS flows, keepalive) with real correctness risk for zero user-visible gain.
5. **mochi for both directions**: impossible — mochi is a server only; the printer side needs a client library (paho).

## 12. Verification plan

1. **Unit**: filter resolution tables (exact serial → one printer, serial wildcard → all printers, off-grammar/unknown → deny); refcount transitions (first subscribe → upstream sub, last unsubscribe → upstream unsub, disconnect reconciliation); deny paths (unknown serial, off-grammar topic).
2. **Integration** (fake printer): run a mochi broker with `bblp` auth + TLS as a stand-in printer; drive 3 fake clients; assert merged upstream subscriptions, report fan-out to exactly the subscribed clients, SUBACK 0x80 on unknown serial, warmup pushall on first subscribe and after forced upstream reconnect. Assert printer-parity behavior: CONNACK code 4 on wrong access code, QoS cap on SUBACK grants, retain stripping, same-ID session takeover. Assert routing correctness: exactly ONE upstream publish per downstream QoS 1 publish (no retransmit duplicates), `device/+/report` fans out from all printers, publishes to `/report` are denied, requests never fan out to other downstream clients. Assert availability lifecycle: SUBACK 0x80 while the printer is stopped, background retries follow the configured backoff schedule (no tight loop), and the proxy reconnects, resubscribes, and sends warmup pushall when the printer returns — with no client action.
3. **Smoke (manual)**: real P1S in LAN Mode; Bambu Studio and Home Assistant both connect to the proxy on `:8883` with the printer's access code, as if it were the printer; observe `pushall` warmup and delta flow in debug logs.

## 13. Future work (explicitly out of scope for v1)

- Camera (6000/322) and FTPS (990) TCP passthrough, completing Bambu Studio's add-by-IP probe path.
- Per-client serial ACL binding (a client may only touch the serial matching its access code).
- Optional Prometheus metrics endpoint.
- On-disk subscription persistence across proxy restarts.

## 14. Container deployment

The image is multi-arch (`linux/amd64`, `linux/arm64`) and stateless by default: the self-signed certificate is generated in memory per start (clients skip verification, as they do against printers), and no volumes are required. Operators who want a stable certificate mount a writable directory and set `BMBPX_CERT_FILE`/`BMBPX_KEY_FILE`.

### 14.1 Configuration sources

Configuration comes from a YAML file, environment variables, or both; environment overrides replace file values per field. With no file, the proxy runs from environment alone.

| Variable | Default | Meaning |
|---|---|---|
| `BMBPX_PRINTERS` | — | Semicolon-separated printer entries: `serial=SN,address=host:port,password=code[,username=bblp][,tls=true][,insecure_skip_verify=true]` |
| `BMBPX_LISTEN_PORT` | `8883` | Downstream MQTT port |
| `BMBPX_LISTEN_TLS` | `true` | TLS on the downstream listener |
| `BMBPX_CERT_FILE` / `BMBPX_KEY_FILE` | empty | Empty = ephemeral in-memory self-signed certificate |
| `BMBPX_AUTH_MODE` | `printer` | `printer` or `accept_all` |
| `BMBPX_LOG_LEVEL` | `info` | `debug` logs routing decisions |
| `BMBPX_HEALTH_PORT` | `8080` in the image, `0` (off) outside | Health endpoint port |

Behavior tuning (`behavior:` in YAML) has no environment surface — its defaults match the design.

### 14.2 Health monitoring

The health server (config `health.port`, env `BMBPX_HEALTH_PORT`) serves:

- `/livez`, `/readyz` — `200 ok` once the process serves. Upstream connectivity deliberately does NOT affect readiness: clients stay connected while printers recover, and readiness must not flap with printer power states.
- `/status` — JSON `{"status":"ok","upstreams":{"<serial>":true|false}}` for dashboards and scrape jobs.

The image runs as a non-root user and ships a `HEALTHCHECK` probing `/livez` (interval 30 s, timeout 3 s, start period 5 s).

### 14.3 Build and run

```sh
docker buildx build --platform linux/amd64,linux/arm64 -t bambu-mqtt-proxy .

# env-only (stateless):
docker run -d -p 8883:8883 -p 8080:8080 \
  -e BMBPX_PRINTERS='serial=SN,address=printer.lan:8883,tls=true,password=CODE' \
  bambu-mqtt-proxy

# config file:
docker run -d -p 8883:8883 -p 8080:8080 \
  -v /path/to/bambu-mqtt-proxy.yaml:/config/bambu-mqtt-proxy.yaml:ro \
  bambu-mqtt-proxy
```
