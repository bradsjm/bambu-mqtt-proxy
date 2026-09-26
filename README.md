# bambu-mqtt-proxy

[![CI](https://github.com/bradsjm/bambu-mqtt-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/bradsjm/bambu-mqtt-proxy/actions/workflows/ci.yml)
[![Docker](https://github.com/bradsjm/bambu-mqtt-proxy/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/bradsjm/bambu-mqtt-proxy/actions/workflows/docker-publish.yml)
![Platforms](https://img.shields.io/badge/platform-linux%20amd64%20%7C%20arm64-blue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

One MQTT endpoint for all your Bambu Lab printers. A small, stateless, multi-arch
Go proxy that accepts many TLS MQTT clients on a single port and routes traffic
to each printer over **one** upstream connection, keyed by the serial number in
the Bambu topic path.

Bambu printers tolerate only a handful of MQTT connections (~4 on P1/X1, a
single one on A1). Every app you point at the printer — Bambu Studio, Home
Assistant, xtouch, OctoPrint plugins — burns one of those slots. This proxy
poses as the printer: your apps connect to it exactly as they would to the
printer (same port, `bblp` + access code, self-signed TLS), and the proxy holds
a single upstream connection per printer.

```
 Bambu Studio   Home Assistant   xtouch   ...        (≈10+ TLS clients)
      │               │             │
      └───────────────┼─────────────┘
                      ▼
        ┌──────────────────────────────┐
        │  bambu-mqtt-proxy            │
        │  TLS :8883 (self-signed)     │
        │  TLS :6000 raw camera        │
        │  serial → upstream table     │
        └───┬──────────────┬───────────┘
            │ 1 conn       │ 1 conn
            ▼              ▼
      P1S :8883        X1 :8883
```

## Features

- **Transparent** — apps work unchanged: port 8883, username `bblp`, printer
  access code, certificate verification skipped. Requests and reports are
  forwarded byte-for-byte (compatible with 2025+ signed-firmware printers).
- **One upstream connection per printer** — N clients share one merged
  subscription per printer; the printer's connection limit stops mattering.
- **Serial-based MQTT routing** — a single endpoint serves every configured
  printer; topics `device/{serial}/report` and `device/{serial}/request` select
  the target. The raw camera endpoint below is the one exception: it routes by
  access code.
- **Printer outage tolerant** — capped exponential backoff with jitter,
  automatic reconnect + resubscribe, and a `pushall` warmup so late joiners get
  full state (P1 printers otherwise send deltas only).
- **Printer-compatible MQTT** — MQTT 3.1.1, QoS capped at 1, same-ID session
  takeover, auth refusal, retain stripping.
- **Health endpoints** — `/livez`, `/readyz`, and `/status` (per-printer
  upstream connectivity JSON) for Docker/Kubernetes supervision.
- **P1/A1 camera wall** — serial-addressed JPEG snapshots and live MJPEG
  streams from the printer's chamber camera, plus a dashboard page that pairs
  every camera with color-coded print state, progress, and temperatures.
- **Raw camera passthrough** — a printer-compatible TLS listener on port 6000:
  camera apps authenticate with `bblp` plus a printer's access code, and the
  access code selects the printer.
- **Optional Gadget AI failure detection** — with an OctoEverywhere API key,
  snapshots from active prints are analyzed by the Gadget service and the proxy
  pauses the print on a likely failure. No key, no uploads, no automatic pauses.
- **Stateless** — no database, no volumes required. Config from a YAML file,
  environment variables, or both.
- **Multi-arch** — `linux/amd64` and `linux/arm64` images published automatically.

## Quick start

Prerequisite: enable **LAN Mode** on each printer (Settings → Network → LAN
Mode; Developer Mode is additionally required for control commands on current
firmware) and note the access code.

### Docker (env only — stateless)

```sh
docker run -d --name bambu-mqtt-proxy -p 8883:8883 -p 6000:6000 -p 8080:8080 \
  -e BMBPX_PRINTERS='serial=01P00A123456789,address=192.168.1.42:8883,tls=true,password=12345678,name=Garage P1S' \
  ghcr.io/bradsjm/bambu-mqtt-proxy:latest
```

The optional `name=…` part is a friendly display label for the camera wall.
MQTT routing always uses the serial number; the raw camera endpoint on port
6000 instead identifies the printer by the access code a client connects with.

### Docker (config file)

```sh
docker run -d --name bambu-mqtt-proxy -p 8883:8883 -p 6000:6000 -p 8080:8080 \
  -v $(pwd)/bambu-mqtt-proxy.yaml:/config/bambu-mqtt-proxy.yaml:ro \
  ghcr.io/bradsjm/bambu-mqtt-proxy:latest
```

Point your apps at the proxy host, port 8883, username `bblp`, and any
configured printer's access code. Camera apps that speak the printer's camera
protocol point at the proxy host, port 6000, with the same `bblp` username and
the target printer's access code.

### Docker Compose

```sh
cp compose.example.yaml compose.yaml   # then fill in your printers
docker compose up -d
```

### From source

```sh
go build -o bambu-mqtt-proxy ./cmd/bambu-mqtt-proxy
./bambu-mqtt-proxy -config bambu-mqtt-proxy.yaml
```

## Configuration

A YAML file and `BMBPX_*` environment variables may be combined (env overrides
file per field). See [`config.example.yaml`](config.example.yaml) and
[DESIGN.md §15](DESIGN.md) for the full reference.

```yaml
listen:
  - port: 8883
    tls: true
    cert_file: ./certs/proxy.crt   # generated on first start if missing
    key_file: ./certs/proxy.key
auth:
  mode: printer                    # require bblp + a configured access code
printers:
  - serial: "01P00A123456789"
    address: "192.168.1.42:8883"
    tls: true
    insecure_skip_verify: true
    username: "bblp"
    password: "12345678"
  - serial: "01S00C351100139"
    name: "Garage P1S"               # optional friendly label for the camera wall; never used for routing
    model: "P1S"                     # optional; camera support is auto-detected from the serial prefix
    address: "192.168.1.42:8883"
    tls: true
    insecure_skip_verify: true
    username: "bblp"
    password: "87654321"
behavior:
  qos_max: 1
  warmup_commands:
    - '{"pushing":{"sequence_id":"0","command":"pushall"}}'
http:
  port: 8080                         # 0 disables the HTTP server entirely
camera:
  enabled: true                      # false removes camera routes, the camera wall, and the raw camera listener on port 6000
log:
  level: info
```

| Environment variable | Default | Meaning |
|---|---|---|
| `BMBPX_PRINTERS` | — | Semicolon-separated printers: `serial=…,address=…,password=…[,name=…][,model=…][,username=…][,tls=…][,insecure_skip_verify=…]` |
| `BMBPX_LISTEN_PORT` | `8883` | Downstream MQTT port |
| `BMBPX_LISTEN_TLS` | `true` | TLS on the downstream listener |
| `BMBPX_CERT_FILE` / `BMBPX_KEY_FILE` | *(empty)* | Empty = ephemeral in-memory self-signed certificate |
| `BMBPX_AUTH_MODE` | `printer` | `printer` or `accept_all` |
| `BMBPX_LOG_LEVEL` | `info` | `info` logs client/upstream state, subscriptions, retries, and backoffs; `debug` adds per-packet routing |
| `BMBPX_HTTP_PORT` | `8080` | Shared health + camera HTTP port; `0` disables HTTP (the raw camera listener on 6000 keeps serving while cameras are enabled) |
| `BMBPX_CAMERA_ENABLED` | `true` | `false` removes the camera routes, the camera wall, and their MQTT report subscriptions, plus the raw camera listener on port 6000 |
| `BMBPX_OCTOEVERYWHERE_API_KEY` | *(empty)* | OctoEverywhere Gadget API key; empty = detection off. Setting it consents to external snapshot uploads and automatic pauses — see [Gadget AI failure detection](#gadget-ai-failure-detection-optional) |

At the default `info` level, logs identify downstream clients by MQTT client ID
and remote address, show which configured printers each client subscribes to,
and report upstream connection attempts, recovery, merged subscriptions,
warmup commands, retries, and measured reconnect delays. Use `debug` when
per-packet request routing is also required.

## Health and camera HTTP

Health and camera endpoints share one HTTP listener (`BMBPX_HTTP_PORT`,
YAML `http.port`). When cameras are disabled only the health endpoints are
served; with `http.port: 0` no HTTP server starts at all. The raw camera
endpoint is separate: it listens on TLS port 6000 whenever cameras are enabled
— including with `http.port: 0` — and never starts when they are disabled.

| Endpoint | Meaning |
|---|---|
| `/livez`, `/readyz` | `200 ok` once serving (printer state deliberately excluded — clients stay connected while printers recover) |
| `/status` | JSON: `{"status":"ok","upstreams":{"<serial>":true\|false}}`, plus a `detection` map per printer when the OctoEverywhere key is set |
| `/camera/{serial}/snapshot` | Single JPEG frame (P1/A1 camera protocol, port 6000) |
| `/camera/{serial}/stream` | Live multipart MJPEG stream |
| `/camera/status` | Display state for every printer (no credentials) |
| `/camera/events` | The same display state as server-sent events: on connect, on change, and at least every 10 s |
| `/camwall` | Multi-printer camera wall dashboard |
| `/favicon.ico`, `/apple-touch-icon.png` | Camera wall browser-tab, bookmark, and home-screen icons |

The camera wall receives printer state over `/camera/events` and sizes its tiles to the
window width, from one column on phones to a full grid on wall displays. Each
tile shows its configured `name` (or model and serial), its state with an icon
and color (printing, paused, preparing, failed, finished, idle, offline, and
"no recent data" when a busy printer has not reported for 2 minutes), a
progress edge on the camera, and a headline progress/time-left figure. Print
errors and HMS alerts appear on every tile as a severity-colored banner, with
the full list and last report age in the full detail level. Click or tap a tile's details panel to
step through compact, vitals (progress, layers, finish time, nozzle/bed/chamber
temperature gauges), and full details.
With an OctoEverywhere key, each tile also carries a compact AI-inspection
badge with the current quality score; inspection findings join the alert rows
at the same severity scale, higher detail levels add inspection facts, and
important transitions (a pause sent or confirmed, a raised warning, a lost
camera view, suspension) are announced to assistive technology. Without a key
none of these elements exist and the wall is unchanged.
Click a camera to focus it full-width.
Drag the grip (or focus it and use the arrow keys) to reorder tiles. The
fleet chips in the top bar count printers per state and double as filters;
"needs attention" collects failed, paused, stale, and serious-alert printers,
plus detection warnings or pause attempts when the Gadget integration is on.

Focused cameras and active prints (running or paused) hold the live streams,
up to four visible, connected printers by default. Other visible tiles use
snapshots; off-screen tiles, disconnected printers, and background browser
tabs hold no camera connection. The settings dialog sets the live-stream cap
(1–16), snapshot refresh interval (2–60 seconds), tile size, and detail level
for every tile, plus an option to keep the screen awake. Order, filters, detail
levels, and settings persist in that browser's local storage only; the proxy
stores no wall state. `?kiosk=1` hides the top bar and always requests a screen
wake lock, and `?maxLive=` / `?interval=` override the stored stream settings.
Browsers grant wake locks only on HTTPS or `localhost` pages, and over plain
HTTP they allow about six connections per host, shared by live streams,
snapshots, and the status stream.

Camera capture is restricted to models that use the Bambu chamber image
protocol: `P1P`, `P1S`, `A1`, and `A1MINI`. Support is auto-detected from the
serial prefix (`01P`=P1P, `01S`=P1S, `030`=A1 MINI, `039`=A1), so no
per-printer configuration is required. An explicit `model` field overrides
the inference if you ever need it. Ineligible serials (X1-class RTSP cameras,
unknown prefixes) answer `404` (unknown serial) or `422` (unsupported model)
without ever opening a camera socket. Camera and camera wall endpoints are
unauthenticated by design — anyone who can reach the HTTP port can view
cameras and telemetry, and the state endpoint never includes credentials.
Chamber temperature is shown only for models known to have a physical chamber
sensor (including X1/X2/P2/H2 models). P1 and A1 models omit the chamber
reading even if their MQTT report contains `chamber_temper`.

The raw camera endpoint serves the printer's own camera protocol on TCP port
6000. A client connects over TLS, sends the printer's 80-byte `bblp` +
access-code authentication, and receives the printer's native JPEG frame flow
unchanged. No topic serial travels on this path: the access code selects the
printer, so the proxy needs unique access codes across printers to route raw
camera clients correctly (MQTT routing is unaffected, and the proxy does not
reject duplicate codes). The listener starts with the camera feature — even
with `http.port: 0` — and a port or certificate failure fails startup with a
wrapped error.

### Gadget AI failure detection (optional)

Setting `BMBPX_OCTOEVERYWHERE_API_KEY` to an
[OctoEverywhere Gadget API key](https://octoeverywhere.com/gadgetapi) enables AI
print-failure detection. The key is the only setting — there is no YAML field
and no usage bookkeeping; the proxy stays stateless. While a print is active on
a camera-capable printer, the proxy uploads the current camera snapshot to
OctoEverywhere's Gadget service at whatever pace each response directs — the
interval is dynamic and provider-controlled, never below the 5-second minimum
and possibly well under 20 seconds — and receives a print-quality score (1–10)
plus warning and pause recommendations. Warnings are
surfaced per printer as a `detection` object on `/camera/status`,
`/camera/events`, and `/status`; a pause recommendation makes the proxy send a
single LAN `pause` command to the printer. Each object's `state` — starting,
idle, monitoring, attention, paused, degraded, unsupported, or blocked — shows
at a glance what detection is doing for that printer.

**Consent.** Adding the key is an explicit opt-in to both halves of the
service: camera snapshots leave your LAN for OctoEverywhere's servers, and the
proxy is authorized to pause prints without asking. Leave the key unset to keep
every image local and automatic pauses impossible. OctoEverywhere documents
that images submitted through the public Gadget API are not used for AI model
training.

**Eligibility and gates.** Detection rides on the camera pipeline, so it covers
exactly the camera-capable models (P1P, P1S, A1, A1MINI), only while telemetry
shows a print actively running. It requires the camera feature
(`BMBPX_CAMERA_ENABLED`, default on): with cameras disabled there are no frames
to analyze; with the key set, nothing runs and every printer's `detection`
object reports `blocked`. It does not require the HTTP listener — detection
keeps running with `BMBPX_HTTP_PORT=0`; only its status endpoints are then
absent.

**Usage and allowance.** The proxy tracks no quota and offers no usage
forecasts; allowance enforcement lives entirely on OctoEverywhere's side. Each
account includes 90,000 free inspection calls per monthly billing period —
about 500 print hours at the 20-second cadence the pricing assumes, but only
about 125 hours if the service directs 5-second inspections — and the
allowance is shared across the whole account: the same key in any other
printer, tool, or proxy instance consumes the same budget. Accounts default to
**Free Usage Only**, so inspections stop at the allowance instead of incurring
charges.

**Account errors suspend until restart.** An invalid or disabled key, a billing
failure, an IP restriction (another key claimed the account's public IP), or an
exhausted free allowance suspends detection for the rest of the proxy run, with
the reason logged. Fix the account issue and restart the proxy to resume.
Transient failures — network errors, timeouts, server errors, rate limiting —
are retried with capped backoff and never affect printing or camera serving;
when the primary upload URL fails at the connection or server level, the
context switches to the API's documented fallback URL. While suspended, the
last detection state stays visible, flagged with a suspension message.

**Limitations.** Monitoring is per-print and advisory. The proxy pauses only
when Gadget's confidence reaches the recommendation threshold, and the pause is
a normal LAN command — it needs a printer that accepts LAN commands (Developer
Mode on 2025+ signed firmware, as for any client) and a reachable printer.
Each recommendation pauses at most once; the proxy expects the printer to
confirm the pause in its reports and never retries a pause or resumes the
print automatically. Sessions are keyed to print identity and kept fresh
deliberately: each detected print session opens one analysis context, and it is
invalidated whenever its inputs go stale (old reports, stale camera frames, or
state-transition churn), so Gadget's temporal model is never fed history from a
different print.
Detection is stateless like the rest of the proxy: it keeps no inspection
history, and a restart clears every status and context. The feature follows
OctoEverywhere's documented API; its verification scope is described in
DESIGN.md §13.

## Security notes

- Runs as a non-root user in the container; the config file and environment
  contain printer access codes — protect them accordingly (never commit
  `bambu-mqtt-proxy.yaml`).
- TLS certificates are unverified on both hops, matching Bambu's own LAN
  protocol; the proxy is intended for trusted home LANs.
- The raw camera endpoint on port 6000 authenticates with `bblp` plus a
  configured printer access code; the code selects the printer.
- The optional OctoEverywhere key is a secret: keep it in your environment
  (`.env` is git-ignored) and share the account with caution — setting it
  authorizes external upload of camera snapshots and automatic print pauses,
  and usage is billed/limited per OctoEverywhere account, not per proxy.

## Development

```sh
go test -race ./...          # unit + fake-printer integration suite
go build ./cmd/bambu-mqtt-proxy
docker buildx build --platform linux/amd64,linux/arm64 -t bambu-mqtt-proxy .
```

Design, protocol research, failure-mode analysis, and the verification plan
live in [DESIGN.md](DESIGN.md).

## License

[MIT](LICENSE)
