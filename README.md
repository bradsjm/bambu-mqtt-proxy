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
- **Serial-based routing** — a single endpoint serves every configured printer;
  topics `device/{serial}/report` and `device/{serial}/request` select the target.
- **Printer outage tolerant** — capped exponential backoff with jitter,
  automatic reconnect + resubscribe, and a `pushall` warmup so late joiners get
  full state (P1 printers otherwise send deltas only).
- **Printer-compatible MQTT** — MQTT 3.1.1, QoS capped at 1, same-ID session
  takeover, auth refusal, retain stripping.
- **Health endpoints** — `/livez`, `/readyz`, and `/status` (per-printer
  upstream connectivity JSON) for Docker/Kubernetes supervision.
- **Stateless** — no database, no volumes required. Config from a YAML file,
  environment variables, or both.
- **Multi-arch** — `linux/amd64` and `linux/arm64` images published automatically.

## Quick start

Prerequisite: enable **LAN Mode** on each printer (Settings → Network → LAN
Mode; Developer Mode is additionally required for control commands on current
firmware) and note the access code.

### Docker (env only — stateless)

```sh
docker run -d --name bambu-mqtt-proxy -p 8883:8883 -p 8080:8080 \
  -e BMBPX_PRINTERS='serial=01P00A123456789,address=192.168.1.42:8883,tls=true,password=12345678' \
  ghcr.io/bradsjm/bambu-mqtt-proxy:latest
```

### Docker (config file)

```sh
docker run -d --name bambu-mqtt-proxy -p 8883:8883 -p 8080:8080 \
  -v $(pwd)/bambu-mqtt-proxy.yaml:/config/bambu-mqtt-proxy.yaml:ro \
  ghcr.io/bradsjm/bambu-mqtt-proxy:latest
```

Point your apps at the proxy host, port 8883, username `bblp`, and any
configured printer's access code.

### From source

```sh
go build -o bambu-mqtt-proxy ./cmd/bambu-mqtt-proxy
./bambu-mqtt-proxy -config bambu-mqtt-proxy.yaml
```

## Configuration

A YAML file and `BMBPX_*` environment variables may be combined (env overrides
file per field). See [`config.example.yaml`](config.example.yaml) and
[DESIGN.md §14](DESIGN.md) for the full reference.

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
behavior:
  qos_max: 1
  warmup_commands:
    - '{"pushing":{"sequence_id":"0","command":"pushall"}}'
log:
  level: info
```

| Environment variable | Default | Meaning |
|---|---|---|
| `BMBPX_PRINTERS` | — | Semicolon-separated printers: `serial=…,address=…,password=…[,username=…][,tls=…][,insecure_skip_verify=…]` |
| `BMBPX_LISTEN_PORT` | `8883` | Downstream MQTT port |
| `BMBPX_LISTEN_TLS` | `true` | TLS on the downstream listener |
| `BMBPX_CERT_FILE` / `BMBPX_KEY_FILE` | *(empty)* | Empty = ephemeral in-memory self-signed certificate |
| `BMBPX_AUTH_MODE` | `printer` | `printer` or `accept_all` |
| `BMBPX_LOG_LEVEL` | `info` | `debug` logs every routing decision |
| `BMBPX_HEALTH_PORT` | `8080` in image, `0` elsewhere | Health endpoint port |

## Health

| Endpoint | Meaning |
|---|---|
| `/livez`, `/readyz` | `200 ok` once serving (printer state deliberately excluded — clients stay connected while printers recover) |
| `/status` | JSON: `{"status":"ok","upstreams":{"<serial>":true\|false}}` |

## Security notes

- Runs as a non-root user in the container; the config file and environment
  contain printer access codes — protect them accordingly (never commit
  `bambu-mqtt-proxy.yaml`).
- TLS certificates are unverified on both hops, matching Bambu's own LAN
  protocol; the proxy is intended for trusted home LANs.

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
