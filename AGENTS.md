# Repository Guidelines

## Project Overview

`bambu-mqtt-proxy` is a stateless Go proxy for Bambu Lab printers. Many MQTT clients connect to one downstream endpoint, and the proxy routes each printer's traffic by serial number while keeping one upstream MQTT connection per configured printer. A shared HTTP service provides health and optional P1/A1 camera endpoints.

## Architecture & Data Flow

`cmd/bambu-mqtt-proxy/main.go` loads YAML and `BMBPX_*` overrides, applies defaults, validates configuration, wires services, and handles shutdown signals. MQTT reports flow from the per-printer connections through the broker to downstream subscribers. Client requests flow through broker ACL and routing hooks to the matching printer; payloads pass through unchanged.

- `internal/config` owns config parsing, defaults, environment overrides, and validation.
- `internal/broker` adapts the mochi MQTT server and its hooks. `internal/routing` resolves topic filters against configured serials.
- `internal/upstream` owns lazy per-printer connections, merged subscriptions, reconnects, and warmup commands.
- `internal/telemetry` observes reports without changing forwarding. `internal/camera` owns camera capture and endpoints. `internal/health` and `internal/httpsrv` provide the shared HTTP service.

Keep dependencies explicit at constructors and preserve package ownership. The broker injector is attached after construction to break its callback cycle with the upstream pool.

## Key Directories

| Path | Purpose |
| --- | --- |
| `cmd/bambu-mqtt-proxy/` | Application entry point and service wiring |
| `internal/` | Config, broker, routing, upstream, telemetry, camera, HTTP, health, and TLS packages |
| `internal/integration/` | End-to-end tests with fake printers and MQTT clients |
| `.github/workflows/` | CI checks and multi-architecture image publishing |

## Development Commands

```sh
go build -o bambu-mqtt-proxy ./cmd/bambu-mqtt-proxy
./bambu-mqtt-proxy -config bambu-mqtt-proxy.yaml
go test -race ./...
go vet ./...
test -z "$(gofmt -l .)" || (gofmt -l . && exit 1)
docker buildx build --platform linux/amd64,linux/arm64 -t bambu-mqtt-proxy .
```

CI runs the formatting check, `go vet ./...`, and `go test -race ./...` on pushes to `main` and pull requests.

## Code Conventions & Common Patterns

- Use standard Go formatting and package-local, descriptive names. Document exported declarations with Go doc comments.
- Pass dependencies to constructors. Use narrow interfaces at package boundaries where components need only a small contract.
- Guard shared mutable state with mutexes. Use explicit goroutines and channels for asynchronous work, and provide context or stop-channel shutdown paths.
- Wrap startup errors with operation context using `%w`. Log errors on asynchronous paths that cannot return them to the caller.
- Keep MQTT payloads byte-for-byte unchanged. Apply routing and access rules in broker hooks, not in payload transformations.
- Treat printer serials and configured topic filters as routing keys. `internal/routing/topic.go` defines the accepted topic grammar.

## Important Files

- `README.md` — quick start, configuration summary, endpoints, and developer commands.
- `DESIGN.md` — protocol background, package boundaries, failure behavior, and verification plan. Check details against source and examples when they affect current behavior.
- `config.example.yaml` and `compose.example.yaml` — annotated application and container configuration examples.
- `go.mod` and `go.sum` — Go version and dependency versions.
- `.github/workflows/ci.yml` — required formatting, vet, and race-test checks.

## Runtime/Tooling Preferences

Use Go `1.26.5` and Go modules, as declared in `go.mod`. No other package manager or lint tool is configured. The container build uses `CGO_ENABLED=0` and supports `linux/amd64` and `linux/arm64`.

Configuration can combine YAML and `BMBPX_*` variables; environment values override file values per field. Use `config.example.yaml` and the README's environment-variable table for supported settings. Do not commit `bambu-mqtt-proxy.yaml` or printer credentials.

## Testing & QA

Tests use Go's standard `testing` package. Unit tests live beside packages; `internal/integration/` runs the proxy against fake TLS MQTT printers. Prefer focused package tests during iteration, then run `go test -race ./...` and `go vet ./...` before completion. There is no stated numeric coverage threshold.

Use `DESIGN.md`'s verification plan and existing integration cases for MQTT routing, authentication, subscription merging, warmup, and outage behavior. Camera tests use fake camera servers; keep network waits bounded in tests.
