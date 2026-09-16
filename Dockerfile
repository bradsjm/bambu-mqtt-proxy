# syntax=docker/dockerfile:1
# Multi-arch: docker buildx build --platform linux/amd64,linux/arm64 -t bambu-mqtt-proxy .
# Go cross-compiles in the build stage, so no QEMU emulation is required.

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/bambu-mqtt-proxy ./cmd/bambu-mqtt-proxy

FROM alpine:3.22
RUN addgroup -S proxy && adduser -S -G proxy proxy \
    && apk add --no-cache ca-certificates
COPY --from=build --chown=proxy:proxy /out/bambu-mqtt-proxy /bambu-mqtt-proxy
USER proxy
# Container defaults: TLS MQTT on 8883, health on 8080. A mounted config file
# (CMD default) overrides these; unset any of them to fall back to file values.
ENV BMBPX_LISTEN_PORT=8883 \
    BMBPX_LISTEN_TLS=true \
    BMBPX_HEALTH_PORT=8080
EXPOSE 8883 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/livez >/dev/null 2>&1 || exit 1
ENTRYPOINT ["/bambu-mqtt-proxy"]
# The file is optional: when the mount is absent the proxy runs from
# BMBPX_* environment variables alone.
CMD ["-config=/config/bambu-mqtt-proxy.yaml"]
