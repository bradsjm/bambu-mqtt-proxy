// Command bambu-mqtt-proxy is a multiplexing MQTT proxy for Bambu Lab
// printers: one TLS MQTT endpoint for many clients, one upstream connection
// per printer, routed by the serial number in device/{serial}/... topics.
// Configuration comes from a YAML file, BMBPX_* environment variables, or both.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bambu-mqtt-proxy/internal/broker"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/health"
	"bambu-mqtt-proxy/internal/httpsrv"
	"bambu-mqtt-proxy/internal/routing"
	"bambu-mqtt-proxy/internal/telemetry"
	"bambu-mqtt-proxy/internal/upstream"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bambu-mqtt-proxy:", err)
		os.Exit(1)
	}
}

// run resolves configuration (file, environment, or both), wires routing,
// the upstream pool, broker, and health server, then serves until an
// interrupt signal.
func run() error {
	configPath := flag.String("config", "", "path to the YAML config file (optional when BMBPX_* env vars are set)")
	logLevel := flag.String("log-level", "", "override log level (debug, info, warn, error)")
	flag.Parse()

	cfg, fileFound, err := resolveConfig(*configPath)
	if err != nil {
		return err
	}

	logger, err := newLogger(pick(cfg.Log.Level, *logLevel))
	if err != nil {
		return err
	}
	if !fileFound {
		logger.Info("config file not found; using environment configuration", "path", *configPath)
	}

	serials := make([]string, 0, len(cfg.Printers))
	for _, p := range cfg.Printers {
		serials = append(serials, p.Serial)
	}
	table := routing.NewTable(serials)
	inject := broker.NewInjector(logger)
	pool := upstream.NewPool(cfg.Printers, inject, cfg.Behavior, logger)
	// Telemetry observes upstream reports without changing forwarding. The
	// wrapper stamps each report with its per-connection delivery order and
	// the connection generation it arrived on, read before the merge, so
	// detection can never treat a pre-reconnect observation as current.
	state := telemetry.NewCache(cfg.Printers, logger)
	pool.SetObserver(func(serial string, seq, gen uint64, payload []byte) {
		state.ObserveReport(serial, seq, gen, payload)
	})

	var cameras *camera.Manager
	var renderer *camera.StatusRenderer
	if cfg.CameraEnabled() {
		cameras = camera.NewManager(cfg.Printers, logger)
		renderer = camera.NewStatusRenderer(cameras, state, pool)
	}

	// Optional OctoEverywhere Gadget detection. The key is env-only; with a
	// key but the camera feature disabled the engine stays visible in the
	// blocked state and performs no camera or API activity.
	// The engine is constructed here but started only after the broker is
	// serving: a broker startup failure must not leave workers running.
	var detector *detection.Engine
	if cfg.DetectionEnabled() {
		client := detection.NewGadgetClient(cfg.OctoEverywhereAPIKey)
		if !cfg.CameraEnabled() {
			detector = detection.New(cfg.Printers, client, idleFrames{}, state, pool, pool, logger)
			detector.SetBlocked(detection.ReasonCameraDisabled)
			logger.Warn("detection blocked: the camera feature is disabled",
				"env", config.EnvCameraEnable)
		} else {
			detector = detection.New(cfg.Printers, client, cameraFrames{m: cameras}, state, pool, pool, logger)
			logger.Info("octoeverywhere detection enabled")
		}
	}
	if renderer != nil && detector != nil {
		renderer.SetDetection(detector)
	}

	srv, err := broker.New(cfg, table, pool, inject, logger)
	if err != nil {
		return err
	}

	// Serve is non-blocking: it starts listeners and the event loop, then
	// returns. Block on signals instead.
	if err := srv.Serve(); err != nil {
		pool.Stop()
		return fmt.Errorf("broker: %w", err)
	}

	// The broker is up. Defer every teardown from here so both the signal
	// path and later startup errors (an HTTP bind failure) stop the
	// detector, camera captures, upstream pool, HTTP server, and broker —
	// in that order. Each stop is idempotent, so the single deferred call
	// never double-stops a service.
	var httpSrv *httpsrv.Server
	defer func() {
		logger.Info("shutting down")
		if detector != nil {
			detector.Close()
		}
		if cameras != nil {
			cameras.Close()
		}
		pool.Stop()
		if httpSrv != nil {
			httpSrv.Stop()
		}
		_ = srv.Close()
	}()

	// Start detection workers only after the broker accepted its
	// listeners.
	if detector != nil {
		detector.Start()
	}

	// Hold one report interest per printer when anything consumes live
	// state: the camera wall (HTTP on) or detection. Async on purpose: the
	// interest is recorded while printers may still be offline, and
	// onConnect restores the recorded interests on reconnect.
	if cfg.CameraEnabled() && (cfg.HTTP.Port > 0 || detector != nil) {
		for _, p := range cfg.Printers {
			pool.SubscribeAsync(p.Serial, fmt.Sprintf("device/%s/report", p.Serial), 1)
		}
	}

	if cfg.HTTP.Port > 0 {
		httpSrv = httpsrv.New(cfg.HTTP.Port, logger)
		health.Routes(httpSrv.Mux(), pool, detectionSource(detector))
		if cfg.CameraEnabled() {
			camera.Register(httpSrv.Mux(), cameras)
			renderer.RegisterStatus(httpSrv.Mux())
			logger.Info("camera endpoints and camera wall serving", "port", cfg.HTTP.Port)
		}
		if err := httpSrv.Start(); err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		logger.Info("http endpoints serving", "port", cfg.HTTP.Port)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()
	return nil
}

// detectionSource adapts the optional detection engine to the health
// endpoint contract. A disabled feature must become a true nil interface:
// the nil *detection.Engine converted directly is a typed nil that the
// /status handler cannot tell apart from a live engine, and calling
// DetectionMap on it panics.
func detectionSource(e *detection.Engine) health.DetectionSource {
	if e == nil {
		return nil
	}
	return e
}

// resolveConfig builds the configuration from an optional YAML file with
// BMBPX_* environment overrides on top. When neither is present it fails with
// the searched path. It reports whether the file was found.
func resolveConfig(configPath string) (*config.Config, bool, error) {
	if configPath == "" {
		configPath = config.DefaultConfigName()
	}
	cfg := &config.Config{HTTP: config.HTTP{Port: config.PortUnset}}
	fileFound := false
	if _, err := os.Stat(configPath); err == nil {
		var err error
		cfg, err = config.Load(configPath)
		if err != nil {
			return nil, false, err
		}
		fileFound = true
	}
	printersFromEnv, err := cfg.ApplyEnv()
	if err != nil {
		return nil, false, err
	}
	if !fileFound && !printersFromEnv {
		return nil, false, fmt.Errorf("no configuration: file %q not found and %s not set", configPath, config.EnvPrinters)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, false, err
	}
	return cfg, fileFound, nil
}

// pick returns the override when set, else the configured value.
func pick(configured, override string) string {
	if override != "" {
		return override
	}
	return configured
}

// newLogger builds the slog logger at the configured level.
func newLogger(level string) (*slog.Logger, error) {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lv})), nil
}

// cameraFrames adapts the camera manager to the detection engine's frame
// source. The engine receives plain values, so it never imports the camera
// package and the camera package never imports detection.
type cameraFrames struct {
	m *camera.Manager
}

// Acquire starts (or joins) the shared capture for serial; false means the
// serial is unknown or its model cannot serve camera frames.
func (f cameraFrames) Acquire(serial string) bool {
	_, st := f.m.Acquire(serial)
	return st == camera.StatusOK
}

// Release drops one camera consumer interest taken with Acquire.
func (f cameraFrames) Release(serial string) {
	f.m.Release(serial)
}

// WaitFrame waits up to timeout for a frame newer than after.
func (f cameraFrames) WaitFrame(serial string, ctx context.Context, after uint64,
	timeout time.Duration) (detection.Frame, bool) {
	frame := f.m.Wait(serial, ctx, after, timeout)
	if frame == nil {
		return detection.Frame{}, false
	}
	return detection.Frame{JPEG: frame.JPEG, Seq: frame.Seq, Captured: frame.Captured}, true
}

// idleFrames is the frame source for blocked detection: the engine parks its
// workers before touching it, but the dependency stays non-nil.
type idleFrames struct{}

func (idleFrames) Acquire(string) bool { return false }

func (idleFrames) Release(string) {}

func (idleFrames) WaitFrame(string, context.Context, uint64, time.Duration) (detection.Frame, bool) {
	return detection.Frame{}, false
}
