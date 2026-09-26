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

	"bambu-mqtt-proxy/internal/broker"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
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
	// Telemetry observes upstream reports without changing forwarding.
	state := telemetry.NewCache(cfg.Printers, logger)
	pool.SetObserver(state.Observe)

	var cameras *camera.Manager
	var renderer *camera.StatusRenderer
	if cfg.CameraEnabled() {
		cameras = camera.NewManager(cfg.Printers, logger)
		renderer = camera.NewStatusRenderer(cameras, state, pool)
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

	var httpSrv *httpsrv.Server
	if cfg.HTTP.Port > 0 {
		httpSrv = httpsrv.New(cfg.HTTP.Port, logger)
		health.Routes(httpSrv.Mux(), pool)
		if cfg.CameraEnabled() {
			camera.Register(httpSrv.Mux(), cameras)
			renderer.RegisterStatus(httpSrv.Mux())
			// Hold one report interest per printer so the overlay shows
			// live state without any downstream MQTT clients. Async on
			// purpose: HTTP must start while printers are still offline,
			// and onConnect restores the recorded interests on reconnect.
			for _, p := range cfg.Printers {
				pool.SubscribeAsync(p.Serial, fmt.Sprintf("device/%s/report", p.Serial), 1)
			}
			logger.Info("camera and overlay endpoints serving", "port", cfg.HTTP.Port)
		}
		if err := httpSrv.Start(); err != nil {
			pool.Stop()
			_ = srv.Close()
			return fmt.Errorf("http server: %w", err)
		}
		logger.Info("http endpoints serving", "port", cfg.HTTP.Port)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()

	logger.Info("shutting down")
	if cameras != nil {
		cameras.Close()
	}
	pool.Stop()
	if httpSrv != nil {
		httpSrv.Stop()
	}
	_ = srv.Close()
	return nil
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
