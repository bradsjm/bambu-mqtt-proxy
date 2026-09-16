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
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/health"
	"bambu-mqtt-proxy/internal/routing"
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

	var healthSrv *health.Server
	if cfg.Health.Port > 0 {
		healthSrv = health.New(cfg.Health.Port, pool, logger)
		healthSrv.Start()
		logger.Info("health endpoints serving", "port", cfg.Health.Port)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()

	logger.Info("shutting down")
	pool.Stop()
	if healthSrv != nil {
		healthSrv.Stop()
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
	cfg := &config.Config{}
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
