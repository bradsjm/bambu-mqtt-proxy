// Command bambu-mqtt-proxy is a multiplexing MQTT proxy for Bambu Lab
// printers: one TLS MQTT endpoint for many clients, one upstream connection
// per printer, routed by the serial number in device/{serial}/... topics.
// Configuration comes from a YAML file, BMBPX_* environment variables, or both.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/broker"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/configui"
	"bambu-mqtt-proxy/internal/control"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/health"
	"bambu-mqtt-proxy/internal/httpsrv"
	"bambu-mqtt-proxy/internal/jobpreview"
	"bambu-mqtt-proxy/internal/mcpserver"
	"bambu-mqtt-proxy/internal/notification"
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

// run resolves configuration (file, environment, or both), serves until an
// interrupt signal, and restarts every service whenever the /config page
// saves the file. A saved config that fails to start is rolled back.
func run() error {
	configPath := flag.String("config", "", "path to the YAML config file (optional; the /config page creates it)")
	logLevel := flag.String("log-level", "", "override log level (debug, info, warn, error)")
	flag.Parse()
	path := *configPath
	if path == "" {
		path = config.DefaultConfigName()
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	store := configui.NewStore(path)
	var saved *configui.Reload
	for {
		next, err := serveOnce(sigCtx, path, *logLevel, store)
		if err != nil {
			if saved == nil {
				return err
			}
			// The last save cannot start: restore the previous file and
			// report the failure on the page once services are back.
			fmt.Fprintln(os.Stderr, "bambu-mqtt-proxy: saved config failed to start; restoring the previous file:", err)
			store.Failed(err)
			if rerr := store.Restore(*saved); rerr != nil {
				return errors.Join(err, rerr)
			}
			saved = nil
			continue
		}
		if next == nil {
			return nil
		}
		saved = next
	}
}

// serveOnce builds and serves every service from the current configuration.
// It returns a reload request when the /config page saved the file, or nil
// when the process should exit.
func serveOnce(sigCtx context.Context, path, logLevel string, store *configui.Store) (*configui.Reload, error) {
	cfg, fileFound, err := resolveConfig(path)
	if err != nil {
		return nil, err
	}

	logger, err := newLogger(pick(cfg.Log.Level, logLevel))
	if err != nil {
		return nil, err
	}
	if len(cfg.Printers) == 0 {
		return serveSetup(sigCtx, cfg, path, store, logger)
	}
	if !fileFound {
		logger.Info("config file not found; using environment configuration", "path", path)
	}

	serials := make([]string, 0, len(cfg.Printers))
	for _, p := range cfg.Printers {
		serials = append(serials, p.Serial)
	}
	table := routing.NewTable(serials)
	inject := broker.NewInjector(logger)
	pool := upstream.NewPool(cfg.Printers, inject, cfg.Behavior, logger)
	activities := activity.New(cfg.Printers)
	pool.SetConnectivityObserver(func(serial string, connected bool, err error) {
		if connected {
			activities.Record(serial, "connected", activity.Info, "Upstream connection established")
			return
		}
		activities.Record(serial, "disconnected", activity.Warning, "Upstream connection lost; reconnecting")
	})
	// Telemetry observes upstream reports without changing forwarding. The
	// wrapper stamps each report with its per-connection delivery order and
	// the connection generation it arrived on, read before the merge, so
	// detection can never treat a pre-reconnect observation as current.
	state := telemetry.NewCache(cfg.Printers, logger)
	state.SetActivity(activities)
	// Printer controls: the only allow-listed path from the camera wall and
	// MCP to printer commands. No heater or temperature command exists.
	controls := control.New(pool, state, activities)
	pool.SetObserver(func(serial string, seq, gen uint64, payload []byte) {
		state.ObserveReport(serial, seq, gen, payload)
	})
	// Printer job preview: one best-effort FTPS retrieval of the current
	// print's sliced 3MF render and metadata per settled job, shared by the
	// camera wall and MCP. It serves through the shared HTTP listener and
	// needs at least one consumer; with neither consumer the service must
	// not exist, so no FTPS socket can ever open. Like the other optional
	// dependencies, a disabled feature must leave the consumer side truly
	// nil.
	var previews *jobpreview.Service
	if cfg.JobPreviewEnabled() && cfg.HTTP.Port > 0 && (cfg.CameraEnabled() || cfg.MCPEnabled()) {
		previews = jobpreview.New(cfg.Printers, state, pool, logger)
	}

	var cameras *camera.Manager
	var renderer *camera.StatusRenderer
	if cfg.CameraEnabled() {
		if cfg.HTTP.Port > 0 {
			cameras = camera.NewWebManager(cfg.Printers, logger)
		} else {
			cameras = camera.NewManager(cfg.Printers, logger)
		}
		renderer = camera.NewStatusRenderer(cameras, state, pool)
		renderer.SetActivity(activities)
		renderer.SetControl(controls)
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
		detector.SetActivity(activities)
	}
	if renderer != nil && detector != nil {
		renderer.SetDetection(detector)
		renderer.SetDetectionControl(detector)
	}
	if renderer != nil && previews != nil {
		renderer.SetJobPreview(previews)
	}
	// Optional Pushover print notifications. The notifier registers as an
	// activity observer before the broker serves, so entries recorded
	// during startup reach it; upstream connections are lazy.
	var notifier *notification.Service
	if cfg.Notifications.Enabled {
		notifier = notification.New(cfg.Notifications, state, cameras, logger)
		activities.SetObserver(notifier.Observe)
	}
	// MCP endpoint on the shared HTTP listener, on by default and disabled
	// with mcp.enabled: false / BMBPX_MCP_ENABLED=false. Its sampler reads
	// cached state for every configured printer; printer commands go only
	// through the allow-listed control service. Disabled features must leave the
	// Deps interface fields truly nil: a typed nil would pass the nil check
	// and panic on first use.
	var mcpsrv *mcpserver.Server
	if cfg.MCPEnabled() {
		deps := mcpserver.Deps{
			Printers:     cfg.Printers,
			State:        state,
			Connectivity: pool,
			Generations:  pool,
			Activity:     activities,
			Control:      controls,
			Log:          logger,
		}
		if cameras != nil {
			deps.Cameras = cameras
		}
		if detector != nil {
			deps.Detector = detector
			deps.DetectorControl = detector
		}
		if previews != nil {
			deps.JobPreviews = previews
		}
		mcpsrv = mcpserver.New(deps)
	}

	srv, err := broker.New(cfg, table, pool, inject, logger)
	if err != nil {
		return nil, err
	}

	// Serve is non-blocking: it starts listeners and the event loop, then
	// returns. Block on signals instead.
	if err := srv.Serve(); err != nil {
		pool.Stop()
		_ = srv.Close()
		return nil, fmt.Errorf("broker: %w", err)
	}
	if notifier != nil {
		notifier.Start()
	}

	// The broker is up. Defer every teardown from here so both the signal
	// path and later startup errors (a raw camera or HTTP bind failure) stop
	// the detector, MCP, HTTP server, preview scheduler, raw camera
	// listener, camera captures, upstream pool, and broker — in that order.
	// HTTP stops first among the shared services so its streaming handlers
	// end before the captures and connections they read, and the preview
	// scheduler closes after both of its consumers (MCP and the HTTP
	// handlers) stop but before the cameras and pool it reads. The raw
	// listener closes before the capture manager so its camera sockets never
	// outlive the captures they read. Each stop is idempotent, so the single
	// deferred call never double-stops a service.
	var httpSrv *httpsrv.Server
	var raw *camera.RawServer
	defer func() {
		if sigCtx.Err() != nil {
			logger.Info("shutting down")
		} else {
			logger.Info("restarting with the saved configuration")
		}
		if detector != nil {
			detector.Close()
		}
		if mcpsrv != nil {
			mcpsrv.Close()
		}
		if httpSrv != nil {
			httpSrv.Stop()
		}
		if previews != nil {
			previews.Close()
		}
		if raw != nil {
			raw.Close()
		}
		if notifier != nil {
			notifier.Close()
		}
		if cameras != nil {
			cameras.Close()
		}
		pool.Stop()
		_ = srv.Close()
	}()

	// The preview scheduler starts only after the broker is serving and the
	// deferred teardown is installed, so any later startup failure still
	// closes it on the single shutdown path.
	if previews != nil {
		previews.Start()
	}

	// Raw camera endpoint: the camera feature keeps its printer-compatible
	// listener even when the HTTP port is off. Starting it after the
	// deferred teardown is installed keeps a bind or certificate failure on
	// the same full shutdown path.
	if cfg.CameraEnabled() {
		r := camera.NewRawServer(cameras, logger)
		if err := r.Start(); err != nil {
			return nil, fmt.Errorf("raw camera endpoint: %w", err)
		}
		raw = r
		logger.Info("raw camera endpoint serving", "port", camera.Port)
	}

	// Start detection workers only after the broker accepted its
	// listeners.
	if detector != nil {
		detector.Start()
	}

	// Hold one report interest per printer when anything consumes live
	// state: the camera wall (HTTP on) or detection. Async on purpose: the
	// interest is recorded while printers may still be offline, and
	// the connection supervisor restores the recorded interests on each
	// (re)connect. MCP counts as a live-state consumer even with cameras
	// disabled, and notifications need the reports that produce activity
	// events even with no dashboard or MQTT client attached.
	if (cfg.CameraEnabled() && (cfg.HTTP.Port > 0 || detector != nil)) || cfg.MCPEnabled() || cfg.Notifications.Enabled {
		for _, p := range cfg.Printers {
			pool.SubscribeAsync(p.Serial, fmt.Sprintf("device/%s/report", p.Serial))
		}
	}

	if cfg.HTTP.Port > 0 {
		httpSrv = httpsrv.New(cfg.HTTP.Port, logger)
		activities.Register(httpSrv.Mux())
		health.Routes(httpSrv.Mux(), pool, detectionSource(detector))
		store.Register(httpSrv.Mux())
		if mcpsrv != nil {
			mcpsrv.Register(httpSrv.Mux())
			mcpsrv.Start()
			logger.Info("mcp endpoint serving", "path", "/mcp")
		}
		if previews != nil {
			previews.Register(httpSrv.Mux())
			logger.Info("job preview image route serving", "port", cfg.HTTP.Port)
		}
		if cfg.CameraEnabled() {
			camera.Register(httpSrv.Mux(), cameras)
			renderer.RegisterStatus(httpSrv.Mux())
			logger.Info("camera endpoints and camera wall serving", "port", cfg.HTTP.Port)
		}
		// Root: setup mode sends visitors to the configuration page; with
		// printers configured the camera wall is the main page. Without
		// cameras the wall does not exist, so / stays a plain 404.
		httpSrv.Mux().Handle("GET /{$}", http.RedirectHandler("/camwall", http.StatusFound))
		if err := httpSrv.Start(); err != nil {
			return nil, fmt.Errorf("http server: %w", err)
		}
		logger.Info("http endpoints serving", "port", cfg.HTTP.Port)
	}

	store.Applied()
	return waitReload(sigCtx, store), nil
}

// serveSetup serves only the HTTP health and configuration endpoints while
// no printer is configured, so a first run needs no config file.
func serveSetup(sigCtx context.Context, cfg *config.Config, path string, store *configui.Store,
	logger *slog.Logger) (*configui.Reload, error) {
	if cfg.HTTP.Port == 0 {
		return nil, fmt.Errorf("no printers configured in %q or %s, and HTTP is disabled (http.port: 0)",
			path, config.EnvPrinters)
	}
	pool := upstream.NewPool(nil, nil, cfg.Behavior, logger)
	defer pool.Stop()
	httpSrv := httpsrv.New(cfg.HTTP.Port, logger)
	health.Routes(httpSrv.Mux(), pool, nil)
	store.Register(httpSrv.Mux())
	httpSrv.Mux().Handle("GET /{$}", http.RedirectHandler("/config", http.StatusFound))
	if err := httpSrv.Start(); err != nil {
		return nil, fmt.Errorf("http server: %w", err)
	}
	defer httpSrv.Stop()
	logger.Warn("no printers configured; add them on the configuration page",
		"url", fmt.Sprintf("http://<host>:%d/config", cfg.HTTP.Port), "path", path)
	store.Applied()
	return waitReload(sigCtx, store), nil
}

// waitReload blocks until a signal (nil) or a saved configuration.
func waitReload(sigCtx context.Context, store *configui.Store) *configui.Reload {
	select {
	case <-sigCtx.Done():
		return nil
	case r := <-store.Reloads():
		return &r
	}
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
// BMBPX_* environment overrides on top. With neither present it returns the
// defaults with no printers (setup mode). It reports whether the file was
// found.
func resolveConfig(configPath string) (*config.Config, bool, error) {
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
	if _, err := cfg.ApplyEnv(); err != nil {
		return nil, false, err
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
