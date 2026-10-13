// Command bambu-mqtt-proxy is a multiplexing MQTT proxy for Bambu Lab
// printers: one TLS MQTT endpoint for many clients, one upstream connection
// per printer, routed by the serial number in device/{serial}/... topics.
// Configuration comes from a YAML file; BMBPX_HTTP_PORT and BMBPX_LOG_LEVEL
// override the HTTP port and the log level.
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
	"path/filepath"
	"syscall"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/broker"
	"bambu-mqtt-proxy/internal/camera"
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/configui"
	"bambu-mqtt-proxy/internal/control"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/health"
	"bambu-mqtt-proxy/internal/hmscodes"
	"bambu-mqtt-proxy/internal/httpsrv"
	"bambu-mqtt-proxy/internal/jobpreview"
	"bambu-mqtt-proxy/internal/mcpserver"
	"bambu-mqtt-proxy/internal/module"
	"bambu-mqtt-proxy/internal/notification"
	"bambu-mqtt-proxy/internal/pandabreath"
	"bambu-mqtt-proxy/internal/pandapwr"
	"bambu-mqtt-proxy/internal/platecheck"
	"bambu-mqtt-proxy/internal/printer"
	"bambu-mqtt-proxy/internal/routing"
	"bambu-mqtt-proxy/internal/safety"
)

// catalogBaseURL selects the official catalog endpoint; startup tests use loopback.
var catalogBaseURL = hmscodes.DefaultBaseURL

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bambu-mqtt-proxy:", err)
		os.Exit(1)
	}
}

// run resolves the configuration, serves until an interrupt signal, and
// re-executes the binary after the /config page saves the file. A startup
// failure after the configuration loads leaves only health and /config
// serving, with the error shown on the page.
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
	cfg, err := resolveConfig(path)
	if cfg == nil {
		return err
	}
	logger, lerr := newLogger(pick(cfg.Log.Level, *logLevel))
	if lerr != nil {
		logger, _ = newLogger("info")
		err = errors.Join(err, lerr)
	}
	if cfg.HTTP.ExplicitZero() {
		logger.Warn("http.port 0 means unset; serving HTTP on the default port", "port", cfg.HTTP.Port)
	}
	store := configui.NewStore(path)
	store.SetHTTPPort(cfg.HTTP.Port)
	setup := err != nil || len(cfg.Printers) == 0
	restart := false
	if !setup {
		var serr error
		if restart, serr = serve(sigCtx, cfg, logger, store); serr != nil {
			err, setup = serr, true
		}
	}
	if setup {
		if err != nil {
			logger.Error("startup failed; serving only health and /config", "error", err)
			store.SetStartupError(err)
		} else {
			logger.Warn("no printers configured; add them on the configuration page",
				"url", fmt.Sprintf("http://<host>:%d/config", cfg.HTTP.Port), "path", path)
		}
		var ferr error
		if restart, ferr = serveSetup(sigCtx, cfg, store, logger); ferr != nil {
			return errors.Join(err, ferr)
		}
	}
	if !restart {
		return nil
	}
	stop()
	logger.Info("restarting with the saved configuration")
	exe, err := os.Executable()
	if err == nil {
		err = syscall.Exec(exe, os.Args, os.Environ())
	}
	return fmt.Errorf("restart: %w", err)
}

// serve builds and serves every service from cfg until a signal or a saved
// configuration. It reports true when the /config page saved the file, and
// returns an error, after tearing every service down, when startup fails.
func serve(sigCtx context.Context, cfg *config.Config, logger *slog.Logger, store *configui.Store) (bool, error) {
	serials := make([]string, 0, len(cfg.Printers))
	aliases := make(map[string]string, len(cfg.Printers))
	for _, p := range cfg.Printers {
		serials = append(serials, p.Serial)
		if p.Alias != "" {
			aliases[p.Serial] = p.Alias
		}
	}
	table := routing.NewTable(serials, aliases)
	inject := broker.NewInjector(logger)
	activities := activity.New(cfg.Printers)
	activities.SetLogger(logger)
	printerStore := printer.NewStore(cfg.Printers)
	printerStore.SetActivity(activities)
	// catalog supplies cached and downloaded messages for each real serial prefix.
	catalog := hmscodes.New(catalogBaseURL, filepath.Join(filepath.Dir(store.Path()), "hms"), serials, logger)
	printerStore.SetCatalog(catalog)
	links := printer.New(cfg.Printers, logger, func(serial, topic string, payload []byte) {
		inject.PublishDownstream(topic, payload)
		printerStore.Apply(serial, payload)
	}, func(serial string, online bool) {
		printerStore.SetOnline(serial, online)
	})
	// Panda Breath accessory chamber readings: one read-only WebSocket per
	// printer configured with a panda_breath address, for models whose
	// reports carry no usable chamber sensor. The wall and MCP project the
	// reading through the document store, so the observer runs
	// independently of the camera feature.
	breath := pandabreath.New(cfg.Printers, logger)
	printerStore.SetChamberReader(breath.Chamber)
	// Panda PWR smart plug power: one HTTP poll loop per printer
	// configured with a panda_pwr address, feeding the wall panel and
	// MCP state.modules.pandapwr.
	pwr := pandapwr.New(cfg.Printers, logger)
	// Printer controls: the only allow-listed path from the camera wall and
	// MCP to printer commands. No heater or temperature command exists.
	controls := control.New(links, printerStore, activities)
	// Printer job preview: one best-effort FTPS retrieval of the current
	// print's sliced 3MF render and metadata per settled job, shared by the
	// camera wall and MCP. It needs at least one consumer; with neither consumer the service must
	// not exist, so no FTPS socket can ever open. A disabled feature must
	// leave the consumer side truly nil.
	var previews *jobpreview.Service
	if cfg.CameraEnabled() || cfg.MCPEnabled() {
		previews = jobpreview.New(cfg.Printers, printerStore, logger)
		previews.SetActivity(activities)
	}

	plateSettings, err := platecheck.SettingsOf(cfg)
	if err != nil {
		return false, err
	}
	var cameras *camera.Manager
	var renderer *camera.StatusRenderer
	if cfg.CameraEnabled() {
		cameras = camera.NewWebManager(cfg.Printers, logger)
		renderer = camera.NewStatusRenderer(cameras, printerStore)
		renderer.SetActivity(activities)
		renderer.SetControl(controls)
	}
	// Optional OctoEverywhere Gadget detection: the YAML detection section.
	// With a key but the camera feature disabled the engine stays visible in
	// the blocked state and performs no camera or API activity.
	// The engine is constructed here but started only after the broker is
	// serving: a broker startup failure must not leave workers running.
	var detector *detection.Engine
	detectionSettings, err := detection.SettingsOf(cfg)
	if err != nil {
		return false, err
	}
	if detectionSettings.On() {
		client := detection.NewGadgetClient(detectionSettings.Key())
		if !cfg.CameraEnabled() {
			detector = detection.New(cfg.Printers, client, detection.IdleFrames{}, printerStore, links, logger)
			detector.SetBlocked(detection.ReasonCameraDisabled)
			logger.Warn("detection blocked: the camera feature is disabled")
		} else {
			detector = detection.New(cfg.Printers, client, detection.CameraFrames(cameras), printerStore, links, logger)
			logger.Info("octoeverywhere detection enabled")
		}
		detector.SetActivity(activities)
	}
	// Optional build-plate checks through a vision provider: the YAML
	// platecheck section. With credentials but the camera feature disabled
	// the service stays visible in the blocked state and performs no camera
	// or provider activity. With the feature off and cameras serving, a bare
	// service keeps the snapshot diagnostic route available to the
	// configuration page.
	var plates *platecheck.Service
	if plateSettings.On() || cfg.CameraEnabled() {
		var client platecheck.DecisionClient // nil while off
		if plateSettings.On() {
			client = platecheck.NewClient(plateSettings, logger)
		}
		if !cfg.CameraEnabled() {
			plates = platecheck.New(cfg.Printers, plateSettings, client, platecheck.IdleFrames{}, printerStore, links, logger)
			plates.SetBlocked(platecheck.ReasonCameraDisabled)
			logger.Warn("plate check blocked: the camera feature is disabled")
		} else {
			plates = platecheck.New(cfg.Printers, plateSettings, client, platecheck.CameraFrames(cameras), printerStore, links, logger)
		}
		plates.SetActivity(activities)
	}
	// Optional Pushover print notifications. The notifier registers as an
	// activity observer before the broker serves, so entries recorded
	// during startup reach it.
	var notifier *notification.Service
	notificationSettings, err := notification.SettingsOf(cfg)
	if err != nil {
		return false, fmt.Errorf("notification settings: %w", err)
	}
	if notificationSettings.Enabled {
		notifier = notification.New(notificationSettings, cfg.Printers, printerStore, cameras, logger)
		activities.SetObserver(notifier.Observe)
	}

	// Optional over-temperature safety stop: the YAML safety section. It
	// uses the notification section's Pushover credentials even while
	// ordinary notifications are disabled.
	var safe *safety.Service
	safetySettings, err := safety.SettingsOf(cfg)
	if err != nil {
		return false, fmt.Errorf("safety settings: %w", err)
	}
	if safetySettings.Enabled {
		safe = safety.New(cfg.Printers, printerStore, breath, links, activities, notificationSettings, logger)
	}

	// Optional modules retain their configured lifecycle order.
	var mods []module.Module
	if notifier != nil {
		mods = append(mods, notifier.Module())
	}
	if previews != nil {
		mods = append(mods, previews.Module())
	}
	mods = append(mods, breath.Module(), pwr.Module())
	if detector != nil {
		mods = append(mods, detector.Module())
	}
	if plates != nil {
		mods = append(mods, plates.Module())
	}
	if safe != nil {
		mods = append(mods, safe.Module())
	}
	if err := module.Check(mods); err != nil {
		return false, fmt.Errorf("modules: %w", err)
	}
	if renderer != nil {
		if err := renderer.SetModules(mods); err != nil {
			return false, fmt.Errorf("camera modules: %w", err)
		}
	}
	// MCP endpoint on the shared HTTP listener, on by default and disabled
	// with mcp.enabled: false. It reads
	// cached state for every configured printer; printer commands go only
	// through the allow-listed control service. Disabled features must leave the
	// Deps interface fields truly nil: a typed nil would pass the nil check
	// and panic on first use.
	var mcpsrv *mcpserver.Server
	if cfg.MCPEnabled() {
		deps := mcpserver.Deps{
			Printers: cfg.Printers,
			State:    printerStore,
			Activity: activities,
			Control:  controls,
			Modules:  mods,
			Log:      logger,
		}
		if cameras != nil {
			deps.Cameras = cameras
		}
		mcpsrv = mcpserver.New(deps)
	}

	srv, err := broker.New(cfg, table, links, inject, logger)
	if err != nil {
		return false, err
	}

	// Serve is non-blocking: it starts listeners and the event loop, then
	// returns. Block on signals instead.
	if err := srv.Serve(); err != nil {
		links.Close()
		_ = srv.Close()
		return false, fmt.Errorf("broker: %w", err)
	}

	// Install teardown before starting module goroutines or camera listeners.
	var httpSrv *httpsrv.Server
	var raw *camera.RawServer
	var cancels []context.CancelFunc
	var done []chan struct{}
	defer func() {
		logger.Info("shutting down")
		if mcpsrv != nil {
			mcpsrv.Close()
		}
		if httpSrv != nil {
			httpSrv.Stop()
		}
		for i := len(cancels) - 1; i >= 0; i-- {
			cancels[i]()
		}
		for i := len(done) - 1; i >= 0; i-- {
			<-done[i]
		}
		if raw != nil {
			raw.Close()
		}
		if cameras != nil {
			cameras.Close()
		}
		links.Close()
		_ = srv.Close()
	}()

	// Raw camera endpoint: the camera feature keeps its printer-compatible
	// listener even when the HTTP port is off. Starting it after the
	// deferred teardown is installed keeps a bind or certificate failure on
	// the same full shutdown path.
	if cfg.CameraEnabled() {
		r := camera.NewRawServer(cameras, logger)
		if err := r.Start(); err != nil {
			return false, fmt.Errorf("raw camera endpoint: %w", err)
		}
		raw = r
		logger.Info("raw camera endpoint serving", "port", camera.Port)
	}

	// catalogCtx and cancelCatalog control refreshes before printer reports arrive.
	catalogCtx, cancelCatalog := context.WithCancel(context.Background())
	// catalogDone lets teardown wait until every catalog worker stops.
	catalogDone := make(chan struct{})
	cancels = append(cancels, cancelCatalog)
	done = append(done, catalogDone)
	go func() {
		defer close(catalogDone)
		_ = catalog.Run(catalogCtx)
	}()

	// Start printer links at boot before modules, without waiting for printers.
	links.Start()
	// Start modules after the broker and camera listener are ready.
	for _, mod := range mods {
		if mod.Run != nil {
			ctx, cancel := context.WithCancel(context.Background())
			finished := make(chan struct{})
			cancels = append(cancels, cancel)
			done = append(done, finished)
			go func() {
				defer close(finished)
				if err := mod.Run(ctx); err != nil && ctx.Err() == nil {
					logger.Error("module stopped", "module", mod.Name, "error", err)
				}
			}()
		}
	}

	httpSrv = httpsrv.New(cfg.HTTP.Port, logger)
	activities.Register(httpSrv.Mux())
	sections := make(map[string]func() any)
	for _, mod := range mods {
		if mod.StatusValue != nil {
			sections[mod.Name] = mod.StatusValue
		}
	}
	health.Routes(httpSrv.Mux(), links, sections)
	store.Register(httpSrv.Mux())
	if mcpsrv != nil {
		mcpsrv.Register(httpSrv.Mux())
		logger.Info("mcp endpoint serving", "path", "/mcp")
	}
	if cfg.CameraEnabled() {
		camera.Register(httpSrv.Mux(), cameras)
		renderer.RegisterStatus(httpSrv.Mux())
		logger.Info("camera endpoints and camera wall serving", "port", cfg.HTTP.Port)
	}
	module.Mount(httpSrv.Mux(), mods)
	httpSrv.Mux().Handle("GET /{$}", http.RedirectHandler("/camwall", http.StatusFound))
	if err := httpSrv.Start(); err != nil {
		return false, fmt.Errorf("http server: %w", err)
	}
	logger.Info("http endpoints serving", "port", cfg.HTTP.Port)
	return waitStop(sigCtx, store), nil
}

// serveSetup serves only the health and /config endpoints, for a first run
// without printers or after a startup failure that /config shows. A bind
// failure on the HTTP port is an error.
func serveSetup(sigCtx context.Context, cfg *config.Config, store *configui.Store, logger *slog.Logger) (bool, error) {
	links := printer.New(nil, logger, nil, nil)
	defer links.Close()
	httpSrv := httpsrv.New(cfg.HTTP.Port, logger)
	health.Routes(httpSrv.Mux(), links, nil)
	store.Register(httpSrv.Mux())
	httpSrv.Mux().Handle("GET /{$}", http.RedirectHandler("/config", http.StatusFound))
	if err := httpSrv.Start(); err != nil {
		return false, fmt.Errorf("http server: %w", err)
	}
	defer httpSrv.Stop()
	return waitStop(sigCtx, store), nil
}

// waitStop blocks until a signal (false) or a saved configuration (true).
func waitStop(sigCtx context.Context, store *configui.Store) bool {
	select {
	case <-sigCtx.Done():
		return false
	case <-store.Saved():
		return true
	}
}

// resolveConfig builds the configuration from an optional YAML file.
// BMBPX_HTTP_PORT and BMBPX_LOG_LEVEL override the HTTP port and the log
// level. Without a file it returns the defaults with no printers (setup
// mode). A file that cannot be read or parsed returns a nil configuration;
// a configuration that fails validation is returned with its error so the
// caller can serve /config.
func resolveConfig(configPath string) (*config.Config, error) {
	cfg := &config.Config{}
	if _, err := os.Stat(configPath); err == nil {
		if cfg, err = config.Load(configPath); err != nil {
			return nil, err
		}
	}
	envErr := cfg.ApplyEnv()
	cfg.ApplyDefaults()
	if envErr != nil {
		return cfg, envErr
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	// Effective-only: the module sections re-check the effective values.
	return cfg, cfg.ValidateEffective()
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
