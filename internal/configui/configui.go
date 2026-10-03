// Package configui serves the /config page and its JSON API. The page edits
// the YAML config file; saving writes the file and asks the process to
// restart its services with the new settings. Printer access codes are
// write-only: the API reports whether a code is stored but never returns it.
package configui

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/notification"
)

//go:embed config.html
var pageHTML []byte

// fileHeader starts every file written by the page.
const fileHeader = "# bambu-mqtt-proxy configuration, written by the /config page.\n" +
	"# Comments are not preserved when the page saves this file.\n"

// Reload asks the process to restart its services from the config file. It
// carries the file content before the save so a config that fails to start
// can be rolled back.
type Reload struct {
	Previous []byte
	Existed  bool
}

// Store owns the config file path and the apply status shared across
// service restarts. One Store lives for the whole process.
type Store struct {
	path    string
	reloads chan Reload
	rename  func(string, string) error
	// sendTest delivers the /config test notification; replaced by tests.
	sendTest func(context.Context, config.Notifications, string, string, []byte) error
	// probePrinter performs the /config printer connection test; replaced
	// by tests.
	probePrinter func(context.Context, config.Printer) error
	// probeDetection performs the /config Gadget API key test; replaced by
	// tests.
	probeDetection func(context.Context, string) error

	mu         sync.Mutex
	generation uint64
	applyErr   string
	failure    string
	pending    bool
}

// NewStore returns a store for the YAML file at path.
func NewStore(path string) *Store {
	return &Store{
		path:         path,
		rename:       os.Rename,
		reloads:      make(chan Reload, 1),
		sendTest:     notification.Send,
		probePrinter: probePrinterConn,
		probeDetection: func(ctx context.Context, key string) error {
			// The session is discarded: the test never processes frames.
			_, err := detection.NewGadgetClient(key).CreateContext(ctx)
			return err
		},
	}
}

// Reloads delivers one request per successful save.
func (s *Store) Reloads() <-chan Reload { return s.reloads }

// Applied records that services started from the current file. A failure
// recorded with Failed since the previous start is reported to the page.
func (s *Store) Applied() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generation++
	s.applyErr, s.failure = s.failure, ""
	s.pending = false
}

// Failed records why the most recently saved config could not start.
func (s *Store) Failed(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failure = err.Error()
}

// Restore puts the file back the way it was before the save in r.
func (s *Store) Restore(r Reload) error {
	if !r.Existed {
		if err := os.Remove(s.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", s.path, err)
		}
		return nil
	}
	return s.writeFile(r.Previous)
}

// Register adds GET /config (page), GET /config/api (settings),
// PUT /config/api (save and apply), POST /config/notifications/test
// (one test notification), POST /config/printers/test (one draft printer
// connection test), and POST /config/detection/test (one Gadget API key
// check). Writes are protected against cross-origin browser requests.
func (s *Store) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(pageHTML)
	})
	mux.HandleFunc("GET /config/api", s.handleGet)
	protection := http.NewCrossOriginProtection()
	mux.Handle("PUT /config/api", protection.Handler(http.HandlerFunc(s.handlePut)))
	mux.Handle("POST /config/notifications/test", protection.Handler(http.HandlerFunc(s.handleNotificationTest)))
	mux.Handle("POST /config/printers/test", protection.Handler(http.HandlerFunc(s.handlePrinterTest)))
	mux.Handle("POST /config/detection/test", protection.Handler(http.HandlerFunc(s.handleDetectionTest)))
}

// View is the editable configuration exchanged with the page.
type View struct {
	Printers      []PrinterView     `json:"printers"`
	Listen        []ListenerView    `json:"listen"`
	AuthMode      string            `json:"auth_mode"`
	HTTPPort      int               `json:"http_port"`
	CameraEnabled bool              `json:"camera_enabled"`
	MCPEnabled    bool              `json:"mcp_enabled"`
	Detection     DetectionView     `json:"detection"`
	LogLevel      string            `json:"log_level"`
	Behavior      BehaviorView      `json:"behavior"`
	Notifications NotificationsView `json:"notifications"`
}

// DetectionView is the Gadget AI detection section. APIKey is accepted on
// save and never returned; has_api_key reports an effective key — the
// stored file key or the BMBPX_OCTOEVERYWHERE_API_KEY environment value —
// without revealing it. A blank submitted key keeps the stored file key,
// and the environment secret is never written to the file.
type DetectionView struct {
	Enabled   bool   `json:"enabled"`
	HasAPIKey bool   `json:"has_api_key"`
	APIKey    string `json:"api_key,omitempty"`
}

// PrinterView is one printer. AccessCode is accepted on save and never
// returned; PreviousSerial names the stored printer an edit started from.
type PrinterView struct {
	Serial string `json:"serial"`
	Name   string `json:"name"`
	Model  string `json:"model"`
	// PandaBreath is the optional Panda Breath sensor WebSocket address.
	// Plain data, unlike the access code: the API returns it and a blank
	// submitted value clears it.
	PandaBreath        string `json:"panda_breath"`
	Address            string `json:"address"`
	TLS                bool   `json:"tls"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify"`
	Username           string `json:"username"`
	HasAccessCode      bool   `json:"has_access_code"`
	AccessCode         string `json:"access_code,omitempty"`
	PreviousSerial     string `json:"previous_serial,omitempty"`
}

// ListenerView is one downstream MQTT listener.
type ListenerView struct {
	Port     int    `json:"port"`
	TLS      bool   `json:"tls"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

// BehaviorView holds the upstream connection settings.
type BehaviorView struct {
	WarmupCommands        []string `json:"warmup_commands"`
	KeepaliveSeconds      int      `json:"keepalive_seconds"`
	ConnectTimeoutSeconds int      `json:"connect_timeout_seconds"`
	BackoffInitialSeconds int      `json:"backoff_initial_seconds"`
	BackoffMaxSeconds     int      `json:"backoff_max_seconds"`
}

// NotificationsView is the notification section of the page. AppToken and
// UserKey are accepted on save and never returned.
type NotificationsView struct {
	Enabled  bool         `json:"enabled"`
	Provider string       `json:"provider"`
	Pushover PushoverView `json:"pushover"`
}

// PushoverView holds Pushover credentials. The has_* flags report stored
// credentials to the page; incoming flags are ignored on save.
type PushoverView struct {
	HasAppToken bool   `json:"has_app_token"`
	HasUserKey  bool   `json:"has_user_key"`
	AppToken    string `json:"app_token,omitempty"`
	UserKey     string `json:"user_key,omitempty"`
}

type meta struct {
	Path       string `json:"path"`
	FileExists bool   `json:"file_exists"`
	FileError  string `json:"file_error,omitempty"`
	Generation uint64 `json:"generation"`
	ApplyError string `json:"apply_error,omitempty"`
	// CameraEnabled appears only while BMBPX_CAMERA_ENABLED overrides the
	// stored value: it carries the parsed effective switch so the page can
	// describe detection against the cameras that will actually run. The
	// editable stored switch stays in config.camera_enabled.
	CameraEnabled *bool             `json:"camera_enabled,omitempty"`
	EnvOverrides  map[string]string `json:"env_overrides"`
}

func (s *Store) handleGet(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	m := meta{Path: s.path, Generation: s.generation, ApplyError: s.applyErr, EnvOverrides: envOverrides()}
	s.mu.Unlock()
	// Same presence rule as ApplyEnv: a nonempty value overrides the file.
	// An unparseable value is left alone here — startup already rejects it
	// — rather than guessing an effective state for display.
	if v := os.Getenv(config.EnvCameraEnable); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			m.CameraEnabled = &b
		}
	}

	cfg := &config.Config{HTTP: config.HTTP{Port: config.PortUnset}}
	raw, existed, err := readFile(s.path)
	m.FileExists = existed
	if err != nil {
		m.FileError = err.Error()
	} else if existed {
		if parsed, err := config.Parse(raw); err != nil {
			m.FileError = err.Error()
		} else {
			cfg = parsed
		}
	}
	cfg.ApplyDefaults()
	writeJSON(w, http.StatusOK, map[string]any{"config": toView(cfg), "meta": m})
}

func (s *Store) handlePut(w http.ResponseWriter, r *http.Request) {
	var in View
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "The settings could not be read: "+err.Error())
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending {
		writeError(w, http.StatusConflict, "The proxy is still restarting with the last saved settings. Try again in a moment.")
		return
	}
	prev, existed, err := readFile(s.path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var stored *config.Config
	if existed {
		// An unreadable file has no codes to keep; the save replaces it.
		if cur, err := config.Parse(prev); err == nil {
			stored = cur
		}
	}
	cfg, err := in.toConfig(stored)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	body, err := yaml.Marshal(cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := append([]byte(fileHeader), body...)
	effective, err := check(out)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if err := s.writeFile(out); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.pending = true
	s.reloads <- Reload{Previous: prev, Existed: existed}
	writeJSON(w, http.StatusOK, map[string]any{
		"saved":     true,
		"path":      s.path,
		"http_port": effective.HTTP.Port,
	})
}

// handleNotificationTest sends one test notification with the submitted
// notification settings on top of the stored credentials. It reads the
// file without the save mutex and never saves, reloads, or returns either
// credential.
func (s *Store) handleNotificationTest(w http.ResponseWriter, r *http.Request) {
	var in NotificationsView
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "The settings could not be read: "+err.Error())
		return
	}

	raw, existed, err := readFile(s.path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var stored config.Notifications
	if existed {
		// An unreadable file has no stored credentials to keep.
		if cur, err := config.Parse(raw); err == nil {
			stored = cur.Notifications
		}
	}
	cfg := in.toConfig(stored)
	cfg.Enabled = true
	if err := cfg.Validate(); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	if err := s.sendTest(ctx, cfg, "bambu-mqtt-proxy test", "Pushover notifications are working.", nil); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sent": true})
}

// handlePrinterTest performs one bounded MQTT connect with the submitted
// draft printer settings and reports connectivity or authentication — it
// never verifies the serial, never subscribes, never publishes, and never
// saves. The application flow: decode the draft, reject settings that
// cannot run, reject destinations that a configured printer already owns
// (the test must not disturb a configured printer), then make exactly one
// connect attempt with a unique client id under a fixed timeout. Passing
// the test stays optional: saving and adding work exactly as before.
func (s *Store) handlePrinterTest(w http.ResponseWriter, r *http.Request) {
	var in PrinterView
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "The settings could not be read: "+err.Error())
		return
	}
	p := config.Printer{
		Serial:             strings.TrimSpace(in.Serial),
		Address:            strings.TrimSpace(in.Address),
		TLS:                in.TLS,
		InsecureSkipVerify: in.InsecureSkipVerify,
		Username:           strings.TrimSpace(in.Username),
		Password:           in.AccessCode,
	}
	if p.Username == "" {
		p.Username = "bblp"
	}
	if p.Serial == "" {
		writeError(w, http.StatusUnprocessableEntity, "Enter the printer's serial number.")
		return
	}
	if p.Address == "" {
		writeError(w, http.StatusUnprocessableEntity, "Enter the printer's address, for example 192.168.1.42:8883.")
		return
	}
	if strings.Contains(p.Address, "://") {
		writeError(w, http.StatusUnprocessableEntity, "Enter the address without a scheme, for example 192.168.1.42:8883.")
		return
	}
	// URL delimiters never belong in a host:port. Without this check a
	// userinfo or path fragment would reach the dialer or a URL parser and
	// send the test somewhere the user did not type.
	if strings.ContainsAny(p.Address, "/?#@") {
		writeError(w, http.StatusUnprocessableEntity, "Enter the address as host:port, for example 192.168.1.42:8883.")
		return
	}
	// The port must be explicit and numeric: paho would otherwise dial the
	// plain-MQTT default port 1883, which no Bambu printer serves.
	host, port, err := net.SplitHostPort(p.Address)
	if n, perr := strconv.Atoi(port); err != nil || host == "" || perr != nil || n < 1 || n > 65535 {
		writeError(w, http.StatusUnprocessableEntity, "Enter the address as host:port, for example 192.168.1.42:8883.")
		return
	}
	if p.Password == "" {
		writeError(w, http.StatusUnprocessableEntity, "Enter the printer's access code.")
		return
	}
	// A simple rejection, no alias resolution: a test connect may never
	// touch a destination a configured printer already owns, so both the
	// serial and the address are refused when they match the effective
	// configuration (stored file printers with the BMBPX_* environment
	// applied, exactly as startup resolves them).
	configured, err := s.effectivePrinters()
	if err != nil {
		// Fail closed: without the configured list the proxy cannot tell
		// whether the destination belongs to a configured printer, and the
		// test must never disturb one.
		writeError(w, http.StatusInternalServerError, "The configured printers could not be read, so the proxy cannot tell whether this destination is already configured. "+err.Error())
		return
	}
	for _, c := range configured {
		if strings.EqualFold(c.Serial, p.Serial) {
			writeError(w, http.StatusConflict, fmt.Sprintf("Printer %s is already configured. Test is only available for new printers.", c.Serial))
			return
		}
		if c.Address == p.Address {
			writeError(w, http.StatusConflict, fmt.Sprintf("Address %s is already used by printer %s.", p.Address, c.Serial))
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), testConnectTimeout)
	defer cancel()
	if err := s.probePrinter(ctx, p); err != nil {
		writeError(w, http.StatusBadGateway, classifyProbeError(err, p.Address))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleDetectionTest verifies one Gadget API key with a single
// CreateContext request — the same free call the detection engine makes
// before any print exists — and nothing else: no Process call, no image,
// no engine. The application flow: the submitted key wins, a blank
// submission falls back to the stored file key, and an existing
// BMBPX_OCTOEVERYWHERE_API_KEY overrides both, even when set-but-empty,
// exactly like startup. Nothing is saved.
func (s *Store) handleDetectionTest(w http.ResponseWriter, r *http.Request) {
	var in DetectionView
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "The settings could not be read: "+err.Error())
		return
	}

	key := strings.TrimSpace(in.APIKey)
	if key == "" {
		raw, existed, err := readFile(s.path)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if existed {
			// An unreadable file has no stored key to fall back on.
			if cur, perr := config.Parse(raw); perr == nil {
				key = cur.Detection.APIKey
			}
		}
	}
	// Presence, not value: an empty environment variable clears the stored
	// key, so an existing variable always decides, like ApplyEnv.
	if v, ok := os.LookupEnv(config.EnvOctoEverywhereAPIKey); ok {
		key = strings.TrimSpace(v)
	}
	if key == "" {
		if _, ok := os.LookupEnv(config.EnvOctoEverywhereAPIKey); ok {
			writeError(w, http.StatusUnprocessableEntity, config.EnvOctoEverywhereAPIKey+" is set but empty, so there is no key to test. Set the variable to a valid key.")
			return
		}
		writeError(w, http.StatusUnprocessableEntity, "No Gadget API key is configured. Enter a key or set "+config.EnvOctoEverywhereAPIKey+".")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), detectionTestTimeout)
	defer cancel()
	if err := s.probeDetection(ctx, key); err != nil {
		writeError(w, http.StatusBadGateway, classifyDetectionError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// effectivePrinters returns the printers a test connect must stay away
// from: the stored file list when the file parses, with the BMBPX_*
// environment applied as startup would — BMBPX_PRINTERS replaces the list
// entirely when set. Read, parse, and environment failures are errors, so
// the caller can fail closed instead of testing against an empty list.
func (s *Store) effectivePrinters() ([]config.Printer, error) {
	var ps []config.Printer
	raw, existed, err := readFile(s.path)
	if err != nil {
		return nil, err
	}
	if existed {
		c, perr := config.Parse(raw)
		if perr != nil {
			return nil, perr
		}
		ps = c.Printers
	}
	c := &config.Config{Printers: ps}
	if _, aerr := c.ApplyEnv(); aerr != nil {
		return nil, aerr
	}
	return c.Printers, nil
}

// check validates the file as written, both on its own and with the
// environment overrides the process applies on start.
func check(raw []byte) (*config.Config, error) {
	file, err := config.Parse(raw)
	if err != nil {
		return nil, err
	}
	file.ApplyDefaults()
	if err := file.Validate(); err != nil {
		return nil, err
	}
	eff, err := config.Parse(raw)
	if err != nil {
		return nil, err
	}
	if _, err := eff.ApplyEnv(); err != nil {
		return nil, err
	}
	eff.ApplyDefaults()
	if err := eff.Validate(); err != nil {
		return nil, fmt.Errorf("with environment overrides: %w", err)
	}
	// The detection key requirement is effective-only: the file alone may
	// enable detection with a blank key because the environment can supply
	// it, so an environment-only deployment can enable detection from the
	// page without persisting the secret.
	if err := eff.ValidateDetection(); err != nil {
		return nil, err
	}
	return eff, nil
}

func toView(c *config.Config) View {
	// Effective detection flags for display: the environment key overrides
	// the stored key without mutating the stored config. ApplyEnv stays the
	// only runtime resolver; this copy exists so the page shows the state a
	// restart would serve.
	detEff := *c
	if v, ok := os.LookupEnv(config.EnvOctoEverywhereAPIKey); ok {
		detEff.Detection.APIKey = strings.TrimSpace(v)
	}
	v := View{
		AuthMode:      c.Auth.Mode,
		HTTPPort:      c.HTTP.Port,
		CameraEnabled: c.CameraEnabled(),
		MCPEnabled:    c.MCP.Enabled == nil || *c.MCP.Enabled,
		Detection: DetectionView{
			Enabled:   detEff.DetectionEnabled(),
			HasAPIKey: detEff.DetectionKey() != "",
		},
		LogLevel: c.Log.Level,
		Printers: []PrinterView{},
		Listen:   []ListenerView{},
		Behavior: BehaviorView{
			WarmupCommands:        c.Behavior.WarmupCommands,
			KeepaliveSeconds:      c.Behavior.UpstreamKeepaliveSeconds,
			ConnectTimeoutSeconds: c.Behavior.UpstreamConnectTimeoutSeconds,
			BackoffInitialSeconds: c.Behavior.UpstreamBackoffInitialSeconds,
			BackoffMaxSeconds:     c.Behavior.UpstreamBackoffMaxSeconds,
		},
		Notifications: NotificationsView{
			Enabled:  c.Notifications.Enabled,
			Provider: c.Notifications.Provider,
			Pushover: PushoverView{
				HasAppToken: c.Notifications.Pushover.AppToken != "",
				HasUserKey:  c.Notifications.Pushover.UserKey != "",
			},
		},
	}
	for _, p := range c.Printers {
		v.Printers = append(v.Printers, PrinterView{
			Serial:             p.Serial,
			Name:               p.Name,
			Model:              p.Model,
			PandaBreath:        p.PandaBreath,
			Address:            p.Address,
			TLS:                p.TLS,
			InsecureSkipVerify: p.InsecureSkipVerify,
			Username:           p.Username,
			HasAccessCode:      p.Password != "",
		})
	}
	for _, l := range c.Listen {
		v.Listen = append(v.Listen, ListenerView(l))
	}
	return v
}

// toConfig builds the file configuration. A blank access code keeps the
// stored code of the printer the edit started from, but only while its
// address is unchanged: a stored code is never sent to a new host. Blank
// notification credentials keep their stored values. A nil stored config
// (missing or unreadable file) keeps submitted values as written.
func (v View) toConfig(stored *config.Config) (*config.Config, error) {
	var prevPrinters []config.Printer
	if stored != nil {
		prevPrinters = stored.Printers
	}
	byserial := make(map[string]config.Printer, len(prevPrinters))
	for _, p := range prevPrinters {
		byserial[p.Serial] = p
	}
	camera, mcp := v.CameraEnabled, v.MCPEnabled
	enabled := v.Detection.Enabled
	detection := config.Detection{Enabled: &enabled}
	if key := strings.TrimSpace(v.Detection.APIKey); key != "" {
		detection.APIKey = key
	} else if stored != nil {
		// The page never receives stored or environment secrets back, so a
		// blank key keeps the stored file key. Disabling preserves it too:
		// the switch, not a key removal, is the off action.
		detection.APIKey = stored.Detection.APIKey
	}
	c := &config.Config{
		Auth:      config.Auth{Mode: v.AuthMode},
		HTTP:      config.HTTP{Port: v.HTTPPort},
		Camera:    config.Camera{Enabled: &camera},
		MCP:       config.MCP{Enabled: &mcp},
		Detection: detection,
		Log:       config.Log{Level: v.LogLevel},
		Printers:  []config.Printer{},
		Behavior: config.Behavior{
			UpstreamKeepaliveSeconds:      v.Behavior.KeepaliveSeconds,
			UpstreamConnectTimeoutSeconds: v.Behavior.ConnectTimeoutSeconds,
			UpstreamBackoffInitialSeconds: v.Behavior.BackoffInitialSeconds,
			UpstreamBackoffMaxSeconds:     v.Behavior.BackoffMaxSeconds,
		},
	}
	storedNotifications := config.Notifications{}
	if stored != nil {
		storedNotifications = stored.Notifications
	}
	c.Notifications = v.Notifications.toConfig(storedNotifications)
	for _, cmd := range v.Behavior.WarmupCommands {
		if cmd = strings.TrimSpace(cmd); cmd != "" {
			c.Behavior.WarmupCommands = append(c.Behavior.WarmupCommands, cmd)
		}
	}
	for _, l := range v.Listen {
		c.Listen = append(c.Listen, config.Listener{
			Port:     l.Port,
			TLS:      l.TLS,
			CertFile: strings.TrimSpace(l.CertFile),
			KeyFile:  strings.TrimSpace(l.KeyFile),
		})
	}
	for _, p := range v.Printers {
		out := config.Printer{
			Serial:             strings.TrimSpace(p.Serial),
			Name:               strings.TrimSpace(p.Name),
			Model:              strings.TrimSpace(p.Model),
			PandaBreath:        strings.TrimSpace(p.PandaBreath),
			Address:            strings.TrimSpace(p.Address),
			TLS:                p.TLS,
			InsecureSkipVerify: p.InsecureSkipVerify,
			Username:           strings.TrimSpace(p.Username),
			Password:           p.AccessCode,
		}
		if out.Username == "" {
			out.Username = "bblp"
		}
		if out.Serial == "" {
			return nil, errors.New("Every printer needs a serial number.")
		}
		if out.Password == "" {
			prev, ok := byserial[p.PreviousSerial]
			switch {
			case !ok || prev.Password == "":
				return nil, fmt.Errorf("Enter the access code for printer %s.", out.Serial)
			case prev.Address != out.Address:
				return nil, fmt.Errorf("Enter the access code again for printer %s: its address changed.", out.Serial)
			}
			out.Password = prev.Password
		}
		c.Printers = append(c.Printers, out)
	}
	return c, nil
}

// toConfig builds notification settings from the submitted view. Submitted
// values are trimmed, and a blank submitted credential keeps the stored
// one. An empty submitted or stored provider means pushover, and incoming
// has_* flags are ignored.
func (v NotificationsView) toConfig(stored config.Notifications) config.Notifications {
	provider := strings.TrimSpace(v.Provider)
	if provider == "" {
		provider = strings.TrimSpace(stored.Provider)
	}
	if provider == "" {
		provider = "pushover"
	}
	out := config.Notifications{
		Enabled:  v.Enabled,
		Provider: provider,
		Pushover: config.Pushover{
			AppToken: strings.TrimSpace(v.Pushover.AppToken),
			UserKey:  strings.TrimSpace(v.Pushover.UserKey),
		},
	}
	if out.Pushover.AppToken == "" {
		out.Pushover.AppToken = strings.TrimSpace(stored.Pushover.AppToken)
	}
	if out.Pushover.UserKey == "" {
		out.Pushover.UserKey = strings.TrimSpace(stored.Pushover.UserKey)
	}
	return out
}

// envOverrides maps page field names to the BMBPX_* variable that
// overrides them, using the same presence rules as config.ApplyEnv.
func envOverrides() map[string]string {
	out := map[string]string{}
	set := func(field, env string) {
		if v, ok := os.LookupEnv(env); ok && v != "" {
			if _, dup := out[field]; !dup {
				out[field] = env
			}
		}
	}
	set("printers", config.EnvPrinters)
	for _, env := range []string{config.EnvListenPort, config.EnvListenTLS, config.EnvCertFile, config.EnvKeyFile} {
		if _, ok := os.LookupEnv(env); ok {
			if _, dup := out["listen"]; !dup {
				out["listen"] = env
			}
		}
	}
	set("auth_mode", config.EnvAuthMode)
	set("log_level", config.EnvLogLevel)
	set("http_port", config.EnvHTTPPort)
	set("camera_enabled", config.EnvCameraEnable)
	set("mcp_enabled", config.EnvMCPEnable)
	// The key override applies by presence, even when the value is empty:
	// an empty variable clears a stored key, so the page must lock the
	// field either way.
	if _, ok := os.LookupEnv(config.EnvOctoEverywhereAPIKey); ok {
		out["detection_api_key"] = config.EnvOctoEverywhereAPIKey
	}
	return out
}

func readFile(path string) ([]byte, bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read %s: %w", path, err)
	}
	return raw, true, nil
}

// writeFile replaces the config file atomically: it writes a sibling
// temporary file, syncs it, and renames it over the destination, keeping
// owner-only permissions and creating the directory when needed. The
// previous file survives every failure: an error removes only the temporary
// file. A rename failure — a bind-mounted single file cannot be replaced —
// is reported with the remedy instead of falling back to an in-place write,
// which a crash could leave half written.
func (s *Store) writeFile(data []byte) error {
	path := s.path
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, ".bambu-mqtt-proxy-*.yaml")
	if err != nil {
		return fmt.Errorf("create temporary file in %s: %w", dir, err)
	}
	name := tmp.Name()
	discard := func() {
		_ = tmp.Close()
		_ = os.Remove(name)
	}

	if _, err := tmp.Write(data); err != nil {
		discard()
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := tmp.Sync(); err != nil {
		discard()
		return fmt.Errorf("sync %s: %w", name, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("close %s: %w", name, err)
	}
	if err := s.rename(name, path); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("rename %s to %s: %w: %s", name, path, err,
			"configuration saves require a writable directory mount; single-file bind mounts cannot be replaced atomically")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
