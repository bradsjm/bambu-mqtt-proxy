// Package configui serves the /config page and its JSON API. The page edits
// the YAML config file; saving writes the file and asks the process to
// restart its services with the new settings. Printer access codes are
// write-only: the API reports whether a code is stored but never returns it.
package configui

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"

	"bambu-mqtt-proxy/internal/config"
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

	mu         sync.Mutex
	generation uint64
	applyErr   string
	failure    string
	pending    bool
}

// NewStore returns a store for the YAML file at path.
func NewStore(path string) *Store {
	return &Store{path: path, reloads: make(chan Reload, 1)}
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
	return writeFile(s.path, r.Previous)
}

// Register adds GET /config (page), GET /config/api (settings) and
// PUT /config/api (save and apply). Writes are protected against
// cross-origin browser requests.
func (s *Store) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(pageHTML)
	})
	mux.HandleFunc("GET /config/api", s.handleGet)
	protection := http.NewCrossOriginProtection()
	mux.Handle("PUT /config/api", protection.Handler(http.HandlerFunc(s.handlePut)))
}

// View is the editable configuration exchanged with the page.
type View struct {
	Printers      []PrinterView  `json:"printers"`
	Listen        []ListenerView `json:"listen"`
	AuthMode      string         `json:"auth_mode"`
	HTTPPort      int            `json:"http_port"`
	CameraEnabled bool           `json:"camera_enabled"`
	MCPEnabled    bool           `json:"mcp_enabled"`
	LogLevel      string         `json:"log_level"`
	Behavior      BehaviorView   `json:"behavior"`
}

// PrinterView is one printer. AccessCode is accepted on save and never
// returned; PreviousSerial names the stored printer an edit started from.
type PrinterView struct {
	Serial             string `json:"serial"`
	Name               string `json:"name"`
	Model              string `json:"model"`
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
	QoSMax                int      `json:"qos_max"`
	WarmupCommands        []string `json:"warmup_commands"`
	KeepaliveSeconds      int      `json:"keepalive_seconds"`
	ConnectTimeoutSeconds int      `json:"connect_timeout_seconds"`
	BackoffInitialSeconds int      `json:"backoff_initial_seconds"`
	BackoffMaxSeconds     int      `json:"backoff_max_seconds"`
}

type meta struct {
	Path         string            `json:"path"`
	FileExists   bool              `json:"file_exists"`
	FileError    string            `json:"file_error,omitempty"`
	Generation   uint64            `json:"generation"`
	ApplyError   string            `json:"apply_error,omitempty"`
	EnvOverrides map[string]string `json:"env_overrides"`
}

func (s *Store) handleGet(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	m := meta{Path: s.path, Generation: s.generation, ApplyError: s.applyErr, EnvOverrides: envOverrides()}
	s.mu.Unlock()

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
	var stored []config.Printer
	if existed {
		// An unreadable file has no codes to keep; the save replaces it.
		if cur, err := config.Parse(prev); err == nil {
			stored = cur.Printers
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
	if err := writeFile(s.path, out); err != nil {
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
	return eff, nil
}

func toView(c *config.Config) View {
	v := View{
		AuthMode:      c.Auth.Mode,
		HTTPPort:      c.HTTP.Port,
		CameraEnabled: c.CameraEnabled(),
		MCPEnabled:    c.MCP.Enabled == nil || *c.MCP.Enabled,
		LogLevel:      c.Log.Level,
		Printers:      []PrinterView{},
		Listen:        []ListenerView{},
		Behavior: BehaviorView{
			QoSMax:                int(c.Behavior.QoSMax),
			WarmupCommands:        c.Behavior.WarmupCommands,
			KeepaliveSeconds:      c.Behavior.UpstreamKeepaliveSeconds,
			ConnectTimeoutSeconds: c.Behavior.UpstreamConnectTimeoutSeconds,
			BackoffInitialSeconds: c.Behavior.UpstreamBackoffInitialSeconds,
			BackoffMaxSeconds:     c.Behavior.UpstreamBackoffMaxSeconds,
		},
	}
	for _, p := range c.Printers {
		v.Printers = append(v.Printers, PrinterView{
			Serial:             p.Serial,
			Name:               p.Name,
			Model:              p.Model,
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
// address is unchanged: a stored code is never sent to a new host.
func (v View) toConfig(stored []config.Printer) (*config.Config, error) {
	if v.Behavior.QoSMax < 0 || v.Behavior.QoSMax > 2 {
		return nil, errors.New("Maximum QoS must be 0, 1 or 2.")
	}
	byserial := make(map[string]config.Printer, len(stored))
	for _, p := range stored {
		byserial[p.Serial] = p
	}
	camera, mcp := v.CameraEnabled, v.MCPEnabled
	c := &config.Config{
		Auth:     config.Auth{Mode: v.AuthMode},
		HTTP:     config.HTTP{Port: v.HTTPPort},
		Camera:   config.Camera{Enabled: &camera},
		MCP:      config.MCP{Enabled: &mcp},
		Log:      config.Log{Level: v.LogLevel},
		Printers: []config.Printer{},
		Behavior: config.Behavior{
			QoSMax:                        byte(v.Behavior.QoSMax),
			UpstreamKeepaliveSeconds:      v.Behavior.KeepaliveSeconds,
			UpstreamConnectTimeoutSeconds: v.Behavior.ConnectTimeoutSeconds,
			UpstreamBackoffInitialSeconds: v.Behavior.BackoffInitialSeconds,
			UpstreamBackoffMaxSeconds:     v.Behavior.BackoffMaxSeconds,
		},
	}
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

// writeFile replaces path atomically with owner-only permissions, creating
// the directory when needed. A bind-mounted single file cannot be renamed
// over, so that case falls back to rewriting the file in place.
func writeFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".bambu-mqtt-proxy-*.yaml")
	if err == nil {
		name := tmp.Name()
		_, werr := tmp.Write(data)
		cerr := tmp.Close()
		if werr == nil && cerr == nil {
			if err := os.Rename(name, path); err == nil {
				return nil
			}
		}
		_ = os.Remove(name)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
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
