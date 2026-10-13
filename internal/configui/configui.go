// Package configui serves the /config page and its JSON API. The page edits
// the YAML config file; saving writes the file atomically and signals the
// process to restart itself with the new settings. Printer access codes are
// write-only: the API reports whether a code is stored but never returns it.
package configui

import (
	"bytes"
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
	"bambu-mqtt-proxy/internal/jsonobj"
)

//go:embed config.html
var pageHTML []byte

// fileHeader starts every file written by the page.
const fileHeader = "# bambu-mqtt-proxy configuration, written by the /config page.\n" +
	"# Comments are not preserved when the page saves this file.\n"

// Store owns the config file path and the state the page reports. One Store
// lives for the whole process.
type Store struct {
	path string
	// httpPort is the port this process serves; 0 when unknown.
	httpPort int
	// started identifies this process to the page, which waits for it to
	// change after a save.
	started int64
	// saved delivers one signal after a save is written and answered.
	saved  chan struct{}
	rename func(string, string) error
	// bind checks that the process can listen on a port; replaced by tests.
	bind func(port int) error
	// probePrinter performs the /config printer connection test; replaced
	// by tests.
	probePrinter func(context.Context, config.Printer) error

	mu         sync.Mutex
	startupErr string
}

// NewStore returns a store for the YAML file at path.
func NewStore(path string) *Store {
	return &Store{
		path:         path,
		started:      time.Now().UnixNano(),
		saved:        make(chan struct{}, 1),
		rename:       os.Rename,
		bind:         tryBind,
		probePrinter: probePrinterConn,
	}
}

// Path returns the immutable YAML configuration path.
func (s *Store) Path() string { return s.path }

// SetHTTPPort records the port this process serves. A save that changes it
// test-binds the new port first; with no port recorded the check is skipped.
func (s *Store) SetHTTPPort(port int) { s.httpPort = port }

// Saved delivers one signal per successful save, after the response is
// sent. The caller shuts down and restarts the process.
func (s *Store) Saved() <-chan struct{} { return s.saved }

// SetStartupError records why the proxy could not start; the page shows it.
func (s *Store) SetStartupError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startupErr = err.Error()
}

// tryBind listens on the TCP port once and releases it.
func tryBind(port int) error {
	ln, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return err
	}
	return ln.Close()
}

// Register adds the configuration page, its API, and test actions.
// Writes are protected against cross-origin browser requests.
func (s *Store) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(pageHTML)
	})
	mux.HandleFunc("GET /config/api", s.handleGet)
	protection := http.NewCrossOriginProtection()
	mux.Handle("PUT /config/api", protection.Handler(http.HandlerFunc(s.handlePut)))
	mux.Handle("POST /config/printers/test", protection.Handler(http.HandlerFunc(s.handlePrinterTest)))
	for _, section := range config.Sections() {
		if section.Test != nil {
			mux.Handle("POST /config/"+section.Key+"/test", protection.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s.handleSectionTest(w, r, section)
			})))
		}
	}
}

// View is the editable configuration exchanged with the page.
type View struct {
	Printers        []PrinterView        `json:"printers"`
	PrinterSettings []PrinterSettingView `json:"printer_settings"`
	Listen          []ListenerView       `json:"listen"`
	AuthMode        string               `json:"auth_mode"`
	HTTPPort        int                  `json:"http_port"`
	CameraEnabled   bool                 `json:"camera_enabled"`
	MCPEnabled      bool                 `json:"mcp_enabled"`
	LogLevel        string               `json:"log_level"`
	sections        map[string]any
}

// MarshalJSON adds module-owned page members to the core view.
func (v View) MarshalJSON() ([]byte, error) {
	type core View
	body, err := json.Marshal(core(v))
	if err != nil {
		return nil, err
	}
	return jsonobj.Append(body, v.sections)
}

// PrinterSettingView describes a module's optional printer field on the page.
type PrinterSettingView struct {
	Key         string `json:"key"`
	Label       string `json:"label"`
	Placeholder string `json:"placeholder"`
	Hint        string `json:"hint"`
}

// PrinterView is one printer. AccessCode is accepted on save and never
// returned; PreviousSerial names the stored printer an edit started from.
type PrinterView struct {
	Serial string `json:"serial"`
	// Alias is the optional downstream serial. Apps address the printer
	// by the alias when it is set; a blank alias uses the serial.
	Alias string `json:"alias"`
	Name  string `json:"name"`
	Model string `json:"model"`
	// Settings holds optional module values returned by the API.
	// A blank submitted value clears a setting.
	Settings           map[string]string `json:"settings"`
	Address            string            `json:"address"`
	TLS                bool              `json:"tls"`
	InsecureSkipVerify bool              `json:"insecure_skip_verify"`
	Username           string            `json:"username"`
	HasAccessCode      bool              `json:"has_access_code"`
	AccessCode         string            `json:"access_code,omitempty"`
	PreviousSerial     string            `json:"previous_serial,omitempty"`
}

// ListenerView is one downstream MQTT listener.
type ListenerView struct {
	Port     int    `json:"port"`
	TLS      bool   `json:"tls"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

type meta struct {
	Path       string `json:"path"`
	FileExists bool   `json:"file_exists"`
	FileError  string `json:"file_error,omitempty"`
	// Started identifies the serving process; it changes on every restart.
	Started int64 `json:"started"`
	// StartupError is why the proxy could not start with the saved file.
	StartupError string `json:"startup_error,omitempty"`
	// EnvOverrides lists page fields a surviving environment variable
	// (.env file) overrides. Only http_port and log_level can be overridden.
	EnvOverrides map[string]string `json:"env_overrides"`
}

func (s *Store) handleGet(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	m := meta{Path: s.path, Started: s.started, StartupError: s.startupErr, EnvOverrides: envOverrides()}
	s.mu.Unlock()

	cfg := &config.Config{}
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
	var members map[string]json.RawMessage
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&members); err != nil {
		writeError(w, http.StatusBadRequest, "The settings could not be read: "+err.Error())
		return
	}
	submitted := make(map[string]json.RawMessage)
	for _, section := range config.Sections() {
		submitted[section.Key] = members[section.Key]
		delete(members, section.Key)
	}
	core, err := json.Marshal(members)
	if err != nil {
		writeError(w, http.StatusBadRequest, "The settings could not be read: "+err.Error())
		return
	}
	var in View
	dec = json.NewDecoder(bytes.NewReader(core))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "The settings could not be read: "+err.Error())
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
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
	for _, section := range config.Sections() {
		if section.Save != nil {
			if err := section.Save(submitted[section.Key], stored, cfg); err != nil {
				writeSectionError(w, http.StatusUnprocessableEntity, err)
				return
			}
		}
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
	if port := effective.HTTP.Port; s.httpPort != 0 && port != s.httpPort {
		if err := s.bind(port); err != nil {
			writeError(w, http.StatusUnprocessableEntity, fmt.Sprintf("HTTP port %d cannot be used: %v. Nothing was saved.", port, err))
			return
		}
	}
	if err := s.writeFile(out); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"saved":     true,
		"path":      s.path,
		"http_port": effective.HTTP.Port,
	})
	_ = http.NewResponseController(w).Flush()
	select {
	case s.saved <- struct{}{}:
	default:
	}
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
		writeError(w, http.StatusUnprocessableEntity, "Enter the printer's IP address or host name, for example 192.168.1.42.")
		return
	}
	if strings.Contains(p.Address, "://") {
		writeError(w, http.StatusUnprocessableEntity, "Enter the address without a scheme, for example 192.168.1.42.")
		return
	}
	// URL delimiters never belong in a host:port. Without this check a
	// userinfo or path fragment would reach the dialer or a URL parser and
	// send the test somewhere the user did not type.
	if strings.ContainsAny(p.Address, "/?#@") {
		writeError(w, http.StatusUnprocessableEntity, "Enter the address as a host name or IP address with an optional port, for example 192.168.1.42 or 192.168.1.42:1883.")
		return
	}
	// The page accepts a bare host, so the address is normalized to use the
	// printer's fixed MQTT TLS port before the digit checks below.
	p.Address = config.WithDefaultPrinterPort(p.Address)
	// The port must be explicit and numeric: paho would otherwise dial the
	// plain-MQTT default port 1883, which no Bambu printer serves.
	host, port, err := net.SplitHostPort(p.Address)
	if n, perr := strconv.Atoi(port); err != nil || host == "" || perr != nil || n < 1 || n > 65535 {
		writeError(w, http.StatusUnprocessableEntity, "Enter the address as a host name or IP address with an optional port, for example 192.168.1.42 or 192.168.1.42:1883.")
		return
	}
	if p.Password == "" {
		writeError(w, http.StatusUnprocessableEntity, "Enter the printer's access code.")
		return
	}
	// A simple rejection, no alias resolution: a test connect may never
	// touch a destination a configured printer already owns, so both the
	// serial and the address are refused when they match the effective
	// configuration (the stored file printers, exactly as startup resolves
	// them).
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
		if config.WithDefaultPrinterPort(c.Address) == p.Address {
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

func (s *Store) handleSectionTest(w http.ResponseWriter, r *http.Request, section config.Section) {
	var submitted json.RawMessage
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	if err := dec.Decode(&submitted); err != nil {
		writeError(w, http.StatusBadRequest, "The settings could not be read: "+err.Error())
		return
	}
	stored := func() (*config.Config, error) {
		raw, existed, err := readFile(s.path)
		if err != nil || !existed {
			return nil, err
		}
		// An unreadable configuration has no stored section to fall back on.
		file, _ := config.Parse(raw)
		return file, nil
	}
	if err := section.Test(r.Context(), submitted, stored); err != nil {
		writeSectionError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func writeSectionError(w http.ResponseWriter, status int, err error) {
	var pageErr *config.PageError
	if errors.As(err, &pageErr) {
		status = pageErr.Status
	}
	writeError(w, status, err.Error())
}

// effectivePrinters returns the printers a test connect must stay away
// from: the stored file list when the file parses, exactly as startup
// resolves it. Read and parse failures are errors, so the caller can fail
// closed instead of testing against an empty list.
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
	if aerr := c.ApplyEnv(); aerr != nil {
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
	if err := eff.ApplyEnv(); err != nil {
		return nil, err
	}
	eff.ApplyDefaults()
	if err := eff.Validate(); err != nil {
		return nil, fmt.Errorf("with environment overrides: %w", err)
	}
	// Module requirements can depend on values the overrides replace.
	if err := eff.ValidateEffective(); err != nil {
		return nil, err
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
	}
	v.sections = make(map[string]any)
	for _, section := range config.Sections() {
		if section.View != nil {
			v.sections[section.Key] = section.View(c)
		}
	}
	v.PrinterSettings = []PrinterSettingView{}
	for _, s := range config.PrinterSettings() {
		v.PrinterSettings = append(v.PrinterSettings, PrinterSettingView{
			Key: s.Key, Label: s.Label, Placeholder: s.Placeholder, Hint: s.Hint,
		})
	}
	for _, p := range c.Printers {
		settings := map[string]string{}
		for _, s := range config.PrinterSettings() {
			if value := p.Setting(s.Key); value != "" {
				settings[s.Key] = value
			}
		}
		v.Printers = append(v.Printers, PrinterView{
			Serial:             p.Serial,
			Alias:              p.Alias,
			Name:               p.Name,
			Model:              p.Model,
			Settings:           settings,
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
// A nil stored config (missing or unreadable file) keeps submitted values
// as written.
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
	c := &config.Config{
		Auth:     config.Auth{Mode: v.AuthMode},
		HTTP:     config.HTTP{Port: v.HTTPPort},
		Camera:   config.Camera{Enabled: &camera},
		MCP:      config.MCP{Enabled: &mcp},
		Log:      config.Log{Level: v.LogLevel},
		Printers: []config.Printer{},
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
			Alias:              strings.TrimSpace(p.Alias),
			Name:               strings.TrimSpace(p.Name),
			Model:              strings.TrimSpace(p.Model),
			Address:            strings.TrimSpace(p.Address),
			TLS:                p.TLS,
			InsecureSkipVerify: p.InsecureSkipVerify,
			Username:           strings.TrimSpace(p.Username),
			Password:           p.AccessCode,
		}
		for _, s := range config.PrinterSettings() {
			if value := strings.TrimSpace(p.Settings[s.Key]); value != "" {
				if out.Settings == nil {
					out.Settings = make(map[string]any)
				}
				out.Settings[s.Key] = value
			}
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
			case config.WithDefaultPrinterPort(prev.Address) != config.WithDefaultPrinterPort(out.Address):
				return nil, fmt.Errorf("Enter the access code again for printer %s: its address changed.", out.Serial)
			}
			out.Password = prev.Password
		}
		c.Printers = append(c.Printers, out)
	}
	return c, nil
}

// envOverrides maps page field names to the environment variable that
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
	set("log_level", config.EnvLogLevel)
	set("http_port", config.EnvHTTPPort)
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
