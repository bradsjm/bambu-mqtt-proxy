// Package config loads and validates the bambu-mqtt-proxy configuration from
// a YAML file, environment variables, or both.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// DefaultWarmupPushall is the warmup command published upstream when the first
// downstream subscriber appears for a printer and after each upstream reconnect.
const DefaultWarmupPushall = `{"pushing":{"sequence_id":"0","command":"pushall"}}`

// Downstream authentication modes.
const (
	// AuthModePrinter requires username bblp and a password matching any
	// configured printer access code.
	AuthModePrinter = "printer"
	// AuthModeAcceptAll allows any CONNECT.
	AuthModeAcceptAll = "accept_all"
)

// Environment variables that configure the proxy without a file. Per-field
// overrides replace the file value; BMBPX_PRINTERS replaces the printers list.
// Setting any listen variable replaces the listener definition with a single
// listener assembled from the listen variables.
const (
	EnvPrinters     = "BMBPX_PRINTERS"
	EnvListenPort   = "BMBPX_LISTEN_PORT"
	EnvListenTLS    = "BMBPX_LISTEN_TLS"
	EnvCertFile     = "BMBPX_CERT_FILE"
	EnvKeyFile      = "BMBPX_KEY_FILE"
	EnvAuthMode     = "BMBPX_AUTH_MODE"
	EnvLogLevel     = "BMBPX_LOG_LEVEL"
	EnvHTTPPort     = "BMBPX_HTTP_PORT"
	EnvCameraEnable = "BMBPX_CAMERA_ENABLED"
	// EnvMCPEnable controls the Model Context Protocol endpoint on the
	// shared HTTP port. MCP is enabled by default; an explicit false
	// disables it.
	EnvMCPEnable = "BMBPX_MCP_ENABLED"
	// EnvJobPreview controls the printer job preview feature: the
	// best-effort retrieval of the current print's sliced 3MF plate image
	// and metadata from the printer. Job preview is enabled by default;
	// an explicit false disables it. The switch is environment-only:
	// there is no YAML field and no configuration-page control, and
	// resolveConfig re-applies the environment after every
	// configuration-page save.
	EnvJobPreview = "BMBPX_JOB_PREVIEW"
	// EnvOctoEverywhereAPIKey overrides the stored Gadget API key for the
	// optional OctoEverywhere Gadget AI print failure detection. Whenever
	// the variable exists it replaces detection.api_key — an empty value
	// clears a stored key — while unset leaves the file value in place.
	EnvOctoEverywhereAPIKey = "BMBPX_OCTOEVERYWHERE_API_KEY"
	defaultFileName         = "bambu-mqtt-proxy.yaml"
)

// defaultListenPort is the design-default downstream MQTT port. ApplyDefaults
// pairs it with TLS and a generated self-signed certificate when neither the
// file nor the environment configures a listener, and ApplyEnv starts its
// replacement listener from the same base.
const defaultListenPort = 8883

// Listener describes one downstream MQTT listener.
type Listener struct {
	Port     int    `yaml:"port"`
	TLS      bool   `yaml:"tls"`
	CertFile string `yaml:"cert_file,omitempty"`
	KeyFile  string `yaml:"key_file,omitempty"`
}

// Auth configures downstream CONNECT authentication.
type Auth struct {
	Mode string `yaml:"mode"`
}

// PrinterSetting describes one optional per-printer string option owned by a module.
type PrinterSetting struct {
	Key, Label, Placeholder, Hint string
	Validate                      func(value string) error
}

// printerSettings holds module settings registered during program initialization.
var printerSettings []PrinterSetting

// RegisterPrinterSetting registers a module's optional printer setting.
// Registration happens only during program initialization, before any config is loaded.
func RegisterPrinterSetting(s PrinterSetting) {
	printerSettings = append(printerSettings, s)
}

// PrinterSettings returns a copy of the registered module printer settings.
func PrinterSettings() []PrinterSetting {
	return append([]PrinterSetting(nil), printerSettings...)
}

// Printer describes one upstream Bambu printer MQTT endpoint.
type Printer struct {
	Serial  string `yaml:"serial"`
	Address string `yaml:"address"`
	// Name is an optional friendly label shown on the camera wall. It never
	// affects routing, which is keyed by Serial.
	Name string `yaml:"name,omitempty"`
	// Model is the optional printer model, free-form (P1S, A1MINI,
	// X1C, ...). Camera capture requires one of P1P, P1S, A1, A1MINI; see
	// CameraSupported. Any other model still proxies MQTT.
	Model              string `yaml:"model,omitempty"`
	TLS                bool   `yaml:"tls"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
	Username           string `yaml:"username"`
	Password           string `yaml:"password"`
	// Settings holds optional per-printer settings owned by modules, inline
	// at the printer level in YAML. Values keep any YAML shape, so unknown
	// printer keys load as they did before modules owned settings; read a
	// module's string value with Setting.
	Settings map[string]any `yaml:",inline"`
}

// Setting returns the trimmed string value of one module setting, or empty
// when the key is unset or its value is not a string.
func (p Printer) Setting(key string) string {
	v, _ := p.Settings[key].(string)
	return strings.TrimSpace(v)
}

// Behavior holds routing and upstream connection tuning knobs.
type Behavior struct {
	WarmupCommands                []string `yaml:"warmup_commands"`
	UpstreamKeepaliveSeconds      int      `yaml:"upstream_keepalive_seconds"`
	UpstreamConnectTimeoutSeconds int      `yaml:"upstream_connect_timeout_seconds"`
	UpstreamBackoffInitialSeconds int      `yaml:"upstream_backoff_initial_seconds"`
	UpstreamBackoffMaxSeconds     int      `yaml:"upstream_backoff_max_seconds"`
}

// Log holds logging configuration.
type Log struct {
	Level string `yaml:"level"`
}

// HTTP holds the shared health and camera HTTP endpoint configuration.
type HTTP struct {
	// Port serves /livez, /readyz, /status, the camera endpoints and the
	// camera wall; 0 disables the endpoint. Unset is represented by the
	// PortUnset sentinel until ApplyDefaults fills in the design default.
	Port int `yaml:"port"`
}

// PortUnset marks an HTTP port that neither the file nor the environment
// configured; ApplyDefaults replaces it with the design default. An
// explicit 0 (disable HTTP) survives defaults untouched.
const PortUnset = -1

// Camera holds the camera and camera wall feature configuration.
type Camera struct {
	// Enabled serves the camera and camera wall routes; false removes the
	// routes, stops camera workers, and skips camera wall MQTT interests.
	Enabled *bool `yaml:"enabled"`
}

// MCP holds the Model Context Protocol endpoint configuration. The endpoint
// serves only read-only tools and resources on the shared HTTP listener; it
// never exposes printer control. Enabled defaults to on: nil means enabled,
// and an explicit false disables the endpoint.
type MCP struct {
	Enabled *bool `yaml:"enabled"`
}

// Notifications configures optional print alerts sent to one recipient.
type Notifications struct {
	Enabled  bool     `yaml:"enabled"`
	Provider string   `yaml:"provider"`
	Pushover Pushover `yaml:"pushover"`
}

// Pushover holds the Pushover application token and user key.
type Pushover struct {
	AppToken string `yaml:"app_token"`
	UserKey  string `yaml:"user_key"`
}

// Detection configures the optional OctoEverywhere Gadget AI print failure
// detection: one global switch plus the stored API key. A nil Enabled keeps
// the historical key-based default — detection runs exactly when a key is
// configured — so existing environment-only deployments are unchanged. An
// explicit false disables the feature even when a key exists; an explicit
// true requires a key after environment overrides (ValidateDetection).
type Detection struct {
	Enabled *bool  `yaml:"enabled"`
	APIKey  string `yaml:"api_key"`
}

// Validate rejects notification settings that cannot run. An empty provider
// means pushover; any other provider is unsupported. Disabled notifications
// may keep blank or stale credentials; enabled notifications require both.
func (n Notifications) Validate() error {
	switch n.Provider {
	case "", "pushover":
	default:
		return fmt.Errorf("notifications: unsupported provider %q", n.Provider)
	}
	if !n.Enabled {
		return nil
	}
	if strings.TrimSpace(n.Pushover.AppToken) == "" {
		return fmt.Errorf("notifications: pushover app token is required")
	}
	if strings.TrimSpace(n.Pushover.UserKey) == "" {
		return fmt.Errorf("notifications: pushover user key is required")
	}
	return nil
}

// Config is the top-level proxy configuration.
type Config struct {
	Listen []Listener `yaml:"listen"`
	Auth   Auth       `yaml:"auth"`
	// Printers is the upstream printer list.
	Printers      []Printer     `yaml:"printers"`
	Behavior      Behavior      `yaml:"behavior"`
	Log           Log           `yaml:"log"`
	HTTP          HTTP          `yaml:"http"`
	Camera        Camera        `yaml:"camera"`
	MCP           MCP           `yaml:"mcp"`
	Notifications Notifications `yaml:"notifications"`
	// Detection is the optional Gadget AI failure detection section. The
	// BMBPX_OCTOEVERYWHERE_API_KEY environment variable overrides the
	// stored key whenever the variable exists.
	Detection Detection `yaml:"detection"`
	// JobPreview is the printer job preview switch applied from
	// BMBPX_JOB_PREVIEW only; yaml:"-" keeps it out of files. Nil means
	// enabled.
	JobPreview *bool `yaml:"-"`
}

// DefaultConfigName is the file probed when no -config flag is given.
func DefaultConfigName() string { return defaultFileName }

// NormalizeModel maps common printer model spellings to a canonical name.
// It returns the uppercased trimmed input for unknown models.
func NormalizeModel(model string) string {
	m := strings.ToUpper(strings.TrimSpace(model))
	return strings.ReplaceAll(m, " ", "")
}

// CameraSupported reports whether the printer model uses the Bambu chamber
// image camera protocol (P1 and A1 series). Unknown or empty models are not
// supported: camera eligibility requires an explicit model.
func CameraSupported(model string) bool {
	switch NormalizeModel(model) {
	case "P1P", "P1S", "A1", "A1MINI":
		return true
	default:
		return false
	}
}

// serialModelPrefixes maps community-observed Bambu serial prefixes to
// printer models. 01P (P1P) and 01S (P1S) were verified against live
// printers; 030 (A1 MINI) and 039 (A1) come from community references.
var serialModelPrefixes = []struct {
	prefix string
	model  string
}{
	{"01P", "P1P"},
	{"01S", "P1S"},
	{"030", "A1MINI"},
	{"039", "A1"},
}

// ModelFromSerial infers the printer model from the serial prefix.
// It returns "" for unknown prefixes rather than guessing.
func ModelFromSerial(serial string) string {
	s := strings.ToUpper(strings.TrimSpace(serial))
	for _, p := range serialModelPrefixes {
		if strings.HasPrefix(s, p.prefix) {
			return p.model
		}
	}
	return ""
}

// CameraEligible decides camera support for one printer: an explicit model
// wins when present; otherwise the model is inferred from the serial
// prefix, so existing configs work without adding model fields.
func CameraEligible(model, serial string) bool {
	if strings.TrimSpace(model) != "" {
		return CameraSupported(model)
	}
	return CameraSupported(ModelFromSerial(serial))
}

// chamberTemperatureModels lists printer models with a physical chamber
// temperature sensor. P1 and A1 series report a meaningless chamber value,
// so they are deliberately absent. This list follows Bambuddy's
// CHAMBER_TEMP_SUPPORTED_MODELS source set.
var chamberTemperatureModels = map[string]struct{}{
	"X1": {}, "X1C": {}, "X1E": {},
	"X2D": {}, "P2S": {},
	"H2C": {}, "H2D": {}, "H2DPRO": {}, "H2S": {},
	"BL-P001": {}, "C13": {}, "N6": {},
	"O1D": {}, "O1C": {}, "O1C2": {}, "O1S": {}, "O1E": {}, "O2D": {}, "N7": {},
}

// ChamberTemperatureSupported reports whether this printer model has a
// physical chamber temperature sensor. Explicit models take precedence;
// without one, only a serial-derived model can establish support.
func ChamberTemperatureSupported(model, serial string) bool {
	if strings.TrimSpace(model) == "" {
		model = ModelFromSerial(serial)
	}
	_, ok := chamberTemperatureModels[NormalizeModel(model)]
	return ok
}

// CameraEnabled reports whether the camera and camera wall routes should be
// served. Cameras are enabled unless explicitly disabled.
func (c *Config) CameraEnabled() bool {
	return c.Camera.Enabled == nil || *c.Camera.Enabled
}

// DetectionEnabled reports whether the OctoEverywhere Gadget detection
// feature should run. An explicit enabled flag wins. Without one, the
// historical key-based default applies: detection runs exactly when an
// API key is configured. The field is a snapshot: callers resolve
// BMBPX_OCTOEVERYWHERE_API_KEY through ApplyEnv first; the /config page
// resolves the environment locally for display only.
func (c *Config) DetectionEnabled() bool {
	if c.Detection.Enabled != nil {
		return *c.Detection.Enabled
	}
	return c.DetectionKey() != ""
}

// DetectionKey returns the configured API key, trimmed. ApplyEnv has
// already resolved the environment when this matters at runtime.
func (c *Config) DetectionKey() string {
	return strings.TrimSpace(c.Detection.APIKey)
}

// ValidateDetection rejects a configuration that enables detection without
// any usable API key. It is an effective-configuration check, applied after
// ApplyEnv: file validation deliberately accepts detection.enabled with a
// blank api_key because the environment can supply the key, so an
// environment-only deployment can enable detection from the /config page
// without persisting the secret. A set-but-empty environment value clears
// the stored key and therefore also rejects enabling.
func (c *Config) ValidateDetection() error {
	if c.DetectionEnabled() && c.DetectionKey() == "" {
		return fmt.Errorf("detection: enabled requires an API key (detection.api_key or %s)", EnvOctoEverywhereAPIKey)
	}
	return nil
}

// MCPEnabled reports whether the read-only MCP endpoint should be served on
// the shared HTTP listener. It defaults to on; an explicit false disables
// it, and it cannot serve without the shared HTTP listener, so http.port 0
// keeps it off regardless.
func (c *Config) MCPEnabled() bool {
	if c.HTTP.Port <= 0 {
		return false
	}
	return c.MCP.Enabled == nil || *c.MCP.Enabled
}

// JobPreviewEnabled reports whether the printer job preview feature should
// run: the scheduler that retrieves the current print's sliced 3MF, the
// cached plate image route, and the MCP preview tool. It defaults to on;
// an explicit false (from BMBPX_JOB_PREVIEW only) disables it.
func (c *Config) JobPreviewEnabled() bool {
	return c.JobPreview == nil || *c.JobPreview
}

// Load reads and parses the YAML configuration at path. Defaults and
// validation are applied by the caller after environment overrides.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw)
}

// Parse decodes YAML configuration bytes. An unset http.port stays
// PortUnset so ApplyDefaults can tell it apart from an explicit 0.
func Parse(raw []byte) (*Config, error) {
	cfg := Config{HTTP: HTTP{Port: PortUnset}}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

// ApplyEnv applies BMBPX_* environment overrides onto the configuration. It
// reports whether the printers list came from the environment.
func (c *Config) ApplyEnv() (bool, error) {
	printersFromEnv := false
	if v := os.Getenv(EnvPrinters); v != "" {
		ps, err := parsePrintersEnv(v)
		if err != nil {
			return false, err
		}
		c.Printers = ps
		printersFromEnv = true
	}

	_, portSet := os.LookupEnv(EnvListenPort)
	_, tlsSet := os.LookupEnv(EnvListenTLS)
	_, certSet := os.LookupEnv(EnvCertFile)
	_, keySet := os.LookupEnv(EnvKeyFile)
	if portSet || tlsSet || certSet || keySet {
		ln := Listener{
			Port:     defaultListenPort,
			TLS:      true,
			CertFile: os.Getenv(EnvCertFile),
			KeyFile:  os.Getenv(EnvKeyFile),
		}
		if v := os.Getenv(EnvListenPort); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > 65535 {
				return false, fmt.Errorf("%s: invalid port %q", EnvListenPort, v)
			}
			ln.Port = n
		}
		if v := os.Getenv(EnvListenTLS); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return false, fmt.Errorf("%s: invalid bool %q", EnvListenTLS, v)
			}
			ln.TLS = b
		}
		c.Listen = []Listener{ln}
	}

	if v, ok := os.LookupEnv(EnvAuthMode); ok && v != "" {
		c.Auth.Mode = v
	}
	if v, ok := os.LookupEnv(EnvLogLevel); ok && v != "" {
		c.Log.Level = v
	}
	if v, ok := os.LookupEnv(EnvHTTPPort); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 65535 {
			return false, fmt.Errorf("%s: invalid port %q", EnvHTTPPort, v)
		}
		c.HTTP.Port = n
	}
	if v, ok := os.LookupEnv(EnvCameraEnable); ok && v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("%s: invalid bool %q", EnvCameraEnable, v)
		}
		c.Camera.Enabled = &b
	}
	if v, ok := os.LookupEnv(EnvMCPEnable); ok && v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("%s: invalid bool %q", EnvMCPEnable, v)
		}
		c.MCP.Enabled = &b
	}
	// The job preview switch is environment-only: clear any previous
	// application first so a re-apply after a configuration-page save
	// follows a removed variable back to the default, then apply a
	// nonempty value. Unset or empty keeps the enabled default.
	c.JobPreview = nil
	if v, ok := os.LookupEnv(EnvJobPreview); ok && v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("%s: invalid bool %q", EnvJobPreview, v)
		}
		c.JobPreview = &b
	}
	if v, ok := os.LookupEnv(EnvOctoEverywhereAPIKey); ok {
		c.Detection.APIKey = strings.TrimSpace(v)
	}
	return printersFromEnv, nil
}

// parsePrintersEnv parses the BMBPX_PRINTERS format: printer entries
// separated by ';', each a comma-separated key=value list with keys serial,
// address, name, model, password, username, tls, insecure_skip_verify,
// and registered module printer setting keys.
func parsePrintersEnv(v string) ([]Printer, error) {
	var out []Printer
	for _, entry := range strings.Split(v, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		p, err := parsePrinterEntry(entry)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", EnvPrinters, err)
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no printer entries found", EnvPrinters)
	}
	return out, nil
}

// parsePrinterEntry parses one comma-separated printer definition.
func parsePrinterEntry(entry string) (Printer, error) {
	p := Printer{Username: "bblp", TLS: true, InsecureSkipVerify: true}
	for _, kv := range strings.Split(entry, ",") {
		k, val, found := strings.Cut(strings.TrimSpace(kv), "=")
		if !found {
			return Printer{}, fmt.Errorf("expected key=value, got %q", kv)
		}
		var err error
		switch strings.ToLower(k) {
		case "serial":
			p.Serial = val
		case "address":
			p.Address = val
		case "name":
			p.Name = strings.TrimSpace(val)
		case "model":
			p.Model = val
		case "password":
			p.Password = val
		case "username":
			p.Username = val
		case "tls":
			p.TLS, err = strconv.ParseBool(val)
		case "insecure_skip_verify":
			p.InsecureSkipVerify, err = strconv.ParseBool(val)
		default:
			registered := false
			for _, s := range printerSettings {
				if s.Key == strings.ToLower(k) {
					if p.Settings == nil {
						p.Settings = make(map[string]any)
					}
					p.Settings[s.Key] = strings.TrimSpace(val)
					registered = true
					break
				}
			}
			if !registered {
				return Printer{}, fmt.Errorf("unknown key %q", k)
			}
		}
		if err != nil {
			return Printer{}, fmt.Errorf("key %q: %w", k, err)
		}
	}
	if p.Serial == "" || p.Address == "" || p.Password == "" {
		return Printer{}, fmt.Errorf("serial, address and password are required")
	}
	return p, nil
}

// ApplyDefaults fills unset values with the design defaults so callers may
// construct partial configs programmatically (tests).
func (c *Config) ApplyDefaults() {
	if c.Auth.Mode == "" {
		c.Auth.Mode = AuthModePrinter
	}
	if len(c.Behavior.WarmupCommands) == 0 {
		c.Behavior.WarmupCommands = []string{DefaultWarmupPushall}
	}
	setIfZero := func(p *int, v int) {
		if *p == 0 {
			*p = v
		}
	}
	setIfZero(&c.Behavior.UpstreamKeepaliveSeconds, 30)
	setIfZero(&c.Behavior.UpstreamConnectTimeoutSeconds, 5)
	setIfZero(&c.Behavior.UpstreamBackoffInitialSeconds, 1)
	setIfZero(&c.Behavior.UpstreamBackoffMaxSeconds, 30)
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	// No listener from the file or the environment: serve the design default
	// endpoint; tlsutil generates a self-signed certificate when cert_file
	// and key_file are empty.
	if len(c.Listen) == 0 {
		c.Listen = []Listener{{Port: defaultListenPort, TLS: true}}
	}
	if c.HTTP.Port == PortUnset {
		c.HTTP.Port = 8080
	}
	for i := range c.Printers {
		if c.Printers[i].Username == "" {
			c.Printers[i].Username = "bblp"
		}
	}
	if c.Notifications.Provider == "" {
		c.Notifications.Provider = "pushover"
	}
}

// Validate rejects configurations that cannot run. An empty printer list is
// accepted; the caller decides how to serve it (setup mode).
func (c *Config) Validate() error {
	if len(c.Listen) == 0 {
		return fmt.Errorf("listen: at least one listener is required")
	}
	for i, l := range c.Listen {
		if l.Port < 1 || l.Port > 65535 {
			return fmt.Errorf("listen[%d]: port %d out of range", i, l.Port)
		}
		if l.TLS && (l.CertFile == "") != (l.KeyFile == "") {
			return fmt.Errorf("listen[%d]: cert_file and key_file must both be set or both empty", i)
		}
	}
	switch c.Auth.Mode {
	case AuthModePrinter, AuthModeAcceptAll:
	default:
		return fmt.Errorf("auth: unknown mode %q", c.Auth.Mode)
	}
	// Zero printers is valid: the proxy then runs in setup mode and serves
	// only the HTTP configuration page.
	seen := make(map[string]bool, len(c.Printers))
	for i, p := range c.Printers {
		if p.Serial == "" {
			return fmt.Errorf("printers[%d]: serial is required", i)
		}
		if seen[p.Serial] {
			return fmt.Errorf("printers[%d]: duplicate serial %q", i, p.Serial)
		}
		seen[p.Serial] = true
		if p.Address == "" {
			return fmt.Errorf("printers[%d] (%s): address is required", i, p.Serial)
		}
		if p.Password == "" {
			return fmt.Errorf("printers[%d] (%s): password (LAN access code) is required", i, p.Serial)
		}
		for _, s := range printerSettings {
			raw, ok := p.Settings[s.Key]
			if !ok || raw == nil {
				continue
			}
			// A registered setting must be a string: a number or list
			// would otherwise read as unset and silently disable it.
			if _, isString := raw.(string); !isString {
				return fmt.Errorf("printers[%d] (%s): %s must be a string", i, p.Serial, s.Key)
			}
			if value := p.Setting(s.Key); value != "" && s.Validate != nil {
				if err := s.Validate(value); err != nil {
					return fmt.Errorf("printers[%d] (%s): %s %q %w", i, p.Serial, s.Key, value, err)
				}
			}
		}
	}
	if c.Behavior.UpstreamKeepaliveSeconds <= 0 ||
		c.Behavior.UpstreamConnectTimeoutSeconds <= 0 ||
		c.Behavior.UpstreamBackoffInitialSeconds <= 0 ||
		c.Behavior.UpstreamBackoffMaxSeconds < c.Behavior.UpstreamBackoffInitialSeconds {
		return fmt.Errorf("behavior: upstream timeouts and backoff must be positive with max >= initial")
	}
	// PortUnset is valid pre-defaults; explicit 0 disables HTTP.
	if (c.HTTP.Port < 0 && c.HTTP.Port != PortUnset) || c.HTTP.Port > 65535 {
		return fmt.Errorf("http: port %d out of range (0 disables)", c.HTTP.Port)
	}
	return c.Notifications.Validate()
}

// DisplayModel names a printer model: explicit model configuration first,
// then known legacy P1/A1 inference and verified RTSPS serial prefixes.
func DisplayModel(model, serial string) string {
	if strings.TrimSpace(model) != "" {
		return NormalizeModel(model)
	}
	if inferred := ModelFromSerial(serial); inferred != "" {
		return inferred
	}
	s := strings.ToUpper(strings.TrimSpace(serial))
	for _, mapping := range rtspSerialPrefixes {
		if strings.HasPrefix(s, mapping.prefix) {
			return mapping.model
		}
	}
	return ""
}

// rtspSerialPrefixes maps verified RTSPS printer serial prefixes to models.
var rtspSerialPrefixes = []struct {
	prefix string
	model  string
}{
	{"00M", "X1C"},
	{"00W", "X1"},
	{"03W", "X1E"},
	{"22E", "P2S"},
	{"093", "H2S"},
	{"094", "H2D"},
}
