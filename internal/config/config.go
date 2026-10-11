// Package config loads and validates the bambu-mqtt-proxy configuration
// from a YAML file. BMBPX_LOG_LEVEL and BMBPX_HTTP_PORT override two file
// values; every other setting is YAML only.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Downstream authentication modes.
const (
	// AuthModePrinter requires username bblp and a password matching any
	// configured printer access code.
	AuthModePrinter = "printer"
	// AuthModeAcceptAll allows any CONNECT.
	AuthModeAcceptAll = "accept_all"
)

// Environment variables that override the YAML file. A nonempty value
// replaces the file value; every other setting is YAML only.
const (
	// EnvLogLevel overrides log.level: a nonempty value replaces the
	// stored file value.
	EnvLogLevel = "BMBPX_LOG_LEVEL"
	// EnvHTTPPort overrides http.port: a valid value 0-65535 replaces the
	// stored file value, and 0 is treated as unset.
	EnvHTTPPort = "BMBPX_HTTP_PORT"

	defaultFileName = "bambu-mqtt-proxy.yaml"
)

// defaultListenPort is the design-default downstream MQTT port. ApplyDefaults
// pairs it with TLS and a generated self-signed certificate when the file
// does not configure a listener.
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
	// Alias is an optional downstream serial. When set, downstream MQTT
	// clients must address this printer as device/{Alias}/... and the real
	// Serial is rejected downstream; upstream traffic always uses Serial.
	Alias string `yaml:"alias,omitempty"`
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

// Log holds logging configuration.
type Log struct {
	Level string `yaml:"level"`
}

// HTTP holds the shared health, camera, and configuration HTTP endpoint
// configuration.
type HTTP struct {
	// Port serves /livez, /readyz, /status, /config, the camera endpoints and
	// the camera wall. 0 means unset and ApplyDefaults replaces it with 8080.
	Port int `yaml:"port"`
	// zero records that the file or environment set the port to 0 explicitly.
	zero bool
}

// UnmarshalYAML decodes the port and remembers an explicit 0 for ExplicitZero.
func (h *HTTP) UnmarshalYAML(n *yaml.Node) error {
	var raw struct {
		Port *int `yaml:"port"`
	}
	if err := n.Decode(&raw); err != nil {
		return err
	}
	if raw.Port != nil {
		h.Port = *raw.Port
		h.zero = *raw.Port == 0
	}
	return nil
}

// ExplicitZero reports whether the configuration set http.port to 0, which
// ApplyDefaults treats as unset; the caller warns about it.
func (h HTTP) ExplicitZero() bool { return h.zero }

// Camera holds the camera and camera wall feature configuration.
type Camera struct {
	// Enabled serves the camera and camera wall routes; false removes the
	// routes, stops camera workers, and skips camera wall MQTT interests.
	Enabled *bool `yaml:"enabled"`
}

// MCP holds the Model Context Protocol endpoint configuration on the shared
// HTTP listener. Enabled defaults to on: nil means enabled, and an explicit
// false disables the endpoint.
type MCP struct {
	Enabled *bool `yaml:"enabled"`
}

// Config is the top-level proxy configuration.
type Config struct {
	Listen []Listener `yaml:"listen"`
	Auth   Auth       `yaml:"auth"`
	// Printers is the upstream printer list.
	Printers []Printer `yaml:"printers"`
	Log      Log       `yaml:"log"`
	HTTP     HTTP      `yaml:"http"`
	Camera   Camera    `yaml:"camera"`
	MCP      MCP       `yaml:"mcp"`
	// Sections holds module-owned top-level YAML sections and unknown keys.
	Sections map[string]yaml.Node `yaml:",inline"`
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

// MCPEnabled reports whether the read-only MCP endpoint should be served on
// the shared HTTP listener. It defaults to on; an explicit false disables it.
func (c *Config) MCPEnabled() bool {
	return c.MCP.Enabled == nil || *c.MCP.Enabled
}

// Load reads and parses the YAML configuration at path. Defaults,
// validation, and the two environment overrides are applied by the caller.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw)
}

// Parse decodes YAML configuration bytes. Unknown keys, including the
// removed behavior section, are ignored.
func Parse(raw []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

// ApplyEnv applies the two surviving environment variables onto the
// configuration: BMBPX_LOG_LEVEL overrides log.level and BMBPX_HTTP_PORT
// overrides http.port. A nonempty value replaces the file value, and an
// invalid BMBPX_HTTP_PORT value is an error.
func (c *Config) ApplyEnv() error {
	if v, ok := os.LookupEnv(EnvLogLevel); ok && v != "" {
		c.Log.Level = v
	}
	if v, ok := os.LookupEnv(EnvHTTPPort); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 65535 {
			return fmt.Errorf("%s: invalid port %q", EnvHTTPPort, v)
		}
		c.HTTP.Port = n
		c.HTTP.zero = n == 0
	}
	return nil
}

// ApplyDefaults fills unset values with the design defaults so callers may
// construct partial configs programmatically (tests).
func (c *Config) ApplyDefaults() {
	if c.Auth.Mode == "" {
		c.Auth.Mode = AuthModePrinter
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	// No listener from the file or the environment: serve the design default
	// endpoint; tlsutil generates a self-signed certificate when cert_file
	// and key_file are empty.
	if len(c.Listen) == 0 {
		c.Listen = []Listener{{Port: defaultListenPort, TLS: true}}
	}
	if c.HTTP.Port == 0 {
		c.HTTP.Port = 8080
	}
	for i := range c.Printers {
		if c.Printers[i].Username == "" {
			c.Printers[i].Username = "bblp"
		}
		c.Printers[i].Address = WithDefaultPrinterPort(c.Printers[i].Address)
	}
}

// Validate rejects configurations that cannot run. An empty printer list is
// accepted; the caller decides how to serve it (setup mode).
func (c *Config) Validate() error {
	for _, s := range sections {
		if _, present := c.Sections[s.Key]; present {
			if _, err := c.Section(s.Key); err != nil {
				return err
			}
		}
	}
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
	used := make(map[string]bool, 2*len(c.Printers))
	for serial := range seen {
		used[serial] = true
	}
	for i, p := range c.Printers {
		if p.Alias == "" {
			continue
		}
		if !validAlias(p.Alias) {
			return fmt.Errorf("printers[%d] (%s): alias %q must contain only uppercase letters and digits", i, p.Serial, p.Alias)
		}
		if used[p.Alias] {
			return fmt.Errorf("printers[%d] (%s): alias %q duplicates a configured serial or alias", i, p.Serial, p.Alias)
		}
		used[p.Alias] = true
	}
	if c.HTTP.Port < 1 || c.HTTP.Port > 65535 {
		return fmt.Errorf("http: port %d out of range", c.HTTP.Port)
	}
	return nil
}

// validAlias reports whether an alias matches the downstream ID grammar:
// one or more uppercase letters and digits.
func validAlias(alias string) bool {
	for _, c := range alias {
		if (c < '0' || c > '9') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return alias != ""
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
