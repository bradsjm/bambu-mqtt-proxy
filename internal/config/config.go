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
	EnvHealthPort   = "BMBPX_HEALTH_PORT"
	defaultFileName = "bambu-mqtt-proxy.yaml"
)

// Listener describes one downstream MQTT listener.
type Listener struct {
	Port     int    `yaml:"port"`
	TLS      bool   `yaml:"tls"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// Auth configures downstream CONNECT authentication.
type Auth struct {
	Mode string `yaml:"mode"`
}

// Printer describes one upstream Bambu printer MQTT endpoint.
type Printer struct {
	Serial             string `yaml:"serial"`
	Address            string `yaml:"address"`
	TLS                bool   `yaml:"tls"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
	Username           string `yaml:"username"`
	Password           string `yaml:"password"`
}

// Behavior holds routing and upstream connection tuning knobs.
type Behavior struct {
	QoSMax                        byte     `yaml:"qos_max"`
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

// Health holds health endpoint configuration.
type Health struct {
	// Port serves /livez, /readyz and /status; 0 disables the endpoint.
	Port int `yaml:"port"`
}

// Config is the top-level proxy configuration.
type Config struct {
	Listen   []Listener `yaml:"listen"`
	Auth     Auth       `yaml:"auth"`
	Printers []Printer  `yaml:"printers"`
	Behavior Behavior   `yaml:"behavior"`
	Log      Log        `yaml:"log"`
	Health   Health     `yaml:"health"`
}

// DefaultConfigName is the file probed when no -config flag is given.
func DefaultConfigName() string { return defaultFileName }

// Load reads and parses the YAML configuration at path. Defaults and
// validation are applied by the caller after environment overrides.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
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
			Port:     8883,
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
	if v, ok := os.LookupEnv(EnvHealthPort); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 65535 {
			return false, fmt.Errorf("%s: invalid port %q", EnvHealthPort, v)
		}
		c.Health.Port = n
	}
	return printersFromEnv, nil
}

// parsePrintersEnv parses the BMBPX_PRINTERS format: printer entries
// separated by ';', each a comma-separated key=value list with keys serial,
// address, password, username, tls, insecure_skip_verify.
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
		case "password":
			p.Password = val
		case "username":
			p.Username = val
		case "tls":
			p.TLS, err = strconv.ParseBool(val)
		case "insecure_skip_verify":
			p.InsecureSkipVerify, err = strconv.ParseBool(val)
		default:
			return Printer{}, fmt.Errorf("unknown key %q", k)
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
	if c.Behavior.QoSMax == 0 {
		c.Behavior.QoSMax = 1
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	for i := range c.Printers {
		if c.Printers[i].Username == "" {
			c.Printers[i].Username = "bblp"
		}
	}
}

// Validate rejects configurations that cannot run.
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
	if len(c.Printers) == 0 {
		return fmt.Errorf("printers: at least one printer is required")
	}
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
	}
	if c.Behavior.QoSMax > 2 {
		return fmt.Errorf("behavior: qos_max %d out of range (0-2)", c.Behavior.QoSMax)
	}
	if c.Behavior.UpstreamKeepaliveSeconds <= 0 ||
		c.Behavior.UpstreamConnectTimeoutSeconds <= 0 ||
		c.Behavior.UpstreamBackoffInitialSeconds <= 0 ||
		c.Behavior.UpstreamBackoffMaxSeconds < c.Behavior.UpstreamBackoffInitialSeconds {
		return fmt.Errorf("behavior: upstream timeouts and backoff must be positive with max >= initial")
	}
	if c.Health.Port < 0 || c.Health.Port > 65535 {
		return fmt.Errorf("health: port %d out of range (0 disables)", c.Health.Port)
	}
	return nil
}
