// Environment-only job preview switch. The section value keeps a YAML
// shape for the generic machinery, but ApplyEnv always overwrites it from
// the environment, so a job_preview value in a configuration file is
// ignored and the switch never reaches the configuration page: the
// section has no View, Save, Test, or EnvOverrides hook.
package jobpreview

import (
	"fmt"
	"os"
	"strconv"

	"bambu-mqtt-proxy/internal/config"
)

// EnvSwitch controls the printer job preview feature: the scheduler that
// retrieves the current print's sliced 3MF, the cached plate image route,
// and the MCP preview tool. Job preview is enabled by default; an explicit
// false disables it. The switch is environment-only: there is no YAML
// field and no configuration-page control, and the main resolver re-applies
// the environment after every configuration-page save.
const EnvSwitch = "BMBPX_JOB_PREVIEW"

// Switch is the job preview section value. Enabled is nil when the
// environment did not set the switch; nil means enabled. ApplyEnv refreshes
// the section from the environment on every call, so a job_preview value in
// a configuration file never survives into the effective configuration.
type Switch struct {
	Enabled *bool `yaml:"enabled"`
}

// ConfigSection owns the environment-only job preview switch. Register it
// during program initialization alongside the other module sections.
var ConfigSection = config.Section{
	Key: "job_preview",
	New: func() any { return &Switch{} },
	// ApplyEnv always replaces the section from the environment: unset or
	// empty keeps the enabled default, a parseable value wins, and an
	// invalid value is the startup error. Re-applying after the variable
	// disappears resets the switch, so a removed override cannot linger.
	ApplyEnv: func(c *config.Config) error {
		c.SetSection("job_preview", Switch{})
		if v, ok := os.LookupEnv(EnvSwitch); ok && v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("%s: invalid bool %q", EnvSwitch, v)
			}
			c.SetSection("job_preview", Switch{Enabled: &b})
		}
		return nil
	},
}

// Enabled reports whether the printer job preview feature should run. It
// reads the effective configuration, whose section ApplyEnv already set and
// whose registered sections the startup validation already decoded, so the
// decision is environment-only. A nil Enabled or a decode error keeps the
// enabled default.
func Enabled(c *config.Config) bool {
	v, err := c.Section("job_preview")
	if err != nil {
		return true
	}
	s, ok := v.(*Switch)
	if !ok || s.Enabled == nil {
		return true
	}
	return *s.Enabled
}
