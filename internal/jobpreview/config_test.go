// Section tests for the environment-only job preview switch. TestMain
// registers the section so the package tests exercise the generic config
// machinery exactly as the program registers it.
package jobpreview

import (
	"os"
	"testing"

	"bambu-mqtt-proxy/internal/config"
)

func TestMain(m *testing.M) {
	config.RegisterSection(ConfigSection)
	os.Exit(m.Run())
}

// unsetSwitchForTest clears BMBPX_JOB_PREVIEW: t.Setenv records the
// original value and restores it at cleanup; os.Unsetenv then removes the
// variable so LookupEnv reports absence.
func unsetSwitchForTest(t *testing.T) {
	t.Helper()
	t.Setenv(EnvSwitch, "")
	if err := os.Unsetenv(EnvSwitch); err != nil {
		t.Fatalf("unset %s: %v", EnvSwitch, err)
	}
}

// readSwitch decodes the job preview section from a configuration.
func readSwitch(t *testing.T, c *config.Config) Switch {
	t.Helper()
	v, err := c.Section("job_preview")
	if err != nil {
		t.Fatalf("Section: %v", err)
	}
	s, ok := v.(*Switch)
	if !ok {
		t.Fatalf("section = %T, want *Switch", v)
	}
	return *s
}

func TestApplyEnvJobPreview(t *testing.T) {
	unsetSwitchForTest(t)
	cfg := &config.Config{}
	if !Enabled(cfg) || readSwitch(t, cfg).Enabled != nil {
		t.Fatal("job preview must default to enabled with a nil switch")
	}

	// An empty value means unset.
	t.Setenv(EnvSwitch, "")
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if readSwitch(t, cfg).Enabled != nil || !Enabled(cfg) {
		t.Fatal("an empty variable must keep the default")
	}

	// An explicit false survives as a typed value.
	t.Setenv(EnvSwitch, "false")
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	sw := readSwitch(t, cfg)
	if sw.Enabled == nil || *sw.Enabled || Enabled(cfg) {
		t.Fatal("BMBPX_JOB_PREVIEW=false must disable job preview")
	}

	// Numeric and word spellings parse like the other bool switches.
	t.Setenv(EnvSwitch, "1")
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	sw = readSwitch(t, cfg)
	if sw.Enabled == nil || !*sw.Enabled || !Enabled(cfg) {
		t.Fatal("BMBPX_JOB_PREVIEW=1 must enable job preview")
	}

	// Invalid input is the startup error.
	t.Setenv(EnvSwitch, "bogus")
	_, err := cfg.ApplyEnv()
	if err == nil || err.Error() != `BMBPX_JOB_PREVIEW: invalid bool "bogus"` {
		t.Fatalf("invalid value error = %v, want BMBPX_JOB_PREVIEW: invalid bool \"bogus\"", err)
	}

	// Re-applying after the variable disappears resets the switch, so the
	// post-save re-apply never keeps a stale value.
	unsetSwitchForTest(t)
	disabled := false
	cfg.SetSection("job_preview", Switch{Enabled: &disabled})
	if _, err := cfg.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if readSwitch(t, cfg).Enabled != nil || !Enabled(cfg) {
		t.Fatal("re-apply without the variable must reset the switch")
	}
}

// TestFileJobPreviewIgnored pins the environment-only contract: Parse keeps
// a job_preview section as a stored top-level key, ApplyEnv overwrites it
// from BMBPX_JOB_PREVIEW, and the module therefore never sees a
// file-provided switch value or shows one on the page.
func TestFileJobPreviewIgnored(t *testing.T) {
	unsetSwitchForTest(t)
	parsed, err := config.Parse([]byte("printers: []\njob_preview:\n  enabled: false\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if sw := readSwitch(t, parsed); sw.Enabled == nil || *sw.Enabled {
		t.Fatalf("file switch = %+v, want the parsed file value", readSwitch(t, parsed))
	}
	if _, err := parsed.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if readSwitch(t, parsed).Enabled != nil || !Enabled(parsed) {
		t.Fatal("a file job_preview value must be ignored after ApplyEnv")
	}

	// A bare file scalar fails the section decode; Enabled keeps the
	// enabled default for that malformed case, and ApplyEnv still
	// overwrites it with the environment decision.
	bare, err := config.Parse([]byte("printers: []\njob_preview: false\n"))
	if err != nil {
		t.Fatalf("Parse bare scalar: %v", err)
	}
	if !Enabled(bare) {
		t.Fatal("an undecodable file section must keep the enabled default")
	}
	if _, err := bare.ApplyEnv(); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if !Enabled(bare) || readSwitch(t, bare).Enabled != nil {
		t.Fatal("a bare scalar file value must be ignored after ApplyEnv")
	}
}
