package routing

import "testing"

func TestAllowedSubscribe(t *testing.T) {
	table := NewTable([]string{"A", "B"}, nil)
	allow := []string{
		"device/A/report", "device/A/request", "device/A/#",
		"device/+/report", "device/+/request", "device/#",
	}
	deny := []string{
		"device/A/other", "device/C/report", "A/report", "device/A",
		"device/+/+/report", "device/A/report/x", "", "#", "device",
	}
	for _, f := range allow {
		if !table.AllowedSubscribe(f) {
			t.Errorf("AllowedSubscribe(%q) = false, want true", f)
		}
	}
	for _, f := range deny {
		if table.AllowedSubscribe(f) {
			t.Errorf("AllowedSubscribe(%q) = true, want false", f)
		}
	}
}

func TestAllowedSubscribeEmptyTable(t *testing.T) {
	table := NewTable(nil, nil)
	for _, f := range []string{"device/+/report", "device/#", "device/A/report"} {
		if table.AllowedSubscribe(f) {
			t.Errorf("empty table AllowedSubscribe(%q) = true, want false", f)
		}
	}
}

func TestRequestSerial(t *testing.T) {
	table := NewTable([]string{"A"}, nil)
	if serial, ok := table.RequestSerial("device/A/request"); serial != "A" || !ok {
		t.Errorf("RequestSerial(device/A/request) = (%q, %v), want (A, true)", serial, ok)
	}
	deny := []string{
		"device/A/report", "device/C/request", "device/A/request/extra",
		"device/+/request", "device/A", "device/A/report/x",
	}
	for _, f := range deny {
		if serial, ok := table.RequestSerial(f); serial != "" || ok {
			t.Errorf("RequestSerial(%q) = (%q, %v), want (\"\", false)", f, serial, ok)
		}
	}
}

func TestAliasSubscribePublish(t *testing.T) {
	table := NewTable([]string{"REAL1", "PLAIN"}, map[string]string{"REAL1": "ALIAS1"})
	for _, f := range []string{"device/ALIAS1/report", "device/ALIAS1/request", "device/ALIAS1/#"} {
		if !table.AllowedSubscribe(f) {
			t.Errorf("AllowedSubscribe(%q) = false, want true", f)
		}
	}
	for _, f := range []string{"device/REAL1/report", "device/REAL1/request", "device/REAL1/#"} {
		if table.AllowedSubscribe(f) {
			t.Errorf("AllowedSubscribe(%q) = true, want false", f)
		}
	}
	if serial, ok := table.RequestSerial("device/ALIAS1/request"); serial != "REAL1" || !ok {
		t.Errorf("RequestSerial(alias request) = (%q, %v), want (REAL1, true)", serial, ok)
	}
	if serial, ok := table.RequestSerial("device/REAL1/request"); serial != "" || ok {
		t.Errorf("RequestSerial(real serial request) = (%q, %v), want (\"\", false)", serial, ok)
	}
	if serial, ok := table.RequestSerial("device/PLAIN/request"); serial != "PLAIN" || !ok {
		t.Errorf("RequestSerial(plain request) = (%q, %v), want (PLAIN, true)", serial, ok)
	}
}

func TestAliasWildcardExpansion(t *testing.T) {
	table := NewTable([]string{"REAL1", "PLAIN"}, map[string]string{"REAL1": "ALIAS1"})
	for _, f := range []string{"device/+/report", "device/#"} {
		if !table.AllowedSubscribe(f) {
			t.Errorf("AllowedSubscribe(%q) = false, want true", f)
		}
	}
}

func TestDownstreamUpstreamRoundTrip(t *testing.T) {
	table := NewTable([]string{"REAL1", "PLAIN"}, map[string]string{"REAL1": "ALIAS1"})
	if got := table.Downstream("device/REAL1/report"); got != "device/ALIAS1/report" {
		t.Errorf("Downstream = %q, want device/ALIAS1/report", got)
	}
	if got := table.Downstream("device/PLAIN/report"); got != "device/PLAIN/report" {
		t.Errorf("Downstream unaliased = %q, want unchanged", got)
	}
	if got := table.Downstream("other/REAL1/report"); got != "other/REAL1/report" {
		t.Errorf("Downstream off-grammar = %q, want unchanged", got)
	}
	if got := table.Upstream("device/ALIAS1/request"); got != "device/REAL1/request" {
		t.Errorf("Upstream = %q, want device/REAL1/request", got)
	}
	if got := table.Upstream("device/PLAIN/request"); got != "device/PLAIN/request" {
		t.Errorf("Upstream unaliased = %q, want unchanged", got)
	}
	if got := table.Upstream("other/ALIAS1/request"); got != "other/ALIAS1/request" {
		t.Errorf("Upstream off-grammar = %q, want unchanged", got)
	}
	if got := table.Upstream("device/UNKNOWN/request"); got != "device/UNKNOWN/request" {
		t.Errorf("Upstream unknown = %q, want unchanged", got)
	}
	if got := table.Upstream(table.Downstream("device/REAL1/report")); got != "device/REAL1/report" {
		t.Errorf("round-trip = %q, want device/REAL1/report", got)
	}
}

func TestEmptyAliasEntryMeansNoAlias(t *testing.T) {
	table := NewTable([]string{"A"}, map[string]string{"A": ""})
	if !table.AllowedSubscribe("device/A/report") {
		t.Error("empty alias entry rejected the real serial")
	}
	if serial, ok := table.RequestSerial("device/A/request"); !ok || serial != "A" {
		t.Error("empty alias entry rejected real serial publish")
	}
	if got := table.Downstream("device/A/report"); got != "device/A/report" {
		t.Errorf("Downstream = %q, want unchanged", got)
	}
}
