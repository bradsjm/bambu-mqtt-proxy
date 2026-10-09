package routing

import "testing"

func TestSerialOf(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"device/SN1/report", "SN1"},
		{"device/+/report", "+"},
		{"device/#", "#"},
		{"device/SN1/request/extra", "SN1"},
		{"foo/bar", ""},
		{"device", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := SerialOf(c.in); got != c.want {
			t.Errorf("SerialOf(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestPrintersFor(t *testing.T) {
	table := NewTable([]string{"A", "B"}, nil)
	cases := []struct {
		filter string
		want   []string
	}{
		{"device/A/report", []string{"A"}},
		{"device/A/request", []string{"A"}},
		{"device/+/report", []string{"A", "B"}},
		{"device/#", []string{"A", "B"}},
		{"device/C/report", nil},
		{"zigbee/A", nil},
	}
	for _, c := range cases {
		got := table.PrintersFor(c.filter)
		if len(got) != len(c.want) {
			t.Errorf("PrintersFor(%q) = %v, want %v", c.filter, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("PrintersFor(%q) = %v, want %v", c.filter, got, c.want)
			}
		}
	}
}

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

func TestAllowedPublish(t *testing.T) {
	table := NewTable([]string{"A"}, nil)
	allow := []string{"device/A/request"}
	deny := []string{
		"device/A/report", "device/C/request", "device/A/request/extra",
		"device/+/request", "device/A", "device/A/report/x",
	}
	for _, f := range allow {
		if !table.AllowedPublish(f) {
			t.Errorf("AllowedPublish(%q) = false, want true", f)
		}
	}
	for _, f := range deny {
		if table.AllowedPublish(f) {
			t.Errorf("AllowedPublish(%q) = true, want false", f)
		}
	}
}

func TestAliasPrintersFor(t *testing.T) {
	table := NewTable([]string{"REAL1", "PLAIN"}, map[string]string{"REAL1": "ALIAS1"})
	got := table.PrintersFor("device/ALIAS1/report")
	if len(got) != 1 || got[0] != "REAL1" {
		t.Fatalf("PrintersFor(alias) = %v, want [REAL1]", got)
	}
	got = table.PrintersFor("device/ALIAS1/request")
	if len(got) != 1 || got[0] != "REAL1" {
		t.Fatalf("PrintersFor(alias request) = %v, want [REAL1]", got)
	}
	if got := table.PrintersFor("device/REAL1/report"); got != nil {
		t.Fatalf("PrintersFor(real serial) = %v, want nil", got)
	}
	got = table.PrintersFor("device/PLAIN/report")
	if len(got) != 1 || got[0] != "PLAIN" {
		t.Fatalf("PrintersFor(plain) = %v, want [PLAIN]", got)
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
	if !table.AllowedPublish("device/ALIAS1/request") {
		t.Error("AllowedPublish(alias request) = false, want true")
	}
	if table.AllowedPublish("device/REAL1/request") {
		t.Error("AllowedPublish(real serial request) = true, want false")
	}
	if !table.AllowedPublish("device/PLAIN/request") {
		t.Error("AllowedPublish(plain request) = false, want true")
	}
}

func TestAliasWildcardExpansion(t *testing.T) {
	table := NewTable([]string{"REAL1", "PLAIN"}, map[string]string{"REAL1": "ALIAS1"})
	for _, f := range []string{"device/+/report", "device/#"} {
		got := table.PrintersFor(f)
		if len(got) != 2 || got[0] != "REAL1" || got[1] != "PLAIN" {
			t.Errorf("PrintersFor(%q) = %v, want [REAL1 PLAIN]", f, got)
		}
	}
	if !table.AllowedSubscribe("device/+/report") {
		t.Error("AllowedSubscribe(device/+/report) = false, want true")
	}
	if !table.AllowedSubscribe("device/#") {
		t.Error("AllowedSubscribe(device/#) = false, want true")
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
	if !table.AllowedPublish("device/A/request") {
		t.Error("empty alias entry rejected real serial publish")
	}
	if got := table.Downstream("device/A/report"); got != "device/A/report" {
		t.Errorf("Downstream = %q, want unchanged", got)
	}
}
