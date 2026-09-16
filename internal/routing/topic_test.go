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
	table := NewTable([]string{"A", "B"})
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
	table := NewTable([]string{"A", "B"})
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
	table := NewTable(nil)
	for _, f := range []string{"device/+/report", "device/#", "device/A/report"} {
		if table.AllowedSubscribe(f) {
			t.Errorf("empty table AllowedSubscribe(%q) = true, want false", f)
		}
	}
}

func TestAllowedPublish(t *testing.T) {
	table := NewTable([]string{"A"})
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
