package printerview

import (
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/telemetry"
)

func build(t *testing.T, serial string, reports ...string) View {
	t.Helper()
	c := telemetry.NewCache([]config.Printer{{Serial: serial, Name: "Shop"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, r := range reports {
		c.ObserveReport(serial, 0, 3, []byte(r))
	}
	st, _ := c.State(serial)
	sv, _ := c.Session(serial)
	return Build(st, sv, true, 3, time.Now())
}

func TestBuildPrintErrorAndChamberTemp(t *testing.T) {
	v := build(t, "01S00C351100139", `{"print":{"gcode_state":"RUNNING","print_error":50348044,"chamber_temper":30,"spd_lvl":3,"lights_report":[{"node":"chamber_light","mode":"on"}]}}`)
	if v.PrintError == nil || v.PrintError.ID != "0300_400C" || v.PrintError.URL == "" {
		t.Fatalf("print_error = %+v", v.PrintError)
	}
	if v.ChamberTemp != nil {
		t.Fatalf("P1S chamber_temp = %v, want null", *v.ChamberTemp)
	}
	if v.Model != "P1S" || !v.Printing || *v.SpeedProfile != "sport" || *v.ChamberLight != "on" || !v.Freshness.Fresh {
		t.Fatalf("view = %+v", v)
	}
	if v.ActiveAlerts() != 1 {
		t.Fatalf("alerts = %d", v.ActiveAlerts())
	}
}

func TestBuildEmitsEmptyListsAndNulls(t *testing.T) {
	v := build(t, "S1")
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	for _, want := range []string{`"hms":[]`, `"ams":[]`, `"print_error":null`, `"print_state":null`, `"chamber_light":null`, `"fans":{"part":null`} {
		if !strings.Contains(s, want) {
			t.Fatalf("marshalled view missing %s: %s", want, s)
		}
	}
}
