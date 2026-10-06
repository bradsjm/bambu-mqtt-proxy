// Package printerview builds the one printer projection shared by the
// camera wall (/camera/status) and the MCP endpoint, so both surfaces carry
// the same fields under the same names. Every field is emitted; null means
// the printer has not reported it, never a guessed zero.
package printerview

import (
	"fmt"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/hmscodes"
	"bambu-mqtt-proxy/internal/telemetry"
)

// FreshnessWindow bounds how long since the last real report a printer is
// still called fresh. It matches the detection engine's reportFreshMax so
// every consumer shares one staleness semantic.
const FreshnessWindow = 15 * time.Second

// View is the shared printer projection.
type View struct {
	Serial    string `json:"serial"`
	Name      string `json:"name"`
	Model     string `json:"model"`
	Connected bool   `json:"connected"`
	// PrintState is the merged gcode_state; null when never reported.
	PrintState *string `json:"print_state"`
	Printing   bool    `json:"printing"`
	// Stage is the printer's current action mapped from stg_cur; null for
	// idle sentinels and unknown ids, so print_state stays the headline.
	Stage   *string `json:"stage"`
	JobName *string `json:"job_name"`
	// StartedAt is gcode_start_time in RFC3339 UTC.
	StartedAt *string `json:"started_at"`
	// Progress is mc_percent with source presence: null means never
	// reported.
	Progress         *float64 `json:"progress"`
	RemainingMinutes *float64 `json:"remaining_minutes"`
	LayerNum         *int     `json:"layer_num"`
	TotalLayers      *int     `json:"total_layers"`
	// SpeedProfile is silent, standard, sport, or ludicrous.
	SpeedProfile *string `json:"speed_profile"`
	SpeedPercent *int    `json:"speed_percent"`
	// ChamberLight is on, off, or flashing.
	ChamberLight   *string  `json:"chamber_light"`
	NozzleTemp     *float64 `json:"nozzle_temp"`
	NozzleTarget   *float64 `json:"nozzle_target"`
	BedTemp        *float64 `json:"bed_temp"`
	BedTarget      *float64 `json:"bed_target"`
	ChamberTemp    *float64 `json:"chamber_temp"`
	Fans           FansView `json:"fans"`
	NozzleDiameter *float64 `json:"nozzle_diameter"`
	NozzleType     *string  `json:"nozzle_type"`
	WifiSignalDBm  *int     `json:"wifi_signal_dbm"`
	SDCard         *bool    `json:"sd_card"`
	// ExtSpoolSensor reports filament at the external spool sensor.
	ExtSpoolSensor       *bool `json:"ext_spool_filament_detected"`
	Timelapse            *bool `json:"timelapse"`
	FirstLayerInspection *bool `json:"first_layer_inspection"`
	SpaghettiDetection   *bool `json:"spaghetti_detection"`
	// PrintError is null when print_error is 0 or was never reported.
	PrintError *Alert  `json:"print_error"`
	HMS        []Alert `json:"hms"`
	// AMS lists conventional four-slot AMS units sorted by unit id, and
	// ExtSpool the single external spool.
	AMS       []telemetry.AMSUnit `json:"ams"`
	ExtSpool  *telemetry.AMSSlot  `json:"ext_spool"`
	Freshness Freshness           `json:"freshness"`
}

// FansView holds fan speeds in percent.
type FansView struct {
	Part      *int `json:"part"`
	Aux       *int `json:"aux"`
	Chamber   *int `json:"chamber"`
	Heatbreak *int `json:"heatbreak"`
}

// Alert is one printer error or HMS alert. Text and Fix come from the
// error-code dataset and stay empty for codes it does not cover; URL links
// the code to its lookup page.
type Alert struct {
	ID       string `json:"id"`
	Severity string `json:"severity"`
	Text     string `json:"text,omitempty"`
	Fix      string `json:"fix,omitempty"`
	URL      string `json:"url,omitempty"`
}

// Freshness describes real-report evidence for one printer. A report only
// counts when it carried print state; command ACKs never refresh it.
type Freshness struct {
	// Fresh is true when a real report arrived within the freshness window
	// on the upstream connection that is still current.
	Fresh bool `json:"fresh"`
	// LastReportAgeSeconds is the age of the last real report; null when no
	// real report was ever observed.
	LastReportAgeSeconds *float64 `json:"last_report_age_seconds"`
	// UpstreamGeneration identifies the connection the last real report
	// arrived on; null when none was ever observed. It changes on every
	// reconnect, so a pre-reconnect observation never looks current.
	UpstreamGeneration *uint64 `json:"upstream_generation"`
}

// speedProfiles names Bambu spd_lvl values 1..4.
var speedProfiles = [...]string{"silent", "standard", "sport", "ludicrous"}

// Build projects one printer. currentGen is the live upstream connection
// generation; now is injected so tests can freeze time.
func Build(st telemetry.State, sv telemetry.SessionView, connected bool, currentGen uint64, now time.Time) View {
	v := View{
		Serial:               st.Serial,
		Name:                 st.Name,
		Model:                config.DisplayModel(st.Model, st.Serial),
		Connected:            connected,
		PrintState:           nullable(st.PrintingState),
		Printing:             sv.Active,
		Stage:                nullable(StageName(st.Stage)),
		JobName:              nullable(st.Filename),
		Progress:             sv.Progress,
		RemainingMinutes:     sv.RemainingMin,
		LayerNum:             st.LayerNum,
		TotalLayers:          st.TotalLayers,
		SpeedPercent:         st.SpeedPercent,
		ChamberLight:         nullable(st.ChamberLight),
		NozzleTemp:           st.NozzleTemp,
		NozzleTarget:         st.NozzleTarget,
		BedTemp:              st.BedTemp,
		BedTarget:            st.BedTarget,
		Fans:                 FansView{Part: st.Fans.Part, Aux: st.Fans.Aux, Chamber: st.Fans.Chamber, Heatbreak: st.Fans.Heatbreak},
		NozzleDiameter:       st.NozzleDiameter,
		NozzleType:           nullable(st.NozzleType),
		WifiSignalDBm:        st.WifiDBm,
		SDCard:               st.SDCard,
		ExtSpoolSensor:       st.ExtSpoolSensor,
		Timelapse:            st.Timelapse,
		FirstLayerInspection: st.FirstLayerInspection,
		SpaghettiDetection:   st.SpaghettiDetection,
		HMS:                  []Alert{},
		AMS:                  st.AMS,
		ExtSpool:             st.ExtSpool,
	}
	if v.AMS == nil {
		v.AMS = []telemetry.AMSUnit{}
	}
	if !st.StartedAt.IsZero() {
		s := st.StartedAt.UTC().Format(time.RFC3339)
		v.StartedAt = &s
	}
	if p := sv.SpeedProfile; p != nil && *p >= 1 && *p <= len(speedProfiles) {
		name := speedProfiles[*p-1]
		v.SpeedProfile = &name
	}
	// Native chamber temperature: only models with a physical sensor
	// project the report value, resolved through DisplayModel so the same
	// model inference that names the printer also decides the sensor (a
	// model-less RTSPS-prefix serial is an X1-class printer with a real
	// sensor). Without a native sensor, a fresh accessory chamber reading
	// from a module fills the same field; the native sensor always wins.
	if config.ChamberTemperatureSupported(config.DisplayModel(st.Model, st.Serial), st.Serial) {
		v.ChamberTemp = st.ChamberTemp
	} else if st.AccessoryChamberTemp != nil && now.Sub(st.AccessoryChamberAt) <= FreshnessWindow {
		v.ChamberTemp = st.AccessoryChamberTemp
	}
	if st.PrintError != 0 {
		code := uint32(st.PrintError)
		// The link is set even when the dataset does not describe the
		// code, so every alert can reach the lookup tool.
		info := hmscodes.PrintError(code)
		a := Alert{ID: fmt.Sprintf("%04X_%04X", code>>16, code&0xFFFF), Severity: "unknown", URL: info.URL}
		if info.Title != "" {
			a.Text, a.Fix = info.Title, info.Fix
		}
		if info.Severity != "" {
			a.Severity = info.Severity
		}
		v.PrintError = &a
	}
	for _, h := range st.HMS {
		info := hmscodes.HMS(h.Attr)
		a := Alert{ID: h.ID(), Severity: h.Severity(), URL: info.URL}
		if info.Title != "" {
			a.Text, a.Fix = info.Title, info.Fix
		}
		v.HMS = append(v.HMS, a)
	}
	if !sv.ObsAt.IsZero() {
		age := now.Sub(sv.ObsAt).Seconds()
		gen := sv.ObsGen
		v.Freshness = Freshness{
			Fresh:                age <= FreshnessWindow.Seconds() && currentGen == sv.ObsGen,
			LastReportAgeSeconds: &age,
			UpstreamGeneration:   &gen,
		}
	}
	return v
}

// ActiveAlerts counts the printer error plus HMS alerts.
func (v View) ActiveAlerts() int {
	n := len(v.HMS)
	if v.PrintError != nil {
		n++
	}
	return n
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
