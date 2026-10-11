// View fields share the camera wall and MCP JSON contract.
// Null values mean that the printer has not reported a usable value.
package printer

import (
	"fmt"
	"time"

	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/hmscodes"
)

// FreshnessWindow bounds how long an accessory chamber reading stays valid.
// It no longer gates printer liveness, which is the printer link state; it
// only bounds the accessory fallback in Build and the pandabreath link state.
const FreshnessWindow = 15 * time.Second

// View is the shared printer projection.
type View struct {
	// Serial is the configured routing identity.
	Serial string `json:"serial"`
	// Name is the configured display name.
	Name string `json:"name"`
	// Model is the configured or serial-derived model.
	Model string `json:"model"`
	// Connected is the current printer link state.
	Connected bool `json:"connected"`
	// PrintState is the merged gcode_state; null when never reported.
	PrintState *string `json:"print_state"`
	// Printing reports RUNNING, PAUSE, or PAUSED from gcode_state.
	Printing bool `json:"printing"`
	// Stage names the reported stg_cur action when the id is known.
	// Idle sentinels and unknown ids remain null.
	Stage *string `json:"stage"`
	// JobName is the reported subtask_name.
	JobName *string `json:"job_name"`
	// StartedAt is gcode_start_time in RFC3339 UTC.
	StartedAt *string `json:"started_at"`
	// Progress is the reported mc_percent, or null when unknown.
	Progress *float64 `json:"progress"`
	// RemainingMinutes is the reported mc_remaining_time.
	RemainingMinutes *float64 `json:"remaining_minutes"`
	// LayerNum is the reported layer_num.
	LayerNum *int `json:"layer_num"`
	// TotalLayers is the reported total_layer_num.
	TotalLayers *int `json:"total_layers"`
	// SpeedProfile is silent, standard, sport, or ludicrous.
	SpeedProfile *string `json:"speed_profile"`
	// SpeedPercent is the reported spd_mag.
	SpeedPercent *int `json:"speed_percent"`
	// ChamberLight is on, off, or flashing.
	ChamberLight *string `json:"chamber_light"`
	// NozzleTemp is the reported nozzle temperature in Celsius.
	NozzleTemp *float64 `json:"nozzle_temp"`
	// NozzleTarget is the reported nozzle setpoint in Celsius.
	NozzleTarget *float64 `json:"nozzle_target"`
	// BedTemp is the reported bed temperature in Celsius.
	BedTemp *float64 `json:"bed_temp"`
	// BedTarget is the reported bed setpoint in Celsius.
	BedTarget *float64 `json:"bed_target"`
	// ChamberTemp is the native or fresh accessory chamber reading.
	ChamberTemp *float64 `json:"chamber_temp"`
	// Fans holds the reported fan percentages.
	Fans FansView `json:"fans"`
	// NozzleDiameter is the reported nozzle diameter in millimeters.
	NozzleDiameter *float64 `json:"nozzle_diameter"`
	// NozzleType is the reported nozzle type.
	NozzleType *string `json:"nozzle_type"`
	// WifiSignalDBm is the reported wireless signal strength.
	WifiSignalDBm *int `json:"wifi_signal_dbm"`
	// SDCard reports whether the printer sees an SD card.
	SDCard *bool `json:"sd_card"`
	// ExtSpoolSensor reports filament at the external spool sensor.
	ExtSpoolSensor *bool `json:"ext_spool_filament_detected"`
	// Timelapse is the reported camera recording setting.
	Timelapse *bool `json:"timelapse"`
	// FirstLayerInspection is the reported printer inspection setting.
	FirstLayerInspection *bool `json:"first_layer_inspection"`
	// SpaghettiDetection is the reported printer detection setting.
	SpaghettiDetection *bool `json:"spaghetti_detection"`
	// PrintError is null when print_error is 0 or was never reported.
	PrintError *Alert `json:"print_error"`
	// HMS lists the current health alerts.
	HMS []Alert `json:"hms"`
	// AMS lists conventional four-slot units sorted by unit id.
	AMS []AMSUnit `json:"ams"`
	// ExtSpool is the reported external filament spool.
	ExtSpool *AMSSlot `json:"ext_spool"`
	// Freshness holds link state and the display-only report age.
	Freshness Freshness `json:"freshness"`
}

// FansView holds fan speeds in percent.
type FansView struct {
	// Part is the part-cooling fan percentage.
	Part *int `json:"part"`
	// Aux is the auxiliary fan percentage.
	Aux *int `json:"aux"`
	// Chamber is the chamber fan percentage.
	Chamber *int `json:"chamber"`
	// Heatbreak is the heatbreak fan percentage.
	Heatbreak *int `json:"heatbreak"`
}

// Alert is one printer error or HMS alert. Text and Fix come from the
// error-code dataset and stay empty for codes it does not cover; URL links
// the code to its lookup page.
type Alert struct {
	// ID is the formatted printer error or HMS identifier.
	ID string `json:"id"`
	// Severity is the alert severity.
	Severity string `json:"severity"`
	// Text is the dataset description when known.
	Text string `json:"text,omitempty"`
	// Fix is the dataset suggested remedy when known.
	Fix string `json:"fix,omitempty"`
	// URL links to the alert lookup page.
	URL string `json:"url,omitempty"`
}

// Freshness describes liveness evidence for one printer. Fresh is the printer
// link state; the age is a diagnostic that never gates liveness.
type Freshness struct {
	// Fresh is true when the printer link is live.
	Fresh bool `json:"fresh"`
	// LastReportAgeSeconds is the display-only age of the last object report.
	// It is null until the first object report arrives.
	LastReportAgeSeconds *float64 `json:"last_report_age_seconds"`
}

// speedProfiles names Bambu spd_lvl values 1..4.
var speedProfiles = [...]string{"silent", "standard", "sport", "ludicrous"}

// Build projects a printer document at the supplied display time.
func Build(d Doc, identity config.Printer, online bool, lastReportAt time.Time, now time.Time) View {
	state, _ := d.GcodeState()
	job, _ := d.JobName()
	stage := valuePtr(d.Stage())
	nozzleType, _ := d.NozzleType()
	light, _ := d.ChamberLight()
	temps, _ := d.Temps()
	fans, _ := d.Fans()
	ams, _ := d.AMS()
	ext, _ := d.ExtSpool()
	v := View{
		Serial:               identity.Serial,
		Name:                 identity.Name,
		Model:                config.DisplayModel(identity.Model, identity.Serial),
		Connected:            online,
		PrintState:           nullable(state),
		Printing:             state == "RUNNING" || state == "PAUSE" || state == "PAUSED",
		Stage:                nullable(StageName(stage)),
		JobName:              nullable(job),
		Progress:             valuePtr(d.Progress()),
		RemainingMinutes:     valuePtr(d.RemainingMin()),
		LayerNum:             valuePtr(d.Layer()),
		TotalLayers:          valuePtr(d.TotalLayers()),
		SpeedPercent:         valuePtr(d.SpeedPercent()),
		ChamberLight:         nullable(light),
		NozzleTemp:           temps.Nozzle,
		NozzleTarget:         temps.NozzleTarget,
		BedTemp:              temps.Bed,
		BedTarget:            temps.BedTarget,
		Fans:                 fans,
		NozzleDiameter:       valuePtr(d.NozzleDiameter()),
		NozzleType:           nullable(nozzleType),
		WifiSignalDBm:        valuePtr(d.WifiSignal()),
		SDCard:               valuePtr(d.SDCard()),
		ExtSpoolSensor:       valuePtr(d.ExtSpoolSensor()),
		Timelapse:            valuePtr(d.Timelapse()),
		FirstLayerInspection: valuePtr(d.FirstLayerInspection()),
		SpaghettiDetection:   valuePtr(d.SpaghettiDetection()),
		HMS:                  []Alert{},
		AMS:                  ams,
		ExtSpool:             ext,
	}
	if t, ok := d.StartedAt(); ok {
		s := t.Format(time.RFC3339)
		v.StartedAt = &s
	}
	if n, ok := d.SpeedLevel(); ok && n >= 1 && n <= len(speedProfiles) {
		name := speedProfiles[n-1]
		v.SpeedProfile = &name
	}
	if config.ChamberTemperatureSupported(v.Model, v.Serial) {
		v.ChamberTemp = temps.Chamber
	}
	if n, ok := d.PrintError(); ok && n != 0 {
		code := uint32(n)
		info := hmscodes.PrintError(code)
		a := Alert{ID: fmt.Sprintf("%04X_%04X", code>>16, code&0xFFFF), Severity: "unknown", URL: info.URL, Text: info.Title, Fix: info.Fix}
		if info.Severity != "" {
			a.Severity = info.Severity
		}
		v.PrintError = &a
	}
	alerts, _ := d.HMS()
	for _, h := range alerts {
		info := hmscodes.HMS(h.Attr)
		v.HMS = append(v.HMS, Alert{ID: h.ID(), Severity: h.Severity(), URL: info.URL, Text: info.Title, Fix: info.Fix})
	}
	v.Freshness.Fresh = online
	if !lastReportAt.IsZero() {
		age := now.Sub(lastReportAt).Seconds()
		v.Freshness.LastReportAgeSeconds = &age
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

// nullable omits empty strings.
func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
