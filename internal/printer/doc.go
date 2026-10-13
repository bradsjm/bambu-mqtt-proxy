package printer

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// Doc is one printer's merged report document; readers must not mutate it.
type Doc map[string]any

// mergeDoc copies maps on changed paths and replaces complete print reports.
func mergeDoc(prev Doc, delta map[string]any, topLevel bool) Doc {
	out := make(Doc, len(prev)+len(delta))
	for k, v := range prev {
		out[k] = v
	}
	for k, v := range delta {
		obj, isObj := v.(map[string]any)
		full := false
		if topLevel && k == "print" && isObj {
			n, ok := obj["msg"].(float64)
			full = ok && n == 0
		}
		if old, ok := prev[k].(map[string]any); isObj && ok && !full {
			out[k] = map[string]any(mergeDoc(Doc(old), obj, false))
		} else {
			out[k] = v
		}
	}
	return out
}

// print reads the current print object.
func (d Doc) print() map[string]any { obj, _ := d["print"].(map[string]any); return obj }

// numberField reads a finite number or numeric string.
func numberField(obj map[string]any, key string) (float64, bool) {
	var n float64
	switch v := obj[key].(type) {
	case float64:
		n = v
	case string:
		var err error
		n, err = strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	return n, !math.IsNaN(n) && !math.IsInf(n, 0)
}

// intField reads a numeric integer projection.
func intField(obj map[string]any, key string) (int, bool) {
	n, ok := numberField(obj, key)
	return int(n), ok
}

// stringField reads a string.
func stringField(obj map[string]any, key string) (string, bool) {
	s, ok := obj[key].(string)
	return s, ok
}

// boolField reads a boolean.
func boolField(obj map[string]any, key string) (bool, bool) { b, ok := obj[key].(bool); return b, ok }

// StartedAt reads the printer's positive Unix start time.
func (d Doc) StartedAt() (time.Time, bool) {
	n, ok := numberField(d.print(), "gcode_start_time")
	if !ok || n <= 0 {
		return time.Time{}, false
	}
	return time.Unix(int64(n), 0).UTC(), true
}

// WifiSignal reads the reported signal in dBm.
func (d Doc) WifiSignal() (int, bool) {
	if s, ok := stringField(d.print(), "wifi_signal"); ok {
		n, err := strconv.Atoi(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), "dBm")))
		return n, err == nil
	}
	return intField(d.print(), "wifi_signal")
}

// ExtSpoolSensor reads the external filament sensor switch.
func (d Doc) ExtSpoolSensor() (bool, bool) {
	n, ok := intField(d.print(), "hw_switch_state")
	return n == 1, ok && (n == 0 || n == 1)
}

// Timelapse reads the camera recording setting.
func (d Doc) Timelapse() (bool, bool) {
	obj, _ := d.print()["ipcam"].(map[string]any)
	s, ok := stringField(obj, "timelapse")
	return s == "enable", ok && (s == "enable" || s == "disable")
}

// FirstLayerInspection reads the printer inspection setting.
func (d Doc) FirstLayerInspection() (bool, bool) {
	obj, _ := d.print()["xcam"].(map[string]any)
	return boolField(obj, "first_layer_inspector")
}

// SpaghettiDetection reads the printer detection setting.
func (d Doc) SpaghettiDetection() (bool, bool) {
	obj, _ := d.print()["xcam"].(map[string]any)
	return boolField(obj, "spaghetti_detector")
}

// ChamberLight reads the chamber light mode from the current lights array.
func (d Doc) ChamberLight() (string, bool) {
	list, ok := d.print()["lights_report"].([]any)
	if !ok {
		return "", false
	}
	for _, item := range list {
		obj, _ := item.(map[string]any)
		if obj["node"] == "chamber_light" {
			s, ok := stringField(obj, "mode")
			return s, ok && (s == "on" || s == "off" || s == "flashing")
		}
	}
	return "", false
}

// Temps holds independently reported temperatures.
type Temps struct {
	// Nozzle is the nozzle reading.
	Nozzle *float64
	// NozzleTarget is the nozzle setpoint.
	NozzleTarget *float64
	// Bed is the bed reading.
	Bed *float64
	// BedTarget is the bed setpoint.
	BedTarget *float64
	// Chamber is the native chamber reading.
	Chamber *float64
}

// valuePtr preserves source presence.
func valuePtr[T any](value T, ok bool) *T {
	if !ok {
		return nil
	}
	return new(value)
}

// Temps reads temperature values without model inference.
func (d Doc) Temps() (Temps, bool) {
	p := d.print()
	t := Temps{valuePtr(numberField(p, "nozzle_temper")), valuePtr(numberField(p, "nozzle_target_temper")), valuePtr(numberField(p, "bed_temper")), valuePtr(numberField(p, "bed_target_temper")), valuePtr(numberField(p, "chamber_temper"))}
	return t, t.Nozzle != nil || t.NozzleTarget != nil || t.Bed != nil || t.BedTarget != nil || t.Chamber != nil
}

// Fans reads fan speeds as percentages.
func (d Doc) Fans() (FansView, bool) {
	var f FansView
	for key, dst := range map[string]**int{"cooling_fan_speed": &f.Part, "big_fan1_speed": &f.Aux, "big_fan2_speed": &f.Chamber, "heatbreak_fan_speed": &f.Heatbreak} {
		n, ok := numberField(d.print(), key)
		if !ok || n < 0 {
			continue
		}
		pct := int(math.Round(n * 100 / 15))
		if n > 15 {
			pct = min(int(math.Round(n)), 100)
		}
		*dst = &pct
	}
	return f, f.Part != nil || f.Aux != nil || f.Chamber != nil || f.Heatbreak != nil
}

// GcodeState reads print.gcode_state.
func (d Doc) GcodeState() (string, bool) { return stringField(d.print(), "gcode_state") }

// JobName reads print.subtask_name.
func (d Doc) JobName() (string, bool) { return stringField(d.print(), "subtask_name") }

// GcodeFile reads print.gcode_file.
func (d Doc) GcodeFile() (string, bool) { return stringField(d.print(), "gcode_file") }

// PlateIndex reads print.plate_idx.
func (d Doc) PlateIndex() (int, bool) { return intField(d.print(), "plate_idx") }

// Progress reads print.mc_percent.
func (d Doc) Progress() (float64, bool) { return numberField(d.print(), "mc_percent") }

// RemainingMin reads print.mc_remaining_time.
func (d Doc) RemainingMin() (float64, bool) { return numberField(d.print(), "mc_remaining_time") }

// Layer reads print.layer_num.
func (d Doc) Layer() (int, bool) { return intField(d.print(), "layer_num") }

// TotalLayers reads print.total_layer_num.
func (d Doc) TotalLayers() (int, bool) { return intField(d.print(), "total_layer_num") }

// Stage reads print.stg_cur.
func (d Doc) Stage() (int, bool) { return intField(d.print(), "stg_cur") }

// SpeedLevel reads print.spd_lvl.
func (d Doc) SpeedLevel() (int, bool) { return intField(d.print(), "spd_lvl") }

// SpeedPercent reads print.spd_mag.
func (d Doc) SpeedPercent() (int, bool) { return intField(d.print(), "spd_mag") }

// PrintError reads print.print_error.
func (d Doc) PrintError() (int, bool) { return intField(d.print(), "print_error") }

// NozzleDiameter reads a positive print.nozzle_diameter.
func (d Doc) NozzleDiameter() (float64, bool) {
	n, ok := numberField(d.print(), "nozzle_diameter")
	return n, ok && n > 0
}

// NozzleType reads print.nozzle_type.
func (d Doc) NozzleType() (string, bool) { return stringField(d.print(), "nozzle_type") }

// SDCard reads print.sdcard.
func (d Doc) SDCard() (bool, bool) { return boolField(d.print(), "sdcard") }

// HMSAlert is one Bambu Health Management System report entry.
type HMSAlert struct {
	// Attr identifies the affected printer subsystem.
	Attr uint32
	// Code carries alert severity and the error code.
	Code uint32
}

// Severity decodes the severity level carried in the high half of Code.
func (a HMSAlert) Severity() string {
	switch a.Code >> 16 {
	case 1:
		return "fatal"
	case 2:
		return "serious"
	case 3:
		return "common"
	case 4:
		return "info"
	default:
		return "unknown"
	}
}
