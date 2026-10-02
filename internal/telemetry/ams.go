package telemetry

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This file holds the display-only projection of the Bambu AMS report
// objects: conventional four-slot units, the single external spool, the
// active filament selection, and the raw stage id. The JSON tags double as
// the /camera/status wire contract served by the camera package.

// AMSUnit is one conventional four-slot AMS unit in merged display form.
type AMSUnit struct {
	ID int `json:"id"`
	// Humidity is the raw humidity sensor percentage (1..100); power-on
	// placeholder values keep the field omitted instead of showing a
	// meaningless 0.
	Humidity *int `json:"humidity,omitempty"`
	// Temp is the AMS ambient temperature in degrees Celsius. Only finite
	// plausible readings (0..100 Celsius) merge: the firmware's
	// out-of-range sentinels (an observed 6503.6) are bogus startup
	// readings, so a rejected value retains the prior valid one and a unit
	// that never reported a plausible value stays unknown.
	Temp  *float64  `json:"temp,omitempty"`
	Slots []AMSSlot `json:"slots"`
}

// AMSSlot is one filament slot: a member slot of an AMS unit (ids 0-3) or,
// with the legacy id 254, the single external spool. Loaded distinguishes a
// never-reported slot (nil) from a resolved-empty one (false). Remain is
// the remaining-filament estimate in percent; nil means unavailable, which
// the hardware reports as -1.
type AMSSlot struct {
	ID       int    `json:"id"`
	Loaded   *bool  `json:"loaded,omitempty"`
	Active   bool   `json:"active,omitempty"`
	Material string `json:"material,omitempty"`
	// Color is the reported tray color as RRGGBBAA hex, empty when the
	// printer reported none or only the all-zero placeholder.
	Color  string `json:"color,omitempty"`
	Remain *int   `json:"remain,omitempty"`
	// SubBrand, NozzleTempMin and NozzleTempMax follow Material's
	// persistence rules.
	SubBrand      string `json:"sub_brand,omitempty"`
	NozzleTempMin *int   `json:"nozzle_temp_min,omitempty"`
	NozzleTempMax *int   `json:"nozzle_temp_max,omitempty"`
	// stateReported records that this slot's firmware reports the state
	// bitfield, so occupancy resolves from that bitfield instead of the
	// metadata fallback; state is the last reported bitfield value.
	stateReported bool
	state         int
	// idx is the raw tray_info_idx filament profile id. It feeds the
	// metadata fallback next to Material but is not displayed.
	idx string
}

// Report-format constants from the established Bambu AMS encoding.
const (
	amsSlotsPerUnit       = 4   // conventional AMS units expose four slots
	amsMaxUnitID          = 127 // higher unit ids are AMS HT hardware, not modeled
	amsTrayStateSpool     = 0x01
	amsTrayStateSteady    = 0x08
	amsTrayStateLegacyMax = 3
	extSpoolID            = 254 // legacy tray id of the external spool
)

// mergeDisplay applies the display-only report fields: the stage id, the
// AMS unit and slot deltas keyed by their ids, the external spool, and the
// active selection. It runs before the real-print report gate because stage
// and AMS deltas ride in payloads whose print object the gate must keep
// rejecting for detection. It reads and writes only display fields, so
// freshness evidence and session bookkeeping stay exactly as the gate and
// trackSession left them. log is the cache logger, threaded only for the
// DEBUG sensor diagnostic in mergeAMS.
func mergeDisplay(st *State, printObj map[string]any, log *slog.Logger) {
	if v, ok := intField(printObj, "stg_cur"); ok {
		st.Stage = &v
	}
	mergeExtras(st, printObj)
	if raw, ok := lookup(printObj, "ams"); ok {
		if amsObj, ok := raw.(map[string]any); ok {
			if v, ok := intField(amsObj, "tray_now"); ok {
				st.trayNow = &v
			}
			mergeAMS(st, amsObj, log)
		}
	}
	if raw, ok := lookup(printObj, "vt_tray"); ok {
		if tray, ok := raw.(map[string]any); ok {
			mergeExtSpool(st, tray)
		}
	}
	resolveSelection(st)
}

// mergeExtras applies the remaining display-only report fields. Each field
// updates only when present and well-formed; anything else keeps the
// previous value. None of these keys are print-state markers.
func mergeExtras(st *State, printObj map[string]any) {
	if raw, ok := lookup(printObj, "lights_report"); ok {
		if list, ok := raw.([]any); ok {
			for _, item := range list {
				obj, ok := item.(map[string]any)
				if !ok {
					continue
				}
				if node, _ := stringField(obj, "node"); node != "chamber_light" {
					continue
				}
				if mode, ok := stringField(obj, "mode"); ok && (mode == "on" || mode == "off" || mode == "flashing") {
					st.ChamberLight = mode
				}
			}
		}
	}
	if raw, ok := lookup(printObj, "gcode_start_time"); ok {
		if s, isStr := raw.(string); isStr && strings.TrimSpace(s) == "" {
			st.StartedAt = time.Time{}
		} else if v, ok := numberField(printObj, "gcode_start_time"); ok {
			if v <= 0 {
				st.StartedAt = time.Time{}
			} else {
				st.StartedAt = time.Unix(int64(v), 0).UTC()
			}
		}
	}
	if v, ok := intField(printObj, "spd_mag"); ok {
		st.SpeedPercent = &v
	}
	fan := func(key string, dst **int) {
		v, ok := numberField(printObj, key)
		if !ok || v < 0 {
			return
		}
		// Bambu reports fan speed on a 0..15 scale; larger values are
		// already percents.
		pct := int(math.Round(v * 100 / 15))
		if v > 15 {
			pct = min(int(math.Round(v)), 100)
		}
		*dst = &pct
	}
	fans := st.Fans
	fan("cooling_fan_speed", &fans.Part)
	fan("big_fan1_speed", &fans.Aux)
	fan("big_fan2_speed", &fans.Chamber)
	fan("heatbreak_fan_speed", &fans.Heatbreak)
	st.Fans = fans
	if v, ok := numberField(printObj, "nozzle_diameter"); ok && v > 0 {
		st.NozzleDiameter = &v
	}
	if v, ok := stringField(printObj, "nozzle_type"); ok && v != "" {
		st.NozzleType = v
	}
	if s, ok := stringField(printObj, "wifi_signal"); ok {
		if v, err := strconv.Atoi(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), "dBm"))); err == nil {
			st.WifiDBm = &v
		}
	}
	if v, ok := boolField(printObj, "sdcard"); ok {
		st.SDCard = &v
	}
	if v, ok := intField(printObj, "hw_switch_state"); ok && (v == 0 || v == 1) {
		present := v == 1
		st.ExtSpoolSensor = &present
	}
	if raw, ok := lookup(printObj, "ipcam"); ok {
		if obj, ok := raw.(map[string]any); ok {
			if s, ok := stringField(obj, "timelapse"); ok && (s == "enable" || s == "disable") {
				on := s == "enable"
				st.Timelapse = &on
			}
		}
	}
	if raw, ok := lookup(printObj, "xcam"); ok {
		if obj, ok := raw.(map[string]any); ok {
			if v, ok := boolField(obj, "first_layer_inspector"); ok {
				st.FirstLayerInspection = &v
			}
			if v, ok := boolField(obj, "spaghetti_detector"); ok {
				st.SpaghettiDetection = &v
			}
		}
	}
}

// boolField extracts a JSON boolean.
func boolField(obj map[string]any, key string) (bool, bool) {
	v, ok := lookup(obj, key)
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

// mergeAMS merges one report's AMS unit deltas into st by unit and slot id.
// Units and slots the report omits keep their merged values. A unit appears
// on first sight with four never-reported slots and is never removed: the
// delta stream offers no sufficient unit-removal evidence. The cache logger
// receives one DEBUG sensor record per report unit that carries
// humidity_raw or temp; the record observes already-parsed report fields
// and the retained merge result, never fetches, and is the only
// diagnostic in the merge path.
func mergeAMS(st *State, amsObj map[string]any, log *slog.Logger) {
	raw, ok := lookup(amsObj, "ams")
	if !ok {
		return
	}
	reports, ok := raw.([]any)
	if !ok || len(reports) == 0 {
		return
	}
	merged := append([]AMSUnit(nil), st.AMS...)
	for _, item := range reports {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id, ok := intField(obj, "id")
		if !ok || id < 0 || id > amsMaxUnitID {
			continue
		}
		at := -1
		for i := range merged {
			if merged[i].ID == id {
				at = i
				break
			}
		}
		if at == -1 {
			merged = append(merged, newAMSUnit(id))
			at = len(merged) - 1
		}
		unit := merged[at]
		unit.Slots = append([]AMSSlot(nil), unit.Slots...)
		_, humidityPresent := lookup(obj, "humidity_raw")
		humidityRaw, humidityParsed := intField(obj, "humidity_raw")
		humidityValid := humidityParsed && humidityRaw >= 1 && humidityRaw <= 100
		if humidityValid {
			unit.Humidity = &humidityRaw
		}
		_, tempPresent := lookup(obj, "temp")
		// Plausible AMS ambient readings are 0..100 Celsius (numberField
		// already rejected nonfinite values); out-of-range sentinels like
		// the observed 6503.6 retain the prior valid reading.
		tempRaw, tempParsed := numberField(obj, "temp")
		tempValid := tempParsed && tempRaw >= 0 && tempRaw <= 100
		if tempValid {
			unit.Temp = &tempRaw
		}
		// DEBUG sensor diagnostic at the existing report observation path:
		// one record per unit whose report carries a sensor field, with the
		// raw sensor value when it parsed as a finite number and the
		// retained effective value when known. This observes the report the
		// merge already parsed — never a fetch. Typed numbers and flags
		// only: arbitrary report strings never enter the record, so no new
		// settings, timers, or rate-limit state are needed; the logger
		// level gates every emission.
		if (humidityPresent || tempPresent) && log != nil &&
			log.Enabled(context.Background(), slog.LevelDebug) {
			attrs := make([]slog.Attr, 0, 10)
			attrs = append(attrs,
				slog.String("serial", st.Serial),
				slog.Int("unit_id", id),
				slog.Bool("humidity_present", humidityPresent),
				slog.Bool("humidity_valid", humidityValid),
				slog.Bool("temp_present", tempPresent),
				slog.Bool("temp_valid", tempValid),
			)
			if humidityParsed {
				attrs = append(attrs, slog.Int("humidity_raw", humidityRaw))
			}
			if tempParsed {
				attrs = append(attrs, slog.Float64("temp_raw", tempRaw))
			}
			if unit.Humidity != nil {
				attrs = append(attrs, slog.Int("humidity", *unit.Humidity))
			}
			if unit.Temp != nil {
				attrs = append(attrs, slog.Float64("temp", *unit.Temp))
			}
			log.LogAttrs(context.Background(), slog.LevelDebug, "ams_sensor", attrs...)
		}
		if trays, ok := lookup(obj, "tray"); ok {
			if list, ok := trays.([]any); ok {
				for _, t := range list {
					tray, ok := t.(map[string]any)
					if !ok {
						continue
					}
					if sid, ok := intField(tray, "id"); ok && sid >= 0 && sid < amsSlotsPerUnit {
						unit.Slots[sid] = mergeSlot(unit.Slots[sid], tray)
					}
				}
			}
		}
		merged[at] = unit
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].ID < merged[j].ID })
	st.AMS = merged
}

// newAMSUnit creates a never-reported unit: four slots with unknown
// occupancy, so consumers can keep an honest Unknown label.
func newAMSUnit(id int) AMSUnit {
	u := AMSUnit{ID: id, Slots: make([]AMSSlot, amsSlotsPerUnit)}
	for i := range u.Slots {
		u.Slots[i].ID = i
	}
	return u
}

// mergeSlot merges one tray delta onto an AMS slot. Filament metadata
// persists across deltas; a metadata-only refresh (id and state alone)
// updates state but never metadata. Occupancy resolves from the reported
// state bitfield once the firmware sends one, else from metadata presence.
// A resolved-empty slot drops its stale filament data with it — that
// bitfield is the removal evidence, and nothing else clears metadata.
func mergeSlot(slot AMSSlot, tray map[string]any) AMSSlot {
	if !trayMetadataOnly(tray) {
		if v, ok := stringField(tray, "tray_info_idx"); ok {
			slot.idx = v
		}
		if v, ok := stringField(tray, "tray_type"); ok {
			slot.Material = v
		}
		if v, ok := stringField(tray, "tray_color"); ok {
			slot.Color = trayColor(v)
		}
		if v, ok := stringField(tray, "tray_sub_brands"); ok {
			slot.SubBrand = v
		}
		if v, ok := intField(tray, "nozzle_temp_min"); ok {
			slot.NozzleTempMin = &v
		}
		if v, ok := intField(tray, "nozzle_temp_max"); ok {
			slot.NozzleTempMax = &v
		}
		if v, ok := intField(tray, "remain"); ok {
			slot.Remain = nil
			if v >= 0 {
				slot.Remain = &v
			}
		}
	}
	if v, ok := intField(tray, "state"); ok {
		slot.stateReported = true
		slot.state = v
	}
	loaded := slotLoaded(slot)
	slot.Loaded = &loaded
	if !loaded {
		slot.idx, slot.Material, slot.Color, slot.Remain = "", "", "", nil
		slot.SubBrand, slot.NozzleTempMin, slot.NozzleTempMax = "", nil, nil
	}
	return slot
}

// mergeExtSpool merges the single external spool (vt_tray). The established
// P1/A1 format keeps the last mounted filament there and reports no state
// bitfield for it, so occupancy comes from filament metadata only, and a
// metadata-only vt_tray delta is the explicit removal evidence. Its remain
// field carries hardware placeholders rather than an estimate, so it never
// becomes one.
func mergeExtSpool(st *State, tray map[string]any) {
	ext := &AMSSlot{ID: extSpoolID}
	if !trayMetadataOnly(tray) {
		if st.ExtSpool != nil {
			copied := *st.ExtSpool
			ext = &copied
		}
		if v, ok := stringField(tray, "tray_info_idx"); ok {
			ext.idx = v
		}
		if v, ok := stringField(tray, "tray_type"); ok {
			ext.Material = v
		}
		if v, ok := stringField(tray, "tray_color"); ok {
			ext.Color = trayColor(v)
		}
	}
	loaded := ext.idx != "" || (ext.Material != "" && ext.Material != "Empty")
	ext.Loaded = &loaded
	if !loaded {
		ext.idx, ext.Material, ext.Color = "", "", ""
	}
	st.ExtSpool = ext
}

// resolveSelection marks the filament the printer currently feeds. Only the
// supported single-extruder encodings of tray_now apply: 255 selects
// nothing, 254 the external spool, values below 80 the unit>>2 / slot&3
// pair. AMS HT targets (80 and above) and unknown values select nothing
// instead of guessing, and a selection is marked only on units and slots
// that actually exist.
func resolveSelection(st *State) {
	if st.trayNow == nil {
		return
	}
	unitID, slotID, extActive, known := decodeSelection(*st.trayNow)
	units := append([]AMSUnit(nil), st.AMS...)
	for i := range units {
		units[i].Slots = append([]AMSSlot(nil), units[i].Slots...)
		for j := range units[i].Slots {
			units[i].Slots[j].Active = known && !extActive && unitID >= 0 &&
				units[i].ID == unitID && units[i].Slots[j].ID == slotID
		}
	}
	st.AMS = units
	if st.ExtSpool != nil {
		ext := *st.ExtSpool
		ext.Active = known && extActive
		st.ExtSpool = &ext
	}
}

// decodeSelection maps a reported tray_now value to the selected unit and
// slot, with unitID -1 meaning a known "nothing selected". known is false
// for the AMS HT range and unmapped encodings, which must never mark a
// selection.
func decodeSelection(now int) (unitID, slotID int, extActive, known bool) {
	switch {
	case now == 255:
		return -1, -1, false, true
	case now == 254:
		return 0, 0, true, true
	case now >= 0 && now < 80:
		return now >> 2, now & 0x3, false, true
	}
	return 0, 0, false, false
}

// slotLoaded resolves slot occupancy. A reported state bitfield wins: a
// spool counts only with the spool bit set and, outside the legacy 0-3
// encodings, only once the tray is steady rather than loading or scanning.
// Without any state report, an assigned filament profile or a real material
// type means a spool is loaded.
func slotLoaded(slot AMSSlot) bool {
	if slot.stateReported {
		if slot.state&amsTrayStateSpool == 0 {
			return false
		}
		if slot.state <= amsTrayStateLegacyMax {
			return slot.state == amsTrayStateLegacyMax
		}
		return slot.state&amsTrayStateSteady != 0
	}
	return slot.idx != "" || (slot.Material != "" && slot.Material != "Empty")
}

// trayMetadataOnly reports whether a tray delta carries only identity and
// state, the firmware's minimal refresh. Like the established parser, such
// a delta updates state but never filament metadata.
func trayMetadataOnly(tray map[string]any) bool {
	if _, ok := lookup(tray, "id"); !ok {
		return false
	}
	for k := range tray {
		if !equalFold(k, "id") && !equalFold(k, "state") {
			return false
		}
	}
	return true
}

// trayColor keeps reported tray colors that render: exactly eight hex
// digits that are not the all-zero "no color" placeholder. Anything else
// stays unknown.
func trayColor(v string) string {
	if len(v) != 8 {
		return ""
	}
	if _, err := strconv.ParseUint(v, 16, 32); err != nil {
		return ""
	}
	if v == "00000000" {
		return ""
	}
	return v
}
