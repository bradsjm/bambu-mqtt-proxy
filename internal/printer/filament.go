package printer

import (
	"sort"
	"strconv"
)

// AMSUnit is a conventional four-slot filament unit.
type AMSUnit struct {
	// ID is the unit identifier.
	ID int `json:"id"`
	// Humidity is a valid reported percentage.
	Humidity *int `json:"humidity,omitempty"`
	// Temp is a valid ambient reading in Celsius.
	Temp *float64 `json:"temp,omitempty"`
	// Slots are the four filament positions.
	Slots []AMSSlot `json:"slots"`
}

// AMSSlot is an AMS position or external spool.
type AMSSlot struct {
	// ID is the position identifier.
	ID int `json:"id"`
	// Loaded is reported filament occupancy.
	Loaded *bool `json:"loaded,omitempty"`
	// Active marks the reported selection.
	Active bool `json:"active,omitempty"`
	// Material is the reported filament type.
	Material string `json:"material,omitempty"`
	// Color is the reported RRGGBBAA color.
	Color string `json:"color,omitempty"`
	// Remain is the available filament percentage.
	Remain *int `json:"remain,omitempty"`
	// SubBrand is the filament brand.
	SubBrand string `json:"sub_brand,omitempty"`
	// NozzleTempMin is the filament minimum setpoint.
	NozzleTempMin *int `json:"nozzle_temp_min,omitempty"`
	// NozzleTempMax is the filament maximum setpoint.
	NozzleTempMax *int `json:"nozzle_temp_max,omitempty"`
}

// HMS reads the current alert array.
func (d Doc) HMS() ([]HMSAlert, bool) {
	list, ok := d.print()["hms"].([]any)
	alerts := make([]HMSAlert, 0, len(list))
	for _, item := range list {
		obj, _ := item.(map[string]any)
		attr, a := numberField(obj, "attr")
		code, c := numberField(obj, "code")
		if a && c {
			alerts = append(alerts, HMSAlert{uint32(attr), uint32(code)})
		}
	}
	return alerts, ok
}

// filamentSlot projects the current tray metadata.
func filamentSlot(obj map[string]any, id int) AMSSlot {
	s := AMSSlot{ID: id}
	s.Material, _ = stringField(obj, "tray_type")
	s.SubBrand, _ = stringField(obj, "tray_sub_brands")
	color, _ := stringField(obj, "tray_color")
	if len(color) == 8 && color != "00000000" {
		if _, err := strconv.ParseUint(color, 16, 32); err == nil {
			s.Color = color
		}
	}
	s.NozzleTempMin = valuePtr(intField(obj, "nozzle_temp_min"))
	s.NozzleTempMax = valuePtr(intField(obj, "nozzle_temp_max"))
	if n, ok := intField(obj, "remain"); ok && n >= 0 {
		s.Remain = &n
	}
	idx, _ := stringField(obj, "tray_info_idx")
	loaded := idx != "" || (s.Material != "" && s.Material != "Empty")
	s.Loaded = &loaded
	return s
}

// AMS projects the current AMS array and reported occupancy bits.
func (d Doc) AMS() ([]AMSUnit, bool) {
	obj, _ := d.print()["ams"].(map[string]any)
	list, ok := obj["ams"].([]any)
	out := make([]AMSUnit, 0, len(list))
	selected, haveSelection := intField(obj, "tray_now")
	hexBits, _ := obj["tray_exist_bits"].(string) // Bambu reports this mask as a hex string.
	bits, bitsErr := strconv.ParseUint(hexBits, 16, 64)
	haveBits := bitsErr == nil
	for _, item := range list {
		unit, _ := item.(map[string]any)
		id, valid := intField(unit, "id")
		if !valid || id < 0 || id > 127 {
			continue
		}
		u := AMSUnit{ID: id, Slots: make([]AMSSlot, 4)}
		for i := range u.Slots {
			u.Slots[i].ID = i
		}
		if h, ok := intField(unit, "humidity_raw"); ok && h >= 1 && h <= 100 {
			u.Humidity = &h
		}
		if t, ok := numberField(unit, "temp"); ok && t >= 0 && t <= 100 {
			u.Temp = &t
		}
		trays, _ := unit["tray"].([]any)
		for _, raw := range trays {
			tray, _ := raw.(map[string]any)
			sid, valid := intField(tray, "id")
			if valid && sid >= 0 && sid < 4 {
				u.Slots[sid] = filamentSlot(tray, sid)
				if !haveBits {
					if state, ok := intField(tray, "state"); ok {
						loaded := state&1 != 0 && (state == 3 || state > 3 && state&8 != 0)
						u.Slots[sid].Loaded = new(loaded)
					}
				}
			}
		}
		for i := range u.Slots {
			if haveBits {
				loaded := bits&(1<<uint(id*4+i)) != 0
				u.Slots[i].Loaded = &loaded
			}
			u.Slots[i].Active = haveSelection && selected >= 0 && selected < 80 && selected>>2 == id && selected&3 == i
		}
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, ok
}

// ExtSpool projects the current external spool object.
func (d Doc) ExtSpool() (*AMSSlot, bool) {
	obj, ok := d.print()["vt_tray"].(map[string]any)
	if !ok {
		return nil, false
	}
	s := filamentSlot(obj, 254)
	s.Remain = nil
	s.SubBrand, s.NozzleTempMin, s.NozzleTempMax = "", nil, nil
	ams, _ := d.print()["ams"].(map[string]any)
	n, have := intField(ams, "tray_now")
	s.Active = have && n == 254
	return &s, true
}
