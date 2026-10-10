package printer

import "fmt"

// Pause sends the Bambu pause command.
func (ls *Links) Pause(serial string) error {
	return ls.Send(serial, []byte(`{"print":{"sequence_id":"0","command":"pause"}}`))
}

// Resume sends the Bambu resume command.
func (ls *Links) Resume(serial string) error {
	return ls.Send(serial, []byte(`{"print":{"sequence_id":"0","command":"resume"}}`))
}

// Stop sends the Bambu stop command.
func (ls *Links) Stop(serial string) error {
	return ls.Send(serial, []byte(`{"print":{"sequence_id":"0","command":"stop"}}`))
}

// Light sends the Bambu chamber-light command.
func (ls *Links) Light(serial string, on bool) error {
	mode := "off"
	if on {
		mode = "on"
	}
	return ls.Send(serial, []byte(`{"system":{"sequence_id":"0","command":"ledctrl","led_node":"chamber_light","led_mode":"`+mode+
		`","led_on_time":500,"led_off_time":500,"loop_times":0,"interval_time":0}}`))
}

// Speed sends the Bambu speed profile from 1 (silent) to 4 (ludicrous).
func (ls *Links) Speed(serial string, level int) error {
	if level < 1 || level > 4 {
		return fmt.Errorf("speed %s: unsupported profile %d", serial, level)
	}
	return ls.Send(serial, []byte(fmt.Sprintf(`{"print":{"sequence_id":"0","command":"print_speed","param":"%d"}}`, level)))
}
