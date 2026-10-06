package pandabreath

import (
	"fmt"
	"net/url"

	"bambu-mqtt-proxy/internal/config"
)

// AddressKey is the per-printer setting key for a Panda Breath WebSocket address.
const AddressKey = "panda_breath"

// AddressSetting describes and validates the optional Panda Breath address.
var AddressSetting = config.PrinterSetting{
	Key:         AddressKey,
	Label:       "Panda Breath address",
	Placeholder: "ws://panda-breath-blue.iot/ws",
	Hint:        "WebSocket address of a Panda Breath sensor. It supplies the chamber temperature for printers without a chamber sensor, such as the P1 and A1 series.",
	Validate: func(value string) error {
		u, err := url.Parse(value)
		if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" || u.User != nil || u.Fragment != "" {
			return fmt.Errorf("must be a ws:// or wss:// URL with a host and no credentials or fragment")
		}
		return nil
	},
}
