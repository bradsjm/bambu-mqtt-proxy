package pandabreath

import "bambu-mqtt-proxy/internal/config"

// AddressKey is the per-printer setting key for a Panda Breath address.
const AddressKey = "panda_breath"

// AddressSetting describes and validates the optional Panda Breath address:
// the proxy connects with ws://<host>/ws from the host form HostOnly gives.
var AddressSetting = config.PrinterSetting{
	Key:         AddressKey,
	Label:       "Panda Breath address",
	Placeholder: "panda-breath-blue.iot",
	Hint:        "Host name or IP address of a Panda Breath sensor, with an optional port. It supplies the chamber temperature for printers without a chamber sensor, such as the P1 and A1 series.",
	Validate: func(value string) error {
		_, err := config.HostOnly(value)
		return err
	},
}
