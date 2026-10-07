package pandapwr

import "bambu-mqtt-proxy/internal/config"

// AddressKey is the per-printer setting key for a Panda PWR address.
const AddressKey = "panda_pwr"

// AddressSetting describes and validates the optional Panda PWR address:
// the proxy polls http://<host>/update_ele_data from the host form HostOnly
// gives.
var AddressSetting = config.PrinterSetting{
	Key:         AddressKey,
	Label:       "Panda PWR address",
	Placeholder: "192.168.4.1",
	Hint:        "Host name or IP address of a Panda PWR smart plug, with an optional port. It supplies the printer's power draw.",
	Validate: func(value string) error {
		_, err := config.HostOnly(value)
		return err
	},
}
