package pandapwr

import (
	"fmt"
	"net/url"

	"bambu-mqtt-proxy/internal/config"
)

// AddressKey is the per-printer setting key for a Panda PWR HTTP address.
const AddressKey = "panda_pwr"

// AddressSetting describes and validates the optional Panda PWR address.
var AddressSetting = config.PrinterSetting{
	Key:         AddressKey,
	Label:       "Panda PWR address",
	Placeholder: "http://192.168.4.1",
	Hint:        "HTTP address of a Panda PWR smart plug; it supplies the printer's power draw.",
	Validate: func(value string) error {
		if !validAddress(value) {
			return fmt.Errorf("must be an http:// or https:// URL with a host and no credentials, query, or fragment")
		}
		return nil
	},
}

// validAddress accepts only an http:// or https:// URL with a host and no
// credentials, query, or fragment: the store appends one fixed path to
// the value and logs it never, so anything the poll loop could not send
// or must not echo is rejected here.
func validAddress(addr string) bool {
	u, err := url.Parse(addr)
	if err != nil {
		return false
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	return true
}
