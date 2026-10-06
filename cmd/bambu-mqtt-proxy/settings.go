package main

import (
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/pandabreath"
)

// init registers the single list of module printer settings before config loading.
func init() {
	config.RegisterPrinterSetting(pandabreath.AddressSetting)
}
