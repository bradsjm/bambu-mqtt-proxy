package main

import (
	"bambu-mqtt-proxy/internal/config"
	"bambu-mqtt-proxy/internal/detection"
	"bambu-mqtt-proxy/internal/jobpreview"
	"bambu-mqtt-proxy/internal/notification"
	"bambu-mqtt-proxy/internal/pandabreath"
)

// init registers module printer settings and global sections before config loading.
func init() {
	config.RegisterPrinterSetting(pandabreath.AddressSetting)
	config.RegisterSection(detection.ConfigSection)
	config.RegisterSection(notification.ConfigSection)
	config.RegisterSection(jobpreview.ConfigSection)
}
