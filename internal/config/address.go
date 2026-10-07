package config

import (
	"errors"
	"net"
	"strconv"
	"strings"
)

// DefaultPrinterPort is the MQTT TLS port of every Bambu printer.
const DefaultPrinterPort = "8883"

// WithDefaultPrinterPort returns addr with DefaultPrinterPort appended when
// addr has no port. An empty addr stays empty so Validate can report it.
func WithDefaultPrinterPort(addr string) string {
	if addr == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	return net.JoinHostPort(strings.Trim(addr, "[]"), DefaultPrinterPort)
}

// errAddressForm is the one rejection message of HostOnly. Address values
// never carry a scheme, path, or credentials, so one message covers every
// malformed form and this package's module settings reuse it.
var errAddressForm = errors.New("must be a host name or IP address with an optional port, without a scheme, path, or credentials")

// HostOnly validates a host name or IP address with an optional port and
// returns it in URL host form. A bare IPv6 address gets brackets.
func HostOnly(value string) (string, error) {
	if value == "" || strings.ContainsAny(value, "/?#@ \t") {
		return "", errAddressForm
	}
	if host, port, err := net.SplitHostPort(value); err == nil {
		if host == "" {
			return "", errAddressForm
		}
		n, perr := strconv.Atoi(port)
		if perr != nil || n < 1 || n > 65535 {
			return "", errAddressForm
		}
		return value, nil
	}
	if ip := net.ParseIP(value); ip != nil && strings.Contains(value, ":") {
		return "[" + value + "]", nil
	}
	if strings.ContainsAny(value, ":[]") {
		return "", errAddressForm
	}
	return value, nil
}
