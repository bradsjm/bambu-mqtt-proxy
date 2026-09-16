// Package routing resolves MQTT topics and filters to configured printer serials.
package routing

import "strings"

// Table holds the configured printer serials and resolves topics and filters
// against them.
type Table struct {
	serials map[string]struct{}
	order   []string // serials in config order for deterministic wildcard expansion
}

// NewTable builds a routing table from the configured printer serials.
func NewTable(serials []string) *Table {
	t := &Table{serials: make(map[string]struct{}, len(serials))}
	for _, s := range serials {
		t.serials[s] = struct{}{}
		t.order = append(t.order, s)
	}
	return t
}

// SerialOf returns the second level of a topic or filter (device/{x}/...):
// the serial, the wildcard marker, or "" when off-grammar.
func SerialOf(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) < 2 || parts[0] != "device" {
		return ""
	}
	return parts[1]
}

// PrintersFor resolves a subscribe filter or publish topic to the printer
// serials it targets. An exact known serial resolves to itself; a serial-level
// wildcard resolves to all printers; anything else resolves to none.
func (t *Table) PrintersFor(filter string) []string {
	s := SerialOf(filter)
	switch {
	case s == "+" || s == "#":
		return t.order
	case s == "":
		return nil
	default:
		if _, ok := t.serials[s]; ok {
			return []string{s}
		}
		return nil
	}
}

// AllowedSubscribe reports whether a subscribe filter matches the proxy
// grammar device/{serial|+}/report|request (or a serial-level # wildcard) and
// targets at least one configured printer.
func (t *Table) AllowedSubscribe(filter string) bool {
	parts := strings.Split(filter, "/")
	switch {
	case len(parts) == 2 && parts[0] == "device" && parts[1] == "#":
		return len(t.order) > 0
	case len(parts) == 3 && parts[0] == "device":
		if parts[2] != "report" && parts[2] != "request" && parts[2] != "#" {
			return false
		}
		return t.knownOrWildcard(parts[1])
	default:
		return false
	}
}

// AllowedPublish reports whether a publish topic is exactly
// device/{knownSerial}/request, the only topic downstream clients may write.
func (t *Table) AllowedPublish(topic string) bool {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 || parts[0] != "device" || parts[2] != "request" {
		return false
	}
	_, ok := t.serials[parts[1]]
	return ok
}

// knownOrWildcard reports whether s is the + wildcard or a configured serial.
func (t *Table) knownOrWildcard(s string) bool {
	if s == "+" {
		return len(t.order) > 0
	}
	_, ok := t.serials[s]
	return ok
}
