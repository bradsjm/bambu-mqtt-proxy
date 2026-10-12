// Package routing resolves MQTT topics and filters to configured printer serials.
package routing

import "strings"

// Table holds the configured printer serials and resolves topics and filters
// against them.
type Table struct {
	downstream map[string]string // downstream ID (alias or serial) -> real serial.
	upstream   map[string]string // real serial -> downstream ID.
	order      []string          // order lists serials in config order; its length gates wildcard subscriptions.
}

// NewTable builds a routing table from the configured printer serials.
// Aliases maps a real serial to its downstream alias; a nil map or an empty
// entry leaves that printer addressed by its serial.
func NewTable(serials []string, aliases map[string]string) *Table {
	t := &Table{
		downstream: make(map[string]string, len(serials)),
		upstream:   make(map[string]string, len(serials)),
	}
	for _, s := range serials {
		t.order = append(t.order, s)
		id := s
		if alias, ok := aliases[s]; ok && alias != "" {
			id = alias
		}
		t.downstream[id] = s
		t.upstream[s] = id
	}
	return t
}

// AllowedSubscribe reports whether a subscribe filter matches the proxy
// grammar device/{downstreamID|+}/report|request (or a serial-level #
// wildcard) and targets at least one configured printer.
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

// RequestSerial resolves a downstream publish topic to the real serial it
// commands. Only device/{downstreamID}/request with a configured downstream
// ID resolves; every other topic, including wildcards, extra levels, report
// topics, and an aliased printer's real serial, returns "", false.
func (t *Table) RequestSerial(topic string) (string, bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 || parts[0] != "device" || parts[2] != "request" {
		return "", false
	}
	serial, ok := t.downstream[parts[1]]
	return serial, ok
}

// Downstream rewrites a real-serial device topic to its downstream ID.
// Non-device topics and unknown serials pass through unchanged.
func (t *Table) Downstream(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) < 2 || parts[0] != "device" {
		return topic
	}
	id, ok := t.upstream[parts[1]]
	if !ok {
		return topic
	}
	parts[1] = id
	return strings.Join(parts, "/")
}

// Upstream rewrites a downstream-ID device topic to its real serial.
// Non-device topics and unknown IDs pass through unchanged.
func (t *Table) Upstream(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) < 2 || parts[0] != "device" {
		return topic
	}
	serial, ok := t.downstream[parts[1]]
	if !ok {
		return topic
	}
	parts[1] = serial
	return strings.Join(parts, "/")
}

// knownOrWildcard reports whether s is the + wildcard or a configured
// downstream ID.
func (t *Table) knownOrWildcard(s string) bool {
	if s == "+" {
		return len(t.order) > 0
	}
	_, ok := t.downstream[s]
	return ok
}
