package printer

import (
	"encoding/json"
	"sort"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/activity"
	"bambu-mqtt-proxy/internal/config"
)

// storedPrinter holds one configured printer's report and link state.
type storedPrinter struct {
	// identity is the configured display identity.
	identity config.Printer
	// doc is immutable after publication.
	doc Doc
	// online is current link connectivity.
	online bool
	// lastReportAt is the display-only report timestamp.
	lastReportAt time.Time
}

// Store holds documents and connectivity under one mutex.
type Store struct {
	// eventMu preserves report and connectivity dispatch order.
	eventMu sync.Mutex
	// subscribers receive events outside the document lock.
	subscribers []func(Event)
	// activity records transitions before subscribers receive them.
	activity *activity.Log
	// mu protects printer entries and the accessory reader.
	mu sync.Mutex
	// printers indexes configured serials.
	printers map[string]storedPrinter
	// chamberRead reads the accessory sensor outside the store lock.
	chamberRead func(string) (float64, time.Time, bool)
}

// NewStore indexes configured printers before reports arrive.
func NewStore(printers []config.Printer) *Store {
	s := &Store{printers: make(map[string]storedPrinter, len(printers))}
	for _, p := range printers {
		s.printers[p.Serial] = storedPrinter{identity: p, doc: Doc{}}
	}
	return s
}

// Apply merges an object report and records its display timestamp.
func (s *Store) Apply(serial string, payload []byte) {
	var delta map[string]any
	if json.Unmarshal(payload, &delta) != nil || delta == nil {
		return
	}
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	s.mu.Lock()
	p, ok := s.printers[serial]
	if !ok {
		s.mu.Unlock()
		return
	}
	prev := p.doc
	p.doc = mergeDoc(prev, delta, true)
	p.lastReportAt = time.Now()
	s.printers[serial] = p
	log, subscribers := s.activity, append([]func(Event){}, s.subscribers...)
	s.mu.Unlock()
	transitions := Transitions(prev, p.doc)
	event := Event{Serial: serial, Prev: prev, Cur: p.doc, Online: p.online}
	for _, transition := range transitions {
		log.RecordLink(serial, transition.Kind, transition.Severity, transition.Message, transition.URL)
		event.Kinds = append(event.Kinds, transition.Kind)
	}
	for _, fn := range subscribers {
		fn(event)
	}
}

// SetOnline records link connectivity without clearing the document.
func (s *Store) SetOnline(serial string, online bool) {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	s.mu.Lock()
	p, ok := s.printers[serial]
	if !ok || p.online == online {
		s.mu.Unlock()
		return
	}
	p.online = online
	s.printers[serial] = p
	log, subscribers := s.activity, append([]func(Event){}, s.subscribers...)
	s.mu.Unlock()
	kind, severity, message := "disconnected", activity.Warning, "Printer connection lost, reconnecting"
	if online {
		kind, severity, message = "connected", activity.Info, "Printer connected"
	}
	log.Record(serial, kind, severity, message)
	event := Event{Serial: serial, Prev: p.doc, Cur: p.doc, Online: online, Kinds: []string{kind}}
	for _, fn := range subscribers {
		fn(event)
	}
}

// Subscribe registers a synchronous, non-blocking report callback at any time.
// Callbacks must not send printer commands.
func (s *Store) Subscribe(fn func(Event)) {
	if fn == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = append(s.subscribers, fn)
}

// SetActivity attaches the transition log.
func (s *Store) SetActivity(log *activity.Log) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activity = log
}

// Get returns a read-only published document and connectivity.
func (s *Store) Get(serial string) (Doc, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.printers[serial]
	return p.doc, p.online, ok
}

// SetChamberReader attaches the accessory sensor during wiring.
func (s *Store) SetChamberReader(read func(string) (float64, time.Time, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chamberRead = read
}

// View returns one configured printer's current display projection.
func (s *Store) View(serial string) (View, bool) {
	s.mu.Lock()
	p, ok := s.printers[serial]
	read := s.chamberRead
	s.mu.Unlock()
	if !ok {
		return View{}, false
	}
	now := time.Now()
	v := Build(p.doc, p.identity, p.online, p.lastReportAt, now)
	if read != nil && !config.ChamberTemperatureSupported(v.Model, v.Serial) {
		if temp, at, ok := read(serial); ok && now.Sub(at) <= FreshnessWindow {
			v.ChamberTemp = &temp
		}
	}
	return v, true
}

// Views returns projections sorted by serial.
func (s *Store) Views() []View {
	s.mu.Lock()
	serials := make([]string, 0, len(s.printers))
	for serial := range s.printers {
		serials = append(serials, serial)
	}
	s.mu.Unlock()
	sort.Strings(serials)
	out := make([]View, 0, len(serials))
	for _, serial := range serials {
		v, _ := s.View(serial)
		out = append(out, v)
	}
	return out
}
