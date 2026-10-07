// Package pandapwr polls a BigTreeTech Panda PWR smart plug over HTTP and
// records its power draw for printers configured with the panda_pwr
// address. A printer with an address in its config.Printer.Settings gains
// a power panel on the camera wall and serves the reading through the
// module State hook as MCP state.modules.pandapwr.
//
// Flow: the store polls the device's read-only update_ele_data endpoint
// and accepts exactly the power field as a numeric, nonnegative value. The
// device answers one request at a time, so a concurrent request from
// another client fails; a single failed poll gets one fast retry before
// the loop returns to the steady cadence. Readings carry their observation
// time, so a device that stops answering stops projecting instead of
// freezing a stale value. Failures are logged as bounded reason classes
// only: the configured host and the response body never reach the log.
package pandapwr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Poll timing tuning. Constants shape the poll cadence, the HTTP request,
// and the longevity of a reading; they are not configurable because the
// device endpoint is fixed and one poller per printer keeps the device
// from dropping concurrent requests from other clients.
const (
	// pollTimeout bounds one GET request total.
	pollTimeout = 5 * time.Second
	// maxBodyBytes bounds one response body. The measurement frame is
	// tiny; anything larger is not a Panda PWR response.
	maxBodyBytes = 4 << 10
	// freshWindow keeps a reading fresh for this long after its poll. The
	// steady cadence refreshes well inside it, so only a device outage
	// can age a reading out.
	freshWindow = 90 * time.Second
)

// delayKind selects which package delay pollDelay returns.
type delayKind int

const (
	// delayInitial waits before the first poll after Start.
	delayInitial delayKind = iota
	// delayCadence waits between steady-state polls.
	delayCadence
	// delayRetry waits before the single fast retry after a failure.
	delayRetry
)

// pollDelay returns the wait before the next poll for one delay kind. It
// is a variable so tests can shorten waits; production code never swaps
// it, and no option exposes it.
var pollDelay = func(kind delayKind) time.Duration {
	switch kind {
	case delayInitial:
		return time.Duration(rand.Int64N(30000)) * time.Millisecond
	case delayRetry:
		return time.Duration(rand.Int64N(3000)+2000) * time.Millisecond
	default:
		return time.Duration(rand.Int64N(10000)+25000) * time.Millisecond
	}
}

// timeNow returns the current time. It is a variable so tests can control
// the observation and freshness clocks; production code never swaps it.
var timeNow = time.Now

// target is one polled device.
type target struct {
	serial string // routing key shared with the telemetry cache
	addr   string // fixed http:// base built from the configured host
}

// reading is one accepted power observation.
type reading struct {
	power float64   // power draw in watts
	at    time.Time // arrival time of the response that carried it
}

// Store holds the latest power reading per printer and runs one HTTP poll
// loop per configured Panda PWR address. The zero value is not usable;
// construct with New.
type Store struct {
	log     *slog.Logger // process logger; hosts and bodies never reach it
	targets []target     // printers configured with a Panda PWR address

	mu       sync.Mutex         // guards readings
	readings map[string]reading // latest accepted reading per serial

	cancel context.CancelFunc // set by Start; nil before it and after Stop
	done   chan struct{}      // closed when the poll loops have returned
}

// New returns a store for the printers configured with a Panda PWR
// address. Printers without an address get no poll loop; a store with no
// configured device starts nothing.
func New(printers []config.Printer, log *slog.Logger) *Store {
	s := &Store{log: log, readings: make(map[string]reading)}
	for _, p := range printers {
		if addr := p.Setting(AddressKey); addr != "" {
			host, err := config.HostOnly(addr)
			if err != nil {
				// Validate rejects this value before New runs.
				continue
			}
			s.targets = append(s.targets, target{serial: p.Serial, addr: "http://" + host})
		}
	}
	return s
}

// Start launches one poll loop per configured device. It returns
// immediately; with no configured device it does nothing. Stop must not be
// called concurrently with Start or itself: one caller (the service
// wiring) owns the lifecycle.
func (s *Store) Start() {
	if len(s.targets) == 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		s.log.Info("panda pwr poller started", "devices", len(s.targets))
		var wg sync.WaitGroup
		for _, t := range s.targets {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.pollLoop(ctx, t)
			}()
		}
		wg.Wait()
	}()
}

// Stop ends every poll loop and waits for its goroutine, cancelling the
// bounded request context so a stalled device response cannot hold
// shutdown. It is safe to call repeatedly and before Start.
func (s *Store) Stop() {
	if s.cancel == nil {
		return
	}
	s.cancel()
	<-s.done
	s.cancel, s.done = nil, nil
}

// pollLoop keeps polling one device until ctx is done: wait the selected
// delay, poll once, then choose the next delay from the outcome. A failed
// poll gets exactly one fast retry before the second consecutive failure
// restores the steady cadence, so a busy device that answers the retry
// loses far less fresh coverage than a backoff would accept. Failures are
// logged only as bounded reason classes on transitions between failing
// and succeeding streaks; the configured host never enters the log.
func (s *Store) pollLoop(ctx context.Context, t target) {
	kind := delayInitial
	failing := false // last poll already failed and the failure was logged
	for {
		select {
		case <-time.After(pollDelay(kind)):
		case <-ctx.Done():
			return
		}
		if ctx.Err() != nil {
			return
		}
		ok, reason := s.poll(ctx, t)
		if ctx.Err() != nil {
			return
		}
		switch {
		case ok:
			if failing {
				s.log.Info("panda pwr poll recovered",
					"serial", t.serial)
			}
			failing = false
			kind = delayCadence
		case !failing:
			s.log.Warn("panda pwr poll failing",
				"serial", t.serial, "reason", reason)
			failing = true
			kind = delayRetry
		default:
			// Second consecutive failure: the retry is spent; go back
			// to the steady cadence without logging again.
			kind = delayCadence
		}
	}
}

// client is the outbound HTTP client for one poll: no keep-alives, a hard
// total timeout, and no redirects. Responses are tiny and one-shot, and a
// redirect would point the poll loop somewhere the configured address
// validation never checked.
var client = &http.Client{
	Timeout: pollTimeout,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
	Transport: &http.Transport{DisableKeepAlives: true},
}

// poll performs one GET update_ele_data request and, on success, stores
// the freshly observed power reading. It reports whether the poll
// produced an accepted reading and a bounded failure reason for the log;
// the raw error and the response body never leave this function.
func (s *Store) poll(ctx context.Context, t target) (ok bool, reason string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		t.addr+"/update_ele_data", nil)
	if err != nil {
		return false, "transport"
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, failReason(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, "status"
	}
	power, err := decodePower(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return false, "decode"
	}
	r := reading{power: power, at: timeNow()}
	s.mu.Lock()
	s.readings[t.serial] = r
	s.mu.Unlock()
	return true, ""
}

// failReason classifies a request failure into a bounded, payload-free
// reason class for the log. Transport errors can echo the configured host,
// including userinfo it may hold, so the raw error stays out of the log.
func failReason(err error) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return "transport"
}

// decodePower extracts the one reading this poller accepts: a JSON body
// whose power field is a finite, nonnegative number. Malformed bodies,
// truncated responses, missing or null values, and nonnumeric encodings
// all fail the typed decode; json decoders cannot produce a nonfinite
// float from valid JSON, and the explicit checks catch one that never
// should exist. Every other field is ignored, and body contents never
// reach the log.
func decodePower(r io.Reader) (float64, error) {
	var frame struct {
		// A nil pointer means the field was missing or null; a string
		// or object fails the decode.
		Power *float64 `json:"power"`
	}
	if err := json.NewDecoder(r).Decode(&frame); err != nil {
		return 0, err
	}
	if frame.Power == nil {
		return 0, errors.New("power missing")
	}
	if math.IsNaN(*frame.Power) || math.IsInf(*frame.Power, 1) || math.IsInf(*frame.Power, -1) || *frame.Power < 0 {
		return 0, errors.New("power not finite or negative")
	}
	return *frame.Power, nil
}
