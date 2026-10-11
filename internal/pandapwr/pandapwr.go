// Package pandapwr polls a BigTreeTech Panda PWR smart plug over HTTP and records its power draw for printers configured with the panda_pwr address.
package pandapwr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"bambu-mqtt-proxy/internal/config"
)

// Poll timing tuning.
const (
	// pollTimeout bounds one GET request total.
	// pollTimeout is maxBodyBytes bounds one response body.
	pollTimeout = 5 * time.Second
	// maxBodyBytes is freshWindow keeps a reading fresh for this long after its poll.
	maxBodyBytes = 4 << 10
	freshWindow  = 90 * time.Second
)

// timeNow returns the current time.
var timeNow = time.Now

// target is one polled device.
type target struct {
	// serial is the configured printer routing key.
	serial string
	// addr is fixed http:// base built from the configured host.
	addr string
}

// reading is one accepted power observation.
type reading struct {
	// power is power draw in watts.
	power float64
	// at is arrival time of the response that carried it.
	at time.Time
}

// Store holds the latest power reading per printer and runs one HTTP poll loop per configured Panda PWR address.
type Store struct {
	// log is process logger; hosts and bodies never reach it.
	log *slog.Logger
	// targets is printers configured with a Panda PWR address.
	targets []target

	// mu is guards readings.
	mu sync.Mutex
	// readings is latest accepted reading per serial.
	readings map[string]reading
}

// New returns a store for the printers configured with a Panda PWR address.
func New(printers []config.Printer, log *slog.Logger) *Store {
	s := &Store{log: log, readings: make(map[string]reading)}
	for _, p := range printers {
		if addr := p.Setting(AddressKey); addr != "" {
			host, err := config.HostOnly(addr)
			// if is validate rejects this value before New runs.
			if err != nil {
				continue
			}
			s.targets = append(s.targets, target{serial: p.Serial, addr: "http://" + host})
		}
	}
	return s
}

// Run polls all devices once per tick until cancellation.
func (s *Store) Run(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			var wg sync.WaitGroup
			for _, t := range s.targets {
				wg.Go(func() {
					if ok, reason := s.poll(ctx, t); !ok && ctx.Err() == nil {
						s.log.Warn("panda pwr poll failed", "serial", t.serial, "reason", reason)
					}
				})
			}
			wg.Wait()
		}
	}
}

// client is the outbound HTTP client for one poll: no keep-alives, a hard total timeout, and no redirects.
var client = &http.Client{
	Timeout: pollTimeout,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
	Transport: &http.Transport{DisableKeepAlives: true},
}

// poll performs one GET update_ele_data request and, on success, stores the freshly observed power reading.
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

// PowerOff switches the Panda PWR relay at the configured address off with one POST /set power=0 request.
func PowerOff(ctx context.Context, address string) error {
	host, err := config.HostOnly(address)
	if err != nil {
		return fmt.Errorf("panda pwr power off %q: %w", address, err)
	}
	url := "http://" + host + "/set"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader("power=0"))
	if err != nil {
		return fmt.Errorf("panda pwr power off: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("panda pwr power off: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("panda pwr power off %s: HTTP %s", url, resp.Status)
	}
	return nil
}

// failReason classifies a request failure into a bounded, payload-free reason class for the log.
func failReason(err error) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return "transport"
}

// decodePower extracts the one reading this poller accepts: a JSON body whose power field is a finite, nonnegative number.
func decodePower(r io.Reader) (float64, error) {
	// var is a nil pointer means the field was missing or null; a string or object fails the decode.
	var frame struct {
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
