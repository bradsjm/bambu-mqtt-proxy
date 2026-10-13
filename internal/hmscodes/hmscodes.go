// Package hmscodes downloads Bambu Lab's official per-model message catalog
// from e.bambulab.com and resolves printer health (HMS) and screen error
// codes from it.
//
// The catalog is fetched per three-character model prefix derived from the
// printer serial, cached as raw JSON beside the configuration, and refreshed
// in the background. Snapshots are immutable views: a snapshot taken before
// an update keeps the old messages.
package hmscodes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultBaseURL is the official Bambu Lab message catalog host.
const DefaultBaseURL = "https://e.bambulab.com"

// ErrorTableURL links screen codes to Bambu's error-code table.
const ErrorTableURL = "https://wiki.bambulab.com/en/hms/error-code"

// State reports whether a catalog snapshot can resolve codes.
type State int

const (
	// StateLoading means the catalog download has not completed yet.
	StateLoading State = iota
	// StateReady means lookups resolve against the published table, which may be empty after result 201 without a verified full catalog.
	StateReady
	// StateFailed means the last download attempt failed and no full table is available.
	StateFailed
)

// Lookup is the result of resolving one code against a snapshot.
type Lookup struct {
	// State is the catalog state behind this result.
	State State
	// Listed reports whether the Ready catalog contains the code. It
	// distinguishes a missing screen code from a listed code with an
	// empty intro.
	Listed bool
	// Message is the official catalog intro. It is empty for unlisted
	// codes and for listed codes with no text.
	Message string
	// Err is the underlying download error for Failed lookups.
	Err string
}

// table is one model's immutable message table.
type table struct {
	// state reports whether the table resolves codes.
	state State
	// version is the last full catalog version.
	version int64
	// err holds the last failure text.
	err string
	// hms maps uppercase long codes to intros.
	hms map[string]string
	// errors maps uppercase screen codes to intros.
	errors map[string]string
}

// Snapshot is an immutable view of one model's message table. The zero value
// means Loading.
type Snapshot struct {
	// table is the published immutable table.
	table *table
}

// Catalog downloads and caches the official message catalog per model prefix.
type Catalog struct {
	// mu guards the published tables.
	mu sync.Mutex
	// tables indexes model prefixes to published tables.
	tables map[string]*table
	// prefixes lists unique configured model prefixes.
	prefixes []string
	// baseURL is the catalog host.
	baseURL string
	// dir is the cache directory; empty means memory-only.
	dir string
	// client performs catalog downloads.
	client *http.Client
	// logger records catalog operations.
	logger *slog.Logger
}

// New loads cached catalogs synchronously and returns a catalog that Run
// refreshes. Serials contribute their first three characters as model
// prefixes; shorter serials are ignored. An empty baseURL selects
// DefaultBaseURL. An empty dir means memory-only: no cache files are read or
// written. A nil logger uses slog.Default.
func New(baseURL, dir string, serials []string, logger *slog.Logger) *Catalog {
	if logger == nil {
		logger = slog.Default()
	}
	if baseURL == "" {
		baseURL = DefaultBaseURL
	} else {
		baseURL = strings.TrimSuffix(baseURL, "/")
	}
	// seen deduplicates model prefixes.
	seen := make(map[string]struct{}, len(serials))
	// prefixes lists unique worker prefixes.
	prefixes := make([]string, 0, len(serials))
	// s is one configured serial.
	for _, s := range serials {
		if len(s) < 3 {
			continue
		}
		// p is the serial's model prefix.
		p := s[:3]
		// ok reports a duplicate prefix.
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		prefixes = append(prefixes, p)
	}
	// c is the catalog with its worker set.
	c := &Catalog{
		tables:   make(map[string]*table, len(prefixes)),
		prefixes: prefixes,
		baseURL:  baseURL,
		dir:      dir,
		client:   &http.Client{Timeout: 20 * time.Second},
		logger:   logger,
	}
	if dir == "" {
		return c
	}
	// prefix is one model prefix with a cache file.
	for _, prefix := range prefixes {
		// path is the prefix cache file.
		path := filepath.Join(dir, "hms_en_"+prefix+".json")
		// data holds raw cache bytes.
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			c.logger.Warn("Bambu message catalog cache invalid; downloading the full catalog", "operation", "load message catalog cache", "prefix", prefix, "path", path, "error", err, "next_action", "downloading the full catalog")
			continue
		}
		// version, hmsMap, errMap, and notModified decode the cached response.
		version, hmsMap, errMap, notModified, err := parseCatalog(data)
		if err != nil {
			c.logger.Warn("Bambu message catalog cache invalid; downloading the full catalog", "operation", "load message catalog cache", "prefix", prefix, "path", path, "error", err, "next_action", "downloading the full catalog")
			continue
		}
		if notModified {
			// cacheErr reports a non-full cached response.
			cacheErr := errors.New("cache response result 201 contains no full catalog")
			c.logger.Warn("Bambu message catalog cache invalid; downloading the full catalog", "operation", "load message catalog cache", "prefix", prefix, "path", path, "error", cacheErr, "next_action", "downloading the full catalog")
			continue
		}
		c.tables[prefix] = &table{state: StateReady, version: version, hms: hmsMap, errors: errMap}
	}
	return c
}

// Snapshot selects the table for serial's three-character model prefix. A nil
// catalog, a short serial, or a missing prefix returns the zero snapshot,
// which means Loading.
func (c *Catalog) Snapshot(serial string) Snapshot {
	if c == nil || len(serial) < 3 {
		return Snapshot{}
	}
	// prefix is the serial's model prefix.
	prefix := serial[:3]
	c.mu.Lock()
	// t is the published table for the prefix.
	t := c.tables[prefix]
	c.mu.Unlock()
	if t == nil {
		return Snapshot{}
	}
	return Snapshot{table: t}
}

// HMS resolves a health code by its full long code. Ready lookups report
// whether the code is listed and its official text. Failed lookups carry the
// download error. Loading applies before the first full table arrives.
func (s Snapshot) HMS(attr, code uint32) Lookup {
	if s.table == nil {
		return Lookup{State: StateLoading}
	}
	switch s.table.state {
	case StateReady:
		// msg and ok are the listed intro and its presence.
		msg, ok := s.table.hms[HMSCode(attr, code)]
		return Lookup{State: StateReady, Listed: ok, Message: msg}
	case StateFailed:
		return Lookup{State: StateFailed, Err: s.table.err}
	default:
		return Lookup{State: StateLoading}
	}
}

// Error resolves a screen code. Ready lookups report whether the code is
// listed and its official text. Failed lookups carry the download error.
// Loading applies before the first full table arrives.
func (s Snapshot) Error(code uint32) Lookup {
	if s.table == nil {
		return Lookup{State: StateLoading}
	}
	switch s.table.state {
	case StateReady:
		// msg and ok are the listed intro and its presence.
		msg, ok := s.table.errors[fmt.Sprintf("%08X", code)]
		return Lookup{State: StateReady, Listed: ok, Message: msg}
	case StateFailed:
		return Lookup{State: StateFailed, Err: s.table.err}
	default:
		return Lookup{State: StateLoading}
	}
}

// Run refreshes each configured model prefix until cancellation, then returns
// nil. Each worker downloads immediately and retries after its next interval.
func (c *Catalog) Run(ctx context.Context) error {
	if c == nil {
		return nil
	}
	// wg waits for all prefix workers.
	var wg sync.WaitGroup
	// prefix is one configured model prefix.
	for _, prefix := range c.prefixes {
		wg.Add(1)
		go func(prefix string) {
			defer wg.Done()
			c.runWorker(ctx, prefix)
		}(prefix)
	}
	wg.Wait()
	return nil
}

// runWorker downloads one prefix immediately, then waits for the next attempt
// with a cancellable timer.
func (c *Catalog) runWorker(ctx context.Context, prefix string) {
	for {
		// delay is the interval before the next attempt.
		delay := c.refresh(ctx, prefix)
		// timer waits for the next attempt.
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// refresh downloads one prefix and returns the retry interval: one minute
// after a failure, 24 hours after a full response or result 201.
func (c *Catalog) refresh(ctx context.Context, prefix string) time.Duration {
	c.mu.Lock()
	// cur is the currently published table.
	cur := c.tables[prefix]
	c.mu.Unlock()
	// curVersion is the conditional request version.
	var curVersion int64
	if cur != nil && cur.state == StateReady {
		curVersion = cur.version
	}
	// requestURL is the catalog query for the prefix.
	requestURL := c.baseURL + "/query.php?lang=en"
	if curVersion > 0 {
		requestURL += "&v=" + strconv.FormatInt(curVersion, 10)
	}
	requestURL += "&d=" + prefix
	// req is the context-bound catalog request.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		if ctx.Err() != nil {
			return time.Minute
		}
		c.publishFailure(prefix, requestURL, err)
		return time.Minute
	}
	// resp is the catalog HTTP response.
	resp, err := c.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return time.Minute
		}
		c.publishFailure(prefix, requestURL, err)
		return time.Minute
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if ctx.Err() != nil {
			return time.Minute
		}
		c.publishFailure(prefix, requestURL, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode))
		return time.Minute
	}
	// body holds the raw response bytes.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		if ctx.Err() != nil {
			return time.Minute
		}
		c.publishFailure(prefix, requestURL, err)
		return time.Minute
	}
	// version, hmsMap, errMap, and notModified decode the response.
	version, hmsMap, errMap, notModified, err := parseCatalog(body)
	if err != nil {
		if ctx.Err() != nil {
			return time.Minute
		}
		c.publishFailure(prefix, requestURL, err)
		return time.Minute
	}
	if notModified {
		c.mu.Lock()
		// cur2 is the table at 201 handling time.
		cur2 := c.tables[prefix]
		if cur2 == nil || cur2.state != StateReady {
			// No prior full table: publish an empty Ready table so
			// health codes without text stay hidden instead of
			// Loading forever. This does not establish a verified
			// empty catalog.
			c.tables[prefix] = &table{state: StateReady, hms: make(map[string]string), errors: make(map[string]string)}
		}
		c.mu.Unlock()
		c.logger.Debug("Bambu message catalog unchanged", "prefix", prefix)
		return 24 * time.Hour
	}
	c.mu.Lock()
	// cur3 is the table at version comparison time.
	cur3 := c.tables[prefix]
	// replace reports whether the download is newer.
	replace := true
	if cur3 != nil && cur3.state == StateReady && version <= cur3.version {
		replace = false
	}
	if replace {
		c.tables[prefix] = &table{state: StateReady, version: version, hms: hmsMap, errors: errMap}
	}
	c.mu.Unlock()
	if !replace {
		return 24 * time.Hour
	}
	c.logger.Info("Bambu message catalog updated", "prefix", prefix, "version", version, "hms_codes", len(hmsMap), "error_codes", len(errMap))
	// err is the cache write failure.
	if err := c.saveCache(prefix, body); err != nil {
		// path is the prefix cache file.
		path := filepath.Join(c.dir, "hms_en_"+prefix+".json")
		c.logger.Warn("Bambu message catalog cache write failed; keeping the catalog in memory", "operation", "save message catalog cache", "prefix", prefix, "path", path, "error", err, "next_action", "keeping the catalog in memory")
	}
	return 24 * time.Hour
}

// publishFailure keeps an existing Ready table and otherwise publishes Failed
// with the underlying error text.
func (c *Catalog) publishFailure(prefix, requestURL string, err error) {
	c.mu.Lock()
	// cur is the currently published table.
	cur := c.tables[prefix]
	if cur == nil || cur.state != StateReady {
		c.tables[prefix] = &table{state: StateFailed, err: err.Error()}
	}
	c.mu.Unlock()
	c.logger.Warn("Bambu message catalog download failed; retry in one minute", "operation", "download message catalog", "prefix", prefix, "url", requestURL, "error", err, "next_action", "retry in one minute")
}

// saveCache stores the raw successful response atomically. A memory-only
// catalog skips every directory operation. Failures keep the downloaded table
// in memory.
func (c *Catalog) saveCache(prefix string, data []byte) error {
	if c.dir == "" {
		return nil
	}
	// path is the prefix cache file.
	path := filepath.Join(c.dir, "hms_en_"+prefix+".json")
	// err is the cache directory failure.
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return err
	}
	// tmp is the temporary cache file.
	tmp, err := os.CreateTemp(c.dir, "hms_en_"+prefix+".*.tmp")
	if err != nil {
		return err
	}
	// name is the temporary file path for cleanup.
	name := tmp.Name()
	// err is the temporary file write failure.
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	// err is the temporary file close failure.
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	// err is the cache rename failure.
	if err := os.Rename(name, path); err != nil {
		_ = os.Remove(name)
		return err
	}
	return nil
}

// HMSCode formats the 16-character HMS long code from the report's attr and
// code words. Levels of five or more normalize to zero.
func HMSCode(attr, code uint32) string {
	// level is the normalized severity level.
	level := code >> 16
	if level >= 5 {
		level = 0
	}
	return fmt.Sprintf("%08X00%02X%04X", attr, level, code&0xFFFF)
}

// HMSURL links a health code to its Bambu wiki page through the catalog
// redirect, using the printer's real serial.
func HMSURL(serial string, attr, code uint32) string {
	return "https://e.bambulab.com/index.php?e=" + HMSCode(attr, code) + "&d=" + url.QueryEscape(serial) + "&s=device_hms&lang=en"
}

// catalogResponse is the outer e.bambulab.com envelope. Result is a pointer
// to distinguish a missing field; Ver stays raw so numbers and quoted decimal
// strings both parse. Nested table versions and unrelated fields are ignored.
type catalogResponse struct {
	// Result is the envelope status: 0 full, 201 unchanged.
	Result *int `json:"result"`
	// Ver is the outer version number or quoted decimal.
	Ver json.RawMessage `json:"ver"`
	// Data holds the message tables.
	Data *catalogData `json:"data"`
}

// catalogData holds the two required message tables.
type catalogData struct {
	// HMS is the health message table.
	HMS catalogTable `json:"device_hms"`
	// Errors is the screen message table.
	Errors catalogTable `json:"device_error"`
}

// catalogTable holds one table's English entries.
type catalogTable struct {
	// En holds the English messages.
	En []catalogItem `json:"en"`
}

// catalogItem is one official message. Empty intros are preserved.
type catalogItem struct {
	// Ecode is the catalog code.
	Ecode string `json:"ecode"`
	// Intro is the official message.
	Intro string `json:"intro"`
}

// parseCatalog decodes a raw catalog response. Result 201 sets notModified and
// requires no other fields. Result 0 returns the version and uppercase-keyed
// maps.
func parseCatalog(data []byte) (version int64, hms, errs map[string]string, notModified bool, err error) {
	// resp is the decoded envelope.
	var resp catalogResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return 0, nil, nil, false, err
	}
	if resp.Result == nil {
		return 0, nil, nil, false, errors.New("missing result")
	}
	switch *resp.Result {
	case 0:
	case 201:
		return 0, nil, nil, true, nil
	default:
		return 0, nil, nil, false, fmt.Errorf("unexpected result %d", *resp.Result)
	}
	if resp.Data == nil {
		return 0, nil, nil, false, errors.New("missing data")
	}
	if resp.Data.HMS.En == nil {
		return 0, nil, nil, false, errors.New("missing device_hms.en")
	}
	if resp.Data.Errors.En == nil {
		return 0, nil, nil, false, errors.New("missing device_error.en")
	}
	// v is the parsed outer version.
	v, err := parseVersion(resp.Ver)
	if err != nil {
		return 0, nil, nil, false, err
	}
	hms = make(map[string]string, len(resp.Data.HMS.En))
	// it is one catalog entry.
	for _, it := range resp.Data.HMS.En {
		hms[strings.ToUpper(it.Ecode)] = it.Intro
	}
	errs = make(map[string]string, len(resp.Data.Errors.En))
	// it is one error-table entry.
	for _, it := range resp.Data.Errors.En {
		errs[strings.ToUpper(it.Ecode)] = it.Intro
	}
	return v, hms, errs, false, nil
}

// parseVersion accepts a JSON number or quoted decimal string as a
// nonnegative version.
func parseVersion(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return 0, errors.New("invalid ver")
	}
	// n is the numeric version candidate.
	var n int64
	// err is the numeric version decode result.
	if err := json.Unmarshal(raw, &n); err == nil {
		if n < 0 {
			return 0, errors.New("invalid ver")
		}
		return n, nil
	}
	// s is the quoted version candidate.
	var s string
	// err is the quoted version decode result.
	if err := json.Unmarshal(raw, &s); err == nil {
		// n is the parsed quoted version.
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 {
			return 0, errors.New("invalid ver")
		}
		return n, nil
	}
	return 0, errors.New("invalid ver")
}
