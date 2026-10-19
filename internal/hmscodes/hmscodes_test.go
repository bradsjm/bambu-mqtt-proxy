package hmscodes

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	lifeSerial = "01S00C351100139"
	lifePrefix = "01S"
	lifeAttr   = uint32(0x07007000)
	lifeCode   = uint32(0x00020004)
	lifeScreen = uint32(0x07008004)
	lifeHMSA   = "Failed to pull back the filament from the toolhead to AMS. Possible cause: filament or spool stuck."
	lifeHMSB   = "Replacement health message."
	lifeErrA   = "AMS failed to pull back filament. This could be due to a stuck spool or the end of the filament being stuck in the path."
)

// discardLogger keeps test output quiet.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fullBody builds an official-shaped catalog response with the given outer
// version and English entry lists. Extra fields such as the top-level t and
// the nested table versions must be ignored.
func fullBody(ver, hmsEn, errEn string) string {
	return `{"result":0,"t":1750000000,"ver":` + ver +
		`,"data":{"device_hms":{"ver":202609231145,"en":[` + hmsEn +
		`]},"device_error":{"ver":202609231145,"en":[` + errEn + `]}}}`
}

func hmsEntry(ecode, intro string) string {
	return `{"ecode":"` + ecode + `","intro":"` + intro + `"}`
}

func errEntry(ecode, intro string) string {
	return `{"ecode":"` + ecode + `","intro":"` + intro + `"}`
}

// lifeEnv exposes the catalog under test and the observed requests.
type lifeEnv struct {
	catalog  *Catalog
	ctx      context.Context
	query    func() url.Values
	requests func() int
}

// checkFailedErr requires both lookup kinds to carry a download error.
func checkFailedErr(t *testing.T, env *lifeEnv) {
	t.Helper()
	snap := env.catalog.Snapshot(lifeSerial)
	if got := snap.HMS(lifeAttr, lifeCode); got.Err == "" {
		t.Fatal("HMS err empty, want the download error")
	}
	if got := snap.Error(lifeScreen); got.Err == "" {
		t.Fatal("Error err empty, want the download error")
	}
}

func TestCatalogLifecycle(t *testing.T) {
	hmsA := hmsEntry("0700700000020004", lifeHMSA)
	hmsB := hmsEntry("0700700000020004", lifeHMSB)
	errA := errEntry("07008004", lifeErrA)
	cases := []struct {
		name string
		// cache is pre-written to hms_en_01S.json before New.
		cache string
		// dirMemory selects a memory-only catalog.
		dirMemory bool
		// closedServer makes every request fail at the transport.
		closedServer bool
		status       int
		body         string
		wantDelay    time.Duration
		wantState    State
		// wantV is the expected v query value; wantVSet false means v must be absent.
		wantV    string
		wantVSet bool
		// wantCacheWrite requires the cache file to hold the raw response body.
		wantCacheWrite bool
		pre            func(t *testing.T, c *Catalog)
		check          func(t *testing.T, env *lifeEnv)
	}{
		{
			name:           "full response publishes table without version query",
			body:           fullBody("202609231145", hmsA, errA),
			wantDelay:      24 * time.Hour,
			wantState:      StateReady,
			wantCacheWrite: true,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				snap := env.catalog.Snapshot(lifeSerial)
				if got := snap.HMS(lifeAttr, lifeCode); !got.Listed || got.Message != lifeHMSA {
					t.Fatalf("HMS = %+v, want listed %q", got, lifeHMSA)
				}
				if got := snap.Error(lifeScreen); !got.Listed || got.Message != lifeErrA {
					t.Fatalf("Error = %+v, want listed %q", got, lifeErrA)
				}
			},
		},
		{
			name:      "cached table sends version and 201 keeps it",
			cache:     fullBody("100", hmsA, errA),
			body:      `{"result":201}`,
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			wantV:     "100",
			wantVSet:  true,
			pre: func(t *testing.T, c *Catalog) {
				t.Helper()
				// The cache loads synchronously in New, before any download.
				if got := c.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); got.State != StateReady || got.Message != lifeHMSA {
					t.Fatalf("cached HMS = %+v, want ready %q", got, lifeHMSA)
				}
			},
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				if got := env.catalog.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); !got.Listed || got.Message != lifeHMSA {
					t.Fatalf("HMS after 201 = %+v, want retained %q", got, lifeHMSA)
				}
			},
		},
		{
			name:      "201 without prior table publishes empty ready",
			body:      `{"result":201}`,
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				snap := env.catalog.Snapshot(lifeSerial)
				if got := snap.HMS(lifeAttr, lifeCode); got.Listed || got.Message != "" {
					t.Fatalf("HMS = %+v, want unlisted", got)
				}
				if got := snap.Error(lifeScreen); got.Listed || got.Message != "" {
					t.Fatalf("Error = %+v, want unlisted", got)
				}
			},
		},
		{
			name:      "stale version retains existing table",
			cache:     fullBody("100", hmsA, errA),
			body:      fullBody("50", hmsB, errA),
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			wantV:     "100",
			wantVSet:  true,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				if got := env.catalog.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); got.Message != lifeHMSA {
					t.Fatalf("HMS = %+v, want retained %q", got, lifeHMSA)
				}
			},
		},
		{
			name:      "equal version retains existing table",
			cache:     fullBody("100", hmsA, errA),
			body:      fullBody("100", hmsB, errA),
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			wantV:     "100",
			wantVSet:  true,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				if got := env.catalog.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); got.Message != lifeHMSA {
					t.Fatalf("HMS = %+v, want retained %q", got, lifeHMSA)
				}
			},
		},
		{
			name:      "greater version replaces table",
			cache:     fullBody("100", hmsA, errA),
			body:      fullBody("200", hmsB, errA),
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			wantV:     "100",
			wantVSet:  true,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				if got := env.catalog.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); !got.Listed || got.Message != lifeHMSB {
					t.Fatalf("HMS = %+v, want replaced %q", got, lifeHMSB)
				}
			},
		},
		{
			name:      "fresh version zero publishes",
			body:      fullBody("0", hmsA, errA),
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				if got := env.catalog.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); !got.Listed || got.Message != lifeHMSA {
					t.Fatalf("HMS = %+v, want listed %q", got, lifeHMSA)
				}
			},
		},
		{
			name:      "string version parses and queries",
			body:      fullBody(`"202609231145"`, hmsA, errA),
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				if got := env.catalog.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); !got.Listed || got.Message != lifeHMSA {
					t.Fatalf("HMS = %+v, want listed %q", got, lifeHMSA)
				}
				// A second refresh must send the parsed version back.
				if delay := env.catalog.refresh(env.ctx, lifePrefix); delay != 24*time.Hour {
					t.Fatalf("second refresh delay = %v, want %v", delay, 24*time.Hour)
				}
				if got := env.query().Get("v"); got != "202609231145" {
					t.Fatalf("v query = %q, want %q", got, "202609231145")
				}
				if n := env.requests(); n != 2 {
					t.Fatalf("requests = %d, want 2", n)
				}
			},
		},
		{
			name:      "missing version fails",
			body:      `{"result":0,"data":{"device_hms":{"en":[]},"device_error":{"en":[]}}}`,
			wantDelay: time.Minute,
			wantState: StateFailed,
			check:     checkFailedErr,
		},
		{
			name:      "negative version fails",
			body:      fullBody("-1", "", ""),
			wantDelay: time.Minute,
			wantState: StateFailed,
			check:     checkFailedErr,
		},
		{
			name:      "non-numeric string version fails",
			body:      fullBody(`"abc"`, "", ""),
			wantDelay: time.Minute,
			wantState: StateFailed,
			check:     checkFailedErr,
		},
		{
			name:      "listed empty intro versus absent code",
			body:      fullBody("1", hmsEntry("0700700000020004", ""), ""),
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				snap := env.catalog.Snapshot(lifeSerial)
				if got := snap.HMS(lifeAttr, lifeCode); !got.Listed || got.Message != "" {
					t.Fatalf("known HMS = %+v, want listed with empty message", got)
				}
				if got := snap.HMS(0x07007000, 0x00030009); got.Listed || got.Message != "" {
					t.Fatalf("absent HMS = %+v, want unlisted", got)
				}
				if got := snap.Error(lifeScreen); got.Listed || got.Message != "" {
					t.Fatalf("absent Error = %+v, want unlisted", got)
				}
			},
		},
		{
			name:      "lowercase ecodes match lookups",
			body:      fullBody("1", hmsEntry("0700abcd00020004", lifeHMSA), errEntry("0700800a", lifeErrA)),
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				snap := env.catalog.Snapshot(lifeSerial)
				if got := snap.HMS(0x0700ABCD, 0x00020004); !got.Listed || got.Message != lifeHMSA {
					t.Fatalf("HMS = %+v, want listed %q", got, lifeHMSA)
				}
				if got := snap.Error(0x0700800A); !got.Listed || got.Message != lifeErrA {
					t.Fatalf("Error = %+v, want listed %q", got, lifeErrA)
				}
			},
		},
		{
			name:      "http error without table fails",
			status:    http.StatusInternalServerError,
			body:      "boom",
			wantDelay: time.Minute,
			wantState: StateFailed,
			check:     checkFailedErr,
		},
		{
			name:      "http error keeps last good table",
			cache:     fullBody("100", hmsA, errA),
			status:    http.StatusInternalServerError,
			body:      "boom",
			wantDelay: time.Minute,
			wantState: StateReady,
			wantV:     "100",
			wantVSet:  true,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				if got := env.catalog.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); !got.Listed || got.Message != lifeHMSA {
					t.Fatalf("HMS = %+v, want retained %q", got, lifeHMSA)
				}
			},
		},
		{
			name:         "transport error fails",
			closedServer: true,
			wantDelay:    time.Minute,
			wantState:    StateFailed,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				if got := env.catalog.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); got.Err == "" {
					t.Fatal("HMS err empty, want transport error text")
				}
			},
		},
		{
			name:      "malformed json fails",
			body:      `{"result":0,`,
			wantDelay: time.Minute,
			wantState: StateFailed,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				if got := env.catalog.Snapshot(lifeSerial).Error(lifeScreen); got.Err == "" {
					t.Fatal("Error err empty, want JSON error text")
				}
			},
		},
		{
			name:      "missing result fails",
			body:      `{"ver":1,"data":{"device_hms":{"en":[]},"device_error":{"en":[]}}}`,
			wantDelay: time.Minute,
			wantState: StateFailed,
			check:     checkFailedErr,
		},
		{
			name:      "unexpected result fails",
			body:      `{"result":500}`,
			wantDelay: time.Minute,
			wantState: StateFailed,
			check:     checkFailedErr,
		},
		{
			name:      "missing data fails",
			body:      `{"result":0,"ver":1}`,
			wantDelay: time.Minute,
			wantState: StateFailed,
			check:     checkFailedErr,
		},
		{
			name:      "missing hms table fails",
			body:      `{"result":0,"ver":1,"data":{"device_error":{"en":[]}}}`,
			wantDelay: time.Minute,
			wantState: StateFailed,
			check:     checkFailedErr,
		},
		{
			name:      "null error array fails",
			body:      `{"result":0,"ver":1,"data":{"device_hms":{"en":[]},"device_error":{"en":null}}}`,
			wantDelay: time.Minute,
			wantState: StateFailed,
			check:     checkFailedErr,
		},
		{
			name:      "empty arrays publish ready",
			body:      `{"result":0,"ver":1,"data":{"device_hms":{"en":[]},"device_error":{"en":[]}}}`,
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				snap := env.catalog.Snapshot(lifeSerial)
				if got := snap.HMS(lifeAttr, lifeCode); got.Listed || got.Message != "" {
					t.Fatalf("HMS = %+v, want unlisted", got)
				}
				if got := snap.Error(lifeScreen); got.Listed || got.Message != "" {
					t.Fatalf("Error = %+v, want unlisted", got)
				}
			},
		},
		{
			name:      "memory-only download stays ready",
			dirMemory: true,
			body:      fullBody("1", hmsA, errA),
			wantDelay: 24 * time.Hour,
			wantState: StateReady,
			check: func(t *testing.T, env *lifeEnv) {
				t.Helper()
				snap := env.catalog.Snapshot(lifeSerial)
				if got := snap.HMS(lifeAttr, lifeCode); !got.Listed || got.Message != lifeHMSA {
					t.Fatalf("HMS = %+v, want listed %q", got, lifeHMSA)
				}
				if got := snap.Error(lifeScreen); !got.Listed || got.Message != lifeErrA {
					t.Fatalf("Error = %+v, want listed %q", got, lifeErrA)
				}
				// Memory-only must not create cache files.
				if _, err := os.Stat("hms_en_" + lifePrefix + ".json"); !os.IsNotExist(err) {
					t.Fatalf("memory-only catalog created cache file hms_en_%s.json", lifePrefix)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.dirMemory {
				dir = ""
				_ = os.Remove("hms_en_" + lifePrefix + ".json")
				t.Cleanup(func() { _ = os.Remove("hms_en_" + lifePrefix + ".json") })
			}
			logger := discardLogger()
			var mu sync.Mutex
			var gotQuery url.Values
			var gotCount int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				gotQuery = r.URL.Query()
				gotCount++
				mu.Unlock()
				if tc.status != 0 {
					w.WriteHeader(tc.status)
				}
				_, _ = io.WriteString(w, tc.body)
			}))
			if tc.closedServer {
				srv.Close()
			} else {
				defer srv.Close()
			}
			if tc.cache != "" {
				if err := os.WriteFile(filepath.Join(dir, "hms_en_"+lifePrefix+".json"), []byte(tc.cache), 0o644); err != nil {
					t.Fatalf("write cache: %v", err)
				}
			}
			c := New(srv.URL, dir, []string{lifeSerial}, logger)
			if tc.pre != nil {
				tc.pre(t, c)
			}
			ctx := context.Background()
			if delay := c.refresh(ctx, lifePrefix); delay != tc.wantDelay {
				t.Fatalf("refresh delay = %v, want %v", delay, tc.wantDelay)
			}
			snap := c.Snapshot(lifeSerial)
			if got := snap.HMS(lifeAttr, lifeCode); got.State != tc.wantState {
				t.Fatalf("HMS state = %v, want %v", got.State, tc.wantState)
			}
			if got := snap.Error(lifeScreen); got.State != tc.wantState {
				t.Fatalf("Error state = %v, want %v", got.State, tc.wantState)
			}
			env := &lifeEnv{
				catalog: c,
				ctx:     ctx,
				query: func() url.Values {
					mu.Lock()
					defer mu.Unlock()
					return gotQuery
				},
				requests: func() int {
					mu.Lock()
					defer mu.Unlock()
					return gotCount
				},
			}
			if !tc.closedServer {
				if n := env.requests(); n != 1 {
					t.Fatalf("requests = %d, want 1", n)
				}
				q := env.query()
				if got := q.Get("lang"); got != "en" {
					t.Fatalf("lang query = %q, want %q", got, "en")
				}
				if got := q.Get("d"); got != lifePrefix {
					t.Fatalf("d query = %q, want %q", got, lifePrefix)
				}
				gotV, hasV := q["v"]
				if hasV != tc.wantVSet {
					t.Fatalf("v present = %v, want %v (query %v)", hasV, tc.wantVSet, q)
				}
				if hasV && gotV[0] != tc.wantV {
					t.Fatalf("v query = %q, want %q", gotV[0], tc.wantV)
				}
			}
			if tc.wantCacheWrite {
				data, err := os.ReadFile(filepath.Join(dir, "hms_en_"+lifePrefix+".json"))
				if err != nil {
					t.Fatalf("read cache: %v", err)
				}
				if string(data) != tc.body {
					t.Fatalf("cache = %q, want raw response %q", data, tc.body)
				}
			}
			if tc.check != nil {
				tc.check(t, env)
			}
		})
	}
}

func TestCacheFailure(t *testing.T) {
	ctx := context.Background()
	good := fullBody("7", hmsEntry("0700700000020004", lifeHMSA), errEntry("07008004", lifeErrA))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, good)
	}))
	defer srv.Close()

	badCaches := []struct {
		name string
		body string
	}{
		{"corrupt cache triggers a full fetch", `{"result":0,`},
		{"result 201 cache is not a full catalog", `{"result":201}`},
	}
	for _, tc := range badCaches {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "hms_en_"+lifePrefix+".json")
			if err := os.WriteFile(path, []byte(tc.body), 0o644); err != nil {
				t.Fatalf("write cache: %v", err)
			}
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			c := New(srv.URL, dir, []string{lifeSerial}, logger)
			if got := c.Snapshot(lifeSerial).Error(lifeScreen); got.State != StateLoading {
				t.Fatalf("state after bad cache = %v, want Loading", got.State)
			}
			if delay := c.refresh(ctx, lifePrefix); delay != 24*time.Hour {
				t.Fatalf("refresh delay = %v, want %v", delay, 24*time.Hour)
			}
			if got := c.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); !got.Listed || got.Message != lifeHMSA {
				t.Fatalf("HMS = %+v, want listed %q", got, lifeHMSA)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read cache: %v", err)
			}
			if string(data) != good {
				t.Fatalf("cache = %q, want full response %q", data, good)
			}
			for _, want := range []string{lifePrefix, path} {
				if !strings.Contains(logs.String(), want) {
					t.Fatalf("logs missing %q:\n%s", want, logs.String())
				}
			}
		})
	}

	t.Run("unwritable cache directory keeps catalog in memory", func(t *testing.T) {
		// A regular file where the cache directory belongs makes every
		// directory operation fail deterministically.
		blocker := filepath.Join(t.TempDir(), "cache")
		if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
			t.Fatalf("write blocker: %v", err)
		}
		c := New(srv.URL, blocker, []string{lifeSerial}, discardLogger())
		if delay := c.refresh(ctx, lifePrefix); delay != 24*time.Hour {
			t.Fatalf("refresh delay = %v, want %v", delay, 24*time.Hour)
		}
		snap := c.Snapshot(lifeSerial)
		if got := snap.HMS(lifeAttr, lifeCode); !got.Listed || got.Message != lifeHMSA {
			t.Fatalf("HMS = %+v, want listed %q", got, lifeHMSA)
		}
		if got := snap.Error(lifeScreen); !got.Listed || got.Message != lifeErrA {
			t.Fatalf("Error = %+v, want listed %q", got, lifeErrA)
		}
	})
}

func TestSnapshotStable(t *testing.T) {
	otherSerial := "00W00X123400001"
	dir := t.TempDir()
	caches := map[string]string{
		"hms_en_01S.json": fullBody("100", hmsEntry("0700700000020004", lifeHMSA), ""),
		"hms_en_00W.json": fullBody("100", "", errEntry("07008004", lifeErrA)),
	}
	for name, body := range caches {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write cache %s: %v", name, err)
		}
	}
	updated := fullBody("200", hmsEntry("0700700000020004", lifeHMSB), "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `{"result":201}`
		if r.URL.Query().Get("d") == lifePrefix {
			body = updated
		}
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	c := New(srv.URL, dir, []string{lifeSerial, otherSerial}, discardLogger())
	before := c.Snapshot(lifeSerial)
	if got := before.HMS(lifeAttr, lifeCode); got.State != StateReady || got.Message != lifeHMSA {
		t.Fatalf("HMS before update = %+v, want ready %q", got, lifeHMSA)
	}
	if delay := c.refresh(context.Background(), lifePrefix); delay != 24*time.Hour {
		t.Fatalf("refresh delay = %v, want %v", delay, 24*time.Hour)
	}
	// A snapshot taken before the update must remain unchanged.
	if got := before.HMS(lifeAttr, lifeCode); got.Message != lifeHMSA {
		t.Fatalf("old snapshot HMS = %+v, want unchanged %q", got, lifeHMSA)
	}
	if got := c.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); got.Message != lifeHMSB {
		t.Fatalf("new snapshot HMS = %+v, want updated %q", got, lifeHMSB)
	}
	// A different model prefix cannot leak messages.
	other := c.Snapshot(otherSerial)
	if got := other.HMS(lifeAttr, lifeCode); got.Listed {
		t.Fatalf("other model HMS = %+v, want unlisted", got)
	}
	if got := other.Error(lifeScreen); !got.Listed || got.Message != lifeErrA {
		t.Fatalf("other model Error = %+v, want listed %q", got, lifeErrA)
	}
}

func TestSnapshotMissing(t *testing.T) {
	var nilCatalog *Catalog
	if got := nilCatalog.Snapshot(lifeSerial).HMS(lifeAttr, lifeCode); got.State != StateLoading {
		t.Fatalf("nil catalog state = %v, want Loading", got.State)
	}
	c := New("", "", []string{lifeSerial}, discardLogger())
	if got := c.Snapshot("AB").Error(lifeScreen); got.State != StateLoading {
		t.Fatalf("short serial state = %v, want Loading", got.State)
	}
	if got := c.Snapshot("00W00X123400001").HMS(lifeAttr, lifeCode); got.State != StateLoading {
		t.Fatalf("unknown prefix state = %v, want Loading", got.State)
	}
	if got := c.Snapshot(lifeSerial).Error(lifeScreen); got.State != StateLoading {
		t.Fatalf("undownloaded prefix state = %v, want Loading", got.State)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	requestStarted := make(chan struct{})
	handlerCanceled := make(chan struct{}, 1)
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(requestStarted) })
		<-r.Context().Done()
		select {
		case handlerCanceled <- struct{}{}:
		default:
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := New(srv.URL, "", []string{lifeSerial}, discardLogger())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-requestStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for catalog request")
	}
	cancel()
	select {
	case <-handlerCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for handler cancellation")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Run to stop")
	}
}

func TestHMSCode(t *testing.T) {
	cases := []struct {
		attr uint32
		code uint32
		want string
	}{
		{0x07007000, 0x00020004, "0700700000020004"},
		{0x07007000, 0x00040007, "0700700000040007"},
		{0x07007000, 0x00050006, "0700700000000006"},
		{0x07007000, 0xFFFF0009, "0700700000000009"},
	}
	for _, tc := range cases {
		if got := HMSCode(tc.attr, tc.code); got != tc.want {
			t.Errorf("HMSCode(%08X, %08X) = %s, want %s", tc.attr, tc.code, got, tc.want)
		}
	}
}

func TestHMSURL(t *testing.T) {
	want := "https://e.bambulab.com/index.php?e=0700700000020004&d=01S00C351100139&s=device_hms&lang=en"
	if got := HMSURL("01S00C351100139", 0x07007000, 0x00020004); got != want {
		t.Fatalf("HMSURL = %q, want %q", got, want)
	}
}
