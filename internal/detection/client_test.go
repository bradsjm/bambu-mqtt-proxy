package detection

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testKey is a fake credential used to assert the real key never leaks.
const testKey = "prod_FAKE_KEY_FOR_TESTS"

// ctxSecret is a fake context id embedded in URLs; tests assert it never
// reaches any rendered error.
const ctxSecret = "CTXSECRET01"

// routeClient builds a client whose validated vendor URLs are rewritten to
// the test server by a RoundTripper, so URL validation can be exercised
// against httptest hosts.
func routeClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	transport := &rerouteTransport{base: http.DefaultTransport, target: server.URL}
	return &Client{
		apiKey:    testKey,
		createURL: apiCreateContextURL,
		http: &http.Client{
			Timeout:   5 * time.Second,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// rerouteTransport rewrites requests to the test server while preserving
// the original URL for validation. It never mutates the original request.
type rerouteTransport struct {
	base   http.RoundTripper
	target string
}

func (r *rerouteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rewritten := req.Clone(req.Context())
	u := *req.URL
	u.Scheme, u.Host = mustSplitURL(r.target)
	rewritten.URL = &u
	resp, err := r.base.RoundTrip(rewritten)
	if resp != nil {
		resp.Request = req
	}
	return resp, err
}

func mustSplitURL(raw string) (scheme, host string) {
	i := strings.Index(raw, "://")
	return raw[:i], raw[i+3:]
}

func TestValidVendorURL(t *testing.T) {
	for _, good := range []string{
		"https://gadget-pv1-oeapi.octoeverywhere.com/api/gadget/v1/createcontext",
		"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/abc",
		"https://octoeverywhere.com/x",
		"https://a.b.c.octoeverywhere.com/deep/path",
		"https://gadget-regional.octoeverywhere.com:443/api/gadget/v1/process/abc",
		"https://GADGET-PV1-OEAPI.OCTOEVERYWHERE.COM/x",
		"https://gadget-regional.octoeverywhere.com/p?query=1",
	} {
		if err := validVendorURL(good); err != nil {
			t.Fatalf("validVendorURL(%q) = %v, want nil", good, err)
		}
	}
	for _, bad := range []string{
		"http://gadget-pv1-oeapi.octoeverywhere.com/api/gadget/v1/createcontext",
		"https://gadget-pv1-oeapi.octoeverywhere.com.evil.example/api/gadget/v1/process/x",
		"https://example.com/api/gadget/v1/process/x",
		"https://eviloctoeverywhere.com/x",
		"https://octoeverywhere.com.evil.example/x",
		"https://example.com/octoeverywhere.com",
		"https://user:pass@gadget-regional.octoeverywhere.com/api/gadget/v1/process/x",
		"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/x#frag",
		"https://gadget-regional.octoeverywhere.com:8443/api/gadget/v1/process/x",
		"https://gadget-regional.octoeverywhere.com:80/x",
		"https://[::1]/x",
		"not a url at all %zz",
		"",
	} {
		if err := validVendorURL(bad); err == nil {
			t.Fatalf("validVendorURL(%q) = nil, want error", bad)
		}
	}
}

// TestValidVendorURLErrorsSanitized asserts rejection errors never echo the
// raw URL, whose path embeds the context id.
func TestValidVendorURLErrorsSanitized(t *testing.T) {
	const path = "/api/gadget/v1/process/" + ctxSecret
	for _, bad := range []string{
		"http://gadget-regional.octoeverywhere.com" + path,
		"https://user:pass@gadget-regional.octoeverywhere.com" + path,
		"https://gadget-regional.octoeverywhere.com:8443" + path,
		"https://gadget-regional.octoeverywhere.com" + path + "#frag",
		"https://evil.example" + path,
		"https://octoeverywhere.com.evil.example" + path,
		"https://eviloctoeverywhere.com" + path,
	} {
		err := validVendorURL(bad)
		if err == nil {
			t.Fatalf("validVendorURL(%q) = nil, want error", bad)
		}
		msg := err.Error()
		if strings.Contains(msg, "://") || strings.Contains(msg, ctxSecret) ||
			strings.Contains(msg, "/api/gadget") ||
			strings.Contains(msg, "octoeverywhere.com") || strings.Contains(msg, "evil.example") {
			t.Fatalf("error leaks url context: %q", msg)
		}
	}
}

func TestCreateContextSuccess(t *testing.T) {
	var gotKey, gotBody, gotType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-API-Key")
		gotType = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ContextId":"CTX1","ProcessRequestUrl":"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/CTX1","FallbackProcessRequestUrl":"https://gadget-pv1-oeapi.octoeverywhere.com/api/gadget/v1/process/CTX1"}`))
	}))
	defer server.Close()

	client := routeClient(t, server)
	session, err := client.CreateContext(context.Background())
	if err != nil {
		t.Fatalf("CreateContext: %v", err)
	}
	if gotKey != testKey || gotBody != "{}" || !strings.HasPrefix(gotType, "application/json") {
		t.Fatalf("request headers/body = %q/%q/%q", gotKey, gotBody, gotType)
	}
	if session.ContextID != "CTX1" {
		t.Fatalf("context id = %q", session.ContextID)
	}
	if err := validVendorURL(session.ProcessURL); err != nil {
		t.Fatalf("primary url rejected: %v", err)
	}
	if err := validVendorURL(session.FallbackURL); err != nil {
		t.Fatalf("fallback url rejected: %v", err)
	}
}

// TestCreateContextValidatesURLsImmediately asserts both returned process
// URLs are validated before the session is handed out, and that rejection
// errors name the failing URL class without echoing it.
func TestCreateContextValidatesURLsImmediately(t *testing.T) {
	cases := []struct {
		name     string
		primary  string
		fallback string
		wantIn   string
	}{
		{
			name:     "primary not https",
			primary:  "http://gadget-regional.octoeverywhere.com/api/gadget/v1/process/" + ctxSecret,
			fallback: "https://gadget-pv1-oeapi.octoeverywhere.com/api/gadget/v1/process/" + ctxSecret,
			wantIn:   "primary",
		},
		{
			name:     "fallback wrong host",
			primary:  "https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/" + ctxSecret,
			fallback: "https://evil.example/api/gadget/v1/process/" + ctxSecret,
			wantIn:   "fallback",
		},
		{
			name:     "fallback userinfo",
			primary:  "https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/" + ctxSecret,
			fallback: "https://user:pass@gadget-regional.octoeverywhere.com/api/gadget/v1/process/" + ctxSecret,
			wantIn:   "fallback",
		},
		{
			name:     "fallback non-default port",
			primary:  "https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/" + ctxSecret,
			fallback: "https://gadget-regional.octoeverywhere.com:8443/api/gadget/v1/process/" + ctxSecret,
			wantIn:   "fallback",
		},
		{
			name:     "fallback fragment",
			primary:  "https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/" + ctxSecret,
			fallback: "https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/" + ctxSecret + "#x",
			wantIn:   "fallback",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"ContextId":"` + ctxSecret + `","ProcessRequestUrl":` + quoteGoString(tc.primary) + `,"FallbackProcessRequestUrl":` + quoteGoString(tc.fallback) + `}`))
			}))
			defer server.Close()
			client := routeClient(t, server)
			_, err := client.CreateContext(context.Background())
			if err == nil {
				t.Fatal("CreateContext = nil error, want url rejection")
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.wantIn) {
				t.Fatalf("error %q does not name the %s url", msg, tc.wantIn)
			}
			if strings.Contains(msg, ctxSecret) || strings.Contains(msg, "://") ||
				strings.Contains(msg, tc.primary) || strings.Contains(msg, tc.fallback) {
				t.Fatalf("error leaks url context: %q", msg)
			}
		})
	}
}

// TestCreateContextMissingFields and malformed bodies must fail with
// sanitized errors.
func TestCreateContextBadResponses(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"empty object", `{}`},
		{"missing fallback", `{"ContextId":"C","ProcessRequestUrl":"https://gadget-regional.octoeverywhere.com/p"}`},
		{"malformed json", `{"ContextId":`},
		{"wrong types", `{"ContextId":5,"ProcessRequestUrl":[],"FallbackProcessRequestUrl":{}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := routeClient(t, server)
			_, err := client.CreateContext(context.Background())
			if err == nil {
				t.Fatal("CreateContext = nil error, want response rejection")
			}
			if strings.Contains(err.Error(), tc.body) {
				t.Fatalf("error echoes response body: %q", err.Error())
			}
		})
	}
}

func TestProcessSuccess(t *testing.T) {
	var gotKey, gotFile, gotPartType string
	var gotImage []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-API-Key")
		_, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		mr := multipart.NewReader(r.Body, params["boundary"])
		form, err := mr.ReadForm(1 << 20)
		if err != nil {
			t.Errorf("read form: %v", err)
			return
		}
		files := form.File["image"]
		if len(files) != 1 {
			t.Errorf("form files = %d, want 1", len(files))
			return
		}
		gotFile, gotPartType = files[0].Filename, files[0].Header.Get("Content-Type")
		f, _ := files[0].Open()
		gotImage, _ = io.ReadAll(f)
		_, _ = w.Write([]byte(`{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"FasterInspectionSuggested":true,"PrintQuality":8,"WarningSuggested":false,"PauseSuggested":false,"Score":42}`))
	}))
	defer server.Close()

	client := routeClient(t, server)
	jpeg := []byte{0xFF, 0xD8, 0x00, 0x01, 0xFF, 0xD9}
	res, err := client.Process(context.Background(),
		"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/CTX1", jpeg)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if gotKey != testKey {
		t.Fatalf("X-API-Key = %q", gotKey)
	}
	if gotFile != "print.jpg" || gotPartType != "image/jpeg" || !bytes.Equal(gotImage, jpeg) {
		t.Fatalf("multipart part = %q/%q/%d bytes", gotFile, gotPartType, len(gotImage))
	}
	want := Result{
		Intervals:                 IntervalSec{Minimum: 5, Recommended: 20},
		FasterInspectionSuggested: true,
		PrintQuality:              8,
		WarningSuggested:          false,
		PauseSuggested:            false,
		Score:                     42,
	}
	if !reflect.DeepEqual(res, want) {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
}

// TestProcessResponseContract pins the required response contract: quality
// within 1..10, both warning and pause flags present, positive sane
// intervals, and optional Faster defaulting to false.
func TestProcessResponseContract(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr bool
		want    Result
	}{
		{
			name: "faster absent defaults false",
			body: `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":7,"WarningSuggested":false,"PauseSuggested":true}`,
			want: Result{Intervals: IntervalSec{5, 20}, PrintQuality: 7, PauseSuggested: true},
		},
		{
			name: "faster explicit false",
			body: `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"FasterInspectionSuggested":false,"PrintQuality":7,"WarningSuggested":false,"PauseSuggested":false}`,
			want: Result{Intervals: IntervalSec{5, 20}, PrintQuality: 7},
		},
		{
			name: "quality lower boundary 1",
			body: `{"NextProcessIntervalSec":{"Minimum":1,"Recommended":1},"PrintQuality":1,"WarningSuggested":true,"PauseSuggested":true}`,
			want: Result{Intervals: IntervalSec{1, 1}, PrintQuality: 1, WarningSuggested: true, PauseSuggested: true},
		},
		{
			name: "quality upper boundary 10",
			body: `{"NextProcessIntervalSec":{"Minimum":3600,"Recommended":3600},"PrintQuality":10,"WarningSuggested":false,"PauseSuggested":false,"Score":100}`,
			want: Result{Intervals: IntervalSec{3600, 3600}, PrintQuality: 10, Score: 100},
		},
		{
			name:    "quality below boundary 0",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":0,"WarningSuggested":false,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "quality above boundary 11",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":11,"WarningSuggested":false,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "quality negative",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":-1,"WarningSuggested":false,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "quality missing",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"WarningSuggested":false,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "quality wrong type",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":"8","WarningSuggested":false,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "warning missing",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":7,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "pause missing",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":7,"WarningSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "both flags missing",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":7}`,
			wantErr: true,
		},
		{
			name:    "intervals missing entirely rejected",
			body:    `{"PrintQuality":5,"WarningSuggested":false,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "intervals zero and negative rejected",
			body:    `{"NextProcessIntervalSec":{"Minimum":-3,"Recommended":0},"PrintQuality":5,"WarningSuggested":false,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name: "intervals pass through verbatim beyond any cap",
			body: `{"NextProcessIntervalSec":{"Minimum":100000,"Recommended":4000},"PrintQuality":5,"WarningSuggested":false,"PauseSuggested":false}`,
			want: Result{Intervals: IntervalSec{100000, 4000}, PrintQuality: 5},
		},
		{
			name: "intervals boundary one accepted verbatim",
			body: `{"NextProcessIntervalSec":{"Minimum":1,"Recommended":1},"PrintQuality":5,"WarningSuggested":false,"PauseSuggested":false}`,
			want: Result{Intervals: IntervalSec{1, 1}, PrintQuality: 5},
		},
		{
			name: "intervals at overflow boundary accepted",
			body: `{"NextProcessIntervalSec":{"Minimum":` + strconv.FormatInt(maxIntervalSeconds, 10) + `,"Recommended":1},"PrintQuality":5,"WarningSuggested":false,"PauseSuggested":false}`,
			want: Result{Intervals: IntervalSec{int(maxIntervalSeconds), 1}, PrintQuality: 5},
		},
		{
			name:    "intervals beyond overflow boundary rejected",
			body:    `{"NextProcessIntervalSec":{"Minimum":` + strconv.FormatInt(maxIntervalSeconds+1, 10) + `,"Recommended":1},"PrintQuality":5,"WarningSuggested":false,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "quality null treated as absent",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":null,"WarningSuggested":false,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "warning null treated as absent",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":7,"WarningSuggested":null,"PauseSuggested":false}`,
			wantErr: true,
		},
		{
			name:    "pause null treated as absent",
			body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":7,"WarningSuggested":false,"PauseSuggested":null}`,
			wantErr: true,
		},
		{
			name: "faster null defaults false",
			body: `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"FasterInspectionSuggested":null,"PrintQuality":7,"WarningSuggested":false,"PauseSuggested":false}`,
			want: Result{Intervals: IntervalSec{5, 20}, PrintQuality: 7},
		},
		{
			name: "score null defaults zero",
			body: `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":7,"WarningSuggested":false,"PauseSuggested":false,"Score":null}`,
			want: Result{Intervals: IntervalSec{5, 20}, PrintQuality: 7},
		},
		{
			name:    "malformed json",
			body:    `{"PrintQuality":`,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := routeClient(t, server)
			res, err := client.Process(context.Background(),
				"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/C", []byte{0xFF, 0xD8, 0xFF, 0xD9})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Process = %+v, nil error, want rejection", res)
				}
				if strings.Contains(err.Error(), tc.body) {
					t.Fatalf("error echoes response body: %q", err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if !reflect.DeepEqual(res, tc.want) {
				t.Fatalf("result = %+v, want %+v", res, tc.want)
			}
		})
	}
}

// TestProcessErrorNeverEchoesBody asserts a contract violation in a body
// carrying provider-looking secret text keeps that text out of the error.
func TestProcessErrorNeverEchoesBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"PrintQuality":99,"ErrorDetails":"TOPSECRETDETAIL","Debug":"TOPSECRETDEBUG"}`))
	}))
	defer server.Close()
	client := routeClient(t, server)
	_, err := client.Process(context.Background(),
		"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/"+ctxSecret, []byte{0xFF, 0xD8, 0xFF, 0xD9})
	if err == nil {
		t.Fatal("Process = nil error, want contract rejection")
	}
	msg := err.Error()
	for _, secret := range []string{"TOPSECRETDETAIL", "TOPSECRETDEBUG", ctxSecret, "://"} {
		if strings.Contains(msg, secret) {
			t.Fatalf("error leaks %q: %q", secret, msg)
		}
	}
}

func TestProcessRejectsLargeFramesWithoutRequest(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	defer server.Close()
	client := routeClient(t, server)
	_, err := client.Process(context.Background(),
		"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/C", make([]byte, maxImageBytes+1))
	if !IsFrameTooLarge(err) {
		t.Fatalf("IsFrameTooLarge = %v for %v", false, err)
	}
	if called {
		t.Fatal("server must not be called for oversized frames")
	}
}

func TestProcessRejectsNonVendorURL(t *testing.T) {
	client := NewGadgetClient(testKey)
	for _, u := range []string{
		"http://gadget-regional.octoeverywhere.com/api/gadget/v1/process/C",
		"https://evil.example/api/gadget/v1/process/C",
		"https://user:pass@gadget-regional.octoeverywhere.com/api/gadget/v1/process/C",
		"https://gadget-regional.octoeverywhere.com:8443/api/gadget/v1/process/C",
	} {
		_, err := client.Process(context.Background(), u, []byte{0xFF, 0xD8, 0xFF, 0xD9})
		if err == nil {
			t.Fatalf("Process(%q) = nil error, want rejection", u)
		}
		if strings.Contains(err.Error(), u) || strings.Contains(err.Error(), "://") {
			t.Fatalf("rejection error leaks the url: %q", err.Error())
		}
	}
}

func TestAPIErrorClassification(t *testing.T) {
	terminal := []string{
		errTypeInvalidKey, errTypeKeyDisabled, errTypePaymentFailed,
		errTypeIPRestricted, errTypeFreeUsageLimit,
	}
	for _, typ := range terminal {
		e := &APIError{Type: typ, Status: 403}
		if !e.AccountTerminal() {
			t.Fatalf("%s must be account-terminal", typ)
		}
		if e.SwitchToFallback() {
			t.Fatalf("%s must not switch to fallback", typ)
		}
	}
	rateLimited := &APIError{Type: "OE_CONTEXT_RATE_LIMITED", Status: 429}
	if rateLimited.AccountTerminal() || rateLimited.SwitchToFallback() {
		t.Fatal("context rate limit must be neither terminal nor fallback-switching")
	}
	throttled := &APIError{Type: errTypeBackendThrottle, Status: 429}
	if throttled.AccountTerminal() || throttled.SwitchToFallback() {
		t.Fatal("throttling must be neither terminal nor fallback-switching")
	}
	if !(&APIError{Type: errTypeInternal, Status: 500}).SwitchToFallback() {
		t.Fatal("internal error must switch to fallback")
	}
	if !(&APIError{Type: errTypeUnknown, Status: 503}).SwitchToFallback() {
		t.Fatal("unknown 5xx must switch to fallback")
	}
	for _, e := range []*APIError{
		{Type: errTypeUnknown, Status: 400},
		{Type: "OE_BAD_ARGS", Status: 400},
		{Type: "OE_ARGS_PARSE_FAILED", Status: 400},
		{Type: "OE_IMAGE_DECODE_FAILED", Status: 422},
	} {
		if e.SwitchToFallback() {
			t.Fatalf("%s %d must not switch to fallback", e.Type, e.Status)
		}
	}
}

// TestProcessErrorMapping asserts non-2XX bodies become sanitized APIErrors:
// the provider ErrorDetails text and the API key never reach the message,
// and hostile ErrorType tokens collapse to the generic unknown type.
func TestProcessErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantType string
	}{
		{
			name:     "clean token keeps details out",
			body:     `{"ErrorType":"OE_API_KEY_IP_RESTRICTED","ErrorDetails":"ip claimed"}`,
			wantType: "OE_API_KEY_IP_RESTRICTED",
		},
		{
			name:     "hostile token collapses to unknown",
			body:     `{"ErrorType":"oe weird <script>alert(1)</script>","ErrorDetails":"TOPSECRET"}`,
			wantType: errTypeUnknown,
		},
		{
			name:     "oversized token collapses to unknown",
			body:     `{"ErrorType":"` + strings.Repeat("OE_LONG_", 20) + `"}`,
			wantType: errTypeUnknown,
		},
		{
			name:     "unknown well-formed token collapses to unknown",
			body:     `{"ErrorType":"OE_NOT_IN_ALLOWLIST_123"}`,
			wantType: errTypeUnknown,
		},
		{
			name:     "non-json body stays unknown",
			body:     `<html>TOPSECRET</html>`,
			wantType: errTypeUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := routeClient(t, server)
			_, err := client.Process(context.Background(),
				"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/C", []byte{0xFF, 0xD8, 0xFF, 0xD9})
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Type != tc.wantType || apiErr.Status != http.StatusForbidden {
				t.Fatalf("err = %v, want APIError type %q", err, tc.wantType)
			}
			msg := err.Error()
			for _, secret := range []string{testKey, "TOPSECRET", "ip claimed", "script", "://"} {
				if strings.Contains(msg, secret) {
					t.Fatalf("error message leaks %q: %q", secret, msg)
				}
			}
			if tc.wantType == "OE_API_KEY_IP_RESTRICTED" && !apiErr.AccountTerminal() {
				t.Fatal("IP restriction must be terminal")
			}
		})
	}
}

func TestProcessDoesNotFollowRedirects(t *testing.T) {
	followed := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		followed = true
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/elsewhere", http.StatusFound)
	}))
	defer server.Close()
	client := routeClient(t, server)
	_, err := client.Process(context.Background(),
		"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/C", []byte{0xFF, 0xD8, 0xFF, 0xD9})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusFound {
		t.Fatalf("err = %v, want 302 treated as non-2XX without following", err)
	}
	if followed {
		t.Fatal("redirect target must not be requested")
	}
}

func TestResponseSizeBound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("a"), maxResponseBytes+1))
	}))
	defer server.Close()
	client := routeClient(t, server)
	_, err := client.Process(context.Background(),
		"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/C", []byte{0xFF, 0xD8, 0xFF, 0xD9})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v, want response size error", err)
	}
}

// TestTransportErrorSanitized asserts connection failures never render the
// request URL (the context path) even though the stdlib wraps them in a
// url.Error carrying the full URL.
func TestTransportErrorSanitized(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	transport := &rerouteTransport{base: http.DefaultTransport, target: deadURL}
	client := &Client{
		apiKey:    testKey,
		createURL: apiCreateContextURL,
		http: &http.Client{
			Timeout:   5 * time.Second,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	_, err := client.Process(context.Background(),
		"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/"+ctxSecret, []byte{0xFF, 0xD8, 0xFF, 0xD9})
	if err == nil {
		t.Fatal("Process = nil error, want transport failure")
	}
	msg := err.Error()
	switch msg {
	case "gadget request failed", "gadget request timed out", "gadget request canceled":
	default:
		t.Fatalf("transport error = %q, want a generic class message", msg)
	}
	for _, leak := range []string{ctxSecret, "/api/gadget", "://", "octoeverywhere", "refused", "reset", "EOF"} {
		if strings.Contains(msg, leak) {
			t.Fatalf("transport error leaks %q: %q", leak, msg)
		}
	}
	// The same sanitization applies to context creation.
	_, err = client.CreateContext(context.Background())
	if err == nil {
		t.Fatal("CreateContext = nil error, want transport failure")
	}
	if m := err.Error(); m != "gadget request failed" && m != "gadget request timed out" && m != "gadget request canceled" {
		t.Fatalf("create transport error = %q, want a generic class message", m)
	}
}

// TestProcessNullFieldsAreAbsent pins the null-as-absent rule: a JSON null
// in a required field fails with the same safe error as an absent field,
// while a null in an optional field keeps its missing-value default.
func TestProcessNullFieldsAreAbsent(t *testing.T) {
	run := func(t *testing.T, body string) (Result, error) {
		t.Helper()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(body))
		}))
		defer server.Close()
		client := routeClient(t, server)
		return client.Process(context.Background(),
			"https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/C", []byte{0xFF, 0xD8, 0xFF, 0xD9})
	}

	t.Run("required nulls fail with missing-field errors", func(t *testing.T) {
		cases := []struct {
			name    string
			body    string
			wantErr string
		}{
			{
				name:    "quality null",
				body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":null,"WarningSuggested":false,"PauseSuggested":false}`,
				wantErr: "gadget process response: missing required PrintQuality",
			},
			{
				name:    "warning null",
				body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":7,"WarningSuggested":null,"PauseSuggested":false}`,
				wantErr: "gadget process response: missing required WarningSuggested or PauseSuggested",
			},
			{
				name:    "pause null",
				body:    `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"PrintQuality":7,"WarningSuggested":false,"PauseSuggested":null}`,
				wantErr: "gadget process response: missing required WarningSuggested or PauseSuggested",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				res, err := run(t, tc.body)
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				// Valid timing intervals still ride along with the partial
				// result so the engine can retain the provider pacing.
				if res.Intervals != (IntervalSec{Minimum: 5, Recommended: 20}) {
					t.Fatalf("intervals = %+v, want the parsed guidance", res.Intervals)
				}
			})
		}
	})

	t.Run("optional nulls keep defaults", func(t *testing.T) {
		res, err := run(t, `{"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"FasterInspectionSuggested":null,"PrintQuality":7,"WarningSuggested":false,"PauseSuggested":false,"Score":null}`)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		want := Result{Intervals: IntervalSec{Minimum: 5, Recommended: 20}, PrintQuality: 7}
		if !reflect.DeepEqual(res, want) {
			t.Fatalf("result = %+v, want %+v", res, want)
		}
	})
}

// debugLogClient builds a routed client whose debug logs land in a buffer.
func debugLogClient(t *testing.T, handler http.HandlerFunc) (*Client, *bytes.Buffer) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := routeClient(t, server)
	var logs bytes.Buffer
	client.setLogger(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return client, &logs
}

// assertNoProviderText fails when the debug log carries any URL context,
// provider body text, provider-controlled header text, or raw log keys the
// sanitized format removed.
func assertNoProviderText(t *testing.T, logs *bytes.Buffer) {
	t.Helper()
	log := logs.String()
	for _, leak := range []string{ctxSecret, "://", "/api/gadget"} {
		if strings.Contains(log, leak) {
			t.Fatalf("debug log leaks url context %q: %s", leak, log)
		}
	}
	for _, key := range []string{"request_url=", "http_status_line=", "response_content_type=", "response_body=", "response_body_base64=", "read_error=", "close_error=", "transport_error="} {
		if strings.Contains(log, key) {
			t.Fatalf("debug log contains removed key %q: %s", key, log)
		}
	}
}

// TestGadgetDebugLogsSanitized asserts debug logs for successes, provider
// error bodies, read failures, transport failures, and canceled requests
// keep marker secrets, URLs, bodies, and provider-controlled text out while
// numeric status and duration metadata remain.
func TestGadgetDebugLogsSanitized(t *testing.T) {
	const bodySecret = "TOPSECRETRESPONSE"
	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xD9}
	processURL := "https://gadget-regional.octoeverywhere.com/api/gadget/v1/process/" + ctxSecret

	t.Run("success keeps numeric metadata only", func(t *testing.T) {
		client, logs := debugLogClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/evil")
			_, _ = w.Write([]byte(`{"PrintQuality":8,"WarningSuggested":false,"PauseSuggested":false,` +
				`"NextProcessIntervalSec":{"Minimum":5,"Recommended":20},"note":"` + bodySecret + `"}`))
		})
		if _, err := client.Process(context.Background(), processURL, jpeg); err != nil {
			t.Fatalf("Process: %v", err)
		}
		assertNoProviderText(t, logs)
		if strings.Contains(logs.String(), bodySecret) {
			t.Fatalf("debug log contains the response body: %s", logs.String())
		}
		log := logs.String()
		for _, field := range []string{
			"request_host=gadget-regional.octoeverywhere.com",
			"http_status=200", "duration_ms=", "response_body_bytes=", "response_content_length=",
		} {
			if !strings.Contains(log, field) {
				t.Errorf("debug log missing %q: %s", field, log)
			}
		}
	})

	t.Run("read failure keeps a fixed class", func(t *testing.T) {
		client, logs := debugLogClient(t, func(w http.ResponseWriter, _ *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("response does not support hijacking")
				return
			}
			conn, buf, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort")
			buf.Flush()
			conn.Close()
		})
		_, err := client.Process(context.Background(), processURL, jpeg)
		if err == nil || !isTransportError(err) {
			t.Fatalf("err = %v, want a transport read failure", err)
		}
		assertNoProviderText(t, logs)
		log := logs.String()
		if !strings.Contains(log, `read_error_class="gadget response read failed"`) {
			t.Errorf("debug log missing the fixed read class: %s", log)
		}
		for _, raw := range []string{"unexpected EOF", "EOF", "reset"} {
			if strings.Contains(log, raw) {
				t.Errorf("debug log exposes raw read error text %q: %s", raw, log)
			}
		}
	})

	t.Run("transport failure keeps a fixed class", func(t *testing.T) {
		dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		deadURL := dead.URL
		dead.Close()
		client := routeClient(t, dead)
		// Rebuild the client against the dead server URL and attach debug logs.
		client.http = &http.Client{
			Timeout:   5 * time.Second,
			Transport: &rerouteTransport{base: http.DefaultTransport, target: deadURL},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		var logs bytes.Buffer
		client.setLogger(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		_, err := client.Process(context.Background(), processURL, jpeg)
		if err == nil {
			t.Fatal("Process = nil error, want transport failure")
		}
		assertNoProviderText(t, &logs)
		log := logs.String()
		if !strings.Contains(log, `transport_class="gadget request failed"`) {
			t.Errorf("debug log missing the fixed transport class: %s", log)
		}
		for _, raw := range []string{"refused", "reset", "EOF", "connect:"} {
			if strings.Contains(log, raw) {
				t.Errorf("debug log exposes raw transport error text %q: %s", raw, log)
			}
		}
	})

	t.Run("canceled request keeps a fixed class", func(t *testing.T) {
		client, logs := debugLogClient(t, func(http.ResponseWriter, *http.Request) {
			t.Error("server must not observe a canceled request")
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := client.Process(ctx, processURL, jpeg)
		if err == nil || !isTransportError(err) {
			t.Fatalf("err = %v, want a canceled transport error", err)
		}
		assertNoProviderText(t, logs)
		if !strings.Contains(logs.String(), `transport_class="gadget request canceled"`) {
			t.Errorf("debug log missing the canceled class: %s", logs.String())
		}
	})
}

// quoteGoString renders s as a JSON string literal; test inputs are fixed
// constants so no escaping surprises arise.
func quoteGoString(s string) string {
	var b bytes.Buffer
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
