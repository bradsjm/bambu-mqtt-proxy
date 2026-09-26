// Gadget API client for the optional OctoEverywhere AI print failure
// detection: create one context per print session and upload camera frames.
// Endpoints, error taxonomy, and limits follow the official developer docs
// (docs.octoeverywhere.com, Gadget AI Failure Detection Developer Docs).
//
// Security posture: the API key travels only in the X-API-Key header; every
// request URL is validated to HTTPS on the vendor domain; redirects are
// never followed; and every error rendered here is sanitized — no raw URLs,
// no provider ErrorDetails, and no response body fragments — so context ids
// or provider-side text can never reach logs or public status payloads.
package detection

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"
)

// Gadget API endpoints and hard limits. These are vendor guarantees and
// conservative local caps, not configuration knobs.
const (
	// apiCreateContextURL is the primary create-context endpoint. Context
	// creation is free and always targets the primary host.
	apiCreateContextURL = "https://gadget-pv1-oeapi.octoeverywhere.com/api/gadget/v1/createcontext"
	// vendorHostSuffix restricts every request URL to the vendor domain.
	vendorHostSuffix = ".octoeverywhere.com"
	// bareVendorHost is the vendor apex domain, also accepted as a host.
	bareVendorHost = "octoeverywhere.com"
	// maxImageBytes is the Process API image cap: 6 MiB excluding multipart
	// overhead. Larger frames are never sent.
	maxImageBytes = 6 << 20
	// maxResponseBytes bounds every response body read.
	maxResponseBytes = 64 << 10
	// requestTimeout bounds one HTTP request.
	requestTimeout = 15 * time.Second
	// maxIntervalSeconds is the largest second count that converts to a
	// time.Duration without overflow (math.MaxInt64 / int64(time.Second)).
	// The divisor must be an integer-kind literal: a float-kind literal
	// such as 1e9 turns the division into floating-point and yields a
	// non-integer untyped float constant.
	maxIntervalSeconds = math.MaxInt64 / 1000000000
)

// Session is one Gadget analysis context for a single print. Process URLs
// are validated at creation time and already embed the context.
type Session struct {
	ContextID   string
	ProcessURL  string
	FallbackURL string
}

// IntervalSec carries the server's next-request timing guidance.
type IntervalSec struct {
	Minimum     int `json:"Minimum"`
	Recommended int `json:"Recommended"`
}

// Result is one successful Process API response.
type Result struct {
	Intervals                 IntervalSec `json:"NextProcessIntervalSec"`
	FasterInspectionSuggested bool        `json:"FasterInspectionSuggested"`
	PrintQuality              int         `json:"PrintQuality"`
	WarningSuggested          bool        `json:"WarningSuggested"`
	PauseSuggested            bool        `json:"PauseSuggested"`
	Score                     int         `json:"Score"`
}

// APIError is the common Gadget error body returned on non-2XX responses.
// It intentionally carries only the status code and the classified error
// type token: the provider's ErrorDetails text is never parsed or stored,
// because it may contain provider-side context that must not surface in
// logs or status payloads.
type APIError struct {
	Type   string
	Status int
}

// Error renders the status and the sanitized error type. It never contains
// credentials, URLs, context ids, or provider detail text.
func (e *APIError) Error() string {
	return fmt.Sprintf("gadget api error: status %d: %s", e.Status, e.Type)
}

// Terminal account-wide errors suspend detection until restart (official
// docs: fix the key or the account; retrying cannot help).
const (
	errTypeInvalidKey      = "OE_INVALID_API_KEY"
	errTypeKeyDisabled     = "OE_API_KEY_DISABLED"
	errTypePaymentFailed   = "OE_API_KEY_BLOCKED_PAYMENT_FAILED"
	errTypeIPRestricted    = "OE_API_KEY_IP_RESTRICTED"
	errTypeFreeUsageLimit  = "OE_FREE_USAGE_LIMIT_REACHED"
	errTypeInternal        = "OE_INTERNAL_ERROR"
	errTypeBackendThrottle = "OE_BACKEND_THROTTLED"
	errTypeUnknown         = "OE_UNKNOWN"
	errTypeContextRate     = "OE_CONTEXT_RATE_LIMITED"
	errTypeImageDecode     = "OE_IMAGE_DECODE_FAILED"
	errTypeBadArgs         = "OE_BAD_ARGS"
	errTypeArgsParse       = "OE_ARGS_PARSE_FAILED"
)

// knownErrTypes is the fixed allowlist of provider error-type tokens from
// the official docs. Any other token — unknown, misspelled, or crafted —
// maps to errTypeUnknown, so attacker-controlled strings can never reach
// logs or public status payloads.
var knownErrTypes = map[string]bool{
	errTypeInvalidKey:      true,
	errTypeKeyDisabled:     true,
	errTypePaymentFailed:   true,
	errTypeIPRestricted:    true,
	errTypeFreeUsageLimit:  true,
	errTypeInternal:        true,
	errTypeBackendThrottle: true,
	errTypeContextRate:     true,
	errTypeImageDecode:     true,
	errTypeBadArgs:         true,
	errTypeArgsParse:       true,
}

// AccountTerminal reports whether the error suspends all detection
// account-wide until process restart.
func (e *APIError) AccountTerminal() bool {
	switch e.Type {
	case errTypeInvalidKey, errTypeKeyDisabled, errTypePaymentFailed,
		errTypeIPRestricted, errTypeFreeUsageLimit:
		return true
	}
	return false
}

// SwitchToFallback reports whether this error class authorizes switching to
// the context's fallback process URL: server-unavailable errors only.
// Throttling and rate limiting (retry the same URL later), invalid
// requests, account problems, IP restrictions, and usage limits are never
// fixed by switching URLs.
func (e *APIError) SwitchToFallback() bool {
	switch e.Type {
	case errTypeInternal:
		return true
	case errTypeUnknown:
		return e.Status >= 500
	}
	return false
}

// frameTooLargeError marks an image rejected locally before any request.
type frameTooLargeError struct{ size int }

func (e *frameTooLargeError) Error() string {
	return fmt.Sprintf("frame %d bytes exceeds the %d byte process limit", e.size, maxImageBytes)
}

// IsFrameTooLarge reports whether the error was a locally rejected frame.
func IsFrameTooLarge(err error) bool {
	var ftl *frameTooLargeError
	return errors.As(err, &ftl)
}

// Client calls the Gadget API with the account key. It is safe for
// concurrent use; the detection engine serializes calls per printer anyway.
type Client struct {
	apiKey    string
	createURL string
	http      *http.Client
}

// NewGadgetClient builds the client. The key only ever travels in the
// X-API-Key header: it is never logged, never rendered in errors, and never
// exposed by any status payload.
func NewGadgetClient(apiKey string) *Client {
	return &Client{
		apiKey:    apiKey,
		createURL: apiCreateContextURL,
		http:      newHTTPClient(),
	}
}

// newHTTPClient returns a client that never follows redirects and bounds
// every request with one overall timeout.
func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// validVendorURL enforces the HTTPS vendor host on every URL the API or the
// configuration hands us: HTTPS scheme, vendor domain, no userinfo, no
// fragment, and no non-default port. Errors are sanitized: they never echo
// the raw URL, whose path embeds the context id.
func validVendorURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return errors.New("gadget url rejected: cannot parse url or host missing")
	}
	if u.Scheme != "https" {
		return errors.New("gadget url rejected: scheme is not https")
	}
	if u.User != nil {
		return errors.New("gadget url rejected: userinfo is forbidden")
	}
	if u.Fragment != "" {
		return errors.New("gadget url rejected: fragment is forbidden")
	}
	if p := u.Port(); p != "" && p != "443" {
		return errors.New("gadget url rejected: non-default port is forbidden")
	}
	host := strings.ToLower(u.Hostname())
	if host != bareVendorHost && !strings.HasSuffix(host, vendorHostSuffix) {
		return errors.New("gadget url rejected: host is not a vendor domain")
	}
	return nil
}

// decodeJSON unmarshals a response body into v. Parse failures are
// summarized without echoing any body fragment, because provider responses
// may contain context or text that must not surface in errors.
func decodeJSON(kind string, body []byte, v any) error {
	if err := json.Unmarshal(body, v); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return fmt.Errorf("gadget %s: unexpected json value types", kind)
		}
		return fmt.Errorf("gadget %s: malformed json response", kind)
	}
	return nil
}

// CreateContext starts one print context. The response carries the context
// id plus two complete process URLs; both URLs are validated immediately
// against the vendor URL policy before the session is returned.
func (c *Client) CreateContext(ctx context.Context) (Session, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.createURL, bytes.NewReader([]byte("{}")))
	if err != nil {
		return Session{}, fmt.Errorf("gadget create context request: %w", err)
	}
	req.Header.Set("X-API-Key", c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	body, err := c.do(req)
	if err != nil {
		return Session{}, err
	}
	var out struct {
		ContextID                 string `json:"ContextId"`
		ProcessRequestURL         string `json:"ProcessRequestUrl"`
		FallbackProcessRequestURL string `json:"FallbackProcessRequestUrl"`
	}
	if err := decodeJSON("create context response", body, &out); err != nil {
		return Session{}, err
	}
	if out.ContextID == "" || out.ProcessRequestURL == "" || out.FallbackProcessRequestURL == "" {
		return Session{}, errors.New("gadget create context response missing fields")
	}
	if err := validVendorURL(out.ProcessRequestURL); err != nil {
		return Session{}, fmt.Errorf("gadget create context: primary process url rejected: %w", err)
	}
	if err := validVendorURL(out.FallbackProcessRequestURL); err != nil {
		return Session{}, fmt.Errorf("gadget create context: fallback process url rejected: %w", err)
	}
	return Session{
		ContextID:   out.ContextID,
		ProcessURL:  out.ProcessRequestURL,
		FallbackURL: out.FallbackProcessRequestURL,
	}, nil
}

// Process uploads one JPEG frame to url (the context's primary or fallback
// process URL) and parses the analysis result. Frames above the vendor's
// 6 MiB image cap are rejected locally without a request. The response must
// satisfy the required contract: PrintQuality within 1..10, both
// WarningSuggested and PauseSuggested present, and both NextProcessIntervalSec
// values present and positive. Valid intervals pass through verbatim — they
// are never floored or capped, so the vendor keeps full control of the
// inspection cadence. FasterInspectionSuggested is optional and defaults to
// false.
func (c *Client) Process(ctx context.Context, url string, jpeg []byte) (Result, error) {
	if len(jpeg) > maxImageBytes {
		return Result{}, &frameTooLargeError{size: len(jpeg)}
	}
	if err := validVendorURL(url); err != nil {
		return Result{}, err
	}
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", `form-data; name="image"; filename="print.jpg"`)
	h.Set("Content-Type", "image/jpeg")
	part, err := mw.CreatePart(h)
	if err != nil {
		return Result{}, fmt.Errorf("gadget process form: %w", err)
	}
	if _, err := part.Write(jpeg); err != nil {
		return Result{}, fmt.Errorf("gadget process form: %w", err)
	}
	if err := mw.Close(); err != nil {
		return Result{}, fmt.Errorf("gadget process form: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return Result{}, fmt.Errorf("gadget process request: %w", err)
	}
	req.Header.Set("X-API-Key", c.apiKey)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	raw, err := c.do(req)
	if err != nil {
		return Result{}, err
	}
	var parsed struct {
		Intervals struct {
			Minimum     *int `json:"Minimum"`
			Recommended *int `json:"Recommended"`
		} `json:"NextProcessIntervalSec"`
		FasterInspection *bool `json:"FasterInspectionSuggested"`
		PrintQuality     *int  `json:"PrintQuality"`
		WarningSuggested *bool `json:"WarningSuggested"`
		PauseSuggested   *bool `json:"PauseSuggested"`
		Score            *int  `json:"Score"`
	}
	if err := decodeJSON("process response", raw, &parsed); err != nil {
		return Result{}, err
	}
	if parsed.PrintQuality == nil {
		return Result{}, errors.New("gadget process response: missing required PrintQuality")
	}
	if *parsed.PrintQuality < 1 || *parsed.PrintQuality > 10 {
		return Result{}, errors.New("gadget process response: PrintQuality outside 1..10")
	}
	if parsed.WarningSuggested == nil || parsed.PauseSuggested == nil {
		return Result{}, errors.New("gadget process response: missing required WarningSuggested or PauseSuggested")
	}
	if parsed.Intervals.Minimum == nil || parsed.Intervals.Recommended == nil ||
		*parsed.Intervals.Minimum < 1 || *parsed.Intervals.Recommended < 1 ||
		*parsed.Intervals.Minimum > maxIntervalSeconds || *parsed.Intervals.Recommended > maxIntervalSeconds {
		return Result{}, errors.New("gadget process response: NextProcessIntervalSec Minimum and Recommended must be present, positive, and within duration range")
	}
	res := Result{
		Intervals: IntervalSec{
			Minimum:     *parsed.Intervals.Minimum,
			Recommended: *parsed.Intervals.Recommended,
		},
		PrintQuality:              *parsed.PrintQuality,
		WarningSuggested:          *parsed.WarningSuggested,
		PauseSuggested:            *parsed.PauseSuggested,
		FasterInspectionSuggested: parsed.FasterInspection != nil && *parsed.FasterInspection,
	}
	if parsed.Score != nil {
		res.Score = *parsed.Score
	}
	return res, nil
}

// do sends one authenticated request and returns the bounded response body.
// Non-2XX responses become *APIError; redirects are never followed.
// Transport failures become sanitized generic class errors: neither the
// request URL and host nor the underlying cause text is ever rendered,
// because both may embed context or arbitrary provider-side strings.
func (c *Client) do(req *http.Request) ([]byte, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			return nil, errors.New("gadget request canceled")
		case errors.Is(err, context.DeadlineExceeded):
			return nil, errors.New("gadget request timed out")
		}
		return nil, errors.New("gadget request failed")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, errors.New("gadget response read failed")
	}
	if len(body) > maxResponseBytes {
		return nil, fmt.Errorf("gadget response exceeds %d bytes", maxResponseBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		apiErr := &APIError{Type: errTypeUnknown, Status: resp.StatusCode}
		var parsed struct {
			ErrorType string `json:"ErrorType"`
		}
		if json.Unmarshal(body, &parsed) == nil {
			if knownErrTypes[parsed.ErrorType] {
				apiErr.Type = parsed.ErrorType
			}
		}
		return nil, apiErr
	}
	return body, nil
}
