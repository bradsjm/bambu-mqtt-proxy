package platecheck

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image/jpeg"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client limits bound remote requests and local JPEG inspection.
const (
	// requestTimeout bounds each provider call.
	requestTimeout = 15 * time.Second
	// maxResponseBytes bounds provider JSON.
	maxResponseBytes = 64 << 10
	// maxImageBytes is Clef's per-image byte limit.
	maxImageBytes = 4 << 20
	// maxImagePixels is Clef's per-image pixel limit.
	maxImagePixels = 16_000_000
	// assessableMin is a local view-quality heuristic, not a physical guarantee.
	assessableMin = 0.8
)

// Result holds only numeric answers and allowlisted usage from a valid response.
type Result struct {
	// PClear is the probability that the plate is clear.
	PClear float64 `json:"p_clear"`
	// POccupied is one minus PClear.
	POccupied float64 `json:"p_occupied"`
	// PAssessable is the probability that the image can be assessed.
	PAssessable float64 `json:"p_assessable"`
	// Model is the configured model, not arbitrary provider text.
	Model string `json:"model"`
	// InputTokens is nonnegative provider input usage.
	InputTokens int `json:"input_tokens"`
	// OutputTokens is nonnegative provider output usage.
	OutputTokens int `json:"output_tokens"`
}

// DecisionClient is the narrow connectivity and image-evaluation boundary.
type DecisionClient interface {
	// Probe checks a text-only answer without uploading an image.
	Probe(context.Context) error
	// Evaluate sends one JPEG and returns the two required probabilities.
	Evaluate(context.Context, []byte, string) (Result, error)
}

// Client sends bounded HTTPS requests without redirects or credential logging.
type Client struct {
	// settings is the construction-time effective configuration.
	settings Settings
	// keyEndpoint pins the credential to its construction-time destination.
	keyEndpoint string
	// http is replaceable by trusted package tests only.
	http *http.Client
	// log is the host-owned component logger.
	log *slog.Logger
}

// NewClient constructs a client without making connections.
func NewClient(settings Settings, log *slog.Logger) *Client {
	if log != nil {
		log = log.With("origin", "platecheck", "component", "clef_client")
	}
	return &Client{settings: settings, keyEndpoint: strings.TrimSpace(settings.Endpoint), log: log, http: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

// safeError stores a fixed category without provider text.
type safeError struct {
	// code is one fixed allowlisted failure category.
	code string
}

// Error returns the fixed user-facing sentence.
func (e *safeError) Error() string { return fixedSentence(e.code) }

// errorCategory converts failures to an allowlisted category.
func errorCategory(err error) string {
	var safe *safeError
	if errors.As(err, &safe) {
		return safe.code
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return "timeout"
	}
	return "endpoint_unreachable"
}

// fixedSentence supplies safe text for each local or provider failure.
func fixedSentence(code string) string {
	switch code {
	case "auth_rejected":
		return "Clef rejected the API key. Check the key and its permissions."
	case "endpoint_unreachable":
		return "The Clef endpoint could not be reached. Check the endpoint and try again."
	case "timeout":
		return "The Clef request timed out. Try again."
	case "rate_limited":
		return "Clef rate-limited the request. Try again later."
	case "bad_response":
		return "Clef returned an invalid response. Check the endpoint and model."
	case "model_error":
		return "Clef could not evaluate the request. Try again later."
	case "invalid_image":
		return "The camera image is not a valid JPEG. The print would continue."
	case "image_too_large":
		return "The camera image exceeds the Clef image limit. The print would continue."
	case "canceled":
		return "The plate check was canceled."
	case "unsupported_camera":
		return "This printer model has no supported camera."
	case "camera_unavailable":
		return "The camera is unavailable. The print would continue."
	case "capture_timeout":
		return "No fresh camera image arrived in time. The print would continue."
	default:
		return "Startup evidence changed or expired. The print continued."
	}
}

// question is Clef's noul question input.
type question struct {
	// Type is always noul.
	Type string `json:"type"`
	// Instructions is the fixed question.
	Instructions string `json:"instructions"`
	// Criteria describes the two possible answers.
	Criteria map[string]string `json:"criteria,omitempty"`
}

// imageInput contains exactly the JPEG sent for evaluation.
type imageInput struct {
	// ContentType fixes the uploaded media type.
	ContentType string `json:"content_type"`
	// Base64 carries the original bytes without conversion.
	Base64 string `json:"base64"`
}

// requestInput is the bounded Clef wire input.
type requestInput struct {
	// Model selects the configured model.
	Model string `json:"model"`
	// State identifies the content being evaluated.
	State string `json:"state"`
	// Questions is the fixed question map.
	Questions map[string]question `json:"questions"`
	// Images is absent from connectivity probes.
	Images []imageInput `json:"images,omitempty"`
}

// answer requires both a noul type and a present numeric value.
type answer struct {
	// Type identifies the answer schema.
	Type string `json:"type"`
	// Noul is nil for missing or null answers.
	Noul *float64 `json:"noul"`
}

// responseResult accepts only expected answers and numeric usage.
type responseResult struct {
	// Answers contains only the three known answer identifiers.
	Answers struct {
		// Clear is the plate-clear answer.
		Clear answer `json:"plate_clear"`
		// Assessable is the view-quality answer.
		Assessable answer `json:"view_assessable"`
		// Connection is the text-only probe answer.
		Connection answer `json:"connection_test"`
	} `json:"answers"`
	// Usage contains nonnegative integer counts only.
	Usage struct {
		// InputTokens is the provider's input count.
		InputTokens *int `json:"input_tokens"`
		// OutputTokens is the provider's output count.
		OutputTokens *int `json:"output_tokens"`
	} `json:"usage"`
}

// Probe checks a text-only noul answer without uploading a camera image.
func (c *Client) Probe(ctx context.Context) error {
	r, err := c.request(ctx, requestInput{Model: c.settings.Model, State: "Connection test.", Questions: map[string]question{"connection_test": {Type: "noul", Instructions: "Is this a connectivity test?"}}})
	if err != nil {
		return err
	}
	if !validAnswer(r.Answers.Connection) {
		return &safeError{code: "bad_response"}
	}
	return nil
}

// imageDimensions checks JPEG headers and Clef limits without a full decode.
func imageDimensions(data []byte) (int, int, error) {
	if len(data) > maxImageBytes {
		return 0, 0, &safeError{code: "image_too_large"}
	}
	cfg, err := jpeg.DecodeConfig(bytes.NewReader(data))
	if err != nil || cfg.Width <= 0 || cfg.Height <= 0 {
		return 0, 0, &safeError{code: "invalid_image"}
	}
	if int64(cfg.Width)*int64(cfg.Height) > maxImagePixels {
		return 0, 0, &safeError{code: "image_too_large"}
	}
	return cfg.Width, cfg.Height, nil
}

// Evaluate uploads one original JPEG and parses the two fixed numeric answers.
func (c *Client) Evaluate(ctx context.Context, data []byte, printerModel string) (Result, error) {
	if _, _, err := imageDimensions(data); err != nil {
		return Result{}, err
	}
	questions := map[string]question{
		"view_assessable": {Type: "noul", Instructions: "Is enough of the build plate visible and adequately illuminated to decide whether a previous printed part or other foreign object is present?", Criteria: map[string]string{"true": "The relevant plate surface is visible, in focus, and sufficiently lit.", "false": "The plate is dark, obscured, out of view, or too unclear to judge."}},
		"plate_clear":     {Type: "noul", Instructions: "Is the build plate clear of previous printed parts or other foreign objects?", Criteria: map[string]string{"true": "No previous printed part, loose object, or significant loose filament remains on the plate. Plate texture, reflections, printed plate markings, the nozzle, and the printer toolhead do not count as foreign objects.", "false": "A previous printed part, loose object, or significant loose filament is visible on the plate."}},
	}
	// A bounded local model label cannot grow the provider request without limit.
	if len(printerModel) > 64 {
		printerModel = printerModel[:64]
	}
	r, err := c.request(ctx, requestInput{Model: c.settings.Model, State: "Startup build-plate view for printer model " + printerModel + ".", Questions: questions, Images: []imageInput{{ContentType: "image/jpeg", Base64: base64.StdEncoding.EncodeToString(data)}}})
	if err != nil {
		return Result{}, err
	}
	if !validAnswer(r.Answers.Clear) || !validAnswer(r.Answers.Assessable) {
		return Result{}, &safeError{code: "bad_response"}
	}
	return Result{PClear: *r.Answers.Clear.Noul, POccupied: 1 - *r.Answers.Clear.Noul, PAssessable: *r.Answers.Assessable.Noul, Model: c.settings.Model, InputTokens: *r.Usage.InputTokens, OutputTokens: *r.Usage.OutputTokens}, nil
}

// validAnswer rejects missing, wrongly typed, or nonfinite probabilities.
func validAnswer(a answer) bool {
	return a.Type == "noul" && a.Noul != nil && !math.IsNaN(*a.Noul) && !math.IsInf(*a.Noul, 0) && *a.Noul >= 0 && *a.Noul <= 1
}

// request performs one bounded call and strips all untrusted failure detail.
func (c *Client) request(ctx context.Context, in requestInput) (out responseResult, err error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	started := time.Now()
	status := 0
	defer func() {
		if c.log != nil && c.log.Enabled(ctx, slog.LevelDebug) {
			code := ""
			if err != nil {
				code = errorCategory(err)
			}
			attrs := []any{"operation", operationOf(ctx), "model", c.settings.Model, "http_status", status, "duration_ms", time.Since(started).Milliseconds(), "error_code", code}
			if out.Usage.InputTokens != nil && out.Usage.OutputTokens != nil {
				attrs = append(attrs, "input_tokens", *out.Usage.InputTokens, "output_tokens", *out.Usage.OutputTokens)
			}
			c.log.DebugContext(ctx, "Clef request completed", attrs...)
		}
	}()
	// Never attach a retained bearer credential to a changed or insecure destination.
	endpoint := strings.TrimSpace(c.settings.Endpoint)
	u, parseErr := url.Parse(endpoint)
	if parseErr != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || endpoint != c.keyEndpoint {
		return out, &safeError{code: "auth_rejected"}
	}
	raw, marshalErr := json.Marshal(in)
	if marshalErr != nil {
		return out, &safeError{code: "bad_response"}
	}
	req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if reqErr != nil {
		return out, &safeError{code: "endpoint_unreachable"}
	}
	req.Header.Set("Authorization", "Bearer "+c.settings.Key())
	req.Header.Set("Content-Type", "application/json")
	resp, sendErr := c.http.Do(req)
	if sendErr != nil {
		return out, &safeError{code: errorCategory(sendErr)}
	}
	defer resp.Body.Close()
	status = resp.StatusCode
	switch {
	case status == 401 || status == 403:
		return out, &safeError{code: "auth_rejected"}
	case status == 429:
		return out, &safeError{code: "rate_limited"}
	case status >= 500:
		return out, &safeError{code: "model_error"}
	case status < 200 || status >= 300:
		return out, &safeError{code: "bad_response"}
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if readErr != nil {
		return out, &safeError{code: errorCategory(readErr)}
	}
	if len(body) > maxResponseBytes {
		return out, &safeError{code: "bad_response"}
	}
	var envelope struct {
		// Success distinguishes a Workers AI envelope from a direct result.
		Success *bool `json:"success"`
		// Result is the bounded wrapped payload, never logged directly.
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return out, &safeError{code: "bad_response"}
	}
	if envelope.Success != nil {
		if !*envelope.Success {
			return out, &safeError{code: "model_error"}
		}
		if len(envelope.Result) == 0 || bytes.Equal(envelope.Result, []byte("null")) {
			return out, &safeError{code: "bad_response"}
		}
		body = envelope.Result
	}
	if json.Unmarshal(body, &out) != nil || out.Usage.InputTokens == nil || out.Usage.OutputTokens == nil || *out.Usage.InputTokens < 0 || *out.Usage.OutputTokens < 0 {
		return responseResult{}, &safeError{code: "bad_response"}
	}
	return out, nil
}

// classify applies the same strict occupied threshold in automatic and diagnostic paths.
func classify(result Result, cutoff float64) string {
	if result.PAssessable < assessableMin {
		return "inconclusive"
	}
	n := int(math.Round(cutoff * 100))
	if result.PClear < float64(100-n)/100 {
		return "would_stop"
	}
	if result.POccupied >= 0.5 {
		return "below_threshold"
	}
	return "clear"
}
