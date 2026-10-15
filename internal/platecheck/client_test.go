package platecheck

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testJPEG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 8, 6))
	img.Set(1, 1, color.White)
	var b bytes.Buffer
	if err := jpeg.Encode(&b, img, nil); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func sizedJPEG(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	var b bytes.Buffer
	if err := jpeg.Encode(&b, img, nil); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func resultJSON(clear, assessable float64) string {
	b, _ := json.Marshal(map[string]any{"model": "clef", "answers": map[string]any{"plate_clear": map[string]any{"type": "noul", "noul": clear}, "view_assessable": map[string]any{"type": "noul", "noul": assessable}}, "usage": map[string]int{"input_tokens": 11, "output_tokens": 3}})
	return string(b)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func fakeHTTP(status int, body string) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
}

func TestClientTLSWireAndProbeNoImage(t *testing.T) {
	jpegBytes := testJPEG(t)
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer private-key" || r.Header.Get("Content-Type") != "application/json" || r.Method != "POST" {
			t.Error("request headers")
		}
		var in requestInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
		}
		if in.Model != "clef" || in.State == "" {
			t.Error("request model/state")
		}
		if calls == 1 {
			if len(in.Images) != 0 || len(in.Questions) != 1 || in.Questions["connection_test"].Instructions != "Is this a connectivity test?" {
				t.Error("probe uploaded image or wrong question")
			}
			_, _ = io.WriteString(w, `{"answers":{"connection_test":{"type":"noul","noul":0.9}},"usage":{"input_tokens":1,"output_tokens":1}}`)
		} else {
			if len(in.Images) != 1 || in.Images[0].ContentType != "image/jpeg" || len(in.Questions) != 2 {
				t.Error("evaluation shape")
			}
			decoded, err := base64.StdEncoding.DecodeString(in.Images[0].Base64)
			if err != nil || !bytes.Equal(decoded, jpegBytes) {
				t.Error("JPEG changed")
			}
			if in.Questions["plate_clear"].Type != "noul" || !strings.Contains(in.Questions["plate_clear"].Criteria["true"], "toolhead") || !strings.Contains(in.State, "P1S") {
				t.Error("fixed policy question")
			}
			_, _ = io.WriteString(w, `{"success":true,"result":`+resultJSON(.127, .962)+`}`)
		}
	}))
	defer server.Close()
	s := testSettings()
	s.Endpoint = server.URL
	c := NewClient(s, nil)
	c.http.Transport = server.Client().Transport
	if err := c.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, err := c.Evaluate(context.Background(), jpegBytes, "P1S")
	if err != nil || r.PClear != .127 || r.POccupied != 1-.127 || r.PAssessable != .962 || r.Model != "clef" || r.InputTokens != 11 {
		t.Fatalf("result %+v %v", r, err)
	}
}

func TestClientResponseFailuresAreSafe(t *testing.T) {
	jpegBytes := testJPEG(t)
	cases := []struct {
		name       string
		status     int
		body, code string
	}{
		{"direct", 200, resultJSON(.1, .9), ""},
		{"wrapped", 200, `{"success":true,"result":` + resultJSON(.1, .9) + `}`, ""},
		{"missing", 200, `{"answers":{}}`, "bad_response"},
		{"missing clear", 200, `{"answers":{"plate_clear":{"type":"noul"},"view_assessable":{"type":"noul","noul":0.9}}}`, "bad_response"},
		{"null", 200, `{"answers":{"plate_clear":{"type":"noul","noul":null}}}`, "bad_response"},
		{"bool", 200, `{"answers":{"plate_clear":{"type":"noul","noul":false}}}`, "bad_response"},
		{"wrong type", 200, strings.Replace(resultJSON(.1, .9), `"noul"`, `"choice"`, 1), "bad_response"},
		{"negative", 200, resultJSON(-.1, .9), "bad_response"},
		{"above", 200, resultJSON(.1, 1.1), "bad_response"},
		{"unsuccessful", 200, `{"success":false,"errors":["private-key marker\nforged"]}`, "model_error"},
		{"malformed", 200, `<html>private-key marker</html>`, "bad_response"},
		{"trailing", 200, resultJSON(.1, .9) + ` {}`, "bad_response"},
		{"null envelope", 200, `{"success":true,"result":null}`, "bad_response"},
		{"wrong success", 200, `{"success":"true","result":{}}`, "bad_response"},
		{"negative tokens", 200, strings.Replace(resultJSON(.1, .9), `"input_tokens":11`, `"input_tokens":-1`, 1), "bad_response"},
		{"fraction tokens", 200, strings.Replace(resultJSON(.1, .9), `"input_tokens":11`, `"input_tokens":1.1`, 1), "bad_response"},
		{"large", 200, strings.Repeat("x", maxResponseBytes+1), "bad_response"},
		{"401", 401, "marker private-key", "auth_rejected"},
		{"403", 403, "marker private-key", "auth_rejected"},
		{"429", 429, "marker private-key", "rate_limited"},
		{"500", 500, "marker private-key", "model_error"},
		{"redirect", 302, "marker private-key", "bad_response"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(testSettings(), nil)
			c.http = fakeHTTP(tc.status, tc.body)
			r, err := c.Evaluate(context.Background(), jpegBytes, "P1S")
			if tc.code == "" {
				if err != nil || r.PClear != .1 {
					t.Fatalf("%+v %v", r, err)
				}
				return
			}
			if err == nil || errorCategory(err) != tc.code || strings.Contains(err.Error(), "marker") || strings.Contains(err.Error(), "private-key") {
				t.Fatalf("unsafe/wrong error %v", err)
			}
		})
	}
}

func TestClientRejectsRedirectWithoutSecondRequest(t *testing.T) {
	secondCalls := 0
	second := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls++
		_, _ = io.WriteString(w, resultJSON(.1, .9))
	}))
	defer second.Close()
	first := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, second.URL, http.StatusTemporaryRedirect)
	}))
	defer first.Close()
	s := testSettings()
	s.Endpoint = first.URL
	c := NewClient(s, nil)
	c.http.Transport = first.Client().Transport
	_, err := c.Evaluate(context.Background(), testJPEG(t), "P1S")
	if err == nil || errorCategory(err) != "bad_response" || secondCalls != 0 {
		t.Fatalf("redirect %v calls %d", err, secondCalls)
	}
}

func TestClientImageAndContextBounds(t *testing.T) {
	c := NewClient(testSettings(), nil)
	calls := 0
	c.http = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) { calls++; return nil, r.Context().Err() })}
	for _, data := range [][]byte{nil, []byte("not jpeg"), make([]byte, maxImageBytes+1)} {
		if _, err := c.Evaluate(context.Background(), data, "P1S"); err == nil {
			t.Fatal("accepted invalid image")
		}
	}
	data := append([]byte(nil), testJPEG(t)...)
	for i := range len(data) - 8 {
		if data[i] == 0xff && (data[i+1] == 0xc0 || data[i+1] == 0xc2) {
			binary.BigEndian.PutUint16(data[i+5:i+7], 5000)
			binary.BigEndian.PutUint16(data[i+7:i+9], 5000)
			break
		}
	}
	if _, err := c.Evaluate(context.Background(), data, "P1S"); err == nil || errorCategory(err) != "image_too_large" {
		t.Fatalf("pixel bound %v", err)
	}
	if calls != 0 {
		t.Fatal("invalid image uploaded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Evaluate(ctx, testJPEG(t), "P1S"); err == nil || errorCategory(err) != "canceled" {
		t.Fatalf("cancel %v", err)
	}
	ctx, cancel = context.WithDeadline(context.Background(), timeInPast())
	defer cancel()
	if _, err := c.Evaluate(ctx, testJPEG(t), "P1S"); err == nil || errorCategory(err) != "timeout" {
		t.Fatalf("timeout %v", err)
	}
}

func TestClientLogsOnlyAllowedMetadata(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := testSettings()
	s.Endpoint = "https://secret.example/accounts/endpoint-marker"
	s.APIKey = "credential-marker"
	c := NewClient(s, log)
	c.http = fakeHTTP(401, "hostile-marker\nforged")
	data := testJPEG(t)
	_, _ = c.Evaluate(context.WithValue(context.Background(), operationKey{}, uint64(7)), data, "P1S")
	for _, marker := range []string{"secret.example", "endpoint-marker", "credential-marker", "hostile-marker", base64.StdEncoding.EncodeToString(data)} {
		if strings.Contains(logs.String(), marker) {
			t.Fatalf("leaked %s", marker)
		}
	}
	if !strings.Contains(logs.String(), "http_status=401") || !strings.Contains(logs.String(), "operation=7") || !strings.Contains(logs.String(), "auth_rejected") {
		t.Fatal(logs.String())
	}
}

func TestClientMissingUsageAndProbeAnswerTypes(t *testing.T) {
	for _, raw := range []string{
		`{"answers":{"connection_test":{"type":"noul","noul":0.9}}}`,
		`{"answers":{"connection_test":{"type":"noul","noul":0.9}},"usage":{"input_tokens":null,"output_tokens":1}}`,
		`{"answers":{"connection_test":{"type":"noul","noul":true}},"usage":{"input_tokens":1,"output_tokens":1}}`,
		`{"answers":{"connection_test":{"type":"noul"}},"usage":{"input_tokens":1,"output_tokens":1}}`,
	} {
		c := NewClient(testSettings(), nil)
		c.http = fakeHTTP(200, raw)
		if err := c.Probe(context.Background()); err == nil || errorCategory(err) != "bad_response" {
			t.Fatalf("accepted probe %s: %v", raw, err)
		}
	}
}

func TestClientRequestDeadlineAndAllowlistedUsageLogs(t *testing.T) {
	var logs bytes.Buffer
	c := NewClient(testSettings(), slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	c.http = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || deadline.After(time.Now().Add(requestTimeout)) {
			t.Error("missing request deadline")
		}
		return nil, context.DeadlineExceeded
	})}
	if _, err := c.Evaluate(context.Background(), testJPEG(t), "P1S"); errorCategory(err) != "timeout" {
		t.Fatal(err)
	}
	body := strings.TrimSuffix(resultJSON(.1, .9), "}") + `,"hostile":"marker\nforged","image":"base64-marker"}`
	c.http = fakeHTTP(200, body)
	if _, err := c.Evaluate(context.Background(), testJPEG(t), "P1S"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "input_tokens=11") || !strings.Contains(logs.String(), "output_tokens=3") || strings.Contains(logs.String(), "marker") || strings.Contains(logs.String(), "forged") {
		t.Fatal(logs.String())
	}
}

func TestClientPinsCredentialDestination(t *testing.T) {
	var contacts int
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { contacts++; _, _ = io.WriteString(w, resultJSON(.1, .9)) }))
	defer target.Close()
	c := NewClient(testSettings(), nil)
	c.http.Transport = target.Client().Transport
	c.settings.Endpoint = target.URL
	if _, err := c.Evaluate(context.Background(), testJPEG(t), "P1S"); err == nil || errorCategory(err) != "auth_rejected" {
		t.Fatalf("changed client destination %v", err)
	}
	if contacts != 0 {
		t.Fatal("retained credential sent to a changed endpoint")
	}
	c = NewClient(Settings{Provider: ProviderCustom, Endpoint: "http://insecure.example/check", APIKey: "secret", Model: "clef"}, nil)
	c.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("insecure credential request"); return nil, nil })}
	if err := c.Probe(context.Background()); err == nil {
		t.Fatal("insecure endpoint accepted")
	}
}

func TestClientTypeSafeWire(t *testing.T) {
	s := Settings{Provider: ProviderTypeSafe, APIKey: "private-key", Model: typesafeModel, StopConfidence: .5, MinClarity: DefaultMinClarity, FirstLayer: FirstLayerSettings{PauseConfidence: DefaultPauseConfidence}}
	c := NewClient(s, nil)
	frame := sizedJPEG(t, 1280, 720)
	seen := map[string]int{}
	c.http = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != typesafeURL {
			t.Errorf("URL %s", r.URL.String())
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Model     string                   `json:"model"`
			State     typesafeState            `json:"state"`
			Questions map[string]question      `json:"questions"`
			Assets    map[string]typesafeAsset `json:"assets"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
		if body.Model != typesafeModel || body.State.Description == "" {
			t.Error("model/state")
		}
		if bytes.Contains(raw, []byte("images")) && body.State.Images == nil {
			t.Error("legacy images member sent")
		}
		if len(body.State.Images) == 0 {
			if r.Header.Get("TypeSafe-Danger-Zone-Images") != "" {
				t.Error("probe sent the images header")
			}
			if body.Assets != nil {
				t.Error("probe sent assets")
			}
			if len(body.Questions) != 1 {
				t.Error("probe questions")
			}
			seen["probe"]++
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"answers":{"connection_test":{"type":"noul","noul":0.9}},"usage":{"input_tokens":1,"output_tokens":1}}`)), Header: make(http.Header)}, nil
		}
		if r.Header.Get("TypeSafe-Danger-Zone-Images") != "true" {
			t.Error("missing images header")
		}
		if len(body.State.Images) == 1 {
			ref := body.State.Images[0]
			asset, ok := body.Assets[ref]
			if !ok || len(body.Assets) != 1 || asset.Type != "image" || asset.Format != "jpg" {
				t.Error("asset shape")
			}
			decoded, err := base64.StdEncoding.DecodeString(asset.Data)
			if err != nil || !bytes.Equal(decoded, frame) {
				t.Error("JPEG changed")
			}
			for _, q := range body.Questions {
				if strings.Contains(q.Instructions, ref) {
					t.Error("reference leaked into instructions")
				}
			}
			if len(body.Questions) != 2 {
				t.Error("startup questions")
			}
			seen["evaluate"]++
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(resultJSON(.1, .9))), Header: make(http.Header)}, nil
		}
		if len(body.State.Images) == 2 {
			if body.State.Images[0] == body.State.Images[1] || len(body.Assets) != 2 {
				t.Error("references not distinct")
			}
			for i, ref := range body.State.Images {
				asset, ok := body.Assets[ref]
				if !ok || asset.Type != "image" || asset.Format != "jpg" {
					t.Errorf("asset %d shape", i)
				}
			}
			if len(body.Questions) != 5 {
				t.Error("layer questions")
			}
			seen["layer"]++
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(layerJSON())), Header: make(http.Header)}, nil
		}
		t.Fatalf("unexpected image count %d", len(body.State.Images))
		return nil, nil
	})}
	if err := c.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, err := c.Evaluate(context.Background(), frame, "P1S")
	if err != nil || r.PClear != .1 {
		t.Fatalf("evaluate %+v %v", r, err)
	}
	lr, err := c.EvaluateFirstLayer(context.Background(), [][]byte{frame, frame}, "P1S")
	if err != nil || lr.PAssessable != .8 {
		t.Fatalf("layer %+v %v", lr, err)
	}
	if seen["probe"] != 1 || seen["evaluate"] != 1 || seen["layer"] != 1 {
		t.Fatalf("calls %v", seen)
	}
}

func TestFitImage(t *testing.T) {
	large := sizedJPEG(t, 1920, 1080)
	fitted, w, h, err := fitImage(ProviderTypeSafe, large)
	if err != nil || w != 1568 || h != 882 {
		t.Fatalf("scaled %dx%d %v", w, h, err)
	}
	if cfg, err := jpeg.DecodeConfig(bytes.NewReader(fitted)); err != nil || cfg.Width != 1568 || cfg.Height != 882 {
		t.Fatalf("output headers %+v %v", cfg, err)
	}
	again, _, _, err := fitImage(ProviderTypeSafe, fitted)
	if err != nil || !bytes.Equal(again, fitted) {
		t.Fatal("fitImage not idempotent")
	}
	small := sizedJPEG(t, 1280, 720)
	same, _, _, err := fitImage(ProviderTypeSafe, small)
	if err != nil || !bytes.Equal(same, small) {
		t.Fatal("in-limit image changed")
	}
	custom, _, _, err := fitImage(ProviderCustom, large)
	if err != nil || !bytes.Equal(custom, large) {
		t.Fatal("custom image changed")
	}
	if _, _, _, err := fitImage(ProviderTypeSafe, sizedJPEG(t, 200, 200)); errorCategory(err) != "image_too_small" {
		t.Fatalf("small accepted: %v", err)
	}
	if _, _, _, err := fitImage(ProviderTypeSafe, sizedJPEG(t, 4000, 300)); errorCategory(err) != "image_too_small" {
		t.Fatalf("wide accepted: %v", err)
	}
}
