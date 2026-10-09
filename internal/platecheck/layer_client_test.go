package platecheck

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
)

func layerJSON() string {
	return `{"answers":{"view_assessable":{"type":"noul","noul":0.8},"filament_tangled":{"type":"noul","noul":0},"part_detached":{"type":"noul","noul":1},"nozzle_blob":{"type":"noul","noul":0.7},"layer_incomplete":{"type":"noul","noul":0.99}},"usage":{"input_tokens":11,"output_tokens":3}}`
}

func TestFirstLayerFiveQuestionsTwoOriginalImages(t *testing.T) {
	images := [][]byte{testJPEG(t), append(testJPEG(t), 0)}
	c := NewClient(testSettings(), nil)
	c.http = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var in requestInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		if len(in.Images) != 2 || len(in.Questions) != 5 || !strings.Contains(in.State, "P1S") || in.Model != "clef" {
			t.Fatalf("wire shape %+v", in)
		}
		for i, image := range in.Images {
			data, err := base64.StdEncoding.DecodeString(image.Base64)
			if err != nil || !bytes.Equal(data, images[i]) || image.ContentType != "image/jpeg" {
				t.Fatal("original JPEG changed")
			}
		}
		for _, key := range []string{"view_assessable", "filament_tangled", "part_detached", "nozzle_blob", "layer_incomplete"} {
			q := in.Questions[key]
			if q.Type != "noul" {
				t.Fatalf("missing noul %s", key)
			}
			for _, exclusion := range []string{"plate texture", "plate markings", "reflections", "brim", "skirt", "purge lines", "designed holes", "toolhead", "normal thin strings", "expected footprint"} {
				if !strings.Contains(q.Instructions, exclusion) {
					t.Errorf("%s misses %s", key, exclusion)
				}
			}
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"success":true,"result":` + layerJSON() + `}`)), Header: make(http.Header)}, nil
	})}
	r, err := c.EvaluateFirstLayer(context.Background(), images, "P1S")
	if err != nil || r.PAssessable != .8 || r.PTangled != 0 || r.PDetached != 1 || r.PNozzleBlob != .7 || r.PIncomplete != .99 || r.Model != "clef" || r.InputTokens != 11 || r.OutputTokens != 3 {
		t.Fatalf("result %+v %v", r, err)
	}
}

func TestFirstLayerResponseValidation(t *testing.T) {
	for _, key := range []string{"view_assessable", "filament_tangled", "part_detached", "nozzle_blob", "layer_incomplete"} {
		for _, invalid := range []string{`null`, `{"type":"noul"}`, `{"type":"noul","noul":null}`, `{"type":"noul","noul":true}`, `{"type":"choice","noul":0.8}`, `{"type":"noul","noul":-0.1}`, `{"type":"noul","noul":1.1}`, `{"type":"noul","noul":1e999}`} {
			t.Run(key+invalid, func(t *testing.T) {
				var body map[string]json.RawMessage
				_ = json.Unmarshal([]byte(layerJSON()), &body)
				var answers map[string]json.RawMessage
				_ = json.Unmarshal(body["answers"], &answers)
				answers[key] = json.RawMessage(invalid)
				body["answers"], _ = json.Marshal(answers)
				raw, _ := json.Marshal(body)
				c := NewClient(testSettings(), nil)
				c.http = fakeHTTP(200, string(raw))
				if _, err := c.EvaluateFirstLayer(context.Background(), [][]byte{testJPEG(t), testJPEG(t)}, "P1S"); errorCategory(err) != "bad_response" {
					t.Fatalf("accepted %s: %v", raw, err)
				}
			})
		}
	}
	for _, body := range []string{`{"answers":{},"usage":{"input_tokens":1,"output_tokens":1}}`, strings.Replace(layerJSON(), `"input_tokens":11`, `"input_tokens":-1`, 1), strings.Replace(layerJSON(), `"output_tokens":3`, `"output_tokens":null`, 1), layerJSON() + ` {}`, `{"success":false,"result":` + layerJSON() + `}`} {
		c := NewClient(testSettings(), nil)
		c.http = fakeHTTP(200, body)
		if _, err := c.EvaluateFirstLayer(context.Background(), [][]byte{testJPEG(t), testJPEG(t)}, "P1S"); err == nil {
			t.Fatalf("accepted partial/invalid response %s", body)
		}
	}
}

func TestFirstLayerImageValidationBeforeUpload(t *testing.T) {
	c := NewClient(testSettings(), nil)
	c.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("invalid image uploaded"); return nil, nil })}
	for _, frames := range [][][]byte{nil, {testJPEG(t)}, {testJPEG(t), []byte("not jpeg")}, {testJPEG(t), make([]byte, maxImageBytes+1)}, {testJPEG(t), testJPEG(t), testJPEG(t)}} {
		if _, err := c.EvaluateFirstLayer(context.Background(), frames, "P1S"); err == nil {
			t.Fatal("invalid image set accepted")
		}
	}
}

func TestFirstLayerStrictCutoffAndWarningOnlyMissingMaterial(t *testing.T) {
	for _, tc := range []struct {
		result LayerResult
		want   string
	}{
		{LayerResult{PAssessable: .8, PTangled: .7}, "warning"},
		{LayerResult{PAssessable: .8, PTangled: math.Nextafter(.7, 1)}, "pause"},
		{LayerResult{PAssessable: .8, PDetached: .71}, "pause"},
		{LayerResult{PAssessable: .8, PNozzleBlob: .71}, "pause"},
		{LayerResult{PAssessable: .8, PIncomplete: 1}, "warning"},
		{LayerResult{PAssessable: .8, PIncomplete: .5}, "passed"},
		{LayerResult{PAssessable: math.Nextafter(.8, 0), PTangled: 1}, "inconclusive"},
	} {
		if got := classifyLayer(tc.result, .7); got != tc.want {
			t.Fatalf("%+v: %s != %s", tc.result, got, tc.want)
		}
	}
	for _, bad := range []float64{math.NaN(), math.Inf(1), -.1, 1.1} {
		if validLayerResult(LayerResult{PAssessable: .9, PIncomplete: bad}) {
			t.Fatal("invalid implementation score accepted")
		}
	}
}
