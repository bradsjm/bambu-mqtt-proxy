package platecheck

import (
	"context"
	"encoding/base64"
)

// LayerResult holds only validated scores and allowlisted provider usage.
type LayerResult struct {
	// PAssessable is the probability that the printed layer can be assessed.
	PAssessable float64 `json:"p_assessable"`
	// PTangled is the probability of a substantial abnormal filament tangle.
	PTangled float64 `json:"p_tangled"`
	// PDetached is the probability of a detached or displaced printed part.
	PDetached float64 `json:"p_detached"`
	// PNozzleBlob is the probability of an abnormal filament blob at the nozzle.
	PNozzleBlob float64 `json:"p_nozzle_blob"`
	// PIncomplete is a warning-only probability of visible missing material.
	PIncomplete float64 `json:"p_incomplete"`
	// Model is the configured model, never arbitrary provider text.
	Model string `json:"model"`
	// InputTokens is the nonnegative provider input token count.
	InputTokens int `json:"input_tokens"`
	// OutputTokens is the nonnegative provider output token count.
	OutputTokens int `json:"output_tokens"`
}

// EvaluateFirstLayer evaluates two original JPEGs without changing startup diagnostics.
func (c *Client) EvaluateFirstLayer(ctx context.Context, frames [][]byte, printerModel string) (LayerResult, error) {
	if len(frames) != 2 {
		return LayerResult{}, &safeError{code: "invalid_image"}
	}
	images := make([]imageInput, 0, 2)
	for _, data := range frames {
		if _, _, err := imageDimensions(data); err != nil {
			return LayerResult{}, err
		}
		images = append(images, imageInput{ContentType: "image/jpeg", Base64: base64.StdEncoding.EncodeToString(data)})
	}
	if len(printerModel) > 64 {
		printerModel = printerModel[:64]
	}
	const exclusions = " Exclude plate texture, plate markings, reflections, brim, skirt, purge lines, designed holes, the printer toolhead itself, and normal thin strings. Do not infer a missing entire layer without a visible expected footprint."
	questions := map[string]question{
		"view_assessable":  {Type: "noul", Instructions: "Across these two views, is enough of the printed first layer visible, in focus, and adequately illuminated to assess visible defects?" + exclusions, Criteria: map[string]string{"true": "The relevant printed surface is visible and clear enough to judge.", "false": "The print is obscured, dark, out of view, or too unclear to judge."}},
		"filament_tangled": {Type: "noul", Instructions: "Is a substantial loose filament tangle or spaghetti visibly present in either view?" + exclusions, Criteria: map[string]string{"true": "A substantial abnormal loose filament tangle is visible.", "false": "No substantial abnormal tangle is visible."}},
		"part_detached":    {Type: "noul", Instructions: "Is a printed part visibly detached, lifted, or displaced from its intended position on the plate?" + exclusions, Criteria: map[string]string{"true": "A visibly detached or displaced printed part is present.", "false": "No detached or displaced printed part is visible; absence of a whole layer alone is not evidence of detachment."}},
		"nozzle_blob":      {Type: "noul", Instructions: "Is a substantial abnormal blob of extruded filament, several millimetres wide, visibly attached around the nozzle?" + exclusions, Criteria: map[string]string{"true": "A substantial accumulated filament blob several millimetres wide is visible around the nozzle.", "false": "No substantial nozzle blob is visible; normal small ooze and tiny routine deposits do not count."}},
		"layer_incomplete": {Type: "noul", Instructions: "Does the visible first-layer footprint show missing deposited material or abnormal gaps? This answer is warning-only." + exclusions, Criteria: map[string]string{"true": "Visible evidence inside an expected footprint shows missing material.", "false": "No abnormal missing material is visible, or an expected footprint cannot be established."}},
	}
	r, err := c.request(ctx, requestInput{Model: c.settings.Model, State: "Two post-first-layer views for printer model " + printerModel + ".", Questions: questions, Images: images})
	if err != nil {
		return LayerResult{}, err
	}
	for _, a := range []answer{r.Answers.Assessable, r.Answers.Tangled, r.Answers.Detached, r.Answers.NozzleBlob, r.Answers.Incomplete} {
		if !validAnswer(a) {
			return LayerResult{}, &safeError{code: "bad_response"}
		}
	}
	return LayerResult{PAssessable: *r.Answers.Assessable.Noul, PTangled: *r.Answers.Tangled.Noul, PDetached: *r.Answers.Detached.Noul, PNozzleBlob: *r.Answers.NozzleBlob.Noul, PIncomplete: *r.Answers.Incomplete.Noul, Model: c.settings.Model, InputTokens: *r.Usage.InputTokens, OutputTokens: *r.Usage.OutputTokens}, nil
}

// validLayerResult also validates scores returned by replacement client implementations.
func validLayerResult(r LayerResult) bool {
	for _, p := range []float64{r.PAssessable, r.PTangled, r.PDetached, r.PNozzleBlob, r.PIncomplete} {
		if !validAnswer(answer{Type: "noul", Noul: &p}) {
			return false
		}
	}
	return r.InputTokens >= 0 && r.OutputTokens >= 0
}

// classifyLayer keeps missing material warning-only and uses a strict pause cutoff.
func classifyLayer(r LayerResult, cutoff float64) string {
	if r.PAssessable < assessableMin {
		return "inconclusive"
	}
	if r.PTangled > cutoff || r.PDetached > cutoff || r.PNozzleBlob > cutoff {
		return "pause"
	}
	if r.PTangled > .5 || r.PDetached > .5 || r.PNozzleBlob > .5 || r.PIncomplete > .5 {
		return "warning"
	}
	return "passed"
}
