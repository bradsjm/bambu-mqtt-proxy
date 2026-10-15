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

// EvaluateFirstLayer evaluates two camera JPEGs, scaled down only when the provider limits require it, without changing startup diagnostics.
func (c *Client) EvaluateFirstLayer(ctx context.Context, frames [][]byte, printerModel string) (LayerResult, error) {
	if len(frames) != 2 {
		return LayerResult{}, &safeError{code: "invalid_image"}
	}
	images := make([]imageInput, 0, 2)
	for _, data := range frames {
		fitted, _, _, err := fitImage(c.settings.Provider, data)
		if err != nil {
			return LayerResult{}, err
		}
		images = append(images, imageInput{ContentType: "image/jpeg", Base64: base64.StdEncoding.EncodeToString(fitted)})
	}
	if len(printerModel) > 64 {
		printerModel = printerModel[:64]
	}
	const exclusions = " Exclude plate texture, plate markings, reflections, brim, skirt, purge lines, designed holes, the printer toolhead itself, and normal thin strings. Do not infer a missing entire layer without a visible expected footprint."
	// visibleOnly keeps defect answers to direct evidence when the toolhead hides part of the print.
	const visibleOnly = " Use direct visible evidence only. Do not infer a defect or its absence in areas hidden by the toolhead."
	questions := map[string]question{
		"view_assessable":  {Type: "noul", Instructions: "Across these two views, are the visible printed areas sharp and adequately illuminated to assess visible defects? Partial toolhead occlusion alone does not make the views unassessable. Judge only visible evidence, not hidden areas." + exclusions, Criteria: map[string]string{"true": "Visible printed areas are in focus and sufficiently lit to judge defects, even if the toolhead hides another area.", "false": "The images are dark, blurred, out of view, or almost all relevant printed areas are hidden."}},
		"filament_tangled": {Type: "noul", Instructions: "Is a substantial loose filament tangle or spaghetti visibly present in either view?" + visibleOnly + exclusions, Criteria: map[string]string{"true": "A substantial abnormal loose filament tangle is visible.", "false": "No substantial abnormal tangle is visible."}},
		"part_detached":    {Type: "noul", Instructions: "Is a printed part visibly detached, lifted, or displaced from its intended position on the plate?" + visibleOnly + exclusions, Criteria: map[string]string{"true": "A visibly detached or displaced printed part is present.", "false": "No detached or displaced printed part is visible; absence of a whole layer alone is not evidence of detachment."}},
		"nozzle_blob":      {Type: "noul", Instructions: "Is a substantial abnormal blob of extruded filament, several millimetres wide, visibly attached around the nozzle?" + visibleOnly + exclusions, Criteria: map[string]string{"true": "A substantial accumulated filament blob several millimetres wide is visible around the nozzle.", "false": "No substantial nozzle blob is visible; normal small ooze and tiny routine deposits do not count."}},
		"layer_incomplete": {Type: "noul", Instructions: "Does the visible first-layer footprint show missing deposited material or abnormal gaps? This answer is warning-only." + visibleOnly + " Areas hidden by the toolhead are not evidence of missing material." + exclusions, Criteria: map[string]string{"true": "Visible evidence inside an expected footprint shows missing material.", "false": "No abnormal missing material is visible, or an expected footprint cannot be established."}},
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

// classifyLayer pauses only on a clear image and warns on any visible score above 50%.
// Missing material alone never pauses. Low clarity blocks the pause and the pass, but visible
// evidence can still warn: scores describe visible areas, not proof that hidden areas are sound.
// A check is unclear when the image clarity is strictly below minClarity.
func classifyLayer(r LayerResult, cutoff, minClarity float64) string {
	if r.PAssessable >= minClarity && (r.PTangled > cutoff || r.PDetached > cutoff || r.PNozzleBlob > cutoff) {
		return "pause"
	}
	if r.PTangled > .5 || r.PDetached > .5 || r.PNozzleBlob > .5 || r.PIncomplete > .5 {
		return "warning"
	}
	if r.PAssessable < minClarity {
		return "inconclusive"
	}
	return "passed"
}
