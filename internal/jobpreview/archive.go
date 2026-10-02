// The archive parser turns one downloaded 3MF file into a bounded preview
// draft. Every byte inside the archive is untrusted printer and model
// output: entries outside the allowlist are never opened, each requested
// entry is streamed through a hard read cap and a ZIP CRC check, XML and
// JSON must fully validate — including trailing content — before any value
// is committed, and all exported strings are whitespace-normalized and
// capped by rune count. Failures are isolated per entry, so one malformed
// entry clears only the fields sourced from it. Plate G-code entries are
// used for name-based plate resolution only and are never decompressed.

package jobpreview

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"image/png"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"bambu-mqtt-proxy/internal/telemetry"

	"golang.org/x/net/html"
)

const (
	sourcePrinter3mf = "printer_3mf"

	// Entry allowlist, case-sensitive and root-relative. Everything else
	// in the archive stays unread.
	modelEntry           = "3D/3dmodel.model"
	modelSettingsEntry   = "Metadata/model_settings.config"
	sliceInfoEntry       = "Metadata/slice_info.config"
	projectSettingsEntry = "Metadata/project_settings.config"

	// Structural and read caps.
	maxZipEntries      = 4096
	maxXMLDepth        = 64
	entryCapBytes      = int64(1) << 20 // per XML/JSON entry
	imageCapBytes      = int64(4) << 20 // decompressed PNG file
	cumulativeCapBytes = int64(8) << 20 // across requested non-image entries
	maxPixelDim        = 2048

	// Output caps. Truncation is reported only when content is actually
	// omitted, never when a value merely equals its cap.
	capTextRunes        = 256 // title, designer, license, plate and object names
	capDescriptionRunes = 2000
	capMessageRunes     = 256 // warning text
	capIDRunes          = 256 // material id/type, bed type, warning code
	maxMaterials        = 16
	maxObjectGroups     = 64
	maxWarnings         = 16
)

// Fixed parse-failure categories. The retrieval caller logs exactly one of
// these per attempt; categories never carry archive text, entry names, or
// filesystem paths.
const (
	catArchive        = "archive"
	catOversized      = "oversized"
	catAmbiguousPlate = "ambiguous_plate"
	catImageMissing   = "image_missing"
)

// archiveError is a fixed-category parse failure. detail is always a
// constant phrase derived from nothing in the archive or on disk.
type archiveError struct {
	category string
	detail   string
}

func (e *archiveError) Error() string { return "jobpreview: " + e.detail }

func archiveErr(category, detail string) error {
	return &archiveError{category: category, detail: detail}
}

// errorCategory reports the fixed category of a parse failure, or "" for
// other errors and nil.
func errorCategory(err error) string {
	var ae *archiveError
	if errors.As(err, &ae) {
		return ae.category
	}
	return ""
}

// categoryRank orders categories by severity for the single logged outcome
// when several entries failed.
func categoryRank(cat string) int {
	switch cat {
	case catArchive:
		return 0
	case catOversized:
		return 1
	case catAmbiguousPlate:
		return 2
	case catImageMissing:
		return 3
	}
	return 4
}

// firstFailure picks the most severe fixed-category failure so the
// retrieval caller logs one bounded outcome per attempt.
func firstFailure(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	best := errs[0]
	for _, e := range errs[1:] {
		if categoryRank(errorCategory(e)) < categoryRank(errorCategory(best)) {
			best = e
		}
	}
	return best
}

// parseArchive validates the downloaded 3MF archive at path against the
// preview job identity and returns a result draft. The caller owns
// scheduling, the temporary file lifecycle, and the final View stamps
// (retrieved_at, image_url); the View here carries only the archive
// outcome. The returned error, when non-nil, always carries a fixed
// category, and a partial result may still accompany it when parts of the
// archive parsed cleanly. path never appears in errors or results.
func parseArchive(path string, job telemetry.JobView) (Result, error) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return Result{}, archiveErr(catArchive, "open archive")
	}
	defer zr.Close()
	if len(zr.File) > maxZipEntries {
		return Result{}, archiveErr(catOversized, "archive entry count")
	}
	idx := indexArchive(&zr.Reader)
	budget := cumulativeCapBytes

	// Plate identity is resolved from entry names only; plate G-code
	// entries are never opened.
	plate, plateErr := resolvePlate(job, idx)

	res := Result{}
	md := &Metadata{Source: sourcePrinter3mf, Materials: []Material{}, Objects: []Object{}, Warnings: []SlicerWarning{}}
	var failures []error
	fail := func(err error) {
		if err != nil {
			failures = append(failures, err)
		}
	}
	withEntry := func(name string, limit int64, budgeted bool, fn func(data []byte) error) {
		f := idx.lookup(name)
		if f == nil {
			// Absent allowlisted entries simply contribute no fields;
			// the missing plate image is recorded by the caller below.
			return
		}
		var b *int64
		if budgeted {
			b = &budget
		}
		data, err := readZipEntry(f, limit, b)
		if err != nil {
			fail(err)
			return
		}
		if err := fn(data); err != nil {
			fail(err)
		}
	}

	withEntry(modelEntry, entryCapBytes, true, func(data []byte) error {
		meta, err := parseModelXML(data)
		if err != nil {
			return err
		}
		commitModelText(md, meta)
		return nil
	})

	if plateErr != nil {
		fail(plateErr)
	}

	if plate > 0 {
		ensurePlate := func() *Plate {
			if md.Plate == nil {
				md.Plate = &Plate{Index: plate}
			}
			return md.Plate
		}

		withEntry(modelSettingsEntry, entryCapBytes, true, func(data []byte) error {
			name, trunc, err := parseModelSettings(data, plate)
			if err != nil {
				return err
			}
			if name != nil {
				ensurePlate().Name = name
				md.Truncated = md.Truncated || trunc
			}
			return nil
		})
		withEntry(sliceInfoEntry, entryCapBytes, true, func(data []byte) error {
			d, err := parseSliceInfo(data, plate)
			if err != nil {
				return err
			}
			commitSliceInfo(md, ensurePlate, d)
			return nil
		})
		withEntry(plateEntryName(plate, ".json"), entryCapBytes, true, func(data []byte) error {
			bed, trunc, err := parseBedType(data)
			if err != nil {
				return err
			}
			if bed != nil {
				ensurePlate().BedType = bed
				md.Truncated = md.Truncated || trunc
			}
			return nil
		})
		withEntry(projectSettingsEntry, entryCapBytes, true, func(data []byte) error {
			pd, err := parseProjectSettings(data)
			if err != nil {
				return err
			}
			if pd.layerHeight != nil || pd.infill != nil {
				md.Process = &Process{LayerHeightMM: pd.layerHeight, InfillPercent: pd.infill}
			}
			return nil
		})

		pngName := plateEntryName(plate, ".png")
		f := idx.lookup(pngName)
		switch {
		case f == nil:
			fail(archiveErr(catImageMissing, "plate image absent"))
		default:
			withEntry(pngName, imageCapBytes, false, func(data []byte) error {
				accepted, err := decodePNG(data)
				if err != nil {
					return err
				}
				res.PNG = accepted
				return nil
			})
		}
	}

	if !md.empty() {
		res.Metadata = md
	}
	if res.PNG != nil {
		res.Preview.Status = StatusReady
	} else {
		res.Preview.Status = StatusUnavailable
	}
	res.Preview.JobName = job.Name
	res.Preview.Current = job.Active
	return res, firstFailure(failures)
}

// plateEntryName builds the canonical plate sidecar or image entry name.
func plateEntryName(plate int, ext string) string {
	return fmt.Sprintf("Metadata/plate_%d%s", plate, ext)
}

// archiveIndex catalogs the central directory. Only names are inspected
// here; no entry content is read.
type archiveIndex struct {
	files  map[string]*zip.File
	dup    map[string]bool
	plates map[int]bool
}

// indexArchive records the first file per exact name, marks duplicated
// names, and collects the distinct plate numbers of plate G-code entries.
func indexArchive(r *zip.Reader) *archiveIndex {
	idx := &archiveIndex{
		files:  make(map[string]*zip.File),
		dup:    make(map[string]bool),
		plates: make(map[int]bool),
	}
	for _, f := range r.File {
		if _, seen := idx.files[f.Name]; seen {
			idx.dup[f.Name] = true
			continue
		}
		idx.files[f.Name] = f
		if n, ok := archivePlateGcode(f.Name); ok {
			idx.plates[n] = true
		}
	}
	return idx
}

// lookup returns the single entry with the exact case-sensitive name, or
// nil when the entry is absent or duplicated: a duplicated requested path
// is ambiguous and invalidates that entry only.
func (ix *archiveIndex) lookup(name string) *zip.File {
	if ix.dup[name] {
		return nil
	}
	return ix.files[name]
}

// resolvePlate applies the selection order: a positive reported plate
// index, then the plate number inside the reported gcode file name, then
// exactly one plate G-code entry present in the archive. Conflicting
// explicit sources, several unnamed archive plates, and no plate evidence
// at all leave the plate unresolved; nothing is guessed and plate 1 is
// never chosen by default.
func resolvePlate(job telemetry.JobView, idx *archiveIndex) (int, error) {
	var explicit []int
	if job.PlateIndex != nil && *job.PlateIndex > 0 {
		explicit = append(explicit, *job.PlateIndex)
	}
	if n := plateFromGCodeFile(job.GCodeFile); n > 0 {
		explicit = append(explicit, n)
	}
	switch {
	case len(explicit) == 1:
		return explicit[0], nil
	case len(explicit) > 1:
		if explicit[0] == explicit[1] {
			return explicit[0], nil
		}
		return 0, archiveErr(catAmbiguousPlate, "conflicting plate sources")
	}
	if len(idx.plates) == 1 {
		for n := range idx.plates {
			return n, nil
		}
	}
	return 0, archiveErr(catAmbiguousPlate, "unresolved plate")
}

// plateFilePattern reads the plate number out of a reported gcode file
// name; the match is case-insensitive.
var plateFilePattern = regexp.MustCompile(`(?i)plate_([0-9]+)\.gcode`)

func plateFromGCodeFile(name string) int {
	m := plateFilePattern.FindStringSubmatch(name)
	if m == nil {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// archivePlateGcode matches exactly Metadata/plate_N.gcode with a
// canonical decimal N, case-sensitively per the allowlist.
func archivePlateGcode(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, "Metadata/plate_")
	if !ok {
		return 0, false
	}
	num, ok := strings.CutSuffix(rest, ".gcode")
	if !ok || num == "" {
		return 0, false
	}
	n, err := strconv.Atoi(num)
	if err != nil || n <= 0 || strconv.Itoa(n) != num {
		return 0, false
	}
	return n, true
}

// readZipEntry streams one entry under the per-entry cap and the shared
// cumulative budget. Declared sizes beyond the cap skip decompression
// entirely; because the read only succeeds on true EOF, an accepted result
// has passed the ZIP CRC check. A stream that reaches its allowance is
// rejected as oversized without parse. Hostile archives can lie about
// declared sizes, so the streaming allowance is enforced as well.
func readZipEntry(f *zip.File, limit int64, budget *int64) ([]byte, error) {
	if f.UncompressedSize64 > uint64(limit) {
		return nil, archiveErr(catOversized, "entry declared size")
	}
	allowance := limit + 1
	if budget != nil && *budget < allowance {
		allowance = *budget + 1
	}
	rc, err := f.Open()
	if err != nil {
		return nil, archiveErr(catArchive, "open entry")
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, allowance))
	if err != nil {
		// Covers CRC mismatches at EOF and malformed deflate streams.
		return nil, archiveErr(catArchive, "entry read")
	}
	if budget != nil {
		*budget -= int64(len(data))
	}
	if int64(len(data)) >= allowance {
		return nil, archiveErr(catOversized, "entry read cap")
	}
	return data, nil
}

// modelMetaKeys are the root-level 3D/3dmodel.model metadata values the
// parser selects, matched case-insensitively by local name.
var modelMetaKeys = [4]string{"Title", "Description", "Designer", "License"}

func canonicalModelKey(name string) string {
	for _, k := range modelMetaKeys {
		if strings.EqualFold(name, k) {
			return k
		}
	}
	return ""
}

// parseModelXML streams 3D/3dmodel.model and collects the raw root-level
// metadata values by name. Namespace prefixes are ignored (local names
// only). The whole document must parse: a truncated document, a second
// root element, non-whitespace content outside the root, nesting beyond
// the depth cap, or markup inside a metadata value rejects the entry.
func parseModelXML(data []byte) (map[string]string, error) {
	meta := make(map[string]string)
	dec := xml.NewDecoder(bytes.NewReader(data))
	depth := 0
	rootSeen := false
	key := ""
	var buf []byte
	inMeta := false
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, archiveErr(catArchive, "model xml syntax")
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			if depth > maxXMLDepth {
				return nil, archiveErr(catArchive, "xml depth")
			}
			switch {
			case depth == 1:
				if rootSeen || t.Name.Local != "model" {
					return nil, archiveErr(catArchive, "model xml root")
				}
				rootSeen = true
			case depth == 2 && t.Name.Local == "metadata":
				key = canonicalModelKey(attrValue(&t, "name"))
				buf = buf[:0]
				inMeta = key != ""
			case inMeta:
				// Nested markup inside a metadata value is not
				// something this parser interprets.
				return nil, archiveErr(catArchive, "model xml metadata element")
			}
		case xml.CharData:
			switch {
			case inMeta:
				buf = append(buf, t...)
			case depth == 0 && strings.TrimSpace(string(t)) != "":
				return nil, archiveErr(catArchive, "model xml trailing")
			}
		case xml.EndElement:
			if inMeta && depth == 2 {
				if _, exists := meta[key]; !exists {
					meta[key] = string(buf)
				}
				inMeta = false
			}
			depth--
		}
	}
	if !rootSeen {
		return nil, archiveErr(catArchive, "model xml root")
	}
	return meta, nil
}

// commitModelText normalizes and caps the raw model metadata into the
// projection. Title, designer, and license are single-line plain text; the
// description may carry HTML from the model author and is reduced to plain
// text with paragraph spacing. Everything here is untrusted model-authored
// text and is never rendered as markup by any consumer.
func commitModelText(md *Metadata, meta map[string]string) {
	if v, ok := meta["Title"]; ok {
		if s := cappedText(v, capTextRunes, &md.Truncated); s != nil {
			md.Title = s
		}
	}
	if v, ok := meta["Description"]; ok {
		if s := cappedParagraph(htmlToPlainText(decodeEntitiesUpToTwice(v)), capDescriptionRunes, &md.Truncated); s != nil {
			md.Description = s
		}
	}
	if v, ok := meta["Designer"]; ok {
		if s := cappedText(v, capTextRunes, &md.Truncated); s != nil {
			md.Designer = s
		}
	}
	if v, ok := meta["License"]; ok {
		if s := cappedText(v, capTextRunes, &md.Truncated); s != nil {
			md.License = s
		}
	}
}

// xmlPair is one metadata key/value attribute pair.
type xmlPair struct {
	key, value string
}

type filamentAttr struct{ id, typ, color, usedG, usedM string }
type objectAttr struct{ name, skipped string }
type warningAttr struct{ msg, errorCode string }

// configPlate is one plate element of an XML sidecar.
type configPlate struct {
	pairs     []xmlPair
	filaments []filamentAttr
	objects   []objectAttr
	warnings  []warningAttr
}

// plateMetaKeys are the only plate metadata keys any parser extracts
// (plater_id/plater_name for model_settings, the rest for slice_info).
// Every other key is dropped before storage or duplicate checking, so a
// hostile sidecar can neither accumulate arbitrary maps nor slow key
// insertion; stored pairs per plate stay bounded by this set.
var plateMetaKeys = map[string]bool{
	"plater_id":        true,
	"plater_name":      true,
	"index":            true,
	"prediction":       true,
	"weight":           true,
	"first_layer_time": true,
	"support_used":     true,
}

// configFile is a parsed XML sidecar: the plate elements in archive
// order.
type configFile struct {
	plates []configPlate
}

// parseConfigXML streams one of the XML sidecars (root element config).
// It records plate metadata pairs — first value wins per key — and
// per-plate filament, object, and warning elements; root-level metadata
// elements are ignored. Local element and attribute names are matched, so
// namespace prefixes do not matter. The document must parse completely;
// trailing content, a second root, or nesting beyond the depth cap
// rejects the entry.
func parseConfigXML(data []byte) (configFile, error) {
	var cf configFile
	dec := xml.NewDecoder(bytes.NewReader(data))
	depth := 0
	rootSeen := false
	cur := -1
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return configFile{}, archiveErr(catArchive, "config xml syntax")
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			if depth > maxXMLDepth {
				return configFile{}, archiveErr(catArchive, "xml depth")
			}
			switch {
			case depth == 1:
				if rootSeen || t.Name.Local != "config" {
					return configFile{}, archiveErr(catArchive, "config xml root")
				}
				rootSeen = true
			case depth == 2 && t.Name.Local == "plate":
				cf.plates = append(cf.plates, configPlate{})
				cur = len(cf.plates) - 1
			case cur >= 0 && depth == 3:
				switch t.Name.Local {
				case "metadata":
					if k := attrValue(&t, "key"); plateMetaKeys[k] {
						p := &cf.plates[cur]
						if _, exists := lookupPair(p.pairs, k); !exists {
							p.pairs = append(p.pairs, xmlPair{k, attrValue(&t, "value")})
						}
					}
				case "filament":
					cf.plates[cur].filaments = append(cf.plates[cur].filaments, filamentAttr{
						id:    attrValue(&t, "id"),
						typ:   attrValue(&t, "type"),
						color: attrValue(&t, "color"),
						usedG: attrValue(&t, "used_g"),
						usedM: attrValue(&t, "used_m"),
					})
				case "object":
					cf.plates[cur].objects = append(cf.plates[cur].objects, objectAttr{
						name:    attrValue(&t, "name"),
						skipped: attrValue(&t, "skipped"),
					})
				case "warning":
					cf.plates[cur].warnings = append(cf.plates[cur].warnings, warningAttr{
						msg:       attrValue(&t, "msg"),
						errorCode: attrValue(&t, "error_code"),
					})
				}
			}
		case xml.CharData:
			if depth == 0 && strings.TrimSpace(string(t)) != "" {
				return configFile{}, archiveErr(catArchive, "config xml trailing")
			}
		case xml.EndElement:
			if depth == 2 && cur >= 0 {
				// The plate element closed; cur can only be set while
				// inside it.
				cur = -1
			}
			depth--
		}
	}
	if !rootSeen {
		return configFile{}, archiveErr(catArchive, "config xml root")
	}
	return cf, nil
}

// lookupPair returns the first value recorded for the key.
func lookupPair(pairs []xmlPair, key string) (string, bool) {
	for _, kv := range pairs {
		if kv.key == key {
			return kv.value, true
		}
	}
	return "", false
}

// parseModelSettings joins the selected plate index to plater_id and
// returns its plater_name: nil when the plate is not listed, the name is
// absent, or it is empty after normalization.
func parseModelSettings(data []byte, plate int) (*string, bool, error) {
	cf, err := parseConfigXML(data)
	if err != nil {
		return nil, false, err
	}
	for _, p := range cf.plates {
		id, ok := lookupPair(p.pairs, "plater_id")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(id))
		if err != nil || n != plate {
			continue
		}
		name, ok := lookupPair(p.pairs, "plater_name")
		if !ok {
			return nil, false, nil
		}
		s, trunc := capRunes(collapseSpace(name), capTextRunes)
		if s == "" {
			return nil, false, nil
		}
		return &s, trunc, nil
	}
	return nil, false, nil
}

// sliceData is the staged outcome of the selected plate in slice_info; it
// is committed only after the entry fully validated and only when the
// selected plate is present.
type sliceData struct {
	found             bool
	estimatedSeconds  *float64
	weightG           *float64
	firstLayerSeconds *float64
	supports          *bool
	materials         []Material
	objects           []Object
	objectCount       *int
	warnings          []SlicerWarning
	truncated         bool
}

// knownWarnings maps archive warning identifiers to fixed human text.
// Unknown identifiers keep their raw wording with underscores converted to
// spaces; they are slicer notes, not live faults, and no live HMS lookup
// runs for them.
var knownWarnings = map[string]string{
	"bed_temperature_too_high_than_filament": "Sliced bed temperature exceeds the filament recommendation",
}

// parseSliceInfo reads the selected plate from slice_info: sliced
// estimates and weight, support usage, archive filament rows (never AMS
// slots), non-skipped objects grouped by original name in archive order,
// and archived slicer warnings. A plate element that does not list the
// selected index contributes nothing; object_count is the full non-skipped
// count even when the grouped array alone is capped.
func parseSliceInfo(data []byte, plate int) (sliceData, error) {
	d := sliceData{
		materials: []Material{},
		objects:   []Object{},
		warnings:  []SlicerWarning{},
	}
	cf, err := parseConfigXML(data)
	if err != nil {
		return d, err
	}
	var sel *configPlate
	for i := range cf.plates {
		v, ok := lookupPair(cf.plates[i].pairs, "index")
		if !ok {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n == plate {
			sel = &cf.plates[i]
			break
		}
	}
	if sel == nil {
		return d, nil
	}
	d.found = true

	if v, ok := lookupPair(sel.pairs, "prediction"); ok {
		if f, ok := parseNumericString(v); ok && f >= 0 {
			d.estimatedSeconds = &f
		}
	}
	if v, ok := lookupPair(sel.pairs, "weight"); ok {
		if f, ok := parseNumericString(v); ok && f >= 0 {
			d.weightG = &f
		}
	}
	if v, ok := lookupPair(sel.pairs, "first_layer_time"); ok {
		if f, ok := parseNumericString(v); ok && f > 0 {
			d.firstLayerSeconds = &f
		}
	}
	if v, ok := lookupPair(sel.pairs, "support_used"); ok {
		if b, err := strconv.ParseBool(strings.TrimSpace(v)); err == nil {
			d.supports = &b
		}
	}

	for _, fl := range sel.filaments {
		if len(d.materials) == maxMaterials {
			d.truncated = true
			break
		}
		m := Material{ID: cappedID(fl.id, &d.truncated)}
		m.Type = cappedText(fl.typ, capIDRunes, &d.truncated)
		m.Color = normalizeColor(fl.color)
		if f, ok := parseNumericString(fl.usedG); ok && f >= 0 {
			m.UsedG = &f
		}
		if f, ok := parseNumericString(fl.usedM); ok && f >= 0 {
			m.UsedM = &f
		}
		d.materials = append(d.materials, m)
	}

	total := 0
	order := make(map[string]int)
	for _, ob := range sel.objects {
		if skipped, err := strconv.ParseBool(strings.TrimSpace(ob.skipped)); err == nil && skipped {
			continue
		}
		total++
		if gi, ok := order[ob.name]; ok {
			d.objects[gi].Count++
			continue
		}
		if len(d.objects) == maxObjectGroups {
			d.truncated = true
			continue
		}
		name, trunc := capRunes(collapseSpace(ob.name), capTextRunes)
		d.truncated = d.truncated || trunc
		order[ob.name] = len(d.objects)
		d.objects = append(d.objects, Object{Name: name, Count: 1})
	}
	d.objectCount = &total

	for _, w := range sel.warnings {
		ident := strings.TrimSpace(w.msg)
		if ident == "" {
			ident = strings.TrimSpace(w.errorCode)
		}
		if ident == "" {
			continue
		}
		if len(d.warnings) == maxWarnings {
			d.truncated = true
			break
		}
		code := strings.TrimSpace(w.errorCode)
		if code == "" {
			code = ident
		}
		message, known := knownWarnings[ident]
		if !known {
			message = strings.ReplaceAll(ident, "_", " ")
		}
		code, codeTrunc := capRunes(code, capIDRunes)
		message, trunc := capRunes(message, capMessageRunes)
		d.truncated = d.truncated || codeTrunc || trunc
		d.warnings = append(d.warnings, SlicerWarning{Code: code, Message: message})
	}
	return d, nil
}

// commitSliceInfo moves the staged slice outcome into the projection.
func commitSliceInfo(md *Metadata, ensurePlate func() *Plate, d sliceData) {
	if !d.found {
		// The selected plate is absent from slice_info, so no field of
		// this entry was accepted: commit nothing, not even an
		// index-only plate.
		return
	}
	p := ensurePlate()
	p.EstimatedSeconds = d.estimatedSeconds
	p.WeightG = d.weightG
	p.FirstLayerSeconds = d.firstLayerSeconds
	p.Supports = d.supports
	md.Materials = d.materials
	md.Objects = d.objects
	md.ObjectCount = d.objectCount
	md.Warnings = d.warnings
	md.Truncated = md.Truncated || d.truncated
}

// parseBedType reads the plate type from the selected plate JSON. The
// value may sit at the top level or under a plate object; everything else
// in the file is ignored.
func parseBedType(data []byte) (*string, bool, error) {
	obj, err := decodeJSONObject(data, "plate json")
	if err != nil {
		return nil, false, err
	}
	v, ok := obj["bed_type"]
	if !ok {
		if sub, ok := obj["plate"].(map[string]any); ok {
			v = sub["bed_type"]
		}
	}
	s, ok := v.(string)
	if !ok {
		return nil, false, nil
	}
	var trunc bool
	return cappedText(s, capIDRunes, &trunc), trunc, nil
}

// projectData holds the accepted global sliced defaults.
type projectData struct {
	layerHeight *float64
	infill      *float64
}

// parseProjectSettings reads only the global sliced layer height and
// infill density from the JSON project settings of current Studio
// archives; any other content rejects the entry. Project filament arrays
// and every other profile key are ignored.
func parseProjectSettings(data []byte) (projectData, error) {
	var pd projectData
	obj, err := decodeJSONObject(data, "project settings")
	if err != nil {
		return pd, err
	}
	if v, ok := obj["layer_height"]; ok {
		if f, ok := parseNumeric(v); ok && f > 0 {
			pd.layerHeight = &f
		}
	}
	if v, ok := obj["sparse_infill_density"]; ok {
		if f, ok := parsePercent(v); ok && f >= 0 && f <= 100 {
			pd.infill = &f
		}
	}
	return pd, nil
}

// decodePNG validates the archived plate render: header first, then the
// pixel cap, then a full decode before the original bytes are accepted
// for serving.
func decodePNG(data []byte) ([]byte, error) {
	cfg, err := png.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, archiveErr(catArchive, "png header")
	}
	if cfg.Width > maxPixelDim || cfg.Height > maxPixelDim {
		return nil, archiveErr(catOversized, "png dimensions")
	}
	if _, err := png.Decode(bytes.NewReader(data)); err != nil {
		return nil, archiveErr(catArchive, "png decode")
	}
	return data, nil
}

// decodeJSONObject decodes one JSON object with number preservation and
// rejects trailing content after the value. A valid JSON document that is
// not an object yields an empty result without error.
func decodeJSONObject(data []byte, detail string) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, archiveErr(catArchive, detail+" syntax")
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, archiveErr(catArchive, detail+" trailing")
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return map[string]any{}, nil
	}
	return obj, nil
}

// decodeEntitiesUpToTwice resolves HTML entities at most twice; model
// descriptions are sometimes double-escaped. The loop stops as soon as the
// text is stable, so repeated nesting cannot blow up.
func decodeEntitiesUpToTwice(s string) string {
	for range 2 {
		next := html.UnescapeString(s)
		if next == s {
			break
		}
		s = next
	}
	return s
}

// blockElements produce a paragraph boundary in extracted description text.
var blockElements = map[string]bool{
	"address": true, "article": true, "aside": true, "blockquote": true,
	"br": true, "dd": true, "div": true, "dl": true, "dt": true,
	"fieldset": true, "figcaption": true, "figure": true, "footer": true,
	"form": true, "h1": true, "h2": true, "h3": true, "h4": true,
	"h5": true, "h6": true, "header": true, "hr": true, "li": true,
	"main": true, "nav": true, "ol": true, "p": true, "pre": true,
	"section": true, "table": true, "tbody": true, "td": true,
	"tfoot": true, "th": true, "thead": true, "tr": true, "ul": true,
}

// htmlToPlainText reduces untrusted markup to plain text: entity-decoded
// text content with paragraph boundaries at block elements. Script and
// style contents are dropped; comments, doctypes, and attribute values
// never appear in the output. Malformed markup yields the text collected
// so far; the input is already bounded by the entry read cap.
func htmlToPlainText(s string) string {
	tk := html.NewTokenizer(strings.NewReader(s))
	var lines []string
	var cur strings.Builder
	appendSpace := false
	skip := ""
	appendText := func(txt string) {
		for _, r := range txt {
			if unicode.IsSpace(r) {
				if cur.Len() > 0 {
					appendSpace = true
				}
				continue
			}
			if appendSpace && cur.Len() > 0 {
				cur.WriteByte(' ')
			}
			cur.WriteRune(r)
			appendSpace = false
		}
	}
	brk := func() {
		if line := strings.TrimSpace(cur.String()); line != "" {
			lines = append(lines, line)
		}
		cur.Reset()
		appendSpace = false
	}
	for {
		tt := tk.Next()
		switch tt {
		case html.ErrorToken:
			brk()
			return strings.Join(lines, "\n")
		case html.TextToken:
			if skip != "" {
				continue
			}
			appendText(string(tk.Text()))
		case html.StartTagToken, html.SelfClosingTagToken, html.EndTagToken:
			name := strings.ToLower(tk.Token().Data)
			if skip != "" {
				if tt == html.EndTagToken && name == skip {
					skip = ""
				}
				continue
			}
			switch {
			case tt == html.StartTagToken && (name == "script" || name == "style"):
				skip = name
			case blockElements[name]:
				brk()
			}
		}
	}
}

// collapseSpace trims the text and reduces internal whitespace runs to
// single spaces.
func collapseSpace(s string) string {
	var b strings.Builder
	space := true
	for _, r := range s {
		if unicode.IsSpace(r) {
			space = true
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteRune(r)
		space = false
	}
	return b.String()
}

// capRunes bounds s to n runes and reports whether content was omitted; a
// value exactly at the cap is not truncation.
func capRunes(s string, n int) (string, bool) {
	if utf8.RuneCountInString(s) <= n {
		return s, false
	}
	return string([]rune(s)[:n]), true
}

// cappedID caps a non-nullable untrusted identifier and records
// truncation.
func cappedID(s string, trunc *bool) string {
	v, t := capRunes(collapseSpace(s), capIDRunes)
	if trunc != nil {
		*trunc = *trunc || t
	}
	return v
}

// cappedText normalizes untrusted text and caps it, returning nil for
// empty results and recording truncation in trunc when non-nil.
func cappedText(s string, n int, trunc *bool) *string {
	v, t := capRunes(collapseSpace(s), n)
	if trunc != nil {
		*trunc = *trunc || t
	}
	if v == "" {
		return nil
	}
	return &v
}

// cappedParagraph caps multi-line description text while keeping the
// paragraph line breaks htmlToPlainText produced: only outer space is
// trimmed and interior lines stay intact.
func cappedParagraph(s string, n int, trunc *bool) *string {
	v, t := capRunes(strings.TrimSpace(s), n)
	if trunc != nil {
		*trunc = *trunc || t
	}
	if v == "" {
		return nil
	}
	return &v
}

// anyString renders a decoded JSON scalar as its text form.
func anyString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	}
	return "", false
}

// parseNumeric accepts a JSON number or numeric string and only finite
// values; NaN and Inf spellings are rejected so measurements stay real.
func parseNumeric(v any) (float64, bool) {
	s, ok := anyString(v)
	if !ok {
		return 0, false
	}
	return parseNumericString(s)
}

// parsePercent accepts a number or numeric string with one optional
// trailing percent sign.
func parsePercent(v any) (float64, bool) {
	s, ok := anyString(v)
	if !ok {
		return 0, false
	}
	return parseNumericString(strings.TrimSuffix(strings.TrimSpace(s), "%"))
}

// parseNumericString parses a finite decimal number.
func parseNumericString(s string) (float64, bool) {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// normalizeColor keeps only #RRGGBB and #RRGGBBAA hex colors; any other
// notation or length becomes nil. A color never implies the installed
// spool.
func normalizeColor(s string) *string {
	t := strings.TrimPrefix(strings.TrimSpace(s), "#")
	if len(t) != 6 && len(t) != 8 {
		return nil
	}
	for _, r := range t {
		if !strings.ContainsRune("0123456789abcdefABCDEF", r) {
			return nil
		}
	}
	out := "#" + strings.ToUpper(t)
	return &out
}

// empty reports whether no archive field was accepted, so a nil-able
// job_metadata stays null. A non-null Metadata always has non-nil arrays.
func (m *Metadata) empty() bool {
	return m.Title == nil && m.Description == nil && m.Designer == nil &&
		m.License == nil && m.Plate == nil && len(m.Materials) == 0 &&
		len(m.Objects) == 0 && m.ObjectCount == nil && len(m.Warnings) == 0 &&
		m.Process == nil && !m.Truncated
}

// attrValue returns the attribute value by local name; namespace prefixes
// are ignored.
func attrValue(start *xml.StartElement, local string) string {
	for _, a := range start.Attr {
		if a.Name.Local == local {
			return a.Value
		}
	}
	return ""
}
