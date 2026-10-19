package jobpreview

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"image/png"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

// pair holds an XML metadata value.
type pair struct {
	// Key names the value.
	Key string `xml:"key,attr"`
	// Value contains sidecar text.
	Value string `xml:"value,attr"`
	// Name names model metadata.
	Name string `xml:"name,attr"`
	// Text contains model metadata text.
	Text string `xml:",chardata"`
}

// sidePlate holds one plate's slice facts.
type sidePlate struct {
	// Metadata contains plate properties.
	Metadata []pair `xml:"metadata"`
	// Filaments contains sliced material usage.
	Filaments []struct {
		ID    string `xml:"id,attr"`
		Type  string `xml:"type,attr"`
		Color string `xml:"color,attr"`
		UsedG string `xml:"used_g,attr"`
		UsedM string `xml:"used_m,attr"`
	} `xml:"filament"`
	// Objects contains sliced instances.
	Objects []struct {
		Name    string `xml:"name,attr"`
		Skipped bool   `xml:"skipped,attr"`
	} `xml:"object"`
	// Warnings contains archived slicer advisories.
	Warnings []struct {
		Message string `xml:"msg,attr"`
		Code    string `xml:"error_code,attr"`
	} `xml:"warning"`
}

// values maps sidecar metadata keys to values.
func (p sidePlate) values() map[string]string {
	values := make(map[string]string)
	for _, kv := range p.Metadata {
		values[kv.Key] = kv.Value
	}
	return values
}

// platePattern identifies plate selectors in filenames.
var platePattern = regexp.MustCompile(`plate_(\d+)`)

// plateNumber reads the plate number embedded in a path.
func plateNumber(name string) int {
	match := platePattern.FindStringSubmatch(name)
	if len(match) == 0 {
		return 0
	}
	n, _ := strconv.Atoi(match[1])
	return n
}

// parseArchive extracts the selected plate image and wall metadata.
func parseArchive(data []byte, k jobKey) (Result, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return Result{}, err
	}
	files := make(map[string]*zip.File)
	plates := make(map[int]bool)
	// sliced holds plates whose G-code the archive carries; a printer-bound 3MF has one.
	sliced := make(map[int]bool)
	for _, f := range zr.File {
		files[f.Name] = f
		if n := plateNumber(f.Name); n > 0 {
			plates[n] = true
			if f.Name == fmt.Sprintf("Metadata/plate_%d.gcode", n) {
				sliced[n] = true
			}
		}
	}
	plate := k.Plate
	if plate <= 0 {
		plate = plateNumber(k.File)
	}
	for _, candidates := range []map[int]bool{sliced, plates} {
		if plate <= 0 && len(candidates) == 1 {
			for n := range candidates {
				plate = n
			}
		}
	}
	if plate <= 0 {
		return Result{}, fmt.Errorf("plate selection unavailable")
	}
	budget := readCap
	read := func(name string) ([]byte, error) {
		f := files[name]
		if f == nil {
			return nil, nil
		}
		r, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer r.Close()
		b, err := io.ReadAll(io.LimitReader(r, budget+1))
		budget -= int64(len(b))
		if budget < 0 {
			return nil, fmt.Errorf("archive exceeds total read cap")
		}
		return b, err
	}
	image, err := read(fmt.Sprintf("Metadata/plate_%d.png", plate))
	if err != nil {
		return Result{}, err
	}
	if _, err := png.DecodeConfig(bytes.NewReader(image)); err != nil {
		return Result{}, err
	}
	md := &Metadata{Source: "printer_3mf", Plate: &Plate{Index: plate}, Materials: []Material{}, Objects: []Object{}, Warnings: []SlicerWarning{}}
	model, err := read("3D/3dmodel.model")
	if err != nil {
		return Result{}, err
	}
	var modelXML struct {
		Metadata []pair `xml:"metadata"`
	}
	if len(model) > 0 {
		if err := xml.Unmarshal(model, &modelXML); err != nil {
			return Result{}, err
		}
	}
	for _, kv := range modelXML.Metadata {
		switch strings.ToLower(kv.Name) {
		case "title":
			md.Title = text(kv.Text)
		case "description":
			md.Description = text(kv.Text)
		case "designer":
			md.Designer = text(kv.Text)
		case "license":
			md.License = text(kv.Text)
		}
	}
	for _, name := range []string{"Metadata/model_settings.config", "Metadata/slice_info.config"} {
		data, err := read(name)
		if err != nil {
			return Result{}, err
		}
		if len(data) == 0 {
			continue
		}
		var cf struct {
			Plates []sidePlate `xml:"plate"`
		}
		if err := xml.Unmarshal(data, &cf); err != nil {
			return Result{}, err
		}
		for _, p := range cf.Plates {
			values := p.values()
			id := values["index"]
			if id == "" {
				id = values["plater_id"]
			}
			if n, _ := strconv.Atoi(id); n != plate {
				continue
			}
			if name == "Metadata/model_settings.config" {
				md.Plate.Name = text(values["plater_name"])
				continue
			}
			md.Plate.EstimatedSeconds = number(values["prediction"])
			md.Plate.WeightG = number(values["weight"])
			md.Plate.FirstLayerSeconds = number(values["first_layer_time"])
			if b, err := strconv.ParseBool(values["support_used"]); err == nil {
				md.Plate.Supports = &b
			}
			for _, f := range p.Filaments {
				md.Materials = append(md.Materials, Material{ID: f.ID, Type: text(f.Type), Color: text(f.Color), UsedG: number(f.UsedG), UsedM: number(f.UsedM)})
			}
			count := 0
			groups := make(map[string]int)
			for _, o := range p.Objects {
				if o.Skipped {
					continue
				}
				count++
				if i, ok := groups[o.Name]; ok {
					md.Objects[i].Count++
				} else {
					groups[o.Name] = len(md.Objects)
					md.Objects = append(md.Objects, Object{Name: o.Name, Count: 1})
				}
			}
			md.ObjectCount = &count
			for _, w := range p.Warnings {
				code := w.Code
				if code == "" {
					code = w.Message
				}
				message := strings.ReplaceAll(w.Message, "_", " ")
				if w.Message == "bed_temperature_too_high_than_filament" {
					message = "Keep door open on enclosed printers to avoid nozzle clogs"
				}
				if message == "" {
					message = code
				}
				md.Warnings = append(md.Warnings, SlicerWarning{Code: code, Message: message})
			}
		}
	}
	for _, name := range []string{fmt.Sprintf("Metadata/plate_%d.json", plate), "Metadata/project_settings.config"} {
		data, err := read(name)
		if err != nil {
			return Result{}, err
		}
		if len(data) == 0 {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal(data, &obj); err != nil {
			return Result{}, err
		}
		if strings.HasSuffix(name, ".json") {
			bed, _ := obj["bed_type"].(string)
			if sub, ok := obj["plate"].(map[string]any); ok && bed == "" {
				bed, _ = sub["bed_type"].(string)
			}
			md.Plate.BedType = text(bed)
		} else {
			md.Process = &Process{LayerHeightMM: number(fmt.Sprint(obj["layer_height"])), InfillPercent: number(fmt.Sprint(obj["sparse_infill_density"]))}
		}
	}
	return Result{Preview: View{Status: StatusReady, JobName: k.Name}, Metadata: md, PNG: image}, nil
}

// number reads a finite nonnegative archive number or percent.
func number(s string) *float64 {
	n, err := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(s), "%"), 64)
	if err != nil || n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
		return nil
	}
	return &n
}

// text converts model-authored markup to plain display text.
func text(s string) *string {
	tokenizer := html.NewTokenizer(strings.NewReader(s))
	var out strings.Builder
	for {
		token := tokenizer.Next()
		if token == html.ErrorToken {
			break
		}
		if token == html.TextToken {
			out.Write(tokenizer.Text())
			out.WriteByte(' ')
		}
	}
	plain := strings.Join(strings.Fields(out.String()), " ")
	if plain == "" {
		return nil
	}
	return &plain
}
