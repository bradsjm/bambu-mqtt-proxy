package config

import (
	"context"
	"encoding/json"
	"fmt"

	"gopkg.in/yaml.v3"
)

// Section describes a module-owned top-level YAML section. Register it during
// program initialization. Hooks must not mutate configs except Save's out.
type Section struct {
	// Key names the YAML section, page API member, and test route segment.
	Key string
	// New returns a pointer to the zero section value for YAML decoding.
	New func() any
	// Validate checks the effective configuration.
	Validate func(c *Config) error
	// View returns the stored section's secret-free page representation.
	View func(file *Config) any
	// Save builds the file section from a submitted page member.
	Save func(submitted json.RawMessage, stored, out *Config) error
	// Test runs the optional page test action and reads stored settings only
	// when needed through stored.
	Test func(ctx context.Context, submitted json.RawMessage, stored func() (*Config, error)) error
}

// PageError selects the HTTP status and message of a section Save or Test error.
type PageError struct {
	Status  int
	Message string
}

func (e *PageError) Error() string { return e.Message }

var sections []Section

// RegisterSection registers a module section during program initialization.
// A duplicate key panics.
func RegisterSection(s Section) {
	for _, existing := range sections {
		if existing.Key == s.Key {
			panic("duplicate config section: " + s.Key)
		}
	}
	sections = append(sections, s)
}

// Sections returns a copy of the registered module sections.
func Sections() []Section { return append([]Section(nil), sections...) }

// Section decodes the named module section without changing the configuration.
func (c *Config) Section(key string) (any, error) {
	for _, s := range sections {
		if s.Key != key {
			continue
		}
		v := s.New()
		if c == nil {
			return v, nil
		}
		raw, present := c.Sections[key]
		if !present {
			return v, nil
		}
		if err := raw.Decode(v); err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		return v, nil
	}
	return nil, fmt.Errorf("unregistered config section: %s", key)
}

// SetSection stores one module-owned section.
func (c *Config) SetSection(key string, v any) {
	var node yaml.Node
	if err := node.Encode(v); err != nil {
		panic(fmt.Errorf("encode config section %s: %w", key, err))
	}
	if c.Sections == nil {
		c.Sections = make(map[string]yaml.Node)
	}
	c.Sections[key] = node
}

// ValidateEffective checks module requirements on the effective configuration.
func (c *Config) ValidateEffective() error {
	for _, s := range sections {
		if s.Validate != nil {
			if err := s.Validate(c); err != nil {
				return err
			}
		}
	}
	return nil
}
