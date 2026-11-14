package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// Parse decodes YAML (a superset of JSON), then Normalizes and Validates.
func Parse(data []byte) (*Config, error) { return ParseYAML(data) }

// ParseYAML strictly decodes a YAML document (unknown fields are errors),
// then Normalizes and Validates it.
func ParseYAML(data []byte) (*Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) { // yaml.v3 reports an empty document as EOF
			return nil, errors.New("config: empty document")
		}
		return nil, fmt.Errorf("config: decode yaml: %w", err)
	}
	return finish(&c)
}

// ParseJSON strictly decodes a JSON document (unknown fields are errors),
// then Normalizes and Validates it.
func ParseJSON(data []byte) (*Config, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("config: empty document")
		}
		return nil, fmt.Errorf("config: decode json: %w", err)
	}
	// Decode stops after the first value; trailing garbage must not pass.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("config: decode json: unexpected data after top-level value")
	}
	return finish(&c)
}

func finish(c *Config) (*Config, error) {
	c.Normalize()
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return c, nil
}
