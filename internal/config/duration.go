package config

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Std converts to the standard library type.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// parseDuration accepts only Go duration strings with a unit ("2s",
// "150ms"); negatives are rejected here so every decode path agrees.
func parseDuration(s string) (Duration, error) {
	td, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q (want e.g. \"2s\" or \"150ms\"): %w", s, err)
	}
	if td < 0 {
		return 0, fmt.Errorf("invalid duration %q: must not be negative", s)
	}
	return Duration(td), nil
}

// MarshalJSON writes the duration as a string such as "2s".
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON accepts only JSON strings; a bare number would be
// ambiguous (seconds? nanoseconds?), so it is an error.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"2s\": %w", err)
	}
	v, err := parseDuration(s)
	if err != nil {
		return err
	}
	*d = v
	return nil
}

// MarshalYAML writes the duration as a string such as "2s".
func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}

// UnmarshalYAML parses a scalar duration string.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: duration must be a string like \"2s\"", n.Line)
	}
	v, err := parseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*d = v
	return nil
}
