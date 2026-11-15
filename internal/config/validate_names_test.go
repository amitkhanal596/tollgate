package config

import (
	"errors"
	"strings"
	"testing"
)

// Orchestrator decision (Phase 4): route and upstream names must match
// [A-Za-z0-9._-]{1,64}. A name containing "/" cannot be addressed through the
// admin API path (/v1/routes/{name}), and spaces or "%" make URLs ambiguous.

// renameUpstream renames upstream i and every route that points at it.
func renameUpstream(c *Config, i int, name string) {
	old := c.Upstreams[i].Name
	c.Upstreams[i].Name = name
	for j := range c.Routes {
		if c.Routes[j].Upstream == old {
			c.Routes[j].Upstream = name
		}
	}
}

func TestValidateNamePattern(t *testing.T) {
	long64 := strings.Repeat("a", 64)
	long65 := strings.Repeat("a", 65)
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"empty", "", false},
		{"65 chars", long65, false},
		{"slash", "a/b", false},
		{"space", "a b", false},
		{"percent escape", "a%2f", false},
		{"non-ascii", "é", false},
		{"question mark", "a?b", false},
		{"backslash", `a\b`, false},
		{"newline", "a\nb", false},
		{"plain", "users-v2", true},
		{"dots and underscore", "a.b_c", true},
		{"64 chars", long64, true},
		{"single char", "x", true},
		{"digits first", "9lives", true},
	}
	kinds := []struct {
		kind  string
		apply func(c *Config, v string)
	}{
		{"route", func(c *Config, v string) { c.Routes[0].Name = v }},
		{"upstream", func(c *Config, v string) { renameUpstream(c, 0, v) }},
	}
	for _, k := range kinds {
		for _, tc := range tests {
			t.Run(k.kind+"/"+tc.name, func(t *testing.T) {
				c := validConfig()
				k.apply(c, tc.value)
				err := c.Validate()
				if tc.valid {
					if err != nil {
						t.Fatalf("%s name %q should be valid: %v", k.kind, tc.value, err)
					}
					return
				}
				if err == nil {
					t.Fatalf("%s name %q should be rejected", k.kind, tc.value)
				}
				if !errors.Is(err, ErrInvalid) {
					t.Errorf("error does not wrap ErrInvalid: %v", err)
				}
				if !strings.Contains(strings.ToLower(err.Error()), "name") {
					t.Errorf("error %q should mention the name field", err)
				}
			})
		}
	}
}
