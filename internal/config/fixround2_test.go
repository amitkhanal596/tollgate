package config

import (
	"errors"
	"math"
	"strings"
	"testing"
)

// D11: API-key client IDs are bounded and printable, and rate limits have an
// upper bound (a huge burst or rate makes the limiter meaningless and its
// float arithmetic lossy).
func TestValidateFixRound2(t *testing.T) {
	const max = 1_000_000
	tests := []struct {
		name      string
		mutate    func(c *Config)
		wantInMsg string // "" means the config must be valid
	}{
		{"client_id with CRLF", func(c *Config) { c.Auth.APIKeys[0].ClientID = "a\r\nX-Admin: 1" }, "auth.api_keys[0].client_id"},
		{"client_id with NUL", func(c *Config) { c.Auth.APIKeys[0].ClientID = "a\x00b" }, "auth.api_keys[0].client_id"},
		{"client_id with tab", func(c *Config) { c.Auth.APIKeys[0].ClientID = "a\tb" }, "auth.api_keys[0].client_id"},
		{"client_id with DEL", func(c *Config) { c.Auth.APIKeys[0].ClientID = "a\x7fb" }, "auth.api_keys[0].client_id"},
		{"client_id 257 bytes", func(c *Config) { c.Auth.APIKeys[0].ClientID = strings.Repeat("a", 257) }, "auth.api_keys[0].client_id"},
		{"client_id exactly 256 bytes", func(c *Config) { c.Auth.APIKeys[0].ClientID = strings.Repeat("a", 256) }, ""},
		{"client_id utf-8", func(c *Config) { c.Auth.APIKeys[0].ClientID = "müller" }, ""},
		{"burst above 1e6", func(c *Config) { c.Routes[0].RateLimit.Burst = max + 1 }, "burst"},
		{"burst exactly 1e6", func(c *Config) { c.Routes[0].RateLimit.Burst = max }, ""},
		{"rate above 1e6", func(c *Config) { c.Routes[0].RateLimit.RequestsPerSecond = max + 1 }, "requests_per_second"},
		{"rate exactly 1e6", func(c *Config) { c.Routes[0].RateLimit.RequestsPerSecond = max }, ""},
		{"rate +Inf", func(c *Config) { c.Routes[0].RateLimit.RequestsPerSecond = math.Inf(1) }, "requests_per_second"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(c)
			err := c.Validate()
			if tc.wantInMsg == "" {
				if err != nil {
					t.Fatalf("want valid, got %v", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
			if !strings.Contains(err.Error(), tc.wantInMsg) {
				t.Errorf("error %q does not name %q", err, tc.wantInMsg)
			}
		})
	}
}
