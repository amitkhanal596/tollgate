package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidateBaselinePasses(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("baseline should be valid: %v", err)
	}
}

// Each case mutates the valid baseline in exactly one way. wantInMsg must
// appear (case-insensitively) in the error so operators can find the field.
func TestValidateRejects(t *testing.T) {
	neg := Duration(-time.Second)
	tests := []struct {
		name      string
		mutate    func(c *Config)
		wantInMsg string
	}{
		{"empty route name", func(c *Config) { c.Routes[0].Name = "" }, "name"},
		{"duplicate route names", func(c *Config) { c.Routes[1].Name = "api" }, "api"},
		{"empty upstream name", func(c *Config) { c.Upstreams[0].Name = "" }, "name"},
		{"duplicate upstream names", func(c *Config) { c.Upstreams[1].Name = "web" }, "web"},
		{"unknown upstream reference", func(c *Config) { c.Routes[0].Upstream = "nope" }, "nope"},
		{"path_prefix without slash", func(c *Config) { c.Routes[0].PathPrefix = "api" }, "path_prefix"},
		{"empty path_prefix", func(c *Config) { c.Routes[0].PathPrefix = "" }, "path_prefix"},
		{"unknown auth mode", func(c *Config) { c.Routes[0].Auth = "basic" }, "auth"},
		{"unknown algorithm", func(c *Config) { c.Upstreams[0].Algorithm = "random" }, "algorithm"},
		{"upstream with neither backends nor consul", func(c *Config) { c.Upstreams[0].Backends = nil }, "backends"},
		{"upstream with both backends and consul", func(c *Config) { c.Upstreams[0].Consul = &ConsulService{Service: "x"} }, "consul"},
		{"backend ftp scheme", func(c *Config) { c.Upstreams[0].Backends = []string{"ftp://h:1"} }, "backends"},
		{"backend without scheme", func(c *Config) { c.Upstreams[0].Backends = []string{"127.0.0.1:8080"} }, "backends"},
		{"backend relative path", func(c *Config) { c.Upstreams[0].Backends = []string{"/x"} }, "backends"},
		{"backend without host", func(c *Config) { c.Upstreams[0].Backends = []string{"http://"} }, "backends"},
		{"backend with path", func(c *Config) { c.Upstreams[0].Backends = []string{"http://h:80/api"} }, "backends"},
		{"one bad backend among good", func(c *Config) { c.Upstreams[0].Backends = append(c.Upstreams[0].Backends, "nope") }, "backends"},
		{"rate zero", func(c *Config) { c.Routes[0].RateLimit.RequestsPerSecond = 0 }, "requests_per_second"},
		{"rate negative", func(c *Config) { c.Routes[0].RateLimit.RequestsPerSecond = -1 }, "requests_per_second"},
		{"burst zero", func(c *Config) { c.Routes[0].RateLimit.Burst = 0 }, "burst"},
		{"burst negative", func(c *Config) { c.Routes[0].RateLimit.Burst = -5 }, "burst"},
		{"negative route dial timeout", func(c *Config) { c.Routes[0].Timeouts.Dial = neg }, "dial"},
		{"negative route response_header timeout", func(c *Config) { c.Routes[0].Timeouts.ResponseHeader = neg }, "response_header"},
		{"negative route request timeout", func(c *Config) { c.Routes[0].Timeouts.Request = neg }, "request"},
		{"negative default dial timeout", func(c *Config) { c.Defaults.Timeouts.Dial = neg }, "dial"},
		{"health check path without slash", func(c *Config) { c.Upstreams[0].HealthCheck.Path = "healthz" }, "health_check"},
		{"health check zero healthy threshold", func(c *Config) { c.Upstreams[0].HealthCheck.HealthyThreshold = 0 }, "health_check"},
		{"health check zero unhealthy threshold", func(c *Config) { c.Upstreams[0].HealthCheck.UnhealthyThreshold = 0 }, "health_check"},
		{"health check zero timeout", func(c *Config) { c.Upstreams[0].HealthCheck.Timeout = 0 }, "health_check"},
		{"health check timeout equals interval", func(c *Config) {
			c.Upstreams[0].HealthCheck.Timeout = c.Upstreams[0].HealthCheck.Interval
		}, "health_check"},
		{"health check timeout above interval", func(c *Config) {
			c.Upstreams[0].HealthCheck.Timeout = Duration(time.Minute)
		}, "health_check"},
		{"api_key route without api keys", func(c *Config) { c.Auth.APIKeys = nil }, "api_keys"},
		{"jwt route without jwt settings", func(c *Config) { c.Auth.JWT = nil }, "jwt"},
		{"identical host and path_prefix", func(c *Config) {
			c.Routes[1].Host, c.Routes[1].PathPrefix = c.Routes[0].Host, c.Routes[0].PathPrefix
		}, "path_prefix"},
		{"api key empty client_id", func(c *Config) { c.Auth.APIKeys[0].ClientID = "" }, "client_id"},
		{"api key duplicate client_id", func(c *Config) {
			c.Auth.APIKeys = append(c.Auth.APIKeys, APIKey{ClientID: "acme", SHA256: keyB})
		}, "client_id"},
		{"api key sha256 empty", func(c *Config) { c.Auth.APIKeys[0].SHA256 = "" }, "sha256"},
		{"api key sha256 too short", func(c *Config) { c.Auth.APIKeys[0].SHA256 = keyA[:63] }, "sha256"},
		{"api key sha256 too long", func(c *Config) { c.Auth.APIKeys[0].SHA256 = keyA + "a" }, "sha256"},
		{"api key sha256 not hex", func(c *Config) { c.Auth.APIKeys[0].SHA256 = strings.Repeat("z", 64) }, "sha256"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(c)
			err := c.Validate()
			if err == nil {
				t.Fatal("expected an error")
			}
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("error does not wrap ErrInvalid: %v", err)
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.wantInMsg)) {
				t.Errorf("error %q does not mention %q", err, tc.wantInMsg)
			}
		})
	}
}

// Same path_prefix on different hosts, or same host with different prefixes, is fine.
func TestValidateAcceptsDistinctHostPathPairs(t *testing.T) {
	c := validConfig()
	c.Routes[1].PathPrefix = c.Routes[0].PathPrefix // same prefix, different host ("" vs api.example.com)
	if err := c.Validate(); err != nil {
		t.Fatalf("same prefix on different hosts must be valid: %v", err)
	}
}

// Validate reports every problem (errors.Join), each wrapping ErrInvalid.
func TestValidateReportsAllProblems(t *testing.T) {
	c := validConfig()
	c.Routes[0].Name = "dup"
	c.Routes[1].Name = "dup"
	c.Routes[2].Upstream = "ghost"
	c.Routes[0].RateLimit.Burst = 0
	err := c.Validate()
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("not ErrInvalid: %v", err)
	}
	for _, want := range []string{"dup", "ghost", "burst"} {
		if !strings.Contains(strings.ToLower(err.Error()), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}
