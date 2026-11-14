package config

import (
	"reflect"
	"testing"
	"time"
)

func TestNormalizeDefaults(t *testing.T) {
	tests := []struct {
		name  string
		in    *Config
		check func(t *testing.T, c *Config)
	}{
		{
			name: "empty algorithm becomes round_robin, explicit kept",
			in:   &Config{Upstreams: []Upstream{{Name: "a"}, {Name: "b", Algorithm: AlgorithmLeastConn}}},
			check: func(t *testing.T, c *Config) {
				if c.Upstreams[0].Algorithm != AlgorithmRoundRobin {
					t.Errorf("a: %q", c.Upstreams[0].Algorithm)
				}
				if c.Upstreams[1].Algorithm != AlgorithmLeastConn {
					t.Errorf("b: %q", c.Upstreams[1].Algorithm)
				}
			},
		},
		{
			name: "empty auth becomes none, explicit kept",
			in:   &Config{Routes: []Route{{Name: "a"}, {Name: "b", Auth: AuthJWT}}},
			check: func(t *testing.T, c *Config) {
				if c.Routes[0].Auth != AuthNone || c.Routes[1].Auth != AuthJWT {
					t.Errorf("auth = %q, %q", c.Routes[0].Auth, c.Routes[1].Auth)
				}
			},
		},
		{
			name: "host lower-cased, star becomes empty",
			in:   &Config{Routes: []Route{{Host: "API.Example.COM"}, {Host: "*"}, {Host: ""}, {Host: "a.b:8080"}}},
			check: func(t *testing.T, c *Config) {
				got := []string{c.Routes[0].Host, c.Routes[1].Host, c.Routes[2].Host, c.Routes[3].Host}
				want := []string{"api.example.com", "", "", "a.b:8080"}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("hosts = %q want %q", got, want)
				}
			},
		},
		{
			name: "zero health check fields filled, path kept",
			in:   &Config{Upstreams: []Upstream{{Name: "a", HealthCheck: &HealthCheck{Path: "/h"}}}},
			check: func(t *testing.T, c *Config) {
				want := HealthCheck{
					Path: "/h", Interval: Duration(DefaultHealthInterval), Timeout: Duration(DefaultHealthTimeout),
					HealthyThreshold: DefaultHealthyThreshold, UnhealthyThreshold: DefaultUnhealthyThreshold,
				}
				if got := *c.Upstreams[0].HealthCheck; got != want {
					t.Errorf("got %+v want %+v", got, want)
				}
			},
		},
		{
			name: "explicit health check fields preserved",
			in: &Config{Upstreams: []Upstream{{Name: "a", HealthCheck: &HealthCheck{
				Path: "/h", Interval: Duration(time.Minute), HealthyThreshold: 9,
			}}}},
			check: func(t *testing.T, c *Config) {
				want := HealthCheck{
					Path: "/h", Interval: Duration(time.Minute), Timeout: Duration(DefaultHealthTimeout),
					HealthyThreshold: 9, UnhealthyThreshold: DefaultUnhealthyThreshold,
				}
				if got := *c.Upstreams[0].HealthCheck; got != want {
					t.Errorf("got %+v want %+v", got, want)
				}
			},
		},
		{
			name: "zero passive health fields filled",
			in:   &Config{Upstreams: []Upstream{{Name: "a", PassiveHealth: &PassiveHealth{}}}},
			check: func(t *testing.T, c *Config) {
				want := PassiveHealth{ConsecutiveFailures: DefaultPassiveConsecutiveFailure, Cooloff: Duration(DefaultPassiveCooloff)}
				if got := *c.Upstreams[0].PassiveHealth; got != want {
					t.Errorf("got %+v want %+v", got, want)
				}
			},
		},
		{
			name: "explicit passive health preserved",
			in:   &Config{Upstreams: []Upstream{{Name: "a", PassiveHealth: &PassiveHealth{ConsecutiveFailures: 2, Cooloff: Duration(time.Second)}}}},
			check: func(t *testing.T, c *Config) {
				want := PassiveHealth{ConsecutiveFailures: 2, Cooloff: Duration(time.Second)}
				if got := *c.Upstreams[0].PassiveHealth; got != want {
					t.Errorf("got %+v want %+v", got, want)
				}
			},
		},
		{
			name: "nil health blocks stay nil",
			in:   &Config{Upstreams: []Upstream{{Name: "a"}}},
			check: func(t *testing.T, c *Config) {
				if c.Upstreams[0].HealthCheck != nil || c.Upstreams[0].PassiveHealth != nil {
					t.Errorf("allocated: %+v", c.Upstreams[0])
				}
			},
		},
		{
			name: "route timeouts and defaults are not filled",
			in:   &Config{Routes: []Route{{Name: "a"}}},
			check: func(t *testing.T, c *Config) {
				if c.Routes[0].Timeouts != (Timeouts{}) || c.Defaults.Timeouts != (Timeouts{}) {
					t.Errorf("timeouts filled: %+v %+v", c.Routes[0].Timeouts, c.Defaults.Timeouts)
				}
			},
		},
		{
			name: "rate limit and auth blocks untouched",
			in:   &Config{Routes: []Route{{Name: "a"}}},
			check: func(t *testing.T, c *Config) {
				if c.Routes[0].RateLimit != nil || c.Auth.JWT != nil || c.Auth.APIKeys != nil {
					t.Errorf("allocated something: %+v", c)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.in.Normalize()
			tc.check(t, tc.in)
		})
	}
}

func TestNormalizeIdempotent(t *testing.T) {
	inputs := map[string]*Config{
		"empty": {},
		"full":  fullConfigNormalized(),
		"sparse": {
			Routes:    []Route{{Name: "a", Host: "X.Y"}, {Name: "b", Host: "*"}},
			Upstreams: []Upstream{{Name: "u", HealthCheck: &HealthCheck{Path: "/h"}, PassiveHealth: &PassiveHealth{}}},
		},
	}
	for name, c := range inputs {
		t.Run(name, func(t *testing.T) {
			c.Normalize()
			once := mustJSONClone(t, c)
			c.Normalize()
			if !reflect.DeepEqual(once, c) {
				t.Fatalf("not idempotent:\nonce:  %+v\ntwice: %+v", once, c)
			}
		})
	}
}

// mustJSONClone deep-copies via reflection-free struct copy using the yaml/json-free path:
// it re-parses nothing; Config has only value fields and pointers, so copy by hand.
func mustJSONClone(t *testing.T, c *Config) *Config {
	t.Helper()
	cp := *c
	cp.Routes = append([]Route(nil), c.Routes...)
	for i := range cp.Routes {
		if r := cp.Routes[i].RateLimit; r != nil {
			v := *r
			cp.Routes[i].RateLimit = &v
		}
	}
	cp.Upstreams = append([]Upstream(nil), c.Upstreams...)
	for i := range cp.Upstreams {
		if h := cp.Upstreams[i].HealthCheck; h != nil {
			v := *h
			cp.Upstreams[i].HealthCheck = &v
		}
		if p := cp.Upstreams[i].PassiveHealth; p != nil {
			v := *p
			cp.Upstreams[i].PassiveHealth = &v
		}
	}
	return &cp
}
