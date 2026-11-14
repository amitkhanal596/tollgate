package config

import (
	"strings"
	"testing"
	"time"
)

var (
	keyA = strings.Repeat("a", 64)
	keyB = strings.Repeat("b", 64)
)

// fullYAML is a realistic document: 2 routes, 2 upstreams, api keys, JWT,
// rate limit, health checks. Host casing and "*" are deliberately raw.
var fullYAML = `
version: 7
defaults:
  timeouts:
    dial: 1s
    response_header: 5s
    request: 20s
auth:
  api_keys:
    - client_id: acme
      sha256: ` + keyA + `
    - client_id: globex
      sha256: ` + keyB + `
  jwt:
    algorithm: HS256
    hmac_secret_env: TOLLGATE_JWT_SECRET
    issuer: https://issuer.example.com
    audience: tollgate
    client_id_claim: sub
routes:
  - name: api
    host: API.Example.com
    path_prefix: /v1
    upstream: web
    auth: api_key
    rate_limit:
      requests_per_second: 10.5
      burst: 20
    timeouts:
      request: 15s
  - name: billing
    host: "*"
    path_prefix: /billing
    upstream: billing
    auth: jwt
upstreams:
  - name: web
    algorithm: least_conn
    backends:
      - http://127.0.0.1:8081
      - http://127.0.0.1:8082
    health_check:
      path: /healthz
      interval: 2s
      timeout: 500ms
      healthy_threshold: 2
      unhealthy_threshold: 4
    passive_health:
      consecutive_failures: 3
      cooloff: 10s
  - name: billing
    consul:
      service: billing
      tag: prod
      scheme: https
`

// fullConfigNormalized is what ParseYAML(fullYAML) must produce.
func fullConfigNormalized() *Config {
	return &Config{
		Version: 7,
		Defaults: Defaults{Timeouts: Timeouts{
			Dial:           Duration(time.Second),
			ResponseHeader: Duration(5 * time.Second),
			Request:        Duration(20 * time.Second),
		}},
		Auth: AuthSettings{
			APIKeys: []APIKey{{ClientID: "acme", SHA256: keyA}, {ClientID: "globex", SHA256: keyB}},
			JWT: &JWTSettings{
				Algorithm: JWTHS256, HMACSecretEnv: "TOLLGATE_JWT_SECRET",
				Issuer: "https://issuer.example.com", Audience: "tollgate", ClientIDClaim: "sub",
			},
		},
		Routes: []Route{
			{
				Name: "api", Host: "api.example.com", PathPrefix: "/v1", Upstream: "web",
				Auth:      AuthAPIKey,
				RateLimit: &RateLimit{RequestsPerSecond: 10.5, Burst: 20},
				Timeouts:  Timeouts{Request: Duration(15 * time.Second)},
			},
			{Name: "billing", Host: "", PathPrefix: "/billing", Upstream: "billing", Auth: AuthJWT},
		},
		Upstreams: []Upstream{
			{
				Name: "web", Algorithm: AlgorithmLeastConn,
				Backends: []string{"http://127.0.0.1:8081", "http://127.0.0.1:8082"},
				HealthCheck: &HealthCheck{
					Path: "/healthz", Interval: Duration(2 * time.Second), Timeout: Duration(500 * time.Millisecond),
					HealthyThreshold: 2, UnhealthyThreshold: 4,
				},
				PassiveHealth: &PassiveHealth{ConsecutiveFailures: 3, Cooloff: Duration(10 * time.Second)},
			},
			{
				Name: "billing", Algorithm: AlgorithmRoundRobin,
				Consul: &ConsulService{Service: "billing", Tag: "prod", Scheme: "https"},
			},
		},
	}
}

// validConfig is a fully explicit (already normalized) config that passes Validate.
func validConfig() *Config {
	return &Config{
		Version: 1,
		Auth: AuthSettings{
			APIKeys: []APIKey{{ClientID: "acme", SHA256: keyA}},
			JWT:     &JWTSettings{Algorithm: JWTHS256, HMACSecretEnv: "SECRET", ClientIDClaim: "sub"},
		},
		Routes: []Route{
			{Name: "api", Host: "api.example.com", PathPrefix: "/api", Upstream: "web", Auth: AuthAPIKey,
				RateLimit: &RateLimit{RequestsPerSecond: 10, Burst: 20}},
			{Name: "billing", PathPrefix: "/billing", Upstream: "billing", Auth: AuthJWT},
			{Name: "public", PathPrefix: "/", Upstream: "web", Auth: AuthNone},
		},
		Upstreams: []Upstream{
			{
				Name: "web", Algorithm: AlgorithmRoundRobin,
				Backends: []string{"http://127.0.0.1:8081", "https://backend.internal:8443"},
				HealthCheck: &HealthCheck{Path: "/healthz", Interval: Duration(5 * time.Second),
					Timeout: Duration(time.Second), HealthyThreshold: 2, UnhealthyThreshold: 3},
				PassiveHealth: &PassiveHealth{ConsecutiveFailures: 5, Cooloff: Duration(30 * time.Second)},
			},
			{Name: "billing", Algorithm: AlgorithmLeastConn, Consul: &ConsulService{Service: "billing"}},
		},
	}
}

// waitFor polls cond until it is true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", d, what)
}
