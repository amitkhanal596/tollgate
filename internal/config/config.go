// Package config defines Tollgate's declarative configuration model and the
// ConfigSource abstraction that delivers successive configs to the gateway.
//
// The same structs are used for:
//   - the YAML file the data plane can run from standalone (Phases 1-3),
//   - the JSON documents the control plane stores in Postgres and streams
//     over SSE (Phase 4),
//   - tests.
//
// Dependency rule: config is a leaf. It imports only the standard library
// and the YAML library, never another internal package
// (docs/architecture.md).
//
// Planned functions (Phase 1):
//
//	// Parse decodes YAML (a superset of JSON), then calls Normalize and
//	// Validate. Unknown fields are an error. It is what FileSource uses.
//	func Parse(data []byte) (*Config, error)
//	// Normalize fills defaults in place: lower-cases hosts, maps Host "*"
//	// to "", sets Algorithm to round_robin when empty, sets Auth to
//	// "none" when empty, strips a trailing dot and IPv6 brackets from
//	// hosts, canonicalises backend URLs (lower-case scheme/host, default
//	// port removed), and fills zero health-check fields with the Default*
//	// constants below (timeout = min(DefaultHealthTimeout, interval/2)). It does not fill route timeouts; those are
//	// resolved at snapshot build time (route -> Defaults -> Default*).
//	func (c *Config) Normalize()
//	// Validate returns nil or an error joining (errors.Join) one error per
//	// problem, each wrapping ErrInvalid. Rules:
//	//   - route names unique and matching [A-Za-z0-9._-]{1,64} (a name is
//	//     one admin API path segment, so "/" would make it unaddressable);
//	//     host has no port; path_prefix is a clean literal path
//	//     (starts with "/", no percent-escapes, ";",
//	//     "//", "." or ".." segments, backslashes or control characters);
//	//     the (host, path_prefix) pair is unique; upstream exists;
//	//     auth is none|api_key|jwt; api_key needs >= 1 Auth.APIKeys entry;
//	//     jwt needs Auth.JWT; rate_limit has requests_per_second > 0 and
//	//     burst >= 1; timeouts >= 0.
//	//   - upstream names unique and matching [A-Za-z0-9._-]{1,64}; exactly
//	//     one of backends / consul is set; each backend is an absolute http(s) URL with a
//	//     host and no path, unique within the upstream after
//	//     canonicalisation; algorithm is round_robin|least_conn; health
//	//     thresholds >= 1; 0 < timeout < interval.
//	//   - API keys: client_id unique, sha256 64 lower-case hex and unique.
//	//   - auth.jwt, whenever present: algorithm is HS256|RS256; HS256
//	//     requires hmac_secret_env and forbids public_key_file; RS256
//	//     requires public_key_file and forbids hmac_secret_env.
//	//   - An empty config (no routes) is valid: the control plane starts
//	//     empty. FileSource additionally rejects a file with no routes.
//	func (c *Config) Validate() error
//
// Every ConfigSource delivers configs that have already been Normalized and
// Validated, so the gateway never sees an invalid config.
package config

import (
	"errors"
	"time"
)

// ErrInvalid is wrapped by every validation error, so callers can test
// errors.Is(err, config.ErrInvalid). The control plane maps it to 400.
var ErrInvalid = errors.New("config: invalid")

// Config is one complete, versioned gateway configuration. It is treated as
// immutable once a ConfigSource has delivered it: neither the source nor the
// consumer may modify it afterwards.
type Config struct {
	// Version identifies this config. It is assigned head+1 by the control
	// plane inside its advisory-lock transaction, so versions are strictly
	// increasing (ADR 0005). The file source delivers the file's value if it
	// is higher than the last one delivered, else last+1. Ignored on input
	// to the admin API.
	Version int64 `yaml:"version" json:"version"`

	// Defaults apply to every route that does not override them.
	Defaults Defaults `yaml:"defaults" json:"defaults"`

	// Auth holds the credentials routes can require.
	Auth AuthSettings `yaml:"auth" json:"auth"`

	// Routes map incoming requests to upstreams. Order is irrelevant:
	// matching is by specificity, not by position (see Route).
	Routes []Route `yaml:"routes" json:"routes"`

	// Upstreams are named pools of backends.
	Upstreams []Upstream `yaml:"upstreams" json:"upstreams"`
}

// Defaults are gateway-wide settings that routes inherit.
type Defaults struct {
	// Timeouts used when a route leaves a field at zero. Zero fields here
	// fall back to the Default*Timeout constants.
	Timeouts Timeouts `yaml:"timeouts" json:"timeouts"`
}

// Route matches requests by host and path prefix and sends them to an
// upstream, applying the route's auth and rate-limit policy.
//
// Matching rules (implemented by the proxy's router):
//  1. Host: the request host is lower-cased and its port stripped. Routes
//     whose Host equals it exactly are considered first. Only if none of
//     them matches the path are wildcard routes (Host == "") considered.
//     So a host-specific route always beats a wildcard route.
//  2. Path: among the candidate routes, the longest PathPrefix that matches
//     wins. Matching is on segment boundaries: "/api" matches "/api" and
//     "/api/users" but not "/apix". A prefix ending in "/" (including "/")
//     matches any path that starts with it.
//  3. No match: 404.
type Route struct {
	// Name is a unique, stable identifier. It is used in the rate-limit key
	// and as the Prometheus "route" label (bounded cardinality, unlike raw
	// paths), so renaming a route resets its buckets and metrics series.
	Name string `yaml:"name" json:"name"`

	// Host is an exact host name, or "" / "*" for any host.
	Host string `yaml:"host,omitempty" json:"host,omitempty"`

	// PathPrefix must start with "/". The path is forwarded unchanged.
	PathPrefix string `yaml:"path_prefix" json:"path_prefix"`

	// Upstream is the Name of an Upstream in the same Config.
	Upstream string `yaml:"upstream" json:"upstream"`

	// Auth is the authentication the route requires. Empty means none.
	Auth AuthMode `yaml:"auth,omitempty" json:"auth,omitempty"`

	// RateLimit is applied per client identity on this route. Nil means
	// the route is not rate limited.
	RateLimit *RateLimit `yaml:"rate_limit,omitempty" json:"rate_limit,omitempty"`

	// Timeouts override Defaults.Timeouts field by field (zero = inherit).
	Timeouts Timeouts `yaml:"timeouts" json:"timeouts"`
}

// AuthMode selects how a route authenticates clients.
type AuthMode string

// Supported auth modes.
const (
	// AuthNone: no authentication; rate limiting is keyed by client IP.
	AuthNone AuthMode = "none"
	// AuthAPIKey: the client sends a key in the X-API-Key header; the key's
	// ClientID is the identity used for rate limiting.
	AuthAPIKey AuthMode = "api_key"
	// AuthJWT: the client sends "Authorization: Bearer <jwt>"; the claim
	// named by JWTSettings.ClientIDClaim is the identity.
	AuthJWT AuthMode = "jwt"
)

// RateLimit is a per-route token-bucket policy (ADR 0004). Each client
// identity gets its own bucket on each route.
type RateLimit struct {
	// RequestsPerSecond is the sustained rate (refill rate). Must be > 0.
	RequestsPerSecond float64 `yaml:"requests_per_second" json:"requests_per_second"`
	// Burst is the bucket capacity. Must be >= 1.
	Burst int `yaml:"burst" json:"burst"`
}

// Timeouts bound how long the gateway waits on an upstream. A zero field
// means "inherit" (route -> Defaults -> built-in constant).
type Timeouts struct {
	// Dial bounds establishing the TCP (and TLS) connection to a backend.
	Dial Duration `yaml:"dial,omitempty" json:"dial,omitempty"`
	// ResponseHeader bounds the wait for the backend's response headers
	// after the request has been written.
	ResponseHeader Duration `yaml:"response_header,omitempty" json:"response_header,omitempty"`
	// Request bounds the whole proxied exchange, including streaming the
	// response body. It is a hard cap; there is no disabled value, so
	// streaming or long-poll routes must set a large explicit value such as
	// 1h. Enforced with a context deadline on the request.
	Request Duration `yaml:"request,omitempty" json:"request,omitempty"`
}

// Built-in defaults used when neither the route nor Defaults set a value.
const (
	DefaultDialTimeout           = 2 * time.Second
	DefaultResponseHeaderTimeout = 10 * time.Second
	DefaultRequestTimeout        = 30 * time.Second
)

// Duration is a time.Duration that is written in config as a Go duration
// string such as "250ms" or "2s".
//
// Phase 1 adds MarshalJSON/UnmarshalJSON and MarshalYAML/UnmarshalYAML
// methods that use time.ParseDuration / time.Duration.String. A named type
// is required because encoding/json would otherwise encode durations as
// integer nanoseconds, and yaml.v3 only special-cases the exact type
// time.Duration.
type Duration time.Duration

// Algorithm names a load-balancing policy (ADR 0003).
type Algorithm string

// Supported balancing algorithms.
const (
	AlgorithmRoundRobin Algorithm = "round_robin"
	AlgorithmLeastConn  Algorithm = "least_conn"
)

// Upstream is a named pool of backends plus how to balance and health-check
// them. Exactly one of Backends or Consul must be set.
type Upstream struct {
	// Name is unique within the Config and referenced by Route.Upstream.
	Name string `yaml:"name" json:"name"`

	// Algorithm defaults to round_robin.
	Algorithm Algorithm `yaml:"algorithm,omitempty" json:"algorithm,omitempty"`

	// Backends is a static list of base URLs, e.g. "http://10.0.0.7:8080".
	Backends []string `yaml:"backends,omitempty" json:"backends,omitempty"`

	// Consul, when set, takes the backend list from Consul's passing
	// instances of a service instead of from Backends (ADR 0007).
	Consul *ConsulService `yaml:"consul,omitempty" json:"consul,omitempty"`

	// HealthCheck enables active probing. Nil disables it, in which case
	// backends are considered actively healthy and only passive ejection
	// (if configured) removes them.
	HealthCheck *HealthCheck `yaml:"health_check,omitempty" json:"health_check,omitempty"`

	// PassiveHealth enables ejection on consecutive failures. Nil disables
	// it.
	PassiveHealth *PassiveHealth `yaml:"passive_health,omitempty" json:"passive_health,omitempty"`
}

// ConsulService selects backends from Consul's health API (ADR 0007).
type ConsulService struct {
	// Service is the Consul service name.
	Service string `yaml:"service" json:"service"`
	// Tag, if non-empty, keeps only instances carrying this tag.
	Tag string `yaml:"tag,omitempty" json:"tag,omitempty"`
	// Scheme used to build backend URLs from instance addresses. Default
	// "http".
	Scheme string `yaml:"scheme,omitempty" json:"scheme,omitempty"`
}

// HealthCheck configures active HTTP probing of every backend in an
// upstream (ADR 0006). A probe succeeds on a 2xx response within Timeout.
type HealthCheck struct {
	// Path is requested on each backend, e.g. "/healthz".
	Path string `yaml:"path" json:"path"`
	// Interval between probes of one backend.
	Interval Duration `yaml:"interval,omitempty" json:"interval,omitempty"`
	// Timeout for a single probe. Must be less than Interval. Default
	// min(1s, interval/2).
	Timeout Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	// HealthyThreshold ("rise") is the number of consecutive successes
	// that mark a down backend up again.
	HealthyThreshold int `yaml:"healthy_threshold,omitempty" json:"healthy_threshold,omitempty"`
	// UnhealthyThreshold ("fall") is the number of consecutive failures
	// that mark an up backend down.
	UnhealthyThreshold int `yaml:"unhealthy_threshold,omitempty" json:"unhealthy_threshold,omitempty"`
}

// PassiveHealth configures ejection based on real traffic (ADR 0006). A
// failure is a transport error (dial, reset, timeout) or a 5xx response.
type PassiveHealth struct {
	// ConsecutiveFailures that trigger ejection.
	ConsecutiveFailures int `yaml:"consecutive_failures,omitempty" json:"consecutive_failures,omitempty"`
	// Cooloff is how long an ejected backend receives no traffic.
	Cooloff Duration `yaml:"cooloff,omitempty" json:"cooloff,omitempty"`
}

// Health defaults applied by Normalize to zero fields.
const (
	DefaultHealthInterval            = 5 * time.Second
	DefaultHealthTimeout             = 1 * time.Second
	DefaultHealthyThreshold          = 2
	DefaultUnhealthyThreshold        = 3
	DefaultPassiveConsecutiveFailure = 5
	DefaultPassiveCooloff            = 30 * time.Second
)

// AuthSettings holds the credentials that routes with Auth != none check
// against.
type AuthSettings struct {
	// APIKeys are the accepted API keys. Only hashes are stored, so a leaked
	// config (database dump, SSE capture) does not leak usable keys.
	APIKeys []APIKey `yaml:"api_keys,omitempty" json:"api_keys,omitempty"`

	// JWT configures bearer-token validation. Required if any route uses
	// AuthJWT.
	JWT *JWTSettings `yaml:"jwt,omitempty" json:"jwt,omitempty"`
}

// APIKey is one accepted API key.
type APIKey struct {
	// ClientID names the client; it is the rate-limit identity and appears
	// in logs. Must be unique.
	ClientID string `yaml:"client_id" json:"client_id"`
	// SHA256 is the lower-case hex SHA-256 of the raw key. The gateway
	// hashes the presented key and looks the hash up in a map.
	SHA256 string `yaml:"sha256" json:"sha256"`
}

// JWTAlgorithm is an accepted JWT signing algorithm. Only the configured
// algorithm is accepted, which blocks "alg: none" and algorithm-confusion
// attacks.
type JWTAlgorithm string

// Supported JWT algorithms.
const (
	JWTHS256 JWTAlgorithm = "HS256"
	JWTRS256 JWTAlgorithm = "RS256"
)

// JWTSettings configures JWT validation. Key material is referenced, never
// embedded, so secrets are not stored in Postgres or sent over the stream.
type JWTSettings struct {
	// Algorithm is HS256 or RS256.
	Algorithm JWTAlgorithm `yaml:"algorithm" json:"algorithm"`
	// HMACSecretEnv names the environment variable holding the HS256 secret.
	HMACSecretEnv string `yaml:"hmac_secret_env,omitempty" json:"hmac_secret_env,omitempty"`
	// PublicKeyFile is a path to a PEM RSA public key for RS256.
	PublicKeyFile string `yaml:"public_key_file,omitempty" json:"public_key_file,omitempty"`
	// Issuer, if non-empty, must equal the "iss" claim.
	Issuer string `yaml:"issuer,omitempty" json:"issuer,omitempty"`
	// Audience, if non-empty, must be contained in the "aud" claim.
	Audience string `yaml:"audience,omitempty" json:"audience,omitempty"`
	// ClientIDClaim names the claim used as the client identity.
	// Default "sub".
	ClientIDClaim string `yaml:"client_id_claim,omitempty" json:"client_id_claim,omitempty"`
}
