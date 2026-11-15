package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// Validate returns nil or an errors.Join of one error per problem, each
// wrapping ErrInvalid. Messages name the offending field by its yaml key.
func (c *Config) Validate() error {
	var errs []error
	bad := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("%w: "+format, append([]any{ErrInvalid}, args...)...))
	}

	validateTimeouts("defaults.timeouts", c.Defaults.Timeouts, bad)

	upstreams := make(map[string]bool, len(c.Upstreams))
	for i, u := range c.Upstreams {
		where := fmt.Sprintf("upstreams[%d]", i)
		if u.Name == "" {
			bad("%s.name must not be empty", where)
		} else if !validName(u.Name) {
			bad("%s.name %q %s", where, u.Name, nameRule)
		} else if upstreams[u.Name] {
			bad("%s.name %q is duplicated", where, u.Name)
		}
		upstreams[u.Name] = true
		validateUpstream(where, u, bad)
	}

	usesAPIKey, usesJWT := false, false
	routeNames := make(map[string]bool, len(c.Routes))
	type hostPath struct{ host, path string }
	pairs := make(map[hostPath]bool, len(c.Routes))
	for i, r := range c.Routes {
		where := fmt.Sprintf("routes[%d]", i)
		if r.Name == "" {
			bad("%s.name must not be empty", where)
		} else if !validName(r.Name) {
			bad("%s.name %q %s", where, r.Name, nameRule)
		} else if routeNames[r.Name] {
			bad("%s.name %q is duplicated", where, r.Name)
		}
		routeNames[r.Name] = true

		if msg := checkPathPrefix(r.PathPrefix); msg != "" {
			bad("%s.path_prefix %q %s", where, r.PathPrefix, msg)
		}
		// A port would never match: the router strips the port from the request host.
		if _, _, err := net.SplitHostPort(r.Host); err == nil {
			bad("%s.host %q must not contain a port", where, r.Host)
		}
		// Two routes with the same (host, path_prefix) would be ambiguous.
		key := hostPath{r.Host, r.PathPrefix}
		if pairs[key] {
			bad("%s: path_prefix %q with host %q is duplicated", where, r.PathPrefix, r.Host)
		}
		pairs[key] = true

		if !upstreams[r.Upstream] {
			bad("%s.upstream %q does not match any upstream name", where, r.Upstream)
		}

		switch r.Auth {
		case AuthNone, "":
		case AuthAPIKey:
			usesAPIKey = true
		case AuthJWT:
			usesJWT = true
		default:
			bad("%s.auth %q must be none, api_key or jwt", where, r.Auth)
		}

		if rl := r.RateLimit; rl != nil {
			// "> 0 && <= max" also rejects NaN and +Inf: every comparison
			// with NaN is false, and +Inf is above the bound.
			if !(rl.RequestsPerSecond > 0 && rl.RequestsPerSecond <= MaxRateLimit) {
				bad("%s.rate_limit.requests_per_second must be > 0 and <= %d", where, MaxRateLimit)
			}
			if rl.Burst < 1 || rl.Burst > MaxRateLimit {
				bad("%s.rate_limit.burst must be >= 1 and <= %d", where, MaxRateLimit)
			}
		}
		validateTimeouts(where+".timeouts", r.Timeouts, bad)
	}

	if usesAPIKey && len(c.Auth.APIKeys) == 0 {
		bad("auth.api_keys must have at least one entry because a route uses auth api_key")
	}
	if usesJWT && c.Auth.JWT == nil {
		bad("auth.jwt must be set because a route uses auth jwt")
	}
	clients := make(map[string]bool, len(c.Auth.APIKeys))
	hashes := make(map[string]bool, len(c.Auth.APIKeys))
	for i, k := range c.Auth.APIKeys {
		where := fmt.Sprintf("auth.api_keys[%d]", i)
		if k.ClientID == "" {
			bad("%s.client_id must not be empty", where)
		} else if !ValidClientID(k.ClientID) {
			// The value is not echoed: it may hold control bytes or be huge.
			bad("%s.client_id %s", where, ClientIDRule)
		} else if clients[k.ClientID] {
			bad("%s.client_id %q is duplicated", where, k.ClientID)
		}
		clients[k.ClientID] = true
		// The gateway compares against lower-case hex of a SHA-256 digest.
		if b, err := hex.DecodeString(k.SHA256); err != nil || len(b) != 32 || k.SHA256 != strings.ToLower(k.SHA256) {
			bad("%s.sha256 must be 64 lower-case hex characters", where)
		} else if hashes[k.SHA256] {
			bad("%s.sha256 is duplicated", where)
		}
		hashes[k.SHA256] = true
	}
	if j := c.Auth.JWT; j != nil && j.Algorithm != JWTHS256 && j.Algorithm != JWTRS256 {
		bad("auth.jwt.algorithm %q must be HS256 or RS256", j.Algorithm)
	}

	return errors.Join(errs...)
}

// MaxRateLimit bounds rate_limit.burst and rate_limit.requests_per_second.
// The limiter keeps tokens as float64 nanotokens (tokens * 1e9): a burst
// near MaxInt is far beyond float64's exact-integer range, so the token
// arithmetic loses whole tokens and the int conversion behind
// X-RateLimit-Remaining overflows into nonsense. No real per-client limit is
// anywhere near a million per second, and at 1e6 * 1e9 = 1e15 the math is
// still exact (float64 is exact up to 2^53, about 9e15).
const MaxRateLimit = 1_000_000

// MaxClientIDLen bounds a client ID in bytes (API key client_id or JWT
// client-ID claim).
const MaxClientIDLen = 256

// ClientIDRule describes ValidClientID for error messages.
const ClientIDRule = "must be at most 256 bytes with no control characters"

// ValidClientID reports whether s is usable as a client identity: non-empty,
// at most MaxClientIDLen bytes, and free of ASCII control bytes (< 0x20 and
// 0x7f). The ID becomes the X-Tollgate-Client-ID upstream header, a
// rate-limit key and a log field, so CR/LF/NUL would be header injection
// (or a transport error that looks like a backend failure). Bytes >= 0x80
// are allowed, so UTF-8 names stay valid. It lives in config (not auth) so
// that Validate and the auth package enforce one shared rule.
func ValidClientID(s string) bool {
	if s == "" || len(s) > MaxClientIDLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

// maxNameLen bounds route and upstream names, which appear in admin API
// paths, metric labels and logs.
const maxNameLen = 64

const nameRule = "must be 1-64 characters of A-Z, a-z, 0-9, '.', '_' or '-'"

// validName reports whether s matches [A-Za-z0-9._-]{1,64}. Names are a
// single admin API path segment (/v1/routes/{name}), so "/" would make an
// object unaddressable, and spaces or "%" would make URLs ambiguous.
func validName(s string) bool {
	if len(s) == 0 || len(s) > maxNameLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isNameByte(s[i]) {
			return false
		}
	}
	return true
}

// isNameByte is the byte class [A-Za-z0-9._-] shared by names and node IDs.
func isNameByte(c byte) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '.' || c == '_' || c == '-'
}

func validateTimeouts(where string, t Timeouts, bad func(string, ...any)) {
	for _, f := range []struct {
		key string
		d   Duration
	}{{"dial", t.Dial}, {"response_header", t.ResponseHeader}, {"request", t.Request}} {
		if f.d < 0 {
			bad("%s.%s must be >= 0", where, f.key)
		}
	}
}

func validateUpstream(where string, u Upstream, bad func(string, ...any)) {
	switch u.Algorithm {
	case AlgorithmRoundRobin, AlgorithmLeastConn:
	default:
		bad("%s.algorithm %q must be round_robin or least_conn", where, u.Algorithm)
	}

	switch {
	case len(u.Backends) > 0 && u.Consul != nil:
		bad("%s: set exactly one of backends or consul, not both", where)
	case len(u.Backends) == 0 && u.Consul == nil:
		bad("%s: set exactly one of backends or consul", where)
	}
	seen := make(map[string]bool, len(u.Backends))
	for j, b := range u.Backends {
		if err := checkBackend(b); err != nil {
			bad("%s.backends[%d] %q: %v", where, j, b, err)
			continue
		}
		if cb, ok := canonicalBackend(b); ok {
			if seen[cb] {
				bad("%s.backends[%d] %q duplicates another backend of this upstream", where, j, b)
			}
			seen[cb] = true
		}
	}
	if cs := u.Consul; cs != nil {
		if cs.Service == "" {
			bad("%s.consul.service must not be empty", where)
		}
		if cs.Scheme != "" && cs.Scheme != "http" && cs.Scheme != "https" {
			bad("%s.consul.scheme %q must be http or https", where, cs.Scheme)
		}
	}

	if h := u.HealthCheck; h != nil {
		p := where + ".health_check"
		if !strings.HasPrefix(h.Path, "/") {
			bad("%s.path %q must start with \"/\"", p, h.Path)
		}
		if h.HealthyThreshold < 1 {
			bad("%s.healthy_threshold must be >= 1", p)
		}
		if h.UnhealthyThreshold < 1 {
			bad("%s.unhealthy_threshold must be >= 1", p)
		}
		if h.Timeout <= 0 || h.Timeout >= h.Interval {
			bad("%s: timeout (%v) must be > 0 and less than interval (%v)", p, h.Timeout.Std(), h.Interval.Std())
		}
	}
	if ph := u.PassiveHealth; ph != nil {
		p := where + ".passive_health"
		if ph.ConsecutiveFailures < 1 {
			bad("%s.consecutive_failures must be >= 1", p)
		}
		if ph.Cooloff <= 0 {
			bad("%s.cooloff must be > 0", p)
		}
	}
}

// checkBackend requires an absolute http(s) base URL with a host and no path.
func checkBackend(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return errors.New("not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("must be an absolute http or https URL")
	}
	if u.Hostname() == "" {
		return errors.New("missing host")
	}
	if u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return errors.New("must be a base URL with no path, query or credentials")
	}
	return nil
}

// checkPathPrefix returns "" if p is a clean literal path. The gateway answers
// 400 to request paths with these features before routing, so a prefix
// containing them could never match. A trailing "/" is allowed.
func checkPathPrefix(p string) string {
	if !strings.HasPrefix(p, "/") {
		return "must start with \"/\""
	}
	for i := 0; i < len(p); i++ {
		switch c := p[i]; {
		case c == '%':
			return "must not contain percent-escapes"
		case c == ';':
			return "must not contain \";\""
		case c == '\\':
			return "must not contain a backslash"
		case c < 0x20 || c == 0x7f:
			return "must not contain control characters"
		}
	}
	if strings.Contains(p, "//") {
		return "must not contain an empty segment"
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "." || seg == ".." {
			return "must not contain \".\" or \"..\" segments"
		}
	}
	return ""
}
