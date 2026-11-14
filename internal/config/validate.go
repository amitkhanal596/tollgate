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
			if !(rl.RequestsPerSecond > 0) { // also rejects NaN
				bad("%s.rate_limit.requests_per_second must be > 0", where)
			}
			if rl.Burst < 1 {
				bad("%s.rate_limit.burst must be >= 1", where)
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
