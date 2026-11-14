package config

import (
	"net/url"
	"strings"
)

// Normalize fills defaults in place. It is idempotent.
func (c *Config) Normalize() {
	for i := range c.Routes {
		r := &c.Routes[i]
		// Hosts are matched case-insensitively; "*" and "" both mean any host.
		r.Host = strings.ToLower(r.Host)
		r.Host = strings.TrimSuffix(r.Host, ".") // "example.com." is the same host
		if strings.HasPrefix(r.Host, "[") && strings.HasSuffix(r.Host, "]") {
			r.Host = r.Host[1 : len(r.Host)-1] // request hosts are compared without IPv6 brackets
		}
		if r.Host == "*" {
			r.Host = ""
		}
		if r.Auth == "" {
			r.Auth = AuthNone
		}
	}
	for i := range c.Upstreams {
		u := &c.Upstreams[i]
		if u.Algorithm == "" {
			u.Algorithm = AlgorithmRoundRobin
		}
		for j, b := range u.Backends {
			if cb, ok := canonicalBackend(b); ok {
				u.Backends[j] = cb
			}
		}
		if h := u.HealthCheck; h != nil {
			if h.Interval == 0 {
				h.Interval = Duration(DefaultHealthInterval)
			}
			if h.Timeout == 0 {
				// Derived after interval is defaulted so a short interval alone
				// still satisfies timeout < interval.
				h.Timeout = min(Duration(DefaultHealthTimeout), h.Interval/2)
			}
			if h.HealthyThreshold == 0 {
				h.HealthyThreshold = DefaultHealthyThreshold
			}
			if h.UnhealthyThreshold == 0 {
				h.UnhealthyThreshold = DefaultUnhealthyThreshold
			}
		}
		if p := u.PassiveHealth; p != nil {
			if p.ConsecutiveFailures == 0 {
				p.ConsecutiveFailures = DefaultPassiveConsecutiveFailure
			}
			if p.Cooloff == 0 {
				p.Cooloff = Duration(DefaultPassiveCooloff)
			}
		}
	}
}

// canonicalBackend lower-cases scheme and host and drops the default port, so
// equivalent spellings compare equal. ok is false if s does not parse.
func canonicalBackend(s string) (string, bool) {
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return "", false
	}
	u.Scheme = strings.ToLower(u.Scheme)
	host, port := strings.ToLower(u.Hostname()), u.Port()
	if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		port = ""
	}
	if strings.Contains(host, ":") { // IPv6 literal needs brackets again
		host = "[" + host + "]"
	}
	u.Host = host
	if port != "" {
		u.Host += ":" + port
	}
	return u.String(), true
}
