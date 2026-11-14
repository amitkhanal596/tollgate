package config

import "strings"

// Normalize fills defaults in place. It is idempotent.
func (c *Config) Normalize() {
	for i := range c.Routes {
		r := &c.Routes[i]
		// Hosts are matched case-insensitively; "*" and "" both mean any host.
		r.Host = strings.ToLower(r.Host)
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
		if h := u.HealthCheck; h != nil {
			if h.Interval == 0 {
				h.Interval = Duration(DefaultHealthInterval)
			}
			if h.Timeout == 0 {
				h.Timeout = Duration(DefaultHealthTimeout)
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
