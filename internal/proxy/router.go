package proxy

import (
	"net"
	"sort"
	"strings"
)

// router maps (host, path) to a route. It is built once per snapshot and is
// read-only afterwards, so Match needs no locking.
type router struct {
	// hosts holds routes for an exact (lower-case, port-less) host, sorted
	// by path prefix length, longest first.
	hosts map[string][]*routeRT
	// wildcard holds routes with no host (config "" or "*"), same order.
	wildcard []*routeRT
}

// newRouter indexes routes by host and orders each candidate list so that
// Match can return the first prefix that fits (= the longest one).
func newRouter(routes []*routeRT) router {
	rtr := router{hosts: make(map[string][]*routeRT)}
	for _, r := range routes {
		if r.host == "" {
			rtr.wildcard = append(rtr.wildcard, r)
		} else {
			rtr.hosts[r.host] = append(rtr.hosts[r.host], r)
		}
	}
	byLongestPrefix := func(rs []*routeRT) {
		// Stable so that (invalid, but possible) duplicate prefixes resolve
		// deterministically to the one listed first in the config.
		sort.SliceStable(rs, func(i, j int) bool { return len(rs[i].prefix) > len(rs[j].prefix) })
	}
	for _, rs := range rtr.hosts {
		byLongestPrefix(rs)
	}
	byLongestPrefix(rtr.wildcard)
	return rtr
}

// Match returns the route for a request, or nil. host is the raw Host
// header (any case, optional port); path is the decoded URL path.
//
// Exact-host routes are tried first and wildcard routes only if none of
// them fits the path, so a host-specific route always beats a wildcard
// route even when the wildcard has a longer prefix (config.Route docs).
func (rtr *router) Match(host, path string) *routeRT {
	if path == "" {
		path = "/"
	}
	if h := normalizeRequestHost(host); h != "" {
		if r := firstPrefixMatch(rtr.hosts[h], path); r != nil {
			return r
		}
	}
	return firstPrefixMatch(rtr.wildcard, path)
}

func firstPrefixMatch(rs []*routeRT, path string) *routeRT {
	for _, r := range rs {
		if prefixMatches(r.prefix, path) {
			return r
		}
	}
	return nil
}

// prefixMatches reports whether path is inside prefix on a segment
// boundary: "/api" matches "/api" and "/api/x" but not "/apix". A prefix
// that ends in "/" already marks a boundary, so a plain string prefix check
// is enough for it.
func prefixMatches(prefix, path string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	if len(path) == len(prefix) || strings.HasSuffix(prefix, "/") {
		return true
	}
	return path[len(prefix)] == '/'
}

// normalizeRequestHost lower-cases the Host header and strips the port, the
// IPv6 brackets and a trailing dot, so "API.Example.com.:8443" and
// "api.example.com" select the same routes.
func normalizeRequestHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	} else {
		// No port: SplitHostPort fails, but "[::1]" still has brackets.
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}
	host = strings.TrimSuffix(host, ".")
	// ToLower returns its input without allocating when it is already
	// lower case, which is the common case.
	return strings.ToLower(host)
}

// normalizeConfigHost maps a route's configured host to its router key:
// "" and "*" mean "any host" (key ""). config.Normalize already does this;
// repeating it keeps the router correct for hand-built configs too.
func normalizeConfigHost(host string) string {
	if host == "*" {
		return ""
	}
	return strings.ToLower(host)
}

// hasDotSegment reports whether a decoded path contains a "." or ".."
// segment. Such paths are rejected because the router would match the raw
// prefix ("/public/../admin" fits "/public") while the backend may resolve
// the dot segments to a different resource, letting a client bypass the
// per-route policy of the route the backend actually serves. Well-behaved
// clients remove dot segments before sending (RFC 3986 section 5.2.4).
func hasDotSegment(path string) bool {
	for len(path) > 0 {
		i := strings.IndexByte(path, '/')
		seg := path
		if i >= 0 {
			seg, path = path[:i], path[i+1:]
		} else {
			path = ""
		}
		if seg == "." || seg == ".." {
			return true
		}
	}
	return false
}
