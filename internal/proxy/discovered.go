package proxy

import (
	"errors"
	"net"

	"github.com/amitkhanal596/tollgate/internal/discovery"
)

// Discovered endpoint validation (fix round 5, D3).
//
// Trust assumption (documented, not enforced): an address Consul reports
// for a referenced service is used as an egress target, and requests to it
// carry whatever the client sent that we forward (Authorization included).
// Whoever can register instances of that service in Consul can therefore
// receive those credentials; protecting the catalog (Consul ACLs) is the
// operator's job. What the gateway DOES enforce is that the address is a
// plain host:port, so a malformed or hostile registration cannot smuggle a
// path, query, userinfo or anything else into the backend URL we build.

// Errors of checkEndpointAddress; they end up in a WARN log line.
var (
	errEndpointNoPort = errors.New("not host:port")
	errEndpointPort   = errors.New("port must be 1-65535")
	errEndpointHost   = errors.New("host must be an IP address or a DNS name of [A-Za-z0-9-] labels")
)

// checkEndpointAddress accepts "host:port" where port is 1-65535 in plain
// decimal and host is an IP address or a DNS name: dot-separated labels of
// 1-63 bytes of [A-Za-z0-9-], at most 253 bytes in total.
func checkEndpointAddress(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return errEndpointNoPort
	}
	if !validPort(port) {
		return errEndpointPort
	}
	// net.ParseIP rejects IPv6 zones ("fe80::1%eth0"), which would need
	// escaping in a URL host; Consul instances are not addressed that way.
	if net.ParseIP(host) != nil || validDNSName(host) {
		return nil
	}
	return errEndpointHost
}

// validPort is stricter than strconv.Atoi, which would accept "+80".
func validPort(s string) bool {
	if len(s) == 0 || len(s) > 5 {
		return false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return false
		}
		n = n*10 + int(c-'0')
	}
	return n >= 1 && n <= 65535
}

func validDNSName(s string) bool {
	if len(s) == 0 || len(s) > 253 {
		return false
	}
	label := 0 // length of the current label
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '.':
			if label == 0 {
				return false // empty label: leading dot or ".."
			}
			label = 0
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', c == '-':
			label++
			if label > 63 {
				return false
			}
		default:
			return false
		}
	}
	return label > 0 // no trailing dot
}

// validEndpoints returns eps without the endpoints whose address fails
// checkEndpointAddress, logging one WARN per dropped endpoint. It runs once
// per endpoint set the resolver delivers (sets are only sent on change), so
// a bad registration is reported once per change, never per request. eps is
// returned as-is when everything is valid (sets are immutable, so sharing is
// fine); otherwise a new, non-nil slice.
func (rc *reconciler) validEndpoints(q discovery.Query, eps []discovery.Endpoint) []discovery.Endpoint {
	var out []discovery.Endpoint
	for i, ep := range eps {
		if err := checkEndpointAddress(ep.Address); err != nil {
			if out == nil {
				out = append(make([]discovery.Endpoint, 0, len(eps)), eps[:i]...)
			}
			rc.g.logger.Warn("ignoring discovered endpoint with an invalid address",
				"service", q.Service, "tag", q.Tag, "endpoint", ep.ID, "address", ep.Address, "err", err)
			continue
		}
		if out != nil {
			out = append(out, ep)
		}
	}
	if out == nil {
		return eps
	}
	return out
}
