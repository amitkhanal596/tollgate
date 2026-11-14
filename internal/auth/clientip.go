package auth

import "net/netip"

// ipv6KeyBits is the IPv6 prefix length that forms one rate-limit identity.
// A single subscriber is usually handed a whole /64, so keying on the full
// address would let one client rotate through 2^64 fresh buckets.
const ipv6KeyBits = 64

// ClientIPKey turns an http.Request.RemoteAddr into the rate-limit identity
// for unauthenticated clients and for the failed-auth throttle.
//
//   - The port is stripped ("203.0.113.7:5000" -> "203.0.113.7").
//   - IPv4 addresses are returned as-is.
//   - IPv4-mapped IPv6 ("::ffff:203.0.113.7") is un-mapped to plain IPv4
//     first; otherwise every IPv4 client of a dual-stack listener would fall
//     into the same /64 bucket.
//   - Other IPv6 addresses are masked to their /64 and rendered as a prefix,
//     e.g. "2001:db8:1:2::/64". Any zone is dropped.
//   - Input that does not parse is returned unchanged.
//
// X-Forwarded-For is not consulted (not trusted in v1). ClientIPKey never
// panics and is safe for concurrent use.
func ClientIPKey(remoteAddr string) string {
	var addr netip.Addr
	if ap, err := netip.ParseAddrPort(remoteAddr); err == nil {
		addr = ap.Addr()
	} else if a, err := netip.ParseAddr(remoteAddr); err == nil {
		// A bare address (no port) still gets normalised so that the same
		// client always lands in the same bucket.
		addr = a
	} else {
		return remoteAddr
	}

	addr = addr.Unmap()
	if addr.Is4() {
		return addr.String()
	}
	// Zones are link-local interface names, not part of the client identity.
	return netip.PrefixFrom(addr.WithZone(""), ipv6KeyBits).Masked().String()
}
