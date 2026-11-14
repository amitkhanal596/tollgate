package proxy

import (
	"errors"
	"net/url"
	"strings"
)

// Reasons checkPath rejects a path. They are logged, never sent to the
// client (the client gets a generic 400).
var (
	errPathDotSegment   = errors.New("path has a '.' or '..' segment")
	errPathEmptySegment = errors.New("path has an empty segment")
	errPathSemicolon    = errors.New("path contains ';'")
	errPathBackslash    = errors.New("path contains a backslash")
	errPathEncodedSep   = errors.New("path contains an encoded '/', '\\' or '.'")
	errPathControlByte  = errors.New("path contains a control byte")
)

// checkPath rejects request paths that the router and an upstream could
// interpret differently (docs/architecture.md section 11 row 1). The router
// matches prefixes on u.Path; the upstream receives u.EscapedPath() and may
// normalise it (resolve dot segments, merge slashes, strip path parameters,
// treat '\' as '/', decode twice). If any of those normalisations can move
// the request to a different prefix, a client could reach a protected route
// through a public one, so such paths get a 400 before routing.
//
// The checks run on BOTH the decoded and the escaped form, so a forbidden
// character cannot hide behind percent-encoding in either direction.
func checkPath(u *url.URL) error {
	// EscapedPath (not RawPath) is what the transport writes on the wire:
	// it ignores a RawPath that is not a valid encoding of Path, so these
	// two strings cover everything the router sees and the upstream gets.
	for _, p := range [...]string{u.Path, u.EscapedPath()} {
		if err := checkPathBytes(p); err != nil {
			return err
		}
		if err := checkPathSegments(p); err != nil {
			return err
		}
	}
	return nil
}

// checkPathBytes looks for forbidden bytes and encoded separators in p.
func checkPathBytes(p string) error {
	for i := 0; i < len(p); i++ {
		switch c := p[i]; {
		case c < 0x20 || c == 0x7f:
			// Upstreams and log pipelines truncate at NUL or split on
			// CR/LF, so the path they act on differs from the one we routed.
			return errPathControlByte
		case c == ';':
			// Tomcat/Jetty strip ";params" per segment, so "/x/..;/admin"
			// becomes "/x/../admin" there (and then "/admin").
			return errPathSemicolon
		case c == '\\':
			// IIS, some Windows-backed and Node/Express stacks treat '\' as
			// '/', which turns "..\admin" into a dot segment.
			return errPathBackslash
		case c == '%' && i+2 < len(p):
			// An encoded '/', '\' or '.' in the raw path is decoded by
			// some upstreams before routing ("/public%2f..%2fadmin"); in
			// the decoded path it means the client double-encoded it to
			// survive one decode, aimed at upstreams that decode twice.
			switch strings.ToLower(p[i+1 : i+3]) {
			case "2f", "5c", "2e":
				return errPathEncodedSep
			}
		}
	}
	return nil
}

// checkPathSegments rejects dot segments and empty segments in p.
func checkPathSegments(p string) error {
	if p == "" || p == "/" {
		return nil // the router treats both as "/"
	}
	// The leading '/' starts the first segment; a single trailing '/' is
	// an ordinary directory-style path, not an empty segment. ("//" trims
	// to "", which the loop sees as one empty segment.)
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	for {
		seg, rest, more := strings.Cut(p, "/")
		switch seg {
		case ".", "..":
			// Every RFC 3986 client and most upstreams resolve dot
			// segments, so "/public/../admin" is served as "/admin" while
			// we would have routed it by the "/public" prefix.
			return errPathDotSegment
		case "":
			// nginx (merge_slashes), Spring and many frameworks collapse
			// "//", so "//admin" reaches "/admin" while we route it to "/".
			return errPathEmptySegment
		}
		if !more {
			return nil
		}
		p = rest
	}
}
