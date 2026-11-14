package proxy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"slices"
	"strings"

	"github.com/amitkhanal596/tollgate/internal/balancer"
)

// HeaderRequestID carries the per-request correlation ID to the upstream
// and back to the client.
const HeaderRequestID = "X-Request-ID"

// HeaderClientID tells the upstream which authenticated client made the
// request. The gateway ALWAYS removes any inbound copy (every route, every
// spelling), so an upstream can trust it: it is only ever set by the
// gateway, and only from an authenticated Identity (Phase 3), never from
// the client IP (docs/architecture.md section 11 row 12).
const HeaderClientID = "X-Tollgate-Client-ID"

// statusClientClosedRequest is nginx's non-standard code for "the client
// went away before we answered". It is logged, and also written: if nothing
// were written, net/http would finish the response with an implicit
// "200 OK", which a half-closed client (or an access log in front of us)
// would see as success.
const statusClientClosedRequest = 499

// maxRequestIDLen bounds a client-supplied request ID so it cannot bloat
// logs or upstream headers.
const maxRequestIDLen = 128

// ServeHTTP is the data-plane request path (docs/architecture.md section 4).
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The ID is put on the response by writeError and ModifyResponse, not
	// here: ReverseProxy clears the response header map after relaying a
	// 1xx (e.g. 103 Early Hints), which would silently drop an early Set.
	reqID := requestID(r.Header.Get(HeaderRequestID))

	// One atomic load; this request uses this snapshot until it finishes,
	// even if a reload swaps in a newer one meanwhile (ADR 0002).
	s := g.snap.Load()
	if s == nil {
		writeError(w, http.StatusServiceUnavailable, reqID)
		return
	}

	// Before routing: the router must only ever see paths that every
	// upstream will interpret the same way (see checkPath).
	if err := checkPath(r.URL); err != nil {
		g.logger.Info("unsafe request path rejected", "request_id", reqID, "err", err)
		writeError(w, http.StatusBadRequest, reqID)
		return
	}
	// r.URL.Path is exactly what is forwarded (rewrite joins it onto the
	// backend URL via EscapedPath, which checkPath has verified encodes the
	// same segments), so the route we pick is the one the upstream serves.
	route := s.router.Match(r.Host, r.URL.Path)
	if route == nil {
		writeError(w, http.StatusNotFound, reqID)
		return
	}

	lb := route.upstream.lb
	b, err := lb.Next()
	if err != nil {
		// ErrNoHealthyBackends is the only error Next returns; 503 tells
		// clients the condition is temporary.
		g.logger.Warn("no healthy backend",
			"request_id", reqID, "route", route.name, "upstream", route.upstream.name, "err", err)
		writeError(w, http.StatusServiceUnavailable, reqID)
		return
	}
	// Done runs after the response body has been fully copied (or the
	// attempt failed, or ReverseProxy panicked with ErrAbortHandler), so
	// in-flight counts cover the whole exchange. It touches only b itself,
	// which stays valid even if a reload has since pruned b from the
	// Registry.
	defer lb.Done(b)

	// The request timeout bounds the whole exchange including streaming
	// the body. It derives from the client's context, so a client that
	// disconnects also cancels the upstream request.
	ctx, cancel := context.WithTimeout(r.Context(), route.timeouts.request)
	defer cancel()
	ctx = context.WithValue(ctx, proxyStateKey{}, &proxyState{
		backend:   b,
		requestID: reqID,
		route:     route.name,
	})
	route.proxy.ServeHTTP(w, r.WithContext(ctx))
}

// proxyState is what the shared per-route ReverseProxy hooks need to know
// about the current request. It travels in the request context because one
// ReverseProxy serves many concurrent requests.
type proxyState struct {
	backend   *balancer.Backend
	requestID string
	route     string
}

type proxyStateKey struct{}

func stateFrom(ctx context.Context) *proxyState {
	st, _ := ctx.Value(proxyStateKey{}).(*proxyState)
	return st
}

// newRouteProxy builds the ReverseProxy for one route. It holds no
// per-request state, so concurrent requests share it safely.
func newRouteProxy(route string, transport http.RoundTripper, buffers *bufferPool, logger *slog.Logger, errLog *log.Logger) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite:   rewrite,
		Transport: transport,
		// -1 flushes after every write to the client, so streamed
		// responses (SSE, long polls, chunked progress output) are not
		// held back in a buffer. Large copies bypass the buffer anyway, so
		// the extra flushes cost little.
		FlushInterval: -1,
		BufferPool:    buffers,
		// The client gets our ID, replacing any the upstream sent, so the
		// response carries exactly one. It is set here rather than on the
		// ResponseWriter up front because ReverseProxy copies these headers
		// after any 1xx, and clears the writer's header map after each 1xx.
		// (Phase 2 passive health hooks in here as well.)
		ModifyResponse: func(resp *http.Response) error {
			if st := stateFrom(resp.Request.Context()); st != nil {
				resp.Header.Set(HeaderRequestID, st.requestID)
			} else {
				resp.Header.Del(HeaderRequestID)
			}
			return nil
		},
		ErrorHandler: errorHandler(route, logger),
		// Routes ReverseProxy's own diagnostics into our structured log.
		ErrorLog: errLog,
	}
}

// rewrite points the outbound request at the backend chosen for it.
func rewrite(pr *httputil.ProxyRequest) {
	st := stateFrom(pr.In.Context())
	if st == nil {
		// Unreachable: ServeHTTP always sets the state. Leaving the URL
		// untouched makes the transport fail, which maps to a 502.
		return
	}
	// SetURL also clears Out.Host, so the backend sees its own host name.
	pr.SetURL(st.backend.URL)
	// ReverseProxy may have re-encoded the query (see queryIsClean); put
	// the client's exact bytes back when they are safe to forward.
	if q := pr.In.URL.RawQuery; queryIsClean(q) {
		pr.Out.URL.RawQuery = q
	}
	// With Rewrite, ReverseProxy strips the incoming X-Forwarded-For.
	// Copy it back so SetXForwarded appends the client IP to the chain
	// instead of replacing it. (It is forwarded for the upstream's
	// benefit only; the gateway itself does not trust it in v1.)
	if prior, ok := pr.In.Header["X-Forwarded-For"]; ok {
		pr.Out.Header["X-Forwarded-For"] = slices.Clone(prior)
	}
	pr.SetXForwarded()
	pr.Out.Header.Set(HeaderRequestID, st.requestID)
	// Anti-spoofing: the client must never be able to choose the identity
	// an upstream sees. Done on the outbound copy (handlers should not
	// mutate the inbound request), which is the last stop before the wire.
	stripHeader(pr.Out.Header, HeaderClientID)
}

// stripHeader removes every entry for name. Header.Del only removes the
// canonical key, but a header map built in code (not parsed off the wire)
// can hold non-canonical keys such as "x-tollgate-client-id", and the
// transport writes those out verbatim.
func stripHeader(h http.Header, name string) {
	h.Del(name)
	for k := range h {
		if strings.EqualFold(k, name) {
			delete(h, k) // deleting during range is safe in Go
		}
	}
}

// errorHandler maps transport failures to a status (docs/architecture.md
// section 8). It runs only before any response bytes have been written; a
// failure while streaming the body makes ReverseProxy abort the connection
// instead.
func errorHandler(route string, logger *slog.Logger) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		status := classifyProxyError(r.Context(), err)
		attrs := []any{"route", route, "status", status, "err", err}
		if st := stateFrom(r.Context()); st != nil {
			// The backend address is fine in logs; it never goes in a body.
			attrs = append(attrs, "request_id", st.requestID, "backend", st.backend.ID)
		}
		var reqID string
		if st := stateFrom(r.Context()); st != nil {
			reqID = st.requestID
		}
		if status == statusClientClosedRequest {
			// The client's choice, not a gateway or backend fault: log at
			// Info. Never reported to passive health (section 11 row 11).
			// We still write 499 so net/http does not send its implicit
			// 200 to a half-closed client; the write may simply fail.
			logger.Info("client canceled request", attrs...)
			writeError(w, status, reqID)
			return
		}
		logger.Warn("upstream request failed", attrs...)
		writeError(w, status, reqID)
	}
}

// classifyProxyError decides the status for a failed upstream attempt.
// The request context is checked first because it tells the two "context"
// outcomes apart: the parent (client) context was canceled, or our own
// request-timeout deadline fired.
func classifyProxyError(ctx context.Context, err error) int {
	switch ctx.Err() {
	case context.Canceled:
		return statusClientClosedRequest
	case context.DeadlineExceeded:
		return http.StatusGatewayTimeout
	}
	// A failed dial (refused, unreachable, or dial timeout) means we never
	// reached the backend: that is a bad gateway, not a slow backend.
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return http.StatusBadGateway
	}
	// The transport's ResponseHeaderTimeout surfaces as a net.Error whose
	// Timeout() is true.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return http.StatusGatewayTimeout
	}
	// Connection reset, EOF before headers, malformed response, ...
	return http.StatusBadGateway
}

// writeError writes a small, generic error carrying the request ID. Bodies
// never include backend addresses or error details; those go to the logs,
// joined by request ID.
func writeError(w http.ResponseWriter, status int, reqID string) {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	if reqID != "" {
		h.Set(HeaderRequestID, reqID)
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(statusText(status) + "\n"))
}

// statusText is http.StatusText plus the non-standard 499.
func statusText(status int) string {
	if status == statusClientClosedRequest {
		return "Client Closed Request"
	}
	return http.StatusText(status)
}

// requestID returns the client's ID if it is safe to propagate, otherwise a
// fresh random one. "Safe" means at most 128 bytes of letters, digits and
// "-_.:" (enough for UUIDs, hex, ULIDs and trace IDs), so a hostile value
// cannot inject header, log or HTML content or smuggle list separators.
func requestID(in string) string {
	if validRequestID(in) {
		return in
	}
	var b [16]byte
	// Since Go 1.24 rand.Read cannot fail (it crashes instead). On older
	// toolchains a failure leaves b zeroed, which is still a usable ID and
	// not worth failing the request over.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func validRequestID(s string) bool {
	if s == "" || len(s) > maxRequestIDLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case c == '-', c == '_', c == '.', c == ':':
		default:
			return false
		}
	}
	return true
}

// maxQueryParams mirrors net/url's default ParseQuery limit.
const maxQueryParams = 10000

// queryIsClean reports whether a raw query can be forwarded byte for byte:
// no ';' separators, only well-formed %XX escapes, and not absurdly many
// parameters. These are the checks httputil.ReverseProxy applies before
// deciding to re-encode a query (to stop parameter smuggling between
// parsers that disagree on ';').
//
// We repeat them because ReverseProxy re-encodes EVERY query whenever the
// urlmaxqueryparams GODEBUG is non-empty, and a module whose go.mod says
// "go 1.23" gets urlmaxqueryparams=0 as a compatibility default. Re-encoding
// turns "%20" into "+" and sorts keys, which breaks proxy transparency and
// signed URLs. Unclean queries still get ReverseProxy's sanitized version.
func queryIsClean(q string) bool {
	if strings.Count(q, "&")+1 > maxQueryParams {
		return false
	}
	for i := 0; i < len(q); {
		switch q[i] {
		case ';':
			return false
		case '%':
			if i+2 >= len(q) || !isHex(q[i+1]) || !isHex(q[i+2]) {
				return false
			}
			i += 3
		default:
			i++
		}
	}
	return true
}

func isHex(c byte) bool {
	return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F')
}
