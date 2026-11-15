package proxy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/amitkhanal596/tollgate/internal/auth"
	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/health"
	"github.com/amitkhanal596/tollgate/internal/ratelimit"
)

// HeaderRequestID carries the per-request correlation ID to the upstream
// and back to the client.
const HeaderRequestID = "X-Request-ID"

// HeaderClientID tells the upstream which authenticated client made the
// request. The gateway ALWAYS removes any inbound copy (every route, every
// spelling), so an upstream can trust it: it is only ever set by the
// gateway, and only from an authenticated auth.Identity, never from the
// client IP (docs/architecture.md section 11 row 12).
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

// ServeHTTP is the data-plane request path (docs/architecture.md section
// 4): snapshot -> path check -> route match -> auth -> rate limit ->
// balancer -> proxy -> passive health. It takes no locks: the snapshot is
// one atomic load, and the limiters and Backends are concurrency-safe.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The ID is put on the response by writeError and ModifyResponse, not
	// here: ReverseProxy clears the response header map after relaying a
	// 1xx (e.g. 103 Early Hints), which would silently drop an early Set.
	reqID := requestID(r.Header.Get(HeaderRequestID))

	// One atomic load; this request uses this snapshot until it finishes,
	// even if a reload swaps in a newer one meanwhile (ADR 0002). That
	// includes its auth rules and limits: a request admitted under the old
	// rules finishes under them.
	s := g.snap.Load()
	if s == nil {
		writeError(w, http.StatusServiceUnavailable, reqID)
		return
	}

	// Requests the proxy could never forward are refused here, before the
	// balancer, so they cannot take an in-flight slot or be blamed on a
	// backend: ReverseProxy would hand both to its ErrorHandler, which used
	// to make them 502s and passive-health strikes.
	if r.Method == http.MethodConnect {
		// We are a reverse proxy, not a forward (tunnelling) proxy.
		writeError(w, http.StatusMethodNotAllowed, reqID)
		return
	}
	if !validUpgradeHeader(r.Header) {
		writeError(w, http.StatusBadRequest, reqID)
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

	// Allocated once and shared by admission and the ReverseProxy hooks.
	st := &proxyState{requestID: reqID, route: route.name}

	// Steps 2-3. Both run before the balancer, so a rejected request never
	// takes a round-robin turn or an in-flight slot.
	if !g.admit(w, r, route, st) {
		return
	}

	lb := route.upstream.lb
	b, err := lb.Next()
	if err != nil {
		// ErrNoHealthyBackends is the only error Next returns (every
		// backend down or ejected); 503 tells clients it is temporary.
		g.logger.Warn("no healthy backend",
			"request_id", reqID, "route", route.name, "upstream", route.upstream.name, "err", err)
		st.setRateLimitHeaders(w.Header())
		writeError(w, http.StatusServiceUnavailable, reqID)
		return
	}
	// Done runs after the response body has been fully copied (or the
	// attempt failed, or ReverseProxy panicked with ErrAbortHandler), so
	// in-flight counts cover the whole exchange. It touches only b itself,
	// which stays valid even if a reload has since pruned b from the
	// Registry.
	defer lb.Done(b)
	st.backend = b

	// The request timeout bounds the whole exchange including streaming
	// the body. It derives from the client's context, so a client that
	// disconnects also cancels the upstream request.
	deadlineCtx, cancel := context.WithTimeout(r.Context(), route.timeouts.request)
	defer cancel()
	out := r.WithContext(context.WithValue(deadlineCtx, proxyStateKey{}, st))
	// Only a request that announces a body (Content-Length > 0, or chunked
	// = -1) can stall or break mid-body; bodiless requests (most GETs) skip
	// the wrapper and the deadline callback entirely.
	if r.ContentLength != 0 && r.Body != nil && r.Body != http.NoBody {
		st.hasBody, st.w = true, w
		st.body.rc, st.body.ctx = r.Body, deadlineCtx
		out.Body = &st.body // the inbound r is left untouched
		// Runs only if the deadline fires mid-request (see onDeadline);
		// finishBody runs before cancel (LIFO), so a normal finish never
		// triggers it. Deferred, so it also runs on an ErrAbortHandler panic.
		defer st.finishBody(context.AfterFunc(deadlineCtx, st.onDeadline))
	}
	route.proxy.ServeHTTP(w, out)
}

// proxyState is what the shared per-route ReverseProxy hooks need to know
// about the current request. It travels in the request context because one
// ReverseProxy serves many concurrent requests. Its plain fields are only
// touched by the goroutine serving the request (ReverseProxy calls Rewrite,
// RoundTrip, ModifyResponse and ErrorHandler on it), so they need no
// synchronisation. The exceptions are body (read by the transport's write
// goroutine; atomic fields) and finished (shared with onDeadline; mu).
type proxyState struct {
	backend   *balancer.Backend
	requestID string
	route     string
	// clientID is the authenticated identity; "" on auth: none routes,
	// where nothing identifies the client to the upstream.
	clientID string
	// rl is the route's rate-limit decision, valid when limited is true.
	rl      ratelimit.Decision
	limited bool
	// observed is set once the attempt's outcome has gone to passive
	// health, so it is never counted twice.
	observed bool
	// failStatus is the status observingTransport chose for a failed
	// RoundTrip (0 if RoundTrip did not fail); ErrorHandler writes it.
	failStatus int

	// hasBody marks a request whose body is wrapped in body. w is the
	// client ResponseWriter, used only by onDeadline to unblock a stalled
	// read; set before body is shared.
	hasBody bool
	w       http.ResponseWriter
	body    reqBody
	// mu guards finished. A plain mutex, not an atomic: onDeadline must
	// finish its SetReadDeadline before the handler returns, and finishBody
	// waits for it by taking the lock. It is per request and contended
	// only on the rare deadline path.
	mu       sync.Mutex
	finished bool
}

type proxyStateKey struct{}

func stateFrom(ctx context.Context) *proxyState {
	st, _ := ctx.Value(proxyStateKey{}).(*proxyState)
	return st
}

// setRateLimitHeaders writes the X-RateLimit-* headers of a limited route
// into h; it does nothing on routes without a rate_limit.
func (st *proxyState) setRateLimitHeaders(h http.Header) {
	if st != nil && st.limited {
		setRateLimitHeaders(h, st.rl)
	}
}

// routeHooks are the ReverseProxy callbacks of one route. They hold only
// per-route settings; per-request data comes from the proxyState.
type routeHooks struct {
	route   string
	passive *config.PassiveHealth // nil = passive health off for this upstream
	now     func() time.Time
	logger  *slog.Logger
}

// newRouteProxy builds the ReverseProxy for one route. It holds no
// per-request state, so concurrent requests share it safely.
func (bd *builder) newRouteProxy(route string, transport http.RoundTripper, passive *config.PassiveHealth) *httputil.ReverseProxy {
	h := &routeHooks{route: route, passive: passive, now: bd.now, logger: bd.logger}
	return &httputil.ReverseProxy{
		Rewrite: rewrite,
		// The wrapper is where passive health learns each attempt's outcome.
		Transport: &observingTransport{next: transport, hooks: h},
		// -1 flushes after every write to the client, so streamed
		// responses (SSE, long polls, chunked progress output) are not
		// held back in a buffer. Large copies bypass the buffer anyway, so
		// the extra flushes cost little.
		FlushInterval:  -1,
		BufferPool:     bd.buffers,
		ModifyResponse: h.modifyResponse,
		ErrorHandler:   h.handleError,
		// Routes ReverseProxy's own diagnostics into our structured log.
		ErrorLog: bd.errLog,
	}
}

// modifyResponse runs once the upstream's final response headers have
// arrived, before anything is sent to the client.
func (h *routeHooks) modifyResponse(resp *http.Response) error {
	st := stateFrom(resp.Request.Context())
	if st == nil {
		resp.Header.Del(HeaderRequestID)
		return nil
	}
	// Set on resp.Header, not the ResponseWriter up front: ReverseProxy
	// copies these headers after any 1xx (and clears the writer's map after
	// each 1xx), and Set replaces whatever the upstream sent, so the client
	// gets exactly one request ID and our own rate-limit numbers.
	resp.Header.Set(HeaderRequestID, st.requestID)
	st.setRateLimitHeaders(resp.Header)
	// Step 6 (passive health) already ran in observingTransport.
	return nil
}

// observe reports the attempt's outcome to passive health, at most once.
func (h *routeHooks) observe(st *proxyState, status int, err error) {
	if h.passive == nil || st.observed {
		return
	}
	st.observed = true
	if health.ObservePassive(st.backend, *h.passive, status, err, h.now()) {
		h.logger.Warn("backend ejected after consecutive failures",
			"route", h.route, "backend", st.backend.ID, "request_id", st.requestID,
			"cooloff", time.Duration(h.passive.Cooloff).String())
	}
}

// rewrite points the outbound request at the backend chosen for it.
func rewrite(pr *httputil.ProxyRequest) {
	// Anti-spoofing and credential hygiene, first and unconditionally, on
	// the outbound copy (handlers should not mutate the inbound request),
	// which is the last stop before the wire: the client must never choose
	// the identity an upstream sees, and API keys are the gateway's
	// business only. Authorization is forwarded on purpose (section 4).
	stripHeaders(pr.Out.Header, HeaderClientID, auth.HeaderAPIKey)

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
	// benefit only; the gateway itself never trusts it in v1.)
	if prior, ok := pr.In.Header["X-Forwarded-For"]; ok {
		pr.Out.Header["X-Forwarded-For"] = slices.Clone(prior)
	}
	pr.SetXForwarded()
	pr.Out.Header.Set(HeaderRequestID, st.requestID)
	// Only an authenticated identity is ever sent; never the client IP
	// (section 11 row 12).
	if st.clientID != "" {
		pr.Out.Header.Set(HeaderClientID, st.clientID)
	}
}

// stripHeaders removes every entry for each of names. Header.Del only
// removes the canonical key, but a header map built in code (not parsed off
// the wire) can hold non-canonical keys such as "x-tollgate-client-id", and
// the transport writes those out verbatim.
func stripHeaders(h http.Header, names ...string) {
	for _, name := range names {
		h.Del(name)
	}
	for k := range h {
		for _, name := range names {
			if strings.EqualFold(k, name) {
				delete(h, k) // deleting during range is safe in Go
				break
			}
		}
	}
}

// handleError writes the error response for a failed attempt
// (docs/architecture.md section 8). It never reports to passive health:
// failed RoundTrips were already classified and reported by
// observingTransport, and every other call (a broken protocol upgrade after
// a 101 that was already counted) says nothing new about the backend. It
// runs only before any response bytes have been written; a failure while
// streaming the body makes ReverseProxy abort the connection instead.
func (h *routeHooks) handleError(w http.ResponseWriter, r *http.Request, err error) {
	st := stateFrom(r.Context())
	var status int
	if st != nil {
		status = st.failStatus
	}
	if status == 0 {
		status = classifyProxyError(r.Context(), err)
	}
	attrs := []any{"route", h.route, "status", status, "err", err}
	var reqID string
	if st != nil {
		// The backend address is fine in logs; it never goes in a body.
		attrs = append(attrs, "request_id", st.requestID, "backend", st.backend.ID)
		reqID = st.requestID
	}
	if status == statusClientClosedRequest {
		// The client's choice, not a gateway or backend fault: log at
		// Info, and NEVER report it to passive health, or clients that
		// hang up could eject healthy backends (section 11 row 11).
		// We still write 499 so net/http does not send its implicit 200
		// to a half-closed client; the write may simply fail.
		h.logger.Info("client canceled request", attrs...)
		writeError(w, status, reqID)
		return
	}
	if status == http.StatusRequestTimeout || status == http.StatusBadRequest {
		// The client's request body stalled (408) or was broken (400). The
		// connection's framing is unusable now (unread or malformed body,
		// and for 408 a read deadline in the past), so tell the client and
		// net/http to close it after this response.
		h.logger.Info("client request body failed", attrs...)
		w.Header().Set("Connection", "close")
		st.setRateLimitHeaders(w.Header())
		writeError(w, status, reqID)
		return
	}
	h.logger.Warn("upstream request failed", attrs...)
	st.setRateLimitHeaders(w.Header())
	writeError(w, status, reqID)
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
