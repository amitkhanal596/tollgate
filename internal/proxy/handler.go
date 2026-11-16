package proxy

import (
	"bufio"
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
	"github.com/amitkhanal596/tollgate/internal/metrics"
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
// balancer -> proxy -> passive health, then metrics and the access log
// (step 7). It takes no locks: the snapshot is one atomic load, and the
// limiters, Backends and metric vectors are concurrency-safe.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// One allocation holds everything this request needs to remember: the
	// proxy state the ReverseProxy hooks read, and the status/bytes recorder.
	ex := &exchange{}
	// The ID is put on the response by writeError and ModifyResponse, not
	// here: ReverseProxy clears the response header map after relaying a
	// 1xx (e.g. 103 Early Hints), which would silently drop an early Set.
	ex.st.requestID = requestID(r.Header.Get(HeaderRequestID))
	if g.observing {
		ex.start = time.Now()
		ex.rec.ResponseWriter = w
		w = &ex.rec
		// Deferred, and without recover: it runs on every exit path,
		// including ReverseProxy's http.ErrAbortHandler panic when a body
		// copy fails mid-stream, and the panic then continues unchanged
		// (net/http still aborts the connection, and any other panic keeps
		// its original stack trace).
		defer g.record(ex, r)
	}
	g.serve(w, r, &ex.st)
}

// serve is ServeHTTP without the bookkeeping. st already carries the
// request ID; serve fills in what it learns (route, upstream, backend,
// client ID, snapshot version) as it goes, so the record is right whichever
// exit is taken.
func (g *Gateway) serve(w http.ResponseWriter, r *http.Request, st *proxyState) {
	reqID := st.requestID

	// One atomic load; this request uses this snapshot until it finishes,
	// even if a reload swaps in a newer one meanwhile (ADR 0002). That
	// includes its auth rules and limits: a request admitted under the old
	// rules finishes under them.
	s := g.snap.Load()
	if s == nil {
		writeError(w, http.StatusServiceUnavailable, reqID)
		return
	}
	st.version = s.Version

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
	st.route, st.upstream = route.name, route.upstream.name

	// Steps 2-3. Both run before the balancer, so a rejected request never
	// takes a round-robin turn or an in-flight slot.
	if !g.admit(w, r, route, st) {
		return
	}

	lb := route.upstream.lb
	b, err := lb.Next()
	if err != nil {
		// ErrNoHealthyBackends is the only error Next returns (every
		// backend down or ejected, or a discovered upstream with no
		// endpoints yet); 503 tells clients it is temporary.
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

// exchange is the per-request state, allocated once in ServeHTTP.
type exchange struct {
	st    proxyState
	rec   recorder  // wraps the client's writer only when observing
	start time.Time // set only when observing
}

// record emits the request's metrics and access-log line (section 4 step
// 7). It runs exactly once per request, from ServeHTTP's defer.
func (g *Gateway) record(ex *exchange, r *http.Request) {
	d := time.Since(ex.start)
	st := &ex.st
	status := ex.rec.status
	switch {
	case status == 0 && ex.rec.hijacked:
		// An upgraded connection: ReverseProxy wrote the backend's 101 to
		// the hijacked connection itself, never through WriteHeader.
		status = http.StatusSwitchingProtocols
	case status == 0:
		// Nothing was written: net/http answers an empty 200 on a normal
		// return (on a panic the connection is cut, but there is no better
		// code to report for a response that never started).
		status = http.StatusOK
	}
	// Names only (never paths, queries or client IDs): bounded cardinality.
	// An unmatched request has empty names, which metrics maps to "none".
	if ex.rec.hijacked {
		// Counted, but kept out of the latency histogram: d is how long the
		// upgraded socket stayed open (minutes for a websocket), not how
		// long a request took, and would wreck the percentiles.
		g.metrics.CountRequest(st.route, st.upstream, status)
	} else {
		g.metrics.ObserveRequest(st.route, st.upstream, status, d)
	}
	if g.accessLog == nil {
		return
	}
	var backend string
	if st.backend != nil {
		backend = st.backend.ID
	}
	remoteIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteIP = r.RemoteAddr // no port (e.g. a unix socket); log it as-is
	}
	// No headers, no query string, no credentials: Path excludes the query,
	// and ClientID is only ever an authenticated identity.
	metrics.LogAccess(r.Context(), g.accessLog, metrics.AccessRecord{
		RequestID:     st.requestID,
		Method:        r.Method,
		Host:          r.Host,
		Path:          r.URL.Path,
		Route:         st.route,
		Upstream:      st.upstream,
		Backend:       backend,
		ClientID:      st.clientID,
		RemoteIP:      remoteIP,
		Status:        status,
		Duration:      d,
		BytesOut:      ex.rec.bytes,
		ConfigVersion: st.version,
	})
}

// recorder captures the final status and the body bytes written to the
// client. It forwards Flush and exposes Unwrap, so streaming (ReverseProxy
// flushes through http.ResponseController) and Hijack for protocol
// upgrades keep working through it. Only the request's own goroutine
// writes to it, so its fields need no synchronisation.
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int64
	// hijacked is set when ReverseProxy takes over the connection, which it
	// does only to relay a protocol upgrade after the backend's 101.
	hijacked bool
}

func (rw *recorder) WriteHeader(code int) {
	// 1xx responses (103 Early Hints) are interim; the final status comes
	// later. 101 Switching Protocols is final for an upgrade.
	if rw.status == 0 && (code >= 200 || code == http.StatusSwitchingProtocols) {
		rw.status = code
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *recorder) Write(p []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK // net/http's implicit WriteHeader(200)
	}
	n, err := rw.ResponseWriter.Write(p)
	rw.bytes += int64(n)
	return n, err
}

// FlushError is what http.ResponseController.Flush calls first, so an
// unsupported or failed flush is reported instead of swallowed.
func (rw *recorder) FlushError() error {
	if rw.status == 0 {
		rw.status = http.StatusOK // flushing commits an implicit 200
	}
	return http.NewResponseController(rw.ResponseWriter).Flush()
}

// Flush implements http.Flusher for callers that type-assert it.
func (rw *recorder) Flush() { _ = rw.FlushError() }

// Hijack is what http.ResponseController.Hijack calls first (before
// Unwrap). It is the only reliable sign of an upgraded connection: after a
// 101 from the backend, ReverseProxy hijacks the client connection and
// writes the 101 there itself, so WriteHeader(101) is never called. Errors
// (including http.ErrNotSupported) pass through unchanged, so ReverseProxy
// still reports a writer that cannot hijack.
func (rw *recorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, brw, err := http.NewResponseController(rw.ResponseWriter).Hijack()
	if err == nil {
		rw.hijacked = true
	}
	return conn, brw, err
}

// Unwrap lets http.ResponseController reach the underlying writer
// (SetReadDeadline, ...).
func (rw *recorder) Unwrap() http.ResponseWriter { return rw.ResponseWriter }

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
	// route and upstream are the matched names ("" until a route matched);
	// version is the snapshot's config version (0 when not ready). They
	// exist for metrics and the access log.
	route    string
	upstream string
	version  int64
	// clientID is the authenticated identity of an ADMITTED request; ""
	// on auth: none routes, where nothing identifies the client to the
	// upstream, and on requests rejected by auth or the rate limit.
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
	route    string
	upstream string
	passive  *config.PassiveHealth // nil = passive health off for this upstream
	now      func() time.Time
	logger   *slog.Logger
	series   *backendSeries // counts ejections; never nil
}

// knownLengthFlushInterval is the longest a known-length response's headers
// or body bytes wait in the gateway's write buffer before being flushed.
const knownLengthFlushInterval = 100 * time.Millisecond

// newRouteProxy builds the ReverseProxy for one route. It holds no
// per-request state, so concurrent requests share it safely.
func (bd *builder) newRouteProxy(route, upstream string, transport http.RoundTripper, passive *config.PassiveHealth) *httputil.ReverseProxy {
	h := &routeHooks{route: route, upstream: upstream, passive: passive, now: bd.now, logger: bd.logger, series: bd.series}
	return &httputil.ReverseProxy{
		Rewrite: rewrite,
		// The wrapper is where passive health learns each attempt's outcome.
		Transport: &observingTransport{next: transport, hooks: h},
		// Only applies to known-length responses: ReverseProxy flushes
		// immediately by itself for text/event-stream and for unknown
		// length (ContentLength -1). Here -1 cost 2 write syscalls per
		// small response (39-40 writes for 20 responses; flush was 23% of
		// gateway CPU at 12k rps). 0 held the headers and up to ~4 KiB of
		// a slow known-length body until it completed (1.0 s for a long
		// poll). A positive interval bounds that delay, and a small
		// response is still one write: the copy finishes and stops the
		// flush timer before it fires.
		FlushInterval:  knownLengthFlushInterval,
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
	// ObservePassive ejects with a CAS, so exactly one caller sees true per
	// ejection: one log line and one counter increment.
	if health.ObservePassive(st.backend, *h.passive, status, err, h.now()) {
		h.logger.Warn("backend ejected after consecutive failures",
			"route", h.route, "upstream", h.upstream, "backend", st.backend.ID, "request_id", st.requestID,
			"cooloff", time.Duration(h.passive.Cooloff).String())
		h.series.ejected(st.backend)
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
