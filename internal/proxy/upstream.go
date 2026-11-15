package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// This file classifies upstream outcomes where they are actually known
// (docs/architecture.md sections 4 and 8, ADR 0006):
//
//   - observingTransport wraps the route's pooled transport. Passive health
//     is fed from RoundTrip's result (a response, or a transport error),
//     not from every ErrorHandler call: ReverseProxy also calls its
//     ErrorHandler for problems that never involved the backend.
//   - reqBody wraps the inbound request body, so a failure that is the
//     CLIENT's (malformed chunking, a reset, a stalled upload) is remembered
//     and is neither blamed on the backend nor answered with a 502.
//
// Both are per-request state embedded in proxyState (no extra allocation)
// and start no goroutines. The one goroutine involved is the
// context.AfterFunc callback (onDeadline), and it runs only if the request
// deadline fires while a request with a body is still in flight.

// errBodyAfterDeadline is returned by reqBody.Read once the request deadline
// has passed. It is the gateway refusing to read, not a client fault.
var errBodyAfterDeadline = errors.New("proxy: request deadline passed before the request body was read")

// observingTransport reports each upstream attempt's outcome to passive
// health. One per route, built with the route's ReverseProxy; it holds no
// per-request state (that is in the proxyState), so it allocates nothing.
type observingTransport struct {
	next  http.RoundTripper
	hooks *routeHooks
}

// RoundTrip runs on the goroutine serving the request (ReverseProxy calls it
// synchronously), so writing st.failStatus here and reading it in the
// ErrorHandler needs no synchronisation.
func (t *observingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	st := stateFrom(req.Context())
	if st == nil {
		return resp, err
	}
	if err != nil {
		status, report := classifyFailure(req.Context(), st, err)
		st.failStatus = status
		if report {
			t.hooks.observe(st, status, err)
		}
		return nil, err
	}
	// The outcome is taken at header time; a later body-copy error is not
	// reported (ADR 0006).
	t.hooks.observe(st, resp.StatusCode, nil)
	return resp, nil
}

// classifyFailure maps a failed RoundTrip to the status the client gets and
// whether the failure says something about the backend. Order matters: the
// client-side causes are checked first because a client fault also makes
// the transport fail (and can cancel the context).
func classifyFailure(ctx context.Context, st *proxyState, err error) (status int, backendFault bool) {
	if st.hasBody {
		// Our deadline fired while we were still waiting for the client's
		// body: the client is slow, the backend never had a full request.
		if st.body.stalled.Load() {
			return http.StatusRequestTimeout, false
		}
		// Malformed chunking, a reset or a half-close mid-body. Checked
		// before the 499 rule because a reset also cancels the context, and
		// "the client sent a broken body" is the more specific cause.
		if st.body.clientErr.Load() {
			return http.StatusBadRequest, false
		}
	}
	status = classifyProxyError(ctx, err)
	// Client-canceled (499) is never the backend's fault (section 11 row
	// 11). Our own deadline with a complete request (504), dial failures and
	// resets (502) are.
	return status, status != statusClientClosedRequest
}

// reqBody wraps the inbound request body for the upstream attempt.
//
// Concurrency: Read runs on the transport's write goroutine (or its body
// probe goroutine), Close on either that or the handler goroutine, and
// onDeadline on its own goroutine, while the handler goroutine reads the
// flags. Hence every field written after construction is atomic.
type reqBody struct {
	rc io.ReadCloser
	// ctx is the request-deadline context (set before the body is shared).
	ctx context.Context

	reading   atomic.Bool // a Read is blocked on the client right now
	eof       atomic.Bool // the body was read to the end
	clientErr atomic.Bool // a Read failed for a reason on the client's side
	closed    atomic.Bool // Close was called; later read errors are ours
	stalled   atomic.Bool // the deadline fired while we waited on the client
}

func (b *reqBody) Read(p []byte) (int, error) {
	// Set "reading" BEFORE checking the deadline: either this check sees
	// the deadline, or onDeadline (which runs after the deadline) sees
	// "reading". So a Read that starts around the deadline can never block
	// unnoticed and hang the handler.
	b.reading.Store(true)
	if b.ctx.Err() != nil {
		b.reading.Store(false)
		return 0, errBodyAfterDeadline
	}
	n, err := b.rc.Read(p)
	b.reading.Store(false)
	switch {
	case err == io.EOF:
		b.eof.Store(true)
	case err != nil && !b.closed.Load():
		// The inbound body only fails because of what the client sent (or
		// did not send); after our own Close the error is ours instead.
		b.clientErr.Store(true)
	}
	return n, err
}

func (b *reqBody) Close() error {
	b.closed.Store(true)
	return b.rc.Close()
}

// onDeadline runs (on its own goroutine, via context.AfterFunc) when the
// request context is done while a request with a body is in flight.
//
// If the deadline fired while a body Read is blocked on the client, the
// upload has stalled. That Read cannot be interrupted any other way, and it
// pins everything: the transport waits for its write goroutine before
// RoundTrip returns, and closing the server's body blocks on the same read.
// A read deadline in the past fails the Read at once, so the handler can
// answer 408 and release its in-flight slot. The connection is then
// unusable for further requests, which is why the 408 says Connection:
// close.
//
// If no Read is in progress, the transport is busy elsewhere (dialing,
// waiting for 100-continue, or blocked writing to a backend that stopped
// reading): that is the backend's fault and stays a 504.
func (st *proxyState) onDeadline() {
	if st.body.ctx.Err() != context.DeadlineExceeded {
		return // the client went away; its own reads fail by themselves
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	// finished: the handler has returned and w belongs to net/http again
	// (a read deadline set now would hit the connection's NEXT request).
	if st.finished || st.body.eof.Load() || !st.body.reading.Load() {
		return
	}
	st.body.stalled.Store(true) // before unblocking, so the Read's error is attributed
	// ErrNotSupported (e.g. a recorder in tests) leaves the read blocked;
	// nothing better is possible without a connection to poke.
	_ = http.NewResponseController(st.w).SetReadDeadline(time.Now())
}

// finishBody is deferred by ServeHTTP for requests with a body. After it
// returns, onDeadline can no longer touch w.
func (st *proxyState) finishBody(stop func() bool) {
	if stop() {
		return // onDeadline never ran and now never will
	}
	// onDeadline started (or finished); wait for it under the lock and
	// make any later part of it a no-op.
	st.mu.Lock()
	st.finished = true
	st.mu.Unlock()
}

// validUpgradeHeader reports whether every Upgrade header value is printable
// ASCII. httputil.ReverseProxy fails a request whose upgrade protocol is not
// (it calls the ErrorHandler, which would look like a 502 from the
// backend). Its check applies only with "Connection: upgrade" and only to
// the first value; checking every value unconditionally is a strict
// superset, so that failure can never happen inside the proxy.
func validUpgradeHeader(h http.Header) bool {
	for _, v := range h["Upgrade"] {
		for i := 0; i < len(v); i++ {
			if v[i] < ' ' || v[i] > '~' {
				return false
			}
		}
	}
	return true
}
