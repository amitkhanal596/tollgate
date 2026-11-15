# ADR 0009: Classify backend failures at the round trip, not in the ErrorHandler

Status: Accepted (Phase 2 fix round). Refines ADR 0006 (passive health) and architecture.md §11 row 11.

## Context

Passive health (ADR 0006) ejects a backend after `consecutive_failures` failures in a row. Every failure must therefore be the *backend's* failure, or a client can eject a healthy backend on purpose.

The first implementation reported to passive health from `httputil.ReverseProxy.ErrorHandler`. The Phase 2 review showed that this was an outage: `ReverseProxy` calls its ErrorHandler for problems that never involved the backend, and the handler had already picked a backend before `ReverseProxy` looked at the request.

- Three `CONNECT /x` requests: `ReverseProxy` refuses them, the ErrorHandler answered 502 and counted three failures. The backend was ejected **with zero backend hits**, and the next normal GET got 503.
- The same with three `Upgrade: \xff` headers (ReverseProxy rejects a non-printable upgrade protocol) and with three malformed chunked bodies (the transport fails reading the *client's* body).
- A stalled upload (`Content-Length: 100`, 2 bytes sent) let the gateway's own request deadline fire. That looked like "backend too slow" (504, reported), although the backend never received a complete request. The handler also hung: after the deadline the transport waits for its write goroutine, which is blocked reading the client body.

So the question is: which outcomes say something about the backend, and where in the code is that actually known?

## Options considered

1. **Report from the proxy ErrorHandler** (the original design). One place, little code. But the ErrorHandler only receives an `error`: it cannot tell "the request was never sent" or "the client's body broke" from "the backend reset the connection". Every client-caused or pre-flight failure becomes a backend strike, which is exactly the outage above.
2. **Classify at the round trip and wrap the request body** (chosen). A `RoundTripper` wrapper around each route's pooled transport sees the attempt's real outcome: a response (status known at header time) or a transport error. A thin wrapper around the inbound body records whether a body read failed for a client reason, or stalled past the deadline. Requests that `ReverseProxy` would refuse are rejected before the balancer, so they never reach the transport at all. Costs a little per-request state and two small types.
3. **Count only 502/503/504 and transport errors** (Envoy's "gateway failure" mode). It narrows which *statuses* count, but it does not fix the problem: the CONNECT, Upgrade and chunking cases are transport-level errors that the ErrorHandler maps to 502, so they would still count. It also stops counting a backend that returns 500 on every request. Kept as a possible later tweak to ADR 0006, not a fix for this issue.

## Decision

Option 2.

**Pre-balancer rejections** (`internal/proxy/handler.go`, before route matching, so they never take an in-flight slot or a round-robin turn): `CONNECT` → 405; any `Upgrade` value with a byte outside printable ASCII → 400 (a strict superset of `ReverseProxy`'s own check); unsafe path → 400 (`checkPath`). Auth (401), throttles (429) and "no route" (404) are also decided before the balancer.

**`observingTransport`** (`internal/proxy/upstream.go`) wraps each route's transport. `ReverseProxy` calls it synchronously on the request goroutine, so it needs no locks and writes the per-request `proxyState`:

- Response received: report `status` (5xx = failure, anything else = success) at header time. A later body-copy error is not reported.
- Transport error: `classifyFailure` decides the client status and whether it is the backend's fault, in this order:
  1. the body wrapper saw the deadline fire while a body read was blocked on the client → **408**, not reported;
  2. a body read failed for a client reason (malformed chunking, reset, half-close) → **400**, not reported;
  3. the inbound context was canceled (client gone) → **499**, not reported;
  4. the gateway's own deadline with a complete request → 504, **reported**; a dial error → 502, reported; a response-header timeout → 504, reported; anything else (reset, EOF before headers) → 502, reported.
- The ErrorHandler only writes the status that the wrapper chose. It never reports to passive health.

**`reqBody`** wraps the inbound body only when the request announces one (`ContentLength != 0`). A `context.AfterFunc` on the request deadline checks whether a `Read` is blocked on the client right now. If so, it marks the body `stalled` and sets a read deadline in the past on the client connection. That is the only way to unblock the read, so the handler can answer 408 (with `Connection: close`) and release its in-flight slot. If no read is in progress, the transport is busy elsewhere (dialing, waiting for 100-continue, or writing to a backend that stopped reading). That is the backend's fault and stays a 504. Setting the read deadline up front was tried and rejected: it races the gateway's own deadline and can turn a slow-backend 504 into a 499.

## Consequences

- Good: a client can no longer eject a backend. Never reported: client cancel (499), client body errors (400), stalled uploads (408), and everything rejected before the balancer (400, 401, 404, 405, 429). Reported: backend 5xx, transport errors, and the gateway's deadline when the backend is the slow party.
- Good: the outcome is taken where it is known (header time), so streaming responses are judged by their status, not by how the stream ended.
- Good: the stalled-upload hang is gone. The handler always returns within the request deadline plus the time for one read to fail.
- Cost: a request **with a body** pays about six extra allocations (the `context.AfterFunc` registration and its deferred stop, plus related bookkeeping). The wrapper itself is embedded in the per-request state. Bodiless requests (most GETs) skip the wrapper and the callback entirely.
- Cost: precedence is a judgement call. A client that resets mid-upload both breaks the body and cancels the context. It is answered **400, not 499**, because "the client sent a broken body" is the more specific cause. Neither is reported, so this only affects the status in logs and metrics.
- Cost: after a 408 or 400 the client connection's framing is unusable, so the response carries `Connection: close`.
- Cost: more moving parts in the proxy (one RoundTripper, one body wrapper, one deadline callback guarded by a per-request mutex). Each is pinned by tests that replay the review's outage scenarios.
