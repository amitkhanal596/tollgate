# ADR 0011: Upstream connection pool: a large per-backend idle pool, no connection cap, and a 100 ms flush

Status: Accepted (Phase 8, after the Phase 8 review). Amends the transport rows of architecture.md §4 and §11 row 14. Pool ownership and reuse across swaps are unchanged (ADR 0002).

## Context

The gateway has one `http.Transport` per **transport key** (dial timeout, response-header timeout). The pool is owned by `proxy.Gateway` and reused across snapshots (ADR 0002). Every route and upstream whose timeouts resolve to the same key uses the same transport. `http.Transport` applies its per-host limits per backend `host:port` within each transport. So every per-host number below is **per backend per transport key**, and two routes with the same timeouts that reach the same backend share one pool.

Before Phase 8 the transport kept at most 64 idle connections per backend and 1024 idle in total, and set no limit on open connections. Each route's `ReverseProxy` had `FlushInterval: -1` (flush after every write).

**First full benchmark.** Through the gateway, one backend passed 8k rps but collapsed at 12k. Three backends behind round robin already failed at 4k. The investigation found that most of this came from the test rig. On a laptop under memory pressure the whole Docker VM stalled for 0.1 to 9 s at a time, and with three backends the 8 vCPUs were saturated at 12k. (The bench harness now detects both: a stall watchdog, CPU pinning and rig labels, see `bench/README.md`.) The gateway made it worse in two ways:

- **Connection churn.** With 64 idle connections per backend, every burst above 64 concurrent requests opened extra connections and then closed them when the burst ended, because the idle pool could not keep them. The next burst dialled them again. A VM stall makes the load generator pile up requests, which makes the bursts bigger. The worst run reached **2,505 upstream dials per second** and **17.6k sockets in TIME_WAIT**, against an ephemeral port range of about 28k.
- **Flushing.** A CPU profile at 12k rps showed `FlushInterval: -1` costing **23% of gateway CPU**: a small response took two write syscalls (headers flushed, then the body) instead of one.

**First fix, and what the review found.** The first fix set the idle pool and a new connection cap to the same value (`MaxIdleConnsPerHost` = `MaxConnsPerHost` = 1024) and `FlushInterval: 0`. The independent review tested the cap by lowering it to 4 in a scratch copy of the code:

- SSE streams hold a connection slot for as long as they stay open. Once the cap is reached, the next request **waits inside the transport**. Only the route's request deadline bounds that wait: the dial and response-header timeouts do not cover it.
- The client then gets a **504**. Passive health counts the 504 as a backend failure, and after 3 of them it **ejected a healthy backend**.
- Routes and upstreams that share a transport key share the pool. So one route's streams **starved another route** that sends to the same backend.
- Upgraded (101) connections did not hold a slot in that experiment. Long-lived plain HTTP responses, such as SSE, are the problem.

The review also found a problem with `FlushInterval: 0`. With 0, a known-length response stays in the server's write buffer until the body completes. That buffer holds the headers and up to about 4 KiB of the body. A long poll that sent its headers at once and its body later reached the client after **1.0 s**, all at once.

## Options considered

**Pool sizing:**

1. **Small idle pool (64 per backend, the original).** It caps idle file descriptors low. But every burst above 64 churns connections, which is the dial and TIME_WAIT storm above.
2. **Large idle pool plus a transport cap (`MaxConnsPerHost` = idle limit; the first fix).** It removes the churn and puts a hard ceiling on connections per backend. But the ceiling is enforced as an unbounded wait inside the transport, invisible to the gateway's error classification. That wait turns into 504s that blame a healthy backend, ejections, and starvation across routes that share the key (the review's experiment).
3. **Cap with a bounded slot wait, mapped to 503 and not counted against the backend.** It keeps a ceiling without the misattribution. But it needs a semaphore or wait queue in the request path, its own timeout, and a new failure class in `observingTransport`. That is more hot-path code for a limit nobody has asked for yet. Deferred.
4. **A per-backend max-in-flight ceiling at admission** (before the balancer commits, answering 503 without blaming the backend). This is the right place for a ceiling: the balancer already tracks in-flight counts per backend, and a rejection there is clearly the gateway's choice. It needs a config field and a decision about how it interacts with least-connections. Future work.
5. **Large idle pool, no connection cap** (chosen).

The global idle cap was considered separately. A global `MaxIdleConns` smaller than the sum of the per-backend limits evicts idle connections when several backends are busy at once, which brings the churn back. So it is 0 (no global cap).

**Flushing of known-length responses** (`ReverseProxy` always flushes `text/event-stream` and unknown-length responses immediately, whatever this setting is):

1. **`-1`, flush after every write (the original).** Nothing is ever held back, but a small response takes two write syscalls. That was 23% of gateway CPU at 12k rps.
2. **`0`, no periodic flush (the first fix).** A small response takes one write. But the headers and up to about 4 KiB of a slow known-length body are held until the body completes.
3. **A positive interval, 100 ms** (chosen). A small response still takes one write, because the copy finishes and stops the flush timer before it fires. A slow body is held for at most 100 ms.

## Decision

In `internal/proxy/transport.go`, for every pooled transport:

- `MaxIdleConnsPerHost` = **1024**: a burst of up to 1024 concurrent requests to one backend keeps every connection it opened. A healthy keep-alive connection is closed only by `IdleConnTimeout` (90 s, unchanged).
- `MaxIdleConns` = **0**: no global idle cap.
- `MaxConnsPerHost` left at **0**: no connection cap. A comment on the constants and a test (`TestUpstreamPoolHasNoConnectionCapAndKeepsBursts`) explain and pin this. If a concurrency ceiling is ever wanted, it goes in admission (option 4), not in the transport.

In `internal/proxy/handler.go`, every route's `ReverseProxy` has `FlushInterval` = `knownLengthFlushInterval` = **100 ms**.

Evidence and guards (`internal/proxy/perf_test.go`):

- `TestUpstreamConnectionsReusedUnderConcurrency` runs 4 rounds of 300 concurrent requests and requires that the backends accept at most 300 connections in total. With the old pool it fails: **1008** connections with one backend and **624** with three. It passes with the idle pool alone, without any cap, so the cap never contributed to the churn fix.
- `TestStreamsDoNotStarveOtherRoutesOnSameBackend` holds 50 SSE streams open to one backend through one route. A plain request through another route with the same transport key must still get a prompt 200, and the backend must stay in rotation. 50 is far below the old cap of 1024, so the test is there to catch a small cap being reintroduced.
- `TestSmallResponseIsOneWrite` requires 20 small known-length responses to take 20 client writes. With -1 it was 39-40.
- `TestKnownLengthHeadersAreNotHeldUntilBodyCompletes` and `TestKnownLengthTrickleReachesClientIncrementally` both fail with `FlushInterval: 0`.
- `TestStreamedResponsesAreNotBuffered` checks that SSE and chunked responses are relayed as they are produced.

Cost of the 100 ms interval: about 5 extra allocations and 300 B per known-length response, for the flush timer. `BenchmarkProxy` time per proxied request is unchanged (about 14 µs/op).

## Consequences

- Good: no connection churn for bursts of up to 1024 concurrent requests per backend. Connections are reused across bursts and across config swaps.
- Good: long-lived streams cannot starve other requests or routes, and the pool can never cause a 504 or an ejection: no request waits inside the transport for a connection slot.
- Good: small responses take one write. Slow known-length responses reach the client within 100 ms of each piece arriving.
- Cost: **idle connections are bounded per backend per transport key, not globally.** Each idle connection costs a file descriptor, two goroutines and their read/write buffers until it has been idle for 90 s. With very many discovered backends, times the number of transport keys, a full pool can press on the process's fd limit.
- Cost: above 1024 concurrent requests to one backend, the surplus is still dialled and closed on return, so it churns as before.
- Cost: **no per-backend concurrency ceiling** and no backpressure toward a slow backend, other than timeouts and least-connections steering traffic away. A slow backend can accumulate as many connections as requests arrive within one route request timeout. If that becomes a problem, the fix is option 4.
- Cost: about 5 allocations and 300 B more per known-length response.
- Lesson: **a performance fix needs the same independent review as a feature.** The first version of this fix traded a benchmark artifact (churn amplified by a stalling VM) for a production failure mode (healthy backends ejected and routes starved by long-lived streams). Only a test that reproduced the production scenario found it.
