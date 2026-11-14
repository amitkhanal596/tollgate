# ADR 0004: A hand-written token bucket per (route, client)

Status: Accepted (Phase 0)

## Context

Routes can set `rate_limit: {requests_per_second, burst}`. The limit applies per client. The client is the API key's client ID or the JWT subject, or the client IP when the route has no auth. A request over the limit gets `429 Too Many Requests` with a correct `Retry-After` header. The limiter runs on every request, so it must be O(1) and cheap. It must also use bounded memory even when there are millions of distinct clients, for example when IPs are spoofed. The algorithm must be written from scratch.

## Options considered

1. **Fixed-window counter**: count requests per client per calendar second or minute. It is tiny and simple. But a client can send a full window's quota at 0:59 and another at 1:00, which is 2x the limit in a short burst. It also has no separate burst setting.
2. **Sliding-window log**: store a timestamp for every request in the window. It is exact, but memory per client is O(limit), which is unacceptable for high limits.
3. **Sliding-window counter**: a weighted mix of the current and previous fixed windows. It is O(1) and fairly smooth. But it is an approximation, it has no explicit burst setting, and the right `Retry-After` is awkward to compute.
4. **Token bucket**: each client has a bucket of up to `burst` tokens that refills at `rate` per second, and a request spends one token. It is O(1) with two numbers per client. Rate and burst map one-to-one onto our config, and `Retry-After` falls straight out of the arithmetic.
5. **GCRA**: one timestamp per client ("theoretical arrival time"). It is mathematically equivalent to a token bucket and uses even less memory, but it is harder to explain and to reason about.

There is a second, separate choice: **local versus global** state. A shared store like Redis gives an exact fleet-wide limit, but it adds a network hop to every request and a dependency that is not allowed.

## Decision

A **token bucket with lazy refill**, kept **locally in each gateway process**. `ratelimit.Limiter.Allow(key, limit) Decision`.

For each key, the bucket holds `tokens float64`, `last time.Time` and `fullAt time.Time`. On `Allow` at time `now`:

```
elapsed = max(0, now.Sub(last))            // monotonic clock; backwards = 0
tokens  = min(burst, tokens + elapsed.Seconds()*rate)
last    = now
if tokens >= 1 { tokens -= 1; allowed }    // check-and-consume under the shard lock
else          { retryAfter = (1 - tokens) / rate seconds }
fullAt  = now + (burst - tokens)/rate
```

- **Retry-After:** the time until one whole token exists, rounded **up** to whole seconds, with a minimum of 1. Rounding down would tell an obedient client to come back too early and earn a second 429. We also set `X-RateLimit-Limit` (burst) and `X-RateLimit-Remaining` (whole tokens left).
- **Key:** `route.Name + "\x00" + clientID`. Each route has its own bucket per client. The NUL separator stops two different (route, client) pairs from producing the same string.
- **Limit is passed on every call**, so it is never stored as config. When a route's rate changes, the next request uses the new rate, and tokens are clamped to the new burst. The limiter lives outside the config snapshot (ADR 0002), so buckets survive hot reloads.
- **Concurrency:** 32 shards, each a `sync.Mutex` and a `map[string]*bucket`, selected by an FNV-1a hash of the key. That gives short critical sections and little contention across cores.
- **Memory bound and eviction:** a bucket that has been idle until `fullAt` is full again, which makes it identical to a brand-new bucket, so **deleting it is lossless**. `Run(ctx)` sweeps the shards every `SweepInterval` (default 1 minute) and deletes buckets where `now >= fullAt`. A hard cap, `MaxKeys` (default 100k), protects against key floods. When it is reached, inserting a new key evicts an arbitrary bucket from the same shard (Go's map iteration order is random). The cost of that eviction is one client briefly getting a fresh burst, so we fail open for that client rather than refusing new ones. Each bucket is roughly 100 bytes plus its key, so the default cap is about 10–20 MB at most.
- **Clock injection:** `Options.Now` lets tests advance a fake clock instead of sleeping, so limiter tests are exact and fast.

## Consequences

- Good: O(1) time and about 100 bytes per active client, with idle clients cleaned up for free. Rate and burst are what operators already understand.
- Good: correct and exact `Retry-After`, and fully deterministic tests.
- Cost: **limits are per gateway replica.** Behind R replicas, a client can get up to R × rate if the external load balancer spreads its requests. Mitigations, in order of cost: document it; configure `rate / R`; later, a global limiter (Redis plus GCRA) or "local with periodic sync".
- Cost: at `MaxKeys` the limiter fails open for evicted clients. We accept that in exchange for bounded memory and never failing closed on legitimate new clients.
- Cost: there is a tiny floating-point error in `tokens`. It is irrelevant at human-scale rates and is pinned by tests at boundary values.
