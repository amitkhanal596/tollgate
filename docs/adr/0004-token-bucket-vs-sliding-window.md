# ADR 0004: A hand-written token bucket per (route, client)

Status: Accepted (Phase 0; amended after the Phase 0 review, see architecture.md §11 items 10 and 18)

## Context

Routes can set `rate_limit: {requests_per_second, burst}`. The limit applies per client. The client is the API key's client ID or the JWT subject. When the route has no auth, the client is the client IP, with IPv6 addresses masked to their /64 prefix (a single host usually controls a whole /64, so per-address keys would be trivially bypassed). A request over the limit gets `429 Too Many Requests` with a correct `Retry-After` header. The limiter runs on every request, so it must be O(1) and cheap. It must also use bounded memory even when there are millions of distinct clients, for example when IPs are spoofed, and running out of room must not become a way around the limit. The algorithm must be written from scratch.

## Options considered

1. **Fixed-window counter**: count requests per client per calendar second or minute. It is tiny and simple. But a client can send a full window's quota at 0:59 and another at 1:00, which is 2x the limit in a short burst. It also has no separate burst setting.
2. **Sliding-window log**: store a timestamp for every request in the window. It is exact, but memory per client is O(limit), which is unacceptable for high limits.
3. **Sliding-window counter**: a weighted mix of the current and previous fixed windows. It is O(1) and fairly smooth. But it is an approximation, it has no explicit burst setting, and the right `Retry-After` is awkward to compute.
4. **Token bucket**: each client has a bucket of up to `burst` tokens that refills at `rate` per second, and a request spends one token. It is O(1) with two numbers per client. Rate and burst map one-to-one onto our config, and `Retry-After` falls straight out of the arithmetic.
5. **GCRA**: one timestamp per client ("theoretical arrival time"). It is mathematically equivalent to a token bucket and uses even less memory, but it is harder to explain and to reason about.

There is a second, separate choice: **local versus global** state. A shared store like Redis gives an exact fleet-wide limit, but it adds a network hop to every request and a dependency that is not allowed.

## Decision

A **token bucket with lazy refill**, kept **locally in each gateway process**. `ratelimit.Limiter.Allow(key, limit) Decision`, plus a read-only `Peek(key, limit) Decision` used by the failed-auth throttle below.

For each key, the bucket holds `tokens float64`, `last time.Time` and `fullAt time.Time`. On `Allow` at time `now`:

```
elapsed = max(0, now.Sub(last))            // monotonic clock; backwards = 0
tokens  = min(burst, tokens + elapsed.Seconds()*rate)   // rate and burst of the limit passed to THIS call
last    = now
if tokens >= 1 { tokens -= 1; allowed }    // check-and-consume under the shard lock
else          { retryAfter = (1 - tokens) / rate seconds }
fullAt  = now + (burst - tokens)/rate
```

`Peek` runs the same refill arithmetic on a copy and returns the Decision that `Allow` would return, without consuming a token, without writing the bucket back and without creating a bucket. An unknown key peeks as `Allowed` with `Remaining = burst`.

- **Retry-After:** the time until one whole token exists, rounded **up** to whole seconds, with a minimum of 1. Rounding down would tell an obedient client to come back too early and earn a second 429. We also set `X-RateLimit-Limit` (burst) and `X-RateLimit-Remaining` (whole tokens left).
- **Key:** `route.Name + "\x00" + identity`. Each route has its own bucket per client. The NUL separator stops two different (route, client) pairs from producing the same string. The identity is the authenticated `ClientID`, or for `auth: none` routes the client-IP key: the IP from `RemoteAddr` with the port stripped, IPv4 unchanged, IPv4-mapped IPv6 (`::ffff:a.b.c.d`) un-mapped to IPv4 first, and other IPv6 masked to /64 (for example `2001:db8:1:2::/64`). Un-mapping must happen before masking, or every IPv4 client of a dual-stack listener would share one /64 bucket.
- **Limit is passed on every call**, so it is never stored as config. When a route's rate changes, the next request uses the new rate, and tokens are clamped to the new burst. The limiter lives outside the config snapshot (ADR 0002), so buckets survive hot reloads.
- **Limit changes and idle deletion (accepted bound).** `fullAt` is computed under the limit in force at the bucket's last `Allow`. The sweeper and the at-capacity eviction never see the route's *current* limit, so a key that is idle across a limit change can be deleted as "full" earlier than the new limit would say, and then recreated full at the new burst. We **accept and document** this bound: the over-admission is **less than one burst of the new limit, per key, once per limit change**, and limit changes are rare operator actions. The alternative (store the last-charged limit on each bucket and re-clamp when it differs) was rejected: `Allow` already clamps to the passed burst on every call, so the stored limit adds nothing on the `Allow` path, and it cannot close the gap, because deletion happens in the sweeper, which never sees the new limit. The only complete fix is for the limiter to know the config, which would undo the "limit passed per call" design above.
- **Concurrency:** 32 shards, each a `sync.Mutex` and a `map[string]*bucket`, selected by an FNV-1a hash of the key. That gives short critical sections and little contention across cores.
- **Memory bound: idle sweep.** A bucket that has been idle until `fullAt` is full again, which makes it identical to a brand-new bucket, so **deleting it is lossless** (subject to the limit-change bound above). `Run(ctx)` sweeps the shards every `SweepInterval` (default 1 minute) and deletes buckets where `now >= fullAt`.
- **Memory bound: hard cap, fail closed for new keys.** `MaxKeys` (default 100k) is enforced per shard as `ceil(MaxKeys/32)`, so the check needs no global counter. When a new key arrives at a full shard, the limiter looks at up to **8** existing buckets in map-iteration order (Go randomises the start) and evicts the first one that is idle (`now >= fullAt`), which loses nothing. If none of the sampled buckets is idle, the new key is **denied**: `Decision{Allowed: false, AtCapacity: true, RetryAfter: 1s}`, and the proxy answers 429 with `Retry-After: 1`. Buckets that still hold state (a throttled or partly drained client) are **never** evicted. This fails **closed**, but only for identities the limiter has not seen yet. The reason: the old "evict an arbitrary bucket" policy let an attacker who can mint keys (a /48 of IPv6 holds 65,536 /64s) wipe the throttled buckets of real clients, or of their own other identities, which is exactly a bypass. Failing closed limits the damage to the flood's own new identities and to legitimate first-time clients during the flood. That is visible (`tollgate_ratelimited_total{reason="capacity"}`) and recovers on its own once sweeps reclaim idle buckets. The sample size bounds the work done under the shard lock, so a key flood cannot turn into a CPU amplification. On authenticated routes, keys are bounded by the number of valid client IDs times routes, so in practice only `auth: none` routes can reach the cap.
- **Failed-auth throttle.** Credential guessing, and the CPU cost of RS256 verification, are limited by a **second, separate `TokenBucket` instance** keyed by the same client-IP key (/64 for IPv6), with a fixed policy of **burst 10, refill 1 token per second**. On an `api_key` or `jwt` route the proxy first `Peek`s the client's failed-auth bucket. If it is empty, it answers 429 + `Retry-After` **without attempting verification**. Otherwise it authenticates. Only an `ErrInvalidCredentials` result charges the bucket (`Allow`). Missing credentials cost no crypto and are not charged, and successes are never charged, so a busy legitimate client is unaffected. A separate instance keeps an auth-failure flood from consuming the main limiter's key budget, and it needs no key-namespace convention. Peek-then-charge is not atomic, so N concurrent failures from one IP can overshoot by at most N. That is accepted. Clients behind one NAT share this bucket, and 10 burst at 1/s is lenient enough for that.
- **Clock injection:** `Options.Now` lets tests advance a fake clock instead of sleeping, so limiter tests are exact and fast.

## Consequences

- Good: O(1) time and about 100 bytes per active client, with idle clients cleaned up for free. Rate and burst are what operators already understand.
- Good: correct and exact `Retry-After`, and fully deterministic tests.
- Good: neither a key flood nor a limit change can reset a throttled client's bucket. The only over-admission paths are bounded and documented (one burst per key per limit change, and concurrency overshoot on the failed-auth throttle).
- Cost: **limits are per gateway replica.** Behind R replicas, a client can get up to R × rate if the external load balancer spreads its requests. Mitigations, in order of cost: document it; configure `rate / R`; later, a global limiter (Redis plus GCRA) or "local with periodic sync".
- Cost: at `MaxKeys` the limiter **fails closed for new identities** (429) until idle buckets free up. We accept this in exchange for bounded memory and for never handing out a fresh bucket to someone who was being throttled. It is alertable through the `reason="capacity"` counter.
- Cost: a /64 key groups every device on one IPv6 subnet, and an IPv4 NAT groups every client behind it. Both share one bucket on `auth: none` routes. `trusted_proxies` / X-Forwarded-For handling is future work (architecture.md §10).
- Cost: there is a tiny floating-point error in `tokens`. It is irrelevant at human-scale rates and is pinned by tests at boundary values.
