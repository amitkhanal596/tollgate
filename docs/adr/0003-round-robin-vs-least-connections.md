# ADR 0003: Round-robin and least-connections, selectable per upstream

Status: Accepted (Phase 0)

## Context

Each upstream has several backend instances, and for every request the gateway must pick one that is healthy. Upstreams differ. Some serve short, uniform requests to identical instances. Others have very different request durations (uploads, reports, long polls) or instances that run at different speeds (noisy neighbours, GC pauses, mixed hardware). PLAN.md calls for both round-robin (RR) and least-connections (LC), written by hand.

## Options considered

1. **Round-robin**: an atomic counter, and request *i* goes to backend `i mod N`. It costs one atomic add, splits request *counts* exactly evenly, and needs no feedback. But it ignores load: a slow backend keeps getting 1/N of the traffic while its queue grows.
2. **Least-connections**: pick the available backend with the fewest in-flight requests. It adapts on its own, because a slow backend builds up in-flight requests and gets fewer new ones. The costs: an O(N) scan per request, it depends on every request reporting `Done`, it counts only *this* gateway replica's requests, and it has a known trap. A broken backend that fails *fast* has few in-flight requests, so least-connections sends it *more* traffic (the "black hole"). Passive ejection (ADR 0006) closes that trap.
3. **Random / power-of-two-choices (P2C)**: sample two backends and take the one with fewer in-flight requests. Nearly as good as least-connections, with O(1) work per request, and it avoids herding when many gateways share stale counts. It is the best choice for large N, but slightly harder to explain and test deterministically.
4. **Weighted RR, consistent hashing, EWMA latency**: useful for canaries, cache affinity or latency-aware routing. They are out of scope for v1. Weighted routing is a stretch goal.

## Decision

Implement **both RR and LC** behind the `balancer.Balancer` interface, chosen per upstream with `algorithm: round_robin | least_conn`. The default is `round_robin`.

- **Interface:** `Next() (*Backend, error)` increments the chosen backend's in-flight count. `Done(*Backend)` decrements it. Both algorithms track in-flight counts, so metrics and graceful drain work the same regardless of the algorithm.
- **RR:** an `atomic.Uint64` cursor. It skips unavailable backends and tries at most N positions before returning `ErrNoHealthyBackends`. The cursor starts at a random offset so that a fleet of gateways does not all hit backend 0 right after a reload.
- **LC:** a full scan of the backends, reading the atomics without a lock. The scan starts at a rotating offset, so ties are spread round-robin. Without that, at low load every request would go to `backends[0]`. Two concurrent picks can choose the same backend. That is benign, and both increments are counted.
- **Shared state:** in-flight counts live on the shared `*Backend` (ADR 0002), so LC's view stays accurate across config swaps.

When to choose which:

| Situation | Pick |
|---|---|
| Identical instances, short and uniform requests (most JSON APIs) | `round_robin`: cheapest and perfectly even |
| Variable request duration, streaming or long-polling, mixed instance sizes, a backend that sometimes slows down | `least_conn`: moves traffic away from the slow instance. Streaming and long-poll routes must **also** set a large explicit `timeouts.request` (for example `"1h"`); see below. |
| Very many backends (hundreds) | P2C (future work) |

**Streaming and long-poll routes need an explicit request timeout.** The balancer choice does not change how long a request may live. `timeouts.request` is a hard cap on the whole exchange, including streaming the response body, and it has **no "disabled" value** (0 means "inherit", and the built-in default is 30 s). Unbounded requests are a resource-exhaustion risk: each one pins a goroutine, an upstream connection, a least-connections slot and an old snapshot. So a route that streams or long-polls must set a large explicit value, such as `timeouts: {request: "1h"}`. Otherwise its streams are cut at 30 s with a 504 or a truncated body.

## Consequences

- Good: the choice is a one-line config change, so the benchmark in Phase 8 can compare both on a deliberately slow backend. That is a great demo.
- Good: both algorithms are about 40 lines each, lock-free, and testable with an injected clock and real `*Backend` values.
- Cost: LC's correctness depends on `Done` being called exactly once. The proxy must `defer Done` right after a successful `Next`, and the reviewer checks this.
- Cost: LC only sees this replica's in-flight requests. With R gateways, its picture of the fleet is partial. That is acceptable, because each replica still balances its own share.
- Cost: on its own, LC favours fast-failing backends. Passive ejection must be enabled on `least_conn` upstreams that matter (the docs will recommend it).
