# ADR 0002: Hot reload with an atomically swapped immutable snapshot

Status: Accepted (Phase 0; Registry row amended by ADR 0010: backends that leave the live set keep their state for a grace period)

## Context

Every request reads the config: it needs the route table, the upstream's balancer, the auth policy, the rate limit and the timeouts. Config changes happen rarely (a control-plane publish, a Consul membership change, a YAML edit), but they must apply without a restart and without failing any in-flight request. Reads outnumber writes by roughly a million to one, and the read path is the gateway's hot path.

Some state is not config. It is runtime state that has to keep its value across a reload:

- backend health (active up/down, passive ejection deadline, failure counter),
- per-backend in-flight request counts (least-connections depends on them),
- rate-limit buckets,
- pooled upstream TCP connections.

## Options considered

1. **A `sync.RWMutex` around a mutable config struct.** Familiar. But each request does `RLock`, which is an atomic write to a shared counter, so the cache line bounces between cores under load. The bigger problem is the critical section. If a request holds the read lock for its whole lifetime (possibly seconds), a writer waits behind every slow request, and new readers queue behind the waiting writer, so the reload stalls traffic. If a request holds it only for the lookup, it then uses objects that a writer may change mid-request, so you end up copying anyway.
2. **An `atomic.Pointer[Snapshot]` to an immutable snapshot (copy-on-write).** (chosen) Readers do one atomic load and get a consistent view for the whole request. The writer builds a complete new snapshot off to the side, then publishes it with one `Store`. The cost is rebuilding the whole snapshot on every change, which is O(config size), and the discipline that a published snapshot is never mutated.
3. **Restart or re-exec the process with socket hand-off (SO_REUSEPORT, nginx-style).** No in-process sharing at all. But it drops all runtime state (health, buckets, connection pools), is slow, and is complex to do correctly in Go.
4. **Fine-grained mutation**: a `sync.Map` of routes, per-upstream locks, incremental diffs. Lower rebuild cost, but a request can see half of an update (a new route pointing at an upstream that has not been added yet), and the locking is hard to reason about.

`atomic.Value` would also work for option 2. `atomic.Pointer[T]` (Go 1.19+) is the type-safe version and needs no type assertions.

## Decision

Option 2. The gateway keeps `atomic.Pointer[proxy.Snapshot]`.

- **Request path:** `s := snap.Load()` once at the start of the request. Everything for that request comes from `s`. The request never reloads mid-flight.
- **Swap path:** one goroutine, the reconcile loop, does all builds and swaps. Because there is only one writer, a plain `Store` is enough, with no compare-and-swap. It builds from (latest config, latest Consul endpoints) and then calls `Store`. If the build fails, the old snapshot stays. After the `Store`, in this order: `Registry.Prune` (live IDs plus those in their grace period), drop unreferenced transports, switch the metrics' live backend set (deleting departed backends' series), `Checker.Sync`. The series switch must precede `Sync`, because stopping a departed backend's probe fires a reset-to-up callback that would otherwise recreate the series just deleted.
- **Old snapshots** are never closed or freed by hand. In-flight requests hold a pointer, and the garbage collector reclaims the snapshot after the last one finishes. This works only because of the next rule.
- **A Snapshot owns no goroutines and no resources that need closing.** It is plain data plus pointers to long-lived objects. Everything stateful lives outside it and is carried across swaps:

| Runtime state | Where it lives | How it survives a swap |
|---|---|---|
| Health, ejection, in-flight | `*balancer.Backend` atomics | A long-lived `balancer.Registry` returns the **same pointer** for the same backend ID (`upstream|url`). A backend that leaves the live set is out of rotation at the swap (its `tollgate_backend_*` series are deleted and its probe stopped), but it stays in the Registry for a **grace period** (60 s, ADR 0010), so an ID that comes back within it gets the same pointer with its ejection and in-flight state. After the grace it is pruned (at the start of the next rebuild), and a later return gets a **new** pointer with fresh state. Old requests always finish on the pointer they hold. |
| Balancer cursor | Inside the per-snapshot `Balancer` | Not carried over. Round-robin restarts at a random offset, which is harmless. |
| Rate-limit buckets | Three `ratelimit.TokenBucket`s owned by `proxy.Gateway` (authenticated identities, IP-keyed clients, failed-auth throttle; ADR 0004), swept by `Gateway.Run` | Not in the snapshot at all. The route's `Limit` is passed on every `Allow` call, so a new rate applies at the next request with no migration. |
| Connection pools | A transport pool keyed by (dial timeout, response-header timeout), owned by `proxy.Gateway`, not by the snapshot | Reused across swaps, so a reload does not throw away warm connections. After each swap, pool entries that the new snapshot no longer references get `CloseIdleConnections()` and are dropped from the pool. Requests still running on an old snapshot keep their transport and finish normally. Idle connections to removed backends inside a still-used transport expire through `IdleConnTimeout` (90 s). Pool sizing (up to 1024 idle connections per backend, no connection cap) is ADR 0011. |
| Probe goroutines | `health.Checker` | After each swap, the reconcile loop calls `Sync(targets)`. The Checker diffs targets by **`*Backend` pointer identity and check settings**, not by ID: a new pointer for a known ID (a backend that was pruned after its grace period and re-added) or changed settings restarts the probe. A probe stopped because its target lost its health check, or disappeared, resets that backend's active health to up. |

**Backend is a concrete struct with atomic fields, not an interface.** The alternative was a `Backend` interface (`Healthy()`, `InFlight()`, ...). That is easy to fake in tests, but every call goes through dynamic dispatch, and nothing would make two snapshots share the *same* state object. The struct pointer is the identity that the Registry hands out, and the Registry is what makes state survive. Tests can construct real `*Backend` values directly, so no fake is needed.

## Consequences

- Good: the read path is lock-free and does not contend with other requests. Reloads never block requests, and requests never block reloads.
- Good: each request sees one consistent config version from start to finish, which makes "zero dropped requests during hot reload" easy to argue for (and Phase 8 measures it).
- Good: rollback is just another swap.
- Cost: every change rebuilds the whole snapshot. That takes microseconds to milliseconds for hundreds of routes, so it is fine at this scale. At 100k routes you would want incremental structures.
- Cost: immutability is a convention, not something the compiler enforces. A data race on a snapshot field would be a bug. `go test -race`, plus the rule that only the builder writes snapshot fields, are the guards.
- Cost: an old snapshot can stay in memory for as long as the slowest in-flight request (up to the request timeout). This is bounded and small.
