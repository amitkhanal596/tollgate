# ADR 0007: Discover backends with Consul blocking queries in the data plane

Status: Accepted (Phase 0; watcher options and index rules pinned by the watcher in `internal/discovery/consul.go`; the reconcile-loop wiring is Phase 5)

## Context

In Phase 5, upstream instances come and go through autoscaling, deploys and crashes. A static `backends:` list goes stale. Consul already knows which instances of each service exist and whether they pass their checks. Two questions need answers: how the gateway learns about instance changes, and how that information merges with the control plane's config.

## Options considered

1. **Static lists only, edited through the control plane.** No new dependency. But every instance change has to become a config version, made by a human or a script. It is slow and creates noisy history.
2. **DNS** (Consul DNS or a Kubernetes headless Service) through `net.Resolver`. No new dependency, and it works everywhere. But it is poll-based and governed by TTL, so staleness is bounded by the TTL plus the poll interval. A records carry no ports (you need SRV lookups), there is no push, and caching in resolvers is inconsistent.
3. **Consul HTTP blocking queries in the data plane** (chosen): `GET /v1/health/service/<svc>?passing=true&index=<X-Consul-Index>&wait=5m`. The request hangs until the result changes, so updates arrive in milliseconds. The response contains only *passing* instances, with address, port and tags. The cost is a dependency on `hashicorp/consul/api` and on a reachable Consul agent.
4. **The control plane watches Consul and pushes resolved IPs as new config versions.** Only one Consul client in the system. But it couples instance churn to config versioning: hundreds of versions a day, a useless audit log, and a rollback to v41 would bring back IPs that died long ago. It also makes the control plane necessary for failover, which breaks static stability (ADR 0001). Rejected.

## Decision

Option 3, behind `discovery.Resolver` (`Watch(ctx, Query) (<-chan []Endpoint, error)`).

**Merge rule: the control plane says *what*, Consul says *where*.** The config says "upstream `users` uses Consul service `users`, tag `v2`, `least_conn`, with these health settings". Consul supplies the current set of passing `host:port` pairs. The gateway's reconcile loop holds the latest config and the latest endpoint set for each watched service. A change to either one triggers a snapshot rebuild and an atomic swap (ADR 0002). Backend IDs are `upstream|url`, so an instance that stays in the set keeps its health state and in-flight count across rebuilds.

- **Watch lifecycle:** a watch is started when a config first references a (service, tag) pair, and canceled when no upstream references it any more. Each watch gets a child context, and the reconcile loop waits for the watch goroutines on shutdown.
- **Forwarders cannot deadlock and cannot deliver stale sets.** Each watch has a forwarder goroutine that copies values from the watch channel into the loop's single update channel.
  - **No deadlock:** every forwarder send is a `select` on that send and the watch's `ctx.Done()`. When the loop cancels a watch (unreferenced service, or shutdown) and then waits for it, a forwarder that is blocked on the send exits instead of deadlocking the loop.
  - **No stale sets:** each watch carries a **generation number** (a `uint64` incremented by the loop every time it starts a watch). The forwarder stamps every update with it, and the loop drops any update whose generation is not the current generation for that (service, tag). Otherwise, if a service was unreferenced and re-referenced in quick succession, an update already in flight from the *old* watch could overwrite the new watch's fresher set.
- **Duplicate addresses:** Consul can list two instances (different IDs) with the same `host:port`. The watcher de-duplicates each endpoint set by `Address`, keeping the entry with the lowest `ID` (`node/serviceID`), and sorts it by `ID`; the snapshot builder also drops a repeated Registry ID. Otherwise the same backend would appear twice in the balancer, getting double weight and a duplicate Registry ID. A service registered without an address uses its node's address.
- **Watcher API:** `discovery.NewConsul(client *consulapi.Client, opts ConsulOptions) *Consul`. `ConsulOptions` (zero values take the defaults): `Logger`, `WaitTime` (default **5 m**, the blocking-query `wait`), `BackoffMin` (default **500 ms**), `BackoffMax` (default **30 s**; raised to `BackoffMin` if lower), `MinInterval` (default **1 s**), and `Sleep func(ctx, d) error` (injected by tests; default a timer that honours ctx). `Watch` fails synchronously only for an empty service name; the watcher goroutine is the only sender and closes the channel when ctx is done.
- **Blocking-query hygiene** (following Consul's guidance). Index rules, applied to the returned `X-Consul-Index` in this order: if it is **0**, use **1**; if it **went backwards** (lower than the current index), reset to **0**; otherwise use it. `MinInterval` is a floor between two queries, applied only when the previous query returned sooner than that, so a misbehaving agent that answers instantly cannot make the loop spin, while a long blocking wait is never followed by an extra pause. Errors back off exponentially from `BackoffMin` to `BackoffMax` with jitter in `[d/2, d]`, clamped so the sleeps are **non-decreasing** (jitter never makes a later sleep shorter than the previous one); a success resets the backoff. The first successful result is always sent; after that, only a changed (sorted) endpoint set is sent, latest-wins on a capacity-1 channel.
- **Consul unreachable:** keep the last known endpoints. Never empty an upstream because of an error.
- **Consul reports zero passing instances:** trust it. The upstream becomes empty and requests get 503.
- **Startup:** `/readyz` stays false until every referenced service has had its first answer, or 10 s pass. That way the gateway does not take traffic while its Consul-backed upstreams are still empty.
- Our own active and passive checks (ADR 0006) still apply on top of Consul's filter. Consul's checks run from the agent, but ours see the gateway's network path.
- The gateway talks to the **local** Consul agent (a sidecar in Compose, a DaemonSet or the host agent in Kubernetes), never to remote servers directly.

## Consequences

- Good: instance churn is handled within milliseconds, without control-plane or Postgres involvement, so failover keeps working even when the control plane is down.
- Good: config history stays meaningful, because it records intent, not IP addresses.
- Cost: every gateway holds one long-poll per referenced service to its agent. That is cheap, because agents are designed for this.
- Cost: there are two sources of truth to explain. The merge rule above is the answer: config decides which service and how to balance it, Consul decides which instances.
- Cost: a new dependency and failure mode (the Consul agent). This is mitigated by keeping last-known endpoints and by static `backends:` remaining a fully supported alternative per upstream.
