# ADR 0007: Discover backends with Consul blocking queries in the data plane

Status: Accepted (Phase 0; implemented in Phase 5: watcher in `internal/discovery/consul.go`, reconcile-loop wiring in `internal/proxy/reconcile.go`; amended by the Phase 5 review fix round, see ADR 0010)

## Context

In Phase 5, upstream instances come and go through autoscaling, deploys and crashes. A static `backends:` list goes stale. Consul already knows which instances of each service exist and whether they pass their checks. Two questions need answers: how the gateway learns about instance changes, and how that information merges with the control plane's config.

## Options considered

1. **Static lists only, edited through the control plane.** No new dependency. But every instance change has to become a config version, made by a human or a script. It is slow and creates noisy history.
2. **DNS** (Consul DNS or a Kubernetes headless Service) through `net.Resolver`. No new dependency, and it works everywhere. But it is poll-based and governed by TTL, so staleness is bounded by the TTL plus the poll interval. A records carry no ports (you need SRV lookups), there is no push, and caching in resolvers is inconsistent.
3. **Consul HTTP blocking queries in the data plane** (chosen): `GET /v1/health/service/<svc>?passing=true&index=<X-Consul-Index>&wait=5m`. The request hangs until the result changes, so updates arrive in milliseconds. The response contains only *passing* instances, with address, port and tags. The cost is a dependency on `hashicorp/consul/api` and on a reachable Consul agent.
4. **The control plane watches Consul and pushes resolved IPs as new config versions.** Only one Consul client in the system. But it couples instance churn to config versioning: hundreds of versions a day, a useless audit log, and a rollback to v41 would bring back IPs that died long ago. It also makes the control plane necessary for failover, which breaks static stability (ADR 0001). Rejected.

## Decision

Option 3, behind `discovery.Resolver` (`Watch(ctx, Query) (<-chan []Endpoint, error)`), enabled by `cmd/gateway -consul-addr host:port`. Without a Resolver, `consul:` upstreams have no backends (503) and every build logs a warning.

**Merge rule: the control plane says *what*, Consul says *where*.** The config says "upstream `users` uses Consul service `users`, tag `v2`, `least_conn`, with these health settings". Consul supplies the current set of passing `host:port` pairs. The gateway's reconcile loop holds the live config and the latest endpoint set per watch. A new config or a new endpoint set triggers a snapshot rebuild and an atomic swap (ADR 0002). Backend IDs are `upstream|url`, so an instance that stays in the set keeps its health state and in-flight count across rebuilds.

**Watches (reconcile loop, `internal/proxy/reconcile.go`).**

- **One watch per (service, tag)**, shared by every upstream that references it, whatever their schemes. The watch key is `discovery.Query{Service, Tag}`.
- **Reconciled only after a config is applied.** The loop builds and swaps first, then starts watches for new (service, tag) pairs and cancels unreferenced ones. Diffing before the build would cancel watches that the still-serving old snapshot needs whenever the new config is then rejected. A rejected config leaves the watches alone.
- **Generations.** Each watch gets a `uint64` generation from the loop when it starts. Its forwarder stamps every update with it, and the loop drops any update whose generation is not the current one for that key. Otherwise, if a service was unreferenced and re-referenced in quick succession, an update already in flight from the old watch could overwrite the new watch's set.
- **Forwarders cannot deadlock.** Each watch has a forwarder goroutine that copies values into the loop's one unbuffered update channel. It selects on its watch ctx both while waiting for a value and while sending, so a canceled watch (unreferenced, or shutdown) never blocks the loop, and it does not rely on the resolver closing its channel. On exit the loop cancels every watch and waits for every forwarder. The resolver's own watcher goroutine exits on the same ctx (its in-flight query is canceled with it).
- **Endpoint-triggered rebuilds** use the live config, **reuse the live snapshot's authenticators** (no re-reading of key files or secret env vars on every Consul change, and no failure because one disappeared since the config was applied), create no version and are **never acknowledged** to the config source. A failure there (not expected, since the config already built once) is logged and the old snapshot keeps serving.
- **Readiness, carry-over and flap protection** are ADR 0010: the first config is not ready until every referenced service has answered or 10 s pass; an upstream whose query has not answered keeps its previous backends by upstream name; backends that leave the live set stay in the Registry for 60 s.

**Endpoint validation (trust boundary).** Every address in a delivered set must be `host:port` with a plain decimal port 1-65535 and a host that is an IP address (no IPv6 zone) or a DNS name of `[A-Za-z0-9-]` labels (1-63 bytes each, at most 253 in total, no trailing dot). Anything else is dropped with one WARN per endpoint per delivered set, so a malformed or hostile registration cannot smuggle a path, query or userinfo into the backend URL. The URL is the upstream's `consul.scheme` (default `http`) plus the address. **TRUST ASSUMPTION:** a Consul-supplied address is an egress target that receives forwarded client credentials (`Authorization` is forwarded, architecture.md §4). Whoever can register instances of a referenced service can receive them. Consul ACLs on service registration are the control; the gateway does not enforce an allow-list.

**Empty sets.**

- **Consul unreachable or erroring:** keep the last known endpoints. The watcher never sends an empty set because of an error.
- **A healthy Consul reports zero passing instances:** trust it. The upstream becomes empty and requests get 503. There is no panic threshold (for example "ignore an update that removes more than half the instances"). That is a documented choice: a threshold would keep sending traffic to instances Consul says are failing, and choosing its value is guesswork. Panic mode is future work (ADR 0006).
- **An empty first answer** logs one WARN per upstream using that query ("check the service name and tag"), because a typo would otherwise only show up as a silent 503.

**Watcher (`internal/discovery/consul.go`).**

- `discovery.NewConsul(client *consulapi.Client, opts ConsulOptions) *Consul`. `ConsulOptions` (zero values take the defaults): `Logger`, `WaitTime` (default **5 m**, the blocking-query `wait`), `BackoffMin` (**500 ms**), `BackoffMax` (**30 s**; raised to `BackoffMin` if lower), `MinInterval` (**1 s**), `QueryMargin` (**5 s**), and `Sleep func(ctx, d) error` (injected by tests). `Watch` fails synchronously only for an empty service name; the watcher goroutine is the only sender and closes the channel when ctx is done.
- **Per-query deadline** `WaitTime + WaitTime/16 + QueryMargin`. Consul adds up to `WaitTime/16` of jitter to a blocking wait, so a healthy agent may answer that late; the margin covers the network. An agent that accepts the connection and never answers (found in review: it stalled a watch forever) now fails that query, which backs off and retries like any error.
- **Index rules**, applied to the returned `X-Consul-Index` in this order: if it is **0**, use **1**; if it **went backwards**, reset to **0**; otherwise use it. `MinInterval` is a floor between two queries, applied only when the previous one returned sooner, so a misbehaving agent cannot make the loop spin, while a long blocking wait is never followed by an extra pause. Errors back off exponentially from `BackoffMin` to `BackoffMax` with jitter in `[d/2, d]`, clamped so sleeps never decrease; a success resets the backoff.
- **Sets.** De-duplicated by `Address` (lowest `ID` = `node/serviceID` wins) and sorted by `ID`; a service registered without an address uses its node's address. The snapshot builder also drops a repeated Registry ID. The first successful result is always sent; after that only a changed set, latest-wins on a capacity-1 channel.
- The gateway's Consul client never uses `HTTP_PROXY` (same reasoning as backend transports and probes), and `CONSUL_HTTP_TOKEN` and friends still apply through `consulapi.DefaultConfig`.

Our own active and passive checks (ADR 0006) still apply on top of Consul's filter: Consul's checks run from the agent, ours see the gateway's network path.

**Agent placement.** In production the gateway talks to the **local** Consul agent (a DaemonSet or host agent), never to remote servers directly. The demo stacks run one dev agent reached by service name (`consul:8500` in Compose and in the kind manifests).

## Consequences

- Good: instance churn is handled within milliseconds, without control-plane or Postgres involvement, so failover keeps working even when the control plane is down.
- Good: config history stays meaningful, because it records intent, not IP addresses.
- Cost: every gateway holds one long-poll per referenced (service, tag) to its agent. That is cheap, because agents are designed for this.
- Cost: there are two sources of truth to explain. The merge rule above is the answer: config decides which service and how to balance it, Consul decides which instances.
- Cost: a new dependency and failure mode (the Consul agent). Mitigated by keeping last-known endpoints, by the bounded readiness gate, and by static `backends:` remaining a fully supported alternative per upstream.
- Cost: the Consul catalog is part of the trust boundary (see the trust assumption above).
