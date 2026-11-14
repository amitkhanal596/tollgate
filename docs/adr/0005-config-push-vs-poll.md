# ADR 0005: Push config to data planes over Server-Sent Events

Status: Accepted (Phase 0)

## Context

The control plane stores every config as an immutable, numbered version in Postgres. Every gateway replica must learn about version N+1 quickly (target: under 1 second), survive control-plane restarts and network blips, and never apply versions out of order. We also need rollback to the previous version. Only the Go standard library is available for the transport (no gRPC).

## Options considered

1. **Periodic polling** (`GET /v1/config` every T seconds, with `ETag`/`If-None-Match`). It is trivial, stateless, and works through any proxy. But propagation takes up to T, and load grows with replicas divided by T. Lowering T trades load for latency.
2. **Long polling** (`GET /v1/config?since=N` blocks until head > N or 60 s pass, the same pattern as Consul's blocking queries). It is near-instant, plain request/response and easy to test. The client has to keep re-issuing the request. This was a very close second.
3. **SSE stream** (`GET /v1/config/stream`, `text/event-stream`). (chosen) The server pushes each new version on one long-lived connection. It is standard-library only on both ends, has built-in resume semantics (`id:` and `Last-Event-ID`), and heartbeats detect dead peers. The costs: long-lived connections complicate control-plane shutdown and run into load balancer idle timeouts, the stream is one-directional, and buffering proxies can break it.
4. **gRPC bidirectional stream, xDS-style** (with ACK/NACK). This is the richest option, but it needs dependencies that are not allowed.
5. **Control plane dials each data plane.** The control plane would need to discover every gateway pod, and the connection direction is inverted through firewalls. Rejected.

## Decision

**SSE with full-config events and monotonic versions.**

Protocol:

```
GET /v1/config/stream?since=41&node=gw-7      (reconnects also send Last-Event-ID: 41; header wins)

HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
X-Accel-Buffering: no

id: 42
event: config
data: {"version":42,"routes":[...],"upstreams":[...]}

: heartbeat            <- comment line every 15 s
```

- **How the data plane says which version it has:** through `since` on the first connect (0 means "I have nothing") and `Last-Event-ID` on reconnects. If the head version is greater than that number, the server immediately sends the head version and then streams every new publish. It sends only the head, never each skipped version, because events are **full snapshots, not diffs**. A missed event therefore cannot corrupt state, and applying the same version twice does nothing. `data:` is one line of JSON (`json.Marshal` never emits newlines), which keeps the framing trivial.
- **Data plane client (`config.RemoteSource`):** it ignores any version less than or equal to the last applied one, validates the rest, delivers them on its latest-wins channel, and writes the last-known-good file. It raises `bufio.Scanner`'s 64 KiB line limit (to about 8 MiB), because configs can be large. If no bytes arrive (not even a heartbeat) for 45 s, it reconnects. Reconnects use exponential backoff from 500 ms to 30 s with full jitter, and the backoff resets after receiving an event.
- **Control plane:** after a version is committed, an in-process hub fans it out to all open streams. Each subscriber has a capacity-1 latest-wins channel, so one slow gateway cannot block the others. On shutdown the hub closes every stream (`http.Server.RegisterOnShutdown`), because `Shutdown` otherwise waits forever on connections that never go idle. The control plane also records `node` and the version last sent, for `GET /v1/dataplanes`.
- **Rollback = publish a new version.** `POST /v1/config/rollback {"to_version": 41}` (default: head-1) copies the body of version 41 into a **new row, version 43**, recording `rolled_back_from = 41`. Versions only ever go up, so the data plane's rule of "apply only if newer" needs no special case. The history stays an append-only audit log, and rolling back a rollback is just another publish. The alternative was moving a "current" pointer back to 41. Gateways at 42 would then reject 41 as stale, and we would need a second generation counter. Rejected.

## Consequences

- Good: propagation is a single network round trip (milliseconds), and nothing is sent while there are no changes apart from heartbeats.
- Good: full snapshots plus monotonic versions plus "newer wins" make the client idempotent and order-safe, with very little logic.
- Cost: long-lived connections. Heartbeats must be shorter than any intermediate idle timeout, and control-plane shutdown must actively close streams.
- Cost: there is no acknowledgement or negative acknowledgement from the data plane. We learn the applied version from `since` on reconnect, `/v1/dataplanes`, and each gateway's `tollgate_config_version` metric. A failed apply is visible in data-plane metrics and logs, not in the control plane.
- Cost: an in-process hub means one control-plane replica. Running several would need Postgres `LISTEN/NOTIFY` to fan out across replicas. That is noted as future work.
- Edge case: if the control plane's database is wiped and versions restart at 1, gateways ignore the "older" versions and keep their current config. That is the safe choice, at the cost of liveness. Recovery is to bump the sequence past the old head.
