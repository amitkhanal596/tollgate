# ADR 0005: Push config to data planes over Server-Sent Events

Status: Accepted (Phase 0; amended after the Phase 0 review, see architecture.md §11 items 2, 5, 6, 15 and 16; amended again by the Phase 4 design: `Cache-Control: no-store`, the client sends `since` only, equal jitter with a frame-based reset, LKG writes moved off the reconcile goroutine, rejected versions are not re-applied. The binding protocol details are in architecture.md §5.1 and §6.5; storage is ADR 0008.)

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
GET /v1/config/stream?since=41&node=gw-7      (the server also honours Last-Event-ID, which wins over since;
Authorization: Bearer <TOLLGATE_DATAPLANE_TOKEN>  RemoteSource sends only since, generic SSE clients send the header)

HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-store
X-Accel-Buffering: no

id: 42
event: config
data: {"version":42,"routes":[...],"upstreams":[...]}

: heartbeat            <- comment line every 15 s
```

- **Version assignment:** `version = head + 1`, computed inside the same transaction that holds `pg_advisory_xact_lock`. Versions are therefore gapless and strictly increasing. There is no Postgres sequence: `nextval` is not transactional, so aborted writes would leave gaps, and the advisory lock already serialises writers, so a sequence adds nothing.
- **How the data plane says which version it has:** through `since` on every connect (0 means "I have nothing"; generic SSE clients may send `Last-Event-ID` instead, and the header wins). It carries the version the gateway has **applied**, not merely received (see Acknowledgement below). If the head version is greater than that number, the server immediately sends the head version and then streams every new publish. It sends only the head, never each skipped version, because events are **full snapshots, not diffs**. A missed event therefore cannot corrupt state, and applying the same version twice does nothing. `data:` is one line of JSON (`json.Marshal` never emits newlines), which keeps the framing trivial. If `since` is greater than head (the database was wiped), the server logs a warning and sends nothing until head passes it.
- **Data plane client (`config.RemoteSource`):** it never delivers a version less than or equal to the highest version it has already delivered in this process. It decodes strictly (`json.Decoder.DisallowUnknownFields`), validates the rest, and delivers them on its latest-wins channel. It does **not** write the last-known-good (LKG) file at this point (see Acknowledgement). It caps one event at 8 MiB (far above the 64 KiB `bufio.Scanner` default, because configs can be large); a bigger event closes the connection. If no bytes arrive (not even a heartbeat) for 45 s, it reconnects. Reconnects use exponential backoff from 500 ms to 30 s with **equal** jitter (a delay in `[cap/2, cap)`), so a control plane that rejects instantly (for example 401) cannot be hammered in a tight loop, while reconnect storms after a control-plane restart are still spread out. The backoff resets after the first complete frame (event or heartbeat) on a connection. Exact rules: architecture.md §5.1.
- **Acknowledgement (`config.Acknowledger`).** A source may implement the optional interface `Acknowledger { Applied(version int64); Rejected(version int64, err error) }`. After every build attempt for a received config, the gateway's reconcile loop calls `Applied` (the snapshot was built and stored) or `Rejected` (the build failed and the old snapshot stays). `RemoteSource` implements it:
  - It keeps each delivered-but-unacknowledged `*Config` keyed by version (at most two: one in the channel, one being built).
  - On `Applied(v)` it advances its `since` cursor to `v` and marks `v` for persistence; its own worker goroutine then writes the LKG file (temp file, fsync, rename). Only `Applied` advances the cursor.
  - On `Rejected(v, err)` it logs, increments a counter, and leaves both the LKG file and the cursor alone. `v` is not re-applied in this process: it is already at or below the "highest delivered" mark, so when a reconnect offers it again it is dropped without a rebuild (no hot loop). After a gateway restart it is tried once more; to make running gateways retry, re-publish it with a rollback to `v`.
  - In both cases it reports `{applied, last_rejected, error}` to `POST /v1/dataplanes/{node}/status`. This report is best-effort: it is sent from RemoteSource's worker goroutine (latest wins, never on the reconcile goroutine), and a failed report is not retried until the next ack. Neither method does disk or network I/O; both return after a short critical section.
  - At startup, an LKG file that fails to parse or validate is ignored. One that is delivered but then `Rejected` simply never advances the cursor. In both cases the gateway stays not-ready and waits for the stream, with `since` set to 0.
- **Control plane hub (ordering).** Two API transactions can commit v5 and v6 and then call `Publish` in the opposite order. So the hub tracks the **maximum version ever published and ignores any lower or equal publish**. Each subscriber has a capacity-1 slot in which the **highest version wins**: under the hub mutex, the slot is replaced only by a strictly higher version. The stream handler **subscribes first, then reads head** from the store. It sends head if head > `since`, then forwards from the subscription only versions greater than the last one it sent. Subscribing before reading head means a publish cannot fall into the gap between the two steps.
- **Control plane shutdown:** on shutdown the hub closes every stream (`http.Server.RegisterOnShutdown`), because `Shutdown` otherwise waits forever on connections that never go idle. The control plane records, per `node`, the version last **offered** (sent on the stream) and the version last **applied** and **rejected** (from the status report, and from `since` at connect time). `GET /v1/dataplanes` shows them side by side, and it is held in memory only.
- **Rollback = publish a new version.** `POST /v1/config/rollback {"to_version": 41}` (default: head-1) copies the body of version 41 into a **new row, version 43**, recording `rolled_back_from = 41`. Versions only ever go up, so the data plane's rule of "apply only if newer" needs no special case. The history stays an append-only audit log, and rolling back a rollback is just another publish. The alternative was moving a "current" pointer back to 41. Gateways at 42 would then reject 41 as stale, and we would need a second generation counter. Rejected.

**Security (v1).**

- **Two bearer tokens, both read from env at control-plane startup:**
  - `TOLLGATE_ADMIN_TOKEN` is required for **every** `/v1/*` endpoint except the two below, including reads (`GET /v1/config`, `/v1/dataplanes`, ...).
  - `TOLLGATE_DATAPLANE_TOKEN` is a read-only token, accepted **only** on `GET /v1/config/stream` and `POST /v1/dataplanes/{node}/status`.
  - The admin token is **not** accepted on those two endpoints, and the data-plane token is not accepted anywhere else, so each token has exactly one job.
- **Startup checks:** the control plane refuses to start if either token is empty or shorter than 32 bytes, or if the two are equal. The gateway refuses to start with `-control-plane` set if `TOLLGATE_DATAPLANE_TOKEN` is empty. The gateway sends the token as `Authorization: Bearer ...`.
- **Comparison:** both sides of every comparison are hashed with SHA-256 and then compared with `crypto/subtle.ConstantTimeCompare`. Hashing first stops the length check from leaking the token length.
- **Unauthenticated endpoints:** `/healthz`, `/readyz` and `/metrics` on the control plane are not under `/v1` and need no token.
- **No TLS between the planes in v1.** This is out of scope and listed as future work (mTLS, or at least TLS with a pinned CA). The tokens therefore cross the cluster network in clear text. The data-plane token can only read configs, which contain hashes and env-var / file *names*, never secrets. The admin token can write, so the admin API must stay cluster-internal (ADR 0001).

**Strict decoding and upgrade order.** Both YAML (`KnownFields(true)`) and JSON (`DisallowUnknownFields`) reject unknown fields, on the admin API and on the stream. As a result, a control plane that adds a new config field produces configs that older gateways reject (and keep serving their old snapshot). **Upgrade data planes first, then the control plane.**

## Consequences

- Good: propagation is a single network round trip (milliseconds), and nothing is sent while there are no changes apart from heartbeats.
- Good: full snapshots plus monotonic versions plus "newer wins" make the client idempotent and order-safe, with very little logic. The hub's max-version rule and "subscribe, then read head" make the server side order-safe too.
- Good: the LKG file only ever holds a config this gateway actually ran, so a restart cannot resurrect a config that fails to build.
- Cost: long-lived connections. Heartbeats must be shorter than any intermediate idle timeout, and control-plane shutdown must actively close streams.
- Cost: the acknowledgement is best-effort and informational. It is a side channel (`POST .../status`), not part of the stream protocol, and the control plane does not act on a rejection (no automatic rollback). Each gateway's `tollgate_config_version` metric remains the authoritative source for its applied version.
- Cost: an in-process hub means one control-plane replica. Running several would need Postgres `LISTEN/NOTIFY` to fan out across replicas. That is noted as future work.
- Cost: no transport encryption between the planes in v1 (see Security).
- Edge case: if the control plane's database is wiped and versions restart at 1, gateways ignore the "older" versions and keep their current config. That is the safe choice, at the cost of liveness. Recovery is a one-off SQL insert that re-seeds `config_versions` with a row whose `version` is at least the highest version any gateway has applied (visible in `tollgate_config_version`). The next publish is then `head + 1` and is accepted everywhere.
