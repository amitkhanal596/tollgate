# ADR 0001: Split Tollgate into a control plane and a data plane

Status: Accepted (Phase 0)

## Context

Tollgate does two very different jobs:

1. **Forward client traffic** quickly and reliably: routing, auth, rate limiting, load balancing, proxying. This is the request path. It must scale horizontally, survive the failure of anything that is not an upstream, and add as little latency as possible.
2. **Manage configuration**: an operator (or CI) adds routes and upstreams, gets them validated, keeps a history, and can roll back. This handles a handful of requests per day, needs a database, and can be down for a minute without hurting anyone, as long as traffic keeps flowing.

These two jobs fail differently, scale differently and need different dependencies, which makes them good candidates for separate processes. The data plane must also run on its own from a YAML file, because Phases 1 to 3 are built before the control plane exists.

## Options considered

1. **One binary: the gateway embeds the admin API and reads Postgres directly.**
   One thing to deploy. But every gateway replica needs database credentials and a pool of connections. A slow or down database affects gateway startup and reloads. The admin API runs in the same process that faces the public internet, so a bug there has a much larger blast radius. N replicas each have to poll or listen to the database.
2. **File-only config (YAML in a ConfigMap, managed with GitOps), no control plane.**
   Simplest to run, and nothing extra can fail. But there is no API for automation. A rollback means a git revert plus a rollout. Propagation depends on the kubelet syncing ConfigMaps, which can take up to about a minute, and there is no version history inside the system. We keep this mode for development and standalone use, but it is not the main path.
3. **Two binaries, `cmd/controlplane` and `cmd/gateway`. The control plane stores versioned configs and streams them to the gateways.** (chosen)
   The data plane has no database dependency, scales on its own, and keeps working when the control plane is down. The cost is a second binary and a distribution protocol to design (ADR 0005). Replicas are also only eventually consistent: for about a second after a publish, different gateways may run different versions.
4. **Adopt Envoy and implement an xDS control plane.** This is the industry answer, but it hides the parts this project exists to build (balancer, limiter, hot reload) and needs gRPC and protobuf dependencies that are not on the allowed list.

## Decision

Option 3.

| Lives in the control plane (`cmd/controlplane`) | Lives in the data plane (`cmd/gateway`) |
|---|---|
| Admin REST API (CRUD for routes and upstreams, rollback) | Listener, router, auth, rate limiter, balancer, reverse proxy |
| Postgres: append-only table of full config versions | Active and passive health checking |
| Validation at write time (shared `internal/config`) | Consul watcher (ADR 0007) |
| SSE stream endpoint `/v1/config/stream` | `ConfigSource` client (file or SSE), atomic snapshot swap (ADR 0002) |
| | `/metrics`, `/healthz`, `/readyz` (and, with the debug-only `-pprof` flag, `/debug/pprof/`), served only on the separate admin listener (`-admin-listen`), never on the traffic port |

Both binaries use the same `internal/config` structs and the same `Validate`, so "valid" means exactly the same thing on both sides.

**Static stability rules.** The data plane must keep working when the control plane does not:

1. The data plane never calls the control plane on the request path.
2. If the control plane goes down, the data plane keeps serving its last-known-good snapshot from memory. The SSE client reconnects with backoff, and a `config_stream_connected` gauge drops to 0 so someone gets alerted.
3. If the data plane restarts while the control plane is down, it uses the on-disk copy of the last **applied** config. The SSE client writes that copy atomically (write a temp file, then rename) only after the gateway reports that it built and swapped in that version (`config.Acknowledger.Applied`, see ADR 0005). A version that passes validation but fails the snapshot build is never written to disk. On startup, a cached copy that fails to parse, validate or build is logged and ignored, and the gateway waits for the stream. If there is neither a usable cached copy nor a control plane, the gateway starts but is not ready: `/readyz` fails and requests get 503 until a config arrives.
4. Config is validated twice. The control plane rejects bad input with 400. The data plane validates again as defence in depth: an invalid pushed config is logged and ignored, never applied. A config that validates but fails the snapshot build (for example a missing JWT key file) is also kept out: the old snapshot stays, and the source is told `Rejected`.

**Security between the planes (v1).** The stream requires a read-only data-plane bearer token, and every other `/v1/*` endpoint requires the admin token (details in ADR 0005). **TLS between the planes is out of scope for v1**: tokens and configs travel in clear text inside the cluster network. Configs contain only key hashes and the *names* of secret-bearing env vars and files, never secrets, which limits what an eavesdropper learns, but a sniffed admin token allows config writes. Until mTLS is added (future work), the admin API must only be reachable from inside the cluster (no public Service or Ingress).

The data plane does not know or care where its config comes from. It consumes a `config.ConfigSource`, and `FileSource` and `RemoteSource` both implement it.

## Consequences

- Good: a control-plane or Postgres outage only blocks *changes*. Traffic is unaffected.
- Good: the gateway image has no database driver or credentials, which means a smaller attack surface.
- Good: each half can be tested on its own. The gateway's tests use a fake `ConfigSource`.
- Cost: two binaries, two Dockerfiles or build targets, and a streaming protocol to get right (reconnects, heartbeats, shutdown).
- Cost: eventual consistency. Different replicas can briefly run different versions. Tollgate exposes the applied version as a metric so this can be observed.
- Cost: there is a code path to keep: standalone YAML mode. It is cheap, because it is just another `ConfigSource`.
