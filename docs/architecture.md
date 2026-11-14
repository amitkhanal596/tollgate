# Tollgate architecture

This is the map for every later phase. The "why" behind each decision lives in `docs/adr/`; this file covers the "what" and "where".

## 1. Binaries

| Binary | Role | External deps |
|---|---|---|
| `cmd/gateway` | Data plane. Serves client traffic. Gets config from a YAML file (`-config file.yaml`) or from the control plane (`-control-plane http://cp:9000`). | consul/api, prometheus, yaml, jwt |
| `cmd/controlplane` | Admin REST API, versioned config in Postgres, SSE config stream. | pgx, prometheus |

The gateway never calls the control plane on the request path, and it keeps serving its last-known-good config while the control plane is down (ADR 0001).

## 2. Packages

| Package | Responsibility | Key types (Phase) |
|---|---|---|
| `internal/config` | Config model (YAML and JSON tags), `Normalize`, `Validate`, the `ConfigSource` interface, `FileSource` (YAML, hash-polled) and `RemoteSource` (SSE client with an on-disk last-known-good copy). | `Config`, `Route`, `Upstream`, `ConfigSource` (0); `FileSource` (1); `RemoteSource` (4) |
| `internal/balancer` | `Backend` runtime record, `Registry` (gives the same `*Backend` for the same ID across swaps), round-robin and least-connections. | `Backend`, `Balancer`, `ErrNoHealthyBackends` (0); `Registry`, `NewRoundRobin`, `NewLeastConn` (2) |
| `internal/health` | Active prober (one goroutine per backend, rise/fall), passive outcome observation (ejection with cooloff). | `Checker`, `Target`, `ObservePassive` (2) |
| `internal/ratelimit` | Hand-written sharded token bucket with idle eviction and a key cap. | `Limiter`, `Limit`, `Decision` (0); `TokenBucket` (3) |
| `internal/auth` | API-key (SHA-256 lookup) and JWT authenticators; identity in the context. | `Authenticator`, `Identity` (0); `NewAPIKey`, `NewJWT` (3) |
| `internal/discovery` | Consul blocking-query watcher producing healthy endpoint sets. | `Resolver`, `Query`, `Endpoint` (0); `Consul` (5) |
| `internal/metrics` | Prometheus collectors registered on an **injected** `*prometheus.Registry` (never the global default registry, per "no global mutable state"). | `Metrics` (6) |
| `internal/proxy` | Composes everything: `Snapshot` and its builder, router, the HTTP handler (request path), transport pool, the `Gateway` reconcile loop that owns the `atomic.Pointer[Snapshot]`. | `Gateway`, `Snapshot` (1; grows in 2–6) |
| `internal/controlplane` *(proposed, see open questions)* | Admin API handlers, Postgres store, SSE hub and stream handler. | `Store`, `Hub`, `API` (4) |

Planned Phase 2 health API (sketch, to be pinned down by test-writer):

```go
type Target struct { Backend *balancer.Backend; Check config.HealthCheck }
func NewChecker(client *http.Client, now func() time.Time, onChange func(b *balancer.Backend, up bool)) *Checker
func (c *Checker) Run(ctx context.Context) error // owns every probe goroutine; returns after all have exited
func (c *Checker) Sync(targets []Target)        // non-blocking, latest wins; Run starts/stops/restarts probes by backend ID
// ObservePassive is stateless: all state is on the Backend, so it survives swaps.
func ObservePassive(b *balancer.Backend, p config.PassiveHealth, status int, err error, now time.Time) (ejected bool)
```

## 3. Dependency rule (acyclic, enforced in review)

Packages are ranked. **A package may import only packages of strictly lower rank.** Imports only go downward, so the graph is acyclic by construction.

```
rank 3   cmd/gateway, cmd/controlplane
rank 2   internal/proxy, internal/controlplane
rank 1   internal/auth, internal/health
rank 0   internal/config, internal/balancer, internal/ratelimit, internal/metrics, internal/discovery
```

Exact allowed internal imports:

| Package | May import (internal) | Third-party |
|---|---|---|
| config | none | yaml.v3 |
| balancer | none | none |
| ratelimit | none | none |
| metrics | none | prometheus |
| discovery | none | consul/api |
| auth | config | golang-jwt/jwt/v5 |
| health | config, balancer | none |
| proxy | config, balancer, ratelimit, metrics, discovery, auth, health | none |
| controlplane | config | pgx, prometheus |
| cmd/gateway | proxy, config, ratelimit, discovery, health, metrics | consul/api, prometheus |
| cmd/controlplane | controlplane, config | pgx |

Consequences of the rule:

- `proxy` and `controlplane` never import each other. The JSON shape of `config.Config` is their only contract.
- Leaves (`balancer`, `ratelimit`, `discovery`) take their own small types (`Limit`, `Query`) instead of config structs. `proxy` does the translation, which keeps the leaves trivially unit-testable.
- `health` reports state changes through a callback instead of importing `metrics`.
- `golang.org/x/sync/errgroup` is **not** an allowed dependency. Use `sync.WaitGroup` plus an error channel.

## 4. Request path (data plane)

PLAN.md draws the pipeline as auth → rate limit → router → balancer → proxy. Because auth mode and rate limits are **per route**, the route *lookup* has to come first. It is cheap and pure. The *enforcement* order is then exactly as in PLAN.md.

```
client
  │
  ▼
0. s := gateway.snap.Load()           nil → 503 (not ready yet)
   assign/propagate X-Request-ID, start timer (metrics/log middleware, outermost)
1. route := s.router.Match(host, path) none → 404
     host: lower-case, strip port; exact-host routes first, then wildcard
     path: longest segment-aware prefix
2. auth   (route.Auth)                 fail → 401 + WWW-Authenticate, generic body
     none: identity = client IP (from RemoteAddr; X-Forwarded-For is not trusted in v1)
     api_key / jwt: identity = ClientID; stored in ctx
3. rate limit (if route.RateLimit)     key = route.Name + "\x00" + identity
     limiter.Allow(key, limit)         denied → 429, Retry-After = ceil(seconds), min 1
     always set X-RateLimit-Limit / X-RateLimit-Remaining
4. b, err := upstream.Balancer.Next()  ErrNoHealthyBackends → 503
     defer upstream.Balancer.Done(b)
5. ctx deadline = route Request timeout
   httputil.ReverseProxy (Rewrite: SetURL(b.URL), SetXForwarded)
     transport = pool[(dial, responseHeader)] (shared across snapshots)
     strip X-API-Key; set X-Tollgate-Client-ID (overwrite, anti-spoofing); keep Authorization
     ErrorHandler: dial/conn error → 502; header/request timeout → 504; client gone → log 499
6. passive health: health.ObservePassive(b, ...) with status or transport error
7. metrics + access log (route, upstream, backend, status, duration, config version)
  │
  ▼
upstream
```

No automatic retries in v1 (see open questions). Timeout resolution: route field, then `Defaults.Timeouts`, then the built-in `Default*Timeout` constant.

### Snapshot (in `internal/proxy`, Phase 1)

Immutable after `Store`. It owns no goroutines and nothing that needs closing (ADR 0002).

```go
type Snapshot struct {
    Version int64
    Config  *config.Config          // for /status and debugging
    hosts   map[string][]*routeRT   // exact host -> routes sorted by prefix length, longest first
    any     []*routeRT              // wildcard-host routes, same order
}
type routeRT struct {
    cfg       config.Route
    upstream  *upstreamRT
    authn     auth.Authenticator    // nil for AuthNone
    limit     *ratelimit.Limit      // nil = unlimited
    timeouts  resolvedTimeouts      // fully defaulted
    transport http.RoundTripper     // from the long-lived pool
}
type upstreamRT struct {
    name     string
    lb       balancer.Balancer
    backends []*balancer.Backend    // from the Registry: same pointers across swaps
    passive  *config.PassiveHealth
}
```

The long-lived state outside the snapshot, owned by `proxy.Gateway`: `balancer.Registry`, `ratelimit.Limiter`, `health.Checker`, the transport pool, discovery watches, and metrics.

## 5. Config flow

```
admin (curl)
  │  PUT /v1/routes/{name}   (Authorization: Bearer $ADMIN_TOKEN, optional If-Match: <version>)
  ▼
controlplane API ── tx: pg_advisory_xact_lock → read head → apply change → Normalize+Validate
  │                     → INSERT config_versions(version = head+1, body jsonb) → COMMIT
  ▼
Hub.Publish(cfg vN) ── fan-out, latest-wins per subscriber
  │
  ▼  SSE: id: N / event: config / data: {json}       (heartbeat every 15 s)
gateway config.RemoteSource ── drop if N <= applied → Normalize+Validate → write last-known-good file
  │                               → send on capacity-1 latest-wins channel
  ▼
proxy.Gateway reconcile loop (single goroutine, the only writer)
  │  inputs: latest *Config, latest []Endpoint per Consul service
  │  1. diff referenced Consul services → start/cancel discovery watches
  │  2. build Snapshot (Registry.Get by ID for each backend; balancer per upstream;
  │     authenticators; limits; transports)       build error → keep old, count failure
  │  3. snap.Store(new)                            ← atomic swap; in-flight requests finish on old
  │  4. Registry prune of removed IDs; Checker.Sync(targets); metrics config_version = N
  ▼
requests from now on see version N
```

In standalone mode, `config.FileSource` replaces everything above the reconcile loop. Rollback is `POST /v1/config/rollback`, which publishes a **new** version whose body equals the old one (ADR 0005).

Discovery watches feed the same loop. Each watch has a small forwarding goroutine that sends `(service key, endpoints)` into one channel the loop selects on. The goroutine exits when its watch channel closes.

## 6. Control plane (Phase 4 outline)

```sql
CREATE TABLE config_versions (
  version          BIGINT PRIMARY KEY,               -- head+1 under advisory lock: gapless, monotonic
  body             JSONB  NOT NULL,                  -- full config.Config, body.version = version
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  author           TEXT   NOT NULL DEFAULT '',
  comment          TEXT   NOT NULL DEFAULT '',
  rolled_back_from BIGINT REFERENCES config_versions(version)
);
```

Writers are serialised with `pg_advisory_xact_lock`. A plain `SELECT … FOR UPDATE` on the head row is not enough under READ COMMITTED, because the second writer re-reads the *old* head and the first update is lost.

API (Go 1.22+ `ServeMux` method patterns, standard library only):

| Method and path | Purpose |
|---|---|
| `GET /v1/config` | head config |
| `PUT /v1/config` | replace the whole document (seed from YAML) |
| `GET /v1/config/versions`, `GET /v1/config/versions/{version}` | history |
| `GET/PUT/DELETE /v1/routes/{name}`, `GET /v1/routes` | route CRUD (each write = new version) |
| `GET/PUT/DELETE /v1/upstreams/{name}`, `GET /v1/upstreams` | upstream CRUD |
| `POST /v1/config/rollback` `{"to_version": N}` | default head-1; creates a new version |
| `GET /v1/config/stream?since=N&node=ID` | SSE (ADR 0005) |
| `GET /v1/dataplanes` | connected nodes and the last version sent to each |
| `/healthz`, `/readyz`, `/metrics` | ops |

Status codes: 400 for validation (`errors.Is(err, config.ErrInvalid)`), 404 for an unknown name or version, 412 for an `If-Match` mismatch, 401 for a missing or wrong admin token (constant-time compare).

## 7. Goroutines and shutdown

Every goroutine has exactly one owner and one way to stop.

| Goroutine | Owner | Stops when |
|---|---|---|
| HTTP server(s) | `cmd/*` main | `srv.Shutdown(drainCtx)` |
| ConfigSource watcher | source | root ctx canceled; it then closes its channel |
| Reconcile loop (`Gateway.Run`) | `cmd/gateway` | config channel closed or ctx done; it cancels and waits for its discovery watches first |
| Discovery watch and forwarder (one per service) | reconcile loop | per-watch child ctx canceled (service unreferenced) or root ctx |
| Probe (one per backend) | `health.Checker.Run` | per-backend ctx canceled by `Sync`, or `Run`'s ctx; `Run` waits for all |
| Limiter sweeper (`TokenBucket.Run`) | `cmd/gateway` | root ctx |
| SSE stream handlers (control plane) | `http.Server` | client disconnects, or `Hub.Close` through `RegisterOnShutdown` |
| SSE hub | `cmd/controlplane` | root ctx |

Gateway shutdown on SIGTERM:

1. Mark not ready, so `/readyz` returns 503 and the load balancer drains this pod.
2. `srv.Shutdown` with a drain deadline. New connections are refused and in-flight requests finish.
3. Cancel the root ctx, so the background goroutines above exit.
4. Wait for them (WaitGroup), then exit. Nothing uses `os.Exit` while goroutines are running.

## 8. Error-to-status mapping (data plane)

| Condition | Status |
|---|---|
| No snapshot yet | 503 |
| No route matches | 404 |
| Missing or invalid credentials | 401 |
| Rate limited | 429 + `Retry-After` |
| `ErrNoHealthyBackends` | 503 |
| Dial refused or connection reset | 502 |
| Response-header or request timeout | 504 |
| Client canceled | logged as 499, nothing written |

## 9. Metrics (Phase 6 outline)

`tollgate_requests_total{route,upstream,code}`, `tollgate_request_duration_seconds{route,upstream}` (histogram), `tollgate_ratelimited_total{route}`, `tollgate_backend_up{upstream,backend}`, `tollgate_backend_inflight{upstream,backend}`, `tollgate_config_version`, `tollgate_config_apply_failures_total`, `tollgate_config_stream_connected`. Labels use route and upstream **names**, never raw paths or client IDs, to keep cardinality bounded.

## 10. Open questions (for the orchestrator or owner)

1. **`internal/controlplane`** is not in the CLAUDE.md layout. Without it, all control-plane logic would sit in `package main`, which hurts testability and the ≥ 80% `internal/` coverage target. Proposed: add it.
2. **`go.mod` says `go 1.27.4`.** Code is written to be valid on Go 1.23+, but that directive forces 1.27 toolchains. If 1.23 compatibility matters (CI matrix, Docker base image), set `go 1.23`.
3. **No retries in v1.** A connection-refused error on an idempotent request could be retried once on another backend, which would improve chaos-test failover numbers. Deferred to keep the proxy small. Revisit after Phase 8.
4. **Client IP for rate limiting comes from `RemoteAddr` only.** Behind a Kubernetes load balancer, every client may appear as the same IP. A `trusted_proxies` setting (CIDRs whose X-Forwarded-For we believe) could be added in Phase 3 or 7.
5. **Passive failure definition:** any 5xx (current) or only 502/503/504 plus transport errors (Envoy's "gateway failure" mode)? See ADR 0006.
6. **Phase 1 needs some balancer** before Phase 2 exists. Suggest Phase 1 implements `Backend`, `Registry` and `NewRoundRobin` (small and pure), and leaves least-connections and health for Phase 2.
