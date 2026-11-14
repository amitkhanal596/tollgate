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
// Sync is non-blocking, latest wins. Run diffs targets by *Backend POINTER identity plus
// Check settings (never by ID alone): new pointer -> start; pointer gone -> stop and
// SetActiveHealthy(true); settings changed -> restart, keeping the current verdict.
// A target whose upstream has no health_check is not passed to Sync, so it counts as "gone".
func (c *Checker) Sync(targets []Target)
// ObservePassive is stateless: all state is on the Backend, so it survives swaps.
// The proxy never calls it for a client-canceled request (499); see §11 item 11.
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
   assign/propagate X-Request-ID, start timer (metrics/log middleware, outermost;
     records in a defer so it also runs when ReverseProxy panics with http.ErrAbortHandler)
0b. path safety check (checkPath)      unsafe → 400, BEFORE routing (§11 item 1)
1. route := s.router.Match(host, path) none → 404
     host: lower-case, strip port; exact-host routes first, then wildcard
     path: longest segment-aware prefix on r.URL.Path, which is exactly the path that
           is forwarded (SetURL joins an empty backend path, RawPath is preserved)
   r.Header.Del("X-Tollgate-Client-ID")  ALWAYS, on every route (anti-spoofing)
   ipKey := clientIPKey(r.RemoteAddr)    port stripped, IPv4 as-is, ::ffff:v4 un-mapped,
                                         IPv6 masked to /64; X-Forwarded-For not trusted in v1
2. auth   (route.Auth)
     none: rate-limit identity = ipKey; NO Client-ID header is set
     api_key / jwt:
       authFail.Peek(ipKey, {1/s, burst 10}) empty → 429 + Retry-After, verification skipped
       Authenticate(r)                  fail → 401 + WWW-Authenticate, generic body;
                                        ErrInvalidCredentials also charges authFail.Allow(ipKey, …)
       identity = ClientID; stored in ctx; set X-Tollgate-Client-ID = ClientID
3. rate limit (if route.RateLimit)     key = route.Name + "\x00" + identity
     limiter.Allow(key, limit)         denied → 429, Retry-After = ceil(seconds), min 1
                                       (AtCapacity → 429, Retry-After: 1)
     always set X-RateLimit-Limit / X-RateLimit-Remaining
4. b, err := upstream.Balancer.Next()  ErrNoHealthyBackends → 503
     defer upstream.Balancer.Done(b)
5. ctx deadline = route Request timeout (hard cap, no "disabled" value)
   httputil.ReverseProxy (Rewrite: SetURL(b.URL), SetXForwarded)
     transport = pool[(dial, responseHeader)] (owned by Gateway, shared across snapshots)
     strip X-API-Key; keep Authorization
     ErrorHandler: dial/conn error → 502; header/request timeout → 504; client gone → log 499
6. passive health: health.ObservePassive(b, ...) with the response status (at header time)
     or the transport error. SKIPPED when the client canceled (inbound ctx canceled → 499).
7. metrics + access log (route, upstream, backend, status, duration, config version)
  │
  ▼
upstream
```

No automatic retries in v1 (see open questions). Timeout resolution: route field, then `Defaults.Timeouts`, then the built-in `Default*Timeout` constant. `request` is a hard cap with no "disabled" value. Streaming and long-poll routes must set a large explicit value such as `"1h"` (ADR 0003).

### Listener and transport limits (Phase 1 fix round)

| Where | Setting | Why |
|---|---|---|
| Traffic `http.Server` (in `proxy.Serve`) | `ReadHeaderTimeout` 5 s, `IdleTimeout` 120 s, `MaxHeaderBytes` 1 MiB | Slowloris and header-flood protection. 120 s is longer than typical fronting-LB idle timeouts (60 s), so the LB closes first. |
| Traffic `http.Server` | **no** `ReadTimeout` / `WriteTimeout` | They would kill streaming responses and slow uploads. The per-route `request` deadline bounds the whole exchange instead. |
| Pooled `http.Transport` | `MaxIdleConnsPerHost` 64, `IdleConnTimeout` 90 s, `DialContext` timeout = dial, `ResponseHeaderTimeout` = response_header | The default of 2 idle connections per host would force constant reconnects under load. |
| Transport pool after a swap | entries not referenced by the new snapshot: `CloseIdleConnections()` and delete | The pool must not grow without bound across timeout changes. In-flight requests on old snapshots keep their reference and finish normally. |
| Admin listener (`-admin-listen`) | `/metrics`, `/healthz`, `/readyz` only | Ops endpoints are never exposed on the traffic port (§11 item 19). |

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

The long-lived state outside the snapshot, owned by `proxy.Gateway`: `balancer.Registry`, `ratelimit.Limiter` (route limits) and the separate failed-auth `ratelimit.Limiter`, `health.Checker`, the transport pool, discovery watches, and metrics.

## 5. Config flow

```
admin (curl)
  │  PUT /v1/routes/{name}   (Authorization: Bearer $TOLLGATE_ADMIN_TOKEN, optional If-Match: <version>)
  ▼
controlplane API ── tx: pg_advisory_xact_lock → read head → apply change → Normalize+Validate
  │                     → INSERT config_versions(version = head+1, body jsonb) → COMMIT
  ▼
Hub.Publish(cfg vN) ── ignored if N <= max published; fan-out into a capacity-1
  │                    "highest version wins" slot per subscriber
  │  stream handler: Subscribe FIRST, then read head; send head if > since;
  │                  then forward only versions > last sent
  │
  ▼  SSE: id: N / event: config / data: {json}       (heartbeat every 15 s; Bearer data-plane token)
gateway config.RemoteSource ── drop if N <= highest delivered → strict decode → Normalize+Validate
  │                               → send on capacity-1 latest-wins channel   (NO disk write here)
  ▼
proxy.Gateway reconcile loop (single goroutine, the only writer)
  │  inputs: latest *Config, latest []Endpoint per Consul service (stale generations dropped)
  │  1. diff referenced Consul services → start/cancel discovery watches
  │  2. build Snapshot (Registry.Get by ID for each backend; balancer per upstream;
  │     authenticators; limits; transports)       build error → keep old, count failure,
  │                                                 src.(config.Acknowledger).Rejected(N, err)
  │  3. snap.Store(new)                            ← atomic swap; in-flight requests finish on old
  │  4. src.(config.Acknowledger).Applied(N)       → RemoteSource writes LKG file, since = N,
  │                                                  async POST /v1/dataplanes/{node}/status
  │  5. removed := Registry.Prune(keep); metrics.ForgetBackend for each removed;
  │     Checker.Sync(targets); close+drop unreferenced transports; metrics config_version = N
  ▼
requests from now on see version N
```

The Acknowledger calls happen only for builds triggered by a **new config**. Rebuilds triggered by discovery updates do not ack. In standalone mode, `config.FileSource` replaces everything above the reconcile loop, and it does not implement `Acknowledger`. Rollback is `POST /v1/config/rollback`, which publishes a **new** version whose body equals the old one (ADR 0005).

Discovery watches feed the same loop. Each watch has a small forwarding goroutine that sends `(service key, generation, endpoints)` into one channel the loop selects on. **Every send selects on the watch's `ctx.Done()`**, so a canceled watch never blocks the loop. The loop drops updates whose generation is not the current generation for that key, and it de-duplicates endpoints by `Address` before building backends (ADR 0007). The goroutine exits when its watch channel closes or its ctx is done.

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
| `GET /v1/config/stream?since=N&node=ID` | SSE (ADR 0005). **Data-plane token only.** |
| `POST /v1/dataplanes/{node}/status` `{"applied":N,"last_rejected":M,"error":"..."}` | best-effort ack from `RemoteSource`. **Data-plane token only.** |
| `GET /v1/dataplanes` | per node: connected, version last **offered**, last **applied**, last **rejected** + error (in memory) |
| `/healthz`, `/readyz`, `/metrics` | ops, no token (not under `/v1`) |

Auth: every `/v1/*` endpoint other than the two marked "data-plane token only" requires `TOLLGATE_ADMIN_TOKEN`, **including reads**. The two data-plane endpoints accept only `TOLLGATE_DATAPLANE_TOKEN`. Both tokens are sent as `Authorization: Bearer`, must be at least 32 bytes and different from each other (otherwise startup fails), and are compared as SHA-256 digests with `subtle.ConstantTimeCompare`. There is no TLS between the planes in v1 (ADR 0005, Security).

Status codes: 400 for validation (`errors.Is(err, config.ErrInvalid)`), 404 for an unknown name or version, 412 for an `If-Match` mismatch, 401 for a missing or wrong token (including the right token on the wrong endpoint).

## 7. Goroutines and shutdown

Every goroutine has exactly one owner and one way to stop.

| Goroutine | Owner | Stops when |
|---|---|---|
| HTTP server(s) | `cmd/*` main | `srv.Shutdown(drainCtx)` |
| ConfigSource watcher | source | root ctx canceled; it then closes its channel |
| Reconcile loop (`Gateway.Run`) | `cmd/gateway` | config channel closed or ctx done; it cancels and waits for its discovery watches first |
| Discovery watch and forwarder (one per service) | reconcile loop | per-watch child ctx canceled (service unreferenced) or root ctx; every forwarder send selects on that ctx |
| Probe (one per backend pointer) | `health.Checker.Run` | per-probe ctx canceled by `Sync`, or `Run`'s ctx; `Run` waits for all |
| Limiter sweepers (`TokenBucket.Run`, two: route limiter and failed-auth limiter) | `cmd/gateway` | root ctx |
| RemoteSource status reporter (Phase 4) | `RemoteSource` | root ctx; exits before the source closes its channel |
| SSE stream handlers (control plane) | `http.Server` | client disconnects, or `Hub.Close` through `RegisterOnShutdown` |
| SSE hub | `cmd/controlplane` | root ctx |

Gateway shutdown on SIGTERM:

1. Mark not ready, so `/readyz` returns 503 and the load balancer drains this pod.
1b. Wait `-shutdown-delay` (default 0; 5 s in the Kubernetes manifest), still serving traffic normally. In Kubernetes, endpoint removal and SIGTERM happen concurrently, so kube-proxy or the ingress can keep sending new connections for a few seconds after SIGTERM. Without the delay, those requests hit a closed listener. The wait is ctx-aware: a second signal skips it. The in-process delay replaces a `preStop: sleep` hook (the distroless image has no `sleep`; Kubernetes 1.30+ `lifecycle.preStop.sleep` is an equivalent alternative). `terminationGracePeriodSeconds` must be at least shutdown-delay + drain + a margin.
2. `srv.Shutdown` with a drain deadline. New connections are refused and in-flight requests finish.
3. Cancel the root ctx, so the background goroutines above exit.
4. Wait for them (WaitGroup), then exit. Nothing uses `os.Exit` while goroutines are running.

## 8. Error-to-status mapping (data plane)

| Condition | Status |
|---|---|
| No snapshot yet | 503 |
| Unsafe path (dot segment, empty segment, encoded `/` `\` `.`, literal `\`, control byte) | 400, before routing |
| No route matches | 404 |
| Missing or invalid credentials | 401 |
| Too many failed auth attempts from this client-IP key | 429 + `Retry-After`, verification not attempted |
| Rate limited, or limiter at capacity for a new key | 429 + `Retry-After` |
| `ErrNoHealthyBackends` | 503 |
| Dial refused or connection reset | 502 |
| Response-header or request timeout | 504 |
| Client canceled | logged as 499, nothing written |

## 9. Metrics (Phase 6 outline)

`tollgate_requests_total{route,upstream,code}`, `tollgate_request_duration_seconds{route,upstream}` (histogram), `tollgate_ratelimited_total{route,reason}` with `reason` = `limit` | `capacity` | `auth_failures`, `tollgate_backend_up{upstream,backend}`, `tollgate_backend_inflight{upstream,backend}`, `tollgate_config_version`, `tollgate_config_apply_failures_total`, `tollgate_config_stream_connected`. Labels use route and upstream **names**, never raw paths or client IDs, to keep cardinality bounded.

- Every `tollgate_backend_*` series of a backend is **deleted** (`DeleteLabelValues`) when the reconcile loop prunes that backend from the Registry. Otherwise Consul churn would leak series forever, and dashboards would show dead backends as down.
- `/metrics`, `/healthz` and `/readyz` are served **only** on the admin listener (`-admin-listen`), never on the traffic port.

## 10. Open questions (for the orchestrator or owner)

1. **`internal/controlplane`** is not in the CLAUDE.md layout. Without it, all control-plane logic would sit in `package main`, which hurts testability and the ≥ 80% `internal/` coverage target. Proposed: add it.
2. **`go.mod` says `go 1.27.4`.** Code is written to be valid on Go 1.23+, but that directive forces 1.27 toolchains. If 1.23 compatibility matters (CI matrix, Docker base image), set `go 1.23`.
3. **No retries in v1.** A connection-refused error on an idempotent request could be retried once on another backend, which would improve chaos-test failover numbers. Deferred to keep the proxy small. Revisit after Phase 8.
4. **Client IP for rate limiting comes from `RemoteAddr` only** (IPv6 masked to /64, §11 item 10). Behind a Kubernetes load balancer, every client may appear as the same IP. A `trusted_proxies` setting (CIDRs whose X-Forwarded-For we believe) could be added in Phase 3 or 7.
5. **Passive failure definition:** any 5xx (current) or only 502/503/504 plus transport errors (Envoy's "gateway failure" mode)? See ADR 0006.
6. **Phase 1 needs some balancer** before Phase 2 exists. Suggest Phase 1 implements `Backend`, `Registry` and `NewRoundRobin` (small and pure), and leaves least-connections and health for Phase 2.

## 11. Review amendments (Phase 0 review)

Builders and test-writers: each row is binding for its phase. The "Contract change" column lists Go doc-comment and signature changes that the **phase builder** applies; the architect does not edit `.go` files. Choices that the orchestrator left to the architect are marked **[A]**.

| # | Finding (one line) | Decision | Implemented in phase | Contract change (file: what) |
|---|---|---|---|---|
| 1 | Router matches a path that the upstream normalises differently (`/public/%2e%2e/admin`), so auth can be bypassed. | 400 **before routing** if the decoded `r.URL.Path` has a `.` or `..` segment, or **[A]** a segment that is `.`/`..` before a `;` (Tomcat-style `..;`), an empty segment (`//`), or a byte < 0x20 or 0x7f. Also 400 if `r.URL.EscapedPath()` contains `%2f`, `%5c` or `%2e` (case-insensitive) or a literal `\`. The router matches `r.URL.Path`, which is exactly the forwarded path. | 1 (fix round) | `internal/proxy`: unexported `checkPath(u *url.URL) error`, called before `router.Match`. `internal/config/config.go` `Route` doc: add rule "0. unsafe paths → 400 before matching". |
| 2 | LKG file and version are committed before the snapshot build can fail. | Optional `config.Acknowledger`. The reconcile loop calls `Applied`/`Rejected` after each **config-triggered** build. `RemoteSource` writes LKG and advances `since` only on `Applied`. Best-effort `POST /v1/dataplanes/{node}/status`. `/v1/dataplanes` shows offered/applied/rejected. An LKG that fails to parse, validate or build is ignored (log, wait for the stream). | 4 | `internal/config/source.go`: add `type Acknowledger interface { Applied(version int64); Rejected(version int64, err error) }`. Doc: optional, called only from the reconcile goroutine, must not block on network I/O. `ConfigSource` doc point 3: "last-known-good" becomes "last **applied** copy". `internal/proxy` `Gateway.Run`: `if a, ok := src.(config.Acknowledger)`. |
| 3 | A re-added backend gets a new `*Backend` from the Registry but is never probed (Sync diffs by ID). | `Checker.Sync`/`Run` diff by **pointer identity + check settings**. A new pointer for a known ID starts a new probe, and the old pointer's probe stops. | 2 | `internal/health`: `Checker`/`Sync` doc as in §2 sketch. Test: prune ID, re-add, assert the new pointer goes down after `fall` failed probes. |
| 4 | Discovery forwarders can deadlock shutdown and deliver stale sets. | Every forwarder send is a `select` with `ctx.Done()`. Each watch has a loop-assigned `generation`, and the loop drops updates whose generation is not the current one for that key. | 5 | `internal/proxy` (unexported): `endpointUpdate{key string; gen uint64; eps []discovery.Endpoint}`. `internal/discovery/discovery.go` `Resolver` doc point 5: "sends never block (latest-wins replace)". |
| 5 | Control-plane hub can publish v6 before v5 and lose the newest. | The hub tracks the max published version and ignores `<=`. The per-subscriber capacity-1 slot is "highest version wins". The stream handler **subscribes first, then reads head**, and forwards only versions greater than the last one it sent. | 4 | `internal/controlplane`: `Hub.Publish(*config.Config)` doc: "no-op if Version <= max published". `Hub.Subscribe() (<-chan *config.Config, func())` doc: slot replaced only by a higher version. Stream handler doc states the subscribe-then-head order. |
| 6 | Config stream is unauthenticated. | `TOLLGATE_DATAPLANE_TOKEN` (Bearer) is the **only** token for `GET /v1/config/stream` and **[A]** `POST /v1/dataplanes/{node}/status`. `TOLLGATE_ADMIN_TOKEN` is required for every other `/v1/*` endpoint, reads included. Comparison is SHA-256 digest + `subtle.ConstantTimeCompare`. **[A]** Startup fails if either token is < 32 B or the two are equal. No TLS between planes in v1 (ADR 0001, 0005). | 4 | `cmd/controlplane`: read both env vars, validate, exit non-zero otherwise. `internal/controlplane`: `requireToken(digest [32]byte) func(http.Handler) http.Handler`. `internal/config` `RemoteSource` options: `Token string`. `cmd/gateway`: `TOLLGATE_DATAPLANE_TOKEN` required when `-control-plane` is set. |
| 7 | A backend stays down forever after its `health_check` is removed. | When a probe stops because the target lost its health check or is gone, the Checker calls `SetActiveHealthy(true)`. A probe restart (settings changed) keeps the current verdict. | 2 | `internal/health` `Checker` doc. Test: backend marked down, `Sync` without it, then `ActiveHealthy() == true`. |
| 8 | Request timeout cannot be disabled, which contradicts ADR 0003's streaming advice. | Keep `request` as a hard cap with no "disabled" value. Streaming and long-poll routes set a large explicit value (e.g. `"1h"`). ADR 0003 fixed. | Docs only | `internal/config/config.go` `Timeouts.Request` doc: "Hard cap; there is no disabled value (0 = inherit). Streaming/long-poll routes must set a large explicit value such as 1h." |
| 9 | JWT validation gaps. | `Validate`: HS256 requires `hmac_secret_env` and forbids `public_key_file`; RS256 the reverse. `NewJWT` fails closed if the secret is empty or < 32 B, or **[A]** if the RS256 key does not parse as an RSA key of ≥ 2048 bits. Parser uses `jwt.WithValidMethods([]string{alg})`, `WithExpirationRequired()`, `WithLeeway(30s)` **[A]**, `WithTimeFunc(now)`, and `WithIssuer`/`WithAudience` when set. A client-ID claim that is missing, empty or not a string → `ErrInvalidCredentials`. | 3 | `internal/config/validate.go` + package doc rule list: the key-source rules. `internal/auth/auth.go` package doc: "exp required; leeway 30 s; NewJWT errors on weak or missing key". `ErrInvalidCredentials` doc: add "empty or non-string client-ID claim". |
| 10 | Rate limiter can be bypassed or fails open. | `auth: none` identity = client-IP key: port stripped, IPv4 as-is, **[A]** `::ffff:` IPv4-mapped un-mapped first, other IPv6 masked to /64. **[A] At capacity:** per-shard cap `ceil(MaxKeys/32)`. A new key samples ≤ 8 buckets and evicts the first **idle** one (`now >= fullAt`). If none is idle, the new key is denied with 429, `Retry-After: 1` (**fail closed for new keys only**, and throttled buckets are never evicted). **[A] Failed-auth throttle:** separate `TokenBucket`, keyed by client-IP key, burst **10**, refill **1/s**. `Peek` before verifying; if empty → 429 without verification. Only `ErrInvalidCredentials` charges it. | 3 | `internal/ratelimit/limiter.go`: add `Peek(key string, limit Limit) Decision` to `Limiter` (no consume, no insert). Add `Decision.AtCapacity bool`. Rewrite the `Options.MaxKeys` doc (old "evicts an arbitrary bucket … fails open" is wrong now). `Limiter` doc: identity is the "client-IP key (/64)". `internal/proxy`: `clientIPKey(remoteAddr string) string`, `Options.AuthFailureLimiter ratelimit.Limiter`, `authFailureLimit = ratelimit.Limit{RatePerSecond: 1, Burst: 10}`. `cmd/gateway`: second `TokenBucket` and its sweeper. |
| 11 | Client disconnects eject healthy backends. | If the inbound request ctx is canceled (client gone → 499), never call `ObservePassive`. Own deadline (`DeadlineExceeded`, 504) is still reported. Outcome is taken at header time, and body-copy errors are not reported. | 2 | `internal/health` `ObservePassive` doc: "callers must not report client-canceled requests". `internal/proxy` `ErrorHandler` doc. Test: client aborts more than `consecutive_failures` times, and the backend stays `Available`. |
| 12 | `X-Tollgate-Client-ID` can be spoofed on unauthenticated routes. | Always `Header.Del` the inbound header on every route. Set it only when an authenticated `Identity` exists. The client IP is never put in it. | 3 (the unconditional `Del` may already land in Phase 1) | `internal/proxy`: exported `const HeaderClientID = "X-Tollgate-Client-ID"`, with a doc comment stating the rule. |
| 13 | Graceful shutdown drops requests in Kubernetes. | `-shutdown-delay` (default 0) between marking not-ready and `Shutdown`. The wait is ctx-aware, and a second signal skips it. The manifest uses 5 s, with `terminationGracePeriodSeconds` ≥ delay + drain + 5 s. The in-process delay replaces a `preStop` sleep. | 1 (fix round, flag) + 7 (manifest) | `cmd/gateway`: flag `-shutdown-delay duration`. `/readyz` = `!shuttingDown && gw.Ready()`. Order: not-ready → delay → `Shutdown(drain)` → cancel root ctx → wait. |
| 14 | Server and client limits are unspecified. | Listener: `ReadHeaderTimeout` 5 s, `IdleTimeout` 120 s, `MaxHeaderBytes` 1 MiB, no `ReadTimeout`/`WriteTimeout`. Transports: `MaxIdleConnsPerHost` 64, `IdleConnTimeout` 90 s. After a swap, unreferenced pool entries get `CloseIdleConnections()` and are dropped. | 1 (fix round) | `internal/proxy` `Serve` doc lists the server settings. Transport-pool doc: per-entry settings plus "retain only keys referenced by the live snapshot". ADR 0002 row updated. |
| 15 | Strict decoding. | YAML `KnownFields(true)` and JSON `DisallowUnknownFields` reject unknown fields everywhere (file, admin API, stream). Upgrade order is **data planes first** (ADR 0005). | 1 (already pinned by tests); 4 for API/stream decoders | `internal/config` `Parse` doc: "unknown fields are an error". Phase 4 JSON decoders (admin API bodies, `RemoteSource`) use `DisallowUnknownFields`. |
| 16 | Version assignment is described two ways (sequence vs head+1). | `version = head + 1` inside the `pg_advisory_xact_lock` transaction. No Postgres sequence anywhere. | Docs only | `internal/config/config.go` `Config.Version` doc: replace "assigns it from a Postgres sequence" with "assigns head+1 inside its advisory-locked write transaction, so versions are gapless and strictly increasing". |
| 17 | `Validate` gaps. | Backend URLs are unique within an upstream (compare `url.Parse(b).String()`, the form `BackendID` uses). API-key `sha256` is unique; `client_id` uniqueness and 64-hex are already enforced. Route `host` must not contain a port (`net.SplitHostPort` succeeds → invalid), and **[A]** `Normalize` strips `[]` from IPv6 route hosts. `Normalize` sets health `timeout` = `min(DefaultHealthTimeout, interval/2)` **after** defaulting `interval`. Discovery endpoints are de-duplicated by `Address` (lowest `ID` wins). | 1 (fix round); dedupe in 5 | `internal/config/validate.go`, `normalize.go`, and the package-doc rule list in `config.go`. `HealthCheck.Timeout` doc: "default min(1s, interval/2)". `internal/proxy`: dedupe helper for endpoint sets. |
| 18 | The idle sweep after a limit change can hand out extra tokens. | **[A] Accept and document the bound:** over-admission is less than one burst of the **new** limit, per key, once per limit change. Do not store the last-charged limit, because the sweeper never sees the new limit, so it cannot close the gap (ADR 0004). | 3 | `internal/ratelimit/limiter.go` `Options.SweepInterval` doc: state the bound. A test pins it: drain under the old limit, change the limit, advance past the old `fullAt`, sweep, then `Allow` → `Remaining == newBurst-1`. |
| 19 | Metrics leak pruned backends, and ops endpoints are exposed on the traffic port. | `tollgate_backend_*` series are deleted when a backend is pruned. `/metrics`, `/healthz` and `/readyz` are served only on `-admin-listen`. | 6 (admin listener exists since 1) | `internal/balancer/registry.go`: `Prune(keep map[string]struct{}) (removed []*Backend)`, which is source-compatible for existing callers. `internal/metrics`: `ForgetBackend(upstream, backend string)`. `cmd/gateway`: the traffic handler never mounts ops paths. Test: traffic port `GET /metrics` → routed or 404, never metrics. |

Notes on the **[A]** refinements:

- **Row 1, extra rejections:** without the `//` rule, a public route `/` plus a protected `/admin` can be bypassed with `//admin` on any upstream that collapses slashes. Without the `..;` rule, Tomcat or Jetty upstreams reopen the bypass.
- **Row 6, status endpoint:** "applied vs offered" needs a back-channel, because SSE is one-way and `since` is only seen at reconnect. So the data-plane token also covers `POST /v1/dataplanes/{node}/status`.
- **Row 10, IPv4-mapped addresses:** if `::ffff:` addresses are not un-mapped before /64 masking, every IPv4 client of a dual-stack listener collapses into one bucket.
