# Tollgate architecture

This is the map for every later phase. The "why" behind each decision lives in `docs/adr/`; this file covers the "what" and "where".

## 1. Binaries

| Binary | Role | External deps |
|---|---|---|
| `cmd/gateway` | Data plane. Serves client traffic. Gets config from a YAML file (`-config file.yaml`). Following the control plane (`-control-plane http://cp:9000`, `config.RemoteSource`) is designed in §5.1 and the source is built, but the flag is not wired into `cmd/gateway` yet. | consul/api, prometheus, yaml, jwt |
| `cmd/controlplane` | Admin REST API, versioned config in Postgres (or in memory for development), SSE config stream, `/metrics`. | pgx, prometheus |

The gateway never calls the control plane on the request path, and it keeps serving its last-known-good config while the control plane is down (ADR 0001).

## 2. Packages

| Package | Responsibility | Key types (Phase) |
|---|---|---|
| `internal/config` | Config model (YAML and JSON tags), `Normalize`, `Validate`, the `ConfigSource` interface, `FileSource` (YAML, hash-polled) and `RemoteSource` (SSE client with an on-disk last-known-good copy). | `Config`, `Route`, `Upstream`, `ConfigSource` (0); `FileSource` (1); `RemoteSource` (4) |
| `internal/balancer` | `Backend` runtime record, `Registry` (gives the same `*Backend` for the same ID across swaps), round-robin and least-connections. | `Backend`, `Balancer`, `ErrNoHealthyBackends` (0); `Registry`, `NewRoundRobin`, `NewLeastConn` (2) |
| `internal/health` | Active prober (one goroutine per backend pointer, rise/fall), passive outcome observation (ejection with cooloff). | `Checker`, `Target`, `ObservePassive` (2) |
| `internal/ratelimit` | Hand-written sharded token bucket with idle eviction and a key cap. | `Limiter`, `Limit`, `Decision` (0); `TokenBucket` (3) |
| `internal/auth` | API-key (SHA-256 lookup) and JWT authenticators, client-IP key. | `Authenticator`, `Identity` (0); `NewAPIKey`, `NewJWT`, `ClientIPKey` (3) |
| `internal/discovery` | Consul blocking-query watcher producing healthy endpoint sets. | `Resolver`, `Query`, `Endpoint` (0); `Consul`, `ConsulOptions` (5) |
| `internal/metrics` | Prometheus collectors registered on an **injected** `prometheus.Registerer` (never the global default registry, per "no global mutable state"), and the access-log helper. | `Metrics`, `ControlPlaneMetrics`, `AccessRecord`, `LogAccess` (6) |
| `internal/proxy` | Composes everything: `Snapshot` and its builder, router, the HTTP handler (request path), transport pool, the `Gateway` reconcile loop that owns the `atomic.Pointer[Snapshot]`, the limiters and the health `Checker`. | `Gateway`, `Options`, `Snapshot`, `Serve` (1; grows in 2–6) |
| `internal/controlplane` (added at the Phase 0 checkpoint) | Admin API handlers, versioned config store (Postgres + in-memory), migrations, SSE hub, data-plane registry and stream handler (§6, ADR 0008). | `Store`, `Hub`, `Nodes`, `Options`, `NewHandler`, `Migrate`, `PostgresStore`, `MemStore` (4) |

Health API (Phase 2, ADR 0006):

```go
type Target struct { Backend *balancer.Backend; Check config.HealthCheck }
// nil client: a private transport that ignores HTTP_PROXY, closed when Run returns.
// Probes never follow redirects and carry no cookie jar.
func NewChecker(client *http.Client, now func() time.Time, onChange func(b *balancer.Backend, up bool)) *Checker
func (c *Checker) Run(ctx context.Context) error // owns every probe goroutine; returns nil after all have exited
// Sync is non-blocking, latest wins. Run diffs targets by *Backend POINTER identity plus
// Check settings (never by ID alone): new pointer -> start; pointer gone -> stop and
// SetActiveHealthy(true); settings changed -> restart, keeping the current verdict.
// A target whose upstream has no health_check is not passed to Sync, so it counts as "gone".
func (c *Checker) Sync(targets []Target)
// ObservePassive is stateless: the streak and ejection deadline live on the Backend.
// Ejects with Backend.TryEject (CAS), so exactly one caller reports each ejection.
// Callers report only backend outcomes (ADR 0009); see §4 step 6.
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
| cmd/controlplane | controlplane, config, metrics | pgx, prometheus |

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
0. reqID := inbound X-Request-ID if it matches [A-Za-z0-9._:-]{1,128}, else 16 random bytes as hex.
     It goes on the response in ModifyResponse and writeError, never up front: ReverseProxy
     clears the response header map after a 1xx (103 Early Hints), which would drop it.
   s := gateway.snap.Load()               nil → 503 (not ready yet)
0b. pre-routing rejections: never reach the balancer, never count against a backend (ADR 0009)
     method CONNECT                        → 405 (a reverse proxy, not a tunnel)
     an Upgrade value with a byte outside printable ASCII → 400
     checkPath(r.URL), run on BOTH r.URL.Path and r.URL.EscapedPath() → 400 if it has:
       a "." or ".." segment; an empty segment ("//"; one trailing "/" is fine);
       ";" anywhere; "\" anywhere; "%2f", "%5c" or "%2e" (any case); a byte < 0x20 or 0x7f
1. route := s.router.Match(r.Host, r.URL.Path)   none → 404
     host: lower-case; port, IPv6 brackets and a trailing "." stripped; exact-host routes
           first, then wildcard
     path: longest segment-aware prefix on r.URL.Path, which is exactly the path forwarded
           (rewrite joins EscapedPath, which checkPath verified encodes the same segments)
2. auth (route.Auth)                       [public, unlimited routes skip steps 2-3 entirely]
     ipKey := auth.ClientIPKey(r.RemoteAddr)   port stripped, IPv4 as-is, ::ffff:v4 un-mapped,
                                               IPv6 masked to /64; X-Forwarded-For not trusted
     none: identity = ipKey; nothing identifies the client to the upstream
     api_key / jwt:
       authLimiter.Peek(ipKey, {1/s, burst 10}) empty → 429 + Retry-After, no verification
       Authenticate(r)          fail → 401 + WWW-Authenticate (per mode), one generic body;
                                ErrInvalidCredentials charges authLimiter.Allow(ipKey, …),
                                and if that charge is AtCapacity → 429 instead (fail closed)
       identity = Identity.ClientID (header-safe, <= 256 bytes)
3. rate limit (if route.RateLimit)         key = route.Name + "\x00" + identity
     limiter (api_key/jwt routes) or anonLimiter (auth: none).Allow(key, limit)
       denied → 429, Retry-After = ceil(seconds), min 1   (AtCapacity → Retry-After: 1)
     X-RateLimit-Limit / -Remaining: on the 429, else later on the response or error
4. b, err := route.upstream.lb.Next()      ErrNoHealthyBackends → 503
     defer lb.Done(b)                      (runs on every exit, incl. an ErrAbortHandler panic)
5. ctx deadline = route request timeout (hard cap, no "disabled" value)
   body announced (ContentLength != 0): wrap it in reqBody + context.AfterFunc(deadline, onDeadline)
   per-route httputil.ReverseProxy (built once per route per snapshot):
     Rewrite (on the OUTBOUND request; the inbound one is never mutated):
       strip X-Tollgate-Client-ID and X-API-Key, every spelling, on every route;
       SetURL(b.URL); restore the client's raw query if it is clean; keep the prior
       X-Forwarded-For and SetXForwarded; set X-Request-ID;
       set X-Tollgate-Client-ID = ClientID only if authenticated. Authorization is forwarded.
     Transport: observingTransport wrapping pool[(dial, responseHeader)] (pool owned by Gateway)
     ModifyResponse: set X-Request-ID (and X-RateLimit-* on limited routes), replacing upstream copies
     ErrorHandler: writes the status observingTransport chose (§8); never reports to health
6. passive health, inside observingTransport.RoundTrip (ADR 0006, 0009):
     response            → ObservePassive(status) at header time (5xx = failure, else success)
     transport error     → 408 stalled upload | 400 client body error | 499 client gone: NOT reported
                           504 own deadline / header timeout | 502 dial, reset: reported
7. metrics + access log (Phase 6, not wired yet: route, upstream, backend, status, duration,
   config version; recorded in a defer so the ErrAbortHandler panic path is counted too)
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
| Pooled `http.Transport` | `Proxy: nil`, `DisableCompression`, HTTP/1.1 only | Backends are addressed directly (never through `HTTP_PROXY`), and the proxy must not add `Accept-Encoding: gzip` and silently decompress. |
| Admin listener (`-admin-listen`) | `/healthz`, `/readyz` (and `/metrics` once Phase 6 wires it) only | Ops endpoints are never exposed on the traffic port (§11 item 19). |

### Snapshot (in `internal/proxy`, Phase 1)

Immutable after `Store`. It owns no goroutines and nothing that needs closing (ADR 0002).

```go
type Snapshot struct {
    Version int64
    Config  *config.Config                 // for status and debugging; never modified
    router  router                         // exact host -> routes (longest prefix first), plus wildcard routes
    upstreams     map[string]*upstreamRT
    backendIDs    map[string]struct{}      // what to keep in the Registry after the swap
    transportKeys map[transportKey]struct{} // what to keep in the transport pool after the swap
    healthTargets []health.Target          // handed to Checker.Sync after the swap
}
type routeRT struct {
    cfg            config.Route
    name, host, prefix string
    upstream       *upstreamRT
    timeouts       resolvedTimeouts        // route -> defaults -> built-in
    proxy          *httputil.ReverseProxy  // one per route per snapshot; transport from the long-lived pool
    authn          auth.Authenticator      // nil for auth: none; one instance per mode per snapshot
    challenge      string                  // WWW-Authenticate value for this mode
    limit          *ratelimit.Limit        // nil = unlimited
    limitKeyPrefix string                  // name + "\x00"
}
type upstreamRT struct {
    name     string
    lb       balancer.Balancer
    backends []*balancer.Backend    // from the Registry: same pointers across swaps
    passive  *config.PassiveHealth  // private copy; nil = off
}
```

The build runs in two passes: one that inspects the config and builds authenticators and can fail, then one that creates Registry entries and transports and cannot fail, so a rejected config leaves nothing half-registered.

The long-lived state outside the snapshot, owned by `proxy.Gateway`: `balancer.Registry`; **three** rate limiters, each with its own key budget (`Options.LimiterMaxKeys`): authenticated identities, IP-keyed (`auth: none`) clients, and the failed-auth throttle (ADR 0004); `health.Checker`; the transport pool and copy-buffer pool. `Gateway.Run` runs the Checker and the sweepers of the limiters it created, for exactly its own lifetime. Discovery watches (Phase 5) and metrics (Phase 6) join this list.

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
  ▼  SSE: id: N / event: config / data: {json}       (heartbeat every 15 s, configurable; Bearer data-plane token)
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
  │  4. src.(config.Acknowledger).Applied(N)       → RemoteSource: since = N (in memory, O(1));
  │                                                  its worker goroutine then writes the LKG file
  │                                                  and POSTs /v1/dataplanes/{node}/status
  │  5. removed := Registry.Prune(keep); metrics.ForgetBackend for each removed;
  │     close+drop unreferenced transports; Checker.Sync(targets); metrics config_version = N
  ▼
requests from now on see version N
```

Status: `Gateway.apply` implements build → `Store` → `Prune` → retain transports → `Checker.Sync` today. The discovery inputs (step 1, Phase 5), the Acknowledger calls (steps 2 and 4, Phase 4 gateway wiring) and the metrics calls (Phase 6) are not wired yet. `Gateway.Run` returns nil when its ctx ends or the source closes its channel; `cmd/gateway` treats the second case as a failure (§7).

The Acknowledger calls happen only for builds triggered by a **new config**. Rebuilds triggered by discovery updates do not ack. In standalone mode, `config.FileSource` replaces everything above the reconcile loop, and it does not implement `Acknowledger`. FileSource polls a content hash (default every 2 s), applies a change only after **two consecutive polls see the same content** (an editor or ConfigMap update may be mid-write), rejects a file with **zero routes** (at startup `Watch` fails; later the update is logged and skipped, since a truncated file is the usual cause), logs bad content once, and stamps every delivered config with a **strictly increasing** version (the file's `version` if higher than the last delivered one, else last + 1). The zero-routes rule is FileSource's own: `Validate`, `RemoteSource` and the control plane all accept an empty config. Rollback is `POST /v1/config/rollback`, which publishes a **new** version whose body equals the old one (ADR 0005).

Discovery watches feed the same loop. Each watch has a small forwarding goroutine that sends `(service key, generation, endpoints)` into one channel the loop selects on. **Every send selects on the watch's `ctx.Done()`**, so a canceled watch never blocks the loop. The loop drops updates whose generation is not the current generation for that key, and it de-duplicates endpoints by `Address` before building backends (ADR 0007). The goroutine exits when its watch channel closes or its ctx is done.

### 5.0 Normalize and Validate (shared by every source and the control plane)

`Normalize` fills defaults; `Validate` returns one error per problem, joined, each wrapping `ErrInvalid`. An empty config (zero routes) is valid.

| Field | Rule |
|---|---|
| route / upstream `name` | `[A-Za-z0-9._-]{1,64}`, unique per kind (it is an admin-API path segment and a metric label) |
| route `path_prefix` | a clean literal path: starts with `/`; no `%`, `;`, `\`, control bytes, `//` or `.`/`..` segments (the gateway answers 400 to such request paths, so the prefix could never match); a trailing `/` is allowed |
| route `host` | no port (the router strips the request's port). Normalize lower-cases it, strips a trailing `.` and IPv6 brackets, and maps `*` to `""` (any host). `(host, path_prefix)` is unique |
| route `upstream`, `auth` | the upstream must exist; `auth` is `none` (default), `api_key` (needs at least one `auth.api_keys` entry) or `jwt` (needs `auth.jwt`) |
| route `rate_limit` | `0 < requests_per_second <= 1,000,000` (NaN and Inf rejected), `1 <= burst <= 1,000,000` (keeps the nanotoken arithmetic exact, ADR 0004) |
| `timeouts` (defaults and routes) | `>= 0`; 0 = inherit |
| upstream | `algorithm` `round_robin` (default) or `least_conn`; exactly one of `backends` and `consul` (`consul.service` non-empty, `scheme` http or https) |
| upstream `backends` | absolute http(s) base URL with a host and no path, query, fragment or credentials. Normalize **canonicalises** them (lower-case scheme and host, default port dropped), and they must be **unique** within the upstream after canonicalisation |
| `health_check` | `path` starts with `/`; thresholds `>= 1`; `0 < timeout < interval`. Defaults: interval 5 s, **timeout `min(1s, interval/2)`** (derived after interval is defaulted), rise 2, fall 3 |
| `passive_health` | `consecutive_failures >= 1`, `cooloff > 0`. Defaults 5 and 30 s |
| `auth.api_keys` | `client_id` non-empty, **header-safe** (no byte < 0x20 or 0x7f) and **<= 256 bytes**, unique (it becomes `X-Tollgate-Client-ID`, a rate-limit key and a log field); `sha256` 64 lower-case hex, **unique** |
| `auth.jwt.algorithm` | `HS256` or `RS256` |

### 5.1 `config.RemoteSource` (Phase 4, binding for test-writer and core-builder)

```go
// internal/config/remotesource.go
type RemoteOptions struct {
    BaseURL       string        // required: absolute http(s) URL of the control plane, e.g. "http://controlplane:9000"; no path other than "/", no query or fragment
    Token         string        // required: TOLLGATE_DATAPLANE_TOKEN, sent as "Authorization: Bearer <Token>"
    NodeID        string        // required: 1..128 bytes of [A-Za-z0-9._-], first byte a letter or digit
    LKGPath       string        // optional: "" disables the last-known-good file
    BackoffMin    time.Duration // default 500ms
    BackoffMax    time.Duration // default 30s
    IdleTimeout   time.Duration // default 45s: reconnect if no byte arrives for this long (3 heartbeats)
    MaxEventBytes int           // default 8 MiB: cap on one line and on one event's accumulated data
    StatusTimeout time.Duration // default 5s: per status POST
    HTTPClient    *http.Client  // default &http.Client{}; its Timeout MUST be 0 (a client timeout would kill the stream)
    Logger        *slog.Logger  // nil discards
    Sleep func(ctx context.Context, d time.Duration) error // backoff wait; default: timer or ctx.Done, returns ctx.Err()
    Rand  func() float64                                   // jitter source in [0,1); default math/rand/v2.Float64
}
func NewRemoteSource(opts RemoteOptions) (*RemoteSource, error)
func (s *RemoteSource) Watch(ctx context.Context) (<-chan *Config, error) // ConfigSource
func (s *RemoteSource) Applied(version int64)                            // Acknowledger (ack.go)
func (s *RemoteSource) Rejected(version int64, err error)                // Acknowledger
func (s *RemoteSource) Connected() bool // true while a stream is open and has delivered a frame; feeds tollgate_config_stream_connected (Phase 6)
```

The clock is injected as `Sleep` (all waiting in the source is backoff) plus `Rand`. A test passes a `Sleep` that records `d` and returns `nil` immediately, so it can assert the exact backoff sequence and its reset without waiting. The idle timeout uses real time; tests set it to milliseconds.

`NewRemoteSource` returns an error (and nothing starts) if `BaseURL` does not parse as an absolute `http`/`https` URL with a host, has a query or fragment, or has a path other than `""`/`"/"`; if `Token` is empty; if `NodeID` is invalid; if `HTTPClient.Timeout != 0`; or if `BackoffMin > BackoffMax` after defaulting. Zero or negative durations and sizes take the defaults.

**State** (one mutex): `delivered` (highest version sent on the channel, the high-water mark, starts 0), `cursor` (highest **applied** version, sent as `since`, starts 0), `pending map[int64]*Config` (delivered, not yet acknowledged), `lkgPending *Config` (applied, not yet written), `lastRejected int64`, `lastErr string`, `reportDirty bool`. Plus a capacity-1 `notify chan struct{}` that is **never closed**.

**Watch / startup.**

1. Second call: `(nil, ErrAlreadyWatching)`. Watch never returns any other error: an unreachable control plane is transient (`ConfigSource` point 2).
2. If `LKGPath != ""` and the file is readable: `config.ParseJSON` (strict, Normalize, Validate). If that succeeds and `Version > 0`, the config is sent on the channel first (the buffer is empty, so this cannot block), `delivered = Version`, `pending[Version] = cfg`. A missing file is the normal first start (logged at Info); an unreadable, unparseable or invalid file, or one with `version <= 0`, is logged at Warn and ignored (never deleted). `cursor` stays 0 until the reconcile loop calls `Applied`.
3. Start two goroutines, the **stream reader** and the **worker**, tracked by a WaitGroup. A third goroutine waits on the WaitGroup and then closes the channel, so the channel closes only after both have exited (`ConfigSource` point 7).
4. The first connection attempt starts immediately, without waiting for the LKG ack. If it connects before `Applied(lkgVersion)`, it sends `since=0` and receives head; if head equals the LKG version, it is dropped by the high-water rule below.

**Connection.** `GET {BaseURL}/v1/config/stream?node={url.QueryEscape(NodeID)}&since={cursor}` with `Authorization: Bearer {Token}` and `Accept: text/event-stream`. The source does **not** send `Last-Event-ID`: `since` already carries the applied cursor, and the server honours `Last-Event-ID` only for generic SSE clients. The request ctx is a child of the Watch ctx, canceled by the idle timer or by shutdown.

- Only `200` with media type `text/event-stream` (parameters ignored) is a stream. Anything else (including transport errors): read and discard at most 4 KiB of body, close it, log (401 at Error level because it means a misconfigured token, everything else at Warn), back off, retry. The source never gives up.
- **Parsing.** Lines end in `\n`; one trailing `\r` is stripped. An empty line dispatches the event being built. A line starting with `:` is a comment (heartbeat). Otherwise the field name is everything before the first `:`, and the value is everything after it with **one** leading space removed (a line with no `:` is a field with an empty value). Fields: `id` (last one wins), `event`, `data` (several data lines join with `\n`); `retry` and unknown fields are ignored. An event whose type is not `config` (including the SSE default `message`) is ignored.
- **Limits.** A single line longer than `MaxEventBytes + 64` bytes, or an event whose accumulated `data` exceeds `MaxEventBytes`, is a protocol error: log at Error, close the connection, back off. (There is no safe way to resynchronise mid-event.)
- **Idle detection.** One timer per connection, armed before the request is sent (so a server that never answers is caught too) and re-armed on **every body read that returns bytes**, not per line or per frame, so a large event trickling in slowly is not cut. If it fires (`IdleTimeout` with no bytes), the request ctx is canceled, the body read fails, and the source reconnects with backoff.
- **Healthy frame.** The first complete frame on a connection (a dispatched event of any type, or a comment line) sets `attempt = 0` and `Connected() = true`. `Connected()` becomes false when the connection ends. Heartbeats count, so a load balancer that cuts idle connections every 60 s does not ratchet the backoff up.

**Handling a `config` event,** in this order:

1. `config.ParseJSON(data)`: strict decode (`DisallowUnknownFields`, trailing data rejected), Normalize, Validate. On error: log at Error ("invalid config from control plane"), drop. The connection stays open.
2. If an `id` field was present and is not exactly `strconv.FormatInt(cfg.Version, 10)`: log, drop.
3. If `cfg.Version <= delivered`: drop (Debug log). This removes duplicates, stale versions after a database restore, and versions this process already delivered, including ones it saw `Rejected`.
4. `delivered = cfg.Version`, `pending[cfg.Version] = cfg`, `replaceLatest(out, cfg)` (the capacity-1 latest-wins helper; RemoteSource is the only sender). If that pushed an unread older config out of the channel, the consumer can never receive it, so it is deleted from `pending` too. That keeps `pending` at most two entries (one in the channel, one being built) even under a fast stream.

Dropped events still count as healthy frames. A config with zero routes is delivered normally: FileSource's zero-route guard exists to catch half-written files, and a control-plane version is an atomic, deliberate, audited commit (it is also how an operator empties a gateway on purpose; rollback undoes it).

**Backoff.** `attempt` starts at 0 and is reset by a healthy frame. Before each reconnect (never before the first connect): `cap = min(BackoffMax, BackoffMin << attempt)` (stop shifting once `cap >= BackoffMax`, so it cannot overflow), `d = cap/2 + Rand()*cap/2` ("equal jitter"), `Sleep(ctx, d)`, then `attempt++`. If `Sleep` returns an error (ctx done), the stream reader exits. Example with `BackoffMin=100ms`, `BackoffMax=400ms`, `Rand` always 0: sleeps of 50, 100, 200, 200 ms ..., then 50 ms again after a healthy frame. Equal jitter instead of full jitter: it still spreads the reconnect storm after a control-plane restart, but never reconnects faster than `BackoffMin/2`, so a control plane that answers 401 instantly cannot be hammered in a tight loop.

**Acknowledgements** (contract in `internal/config/ack.go`; both return after an O(1) critical section and a non-blocking send on `notify`, no I/O):

- `Applied(v)`: if `pending[v]` is missing, log Warn and return. Otherwise `cursor = max(cursor, v)`; `lkgPending = pending[v]` (only if `LKGPath != ""`); delete every pending entry `<= v`; `reportDirty = true`; signal.
- `Rejected(v, err)`: if `pending[v]` is missing, log Warn and return. Otherwise `lastRejected = max(lastRejected, v)`, `lastErr = err.Error()` made valid UTF-8 first (invalid bytes replaced by U+FFFD, so JSON encoding cannot inflate it past the server's limit) and then truncated to 1024 bytes on a rune boundary; delete pending entries `<= v`; `cursor` and the LKG file are left alone; `reportDirty = true`; signal; log at Error.
- **A rejected version is not re-requested and not re-applied within the process.** It stays `<= delivered`, so when the stream offers it again (every reconnect with `since = cursor < v` gets head `v`), step 3 drops it. That costs one event per reconnect and no rebuild, so there is no hot loop of failing builds. `cursor` stays at the version actually running, so the `since` the control plane sees (and shows as `applied_version`) is truthful. After a gateway **restart**, `delivered` starts again from the LKG version, so `v` is tried exactly once more. That is deliberate: the usual causes of a build failure that passed validation (a missing JWT key file or secret env var) are fixed by a redeploy. To make running gateways retry without a restart, the operator re-publishes it: `POST /v1/config/rollback {"to_version": v}` creates `head+1` with the same body. Alternatives rejected: advancing `cursor` on `Rejected` (the control plane would believe a version is running that is not); remembering nothing and rebuilding on every reconnect (a failing build every reconnect, plus log spam).

**Worker goroutine** (the only place with disk or status-network I/O):

- Loop on `notify` or `ctx.Done()`. On `notify`, under the mutex take `lkg := lkgPending; lkgPending = nil` and, if `reportDirty`, a report `{applied: cursor, last_rejected: lastRejected, error: lastErr}` and clear `reportDirty`. Then, outside the mutex, the LKG write **first** and the status POST second, so by the time the control plane shows "applied N" the gateway can also restart into N. (The `since` cursor already advanced in memory inside `Applied`; it does not wait for the disk write.)
- **LKG write** (if `lkg != nil`): `json.Marshal(lkg)` → `os.CreateTemp(filepath.Dir(LKGPath), filepath.Base(LKGPath)+".tmp-*")` (mode 0600) → write → `Sync` → `Close` → `os.Rename(tmp, LKGPath)`. On any error remove the temp file and log at Error; it is not retried until the next `Applied`, and `cursor` is not rolled back (the config is live in memory; the file only matters after a restart).
- **Status POST** (if a report was taken): `POST {BaseURL}/v1/dataplanes/{url.PathEscape(NodeID)}/status`, JSON body with exactly the `controlplane.StatusReport` shape (the source declares its own unexported struct with the same tags), `Content-Type: application/json`, the Bearer token, ctx = `context.WithTimeout(watchCtx, StatusTimeout)`. Anything but 204, or a transport error: log at Warn. Not retried; the next ack sends a fresher report anyway.
- On `ctx.Done()`: if `lkgPending != nil`, perform that final LKG write (disk only, no network), then exit.

**Shutdown.** Canceling the Watch ctx cancels the stream request ctx, so the blocked body read returns and the reader exits (a backoff `Sleep` returns `ctx.Err()`). The worker does its final LKG write and exits. Then the closer goroutine closes the channel. `Applied`/`Rejected` calls after this only update state and do a non-blocking send on `notify` (never closed, nobody reading), so they cannot panic or leak.

**cmd/gateway wiring (designed, not implemented yet: `cmd/gateway` currently accepts only `-config`).** `-control-plane URL` (mutually exclusive with `-config`), `-node-id` (default `os.Hostname()`, which is the pod name in Kubernetes), `-lkg-file` (default `""`; the Kubernetes manifest points it at an `emptyDir`, which survives container restarts but not rescheduling), and `TOLLGATE_DATAPLANE_TOKEN` from the environment (startup fails if empty).

**Cross-plane contract test.** `internal/controlplane` may import `internal/config`, so one end-to-end test runs `NewHandler` (with `MemStore`) behind `httptest.Server` and points a real `config.NewRemoteSource` at it: write → event → `Applied` → status visible in `GET /v1/dataplanes`. It pins the wire format from both sides.

## 6. Control plane (Phase 4)

Storage, versioning, rollback and migrations are decided in ADR 0008. Go contracts: `internal/controlplane/store.go`, `hub.go`, `api.go`.

### 6.1 Storage

```sql
-- internal/controlplane/migrations/0001_config_versions.sql (applied by controlplane.Migrate, ADR 0008)
CREATE TABLE config_versions (
    version          BIGINT      PRIMARY KEY CHECK (version > 0),   -- head+1 under advisory lock: gapless, monotonic
    body             JSONB       NOT NULL,                          -- full config.Config, body.version = version
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    author           TEXT        NOT NULL DEFAULT '',
    comment          TEXT        NOT NULL DEFAULT '',
    rolled_back_from BIGINT      REFERENCES config_versions (version) ON DELETE SET NULL,
    CONSTRAINT config_versions_body_version CHECK ((body ->> 'version')::BIGINT = version),
    CONSTRAINT config_versions_rollback_older CHECK (rolled_back_from IS NULL OR rolled_back_from < version)
);
```

Writers are serialised with `pg_advisory_xact_lock`. `SELECT … FOR UPDATE` on the head row is not enough: under READ COMMITTED the second writer, once unblocked, is handed the *old* head row, computes the same `head+1` and fails on the primary key (a spurious 500 without a retry loop), and on an empty table there is no row to lock at all (ADR 0008).

An empty database is version 0, the empty config. Version 0 is never stored or streamed; gateways stay not-ready until the first write.

### 6.2 Wiring and listener

`cmd/controlplane` is `main` → `run(ctx, args, logOut io.Writer, getenv func(string) string) int`. `run` touches no process-global state (its own `FlagSet`, injected env and log sink), so tests call it directly. `main` uses `signal.NotifyContext` plus `context.AfterFunc(ctx, stop)`, so a second signal kills the process at once.

| Flag | Default | Meaning |
|---|---|---|
| `-listen` | `:9000` | the one listener (below) |
| `-database-url` | `$TOLLGATE_DATABASE_URL` | Postgres DSN; the env var is the fallback when the flag is empty |
| `-memory-store` | false | `MemStore`, nothing persisted: **development only** (logged at Warn). Mutually exclusive with `-database-url` |
| `-seed` | `""` | YAML config stored as the first version if the store is empty |
| `-drain-timeout` | 10 s | `srv.Shutdown` budget; must be > 0 |
| `-heartbeat` | 15 s | stream heartbeat (`Options.Heartbeat`); must be > 0 |

Exit codes: **0** clean shutdown (or `-h`); **2** flag errors (unparseable flags, both or neither store, non-positive durations); **1** startup or runtime failure: a token missing, shorter than 32 bytes or equal to the other (from env only, never logged), database startup, seed failure, listen failure, a server error, or a **drain that exceeds `-drain-timeout`**.

```go
// cmd/controlplane run(), Postgres path
startCtx, cancel := context.WithTimeout(ctx, 10*time.Second) // connect + migrate fail fast
pool, err := pgxpool.New(startCtx, dsn)                      // error logged without the DSN
store, err := controlplane.NewPostgresStore(startCtx, pool)  // runs Migrate (ADR 0008); err → exit 1
hub, nodes := controlplane.NewHub(), controlplane.NewNodes(nil, 0)
applySeed(...)                                // only if -seed is set, see below
reg := prometheus.NewRegistry(); cpm := metrics.NewControlPlane(reg)
h, err := controlplane.NewHandler(controlplane.Options{Store: store, Hub: hub, Nodes: nodes,
    AdminToken: adminTok, DataPlaneToken: dpTok, Heartbeat: *heartbeat,
    Metrics: metrics.Handler(reg), Logger: logger})            // err → exit 1
srv := &http.Server{Handler: instrument(h, cpm),
    ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second, MaxHeaderBytes: 64 << 10}
    // no ReadTimeout/WriteTimeout: they would kill SSE streams; stream writes have their own deadline
srv.RegisterOnShutdown(hub.Close)       // SSE streams end at once instead of holding the drain
srv.ConnState = fresh.track             // remember StateNew connections ...
srv.RegisterOnShutdown(fresh.closeAll)  // ... and close them on shutdown
```

- **Seed.** `-seed file.yaml` is parsed with `config.ParseYAML` and stored with `Store.Update(UpdateRequest{IfMatch: 0, Author: "seed"})`, then published. `IfMatch 0` succeeds only on an empty store, so two control planes starting together cannot both seed. A store that already has a head is left alone (logged, not an error): the stored config is the source of truth, the seed only bootstraps.
- **Fresh connections.** `http.Server.Shutdown` also waits (up to 5 s) for connections that were accepted but never sent a request (`StateNew`). A pooled HTTP client's speculative connection held the drain to its timeout in testing, so those connections are closed at shutdown; they carry no work.
- **Metrics.** `instrument` labels `tollgate_cp_api_*` by method and the matched **mux pattern** (`r.Pattern`, e.g. `PUT /v1/routes/{name}`; `unmatched` when no pattern matched), never the raw path. The head-version gauge is set from `Store.Head` at startup and then by a goroutine subscribed to the `Hub` (subscribed before reading head). The connected-data-planes gauge is sampled from `Nodes.List` every 250 ms, because `Nodes` has no change hook.

**One listener** serves `/v1/*`, `/healthz`, `/readyz` and `/metrics`. The gateway separates ops endpoints from its traffic port because the traffic port is public. That reason does not apply here: the whole control plane is cluster-internal (ADR 0001), every `/v1` endpoint requires a token, and `/metrics` exposes no config or secrets. One port means one Service, one probe target and one Compose port. Alternative: a separate `-ops-listen` so a NetworkPolicy can let Prometheus and the kubelet reach only the ops port. It is cheap to add later if a policy needs it.

**Shutdown** on SIGTERM: `srv.Shutdown(-drain-timeout)` (it runs `hub.Close`, which ends every stream immediately, and closes fresh connections; non-stream requests finish; on timeout `srv.Close` and exit 1) → wait for the serve goroutine → cancel and wait for the two metrics goroutines → `pool.Close()` (deferred) → exit. No shutdown delay: there is one replica, and gateways reconnect with backoff while serving their current config.

### 6.3 Rules that apply to every `/v1` endpoint

1. **Routing.** Go 1.22 `ServeMux` method patterns. An unknown path is 404 and a known path with the wrong method is 405 (with `Allow`), both straight from `ServeMux` with its plain-text body, **before** any token check. Unknown query parameters are ignored. `{name}` and `{node}` are single path segments (`PathValue`, already unescaped).
2. **Check order** (tests rely on it): token → path, query and header syntax (400) → body read (413) and decode (400) → name check (400) → Store (412, then 404/400, then 500).
3. **Auth.** `Authorization: Bearer <token>`: scheme matched case-insensitively, one space, the rest is the token. Both sides are SHA-256 hashed and compared with `subtle.ConstantTimeCompare`. A missing header, another scheme, a wrong token, or the right token on the wrong endpoint is **401** with `WWW-Authenticate: Bearer` and body `{"error":"unauthorized"}` (the same body in every case). The admin token is accepted on every `/v1` endpoint except `GET /v1/config/stream` and `POST /v1/dataplanes/{node}/status`; the data-plane token is accepted **only** on those two. Tokens must be ≥ 32 bytes and different from each other, otherwise `NewHandler` returns an error and the process exits.
4. **Bodies.** Read through `http.MaxBytesReader(MaxBodyBytes = 1 MiB)`; exceeding it is **413** `{"error":"request body too large"}`. Decoded with `json.Decoder.DisallowUnknownFields`; malformed JSON, an unknown field, a wrong type, an empty body (where a body is required) or anything but whitespace after the top-level value is **400**. The request `Content-Type` is not checked (so `curl -d` works). Bodies of GET and DELETE are ignored.
5. **Responses.** JSON bodies have `Content-Type: application/json`. Every non-2xx response from `/v1` (except the `ServeMux` 404/405) has body `ErrorBody` `{"error":"..."}`. A 500 body is always `{"error":"internal error"}`; the detail is logged.
6. **Versions.** Every 2xx response about config carries `ETag: "<version>"` (quoted decimal): the head version for reads of head, routes and upstreams; the stored version for `GET /v1/config/versions/{version}`; the **new** version for every successful write (including 204 deletes). There is no `X-Config-Version` header.
7. **If-Match** (optional, on every write: PUT, DELETE, rollback): exactly one entity tag, `"N"` or bare `N`, N a non-negative decimal int64. Anything else (`*`, `W/"N"`, a list, garbage) is **400**. It is compared with head under the write lock; a mismatch is **412** `{"error":"controlplane: version conflict: if-match N, head is M"}` and nothing is written. Because the Store checks `If-Match` before running `mutate`, a stale `If-Match` on a DELETE of a missing name is 412, not 404. `If-Match: "0"` succeeds only on an empty store.
8. **Audit headers** (optional, on every write): `X-Tollgate-Author` (≤ 128 bytes) and `X-Tollgate-Comment` (≤ 1024 bytes), stored verbatim. Longer is **400**.
9. **Store error mapping:** `errors.Is(err, ErrVersionConflict)` → 412; `ErrNotFound` → 404; `config.ErrInvalid` → 400 with `{"error": err.Error()}` (the joined, one-problem-per-line validation message); anything else → 500. Non-stream handlers call the Store with a `StoreTimeout` (10 s) ctx.
10. **Publishing.** After every successful write, the handler calls `Hub.Publish(cfg)` with the returned config **before** writing the response, then never modifies `cfg`.
11. **Every successful write creates a new version**, even if the document did not change (ADR 0008).
12. **Names.** The path segment is the object's identity. A PUT body whose `name` is `""` takes the path name; a non-empty `name` that differs from the path is **400** `{"error":"name in body does not match path"}`. Renaming is delete + create. PUT replaces an existing object **in place** (same slice position) or appends a new one. List responses are sorted by name.

### 6.4 Endpoints

All `/v1` rows can also return 401 (rule 3) and 500 (rule 9). "Write errors" means: 400 (body, If-Match or audit-header syntax, or the resulting config fails `Validate`), 412 (If-Match), 413 (body too large).

| # | Method and path | Token | Request | Success | Errors |
|---|---|---|---|---|---|
| 1 | `GET /v1/config` | admin | — | 200, head `config.Config`; `ETag` head. Empty store: `{"version":0,...}`, `ETag: "0"` | — |
| 2 | `PUT /v1/config` | admin | full `config.Config`; its `version` is ignored | 200, stored config (normalized, new version); `ETag` new | write errors |
| 3 | `GET /v1/config/versions?limit=L&before=B` | admin | — | 200 `VersionList`, descending. `limit` default 50, values > 500 clamped to 500; `before` absent = from head | 400: L or B not a decimal integer, L < 1, B < 1 |
| 4 | `GET /v1/config/versions/{version}` | admin | — | 200, that version's `config.Config`; `ETag: "<version>"` | 400: not a decimal integer or negative. 404: 0 or not stored |
| 5 | `POST /v1/config/rollback` | admin | optional `RollbackRequest`; empty body, `{}` or `to_version: 0` = head−1 | 200, the new config (version head+1, body of the target); `ETag` new | write errors (400 also: `to_version` < 0, or the target is invalid under today's rules). 404: target not in 1..head, including the default target when head ≤ 1 |
| 6 | `GET /v1/routes` | admin | — | 200 `RouteList`; `ETag` head | — |
| 7 | `GET /v1/routes/{name}` | admin | — | 200 `config.Route` (normalized); `ETag` head | 404: no such route |
| 8 | `PUT /v1/routes/{name}` | admin | `config.Route` | **201** + `Location: /v1/routes/{name}` if it did not exist, **200** if replaced; body = stored route; `ETag` new | write errors (400 also: body name ≠ path; unknown upstream; duplicate host + path_prefix; ...) |
| 9 | `DELETE /v1/routes/{name}` | admin | — | 204, no body; `ETag` new | 404: no such route. 412 |
| 10 | `GET /v1/upstreams` | admin | — | 200 `UpstreamList`; `ETag` head | — |
| 11 | `GET /v1/upstreams/{name}` | admin | — | 200 `config.Upstream` (normalized); `ETag` head | 404: no such upstream |
| 12 | `PUT /v1/upstreams/{name}` | admin | `config.Upstream` | 201 + `Location: /v1/upstreams/{name}` / 200, as row 8 | write errors (400 also: body name ≠ path; both or neither of backends/consul; bad backend URL; ...) |
| 13 | `DELETE /v1/upstreams/{name}` | admin | — | 204; `ETag` new | 404: no such upstream. **400: still referenced by a route** (the validation error names the route; there is no cascade). 412 |
| 14 | `GET /v1/config/stream?node=ID&since=N` | **data-plane** | `Last-Event-ID` optional | 200 `text/event-stream` (§6.5) | 400: `node` missing or invalid; `since` or `Last-Event-ID` not a non-negative decimal int64. 500: `Store.Head` failed (before any stream bytes) |
| 15 | `POST /v1/dataplanes/{node}/status` | **data-plane** | `StatusReport` | 204, no body | 400: invalid `{node}`; malformed body or unknown field; `applied` or `last_rejected` < 0; `error` > 1024 bytes. 413 |
| 16 | `GET /v1/dataplanes` | admin | — | 200 `DataPlaneList` (sorted by node; `[]` if none); no `ETag` | — |
| 17 | `GET /healthz` | none | — | 200 `text/plain` `ok` (process is up) | — |
| 18 | `GET /readyz` | none | — | 200 `text/plain` `ok` if `Store.Ping` succeeds within 2 s (database reachable **and** schema at the newest embedded migration) | 503 `text/plain` `not ready` |
| 19 | `GET /metrics` | none | — | Prometheus exposition from `Options.Metrics` | 404 if `Options.Metrics` is nil |

A valid node ID is 1..128 bytes of `[A-Za-z0-9._-]` starting with a letter or digit (the same rule `RemoteSource` enforces).

Seeding: `PUT /v1/config` with the whole document (optionally `If-Match: "0"`) avoids intermediate versions such as "upstream exists, route not yet", which `PUT /v1/routes/...` one object at a time would stream to gateways.

### 6.5 The config stream (`GET /v1/config/stream`)

Handler sequence, in this order:

1. Token check (data-plane), then parameters: `node` required and valid; `since` optional (default 0); a non-empty `Last-Event-ID` header overrides `since`. Any syntax error → 400 JSON.
2. `sub, cancel := hub.Subscribe(); defer cancel()`. **Subscribe first.**
3. `head, err := store.Head(ctx)` (StoreTimeout). Error → 500 JSON (no stream bytes written yet). Reading head **after** subscribing means a version committed and published between the two steps is caught by one or the other; it cannot fall into a gap.
4. Before committing anything, clear the write deadline (which removes any server-wide `WriteTimeout` and proves per-write deadlines work) and check that the writer can flush (through `Unwrap` chains, so the metrics wrapper is transparent). If either is unsupported → 500 JSON instead of a stream that could hang or buffer forever. Then response headers: `Content-Type: text/event-stream`, `Cache-Control: no-store`, `X-Accel-Buffering: no`; `WriteHeader(200)`; flush (so the client sees the stream open even when there is nothing to send).
5. `disconnect := nodes.Connect(node, since); defer disconnect()`.
6. `lastSent := since`. If `head.Version > since`: write head as an event, `lastSent = head.Version`, `nodes.Offered`. If `head.Version == since`: send nothing. If `since > head.Version` (database wiped): log Warn, send nothing until a version > `since` is published (ADR 0005, Consequences).
7. Loop on `select`:
   - `r.Context().Done()` (client gone) → return.
   - `cfg, ok := <-sub`: `!ok` (hub closed: shutdown) → return; if `cfg.Version > lastSent` → write event, `lastSent = cfg.Version`, `nodes.Offered`; else ignore (the primed slot or a version already sent as head).
   - heartbeat ticker (`Options.Heartbeat`, default 15 s; first tick one interval after connect) → write `HeartbeatComment`, `nodes.Seen`.
8. Every write: `http.NewResponseController(w).SetWriteDeadline(now + StreamWriteTimeout)`, write, `Flush`. Any error (including a flush that is unsupported) → return.

Wire format, byte-exact (`json.Marshal` of the full `config.Config`, one line):

```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-store
X-Accel-Buffering: no

id: 42
event: config
data: {"version":42,"defaults":{"timeouts":{}},"auth":{},"routes":[...],"upstreams":[...]}

: heartbeat

```

The server never sends `retry:` or any event type other than `config`. On shutdown the handler returns and the response ends normally (end of chunked body); the client sees EOF and reconnects with its own backoff (§5.1), which keeps failing harmlessly until the control plane is back.

### 6.6 Data-plane status

`POST /v1/dataplanes/{node}/status` with `{"applied": 42, "last_rejected": 43, "error": "build: jwt public key: open /keys/jwt.pem: no such file"}`. The report describes the gateway process's whole current state, so `Nodes.Report` overwrites `applied_version`, `rejected_version` and `last_error`. `GET /v1/dataplanes` returns, per node, `connected`, `connected_at`, `last_seen`, `last_offered_version`, `applied_version` (from `since` at connect, then from reports), `rejected_version` and `last_error` (`NodeStatus` in `hub.go`). The registry is in memory, informational, lost on restart, and drops disconnected nodes not seen for 1 h (lazily, on `List`). The control plane never acts on a rejection.

## 7. Goroutines and shutdown

Every goroutine has exactly one owner and one way to stop.

| Goroutine | Owner | Stops when |
|---|---|---|
| HTTP server(s) | `cmd/gateway` (traffic and admin, each via `proxy.Serve`), `cmd/controlplane` | their own ctx canceled → `srv.Shutdown(drain)`; `Serve` returns only after its serving goroutine exits |
| ConfigSource watcher | source | its Watch ctx canceled; it then closes its channel |
| Reconcile loop (`Gateway.Run`) | `cmd/gateway` | config channel closed or ctx done; it first stops and waits for everything it started (below) |
| Health `Checker.Run` and limiter sweepers (`TokenBucket.Run`, three: authenticated, IP-keyed, failed-auth) | `Gateway.Run` (only for limiters the Gateway created itself) | `Gateway.Run` returning; it waits for all of them, then closes idle upstream connections |
| Probe (one per backend pointer) | `health.Checker.Run` | per-probe ctx canceled by `Sync`, or `Run`'s ctx; `Run` waits for all |
| `context.AfterFunc` deadline callback (requests with a body only) | the request | runs once if the request deadline fires mid-request; `finishBody` (deferred) waits for it under the request's mutex |
| Discovery watch and forwarder (one per service, Phase 5) | reconcile loop | per-watch child ctx canceled (service unreferenced) or root ctx; every forwarder send selects on that ctx |
| RemoteSource stream reader (Phase 4) | `RemoteSource` | Watch ctx: cancels the stream request (blocked body read returns) and any backoff `Sleep` |
| RemoteSource worker: LKG write + status POST (Phase 4) | `RemoteSource` | Watch ctx, after a final LKG write if one is pending; the channel closes only after both RemoteSource goroutines exit (§5.1) |
| SSE stream handlers (control plane) | `http.Server` | client disconnects, write deadline exceeded, or `Hub.Close` through `RegisterOnShutdown` |
| SSE hub | none: the hub has **no goroutine** (mutex + non-blocking sends) | `Hub.Close` closes every subscriber channel |
| Control-plane metrics: head-version subscriber, connected-planes sampler (250 ms) | `cmd/controlplane` | background ctx canceled after the server has drained (or the hub closed its channel) |

**Gateway process.** `main` → `run(ctx, args, logOut)` → `runWithSource(ctx, args, logOut, newSource)`. `run` passes a `FileSource` factory; `runWithSource` is the seam tests use to inject a source. `main` uses `signal.NotifyContext` and `context.AfterFunc(ctx, stop)`: after the first signal, default signal handling is restored, so a **second signal kills the process immediately**, skipping whatever is left of the delay and the drain. (The Phase 0 plan was "skip the delay but still drain"; the implemented behaviour is the plain kill.) Exit codes: 0 clean, 2 flag errors, 1 startup failure, component failure or drain timeout.

Three contexts, canceled in order (none derives from the signal ctx, so each stage starts only after the previous one finished): traffic, admin, background (reconcile loop and config source). Shutdown on the first SIGTERM:

1. Mark not ready, so `/readyz` returns 503 and the load balancer drains this pod.
1b. Wait `-shutdown-delay` (default 0; 5 s in the Kubernetes manifest), still serving traffic normally. In Kubernetes, endpoint removal and SIGTERM happen concurrently, so kube-proxy or the ingress can keep sending new connections for a few seconds after SIGTERM. Without the delay, those requests hit a closed listener. The wait ends early **only if a component fails** during it (nothing is left worth waiting for); a second signal does not shorten it, it kills the process. The in-process delay replaces a `preStop: sleep` hook (the distroless image has no `sleep`; Kubernetes 1.30+ `lifecycle.preStop.sleep` is an equivalent alternative). `terminationGracePeriodSeconds` must be at least shutdown-delay + drain + a margin.
2. Drain the traffic server (`-drain-timeout`, default 15 s). New connections are refused and in-flight requests finish. Then stop the admin server (2 s), which stayed up meanwhile so probes see 503 rather than a refused port.
3. Cancel the background ctx, so the config source and reconcile loop (and through it the Checker and sweepers) exit.
4. Wait for them, collect any reported errors, then return. Nothing uses `os.Exit` while goroutines are running.

**Component failure.** If a component fails on its own (a listener error, the config source failing to start), the process marks itself not ready and goes straight to step 2, with **no shutdown delay**, and exits 1. That includes the **config stream ending unexpectedly**: `Gateway.Run` returns nil when its source closes the channel, but without the reconcile loop there are no health checks or limiter sweeps, so `cmd/gateway` reports it as an error (`errSourceEnded`) unless the background ctx was canceled.

## 8. Error-to-status mapping (data plane)

Every error response is a short `text/plain` body (the status text, never backend addresses or error details) with `X-Request-ID` and `X-Content-Type-Options: nosniff`. "Reported" means counted by passive health (ADR 0009).

| Condition | Status | Reported |
|---|---|---|
| No snapshot yet | 503 | no |
| `CONNECT` | 405, before routing | no |
| An `Upgrade` value with a byte outside printable ASCII | 400, before routing | no |
| Unsafe path (§4 step 0b: dot or empty segment, `;`, `\`, encoded `/` `\` `.`, control byte; decoded or escaped form) | 400, before routing | no |
| No route matches | 404 | no |
| Missing or invalid credentials | 401 + `WWW-Authenticate` | no |
| Too many failed auth attempts from this client-IP key | 429 + `Retry-After`, verification not attempted | no |
| Invalid credentials while the failed-auth throttle is at capacity | 429 + `Retry-After` (fail closed) | no |
| Rate limited, or limiter at capacity for a new key | 429 + `Retry-After` | no |
| `ErrNoHealthyBackends` | 503 | no |
| Client-caused request body error (malformed chunking, reset or half-close mid-body) | 400 + `Connection: close` | no |
| Stalled upload: the request deadline fired while the gateway was still waiting for the client's body | 408 + `Connection: close` | no |
| Client canceled (inbound ctx canceled) | 499, written (so net/http cannot send an implicit 200 to a half-closed client) | no |
| Dial failure, connection reset, EOF or malformed response before headers | 502 | yes |
| Response-header timeout, or the request deadline with a complete request (backend slow) | 504 | yes |
| Backend response | the backend's status | yes (5xx = failure) |

Precedence among transport failures: 408, then 400, then 499, then 502/504 (a client that resets mid-upload gets 400, not 499).

## 9. Metrics

Defined in `internal/metrics` and registered on an injected registry. The control-plane family is wired in `cmd/controlplane`; the data-plane family is wired into `internal/proxy` and `cmd/gateway` in Phase 6.

| Name | Type | Labels | Notes |
|---|---|---|---|
| `tollgate_requests_total` | counter | `route`, `upstream`, `code` | unmatched requests (400, 404, ...) use `route`/`upstream` = `none` (`metrics.RouteNone`) |
| `tollgate_request_duration_seconds` | histogram | `route`, `upstream` | buckets 0.5 ms to 10 s |
| `tollgate_ratelimited_total` | counter | `route`, `reason` | `reason` = `limit` \| `capacity` \| `auth_throttle` (ADR 0004) |
| `tollgate_auth_failures_total` | counter | `route`, `reason` | 401s by reason (e.g. `missing`, `invalid`) |
| `tollgate_backend_up` | gauge | `upstream`, `backend` | 1 = in rotation |
| `tollgate_backend_inflight` | gauge | `upstream`, `backend` | |
| `tollgate_backend_ejections_total` | counter | `upstream`, `backend` | one per successful `TryEject` |
| `tollgate_config_version` | gauge | | applied version |
| `tollgate_config_apply_failures_total` | counter | | snapshot builds that failed |
| `tollgate_config_stream_connected` | gauge | | 1 while `RemoteSource.Connected()` |
| `tollgate_cp_api_requests_total` | counter | `method`, `route`, `code` | control plane; `route` is the mux pattern, `unmatched` if none |
| `tollgate_cp_api_request_duration_seconds` | histogram | `method`, `route` | control plane |
| `tollgate_cp_config_head_version` | gauge | | control plane; from `Store.Head` at startup, then a `Hub` subscriber |
| `tollgate_cp_dataplanes_connected` | gauge | | control plane; sampled from `Nodes.List` every 250 ms |

- Labels are only route and upstream **names**, backend IDs, mux patterns and small closed sets (code, reason). Never raw paths, client IDs, IPs or query strings, to keep cardinality bounded. The status-code label strings are precomputed, so the hot path does not allocate for them.
- Every `tollgate_backend_*` series of a backend (`up`, `inflight`, `ejections_total`) is **deleted** (`ForgetBackend`) when the reconcile loop prunes that backend from the Registry. Otherwise Consul churn would leak series forever, and dashboards would show dead backends as down.
- On the gateway, `/metrics`, `/healthz` and `/readyz` are served **only** on the admin listener (`-admin-listen`), never on the traffic port. The control plane serves them on its one listener (§6.2).
- The access log (`metrics.LogAccess`) writes one flat line per request with no header, query or credential fields; 5xx at Warn, everything else at Info.

## 10. Open questions (for the orchestrator or owner)

1. **`internal/controlplane`** is not in the CLAUDE.md layout. Without it, all control-plane logic would sit in `package main`, which hurts testability and the ≥ 80% `internal/` coverage target. **Resolved at the Phase 0 checkpoint: added.**
2. **Go version.** The plan requires Go 1.23+. The `go.mod` directive is now **`go 1.26.9`**: it was `go 1.23.0` after Phase 0, and the consul/api and prometheus dependencies raised it. CI and the Docker base image must therefore use a 1.26.9+ toolchain. (The directive also selects GODEBUG compatibility defaults; the proxy restores clean raw queries itself so its behaviour does not depend on `urlmaxqueryparams`.)
3. **No retries in v1.** A connection-refused error on an idempotent request could be retried once on another backend, which would improve chaos-test failover numbers. Deferred to keep the proxy small. Revisit after Phase 8.
4. **Client IP for rate limiting comes from `RemoteAddr` only** (IPv6 masked to /64, §11 item 10). Behind a Kubernetes load balancer, every client may appear as the same IP. A `trusted_proxies` setting (CIDRs whose X-Forwarded-For we believe) could be added in Phase 3 or 7.
5. **Passive failure definition:** any 5xx (current) or only 502/503/504 plus transport errors (Envoy's "gateway failure" mode)? See ADR 0006. Which attempts are reported at all is settled separately in ADR 0009.
6. **Phase 1 needs some balancer** before Phase 2 exists. **Resolved:** Phase 1 shipped `Backend`, `Registry` and `NewRoundRobin`; least-connections and health followed in Phase 2.

## 11. Review amendments (Phase 0 review)

Builders and test-writers: each row is binding for its phase. The "Contract change" column lists Go doc-comment and signature changes that the **phase builder** applies; the architect does not edit `.go` files. Choices that the orchestrator left to the architect are marked **[A]**.

| # | Finding (one line) | Decision | Implemented in phase | Contract change (file: what) |
|---|---|---|---|---|
| 1 | Router matches a path that the upstream normalises differently (`/public/%2e%2e/admin`), so auth can be bypassed. | 400 **before routing** if `r.URL.Path` **or** `r.URL.EscapedPath()` has a `.` or `..` segment, an empty segment (`//`), `;` **anywhere** (orchestrator decision after the Phase 1 review: path parameters are rejected outright, which also covers Tomcat-style `..;`), a `\`, `%2f`, `%5c` or `%2e` (case-insensitive), or a byte < 0x20 or 0x7f. `Validate` rejects a `path_prefix` with any of these (§5.0). The router matches `r.URL.Path`, which is exactly the forwarded path. | 1 (fix round) | `internal/proxy`: unexported `checkPath(u *url.URL) error`, called before `router.Match`. `internal/config/config.go` `Route` doc: add rule "0. unsafe paths → 400 before matching". |
| 2 | LKG file and version are committed before the snapshot build can fail. | Optional `config.Acknowledger`. The reconcile loop calls `Applied`/`Rejected` after each **config-triggered** build. `RemoteSource` writes LKG and advances `since` only on `Applied`. Best-effort `POST /v1/dataplanes/{node}/status`. `/v1/dataplanes` shows offered/applied/rejected. An LKG that fails to parse, validate or build is ignored (log, wait for the stream). | 4 | **Done in `internal/config/ack.go`** (Phase 4 design; do NOT also declare it in `source.go`): `type Acknowledger interface { Applied(version int64); Rejected(version int64, err error) }`, with the full caller/implementation contract in its doc comment. `ConfigSource` doc point 3: "last-known-good" becomes "last **applied** copy". `internal/proxy` `Gateway.Run`: `if a, ok := src.(config.Acknowledger)`. |
| 3 | A re-added backend gets a new `*Backend` from the Registry but is never probed (Sync diffs by ID). | `Checker.Sync`/`Run` diff by **pointer identity + check settings**. A new pointer for a known ID starts a new probe, and the old pointer's probe stops. | 2 | `internal/health`: `Checker`/`Sync` doc as in §2 sketch. Test: prune ID, re-add, assert the new pointer goes down after `fall` failed probes. |
| 4 | Discovery forwarders can deadlock shutdown and deliver stale sets. | Every forwarder send is a `select` with `ctx.Done()`. Each watch has a loop-assigned `generation`, and the loop drops updates whose generation is not the current one for that key. | 5 | `internal/proxy` (unexported): `endpointUpdate{key string; gen uint64; eps []discovery.Endpoint}`. `internal/discovery/discovery.go` `Resolver` doc point 5: "sends never block (latest-wins replace)". |
| 5 | Control-plane hub can publish v6 before v5 and lose the newest. | The hub tracks the max published version and ignores `<=`. The per-subscriber capacity-1 slot is "highest version wins". The stream handler **subscribes first, then reads head**, and forwards only versions greater than the last one it sent. | 4 | `internal/controlplane`: `Hub.Publish(*config.Config)` doc: "no-op if Version <= max published". `Hub.Subscribe() (<-chan *config.Config, func())` doc: slot replaced only by a higher version. Stream handler doc states the subscribe-then-head order. |
| 6 | Config stream is unauthenticated. | `TOLLGATE_DATAPLANE_TOKEN` (Bearer) is the **only** token for `GET /v1/config/stream` and **[A]** `POST /v1/dataplanes/{node}/status`. `TOLLGATE_ADMIN_TOKEN` is required for every other `/v1/*` endpoint, reads included. Comparison is SHA-256 digest + `subtle.ConstantTimeCompare`. **[A]** Startup fails if either token is < 32 B or the two are equal. No TLS between planes in v1 (ADR 0001, 0005). | 4 | `cmd/controlplane`: read both env vars, validate, exit non-zero otherwise. `internal/controlplane`: `requireToken(digest [32]byte) func(http.Handler) http.Handler`. `internal/config` `RemoteSource` options: `Token string`. `cmd/gateway`: `TOLLGATE_DATAPLANE_TOKEN` required when `-control-plane` is set. |
| 7 | A backend stays down forever after its `health_check` is removed. | When a probe stops because the target lost its health check or is gone, the Checker calls `SetActiveHealthy(true)`. A probe restart (settings changed) keeps the current verdict. | 2 | `internal/health` `Checker` doc. Test: backend marked down, `Sync` without it, then `ActiveHealthy() == true`. |
| 8 | Request timeout cannot be disabled, which contradicts ADR 0003's streaming advice. | Keep `request` as a hard cap with no "disabled" value. Streaming and long-poll routes set a large explicit value (e.g. `"1h"`). ADR 0003 fixed. | Docs only | `internal/config/config.go` `Timeouts.Request` doc: "Hard cap; there is no disabled value (0 = inherit). Streaming/long-poll routes must set a large explicit value such as 1h." |
| 9 | JWT validation gaps. | `Validate`: HS256 requires `hmac_secret_env` and forbids `public_key_file`; RS256 the reverse. `NewJWT` fails closed if the secret is empty or < 32 B, or **[A]** if the RS256 key does not parse as an RSA key of ≥ 2048 bits. Parser uses `jwt.WithValidMethods([]string{alg})`, `WithExpirationRequired()`, `WithLeeway(30s)` **[A]**, `WithTimeFunc(now)`, and `WithIssuer`/`WithAudience` when set. A client-ID claim that is missing, empty or not a string → `ErrInvalidCredentials`. | 3 | `internal/config/validate.go` + package doc rule list: the key-source rules. `internal/auth/auth.go` package doc: "exp required; leeway 30 s; NewJWT errors on weak or missing key". `ErrInvalidCredentials` doc: add "empty or non-string client-ID claim". |
| 10 | Rate limiter can be bypassed or fails open. | `auth: none` identity = client-IP key: port stripped, IPv4 as-is, **[A]** `::ffff:` IPv4-mapped un-mapped first, other IPv6 masked to /64. **[A] At capacity:** per-shard cap `ceil(MaxKeys/32)`. A new key samples ≤ 8 buckets and evicts the first **idle** one (`now >= fullAt`). If none is idle, the new key is denied with 429, `Retry-After: 1` (**fail closed for new keys only**, and throttled buckets are never evicted). **[A] Failed-auth throttle:** separate `TokenBucket`, keyed by client-IP key, burst **10**, refill **1/s**. `Peek` before verifying; if empty → 429 without verification. Only `ErrInvalidCredentials` charges it; a charge that comes back `AtCapacity` answers 429 instead of 401 (fail closed). **Phase 3 fix round:** authenticated and IP-keyed identities use separate limiters, so the Gateway owns three, each with its own key budget (ADR 0004). | 3 | `internal/ratelimit/limiter.go`: `Peek(key string, limit Limit) Decision` on `Limiter` (no consume, no insert), `Decision.AtCapacity bool`, `Options.MaxKeys` doc. `internal/auth`: `ClientIPKey(remoteAddr string) string`. `internal/proxy`: `Options.Limiter`, `Options.AuthLimiter`, `Options.LimiterMaxKeys`; `authFailureLimit()` = `{RatePerSecond: 1, Burst: 10}`; the Gateway creates and `Gateway.Run` sweeps the limiters it owns. |
| 11 | Client disconnects eject healthy backends. | If the inbound request ctx is canceled (client gone → 499), never call `ObservePassive`. Own deadline (`DeadlineExceeded`, 504) is still reported. Outcome is taken at header time, and body-copy errors are not reported. **Phase 2 fix round (ADR 0009):** reporting moved from the ErrorHandler into a `RoundTripper` wrapper; client body errors (400), stalled uploads (408) and pre-balancer rejections (CONNECT, bad Upgrade, bad path) are not reported either. | 2 | `internal/health` `ObservePassive` doc: "callers must not report client-canceled requests". `internal/proxy`: `observingTransport`, `reqBody`, `classifyFailure`. Tests replay the review's scenarios (aborts, CONNECT, bad Upgrade, malformed chunking, stalled upload) more than `consecutive_failures` times, and the backend stays `Available`. |
| 12 | `X-Tollgate-Client-ID` can be spoofed on unauthenticated routes. | Always strip it, in every spelling, from the **outbound** request in `Rewrite` on every route (the inbound request is never mutated); `X-API-Key` is stripped the same way. Set it only when an authenticated `Identity` exists. The client IP is never put in it. | 3 | `internal/proxy`: exported `const HeaderClientID = "X-Tollgate-Client-ID"`, with a doc comment stating the rule. |
| 13 | Graceful shutdown drops requests in Kubernetes. | `-shutdown-delay` (default 0) between marking not-ready and `Shutdown`. The wait ends early only if a component fails; a **second signal kills the process** (it does not skip to the drain, §7). The manifest uses 5 s, with `terminationGracePeriodSeconds` ≥ delay + drain + 5 s. The in-process delay replaces a `preStop` sleep. | 1 (fix round, flag) + 7 (manifest) | `cmd/gateway`: flag `-shutdown-delay duration`. `/readyz` = `!shuttingDown && gw.Ready()`. Order: not-ready → delay → drain traffic → stop admin → cancel background ctx → wait. |
| 14 | Server and client limits are unspecified. | Listener: `ReadHeaderTimeout` 5 s, `IdleTimeout` 120 s, `MaxHeaderBytes` 1 MiB, no `ReadTimeout`/`WriteTimeout`. Transports: `MaxIdleConnsPerHost` 64, `IdleConnTimeout` 90 s. After a swap, unreferenced pool entries get `CloseIdleConnections()` and are dropped. | 1 (fix round) | `internal/proxy` `Serve` doc lists the server settings. Transport-pool doc: per-entry settings plus "retain only keys referenced by the live snapshot". ADR 0002 row updated. |
| 15 | Strict decoding. | YAML `KnownFields(true)` and JSON `DisallowUnknownFields` reject unknown fields everywhere (file, admin API, stream). Upgrade order is **data planes first** (ADR 0005). | 1 (already pinned by tests); 4 for API/stream decoders | `internal/config` `Parse` doc: "unknown fields are an error". Phase 4 JSON decoders (admin API bodies, `RemoteSource`) use `DisallowUnknownFields`. |
| 16 | Version assignment is described two ways (sequence vs head+1). | `version = head + 1` inside the `pg_advisory_xact_lock` transaction. No Postgres sequence anywhere. | Docs only | `internal/config/config.go` `Config.Version` doc: replace "assigns it from a Postgres sequence" with "assigns head+1 inside its advisory-locked write transaction, so versions are gapless and strictly increasing". |
| 17 | `Validate` gaps. | Backend URLs are unique within an upstream after `Normalize` canonicalises them (lower-case scheme and host, default port dropped), so `http://Example.test:80` and `http://example.test` are duplicates. API-key `sha256` is unique; `client_id` uniqueness and 64-hex are already enforced. Route `host` must not contain a port (`net.SplitHostPort` succeeds → invalid), and **[A]** `Normalize` strips `[]` from IPv6 route hosts. `Normalize` sets health `timeout` = `min(DefaultHealthTimeout, interval/2)` **after** defaulting `interval`. Discovery endpoints are de-duplicated by `Address` (lowest `ID` wins). | 1 (fix round); dedupe in 5 | `internal/config/validate.go`, `normalize.go`, and the package-doc rule list in `config.go`. `HealthCheck.Timeout` doc: "default min(1s, interval/2)". Endpoint dedupe is done in the Consul watcher (`discovery.toEndpoints`); the snapshot builder also drops a repeated Registry ID. |
| 18 | The idle sweep after a limit change can hand out extra tokens. | **[A] Accept and document the bound:** over-admission is less than one burst of the **new** limit, per key, once per limit change. Do not store the last-charged limit, because the sweeper never sees the new limit, so it cannot close the gap (ADR 0004). | 3 | `internal/ratelimit/limiter.go` `Options.SweepInterval` doc: state the bound. A test pins it: drain under the old limit, change the limit, advance past the old `fullAt`, sweep, then `Allow` → `Remaining == newBurst-1`. |
| 19 | Metrics leak pruned backends, and ops endpoints are exposed on the traffic port. | `tollgate_backend_*` series are deleted when a backend is pruned. `/metrics`, `/healthz` and `/readyz` are served only on `-admin-listen`. | 6 (admin listener exists since 1) | `internal/balancer/registry.go`: `Prune(keep map[string]struct{}) (removed []*Backend)`, which is source-compatible for existing callers. `internal/metrics`: `ForgetBackend(upstream, backend string)`. `cmd/gateway`: the traffic handler never mounts ops paths. Test: traffic port `GET /metrics` → routed or 404, never metrics. |

Notes on the **[A]** refinements:

- **Row 1, extra rejections:** without the `//` rule, a public route `/` plus a protected `/admin` can be bypassed with `//admin` on any upstream that collapses slashes. Without a `;` rule, Tomcat or Jetty upstreams (which strip `;params` per segment, turning `/x/..;/admin` into `/x/../admin`) reopen the bypass; rejecting `;` anywhere is simpler than parsing path parameters.
- **Row 6, status endpoint:** "applied vs offered" needs a back-channel, because SSE is one-way and `since` is only seen at reconnect. So the data-plane token also covers `POST /v1/dataplanes/{node}/status`.
- **Row 10, IPv4-mapped addresses:** if `::ffff:` addresses are not un-mapped before /64 masking, every IPv4 client of a dual-stack listener collapses into one bucket.
