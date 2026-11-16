# Progress

## Phase 0 — Design (2026-11-14)

### What was built
- Seven ADRs in `docs/adr/`:
  - 0001: split into a control plane and a data plane (two binaries; the gateway keeps serving its last-known-good config if the control plane is down).
  - 0002: hot reload by atomically swapping an immutable snapshot (`atomic.Pointer[Snapshot]`), not an RWMutex.
  - 0003: round-robin and least-connections, selectable per upstream.
  - 0004: a hand-written token bucket per (route, client), kept locally in each gateway.
  - 0005: config pushed over SSE as full snapshots with monotonic versions; rollback publishes a new version.
  - 0006: active probes plus passive ejection, writing different fields on `Backend`.
  - 0007: Consul blocking queries in the data plane for instance discovery.
- `docs/architecture.md`: binaries, packages, the dependency rule, request path, config flow, goroutine ownership.
- Type-only Go files: `internal/balancer/balancer.go` (Backend, Balancer, ErrNoHealthyBackends), `internal/ratelimit/limiter.go` (Limiter, Limit, Decision), `internal/config/config.go` and `source.go` (config model, ConfigSource, ErrAlreadyWatching), `internal/discovery/discovery.go` (Resolver, Query, Endpoint), `internal/auth/auth.go` (Authenticator, Identity).
- `internal/controlplane` is added to the package map. It is not in the CLAUDE.md layout.

### Decisions
- The gateway holds the active config in `atomic.Pointer[Snapshot]`. A request loads it once and finishes on that version. A new snapshot is built off to the side and published with one store.
- Runtime state lives outside the snapshot. A long-lived `balancer.Registry` returns the same `*Backend` for the same backend ID, so health and in-flight counts survive swaps. Rate-limit buckets live in one long-lived limiter.
- `Limiter.Allow(key, limit)` takes the limit on every call. A rate change applies on the next request, and buckets survive reloads.
- Route lookup happens before auth, because auth and rate limits are per route. Enforcement then follows the PLAN order: auth, rate limit, balancer, proxy.
- The `ConfigSource` channel has capacity 1 and is latest-wins, so a slow consumer does not fall behind a burst of updates.
- Rollback publishes a new version. Rolling back from 7 to 6 produces version 8 with the content of 6, so versions only go up.
- Decisions on the open questions: add `internal/controlplane`; `go 1.23.0`; no retries in v1; client IP for rate limiting comes from `RemoteAddr` only; passive failure means any 5xx or transport error.

### Surprises / things to remember
- The architect agent has no shell, so the interface files were written without a compiler. The orchestrator then ran `gofmt -l`, `go vet ./...` and `go build ./...`. All were clean on the first try.
- `go mod init` with the local Go 1.27.4 toolchain wrote `go 1.27.4` into `go.mod`. That would have forced every CI runner and Docker image onto Go 1.27. It was lowered to `go 1.23.0` to match "Go 1.23+".
- PLAN.md's pipeline (auth → rate limit → router) cannot be implemented literally, because auth mode and rate limits are per-route settings. The route has to be matched first.
- `internal/controlplane` was added even though it is not in the CLAUDE.md layout. Without it the admin API, store and SSE hub would sit in `package main`, untestable and outside the `internal/` coverage target.

## Phase 1 — Data plane core (2026-11-14)

### What was built
- `internal/config`: strict YAML/JSON parsing (unknown fields rejected). `Normalize` and `Validate` report one error per problem, joined, each wrapping `ErrInvalid`. `Duration` is human-readable.
- `FileSource` implements `ConfigSource`. It polls the file by content hash and delivers updates on a capacity-1, latest-wins channel. Invalid updates are skipped, so the last good config keeps serving.
- `internal/balancer`: `Backend` runtime state is kept in atomics. `Registry` returns the same `*Backend` for the same `"upstream|url"` ID, so state survives config swaps. Round-robin uses an atomic cursor.
- `internal/proxy`: host plus longest segment-aware prefix router. The immutable `Snapshot` sits behind `atomic.Pointer`. A single-writer reconcile loop runs build, Store, prune Registry, then trim the transport pool.
- `internal/proxy` also holds one prebuilt `httputil.ReverseProxy` per route, and a transport pool keyed by (dial, response-header) timeouts that outlives snapshots. It sets `X-Request-ID` and drains gracefully in `Serve`.
- Error mapping: 502 dial or connection failure, 504 timeouts, 499 client cancel, 503 no snapshot or no healthy backend, 404 no route, 400 unsafe path.
- `cmd/gateway`: `run(ctx, args, logOut)` with flags `-config`, `-listen`, `-admin-listen` (serves `/healthz` and `/readyz`), `-drain-timeout`, `-config-poll`, `-shutdown-delay`.
- Tests were written first for every package. Coverage measured by the orchestrator: config 95.8%, balancer 100% (after Phase 2 additions), proxy 92.2%, cmd/gateway 77.7%. `go vet ./...` is clean and `go test -race ./...` is green.

### Decisions
- Snapshot build runs in two passes: validate with no side effects, then a pass that cannot fail. A rejected config never leaves orphan Registry entries or transports.
- Registry prune runs after `Store`. An in-flight request on the old snapshot may hold a just-pruned `*Backend`. That is safe because `Done` only decrements that object's own counter.
- `ReverseProxy` is built once per route, not per request. The chosen backend reaches `Rewrite` through a context value.
- Dial timeout maps to 502 (backend never reached). Response-header and request timeouts map to 504.
- Path policy (orchestrator decision after review): reject with 400, before routing, any path with a `.` or `..` segment, an empty segment (`//`), `;`, a backslash, encoded `/`, `.` or `\`, or control bytes. Checks run on both the decoded path and the escaped path sent on the wire. Path parameters (`;jsessionid`) are rejected outright, not parsed.
- `FileSource` applies an update only after two consecutive polls see the same hash. It rejects a file with zero routes and always delivers strictly increasing versions. `Validate` still accepts an empty config, because the control plane starts empty.

### Bugs and surprises
1. **Round-robin double share.** "Add 1 to the cursor, modulo N" gave the backend after a down one two turns. With 3 backends and 1 down, traffic split 2/3 to 1/3 instead of 1/2 to 1/2.
   - Caught by the test-first suite.
   - Fix: when `Next` skips `i` unavailable positions, it also advances the cursor by `i`.
2. **Query strings silently rewritten.** With `go 1.23.0` in go.mod and a Go 1.27 toolchain, the compatibility GODEBUG default `urlmaxqueryparams` made `httputil.ReverseProxy` in Rewrite mode re-encode every query. `%20` became `+` and keys were re-sorted, which breaks signed URLs.
   - Caught by the forwarding test (`b=x%20y` arrived as `b=x+y`).
   - Fix: restore the client's raw query when it passes the same cleanliness checks the stdlib uses. Unclean queries still get the sanitised form.
3. **Auth-bypass-shaped routing bug (found in review).** The first implementation only rejected dot segments. The reviewer showed that `//admin`, `/x/..;/admin`, `/x/..%5cadmin`, `/admin%00` and `/admin;jsessionid=1` all matched the public `/` route and were forwarded upstream.
   - Harmless in Phase 1 (no auth yet), but an auth bypass once per-route auth lands.
   - Fix: `checkPath`, 33 unit cases, and raw-TCP end-to-end tests.
4. **Fake 200 for half-closed clients.** When a client sent FIN after its request, Go cancelled the request context. The proxy wrote nothing, and net/http filled in `200 Content-Length: 0`.
   - Fix: the gateway now writes 499.
5. **Half-written config files.** The reviewer truncated the example YAML at every byte offset. 591 of 1623 prefixes still parsed and validated as a different config, one with zero routes (which would 404 everything).
   - Fix: the two-poll debounce and the zero-routes rule.
6. **103 Early Hints dropped X-Request-ID.** `ReverseProxy` clears the response header map after a 1xx.
   - Fix: the ID is now set in `ModifyResponse` and `writeError`.
7. **A test was wrong, not the code.** Found during Phase 3 work; see the Phase 3 notes.

### Review
- Independent reviewer verdict on the first pass: "not ready to sign off". 8 findings (1 high, 3 medium, 4 low), all fixed test-first in a fix round.
- The reviewer found no problems in the concurrency core: snapshot swap, transport retirement with a request in flight, in-flight accounting on abort paths, round-robin atomics, and FileSource channel handling.
- Known deviation: a second SIGTERM kills the process immediately, skipping delay and drain. The intended behaviour was "skip the delay but still drain".

## Phase 2 — Load balancing and health (2026-11-14 to 2026-11-15)

### What was built
- `balancer.NewLeastConn`: picks the available backend with the fewest in-flight requests. Ties rotate.
- `Backend.TryEject`: a compare-and-swap ejection that succeeds only when the backend is not already ejected.
- `Registry.Prune` now returns the backends it removed.
- `health.Checker` (`NewChecker`, `Sync`, `Run`): one probe goroutine per backend, with rise and fall thresholds. `Sync` stores the targets in an `atomic.Pointer` and signals a capacity-1 channel, so many `Sync` calls collapse into one wake-up. The Checker takes no lock.
- `health.ObservePassive`: stateless. The state lives on the `Backend`.
- Wiring: the snapshot build honours `algorithm`. `Gateway.Run` owns the Checker and calls `Sync` after every swap. Passive outcomes are recorded in a RoundTripper wrapper around each route's transport.
- Coverage (measured by the orchestrator): balancer 100%, health 100%, proxy 94.0%.

### Decisions
- In-flight counts live on the shared `*Backend`, so they survive snapshot swaps.
- Least-conn accepts a pick/increment race: two concurrent picks may choose the same backend. The effect is bounded by the number of concurrent pickers and self-corrects. A CAS loop was judged useless because counts change again immediately.
- The Checker diffs targets by `*Backend` pointer and settings, not by ID. A re-added backend is a new pointer.
- When a probe is removed, its backend is reset to up. A probe restart for changed settings keeps the current verdict.
- The first probe is jittered within one interval. Probes never overlap: the next wait is measured from the end of the previous probe.
- Passive: only a success resets the failure streak, so the first failure after cool-off re-ejects.
- All backends down returns 503. There is no panic mode.
- Probes do not follow redirects and ignore `HTTP_PROXY`.

### Bugs and surprises
1. "Eject on exactly the Nth failure" conflicted with "re-eject on the first failure after cool-off". The rule became streak >= N. With a plain `EjectUntil`, every later failure, including requests already in flight, would extend the cool-off and count as a new ejection. The fix is `TryEject`: a burst of 64 concurrent failures ejects exactly once.
2. (HIGH, review) Requests that never reached a backend counted as backend failures. The handler picked a backend before `ReverseProxy` validated the request, and the ErrorHandler fired for client-caused errors. Three `CONNECT /x` requests, three `Upgrade: \xff` headers, or three malformed chunked bodies each produced 502 with zero backend hits, and a normal GET then got 503. A few tiny requests took down an unauthenticated upstream. Fix: CONNECT gets 405 and a bad Upgrade gets 400, both before the balancer. Passive health moved from the ErrorHandler into the transport wrapper, where the outcome is known. Client-side body read errors map to 400 and are not reported.
3. (MEDIUM, review) A stalled upload (`Content-Length: 100`, 2 bytes sent) made the gateway's own deadline count against the backend. The handler also hung: after the deadline, the transport waits for its write goroutine, which is blocked reading the client body, and closing the body takes the same lock. Only an expired read deadline on the client connection breaks the deadlock. Fix: a `context.AfterFunc` callback sets a past read deadline only when a read is actually blocked on the client. The response is 408 and is not reported. Setting the deadline up front was tried and rejected, because it races the gateway's own deadline and can turn a slow-backend 504 into a 499.
4. (review) Least-conn had the same double-share bug round-robin had in Phase 1. With 3 backends and A down, 3000 picks split 2000/1000 between B and C. The original test covered only the all-equal case. Fix: rotate over the tied set in two passes, with no allocation.
5. The Checker's reset-to-up and a predecessor probe's late "down" write could race across two quick Syncs. Fix: the successor always waits for its predecessor to exit before resetting. The wait is bounded because the predecessor is already cancelled.
6. If the config source channel closed unexpectedly, `Gateway.Run` returned nil and the gateway kept serving with health checks stopped. `cmd/gateway` now treats this as a failure and exits 1.

### Review
- First-pass verdict: "NOT READY" (1 high, 1 medium, 5 low).
- The reviewer confirmed as correct: `Done` runs on every exit path, including the `ErrAbortHandler` panic; 401, 429, 404 and 400 never touch the balancer; `TryEject` never extends an active ejection; prune runs before Sync.
- All findings were fixed test-first. `go test -race -count=3` is clean.

## Phase 3 — Rate limiting and auth (2026-11-14 to 2026-11-15)

### What was built
- `ratelimit.TokenBucket`, written from scratch. It has 32 shards, each a mutex and a map. A key maps to a shard with `hash/maphash` and a per-limiter random seed. Refill is lazy, with no timers. Idle buckets are evicted. The package also has `Peek`.
- `auth.NewAPIKey`: looks up the presented key by its SHA-256 digest.
- `auth.NewJWT`: supports HS256 and RS256.
- `auth.ClientIPKey`: the client-IP key function.
- Wiring: request order is path check, route match, auth, rate limit, balancer, proxy. A 401 carries `WWW-Authenticate` and one generic body. A 429 carries `Retry-After` (ceiling seconds, minimum 1) and `X-RateLimit-Limit` and `X-RateLimit-Remaining`.
- `X-API-Key` is always stripped. `X-Tollgate-Client-ID` is always stripped, then set only for authenticated identities.
- Limiters live on the `Gateway`, so buckets survive reloads.
- Coverage: ratelimit 97.3%, auth 98.4%.
- Benchmark (measured by the orchestrator): `BenchmarkAllowParallel` at 187.6 ns/op and 0 allocs/op on an Apple M3 with 8 threads. The test clock takes a shared mutex, so real-clock numbers should be lower.

### Decisions
- `Allow(key, limit)` takes the limit per call, so a changed limit applies on the next request with no migration.
- Key = route name + "\x00" + identity. Identity is the client ID on authenticated routes and the client IP elsewhere. IPv6 is masked to /64, and IPv4-mapped addresses are un-mapped first.
- `X-Forwarded-For` is never trusted.
- A mutex per shard, not atomics: refill is a read-modify-write of several fields, and the maps need a lock anyway.
- `maphash` instead of the ADR's FNV-1a, so an attacker cannot compute offline which keys land in one shard.
- At capacity, a new key may evict only an idle bucket (sampling at most 8). Otherwise it is denied with 429. Throttled buckets are never evicted, so only new keys fail closed.
- Failed-auth throttle: a separate per-IP bucket (burst 10, 1/s) is Peeked before any credential is verified and charged only on invalid credentials. A throttled IP gets 429 even with a valid key. This is a short lockout behind a brute-forcing IP, in exchange for bounded brute-force cost.
- JWT: the algorithm is pinned with `WithValidMethods` and a typed keyfunc. `exp` is required, with 30 s leeway. An HMAC secret under 32 bytes or an RSA key under 2048 bits fails construction.
- Every JWT failure maps to a fixed reason string, because the library's error text quotes the attacker-controlled `alg` header.

### Bugs and surprises
1. Float drift. The first limiter counted whole tokens in float64. Ten 100 ms refills at 1/s summed to 0.9999999999999999, and the burst-1 cap threw the surplus away. A 60-second run admitted 55 instead of 61 (rate 1) and 546 instead of 601 (rate 10). Fix: count nanotokens, so a refill is elapsed_ns × rate, exact for normal rates.
2. On arm64 Go may fuse multiply-add into one FMA instruction, so the refill and the Retry-After calculation could round differently. "Wait exactly Retry-After, then retry" could still be denied. Fix: one shared `accrue` function with an explicit `float64()` conversion. Retry-After is nudged up until that exact expression reaches the target.
3. A test was wrong, not the code. The fractional-accumulation test expected 31 admits for rate 3/s, burst 1, on a 100 ms grid. With burst 1 the bucket can never bank the 0.2 token left over at each 400 ms admit, so the correct answer is 26 (GCRA and x/time/rate agree). The builder refused to bend the code and reported it. The orchestrator corrected the test to burst 2.
4. (MEDIUM, review) Client IDs were not checked for header safety. A validly signed JWT with `sub: "alice\r\nX-Admin: 1"` did not inject a header, because Go's transport refuses the value. It did become a 502 and a passive failure, and three such requests ejected the backend. Fix: client IDs with control bytes or longer than 256 bytes are rejected as invalid credentials (401), in auth and in config validation.
5. (MEDIUM, review) One shared limiter meant a flood of distinct IPs on a public route could fill the key table and deny new clients of authenticated routes. Fix: separate limiters, with separate key budgets, for authenticated identities and for IP-keyed clients.
6. (review) The failed-auth throttle failed open at capacity: an IP that could not be tracked got unlimited verifications. Now a failed attempt that cannot be charged returns 429 instead of 401, so the verification result is not revealed. Valid credentials are unaffected. The failed-auth limiter had also never been capped at all.
7. A burst near MaxInt overflowed the nanotoken arithmetic and produced `X-RateLimit-Remaining: -9223372036854775808`. Rate and burst are now capped at 1,000,000 in validation.

### Review
- Verdict: "approve with fixes". No auth bypass was found after trying alg:none, HS/RS confusion, duplicate headers, hop-by-hop tricks (`Connection: X-API-Key`), trailers, CONNECT/Upgrade/HEAD/OPTIONS, query-string tokens and X-Forwarded-For spoofing.
- 2000 concurrent Allows with burst 50 admitted exactly 50.
- Known, documented limit: the Peek-then-charge overshoot in the failed-auth throttle repeats per refill under concurrency. Measured 10, 2, 2, 3, 3 verifications across waves of 500.

## Phase 4 — Control plane and hot reload (2026-11-15)

### What was built
- `internal/controlplane` has a `Store` interface with two implementations, `MemStore` and `PostgresStore`. Both run the same conformance suite.
- An append-only `config_versions` table. Each row holds the full JSONB document for one version.
- Embedded SQL migrations, applied idempotently under an advisory lock.
- An admin REST API for config, routes, upstreams, versions and rollback, protected by an admin token.
- An SSE config stream and a status-report endpoint, protected by a separate data-plane token.
- A `Hub` with a capacity-1 slot per subscriber (highest version wins) and a node registry.
- `cmd/controlplane` with flags `-listen`, `-database-url`, `-memory-store`, `-seed`, `-drain-timeout`, `-heartbeat`, `-read-timeout` and `-head-poll`.
- Gateway side: `config.RemoteSource`, a hand-written bounded SSE parser with equal-jitter backoff, and a last-known-good file written atomically. The `Acknowledger` interface reports applied versions.
- `cmd/gateway` gained flags `-control-plane`, `-node-id` and `-lkg-path`.
- End-to-end tests run a real in-process control plane against the real gateway. They cover hot reload under load (zero non-200 responses across 20 route flips), rollback, rejected-config reporting, serving with the control plane down, and restart from the last-known-good file.

### Decisions
- Writers are serialised with `pg_advisory_xact_lock`, and a new version is head + 1. `SELECT ... FOR UPDATE` on the head row was rejected because it loses an update under READ COMMITTED.
- Writes use ETag / If-Match optimistic concurrency.
- Rollback creates a new version instead of rewinding the head.
- The SSE stream sends full snapshots, not an event log.
- A subscriber subscribes to the hub before it reads the head, so a publish in the gap cannot be missed.
- Ack after apply: the gateway reports a version as applied, and writes its last-known-good file, only after the snapshot is built and swapped.
- A rejected version is not re-delivered within the same process.
- The admin token and the data-plane token are two separate tokens and are not interchangeable.
- A head poller runs every 2 s. It reads the newest version's metadata and fetches the document only if that version is newer than the hub's maximum. This lets more than one control-plane replica converge, and it repairs a commit that was never published.

### Bugs and surprises
1. Shutdown flake, found by the builder. `http.Server.Shutdown` waits for connections in StateNew, meaning accepted with no request byte read yet. The Go HTTP client can dial a spare connection speculatively. One such idle connection held the control plane's drain until it timed out, and the process exited 1. Fix: track StateNew connections through `ConnState` and close them in a `RegisterOnShutdown` hook.
2. A suspected bug that was not a bug. The architect, reading the code, predicted that the server's 30 s `ReadTimeout` would cut every SSE stream after 30 s. The reviewer built the binary with a 2 s ReadTimeout on Go 1.26.9 and 1.27.4, and the streams stayed open. net/http clears the read deadline before it starts its background read. The timeout was kept, because it is the only time bound on a slowly trickled request body. The doc was corrected, and a regression test now holds a stream open past the timeout. Lesson: confirm with an experiment before "fixing".
3. (HIGH, review) The request metric used the raw HTTP method as a label. Unknown methods get 404 or 405 before any token check. 500 requests with distinct ~2 KB method tokens created 9,000 series and an 18 MB /metrics page, which is an unauthenticated memory and scrape denial of service. Fix: methods outside a fixed set are recorded as "other".
4. (review) Nothing capped the total config size. Ten ~1 MiB upstream PUTs produced a 9.4 MiB version. The gateway's SSE client has an 8 MiB event limit, so it rejected the version in a reconnect loop, and every gateway silently stayed on the old version. Fix: writes above 4 MiB are rejected, which is half the gateway's limit.
5. (review) The node registry grew without bound for any holder of the shared data-plane token. 20,000 status posts gave about 25 MiB. Fix: the registry is capped at 4,096 nodes with O(1) eviction of the least-recently-seen disconnected node. Connected nodes are never evicted, and a new stream gets 503 when the registry is full of connected nodes.
6. (review) Two replicas started with `-seed` raced, and the loser crashed with a version conflict. Reproduced 5/5 against Postgres. Fix: a conflict on the seed write now means "already seeded".
7. Smaller findings. A JSON body of `null` was accepted by PUT /v1/config and emptied the fleet's config. A NUL character in any string gave 200 on the in-memory store and 500 on Postgres, because JSONB cannot store it. An Applied call that arrived after shutdown lost the last-known-good write. A test that claimed to cover the final write still passed with that write deleted; it was rewritten and now fails 5/5 without it.
8. JWT settings were validated only by algorithm name, so the control plane accepted configs that every gateway would reject. Validation now requires the right key setting for each algorithm.

### Review
- Control plane: 1 high, 3 medium and 6 low findings, all fixed test-first. No flaky tests were seen.
- Reviewed and found sound: store concurrency, token comparison (SHA-256 digests, so no length leak), hub ordering, and the SSE parser.
- Gateway wiring: passed with low-severity notes only.
- Coverage, measured by the orchestrator: internal/config 95.3%, internal/controlplane 89.6% with the Postgres suite (77% when that suite skips), cmd/controlplane about 75%.

## Phase 5 — Service discovery (2026-11-15 to 2026-11-16)

### What was built
- `internal/discovery` Consul watcher. One goroutine per watched service long-polls `/v1/health/service` with `passing=true`. It follows Consul's index rules and de-duplicates by address. It sends a sorted set only when the set changed, and errors never produce an empty set.
- An integration test against a real Consul 1.20 container.
- Gateway wiring: one generation-tagged watch per (service, tag), owned by the reconcile loop. Forwarders select on ctx for every send. An endpoint update rebuilds the snapshot without creating a new config version.
- `cmd/gateway` gained the flag `-consul-addr`.

### Decisions
- The reconcile goroutine is the single writer of snapshots and watch state.
- Watches are reconciled only after a config has been applied. A rejected config therefore cannot cancel a watch that the old snapshot still needs.
- Updates from a stale generation are dropped.
- Discovered backends come from the same Registry as configured ones, so health checks, ejection and in-flight state follow them.

### Bugs and surprises
All of these came from the independent review and were reproduced.
1. (HIGH) Ready but serving 503. The ADR said /readyz waits for the first Consul answer, but the implementation only checked that a snapshot existed. A restarted pod with a last-known-good config therefore reported ready while its discovered upstreams had no backends. This lasted indefinitely if Consul was down. Fix: the first config is not ready until every referenced service has answered or 10 s have passed.
2. Changing only the Consul tag of an upstream created a new watch with no data. A routine config push therefore emptied the upstream (503) until Consul answered. Fix: the previous backend set is carried over by upstream name until the new watch's first answer arrives.
3. Discovered addresses were barely validated. Port 0 and `evil.example/../x?y:80` became backends, and two thirds of traffic failed. Fix: strict host:port validation. The docs now state that Consul-supplied addresses are trusted egress targets.
4. A Consul flap erased passive ejection. An ejected backend that Consul dropped and then re-added came back as a new object and took traffic at once, despite a one-hour cool-off. Fix: backends that leave the live set stay in the Registry for a 60 s grace period.
5. A Consul agent that accepted TCP and never answered stalled a watch forever. Each blocking query now has its own deadline.

### Review
- Verdict: "changes requested" (1 high, 3 medium, 3 low). All findings were fixed test-first.
- Coverage: internal/discovery 87.3%.

## Phase 6 — Observability (2026-11-15 to 2026-11-16)

### What was built
- `internal/metrics` on an injected registry, never the default one. Metrics include `requests_total{route,upstream,code}`, the `request_duration_seconds` histogram, `ratelimited_total{route,reason}`, `auth_failures_total`, `backend_up`, `backend_inflight`, `backend_ejections_total`, `config_version`, `config_apply_failures_total`, `config_stream_connected` and `access_log_dropped_total`, plus a control-plane set.
- Every metrics method is nil-safe.
- `LogAccess` writes one flat JSON line per request with fixed keys.
- Gateway wiring: one deferred recorder in `ServeHTTP` counts and logs every request exactly once, on every exit path. `/metrics` is served only on the admin listener.
- A Grafana dashboard with 19 panels.

### Decisions
- Labels are route names, upstream names and backend IDs. Paths, queries and client IDs are never used as labels.
- The in-flight gauge is sampled every 500 ms instead of being updated per request. This keeps two labelled-gauge lookups off the hot path.
- Backend series are written only for backends in the live set, under the same mutex as the swap. A removed backend's series therefore cannot be recreated by the health checker's reset callback.
- A mid-body abort is recorded with the status that was already sent.

### Bugs and surprises
1. WebSocket upgrades were recorded as status 200 with 0 bytes, because ReverseProxy hijacks the connection and never calls WriteHeader(101). The socket's whole lifetime was also counted as request latency, which pushed the route's p99 into the +Inf bucket. Fix: hijacked requests are counted as 101 and kept out of the histogram.
2. `tollgate_backend_up` stayed 1 while a backend was passively ejected. It now means "in rotation".
3. A blocked log writer stalled every request. With stdout piped to a process that nobody read, 190 requests succeeded and then everything hung. Fix: the access log goes through a bounded async writer (8,192 lines) that drops lines and counts them when full.
4. A test-harness bug looked like a hang. Two new tests read the gateway's exit code from a channel and never put it back. Their own cleanup then waited 10 s and failed exactly when the gateway behaved correctly. The builder proved this with a throwaway copy and did not change the code under test.

### Review
- Verdict: "changes requested" (3 medium, 2 low). All findings were fixed.
- Coverage: internal/metrics 96.9%, internal/proxy 93.3%.
- Measurements, builder and reviewer benchmarks on an Apple M3: metrics cost about 240 ns per request with zero extra allocations. The JSON access log adds about 2–3 µs and 3 allocations per request. A proxied loopback request took about 52.4 µs with observability off and about 55.5 µs with metrics and the access log on.

## Phase 7 — Deploy and CI (2026-11-15)

### What was built
- One multi-stage Dockerfile that produces distroless nonroot images: gateway 12 MB, control plane 15 MB, demo 8.3 MB.
- A demo upstream that registers itself in Consul and supports fault injection.
- A compose stack: Postgres, Consul, the control plane (with a seed), the gateway, three demo services, Prometheus, and Grafana with a provisioned dashboard.
- Kustomize manifests for kind: 2 gateway replicas, probes, a shutdown delay, and a read-only root filesystem.
- A GitHub Actions workflow with lint, vet, race tests (Postgres and Consul service containers), an 80% internal coverage gate, builds and images.
- A Makefile.

### Decisions
- Distroless images have no shell. A small `ready` container (the demo binary in probe mode) therefore gates `docker compose up --wait` on both /readyz endpoints.
- k6 runs via Docker on the compose network.

### Bugs and surprises
1. On kind, the control plane crash-looped twice while Postgres was starting, because it gives up after 10 s. Fix: a wait-for-postgres init container in the manifest.
2. golangci-lint v2.5.0 could not load the module. It was built with Go 1.25, and go.mod says 1.26.9. v2.15.0 works.

### Review
- No independent reviewer report exists for this phase. Verification was done by the infra agent and the orchestrator.
- Compose, verified for real: `docker compose up -d --build --wait` took 43.4 s including the Go builds, and 14.4 s warm (target: under 2 minutes). Requests spread over all three discovered demo instances. /api returned 401 without a key, 200 with a key, and 429 with Retry-After under a burst. A route change through the admin API took effect within 0.5 s, and rollback restored it. Stopping one demo container lost 0 of 20 requests. Prometheus targets were up, and the Grafana dashboard was listed.
- kind, verified for real: kind was installed into a scratch directory and the cluster was deleted afterwards. All pods were ready in about 44 s. A rolling restart of the gateway under 150 requests returned 150 × 200.
- Not verified: the GitHub Actions workflow has only been syntax-checked and has not run on GitHub.
- Open: the linter reports 37 findings (34 in test files) that still need cleaning up.

