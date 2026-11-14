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

