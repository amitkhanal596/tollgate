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
