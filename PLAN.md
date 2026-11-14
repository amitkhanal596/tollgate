# Tollgate — Build Plan

A traffic gateway written in Go, with a separate **control plane** and **data plane**: reverse proxying, load balancing, health checks, rate limiting, auth, Consul service discovery and observability. It runs on Docker and Kubernetes and ships through CI.

It's scoped to match Tesla's Traffic Infrastructure internship, which covers DNS, CDN, API Gateway, load balancing, service mesh/Consul, and control and data plane automation.

## Architecture

```
                 ┌──────────────────────── CONTROL PLANE ────────────────────────┐
  admin (curl) → │ Admin REST API (Go) ──► Postgres (versioned route configs)    │
                 │        │                                                      │
                 │        └── config stream (SSE / long-poll, version N) ──┐     │
                 └─────────────────────────────────────────────────────────┼─────┘
                                                                           ▼
  client ──► ┌──────────────────────── DATA PLANE (Tollgate) ────────────────────┐
             │ auth (API key / JWT) → rate limit (token bucket) → router        │
             │   → load balancer (RR / least-conn) → reverse proxy → upstream   │
             │ health checker (active)   Consul watcher (service discovery)     │
             │ /metrics (Prometheus)     structured logs                        │
             └──────────────────────────────────────────────────────────────────┘
                   ▲ Prometheus + Grafana         ▲ Consul agent
```

**Key design idea to talk about:** the data plane holds the active config behind an `atomic.Pointer`. Requests read it lock-free, and when the control plane publishes version N+1, the data plane builds the new config off to the side and swaps it in atomically. That gives hot reload with no restarts and no dropped requests.

## Tech
Go 1.23+, `net/http` + `httputil.ReverseProxy`, Postgres (pgx), Consul (`hashicorp/consul/api`), Prometheus `client_golang`, Grafana, Docker Compose, kind (local Kubernetes), GitHub Actions, k6 for load tests.

## Phases

Each phase ends with passing tests, a review from the `reviewer` agent, a commit, and an update to `docs/progress.md`.

| # | Phase | Deliverable | Owner agents |
|---|-------|-------------|--------------|
| 0 | Design | `docs/adr/` (one doc per major decision), Go module layout, interfaces (`Balancer`, `Limiter`, `ConfigSource`) | architect |
| 1 | Data plane core | Reverse proxy, path/host routing from a YAML file, timeouts, graceful shutdown | test-writer → core-builder → reviewer |
| 2 | Load balancing + health | Round-robin and least-connections; active health checks remove/re-add backends; passive ejection on 5xx | test-writer → core-builder → reviewer |
| 3 | Rate limit + auth | Token bucket per API key (written from scratch, not a library), 429 + `Retry-After`; API-key and JWT middleware | test-writer → core-builder → reviewer |
| 4 | Control plane | Admin REST API (CRUD routes/upstreams), Postgres with versioned configs, config stream to the data plane, atomic hot reload, rollback to version N-1 | architect → builder + core-builder → reviewer |
| 5 | Service discovery | Consul watcher: upstreams come from Consul's healthy services instead of a static list | builder → reviewer |
| 6 | Observability | Prometheus metrics (requests, latency histogram, 429s, upstream health, config version), JSON logs with request IDs, Grafana dashboard JSON | builder |
| 7 | Deploy + CI | Multi-stage Dockerfile, `docker compose up` brings up the full stack (gateway, control plane, Postgres, Consul, 3 demo services, Prometheus, Grafana), kind manifests, GitHub Actions (lint, `go test -race`, build, image) | infra |
| 8 | Prove it | k6 benchmarks (gateway overhead p50/p99, max RPS, limiter accuracy), a chaos script (kill a backend → measure failover time; hot reload under load → count dropped requests), with results in the README | bench-runner → orchestrator |
| 9 | Interview pack | README with diagram and numbers, `docs/interview.md`: 60-second pitch, design tradeoffs, the hardest bug, "what I'd do next", and 20 likely questions with answers | docs-writer, interview-coach |

## Definition of done
- `go test -race ./...` is green, with coverage ≥ 80% on `internal/`
- `docker compose up` gives a working demo in under 2 minutes
- The README has real measured numbers, not estimates
- Every major decision has an ADR (round-robin vs least-conn, push vs poll, token bucket vs sliding window, atomic swap vs RWMutex)

## Stretch (pick 1–2 if time allows)
- Circuit breaker per upstream
- Response caching with Cache-Control + purge API (the CDN angle)
- Tiny authoritative DNS server for `*.tollgate.local` driven by the control plane (the DNS angle)
- Canary / weighted routing (e.g. 90/10 split)
