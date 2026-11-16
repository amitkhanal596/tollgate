# Tollgate benchmarks and chaos tests (Phase 8)

Everything runs against the Compose stack with `deploy/compose/docker-compose.bench.yml`
layered on top (access log off, restart policy off for the demo backends, benchmark
routes in `deploy/compose/bench.yaml`, Prometheus/Grafana not started). Nothing extra is
published to the host.

## Run

```
make bench   # overhead, max RPS, limiter accuracy   ~15 min
make chaos   # hot reload + failover (6 cases)       ~9 min
make bench-all   # both, each on a fresh stack       ~25 min
```

Each target builds the images, starts the stack (`up --wait`), runs the scripts and always
runs `down -v` (also on failure or Ctrl-C). Raw output goes to
`bench/results/raw/<UTC timestamp>/` (gitignored: one JSON per measurement, the per-request
failover logs, `machine.json` with CPU, RAM, Docker Desktop VM CPU/memory limits, Go version,
git commit). `bench/results/latest.json` combines the key numbers. `make bench-all` rebuilds it from that
run only; `make bench` / `make chaos` overlay their own section. The file records the run's UTC
timestamp, git commit, the effective knobs, `"smoke": true` if any knob differs from its default,
and `"complete"` (every measurement taken, both halves present). Per-section metadata is in
`sections`, so a stale section is recognisable by its timestamp and commit. The scripts exit non-zero if a measurement
could not be taken.

Smoke run (about a minute per script):

```
RATE=200 DURATION=5s WARMUP=2s STEPS=1000,4000 STEP_DURATION=5s GAP=1 SETTLE=2 make bench
RATE=200 PRE=3 FAULT=14 REC=12 CHANGES=10 SETTLE=2 make chaos
```

The hot-reload pause between changes is `RELOAD_GAP` (default 0.25 s); `GAP` is the max-RPS
gap between steps.

Individual scripts (stack must be up: `make bench-up`; stop it with `make bench-down`):
`bench/run-k6.sh overhead|maxrps [direct|gateway|gateway3]|limiter`,
`bench/chaos/failover.sh`, `bench/chaos/hot-reload.sh`. Knobs: see the header of each script.

k6 is not installed on the host. It runs as the `grafana/k6` image attached to the compose
network, so it reaches `gateway:8080` and `demo-1:8080` by name. This keeps Docker Desktop's
host port forwarding (a userspace proxy that would add latency and cap throughput) out of
the numbers.

## What each script measures

| Script | Method | Reported |
|---|---|---|
| `k6/overhead.js` | Same constant-arrival-rate load (default 1000 rps, 30 s per phase, 5 s unmeasured warm-up of both paths) sent directly to `demo-1` and through the gateway (`/bare`, no auth, no limit, one backend: the same `demo-1`). Phases run in the order direct, gateway, gateway, direct so drift does not favour a side. Run once with the access log off and once on. | p50/p90/p95/p99/max for each, and gateway minus direct. |
| `k6/maxrps.js` | One constant-arrival-rate step per rate (1k, 2k, 4k, 8k, 12k rps, 20 s each, after a warm-up) so dropped iterations are attributed to a step. Targets: `direct` (the backend's own ceiling), `gateway` (to one backend) and `gateway3` (round robin over three). | Per step: achieved rps, error rate, dropped iterations, p50/p99. Max RPS = highest step with error rate < 0.1%, p99 < 50 ms and under 2% dropped iterations (its measured rate is quoted). |
| `k6/limiter.js` | `/limited` (100 rps, burst 100, per key). Key 00 at 5x the limit for 30 s; then 9 keys at 5x in parallel plus one polite key at 50 rps. Expected allowed = burst + rate x window (window = first to last request). | Allowed vs expected and the percentage error, per-key spread, rejects that are not 429 with Retry-After, polite key rejects (isolation). |
| `chaos/failover.sh` | 200 rps on `/fo-passive` (passive health only), `/fo-active` (active + passive), `/fo-consul` (Consul discovery + both). At T0 `docker kill` (SIGKILL) demo-1, or `POST /admin/fail?mode=500` (alive but failing); 20 s later restore it. Every request is logged with its start time, status and serving instance. T0 and the restart time come from the Docker VM clock (`State.FinishedAt`/`StartedAt`), the same clock as the request log. | Failed requests, first and last failure after T0, failure clusters, failover time (T0 to the end of the first cluster), failures from later re-probes, and time from restore until demo-1 serves again. |
| `chaos/hot-reload.sh` + `k6/hot-reload.js` | 500 rps through `/flip` while one VU makes 100 changes via the control plane: route flips between upstreams (PUT) and every 10th change a `POST /v1/config/rollback`. After each change returns it polls until the response comes from the expected instance. | Requests, non-2xx, connection errors (claim: zero), propagation p50/p90/p99/max (admin call returned to first response with the new config; includes about 1 request round trip), gateway restarts, applied vs head version. |

## Caveats (read before quoting numbers)

- One machine: the load generator, gateway, control plane, Postgres, Consul and the
  backends share the same CPUs inside the Docker Desktop Linux VM (see `machine.json` for its
  CPU and memory limits, which are not the host's). Absolute throughput and tail latency are
  therefore conservative and indicative, not what dedicated hardware would give.
- Docker Desktop on macOS: virtualisation and the VM's networking add jitter; the network is
  a loopback-like virtual bridge, so there is no real network latency.
- The meaningful results are the **overhead delta** (direct vs gateway under identical
  conditions), the **limiter accuracy**, the **zero-drop** claim and the failover times
  (which are timer-driven and not CPU-bound).
- Max RPS is bounded by the weakest component. Check the `direct` run: if the demo backend
  or k6 saturates before the gateway, the gateway number is a lower bound. The demo backend
  is a trivial Go server, so its ceiling is high but not unlimited.
- Steps jump to their rate instantly (constant-arrival-rate), which is harsher than a ramp.
  The first, unmeasured warm-up exists because a cold gateway to backend connection pool
  gave a p99 spike of about 490 ms in a first step during development.
- Failover: the instance is killed with SIGKILL, so it does not deregister from Consul;
  Consul marks it critical after its own 2 s check. Passive ejection (3 consecutive
  failures) is much faster than either, so failures are limited to the requests in flight
  or routed in the first few milliseconds; later failures are the periodic re-probe after
  the 10 s cool-off. `docker kill` on the container id is used instead of
  `docker compose kill` only to avoid compose's start-up time between T0 and the signal.
- Hot reload: propagation is measured by polling every ~2 ms, so its resolution is a few ms.
