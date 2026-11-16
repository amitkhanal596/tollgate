# Tollgate benchmarks and chaos tests (Phase 8)

The goal is numbers you can defend: each one says what it measures, how noisy it is, and
whether the test rig (not the gateway) limited it. Read "How to read the numbers" before
quoting anything.

Everything runs against the Compose stack with `deploy/compose/docker-compose.bench.yml`
layered on top (access log off, restart policy off for the demo backends, fault injection on,
benchmark routes in `deploy/compose/bench.yaml`, Prometheus/Grafana not started). The bench
stack is its own Compose project, **`tollgate-bench`**, and publishes no ports, so it can
run next to the demo stack and `down -v` here never touches the demo's volumes.

## Run

```
make bench       # overhead + noise floor, max RPS x3, limiter     ~26 min
make chaos       # hot reload, failover (6 cases x 3 runs)         ~18 min
make bench-all   # both, each on a fresh stack                     ~45 min
SMOKE=1 make bench-all   # ~5 min, proves the scripts work; results go to bench/results/smoke/
```

Expected wall time of a real `make bench-all`: **about 45 minutes** (plus image build on a cold
cache). Exact command to start a real run: `make bench-all` (same as `bench/run-all.sh all`).

Every part starts from a clean `down -v` of the bench project, builds the images, starts the
stack (`up --wait`), and always tears it down (also on failure or Ctrl-C). On macOS the run is
wrapped in `caffeinate -dimsu`. Results:

- `bench/results/raw/<UTC timestamp>/`: raw files (gitignored): one JSON per measurement, per-request
  logs, `docker stats` samples, the stall watchdog log, TCP counters, `machine.json`, `run.json`.
- `bench/results/latest.json`: the combined results with provenance and validity flags.
- `bench/results/RESULTS.md`: **generated** from `latest.json` by `summarize.py report`. Never edited by hand.

`make bench-all` rebuilds both files from that run only; `make bench` / `make chaos` overlay their own
section. `complete` is true only if both sections are present, non-smoke, valid (no Mac sleep/wake
during the run), made by the same commit and the same gateway/controlplane/demo image IDs, and had no
failed measurement. The reasons it is not are listed in `complete_blockers`.

### Smoke runs

`SMOKE=1`, or setting **any** knob to a non-default value, makes the run a smoke run. It uses short
durations, one repetition, one failover config, writes to `bench/results/smoke/` (so it can never
overwrite a real run), and is flagged `"smoke": true` in `latest.json` and with a banner in `RESULTS.md`.
Do not quote smoke numbers.

### Individual pieces

Bring the stack up with `make bench-up` (stop with `make bench-down`), then:
`bench/run-k6.sh overhead|limiter`, `bench/maxrps.sh`, `bench/chaos/failover.sh`,
`bench/chaos/hot-reload.sh`. Knobs are documented at the top of each script and in `run-all.sh`.
Pass `AA=0` to skip the noise-floor rounds, `REPS=1`, `ROUNDS=2`, `REPEATS=1` etc. to shorten.

k6 is not installed on the host; it runs as `grafana/k6:2.4.0` (pinned in `bench/lib.sh`, verified with
`docker manifest inspect`) attached to the compose network, so it reaches `gateway:8080` and
`demo-1:8080` by name and Docker Desktop's userspace port forwarding is out of the numbers. Control
plane and fault-injection calls are made with `docker exec` into the Consul container, which is on the
same network.

## What each measurement is

| Script | Method | Reported |
|---|---|---|
| `k6/overhead.js` | Same constant-arrival-rate load (1000 rps, 8 s per phase, 5 s unmeasured warm-up) sent directly to `demo-1` and through the gateway (`/bare`: no auth, no limit, one backend: the same `demo-1`). 5 rounds, each ABBA or BAAB alternately. Every A/B round is followed by an A/A round (direct vs direct) that measures noise. Run with the access log off; a second A/B pass with it on. | Per round: gateway minus direct for p50, p90, p99 (each side = mean of its two phases). Median, min, max over rounds; the A/A noise floor beside them; "below noise floor" when the median delta is not larger than the largest A/A delta. No max latency. |
| `maxrps.sh` + `k6/maxrps.js` | One `ramping-arrival-rate` scenario per run climbs 2k, 4k, ..., 16k rps (8 s hold, 1 s ramp), so the same VUs and connections serve every step. Targets: `direct` (the backend's own ceiling), `gateway` (to one backend), `gateway3` (round robin over three). 3 repetitions; targets interleaved in an order randomised per repetition (seed recorded). Alongside: `docker stats` CPU per container, gateway upstream dials (ActiveOpens delta) and TIME_WAIT peak from the gateway's network namespace, the stall watchdog, k6's own warnings. | Per step: status of each repetition, median achieved rate, median and worst p99, CPU, dials/s, TIME_WAIT. Max = highest sustained step below the first gateway failure. |
| `k6/limiter.js` + `analyze_limiter.py` | `/limited` (100 rps, burst 100, per key). Key 00 at 2x the limit for 15 s; then 4 keys at 2x in parallel plus one polite key at 50 rps. Every request is logged. Expected allowed = burst + rate x window. | Per-key error min/median/max. A per-key generator check (largest gap between that key's requests must stay under burst/rate = 1 s), because the expected value only holds if the load never paused longer than that; failing keys are excluded. Polite key: only `rejected == 0` (isolation). |
| `chaos/failover.sh` | 200 rps on `/fo-passive` (passive health only), `/fo-active` (active + passive), `/fo-consul` (Consul + both). At T0 `docker kill` (SIGKILL) demo-1, or `POST /admin/fail?mode=500` (alive but failing). Restored after a random 13-27 s fault. Each of the 6 cases runs 3 times. Every request is logged with start time, status, instance and latency. T0 and the restart time come from the Docker VM clock, the same clock as the request log. | Failover time (T0 to the client-visible end, start + latency, of the first failure cluster; `n/a` when none starts within 2 s), re-probe failures, time back in rotation. All as min / median / max over runs. |
| `chaos/hot-reload.sh` + `k6/hot-reload.js` | 300 rps through `/flip` plus 100 rps through `/hrslow` to a demo-3 that takes 2 s per response (`/admin/fail?mode=slow`), so about 200 requests are in flight at every swap. 100 changes, one every 0.5 s across the whole load window: odd ones flip `/flip` between demo-1 and demo-2, even ones flip `/hrslow` between two upstreams, every 10th is a rollback. After each `/flip` change it polls until the new instance answers. | Requests, non-2xx, connection errors, k6 dropped iterations (all must be zero), **requests whose [start, end] contains a change** (how many requests the claim really covers), propagation as lower bound (clock starts when the admin call returned) and upper bound (starts when it was sent), admin call time, gateway restarts, applied vs head version. |

## How to read the numbers

- **Single laptop, shared VM.** k6, gateway, control plane, Postgres, Consul and the backends all run
  inside one Docker Desktop Linux VM on the same Mac. Absolute throughput and tail latency are
  indicative for this rig, not for dedicated hardware.
- **CPU pinning.** The VM's CPU count is read from `docker info` (not assumed) and split into three
  disjoint cpusets: k6 | gateway | everything else (demo backends, control plane, Postgres, Consul).
  For an 8-vCPU VM: k6 `0-2`, gateway `3-5`, rest `6-7`, with `GOMAXPROCS` on the demos set to the rest
  count. The pinning is recorded in `RESULTS.md`. It stops the generator and the backends from
  stealing the gateway's CPU, but it also means the gateway has a fixed 3 CPUs: a step where the gateway
  CPU set is saturated is reported as gateway-bound. Below 4 vCPUs nothing is pinned.
- **Noise floor.** The A/A (direct vs direct) rounds show how far a "delta" moves with no gateway
  involved. A gateway delta at or below it is labelled **below noise floor** and is not a measured
  effect. p99 especially: it is only quoted next to its noise floor.
- **`rig-limited` / `rig-perturbed` steps.** A max-RPS step that failed is *not* counted against the
  gateway if, during that step: the containers together used over 75% of the VM's vCPUs; the k6 or backend
  CPU set was over 90% busy; k6 logged "Insufficient VUs"; or (`rig-perturbed`) the stall watchdog saw a
  gap over 100 ms (the whole VM froze). Steps that passed stay passed. `docker stats` CPU is sampled
  every 1-2 s, so a step's CPU is an average of a few samples.
- **Ceiling not found.** If the highest step passes, the result is `>= N rps (ceiling not found)`, not a
  maximum. A failing step below a passing one is flagged as non-monotonic and the max is capped below it.
- **Failover.** The first failure appears within tens of ms (requests already routed to the dead
  instance). Passive ejection happens after 3 consecutive failures. Afterwards, once the 10 s cool-off
  expires, the gateway sends one real client request to the still-broken backend: that request fails,
  **a user-visible failure about once per cool-off** for as long as the backend stays broken (and
  indefinitely in passive-only mode). "Back in rotation" for the passive cases (and all `fail500` cases)
  is **bounded by the 10 s cool-off** and its value depends on where in the cool-off the restore lands,
  which is why the fault length is randomised; do not quote a single run. With SIGKILL the instance does
  not deregister from Consul, so Consul finds out with its own health check. The demo's graceful
  shutdown now waits `-deregister-delay` (2 s) after deregistering and re-registers with Consul every
  15 s if Consul forgot it; neither changes these tests, which kill with SIGKILL and restart the container.
  T0 for `kill` is Docker's `State.FinishedAt`, which is stamped a few ms after the process dies.
- **Hot reload.** "0 dropped" is only as strong as the number of requests that spanned a swap: read
  that count, not just the request total. Propagation has a lower bound (from the admin call returning)
  and an upper bound (from the call being sent); the true value is between them. Clock resolution
  1 ms, polling about every 2 ms.
- **Provenance.** Commit, whether tracked files were modified (untracked files such as results do not
  count), and the gateway/controlplane/demo image IDs are recorded right after each bring-up, per
  section. A mixed run (two commits or two image IDs) is not `complete`.
- **Isolation from the demo stack.** The bench project is `tollgate-bench` (network
  `tollgate-bench_default`, volumes `tollgate-bench_pgdata` and `_lkg`). The bench scripts only ever
  `down -v` that project. The two stacks share the image tags `tollgate/*:dev`, so a bench build
  retags them; the demo stack's running containers are unaffected until recreated.

## How to get a valid run

1. Plug in AC power (the run records the power source; battery can throttle the CPU).
2. Keep the lid open and the Mac awake. `run-all.sh` runs under `caffeinate -dimsu`, but a closed lid on
   battery still sleeps. Any Sleep/Wake/DarkWake event in `pmset -g log` inside the run window marks the
   run **invalid** (`complete` is false).
3. Close memory-heavy apps first. The run prints a loud warning if host swap is over 50% used or free
   memory is low (it does not abort). Heavy host swapping froze the whole Docker VM for 0.1-9 s in the
   first run; the stall watchdog (a 1 ms ticker container for the whole run) detects that and labels the
   affected steps `rig-perturbed`.
4. Do not use the machine, and do not run the demo stack's load, while it runs. Docker should be otherwise idle.
5. Check `RESULTS.md`: `complete: yes`, section `VALID`, no host warnings, commit not dirty.

## Files

`run-all.sh` (entry point), `lib.sh` (helpers, pinning, provenance), `stack.sh` (manual up/down),
`maxrps.sh`, `run-k6.sh`, `chaos/*`, `k6/*.js`, `summarize.py` (labelling, merge, report), `rigcheck.py`
(host swap/memory/power, pmset sleep events), `sample_stats.py` (`docker stats` sampler),
`analyze_limiter.py`, `chaos/analyze_failover.py`, `chaos/analyze_hotreload.py`, `stall/` (Go helper
image: stall watchdog and TCP counters; stdlib only, built from `bench/stall/Dockerfile`).
