# Manual to-do: what was not done, and what needs you

This is the list the build could not finish on its own, or should not decide for you. Each item says what to do. Items are ordered by how much they matter.

## 1. Things only you can do

| # | Item | What to do |
|---|---|---|
| 1 | **`internal/controlplane` is outside the layout in `CLAUDE.md`.** It was added so the admin API, store and SSE hub are testable and count toward the 80% coverage target. | Accept it, or update the layout line in `CLAUDE.md` so the rule and the repo agree. |
| 2 | **The repo has never been pushed.** There is no remote. All commits are on `main` (the repo started empty in this session, so there was no branch to protect). | Create the GitHub repo and push when you are ready. Nothing was sent to any outside service. |
| 3 | **CI has never run on GitHub.** `.github/workflows/ci.yml` was checked by reading it and by running the same commands locally (`make test`, `make cover`, `make lint`, `make bench-test`, image builds). The Postgres and Consul service containers in the `test` job have not been exercised by a real runner. | After pushing, watch the first run. Expect small fixes (action versions, service health timing). |
| 4 | **Leftover benchmark files were not deleted.** `bench/results/raw/` (git-ignored, so it is never pushed) holds four superseded runs (`20261116T012935Z`, `013309Z`, `053626Z`, `054500Z`), the final run `20261116T162040Z`, and one empty directory `manual-20261116T114813Z`. Smoke output is in `bench/results/smoke/` (also git-ignored). A cleanup command from an agent was blocked by the permission system, and I did not run it on the agent's behalf. | Keep `bench/results/raw/20261116T162040Z` (the raw data behind `RESULTS.md`; only the six kill-case logs are committed, under `bench/results/evidence/`). Delete the rest if you want: `ls bench/results/raw` first, then remove the directories you do not need. |

## 2. Benchmarks: re-run them on a quiet machine

The numbers in the README are real and come from one complete run, but the rig was not in good shape:

- The laptop was **on battery** and had **87% of its swap in use** (172 MB free). Both are recorded as warnings in `bench/results/RESULTS.md`.
- The gateway-to-one-backend throughput result is **non-monotonic** (12000 rps passed 3 of 3, 14000 failed 1 of 3, 16000 passed 3 of 3). The failing repetition had a 93 ms VM stall, just under the 100 ms threshold that would have labelled it `rig-perturbed`. The threshold was not changed after the fact.
- The p90 and p99 overhead are below the noise floor, so only p50 is quoted.
- The throughput ceiling was **not found** for two of three targets (the top step, 16000 rps, passed).

To do:

1. Plug in AC power, close memory-heavy apps (or reboot), keep the lid open, then run `make bench-all` (about 45 minutes; the recorded run took 39). Do not use the machine while it runs, and do not commit during the run (the provenance check compares the commit at every stage).
2. Check `bench/results/RESULTS.md` says `complete: yes` with no host warnings, then update the README table from it. `make bench-test` checks the analysis code.
3. For a real capacity number, run the load generator on a second machine and raise the top step in `bench/k6/maxrps.js`. On one laptop the generator and the gateway share the CPU.
4. The first run's results (commit `d693166`) were wrong in several ways and were replaced. The story is in [progress.md](progress.md) Phase 8; do not quote those numbers.

## 3. Not built (known limits)

These are deliberate omissions for v1, listed in `docs/architecture.md` §10.1. The ones most likely to matter:

| Limit | Why it matters | Smallest next step |
|---|---|---|
| A black-holed backend fails about one dial timeout of requests (69 requests and 2 s in the Consul kill test) | Passive ejection counts completed failures; a dial to a dead IP takes the full timeout | Lower `timeouts.dial`; retry on another backend when the request never reached one; or eject on the first dial timeout |
| No retries on another backend | A dial failure is a 502 even when other backends are healthy | Retry idempotent requests once, with a retry budget |
| No TLS between gateway and control plane, and no TLS termination | Tokens and config travel in clear text | mTLS between planes; a TLS listener on the gateway |
| Rate limits are per gateway instance | With N replicas a client gets up to N times the limit | Shared counter (Redis) or limits divided by replica count |
| `X-Forwarded-For` is not trusted | Clients behind one NAT or load balancer share a bucket | A `trusted_proxies` setting |
| No per-backend concurrency ceiling | A slow backend is only protected by timeouts and least-connections | Max in-flight per backend in admission, answer 503 (ADR 0011) |
| Idle upstream connections are bounded per backend, not globally | Many discovered backends can press on file-descriptor limits | A global cap with LRU eviction |
| The admin listener has no auth; `-pprof` adds profiling to it | Anyone who can reach it can read metrics, and with `-pprof` run CPU profiles | Keep it cluster-internal; bind pprof to loopback |
| No "panic mode" | If every backend is marked down the route returns 503 | Fail open when all backends are down |
| HTTP/1.1 upstreams only | No HTTP/2 or gRPC to backends | Configure the transport for h2 |

Smaller things:

- With `-pprof`, a CPU profile still running at shutdown will probably outlast the admin listener's 2 s drain, and the gateway would then exit with code 1. Read from the code, not tested.
- Docker images are pinned by tag, not by digest.
- `TestFileSourceUnstableContentNotApplied` still depends on the wall clock. It flaked once under load, and now has ten times the margin and skips itself if the machine stalls for a whole poll interval. A proper fix is an injectable tick channel in `FileSource`.
- The Consul dev agent in the demo has no ACLs, and Grafana uses a demo-only admin password. Both are labelled in the Compose file.

## 4. Verified locally, not elsewhere

- **Postgres and Consul integration tests** run only when `TOLLGATE_TEST_DATABASE_URL` / `TOLLGATE_TEST_CONSUL_ADDR` are set. They were run against local Docker containers during the build. The final coverage figure (90.8% of `internal/`) was measured without them, so it is a lower bound.
- **Kubernetes manifests** were applied once to a throwaway `kind` cluster (deleted afterwards) with a `kind` binary kept outside the repo. `kind`, `k6` and `golangci-lint` are not installed on this Mac; `k6` and `golangci-lint` run from pinned Docker images.
- **`go.mod` says `go 1.26.9`** (raised by dependencies); the local toolchain is newer. PLAN.md asks for Go 1.23+, which holds, but a machine with exactly Go 1.23 cannot build it.
- **The README quick-start commands** were run against the demo stack and behave as documented (`make up` took 19 s warm on the final commit).

## 5. If you find a bug

`docs/progress.md` records every bug the reviews found and how it was fixed. Add yours in the same format: what you saw, why it happened, the fix, the test that now pins it.
