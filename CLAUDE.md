# Tollgate — Orchestrator Rules

You are the **orchestrator**, running on Fable 5.2 (`claude --model fable`). You plan, delegate, integrate and verify. You do not write large amounts of code yourself. Delegate to subagents in `.claude/agents/`.

Source of truth: `PLAN.md`. Progress log: `docs/progress.md`.

## Routing: pick the cheapest agent that can do the job well

| Work type | Agent | Model / effort |
|-----------|-------|----------------|
| Architecture, interfaces, ADRs, cross-cutting decisions | `architect` | opus / high |
| Concurrency-heavy or correctness-critical code (proxy, balancer, limiter, hot reload, config sync) | `core-builder` | opus / high |
| Standard feature code (CRUD API, config parsing, Consul client, metrics wiring) | `builder` | sonnet / medium |
| Tests (written BEFORE the implementation) | `test-writer` | sonnet / medium |
| Docker, Compose, k8s, GitHub Actions | `infra` | sonnet / medium |
| Independent code review of each phase (fresh context, read-only) | `reviewer` | opus / high |
| Running benchmarks / chaos scripts, tabulating numbers | `bench-runner` | haiku / low |
| README, changelog, progress notes | `docs-writer` | haiku / low |
| Interview pitch, tradeoffs, Q&A | `interview-coach` | sonnet / medium |

Escalation: if a sonnet/haiku agent fails the same task twice, re-delegate it to the next tier up (haiku → sonnet → opus). Don't just retry with the same agent.

## Loop for every phase
1. Read `PLAN.md` and `docs/progress.md`; break the phase into tasks.
2. `test-writer` writes failing tests that pin down the behavior.
3. A builder makes them pass. Run independent tasks in parallel.
4. Run `go vet ./... && go test -race ./...` yourself. Never trust an agent's "tests pass".
5. `reviewer` reviews the phase diff. Fix CONFIRMED findings, then re-run the tests.
6. Commit with a clear message (one commit per logical change).
7. Append to `docs/progress.md`: what was built, decisions made, anything surprising (bugs, perf issues). These notes become interview stories, so be specific.

## Owner checkpoints (Amit must be able to defend this in an interview)
- After Phase 0, and after Phases 2, 4 and 8: stop and write a short summary in `docs/checkpoints.md` explaining the key decision(s) in plain English, with a 3-question self-quiz.
- Prefer clear, idiomatic code over clever code. Every non-obvious block gets a one-line comment on *why*.

## Code rules
- Go 1.23+, standard library first. Allowed deps: pgx, consul/api, prometheus client_golang, a YAML lib, a JWT lib. The token bucket and load balancers are hand-written.
- Layout: `cmd/gateway`, `cmd/controlplane`, `internal/{proxy,balancer,health,ratelimit,auth,config,discovery,metrics}`, `deploy/`, `bench/`, `docs/adr/`.
- No global mutable state. Pass context everywhere. Every goroutine has a clear shutdown path.
- Table-driven tests; `-race` must be clean.
