# ADR 0006: Active probes plus passive ejection

Status: Accepted (Phase 0)

## Context

Backends crash, hang, get redeployed, or start returning errors while their process looks fine. The gateway has to stop sending traffic to a bad backend quickly, send traffic again once it recovers, and avoid flapping (rapid up/down/up) when a backend is borderline. The chaos test in Phase 8 measures failover time, so the detection speed must be explainable.

## Options considered

1. **Active checks only**: a goroutine per backend sends `GET <path>` every `interval`. It detects dead backends even with zero traffic, and it detects recovery. But detection takes roughly `interval × fall`, and a passing `/healthz` does not prove real requests succeed. Probe load grows with backends × gateways divided by `interval`.
2. **Passive checks only** (outlier detection on real traffic): it reacts within milliseconds when traffic is flowing, and it catches "healthz is OK but real endpoints return 500". But it needs traffic to detect anything, and it cannot tell when a backend has recovered, so it needs a timer.
3. **Rely on the platform** (Kubernetes readiness or Consul checks alone): no extra code. But it is slow (often 10 s or more), it does not exist for static upstreams, and it sees the backend from the platform's network position, not the gateway's.
4. **Both active and passive** (chosen), the same approach as Envoy and NGINX Plus.

## Decision

Both, as two independent signals that write **different fields** on `*balancer.Backend`, so they never need to coordinate:

```
Available(now) = !activeDown && now >= ejectedUntil
```

**Active (`health_check`)**: one goroutine per backend, owned by `health.Checker`. It probes `GET base+path` with `timeout`, and a 2xx response counts as success. Up and down use **hysteresis**: a backend goes down after `unhealthy_threshold` consecutive failures (fall, default 3) and comes back up after `healthy_threshold` consecutive successes (rise, default 2). The first probe of each backend starts at a random offset within `interval`, so probes do not arrive in sync. New backends start **up** (the zero value of `activeDown` is false), because Consul has already vetted discovered instances, and starting them down would cause 503s for one interval after every reload. When `health_check` is omitted, active checking is off and `activeDown` stays false.

**Passive (`passive_health`)**: after each attempt, the proxy reports the outcome. A transport error (dial refused, reset, timeout) or any 5xx counts as a failure, and anything else is a success. A success resets the backend's consecutive-failure counter. When the counter reaches `consecutive_failures` (default 5), the backend is ejected until `now + cooloff` (default 30 s). Re-admission is **lazy**: `Available` compares against the deadline, so no timer goroutine is needed. The counter is reset **only by a success**, not when the cooloff ends. So a backend that comes back and fails again is re-ejected on its first failure: a natural half-open probe with no extra state.

**How they combine:** active is the authority on "is the process alive", and passive is a fast, short-lived circuit breaker on real traffic. Active recovery does not clear a passive ejection. The cooloff still runs, which is conservative and keeps the two signals independent. All of this state lives on the shared `*Backend`, so it survives config swaps (ADR 0002). After each swap, `Checker.Sync(targets)` starts probes for new backends and stops probes for removed ones, and restarts a probe if its settings changed. State transitions are reported through a callback, which feeds metrics and logs without the health package importing metrics.

**If every backend is unavailable** we return 503 (`ErrNoHealthyBackends`). We do not implement Envoy's "panic mode" (sending to all backends when too many are unhealthy) in v1.

## Consequences

- Good: under traffic, failover takes about as long as `consecutive_failures` requests, i.e. milliseconds. Without traffic it takes about `interval × fall` (15 s at the defaults). Both numbers are easy to state and to measure in Phase 8.
- Good: lock-free. Each signal does its own atomic writes and the balancer reads them.
- Cost: counting every 5xx as a failure means a single endpoint that keeps returning 500 can eject an otherwise healthy backend. The alternative (count only 502/503/504 and transport errors, like Envoy's "gateway failure" mode) is a one-line change if this causes trouble.
- Cost: without panic mode, a bad health-check config, such as the wrong path, can mark everything down and cause a full outage. Validation and the upstream-health metric are the guards. Panic mode, or a cap on the percentage of backends that can be ejected, is future work.
- Cost: probe traffic grows with backends × gateway replicas.
