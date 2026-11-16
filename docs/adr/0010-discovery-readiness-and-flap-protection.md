# ADR 0010: Discovery readiness gate, carry-over and a backend grace period

Status: Accepted (Phase 5 review fix round). Amends ADR 0007 (Startup, merge rule) and ADR 0002 (Registry row).

## Context

The Phase 5 review reproduced three ways that Consul-backed upstreams lost traffic or protection, although every component did what its ADR said:

1. **Ready, but serving 503 after a restart (HIGH).** `/readyz` meant "a snapshot exists". A restarted pod that loaded its last-known-good config built a snapshot at once, but its `consul:` upstreams had no backends until each watch answered. The load balancer sent traffic to a pod that answered 503. If Consul was down, this lasted indefinitely.
2. **A tag change emptied an upstream.** Changing only `consul.tag` (or `consul.service`, or switching an upstream from static `backends:` to `consul:`) starts a new watch with no data. The rebuild for that config gave the upstream an empty backend set, so a routine config push caused 503s until Consul answered.
3. **A Consul flap erased a passive ejection.** A backend was ejected with a one-hour cooloff. Consul briefly dropped it (its own check failed) and re-added it. The reconcile loop had pruned it from the Registry at the drop, so the re-add got a new `*Backend` with zero state, and it took traffic at once. In-flight counts (least-connections) were lost the same way.

## Options considered

### A. When is a gateway ready while discovery has not answered?

1. **Ready when a snapshot exists** (the old behaviour). Simple, but it is finding 1.
2. **Wait for every referenced service's first answer, with no bound.** Never ready-but-empty. But a Consul agent that is down during a rolling restart keeps every new pod out of rotation forever, including its static routes, and the rollout stalls the whole fleet.
3. **Wait, bounded by a timeout** (chosen). Ready once every service of the first config has answered, or after 10 s. A pod whose Consul is down still serves its static routes after 10 s, and logs which services never answered.

A second question is **which configs gate readiness.** Re-gating on every config would take every serving gateway out of its load balancer at once whenever a push adds a service: a fleet-wide outage of every route. So only the **first** config applied in the process gates readiness; once ready, a gateway never becomes unready because of a config.

### B. A query that has not answered yet

1. **Build with an empty set** (the old behaviour): finding 2.
2. **Hold the whole config back until the new watch answers.** No empty window, but unrelated changes in the same config wait too, the reconcile loop needs a "pending config" state and its acknowledgement gets delayed, and a Consul outage would block every config change.
3. **Carry over the upstream's previous backend set, by upstream name, until the new query's first answer** (chosen). The config applies at once; only the one upstream keeps serving what it served a moment ago. The first answer, even an empty one, replaces it.

### C. A backend that leaves the live set

1. **Prune it from the Registry immediately** (the old behaviour). A re-add gets fresh state: finding 3.
2. **Keep it in the Registry for a grace period** (chosen, 60 s). A flap shorter than the grace gives back the same `*Backend`, with its ejection deadline, failure streak and in-flight count.
3. **Never prune.** No flap can erase state, but memory grows with every address ever seen (autoscaling with fresh IPs), and an address reused months later would inherit a stale ejection.
4. **A separate tombstone map of ejection deadlines by ID.** Same effect as C2, but a second copy of state that the Registry already holds, with its own expiry.

## Decision

A3 + first-config-only, B3 and C2.

**Readiness gate** (`proxy.Options.DiscoveryReadyTimeout`, default 10 s). `Gateway.Ready()` is true once a config is applied and the gate is open. The gate is evaluated for the first config the process applies:

- No `consul:` upstream, or no Resolver (`-consul-addr` unset): open at the swap.
- Otherwise the loop records every watch the config started that has not answered and arms a timer (owned by the loop, no goroutine). Each first answer is applied (snapshot rebuilt) **before** it counts, so `/readyz` turns ready only once the snapshot serves that set. A watch that is canceled (the next config stops referencing it) also counts as settled. When nothing is pending, the gate opens.
- When the timer fires first, the gate opens anyway with a WARN that lists the services that never answered.
- The gate never closes again.

**Carry-over.** When the builder meets a `consul:` upstream whose query has not answered, it reuses the backend URLs that the upstream of the same name had in the live snapshot (none if it is new). The Registry hands back the same `*Backend`s. The new watch's first answer replaces the set.

**Grace period** (`proxy.Options.BackendGrace`, default 60 s, measured with the gateway's clock).

- At each swap, every backend ID of the previous snapshot that the new one lacks is recorded as departed with the current time. An ID that is live again stops being departed.
- `Registry.Prune` keeps live IDs plus departed IDs still within their grace.
- Expiry runs at the start of every rebuild, **before** the build, so a backend that returns after its grace gets a fresh `*Backend` from `Registry.Get`, not the stale one. There is no timer: a departed backend is held until the first rebuild after its grace (a `*Backend` is a few atomics).
- A departed backend is out of rotation at once: it is in no balancer, its `tollgate_backend_*` series are deleted at the swap, and its probe is stopped by `Checker.Sync` (which resets its active verdict to up, ADR 0006).
- **What survives a flap:** the passive ejection deadline and failure streak, and the in-flight count. **What does not:** the active verdict (the probe was stopped and reset to up; a new probe starts on the same pointer when it returns, consistent with "new backends start up", because Consul has just re-vetted it) and the ejection counter series (it restarts from 0, which Prometheus treats as a counter reset).

## Consequences

- Good: a restarted gateway is not put in rotation with empty Consul upstreams, unless Consul has not answered within 10 s, and then the log names the services.
- Good: a tag or service change, or a static-to-Consul switch, never opens a 503 window.
- Good: a Consul flap cannot be used, on purpose or by accident, to bring an ejected backend back early.
- Cost: after the timeout, a pod can still be ready with an empty Consul upstream (503 on those routes, its static routes work). That is the accepted price of not stalling rollouts on a Consul outage.
- Cost: during a carry-over, a tag change keeps serving the old tag's instances until Consul answers (milliseconds normally, longer if the agent is down).
- Cost: the grace applies to **every** removed backend, not only Consul ones. A static backend that an operator removes and re-adds within 60 s keeps its ejection. This is deliberate: a re-add within a minute is far more often an undo than a fix.
- Cost: `DiscoveryReadyTimeout` and `BackendGrace` are `proxy.Options` fields, not `cmd/gateway` flags. Tests set them; the binary uses the defaults.
- Known limit: there is no "panic mode" (ADR 0006). An empty set from a healthy Consul empties the upstream (503), which is ADR 0007's documented choice.
