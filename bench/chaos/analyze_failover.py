#!/usr/bin/env python3
"""Turn a probe.js request log into failover numbers (JSON on stdout).

  analyze_failover.py LOG --id ID --path P --mode kill|fail500 --rate N \
      --t0 EPOCH_MS --t1 EPOCH_MS --victim demo-1 [--fault-s S] [--cooloff 10]

t0 = when the fault began (docker State.FinishedAt, or the E fault_on line);
t1 = when the victim was restored (docker State.StartedAt, or E fault_off).
All times are on the Docker VM clock, so no cross-clock correction is needed.

Every offset is measured from the real t0. A request that starts a few ms
"before" t0 can still be a fault failure (FinishedAt is stamped slightly after
the process dies), so SLACK_MS is used only to decide which failures belong to
the fault, never to shift the clock.

Client-visible impact of a failed request is start + its logged latency (a
request that starts at t0 and times out after 3 s hurts for 3 s).
Standard library only.
"""
import argparse
import json
import sys
from collections import Counter

SLACK_MS = 100



def parse_log(path):
    reqs = []  # (start_ms, status, instance, latency_ms)
    with open(path) as fh:
        for line in fh:
            # k6 console lines look like: time="..." level=info msg="R 1700 200 demo-1 0.4" source=console
            if 'msg="R ' not in line:
                continue
            parts = line.split('msg="', 1)[1].rsplit('"', 1)[0].split()
            if len(parts) >= 5 and parts[0] == "R":
                reqs.append((float(parts[1]), int(parts[2]), parts[3], float(parts[4])))
    reqs.sort()
    return reqs


def analyze(reqs, id, path, mode, rate, t0, t1, victim="demo-1", gap=1.0, near=2.0, fault_s=None, cooloff=10.0):
    """Pure analysis of a request list [(start_ms, status, instance, latency_ms)] (sorted)."""
    fails = [(t, s, lat) for t, s, _, lat in reqs if s != 200]
    before = [f for f in fails if f[0] < t0 - SLACK_MS]       # failures unrelated to the fault
    fault_fails = [f for f in fails if f[0] >= t0 - SLACK_MS]
    during = [f for f in fault_fails if f[0] < t1]
    after = [f for f in fault_fails if f[0] >= t1]

    def off(ms):
        return (ms - t0) / 1000

    clusters = []  # [first_start, last_start, count, max_visible_end, max_latency]
    for t, _, lat in sorted(fault_fails):
        if clusters and t - clusters[-1][1] <= gap * 1000:
            c = clusters[-1]
            c[1] = t
            c[2] += 1
            c[3] = max(c[3], t + lat)
            c[4] = max(c[4], lat)
        else:
            clusters.append([t, t, 1, t + lat, lat])

    window = None
    if during:
        first, last = during[0][0], during[-1][0]
        window = {"first_offset_s": off(first), "last_offset_s": off(last), "span_s": (last - first) / 1000}

    first_cluster = clusters[0] if clusters and off(clusters[0][0]) <= near else None
    later = [c for c in clusters if c is not first_cluster]
    # Re-probes: failures after the first cluster, while the victim was still broken.
    reprobes = [c for c in later if c[0] < t1]
    reprobe_fail_count = sum(c[2] for c in reprobes)
    if fault_s is None:
        fault_s = (t1 - t0) / 1000

    # Recovery: first 200 served by the victim at or after the restore.
    rec = next((t for t, s, i, _ in reqs if t >= t1 and s == 200 and i == victim), None)
    victim_before = [t for t, s, i, _ in reqs if t < t0 and s == 200 and i == victim]
    note = "first 200 served by the victim after restore"
    if rec is None:
        note = "victim never served a request again before the log ended"

    return {
        "test": "failover", "id": id, "path": path, "mode": mode, "rate_rps": rate, "victim": victim,
        "fault_s": fault_s,
        "requests": len(reqs),
        "failed": len(fails),
        "failed_by_status": dict(Counter(str(s) for _, s, _ in fails)),
        "failed_before_fault": len(before),
        "failed_during_fault": len(during),
        "failed_after_restore": len(after),
        "failure_window": window,
        "failure_clusters": [
            {"start_offset_s": off(c[0]), "end_offset_s": off(c[1]), "visible_end_offset_s": off(c[3]),
             "count": c[2], "max_latency_ms": c[4]} for c in clusters
        ],
        # null when no failure cluster starts near the fault (nothing failed, or the failures are unrelated).
        "first_failure_offset_s": off(first_cluster[0]) if first_cluster else None,
        # Fault to the end of the last failed request of the first cluster, including that request's own latency.
        "failover_time_s": off(first_cluster[3]) if first_cluster else None,
        "failover_failed_requests": first_cluster[2] if first_cluster else 0,
        "failover_max_latency_ms": first_cluster[4] if first_cluster else None,
        # Single failed client requests while the victim is still broken: by design the gateway sends
        # one real request to it when the passive cool-off expires. Users see these.
        "reprobe_failures": reprobe_fail_count,
        "reprobe_clusters": len(reprobes),
        "reprobe_failures_per_cooloff": reprobe_fail_count / (fault_s / cooloff) if fault_s > 0 else None,
        "failed_after_first_cluster": sum(c[2] for c in later),
        "victim_served_before_fault": len(victim_before),
        "recovery_s": ((rec - t1) / 1000) if rec is not None else None,
        "recovery_note": note,
        "fault_start_ms": t0, "restore_ms": t1,
        "log_span_s": (reqs[-1][0] - reqs[0][0]) / 1000,
    }


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("log")
    p.add_argument("--id", required=True)
    p.add_argument("--path", required=True)
    p.add_argument("--mode", required=True)
    p.add_argument("--rate", type=int, required=True)
    p.add_argument("--t0", type=float, required=True)
    p.add_argument("--t1", type=float, required=True)
    p.add_argument("--victim", default="demo-1")
    p.add_argument("--gap", type=float, default=1.0, help="seconds of quiet that separate failure clusters")
    p.add_argument("--near", type=float, default=2.0, help="the first failure cluster must start within this many seconds of t0 to count as the failover")
    p.add_argument("--fault-s", type=float, default=None, help="how long the fault lasted (recorded)")
    p.add_argument("--cooloff", type=float, default=10.0, help="passive-ejection cool-off in seconds")
    a = p.parse_args(argv)
    reqs = parse_log(a.log)
    if not reqs:
        print("no request lines in log", file=sys.stderr)
        return 1
    res = analyze(reqs, a.id, a.path, a.mode, a.rate, a.t0, a.t1, a.victim, a.gap, a.near, a.fault_s, a.cooloff)
    json.dump(res, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
