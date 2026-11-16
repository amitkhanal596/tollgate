#!/usr/bin/env python3
"""Turn a probe.js request log into failover numbers (JSON on stdout).

  analyze_failover.py LOG --id ID --path P --mode kill|fail500 --rate N \
      --t0 EPOCH_MS --t1 EPOCH_MS --victim demo-1

t0 = when the fault began (docker State.FinishedAt, or the E fault_on line);
t1 = when the victim was restored (docker State.StartedAt, or E fault_off).
All times are on the Docker VM clock, so no cross-clock correction is needed.
Standard library only.
"""
import argparse
import json
import sys
from collections import Counter

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
a = p.parse_args()

reqs = []  # (start_ms, status, instance)
with open(a.log) as fh:
    for line in fh:
        parts = line.split()
        # k6 console lines look like: time="..." level=info msg="R 1700 200 demo-1 0.4" source=console
        if 'msg="R ' in line:
            msg = line.split('msg="', 1)[1].rsplit('"', 1)[0].split()
            parts = msg
        if len(parts) >= 5 and parts[0] == "R":
            reqs.append((float(parts[1]), int(parts[2]), parts[3]))
reqs.sort()
if not reqs:
    print("no request lines in log", file=sys.stderr)
    sys.exit(1)

# FinishedAt is stamped slightly after the process dies, so a request can fail a
# few ms "before" T0; allow 100 ms of slack.
t0, t1 = a.t0 - 100, a.t1
fails = [(t, s) for t, s, _ in reqs if s != 200]
before = [f for f in fails if f[0] < t0]
during = [f for f in fails if t0 <= f[0] < t1]
after = [f for f in fails if f[0] >= t1]

clusters = []
for t, _ in sorted(f for f in fails if f[0] >= t0):
    if clusters and t - clusters[-1][1] <= a.gap * 1000:
        clusters[-1][1] = t
        clusters[-1][2] += 1
    else:
        clusters.append([t, t, 1])

window = None
if during:
    first, last = during[0][0], during[-1][0]
    window = {"first_offset_s": (first - t0) / 1000, "last_offset_s": (last - t0) / 1000, "span_s": (last - first) / 1000}

# Recovery: first 200 served by the victim at or after the restore.
rec = next((t for t, s, i in reqs if t >= t1 and s == 200 and i == a.victim), None)
last_t = reqs[-1][0]
victim_before = [t for t, s, i in reqs if t < t0 and s == 200 and i == a.victim]
note = "first 200 served by the victim after restore"
if rec is None:
    note = "victim never served a request again before the log ended"

res = {
    "test": "failover", "id": a.id, "path": a.path, "mode": a.mode, "rate_rps": a.rate, "victim": a.victim,
    "requests": len(reqs),
    "failed": len(fails),
    "failed_by_status": dict(Counter(str(s) for _, s in fails)),
    "failed_before_fault": len(before),
    "failed_during_fault": len(during),
    "failed_after_restore": len(after),
    "failure_window": window,
    "failure_clusters": [
        {"start_offset_s": (c[0] - t0) / 1000, "end_offset_s": (c[1] - t0) / 1000, "count": c[2]} for c in clusters
    ],
    # The first cluster is the gateway discovering the dead/failing instance;
    # later ones are periodic re-probes after the passive-ejection cool-off.
    "failover_time_s": ((clusters[0][1] - t0) / 1000) if clusters and clusters[0][0] - t0 < 5000 else 0.0,
    "failed_after_first_cluster": sum(c[2] for c in clusters[1:]),
    "victim_served_before_fault": len(victim_before),
    "recovery_s": ((rec - t1) / 1000) if rec is not None else None,
    "recovery_note": note,
    "fault_start_ms": t0, "restore_ms": t1,
    "log_span_s": (last_t - reqs[0][0]) / 1000,
}
json.dump(res, sys.stdout, indent=2)
print()
