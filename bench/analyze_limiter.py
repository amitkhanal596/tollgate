#!/usr/bin/env python3
"""Turn limiter.js's request log into per-key limiter accuracy (limiter.json).

  analyze_limiter.py LOG RAW_JSON OUT_JSON

Expected allowed for a key = burst + rate x window, where window is the span
from the key's first to its last request plus one inter-arrival time. That
formula only holds while the key kept offering more than the limit: a pause
longer than burst/rate lets the bucket fill to its cap, and the surplus tokens
are lost. So every key gets a generator check: its largest gap between
consecutive requests must stay below burst/rate (1 s for 100 rps / burst 100).
Keys that fail the check are reported as generator-limited and excluded from
the error statistics. k6's dropped-iteration counter is kept as information
only: a handful of dropped iterations that leave no long gap do not matter.

The polite key (below the limit) is judged only on "rejected == 0".
Standard library only.
"""
import json
import statistics
import sys
from collections import defaultdict


def read_log(path):
    rows = []  # (key, start_ms, status, retry_after)
    with open(path) as fh:
        for line in fh:
            if 'msg="L ' not in line:
                continue
            parts = line.split('msg="', 1)[1].rsplit('"', 1)[0].split()
            if len(parts) >= 5 and parts[0] == "L":
                rows.append((int(parts[1]), float(parts[2]), int(parts[3]), int(parts[4])))
    return rows


def analyse(key, rows, offered_rps, limit, burst):
    rows = sorted(rows, key=lambda r: r[0])
    ts = [r[0] for r in rows]
    allowed = sum(1 for r in rows if r[1] == 200)
    rejected = sum(1 for r in rows if r[1] == 429)
    other = sum(1 for r in rows if r[1] not in (200, 429))
    window = ((ts[-1] - ts[0]) / 1000 + 1 / offered_rps) if len(ts) > 1 else 0.0
    gaps = [b - a for a, b in zip(ts, ts[1:])]
    max_gap = max(gaps) if gaps else None
    expected = burst + limit * window
    return {
        "key": key, "offered_rps": offered_rps, "window_s": window, "requests": len(rows),
        "allowed": allowed, "rejected": rejected, "other_status": other,
        "expected_allowed": expected,
        "error_pct": (100 * (allowed - expected) / expected) if expected else None,
        "max_gap_ms": max_gap,
        # Offered rate stayed above the limit for the whole window iff no gap exceeded burst/rate.
        "offered_ok": max_gap is not None and max_gap < 1000 * burst / limit,
    }


def dist(vals):
    if not vals:
        return None
    return {"min": min(vals), "median": statistics.median(vals), "max": max(vals)}


def main(log, raw_path, out):
    raw = json.load(open(raw_path))
    c = raw["config"]
    limit, burst, dur = c["rate_limit_rps"], c["burst"], c["duration_s"]
    per_key, polite_rps, nk = c["offered_per_key_rps"], c["polite_rps"], c["noisy_keys"]
    rows = read_log(log)
    if not rows:
        print("no request lines in log", file=sys.stderr)
        return 1
    t_min = min(r[1] for r in rows)
    # Phase 1 (single key) starts at t=0; the multi-key phase starts DURATION+5 s later.
    single_rows = [(r[1], r[2], r[3]) for r in rows if r[0] == 0 and r[1] < t_min + (dur + 2.5) * 1000]
    multi = defaultdict(list)
    for k, t, s, ra in rows:
        if t >= t_min + (dur + 4) * 1000:
            multi[k].append((t, s, ra))
    single = analyse(0, single_rows, per_key, limit, burst)
    noisy = [analyse(k, multi[k], per_key, limit, burst) for k in range(nk)]
    polite = analyse(9, multi[9], polite_rps, limit, burst)
    good = [n for n in noisy if n["offered_ok"]]
    errs = [n["error_pct"] for n in good]
    allowed_counts = [n["allowed"] for n in good]
    missing_ra = sum(1 for k, t, s, ra in rows if s == 429 and ra == 0)
    other = sum(1 for r in rows if r[2] not in (200, 429))
    result = {
        "test": "limiter",
        "config": c,
        "single_key": single,
        "multi_key": {
            "noisy_keys": noisy,
            "noisy_keys_generator_ok": len(good),
            "noisy_error_pct": dist(errs),
            "noisy_allowed": dist(allowed_counts),
            "polite_key": polite,
            "polite_isolated": polite["rejected"] == 0 and polite["requests"] > 0,
        },
        "rejected_total": sum(1 for r in rows if r[2] == 429),
        "bad_rejects": missing_ra + other,
        "missing_retry_after": missing_ra,
        "other_status": other,
        "k6_dropped_iterations": raw.get("k6_dropped_iterations"),
    }
    with open(out, "w") as fh:
        json.dump(result, fh, indent=2)
        fh.write("\n")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(*sys.argv[1:]))
