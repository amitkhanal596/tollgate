#!/usr/bin/env python3
"""Count the requests that were in flight across a config change.

  analyze_hotreload.py LOG RAW_JSON OUT_JSON

LOG is hot-reload.js's console log (R and C lines). A request "spans" a change
when it started before the admin call was sent and ended after the change had
taken effect: the moment the gateway was first seen serving the new config, or
(for changes that cannot be observed) the admin call's return plus 100 ms. That
is conservative: the gateway applies the change somewhere between "sent" and
"seen", and a request that merely overlaps the window without covering all of
it is not counted. Reports spanning requests, and how many of them failed.
Standard library only.
"""
import bisect
import json
import sys

UNOBSERVED_MARGIN_MS = 100


def main(log, raw_path, out):
    reqs, changes = [], []
    with open(log) as fh:
        for line in fh:
            if 'msg="' not in line:
                continue
            p = line.split('msg="', 1)[1].rsplit('"', 1)[0].split()
            if len(p) == 5 and p[0] == "R":
                reqs.append((p[1], float(p[2]), float(p[3]), int(p[4])))
            elif len(p) == 7 and p[0] == "C":
                changes.append({"i": int(p[1]), "kind": p[2], "rollback": p[3] == "1",
                                "sent": float(p[4]), "returned": float(p[5]), "seen": float(p[6])})
    raw = json.load(open(raw_path))
    # Per change: window that a request must fully contain.
    wins = []
    for c in changes:
        end = c["seen"] if c["seen"] else c["returned"] + UNOBSERVED_MARGIN_MS
        wins.append((c["sent"], end))
    wins.sort()
    starts = [w[0] for w in wins]

    spanning = {"F": 0, "S": 0}
    spanning_failed = 0
    spanning_changes = {}  # change index -> how many requests span it
    for kind, s, e, status in reqs:
        # candidates: changes with sent > s (started before the call was sent)
        lo = bisect.bisect_right(starts, s)
        n = 0
        for j in range(lo, len(wins)):
            if wins[j][0] >= e:
                break
            if wins[j][1] < e:
                n += 1
        if n:
            spanning[kind] += 1
            if not 200 <= status <= 299:
                spanning_failed += 1
    per_change = []
    for (sent, end) in wins:
        k = 0
        for kind, s, e, status in reqs:
            if s < sent and e > end:
                k += 1
        per_change.append(k)
    per_change_sorted = sorted(per_change)
    res = dict(raw)
    res["test"] = "hot-reload"
    res["requests_logged"] = len(reqs)
    res["changes_logged"] = len(changes)
    res["spanning_requests"] = spanning["F"] + spanning["S"]
    res["spanning_requests_slow_route"] = spanning["S"]
    res["spanning_requests_fast_route"] = spanning["F"]
    res["spanning_requests_failed"] = spanning_failed
    res["spanning_per_change"] = {
        "min": per_change_sorted[0] if per_change_sorted else None,
        "median": per_change_sorted[len(per_change_sorted) // 2] if per_change_sorted else None,
        "max": per_change_sorted[-1] if per_change_sorted else None,
    }
    res["span_definition"] = ("started before the admin call was sent and ended after the change was observed "
                              f"(or admin return + {UNOBSERVED_MARGIN_MS} ms when not observable)")
    with open(out, "w") as fh:
        json.dump(res, fh, indent=2)
        fh.write("\n")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(*sys.argv[1:]))
