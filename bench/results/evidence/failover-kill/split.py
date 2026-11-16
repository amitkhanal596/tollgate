#!/usr/bin/env python3
"""Split the failed requests of each kill run into fast and slow failures.

Run from this directory: python3 split.py
Log line format (written by bench/k6/probe.js): R <start_ms> <status> <instance> <latency_ms>
"""
import json
import re

LINE = re.compile(r'msg="R (\d+) (\d+) (\S+) ([\d.]+)"')

print("| run | failed | fast (< 100 ms) | slow (>= 900 ms) | slow latency range (ms) "
      "| last slow start after fault (ms) | first slow end after fault (ms) |")
print("|---|---|---|---|---|---|---|")
for case in ("consul-kill", "passive-kill"):
    for r in (1, 2, 3):
        with open(f"failover-{case}-r{r}.json") as f:
            t0 = json.load(f)["fault_start_ms"]
        rows = []
        with open(f"failover-{case}-r{r}.log") as f:
            for line in f:
                m = LINE.search(line)
                if m and int(m.group(2)) >= 500:
                    start = int(m.group(1)) - t0
                    if -200 < start < 3000:  # the first failure cluster only
                        rows.append((start, float(m.group(4))))
        fast = [x for x in rows if x[1] < 100]
        slow = [x for x in rows if x[1] >= 900]
        assert len(fast) + len(slow) == len(rows)
        rng = f"{min(l for _, l in slow):.1f} .. {max(l for _, l in slow):.1f}" if slow else "n/a"
        last = f"{max(s for s, _ in slow):.0f}" if slow else "n/a"
        first_end = f"{min(s + l for s, l in slow):.1f}" if slow else "n/a"
        print(f"| {case} r{r} | {len(rows)} | {len(fast)} | {len(slow)} | {rng} | {last} | {first_end} |")
