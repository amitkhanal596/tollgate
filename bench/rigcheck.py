#!/usr/bin/env python3
"""Host-side validity checks for a benchmark run (macOS; degrades gracefully elsewhere).

  rigcheck.py pre  OUT_JSON      host swap, free memory, power source (+ prints a loud warning if bad)
  rigcheck.py post PRE_JSON START_EPOCH END_EPOCH OUT_JSON
                                 Sleep/Wake/DarkWake events from `pmset -g log` inside the run window

Any sleep/wake event inside the window marks the run invalid: every container
freezes with the host. Warnings never abort the run. Standard library only.
"""
import datetime
import json
import re
import subprocess
import sys


def sh(cmd, timeout=60):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout).stdout
    except Exception:
        return ""


def to_mb(v, unit):
    v = float(v)
    return v * {"K": 1 / 1024, "M": 1, "G": 1024}.get(unit.upper(), 1)


def pre():
    r = {"warnings": []}
    sw = sh("sysctl -n vm.swapusage").strip()
    r["swap_raw"] = sw
    m = re.search(r"total = ([\d.]+)([KMG]).*used = ([\d.]+)([KMG]).*free = ([\d.]+)([KMG])", sw)
    if m:
        total, used = to_mb(m[1], m[2]), to_mb(m[3], m[4])
        r["swap_total_mb"], r["swap_used_mb"] = total, used
        r["swap_used_pct"] = 100 * used / total if total else 0.0
        if r["swap_used_pct"] > 50:
            r["warnings"].append(f"host swap is {r['swap_used_pct']:.0f}% used ({used:.0f} of {total:.0f} MB): "
                                 "expect VM stalls; close memory-heavy apps")
    vm = sh("vm_stat")
    ps = re.search(r"page size of (\d+) bytes", vm)
    page = int(ps[1]) if ps else 16384

    def pages(label):
        mm = re.search(rf"{label}:\s+(\d+)", vm)
        return int(mm[1]) if mm else 0

    free = (pages("Pages free") + pages("Pages speculative")) * page / 1e6
    inactive = pages("Pages inactive") * page / 1e6
    r["free_memory_mb"], r["inactive_memory_mb"] = free, inactive
    ram = sh("sysctl -n hw.memsize").strip()
    if ram.isdigit():
        r["ram_mb"] = int(ram) / 1e6
        if free < 0.02 * r["ram_mb"]:
            r["warnings"].append(f"free memory is low ({free:.0f} MB free of {r['ram_mb']:.0f} MB; "
                                 f"{inactive:.0f} MB inactive)")
    batt = sh("pmset -g batt")
    mm = re.search(r"Now drawing from '([^']+)'", batt)
    r["power_source"] = mm[1] if mm else "unknown"
    if r["power_source"] != "AC Power" and mm:
        r["warnings"].append(f"running on {r['power_source']}, not AC power: CPU may throttle")
    return r


def parse_sleep_events(text, start, end, slack=5):
    """Sleep/Wake/DarkWake lines of `pmset -g log` text whose time is inside [start-slack, end+slack] (epoch s)."""
    ev = []
    # lines look like: 2026-11-16 05:36:26 -0800 Sleep   Entering Sleep state due to ...
    rx = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d [+-]\d{4})\s+(Sleep|Wake|DarkWake)\b(.*)$")
    for line in text.splitlines():
        m = rx.match(line)
        if not m:
            continue
        t = datetime.datetime.strptime(m[1], "%Y-%m-%d %H:%M:%S %z").timestamp()
        if start - slack <= t <= end + slack:
            ev.append({"time": m[1], "event": m[2], "detail": m[3].strip()[:120]})
    return ev


def post(pre_path, start, end, out):
    start, end = float(start), float(end)
    ev = parse_sleep_events(sh("pmset -g log", 120), start, end)
    with open(pre_path) as fh:
        res = json.load(fh)
    res["sleep_wake_events"] = ev
    res["valid"] = not ev
    res["window_utc"] = [datetime.datetime.fromtimestamp(start, datetime.timezone.utc).isoformat(),
                         datetime.datetime.fromtimestamp(end, datetime.timezone.utc).isoformat()]
    with open(out, "w") as fh:
        json.dump(res, fh, indent=2)
    print(f"rig validity: {'VALID' if res['valid'] else 'INVALID, sleep/wake events in the run window'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "pre":
        r = pre()
        with open(sys.argv[2], "w") as fh:
            json.dump(r, fh, indent=2)
        if r["warnings"]:
            bar = "!" * 72
            print(bar, file=sys.stderr)
            for w in r["warnings"]:
                print(f"!! RIG WARNING: {w}", file=sys.stderr)
            print("!! Continuing, but results may be marked rig-perturbed. See bench/README.md.", file=sys.stderr)
            print(bar, file=sys.stderr)
        sys.exit(0)
    if len(sys.argv) == 6 and sys.argv[1] == "post":
        sys.exit(post(*sys.argv[2:]))
    print(__doc__, file=sys.stderr)
    sys.exit(2)
