#!/usr/bin/env python3
"""Validate and print raw bench results; merge them into latest.json.

  summarize.py show FILE.json     human summary; exit 1 if the measurement is not usable
  summarize.py meta RAW_DIR PART COMPLETE   write RAW_DIR/run.json (timestamp, commit, knobs, smoke flag)
  summarize.py merge RAW_DIR OUT PART       PART=all rebuilds OUT from scratch (only this run);
                                            PART=bench|chaos overlays that part onto OUT

Standard library only.
"""
import glob
import json
import os
import sys


def load(path):
    with open(path) as f:
        return json.load(f)


def f(x, nd=2):
    return "n/a" if x is None else f"{x:.{nd}f}"


def show_overhead(d):
    ok = True
    print(f"overhead  {d['config']['rate_rps']} rps x {d['config']['duration_s']:.0f}s per phase, order {d['config']['order']}")
    print(f"  {'':8} {'reqs':>8} {'err%':>6} {'drop':>6} {'p50':>7} {'p90':>7} {'p95':>7} {'p99':>7} {'max':>8}  (ms)")
    for t in ("direct", "gateway"):
        r = d[t]
        l = r["latency_ms"]
        if not l or r["requests"] == 0:
            ok = False
            print(f"  {t:8} NO DATA")
            continue
        print(f"  {t:8} {r['requests']:8.0f} {100*r['error_rate']:6.2f} {r['dropped_iterations']:6.0f} "
              f"{f(l['p50'],3):>7} {f(l['p90'],3):>7} {f(l['p95'],3):>7} {f(l['p99'],3):>7} {f(l['max'],1):>8}")
    o = d.get("overhead_ms") or {}
    if o:
        print(f"  overhead {'':8} {'':6} {'':6} {f(o['p50'],3):>7} {f(o['p90'],3):>7} {f(o['p95'],3):>7} {f(o['p99'],3):>7} {f(o['max'],1):>8}")
    else:
        ok = False
    return ok


def show_maxrps(d):
    c = d["criteria"]
    print(f"maxrps  target={d['target']}  ({d['url']})  criteria: err<{100*c['max_error_rate']}% p99<{c['p99_ms_below']}ms dropped<{100*c['max_dropped_fraction']}%  step={c['step_duration_s']:.0f}s")
    print(f"  {'target':>7} {'achieved':>9} {'err%':>6} {'dropped':>8} {'p50ms':>7} {'p99ms':>8} {'maxms':>8}  ok")
    ok = False
    for s in d["steps"]:
        if s["requests"] > 0:
            ok = True
        print(f"  {s['target_rps']:7d} {s['achieved_rps']:9.0f} {100*s['error_rate']:6.2f} {s['dropped_iterations']:8.0f} "
              f"{f(s['p50_ms']):>7} {f(s['p99_ms']):>8} {f(s['max_ms'],1):>8}  {'yes' if s['sustainable'] else 'NO'}")
    m = d["max_sustainable_rps"]
    print(f"  max sustainable: {'none of the steps passed' if m is None else f'{m:.0f} rps (step target {d['max_sustainable_step_target_rps']})'}")
    return ok


def show_limiter(d):
    c = d["config"]
    print(f"limiter  {c['rate_limit_rps']} rps, burst {c['burst']}, offered {c['offered_multiple']}x for {c['duration_s']:.0f}s")
    s = d["single_key"]
    print(f"  single key: offered {s['requests']} allowed {s['allowed']} expected {s['expected_allowed']:.1f} error {f(s['error_pct'])}% "
          f"(window {s['window_s']:.2f}s, {f(s['allowed_rps_over_window'],1)} allowed/s)")
    m = d["multi_key"]
    print(f"  9 noisy keys: allowed {m['noisy_min_allowed']}..{m['noisy_max_allowed']} per key, max |error| {f(m['noisy_max_abs_error_pct'])}% "
          f"(expected ~{m['noisy_keys'][0]['expected_allowed']:.0f})")
    p = m["polite_key"]
    print(f"  polite key (50 rps, same time): {p['requests']} requests, {p['rejected']} rejected")
    print(f"  rejects: {d['rejected_total']} total, {d['bad_rejects']} not 429-with-Retry-After (missing header: {d['missing_retry_after']}, other status: {d['other_status']})")
    print(f"  dropped iterations (load generator could not keep up): {d['dropped_iterations']}")
    return s["requests"] > 0 and d["rejected_total"] > 0


def show_failover(d):
    print(f"failover  {d['id']}  path {d['path']}  rate {d['rate_rps']} rps  fault: {d['mode']}")
    print(f"  requests {d['requests']}, failed {d['failed']} ({d['failed_by_status']}), failed before fault {d['failed_before_fault']}")
    if d["failure_window"]:
        w = d["failure_window"]
        print(f"  failure window: {w['first_offset_s']:.3f}s .. {w['last_offset_s']:.3f}s after fault ({w['span_s']:.3f}s span), {d['failed_during_fault']} failed during fault")
        for c in d["failure_clusters"]:
            print(f"    cluster {c['start_offset_s']:.3f}s .. {c['end_offset_s']:.3f}s  count {c['count']}")
    else:
        print("  no failed requests during the fault")
    print(f"  failover time (fault -> end of first failure cluster): {f(d['failover_time_s'],3)} s")
    print(f"  failures after the first cluster (re-probes etc.): {d['failed_after_first_cluster']}")
    print(f"  back in rotation: {f(d['recovery_s'],3)} s after restore ({d['recovery_note']})")
    return d["recovery_s"] is not None and d["requests"] > 0


def show_hot(d):
    c = d["config"]
    print(f"hot-reload  {c['load_rps']} rps load, {d['changes_done']}/{c['changes']} changes ({d['puts']} PUT, {d['rollbacks']} rollback)")
    print(f"  requests {d['requests_total']:.0f}, non-2xx {d['non_2xx']:.0f}, connection errors {d['connection_errors']:.0f}, k6 dropped iterations {d['dropped_iterations']:.0f}")
    p = d["propagation_ms"]
    if p:
        print(f"  propagation ms: p50 {f(p['p50'],1)} p90 {f(p['p90'],1)} p99 {f(p['p99'],1)} max {f(p['max'],1)}")
    print(f"  reloader ran {d['reloader_seconds']:.1f}s of {c['load_seconds']:.0f}s load")
    print(f"  admin failures {d['admin_failed']}, propagation timeouts {d['propagation_timeouts']}")
    h = d.get("host")
    if h:
        print(f"  gateway restarted during test: {h['gateway_restarted']}; applied version {h['applied_version']} vs head {h['head_version']}")
    covered = d["reloader_seconds"] < c["load_seconds"] - 1  # load must outlast the last change
    if not covered:
        print("  !! load stopped before the last change finished")
    return bool(p) and covered and d["changes_done"] == c["changes"] and d["propagation_timeouts"] == 0


SHOW = {"overhead": show_overhead, "maxrps": show_maxrps, "limiter": show_limiter,
        "failover": show_failover, "hot-reload": show_hot}


def cmd_show(path):
    d = load(path)
    ok = SHOW[d["test"]](d)
    if not ok:
        print(f"  !! measurement in {os.path.basename(path)} is incomplete", file=sys.stderr)
    return 0 if ok else 1


# Knobs the scripts read, with their defaults. A knob that is set to anything
# else marks the run as a smoke run. RATE has per-script defaults (overhead
# 1000, hot reload 500, failover 200), so any RATE counts as non-default.
DEFAULTS = {
    "RATE": None, "DURATION": "30", "WARMUP": "5", "ROUNDS": "2",
    "STEPS": "1000,2000,4000,8000,12000", "STEP_DURATION": "20", "GAP": "3", "P99_MS": "50",
    "OVER": "5", "CHANGES": "100", "RELOAD_GAP": "0.25", "ROLLBACK_EVERY": "10",
    "PRE": "5", "FAULT": "20", "REC": "25", "CONFIGS": "passive active consul",
    "MODES": "kill fail500", "SKIP_ACCESS_LOG": "0",
}


def norm(v):
    return v[:-1] if v and v.endswith("s") and v[:-1].replace(".", "").isdigit() else v


def sh(cmd):
    import subprocess
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception:
        return ""


def cmd_meta(raw, part, complete):
    import datetime
    knobs, smoke = {}, False
    for k, dflt in DEFAULTS.items():
        env = os.environ.get(k)
        knobs[k] = {"value": env, "default": dflt}
        if env is not None and norm(env) != dflt:
            smoke = True
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    meta = {
        "part": part,
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git_commit": sh(f"cd '{root}' && git rev-parse HEAD 2>/dev/null") or "unknown",
        "git_dirty": bool(sh(f"cd '{root}' && git status --porcelain 2>/dev/null")),
        "complete": complete == "true",
        "smoke": smoke,
        "knobs": knobs,
        "raw_dir": raw,
    }
    with open(os.path.join(raw, "run.json"), "w") as fh:
        json.dump(meta, fh, indent=2)
        fh.write("\n")
    return 0


def cmd_merge(raw, out, part):
    # "all" starts from an empty result set, so the file contains only this run.
    # "bench"/"chaos" overlay their sections onto the existing file; each
    # section keeps its own timestamp/commit/smoke flag so stale ones show.
    res = {}
    if part != "all" and os.path.exists(out):
        try:
            res = load(out)
        except ValueError:
            res = {}
    meta = load(os.path.join(raw, "run.json"))
    for p in sorted(glob.glob(os.path.join(raw, "*.json"))):
        name = os.path.basename(p)[:-5]
        if name in ("machine", "run"):
            continue
        d = load(p)
        t = d.get("test")
        if t == "failover":
            res.setdefault("failover", {})[d["id"]] = d
        elif t == "maxrps":
            res.setdefault("maxrps", {})[name.replace("maxrps-", "")] = d
        elif t == "overhead":
            res.setdefault("overhead", {})[name.replace("overhead", "").lstrip("-") or "access_log_off"] = d
        elif t:
            res[t.replace("-", "_")] = d
    mi = os.path.join(raw, "machine.json")
    if os.path.exists(mi):
        res["machine"] = load(mi)
    secs = res.setdefault("sections", {}) if part != "all" else {}
    for s in (["bench", "chaos"] if part == "all" else [part]):
        secs[s] = meta
    res["sections"] = secs
    res["timestamp_utc"] = meta["timestamp_utc"]
    res["git_commit"] = meta["git_commit"]
    res["knobs"] = meta["knobs"]
    res["smoke"] = any(s["smoke"] for s in secs.values())
    # complete only if both halves are present, complete and from real runs of this commit
    res["complete"] = all(s in secs and secs[s]["complete"] for s in ("bench", "chaos"))
    res["generated_by"] = part
    with open(out, "w") as fh:
        json.dump(res, fh, indent=2)
        fh.write("\n")
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "show":
        sys.exit(cmd_show(sys.argv[2]))
    if len(sys.argv) == 5 and sys.argv[1] == "merge":
        sys.exit(cmd_merge(sys.argv[2], sys.argv[3], sys.argv[4]))
    if len(sys.argv) == 5 and sys.argv[1] == "meta":
        sys.exit(cmd_meta(sys.argv[2], sys.argv[3], sys.argv[4]))
    print(__doc__, file=sys.stderr)
    sys.exit(2)
