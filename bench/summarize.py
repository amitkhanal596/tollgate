#!/usr/bin/env python3
"""Validate, label and summarise raw bench results.

  summarize.py show FILE.json              human summary of one raw result; exit 1 if unusable
  summarize.py is-smoke                    print 1 if SMOKE=1 or any knob differs from its default
  summarize.py meta RAW_DIR PART COMPLETE  write RAW_DIR/run.json (window, knobs, provenance, validity)
  summarize.py merge RAW_DIR OUT PART      PART=all rebuilds OUT from this run only; bench|chaos overlay
  summarize.py report LATEST.json OUT.md   generate RESULTS.md (every number comes from LATEST.json)

Standard library only. Nothing in RESULTS.md is hand-written: the generator
prints what each number measures, how noisy it is, and whether the rig limited it.
"""
import datetime
import glob
import json
import math
import os
import re
import statistics
import sys

STALL_PERTURBED_MS = 100       # watchdog gap above this inside a step: rig-perturbed
CPU_RIG_LIMIT = 0.75           # containers together above this share of the VM's vCPUs: rig-limited
CPUSET_SATURATED = 0.90        # k6 or backend cpuset above this share of its CPUs: rig-limited


def load(path):
    with open(path) as fh:
        return json.load(fh)


def f(x, nd=2):
    return "n/a" if x is None else f"{x:.{nd}f}"


def med(v):
    v = [x for x in v if x is not None]
    return statistics.median(v) if v else None


def mn(v):
    v = [x for x in v if x is not None]
    return min(v) if v else None


def mx(v):
    v = [x for x in v if x is not None]
    return max(v) if v else None


# ----------------------------------------------------------------------------
# overhead: per-round deltas, A/A noise floor
# ----------------------------------------------------------------------------
STATS = ("p50", "p90", "p99")


def overhead_stats(d):
    """Per-round (B - A) deltas for A/B and A/A, from per-phase percentiles.

    A side value = mean of the two phases on that side in the round (ABBA)."""
    rounds = {}
    for p in d["phases"]:
        if not p["latency_ms"] or p["requests"] == 0:
            continue
        rounds.setdefault((p["kind"], p["round"]), {"A": [], "B": []})[p["side"]].append(p["latency_ms"])
    deltas = {"AB": {s: [] for s in STATS}, "AA": {s: [] for s in STATS}}
    abs_a, abs_b = {s: [] for s in STATS}, {s: [] for s in STATS}
    for (kind, _), sides in sorted(rounds.items()):
        if not sides["A"] or not sides["B"]:
            continue
        for s in STATS:
            a = statistics.mean(x[s] for x in sides["A"])
            b = statistics.mean(x[s] for x in sides["B"])
            deltas[kind][s].append(b - a)
            if kind == "AB":
                abs_a[s].append(a)
                abs_b[s].append(b)
    out = {"rounds_ab": len(deltas["AB"]["p50"]), "rounds_aa": len(deltas["AA"]["p50"]),
           "direct_ms": {s: med(abs_a[s]) for s in STATS}, "gateway_ms": {s: med(abs_b[s]) for s in STATS},
           "dropped_phases": sum(1 for p in d["phases"] if p["dropped_iterations"] > 0),
           "error_phases": sum(1 for p in d["phases"] if p["error_rate"] > 0), "stats": {}}
    for s in STATS:
        ab, aa = deltas["AB"][s], deltas["AA"][s]
        noise = max(abs(x) for x in aa) if aa else None  # largest |delta| between identical paths
        m = med(ab)
        out["stats"][s] = {
            "median": m, "min": mn(ab), "max": mx(ab), "per_round": ab, "aa_per_round": aa,
            "noise_floor": noise, "aa_median": med(aa),
            "below_noise_floor": (noise is not None and m is not None and abs(m) <= noise),
        }
    return out


def show_overhead(d):
    ok = True
    c = d["config"]
    o = overhead_stats(d)
    print(f"overhead  {c['rate_rps']} rps x {c['phase_duration_s']:.0f}s per phase, {c['rounds']} ABBA round(s), A/A {'on' if c['aa'] else 'off'}")
    if not o["rounds_ab"]:
        print("  NO DATA")
        return False
    for s in ("p50", "p90", "p99"):
        st = o["stats"][s]
        tag = ""
        if s == "p99" or st["below_noise_floor"]:
            tag = "  (below noise floor)" if st["below_noise_floor"] else ("  (no noise floor measured)" if st["noise_floor"] is None else "")
        print(f"  {s}: direct {f(o['direct_ms'][s],3)} ms, gateway {f(o['gateway_ms'][s],3)} ms; per-round delta median {f(st['median'],3)} "
              f"(min {f(st['min'],3)}, max {f(st['max'],3)}) ms; A/A noise floor {f(st['noise_floor'],3)} ms{tag}")
    print(f"  phases with dropped iterations: {o['dropped_phases']}, with errors: {o['error_phases']}")
    return ok and o["error_phases"] == 0


def show_maxrps(d):
    c = d["criteria"]
    print(f"maxrps  target={d['target']}  criteria: err<{100*c['max_error_rate']}% p99<{c['p99_ms_below']}ms achieved>={100*c['min_achieved_fraction']:.0f}% of target (raw k6 verdict; rig labels come later)")
    print(f"  {'target':>7} {'achieved':>9} {'err%':>6} {'p50ms':>7} {'p99ms':>8}  k6-ok")
    ok = False
    for s in d["steps"]:
        ok = ok or s["requests"] > 0
        print(f"  {s['target_rps']:7d} {s['achieved_rps']:9.0f} {100*s['error_rate']:6.2f} {f(s['p50_ms']):>7} {f(s['p99_ms']):>8}  {'yes' if s['passes_k6'] else 'no'}")
    return ok


def show_limiter(d):
    c = d["config"]
    print(f"limiter  {c['rate_limit_rps']} rps, burst {c['burst']}, offered {c['offered_multiple']}x per key for {c['duration_s']:.0f}s")
    s = d["single_key"]
    print(f"  single key: offered {s['requests']} allowed {s['allowed']} expected {s['expected_allowed']:.1f} error {f(s['error_pct'])}% "
          f"(window {s['window_s']:.2f}s, largest gap between requests {f(s['max_gap_ms'],0)} ms, generator {'ok' if s['offered_ok'] else 'NOT ok'})")
    m = d["multi_key"]
    e = m["noisy_error_pct"]
    if e:
        print(f"  {len(m['noisy_keys'])} noisy keys ({m['noisy_keys_generator_ok']} with a healthy generator): error min {f(e['min'])}% median {f(e['median'])}% max {f(e['max'])}%")
    p = m["polite_key"]
    print(f"  polite key (50 rps, same time): {p['requests']} requests, {p['rejected']} rejected (isolation {'ok' if m['polite_isolated'] else 'BROKEN'})")
    print(f"  rejects: {d['rejected_total']} total, {d['bad_rejects']} not 429-with-Retry-After")
    gen_ok = s["offered_ok"] and m["noisy_keys_generator_ok"] == len(m["noisy_keys"])
    if not gen_ok:
        print("  !! the load generator paused for longer than burst/rate on some key: accuracy numbers are not valid")
    return s["requests"] > 0 and d["rejected_total"] > 0 and gen_ok and m["polite_isolated"] and d["bad_rejects"] == 0


def show_failover(d):
    print(f"failover  {d['id']}  path {d['path']}  rate {d['rate_rps']} rps  fault: {d['mode']} for {f(d.get('fault_s'),1)} s")
    print(f"  requests {d['requests']}, failed {d['failed']} ({d['failed_by_status']}), failed before fault {d['failed_before_fault']}")
    for c in d["failure_clusters"]:
        print(f"    cluster {c['start_offset_s']:.3f}s .. {c['end_offset_s']:.3f}s (client-visible until {c['visible_end_offset_s']:.3f}s)  count {c['count']}  max latency {c['max_latency_ms']:.1f} ms")
    if d["failover_time_s"] is None:
        print("  failover time: n/a (no failure cluster started near the fault)")
    else:
        print(f"  failover time (fault -> client-visible end of first cluster): {f(d['failover_time_s'],3)} s ({d['failover_failed_requests']} failed requests)")
    print(f"  re-probe failures while the victim stayed broken: {d['reprobe_failures']} ({f(d['reprobe_failures_per_cooloff'],2)} per 10 s cool-off, each a failed client request)")
    print(f"  back in rotation: {f(d['recovery_s'],3)} s after restore ({d['recovery_note']})")
    return d["recovery_s"] is not None and d["requests"] > 0


def show_hot(d):
    c = d["config"]
    print(f"hot-reload  {c['load_rps']} rps fast + {c['slow_load_rps']} rps slow load, {d['changes_done']}/{c['changes']} changes ({d['puts']} PUT, {d['rollbacks']} rollback), one every {c['period_s']} s")
    print(f"  requests {d['requests_total']:.0f}, non-2xx {d['non_2xx']:.0f}, connection errors {d['connection_errors']:.0f}, k6 dropped iterations {d['dropped_iterations']:.0f}")
    sp = d["spanning_per_change"]
    print(f"  requests whose [start, end] contains a config change: {d['spanning_requests']} "
          f"({d['spanning_requests_slow_route']} slow route, {d['spanning_requests_fast_route']} fast), failed: {d['spanning_requests_failed']}; per change min {sp['min']} median {sp['median']} max {sp['max']}")
    for key, label in (("propagation_ms", "from admin call returned (lower bound)"), ("propagation_from_send_ms", "from admin call sent (upper bound)")):
        p = d.get(key)
        if p:
            print(f"  propagation {label}: p50 {f(p['p50'],1)} p90 {f(p['p90'],1)} p99 {f(p['p99'],1)} max {f(p['max'],1)} ms")
    a = d.get("admin_call_ms")
    if a:
        print(f"  admin call itself: p50 {f(a['p50'],1)} p99 {f(a['p99'],1)} max {f(a['max'],1)} ms (1 ms clock resolution)")
    print(f"  reloader ran {d['reloader_seconds']:.1f}s of {c['load_seconds']:.0f}s load")
    print(f"  admin failures {d['admin_failed']}, propagation timeouts {d['propagation_timeouts']}")
    h = d.get("host")
    if h:
        print(f"  gateway restarted during test: {h['gateway_restarted']}; applied version {h['applied_version']} vs head {h['head_version']}")
    covered = d["reloader_seconds"] < c["load_seconds"] - 1  # load must outlast the last change
    if not covered:
        print("  !! load stopped before the last change finished")
    return (bool(d.get("propagation_ms")) and covered and d["changes_done"] == c["changes"] and d["propagation_timeouts"] == 0
            and d["dropped_iterations"] == 0 and d["spanning_requests"] > 0)


SHOW = {"overhead": show_overhead, "maxrps": show_maxrps, "limiter": show_limiter,
        "failover": show_failover, "hot-reload": show_hot}


def cmd_show(path):
    d = load(path)
    ok = SHOW[d["test"]](d)
    if not ok:
        print(f"  !! measurement in {os.path.basename(path)} is incomplete or failed its checks", file=sys.stderr)
    return 0 if ok else 1


# ----------------------------------------------------------------------------
# knobs, smoke detection, meta
# ----------------------------------------------------------------------------
# A knob set to anything but its default marks the run as a smoke run (RATE has
# per-script defaults, so any RATE counts). FAULT (fixed fault length) likewise.
DEFAULTS = {
    "RATE": None, "DURATION": "8", "WARMUP": "5", "ROUNDS": "5", "AA": "1",
    "STEPS": "2000,4000,6000,8000,10000,12000,14000,16000", "STEP_DURATION": "8", "RAMP": "1", "REPS": "3",
    "TARGETS": "direct gateway gateway3", "SETTLE": "8", "P99_MS": "50",
    "LIMITER_DURATION": "15", "OVER": "2", "NOISY": "4",
    "CHANGES": "100", "SLOW_RATE": "100", "PERIOD": "0.5", "ROLLBACK_EVERY": "10",
    "PRE": "4", "REC": "15", "REPEATS": "3", "FAULT": None, "FAULT_MIN": "13", "FAULT_MAX": "27",
    "CONFIGS": "passive active consul", "MODES": "kill fail500", "SKIP_ACCESS_LOG": "0",
}


def norm(v):
    return v[:-1] if v and v.endswith("s") and v[:-1].replace(".", "").isdigit() else v


def knobs_and_smoke():
    knobs, smoke = {}, os.environ.get("SMOKE") == "1"
    for k, dflt in DEFAULTS.items():
        env = os.environ.get(k)
        knobs[k] = {"value": env, "default": dflt}
        if env is not None and norm(env) != dflt:
            smoke = True
    return knobs, smoke


def sh(cmd):
    import subprocess
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception:
        return ""


def read_jsonl(path):
    out = []
    if os.path.exists(path):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        pass
    return out


def cmd_meta(raw, part, complete):
    knobs, smoke = knobs_and_smoke()

    def opt(name):
        p = os.path.join(raw, name)
        return load(p) if os.path.exists(p) else None

    plans = {}
    for p in sorted(glob.glob(os.path.join(raw, "maxrps-plan*.json"))):
        plans[os.path.basename(p)[len("maxrps-plan"):-5].lstrip("-") or "main"] = load(p)
    failed = [l.strip() for l in open(os.path.join(raw, "failed.txt"))] if os.path.exists(os.path.join(raw, "failed.txt")) else []
    meta = {
        "part": part,
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_start_epoch": int(os.environ.get("RUN_START", "0") or 0),
        "run_end_epoch": int(os.environ.get("RUN_END", "0") or 0),
        "complete": complete == "true" and not [x for x in failed if x],
        "failed": [x for x in failed if x],
        "smoke": smoke,
        "caffeinated": os.environ.get("TOLLGATE_CAFFEINATED") == "1",
        "knobs": knobs,
        "pinning": opt("pinning.json"),
        "rig": opt("rig.json"),
        "maxrps_plans": plans,
        "provenance_records": read_jsonl(os.path.join(raw, "provenance.jsonl")),
        "raw_dir": raw,
    }
    with open(os.path.join(raw, "run.json"), "w") as fh:
        json.dump(meta, fh, indent=2)
        fh.write("\n")
    return 0


# ----------------------------------------------------------------------------
# max RPS: align k6 steps with docker stats, watchdog, TCP counters
# ----------------------------------------------------------------------------
def cpuset_size(s):
    if not s:
        return None
    n = 0
    for part in s.split(","):
        a, _, b = part.partition("-")
        n += (int(b) - int(a) + 1) if b else 1
    return n


def group_of(name):
    if "-k6-" in name:
        return "k6"
    if "-gateway-" in name:
        return "gateway"
    if "-demo-" in name:
        return "demo"
    return "other"


def read_stall(path):
    out = {}
    if os.path.exists(path):
        for line in open(path):
            p = line.split()
            if len(p) == 2:
                try:
                    out[int(p[0])] = float(p[1])
                except ValueError:
                    pass
    return out


def read_netwatch(raw):
    rows = []
    for p in sorted(glob.glob(os.path.join(raw, "netwatch-*.jsonl"))):
        rows += read_jsonl(p)
    return sorted(rows, key=lambda r: r["t_ms"])


def k6_warnings(path):
    """[(epoch_ms, message)] for 'Insufficient VUs' style warnings in k6's own log."""
    out = []
    if os.path.exists(path):
        for line in open(path, errors="replace"):
            if "Insufficient VUs" not in line and "dropped" not in line.lower():
                continue
            m = re.search(r'time="([^"]+)"', line)
            if m and "Insufficient VUs" in line:
                try:
                    t = datetime.datetime.fromisoformat(m[1].replace("Z", "+00:00")).timestamp() * 1000
                    out.append(t)
                except ValueError:
                    pass
    return out


def enrich_maxrps(raw, name, d, pinning, ncpu):
    """Add per-step rig measurements and a status label to one max-RPS run."""
    start = d.get("start_epoch_ms")
    stall = read_stall(os.path.join(raw, "stall.log"))
    net = read_netwatch(raw)
    stats = read_jsonl(os.path.join(raw, f"{name}.stats.jsonl"))
    clocks = [r["clock"] for r in stats if "clock" in r]
    samples = [r for r in stats if "cpu" in r]
    offset = med([c["vm_ms"] - c["host_ms"] for c in clocks]) if clocks else 0.0  # vm = host + offset
    insuff = k6_warnings(os.path.join(raw, f"{name}.k6.log"))
    k6c, gwc = cpuset_size((pinning or {}).get("k6_cpuset")), cpuset_size((pinning or {}).get("gateway_cpuset"))
    restc = cpuset_size((pinning or {}).get("rest_cpuset"))
    d["insufficient_vus_warnings"] = len(insuff)
    for s in d["steps"]:
        s["labels"], s["cpu_cores"], s["stall_max_ms"] = [], None, None
        s["upstream_dials_per_s"], s["time_wait_peak"] = None, None
        if not start:
            s["status"] = "pass" if s["passes_k6"] else "fail"
            continue
        a = start + s["hold_start_offset_s"] * 1000
        b = start + s["hold_end_offset_s"] * 1000
        # watchdog: one line per second, keyed by the second it covers
        gaps = [g for sec, g in stall.items() if math.floor(a / 1000) <= sec <= math.floor(b / 1000)]
        s["stall_max_ms"] = max(gaps) if gaps else None
        # TCP counters in the gateway's namespace: dials = ActiveOpens delta over the hold
        inside = [r for r in net if a <= r["t_ms"] <= b]
        if len(inside) >= 2:
            s["upstream_dials_per_s"] = (inside[-1]["active_opens"] - inside[0]["active_opens"]) / max(1e-9, (inside[-1]["t_ms"] - inside[0]["t_ms"]) / 1000)
            s["time_wait_peak"] = max(r["time_wait"] for r in inside)
        # docker stats: samples whose reading (about the second before the host timestamp) lies inside the hold
        per = []
        for r in samples:
            mid_vm = r["host_end_ms"] - 500 + offset
            if a <= mid_vm <= b:
                g = {"k6": 0.0, "gateway": 0.0, "demo": 0.0, "other": 0.0}
                for cname, pct in r["cpu"].items():
                    g[group_of(cname)] += pct / 100
                g["total"] = sum(g.values())
                per.append(g)
        if per:
            s["cpu_cores"] = {k: med([g[k] for g in per]) for k in per[0]}
            s["cpu_samples"] = len(per)
        # in-step Insufficient-VUs warnings (k6 log timestamps have 1 s resolution)
        s["insufficient_vus"] = sum(1 for t in insuff if a - 1000 <= t <= b + 1000) > 0
        c = s["cpu_cores"]
        perturbed = s["stall_max_ms"] is not None and s["stall_max_ms"] > STALL_PERTURBED_MS
        limited = []
        if s["insufficient_vus"]:
            limited.append("k6 insufficient VUs")
        if c and ncpu and c["total"] > CPU_RIG_LIMIT * ncpu:
            limited.append(f"containers use {c['total']:.1f} of {ncpu} vCPUs")
        if c and k6c and c["k6"] > CPUSET_SATURATED * k6c:
            limited.append(f"k6 cpuset saturated ({c['k6']:.1f}/{k6c})")
        if c and restc and (c["demo"] + c["other"]) > CPUSET_SATURATED * restc:
            limited.append(f"backend cpuset saturated ({c['demo'] + c['other']:.1f}/{restc})")
        if perturbed:
            s["labels"].append(f"stall {s['stall_max_ms']:.0f} ms")
        s["labels"] += limited
        s["gateway_cpuset_saturated"] = bool(c and gwc and c["gateway"] > CPUSET_SATURATED * gwc)
        if s["passes_k6"]:
            s["status"] = "pass"
        elif perturbed:
            s["status"] = "rig-perturbed"
        elif limited:
            s["status"] = "rig-limited"
        else:
            s["status"] = "fail"
    return d


def aggregate_maxrps(runs):
    """Combine the repetitions of one target into per-step verdicts and a max."""
    if not runs:
        return None
    n = len(runs)
    need = min(2, n)
    steps = []
    for i, s0 in enumerate(runs[0]["steps"]):
        rs = [r["steps"][i] for r in runs if i < len(r["steps"])]
        st = [x["status"] for x in rs]
        pass_n, fail_n = st.count("pass"), st.count("fail")
        valid = pass_n + fail_n
        if fail_n == 0 and valid >= need:
            verdict = "sustained"
        elif fail_n > 0 and pass_n == 0:
            verdict = "failed"
        elif fail_n > 0:
            verdict = "unstable"
        else:
            verdict = "inconclusive (rig)"
        def col(key):
            return [x.get(key) for x in rs]
        steps.append({
            "target_rps": s0["target_rps"], "statuses": st, "verdict": verdict,
            "achieved_median": med(col("achieved_rps")),
            "p99_median": med(col("p99_ms")), "p99_worst": mx(col("p99_ms")),
            "err_worst": mx(col("error_rate")),
            "cores_median": med([(x.get("cpu_cores") or {}).get("total") for x in rs]),
            "gateway_cores_median": med([(x.get("cpu_cores") or {}).get("gateway") for x in rs]),
            "dials_median": med(col("upstream_dials_per_s")), "tw_peak_max": mx(col("time_wait_peak")),
            "stall_max_ms": mx(col("stall_max_ms")),
            "gateway_saturated": any(x.get("gateway_cpuset_saturated") for x in rs),
            "labels": sorted({l for x in rs for l in x.get("labels", [])}),
        })
    # Highest sustained step below the first gateway failure (a lower failure with a
    # higher pass is non-monotonic: flagged, not accepted).
    first_bad = next((i for i, s in enumerate(steps) if s["verdict"] in ("failed", "unstable")), None)
    limit = first_bad if first_bad is not None else len(steps)
    best = max((i for i in range(limit) if steps[i]["verdict"] == "sustained"), default=None)
    sustained_above = [steps[i]["target_rps"] for i in range(limit, len(steps)) if steps[i]["verdict"] == "sustained"]
    nonmono = ({"failing_step": steps[first_bad]["target_rps"], "sustained_above": sustained_above}
               if first_bad is not None and sustained_above else None)
    top = steps[-1]
    ceiling_not_found = first_bad is None and best is not None and best == len(steps) - 1
    rig_tail = first_bad is None and best is not None and best < len(steps) - 1
    if best is None:
        text = (f"no step sustained (first step {steps[0]['target_rps']} rps: {steps[0]['verdict']})")
    elif ceiling_not_found:
        text = f">= {top['target_rps']} rps (ceiling not found: the top tested step passed)"
    elif first_bad is not None:
        text = f"{steps[best]['target_rps']} rps sustained; the gateway failed at {steps[first_bad]['target_rps']} rps ({steps[first_bad]['verdict']})"
    else:
        text = (f">= {steps[best]['target_rps']} rps; steps above were rig-limited or inconclusive, "
                f"so the ceiling was not found")
    return {"runs": n, "steps": steps, "max_step_rps": steps[best]["target_rps"] if best is not None else None,
            "max_achieved_rps": steps[best]["achieved_median"] if best is not None else None,
            "ceiling_not_found": ceiling_not_found or rig_tail, "non_monotonic": nonmono, "text": text}


# ----------------------------------------------------------------------------
# merge
# ----------------------------------------------------------------------------
def cmd_merge(raw, out, part):
    res = {}
    if part != "all" and os.path.exists(out):
        try:
            res = load(out)
        except ValueError:
            res = {}
    meta = load(os.path.join(raw, "run.json"))
    machine = load(os.path.join(raw, "machine.json")) if os.path.exists(os.path.join(raw, "machine.json")) else {}
    ncpu = ((meta.get("pinning") or {}).get("vm_cpus")) or (machine.get("docker") or {}).get("vm_cpus_limit") or 0
    fresh = {"maxrps": {}, "failover": {}, "overhead": {}}
    sec_keys = {"bench": ("overhead", "maxrps", "limiter"), "chaos": ("hot_reload", "failover")}
    for s in (["bench", "chaos"] if part == "all" else [part]):
        for k in sec_keys[s]:  # replace this section's old data wholesale
            res.pop(k, None)
    for p in sorted(glob.glob(os.path.join(raw, "*.json"))):
        name = os.path.basename(p)[:-5]
        if name in ("machine", "run", "pinning", "rig", "rig-pre") or name.startswith("maxrps-plan"):
            continue
        d = load(p)
        t = d.get("test")
        if t == "failover":
            fresh["failover"].setdefault(d["case"], []).append(d)
        elif t == "maxrps":
            m = re.match(r"maxrps-(.+?)(?:-(accesslog))?-r(\d+)$", name)
            target, tag = m[1], m[2]
            enrich_maxrps(raw, name, d, meta.get("pinning"), ncpu)
            d["run"] = int(m[3])
            fresh["maxrps"].setdefault(target + (f"-{tag}" if tag else ""), []).append(d)
        elif t == "overhead":
            fresh["overhead"][name.replace("overhead", "").lstrip("-") or "access_log_off"] = d
        elif t:
            res[t.replace("-", "_")] = d
    for k, v in fresh.items():
        if v:
            res[k] = v
    for v in res.get("failover", {}).values():
        v.sort(key=lambda r: r.get("run", 0))
    for v in res.get("maxrps", {}).values():
        v.sort(key=lambda r: r.get("run", 0))
    if machine:
        res["machine"] = machine
    secs = res.get("sections", {}) if part != "all" else {}
    for s in (["bench", "chaos"] if part == "all" else [part]):
        sm = dict(meta)
        sm["provenance"] = [r for r in meta["provenance_records"] if r["section"] == s]
        sm.pop("provenance_records", None)
        sm["part_run"] = part
        secs[s] = sm
    res["sections"] = secs
    res["timestamp_utc"] = meta["timestamp_utc"]
    res["generated_by"] = part
    res["smoke"] = any(s["smoke"] for s in secs.values())
    res["knobs"] = meta["knobs"]
    blockers = []
    prov = []
    for s in ("bench", "chaos"):
        sec = secs.get(s)
        if not sec:
            blockers.append(f"section '{s}' is missing")
            continue
        if sec["smoke"]:
            blockers.append(f"section '{s}' is a smoke run")
        if not sec["complete"]:
            blockers.append(f"section '{s}' had failed measurements: {', '.join(sec['failed']) or 'unknown'}")
        if not (sec.get("rig") or {}).get("valid", False):
            blockers.append(f"section '{s}' is invalid (sleep/wake events during the run, or no validity record)")
        if not sec["provenance"]:
            blockers.append(f"section '{s}' has no provenance record")
        prov += sec["provenance"]
    heads = {r["git_head"] for r in prov}
    if len(heads) > 1:
        blockers.append(f"sections come from different commits: {sorted(h[:9] for h in heads)}")
    for img in ("gateway", "controlplane", "demo"):
        ids = {r["images"][img] for r in prov}
        if len(ids) > 1:
            blockers.append(f"{img} image differs between brings-up")
    res["provenance"] = {
        "git_head": sorted(heads)[0] if len(heads) == 1 else None,
        "git_dirty": any(r["git_dirty"] for r in prov),
        "images": {img: (sorted({r["images"][img] for r in prov})[0] if len({r["images"][img] for r in prov}) == 1 else None)
                   for img in ("gateway", "controlplane", "demo")},
        "records": prov,
    }
    res["complete"] = not blockers
    res["complete_blockers"] = blockers
    with open(out, "w") as fh:
        json.dump(res, fh, indent=2)
        fh.write("\n")
    return 0


# ----------------------------------------------------------------------------
# report (RESULTS.md)
# ----------------------------------------------------------------------------
def failover_case(runs):
    def vals(k):
        return [r[k] for r in runs if r.get(k) is not None]
    first = runs[0]
    cool_bound = first["config"] == "passive" or first["mode"] == "fail500"
    return {
        "runs": len(runs), "faults": [r.get("fault_s") for r in runs],
        "failover": vals("failover_time_s"), "failover_null": sum(1 for r in runs if r.get("failover_time_s") is None),
        "failed": [r["failed"] for r in runs], "first_failed": [r.get("failover_failed_requests") for r in runs],
        "max_latency": vals("failover_max_latency_ms"),
        "reprobe": [r["reprobe_failures"] for r in runs], "reprobe_per": vals("reprobe_failures_per_cooloff"),
        "recovery": vals("recovery_s"), "recovery_null": sum(1 for r in runs if r.get("recovery_s") is None),
        "cool_bound": cool_bound,
    }


def rng(v, nd=3, unit=""):
    if not v:
        return "n/a"
    return f"{f(min(v),nd)} / {f(statistics.median(v),nd)} / {f(max(v),nd)}{unit}"


def cmd_report(src, out):
    r = load(src)
    L = []
    w = L.append
    smoke = r.get("smoke")
    w("# Tollgate benchmark results")
    w("")
    w("Generated by `bench/summarize.py report` from `latest.json`; no number here is hand-written.")
    w("")
    if smoke:
        w("> **SMOKE RUN.** Short durations and reduced repetitions, produced only to prove the scripts work. "
          "Do not quote any number below.")
        w("")
    w(f"- **complete:** {'yes' if r['complete'] else 'NO'}")
    for b in r.get("complete_blockers", []):
        w(f"  - blocker: {b}")
    pv = r.get("provenance", {})
    w(f"- **commit:** `{pv.get('git_head') or 'mixed or unknown'}`, tracked files modified: {'YES (dirty)' if pv.get('git_dirty') else 'no'}")
    for img, v in (pv.get("images") or {}).items():
        w(f"- **{img} image id:** `{v or 'differs between brings-up'}`")
    w(f"- **generated:** {r.get('timestamp_utc')} (part run: {r.get('generated_by')})")
    m = r.get("machine", {})
    h, dk = m.get("host", {}), m.get("docker", {})
    w(f"- **host:** {h.get('cpu_model')}, {h.get('cpu_cores_logical')} logical cores, {h.get('ram_bytes', 0) / 2**30:.0f} GiB RAM, macOS {h.get('macos')}")
    w(f"- **Docker VM:** {dk.get('vm_cpus_limit')} vCPUs, {dk.get('vm_memory_bytes_limit', 0) / 2**30:.1f} GiB, {dk.get('operating_system')} {dk.get('server_version')}, kernel {dk.get('kernel')}")
    w(f"- **load generator:** {m.get('k6_image')} ({m.get('k6')}), running in a container on the same VM")
    for s, sec in r.get("sections", {}).items():
        rig = sec.get("rig") or {}
        pin = sec.get("pinning") or {}
        w(f"- **section `{s}`:** run {sec['timestamp_utc']}, {'SMOKE' if sec['smoke'] else 'real'}, "
          f"{'VALID' if rig.get('valid') else 'INVALID'} (sleep/wake events in the run window: {len(rig.get('sleep_wake_events', []))}), "
          f"caffeinate {'on' if sec.get('caffeinated') else 'OFF'}")
        w(f"  - host before the run: swap used {f(rig.get('swap_used_pct'),0)}% of {f(rig.get('swap_total_mb'),0)} MB, free memory {f(rig.get('free_memory_mb'),0)} MB "
          f"(+{f(rig.get('inactive_memory_mb'),0)} MB inactive), power: {rig.get('power_source')}")
        for wn in rig.get("warnings", []):
            w(f"  - **host warning:** {wn}")
        w(f"  - CPU pinning (cpuset): k6 `{pin.get('k6_cpuset')}`, gateway `{pin.get('gateway_cpuset')}`, demos + control plane + Postgres + Consul `{pin.get('rest_cpuset')}`; demo GOMAXPROCS {pin.get('demo_gomaxprocs')}")
        for ev in rig.get("sleep_wake_events", []):
            w(f"  - **sleep/wake event:** {ev['time']} {ev['event']} {ev['detail']}")
        for rec in sec.get("provenance", []):
            w(f"  - brought up `{rec['label']}` at {rec['time_utc']}: commit `{rec['git_head'][:12]}`{' (dirty)' if rec['git_dirty'] else ''}, "
              f"gateway `{rec['images']['gateway'][:19]}`")
    w("")
    w("Everything runs on one laptop: the load generator, gateway, control plane, Postgres, Consul and the demo "
      "backends share one Docker Desktop VM. Numbers are for this rig only.")
    w("")

    # ---- overhead
    if "overhead" in r:
        w("## Gateway overhead (added latency, 1 backend)")
        w("")
        w("What it measures: the same constant-arrival-rate load sent directly to `demo-1` and through the gateway "
          "(`/bare`, no auth, no limit) to the same `demo-1`. Each round is four phases in ABBA order; a round's delta is "
          "gateway minus direct (each side the mean of its two phases). The table gives the median of the per-round deltas "
          "and the min..max over rounds. **Noise floor** = the largest per-round delta seen between two identical direct "
          "paths (A/A rounds), i.e. how much the rig alone moves the number. A delta at or below the floor is "
          "**below the noise floor**: not distinguishable from nothing.")
        w("")
        floor_src = None
        for label, d in r["overhead"].items():
            if d["config"]["aa"]:
                floor_src = overhead_stats(d)
        for label, d in sorted(r["overhead"].items()):
            o = overhead_stats(d)
            c = d["config"]
            title = "access log OFF" if label == "access_log_off" else f"access log ON ({label})"
            w(f"### {title}")
            w("")
            w(f"{c['rate_rps']} rps, {c['phase_duration_s']:.0f} s per phase, {o['rounds_ab']} A/B rounds, {o['rounds_aa']} A/A rounds. "
              f"Phases with dropped iterations: {o['dropped_phases']}; with errors: {o['error_phases']}.")
            w("")
            w("| percentile | direct (ms) | gateway (ms) | delta median (ms) | delta min..max (ms) | A/A noise floor (ms) | verdict |")
            w("|---|---|---|---|---|---|---|")
            for s in ("p50", "p90", "p99"):
                st = o["stats"][s]
                noise = st["noise_floor"] if st["noise_floor"] is not None else (floor_src["stats"][s]["noise_floor"] if floor_src else None)
                src = "" if st["noise_floor"] is not None else " (from the access-log-off A/A)"
                below = noise is not None and st["median"] is not None and abs(st["median"]) <= noise
                if noise is None:
                    verdict = "no noise floor measured"
                elif below:
                    verdict = "**below noise floor**"
                else:
                    verdict = "above noise floor"
                if s == "p99" and not below:
                    verdict += " (p99 from ~%d samples per phase; treat as indicative)" % (c["rate_rps"] * c["phase_duration_s"])
                w(f"| {s} | {f(o['direct_ms'][s],3)} | {f(o['gateway_ms'][s],3)} | {f(st['median'],3)} | {f(st['min'],3)} .. {f(st['max'],3)} | {f(noise,3)}{src} | {verdict} |")
            w("")
        w("p99 is only quoted next to its noise floor; max latency is not reported (a single outlier per phase).")
        w("")

    # ---- max rps
    if "maxrps" in r:
        w("## Max sustainable throughput")
        w("")
        w("What it measures: one ramping-arrival-rate load per run, held at each step; a step **passes** if errors < 0.1 %, "
          "p99 < 50 ms and at least 98 % of the target rate was achieved. Each target ran several times, interleaved in a "
          "randomised order (seeds below). Per step the table shows each repetition's status, then median achieved rate, "
          "median/worst p99, containers' CPU, and gateway upstream dials per second and TIME_WAIT peak (from the gateway's network namespace).")
        w("")
        w("Status labels: `ok` = passed. `FAIL` = failed with a healthy rig: counts against the gateway. "
          f"`rig-limited` = failed while containers used more than {int(CPU_RIG_LIMIT*100)}% of the VM's vCPUs, or the k6 or backend CPU set was "
          "saturated, or k6 reported insufficient VUs: the test rig, not the gateway, is the suspect, and it is **not counted as a gateway failure**. "
          f"`rig-perturbed` = the stall watchdog saw a gap over {STALL_PERTURBED_MS} ms in the step (the whole VM froze): also not counted.")
        w("")
        seen_plans = set()
        for sec_name, sec in r.get("sections", {}).items():
            for plan_name, plan in (sec.get("maxrps_plans") or {}).items():
                line = f"- run order (seed `{plan['seed']}`): " + "; ".join(f"rep {i+1}: {' > '.join(o)}" for i, o in enumerate(plan["order"]))
                if line not in seen_plans:
                    seen_plans.add(line)
                    w(line)
        w("")
        for target, runs in sorted(r["maxrps"].items()):
            ag = aggregate_maxrps(runs)
            w(f"### target `{target}`: {ag['text']}")
            w("")
            if ag["non_monotonic"]:
                nm = ag["non_monotonic"]
                w(f"**Non-monotonic result:** step {nm['failing_step']} rps failed but higher step(s) {nm['sustained_above']} passed. "
                  "The max above is capped below the failing step; treat this target as unreliable until explained.")
                w("")
            if ag["ceiling_not_found"]:
                w("**Ceiling not found:** the quoted number is a lower bound, not a measured maximum.")
                w("")
            w(f"{ag['runs']} run(s). Max sustained step: {ag['max_step_rps']} rps (median achieved {f(ag['max_achieved_rps'],0)} rps).")
            w("")
            w("| step rps | statuses | verdict | achieved (median) | p99 ms median / worst | CPU cores total (gateway) | dials/s | TIME_WAIT peak | max stall ms | notes |")
            w("|---|---|---|---|---|---|---|---|---|---|")
            for s in ag["steps"]:
                sts = " ".join({"pass": "ok", "fail": "FAIL"}.get(x, x) for x in s["statuses"])
                notes = "; ".join(s["labels"])
                if s["gateway_saturated"]:
                    notes = (notes + "; " if notes else "") + "gateway CPU set saturated (gateway-bound)"
                w(f"| {s['target_rps']} | {sts} | {s['verdict']} | {f(s['achieved_median'],0)} | {f(s['p99_median'])} / {f(s['p99_worst'])} | "
                  f"{f(s['cores_median'],1)} ({f(s['gateway_cores_median'],1)}) | {f(s['dials_median'],0)} | {f(s['tw_peak_max'],0)} | {f(s['stall_max_ms'],0)} | {notes} |")
            w("")

    # ---- limiter
    if "limiter" in r:
        d = r["limiter"]
        c, s, mk = d["config"], d["single_key"], d["multi_key"]
        w("## Rate limiter accuracy")
        w("")
        w(f"What it measures: `/limited` allows {c['rate_limit_rps']} req/s sustained with burst {c['burst']} per API key. Each key is offered "
          f"{c['offered_multiple']}x the limit for {c['duration_s']:.0f} s. Expected allowed = burst + rate x window (window = first to last request "
          "of that key). The expected value is only valid if the generator never paused longer than burst/rate (1 s) on that key, so each key is "
          "checked for its largest gap between requests; keys that fail the check are excluded.")
        w("")
        w(f"- single key: offered {s['requests']}, allowed {s['allowed']}, expected {s['expected_allowed']:.1f}, error {f(s['error_pct'])}%, largest gap {f(s['max_gap_ms'],0)} ms ({'generator ok' if s['offered_ok'] else 'GENERATOR LIMITED: not valid'})")
        e = mk["noisy_error_pct"]
        if e:
            w(f"- {len(mk['noisy_keys'])} noisy keys in parallel ({mk['noisy_keys_generator_ok']} valid): error vs expected, min / median / max = "
              f"{f(e['min'])}% / {f(e['median'])}% / {f(e['max'])}%; allowed per key {mk['noisy_allowed']['min']} .. {mk['noisy_allowed']['max']}")
        p = mk["polite_key"]
        w(f"- polite key (below the limit, at the same time): {p['requests']} requests, **{p['rejected']} rejected** "
          f"(isolation {'holds' if mk['polite_isolated'] else 'BROKEN'}; no accuracy figure is meaningful for this key)")
        w(f"- rejections that are not 429 with Retry-After: {d['bad_rejects']}")
        w(f"- k6 dropped iterations (information only): {d.get('k6_dropped_iterations')}")
        w("")

    # ---- hot reload
    if "hot_reload" in r:
        d = r["hot_reload"]
        c = d["config"]
        w("## Hot reload under load")
        w("")
        w(f"What it measures: {d['changes_done']} config changes ({d['puts']} route PUTs, {d['rollbacks']} rollbacks) through the control plane, one every "
          f"{c['period_s']} s across the whole {c['load_seconds']:.0f} s load window, while {c['load_rps']} rps of fast requests and {c['slow_load_rps']} rps of "
          f"requests to a backend that takes {c['slow_backend_delay_s']} s per response flow through the gateway. The slow requests are in flight across every swap.")
        w("")
        w(f"- requests: {d['requests_total']:.0f}; non-2xx: {d['non_2xx']:.0f}; connection errors: {d['connection_errors']:.0f}; k6 dropped iterations: {d['dropped_iterations']:.0f}")
        sp = d["spanning_per_change"]
        w(f"- **requests that spanned at least one config change: {d['spanning_requests']}** ({d['spanning_requests_slow_route']} slow, {d['spanning_requests_fast_route']} fast), "
          f"of which failed: {d['spanning_requests_failed']}. Per change: min {sp['min']}, median {sp['median']}, max {sp['max']}. Definition: {d['span_definition']}.")
        for key, label in (("propagation_ms", "from the admin call returning: a LOWER bound"), ("propagation_from_send_ms", "from the admin call being sent: an UPPER bound")):
            pp = d.get(key)
            if pp:
                w(f"- propagation {label}: p50 {f(pp['p50'],1)}, p90 {f(pp['p90'],1)}, p99 {f(pp['p99'],1)}, max {f(pp['max'],1)} ms (1 ms clock, includes about one request round trip)")
        a = d.get("admin_call_ms")
        if a:
            w(f"- the admin call itself took p50 {f(a['p50'],1)}, p99 {f(a['p99'],1)}, max {f(a['max'],1)} ms")
        hh = d.get("host", {})
        w(f"- gateway restarted: {hh.get('gateway_restarted')}; applied version {hh.get('applied_version')} vs head {hh.get('head_version')}")
        w("")

    # ---- failover
    if "failover" in r:
        w("## Failover")
        w("")
        w("What it measures: 200 rps through the gateway to three instances; at T0 `demo-1` is killed (SIGKILL) or made to answer 500 while staying "
          "\"healthy\"; it is restored after a **randomised** fault of 13-27 s (value recorded per run), so the restore lands at a different phase of the "
          "10 s passive cool-off each time. All times are on the Docker VM clock, offsets from the real T0 (no slack applied to the clock). "
          "**Failover time** = T0 to the client-visible end (request start + its latency) of the first failure cluster; `n/a` if no failure cluster began within "
          "2 s of the fault. **Back in rotation** = restore until `demo-1` serves a 200 again. All columns are min / median / max over the runs.")
        w("")
        w("Cases marked *cool-off bound*: the gateway only retries an ejected backend after the 10 s passive cool-off, so \"back in rotation\" is "
          "bounded by 10 s plus whatever phase the restore lands in; the spread across runs is that phase, not gateway speed.")
        w("")
        w("| case | runs | fault s | failover s (min/med/max) | failed in first cluster | max latency ms | re-probe failures | re-probe per cool-off | back in rotation s (min/med/max) | note |")
        w("|---|---|---|---|---|---|---|---|---|---|")
        for case, runs in sorted(r["failover"].items()):
            a = failover_case(runs)
            note = "cool-off bound (<= 10 s)" if a["cool_bound"] else "active health / Consul bound"
            if a["failover_null"]:
                note += f"; failover n/a in {a['failover_null']} run(s)"
            w(f"| {case} | {a['runs']} | {rng(a['faults'],1)} | {rng(a['failover'])} | {rng(a['first_failed'],0)} | {rng(a['max_latency'],1)} | "
              f"{rng(a['reprobe'],0)} | {rng(a['reprobe_per'],2)} | {rng(a['recovery'])} | {note} |")
        w("")
        w("**Re-probe failures are user-visible.** While a backend keeps failing but is still considered healthy (500 mode; or passive-only mode "
          "indefinitely), every time the 10 s cool-off expires the gateway sends one real client request to it, and that request fails. "
          "That is about one failed client request per cool-off for as long as the backend stays broken.")
        w("")
    with open(out, "w") as fh:
        fh.write("\n".join(L) + "\n")
    return 0


if __name__ == "__main__":
    a = sys.argv
    if len(a) == 3 and a[1] == "show":
        sys.exit(cmd_show(a[2]))
    if len(a) == 2 and a[1] == "is-smoke":
        print(1 if knobs_and_smoke()[1] else 0)
        sys.exit(0)
    if len(a) == 5 and a[1] == "merge":
        sys.exit(cmd_merge(a[2], a[3], a[4]))
    if len(a) == 5 and a[1] == "meta":
        sys.exit(cmd_meta(a[2], a[3], a[4]))
    if len(a) == 4 and a[1] == "report":
        sys.exit(cmd_report(a[2], a[3]))
    print(__doc__, file=sys.stderr)
    sys.exit(2)
