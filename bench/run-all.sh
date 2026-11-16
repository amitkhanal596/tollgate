#!/usr/bin/env bash
# One command for the whole Phase 8 measurement run.
#   bench/run-all.sh bench    # overhead (+ noise floor), max RPS x3, limiter   (~26 min)
#   bench/run-all.sh chaos    # hot reload and failover (6 cases x 3 runs)      (~18 min)
#   bench/run-all.sh all      # both, each on a freshly built stack             (~45 min)
#   SMOKE=1 bench/run-all.sh all   # short durations, results in bench/results/smoke/ (~5 min)
#
# Each part: clean `down -v` of the bench project (tollgate-bench; the demo stack
# is never touched), build, start (--wait), record provenance, run its scripts,
# tear down. A stall watchdog container and host checks (swap, memory, power,
# pmset sleep/wake log) cover the whole run. On macOS everything runs under
# `caffeinate -dimsu` so the Mac cannot idle-sleep mid-run.
#
# Raw output:  bench/results/raw/<UTC timestamp>/           (gitignored)
# Combined:    bench/results/latest.json and RESULTS.md (generated from it by
#              summarize.py; `all` rebuilds both from this run only, `bench`/`chaos`
#              overlay their own section).
# Any knob set to a non-default value, or SMOKE=1, makes it a smoke run: results go
# to bench/results/smoke/ instead, flagged as smoke, and a real run is never overwritten.
# Knobs (see bench/README.md): RATE DURATION WARMUP ROUNDS AA STEPS STEP_DURATION RAMP REPS TARGETS
#   SETTLE P99_MS LIMITER_DURATION OVER NOISY CHANGES SLOW_RATE PERIOD ROLLBACK_EVERY PRE REC REPEATS
#   FAULT FAULT_MIN FAULT_MAX CONFIGS MODES SKIP_ACCESS_LOG=1 ACCESSLOG_MAXRPS=1 SEED KEEP_STACK=1
# Exit status is non-zero if any measurement could not be taken (the remaining
# ones still run, and the summary lists what failed).
set -uo pipefail

part="${1:-all}"
case "$part" in bench|chaos|all) ;; *) echo "usage: $0 bench|chaos|all" >&2; exit 2 ;; esac

if [ "$(uname)" = Darwin ] && [ -z "${TOLLGATE_CAFFEINATED:-}" ] && command -v caffeinate >/dev/null 2>&1; then
  export TOLLGATE_CAFFEINATED=1
  exec caffeinate -dimsu "$0" "$@"
fi

HERE="$(cd "$(dirname "$0")" && pwd)"

if [ "${SMOKE:-0}" = 1 ]; then # short defaults; anything already set by the caller wins
  : "${RATE:=200}" "${DURATION:=3s}" "${WARMUP:=2s}" "${ROUNDS:=1}" "${STEPS:=1000,2000}" "${STEP_DURATION:=3}"
  : "${REPS:=1}" "${SETTLE:=2}" "${LIMITER_DURATION:=5}" "${CHANGES:=10}" "${PERIOD:=0.4}" "${SLOW_RATE:=20}"
  : "${PRE:=3}" "${REC:=12}" "${REPEATS:=1}" "${FAULT:=12}" "${CONFIGS:=passive}"
  export RATE DURATION WARMUP ROUNDS STEPS STEP_DURATION REPS SETTLE LIMITER_DURATION CHANGES PERIOD SLOW_RATE PRE REC REPEATS FAULT CONFIGS
fi
is_smoke="$(python3 -I "$HERE/summarize.py" is-smoke)"
if [ "$is_smoke" = 1 ]; then results="$HERE/results/smoke"; else results="$HERE/results"; fi
export OUT_DIR="${OUT_DIR:-$results/raw/$(date -u +%Y%m%dT%H%M%SZ)}"
. "$HERE/lib.sh"
out_dir_init

failed=()
cleanup() {
  cleanup_helpers
  if [ "${KEEP_STACK:-0}" != "1" ]; then bring_down; fi
}
trap 'cleanup' EXIT
trap 'log "interrupted"; exit 130' INT TERM

step() { # step NAME CMD...: run, remember failures, keep going
  local name="$1"; shift
  log ">>> $name"
  "$@" || { log "FAILED: $name"; failed+=("$name"); }
}

machine_info() {
  py - "$OUT_DIR/machine.json" "$K6_IMAGE" <<'PY'
import json, platform, subprocess, sys

def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60).stdout.strip()
    except Exception:
        return ""

info = json.loads(sh("docker info --format '{{json .}}'") or "{}")
mem = sh("sysctl -n hw.memsize") or sh("awk '/MemTotal/ {print $2*1024}' /proc/meminfo")
out = {
    "timestamp_utc": sh("date -u +%Y-%m-%dT%H:%M:%SZ"),
    "host": {
        "os": f"{platform.system()} {platform.release()} {platform.machine()}",
        "macos": sh("sw_vers -productVersion"),
        "cpu_model": sh("sysctl -n machdep.cpu.brand_string") or sh("grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"),
        "cpu_cores_logical": int(sh("sysctl -n hw.ncpu") or sh("nproc") or 0),
        "ram_bytes": int(mem or 0),
    },
    "docker": {
        "client_version": sh("docker version --format '{{.Client.Version}}'"),
        "server_version": info.get("ServerVersion"),
        "operating_system": info.get("OperatingSystem"),
        "kernel": info.get("KernelVersion"),
        "vm_cpus_limit": info.get("NCPU"),       # Docker Desktop "CPUs" setting
        "vm_memory_bytes_limit": info.get("MemTotal"),  # Docker Desktop "Memory" setting
        "storage_driver": info.get("Driver"),
    },
    "go_version": sh("go version"),
    "k6_image": sys.argv[2],
    "k6": sh(f"docker run --rm --entrypoint k6 {sys.argv[2]} version"),
}
json.dump(out, open(sys.argv[1], "w"), indent=2)
PY
}

settle() { sleep "${SETTLE:-8}"; }

run_bench() {
  bring_up bench bench false || { failed+=("stack up (bench)"); return; }
  AA="${AA:-1}" step "overhead + noise floor (access log off)" "$BENCH_DIR/run-k6.sh" overhead; settle
  step "maxrps (direct, gateway, gateway3 x repeats)" "$BENCH_DIR/maxrps.sh"; settle
  step "limiter" "$BENCH_DIR/run-k6.sh" limiter; settle
  if [ "${SKIP_ACCESS_LOG:-0}" != "1" ]; then
    # Recreates only the gateway container, with the access log on (same images).
    if bring_up_gateway_log bench accesslog true; then
      LABEL=accesslog AA=0 step "overhead (access log on)" "$BENCH_DIR/run-k6.sh" overhead; settle
      if [ "${ACCESSLOG_MAXRPS:-0}" = 1 ]; then
        TAG=accesslog TARGETS=gateway step "maxrps gateway (access log on)" "$BENCH_DIR/maxrps.sh"
      fi
    else
      failed+=("stack up (access log on)")
    fi
  fi
}

run_chaos() {
  bring_up chaos chaos false || { failed+=("stack up (chaos)"); return; }
  step "hot reload" "$BENCH_DIR/chaos/hot-reload.sh"; settle
  step "failover" "$BENCH_DIR/chaos/failover.sh"
}

log "raw results: $OUT_DIR   smoke=$is_smoke   pinning: $(pinning_json)"
pinning_json > "$OUT_DIR/pinning.json"
py "$BENCH_DIR/rigcheck.py" pre "$OUT_DIR/rig-pre.json" || log "could not run the host checks"
machine_info || log "could not collect machine info"
build_helper
start_watchdog
run_start="$(date +%s)"

{ [ "$part" = bench ] || [ "$part" = all ]; } && run_bench
if [ "$part" = all ]; then bring_down; fi
{ [ "$part" = chaos ] || [ "$part" = all ]; } && run_chaos

run_end="$(date +%s)"
stop_watchdog "$OUT_DIR/stall.log"
py "$BENCH_DIR/rigcheck.py" post "$OUT_DIR/rig-pre.json" "$run_start" "$run_end" "$OUT_DIR/rig.json" || log "could not read the pmset log"
printf '%s\n' ${failed[@]+"${failed[@]}"} > "$OUT_DIR/failed.txt"

complete=true; [ ${#failed[@]} -eq 0 ] || complete=false
# "all" rebuilds latest.json from this run only; bench/chaos overlay their own section.
RUN_START="$run_start" RUN_END="$run_end" py "$BENCH_DIR/summarize.py" meta "$OUT_DIR" "$part" "$complete" \
  && py "$BENCH_DIR/summarize.py" merge "$OUT_DIR" "$results/latest.json" "$part" \
  && py "$BENCH_DIR/summarize.py" report "$results/latest.json" "$results/RESULTS.md" \
  && log "wrote $results/latest.json and RESULTS.md (part=$part)"

echo
if [ ${#failed[@]} -gt 0 ]; then
  echo "FAILED measurements:"; printf '  - %s\n' "${failed[@]}"
  exit 1
fi
echo "All measurements taken. Raw: $OUT_DIR   Results: $results/RESULTS.md"
