#!/usr/bin/env bash
# One command for the whole Phase 8 measurement run.
#   bench/run-all.sh bench    # overhead, max RPS, limiter accuracy  (~15 min)
#   bench/run-all.sh chaos    # failover (6 cases) and hot reload    (~9 min)
#   bench/run-all.sh all      # both, each on a freshly built stack  (~25 min)
# Each part: build + start the bench stack (--wait), run its scripts, tear the
# stack down with `down -v` (also on failure or Ctrl-C).
#
# Raw output:  bench/results/raw/<UTC timestamp>/   (gitignored)
# Combined:    bench/results/latest.json  (`all` rebuilds it from this run only; `bench`/`chaos`
#              overlay their own section; it records timestamp, commit, knobs, smoke and complete flags)
# Knobs (all optional, see bench/README.md): RATE DURATION WARMUP ROUNDS STEPS
#   STEP_DURATION P99_MS OVER CHANGES RELOAD_GAP CONFIGS MODES PRE FAULT REC
#   SKIP_ACCESS_LOG=1 (skip the access-log-on variants)  KEEP_STACK=1 (leave the stack up)
# Exit status is non-zero if any measurement could not be taken (the remaining
# ones still run, and the summary lists what failed).
set -uo pipefail

part="${1:-all}"
case "$part" in bench|chaos|all) ;; *) echo "usage: $0 bench|chaos|all" >&2; exit 2 ;; esac

export OUT_DIR="${OUT_DIR:-$(cd "$(dirname "$0")" && pwd)/results/raw/$(date -u +%Y%m%dT%H%M%SZ)}"
. "$(dirname "$0")/lib.sh"   # creates OUT_DIR

failed=()
down() {
  kill_stray_k6
  if [ "${KEEP_STACK:-0}" != "1" ]; then "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; fi
}
trap 'down' EXIT
trap 'log "interrupted"; exit 130' INT TERM

step() { # step NAME CMD...: run, remember failures, keep going
  local name="$1"; shift
  log ">>> $name"
  "$@" || { log "FAILED: $name"; failed+=("$name"); }
}

machine_info() {
  local gw_img
  gw_img="$(docker image inspect -f '{{.Id}}' tollgate/gateway:dev 2>/dev/null || echo unknown)"
  py - "$OUT_DIR/machine.json" "$gw_img" <<'PY'
import json, platform, subprocess, sys

def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30).stdout.strip()
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
    "git_commit": sh(f"cd '{sys.argv[1].rsplit('/bench/', 1)[0]}' && git rev-parse HEAD 2>/dev/null") or "unknown (not a git checkout)",
    "git_dirty": bool(sh(f"cd '{sys.argv[1].rsplit('/bench/', 1)[0]}' && git status --porcelain 2>/dev/null")),
    "gateway_image_id": sys.argv[2],
    "k6": sh("docker run --rm --entrypoint k6 grafana/k6:latest version"),
}
json.dump(out, open(sys.argv[1], "w"), indent=2)
PY
}

bring_up() { # bring_up [ACCESS_LOG]
  log "building and starting the bench stack (ACCESS_LOG=${1:-false})"
  ACCESS_LOG="${1:-false}" "${COMPOSE[@]}" up -d --build --wait >"$OUT_DIR/compose-up.log" 2>&1 \
    || { tail -30 "$OUT_DIR/compose-up.log" >&2; return 1; }
  wait_all_instances /bare3 60
}

settle() { sleep "${SETTLE:-5}"; }

run_bench() {
  bring_up false || { failed+=("stack up (bench)"); return; }
  step "overhead (access log off)" "$BENCH_DIR/run-k6.sh" overhead; settle
  for t in direct gateway gateway3; do
    step "maxrps $t" "$BENCH_DIR/run-k6.sh" maxrps "$t"; settle
  done
  step "limiter" "$BENCH_DIR/run-k6.sh" limiter; settle
  if [ "${SKIP_ACCESS_LOG:-0}" != "1" ]; then
    # Recreates only the gateway container, with the access log on.
    bring_up true || { failed+=("stack up (access log on)"); return; }
    LABEL=accesslog step "overhead (access log on)" "$BENCH_DIR/run-k6.sh" overhead; settle
    LABEL=gateway-accesslog step "maxrps gateway (access log on)" "$BENCH_DIR/run-k6.sh" maxrps gateway
  fi
}

run_chaos() {
  bring_up false || { failed+=("stack up (chaos)"); return; }
  step "hot reload" "$BENCH_DIR/chaos/hot-reload.sh"; settle
  step "failover" "$BENCH_DIR/chaos/failover.sh"
}

log "raw results: $OUT_DIR"
machine_info || log "could not collect machine info"

[ "$part" = bench ] || [ "$part" = all ] && run_bench
if [ "$part" = all ]; then "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; fi
[ "$part" = chaos ] || [ "$part" = all ] && run_chaos

complete=true; [ ${#failed[@]} -eq 0 ] || complete=false
# "all" rebuilds latest.json from this run only; bench/chaos overlay their own section.
py "$BENCH_DIR/summarize.py" meta "$OUT_DIR" "$part" "$complete" \
  && py "$BENCH_DIR/summarize.py" merge "$OUT_DIR" "$BENCH_DIR/results/latest.json" "$part" \
  && log "wrote bench/results/latest.json (part=$part complete=$complete)"

echo
if [ ${#failed[@]} -gt 0 ]; then
  echo "FAILED measurements:"; printf '  - %s\n' "${failed[@]}"
  exit 1
fi
echo "All measurements taken. Raw: $OUT_DIR   Combined: $BENCH_DIR/results/latest.json"
