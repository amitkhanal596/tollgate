# Shared helpers for the bench and chaos scripts. Source it; do not run it.
# Compatible with macOS bash 3.2.

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$BENCH_DIR/.." && pwd)"

# Pinned: the measuring tool must not change between runs (checked with
# `docker manifest inspect grafana/k6:2.4.0`).
K6_IMAGE="${K6_IMAGE:-grafana/k6:2.4.0}"
HELPER_IMAGE="tollgate-bench/helper:dev"       # bench/stall: stall watchdog + TCP counters
WATCHDOG_NAME="tollgate-bench-stall"
NETWATCH_NAME="tollgate-bench-netwatch"

# The bench stack has its own compose project, so `down -v` here can never
# delete the demo stack's volumes (pgdata, lkg) and the two can coexist.
PROJECT="tollgate-bench"
NETWORK="${PROJECT}_default"
COMPOSE=(docker compose -p "$PROJECT"
  -f "$ROOT/deploy/compose/docker-compose.yml"
  -f "$ROOT/deploy/compose/docker-compose.bench.yml")
ADMIN_TOKEN="$(sed -n 's/^TOLLGATE_ADMIN_TOKEN=//p' "$ROOT/deploy/compose/demo.env")"
CP_URL="http://controlplane:9000"   # reached from inside the compose network (nothing is published)
GW_URL="http://gateway:8080"

# OUT_DIR: where raw results go. run-all.sh sets it; standalone runs get their own dir.
if [ -z "${OUT_DIR:-}" ]; then
  OUT_DIR="$BENCH_DIR/results/raw/manual-$(date -u +%Y%m%dT%H%M%SZ)"
fi
out_dir_init() { mkdir -p "$OUT_DIR"; }   # scripts call it once they know they will write

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# --- CPU isolation -----------------------------------------------------------
# Read the Docker VM's CPU count and split it into three disjoint sets:
# k6 | gateway | everything else (demo backends, control plane, Postgres, Consul).
# Exports BENCH_NCPU, BENCH_CPUSET_{K6,GW,REST}, BENCH_GOMAXPROCS_REST; the
# compose override reads the last three. With fewer than 4 CPUs nothing is pinned.
cpu_range() { if [ "$1" = "$2" ]; then echo "$1"; else echo "$1-$2"; fi; }
compute_pinning() {
  local n rest k6 gw
  n="$(docker info --format '{{.NCPU}}' 2>/dev/null || echo 0)"
  [ -n "$n" ] || n=0
  export BENCH_NCPU="$n"
  if [ "$n" -lt 4 ]; then
    export BENCH_CPUSET_K6="" BENCH_CPUSET_GW="" BENCH_CPUSET_REST="" BENCH_GOMAXPROCS_REST=""
    return 0
  fi
  rest=$((n / 4)); [ "$rest" -ge 1 ] || rest=1
  k6=$(((n - rest + 1) / 2))
  gw=$((n - rest - k6))
  export BENCH_CPUSET_K6="$(cpu_range 0 $((k6 - 1)))"
  export BENCH_CPUSET_GW="$(cpu_range "$k6" $((k6 + gw - 1)))"
  export BENCH_CPUSET_REST="$(cpu_range $((k6 + gw)) $((n - 1)))"
  export BENCH_GOMAXPROCS_REST="$rest"
  return 0
}
compute_pinning

pinning_json() {
  printf '{"vm_cpus":%s,"k6_cpuset":"%s","gateway_cpuset":"%s","rest_cpuset":"%s","demo_gomaxprocs":"%s"}' \
    "${BENCH_NCPU:-0}" "${BENCH_CPUSET_K6:-}" "${BENCH_CPUSET_GW:-}" "${BENCH_CPUSET_REST:-}" "${BENCH_GOMAXPROCS_REST:-}"
}

require_stack() {
  docker network inspect "$NETWORK" >/dev/null 2>&1 || die "compose network $NETWORK not found; run 'make bench-up' first"
  local id
  id="$("${COMPOSE[@]}" ps -q gateway 2>/dev/null)"
  [ -n "$id" ] || die "bench stack is not up; run 'make bench-up' first"
}

# k6_run SCRIPT [KEY=VALUE ...] -- [extra k6 args]
# Runs k6 in a container on the compose network, pinned to its own CPUs.
# KEY=VALUE pairs become environment variables (read as __ENV.KEY in the script).
# K6_LOG=file also keeps k6's own log output (warnings such as "Insufficient VUs").
k6_run() {
  local script="$1"; shift
  local envs=() extra=() seen_sep=0 a pin=()
  for a in "$@"; do
    if [ "$a" = "--" ]; then seen_sep=1; continue; fi
    if [ $seen_sep -eq 1 ]; then extra+=("$a"); else envs+=(-e "$a"); fi
  done
  [ -z "${BENCH_CPUSET_K6:-}" ] || pin=(--cpuset-cpus "$BENCH_CPUSET_K6")
  # Named so cleanup can find a stray one. --user 0 so it can write to the bind-mounted results dir.
  local cmd=(docker run --rm --name "tollgate-bench-k6-$$-$RANDOM" --user 0 --network "$NETWORK"
    ${pin[@]+"${pin[@]}"}
    -v "$BENCH_DIR/k6:/scripts:ro" -v "$OUT_DIR:/out"
    -e K6_NO_USAGE_REPORT=true -e ADMIN_TOKEN="$ADMIN_TOKEN" ${envs[@]+"${envs[@]}"}
    "$K6_IMAGE" run --quiet "/scripts/$script" ${extra[@]+"${extra[@]}"})
  if [ -n "${K6_LOG:-}" ]; then
    "${cmd[@]}" 2> >(tee "$K6_LOG" >&2)
  else
    "${cmd[@]}"
  fi
}

kill_stray_k6() {
  local ids
  ids="$(docker ps -q --filter 'name=tollgate-bench-k6-')"
  [ -z "$ids" ] || docker kill $ids >/dev/null 2>&1 || true
}

# cid SERVICE -> container id of a compose service
cid() { "${COMPOSE[@]}" ps -aq "$1"; }

# Run curl inside the consul container (it has curl and sits on the compose
# network), so the control plane, the gateway and the demo instances'
# /admin/fail are reachable without publishing any port.
net_curl() { docker exec "$(cid consul)" curl -s -m 5 "$@"; }
cp_get() { net_curl -H "Authorization: Bearer $ADMIN_TOKEN" "$CP_URL$1"; }

reset_faults() {
  local n
  for n in 1 2 3; do net_curl -X POST "http://demo-$n:8080/admin/fail?mode=off" >/dev/null 2>&1 || true; done
}

# wait_all_instances PATH [SECONDS]: until 40 consecutive requests through the
# gateway have all succeeded and covered demo-1..3 (nothing ejected or missing).
wait_all_instances() {
  local path="$1" deadline=$((SECONDS + ${2:-90})) out c
  c="$(cid consul)"
  while [ $SECONDS -lt $deadline ]; do
    out="$(docker exec "$c" sh -c '
      i=0
      while [ $i -lt 40 ]; do
        b=$(curl -s -m 2 -f "'"$GW_URL$path"'/x") || exit 1
        echo "$b" | sed -n "s/.*\"instance\":\"\([^\"]*\)\".*/\1/p"
        i=$((i+1))
      done' 2>/dev/null)" && [ "$(printf '%s\n' "$out" | sort -u | grep -c .)" -ge 3 ] && return 0
    sleep 2
  done
  return 1
}

# Run a python helper in isolated mode (no cwd/user-site imports).
py() { python3 -I "$@"; }

# --- measuring helpers (bench/stall) -----------------------------------------
build_helper() {
  docker build -q -t "$HELPER_IMAGE" "$BENCH_DIR/stall" >/dev/null || die "could not build the helper image"
}

# Stall watchdog: one container for the whole run, on no network, logging
# "<epoch_s> <max_gap_ms>" once a second on the VM clock.
start_watchdog() {
  docker rm -f "$WATCHDOG_NAME" >/dev/null 2>&1 || true
  docker run -d --name "$WATCHDOG_NAME" --network none "$HELPER_IMAGE" stall >/dev/null || die "could not start the stall watchdog"
}
stop_watchdog() { # stop_watchdog OUTFILE
  docker logs "$WATCHDOG_NAME" > "$1" 2>/dev/null || true
  docker rm -f "$WATCHDOG_NAME" >/dev/null 2>&1 || true
}
watchdog_running() { [ "$(docker inspect -f '{{.State.Running}}' "$WATCHDOG_NAME" 2>/dev/null)" = "true" ]; }

# TCP counters in the gateway's network namespace (the gateway image is
# distroless, so a helper container joins its namespace instead).
start_netwatch() {
  docker rm -f "$NETWATCH_NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NETWATCH_NAME" --network "container:$(cid gateway)" "$HELPER_IMAGE" net >/dev/null \
    || die "could not start the TCP counter helper"
}
stop_netwatch() { # stop_netwatch OUTFILE
  docker logs "$NETWATCH_NAME" > "$1" 2>/dev/null || true
  docker rm -f "$NETWATCH_NAME" >/dev/null 2>&1 || true
}

# Remove helpers and any stray k6, whatever state they are in.
cleanup_helpers() {
  kill_stray_k6
  docker rm -f "$NETWATCH_NAME" >/dev/null 2>&1 || true
  docker rm -f "$WATCHDOG_NAME" >/dev/null 2>&1 || true
}

# Start/stop the `docker stats` sampler (bench/sample_stats.py) around a measurement.
SAMPLER_PID=""
start_sampler() { # start_sampler OUTFILE
  python3 -I "$BENCH_DIR/sample_stats.py" "$1" "$WATCHDOG_NAME" &
  SAMPLER_PID=$!
}
stop_sampler() {
  [ -z "$SAMPLER_PID" ] || { kill -TERM "$SAMPLER_PID" 2>/dev/null; wait "$SAMPLER_PID" 2>/dev/null || true; }
  SAMPLER_PID=""
}

# --- provenance ----------------------------------------------------------------
# record_provenance SECTION LABEL: append what is actually running right now
# (call it right AFTER bring_up): git HEAD, whether TRACKED files are modified
# (untracked files such as results do not count), and the IDs of the images the
# running gateway/controlplane/demo containers were created from.
record_provenance() {
  local section="$1" label="$2" g c d
  g="$(docker inspect -f '{{.Image}}' "$(cid gateway)" 2>/dev/null || echo unknown)"
  c="$(docker inspect -f '{{.Image}}' "$(cid controlplane)" 2>/dev/null || echo unknown)"
  d="$(docker inspect -f '{{.Image}}' "$(cid demo-1)" 2>/dev/null || echo unknown)"
  py - "$OUT_DIR/provenance.jsonl" "$ROOT" "$section" "$label" "$g" "$c" "$d" <<'PY'
import json, subprocess, sys, datetime
out, root, section, label, g, c, d = sys.argv[1:8]
def git(*a):
    try:
        return subprocess.run(["git", "-C", root, *a], capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception:
        return ""
rec = {
    "section": section, "label": label,
    "time_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "git_head": git("rev-parse", "HEAD") or "unknown",
    # tracked files only: results and other untracked files must not make the flag useless
    "git_dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
    "images": {"gateway": g, "controlplane": c, "demo": d},
}
with open(out, "a") as fh:
    fh.write(json.dumps(rec) + "\n")
PY
}

# --- stack lifecycle -----------------------------------------------------------
# bring_up SECTION LABEL [ACCESS_LOG]: clean slate for the bench project only
# (down -v), build, start, wait, record provenance, restart the TCP counter helper.
bring_up() {
  local section="$1" label="$2" access="${3:-false}"
  log "bench stack ($PROJECT): clean start, building and starting ($label, access log=$access)"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  compute_pinning
  ACCESS_LOG="$access" "${COMPOSE[@]}" up -d --build --wait >"$OUT_DIR/compose-up-$label.log" 2>&1 \
    || { tail -30 "$OUT_DIR/compose-up-$label.log" >&2; return 1; }
  wait_all_instances /bare3 60 || { log "gateway never routed to all 3 instances"; return 1; }
  record_provenance "$section" "$label"
  return 0
}

# Gateway-only restart with a different access-log setting (same images).
bring_up_gateway_log() { # bring_up_gateway_log SECTION LABEL true|false
  log "recreating the gateway with access log=$3"
  ACCESS_LOG="$3" "${COMPOSE[@]}" up -d --wait --no-deps --force-recreate gateway >>"$OUT_DIR/compose-up-$2.log" 2>&1 || return 1
  wait_all_instances /bare3 60 || return 1
  record_provenance "$1" "$2"
}

bring_down() {
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
