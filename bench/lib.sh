# Shared helpers for the bench and chaos scripts. Source it; do not run it.
# Compatible with macOS bash 3.2.

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$BENCH_DIR/.." && pwd)"
K6_IMAGE="${K6_IMAGE:-grafana/k6:latest}"
NETWORK="tollgate_default"          # compose project "tollgate" + default network
COMPOSE=(docker compose
  -f "$ROOT/deploy/compose/docker-compose.yml"
  -f "$ROOT/deploy/compose/docker-compose.bench.yml")
ADMIN_TOKEN="$(sed -n 's/^TOLLGATE_ADMIN_TOKEN=//p' "$ROOT/deploy/compose/demo.env")"
CP_HOST_URL="http://127.0.0.1:9000"   # control plane, published on loopback by the demo stack
GW_HOST_URL="http://127.0.0.1:8080"

# OUT_DIR: where raw results go. run-all.sh sets it; standalone runs get their own dir.
if [ -z "${OUT_DIR:-}" ]; then
  OUT_DIR="$BENCH_DIR/results/raw/manual-$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "$OUT_DIR"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

require_stack() {
  docker network inspect "$NETWORK" >/dev/null 2>&1 || die "compose network $NETWORK not found; run 'make bench-up' first"
  local id
  id="$("${COMPOSE[@]}" ps -q gateway 2>/dev/null)"
  [ -n "$id" ] || die "bench stack is not up; run 'make bench-up' first"
}

# k6_run SCRIPT [KEY=VALUE ...] -- [extra k6 args]
# Runs k6 in a container on the compose network. KEY=VALUE pairs become
# environment variables (read as __ENV.KEY in the script).
k6_run() {
  local script="$1"; shift
  local envs=() extra=() seen_sep=0 a
  for a in "$@"; do
    if [ "$a" = "--" ]; then seen_sep=1; continue; fi
    if [ $seen_sep -eq 1 ]; then extra+=("$a"); else envs+=(-e "$a"); fi
  done
  # Named so cleanup can find a stray one. --user 0 so it can write to the bind-mounted results dir.
  docker run --rm --name "tollgate-k6-$$-$RANDOM" --user 0 --network "$NETWORK" \
    -v "$BENCH_DIR/k6:/scripts:ro" -v "$OUT_DIR:/out" \
    -e K6_NO_USAGE_REPORT=true -e ADMIN_TOKEN="$ADMIN_TOKEN" ${envs[@]+"${envs[@]}"} \
    "$K6_IMAGE" run --quiet "/scripts/$script" ${extra[@]+"${extra[@]}"}
}

kill_stray_k6() {
  local ids
  ids="$(docker ps -q --filter 'name=tollgate-k6-')"
  [ -z "$ids" ] || docker kill $ids >/dev/null 2>&1 || true
}

# cid SERVICE -> container id of a compose service
cid() { "${COMPOSE[@]}" ps -aq "$1"; }

# Run curl inside the consul container (it has curl and is on the compose
# network), so demo instances' /admin/fail is reachable without publishing it.
net_curl() { docker exec "$(cid consul)" curl -s -m 5 "$@"; }

reset_faults() {
  local n
  for n in 1 2 3; do net_curl -X POST "http://demo-$n:8080/admin/fail?mode=off" >/dev/null 2>&1 || true; done
}

# wait_all_instances PATH: until 40 consecutive requests through the gateway
# have all succeeded and covered demo-1..3 (i.e. nothing is ejected or missing).
wait_all_instances() {
  local path="$1" deadline=$((SECONDS + ${2:-90})) seen i body
  while [ $SECONDS -lt $deadline ]; do
    seen=""
    for i in $(seq 40); do
      body="$(curl -s -m 2 -f "$GW_HOST_URL$path/x" 2>/dev/null)" || { seen=""; break; }
      seen="$seen $(printf '%s' "$body" | sed -n 's/.*"instance":"\([^"]*\)".*/\1/p')"
    done
    if [ -n "$seen" ] && [ "$(printf '%s\n' $seen | sort -u | wc -l | tr -d ' ')" -ge 3 ]; then return 0; fi
    sleep 2
  done
  return 1
}

# Run a python helper in isolated mode (no cwd/user-site imports).
py() { python3 -I "$@"; }
