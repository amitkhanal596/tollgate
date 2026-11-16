#!/usr/bin/env bash
# Hot reload under load: steady traffic through the gateway while N config
# changes (route flips and rollbacks) go through the control plane's admin API,
# spread evenly over the whole load window. demo-3 is put in "slow" mode (2 s
# per response) so that hundreds of requests are in flight across every config
# swap; the analysis counts the requests whose [start, end] interval contains a
# change. Reports requests, non-2xx, connection errors (the claim: zero),
# dropped iterations, and propagation latency as a lower and an upper bound.
#
#   bench/chaos/hot-reload.sh                 # 100 changes, 0.5 s apart, ~1 minute
#   CHANGES=20 PERIOD=0.4 RATE=100 SLOW_RATE=30 bench/chaos/hot-reload.sh
# Env: CHANGES[100] RATE[300] SLOW_RATE[100] PERIOD[0.5] ROLLBACK_EVERY[10]
# Output: $OUT_DIR/hot-reload.json (+ hot-reload.log). Needs a running bench stack.
set -euo pipefail
. "$(dirname "$0")/../lib.sh"

CHANGES="${CHANGES:-100}"; RATE="${RATE:-300}"; SLOW_RATE="${SLOW_RATE:-100}"
PERIOD="${PERIOD:-${RELOAD_GAP:-0.5}}"; ROLLBACK_EVERY="${ROLLBACK_EVERY:-10}"
require_stack
out_dir_init

put_route() { # leave the stack as the seed had it
  net_curl -o /dev/null -X PUT -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
    -d "{\"name\":\"$1\",\"path_prefix\":\"/$1\",\"upstream\":\"$2\"}" "$CP_URL/v1/routes/$1" || true
}
cleanup() {
  kill_stray_k6
  net_curl -X POST "http://demo-3:8080/admin/fail?mode=off" >/dev/null 2>&1 || true
  put_route flip flip-a; put_route hrslow slow-a
}
trap cleanup EXIT

gw="$(cid gateway)"
gw_started="$(docker inspect -f '{{.State.StartedAt}}' "$gw")"
head_before="$(cp_get /v1/config | py -c 'import json,sys;print(json.load(sys.stdin)["version"])')"
log "head version before: $head_before"

# demo-3 answers every request after 2 s from now on, so each request through /hrslow is in flight for 2 s.
net_curl -X POST "http://demo-3:8080/admin/fail?mode=slow" | grep -q slow || die "could not put demo-3 in slow mode (is -fault-injection on?)"

k6_run hot-reload.js "CHANGES=$CHANGES" "RATE=$RATE" "SLOW_RATE=$SLOW_RATE" "PERIOD=$PERIOD" "ROLLBACK_EVERY=$ROLLBACK_EVERY" \
  -- --console-output /out/hot-reload.log
[ -s "$OUT_DIR/hot-reload-raw.json" ] || die "k6 produced no hot-reload-raw.json"
net_curl -X POST "http://demo-3:8080/admin/fail?mode=off" >/dev/null 2>&1 || true

# Did every change reach the gateway, and did it stay up the whole time?
sleep 1
head_after="$(cp_get /v1/config | py -c 'import json,sys;print(json.load(sys.stdin)["version"])')"
applied="$(cp_get /v1/dataplanes | py -c '
import json,sys
d=json.load(sys.stdin)
n=d if isinstance(d,list) else d.get("dataplanes", d.get("items", []))
print(max((x.get("applied_version",0) for x in n), default=0))')"
restarted=false
[ "$(docker inspect -f '{{.State.StartedAt}}' "$gw")" = "$gw_started" ] || restarted=true

py "$BENCH_DIR/chaos/analyze_hotreload.py" "$OUT_DIR/hot-reload.log" "$OUT_DIR/hot-reload-raw.json" "$OUT_DIR/hot-reload.json"
py - "$OUT_DIR/hot-reload.json" "$head_before" "$head_after" "$applied" "$restarted" <<'PY'
import json, sys
path, hb, ha, ap, rs = sys.argv[1:6]
d = json.load(open(path))
d["host"] = {"head_version_before": int(hb), "head_version": int(ha), "applied_version": int(ap),
             "gateway_restarted": rs == "true"}
json.dump(d, open(path, "w"), indent=2)
PY
rm -f "$OUT_DIR/hot-reload-raw.json"
py "$BENCH_DIR/summarize.py" show "$OUT_DIR/hot-reload.json"
