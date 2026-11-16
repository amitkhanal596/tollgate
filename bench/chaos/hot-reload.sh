#!/usr/bin/env bash
# Hot reload under load: steady traffic through the gateway while N config
# changes (route flips between two upstreams, plus rollbacks) go through the
# control plane's admin API. Reports total requests, non-2xx, connection
# errors (the claim: zero dropped requests) and reload propagation latency.
#
#   bench/chaos/hot-reload.sh                 # 100 changes at 500 rps, ~1 minute
#   CHANGES=20 RATE=200 bench/chaos/hot-reload.sh
# Env: CHANGES[100] RATE[500] RELOAD_GAP[0.25] ROLLBACK_EVERY[10]
# Output: $OUT_DIR/hot-reload.json. Needs a running bench stack (make bench-up).
set -euo pipefail
. "$(dirname "$0")/../lib.sh"

CHANGES="${CHANGES:-100}"; RATE="${RATE:-500}"; GAP="${RELOAD_GAP:-0.25}"; ROLLBACK_EVERY="${ROLLBACK_EVERY:-10}"
require_stack

cp_get() { curl -s -m 5 -H "Authorization: Bearer $ADMIN_TOKEN" "$CP_HOST_URL$1"; }
restore_route() { # leave the stack as the seed had it
  curl -s -m 5 -o /dev/null -X PUT -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
    -d '{"name":"flip","path_prefix":"/flip","upstream":"flip-a"}' "$CP_HOST_URL/v1/routes/flip" || true
}
trap 'kill_stray_k6; restore_route' EXIT

gw="$(cid gateway)"
gw_started="$(docker inspect -f '{{.State.StartedAt}}' "$gw")"
head_before="$(cp_get /v1/config | py -c 'import json,sys;print(json.load(sys.stdin)["version"])')"
log "head version before: $head_before"

k6_run hot-reload.js "CHANGES=$CHANGES" "RATE=$RATE" "GAP=$GAP" "ROLLBACK_EVERY=$ROLLBACK_EVERY"
[ -s "$OUT_DIR/hot-reload.json" ] || die "k6 produced no hot-reload.json"

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

py - "$OUT_DIR/hot-reload.json" "$head_before" "$head_after" "$applied" "$restarted" <<'PY'
import json, sys
path, hb, ha, ap, rs = sys.argv[1:6]
d = json.load(open(path))
d["host"] = {"head_version_before": int(hb), "head_version": int(ha), "applied_version": int(ap),
             "gateway_restarted": rs == "true"}
json.dump(d, open(path, "w"), indent=2)
PY
py "$BENCH_DIR/summarize.py" show "$OUT_DIR/hot-reload.json"
