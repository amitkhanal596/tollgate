#!/usr/bin/env bash
# Failover chaos test. For each configuration of the same three demo instances
# (passive health only / active + passive / Consul discovery + active + passive):
#   kill:    SIGKILL demo-1 at T0 under steady load, restart it later.
#   fail500: demo-1 stays alive but answers every request with 500
#            (POST /admin/fail?mode=500), then goes back to normal.
# Each case is run REPEATS times with a randomised fault duration (uniform
# FAULT_MIN..FAULT_MAX seconds, recorded), so the restore lands at a different
# phase of the 10 s passive cool-off each time. Without that, a fixed 20 s fault
# is exactly two cool-offs and the "back in rotation" time is an artifact of the
# alignment. Measures failed requests and their time window, failover time,
# periodic re-probe failures, and the time until demo-1 is served again.
#
#   bench/chaos/failover.sh            # 6 cases x 3 runs
#   CONFIGS="active" MODES="kill" REPEATS=1 bench/chaos/failover.sh
# Env: RATE[200] PRE[4] REC[15] REPEATS[3] FAULT_MIN[13] FAULT_MAX[27] SEED[random]
#      FAULT (fixed fault seconds; overrides the random draw; smoke only)
#      CONFIGS[passive active consul] MODES[kill fail500]
# Output: $OUT_DIR/failover-<config>-<mode>-r<N>.json (+ .log with every request)
# Needs a running bench stack (make bench-up). Exit non-zero if a run could not be measured.
set -euo pipefail
. "$(dirname "$0")/../lib.sh"

RATE="${RATE:-200}"; PRE="${PRE:-4}"; REC="${REC:-15}"; REPEATS="${REPEATS:-3}"
FAULT_MIN="${FAULT_MIN:-13}"; FAULT_MAX="${FAULT_MAX:-27}"
SEED="${SEED:-$RANDOM$RANDOM}"
CONFIGS="${CONFIGS:-passive active consul}"
MODES="${MODES:-kill fail500}"
require_stack
out_dir_init

# epoch ms from a docker RFC3339 timestamp
to_ms() { py -c 'import sys,datetime as d;s=sys.argv[1].replace("Z","+00:00");h,_,r=s.partition(".");f,_,tz=r.partition("+");t=d.datetime.fromisoformat(h+"."+f[:6]+"+"+tz);print(int(t.timestamp()*1000+0.5))' "$1"; }

restore_all() {
  local n id
  for n in 1 2 3; do
    id="$(cid "demo-$n")"
    [ "$(docker inspect -f '{{.State.Running}}' "$id")" = "true" ] || docker start "$id" >/dev/null
  done
  reset_faults
}
trap 'kill_stray_k6; restore_all || true' EXIT

log "failover: seed $SEED, $REPEATS run(s) per case, fault ${FAULT:-uniform $FAULT_MIN..$FAULT_MAX} s"
fail_cases=0
for cfg in $CONFIGS; do
  for mode in $MODES; do
    for run in $(seq "$REPEATS"); do
      case_id="$cfg-$mode"
      id="$case_id-r$run"
      path="/fo-$cfg"
      fault="${FAULT:-$(py -c 'import random,sys;print(round(random.Random(sys.argv[1]).uniform(float(sys.argv[2]),float(sys.argv[3])),1))' "$SEED:$id" "$FAULT_MIN" "$FAULT_MAX")}"
      log "=== $id: fault ${fault}s; settling (all 3 instances must be in rotation)"
      restore_all
      wait_all_instances "$path" 120 || die "$id: gateway never routed to all 3 instances"

      reqlog="$OUT_DIR/failover-$id.log"
      rm -f "$reqlog"
      total="$(py -c 'import math,sys;print(math.ceil(float(sys.argv[1])+float(sys.argv[2])+float(sys.argv[3])+(6 if sys.argv[4]=="kill" else 0)))' "$PRE" "$fault" "$REC" "$mode")"
      restore_at="$(py -c 'import sys;print(float(sys.argv[1])+float(sys.argv[2]))' "$PRE" "$fault")"
      k6_run probe.js "PATH_PREFIX=$path" "RATE=$RATE" "TOTAL=$total" "MODE=$mode" \
        "INJECT_AT=$PRE" "RESTORE_AT=$restore_at" -- --console-output "/out/failover-$id.log" &
      k6pid=$!

      # Wait for the load to be flowing, then let it run PRE seconds before the fault.
      for _ in $(seq 60); do [ -s "$reqlog" ] && break; sleep 0.5; done
      [ -s "$reqlog" ] || { kill $k6pid 2>/dev/null || true; die "$id: load generator produced no output"; }

      victim="$(cid demo-1)"
      if [ "$mode" = "kill" ]; then
        sleep "$PRE"
        docker kill "$victim" >/dev/null              # SIGKILL: no deregistration, no graceful close
        t0="$(to_ms "$(docker inspect -f '{{.State.FinishedAt}}' "$victim")")"
        sleep "$fault"
        docker start "$victim" >/dev/null
        t1="$(to_ms "$(docker inspect -f '{{.State.StartedAt}}' "$victim")")"
      fi
      wait $k6pid || { log "$id: k6 exited non-zero"; fail_cases=$((fail_cases + 1)); continue; }

      if [ "$mode" = "fail500" ]; then
        t0="$(sed -n 's/.*msg="E fault_on \([0-9]*\) 200".*/\1/p' "$reqlog" | head -1)"
        t1="$(sed -n 's/.*msg="E fault_off \([0-9]*\) 200".*/\1/p' "$reqlog" | head -1)"
        [ -n "$t0" ] && [ -n "$t1" ] || { log "$id: fault injection did not complete (see $reqlog)"; fail_cases=$((fail_cases + 1)); continue; }
      fi

      if py "$BENCH_DIR/chaos/analyze_failover.py" "$reqlog" --id "$id" --path "$path" --mode "$mode" \
           --rate "$RATE" --t0 "$t0" --t1 "$t1" --victim demo-1 --fault-s "$fault" > "$OUT_DIR/failover-$id.json"; then
        py - "$OUT_DIR/failover-$id.json" "$case_id" "$cfg" "$run" "$SEED" <<'PY'
import json, sys
p, case, cfg, run, seed = sys.argv[1:6]
d = json.load(open(p))
d.update({"case": case, "config": cfg, "run": int(run), "seed": seed})
json.dump(d, open(p, "w"), indent=2)
PY
        py "$BENCH_DIR/summarize.py" show "$OUT_DIR/failover-$id.json" || fail_cases=$((fail_cases + 1))
      else
        log "$id: analysis failed"; fail_cases=$((fail_cases + 1))
      fi
    done
  done
done

[ "$fail_cases" -eq 0 ] || die "$fail_cases failover run(s) could not be measured"
log "failover: all runs measured"
