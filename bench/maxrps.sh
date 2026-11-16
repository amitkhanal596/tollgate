#!/usr/bin/env bash
# Max-RPS measurement for all targets, repeated and interleaved.
#   bench/maxrps.sh                      # direct, gateway, gateway3, 3 repetitions
#   TARGETS="gateway" REPS=1 STEPS=1000,2000 STEP_DURATION=4 bench/maxrps.sh
# Each repetition runs every target once, in a random order drawn from SEED
# (recorded in maxrps-plan*.json), so run order and drift do not favour one
# target. Per run it records, next to k6's per-step numbers: `docker stats` CPU
# per container, the stall watchdog, gateway upstream dials (ActiveOpens delta)
# and TIME_WAIT peak per step, k6's own log (for "Insufficient VUs").
# summarize.py combines them and labels steps that the rig, not the gateway, limited.
# Env: TARGETS[direct gateway gateway3] REPS[3] SEED[random] TAG[] (file suffix, e.g. accesslog)
#      SETTLE[8] (seconds between runs) STEPS STEP_DURATION RAMP WARMUP P99_MS
# Output: $OUT_DIR/maxrps-<target>[-TAG]-r<N>.json (+ .stats.jsonl, .k6.log), netwatch-maxrps[-TAG].jsonl
set -euo pipefail
. "$(dirname "$0")/lib.sh"
require_stack
out_dir_init

TARGETS="${TARGETS:-direct gateway gateway3}"; REPS="${REPS:-3}"; SEED="${SEED:-$RANDOM$RANDOM}"
TAG="${TAG:-}"; SFX="${TAG:+-$TAG}"; SETTLE="${SETTLE:-8}"

own_watchdog=0
if ! watchdog_running; then start_watchdog; own_watchdog=1; fi
start_netwatch
cleanup() {
  stop_sampler; kill_stray_k6
  stop_netwatch "$OUT_DIR/netwatch-maxrps$SFX.jsonl"
  [ "$own_watchdog" -eq 0 ] || stop_watchdog "$OUT_DIR/stall.log"
}
trap cleanup EXIT

plan="$(py - "$SEED" "$REPS" $TARGETS <<'PY'
import json, random, sys
seed, reps, targets = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
order = []
for r in range(1, reps + 1):
    t = list(targets)
    random.Random(f"{seed}:{r}").shuffle(t)
    order.append(t)
print(json.dumps({"seed": seed, "reps": reps, "order": order}))
PY
)"
echo "$plan" > "$OUT_DIR/maxrps-plan$SFX.json"
log "maxrps plan: $plan"

fail=0
for rep in $(seq "$REPS"); do
  for target in $(py -c 'import json,sys;print(" ".join(json.load(sys.stdin)["order"][int(sys.argv[1])-1]))' "$rep" <<<"$plan"); do
    name="maxrps-$target$SFX-r$rep"
    log "=== $name"
    start_sampler "$OUT_DIR/$name.stats.jsonl"
    K6_LOG="$OUT_DIR/$name.k6.log" k6_run maxrps.js "TARGET=$target" "OUT_NAME=$name.json" \
      ${STEPS:+STEPS=$STEPS} ${STEP_DURATION:+STEP_DURATION=${STEP_DURATION%s}} ${RAMP:+RAMP=$RAMP} \
      ${P99_MS:+P99_MS=$P99_MS} ${WARMUP:+WARMUP=${WARMUP%s}} || { log "$name: k6 failed"; fail=$((fail + 1)); }
    stop_sampler
    [ -s "$OUT_DIR/$name.json" ] || { log "$name: no result"; fail=$((fail + 1)); }
    sleep "$SETTLE"   # let TIME_WAIT sockets and CPU settle between runs
  done
done
[ "$fail" -eq 0 ] || die "$fail max-RPS run(s) failed"
log "maxrps: all runs done (labelling and the report happen in summarize.py)"
