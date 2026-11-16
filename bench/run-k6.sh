#!/usr/bin/env bash
# Run one k6 benchmark against an already running bench stack and print a summary.
#   bench/run-k6.sh overhead
#   bench/run-k6.sh maxrps [direct|gateway|gateway3]
#   bench/run-k6.sh limiter
# Env knobs (defaults in brackets; see bench/README.md):
#   overhead: RATE[1000] DURATION[30s] WARMUP[5s] ROUNDS[2] LABEL (output suffix, e.g. accesslog)
#   maxrps:   STEPS[1000,2000,4000,8000,12000] STEP_DURATION[20s] P99_MS[50] LABEL
#   limiter:  DURATION[30] OVER[5]
# Output: $OUT_DIR/<name>.json. Exit status is non-zero if the measurement failed.
set -euo pipefail
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

what="${1:?usage: run-k6.sh overhead|maxrps [target]|limiter}"
require_stack

case "$what" in
  overhead)
    file="overhead${LABEL:+-$LABEL}.json"
    k6_run overhead.js "OUT_NAME=$file" ${RATE:+RATE=$RATE} ${DURATION:+DURATION=$DURATION} ${WARMUP:+WARMUP=$WARMUP} ${ROUNDS:+ROUNDS=$ROUNDS}
    ;;
  maxrps)
    target="${2:-gateway}"
    label="${LABEL:-$target}"
    k6_run maxrps.js "TARGET=$target" "LABEL=$label" ${STEPS:+STEPS=$STEPS} ${STEP_DURATION:+STEP_DURATION=${STEP_DURATION%s}} ${P99_MS:+P99_MS=$P99_MS} ${GAP:+GAP=$GAP} ${WARMUP:+WARMUP=${WARMUP%s}}
    file="maxrps-$label.json"
    ;;
  limiter)
    k6_run limiter.js ${DURATION:+DURATION=${DURATION%s}} ${OVER:+OVER=$OVER}
    file="limiter.json"
    ;;
  *) die "unknown benchmark: $what" ;;
esac

[ -s "$OUT_DIR/$file" ] || die "k6 produced no $file"
py "$BENCH_DIR/summarize.py" show "$OUT_DIR/$file"
