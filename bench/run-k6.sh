#!/usr/bin/env bash
# Run one k6 benchmark against an already running bench stack and print a summary.
#   bench/run-k6.sh overhead
#   bench/run-k6.sh maxrps
#   bench/run-k6.sh limiter
# Env knobs (defaults in brackets; see bench/README.md):
#   overhead: RATE[1000] DURATION[8s] WARMUP[5s] ROUNDS[5] AA[1] LABEL (output suffix, e.g. accesslog)
#   maxrps:   see bench/maxrps.sh (REPS, SEED, TARGETS, STEPS, STEP_DURATION ...)
#   limiter:  LIMITER_DURATION[15] OVER[2] NOISY[4]
# Output: $OUT_DIR/<name>.json. Exit status is non-zero if the measurement failed.
set -euo pipefail
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

what="${1:?usage: run-k6.sh overhead|maxrps|limiter}"
require_stack
out_dir_init

case "$what" in
  overhead)
    # AA=0 skips the direct-vs-direct noise-floor rounds (used for the access-log-on pass)
    file="overhead${LABEL:+-$LABEL}.json"
    k6_run overhead.js "OUT_NAME=$file" ${RATE:+RATE=$RATE} ${DURATION:+DURATION=$DURATION} ${WARMUP:+WARMUP=$WARMUP} \
      ${ROUNDS:+ROUNDS=$ROUNDS} ${AA:+AA=$AA}
    ;;
  maxrps)
    # all targets, repeated and interleaved: see bench/maxrps.sh
    "$BENCH_DIR/maxrps.sh"
    exit $?
    ;;
  limiter)
    k6_run limiter.js ${LIMITER_DURATION:+DURATION=${LIMITER_DURATION%s}} ${OVER:+OVER=$OVER} ${NOISY:+NOISY=$NOISY} \
      -- --console-output /out/limiter.log
    [ -s "$OUT_DIR/limiter-raw.json" ] || die "k6 produced no limiter-raw.json"
    py "$BENCH_DIR/analyze_limiter.py" "$OUT_DIR/limiter.log" "$OUT_DIR/limiter-raw.json" "$OUT_DIR/limiter.json"
    rm -f "$OUT_DIR/limiter-raw.json"
    file="limiter.json"
    ;;
  *) die "unknown benchmark: $what" ;;
esac

[ -s "$OUT_DIR/$file" ] || die "k6 produced no $file"
py "$BENCH_DIR/summarize.py" show "$OUT_DIR/$file"
