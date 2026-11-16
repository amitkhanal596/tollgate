#!/usr/bin/env bash
# Start or stop the benchmark stack by hand (project tollgate-bench).
#   bench/stack.sh up      clean start (down -v of the bench project first), build, wait
#   bench/stack.sh down    stop it and delete ITS volumes (the demo stack is untouched)
# After `up` the individual scripts work: bench/run-k6.sh overhead|limiter, bench/maxrps.sh,
# bench/chaos/failover.sh, bench/chaos/hot-reload.sh.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
case "${1:-}" in
  up) out_dir_init; bring_up manual manual false && log "stack is up (project $PROJECT); pinning: $(pinning_json)" ;;
  down) bring_down; cleanup_helpers; log "stack down" ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
