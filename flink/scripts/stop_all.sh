#!/usr/bin/env bash
# Stops every running Flink job with a savepoint and logs the savepoint paths.
source "$(dirname "$0")/common.sh"
LOG="$ROOT/flink/scripts/savepoints.log"
JOBS=$(running_jobs)
if [ -z "$JOBS" ]; then echo "No running jobs."; exit 0; fi
for j in $JOBS; do
  echo "Stopping $j with savepoint..."
  out=$(flinkcmd stop --savepointPath /opt/flink/state/savepoints "$j")
  sp=$(echo "$out" | grep -oE 'file:[^ ]+' | tail -1)
  if [ -n "$sp" ]; then
    echo "$(date -Iseconds) $j $sp" | tee -a "$LOG"
  else
    echo "stop-with-savepoint FAILED for $j:"; echo "$out" | tail -5
  fi
done
