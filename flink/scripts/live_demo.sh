#!/usr/bin/env bash
# Live test: start Job 2, 3a, 3b and 3c on EMPTY topics, then publish the delivery data
# and let Flink process it while it arrives. The jobs are left running at the end;
# afterwards run flink/scripts/evaluate.sh and then flink/scripts/stop_all.sh.
# Usage: flink/scripts/live_demo.sh [--speed N]
#   --speed N : simulated seconds per wall-clock second (0 = publish as fast as possible)
# WARNING: deletes and recreates BOTH raw topics and the five downstream topics.
source "$(dirname "$0")/common.sh"
SPEED=0
if [ "${1:-}" = "--speed" ]; then SPEED=${2:-0}; fi
RUN_DIR=${RUN_DIR:-/app/output/run-72b96af88a342375}
METER_TOPIC=water.meter-readings.v1
GID=$(date +%s)

wait_running() {   # wait_running <job_id> <label>
  local st
  for i in $(seq 1 30); do
    st=$(curl -s "http://localhost:8081/jobs/$1" | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])" 2>/dev/null)
    if [ "$st" = "RUNNING" ]; then echo "  $2 RUNNING"; return 0; fi
    sleep 2
  done
  echo "  $2 is not RUNNING (last state: ${st:-unknown})" >&2
  return 1
}

echo "== precheck =="
RUNNING=$(running_jobs)
if [ -n "$RUNNING" ]; then echo "Jobs already running, stop them first:"; echo "$RUNNING"; exit 1; fi

echo "== reset raw and downstream topics =="
ALL_TOPICS="$RAW_TOPIC $METER_TOPIC $DOWNSTREAM_TOPICS"
for t in $ALL_TOPICS; do recreate_topic $t; done
sleep 5
for t in $ALL_TOPICS; do create_topic $t >/dev/null; done

echo "== start the four jobs (inputs are empty) =="
J2=$(submit job2_district_validation.py --group-id "j2-$GID") || exit 1
wait_running "$J2" job2 || exit 1
J3A=$(submit job3_district_features.py --group-id "j3a-$GID") || exit 1
wait_running "$J3A" job3a || exit 1
J3B=$(submit job3b_district_alerts.py --group-id "j3b-$GID") || exit 1
wait_running "$J3B" job3b || exit 1
J3C=$(submit job3c_level_shift.py --group-id "j3c-$GID") || exit 1
wait_running "$J3C" job3c || exit 1
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"

echo "== publish the delivery data (speed=$SPEED) while the jobs run =="
START=$(date +%s)
docker compose run -T --rm --no-deps generator publish-kafka --run-dir "$RUN_DIR" --input-set delivery --bootstrap-servers kafka:9092 --speed "$SPEED" > /tmp/publish.log 2>&1 &
PUB=$!
while kill -0 $PUB 2>/dev/null; do
  echo "  [publishing] raw=$(count_topic $RAW_TOPIC) clean=$(count_topic $CLEAN_TOPIC) features=$(count_topic $FEATURES_TOPIC) alerts=$(count_topic $ALERTS_TOPIC)"
  sleep 15
done
wait $PUB; PUB_RC=$?
echo "publisher exit code: $PUB_RC | events published: $(tail -1 /tmp/publish.log) | $(( $(date +%s) - START )) s"

echo "== wait until the outputs stop growing =="
prev=""; stable=0
for i in $(seq 1 80); do
  cur="$(count_topic $CLEAN_TOPIC) $(count_topic $FEATURES_TOPIC) $(count_topic $RELATIVE_TOPIC) $(count_topic $ALERTS_TOPIC)"
  echo "  clean features relative alerts: $cur"
  if [ "$cur" = "$prev" ]; then stable=$((stable+1)); else stable=0; fi
  if [ "$stable" -ge 3 ]; then break; fi
  prev="$cur"; sleep 15
done

echo "== job health =="
job_health "$J2" job2; job_health "$J3A" job3a; job_health "$J3B" job3b; job_health "$J3C" job3c
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"

echo "== summary =="
for t in $RAW_TOPIC $METER_TOPIC $DOWNSTREAM_TOPICS; do echo "$t: $(count_topic $t)"; done
echo "Jobs are still running. Next: flink/scripts/evaluate.sh, then flink/scripts/stop_all.sh"
