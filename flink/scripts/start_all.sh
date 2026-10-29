#!/usr/bin/env bash
# Usage: flink/scripts/start_all.sh [--reset] [--live]
#   --reset : delete and recreate the 5 downstream topics first (needed for a clean re-run)
#   --live  : keep all jobs running (needs more RAM). Default: run one job at a time,
#             wait for its output topic to stabilise, then cancel it.
source "$(dirname "$0")/common.sh"
RESET=0; LIVE=0
for a in "$@"; do
  case $a in --reset) RESET=1 ;; --live) LIVE=1 ;; *) echo "unknown option $a"; exit 1 ;; esac
done
GID=$(date +%s)

echo "== precheck =="
RUNNING=$(running_jobs)
if [ -n "$RUNNING" ]; then echo "Jobs already running, stop them first:"; echo "$RUNNING"; exit 1; fi
RAW=$(count_topic $RAW_TOPIC); echo "raw rows: $RAW"
if [ "$RAW" -le 0 ]; then echo "Raw topic is empty: publish the delivery data first."; exit 1; fi

if [ "$RESET" = 1 ]; then
  echo "== reset downstream topics =="
  for t in $DOWNSTREAM_TOPICS; do recreate_topic $t; done
  sleep 5
fi
for t in $DOWNSTREAM_TOPICS; do create_topic $t >/dev/null; done
if [ "$RESET" = 0 ]; then
  for t in $DOWNSTREAM_TOPICS; do
    if [ "$(count_topic $t)" -gt 0 ]; then echo "$t is not empty. Use --reset."; exit 1; fi
  done
fi

finish_job() { [ "$LIVE" = 1 ] || flinkcmd cancel "$1" | tail -1; }

echo "== Job 2: validation / dedup =="
J2=$(submit job2_district_validation.py --group-id "j2-$GID") || exit 1
wait_stable $CLEAN_TOPIC || exit 1
echo "clean: $(count_topic $CLEAN_TOPIC) | dead-letter: $(count_topic $DEAD_TOPIC)"
job_health "$J2" "job2"; finish_job "$J2"

echo "== Job 3a: 15-minute features =="
J3A=$(submit job3_district_features.py --group-id "j3a-$GID") || exit 1
wait_stable $FEATURES_TOPIC || exit 1
job_health "$J3A" "job3a"; finish_job "$J3A"

echo "== Job 3b: rules R1 R2 R3 =="
J3B=$(submit job3b_district_alerts.py --group-id "j3b-$GID") || exit 1
wait_stable $RELATIVE_TOPIC || exit 1
job_health "$J3B" "job3b"; finish_job "$J3B"

echo "== Job 3c: rule R4 =="
J3C=$(submit job3c_level_shift.py --group-id "j3c-$GID") || exit 1
sleep 120
job_health "$J3C" "job3c"; finish_job "$J3C"

echo "== summary =="
for t in $DOWNSTREAM_TOPICS; do echo "$t: $(count_topic $t)"; done
echo "Next: flink/scripts/evaluate.sh"
