#!/usr/bin/env bash
# Shared helpers for start_all.sh / stop_all.sh / evaluate.sh
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

KAFKA_BIN=/opt/kafka/bin
BOOT=localhost:9092
RAW_TOPIC=water.district-sensors.v1
CLEAN_TOPIC=water.district-sensors.clean.v1
DEAD_TOPIC=water.dead-letter.district.v1
FEATURES_TOPIC=water.district-features.15m.v1
RELATIVE_TOPIC=water.district-relative.15m.v1
ALERTS_TOPIC=water.district-alerts.v1
DOWNSTREAM_TOPICS="$CLEAN_TOPIC $DEAD_TOPIC $FEATURES_TOPIC $RELATIVE_TOPIC $ALERTS_TOPIC"

kafka() { local tool=$1; shift; docker compose exec -T kafka "$KAFKA_BIN/$tool" "$@"; }

# Runs the flink CLI in the jobmanager and hides the harmless jdk.compiler warnings.
flinkcmd() { docker compose exec -T flink-jobmanager flink "$@" 2>&1 | grep -v 'jdk.compiler'; }

count_topic() {
  local n
  n=$(kafka kafka-get-offsets.sh --bootstrap-server $BOOT --topic "$1" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  echo "${n:-0}"
}

running_jobs() { flinkcmd list -r | grep -oE '[0-9a-f]{32}' | sort -u; }

recreate_topic() {
  kafka kafka-topics.sh --bootstrap-server $BOOT --delete --topic "$1" >/dev/null 2>&1
}

create_topic() {
  kafka kafka-topics.sh --bootstrap-server $BOOT --create --if-not-exists --topic "$1" --partitions 1 --replication-factor 1 2>&1 | grep -E 'Created|already exists'
}

# Waits until the row count of a topic is > 0 and unchanged for two polls (10 s apart).
wait_stable() {
  local topic=$1 prev=-1 cur
  for i in $(seq 1 60); do
    sleep 10
    cur=$(count_topic "$topic")
    echo "  $topic rows: $cur"
    if [ "$cur" = "$prev" ] && [ "$cur" -gt 0 ]; then return 0; fi
    prev=$cur
  done
  echo "TIMEOUT waiting for $topic" >&2
  return 1
}

# submit <job_file> [options...]  -> prints the job id on stdout
submit() {
  local out id
  out=$(flinkcmd run -d -p 1 -py "/opt/flink/jobs/$1" "${@:2}")
  id=$(echo "$out" | grep -oE '[0-9a-f]{32}' | tail -1)
  if [ -z "$id" ]; then
    echo "FAILED to submit $1" >&2
    echo "$out" >&2
    return 1
  fi
  echo "$id"
}

job_health() {  # job_health <id> <label>
  curl -s "http://localhost:8081/jobs/$1" | python3 -c "import sys,json; print('$2 state:', json.load(sys.stdin)['state'])"
  curl -s "http://localhost:8081/jobs/$1/exceptions" | python3 -c "import sys,json; e=json.load(sys.stdin); print('$2 last exception:', (e.get('exceptionHistory',{}).get('entries') or [{}])[0].get('exceptionName'))"
  curl -s "http://localhost:8081/jobs/$1/checkpoints" | python3 -c "import sys,json; c=json.load(sys.stdin)['counts']; print('$2 checkpoints completed:', c['completed'], 'failed:', c['failed'])"
}
