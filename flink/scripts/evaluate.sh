#!/usr/bin/env bash
# Reads the alerts topic and compares it with the restricted truth (offline only).
# Usage: flink/scripts/evaluate.sh [run_dir]
source "$(dirname "$0")/common.sh"
RUN_DIR=${1:-Generator/output/run-bdc85c43085c4dc8}
OUT=/tmp/alerts.ndjson
kafka kafka-console-consumer.sh --bootstrap-server $BOOT --topic $ALERTS_TOPIC --from-beginning --timeout-ms 20000 > $OUT 2>/dev/null
echo "alert lines: $(grep -c '^{' $OUT)"
${PYTHON:-python} analysis/evaluate_alerts.py --alerts $OUT \
  --truth "$RUN_DIR/restricted/scenario-truth.ndjson" \
  --faults "$RUN_DIR/restricted/fault-truth.ndjson"
