#!/usr/bin/env bash
# Start the Dagster UI (http://localhost:3000) with persistent run history in orchestration/.dagster_home.
set -euo pipefail
cd "$(dirname "$0")"
export DAGSTER_HOME="$PWD/.dagster_home"
mkdir -p "$DAGSTER_HOME"
exec uv run dagster dev -h 127.0.0.1 -p 3000
