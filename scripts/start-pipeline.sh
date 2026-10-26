#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -f .env ]]; then cp .env.example .env; fi
# Use the actual socket group on Linux; an explicit exported value takes priority.
if [[ -S /var/run/docker.sock ]]; then
  export DOCKER_GID="${DOCKER_GID:-$(stat -c %g /var/run/docker.sock)}"
fi
docker compose version
docker info >/dev/null
docker compose --profile pipeline config --quiet
docker compose build generator flink-jobmanager spark airflow-scheduler
docker compose --profile pipeline up -d kafka minio kafka-ui catalog-db trino flink-jobmanager flink-taskmanager
# Airflow starts the archiver after initialization. Create it without starting dependencies.
docker compose up --no-deps --no-start archiver
docker compose --profile pipeline up -d airflow-webserver airflow-scheduler
docker compose --profile pipeline ps -a
printf '\nAirflow: http://localhost:8083 (credentials from .env)\nTrigger egypt_water_pipeline with config=smoke, then unpause egypt_water_health.\n'
