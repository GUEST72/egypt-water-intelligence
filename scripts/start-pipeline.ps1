$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed: $args" }
}
Invoke-Docker compose version
Invoke-Docker info
Invoke-Docker compose --profile pipeline config --quiet
Invoke-Docker compose build generator flink-jobmanager spark airflow-scheduler
Invoke-Docker compose --profile pipeline up -d kafka minio kafka-ui catalog-db trino flink-jobmanager flink-taskmanager
Invoke-Docker compose up --no-deps --no-start archiver
Invoke-Docker compose --profile pipeline up -d airflow-webserver airflow-scheduler
Invoke-Docker compose --profile pipeline ps -a
Write-Host "Airflow: http://localhost:8083 (credentials from .env)"
Write-Host "Trigger egypt_water_pipeline with config=smoke, then unpause egypt_water_health."
