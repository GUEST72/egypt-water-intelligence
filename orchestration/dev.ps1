# Start the Dagster UI (http://localhost:3000) with persistent run history in orchestration/.dagster_home.
# Usage, from any directory:  powershell -ExecutionPolicy Bypass -File orchestration\dev.ps1
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$env:DAGSTER_HOME = Join-Path $here ".dagster_home"
New-Item -ItemType Directory -Force -Path $env:DAGSTER_HOME | Out-Null
Set-Location $here
uv run dagster dev -h 127.0.0.1 -p 3000
