# Common operations for the Egypt Water Intelligence pipeline.
#
#   .\run.ps1 setup      build the images and install the Dagster environment (once)
#   .\run.ps1 up         start all services
#   .\run.ps1 dagster    start the Dagster UI on http://localhost:3000 (keep this window open)
#   .\run.ps1 pipeline   generate the dataset if missing, then run every stage for it
#   .\run.ps1 demo       replay the small demo run while the Grafana board (http://localhost:3001) is open
#   .\run.ps1 status     show the services and the Flink jobs
#   .\run.ps1 down       stop all services (all data is kept)
#
# Options: -Config <file in configs\>   dataset for "pipeline" (default pipeline-large.yml)
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("setup", "up", "dagster", "pipeline", "demo", "status", "down")]
    [string]$Command,
    [string]$Config = "pipeline-large.yml"
)
$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
$Orchestration = Join-Path $Root "orchestration"
$env:Path = "$env:USERPROFILE\.local\bin;" + [Environment]::GetEnvironmentVariable("Path", "Machine") + ";" +
            [Environment]::GetEnvironmentVariable("Path", "User")
$env:DAGSTER_HOME = Join-Path $Orchestration ".dagster_home"
New-Item -ItemType Directory -Force $env:DAGSTER_HOME | Out-Null

$AllAssets = "generator_run,raw_run_upload,kafka_delivery_streams,raw_event_archive,flink_district_outputs," +
             "batch_silver_gold,batch_validation,streaming_iceberg_facts,alert_evaluation,dashboard_marts,dashboard_exports"

function Dagster([string[]]$arguments) {
    Push-Location $Orchestration
    try { uv run dagster @arguments; if ($LASTEXITCODE -ne 0) { throw "dagster $($arguments[0]) failed" } }
    finally { Pop-Location }
}

function RunConfigFile([string]$yaml) {
    $file = Join-Path $env:DAGSTER_HOME "run-config.yaml"
    Set-Content -Path $file -Value $yaml -Encoding ascii
    return $file
}

# The run ID is derived from the configuration by the generator's own code.
function RunId([string]$configFile) {
    $code = "from egypt_water_intelligence.generator.config import load_config;" +
            "from egypt_water_intelligence.generator.outputs import run_id;" +
            "print(run_id(load_config('/app/pipeline-configs/$configFile')))"
    $output = docker compose --project-directory $Root run --rm --no-deps --entrypoint python generator -c $code 2>$null
    return ($output | Select-Object -Last 1).Trim()
}

Set-Location $Root
switch ($Command) {
    "setup" {
        docker compose --profile tools build
        Push-Location $Orchestration; uv sync; Pop-Location
    }
    "up" {
        docker compose up -d --wait
        Write-Host "Services are up. Kafka UI http://localhost:8080 | Flink http://localhost:8081 | MinIO http://localhost:9001"
        Write-Host "Trino http://localhost:8082 | Spark History http://localhost:18080 | Grafana http://localhost:3001"
    }
    "dagster" {
        Push-Location $Orchestration
        uv run dagster dev -h 127.0.0.1 -p 3000
        Pop-Location
    }
    "pipeline" {
        $cfg = RunConfigFile "ops:`n  generate_dataset:`n    config:`n      config_file: $Config"
        Dagster @("job", "execute", "-m", "water_orchestration", "-j", "generate_dataset_job", "-c", $cfg)
        $runId = RunId $Config
        Write-Host "Running every stage for $runId (the first publication of a large run takes about an hour)"
        Dagster @("asset", "materialize", "-m", "water_orchestration", "--select", $AllAssets, "--partition", $runId)
    }
    "demo" {
        $settings = "      config_file: demo-live.yml`n      speed: 480`n      stop_main_jobs: true"
        $cfg = RunConfigFile "ops:`n  prepare_demo:`n    config:`n$settings`n  replay_demo:`n    config:`n$settings"
        Write-Host "Open http://localhost:3001 now (Stream = demo); detections appear while the replay runs."
        Dagster @("job", "execute", "-m", "water_orchestration", "-j", "live_demo", "-c", $cfg)
    }
    "status" {
        docker compose ps --format "table {{.Service}}\t{{.Status}}"
        try { (Invoke-RestMethod http://localhost:8081/jobs/overview).jobs | Select-Object name, state | Format-Table }
        catch { Write-Host "Flink is not reachable" }
    }
    "down" {
        docker compose down
    }
}
