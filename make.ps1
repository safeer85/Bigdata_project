<#
Windows shim for the Makefile. GNU make is not installed with Docker Desktop, and
the marker should not have to install it. Every target here mirrors the Makefile
target of the same name.

    .\make.ps1 up
    .\make.ps1 logs speed
    .\make.ps1 demo-idle
#>
param(
    [Parameter(Position = 0)] [string] $Target = "help",
    [Parameter(Position = 1, ValueFromRemainingArguments = $true)] [string[]] $Rest
)

# NOT "Stop". Docker writes its normal build and startup progress to stderr, and
# under $ErrorActionPreference = "Stop" PowerShell turns any stderr from a native
# executable into a TERMINATING error -- so `.\make.ps1 up` died on the very first
# "Service spark-master Building" line. Native commands are checked by their exit
# code instead, which is the only reliable signal they give.
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot

function Invoke-Native {
    <#
        Run a native command and fail only on a non-zero exit code.
        $args is splatted so quoting is preserved.
    #>
    param([Parameter(Mandatory)] [string] $Exe,
          [Parameter(ValueFromRemainingArguments)] [string[]] $Arguments)

    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) {
        Write-Error "$Exe $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

function Ensure-Env {
    if (-not (Test-Path ".env")) {
        Write-Host "creating .env from .env.example"
        Copy-Item ".env.example" ".env"
    }
}

function Show-Urls {
    Write-Host ""
    Write-Host "  API docs        http://localhost:8000/docs"
    Write-Host "  Grafana         http://localhost:3000       (admin/admin)"
    Write-Host "  Airflow         http://localhost:8088       (admin/admin)"
    Write-Host "  Prometheus      http://localhost:9090"
    Write-Host "  Alertmanager    http://localhost:9093"
    Write-Host "  Spark master    http://localhost:8080"
    Write-Host "  Kafka UI        http://localhost:8090       (.\make.ps1 up-tools)"
    Write-Host ""
}

switch ($Target) {
    "help" {
        Write-Host "targets: up up-tools down reset build wait ps logs test smoke urls"
        Write-Host "         demo-idle demo-outage demo-resubmit demo-late-file"
    }
    "build"   { Ensure-Env; Invoke-Native docker compose build }
    "up"      {
        Ensure-Env
        Invoke-Native docker compose up -d --build
        Invoke-Native python scripts/wait_for_stack.py
        Show-Urls
    }
    "up-tools" {
        Ensure-Env
        Invoke-Native docker compose --profile tools up -d --build
        Invoke-Native python scripts/wait_for_stack.py
    }
    "wait"    { Invoke-Native python scripts/wait_for_stack.py }
    "urls"    { Show-Urls }
    "down"    { Invoke-Native docker compose --profile tools down }
    "reset"   {
        Invoke-Native docker compose --profile tools down -v --remove-orphans
        Remove-Item -ErrorAction SilentlyContinue reports\*.html, reports\*.csv
        Write-Host "reset complete - the next `up` starts simulated day 1 again"
    }
    "ps"      { Invoke-Native docker compose ps }
    "logs"    {
        $svc = if ($Rest) { $Rest[0] } else { "speed" }
        & docker compose logs -f --tail=200 $svc
    }
    "test"    {
        Invoke-Native docker compose run --rm --no-deps `
          -v "${PSScriptRoot}/tests:/opt/fleet/tests:ro" `
          -v "${PSScriptRoot}/api:/opt/fleet/api:ro" `
          -v "${PSScriptRoot}/simulators:/opt/fleet/simulators:ro" `
          -v "${PSScriptRoot}/streaming:/opt/fleet/streaming:ro" `
          -e PYTHONPATH=/opt/fleet `
          --entrypoint bash airflow -lc `
          "cd /opt/fleet && python -m pytest tests -q"
    }
    "smoke"          { Invoke-Native python scripts/smoke_test.py }
    "demo-idle"      { Invoke-Native python scripts/demo.py idle }
    "demo-outage"    { Invoke-Native python scripts/demo.py outage }
    "demo-resubmit"  { Invoke-Native python scripts/demo.py resubmit }
    "demo-late-file" { Invoke-Native python scripts/demo.py late-file }
    default { Write-Error "unknown target '$Target'"; exit 1 }
}
