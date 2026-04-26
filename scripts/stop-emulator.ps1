<#
.SYNOPSIS
    Stops all running goccy/bigquery-emulator Docker containers.
#>

Write-Host "Stopping BigQuery emulator containers..." -ForegroundColor Cyan

$containers = docker ps --filter "ancestor=ghcr.io/goccy/bigquery-emulator" --format "{{.ID}}" 2>$null

if (-not $containers) {
    Write-Host "No emulator containers running." -ForegroundColor Yellow
    exit 0
}

foreach ($id in $containers) {
    Write-Host "Stopping container: $id" -ForegroundColor Gray
    docker stop $id 2>$null
}

Write-Host "All emulator containers stopped." -ForegroundColor Green
