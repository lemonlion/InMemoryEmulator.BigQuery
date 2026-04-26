<#
.SYNOPSIS
    Starts the goccy/bigquery-emulator Docker container.
#>
param(
    [string]$Image = 'ghcr.io/goccy/bigquery-emulator:0.6.6',
    [int]$RestPort = 9050,
    [string]$ProjectId = 'test-project'
)

Write-Host "Starting BigQuery emulator container..." -ForegroundColor Cyan
Write-Host "Image: $Image" -ForegroundColor Gray
Write-Host "REST port: $RestPort" -ForegroundColor Gray
Write-Host "Project ID: $ProjectId" -ForegroundColor Gray

$containerId = docker run -d --rm `
    -p "${RestPort}:${RestPort}" `
    $Image `
    --project=$ProjectId

if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to start emulator container"
    exit 1
}

Write-Host "Container started: $containerId" -ForegroundColor Green

# Health check
$healthUrl = "http://localhost:${RestPort}/bigquery/v2/projects/${ProjectId}/datasets"
$maxRetries = 20
for ($i = 0; $i -lt $maxRetries; $i++) {
    try {
        $response = Invoke-WebRequest -Uri $healthUrl -UseBasicParsing -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            Write-Host "Emulator is healthy!" -ForegroundColor Green
            Write-Host "REST API: http://localhost:${RestPort}" -ForegroundColor Cyan
            exit 0
        }
    }
    catch {
        # Not ready yet
    }
    Start-Sleep -Milliseconds (500 * ($i + 1))
    Write-Host "  Waiting for emulator to become healthy (attempt $($i + 1)/$maxRetries)..." -ForegroundColor Yellow
}

Write-Error "Emulator failed to become healthy after $maxRetries retries"
docker stop $containerId 2>$null
exit 1
