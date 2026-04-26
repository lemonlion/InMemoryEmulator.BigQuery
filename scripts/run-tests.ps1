<#
.SYNOPSIS
    Runs tests against the specified target.

.PARAMETER Target
    The test target: 'inmemory' (default), 'emulator', or 'cloud'.

.PARAMETER Filter
    Optional xunit filter expression.

.PARAMETER Configuration
    Build configuration. Default: Release.
#>
param(
    [ValidateSet('inmemory', 'emulator', 'cloud')]
    [string]$Target = 'inmemory',

    [string]$Filter = '',

    [string]$Configuration = 'Release'
)

$env:BIGQUERY_TEST_TARGET = $Target

$filterArgs = @()
if ($Filter) {
    $filterArgs = @('--filter', $Filter)
}

Write-Host "Running tests against target: $Target" -ForegroundColor Cyan
dotnet test --configuration $Configuration @filterArgs --logger "trx;LogFileName=results.trx"
