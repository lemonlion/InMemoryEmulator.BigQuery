$file = (Resolve-Path 'src\BigQuery.InMemoryEmulator\SqlEngine\QueryExecutor.cs').Path
$content = [System.IO.File]::ReadAllText($file)

$old = @'
_ => throw new NotSupportedException($"Unknown function: {func.FunctionName}")
};
}

private object? EvaluateAggregate
'@

$replacement = [System.IO.File]::ReadAllText((Resolve-Path 'phase17c_replacement.txt').Path)

if (-not $content.Contains($old)) {
    Write-Host 'ERROR: Could not find old text'
    exit 1
}

$content = $content.Replace($old, $replacement)
[System.IO.File]::WriteAllText($file, $content)
Write-Host 'UDF evaluation added'
