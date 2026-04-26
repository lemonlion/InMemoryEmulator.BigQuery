$file = 'src\BigQuery.InMemoryEmulator\InMemoryDataset.cs'
$content = Get-Content $file -Raw
$content = $content.Replace(
    'internal ConcurrentDictionary<string, InMemoryTable> Tables { get; } = new();',
    'internal ConcurrentDictionary<string, InMemoryTable> Tables { get; } = new();
	internal ConcurrentDictionary<string, InMemoryRoutine> Routines { get; } = new();')
[System.IO.File]::WriteAllText((Resolve-Path $file).Path, $content)
Write-Host "Added Routines to InMemoryDataset"
