$f = 'src\BigQuery.InMemoryEmulator\InMemoryJob.cs'
$c = [IO.File]::ReadAllText($f)

# Add IsDryRun property
$c = $c.Replace(
    "public IDictionary<string, string>? Labels { get; set; }",
    "public IDictionary<string, string>? Labels { get; set; }`r`n`tpublic bool IsDryRun { get; set; }")

# Add Labels to ToJobResource
$c = $c.Replace(
    "Query = new JobConfigurationQuery { Query = Query, UseLegacySql = false }",
    "Query = new JobConfigurationQuery { Query = Query, UseLegacySql = false },`r`n`t`t`tLabels = Labels as Dictionary<string, string> ?? Labels?.ToDictionary(kv => kv.Key, kv => kv.Value),`r`n`t`t`tDryRun = IsDryRun ? true : null")

# Add StatementType and NumDmlAffectedRows to Statistics.Query
$c = $c.Replace(
    "TotalBytesProcessed = TotalBytesProcessed,`r`n`t`t`t`tTotalBytesBilled = 0,`r`n`t`t`t`tCacheHit = false,",
    "TotalBytesProcessed = TotalBytesProcessed,`r`n`t`t`t`tTotalBytesBilled = 0,`r`n`t`t`t`tCacheHit = false,`r`n`t`t`t`tStatementType = StatementType,`r`n`t`t`t`tNumDmlAffectedRows = NumDmlAffectedRows > 0 ? NumDmlAffectedRows : null,")

[IO.File]::WriteAllText($f, $c)
Write-Output "Updated InMemoryJob"
