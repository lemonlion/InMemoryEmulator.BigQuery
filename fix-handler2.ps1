$f = 'src\BigQuery.InMemoryEmulator\FakeBigQueryHandler.cs'
$c = [IO.File]::ReadAllText($f)

$semi = [char]59  # semicolon

# 1. Update JobRoute regex to capture /cancel action
$c = $c.Replace(
    '@"/bigquery/v2/projects/(?<project>[^/]+)/jobs(?:/(?<job>[^/?]+))?",',
    '@"/bigquery/v2/projects/(?<project>[^/]+)/jobs(?:/(?<job>[^/?]+)(?:/(?<action>[^/?]+))?)?",')

# 2. Add cancel route in RouteJob
$old2 = "if (method == HttpMethod.Post && !hasJobId)" + "`r`n" + "`t`t`treturn await HandleInsertJob(request);"
$new2 = 'var action = match.Groups["action"].Value;' + "`r`n`r`n" + "`t`tif (method == HttpMethod.Post && hasJobId && action == `"cancel`")" + "`r`n" + "`t`t`treturn HandleCancelJob(jobId);" + "`r`n`r`n" + "`t`tif (method == HttpMethod.Post && !hasJobId)" + "`r`n" + "`t`t`treturn await HandleInsertJob(request);"
$c = $c.Replace($old2, $new2)

# 3. Add labels from QueryRequest in HandleSyncQuery
$old3 = "Parameters = body.QueryParameters," + "`r`n" + "`t`t`tStatementType = `"SELECT`"," + "`r`n" + "`t`t};"
$new3 = "Parameters = body.QueryParameters," + "`r`n" + "`t`t`tStatementType = `"SELECT`"," + "`r`n" + "`t`t`tLabels = body.Labels," + "`r`n" + "`t`t};"
$c = $c.Replace($old3, $new3)

# 4. Add dry run in HandleSyncQuery (before try block)
$old4 = "`t`ttry" + "`r`n" + "`t`t{" + "`r`n" + "`t`t`t// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language"
$new4 = @"
		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
		//   "dryRun: If set to true, BigQuery doesn't run the job."
		if (body.DryRun == true)
		{
			job.IsDryRun = true;
			Jobs[job.JobId] = job;
			var dryResponse = new QueryResponse
			{
				Kind = "bigquery#queryResponse",
				JobReference = new JobReference { ProjectId = _store.ProjectId, JobId = job.JobId },
				JobComplete = true,
				TotalBytesProcessed = 0,
			};
			return BuildJsonResponse(dryResponse);
		}

		try
		{
			// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
"@
$c = $c.Replace($old4, $new4)

# 5. Add labels + dryRun + script in HandleInsertJob
$old5a = "StatementType = `"SELECT`"," + "`r`n" + "`t`t};" + "`r`n`r`n" + "`t`ttry" + "`r`n" + "`t`t{" + "`r`n" + "`t`t`tvar executor = new QueryExecutor(_store, defaultDatasetId);" + "`r`n" + "`t`t`texecutor.SetParameters(queryConfig.QueryParameters);" + "`r`n" + "`t`t`tvar (schema, rows) = executor.Execute(queryConfig.Query);"

$containsSemi = "queryConfig.Query.Contains('" + $semi + "')"
$new5a = "StatementType = `"SELECT`"," + "`r`n" + "`t`t`tLabels = body?.Configuration?.Labels," + "`r`n" + "`t`t};" + "`r`n`r`n" + "`t`t// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfiguration" + "`r`n" + "`t`t//   `"dryRun: If set, don't actually run this job.`"" + "`r`n" + "`t`tif (body?.Configuration?.DryRun == true)" + "`r`n" + "`t`t{" + "`r`n" + "`t`t`tjob.IsDryRun = true;" + "`r`n" + "`t`t`tJobs[job.JobId] = job;" + "`r`n" + "`t`t`treturn BuildJsonResponse(job.ToJobResource());" + "`r`n" + "`t`t}" + "`r`n`r`n" + "`t`ttry" + "`r`n" + "`t`t{" + "`r`n" + "`t`t`t// Support multi-statement scripts" + "`r`n" + "`t`t`tTableSchema schema;" + "`r`n" + "`t`t`tList<TableRow> rows;" + "`r`n" + "`t`t`tif ($containsSemi)" + "`r`n" + "`t`t`t{" + "`r`n" + "`t`t`t`tvar procExecutor = new SqlEngine.ProceduralExecutor(_store, defaultDatasetId);" + "`r`n" + "`t`t`t`t(schema, rows) = procExecutor.Execute(queryConfig.Query);" + "`r`n" + "`t`t`t}" + "`r`n" + "`t`t`telse" + "`r`n" + "`t`t`t{" + "`r`n" + "`t`t`t`tvar executor = new QueryExecutor(_store, defaultDatasetId);" + "`r`n" + "`t`t`t`texecutor.SetParameters(queryConfig.QueryParameters);" + "`r`n" + "`t`t`t`t(schema, rows) = executor.Execute(queryConfig.Query);" + "`r`n" + "`t`t`t}"

$c = $c.Replace($old5a, $new5a)

[IO.File]::WriteAllText($f, $c)
Write-Output "Updated FakeBigQueryHandler"
