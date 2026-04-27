$f = 'src\BigQuery.InMemoryEmulator\FakeBigQueryHandler.cs'
$c = [IO.File]::ReadAllText($f)

# 1. Update JobRoute regex to capture /cancel action
$c = $c.Replace(
    '@"/bigquery/v2/projects/(?<project>[^/]+)/jobs(?:/(?<job>[^/?]+))?",',
    '@"/bigquery/v2/projects/(?<project>[^/]+)/jobs(?:/(?<job>[^/?]+)(?:/(?<action>[^/?]+))?)?",')

# 2. Add cancel route in RouteJob
$c = $c.Replace(
    'if (method == HttpMethod.Post && !hasJobId)' + "`r`n" + '			return await HandleInsertJob(request);',
    'var action = match.Groups["action"].Value;' + "`r`n" + "`r`n" + '		if (method == HttpMethod.Post && hasJobId && action == "cancel")' + "`r`n" + '			return HandleCancelJob(jobId);' + "`r`n`r`n" + '		if (method == HttpMethod.Post && !hasJobId)' + "`r`n" + '			return await HandleInsertJob(request);')

# 3. Add labels from QueryRequest in HandleSyncQuery
$c = $c.Replace(
    'Parameters = body.QueryParameters,' + "`r`n" + '			StatementType = "SELECT",' + "`r`n" + '		};',
    'Parameters = body.QueryParameters,' + "`r`n" + '			StatementType = "SELECT",' + "`r`n" + '			Labels = body.Labels,' + "`r`n" + '		};')

# 4. Add dry run in HandleSyncQuery (before try block)
$c = $c.Replace(
    'try' + "`r`n" + '		{' + "`r`n" + '			// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language',
    '// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query' + "`r`n" + '		//   "dryRun: If set to true, BigQuery doesn''t run the job."' + "`r`n" + '		if (body.DryRun == true)' + "`r`n" + '		{' + "`r`n" + '			job.IsDryRun = true;' + "`r`n" + '			Jobs[job.JobId] = job;' + "`r`n" + '			var dryResponse = new QueryResponse' + "`r`n" + '			{' + "`r`n" + '				Kind = "bigquery#queryResponse",' + "`r`n" + '				JobReference = new JobReference { ProjectId = _store.ProjectId, JobId = job.JobId },' + "`r`n" + '				JobComplete = true,' + "`r`n" + '				TotalBytesProcessed = 0,' + "`r`n" + '			};' + "`r`n" + '			return BuildJsonResponse(dryResponse);' + "`r`n" + '		}' + "`r`n`r`n" + '		try' + "`r`n" + '		{' + "`r`n" + '			// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language')

# 5. Add labels + dryRun + script support in HandleInsertJob
$c = $c.Replace(
    'var job = new InMemoryJob(_store.ProjectId, jobId)' + "`r`n" + '		{' + "`r`n" + '			Query = queryConfig.Query,' + "`r`n" + '			DefaultDatasetId = defaultDatasetId,' + "`r`n" + '			Parameters = queryConfig.QueryParameters,' + "`r`n" + '			StatementType = "SELECT",' + "`r`n" + '		};' + "`r`n`r`n" + '		try' + "`r`n" + '		{' + "`r`n" + '			var executor = new QueryExecutor(_store, defaultDatasetId);' + "`r`n" + '			executor.SetParameters(queryConfig.QueryParameters);' + "`r`n" + '			var (schema, rows) = executor.Execute(queryConfig.Query);',
    'var job = new InMemoryJob(_store.ProjectId, jobId)' + "`r`n" + '		{' + "`r`n" + '			Query = queryConfig.Query,' + "`r`n" + '			DefaultDatasetId = defaultDatasetId,' + "`r`n" + '			Parameters = queryConfig.QueryParameters,' + "`r`n" + '			StatementType = "SELECT",' + "`r`n" + '			Labels = body?.Configuration?.Labels,' + "`r`n" + '		};' + "`r`n`r`n" + '		// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#JobConfiguration' + "`r`n" + '		//   "dryRun: If set, don''t actually run this job."' + "`r`n" + '		if (body?.Configuration?.DryRun == true)' + "`r`n" + '		{' + "`r`n" + '			job.IsDryRun = true;' + "`r`n" + '			Jobs[job.JobId] = job;' + "`r`n" + '			return BuildJsonResponse(job.ToJobResource());' + "`r`n" + '		}' + "`r`n`r`n" + '		try' + "`r`n" + '		{' + "`r`n" + '			// Support multi-statement scripts' + "`r`n" + '			TableSchema schema;' + "`r`n" + '			List<TableRow> rows;' + "`r`n" + '			if (queryConfig.Query.Contains(''';'''))' + "`r`n" + '			{' + "`r`n" + '				var procExecutor = new SqlEngine.ProceduralExecutor(_store, defaultDatasetId);' + "`r`n" + '				(schema, rows) = procExecutor.Execute(queryConfig.Query);' + "`r`n" + '			}' + "`r`n" + '			else' + "`r`n" + '			{' + "`r`n" + '				var executor = new QueryExecutor(_store, defaultDatasetId);' + "`r`n" + '				executor.SetParameters(queryConfig.QueryParameters);' + "`r`n" + '				(schema, rows) = executor.Execute(queryConfig.Query);' + "`r`n" + '			}')

# 6. Fix the old executor lines that followed - remove the old ones
# Actually the Replace above should have captured the entire old block. Let me check by removing the extra closing brace issue.
# The old code after was: schema/rows assignment + job.ResultSchema etc. Need to make sure the new code flows correctly.

[IO.File]::WriteAllText($f, $c)
Write-Output "Updated FakeBigQueryHandler"
