$file = 'src\BigQuery.InMemoryEmulator\SqlEngine\ProceduralExecutor.cs'
$content = Get-Content $file -Raw

# Add CREATE FUNCTION handling
$old = '		// CREATE TEMP TABLE'
$new = '		// CREATE [OR REPLACE] FUNCTION
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
		if (upper.StartsWith("CREATE FUNCTION", StringComparison.OrdinalIgnoreCase) ||
			upper.StartsWith("CREATE OR REPLACE FUNCTION", StringComparison.OrdinalIgnoreCase) ||
			upper.StartsWith("CREATE TEMP FUNCTION", StringComparison.OrdinalIgnoreCase) ||
			upper.StartsWith("CREATE OR REPLACE TEMP FUNCTION", StringComparison.OrdinalIgnoreCase))
			return ExecuteCreateFunction(sql);

		// DROP FUNCTION
		if (upper.StartsWith("DROP FUNCTION", StringComparison.OrdinalIgnoreCase))
			return ExecuteDropFunction(sql);

		// CREATE TEMP TABLE'

$content = $content.Replace($old, $new)

# Add execution methods
$methods = @'

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteCreateFunction(string sql)
	{
		var orReplace = sql.Contains("OR REPLACE", StringComparison.OrdinalIgnoreCase);
		var isTemp = sql.Contains("TEMP ", StringComparison.OrdinalIgnoreCase);
		
		// Parse: CREATE [OR REPLACE] [TEMP] FUNCTION [dataset.]name(params) [RETURNS type] AS (body)
		var match = System.Text.RegularExpressions.Regex.Match(sql,
			@"FUNCTION\s+(\w+(?:\.\w+)?)\s*\(([^)]*)\)\s*(?:RETURNS\s+(\w+)\s+)?AS\s*\((.+)\)\s*$",
			System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);

		if (!match.Success)
			throw new InvalidOperationException("Cannot parse CREATE FUNCTION statement.");

		var fullName = match.Groups[1].Value;
		var paramsStr = match.Groups[2].Value;
		var returnType = match.Groups[3].Success ? match.Groups[3].Value : null;
		var body = match.Groups[4].Value.Trim();

		string datasetId;
		string funcName;
		if (fullName.Contains('.'))
		{
			var parts = fullName.Split('.', 2);
			datasetId = parts[0];
			funcName = parts[1];
		}
		else
		{
			datasetId = isTemp ? "_temp" : (_defaultDatasetId ?? throw new InvalidOperationException("No default dataset."));
			funcName = fullName;
		}

		// Parse parameters
		var parameters = new List<(string Name, string Type)>();
		if (!string.IsNullOrWhiteSpace(paramsStr))
		{
			foreach (var p in paramsStr.Split(','))
			{
				var paramParts = p.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
				if (paramParts.Length >= 2)
					parameters.Add((paramParts[0], paramParts[1]));
			}
		}

		// Ensure dataset exists
		if (!_store.Datasets.ContainsKey(datasetId))
			_store.Datasets[datasetId] = new InMemoryDataset(datasetId);

		var dataset = _store.Datasets[datasetId];
		if (dataset.Routines.ContainsKey(funcName) && !orReplace)
			throw new InvalidOperationException($"Already Exists: Function {funcName}");

		var routine = new InMemoryRoutine(datasetId, funcName, "SCALAR_FUNCTION", "SQL", body, parameters, returnType);
		dataset.Routines[funcName] = routine;
		return null;
	}

	private (TableSchema Schema, List<TableRow> Rows)? ExecuteDropFunction(string sql)
	{
		var ifExists = sql.Contains("IF EXISTS", StringComparison.OrdinalIgnoreCase);
		var match = System.Text.RegularExpressions.Regex.Match(sql,
			@"DROP\s+FUNCTION\s+(?:IF\s+EXISTS\s+)?(\w+(?:\.\w+)?)",
			System.Text.RegularExpressions.RegexOptions.IgnoreCase);

		if (!match.Success)
			throw new InvalidOperationException("Cannot parse DROP FUNCTION statement.");

		var fullName = match.Groups[1].Value;
		string datasetId;
		string funcName;
		if (fullName.Contains('.'))
		{
			var parts = fullName.Split('.', 2);
			datasetId = parts[0];
			funcName = parts[1];
		}
		else
		{
			datasetId = _defaultDatasetId ?? throw new InvalidOperationException("No default dataset.");
			funcName = fullName;
		}

		if (_store.Datasets.TryGetValue(datasetId, out var dataset))
		{
			if (!dataset.Routines.TryRemove(funcName, out _) && !ifExists)
				throw new InvalidOperationException($"Not found: Function {funcName}");
		}
		else if (!ifExists)
		{
			throw new InvalidOperationException($"Not found: Dataset {datasetId}");
		}
		return null;
	}

'@

$content = $content.Replace(
    '	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#while',
    "$methods`t// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#while")

[System.IO.File]::WriteAllText((Resolve-Path $file).Path, $content)
Write-Host "Phase 17 function support added"
