using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.InMemoryEmulator.SqlEngine;

/// <summary>
/// Executes multi-statement (procedural) BigQuery scripts.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
/// </summary>
internal class ProceduralExecutor
{
	private readonly InMemoryDataStore _store;
	private readonly string? _defaultDatasetId;
	private readonly Dictionary<string, object?> _variables = new(StringComparer.OrdinalIgnoreCase);
	private readonly HashSet<string> _tempTableNames = new(StringComparer.OrdinalIgnoreCase);
	#pragma warning disable CS0169
	private long _rowCount;
#pragma warning restore CS0169

	public ProceduralExecutor(InMemoryDataStore store, string? defaultDatasetId = null)
	{
		_store = store;
		_defaultDatasetId = defaultDatasetId;
	}

	/// <summary>
	/// Executes a multi-statement script. Statements are separated by semicolons.
	/// Returns the result of the last statement that produces output.
	/// </summary>
	public (TableSchema Schema, List<TableRow> Rows) Execute(string script)
	{
		var statements = SplitStatements(script);
		(TableSchema Schema, List<TableRow> Rows)? lastResult = null;

		foreach (var stmt in statements)
		{
			var trimmed = stmt.Trim();
			if (string.IsNullOrEmpty(trimmed)) continue;

			var result = ExecuteStatement(trimmed);
			if (result.HasValue)
				lastResult = result.Value;
		}

		return lastResult ?? (new TableSchema { Fields = [] }, []);
	}

	private (TableSchema Schema, List<TableRow> Rows)? ExecuteStatement(string sql)
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
		var upper = sql.TrimStart();

		// Strip labels: "label_name: BEGIN ..." → "BEGIN ..."
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#labels
		var labelMatch = System.Text.RegularExpressions.Regex.Match(upper,
			@"^(\w+)\s*:\s*(BEGIN|LOOP|WHILE|FOR|REPEAT)\b",
			System.Text.RegularExpressions.RegexOptions.IgnoreCase);
		if (labelMatch.Success)
		{
			sql = upper.Substring(labelMatch.Groups[1].Length + 1).TrimStart().TrimStart(':').TrimStart();
			upper = sql.TrimStart();
			// Also strip trailing "END label_name" → "END"
			var label = labelMatch.Groups[1].Value;
			sql = System.Text.RegularExpressions.Regex.Replace(sql,
				@"\bEND\s+(LOOP|WHILE|FOR)\s+" + System.Text.RegularExpressions.Regex.Escape(label) + @"\s*$",
				"END $1", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
			sql = System.Text.RegularExpressions.Regex.Replace(sql,
				@"\bEND\s+" + System.Text.RegularExpressions.Regex.Escape(label) + @"\s*$",
				"END", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
			upper = sql.TrimStart();
		}

		// DECLARE var [, var2] type [DEFAULT expr]
		if (upper.StartsWith("DECLARE ", StringComparison.OrdinalIgnoreCase))
			return ExecuteDeclare(sql);

		// SET var = expr
		if (upper.StartsWith("SET ", StringComparison.OrdinalIgnoreCase))
			return ExecuteSet(sql);

		// IF ... THEN ... END IF
		if (upper.StartsWith("IF ", StringComparison.OrdinalIgnoreCase))
			return ExecuteIf(sql);

		// ASSERT condition [AS 'message']
		if (upper.StartsWith("ASSERT ", StringComparison.OrdinalIgnoreCase))
		{
			ExecuteAssert(sql);
			return null;
		}

		// RAISE [USING MESSAGE = 'text']
		if (upper.StartsWith("RAISE", StringComparison.OrdinalIgnoreCase))
		{
			ExecuteRaise(sql);
			return null;
		}

		// RETURN
		if (upper.Equals("RETURN", StringComparison.OrdinalIgnoreCase))
			throw new ReturnException();

		// BREAK / LEAVE
		if (upper.Equals("BREAK", StringComparison.OrdinalIgnoreCase) || upper.Equals("LEAVE", StringComparison.OrdinalIgnoreCase))
			throw new BreakException();

		// CONTINUE / ITERATE
		if (upper.Equals("CONTINUE", StringComparison.OrdinalIgnoreCase) || upper.Equals("ITERATE", StringComparison.OrdinalIgnoreCase))
			throw new ContinueException();

		// Transaction stubs (no-op in emulator) — must come before BEGIN...END check
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#begin_transaction
		if (upper.Equals("BEGIN TRANSACTION", StringComparison.OrdinalIgnoreCase) ||
			upper.Equals("COMMIT TRANSACTION", StringComparison.OrdinalIgnoreCase) ||
			upper.Equals("ROLLBACK TRANSACTION", StringComparison.OrdinalIgnoreCase) ||
			upper.Equals("COMMIT", StringComparison.OrdinalIgnoreCase) ||
			upper.Equals("ROLLBACK", StringComparison.OrdinalIgnoreCase))
			return null;

		// BEGIN ... END
		if (upper.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase))
			return ExecuteBeginEnd(sql);

		// WHILE condition DO stmts END WHILE
		if (upper.StartsWith("WHILE ", StringComparison.OrdinalIgnoreCase))
			return ExecuteWhile(sql);

		// LOOP stmts END LOOP
		if (upper.StartsWith("LOOP", StringComparison.OrdinalIgnoreCase))
			return ExecuteLoop(sql);

		// EXECUTE IMMEDIATE
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#execute_immediate
		if (upper.StartsWith("EXECUTE IMMEDIATE", StringComparison.OrdinalIgnoreCase))
			return ExecuteImmediate(sql);

		// FOR ... IN (query) DO stmts END FOR
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#for-in
		if (upper.StartsWith("FOR ", StringComparison.OrdinalIgnoreCase))
			return ExecuteForIn(sql);

		// REPEAT stmts UNTIL condition END REPEAT
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#repeat
		if (upper.StartsWith("REPEAT", StringComparison.OrdinalIgnoreCase) &&
			!upper.StartsWith("REPEAT(", StringComparison.OrdinalIgnoreCase))
			return ExecuteRepeat(sql);

		// Procedural CASE
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#case_statement
		if (upper.StartsWith("CASE ", StringComparison.OrdinalIgnoreCase) &&
			upper.Contains("END CASE", StringComparison.OrdinalIgnoreCase))
			return ExecuteCaseStatement(sql);

		// CALL procedure_name(args)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#call
		if (upper.StartsWith("CALL ", StringComparison.OrdinalIgnoreCase))
			return ExecuteCall(sql);

		// CREATE [OR REPLACE] FUNCTION
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
		if (upper.StartsWith("CREATE FUNCTION", StringComparison.OrdinalIgnoreCase) ||
			upper.StartsWith("CREATE OR REPLACE FUNCTION", StringComparison.OrdinalIgnoreCase) ||
			upper.StartsWith("CREATE TEMP FUNCTION", StringComparison.OrdinalIgnoreCase) ||
			upper.StartsWith("CREATE OR REPLACE TEMP FUNCTION", StringComparison.OrdinalIgnoreCase))
			return ExecuteCreateFunction(sql);

		// DROP FUNCTION
		if (upper.StartsWith("DROP FUNCTION", StringComparison.OrdinalIgnoreCase))
			return ExecuteDropFunction(sql);

		// CREATE TEMP TABLE ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â session-scoped
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#create_temp_table
		if (upper.StartsWith("CREATE TEMP", StringComparison.OrdinalIgnoreCase) ||
			upper.StartsWith("CREATE TEMPORARY", StringComparison.OrdinalIgnoreCase))
			return ExecuteCreateTempTable(sql);

		// Regular SQL (SELECT, DML, DDL)
		var substituted = SubstituteVariables(sql);
		substituted = QualifyTempTables(substituted);
		var executor = new QueryExecutor(_store, _defaultDatasetId);
		var result = executor.Execute(substituted);
		return (result.Schema, result.Rows);
	}

	private QueryExecutor CreateQueryExecutor()
	{
		return new QueryExecutor(_store, _defaultDatasetId);
	}

	/// <summary>
	/// Substitutes bare variable references in SQL with their literal values.
	/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
	///   "Variables declared in a script can be used in subsequent statements."
	/// </summary>
	private string SubstituteVariables(string sql)
	{
		if (_variables.Count == 0) return sql;
		// Sort by name length descending to avoid partial replacements (e.g., "total" vs "total_sum")
		foreach (var (name, value) in _variables.OrderByDescending(kv => kv.Key.Length))
		{
			var literal = FormatLiteral(value);
			sql = System.Text.RegularExpressions.Regex.Replace(
				sql,
				@"(?<![`@"".\w])\b" + System.Text.RegularExpressions.Regex.Escape(name) + @"\b(?![`"".\w])",
				literal,
				System.Text.RegularExpressions.RegexOptions.IgnoreCase);
		}
		return sql;
	}

	/// <summary>
	/// Qualifies bare temp table references with the _temp dataset prefix using backtick syntax.
	/// </summary>
	private string QualifyTempTables(string sql)
	{
		if (_tempTableNames.Count == 0) return sql;
		foreach (var tableName in _tempTableNames.OrderByDescending(n => n.Length))
		{
			sql = System.Text.RegularExpressions.Regex.Replace(
				sql,
				@"(?<![`.\w])\b" + System.Text.RegularExpressions.Regex.Escape(tableName) + @"\b(?![`.\w])",
				"`_temp." + tableName + "`",
				System.Text.RegularExpressions.RegexOptions.IgnoreCase);
		}
		return sql;
	}

	private static string FormatLiteral(object? value) => value switch
	{
		null => "NULL",
		true => "TRUE",
		false => "FALSE",
		long l => l.ToString(),
		double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
		string s when long.TryParse(s, out var l) => l.ToString(),
		string s when double.TryParse(s, System.Globalization.NumberStyles.Any,
			System.Globalization.CultureInfo.InvariantCulture, out var d) =>
			d.ToString(System.Globalization.CultureInfo.InvariantCulture),
		string s when s.Equals("true", StringComparison.OrdinalIgnoreCase) => "TRUE",
		string s when s.Equals("false", StringComparison.OrdinalIgnoreCase) => "FALSE",
		string s => $"'{s.Replace("'", "''")}'",
		_ => value.ToString() ?? "NULL"
	};

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#declare
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteDeclare(string sql)
	{
		// Simple parsing: DECLARE var_name [, var2] TYPE [DEFAULT expr]
		var body = sql.Substring("DECLARE ".Length).Trim();

		// Check for DEFAULT
		string? defaultExpr = null;
		var defaultIdx = body.IndexOf(" DEFAULT ", StringComparison.OrdinalIgnoreCase);
		if (defaultIdx >= 0)
		{
			defaultExpr = body.Substring(defaultIdx + " DEFAULT ".Length).Trim();
			body = body.Substring(0, defaultIdx).Trim();
		}

		// Split by last space to get type
		var parts = body.Split(' ', StringSplitOptions.RemoveEmptyEntries);
		string? typeName = null;
		string[] varNames;

		if (parts.Length >= 2)
		{
			typeName = parts[^1];
			varNames = string.Join(" ", parts[..^1]).Split(',', StringSplitOptions.TrimEntries);
		}
		else
		{
			varNames = [parts[0]];
		}

		object? defaultValue = null;
		if (defaultExpr != null)
		{
			var substituted = SubstituteVariables(defaultExpr);
			var exec = CreateQueryExecutor();
			var (_, rows) = exec.Execute($"SELECT {substituted}");
			if (rows.Count > 0 && rows[0].F.Count > 0)
				defaultValue = rows[0].F[0].V;
		}

		foreach (var name in varNames)
		{
			_variables[name.Trim()] = defaultValue;
		}
		return null;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#set
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteSet(string sql)
	{
		var body = sql.Substring("SET ".Length).Trim();
		var eqIdx = body.IndexOf('=');
		if (eqIdx < 0) throw new InvalidOperationException("SET statement requires '=' assignment.");

		var varName = body.Substring(0, eqIdx).Trim();
		var expr = body.Substring(eqIdx + 1).Trim();

		var substituted = SubstituteVariables(expr);
		substituted = QualifyTempTables(substituted);
		var exec = CreateQueryExecutor();
		var (_, rows) = exec.Execute($"SELECT {substituted}");
		_variables[varName] = rows.Count > 0 && rows[0].F.Count > 0 ? rows[0].F[0].V : null;
		return null;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#if
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteIf(string sql)
	{
		// Parse IF condition THEN stmts [ELSEIF condition THEN stmts] [ELSE stmts] END IF
		var body = sql.Substring("IF ".Length).Trim();

		// Remove trailing END IF
		if (body.EndsWith("END IF", StringComparison.OrdinalIgnoreCase))
			body = body.Substring(0, body.Length - "END IF".Length).Trim();

		// Split on THEN — allow whitespace (including newlines) around THEN
		var thenMatch = System.Text.RegularExpressions.Regex.Match(body,
			@"\bTHEN\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
		if (!thenMatch.Success) throw new InvalidOperationException("IF statement requires THEN.");

		var condition = body.Substring(0, thenMatch.Index).Trim();
		var rest = body.Substring(thenMatch.Index + thenMatch.Length).Trim();

		// Check for ELSE
		string? elseBlock = null;
		var elseIdx = FindTopLevelKeyword(rest, "ELSE");
		if (elseIdx >= 0)
		{
			elseBlock = rest.Substring(elseIdx + "ELSE".Length).Trim();
			rest = rest.Substring(0, elseIdx).Trim();
		}

		// Evaluate condition
		var substituted = SubstituteVariables(condition);
		var exec = CreateQueryExecutor();
		var (_, condRows) = exec.Execute($"SELECT {substituted}");
		var condValue = condRows.Count > 0 && condRows[0].F.Count > 0 ? condRows[0].F[0].V : null;
		var isTruthy = condValue is true || (condValue is long l && l != 0) || (condValue is string s && bool.TryParse(s, out var b) && b);

		if (isTruthy)
		{
			return ExecuteBlock(rest);
		}
		else if (elseBlock != null)
		{
			return ExecuteBlock(elseBlock);
		}
		return null;
	}

	private (TableSchema Schema, List<TableRow> Rows)? ExecuteBlock(string block)
	{
		var stmts = SplitStatements(block);
		(TableSchema Schema, List<TableRow> Rows)? lastResult = null;
		foreach (var stmt in stmts)
		{
			var trimmed = stmt.Trim();
			if (string.IsNullOrEmpty(trimmed)) continue;
			var result = ExecuteStatement(trimmed);
			if (result.HasValue) lastResult = result.Value;
		}
		return lastResult;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#begin
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteBeginEnd(string sql)
	{
		var body = sql.Trim();
		if (!body.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase))
			throw new InvalidOperationException("Expected BEGIN.");

		body = body.Substring("BEGIN".Length).Trim();

		// Check for EXCEPTION
		string? exceptionBlock = null;
		var excIdx = FindTopLevelKeyword(body, "EXCEPTION");
		string mainBody;
		if (excIdx >= 0)
		{
			mainBody = body.Substring(0, excIdx).Trim();
			var afterExc = body.Substring(excIdx + "EXCEPTION".Length).Trim();
			// Remove END
			if (afterExc.EndsWith("END", StringComparison.OrdinalIgnoreCase))
				afterExc = afterExc.Substring(0, afterExc.Length - "END".Length).Trim();
			// Remove WHEN ERROR THEN
			if (afterExc.StartsWith("WHEN ERROR THEN", StringComparison.OrdinalIgnoreCase))
				afterExc = afterExc.Substring("WHEN ERROR THEN".Length).Trim();
			exceptionBlock = afterExc;
		}
		else
		{
			// Remove END
			if (body.EndsWith("END", StringComparison.OrdinalIgnoreCase))
				body = body.Substring(0, body.Length - "END".Length).Trim();
			mainBody = body;
		}

		try
		{
			return ExecuteBlock(mainBody);
		}
		catch (Exception ex) when (exceptionBlock != null && ex is not ReturnException)
		{
			_variables["@@error.message"] = ex.Message;
			return ExecuteBlock(exceptionBlock);
		}
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#assert
	private void ExecuteAssert(string sql)
	{
		var body = sql.Substring("ASSERT ".Length).Trim();

		string? description = null;
		var asIdx = body.LastIndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
		if (asIdx >= 0)
		{
			description = body.Substring(asIdx + " AS ".Length).Trim().Trim('\'');
			body = body.Substring(0, asIdx).Trim();
		}

		var substituted = SubstituteVariables(body);
		substituted = QualifyTempTables(substituted);
		var exec = CreateQueryExecutor();
		var (_, rows) = exec.Execute($"SELECT {substituted}");
		var value = rows.Count > 0 && rows[0].F.Count > 0 ? rows[0].F[0].V : null;
		var isTruthy = value is true || (value is long l && l != 0) || (value is string s && bool.TryParse(s, out var b) && b);

		if (!isTruthy)
		{
			throw new InvalidOperationException(description ?? "ASSERT failed: expression evaluated to FALSE or NULL.");
		}
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#raise
	private void ExecuteRaise(string sql)
	{
		var body = sql.Substring("RAISE".Length).Trim();
		if (string.IsNullOrEmpty(body))
		{
			// Re-raise: use stored error message
			var msg = _variables.TryGetValue("@@error.message", out var m) ? m?.ToString() : "Unknown error";
			throw new InvalidOperationException(msg);
		}

		// USING MESSAGE = 'text'
		if (body.StartsWith("USING MESSAGE", StringComparison.OrdinalIgnoreCase))
		{
			var eqIdx = body.IndexOf('=');
			if (eqIdx >= 0)
			{
				var msgExpr = body.Substring(eqIdx + 1).Trim();
				var exec = CreateQueryExecutor();
				var (_, rows) = exec.Execute($"SELECT {msgExpr}");
				var msg = rows.Count > 0 && rows[0].F.Count > 0 ? rows[0].F[0].V?.ToString() : "Unknown error";
				throw new InvalidOperationException(msg);
			}
		}
		throw new InvalidOperationException(body);
	}



	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteCreateFunction(string sql)
	{
		var orReplace = sql.Contains("OR REPLACE", StringComparison.OrdinalIgnoreCase);
		var isTemp = sql.Contains("TEMP ", StringComparison.OrdinalIgnoreCase);
		
		// Try JavaScript UDF: FUNCTION name(params) [RETURNS type] LANGUAGE js AS "body"
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#javascript-udf-structure
		var jsMatch = System.Text.RegularExpressions.Regex.Match(sql,
			@"FUNCTION\s+(\w+(?:\.\w+)?)\s*\(([^)]*)\)\s*(?:RETURNS\s+(\w+)\s+)?LANGUAGE\s+js\s+AS\s+(?:r?""""""([\s\S]*?)""""""|""([^""]*)""|'([^']*)')\s*;?\s*$",
			System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);

		if (jsMatch.Success)
		{
			var fullName = jsMatch.Groups[1].Value;
			var paramsStr = jsMatch.Groups[2].Value;
			var returnType = jsMatch.Groups[3].Success ? jsMatch.Groups[3].Value : null;
			// Body from triple-quoted, double-quoted, or single-quoted string
			var body = jsMatch.Groups[4].Success ? jsMatch.Groups[4].Value :
				jsMatch.Groups[5].Success ? jsMatch.Groups[5].Value :
				jsMatch.Groups[6].Value;

			return StoreRoutine(fullName, paramsStr, returnType, body.Trim(), "JAVASCRIPT", isTemp, orReplace);
		}

		// Parse SQL UDF: CREATE [OR REPLACE] [TEMP] FUNCTION [dataset.]name(params) [RETURNS type] AS (body)
		var match = System.Text.RegularExpressions.Regex.Match(sql,
			@"FUNCTION\s+(\w+(?:\.\w+)?)\s*\(([^)]*)\)\s*(?:RETURNS\s+(\w+)\s+)?AS\s*\((.+)\)\s*$",
			System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);

		if (!match.Success)
			throw new InvalidOperationException("Cannot parse CREATE FUNCTION statement.");

		var sqlFullName = match.Groups[1].Value;
		var sqlParamsStr = match.Groups[2].Value;
		var sqlReturnType = match.Groups[3].Success ? match.Groups[3].Value : null;
		var sqlBody = match.Groups[4].Value.Trim();

		return StoreRoutine(sqlFullName, sqlParamsStr, sqlReturnType, sqlBody, "SQL", isTemp, orReplace);
	}

	private (TableSchema Schema, List<TableRow> Rows)? StoreRoutine(
		string fullName, string paramsStr, string? returnType, string body,
		string language, bool isTemp, bool orReplace)
	{
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

		var routine = new InMemoryRoutine(datasetId, funcName, "SCALAR_FUNCTION", language, body, parameters, returnType);
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

		// Check _temp dataset first (TEMP functions shadow persistent ones)
		var found = false;
		if (!fullName.Contains(".") && _store.Datasets.TryGetValue("_temp", out var tempDs))
			found = tempDs.Routines.TryRemove(funcName, out _);

		if (!found && _store.Datasets.TryGetValue(datasetId, out var dataset))
			found = dataset.Routines.TryRemove(funcName, out _);

		if (!found && !ifExists)
			throw new InvalidOperationException($"Not found: Function {funcName}");
return null;
	}
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#while
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteWhile(string sql)
	{
		var body = sql.Substring("WHILE ".Length).Trim();
		// Remove END WHILE
		if (body.EndsWith("END WHILE", StringComparison.OrdinalIgnoreCase))
			body = body.Substring(0, body.Length - "END WHILE".Length).Trim();

		var doIdx = FindTopLevelKeyword(body, "DO");
		if (doIdx < 0) throw new InvalidOperationException("WHILE requires DO.");

		var condition = body.Substring(0, doIdx).Trim();
		var loopBody = body.Substring(doIdx + "DO".Length).Trim();

		(TableSchema Schema, List<TableRow> Rows)? lastResult = null;
		int maxIter = 10000;
		while (maxIter-- > 0)
		{
			var substituted = SubstituteVariables(condition);
			var exec = CreateQueryExecutor();
			var (_, condRows) = exec.Execute($"SELECT {substituted}");
			var condValue = condRows.Count > 0 && condRows[0].F.Count > 0 ? condRows[0].F[0].V : null;
			if (!IsTruthyValue(condValue)) break;

			try
			{
				var result = ExecuteBlock(loopBody);
				if (result.HasValue) lastResult = result.Value;
			}
			catch (BreakException) { break; }
			catch (ContinueException) { continue; }
		}
		return lastResult;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#loop
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteLoop(string sql)
	{
		var body = sql.Trim();
		if (body.StartsWith("LOOP", StringComparison.OrdinalIgnoreCase))
			body = body.Substring("LOOP".Length).Trim();
		if (body.EndsWith("END LOOP", StringComparison.OrdinalIgnoreCase))
			body = body.Substring(0, body.Length - "END LOOP".Length).Trim();

		(TableSchema Schema, List<TableRow> Rows)? lastResult = null;
		int maxIter = 10000;
		while (maxIter-- > 0)
		{
			try
			{
				var result = ExecuteBlock(body);
				if (result.HasValue) lastResult = result.Value;
			}
			catch (BreakException) { break; }
			catch (ContinueException) { continue; }
		}
		return lastResult;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#execute_immediate
	//   "Executes a dynamic SQL statement on the fly."
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteImmediate(string sql)
	{
		var body = sql.Substring("EXECUTE IMMEDIATE".Length).Trim();

		// Check for INTO var
		string? intoVar = null;
		var intoIdx = body.IndexOf(" INTO ", StringComparison.OrdinalIgnoreCase);
		string? usingClause = null;
		var usingIdx = body.IndexOf(" USING ", StringComparison.OrdinalIgnoreCase);
		if (usingIdx >= 0)
		{
			usingClause = body.Substring(usingIdx + " USING ".Length).Trim();
			body = body.Substring(0, usingIdx).Trim();
		}
		if (intoIdx >= 0)
		{
			intoVar = body.Substring(intoIdx + " INTO ".Length).Trim();
			body = body.Substring(0, intoIdx).Trim();
		}

		// Evaluate the SQL expression
		var substituted = SubstituteVariables(body);
		var exec = CreateQueryExecutor();
		var (_, sqlRows) = exec.Execute($"SELECT {substituted}");
		var dynamicSql = sqlRows.Count > 0 && sqlRows[0].F.Count > 0 ? sqlRows[0].F[0].V?.ToString() : null;
		if (dynamicSql is null) return null;

		// Execute the dynamic SQL
		substituted = SubstituteVariables(dynamicSql);
		substituted = QualifyTempTables(substituted);
		var dynExec = new QueryExecutor(_store, _defaultDatasetId);
		var result = dynExec.Execute(substituted);

		// If INTO is specified, store the result in the variable
		if (intoVar != null && result.Rows.Count > 0 && result.Rows[0].F.Count > 0)
		{
			var vars = intoVar.Split(',', StringSplitOptions.TrimEntries);
			for (int i = 0; i < vars.Length && i < result.Rows[0].F.Count; i++)
				_variables[vars[i]] = result.Rows[0].F[i].V;
		}

		return (result.Schema, result.Rows);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#for-in
	//   "Loops over each row produced by a SQL statement."
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteForIn(string sql)
	{
		var body = sql.Substring("FOR ".Length).Trim();
		// Parse: var IN (query) DO stmts END FOR
		var inIdx = body.IndexOf(" IN ", StringComparison.OrdinalIgnoreCase);
		if (inIdx < 0) throw new InvalidOperationException("FOR requires IN.");
		var varName = body.Substring(0, inIdx).Trim();
		var rest = body.Substring(inIdx + " IN ".Length).Trim();

		// Find the query in parentheses
		if (!rest.StartsWith("(")) throw new InvalidOperationException("FOR...IN requires (query).");
		int parenDepth = 0;
		int queryEnd = -1;
		for (int i = 0; i < rest.Length; i++)
		{
			if (rest[i] == '(') parenDepth++;
			else if (rest[i] == ')')
			{
				parenDepth--;
				if (parenDepth == 0) { queryEnd = i; break; }
			}
		}
		if (queryEnd < 0) throw new InvalidOperationException("Unmatched parenthesis in FOR...IN.");
		var query = rest.Substring(1, queryEnd - 1).Trim();
		rest = rest.Substring(queryEnd + 1).Trim();

		// Remove DO ... END FOR
		if (rest.StartsWith("DO", StringComparison.OrdinalIgnoreCase))
			rest = rest.Substring(2).Trim();
		if (rest.EndsWith("END FOR", StringComparison.OrdinalIgnoreCase))
			rest = rest.Substring(0, rest.Length - "END FOR".Length).Trim();

		// Execute the query
		var substituted = SubstituteVariables(query);
		substituted = QualifyTempTables(substituted);
		var exec = new QueryExecutor(_store, _defaultDatasetId);
		var queryResult = exec.Execute(substituted);

		(TableSchema Schema, List<TableRow> Rows)? lastResult = null;
		foreach (var row in queryResult.Rows)
		{
			// Set variable fields: record.col1, record.col2 etc.
			for (int i = 0; i < queryResult.Schema.Fields.Count && i < row.F.Count; i++)
			{
				var fieldName = queryResult.Schema.Fields[i].Name;
				_variables[$"{varName}.{fieldName}"] = row.F[i].V;
			}
			// Also set bare variable name for single-column results
			if (row.F.Count == 1)
				_variables[varName] = row.F[0].V;

			try
			{
				var result = ExecuteBlock(rest);
				if (result.HasValue) lastResult = result.Value;
			}
			catch (BreakException) { break; }
			catch (ContinueException) { continue; }
		}
		return lastResult;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#repeat
	//   "Executes the body until the condition is true."
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteRepeat(string sql)
	{
		var body = sql.Trim();
		if (body.StartsWith("REPEAT", StringComparison.OrdinalIgnoreCase))
			body = body.Substring("REPEAT".Length).Trim();
		if (body.EndsWith("END REPEAT", StringComparison.OrdinalIgnoreCase))
			body = body.Substring(0, body.Length - "END REPEAT".Length).Trim();

		// Split on UNTIL
		var untilIdx = FindTopLevelKeyword(body, "UNTIL");
		if (untilIdx < 0) throw new InvalidOperationException("REPEAT requires UNTIL.");
		var loopBody = body.Substring(0, untilIdx).Trim();
		var condition = body.Substring(untilIdx + "UNTIL".Length).Trim();

		(TableSchema Schema, List<TableRow> Rows)? lastResult = null;
		int maxIter = 10000;
		while (maxIter-- > 0)
		{
			try
			{
				var result = ExecuteBlock(loopBody);
				if (result.HasValue) lastResult = result.Value;
			}
			catch (BreakException) { break; }
			catch (ContinueException) { /* fall through to condition check */ }

			// Check UNTIL condition
			var substituted = SubstituteVariables(condition);
			var exec = CreateQueryExecutor();
			var (_, condRows) = exec.Execute($"SELECT {substituted}");
			var condValue = condRows.Count > 0 && condRows[0].F.Count > 0 ? condRows[0].F[0].V : null;
			if (IsTruthyValue(condValue)) break;
		}
		return lastResult;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#case_statement
	//   "Executes the first matching WHEN clause's statement list."
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteCaseStatement(string sql)
	{
		var body = sql.Trim();
		// Remove CASE prefix
		body = body.Substring("CASE".Length).Trim();
		// Remove END CASE suffix
		if (body.EndsWith("END CASE", StringComparison.OrdinalIgnoreCase))
			body = body.Substring(0, body.Length - "END CASE".Length).Trim();

		// Find first WHEN to extract the case expression
		var firstWhenIdx = FindTopLevelKeyword(body, "WHEN");
		if (firstWhenIdx < 0) throw new InvalidOperationException("CASE requires WHEN.");
		var caseExpr = body.Substring(0, firstWhenIdx).Trim();
		body = body.Substring(firstWhenIdx).Trim();

		// Evaluate case expression
		object? caseValue = null;
		if (!string.IsNullOrEmpty(caseExpr))
		{
			var substituted = SubstituteVariables(caseExpr);
			var exec = CreateQueryExecutor();
			var (_, rows) = exec.Execute($"SELECT {substituted}");
			caseValue = rows.Count > 0 && rows[0].F.Count > 0 ? rows[0].F[0].V : null;
		}

		// Parse WHEN...THEN...ELSE blocks
		var remaining = body;
		while (remaining.Length > 0)
		{
			if (remaining.StartsWith("WHEN ", StringComparison.OrdinalIgnoreCase))
			{
				remaining = remaining.Substring("WHEN ".Length).Trim();
				var thenIdx = FindTopLevelKeyword(remaining, "THEN");
				if (thenIdx < 0) break;
				var whenExpr = remaining.Substring(0, thenIdx).Trim();
				remaining = remaining.Substring(thenIdx + "THEN".Length).Trim();

				// Find next WHEN or ELSE
				var nextWhen = FindTopLevelKeyword(remaining, "WHEN");
				var nextElse = FindTopLevelKeyword(remaining, "ELSE");
				var blockEnd = remaining.Length;
				if (nextWhen >= 0 && (nextElse < 0 || nextWhen < nextElse)) blockEnd = nextWhen;
				else if (nextElse >= 0) blockEnd = nextElse;

				var thenBlock = remaining.Substring(0, blockEnd).Trim();
				remaining = remaining.Substring(blockEnd).Trim();

				// Evaluate WHEN expression
				var substituted = SubstituteVariables(whenExpr);
				var exec = CreateQueryExecutor();
				var (_, rows) = exec.Execute($"SELECT {substituted}");
				var whenValue = rows.Count > 0 && rows[0].F.Count > 0 ? rows[0].F[0].V : null;

				if (caseValue?.ToString() == whenValue?.ToString())
					return ExecuteBlock(thenBlock);
			}
			else if (remaining.StartsWith("ELSE", StringComparison.OrdinalIgnoreCase))
			{
				var elseBlock = remaining.Substring("ELSE".Length).Trim();
				return ExecuteBlock(elseBlock);
			}
			else break;
		}
		return null;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#call
	//   "Calls a stored procedure."
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteCall(string sql)
	{
		// In-memory: CALL is a no-op (procedures are stubs)
		return null;
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#create_temp_table
	private (TableSchema Schema, List<TableRow> Rows)? ExecuteCreateTempTable(string sql)
	{
		// Remove CREATE TEMP/TEMPORARY prefix and delegate to normal executor
		var normalized = System.Text.RegularExpressions.Regex.Replace(
			sql, @"^CREATE\s+TEMP(ORARY)?\s+", "CREATE ", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

		// Extract table name for tracking
		var nameMatch = System.Text.RegularExpressions.Regex.Match(normalized,
			@"^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
			System.Text.RegularExpressions.RegexOptions.IgnoreCase);
		if (nameMatch.Success)
			_tempTableNames.Add(nameMatch.Groups[1].Value);

		// Ensure _temp dataset exists
		if (!_store.Datasets.ContainsKey("_temp"))
			_store.Datasets["_temp"] = new InMemoryDataset("_temp");

		// Redirect to _temp dataset
		var exec = new QueryExecutor(_store, "_temp");
		var r = exec.Execute(normalized);
		return (r.Schema, r.Rows);
	}

	private static bool IsTruthyValue(object? value)
	{
		return value is true || (value is long l && l != 0) || (value is string s && bool.TryParse(s, out var b) && b);
	}

	internal class BreakException : Exception { }
	internal class ContinueException : Exception { }
	/// <summary>Split a script on semicolons, respecting string literals, parentheses, and BEGIN/END blocks.</summary>
	internal static List<string> SplitStatements(string script)
	{
		var result = new List<string>();
		var current = new System.Text.StringBuilder();
		var inString = false;
		var stringChar = '\0';
		var parenDepth = 0;
		var blockDepth = 0;
		string lastKeyword = "";
		bool _lastWordWasEnd = false;

		var words = new System.Text.StringBuilder();

		for (int i = 0; i < script.Length; i++)
		{
			var c = script[i];
			if (inString)
			{
				current.Append(c);
				if (c == stringChar && (i + 1 >= script.Length || script[i + 1] != stringChar))
					inString = false;
				else if (c == stringChar)
				{
					current.Append(script[++i]);
				}
				continue;
			}

			if (c == '\'' || c == '"' || c == '`')
			{
				inString = true;
				stringChar = c;
				current.Append(c);
				continue;
			}
			
			if (c == '(') { parenDepth++; current.Append(c); continue; }
			if (c == ')') { parenDepth--; current.Append(c); continue; }

			// Track BEGIN/END and IF/END IF nesting
			if (char.IsLetterOrDigit(c) || c == '_')
			{
				words.Append(c);
				current.Append(c);
				continue;
			}
			else
			{
				var word = words.ToString().ToUpperInvariant();
				words.Clear();
				if (word == "BEGIN" || (word == "IF" && lastKeyword != "FUNCTION" && lastKeyword != "TABLE") || word == "LOOP" || word == "WHILE" || word == "REPEAT" || word == "FOR" || (word == "CASE" && lastKeyword != "SELECT" && lastKeyword != "WHEN" && lastKeyword != "THEN" && lastKeyword != "ELSE" && lastKeyword != "SET" && lastKeyword != "AND" && lastKeyword != "OR" && lastKeyword != "AS"))
				{
					// Don't count these keywords when they follow END (e.g., END LOOP, END IF)
					if (!_lastWordWasEnd)
						blockDepth++;
					_lastWordWasEnd = false;
				}
				else if (word == "TRANSACTION" && lastKeyword == "BEGIN")
				{
					// BEGIN TRANSACTION should not increase block depth
					blockDepth = Math.Max(0, blockDepth - 1);
					_lastWordWasEnd = false;
				}
				else if (word == "END")
				{
					blockDepth = Math.Max(0, blockDepth - 1);
					_lastWordWasEnd = true;
				}
				else
				{
					_lastWordWasEnd = false;
				}
				if (word.Length > 0) lastKeyword = word;
			}

			if (c == ';' && parenDepth == 0 && blockDepth == 0)
			{
				result.Add(current.ToString());
				current.Clear();
				lastKeyword = "";
			}
			else
			{
				current.Append(c);
			}
		}

		// Check last word
		var lastWord = words.ToString().ToUpperInvariant();
		if (lastWord == "END") blockDepth = Math.Max(0, blockDepth - 1);
		else if (_lastWordWasEnd && (lastWord == "IF" || lastWord == "LOOP" || lastWord == "WHILE" || lastWord == "REPEAT"))
		{ /* already decremented by END */ }

		if (current.Length > 0)
			result.Add(current.ToString());

		return result;
	}

	private static int FindTopLevelKeyword(string text, string keyword)
	{
		var depth = 0;
		var inStr = false;
		var strCh = '\0';

		for (int i = 0; i <= text.Length - keyword.Length; i++)
		{
			var c = text[i];
			if (inStr)
			{
				if (c == strCh) inStr = false;
				continue;
			}
			if (c == '\'' || c == '"') { inStr = true; strCh = c; continue; }
			if (c == '(') { depth++; continue; }
			if (c == ')') { depth--; continue; }

			if (depth == 0 && text.Substring(i, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase))
			{
				// Check word boundary
				if (i > 0 && char.IsLetterOrDigit(text[i - 1])) continue;
				if (i + keyword.Length < text.Length && char.IsLetterOrDigit(text[i + keyword.Length])) continue;
				return i;
			}
		}
		return -1;
	}

	private static string InferType(object? value)
	{
		return value switch
		{
			long => "INT64",
			double => "FLOAT64",
			bool => "BOOL",
			string => "STRING",
			_ => "STRING",
		};
	}

	/// <summary>Thrown by RETURN to exit script execution.</summary>
	internal class ReturnException : Exception { }
}
