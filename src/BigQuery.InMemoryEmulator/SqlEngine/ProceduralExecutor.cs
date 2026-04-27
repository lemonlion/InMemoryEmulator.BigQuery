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

		// BEGIN ... END
		if (upper.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase))
			return ExecuteBeginEnd(sql);

		// WHILE condition DO stmts END WHILE
		if (upper.StartsWith("WHILE ", StringComparison.OrdinalIgnoreCase))
			return ExecuteWhile(sql);

		// LOOP stmts END LOOP
		if (upper.StartsWith("LOOP", StringComparison.OrdinalIgnoreCase))
			return ExecuteLoop(sql);

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
				if (word == "BEGIN" || (word == "IF" && lastKeyword != "FUNCTION" && lastKeyword != "TABLE") || word == "LOOP" || word == "WHILE" || word == "REPEAT")
				{
					// Don't count these keywords when they follow END (e.g., END LOOP, END IF)
					if (!_lastWordWasEnd)
						blockDepth++;
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
