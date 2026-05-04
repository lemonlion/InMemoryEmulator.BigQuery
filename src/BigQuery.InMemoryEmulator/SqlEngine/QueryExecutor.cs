#pragma warning disable CS8600, CS8602, CS8604

using System.Globalization;
using System.Text.RegularExpressions;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.InMemoryEmulator.SqlEngine;

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
//   Core query execution engine evaluates parsed SQL AST against in-memory data.

internal record InMemoryBigQueryResult(TableSchema Schema, List<TableRow> Rows)
{
	public long? DmlAffectedRows { get; init; }
}

internal class QueryExecutor
{
private readonly InMemoryDataStore _store;
private readonly string? _defaultDatasetId;
private IList<QueryParameter>? _parameters;
// CTE results visible to the current query scope (set during ExecuteSelect, read by scalar subqueries)
private Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)>? _activeCteResults;
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#correlated_subqueries
//   "A correlated subquery references a column from outside the subquery."
// Outer row context for correlated subqueries (EXISTS, scalar subquery, IN subquery).
private RowContext? _outerRowContext;
private (RowContext CurrentRow, List<RowContext> AllRows)? _windowContext;

public QueryExecutor(InMemoryDataStore store, string? defaultDatasetId = null)
{
_store = store;
_defaultDatasetId = defaultDatasetId;
}

public void SetParameters(IList<QueryParameter> parameters)
{
_parameters = parameters;
}

public InMemoryBigQueryResult Execute(string sql)
{
// Phase 27: Detect stub DDL patterns before parsing â€” these have complex syntax
// that the parser doesn't handle, but we support them as no-ops for Go emulator parity.
if (IsStubDdl(sql))
	return EmptyResult();

var stmt = SqlParser.ParseSql(sql);
if (stmt is CreateViewStatement cv2)
{
	var asIdx = sql.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
	var viewSql = asIdx >= 0 ? sql[(asIdx + 4)..].Trim() : sql;
	stmt = cv2 with { ViewSql = viewSql };
}
return ExecuteStatement(stmt);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
// Stub patterns that parse-and-ignore for Go emulator parity.
private static readonly System.Text.RegularExpressions.Regex[] StubDdlPatterns =
[
	new(@"^\s*CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	new(@"^\s*DROP\s+PROCEDURE\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	new(@"^\s*CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+FUNCTION\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	new(@"^\s*DROP\s+TABLE\s+FUNCTION\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	new(@"^\s*CREATE\s+(OR\s+REPLACE\s+)?ROW\s+ACCESS\s+POLICY\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	new(@"^\s*DROP\s+(ALL\s+)?ROW\s+ACCESS\s+POLIC(Y|IES)\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	new(@"^\s*CREATE\s+SEARCH\s+INDEX\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	new(@"^\s*DROP\s+SEARCH\s+INDEX\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-control-language
	//   GRANT and REVOKE manage access to datasets, tables, views, and routines.
	//   Accepted as no-ops in the emulator.
	new(@"^\s*GRANT\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	new(@"^\s*REVOKE\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/other-statements#export_data_statement
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/other-statements#load_data_statement
	//   EXPORT DATA and LOAD DATA interact with external storage.
	//   Accepted as no-ops in the emulator.
	new(@"^\s*EXPORT\s+DATA\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
	new(@"^\s*LOAD\s+DATA\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled),
];

private static bool IsStubDdl(string sql)
{
	foreach (var pattern in StubDdlPatterns)
		if (pattern.IsMatch(sql)) return true;
	return false;
}

private InMemoryBigQueryResult ExecuteStatement(SqlStatement stmt)
{
return stmt switch
{
SelectStatement sel => ExecuteSelect(sel),
SetOperationStatement setOp => ExecuteSetOperation(setOp),
InsertValuesStatement ins => ExecuteInsertValues(ins),
InsertSelectStatement ins => ExecuteInsertSelect(ins),
UpdateStatement upd => ExecuteUpdate(upd),
DeleteStatement del => ExecuteDelete(del),
MergeStatement merge => ExecuteMerge(merge),
WithDmlStatement withDml => ExecuteWithDml(withDml),
CreateTableStatement ct => ExecuteCreateTable(ct),
CreateTableAsSelectStatement ctas => ExecuteCreateTableAsSelect(ctas),
DropTableStatement dt => ExecuteDropTable(dt),
AlterTableStatement alt => ExecuteAlterTable(alt),
CreateViewStatement cv => ExecuteCreateView(cv),
DropViewStatement dv => ExecuteDropView(dv),
TruncateTableStatement trunc => ExecuteTruncateTable(trunc),
CreateTableLikeStatement like => ExecuteCreateTableLike(like),
CreateTableCopyStatement copy => ExecuteCreateTableCopy(copy),
CreateSchemaStatement cs => ExecuteCreateSchema(cs),
DropSchemaStatement ds => ExecuteDropSchema(ds),
NoOpDdlStatement _ => EmptyResult(),
_ => throw new NotSupportedException("Unsupported statement type: " + stmt.GetType().Name)
};
}
#region SELECT execution

private InMemoryBigQueryResult ExecuteSelect(SelectStatement sel)
=> ExecuteSelect(sel, null);

private InMemoryBigQueryResult ExecuteSelect(SelectStatement sel,
Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)>? externalCteResults)
{
// Handle CTEs
Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)>? cteResults = externalCteResults;
if (sel.Ctes is { Count: > 0 })
    cteResults = ResolveCtes(sel, externalCteResults);

// Make CTE results visible to scalar subqueries and other expression evaluators
var prevCteResults = _activeCteResults;
if (cteResults is not null)
    _activeCteResults = cteResults;

try
{
// FROM
List<RowContext> rows;
if (sel.From is not null)
rows = ResolveFrom(sel.From, cteResults ?? _activeCteResults);
else
rows = [new RowContext(new Dictionary<string, object?>(), null)];

// Partition filter check
if (sel.From is TableRef tRefCheck)
{
var dsCheck = tRefCheck.DatasetId ?? _defaultDatasetId;
if (dsCheck is not null && _store.Datasets.TryGetValue(dsCheck, out var dsObj) &&
dsObj.Tables.TryGetValue(tRefCheck.TableId, out var tblCheck) &&
tblCheck.RequirePartitionFilter && tblCheck.TimePartitioning is not null)
{
if (sel.Where is null || !ContainsPartitionRef(sel.Where))
throw new InvalidOperationException("Cannot query table with require_partition_filter without a partition filter");
}
}

// WHERE
if (sel.Where is not null)
rows = rows.Where(r => IsTruthy(Evaluate(sel.Where, r))).ToList();

// GROUP BY
if (sel.GroupBy is { Count: > 0 } || sel.Columns.Any(c => ContainsAggregate(c.Expr)))
{
return ExecuteGroupBy(sel, rows);
}

// Window functions
bool hasWindow = sel.Columns.Any(c => ContainsWindowFunction(c.Expr));

// SELECT projection
var (schema, tableRows) = hasWindow ? ProjectWithWindows(sel, rows) : Project(sel, rows, cteResults);

// QUALIFY - filter after window functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
//   "The QUALIFY clause filters the results of window functions."
if (sel.Qualify is not null)
{
    var qualifyRows = new List<TableRow>();
    for (int qi = 0; qi < tableRows.Count; qi++)
    {
        var qDict = RowToDict(tableRows[qi], schema);
        if (qi < rows.Count)
            foreach (var kv in rows[qi].Fields)
                qDict.TryAdd(kv.Key, kv.Value);
        var qCtx = new RowContext(qDict, null);
        var qVal = qi < rows.Count
            ? EvaluateWithWindows(sel.Qualify, qCtx, rows[qi], rows)
            : Evaluate(sel.Qualify, qCtx);
        if (IsTruthy(qVal)) qualifyRows.Add(tableRows[qi]);
    }
    tableRows = qualifyRows;
}

// DISTINCT
if (sel.Distinct)
{
tableRows = tableRows
.GroupBy(r => string.Join("|", r.F?.Select(f => f?.V?.ToString() ?? "NULL") ?? Array.Empty<string>()))
.Select(g => g.First())
.ToList();
}

// ORDER BY
if (sel.OrderBy is { Count: > 0 })
{

// Include source row fields so ORDER BY can reference columns not in SELECT
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
var projFieldNames = schema.Fields.Select(f => f.Name).ToHashSet();
var dicts = tableRows.Select((r, i) =>
{
    var d = ParseTypedRow(RowToDict(r, schema), schema);
    if (i < rows.Count)
        foreach (var kv in rows[i].Fields)
            d.TryAdd(kv.Key, kv.Value);
    return d;
}).ToList();
var contexts = dicts.Select(d => new RowContext(d, null)).ToList();
var resolvedOrderBy = ResolveOrderByAliases(ResolveOrderByOrdinals(sel.OrderBy, sel.Columns), sel.Columns);
contexts = OrderBy(contexts, resolvedOrderBy);
tableRows = contexts.Select(c =>
{
    var proj = new Dictionary<string, object?>();
    foreach (var kv in c.Fields)
        if (projFieldNames.Contains(kv.Key))
            proj[kv.Key] = kv.Value;
    return DictToTableRow(proj);
}).ToList();
}

// OFFSET
if (sel.Offset.HasValue)
tableRows = tableRows.Skip(sel.Offset.Value).ToList();

// LIMIT
if (sel.Limit.HasValue)
tableRows = tableRows.Take(sel.Limit.Value).ToList();

return new InMemoryBigQueryResult(schema, tableRows);
}
finally
{
    _activeCteResults = prevCteResults;
}
}

private Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)> ResolveCtes(
    SelectStatement sel,
    Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)>? externalCteResults = null)
{
var cteResults = externalCteResults != null
    ? new Dictionary<string, (TableSchema, List<Dictionary<string, object?>>)>(externalCteResults, StringComparer.OrdinalIgnoreCase)
    : new Dictionary<string, (TableSchema, List<Dictionary<string, object?>>)>(StringComparer.OrdinalIgnoreCase);
foreach (var cte in sel.Ctes!)
{
    if (sel.IsRecursive && cte.RecursiveBody is not null && cte.UnionBodies is null)
    {
        var baseResult = ExecuteSelect(cte.Body, cteResults);
        var allRows = baseResult.Rows.Select(r => ParseTypedRow(RowToDict(r, baseResult.Schema), baseResult.Schema)).ToList();
        var currentRows = new List<Dictionary<string, object?>>(allRows);
        var recSchema = baseResult.Schema;
        const int maxIterations = 500;
        for (int iter = 0; iter < maxIterations && currentRows.Count > 0; iter++)
        {
            cteResults[cte.Name] = (recSchema, currentRows);
            var recResult = ExecuteSelect(cte.RecursiveBody, cteResults);
            var newRows = recResult.Rows.Select(r => ParseTypedRow(RowToDict(r, recSchema), recSchema)).ToList();
            if (newRows.Count == 0) break;
            allRows.AddRange(newRows);
            currentRows = newRows;
        }
        cteResults[cte.Name] = (recSchema, allRows);
    }
    else if (cte.RecursiveBody is not null || cte.UnionBodies is not null)
    {
        var baseResult = ExecuteSelect(cte.Body, cteResults);
        var allRows = baseResult.Rows.Select(r => ParseTypedRow(RowToDict(r, baseResult.Schema), baseResult.Schema)).ToList();
        var unionSchema = baseResult.Schema;
        if (cte.RecursiveBody is not null)
        {
            var recResult = ExecuteSelect(cte.RecursiveBody, cteResults);
            allRows.AddRange(recResult.Rows.Select(r => ParseTypedRow(RowToDict(r, unionSchema), unionSchema)));
        }
        if (cte.UnionBodies is not null)
        {
            foreach (var unionBody in cte.UnionBodies)
            {
                var unionResult = ExecuteSelect(unionBody, cteResults);
                allRows.AddRange(unionResult.Rows.Select(r => ParseTypedRow(RowToDict(r, unionSchema), unionSchema)));
            }
        }
        cteResults[cte.Name] = (unionSchema, allRows);
    }
    else
    {
        var cteResult = ExecuteSelect(cte.Body, cteResults);
        var cteRows = cteResult.Rows.Select(r => ParseTypedRow(RowToDict(r, cteResult.Schema), cteResult.Schema)).ToList();
        cteResults[cte.Name] = (cteResult.Schema, cteRows);
    }
}
return cteResults;
}

private InMemoryBigQueryResult ExecuteGroupBy(SelectStatement sel, List<RowContext> rows)
{
var groupExprs = sel.GroupBy ?? [];

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause
//   "GROUP BY can reference SELECT list aliases."
groupExprs = ResolveGroupByAliases(groupExprs, sel.Columns);

// Check for ROLLUP
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#rollup
var rollupExpr = groupExprs.OfType<RollupExpr>().FirstOrDefault();
if (rollupExpr is not null)
    return ExecuteRollup(sel, rows, rollupExpr);

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause
//   When there's no explicit GROUP BY but the SELECT contains aggregates
//   (implicit aggregation), the entire table is one group â€” even if empty.
//   SELECT COUNT(*) FROM empty_table returns 1 row (count=0).
if (groupExprs.Count == 0 && rows.Count == 0)
{
    // Implicit aggregation over empty set â†’ produce one result row
    var dict2 = new Dictionary<string, object?>();
    var fields2 = new List<TableFieldSchema>();
    var emptyGroupRows = new List<RowContext>();
    foreach (var col in sel.Columns)
    {
        var name = col.Alias ?? DeriveColumnName(col.Expr);
        var value = EvaluateWithAggregates(col.Expr, emptyGroupRows);
        dict2[name] = value;
        fields2.Add(new TableFieldSchema { Name = name, Type = InferType(value) });
    }
    var emptySchema = new TableSchema { Fields = fields2 };
    return new InMemoryBigQueryResult(emptySchema, new List<TableRow> { DictToTableRow(dict2) });
}

var groups = rows.GroupBy(r =>
string.Join("|", groupExprs.Select(g => Evaluate(g, r)?.ToString() ?? "NULL")));

var resultRows = new List<Dictionary<string, object?>>();
TableSchema? schema = null;

foreach (var group in groups)
{
var groupRows = group.ToList();

// HAVING (evaluate before adding to results)
if (sel.Having is not null)
{
var havingVal = EvaluateWithAggregates(sel.Having, groupRows);
if (!IsTruthy(havingVal)) continue;
}

var dict = new Dictionary<string, object?>();
var fields = new List<TableFieldSchema>();

foreach (var col in sel.Columns)
{
var name = col.Alias ?? DeriveColumnName(col.Expr);
// Skip window functions in first pass — they'll be evaluated after all groups are processed
if (ContainsWindowFunction(col.Expr))
{
	dict[name] = null; // placeholder
	if (schema is null)
		fields.Add(new TableFieldSchema { Name = name, Type = "STRING" });
}
else
{
	var value = EvaluateWithAggregates(col.Expr, groupRows);
	dict[name] = value;
	if (schema is null)
		fields.Add(new TableFieldSchema { Name = name, Type = InferType(value) });
}
}

schema ??= new TableSchema { Fields = fields };
resultRows.Add(dict);
}

schema ??= new TableSchema { Fields = sel.Columns.Select(c =>
new TableFieldSchema { Name = c.Alias ?? DeriveColumnName(c.Expr), Type = "STRING" }).ToList() };

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
//   "Window functions can be applied to the result of GROUP BY aggregation."
// Second pass: evaluate window functions over aggregated result rows
bool hasWindowInGroupBy = sel.Columns.Any(c => ContainsWindowFunction(c.Expr));
if (hasWindowInGroupBy && resultRows.Count > 0)
{
	var aggregatedRowContexts = resultRows.Select(d => new RowContext(d, null)).ToList();
	// Build a mapping from SELECT alias -> column expression for resolving aggregates inside window ORDER BY
	var colAliasMap = new Dictionary<string, SqlExpression>(StringComparer.OrdinalIgnoreCase);
	foreach (var col in sel.Columns)
	{
		var name = col.Alias ?? DeriveColumnName(col.Expr);
		colAliasMap[name] = col.Expr;
	}
	for (int i = 0; i < resultRows.Count; i++)
	{
		var currentRow = aggregatedRowContexts[i];
		foreach (var col in sel.Columns)
		{
			if (!ContainsWindowFunction(col.Expr)) continue;
			var name = col.Alias ?? DeriveColumnName(col.Expr);
			if (col.Expr is WindowFunction wf)
			{
				// Replace aggregates in window ORDER BY with resolved column refs that match the aggregated row
				var resolvedWf = ResolveWindowAggregates(wf, sel.Columns);
				resultRows[i][name] = EvaluateWindow(resolvedWf, currentRow, aggregatedRowContexts);
			}
			else
			{
				// Expression containing a window function nested somewhere
				_windowContext = (currentRow, aggregatedRowContexts);
				resultRows[i][name] = Evaluate(col.Expr, currentRow);
				_windowContext = null;
			}
		}
	}
	// Update schema types based on actual values
	schema = new TableSchema { Fields = sel.Columns.Select(c =>
	{
		var name = c.Alias ?? DeriveColumnName(c.Expr);
		var val = resultRows[0].GetValueOrDefault(name);
		return new TableFieldSchema { Name = name, Type = InferType(val) };
	}).ToList() };
}

if (sel.OrderBy is { Count: > 0 })
{
var ctx2 = resultRows.Select(d => new RowContext(d, null)).ToList();
ctx2 = OrderBy(ctx2, ResolveOrderByExpressions(ResolveOrderByOrdinals(sel.OrderBy, sel.Columns), sel.Columns));
resultRows = ctx2.Select(c => c.Fields).ToList();
}

if (sel.Limit.HasValue)
resultRows = resultRows.Take(sel.Limit.Value).ToList();

var tableRows = resultRows.Select(d => DictToTableRow(d)).ToList();
return new InMemoryBigQueryResult(schema, tableRows);
}

private InMemoryBigQueryResult ExecuteRollup(SelectStatement sel, List<RowContext> rows, RollupExpr rollup)
{
    // ROLLUP(a, b, c) generates grouping sets: (a,b,c), (a,b), (a), ()
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#rollup
    var rollupExprs = rollup.Exprs;
    var allResultRows = new List<Dictionary<string, object?>>();
    TableSchema? schema = null;

    // Generate prefix grouping sets: (all), (all-1), ..., ()
    for (int level = rollupExprs.Count; level >= 0; level--)
    {
        var activeExprs = rollupExprs.Take(level).ToList();
        var nullExprs = rollupExprs.Skip(level).ToList();

        var groups = rows.GroupBy(r =>
            string.Join("|", activeExprs.Select(g => Evaluate(g, r)?.ToString() ?? "NULL")));

        foreach (var group in groups)
        {
            var groupRows = group.ToList();
            if (sel.Having is not null)
            {
                var hv = EvaluateWithAggregates(sel.Having, groupRows);
                if (!IsTruthy(hv)) continue;
            }

            var dict = new Dictionary<string, object?>();
            var fields = new List<TableFieldSchema>();
            foreach (var col in sel.Columns)
            {
                var name = col.Alias ?? DeriveColumnName(col.Expr);
                // For nullified group-by columns, return null
                var colRef = col.Expr is ColumnRef cr ? cr : null;
                bool isNulled = colRef != null && nullExprs.Any(ne =>
                    ne is ColumnRef ncr && ncr.ColumnName.Equals(colRef.ColumnName, StringComparison.OrdinalIgnoreCase));
                dict[name] = isNulled ? null : EvaluateWithAggregates(col.Expr, groupRows);
                if (schema is null)
                    fields.Add(new TableFieldSchema { Name = name, Type = InferType(dict[name]) });
            }
            schema ??= new TableSchema { Fields = fields };
            allResultRows.Add(dict);
        }
    }

    schema ??= new TableSchema { Fields = sel.Columns.Select(c =>
        new TableFieldSchema { Name = c.Alias ?? DeriveColumnName(c.Expr), Type = "STRING" }).ToList() };

    if (sel.OrderBy is { Count: > 0 })
    {
        var ctx2 = allResultRows.Select(d => new RowContext(d, null)).ToList();
        ctx2 = OrderBy(ctx2, ResolveOrderByExpressions(ResolveOrderByOrdinals(sel.OrderBy, sel.Columns), sel.Columns));
        allResultRows = ctx2.Select(c => c.Fields).ToList();
    }

    if (sel.Limit.HasValue)
        allResultRows = allResultRows.Take(sel.Limit.Value).ToList();

    return new InMemoryBigQueryResult(schema, allResultRows.Select(DictToTableRow).ToList());
}

#endregion
#region FROM resolution

private List<RowContext> ResolveFrom(FromClause from,
Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)>? cteResults = null)
{
return from switch
{
TableRef tRef => ResolveTableRef(tRef, cteResults),
JoinClause join => ResolveJoin(join, cteResults),
SubqueryFrom sub => ResolveSubquery(sub),
UnnestClause unnest => ResolveUnnest(unnest),
PivotFrom pivot => ResolvePivot(pivot, cteResults),
UnpivotFrom unpivot => ResolveUnpivot(unpivot, cteResults),
TablesampleFrom sample => ResolveTablesample(sample, cteResults),
_ => throw new NotSupportedException("Unsupported FROM type: " + from.GetType().Name)
};
}

private List<RowContext> ResolvePivot(PivotFrom pivot,
    Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)>? cteResults)
{
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#pivot_operator
    var sourceRows = ResolveFrom(pivot.Source, cteResults);
    var pc = pivot.Pivot;
    var inputCol = pc.InputColumn.ColumnName;
    var aggFunc = pc.Aggregation;

    // Determine grouping columns = all source columns except input and aggregated column
    // Source rows may have both qualified (alias.col) and unqualified (col) keys
    var aggColName = aggFunc.Arg is ColumnRef acr ? acr.ColumnName : null;
    var allCols = sourceRows.Count > 0 ? sourceRows[0].Fields.Keys.ToList() : new List<string>();
    // Only use unqualified column names for grouping to avoid duplicates
    var unqualCols = allCols.Where(c => !c.Contains('.')).ToList();
    var groupCols = unqualCols.Where(c =>
        !c.Equals(inputCol, StringComparison.OrdinalIgnoreCase) &&
        (aggColName == null || !c.Equals(aggColName, StringComparison.OrdinalIgnoreCase))
    ).ToList();

    // Evaluate pivot values
    var pivotValues = pc.PivotValues.Select(pv => {
        var val = pv.Value is LiteralExpr lit ? lit.Value : null;
        var alias = pv.Alias ?? val?.ToString() ?? "NULL";
        return (Value: val, Alias: alias);
    }).ToList();

    // Group source rows by grouping columns
    var groups = sourceRows.GroupBy(r =>
        string.Join("|", groupCols.Select(c => r.Fields.TryGetValue(c, out var v) ? v?.ToString() ?? "NULL" : "NULL")));

    var result = new List<RowContext>();
    foreach (var group in groups)
    {
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var firstRow = group.First();
        foreach (var gc in groupCols)
            dict[gc] = firstRow.Fields.TryGetValue(gc, out var v) ? v : null;

        foreach (var pv in pivotValues)
        {
            var matchingRows = group.Where(r =>
                r.Fields.TryGetValue(inputCol, out var rv) && Equals(rv?.ToString(), pv.Value?.ToString())
            ).ToList();
            if (matchingRows.Count > 0)
                dict[pv.Alias] = EvaluateAggregate(aggFunc, matchingRows);
            else
                dict[pv.Alias] = null;
        }
        result.Add(new RowContext(dict, null));
    }
    return result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unpivot_operator
//   "The UNPIVOT operator rotates columns into rows."
private List<RowContext> ResolveUnpivot(UnpivotFrom unpivot,
    Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)>? cteResults)
{
    var sourceRows = ResolveFrom(unpivot.Source, cteResults);
    var uc = unpivot.Unpivot;
    var inputCols = uc.InputColumns;
    // Non-pivot columns = all columns except the input columns
    var allCols = sourceRows.Count > 0 ? sourceRows[0].Fields.Keys.ToList() : new List<string>();
    var unqualCols = allCols.Where(c => !c.Contains('.')).ToList();
    var nonPivotCols = unqualCols.Where(c =>
        !inputCols.Any(ic => ic.Equals(c, StringComparison.OrdinalIgnoreCase))
    ).ToList();

    var result = new List<RowContext>();
    foreach (var row in sourceRows)
    {
        foreach (var col in inputCols)
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var npc in nonPivotCols)
                dict[npc] = row.Fields.TryGetValue(npc, out var v) ? v : null;
            dict[uc.ValuesColumn] = row.Fields.TryGetValue(col, out var val) ? val : null;
            dict[uc.NameColumn] = col;
            result.Add(new RowContext(dict, null));
        }
    }
    return result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#tablesample_operator
//   "You can use the TABLESAMPLE operator to select a random subset of rows from a table."
private List<RowContext> ResolveTablesample(TablesampleFrom sample,
    Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)>? cteResults)
{
    var sourceRows = ResolveFrom(sample.Source, cteResults);
    var pct = sample.Percent;
    if (pct >= 100.0) return sourceRows;
    if (pct <= 0.0) return new List<RowContext>();
    var rng = new Random();
    return sourceRows.Where(_ => rng.NextDouble() * 100.0 < pct).ToList();
}

private List<RowContext> ResolveTableRef(TableRef tRef,
Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)>? cteResults)
{
var tableName = tRef.TableId;
var alias = tRef.Alias ?? tableName;

// INFORMATION_SCHEMA
if (tableName.Contains("INFORMATION_SCHEMA", StringComparison.OrdinalIgnoreCase) || tRef.DatasetId?.Equals("INFORMATION_SCHEMA", StringComparison.OrdinalIgnoreCase) == true)
{
	var isDs = tRef.DatasetId;
	// If DatasetId is "INFORMATION_SCHEMA" itself, use _defaultDatasetId
	if (isDs?.Equals("INFORMATION_SCHEMA", StringComparison.OrdinalIgnoreCase) == true)
		isDs = _defaultDatasetId;
	var infoRows = ResolveInformationSchema(tableName, alias, isDs ?? _defaultDatasetId);
	// Add alias-qualified keys for JOIN support
	foreach (var ir in infoRows)
		foreach (var kv in ir.Fields.ToList())
			ir.Fields[alias + "." + kv.Key] = kv.Value;
	return infoRows;
}

// Wildcard tables
if (tableName.Contains('*'))
return ResolveWildcardTable(tableName, alias, tRef.DatasetId);

// CTEs
if (cteResults is not null && cteResults.TryGetValue(tableName, out var cte))
{
return cte.Rows.Select(r => new RowContext(
new Dictionary<string, object?>(r.Select(kv =>
new KeyValuePair<string, object?>(alias + "." + kv.Key, kv.Value))
.Concat(r)), alias)).ToList();
}

// Resolve from store
var dsId = tRef.DatasetId ?? _defaultDatasetId;
if (dsId is null || !_store.Datasets.TryGetValue(dsId, out var ds))
throw new InvalidOperationException("Dataset '" + dsId + "' not found");
if (!ds.Tables.TryGetValue(tableName, out var table))
throw new InvalidOperationException("Table '" + tableName + "' not found in dataset '" + dsId + "'");

// If this table is a VIEW, re-execute the view's query to get fresh results
// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ViewDefinition
if (table.ViewQuery is not null)
{
var viewResult = ExecuteSelect(table.ViewQuery);
return viewResult.Rows.Select(r =>
{
	var dict = RowToDict(r, viewResult.Schema);
	var typedDict = ParseTypedRow(dict, viewResult.Schema);
	var allFields = new Dictionary<string, object?>(typedDict);
	foreach (var kv in typedDict) allFields[alias + "." + kv.Key] = kv.Value;
	return new RowContext(allFields, alias);
}).ToList();
}

var rows = new List<RowContext>();
lock (table.RowLock)
{
foreach (var row in table.Rows)
{
var fields = new Dictionary<string, object?>(row.Fields);
if (table.TimePartitioning is not null)
InjectPartitionPseudoColumns(fields, table);

var allFields = new Dictionary<string, object?>(fields);
foreach (var kv in fields)
allFields[alias + "." + kv.Key] = kv.Value;

rows.Add(new RowContext(allFields, alias));
}
}
return rows;
}

private static void InjectPartitionPseudoColumns(Dictionary<string, object?> fields, InMemoryTable table)
{
if (table.TimePartitioning?.Field is string partField && fields.TryGetValue(partField, out var partValue) && partValue is not null)
{
var dto = partValue switch
{
DateTimeOffset d => d,
DateTime d => new DateTimeOffset(d, TimeSpan.Zero),
string s => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture),
_ => DateTimeOffset.Parse(partValue.ToString()!, CultureInfo.InvariantCulture)
};
var truncated = table.TimePartitioning.Type?.ToUpperInvariant() switch
{
"MONTH" => new DateTimeOffset(dto.Year, dto.Month, 1, 0, 0, 0, TimeSpan.Zero),
"YEAR" => new DateTimeOffset(dto.Year, 1, 1, 0, 0, 0, TimeSpan.Zero),
_ => new DateTimeOffset(dto.Year, dto.Month, dto.Day, 0, 0, 0, TimeSpan.Zero),
};
fields["_PARTITIONTIME"] = truncated;
fields["_PARTITIONDATE"] = truncated.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
}
}

	// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.type
	//   INFORMATION_SCHEMA.COLUMNS returns canonical type names (INT64, FLOAT64, BOOL) not legacy aliases.
	private static string NormalizeFieldType(string type) => (type?.ToUpperInvariant()) switch
	{
		"INTEGER" => "INT64",
		"FLOAT" => "FLOAT64",
		"BOOLEAN" => "BOOL",
		_ => type?.ToUpperInvariant() ?? "STRING"
	};

	private List<RowContext> ResolveInformationSchema(string tableName, string alias, string? datasetId = null)
{
var dsId = datasetId ?? _defaultDatasetId;

// SCHEMATA is project-level, not dataset-scoped
if (tableName.EndsWith("SCHEMATA", StringComparison.OrdinalIgnoreCase))
{
	return _store.Datasets.Keys.Select(name => new RowContext(new Dictionary<string, object?>
	{
		["catalog_name"] = _store.ProjectId,
		["schema_name"] = name,
	}, alias)).ToList();
}

if (dsId is null || !_store.Datasets.TryGetValue(dsId, out var ds)) return [];

if (tableName.EndsWith("TABLES", StringComparison.OrdinalIgnoreCase))
{
// Ref: https://cloud.google.com/bigquery/docs/information-schema-tables
//   "table_type: BASE TABLE for a standard table, VIEW for a view"
return ds.Tables.Values.Select(t => new RowContext(new Dictionary<string, object?>
{
["table_catalog"] = _store.ProjectId,
["table_schema"] = dsId,
["table_name"] = t.TableId,
["table_type"] = t.ViewQuery is not null ? "VIEW" : "BASE TABLE",
["creation_time"] = t.CreationTime,
}, alias)).ToList();
}
if (tableName.EndsWith("COLUMNS", StringComparison.OrdinalIgnoreCase))
{
var rows = new List<RowContext>();
foreach (var t in ds.Tables.Values)
{
for (int i = 0; i < t.Schema.Fields.Count; i++)
{
var f = t.Schema.Fields[i];
rows.Add(new RowContext(new Dictionary<string, object?>
{
["table_catalog"] = _store.ProjectId,
["table_schema"] = dsId,
["table_name"] = t.TableId,
["column_name"] = f.Name,
["ordinal_position"] = (long)(i + 1),
["data_type"] = NormalizeFieldType(f.Type),
["is_nullable"] = f.Mode != "REQUIRED" ? "YES" : "NO",
}, alias));
}
}
return rows;
}

// Ref: https://cloud.google.com/bigquery/docs/information-schema-routines
//   "The INFORMATION_SCHEMA.ROUTINES view contains one row for each routine in a dataset."
if (tableName.EndsWith("ROUTINES", StringComparison.OrdinalIgnoreCase))
{
return ds.Routines.Values.Select(r => new RowContext(new Dictionary<string, object?>
{
["specific_catalog"] = _store.ProjectId,
["specific_schema"] = dsId,
["specific_name"] = r.RoutineId,
["routine_catalog"] = _store.ProjectId,
["routine_schema"] = dsId,
["routine_name"] = r.RoutineId,
["routine_type"] = r.RoutineType,
["data_type"] = r.ReturnType,
["routine_body"] = r.Language,
["routine_definition"] = r.Body,
["external_language"] = r.Language == "SQL" ? null : r.Language,
["is_deterministic"] = "NO",
["security_type"] = (object?)null,
["created"] = r.CreationTime,
["last_altered"] = r.CreationTime,
}, alias)).ToList();
}
// Ref: https://cloud.google.com/bigquery/docs/information-schema-views
//   "The INFORMATION_SCHEMA.VIEWS view contains metadata about views."
if (tableName.EndsWith("VIEWS", StringComparison.OrdinalIgnoreCase))
{
return ds.Tables.Values
.Where(t => t.ViewQuery is not null)
.Select(t => new RowContext(new Dictionary<string, object?>
{
["table_catalog"] = _store.ProjectId,
["table_schema"] = dsId,
["table_name"] = t.TableId,
["view_definition"] = t.ViewDefinitionSql ?? "(view definition not available)",
["check_option"] = "NONE",
["use_standard_sql"] = "YES",
}, alias)).ToList();
}
// Ref: https://cloud.google.com/bigquery/docs/information-schema-table-options
//   "The INFORMATION_SCHEMA.TABLE_OPTIONS view contains one row for each option."
if (tableName.EndsWith("TABLE_OPTIONS", StringComparison.OrdinalIgnoreCase))
{
var rows = new List<RowContext>();
foreach (var t in ds.Tables.Values)
{
if (t.Description is not null)
rows.Add(MakeTableOptionRow(dsId, t.TableId, "description", "STRING",
$"\"{t.Description}\"", alias));
if (t.FriendlyName is not null)
rows.Add(MakeTableOptionRow(dsId, t.TableId, "friendly_name", "STRING",
$"\"{t.FriendlyName}\"", alias));
if (t.Labels is { Count: > 0 })
{
var labelPairs = string.Join(", ",
t.Labels.Select(kv => $"\"{kv.Key}\", \"{kv.Value}\""));
rows.Add(MakeTableOptionRow(dsId, t.TableId, "labels",
"ARRAY<STRUCT<STRING, STRING>>", $"[{labelPairs}]", alias));
}
}
return rows;
}
// Ref: https://cloud.google.com/bigquery/docs/information-schema-column-field-paths
//   "The INFORMATION_SCHEMA.COLUMN_FIELD_PATHS view contains one row for each column
//    nested within a RECORD (or STRUCT) column."
if (tableName.EndsWith("COLUMN_FIELD_PATHS", StringComparison.OrdinalIgnoreCase))
{
var rows = new List<RowContext>();
foreach (var t in ds.Tables.Values)
FlattenFieldPaths(rows, dsId, t.TableId, t.Schema.Fields, "", alias);
return rows;
}
// Ref: https://cloud.google.com/bigquery/docs/information-schema-partitions
//   "The INFORMATION_SCHEMA.PARTITIONS view provides one row for each partition."
if (tableName.EndsWith("PARTITIONS", StringComparison.OrdinalIgnoreCase))
{
var rows = new List<RowContext>();
foreach (var t in ds.Tables.Values)
{
if (t.TimePartitioning is null && t.RangePartitioning is null) continue;
var partitionField = t.TimePartitioning?.Field;
var groups = new Dictionary<string, long>();
lock (t.RowLock)
{
foreach (var row in t.Rows)
{
var partId = "__UNPARTITIONED__";
if (partitionField is not null &&
row.Fields.TryGetValue(partitionField, out var val) && val is not null)
{
partId = val.ToString()?.Replace("-", "") ?? "__NULL__";
}
groups.TryGetValue(partId, out var count);
groups[partId] = count + 1;
}
}
if (groups.Count == 0)
groups["__UNPARTITIONED__"] = 0;
foreach (var (partId, count) in groups)
{
rows.Add(new RowContext(new Dictionary<string, object?>
{
["table_catalog"] = _store.ProjectId,
["table_schema"] = dsId,
["table_name"] = t.TableId,
["partition_id"] = partId,
["total_rows"] = count,
["total_logical_bytes"] = (long)0,
}, alias));
}
}
return rows;
}
return [];
}

private RowContext MakeTableOptionRow(string dsId, string tableId,
string optionName, string optionType, string optionValue, string alias)
{
return new RowContext(new Dictionary<string, object?>
{
["table_catalog"] = _store.ProjectId,
["table_schema"] = dsId,
["table_name"] = tableId,
["option_name"] = optionName,
["option_type"] = optionType,
["option_value"] = optionValue,
}, alias);
}

private void FlattenFieldPaths(List<RowContext> rows, string dsId, string tableId,
IList<Google.Apis.Bigquery.v2.Data.TableFieldSchema> fields, string prefix, string alias)
{
foreach (var f in fields)
{
var path = string.IsNullOrEmpty(prefix) ? f.Name : $"{prefix}.{f.Name}";
var topColumn = string.IsNullOrEmpty(prefix) ? f.Name : prefix.Split('.')[0];
rows.Add(new RowContext(new Dictionary<string, object?>
{
["table_catalog"] = _store.ProjectId,
["table_schema"] = dsId,
["table_name"] = tableId,
["column_name"] = topColumn,
["field_path"] = path,
["data_type"] = NormalizeFieldType(f.Type),
["description"] = (object?)null,
}, alias));
if (f.Fields is { Count: > 0 })
FlattenFieldPaths(rows, dsId, tableId, f.Fields, path, alias);
}
}

private List<RowContext> ResolveWildcardTable(string pattern, string alias, string? datasetId)
{
var dsId = datasetId ?? _defaultDatasetId;
if (dsId is null || !_store.Datasets.TryGetValue(dsId, out var ds)) return [];
var cleanPattern = pattern.Replace("`", "");
var prefix = cleanPattern.Replace("*", "");
var rows = new List<RowContext>();
foreach (var (tableName, table) in ds.Tables)
{
if (!tableName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) continue;
var suffix = tableName[prefix.Length..];
lock (table.RowLock)
{
foreach (var row in table.Rows)
{
var fields = new Dictionary<string, object?>(row.Fields) { ["_TABLE_SUFFIX"] = suffix };
foreach (var kv in row.Fields) fields[alias + "." + kv.Key] = kv.Value;
fields[alias + "._TABLE_SUFFIX"] = suffix;
rows.Add(new RowContext(fields, alias));
}
}
}
return rows;
}

private List<RowContext> ResolveJoin(JoinClause join,
Dictionary<string, (TableSchema, List<Dictionary<string, object?>>)>? cteResults)
{
var left = ResolveFrom(join.Left, cteResults);
// For cross join with UNNEST, the UNNEST expression may reference columns from the left side
if (join.Type == JoinType.Cross && join.Right is UnnestClause unnest)
{
return CorrelatedCrossJoinUnnest(left, unnest);
}
var right = ResolveFrom(join.Right, cteResults);

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#using_clause
//   "The USING clause is equivalent to an ON clause that tests equality for each named column."
var onExpr = join.On;
if (onExpr is null && join.UsingColumns is { Count: > 0 })
{
	// Synthesize ON condition: col1 = col1 AND col2 = col2 ...
	// Use unqualified column refs since both sides have the column
	SqlExpression? condition = null;
	foreach (var col in join.UsingColumns)
	{
		// Find which aliases the left and right rows use so we can qualify the refs
		var leftAlias = FindAliasForColumn(left, col);
		var rightAlias = FindAliasForColumn(right, col);
		var leftRef = leftAlias != null ? new ColumnRef(leftAlias, col) : new ColumnRef(null, col);
		var rightRef = rightAlias != null ? new ColumnRef(rightAlias, col) : new ColumnRef(null, col);
		// If both are unqualified, try to differentiate using table alias from the row
		SqlExpression eq;
		if (leftAlias != null || rightAlias != null)
			eq = new BinaryExpr(leftRef, BinaryOp.Eq, rightRef);
		else
			eq = new UsingJoinCondition(col);
		condition = condition is null ? eq : new BinaryExpr(condition, BinaryOp.And, eq);
	}
	onExpr = condition;
}

return join.Type switch
{
JoinType.Cross => CrossJoin(left, right),
JoinType.Inner => InnerJoin(left, right, onExpr!),
JoinType.Left => LeftJoin(left, right, onExpr!),
JoinType.Right => RightJoin(left, right, onExpr!),
JoinType.Full => FullJoin(left, right, onExpr!),
_ => throw new NotSupportedException("Unsupported join type: " + join.Type)
};
}

private static string? FindAliasForColumn(List<RowContext> rows, string column)
{
	if (rows.Count == 0) return null;
	var row = rows[0];
	// Check if there's a qualified key pattern: alias.column
	foreach (var key in row.Fields.Keys)
	{
		var dotIdx = key.IndexOf('.');
		if (dotIdx > 0 && key.Substring(dotIdx + 1).Equals(column, StringComparison.OrdinalIgnoreCase))
			return key.Substring(0, dotIdx);
	}
	return null;
}

private List<RowContext> CorrelatedCrossJoinUnnest(List<RowContext> left, UnnestClause unnest)
{
var result = new List<RowContext>();
foreach (var l in left)
{
var rightRows = ResolveUnnest(unnest, l);
foreach (var r in rightRows) result.Add(MergeRows(l, r));
}
return result;
}

private static List<RowContext> CrossJoin(List<RowContext> left, List<RowContext> right)
{
var result = new List<RowContext>();
foreach (var l in left) foreach (var r in right) result.Add(MergeRows(l, r));
return result;
}

private List<RowContext> InnerJoin(List<RowContext> left, List<RowContext> right, SqlExpression on)
{
var result = new List<RowContext>();
foreach (var l in left) foreach (var r in right)
{
var merged = MergeRows(l, r);
if (IsTruthy(Evaluate(on, merged))) result.Add(merged);
}
return result;
}

private List<RowContext> LeftJoin(List<RowContext> left, List<RowContext> right, SqlExpression on)
{
var result = new List<RowContext>();
foreach (var l in left)
{
bool matched = false;
foreach (var r in right)
{
var merged = MergeRows(l, r);
if (IsTruthy(Evaluate(on, merged))) { result.Add(merged); matched = true; }
}
if (!matched) result.Add(MergeRows(l, NullRow(right.FirstOrDefault())));
}
return result;
}

private List<RowContext> RightJoin(List<RowContext> left, List<RowContext> right, SqlExpression on)
{
var result = new List<RowContext>();
foreach (var r in right)
{
bool matched = false;
foreach (var l in left)
{
var merged = MergeRows(l, r);
if (IsTruthy(Evaluate(on, merged))) { result.Add(merged); matched = true; }
}
if (!matched) result.Add(MergeRows(NullRow(left.FirstOrDefault()), r));
}
return result;
}

private List<RowContext> FullJoin(List<RowContext> left, List<RowContext> right, SqlExpression on)
{
var result = new List<RowContext>();
var matchedRight = new HashSet<int>();
foreach (var l in left)
{
bool matched = false;
for (int i = 0; i < right.Count; i++)
{
var merged = MergeRows(l, right[i]);
if (IsTruthy(Evaluate(on, merged))) { result.Add(merged); matched = true; matchedRight.Add(i); }
}
if (!matched) result.Add(MergeRows(l, NullRow(right.FirstOrDefault())));
}
for (int i = 0; i < right.Count; i++)
if (!matchedRight.Contains(i)) result.Add(MergeRows(NullRow(left.FirstOrDefault()), right[i]));
return result;
}

private static RowContext MergeRows(RowContext left, RowContext right)
{
var fields = new Dictionary<string, object?>(left.Fields);
foreach (var kv in right.Fields) fields.TryAdd(kv.Key, kv.Value);
return new RowContext(fields, null);
}

private static RowContext NullRow(RowContext? template)
{
if (template is null) return new RowContext(new Dictionary<string, object?>(), null);
var fields = new Dictionary<string, object?>();
foreach (var kv in template.Fields) fields[kv.Key] = null;
return new RowContext(fields, null);
}

private List<RowContext> ResolveSubquery(SubqueryFrom sub)
{
var result = sub.Subquery switch
{
    SelectStatement sel => ExecuteSelect(sel, _activeCteResults),
    SetOperationStatement setOp => ExecuteSetOperation(setOp),
    _ => throw new NotSupportedException($"Unsupported subquery type: {sub.Subquery.GetType().Name}")
};
var alias = sub.Alias ?? "subquery";
return result.Rows.Select(r =>
{
var dict = RowToDict(r, result.Schema);
// Parse formatted string values back to typed values based on schema
// This is needed because FormatValue converts typed values to strings for the REST API,
// but derived table consumers need typed values for correct comparison/arithmetic.
var typedDict = ParseTypedRow(dict, result.Schema);
var allFields = new Dictionary<string, object?>(typedDict);
foreach (var kv in typedDict) allFields[alias + "." + kv.Key] = kv.Value;
return new RowContext(allFields, alias);
}).ToList();
}

private List<RowContext> ResolveUnnest(UnnestClause unnest, RowContext? parentRow = null)
{
var alias = unnest.Alias ?? "unnest";
var evalRow = parentRow ?? new RowContext(new Dictionary<string, object?>(), null);
var value = Evaluate(unnest.Expr, evalRow);
if (value is IEnumerable<object?> list)
{
int idx = 0;
return list.Select(item =>
{
var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
if (item is IDictionary<string, object?> structFields)
{
foreach (var kv in structFields)
{
fields[kv.Key] = kv.Value;
fields[alias + "." + kv.Key] = kv.Value;
}
}
fields[alias] = item;
if (unnest.OffsetAlias is not null)
{
fields[unnest.OffsetAlias] = (long)idx;
fields[alias + "." + unnest.OffsetAlias] = (long)idx;
}
idx++;
return new RowContext(fields, alias);
}).ToList();
}
return [];
}

#endregion
#region Projection

private (TableSchema Schema, List<TableRow> Rows) Project(
SelectStatement stmt, List<RowContext> rows,
Dictionary<string, (TableSchema, List<Dictionary<string, object?>>)>? cteResults = null)
{
var items = stmt.Columns;
var schemaFields = new List<TableFieldSchema>();
var resultRows = new List<TableRow>();

foreach (var row in rows)
{
var cells = new Dictionary<string, object?>();
foreach (var item in items)
{
if (item.Expr is StarExpr star)
{
var expanded = ExpandStar(row, star.TableAlias);
foreach (var kv in expanded)
{
if (star.ExceptColumns is not null && star.ExceptColumns.Contains(kv.Key, StringComparer.OrdinalIgnoreCase)) continue;
var replaceExpr = star.ReplaceColumns?.Where(r => r.Alias.Equals(kv.Key, StringComparison.OrdinalIgnoreCase)).Select(r => r.Expr).FirstOrDefault();
cells[kv.Key] = replaceExpr is not null ? Evaluate(replaceExpr, row) : kv.Value;
}
}
else
{
var colName = item.Alias ?? DeriveColumnName(item.Expr);
						while (cells.ContainsKey(colName)) colName += "_";
cells[colName] = Evaluate(item.Expr, row, cteResults: cteResults);
}
}
resultRows.Add(DictToTableRow(cells));
if (schemaFields.Count == 0)
schemaFields.AddRange(cells.Keys.Select(k => new TableFieldSchema { Name = k, Type = InferType(cells[k]) }));
}

if (schemaFields.Count == 0)
		schemaFields.AddRange(items.Where(i => i.Expr is not StarExpr).Select(i => new TableFieldSchema { Name = i.Alias ?? DeriveColumnName(i.Expr), Type = "STRING" }));

return (new TableSchema { Fields = schemaFields }, resultRows);
}

private (TableSchema Schema, List<TableRow> Rows) ProjectWithWindows(
SelectStatement stmt, List<RowContext> rows)
{
var schemaFields = new List<TableFieldSchema>();
var resultRows = new List<TableRow>();

foreach (var row in rows)
{
		_windowContext = (row, rows);
var cells = new Dictionary<string, object?>();
foreach (var item in stmt.Columns)
{
if (item.Expr is StarExpr star)
{
var expanded = ExpandStar(row, star.TableAlias);
foreach (var kv in expanded)
{
if (star.ExceptColumns is not null && star.ExceptColumns.Contains(kv.Key, StringComparer.OrdinalIgnoreCase)) continue;
var replaceExpr = star.ReplaceColumns?.Where(r => r.Alias.Equals(kv.Key, StringComparison.OrdinalIgnoreCase)).Select(r => r.Expr).FirstOrDefault();
cells[kv.Key] = replaceExpr is not null ? Evaluate(replaceExpr, row) : kv.Value;
}
}
else if (item.Expr is WindowFunction wf)
{
var colName = item.Alias ?? DeriveColumnName(wf);
cells[colName] = EvaluateWindow(wf, row, rows);
}
else
{
var colName = item.Alias ?? DeriveColumnName(item.Expr);
						while (cells.ContainsKey(colName)) colName += "_";
cells[colName] = Evaluate(item.Expr, row);
}
}
resultRows.Add(DictToTableRow(cells));
if (schemaFields.Count == 0)
schemaFields.AddRange(cells.Keys.Select(k => new TableFieldSchema { Name = k, Type = InferType(cells[k]) }));
}

return (new TableSchema { Fields = schemaFields }, resultRows);
}

/// <summary>Evaluate an expression that may contain window functions (used by QUALIFY).</summary>
private object? EvaluateWithWindows(SqlExpression expr, RowContext evalRow, RowContext windowRow, List<RowContext> allRows)
{
    return expr switch
    {
        WindowFunction wf => EvaluateWindow(wf, windowRow, allRows),
        BinaryExpr bin => EvaluateBinaryWithWindows(bin, evalRow, windowRow, allRows),
        UnaryExpr ue => ue.Op switch
        {
            UnaryOp.Not => !IsTruthy(EvaluateWithWindows(ue.Operand, evalRow, windowRow, allRows)),
            _ => Evaluate(expr, evalRow)
        },
        _ => Evaluate(expr, evalRow)
    };
}

private object? EvaluateBinaryWithWindows(BinaryExpr bin, RowContext evalRow, RowContext windowRow, List<RowContext> allRows)
{
    var left = bin.Left is WindowFunction lwf ? new LiteralExpr(EvaluateWindow(lwf, windowRow, allRows)) : bin.Left;
    var right = bin.Right is WindowFunction rwf ? new LiteralExpr(EvaluateWindow(rwf, windowRow, allRows)) : bin.Right;
    return EvaluateBinary(new BinaryExpr(left, bin.Op, right), evalRow);
}

private object? EvaluateWindow(WindowFunction wf, RowContext currentRow, List<RowContext> allRows)
{
var partitioned = allRows.AsEnumerable();
if (wf.PartitionBy is { Count: > 0 })
partitioned = allRows.Where(r =>
wf.PartitionBy.All(p => Equals(Evaluate(p, r), Evaluate(p, currentRow))));

var partition = partitioned.ToList();

if (wf.OrderBy is { Count: > 0 })
partition = OrderBy(partition, wf.OrderBy);

var funcName = (wf.Function is FunctionCall wfFn ? wfFn.FunctionName : wf.Function is AggregateCall wfAgg ? wfAgg.FunctionName : "").ToUpperInvariant();

if (funcName == "ROW_NUMBER")
return (long)(partition.IndexOf(currentRow) + 1);

if (funcName == "RANK")
{
// RANK = position of first row with same ORDER BY values + 1
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#rank
for (int i = 0; i < partition.Count; i++)
{
bool same = wf.OrderBy?.All(o =>
Equals(Evaluate(o.Expr, partition[i]), Evaluate(o.Expr, currentRow))) ?? true;
if (same) return (long)(i + 1);
}
return (long)(partition.IndexOf(currentRow) + 1);
}

if (funcName == "DENSE_RANK")
{
var idx = partition.IndexOf(currentRow);
int rank = 1;
for (int i = 1; i <= idx; i++)
{
bool same = wf.OrderBy?.All(o =>
Equals(Evaluate(o.Expr, partition[i - 1]), Evaluate(o.Expr, partition[i]))) ?? true;
if (!same) rank++;
}
return (long)rank;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#ntile
//   "Divides the rows into constant_integer_expression buckets based on row ordering
//    and returns the 1-based bucket number that is assigned to each row."
if (funcName == "NTILE")
{
var fnArgs = wf.Function is FunctionCall ntileFn ? ntileFn.Args : [];
var n = (int)ToLong(Evaluate(fnArgs[0], currentRow));
if (n <= 0) throw new InvalidOperationException("NTILE requires a positive integer");
var idx = partition.IndexOf(currentRow);
int totalRows = partition.Count;
int baseSize = totalRows / n;
int remainder = totalRows % n;
// Buckets 1..remainder have baseSize+1 rows; remainder+1..n have baseSize rows
int bucket = 0;
int accumulated = 0;
for (int b = 1; b <= n; b++)
{
    int bucketSize = baseSize + (b <= remainder ? 1 : 0);
    accumulated += bucketSize;
    if (idx < accumulated) { bucket = b; break; }
}
return (long)bucket;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#percent_rank
//   "Return the percentile rank of a row defined as (RK-1)/(NR-1). Returns 0 if NR=1."
if (funcName == "PERCENT_RANK")
{
int nr = partition.Count;
if (nr <= 1) return 0.0;
// Compute RANK for current row
long rk = 1;
for (int i = 0; i < partition.Count; i++)
{
    bool same = wf.OrderBy?.All(o =>
        Equals(Evaluate(o.Expr, partition[i]), Evaluate(o.Expr, currentRow))) ?? true;
    if (same) { rk = i + 1; break; }
}
return (double)(rk - 1) / (nr - 1);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#cume_dist
//   "Return the relative rank of a row defined as NP/NR."
if (funcName == "CUME_DIST")
{
int nr = partition.Count;
// NP = number of rows that precede or are peers with the current row
int np = 0;
for (int i = 0; i < partition.Count; i++)
{
    bool same = wf.OrderBy?.All(o =>
        Equals(Evaluate(o.Expr, partition[i]), Evaluate(o.Expr, currentRow))) ?? true;
    // Rows that precede (come before or at same ORDER BY value)
    if (CompareOrderByRow(partition[i], currentRow, wf.OrderBy) <= 0) np++;
}
return (double)np / nr;
}

// Navigation functions: FIRST_VALUE, LAST_VALUE, LAG, LEAD
var navArgs = wf.Function is FunctionCall navFn ? navFn.Args : [];

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#first_value
//   "Returns the value of the value_expression for the first row in the current window frame."
if (funcName == "FIRST_VALUE")
{
var framedPartition = GetFramedPartition(wf, partition, currentRow);
return Evaluate(navArgs[0], framedPartition[0]);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#last_value
//   "Returns the value of the value_expression for the last row in the current window frame."
if (funcName == "LAST_VALUE")
{
var framedPartition = GetFramedPartition(wf, partition, currentRow);
return Evaluate(navArgs[0], framedPartition[^1]);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#lag
//   "Returns the value of the value_expression on a preceding row."
if (funcName == "LAG")
{
int offset = navArgs.Count > 1 ? (int)ToLong(Evaluate(navArgs[1], currentRow)) : 1;
object? defaultVal = navArgs.Count > 2 ? Evaluate(navArgs[2], currentRow) : null;
var idx = partition.IndexOf(currentRow);
int targetIdx = idx - offset;
return targetIdx >= 0 ? Evaluate(navArgs[0], partition[targetIdx]) : defaultVal;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#lead
//   "Returns the value of the value_expression on a subsequent row."
if (funcName == "LEAD")
{
int offset = navArgs.Count > 1 ? (int)ToLong(Evaluate(navArgs[1], currentRow)) : 1;
object? defaultVal = navArgs.Count > 2 ? Evaluate(navArgs[2], currentRow) : null;
var idx = partition.IndexOf(currentRow);
int targetIdx = idx + offset;
return targetIdx < partition.Count ? Evaluate(navArgs[0], partition[targetIdx]) : defaultVal;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#nth_value
//   "Returns the value of value_expression at the Nth row of the current window frame."
if (funcName == "NTH_VALUE")
{
int n = navArgs.Count > 1 ? (int)ToLong(Evaluate(navArgs[1], currentRow)) : 1;
if (n <= 0) throw new InvalidOperationException("NTH_VALUE requires a positive integer for N");
var framedPartition = GetFramedPartition(wf, partition, currentRow);
return n <= framedPartition.Count ? Evaluate(navArgs[0], framedPartition[n - 1]) : null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#percentile_cont
//   "Computes the specified percentile value for the value_expression, with linear interpolation."
if (funcName == "PERCENTILE_CONT")
{
var percentile = Convert.ToDouble(Evaluate(navArgs[1], currentRow));
var sortedValues = partition
	.Select(r => Evaluate(navArgs[0], r))
	.Where(v => v is not null)
	.Select(v => Convert.ToDouble(v))
	.OrderBy(v => v)
	.ToList();
if (sortedValues.Count == 0) return null;
if (sortedValues.Count == 1) return sortedValues[0];
var rank = percentile * (sortedValues.Count - 1);
int lower = (int)Math.Floor(rank);
int upper = (int)Math.Ceiling(rank);
if (lower == upper) return sortedValues[lower];
var frac = rank - lower;
return sortedValues[lower] + frac * (sortedValues[upper] - sortedValues[lower]);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#percentile_disc
//   "Computes the specified percentile value for a discrete value_expression."
if (funcName == "PERCENTILE_DISC")
{
var percentile = Convert.ToDouble(Evaluate(navArgs[1], currentRow));
var sortedValues = partition
	.Select(r => Evaluate(navArgs[0], r))
	.Where(v => v is not null)
	.OrderBy(v => v, Comparer<object?>.Create((a, b) => CompareRaw(a!, b!)))
	.ToList();
if (sortedValues.Count == 0) return null;
if (percentile <= 0) return sortedValues[0];
// Return first value whose CUME_DIST >= percentile
for (int i = 0; i < sortedValues.Count; i++)
{
	double cumeDist = (double)(i + 1) / sortedValues.Count;
	if (cumeDist >= percentile) return sortedValues[i];
}
return sortedValues[^1];
}

// Window aggregate functions
if (wf.Function is AggregateCall windowAgg)
{
    var framedPartition = GetFramedPartition(wf, partition, currentRow);
    return EvaluateAggregate(windowAgg, framedPartition);
}
return null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#window_frame_clause
//   "If ORDER BY is specified but no window frame, default is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW."
//   "If neither ORDER BY nor window frame is specified, default is ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING."
private List<RowContext> GetFramedPartition(WindowFunction wf, List<RowContext> partition, RowContext currentRow)
{
    var frame = wf.Frame;
    var currentIdx = partition.IndexOf(currentRow);

    if (frame is null)
    {
        if (wf.OrderBy is { Count: > 0 })
        {
            frame = new FrameSpec(FrameType.Range,
                new FrameBoundary(FrameBoundaryType.UnboundedPreceding, null),
                new FrameBoundary(FrameBoundaryType.CurrentRow, null));
        }
        else
        {
            return partition;
        }
    }

    int startIdx = ResolveBoundaryIndex(frame.Start, currentIdx, partition.Count, wf, partition, currentRow, isStart: true);
    int endIdx = ResolveBoundaryIndex(frame.End, currentIdx, partition.Count, wf, partition, currentRow, isStart: false);

    startIdx = Math.Max(0, startIdx);
    endIdx = Math.Min(partition.Count - 1, endIdx);

    if (startIdx > endIdx) return new List<RowContext>();
    return partition.GetRange(startIdx, endIdx - startIdx + 1);
}

private int ResolveBoundaryIndex(FrameBoundary boundary, int currentIdx, int partitionCount,
    WindowFunction wf, List<RowContext> partition, RowContext currentRow, bool isStart)
{
    return boundary.Type switch
    {
        FrameBoundaryType.UnboundedPreceding => 0,
        FrameBoundaryType.UnboundedFollowing => partitionCount - 1,
        FrameBoundaryType.CurrentRow => wf.Frame?.Type == FrameType.Range
            ? (isStart
                ? FindFirstPeer(partition, currentRow, wf.OrderBy, currentIdx)
                : FindLastPeer(partition, currentRow, wf.OrderBy, currentIdx))
            : currentIdx,
        FrameBoundaryType.Preceding => boundary.Offset is LiteralExpr { Value: long offset }
            ? currentIdx - (int)offset
            : currentIdx,
        FrameBoundaryType.Following => boundary.Offset is LiteralExpr { Value: long offset }
            ? currentIdx + (int)offset
            : currentIdx,
        _ => currentIdx
    };
}

private int FindFirstPeer(List<RowContext> partition, RowContext currentRow, IReadOnlyList<OrderByItem>? orderBy, int currentIdx)
{
    for (int i = currentIdx - 1; i >= 0; i--)
    {
        if (CompareOrderByRow(partition[i], currentRow, orderBy) != 0)
            return i + 1;
    }
    return 0;
}

private int FindLastPeer(List<RowContext> partition, RowContext currentRow, IReadOnlyList<OrderByItem>? orderBy, int currentIdx)
{
    for (int i = currentIdx + 1; i < partition.Count; i++)
    {
        if (CompareOrderByRow(partition[i], currentRow, orderBy) != 0)
            return i - 1;
    }
    return partition.Count - 1;
}


private int CompareOrderByRow(RowContext a, RowContext b, IReadOnlyList<OrderByItem>? orderBy)
{
if (orderBy is null || orderBy.Count == 0) return 0;
foreach (var item in orderBy)
{
    var va = Evaluate(item.Expr, a);
    var vb = Evaluate(item.Expr, b);
    int cmp = CompareRaw(va, vb);
    if (item.Descending) cmp = -cmp;
    if (cmp != 0) return cmp;
}
return 0;
}

private Dictionary<string, object?> ExpandStar(RowContext row, string? tableAlias)
{
var result = new Dictionary<string, object?>();
var prefix = tableAlias is not null ? tableAlias + "." : null;
foreach (var kv in row.Fields)
{
if (kv.Key.Contains('.')) continue; // skip qualified names
if (prefix is not null)
{
if (row.Fields.ContainsKey(prefix + kv.Key))
result[kv.Key] = kv.Value;
}
else
{
if (!kv.Key.StartsWith("_PARTITION")) // skip pseudo columns in star
result[kv.Key] = kv.Value;
}
}
if (result.Count == 0)
{
// Fallback: include all unqualified fields
foreach (var kv in row.Fields)
if (!kv.Key.Contains('.')) result[kv.Key] = kv.Value;
}
return result;
}

#endregion
#region Expression evaluation

private object? Evaluate(SqlExpression expr, RowContext row,
Dictionary<string, (TableSchema, List<Dictionary<string, object?>>)>? cteResults = null)
{
return expr switch
{
LiteralExpr lit => lit.Value,
ColumnRef col => EvaluateColumnRef(col, row),
ParameterRef p => ResolveParameter(p.Name),
BinaryExpr bin => EvaluateBinary(bin, row),
UnaryExpr un => EvaluateUnary(un, row),
FunctionCall fn => EvaluateFunctionCall(fn, row),
AggregateCall _ => throw new InvalidOperationException("Aggregate outside GROUP BY"),
IsNullExpr isNull => Evaluate(isNull.Expr, row) is null == !isNull.IsNot,
IsBoolExpr isBool => EvaluateIsBool(isBool, row),
BetweenExpr btw => EvaluateBetween(btw, row),
InExpr inExpr => EvaluateIn(inExpr, row),
InSubqueryExpr inSub => EvaluateInSubquery(inSub, row),
InUnnestExpr inUnnest => EvaluateInUnnest(inUnnest, row),
LikeExpr like => EvaluateLike(like, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#using_clause
//   "USING compares columns of the same name from both sides of the join."
UsingJoinCondition ujc => EvaluateUsingJoinCondition(ujc, row),
CastExpr cast => EvaluateCast(cast, row),
ArraySubscriptExpr sub => EvaluateArraySubscript(sub, row),
CaseExpr caseExpr => EvaluateCase(caseExpr, row),
ScalarSubquery sub => EvaluateScalarSubquery(sub, row),
ExistsExpr exists => EvaluateExists(exists, row),
ArraySubquery arraySub => EvaluateArraySubquery(arraySub),
StructLiteralExpr structLit => EvaluateStructLiteral(structLit, row),
FieldAccessExpr fa => EvaluateFieldAccess(fa, row),
WindowFunction wf => _windowContext.HasValue ? EvaluateWindow(wf, _windowContext.Value.CurrentRow, _windowContext.Value.AllRows) : throw new InvalidOperationException("Window function in non-window context"),
StarExpr => throw new InvalidOperationException("Star expression in non-projection context"),
VariableRef v => _parameters?.FirstOrDefault(p => p.Name == v.Name)?.ParameterValue?.Value,
LambdaExpr => throw new InvalidOperationException("Lambda expression in non-lambda context"),
_ => throw new NotSupportedException("Unsupported expression: " + expr.GetType().Name)
};
}

private object? EvaluateColumnRef(ColumnRef col, RowContext row)
{
var name = col.ColumnName;
// Check qualified name FIRST when alias is specified (important for MERGE, JOIN contexts)
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#field_path
if (col.TableAlias is not null)
{
var qualified = col.TableAlias + "." + name;
if (row.Fields.TryGetValue(qualified, out var qVal)) return qVal;
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#field_access_operator
//   "Struct field access: expression.fieldname"
if (row.Fields.TryGetValue(col.TableAlias, out var structVal) && structVal is IDictionary<string, object?> structDict)
{
if (structDict.TryGetValue(name, out var fieldVal)) return fieldVal;
}
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#correlated_subqueries
//   When table alias does not match current scope, delegate to outer row context.
if (_outerRowContext is not null)
{
var prevOuter = _outerRowContext;
_outerRowContext = null;
try { return EvaluateColumnRef(col, prevOuter); }
finally { _outerRowContext = prevOuter; }
}
return null;
}
if (row.Fields.TryGetValue(name, out var val)) return val;
// Try any qualified match
foreach (var kv in row.Fields)
{
if (kv.Key.EndsWith("." + name, StringComparison.OrdinalIgnoreCase))
return kv.Value;
}
// Case-insensitive fallback
foreach (var kv in row.Fields)
{
if (kv.Key.Equals(name, StringComparison.OrdinalIgnoreCase))
return kv.Value;
}
// Fall back to outer row context for correlated subqueries (unqualified columns).
if (_outerRowContext is not null)
{
var prevOuter = _outerRowContext;
_outerRowContext = null;
try { return EvaluateColumnRef(col, prevOuter); }
finally { _outerRowContext = prevOuter; }
}
return null;
}

private object? EvaluateBinary(BinaryExpr bin, RowContext row)
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#logical_operators
//   Three-valued logic: NULL AND FALSE = FALSE, NULL AND TRUE = NULL, NULL OR TRUE = TRUE, NULL OR FALSE = NULL.
if (bin.Op == BinaryOp.And)
{
var l = Evaluate(bin.Left, row);
if (l is not null && !IsTruthy(l)) return false;
var r = Evaluate(bin.Right, row);
if (r is not null && !IsTruthy(r)) return false;
if (l is null || r is null) return null;
return true;
}
if (bin.Op == BinaryOp.Or)
{
var l = Evaluate(bin.Left, row);
if (l is not null && IsTruthy(l)) return true;
var r = Evaluate(bin.Right, row);
if (r is not null && IsTruthy(r)) return true;
if (l is null || r is null) return null;
return false;
}
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
//   "?? is equivalent to COALESCE(a, b): returns a if a is not null, otherwise b."
if (bin.Op == BinaryOp.NullCoalesce)
{
var l = Evaluate(bin.Left, row);
return l ?? Evaluate(bin.Right, row);
}

var left = Evaluate(bin.Left, row);
var right = Evaluate(bin.Right, row);

return bin.Op switch
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#comparison_operators
//   "All comparisons return NULL when either of the values being compared is NULL."
BinaryOp.Eq => left is null || right is null ? null : CompareRaw(left, right) == 0,
BinaryOp.Neq => left is null || right is null ? null : CompareRaw(left, right) != 0,
BinaryOp.Lt => left is null || right is null ? null : CompareRaw(left, right) < 0,
BinaryOp.Lte => left is null || right is null ? null : CompareRaw(left, right) <= 0,
BinaryOp.Gt => left is null || right is null ? null : CompareRaw(left, right) > 0,
BinaryOp.Gte => left is null || right is null ? null : CompareRaw(left, right) >= 0,
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#date_arithmetics
//   "DATE + INT64 → DATE: adds a number of days to the date."
BinaryOp.Add => left is DateOnly dLeft && right is long rDays ? dLeft.AddDays((int)rDays)
    : right is DateOnly dRight && left is long lDays ? dRight.AddDays((int)lDays)
    : ArithmeticOp(left, right, (a, b) => a + b, (a, b) => a + b),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#date_arithmetics
//   "DATE - INT64 → DATE: subtracts a number of days from the date."
//   "DATE - DATE → INT64: returns the number of days between two dates."
BinaryOp.Sub => left is DateOnly dSubLeft && right is long rSubDays ? dSubLeft.AddDays(-(int)rSubDays)
    : left is DateOnly dSubL && right is DateOnly dSubR ? (object)(long)(dSubL.ToDateTime(TimeOnly.MinValue) - dSubR.ToDateTime(TimeOnly.MinValue)).Days
    : ArithmeticOp(left, right, (a, b) => a - b, (a, b) => a - b),
BinaryOp.Mul => ArithmeticOp(left, right, (a, b) => a * b, (a, b) => a * b),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#division
//   "Division always returns a FLOAT64, even for integer operands."
BinaryOp.Div => left is null || right is null ? null
    : ToDouble(right) == 0.0 ? throw new DivideByZeroException()
    : (object)(ToDouble(left) / ToDouble(right)),
BinaryOp.Mod => ArithmeticOp(left, right, (a, b) => a % b, (a, b) => a % b),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#concatenation_operator
//   "If one of the operands is NULL, the result is NULL."
BinaryOp.Concat => left is null || right is null ? null : left.ToString() + right.ToString(),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#bitwise_operators
BinaryOp.BitAnd => BitwiseOp(left, right, (a, b) => a & b),
BinaryOp.BitOr => BitwiseOp(left, right, (a, b) => a | b),
BinaryOp.BitXor => BitwiseOp(left, right, (a, b) => a ^ b),
BinaryOp.ShiftLeft => BitwiseOp(left, right, (a, b) => a << (int)b),
BinaryOp.ShiftRight => BitwiseOp(left, right, (a, b) => a >> (int)b),
BinaryOp.And => left is not null && !IsTruthy(left) ? false : right is not null && !IsTruthy(right) ? (object)false : left is null || right is null ? null : (object)true,
BinaryOp.Or => left is not null && IsTruthy(left) ? true : right is not null && IsTruthy(right) ? (object)true : left is null || right is null ? null : (object)false,
_ => throw new NotSupportedException("Unsupported binary operator: " + bin.Op)
};
}

private object? EvaluateUnary(UnaryExpr un, RowContext row)
{
var val = Evaluate(un.Operand, row);
return un.Op switch
{
UnaryOp.Not => val is null ? null : !IsTruthy(val),
UnaryOp.Negate => Negate(val),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#bitwise_operators
//   "~ (bitwise not) — Performs logical negation on each bit."
UnaryOp.BitNot => val is null ? null : val switch
{
long l => (object)~l,
int i => (object)(long)~i,
_ => (object)~Convert.ToInt64(val, CultureInfo.InvariantCulture)
},
_ => throw new NotSupportedException("Unsupported unary operator: " + un.Op)
};
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#bitwise_operators
private static object? BitwiseOp(object? left, object? right, Func<long, long, long> op)
{
if (left is null || right is null) return null;
var l = left is long ll ? ll : Convert.ToInt64(left, CultureInfo.InvariantCulture);
var r = right is long rl ? rl : Convert.ToInt64(right, CultureInfo.InvariantCulture);
return op(l, r);
}

private object? EvaluateIsBool(IsBoolExpr isBool, RowContext row)
{
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#is_operators
    //   "expr IS [NOT] TRUE / FALSE - three-valued logic comparison."
    var val = Evaluate(isBool.Expr, row);
    bool? boolVal = val switch
    {
        bool b => b,
        null => null,
        _ => IsTruthy(val)
    };
    bool result = isBool.Value
        ? boolVal == true    // IS TRUE
        : boolVal == false;  // IS FALSE
    return isBool.IsNot ? !result : result;
}

private object? EvaluateBetween(BetweenExpr btw, RowContext row)
{
var val = Evaluate(btw.Expr, row);
var low = Evaluate(btw.Low, row);
var high = Evaluate(btw.High, row);
if (val is null || low is null || high is null) return null;
var result = CompareRaw(val, low) >= 0 && CompareRaw(val, high) <= 0;
return result;
}

private object? EvaluateIn(InExpr inExpr, RowContext row)
{
var val = Evaluate(inExpr.Expr, row);
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
//   "Returns NULL if search_value is NULL."
if (val is null) return null;
bool hasNull = false;
foreach (var v in inExpr.Values)
{
	var item = Evaluate(v, row);
	if (item is null) { hasNull = true; continue; }
	if (Equals(val, item)) return true;
}
return hasNull ? null : false;
}

private object? EvaluateInSubquery(InSubqueryExpr inSub, RowContext row)
{
var val = Evaluate(inSub.Expr, row);
// Pass active CTE results so subqueries can reference outer CTEs
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#cte_rules
var result = ExecuteSelect(inSub.Subquery, _activeCteResults);
var values = result.Rows.Select(r => r.F?[0]?.V).ToList();
var found = values.Any(v => CompareRaw(val, v) == 0);
return found;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#using_clause
//   "The USING clause requires that the column exists on both sides of the join."
private object? EvaluateUsingJoinCondition(UsingJoinCondition ujc, RowContext row)
{
	var col = ujc.ColumnName;
	// Find all instances of this column in the row (left.col, right.col, or unqualified col)
	var values = new List<object?>();
	foreach (var kv in row.Fields)
	{
		var dotIdx = kv.Key.IndexOf('.');
		var fieldName = dotIdx > 0 ? kv.Key.Substring(dotIdx + 1) : kv.Key;
		if (fieldName.Equals(col, StringComparison.OrdinalIgnoreCase))
			values.Add(kv.Value);
	}
	// If we found at least 2, compare them; if they are all equal, return true
	if (values.Count >= 2)
	{
		var first = values[0];
		for (int i = 1; i < values.Count; i++)
		{
			if (first is null && values[i] is null) continue;
			if (first is null || values[i] is null) return false;
			if (CompareRaw(first, values[i]) != 0) return false;
		}
		return true;
	}
	// Fallback: use unqualified lookup
	if (row.Fields.TryGetValue(col, out _)) return true;
	return null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
//   "value [NOT] IN UNNEST(array_expression)"
//   "Returns NULL if search_value is NULL."
private object? EvaluateInUnnest(InUnnestExpr inUnnest, RowContext row)
{
var val = Evaluate(inUnnest.Expr, row);
if (val is null) return null;
var arrayVal = Evaluate(inUnnest.ArrayExpr, row);
if (arrayVal is null) return null;
if (arrayVal is IEnumerable<object?> list)
{
    bool hasNull = false;
    foreach (var item in list)
    {
        if (item is null) { hasNull = true; continue; }
        if (CompareRaw(val, item) == 0) return true;
    }
    return hasNull ? null : false;
}
return false;
}

private object? EvaluateLike(LikeExpr like, RowContext row)
{
var val = Evaluate(like.Expr, row)?.ToString();
var pattern = Evaluate(like.Pattern, row)?.ToString();
if (val is null || pattern is null) return null;
// Convert SQL LIKE to regex
var regex = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
.Replace("%", ".*").Replace("_", ".") + "$";
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#like_operator
// "LIKE is case-sensitive."
var result = System.Text.RegularExpressions.Regex.IsMatch(val, regex);
return like.IsNot ? !result : result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_subscript_operator
//   OFFSET(n) = zero-based, ORDINAL(n) = one-based, SAFE_ variants return null for out-of-bounds.
private object? EvaluateArraySubscript(ArraySubscriptExpr sub, RowContext row)
{
    var arrayVal = Evaluate(sub.Array, row);
    var indexVal = Evaluate(sub.Index, row);
    if (arrayVal is not IList<object?> list || indexVal is null) return null;
    var idx = (int)ToLong(indexVal);
    var actualIndex = sub.AccessMode switch
    {
        "OFFSET" or "SAFE_OFFSET" => idx,
        "ORDINAL" or "SAFE_ORDINAL" => idx - 1,
        _ => idx
    };
    var isSafe = sub.AccessMode.StartsWith("SAFE_", StringComparison.Ordinal);
    if (actualIndex < 0 || actualIndex >= list.Count)
    {
        if (isSafe) return null;
        throw new InvalidOperationException("Array index out of bounds");
    }
    return list[actualIndex];
}
private object? EvaluateCast(CastExpr cast, RowContext row)
{
var val = Evaluate(cast.Expr, row);
return CastValue(val, cast.TargetType, cast.Safe);
}

private static object? CastValue(object? val, string targetType, bool isSafe)
{
if (val is null) return null;
try
{
return targetType.ToUpperInvariant() switch
{
"INT64" or "INTEGER" or "INT" => val switch
{
long l => l,
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_int64
//   "Halfway cases such as 1.5 or -0.5 round away from zero."
double d => (long)Math.Round(d, MidpointRounding.AwayFromZero),
bool b => b ? 1L : 0L,
string s => long.Parse(s, CultureInfo.InvariantCulture),
_ => Convert.ToInt64(val, CultureInfo.InvariantCulture)
},
"FLOAT64" or "FLOAT" or "NUMERIC" or "BIGNUMERIC" or "DECIMAL" => val switch
{
double d => d,
long l => (double)l,
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#floating_point_literals
//   "inf and -inf represent positive and negative infinity respectively."
string s when s.Equals("inf", StringComparison.OrdinalIgnoreCase) => double.PositiveInfinity,
string s when s.Equals("-inf", StringComparison.OrdinalIgnoreCase) => double.NegativeInfinity,
string s when s.Equals("nan", StringComparison.OrdinalIgnoreCase) => double.NaN,
string s => double.Parse(s, CultureInfo.InvariantCulture),
_ => Convert.ToDouble(val, CultureInfo.InvariantCulture)
},
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
//   "CAST(bytes AS STRING) decodes using UTF-8 encoding."
"STRING" => val is byte[] bytesVal ? System.Text.Encoding.UTF8.GetString(bytesVal) : ConvertToString(val),
"BOOL" or "BOOLEAN" => val switch
{
bool b => b,
string s => bool.Parse(s),
long l => l != 0,
_ => Convert.ToBoolean(val, CultureInfo.InvariantCulture)
},
"TIMESTAMP" => val switch
{
DateTimeOffset dto => dto,
DateOnly d => new DateTimeOffset(d.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero),
DateTime dt => new DateTimeOffset(dt, TimeSpan.Zero),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#timestamp_literals
//   BigQuery accepts "UTC" as a timezone suffix, but .NET ParseExact doesn't.
//   Normalize "UTC" to "+00:00" before parsing.
string s => DateTimeOffset.Parse(NormalizeTimestampString(s), CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal),
_ => DateTimeOffset.Parse(NormalizeTimestampString(val.ToString()!), CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal)
},
"DATE" => val switch
{
DateOnly d => d,
DateTime dt => DateOnly.FromDateTime(dt),
DateTimeOffset dto => DateOnly.FromDateTime(dto.DateTime),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_date
//   BigQuery only accepts ISO 8601 date format (yyyy-MM-dd) for CAST to DATE.
string s => DateOnly.ParseExact(s.Length > 10 ? s[..10] : s, "yyyy-MM-dd", CultureInfo.InvariantCulture),
_ => DateOnly.FromDateTime(DateTime.Parse(val.ToString()!, CultureInfo.InvariantCulture))
},
"DATETIME" => val switch
{
DateTime dt => dt,
DateTimeOffset dto => dto.DateTime,
DateOnly d => d.ToDateTime(TimeOnly.MinValue),
string s => DateTime.Parse(s, CultureInfo.InvariantCulture),
_ => DateTime.Parse(val.ToString()!, CultureInfo.InvariantCulture)
},
"TIME" => val switch
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#time_literals
TimeSpan ts => ts,
string s => TimeSpan.Parse(s, CultureInfo.InvariantCulture),
_ => TimeSpan.Parse(val.ToString()!, CultureInfo.InvariantCulture)
},
"BYTES" => val switch
{
byte[] b => b,
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_bytes
//   "STRING to BYTES: the STRING is cast to BYTES using UTF-8 encoding."
string s => System.Text.Encoding.UTF8.GetBytes(s),
_ => throw new InvalidCastException("Cannot cast to BYTES")
},
_ => val // passthrough for unknown types
};
}
catch when (isSafe)
{
return null;
}
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#timestamp_literals
//   BigQuery accepts "UTC" as a timezone suffix, but .NET DateTimeOffset.Parse doesn't.
private static string NormalizeTimestampString(string s)
{
	if (s.EndsWith(" UTC", StringComparison.OrdinalIgnoreCase))
		return s[..^4] + " +00:00";
	return s;
}

private object? EvaluateCase(CaseExpr caseExpr, RowContext row)
{
if (caseExpr.Operand is not null)
{
var operand = Evaluate(caseExpr.Operand, row);
foreach (var (when, then) in caseExpr.Branches)
{
var whenVal = Evaluate(when, row);
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#case_expr
//   Simple CASE uses equality comparison; NULL = NULL is NULL (not TRUE) in SQL.
if (operand is not null && whenVal is not null && Equals(operand, whenVal))
	return Evaluate(then, row);
}
}
else
{
foreach (var (when, then) in caseExpr.Branches)
if (IsTruthy(Evaluate(when, row))) return Evaluate(then, row);
}
return caseExpr.Else is not null ? Evaluate(caseExpr.Else, row) : null;
}

private object? EvaluateScalarSubquery(ScalarSubquery sub, RowContext row)
{
var prevOuter = _outerRowContext;
_outerRowContext = row;
try
{
var result = sub.Subquery switch
{
    SelectStatement sel => ExecuteSelect(sel, _activeCteResults),
    SetOperationStatement setOp => ExecuteSetOperation(setOp),
    _ => throw new NotSupportedException($"Unsupported subquery type: {sub.Subquery.GetType().Name}")
};
if (result.Rows.Count == 0) return null;
// Re-parse the formatted value back to its typed form using the schema
// so comparisons with typed values (long, double) work correctly.
var rawVal = result.Rows[0].F?[0]?.V;
var fieldType = result.Schema?.Fields?.Count > 0 ? result.Schema.Fields[0].Type : null;
return ParseTypedValue(rawVal, fieldType);
}
finally { _outerRowContext = prevOuter; }
}

private object? EvaluateExists(ExistsExpr exists, RowContext row)
{
var prevOuter = _outerRowContext;
_outerRowContext = row;
try
{
var result = ExecuteSelect(exists.Subquery, _activeCteResults);
return result.Rows.Count > 0;
}
finally { _outerRowContext = prevOuter; }
}

private object? EvaluateArraySubquery(ArraySubquery arraySub)
{
var result = ExecuteSelect(arraySub.Subquery, _activeCteResults);
return result.Rows.Select(r => r.F?[0]?.V).Cast<object?>().ToList();
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#constructing_a_struct
private object? EvaluateStructLiteral(StructLiteralExpr structLit, RowContext row)
{
var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
for (int i = 0; i < structLit.Fields.Count; i++)
{
var (value, name) = structLit.Fields[i];
var key = name ?? $"_field_{i}";
dict[key] = Evaluate(value, row);
}
return dict;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#field_access_operator
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_subscript_operator
//   "array_expression[OFFSET(zero_based_offset)] â€” Accesses an ARRAY element by position."
//   "array_expression[ORDINAL(one_based_offset)] â€” Accesses an ARRAY element by position."
//   "array_expression[SAFE_OFFSET(zero_based_offset)] â€” Like OFFSET but returns NULL if out of range."
//   "array_expression[SAFE_ORDINAL(one_based_offset)] â€” Like ORDINAL but returns NULL if out of range."

private object? EvaluateFieldAccess(FieldAccessExpr fa, RowContext row)
{
var obj = Evaluate(fa.Object, row);
if (obj is IDictionary<string, object?> dict)
{
if (dict.TryGetValue(fa.FieldName, out var val)) return val;
foreach (var kv in dict)
if (kv.Key.Equals(fa.FieldName, StringComparison.OrdinalIgnoreCase)) return kv.Value;
}
return null;
}

private object? ResolveParameter(string name)
{
if (_parameters is null) return null;
var param = _parameters.FirstOrDefault(p => p.Name == name);
if (param is null) return null;
var pv = param.ParameterValue;
if (pv?.Value is not null)
{
var type = 
param.ParameterType?.Type?.ToUpperInvariant() ?? "STRING";
return type switch
{
"INT64" or "INTEGER" => long.Parse(pv.Value, CultureInfo.InvariantCulture),
"FLOAT64" or "FLOAT" => double.Parse(pv.Value, CultureInfo.InvariantCulture),
"BOOL" or "BOOLEAN" => bool.Parse(pv.Value),
_ => pv.Value
};
}
return null;
}

#endregion
#region Function evaluation

private object? EvaluateFunctionCall(FunctionCall fn, RowContext row)
{
var name = fn.FunctionName.ToUpperInvariant();
var args = fn.Args;

return name switch
{
// String functions
"UPPER" => Evaluate(args[0], row)?.ToString()?.ToUpperInvariant(),
"LOWER" => Evaluate(args[0], row)?.ToString()?.ToLowerInvariant(),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#length
//   "Returns the length of a STRING or BYTES value."
"LENGTH" or "CHAR_LENGTH" or "CHARACTER_LENGTH" => EvaluateLength(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#trim
//   "TRIM(value[, characters]) — Removes leading and trailing characters that match characters."
"TRIM" => args.Count >= 2
	? Evaluate(args[0], row)?.ToString()?.Trim((Evaluate(args[1], row)?.ToString() ?? "").ToCharArray())
	: Evaluate(args[0], row)?.ToString()?.Trim(),
"LTRIM" => args.Count >= 2
	? Evaluate(args[0], row)?.ToString()?.TrimStart((Evaluate(args[1], row)?.ToString() ?? "").ToCharArray())
	: Evaluate(args[0], row)?.ToString()?.TrimStart(),
"RTRIM" => args.Count >= 2
	? Evaluate(args[0], row)?.ToString()?.TrimEnd((Evaluate(args[1], row)?.ToString() ?? "").ToCharArray())
	: Evaluate(args[0], row)?.ToString()?.TrimEnd(),
"REVERSE" => Evaluate(args[0], row)?.ToString() is string s ? new string(s.Reverse().ToArray()) : null,
"REPLACE" => EvaluateReplace(args, row),
"SUBSTR" or "SUBSTRING" => EvaluateSubstr(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#concat
//   "If any input argument is NULL, CONCAT returns NULL."
"CONCAT" => EvaluateConcatString(args, row),
"STARTS_WITH" => Evaluate(args[0], row)?.ToString()?.StartsWith(Evaluate(args[1], row)?.ToString() ?? "", StringComparison.Ordinal),
"ENDS_WITH" => Evaluate(args[0], row)?.ToString()?.EndsWith(Evaluate(args[1], row)?.ToString() ?? "", StringComparison.Ordinal),
"CONTAINS_SUBSTR" => Evaluate(args[0], row)?.ToString()?.Contains(Evaluate(args[1], row)?.ToString() ?? "", StringComparison.OrdinalIgnoreCase),
"STRPOS" => EvaluateStrPos(args, row),
"INSTR" => EvaluateInstr(args, row),
"LPAD" => EvaluateLpad(args, row),
"RPAD" => EvaluateRpad(args, row),
"LEFT" => EvaluateLeftRight(args, row, true),
"RIGHT" => EvaluateLeftRight(args, row, false),
"REPEAT" => EvaluateRepeat(args, row),
"SPLIT" => EvaluateSplit(args, row),
"FORMAT" => EvaluateFormat(args, row),
"REGEXP_CONTAINS" => EvaluateRegexpContains(args, row),
"REGEXP_EXTRACT" => EvaluateRegexpExtract(args, row),
"REGEXP_REPLACE" => EvaluateRegexpReplace(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#ascii
//   "Returns 0 if the string is empty."
"ASCII" => Evaluate(args[0], row)?.ToString() is string sa ? (sa.Length > 0 ? (long)sa[0] : 0L) : null,
"CHR" => Evaluate(args[0], row) is object cv ? ((char)Convert.ToInt64(cv)).ToString() : null,
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#unicode
//   "Returns the Unicode code point for the first character in value. Returns 0 if value is empty."
"UNICODE" => EvaluateUnicode(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#byte_length
//   "Gets the number of BYTES in a STRING or BYTES value."
"BYTE_LENGTH" or "OCTET_LENGTH" => EvaluateByteLength(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#initcap
//   "Formats a STRING as proper case."
"INITCAP" => EvaluateInitcap(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#translate
//   "Within a value, replaces each source character with the corresponding target character."
"TRANSLATE" => EvaluateTranslate(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#soundex
//   "Returns a STRING that represents the Soundex code for value."
"SOUNDEX" => EvaluateSoundex(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract_all
//   "Returns an array of all substrings of value that match the regexp."
"REGEXP_EXTRACT_ALL" => EvaluateRegexpExtractAll(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#normalize
//   "Takes a string value and returns it as a normalized string."
"NORMALIZE" => EvaluateNormalize(args, row, caseFold: false),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#normalize_and_casefold
//   "Case-insensitively normalizes the characters in a STRING value."
"NORMALIZE_AND_CASEFOLD" => EvaluateNormalize(args, row, caseFold: true),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#collate
//   "Combines a STRING and a collation specification." In the emulator, collation is not enforced.
"COLLATE" => Evaluate(args[0], row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#to_code_points
//   "Converts a STRING or BYTES value into an array of INT64 code points."
"TO_CODE_POINTS" => EvaluateToCodePoints(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#code_points_to_string
//   "Converts an array of Unicode code points to a STRING."
"CODE_POINTS_TO_STRING" => EvaluateCodePointsToString(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#code_points_to_bytes
//   "Converts an array of extended ASCII code points to a BYTES value."
"CODE_POINTS_TO_BYTES" => EvaluateCodePointsToBytes(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#safe_convert_bytes_to_string
//   "Converts a BYTES value to a STRING value and replace any invalid UTF-8 characters with U+FFFD."
"SAFE_CONVERT_BYTES_TO_STRING" => EvaluateSafeConvertBytesToString(args, row),
"SAFE_DIVIDE" => EvaluateSafeDivide(args, row),
"COALESCE" => args.Select(a => Evaluate(a, row)).FirstOrDefault(v => v is not null),
"IF" => IsTruthy(Evaluate(args[0], row)) ? Evaluate(args[1], row) : Evaluate(args[2], row),
"IFNULL" => Evaluate(args[0], row) ?? Evaluate(args[1], row),
"NULLIF" => EvaluateNullIf(args, row),
"IIF" => IsTruthy(Evaluate(args[0], row)) ? Evaluate(args[1], row) : (args.Count > 2 ? Evaluate(args[2], row) : null),

// Math functions
"ABS" => EvaluateAbs(args, row),
"SIGN" => EvaluateSign(args, row),
"ROUND" => EvaluateRound(args, row),
"TRUNC" or "TRUNCATE" => EvaluateTrunc(args, row),
"CEIL" or "CEILING" => EvaluateCeil(args, row),
"FLOOR" => EvaluateFloor(args, row),
"MOD" => EvaluateMod(args, row),
"POW" or "POWER" => EvaluatePow(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
//   "All mathematical functions return NULL for NULL input parameters."
"SQRT" => EvaluateUnaryMathOrNull(args, row, Math.Sqrt),
"LOG" => EvaluateLogOrNull(args, row),
"LOG10" => EvaluateUnaryMathOrNull(args, row, Math.Log10),
"LN" => EvaluateUnaryMathOrNull(args, row, Math.Log),
"EXP" => EvaluateUnaryMathOrNull(args, row, Math.Exp),
"GREATEST" => EvaluateGreatest(args, row),
"LEAST" => EvaluateLeast(args, row),
"IEEE_DIVIDE" => EvaluateIeeeDivide(args, row),
"DIV" => EvaluateIntDiv(args, row),
"RAND" => new Random().NextDouble(),
"GENERATE_UUID" => Guid.NewGuid().ToString(),
// Trigonometric functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
"SIN" => EvaluateUnaryMathOrNull(args, row, Math.Sin),
"COS" => EvaluateUnaryMathOrNull(args, row, Math.Cos),
"TAN" => EvaluateUnaryMathOrNull(args, row, Math.Tan),
"ASIN" => EvaluateUnaryMathOrNull(args, row, Math.Asin),
"ACOS" => EvaluateUnaryMathOrNull(args, row, Math.Acos),
"ATAN" => EvaluateUnaryMathOrNull(args, row, Math.Atan),
"ATAN2" => EvaluateBinaryMathOrNull(args, row, Math.Atan2),
"SINH" => EvaluateUnaryMathOrNull(args, row, Math.Sinh),
"COSH" => EvaluateUnaryMathOrNull(args, row, Math.Cosh),
"TANH" => EvaluateUnaryMathOrNull(args, row, Math.Tanh),
"ASINH" => EvaluateUnaryMathOrNull(args, row, Math.Asinh),
"ACOSH" => EvaluateUnaryMathOrNull(args, row, Math.Acosh),
"ATANH" => EvaluateUnaryMathOrNull(args, row, Math.Atanh),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#is_inf
//   "Returns TRUE if the value is positive or negative infinity."
"IS_INF" => EvaluateIsInf(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#is_nan
//   "Returns TRUE if the value is a NaN value."
"IS_NAN" => EvaluateIsNan(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_add
"SAFE_ADD" => EvaluateSafeAdd(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_subtract
"SAFE_SUBTRACT" => EvaluateSafeSubtract(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_multiply
"SAFE_MULTIPLY" => EvaluateSafeMultiply(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#safe_negate
"SAFE_NEGATE" => EvaluateSafeNegate(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#range_bucket
//   "Scans through a sorted array and returns the 0-based position of a point's upper bound."
"RANGE_BUCKET" => EvaluateRangeBucket(args, row),

// Date/Time functions
"CURRENT_TIMESTAMP" or "NOW" => DateTimeOffset.UtcNow,
"CURRENT_DATE" => DateOnly.FromDateTime(DateTime.UtcNow),
"CURRENT_DATETIME" => DateTime.UtcNow,
"DATE" => EvaluateDateConstructor(args, row),
"DATETIME" => EvaluateDateTimeConstructor(args, row),
"TIMESTAMP" => EvaluateTimestampConstructor(args, row),
"EXTRACT" => EvaluateExtract(args, row),
"DATE_ADD" => EvaluateDateAdd(args, row),
"DATE_SUB" => EvaluateDateSub(args, row),
"DATE_DIFF" => EvaluateDateDiff(args, row),
"TIMESTAMP_ADD" => EvaluateTimestampAdd(args, row),
"TIMESTAMP_SUB" => EvaluateTimestampSub(args, row),
"TIMESTAMP_DIFF" => EvaluateTimestampDiff(args, row),
"DATE_TRUNC" => EvaluateDateTrunc(args, row),
"TIMESTAMP_TRUNC" => EvaluateTimestampTrunc(args, row),
"FORMAT_TIMESTAMP" => EvaluateFormatTimestamp(args, row),
"PARSE_TIMESTAMP" => EvaluateParseTimestamp(args, row),
"FORMAT_DATE" => EvaluateFormatDate(args, row),
"PARSE_DATE" => EvaluateParseDate(args, row),
"UNIX_SECONDS" => EvaluateUnixSeconds(args, row),
"TIMESTAMP_SECONDS" => EvaluateTimestampSeconds(args, row),
"TIMESTAMP_MILLIS" => EvaluateTimestampMillis(args, row),
"TIMESTAMP_MICROS" => EvaluateTimestampMicros(args, row),
"UNIX_MILLIS" => EvaluateUnixMillis(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#unix_micros
"UNIX_MICROS" => EvaluateUnixMicros(args, row),

// Date functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_from_unix_date
"DATE_FROM_UNIX_DATE" => EvaluateDateFromUnixDate(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#unix_date
"UNIX_DATE" => EvaluateUnixDate(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day
"LAST_DAY" => EvaluateLastDay(args, row),

// Datetime functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_add
"DATETIME_ADD" => EvaluateDatetimeAdd(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_sub
"DATETIME_SUB" => EvaluateDatetimeSub(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_diff
"DATETIME_DIFF" => EvaluateDatetimeDiff(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_trunc
"DATETIME_TRUNC" => EvaluateDatetimeTrunc(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#format_datetime
"FORMAT_DATETIME" => EvaluateFormatDatetime(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#parse_datetime
"PARSE_DATETIME" => EvaluateParseDatetime(args, row),

// Time functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#current_time
"CURRENT_TIME" => DateTime.UtcNow.TimeOfDay,
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time
"TIME" => EvaluateTimeConstructor(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_add
"TIME_ADD" => EvaluateTimeAdd(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_sub
"TIME_SUB" => EvaluateTimeSub(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_diff
"TIME_DIFF" => EvaluateTimeDiff(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_trunc
"TIME_TRUNC" => EvaluateTimeTrunc(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#format_time
"FORMAT_TIME" => EvaluateFormatTime(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#parse_time
"PARSE_TIME" => EvaluateParseTime(args, row),

// Array functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
"ARRAY_LENGTH" => EvaluateArrayLength(args, row),
"ARRAY_TO_STRING" => EvaluateArrayToString(args, row),
"GENERATE_ARRAY" => EvaluateGenerateArray(args, row),
"ARRAY_CONCAT" => EvaluateArrayConcat(args, row),
"ARRAY_REVERSE" => EvaluateArrayReverse(args, row),
"ARRAY_FIRST" => EvaluateArrayFirst(args, row),
"ARRAY_LAST" => EvaluateArrayLast(args, row),
"ARRAY_SLICE" => EvaluateArraySlice(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
"GENERATE_DATE_ARRAY" => EvaluateGenerateDateArray(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_timestamp_array
"GENERATE_TIMESTAMP_ARRAY" => EvaluateGenerateTimestampArray(args, row),
"ARRAY_INCLUDES" => EvaluateArrayIncludes(args, row),
"ARRAY_INCLUDES_ALL" => EvaluateArrayIncludesAll(args, row),
"ARRAY_INCLUDES_ANY" => EvaluateArrayIncludesAny(args, row),
"ARRAY_MAX" => EvaluateArrayMax(args, row),
"ARRAY_MIN" => EvaluateArrayMin(args, row),
"ARRAY_SUM" => EvaluateArraySum(args, row),
"ARRAY_AVG" => EvaluateArrayAvg(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_is_distinct
"ARRAY_IS_DISTINCT" => EvaluateArrayIsDistinct(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_filter
"ARRAY_FILTER" => EvaluateArrayFilter(fn.Args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_transform
"ARRAY_TRANSFORM" => EvaluateArrayTransform(fn.Args, row),

// Conversion functions
"CAST" => args.Count >= 2 ? CastValue(Evaluate(args[0], row), Evaluate(args[1], row)?.ToString() ?? "STRING", false) : null,
"SAFE_CAST" => args.Count >= 2 ? CastValue(Evaluate(args[0], row), Evaluate(args[1], row)?.ToString() ?? "STRING", true) : null,
"TO_JSON_STRING" => EvaluateToJsonString(args, row),

// Type functions
"STRUCT" => args.Select(a => Evaluate(a, row)).ToList(),
"ARRAY" => EvaluateArrayLiteral(args, row),

// Hash/Encoding
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions
"MD5" => EvaluateMd5(args, row),
"SHA1" => EvaluateSha1(args, row),
"SHA256" => EvaluateSha256(args, row),
"SHA512" => EvaluateSha512(args, row),
"FARM_FINGERPRINT" => EvaluateFarmFingerprint(args, row),
"TO_BASE64" => EvaluateToBase64(args, row),
"FROM_BASE64" => EvaluateFromBase64(args, row),
"TO_HEX" => EvaluateToHex(args, row),
"FROM_HEX" => EvaluateFromHex(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#to_base32
//   "Converts a BYTES value to a base32-encoded STRING value."
"TO_BASE32" => EvaluateToBase32(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#from_base32
//   "Converts a base32-encoded STRING into BYTES format."
"FROM_BASE32" => EvaluateFromBase32(args, row),

// JSON functions
"JSON_EXTRACT" or "JSON_QUERY" => EvaluateJsonExtract(args, row),
"JSON_EXTRACT_SCALAR" or "JSON_VALUE" => EvaluateJsonExtractScalar(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
"JSON_EXTRACT_ARRAY" or "JSON_QUERY_ARRAY" => EvaluateJsonExtractArray(args, row),
"JSON_EXTRACT_STRING_ARRAY" or "JSON_VALUE_ARRAY" => EvaluateJsonValueArray(args, row),
"JSON_KEYS" => EvaluateJsonKeys(args, row),
"JSON_SET" => EvaluateJsonSet(args, row),
"JSON_STRIP_NULLS" => EvaluateJsonStripNulls(args, row),
"JSON_TYPE" => EvaluateJsonType(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#parse_json
//   "Takes a JSON-formatted string and returns a JSON value."
"PARSE_JSON" => EvaluateParseJson(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#to_json
//   "Takes a SQL value and returns a JSON value."
"TO_JSON" => EvaluateToJson(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array
//   "Creates a JSON array."
"JSON_ARRAY" => EvaluateJsonArray(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_object
//   "Creates a JSON object."
"JSON_OBJECT" => EvaluateJsonObject(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_remove
//   "Removes a JSON element at a path."
"JSON_REMOVE" => EvaluateJsonRemove(args, row),

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_bool
"LAX_BOOL" => EvaluateLaxBool(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_int64
"LAX_INT64" => EvaluateLaxInt64(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_float64
"LAX_FLOAT64" => EvaluateLaxFloat64(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_string
"LAX_STRING" => EvaluateLaxString(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array_append
"JSON_ARRAY_APPEND" => EvaluateJsonArrayAppend(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array_insert
"JSON_ARRAY_INSERT" => EvaluateJsonArrayInsert(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_contains
"JSON_CONTAINS" => EvaluateJsonContains(args, row),

// Regex additional functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
"REGEXP_INSTR" => EvaluateRegexpInstr(args, row),
"REGEXP_SUBSTR" => EvaluateRegexpSubstr(args, row),

// Bit functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/bit_functions
"BIT_COUNT" => EvaluateBitCount(args, row),

// Interval functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/interval_functions
"MAKE_INTERVAL" => EvaluateMakeInterval(args, row),
"JUSTIFY_HOURS" => EvaluateJustifyHours(args, row),
"JUSTIFY_DAYS" => EvaluateJustifyDays(args, row),
"JUSTIFY_INTERVAL" => EvaluateJustifyInterval(args, row),

// Range functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/range-functions
"RANGE" => EvaluateRange(args, row),
"RANGE_START" => EvaluateRangeStart(args, row),
"RANGE_END" => EvaluateRangeEnd(args, row),
"RANGE_CONTAINS" => EvaluateRangeContains(args, row),
"RANGE_OVERLAPS" => EvaluateRangeOverlaps(args, row),
"GENERATE_RANGE_ARRAY" => EvaluateGenerateRangeArray(args, row),

// Net functions (normalized from NET.HOST â†’ NET_HOST etc.)
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions
"NET_HOST" => EvaluateNetHost(args, row),
"NET_PUBLIC_SUFFIX" => EvaluateNetPublicSuffix(args, row),
"NET_REG_DOMAIN" => EvaluateNetRegDomain(args, row),
"NET_IP_FROM_STRING" => EvaluateNetIpFromString(args, row),
"NET_IP_TO_STRING" => EvaluateNetIpToString(args, row),
"NET_IP_NET_MASK" => EvaluateNetIpNetMask(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netip_trunc
"NET_IP_TRUNC" => EvaluateNetIpTrunc(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netipv4_from_int64
"NET_IPV4_FROM_INT64" => EvaluateNetIpv4FromInt64(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netipv4_to_int64
"NET_IPV4_TO_INT64" => EvaluateNetIpv4ToInt64(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netsafe_ip_from_string
"NET_SAFE_IP_FROM_STRING" => EvaluateNetSafeIpFromString(args, row),

// HLL++ approximate counting (exact in-memory implementation, normalized from HLL_COUNT.INIT â†’ HLL_COUNT_INIT)
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_count_functions
"HLL_COUNT_INIT" or "HLL_COUNT_EXTRACT" => EvaluateHllCount(name, args, row),

// Vector / distance functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/search_functions#vector_search
//   APPROX_* variants use approximate algorithms in real BigQuery; in the emulator they are exact.
"COSINE_DISTANCE" or "EUCLIDEAN_DISTANCE" or "DOT_PRODUCT"
or "APPROX_COSINE_DISTANCE" or "APPROX_EUCLIDEAN_DISTANCE" or "APPROX_DOT_PRODUCT"
    => EvaluateVectorDistanceFunction(name, args, row),

// Geography functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions
"ST_GEOGPOINT" or "ST_GEOGFROMTEXT" or "ST_GEOGFROMWKT" or "ST_GEOGFROMGEOJSON"
or "ST_ASTEXT" or "ST_ASGEOJSON" or "ST_X" or "ST_Y"
or "ST_DISTANCE" or "ST_DWITHIN" or "ST_CONTAINS" or "ST_WITHIN"
or "ST_INTERSECTS" or "ST_DISJOINT" or "ST_EQUALS"
or "ST_AREA" or "ST_LENGTH" or "ST_PERIMETER"
or "ST_NUMPOINTS" or "ST_NPOINTS" or "ST_DIMENSION" or "ST_ISEMPTY" or "ST_GEOMETRYTYPE"
or "ST_MAKELINE" or "ST_CENTROID"
or "ST_ASBINARY" or "ST_GEOGFROMWKB"
or "ST_ISCOLLECTION" or "ST_BOUNDARY"
or "ST_COVEREDBY" or "ST_COVERS" or "ST_TOUCHES"
or "ST_CLOSESTPOINT" or "ST_CONVEXHULL"
or "ST_DIFFERENCE" or "ST_INTERSECTION" or "ST_UNION"
or "ST_BUFFER" or "ST_SIMPLIFY" or "ST_DUMP"
    => EvaluateGeographyFunction(name, args, row),

// Conversion functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#parse_numeric
"PARSE_NUMERIC" or "PARSE_BIGNUMERIC" => EvaluateParseNumeric(args, row),


// JSON type conversion functions (extract typed value from JSON)
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
"BOOL" or "JSON_BOOL" => EvaluateJsonBool(args, row),
"INT64" or "JSON_INT64" => EvaluateJsonInt64(args, row),
"FLOAT64" or "JSON_FLOAT64" => EvaluateJsonFloat64(args, row),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#string_for_json
"STRING" or "JSON_STRING" => EvaluateJsonString(args, row),

// AEAD encryption functions (normalized from KEYS.* â†’ KEYS_*, AEAD.* â†’ AEAD_*)
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aead_encryption_functions
"KEYS_NEW_KEYSET" => EvaluateKeysNewKeyset(args, row),
"KEYS_ROTATE_KEYSET" => EvaluateKeysRotateKeyset(args, row),
"KEYS_ADD_KEY_FROM_RAW_BYTES" => EvaluateKeysAddKeyFromRawBytes(args, row),
"KEYS_KEYSET_FROM_JSON" => EvaluateKeysKeysetFromJson(args, row),
"KEYS_KEYSET_TO_JSON" => EvaluateKeysKeysetToJson(args, row),
"KEYS_KEYSET_LENGTH" => EvaluateKeysKeysetLength(args, row),
"KEYS_KEYSET_CHAIN" => EvaluateKeysKeysetChain(args, row),
"AEAD_ENCRYPT" => EvaluateAeadEncrypt(args, row),
"AEAD_DECRYPT_BYTES" => EvaluateAeadDecryptBytes(args, row),
"AEAD_DECRYPT_STRING" => EvaluateAeadDecryptString(args, row),
"DETERMINISTIC_ENCRYPT" => EvaluateDeterministicEncrypt(args, row),
"DETERMINISTIC_DECRYPT_BYTES" => EvaluateDeterministicDecryptBytes(args, row),
"DETERMINISTIC_DECRYPT_STRING" => EvaluateDeterministicDecryptString(args, row),

// Debugging / utility functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/debugging_functions#error
"ERROR" => throw new InvalidOperationException(Evaluate(args[0], row)?.ToString() ?? "ERROR"),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/security_functions#session_user
"SESSION_USER" => "emulator@bigquery.local",

_ => EvaluateUdf(name, args, row)
};
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#concat
//   "If any input argument is NULL, CONCAT returns NULL."
private object? EvaluateConcatString(IReadOnlyList<SqlExpression> args, RowContext row)
{
var parts = args.Select(a => Evaluate(a, row)).ToList();
if (parts.Any(p => p is null)) return null;
return string.Concat(parts.Select(p => p!.ToString()));
}

private object? EvaluateReplace(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
var from = Evaluate(args[1], row)?.ToString();
var to = Evaluate(args[2], row)?.ToString();
if (str is null || from is null || to is null) return null;
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#replace
//   "If original_value is empty, the original value is returned."
if (from.Length == 0) return str;
return str.Replace(from, to);
}

private object? EvaluateSubstr(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
if (str is null) return null;
var pos = (int)ToLong(Evaluate(args[1], row));
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#substr
//   "If position is 0, it is treated as 1."
//   "If position is negative, the function counts from the end of value, where -1 indicates the last character."
if (pos == 0) pos = 1;
var startIdx = pos > 0 ? pos - 1 : Math.Max(0, str.Length + pos);
if (startIdx >= str.Length) return "";
if (args.Count > 2)
{
var len = (int)ToLong(Evaluate(args[2], row));
return str.Substring(startIdx, Math.Min(len, str.Length - startIdx));
}
return str[startIdx..];
}

private object? EvaluateStrPos(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
var sub = Evaluate(args[1], row)?.ToString();
if (str is null || sub is null) return null;
return (long)(str.IndexOf(sub, StringComparison.Ordinal) + 1);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#instr
//   "INSTR(source_value, search_value[, position[, occurrence]])"
//   Returns the 1-based position of the specified occurrence starting from position.
private object? EvaluateInstr(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var str = Evaluate(args[0], row)?.ToString();
    var sub = Evaluate(args[1], row)?.ToString();
    if (str is null || sub is null) return null;
    int position = args.Count > 2 ? (int)ToLong(Evaluate(args[2], row)) : 1;
    int occurrence = args.Count > 3 ? (int)ToLong(Evaluate(args[3], row)) : 1;
    if (position <= 0) position = 1;
    int startIdx = position - 1;
    for (int i = 0; i < occurrence; i++)
    {
        if (startIdx > str.Length) return 0L;
        int found = str.IndexOf(sub, startIdx, StringComparison.Ordinal);
        if (found < 0) return 0L;
        if (i == occurrence - 1) return (long)(found + 1);
        startIdx = found + 1;
    }
    return 0L;
}

private object? EvaluateLpad(IReadOnlyList<SqlExpression> args, RowContext row)
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#lpad
//   "Returns NULL if any input is NULL."
var rawStr = Evaluate(args[0], row);
if (rawStr is null) return null;
var str = rawStr.ToString() ?? "";
var rawLen = Evaluate(args[1], row);
if (rawLen is null) return null;
var len = (int)ToLong(rawLen);
var pad = args.Count > 2 ? Evaluate(args[2], row)?.ToString() : " ";
if (pad is null) return null;
if (str.Length >= len) return str[..len];
while (str.Length < len) str = pad + str;
return str[..len];
}

private object? EvaluateRpad(IReadOnlyList<SqlExpression> args, RowContext row)
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#rpad
//   "Returns NULL if any input is NULL."
var rawStr = Evaluate(args[0], row);
if (rawStr is null) return null;
var str = rawStr.ToString() ?? "";
var rawLen = Evaluate(args[1], row);
if (rawLen is null) return null;
var len = (int)ToLong(rawLen);
var pad = args.Count > 2 ? Evaluate(args[2], row)?.ToString() : " ";
if (pad is null) return null;
if (str.Length >= len) return str[..len];
while (str.Length < len) str = str + pad;
return str[..len];
}

private object? EvaluateLeftRight(IReadOnlyList<SqlExpression> args, RowContext row, bool isLeft)
{
var str = Evaluate(args[0], row)?.ToString();
if (str is null) return null;
var len = (int)ToLong(Evaluate(args[1], row));
if (len >= str.Length) return str;
return isLeft ? str[..len] : str[^len..];
}

private object? EvaluateRepeat(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
if (str is null) return null;
var count = (int)ToLong(Evaluate(args[1], row));
return string.Concat(Enumerable.Repeat(str, count));
}

private object? EvaluateSplit(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
if (str is null) return null;
var delimiter = args.Count > 1 ? Evaluate(args[1], row)?.ToString() ?? "," : ",";
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split
//   "If delimiter is an empty string, each character in value becomes a separate element."
if (delimiter.Length == 0)
return str.Select(c => (object?)c.ToString()).ToList();
return str.Split(delimiter).Cast<object?>().ToList();
}

private object? EvaluateFormat(IReadOnlyList<SqlExpression> args, RowContext row)
{
var fmt = Evaluate(args[0], row)?.ToString();
if (fmt is null) return null;
var fmtArgs = args.Skip(1).Select(a => Evaluate(a, row)).ToArray();
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#format_string
//   "FORMAT(format_string, ...) — Formats arguments using printf-style format specifiers."
var result = new System.Text.StringBuilder();
int argIdx = 0;
int i = 0;
while (i < fmt.Length)
{
if (fmt[i] == '%' && i + 1 < fmt.Length)
{
if (fmt[i + 1] == '%') { result.Append('%'); i += 2; continue; }
// Parse flags, width, precision, and type
int start = i;
i++; // skip %
// Flags: 0, -, +, space
while (i < fmt.Length && "0-+ ".Contains(fmt[i])) i++;
// Width
while (i < fmt.Length && char.IsDigit(fmt[i])) i++;
// Precision
int precision = -1;
if (i < fmt.Length && fmt[i] == '.')
{
i++;
int precStart = i;
while (i < fmt.Length && char.IsDigit(fmt[i])) i++;
if (i > precStart) precision = int.Parse(fmt[precStart..i], CultureInfo.InvariantCulture);
}
// Type character
if (i < fmt.Length && argIdx < fmtArgs.Length)
{
char type = fmt[i]; i++;
string spec = fmt[start..i]; // e.g., "%.2f", "%03d", "%4d"
var arg = fmtArgs[argIdx++];
result.Append(FormatOneArg(spec, type, precision, arg));
}
}
else
{
result.Append(fmt[i]); i++;
}
}
return result.ToString();
}

private static string FormatOneArg(string spec, char type, int precision, object? arg)
{
if (arg is null) return "NULL";
switch (type)
{
case 'd':
{
long val = arg is long l ? l : Convert.ToInt64(arg, CultureInfo.InvariantCulture);
string csFmt = spec.Replace("%", "").TrimEnd('d');
if (string.IsNullOrEmpty(csFmt)) return val.ToString(CultureInfo.InvariantCulture);
bool zeroPad = csFmt.StartsWith('0');
int width = int.TryParse(csFmt.TrimStart('0', '-'), out var w) ? w : 0;
string s = val.ToString(CultureInfo.InvariantCulture);
if (zeroPad && width > 0) return s.PadLeft(width, '0');
if (width > 0) return s.PadLeft(width);
return s;
}
case 'x':
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements
//   "%x" formats as lowercase hexadecimal
long val = arg is long l ? l : Convert.ToInt64(arg, CultureInfo.InvariantCulture);
return val.ToString("x", CultureInfo.InvariantCulture);
}
case 'X':
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements
//   "%X" formats as uppercase hexadecimal
long val = arg is long l ? l : Convert.ToInt64(arg, CultureInfo.InvariantCulture);
return val.ToString("X", CultureInfo.InvariantCulture);
}
case 'o':
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements
//   "%o" formats as octal
long val = arg is long l ? l : Convert.ToInt64(arg, CultureInfo.InvariantCulture);
return Convert.ToString(val, 8);
}
case 'f':
{
double val = arg is double d ? d : Convert.ToDouble(arg, CultureInfo.InvariantCulture);
if (precision >= 0) return val.ToString("F" + precision, CultureInfo.InvariantCulture);
return val.ToString("F6", CultureInfo.InvariantCulture);
}
case 'e':
{
double val = arg is double d ? d : Convert.ToDouble(arg, CultureInfo.InvariantCulture);
if (precision >= 0) return val.ToString("e" + precision, CultureInfo.InvariantCulture);
return val.ToString("e", CultureInfo.InvariantCulture);
}
case 'E':
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements
//   "%E" formats as scientific notation with uppercase E
double val = arg is double d ? d : Convert.ToDouble(arg, CultureInfo.InvariantCulture);
if (precision >= 0) return val.ToString("E" + precision, CultureInfo.InvariantCulture);
return val.ToString("E", CultureInfo.InvariantCulture);
}
case 'g':
{
double val = arg is double d ? d : Convert.ToDouble(arg, CultureInfo.InvariantCulture);
if (precision >= 0) return val.ToString("G" + precision, CultureInfo.InvariantCulture);
return val.ToString("G", CultureInfo.InvariantCulture);
}
case 's':
{
string sval = ConvertToString(arg) ?? "NULL";
string csFmt = spec.Replace("%", "").TrimEnd('s');
if (string.IsNullOrEmpty(csFmt)) return sval;
bool leftAlign = csFmt.StartsWith('-');
int width = int.TryParse(csFmt.TrimStart('-', '0'), out var w) ? w : 0;
if (width > 0 && leftAlign) return sval.PadRight(width);
if (width > 0) return sval.PadLeft(width);
return sval;
}
case 't': case 'T':
return ConvertToString(arg) ?? "NULL";
default:
return ConvertToString(arg) ?? "NULL";
}
}
private object? EvaluateRegexpContains(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
var pattern = Evaluate(args[1], row)?.ToString();
if (str is null || pattern is null) return null;
return System.Text.RegularExpressions.Regex.IsMatch(str, pattern);
}

private object? EvaluateRegexpExtract(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
var pattern = Evaluate(args[1], row)?.ToString();
if (str is null || pattern is null) return null;
var match = System.Text.RegularExpressions.Regex.Match(str, pattern);
if (!match.Success) return null;
return match.Groups.Count > 1 ? match.Groups[1].Value : match.Value;
}

private object? EvaluateRegexpReplace(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
var pattern = Evaluate(args[1], row)?.ToString();
var replacement = Evaluate(args[2], row)?.ToString() ?? "";
if (str is null || pattern is null) return null;
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_replace
//   BigQuery uses \1, \2 for backreferences; .NET uses $1, $2.
replacement = System.Text.RegularExpressions.Regex.Replace(replacement, @"\\(\d)", "$$$1");
return System.Text.RegularExpressions.Regex.Replace(str, pattern, replacement);
}

private object? EvaluateSafeDivide(IReadOnlyList<SqlExpression> args, RowContext row)
{
var left = Evaluate(args[0], row);
var right = Evaluate(args[1], row);
if (left is null || right is null) return null;
var d = ToDouble(right);
if (d == 0.0) return null;
return ToDouble(left) / d;
}

private object? EvaluateNullIf(IReadOnlyList<SqlExpression> args, RowContext row)
{
var a = Evaluate(args[0], row);
var b = Evaluate(args[1], row);
if (a is null && b is null) return null;
if (a is not null && b is not null && CompareRaw(a, b) == 0) return null;
return a;
}

private object? EvaluateAbs(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
return val switch { long l => Math.Abs(l), double d => Math.Abs(d), _ => null };
}

private object? EvaluateSign(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
return val switch { long l => (long)Math.Sign(l), double d => (long)Math.Sign(d), _ => null };
}

private object? EvaluateRound(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
var d = ToDouble(val);
var digits = args.Count > 1 ? (int)ToLong(Evaluate(args[1], row)) : 0;
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#round
//   Supports negative digits to round to powers of 10.
if (digits < 0)
{
	var factor = Math.Pow(10, -digits);
	return Math.Round(d / factor, MidpointRounding.AwayFromZero) * factor;
}
return Math.Round(d, digits, MidpointRounding.AwayFromZero);
}

private object? EvaluateTrunc(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
var d = ToDouble(val);
var digits = args.Count > 1 ? (int)ToLong(Evaluate(args[1], row)) : 0;
var factor = Math.Pow(10, digits);
return Math.Truncate(d * factor) / factor;
}

private object? EvaluateCeil(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
return Math.Ceiling(ToDouble(val));
}

private object? EvaluateFloor(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
return Math.Floor(ToDouble(val));
}

private object? EvaluateMod(IReadOnlyList<SqlExpression> args, RowContext row)
{
var a = Evaluate(args[0], row);
var b = Evaluate(args[1], row);
if (a is null || b is null) return null;
if (a is long la && b is long lb) return lb == 0 ? throw new DivideByZeroException() : la % lb;
return ToDouble(a) % ToDouble(b);
}

private object? EvaluatePow(IReadOnlyList<SqlExpression> args, RowContext row)
{
var a = Evaluate(args[0], row);
var b = Evaluate(args[1], row);
if (a is null || b is null) return null;
return Math.Pow(ToDouble(a), ToDouble(b));
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#greatest
//   "Returns NULL if any of the inputs is NULL."
private object? EvaluateGreatest(IReadOnlyList<SqlExpression> args, RowContext row)
{
object? best = null;
bool first = true;
foreach (var arg in args)
{
var val = Evaluate(arg, row);
if (val is null) return null;
if (first || CompareRaw(val, best!) > 0) best = val;
first = false;
}
return best;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#least
//   "Returns NULL if any of the inputs is NULL."
private object? EvaluateLeast(IReadOnlyList<SqlExpression> args, RowContext row)
{
object? best = null;
bool first = true;
foreach (var arg in args)
{
var val = Evaluate(arg, row);
if (val is null) return null;
if (first || CompareRaw(val, best!) < 0) best = val;
first = false;
}
return best;
}

private object? EvaluateIeeeDivide(IReadOnlyList<SqlExpression> args, RowContext row)
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#ieee_divide
//   "If one of the input values is NULL, the result is NULL."
var rawA = Evaluate(args[0], row);
var rawB = Evaluate(args[1], row);
if (rawA is null || rawB is null) return null;
var a = ToDouble(rawA);
var b = ToDouble(rawB);
return a / b; // IEEE 754: handles div by zero as Infinity/NaN
}

private object? EvaluateIntDiv(IReadOnlyList<SqlExpression> args, RowContext row)
{
var a = Evaluate(args[0], row);
var b = Evaluate(args[1], row);
if (a is null || b is null) return null;
var la = ToLong(a);
var lb = ToLong(b);
if (lb == 0) throw new DivideByZeroException();
return la / lb;
}

#endregion
#region Date/Time function helpers

private object? EvaluateDateConstructor(IReadOnlyList<SqlExpression> args, RowContext row)
{
if (args.Count == 1)
{
var val = Evaluate(args[0], row);
return val switch
{
DateOnly d => d,
DateTime dt => DateOnly.FromDateTime(dt),
DateTimeOffset dto => DateOnly.FromDateTime(dto.Date),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_date
//   BigQuery only accepts ISO 8601 date format (yyyy-MM-dd) for CAST to DATE.
string s => DateOnly.ParseExact(s.Length > 10 ? s[..10] : s, "yyyy-MM-dd", CultureInfo.InvariantCulture),
_ => DateOnly.FromDateTime(DateTime.Parse(val?.ToString() ?? "", CultureInfo.InvariantCulture))
};
}
return new DateOnly((int)ToLong(Evaluate(args[0], row)),
(int)ToLong(Evaluate(args[1], row)),
(int)ToLong(Evaluate(args[2], row)));
}

private object? EvaluateDateTimeConstructor(IReadOnlyList<SqlExpression> args, RowContext row)
{
if (args.Count == 1)
{
var val = Evaluate(args[0], row);
return val switch
{
DateTime dt => dt,
DateTimeOffset dto => dto.DateTime,
string s => DateTime.Parse(s, CultureInfo.InvariantCulture),
_ => DateTime.Parse(val?.ToString() ?? "", CultureInfo.InvariantCulture)
};
}
if (args.Count == 2)
{
var date = ToDateTime(Evaluate(args[0], row));
var time = Evaluate(args[1], row)?.ToString() ?? "00:00:00";
var ts = TimeSpan.Parse(time, CultureInfo.InvariantCulture);
return date.Date.Add(ts);
}
return new DateTime((int)ToLong(Evaluate(args[0], row)),
(int)ToLong(Evaluate(args[1], row)),
(int)ToLong(Evaluate(args[2], row)),
args.Count > 3 ? (int)ToLong(Evaluate(args[3], row)) : 0,
args.Count > 4 ? (int)ToLong(Evaluate(args[4], row)) : 0,
args.Count > 5 ? (int)ToLong(Evaluate(args[5], row)) : 0);
}

private object? EvaluateTimestampConstructor(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
return val switch
{
DateTimeOffset dto => dto,
DateTime dt => new DateTimeOffset(dt, TimeSpan.Zero),
DateOnly d => new DateTimeOffset(d.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero),
string s => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture),
_ => DateTimeOffset.Parse(val?.ToString() ?? "", CultureInfo.InvariantCulture)
};
}

private object? EvaluateExtract(IReadOnlyList<SqlExpression> args, RowContext row)
{
// EXTRACT is parsed as FunctionCall with args[0] = part name literal, args[1] = date/time expr
var partName = Evaluate(args[0], row)?.ToString()?.ToUpperInvariant();
var val = Evaluate(args[1], row);
if (val is null || partName is null) return null;
var dto = ToDateTimeOffset(val);
return partName switch
{
"YEAR" => (long)dto.Year,
"MONTH" => (long)dto.Month,
"DAY" => (long)dto.Day,
"HOUR" => (long)dto.Hour,
"MINUTE" => (long)dto.Minute,
"SECOND" => (long)dto.Second,
"MILLISECOND" => (long)dto.Millisecond,
"MICROSECOND" => (long)(dto.Millisecond * 1000),
"DAYOFWEEK" => (long)(((int)dto.DayOfWeek) + 1),
"DAYOFYEAR" => (long)dto.DayOfYear,
"WEEK" => (long)CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dto.DateTime, CalendarWeekRule.FirstDay, DayOfWeek.Sunday),
"QUARTER" => (long)((dto.Month - 1) / 3 + 1),
"DATE" => (object)DateOnly.FromDateTime(dto.Date),
"TIME" => dto.TimeOfDay.ToString(),
"ISOWEEK" => (long)CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dto.DateTime, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
//   "ISOYEAR: Returns the ISO 8601 week-numbering year."
"ISOYEAR" => (long)System.Globalization.ISOWeek.GetYear(dto.DateTime),
_ => throw new NotSupportedException("Unsupported EXTRACT part: " + partName)
};
}

private object? EvaluateDateAdd(IReadOnlyList<SqlExpression> args, RowContext row)
{
var raw = Evaluate(args[0], row);
if (raw is null) return null;
var date = ToDateTime(raw);
var interval = ToLong(Evaluate(args[1], row));
var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
var result = AddToPart(date, interval, part);
return raw is DateOnly ? DateOnly.FromDateTime(result) : result;
}

private object? EvaluateDateSub(IReadOnlyList<SqlExpression> args, RowContext row)
{
var raw = Evaluate(args[0], row);
if (raw is null) return null;
var date = ToDateTime(raw);
var interval = ToLong(Evaluate(args[1], row));
var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
var result = AddToPart(date, -interval, part);
return raw is DateOnly ? DateOnly.FromDateTime(result) : result;
}

private object? EvaluateDateDiff(IReadOnlyList<SqlExpression> args, RowContext row)
{
var raw1 = Evaluate(args[0], row);
var raw2 = Evaluate(args[1], row);
if (raw1 is null || raw2 is null) return null;
var date1 = ToDateTime(raw1);
var date2 = ToDateTime(raw2);
var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
return part switch
{
"DAY" => (long)(date1 - date2).TotalDays,
"MONTH" => (long)((date1.Year - date2.Year) * 12 + date1.Month - date2.Month),
"YEAR" => (long)(date1.Year - date2.Year),
"WEEK" => (long)((date1 - date2).TotalDays / 7),
"QUARTER" => (long)(((date1.Year - date2.Year) * 12 + date1.Month - date2.Month) / 3),
_ => (long)(date1 - date2).TotalDays
};
}

private object? EvaluateTimestampAdd(IReadOnlyList<SqlExpression> args, RowContext row)
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_add
//   "Returns NULL if any argument is NULL."
var rawTs = Evaluate(args[0], row);
if (rawTs is null) return null;
var ts = ToDateTimeOffset(rawTs);
var rawInterval = Evaluate(args[1], row);
if (rawInterval is null) return null;
var interval = ToLong(rawInterval);
var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "SECOND";
return part switch
{
"MICROSECOND" => ts.AddTicks(interval * 10),
"MILLISECOND" => ts.AddMilliseconds(interval),
"SECOND" => ts.AddSeconds(interval),
"MINUTE" => ts.AddMinutes(interval),
"HOUR" => ts.AddHours(interval),
"DAY" => ts.AddDays(interval),
_ => ts.AddSeconds(interval)
};
}

private object? EvaluateTimestampSub(IReadOnlyList<SqlExpression> args, RowContext row)
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_sub
//   "Returns NULL if any argument is NULL."
var rawTs = Evaluate(args[0], row);
if (rawTs is null) return null;
var ts = ToDateTimeOffset(rawTs);
var rawInterval = Evaluate(args[1], row);
if (rawInterval is null) return null;
var interval = ToLong(rawInterval);
var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "SECOND";
return part switch
{
"MICROSECOND" => ts.AddTicks(-interval * 10),
"MILLISECOND" => ts.AddMilliseconds(-interval),
"SECOND" => ts.AddSeconds(-interval),
"MINUTE" => ts.AddMinutes(-interval),
"HOUR" => ts.AddHours(-interval),
"DAY" => ts.AddDays(-interval),
_ => ts.AddSeconds(-interval)
};
}

private object? EvaluateTimestampDiff(IReadOnlyList<SqlExpression> args, RowContext row)
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff
var raw1 = Evaluate(args[0], row);
var raw2 = Evaluate(args[1], row);
if (raw1 is null || raw2 is null) return null;
var ts1 = ToDateTimeOffset(raw1);
var ts2 = ToDateTimeOffset(raw2);
var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "SECOND";
var diff = ts1 - ts2;
return part switch
{
"MICROSECOND" => diff.Ticks / 10,
"MILLISECOND" => (long)diff.TotalMilliseconds,
"SECOND" => (long)diff.TotalSeconds,
"MINUTE" => (long)diff.TotalMinutes,
"HOUR" => (long)diff.TotalHours,
"DAY" => (long)diff.TotalDays,
_ => (long)diff.TotalSeconds
};
}

private object? EvaluateDateTrunc(IReadOnlyList<SqlExpression> args, RowContext row)
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_trunc
//   "Returns NULL if date_expression is NULL."
var rawDate = Evaluate(args[0], row);
if (rawDate is null) return null;
var date = ToDateTime(rawDate);
var part = Evaluate(args[1], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
var result = part switch
{
"YEAR" => new DateTime(date.Year, 1, 1),
"MONTH" => new DateTime(date.Year, date.Month, 1),
"QUARTER" => new DateTime(date.Year, ((date.Month - 1) / 3) * 3 + 1, 1),
"WEEK" => date.AddDays(-(int)date.DayOfWeek),
"DAY" => date.Date,
_ => date.Date
};
return DateOnly.FromDateTime(result);
}

private object? EvaluateTimestampTrunc(IReadOnlyList<SqlExpression> args, RowContext row)
{
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_trunc
//   "Returns NULL if timestamp_expression is NULL."
var rawTs = Evaluate(args[0], row);
if (rawTs is null) return null;
var ts = ToDateTimeOffset(rawTs);
var part = Evaluate(args[1], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
return part switch
{
"YEAR" => new DateTimeOffset(ts.Year, 1, 1, 0, 0, 0, TimeSpan.Zero),
"MONTH" => new DateTimeOffset(ts.Year, ts.Month, 1, 0, 0, 0, TimeSpan.Zero),
"DAY" => new DateTimeOffset(ts.Year, ts.Month, ts.Day, 0, 0, 0, TimeSpan.Zero),
"HOUR" => new DateTimeOffset(ts.Year, ts.Month, ts.Day, ts.Hour, 0, 0, TimeSpan.Zero),
"MINUTE" => new DateTimeOffset(ts.Year, ts.Month, ts.Day, ts.Hour, ts.Minute, 0, TimeSpan.Zero),
"SECOND" => new DateTimeOffset(ts.Year, ts.Month, ts.Day, ts.Hour, ts.Minute, ts.Second, TimeSpan.Zero),
_ => new DateTimeOffset(ts.Year, ts.Month, ts.Day, 0, 0, 0, TimeSpan.Zero)
};
}

private object? EvaluateFormatTimestamp(IReadOnlyList<SqlExpression> args, RowContext row)
{
var format = Evaluate(args[0], row)?.ToString();
if (format is null) return null;
var tsVal = Evaluate(args[1], row);
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#format_timestamp
//   "Returns NULL if any argument is NULL."
if (tsVal is null) return null;
var ts = ToDateTimeOffset(tsVal);
return FormatTimestamp(ts, format);
}

private object? EvaluateParseTimestamp(IReadOnlyList<SqlExpression> args, RowContext row)
{
var formatVal = Evaluate(args[0], row);
var strVal = Evaluate(args[1], row);
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#parse_timestamp
//   "Returns NULL if any argument is NULL."
if (formatVal is null || strVal is null) return null;
var format = formatVal.ToString() ?? "";
var str = strVal.ToString() ?? "";
return ParseTimestamp(str, format);}

private object? EvaluateFormatDate(IReadOnlyList<SqlExpression> args, RowContext row)
{
var format = Evaluate(args[0], row)?.ToString() ?? "";
var dateVal = Evaluate(args[1], row);
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#format_date
//   Returns NULL if any argument is NULL.
if (dateVal is null) return null;
var date = ToDateTime(dateVal);
return FormatTimestamp(new DateTimeOffset(date, TimeSpan.Zero), format);
}

private object? EvaluateParseDate(IReadOnlyList<SqlExpression> args, RowContext row)
{
var formatVal = Evaluate(args[0], row);
var strVal = Evaluate(args[1], row);
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#parse_date
//   "Returns NULL if any argument is NULL."
if (formatVal is null || strVal is null) return null;
var format = formatVal.ToString() ?? "";
var str = strVal.ToString() ?? "";
return DateOnly.FromDateTime(ParseTimestamp(str, format).DateTime);}

private object? EvaluateUnixSeconds(IReadOnlyList<SqlExpression> args, RowContext row)
{
var ts = ToDateTimeOffset(Evaluate(args[0], row));
return ts.ToUnixTimeSeconds();
}

private object? EvaluateTimestampSeconds(IReadOnlyList<SqlExpression> args, RowContext row)
{
var secs = ToLong(Evaluate(args[0], row));
return DateTimeOffset.FromUnixTimeSeconds(secs);
}

private object? EvaluateTimestampMillis(IReadOnlyList<SqlExpression> args, RowContext row)
{
var millis = ToLong(Evaluate(args[0], row));
return DateTimeOffset.FromUnixTimeMilliseconds(millis);
}

private object? EvaluateTimestampMicros(IReadOnlyList<SqlExpression> args, RowContext row)
{
var micros = ToLong(Evaluate(args[0], row));
return DateTimeOffset.FromUnixTimeMilliseconds(micros / 1000);
}

private object? EvaluateUnixMillis(IReadOnlyList<SqlExpression> args, RowContext row)
{
var ts = ToDateTimeOffset(Evaluate(args[0], row));
return ts.ToUnixTimeMilliseconds();
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#unix_micros
//   "Returns the number of microseconds since 1970-01-01 00:00:00 UTC."
private object? EvaluateUnixMicros(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var ts = ToDateTimeOffset(Evaluate(args[0], row));
	return (ts - DateTimeOffset.UnixEpoch).Ticks / 10;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_from_unix_date
//   "Interprets int64_expression as the number of days since 1970-01-01."
private object? EvaluateDateFromUnixDate(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var days = ToLong(Evaluate(args[0], row));
	return DateOnly.FromDateTime(new DateTime(1970, 1, 1).AddDays(days));
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#unix_date
//   "Returns the number of days since 1970-01-01."
private object? EvaluateUnixDate(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var date = ToDateTime(Evaluate(args[0], row));
	return (long)(date.Date - new DateTime(1970, 1, 1)).TotalDays;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day
//   "Returns the last day from a date expression."
private object? EvaluateLastDay(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var val = Evaluate(args[0], row);
	if (val is null) return null;
	var date = ToDateTime(val);
	var part = args.Count > 1
		? Evaluate(args[1], row)?.ToString()?.ToUpperInvariant() ?? "MONTH"
		: "MONTH";
	var result = part switch
	{
		"YEAR" => new DateTime(date.Year, 12, 31),
		"QUARTER" => new DateTime(date.Year, ((date.Month - 1) / 3 + 1) * 3, 1).AddMonths(1).AddDays(-1),
		"MONTH" => new DateTime(date.Year, date.Month, DateTime.DaysInMonth(date.Year, date.Month)),
		"WEEK" => date.AddDays(6 - (int)date.DayOfWeek), // week starts Sunday, last day is Saturday
		_ => new DateTime(date.Year, date.Month, DateTime.DaysInMonth(date.Year, date.Month)),
	};
	return DateOnly.FromDateTime(result);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_add
//   "Adds int64_expression units of part to the DATETIME object."
private object? EvaluateDatetimeAdd(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var date = ToDateTime(Evaluate(args[0], row));
	var interval = ToLong(Evaluate(args[1], row));
	var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
	return AddToPart(date, interval, part);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_sub
//   "Subtracts int64_expression units of part from the DATETIME."
private object? EvaluateDatetimeSub(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var date = ToDateTime(Evaluate(args[0], row));
	var interval = ToLong(Evaluate(args[1], row));
	var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
	return AddToPart(date, -interval, part);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_diff
//   "Gets the number of unit boundaries between two DATETIME values."
private object? EvaluateDatetimeDiff(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var dt1 = ToDateTime(Evaluate(args[0], row));
	var dt2 = ToDateTime(Evaluate(args[1], row));
	var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_diff
	//   "Gets the number of unit boundaries between two DATETIME values."
	//   Boundary counting: truncate both values to the part, then diff.
	return part switch
	{
		"MICROSECOND" => (dt1 - dt2).Ticks / 10,
		"MILLISECOND" => (long)(dt1 - dt2).TotalMilliseconds,
		"SECOND" => (long)((new DateTime(dt1.Year, dt1.Month, dt1.Day, dt1.Hour, dt1.Minute, dt1.Second) -
		                     new DateTime(dt2.Year, dt2.Month, dt2.Day, dt2.Hour, dt2.Minute, dt2.Second)).TotalSeconds),
		"MINUTE" => (long)((new DateTime(dt1.Year, dt1.Month, dt1.Day, dt1.Hour, dt1.Minute, 0) -
		                     new DateTime(dt2.Year, dt2.Month, dt2.Day, dt2.Hour, dt2.Minute, 0)).TotalMinutes),
		"HOUR" => (long)((new DateTime(dt1.Year, dt1.Month, dt1.Day, dt1.Hour, 0, 0) -
		                   new DateTime(dt2.Year, dt2.Month, dt2.Day, dt2.Hour, 0, 0)).TotalHours),
		"DAY" => (long)(dt1.Date - dt2.Date).TotalDays,
		"WEEK" => (long)((dt1.Date - dt2.Date).TotalDays / 7),
		"MONTH" => (long)((dt1.Year - dt2.Year) * 12 + dt1.Month - dt2.Month),
		"YEAR" => (long)(dt1.Year - dt2.Year),
		"QUARTER" => (long)(((dt1.Year - dt2.Year) * 12 + dt1.Month - dt2.Month) / 3),
		_ => (long)(dt1.Date - dt2.Date).TotalDays
	};
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_trunc
//   "Truncates a DATETIME value at a particular granularity."
private object? EvaluateDatetimeTrunc(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var date = ToDateTime(Evaluate(args[0], row));
	var part = Evaluate(args[1], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
	return part switch
	{
		"YEAR" => new DateTime(date.Year, 1, 1),
		"MONTH" => new DateTime(date.Year, date.Month, 1),
		"QUARTER" => new DateTime(date.Year, ((date.Month - 1) / 3) * 3 + 1, 1),
		"WEEK" => date.Date.AddDays(-(int)date.DayOfWeek),
		"DAY" => date.Date,
		"HOUR" => new DateTime(date.Year, date.Month, date.Day, date.Hour, 0, 0),
		"MINUTE" => new DateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, 0),
		"SECOND" => new DateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, date.Second),
		_ => date.Date
	};
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#format_datetime
//   "Formats a DATETIME value according to a specified format string."
private object? EvaluateFormatDatetime(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var format = Evaluate(args[0], row)?.ToString();
	if (format is null) return null;
	var dtVal = Evaluate(args[1], row);
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#format_datetime
	//   "Returns NULL if any argument is NULL."
	if (dtVal is null) return null;
	var dt = ToDateTime(dtVal);
	return FormatTimestamp(new DateTimeOffset(dt, TimeSpan.Zero), format);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#parse_datetime
//   "Converts a STRING value to a DATETIME value."
private object? EvaluateParseDatetime(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var formatVal = Evaluate(args[0], row);
	var strVal = Evaluate(args[1], row);
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#parse_datetime
	//   Returns NULL if any argument is NULL.
	if (formatVal is null || strVal is null) return null;
	var format = formatVal.ToString() ?? "";
	var str = strVal.ToString() ?? "";
	return ParseTimestamp(str, format).DateTime;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time
//   "Constructs a TIME object."
private object? EvaluateTimeConstructor(IReadOnlyList<SqlExpression> args, RowContext row)
{
	if (args.Count == 3)
	{
		var h = (int)ToLong(Evaluate(args[0], row));
		var m = (int)ToLong(Evaluate(args[1], row));
		var s = (int)ToLong(Evaluate(args[2], row));
		return new TimeSpan(h, m, s);
	}
	var val = Evaluate(args[0], row);
	return val switch
	{
		TimeSpan ts => ts,
		DateTime dt => dt.TimeOfDay,
		DateTimeOffset dto => dto.TimeOfDay,
		string s => TimeSpan.Parse(s, CultureInfo.InvariantCulture),
		_ => TimeSpan.Parse(val?.ToString() ?? "00:00:00", CultureInfo.InvariantCulture)
	};
}

private static TimeSpan ToTimeSpan(object? val)
{
	return val switch
	{
		TimeSpan ts => ts,
		DateTime dt => dt.TimeOfDay,
		DateTimeOffset dto => dto.TimeOfDay,
		string s => TimeSpan.Parse(s, CultureInfo.InvariantCulture),
		_ => TimeSpan.Parse(val?.ToString() ?? "00:00:00", CultureInfo.InvariantCulture)
	};
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_add
//   "Adds int64_expression units of part to the TIME object."
//   "This function automatically adjusts when values fall outside of the 00:00:00 to 24:00:00 boundary."
private object? EvaluateTimeAdd(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var time = ToTimeSpan(Evaluate(args[0], row));
	var interval = ToLong(Evaluate(args[1], row));
	var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "SECOND";
	var result = AddToTimeSpan(time, interval, part);
	return WrapTime(result);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_sub
//   "Subtracts int64_expression units of part from the TIME object."
private object? EvaluateTimeSub(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var time = ToTimeSpan(Evaluate(args[0], row));
	var interval = ToLong(Evaluate(args[1], row));
	var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "SECOND";
	var result = AddToTimeSpan(time, -interval, part);
	return WrapTime(result);
}

private static TimeSpan AddToTimeSpan(TimeSpan ts, long interval, string part)
{
	return part switch
	{
		"MICROSECOND" => ts.Add(TimeSpan.FromTicks(interval * 10)),
		"MILLISECOND" => ts.Add(TimeSpan.FromMilliseconds(interval)),
		"SECOND" => ts.Add(TimeSpan.FromSeconds(interval)),
		"MINUTE" => ts.Add(TimeSpan.FromMinutes(interval)),
		"HOUR" => ts.Add(TimeSpan.FromHours(interval)),
		_ => ts.Add(TimeSpan.FromSeconds(interval))
	};
}

private static TimeSpan WrapTime(TimeSpan ts)
{
	var ticks = ts.Ticks % TimeSpan.TicksPerDay;
	if (ticks < 0) ticks += TimeSpan.TicksPerDay;
	return new TimeSpan(ticks);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_diff
//   "Gets the number of unit boundaries between two TIME values."
private object? EvaluateTimeDiff(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var t1 = ToTimeSpan(Evaluate(args[0], row));
	var t2 = ToTimeSpan(Evaluate(args[1], row));
	var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "SECOND";
	// Boundary counting: truncate both values to the part, then diff.
	return part switch
	{
		"MICROSECOND" => (t1 - t2).Ticks / 10,
		"MILLISECOND" => (long)(t1 - t2).TotalMilliseconds,
		"SECOND" => (long)(new TimeSpan(t1.Hours, t1.Minutes, t1.Seconds) -
		                    new TimeSpan(t2.Hours, t2.Minutes, t2.Seconds)).TotalSeconds,
		"MINUTE" => (long)(new TimeSpan(t1.Hours, t1.Minutes, 0) -
		                    new TimeSpan(t2.Hours, t2.Minutes, 0)).TotalMinutes,
		"HOUR" => (long)(new TimeSpan(t1.Hours, 0, 0) -
		                  new TimeSpan(t2.Hours, 0, 0)).TotalHours,
		_ => (long)(new TimeSpan(t1.Hours, t1.Minutes, t1.Seconds) -
		             new TimeSpan(t2.Hours, t2.Minutes, t2.Seconds)).TotalSeconds
	};
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_trunc
//   "Truncates a TIME value at a particular granularity."
private object? EvaluateTimeTrunc(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var time = ToTimeSpan(Evaluate(args[0], row));
	var part = Evaluate(args[1], row)?.ToString()?.ToUpperInvariant() ?? "SECOND";
	return part switch
	{
		"HOUR" => new TimeSpan(time.Hours, 0, 0),
		"MINUTE" => new TimeSpan(time.Hours, time.Minutes, 0),
		"SECOND" => new TimeSpan(time.Hours, time.Minutes, time.Seconds),
		"MILLISECOND" => new TimeSpan(time.Hours, time.Minutes, time.Seconds).Add(TimeSpan.FromMilliseconds(time.Milliseconds)),
		_ => time // MICROSECOND = no truncation
	};
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#format_time
//   "Formats a TIME value according to the specified format string."
private object? EvaluateFormatTime(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var format = Evaluate(args[0], row)?.ToString();
	if (format is null) return null;
	var timeVal = Evaluate(args[1], row);
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#format_time
	//   "Returns NULL if any argument is NULL."
	if (timeVal is null) return null;
	var time = ToTimeSpan(timeVal);
	var dto = new DateTimeOffset(2000, 1, 1, time.Hours, time.Minutes, time.Seconds, TimeSpan.Zero);
	return FormatTimestamp(dto, format);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#parse_time
//   "Converts a STRING value to a TIME value."
private object? EvaluateParseTime(IReadOnlyList<SqlExpression> args, RowContext row)
{
	var formatVal = Evaluate(args[0], row);
	var strVal = Evaluate(args[1], row);
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#parse_time
	//   Returns NULL if any argument is NULL.
	if (formatVal is null || strVal is null) return null;
	var format = formatVal.ToString() ?? "";
	var str = strVal.ToString() ?? "";
	var netFormat = format
		.Replace("%H", "HH").Replace("%M", "mm").Replace("%S", "ss")
		.Replace("%T", "HH:mm:ss").Replace("%I", "hh").Replace("%p", "tt");
	if (DateTime.TryParseExact(str, netFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
		return dt.TimeOfDay;
	if (TimeSpan.TryParse(str, CultureInfo.InvariantCulture, out var ts))
		return ts;
	return TimeSpan.Zero;
}

private static DateTime AddToPart(DateTime date, long interval, string part)
{
return part switch
{
"DAY" => date.AddDays(interval),
"MONTH" => date.AddMonths((int)interval),
"YEAR" => date.AddYears((int)interval),
"WEEK" => date.AddDays(interval * 7),
"QUARTER" => date.AddMonths((int)interval * 3),
"HOUR" => date.AddHours(interval),
"MINUTE" => date.AddMinutes(interval),
"SECOND" => date.AddSeconds(interval),
_ => date.AddDays(interval)
};
}

private static DateTimeOffset ToDateTimeOffset(object? val)
{
return val switch
{
DateTimeOffset dto => dto,
DateTime dt => new DateTimeOffset(dt, TimeSpan.Zero),
DateOnly d => new DateTimeOffset(d.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero),
string s => DateTimeOffset.Parse(NormalizeTimestampString(s), CultureInfo.InvariantCulture),
long l => DateTimeOffset.FromUnixTimeSeconds(l),
_ => DateTimeOffset.Parse(NormalizeTimestampString(val?.ToString() ?? ""), CultureInfo.InvariantCulture)
};
}

private static DateTime ToDateTime(object? val)
{
return val switch
{
DateTime dt => dt,
DateTimeOffset dto => dto.DateTime,
DateOnly d => d.ToDateTime(TimeOnly.MinValue),
string s => DateTime.Parse(s, CultureInfo.InvariantCulture),
_ => DateTime.Parse(val?.ToString() ?? "", CultureInfo.InvariantCulture)
};
}

private static string FormatTimestamp(DateTimeOffset ts, string format)
{
// Handle format specifiers that don't have .NET equivalents via pre-substitution
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time
//   "%j: The day of the year as a decimal number (001-366)."
//   "%V: The ISO 8601 week number of the current year as a decimal number (01-53)."
//   "%G: The ISO 8601 year with century as a decimal number."
//   "%u: The weekday (Monday as the first day of the week) as a decimal number (1-7)."
var result = format;
if (result.Contains("%j"))
	result = result.Replace("%j", ts.DayOfYear.ToString("D3"));
if (result.Contains("%V"))
	result = result.Replace("%V", System.Globalization.ISOWeek.GetWeekOfYear(ts.DateTime).ToString("D2"));
if (result.Contains("%G"))
	result = result.Replace("%G", System.Globalization.ISOWeek.GetYear(ts.DateTime).ToString("D4"));
if (result.Contains("%u"))
	result = result.Replace("%u", (ts.DayOfWeek == DayOfWeek.Sunday ? 7 : (int)ts.DayOfWeek).ToString());

// Convert BigQuery format specifiers to .NET
var netFormat = result
.Replace("%Y", "yyyy").Replace("%m", "MM").Replace("%d", "dd")
.Replace("%H", "HH").Replace("%M", "mm").Replace("%S", "ss")
.Replace("%F", "yyyy-MM-dd").Replace("%T", "HH:mm:ss")
.Replace("%E4Y", "yyyy").Replace("%Z", "zzz").Replace("%I", "hh").Replace("%p", "tt").Replace("%y", "yy")
.Replace("%b", "MMM").Replace("%B", "MMMM")
.Replace("%A", "dddd").Replace("%a", "ddd");
return ts.ToString(netFormat, CultureInfo.InvariantCulture);
}

private static DateTimeOffset ParseTimestamp(string str, string format)
{
var netFormat = format
.Replace("%Y", "yyyy").Replace("%m", "MM").Replace("%d", "dd")
.Replace("%H", "HH").Replace("%M", "mm").Replace("%S", "ss")
.Replace("%F", "yyyy-MM-dd").Replace("%T", "HH:mm:ss")
.Replace("%E4Y", "yyyy").Replace("%Z", "zzz").Replace("%I", "hh").Replace("%p", "tt").Replace("%y", "yy")
.Replace("%b", "MMM").Replace("%B", "MMMM");
if (DateTimeOffset.TryParseExact(str, netFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var result))
return result;
return DateTimeOffset.Parse(str, CultureInfo.InvariantCulture);
}

#endregion
#region Remaining function helpers

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#declaring_an_array_type
// When an array contains structs, propagate field names from the first named struct to all unnamed ones.
private object? EvaluateArrayLiteral(IReadOnlyList<SqlExpression> args, RowContext row)
{
var items = args.Select(a => Evaluate(a, row)).ToList();
// Propagate struct field names from the first named struct
IReadOnlyList<string>? fieldNames = null;
foreach (var item in items)
{
if (item is IDictionary<string, object?> d && d.Keys.Any(k => !k.StartsWith("_field_")))
{
fieldNames = d.Keys.ToList();
break;
}
}
if (fieldNames != null)
{
for (int i = 0; i < items.Count; i++)
{
if (items[i] is IDictionary<string, object?> d && d.Keys.All(k => k.StartsWith("_field_")))
{
var renamed = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
var vals = d.Values.ToList();
for (int j = 0; j < Math.Min(fieldNames.Count, vals.Count); j++)
renamed[fieldNames[j]] = vals[j];
items[i] = renamed;
}
}
}
return items;
}

private object? EvaluateArrayLength(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is IList<object?> list) return (long)list.Count;
if (val is IEnumerable<object?> en) return (long)en.Count();
return null;
}

private object? EvaluateArrayToString(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
var delimiter = Evaluate(args[1], row)?.ToString() ?? ",";
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_to_string
//   "If null_text is specified, the function replaces any NULL values in the array with null_text."
//   "If null_text is not specified, NULL values are omitted."
var nullText = args.Count > 2 ? Evaluate(args[2], row)?.ToString() : null;
if (val is IEnumerable<object?> en)
{
	if (nullText is not null)
		return string.Join(delimiter, en.Select(v => ConvertToString(v) ?? nullText));
	else
		return string.Join(delimiter, en.Where(v => v is not null).Select(v => ConvertToString(v) ?? ""));
}
return null;
}

private object? EvaluateGenerateArray(IReadOnlyList<SqlExpression> args, RowContext row)
{
var start = ToLong(Evaluate(args[0], row));
var end = ToLong(Evaluate(args[1], row));
var step = args.Count > 2 ? ToLong(Evaluate(args[2], row)) : 1L;
var result = new List<object?>();
if (step > 0) for (var i = start; i <= end; i += step) result.Add(i);
else if (step < 0) for (var i = start; i >= end; i += step) result.Add(i);
return result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_concat
//   "Concatenates one or more arrays with the same element type into a single array."
//   "Returns NULL if any input argument is NULL."
private object? EvaluateArrayConcat(IReadOnlyList<SqlExpression> args, RowContext row)
{
var result = new List<object?>();
foreach (var arg in args)
{
    var val = Evaluate(arg, row);
    if (val is null) return null;
    if (val is IList<object?> list) result.AddRange(list);
    else if (val is IEnumerable<object?> en) result.AddRange(en);
    else result.Add(val);
}
return result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_reverse
//   "Returns the input ARRAY with elements in reverse order."
private object? EvaluateArrayReverse(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
if (val is IList<object?> list) return list.Reverse().ToList();
return null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_first
//   "Takes an array and returns the first element in the array."
private object? EvaluateArrayFirst(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
if (val is IList<object?> list)
{
    if (list.Count == 0) throw new InvalidOperationException("ARRAY_FIRST on empty array");
    return list[0];
}
return null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_last
//   "Takes an array and returns the last element in the array."
private object? EvaluateArrayLast(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
if (val is IList<object?> list)
{
    if (list.Count == 0) throw new InvalidOperationException("ARRAY_LAST on empty array");
    return list[list.Count - 1];
}
return null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_slice
//   "Returns an array containing zero or more consecutive elements from the input array."
private object? EvaluateArraySlice(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
var startOffsetRaw = Evaluate(args[1], row);
var endOffsetRaw = Evaluate(args[2], row);
if (val is null || startOffsetRaw is null || endOffsetRaw is null) return null;
if (val is not IList<object?> list) return null;
if (list.Count == 0) return new List<object?>();

int startOffset = (int)ToLong(startOffsetRaw);
int endOffset = (int)ToLong(endOffsetRaw);

// Resolve negative offsets and clamp
int ResolveOffset(int offset) =>
    offset >= 0
        ? Math.Min(offset, list.Count - 1)
        : Math.Max(0, list.Count + offset);

int start = ResolveOffset(startOffset);
int end = ResolveOffset(endOffset);
if (start > end) return new List<object?>();
return list.Skip(start).Take(end - start + 1).ToList();
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
//   "Returns an array of dates. The start_date and end_date parameters determine the inclusive
//    start and end of the array."
private object? EvaluateGenerateDateArray(IReadOnlyList<SqlExpression> args, RowContext row)
{
var startVal = Evaluate(args[0], row);
var endVal = Evaluate(args[1], row);
if (startVal is null || endVal is null) return null;
var startDate = ToDateTime(startVal);
var endDate = ToDateTime(endVal);

int step = 1;
string part = "DAY";
if (args.Count >= 3)
{
    step = (int)ToLong(Evaluate(args[2], row));
}
if (args.Count >= 4)
{
    part = Evaluate(args[3], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
}

var result = new List<object?>();
if (step == 0) throw new InvalidOperationException("GENERATE_DATE_ARRAY step cannot be 0");

var current = startDate;
if (step > 0)
{
    while (current <= endDate)
    {
        result.Add(DateOnly.FromDateTime(current));
        current = AddDatePart(current, step, part);
    }
}
else
{
    while (current >= endDate)
    {
        result.Add(DateOnly.FromDateTime(current));
        current = AddDatePart(current, step, part);
    }
}
return result;
}

private static DateTime AddDatePart(DateTime date, int amount, string part)
{
return part.ToUpperInvariant() switch
{
    "DAY" => date.AddDays(amount),
    "WEEK" => date.AddDays(amount * 7),
    "MONTH" => date.AddMonths(amount),
    "QUARTER" => date.AddMonths(amount * 3),
    "YEAR" => date.AddYears(amount),
    _ => date.AddDays(amount)
};
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_timestamp_array
//   "Returns an ARRAY of TIMESTAMPS separated by a given interval."
private object? EvaluateGenerateTimestampArray(IReadOnlyList<SqlExpression> args, RowContext row)
{
var startVal = Evaluate(args[0], row);
var endVal = Evaluate(args[1], row);
if (startVal is null || endVal is null) return null;
var startTs = startVal is DateTimeOffset so ? so : DateTimeOffset.Parse(startVal.ToString()!, System.Globalization.CultureInfo.InvariantCulture);
var endTs = endVal is DateTimeOffset eo ? eo : DateTimeOffset.Parse(endVal.ToString()!, System.Globalization.CultureInfo.InvariantCulture);

int step = args.Count >= 3 ? (int)ToLong(Evaluate(args[2], row)) : 1;
string part = args.Count >= 4 ? Evaluate(args[3], row)?.ToString()?.ToUpperInvariant() ?? "DAY" : "DAY";
if (step == 0) throw new InvalidOperationException("GENERATE_TIMESTAMP_ARRAY step cannot be 0");

var result = new List<object?>();
var current = startTs;
if (step > 0)
{
    while (current <= endTs)
    {
        result.Add(current);
        current = AddTimestampPart(current, step, part);
    }
}
else
{
    while (current >= endTs)
    {
        result.Add(current);
        current = AddTimestampPart(current, step, part);
    }
}
return result;
}

private static DateTimeOffset AddTimestampPart(DateTimeOffset ts, int amount, string part)
{
return part.ToUpperInvariant() switch
{
    "MICROSECOND" => ts.AddTicks(amount * 10),
    "MILLISECOND" => ts.AddMilliseconds(amount),
    "SECOND" => ts.AddSeconds(amount),
    "MINUTE" => ts.AddMinutes(amount),
    "HOUR" => ts.AddHours(amount),
    "DAY" => ts.AddDays(amount),
    _ => ts.AddDays(amount)
};
}

// ARRAY_INCLUDES returns TRUE if the array contains the target value.
private object? EvaluateArrayIncludes(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
var target = Evaluate(args[1], row);
if (val is null) return null;
if (val is IList<object?> list)
    return list.Any(v => Equals(v, target) || (v?.ToString() == target?.ToString()));
return false;
}

// ARRAY_INCLUDES_ALL returns TRUE if every element of the second array is in the first.
private object? EvaluateArrayIncludesAll(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
var targets = Evaluate(args[1], row);
if (val is null || targets is null) return null;
if (val is not IList<object?> list || targets is not IList<object?> targetList) return false;
var strSet = list.Select(v => v?.ToString()).ToHashSet();
return targetList.All(t => strSet.Contains(t?.ToString()));
}

// ARRAY_INCLUDES_ANY returns TRUE if any element of the second array is in the first.
private object? EvaluateArrayIncludesAny(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
var targets = Evaluate(args[1], row);
if (val is null || targets is null) return null;
if (val is not IList<object?> list || targets is not IList<object?> targetList) return false;
var strSet = list.Select(v => v?.ToString()).ToHashSet();
return targetList.Any(t => strSet.Contains(t?.ToString()));
}

// ARRAY_MAX returns the maximum value from an array.
private object? EvaluateArrayMax(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
if (val is not IList<object?> list) return null;
var nonNull = list.Where(v => v is not null).ToList();
if (nonNull.Count == 0) return null;
return nonNull.Aggregate((a, b) => CompareRaw(a!, b!) > 0 ? a : b);
}

// ARRAY_MIN returns the minimum value from an array.
private object? EvaluateArrayMin(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
if (val is not IList<object?> list) return null;
var nonNull = list.Where(v => v is not null).ToList();
if (nonNull.Count == 0) return null;
return nonNull.Aggregate((a, b) => CompareRaw(a!, b!) < 0 ? a : b);
}

// ARRAY_SUM returns the sum of values in an array.
private object? EvaluateArraySum(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
if (val is not IList<object?> list) return null;
return SumValues(list.ToList());
}

// ARRAY_AVG returns the average of values in an array.
private object? EvaluateArrayAvg(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
if (val is not IList<object?> list) return null;
return AvgValues(list.ToList());
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_is_distinct
//   "Returns TRUE if the array contains no repeated elements."
private object? EvaluateArrayIsDistinct(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    if (val is not IList<object?> list) return null;
    var seen = new HashSet<string>();
    foreach (var item in list)
    {
        var key = item is null ? "\0NULL\0" : ConvertToString(item) ?? "\0NULL\0";
        if (!seen.Add(key)) return false;
    }
    return true;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_filter
//   "Returns an array containing elements from the input array for which lambda returns TRUE."
private object? EvaluateArrayFilter(IReadOnlyList<SqlExpression> rawArgs, RowContext row)
{
    var arrVal = Evaluate(rawArgs[0], row);
    if (arrVal is null) return null;
    if (arrVal is not IList<object?> list) return null;
    if (rawArgs.Count < 2 || rawArgs[1] is not LambdaExpr lambda)
        throw new InvalidOperationException("ARRAY_FILTER requires a lambda expression as second argument");
    var result = new List<object?>();
    foreach (var item in list)
    {
        var fields = new Dictionary<string, object?>(row.Fields) { [lambda.ParamName] = item };
        var lambdaRow = new RowContext(fields, row.Alias);
        var cond = Evaluate(lambda.Body, lambdaRow);
        if (IsTruthy(cond)) result.Add(item);
    }
    return result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_transform
//   "Returns an array with the results of applying the lambda to each element."
private object? EvaluateArrayTransform(IReadOnlyList<SqlExpression> rawArgs, RowContext row)
{
    var arrVal = Evaluate(rawArgs[0], row);
    if (arrVal is null) return null;
    if (arrVal is not IList<object?> list) return null;
    if (rawArgs.Count < 2 || rawArgs[1] is not LambdaExpr lambda)
        throw new InvalidOperationException("ARRAY_TRANSFORM requires a lambda expression as second argument");
    var result = new List<object?>();
    foreach (var item in list)
    {
        var fields = new Dictionary<string, object?>(row.Fields) { [lambda.ParamName] = item };
        var lambdaRow = new RowContext(fields, row.Alias);
        result.Add(Evaluate(lambda.Body, lambdaRow));
    }
    return result;
}

private object? EvaluateMd5(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
var bytes = System.Security.Cryptography.MD5.HashData(System.Text.Encoding.UTF8.GetBytes(val));
return bytes;
}

private object? EvaluateSha256(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
var bytes = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(val));
return bytes;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#sha1
//   "Computes the hash of the input using the SHA-1 algorithm. Returns 20 bytes."
private object? EvaluateSha1(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
return System.Security.Cryptography.SHA1.HashData(System.Text.Encoding.UTF8.GetBytes(val));
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#sha512
//   "Computes the hash of the input using the SHA-512 algorithm. Returns 64 bytes."
private object? EvaluateSha512(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
return System.Security.Cryptography.SHA512.HashData(System.Text.Encoding.UTF8.GetBytes(val));
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#farm_fingerprint
//   "Computes the fingerprint using the FarmHash Fingerprint64 algorithm. Returns INT64."
private object? EvaluateFarmFingerprint(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
// Approximate FarmHash Fingerprint64 using a stable hash (FNV-1a 64-bit)
// This is not exact FarmHash but provides deterministic INT64 output.
ulong hash = 14695981039346656037;
foreach (var b in System.Text.Encoding.UTF8.GetBytes(val))
{
    hash ^= b;
    hash *= 1099511628211;
}
return unchecked((long)hash);
}

private object? EvaluateToBase64(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is byte[] bytes) return Convert.ToBase64String(bytes);
if (val is string s) return Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(s));
return null;
}

private object? EvaluateFromBase64(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
return Convert.FromBase64String(val);
}

private object? EvaluateToHex(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is byte[] bytes) return Convert.ToHexString(bytes).ToLowerInvariant();
// Byte literals b'...' are normalized to strings by NormalizeSql; treat as UTF-8 bytes.
if (val is string s) return Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(s)).ToLowerInvariant();
return null;
}

private object? EvaluateFromHex(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
return Convert.FromHexString(val);
}

private object? EvaluateJsonExtract(IReadOnlyList<SqlExpression> args, RowContext row)
{
var json = Evaluate(args[0], row)?.ToString();
var path = Evaluate(args[1], row)?.ToString();
if (json is null || path is null) return null;
try
{
using var doc = System.Text.Json.JsonDocument.Parse(json);
var element = NavigateJsonPath(doc.RootElement, path);
return element?.GetRawText();
}
catch { return null; }
}

private object? EvaluateJsonExtractScalar(IReadOnlyList<SqlExpression> args, RowContext row)
{
var json = Evaluate(args[0], row)?.ToString();
var path = Evaluate(args[1], row)?.ToString();
if (json is null || path is null) return null;
try
{
using var doc = System.Text.Json.JsonDocument.Parse(json);
var element = NavigateJsonPath(doc.RootElement, path);
if (element is null) return null;
return element.Value.ValueKind switch
{
System.Text.Json.JsonValueKind.String => element.Value.GetString(),
System.Text.Json.JsonValueKind.Number => element.Value.GetRawText(),
System.Text.Json.JsonValueKind.True => "true",
System.Text.Json.JsonValueKind.False => "false",
System.Text.Json.JsonValueKind.Null => null,
_ => element.Value.GetRawText()
};
}
catch { return null; }
}

private static System.Text.Json.JsonElement? NavigateJsonPath(System.Text.Json.JsonElement root, string path)
{
// Handle JSONPath like "$.field", "$.field.subfield", "$[0]", "$.arr[1]"
var current = root;
var trimmed = path.StartsWith("$") ? path.Substring(1) : path;
if (trimmed.StartsWith(".")) trimmed = trimmed.Substring(1);
// Split by dots but keep array brackets
var segments = new System.Collections.Generic.List<string>();
var sb = new System.Text.StringBuilder();
foreach (var ch in trimmed)
{
    if (ch == '.' && sb.Length > 0) { segments.Add(sb.ToString()); sb.Clear(); }
    else sb.Append(ch);
}
if (sb.Length > 0) segments.Add(sb.ToString());
foreach (var segment in segments)
{
    if (string.IsNullOrEmpty(segment)) continue;
    // Check for array index e.g. "[1]" or "field[1]"
    var bracketIdx = segment.IndexOf('[');
    if (bracketIdx >= 0)
    {
        var prop = segment.Substring(0, bracketIdx);
        if (!string.IsNullOrEmpty(prop))
        {
            if (current.ValueKind != System.Text.Json.JsonValueKind.Object) return null;
            if (!current.TryGetProperty(prop, out var next)) return null;
            current = next;
        }
        var idxStr = segment.Substring(bracketIdx + 1, segment.Length - bracketIdx - 2);
        if (!int.TryParse(idxStr, out var arrayIdx)) return null;
        if (current.ValueKind != System.Text.Json.JsonValueKind.Array) return null;
        if (arrayIdx < 0 || arrayIdx >= current.GetArrayLength()) return null;
        current = current[arrayIdx];
    }
    else
    {
        if (current.ValueKind != System.Text.Json.JsonValueKind.Object) return null;
        if (!current.TryGetProperty(segment, out var next)) return null;
        current = next;
    }
}
return current;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_extract_array
private object? EvaluateJsonExtractArray(IReadOnlyList<SqlExpression> args, RowContext row)
{
var json = Evaluate(args[0], row)?.ToString();
var path = args.Count > 1 ? Evaluate(args[1], row)?.ToString() : "$";
if (json is null) return null;
try
{
	using var doc = System.Text.Json.JsonDocument.Parse(json);
	var element = NavigateJsonPath(doc.RootElement, path ?? "$");
	if (element is null || element.Value.ValueKind != System.Text.Json.JsonValueKind.Array) return null;
	return element.Value.EnumerateArray().Select(e => (object?)e.GetRawText()).ToList();
}
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_value_array
private object? EvaluateJsonValueArray(IReadOnlyList<SqlExpression> args, RowContext row)
{
var json = Evaluate(args[0], row)?.ToString();
var path = args.Count > 1 ? Evaluate(args[1], row)?.ToString() : "$";
if (json is null) return null;
try
{
	using var doc = System.Text.Json.JsonDocument.Parse(json);
	var element = NavigateJsonPath(doc.RootElement, path ?? "$");
	if (element is null || element.Value.ValueKind != System.Text.Json.JsonValueKind.Array) return null;
	return element.Value.EnumerateArray().Select(e => (object?)(e.ValueKind == System.Text.Json.JsonValueKind.String
		? e.GetString() : e.GetRawText())).ToList();
}
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_keys
private object? EvaluateJsonKeys(IReadOnlyList<SqlExpression> args, RowContext row)
{
var json = Evaluate(args[0], row)?.ToString();
if (json is null) return null;
try
{
	using var doc = System.Text.Json.JsonDocument.Parse(json);
	var root = doc.RootElement;
	if (root.ValueKind != System.Text.Json.JsonValueKind.Object) return null;
	return root.EnumerateObject().Select(p => (object?)p.Name).ToList();
}
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_set
private object? EvaluateJsonSet(IReadOnlyList<SqlExpression> args, RowContext row)
{
var json = Evaluate(args[0], row)?.ToString();
if (json is null || args.Count < 3) return null;
try
{
	var dict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object?>>(json);
	if (dict is null) return json;
	var path = Evaluate(args[1], row)?.ToString()?.TrimStart('$', '.') ?? "";
	var value = Evaluate(args[2], row);
	dict[path] = value;
	return System.Text.Json.JsonSerializer.Serialize(dict);
}
catch { return json; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_strip_nulls
private object? EvaluateJsonStripNulls(IReadOnlyList<SqlExpression> args, RowContext row)
{
var json = Evaluate(args[0], row)?.ToString();
if (json is null) return null;
try
{
	using var doc = System.Text.Json.JsonDocument.Parse(json);
	return StripNulls(doc.RootElement);
}
catch { return json; }
}

private static string StripNulls(System.Text.Json.JsonElement element)
{
if (element.ValueKind == System.Text.Json.JsonValueKind.Object)
{
	var props = element.EnumerateObject()
		.Where(p => p.Value.ValueKind != System.Text.Json.JsonValueKind.Null)
		.Select(p => $"\"{p.Name}\":{StripNulls(p.Value)}");
	return "{" + string.Join(",", props) + "}";
}
if (element.ValueKind == System.Text.Json.JsonValueKind.Array)
{
	var items = element.EnumerateArray().Select(StripNulls);
	return "[" + string.Join(",", items) + "]";
}
return element.GetRawText();
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_type
private object? EvaluateJsonType(IReadOnlyList<SqlExpression> args, RowContext row)
{
var json = Evaluate(args[0], row)?.ToString();
if (json is null) return null;
try
{
	using var doc = System.Text.Json.JsonDocument.Parse(json);
	return doc.RootElement.ValueKind switch
	{
		System.Text.Json.JsonValueKind.Object => "object",
		System.Text.Json.JsonValueKind.Array => "array",
		System.Text.Json.JsonValueKind.String => "string",
		System.Text.Json.JsonValueKind.Number => "number",
		System.Text.Json.JsonValueKind.True or System.Text.Json.JsonValueKind.False => "boolean",
		System.Text.Json.JsonValueKind.Null => "null",
		_ => null
	};
}
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#parse_json
//   "Takes a JSON-formatted string and returns a JSON value."
private object? EvaluateParseJson(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
try
{
    // Validate it's valid JSON and return the normalized representation
    using var doc = System.Text.Json.JsonDocument.Parse(val);
    return doc.RootElement.GetRawText();
}
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#to_json
//   "Takes a SQL value and returns a JSON value."
private object? EvaluateToJson(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return "null";
if (val is bool b) return b ? "true" : "false";
if (val is string s) return System.Text.Json.JsonSerializer.Serialize(s);
if (val is IList<object?> list)
    return "[" + string.Join(",", list.Select(v => v is null ? "null" : System.Text.Json.JsonSerializer.Serialize(v))) + "]";
return System.Text.Json.JsonSerializer.Serialize(val);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array
//   "Creates a JSON array."
private object? EvaluateJsonArray(IReadOnlyList<SqlExpression> args, RowContext row)
{
var elements = args.Select(a => Evaluate(a, row)).ToList();
var parts = elements.Select(v =>
{
    if (v is null) return "null";
    if (v is bool b) return b ? "true" : "false";
    if (v is string s) return System.Text.Json.JsonSerializer.Serialize(s);
    if (v is long or int or double or float) return v.ToString()!;
    return System.Text.Json.JsonSerializer.Serialize(v);
});
return "[" + string.Join(",", parts) + "]";
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_object
//   "Creates a JSON object."
private object? EvaluateJsonObject(IReadOnlyList<SqlExpression> args, RowContext row)
{
if (args.Count == 0) return "{}";
var pairs = new List<string>();
for (int i = 0; i + 1 < args.Count; i += 2)
{
    var key = Evaluate(args[i], row)?.ToString();
    var val = Evaluate(args[i + 1], row);
    if (key is null) continue;
    string jsonKey = System.Text.Json.JsonSerializer.Serialize(key);
    string jsonVal;
    if (val is null) jsonVal = "null";
    else if (val is bool b) jsonVal = b ? "true" : "false";
    else if (val is string s) jsonVal = System.Text.Json.JsonSerializer.Serialize(s);
    else if (val is long or int or double or float) jsonVal = val.ToString()!;
    else jsonVal = System.Text.Json.JsonSerializer.Serialize(val);
    pairs.Add($"{jsonKey}:{jsonVal}");
}
return "{" + string.Join(",", pairs) + "}";
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_remove
//   "Removes a JSON element at a path."
private object? EvaluateJsonRemove(IReadOnlyList<SqlExpression> args, RowContext row)
{
var json = Evaluate(args[0], row)?.ToString();
if (json is null) return null;
var path = Evaluate(args[1], row)?.ToString();
if (path is null) return json;
try
{
    var dict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, System.Text.Json.JsonElement>>(json);
    if (dict is null) return json;
    // Simple path: $.key â†’ remove "key"
    var key = path.TrimStart('$', '.');
    dict.Remove(key);
    return System.Text.Json.JsonSerializer.Serialize(dict);
}
catch { return json; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#to_json_string
//   "Converts a JSON value to a SQL STRING value."
private object? EvaluateToJsonString(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return "null";
    if (val is bool b) return b ? "true" : "false";
    // If already a JSON string (from PARSE_JSON, JSON_OBJECT, etc), return as-is
    if (val is string s)
    {
        var trimmed = s.Trim();
        if ((trimmed.StartsWith("{") && trimmed.EndsWith("}")) ||
            (trimmed.StartsWith("[") && trimmed.EndsWith("]")) ||
            trimmed == "null" || trimmed == "true" || trimmed == "false" ||
            (trimmed.StartsWith("\"") && trimmed.EndsWith("\"")))
        {
            try { System.Text.Json.JsonDocument.Parse(trimmed); return trimmed; }
            catch { }
        }
        return System.Text.Json.JsonSerializer.Serialize(s);
    }
    return System.Text.Json.JsonSerializer.Serialize(val);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#length
//   "Returns the length of a STRING value in characters, or the length of a BYTES value in bytes."
private object? EvaluateLength(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
if (val is byte[] bytes) return (long)bytes.Length;
return (long?)val.ToString()?.Length;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_instr
//   "Returns the 1-based position of the first occurrence of a regex match."
private object? EvaluateRegexpInstr(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
var pattern = Evaluate(args[1], row)?.ToString();
if (str is null || pattern is null) return null;
var m = System.Text.RegularExpressions.Regex.Match(str, pattern);
return m.Success ? (long)(m.Index + 1) : 0L;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_substr
//   "Returns the substring matched by a regular expression."
private object? EvaluateRegexpSubstr(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
var pattern = Evaluate(args[1], row)?.ToString();
if (str is null || pattern is null) return null;
var m = System.Text.RegularExpressions.Regex.Match(str, pattern);
return m.Success ? m.Value : null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/bit_functions#bit_count
//   "Returns the number of bits that are set in the input expression."
private object? EvaluateBitCount(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
var l = ToLong(val);
return (long)System.Numerics.BitOperations.PopCount((ulong)l);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/interval_functions#make_interval
//   "Constructs an INTERVAL value. MAKE_INTERVAL(year, month, day, hour, minute, second)."
private object? EvaluateMakeInterval(IReadOnlyList<SqlExpression> args, RowContext row)
{
long year = args.Count > 0 ? ToLong(Evaluate(args[0], row) ?? 0L) : 0;
long month = args.Count > 1 ? ToLong(Evaluate(args[1], row) ?? 0L) : 0;
long day = args.Count > 2 ? ToLong(Evaluate(args[2], row) ?? 0L) : 0;
long hour = args.Count > 3 ? ToLong(Evaluate(args[3], row) ?? 0L) : 0;
long minute = args.Count > 4 ? ToLong(Evaluate(args[4], row) ?? 0L) : 0;
long second = args.Count > 5 ? ToLong(Evaluate(args[5], row) ?? 0L) : 0;
// BigQuery INTERVAL format: Y-M D H:M:S
return $"{year}-{month} {day} {hour}:{minute}:{second}";
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/interval_functions#justify_hours
//   "Normalizes the INTERVAL so that the hour component is less than 24."
private object? EvaluateJustifyHours(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
return val; // Simplified: intervals are already string-formatted
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/interval_functions#justify_days
//   "Normalizes the INTERVAL so that the day component is less than 30."
private object? EvaluateJustifyDays(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
return val; // Simplified: intervals are already string-formatted
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/interval_functions#justify_interval
//   "Normalizes the INTERVAL."
private object? EvaluateJustifyInterval(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
return val; // Simplified: intervals are already string-formatted
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/range-functions#range
//   "Constructs a RANGE<T> value with lower and upper bounds."
private object? EvaluateRange(IReadOnlyList<SqlExpression> args, RowContext row)
{
var lower = Evaluate(args[0], row);
var upper = args.Count > 1 ? Evaluate(args[1], row) : null;
return new RangeValue(lower, upper);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/range-functions#range_start
//   "Gets the lower bound of a range."
private object? EvaluateRangeStart(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is RangeValue r) return r.Start;
return null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/range-functions#range_end
//   "Gets the upper bound of a range."
private object? EvaluateRangeEnd(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is RangeValue r) return r.End;
return null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/range-functions#range_contains
//   "Checks if a value or range is contained within another range."
private object? EvaluateRangeContains(IReadOnlyList<SqlExpression> args, RowContext row)
{
var outer = Evaluate(args[0], row);
var inner = Evaluate(args[1], row);
if (outer is not RangeValue outerRange) return null;
if (inner is null) return null;
if (inner is RangeValue innerRange)
{
    return CompareValues(outerRange.Start, innerRange.Start) <= 0 &&
           CompareValues(outerRange.End, innerRange.End) >= 0;
}
// Scalar containment: start <= value < end
return CompareValues(outerRange.Start, inner) <= 0 &&
       CompareValues(inner, outerRange.End) < 0;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/range-functions#range_overlaps
//   "Checks if two ranges overlap."
private object? EvaluateRangeOverlaps(IReadOnlyList<SqlExpression> args, RowContext row)
{
var a = Evaluate(args[0], row);
var b = Evaluate(args[1], row);
if (a is not RangeValue ra || b is not RangeValue rb) return null;
// Two ranges overlap if aStart < bEnd AND bStart < aEnd
return CompareValues(ra.Start, rb.End) < 0 &&
       CompareValues(rb.Start, ra.End) < 0;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/range-functions#generate_range_array
//   "Splits a range into an array of subranges."
private object? EvaluateGenerateRangeArray(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is not RangeValue range) return null;
// For simplicity, return a single-element array containing the full range
return new List<object?> { range };
}
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#nethosturl
//   "Takes a URL as a STRING and returns the host."
private object? EvaluateNetHost(IReadOnlyList<SqlExpression> args, RowContext row)
{
var url = Evaluate(args[0], row)?.ToString();
if (url is null) return null;
try { return new Uri(url.Contains("://") ? url : url.StartsWith("//") ? "http:" + url : "http://" + url).Host; }
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netpublic_suffix
//   "Takes a URL and returns the public suffix (e.g., .com, .co.uk)."
private static readonly HashSet<string> _twoLevelTlds = new(StringComparer.OrdinalIgnoreCase)
{
	"co.uk", "org.uk", "ac.uk", "gov.uk", "com.au", "net.au", "org.au",
	"co.nz", "co.jp", "co.kr", "co.in", "com.br", "com.cn", "com.mx",
	"co.za", "com.sg", "com.hk", "co.il", "com.ar", "com.tw"
};

private object? EvaluateNetPublicSuffix(IReadOnlyList<SqlExpression> args, RowContext row)
{
var url = Evaluate(args[0], row)?.ToString();
if (url is null) return null;
try
{
	var host = new Uri(url.Contains("://") ? url : url.StartsWith("//") ? "http:" + url : "http://" + url).Host;
	var parts = host.Split('.');
	if (parts.Length >= 3)
	{
		var twoLevel = parts[^2] + "." + parts[^1];
		if (_twoLevelTlds.Contains(twoLevel)) return twoLevel;
	}
	return parts.Length >= 2 ? parts[^1] : host;
}
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netreg_domain
//   "Takes a URL and returns the registered domain (e.g., google.com)."
private object? EvaluateNetRegDomain(IReadOnlyList<SqlExpression> args, RowContext row)
{
var url = Evaluate(args[0], row)?.ToString();
if (url is null) return null;
try
{
	var host = new Uri(url.Contains("://") ? url : url.StartsWith("//") ? "http:" + url : "http://" + url).Host;
	var parts = host.Split('.');
	if (parts.Length >= 3)
	{
		var twoLevel = parts[^2] + "." + parts[^1];
		if (_twoLevelTlds.Contains(twoLevel))
			return parts.Length >= 4 ? parts[^3] + "." + twoLevel : twoLevel;
	}
	return parts.Length >= 2 ? parts[^2] + "." + parts[^1] : host;
}
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netip_from_string
//   "Converts a STRING containing an IPv4/IPv6 address to BYTES."
private object? EvaluateNetIpFromString(IReadOnlyList<SqlExpression> args, RowContext row)
{
var ip = Evaluate(args[0], row)?.ToString();
if (ip is null) return null;
try { return System.Net.IPAddress.Parse(ip).GetAddressBytes(); }
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netip_to_string
//   "Converts BYTES to a STRING containing an IPv4/IPv6 address."
private object? EvaluateNetIpToString(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is not byte[] bytes) return null;
try { return new System.Net.IPAddress(bytes).ToString(); }
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netip_net_mask
//   "Returns a network mask."
private object? EvaluateNetIpNetMask(IReadOnlyList<SqlExpression> args, RowContext row)
{
var output_bytes = (int)ToLong(Evaluate(args[0], row));
var prefix = (int)ToLong(Evaluate(args[1], row));
var mask = new byte[output_bytes];
for (int i = 0; i < prefix && i < output_bytes * 8; i++)
	mask[i / 8] |= (byte)(128 >> (i % 8));
return mask;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netip_trunc
//   "Converts a BYTES IPv4 or IPv6 address to a BYTES subnet address."
private object? EvaluateNetIpTrunc(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is not byte[] bytes) return null;
var prefix = (int)ToLong(Evaluate(args[1], row));
var result = new byte[bytes.Length];
for (int i = 0; i < bytes.Length; i++)
{
	int bitsForByte = Math.Max(0, Math.Min(8, prefix - i * 8));
	if (bitsForByte == 8) result[i] = bytes[i];
	else if (bitsForByte > 0) result[i] = (byte)(bytes[i] & (0xFF << (8 - bitsForByte)));
}
return result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netipv4_from_int64
//   "Converts an IPv4 address from integer format to binary (BYTES) format."
private object? EvaluateNetIpv4FromInt64(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
var intVal = ToLong(val);
// Convert to 4-byte big-endian
var bytes = new byte[4];
bytes[0] = (byte)((intVal >> 24) & 0xFF);
bytes[1] = (byte)((intVal >> 16) & 0xFF);
bytes[2] = (byte)((intVal >> 8) & 0xFF);
bytes[3] = (byte)(intVal & 0xFF);
return bytes;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netipv4_to_int64
//   "Converts an IPv4 address from binary (BYTES) format to integer format."
private object? EvaluateNetIpv4ToInt64(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is not byte[] bytes || bytes.Length != 4) return null;
return (long)(((uint)bytes[0] << 24) | ((uint)bytes[1] << 16) | ((uint)bytes[2] << 8) | bytes[3]);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netsafe_ip_from_string
//   "Similar to NET.IP_FROM_STRING, but returns NULL instead of producing an error."
private object? EvaluateNetSafeIpFromString(IReadOnlyList<SqlExpression> args, RowContext row)
{
var ip = Evaluate(args[0], row)?.ToString();
if (ip is null) return null;
if (ip.Contains('/')) return null; // CIDR notation not supported
try { return System.Net.IPAddress.Parse(ip).GetAddressBytes(); }
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#parse_numeric
//   "Converts a STRING to a NUMERIC value."
private object? EvaluateParseNumeric(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row)?.ToString();
if (val is null) return null;
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#parse_numeric
// Strip whitespace and commas; handle leading/trailing sign
var s = val.Trim();
bool negative = false;
// Check for leading sign
if (s.StartsWith("-")) { negative = true; s = s.Substring(1).Trim(); }
else if (s.StartsWith("+")) { s = s.Substring(1).Trim(); }
// Check for trailing sign (after stripping leading)
if (s.EndsWith("-")) { negative = !negative; s = s.Substring(0, s.Length - 1).Trim(); }
else if (s.EndsWith("+")) { s = s.Substring(0, s.Length - 1).Trim(); }
s = s.Replace(",", "").Trim();
if (string.IsNullOrEmpty(s)) throw new InvalidOperationException("Invalid PARSE_NUMERIC input");
var result = double.Parse(s, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture);
return negative ? -result : result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#bool_for_json
//   "Converts a JSON boolean to a SQL BOOL value."
private object? EvaluateJsonBool(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
var s = val.ToString()!.Trim();
if (s.Equals("true", StringComparison.OrdinalIgnoreCase)) return true;
if (s.Equals("false", StringComparison.OrdinalIgnoreCase)) return false;
throw new InvalidOperationException($"Cannot convert JSON value to BOOL: {s}");
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#int64_for_json
//   "Converts a JSON number to a SQL INT64 value."
private object? EvaluateJsonInt64(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
var s = val.ToString()!.Trim();
return long.Parse(s, System.Globalization.CultureInfo.InvariantCulture);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#float64_for_json
//   "Converts a JSON number to a SQL FLOAT64 value."
private object? EvaluateJsonFloat64(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
var s = val.ToString()!.Trim();
return double.Parse(s, System.Globalization.CultureInfo.InvariantCulture);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#string_for_json
//   "Converts a JSON string to a SQL STRING value."
private object? EvaluateJsonString(IReadOnlyList<SqlExpression> args, RowContext row)
{
var val = Evaluate(args[0], row);
if (val is null) return null;
var s = val.ToString()!.Trim();
// JSON strings are quoted with double quotes
if (s.StartsWith("\"") && s.EndsWith("\""))
	return s[1..^1];
return s;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_bool
//   "Attempts to convert a JSON value to a SQL BOOL value."
private object? EvaluateLaxBool(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    var s = val.ToString()!.Trim();
    // Try parsing as JSON
    try
    {
        using var doc = System.Text.Json.JsonDocument.Parse(s);
        var root = doc.RootElement;
        return root.ValueKind switch
        {
            System.Text.Json.JsonValueKind.True => (object?)true,
            System.Text.Json.JsonValueKind.False => (object?)false,
            System.Text.Json.JsonValueKind.String => root.GetString()?.ToLowerInvariant() switch
            {
                "true" => (object?)true,
                "false" => (object?)false,
                _ => null
            },
            System.Text.Json.JsonValueKind.Number => root.GetDouble() == 0 ? (object?)false : (object?)true,
            _ => null
        };
    }
    catch
    {
        // Not valid JSON â€” try as raw value
        if (s.Equals("true", StringComparison.OrdinalIgnoreCase)) return true;
        if (s.Equals("false", StringComparison.OrdinalIgnoreCase)) return false;
        if (val is bool b) return b;
        if (val is long l) return l != 0;
        if (val is double d) return d != 0;
        return null;
    }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_int64
//   "Attempts to convert a JSON value to a SQL INT64 value."
private object? EvaluateLaxInt64(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    var s = val.ToString()!.Trim();
    try
    {
        using var doc = System.Text.Json.JsonDocument.Parse(s);
        var root = doc.RootElement;
        return root.ValueKind switch
        {
            System.Text.Json.JsonValueKind.Number => (object?)(long)Math.Round(root.GetDouble()),
            System.Text.Json.JsonValueKind.True => (object?)1L,
            System.Text.Json.JsonValueKind.False => (object?)0L,
            System.Text.Json.JsonValueKind.String => long.TryParse(root.GetString(), System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var parsed) ? (object?)parsed : null,
            _ => null
        };
    }
    catch
    {
        if (val is long l) return l;
        if (val is double dv) return (long)Math.Round(dv);
        if (val is bool bv) return bv ? 1L : 0L;
        if (long.TryParse(s, System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var p)) return p;
        return null;
    }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_float64
//   "Attempts to convert a JSON value to a SQL FLOAT64 value."
private object? EvaluateLaxFloat64(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    var s = val.ToString()!.Trim();
    try
    {
        using var doc = System.Text.Json.JsonDocument.Parse(s);
        var root = doc.RootElement;
        return root.ValueKind switch
        {
            System.Text.Json.JsonValueKind.Number => (object?)root.GetDouble(),
            System.Text.Json.JsonValueKind.String => double.TryParse(root.GetString(), System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var parsed) ? (object?)parsed : null,
            _ => null
        };
    }
    catch
    {
        if (val is double dv) return dv;
        if (val is long lv) return (double)lv;
        if (double.TryParse(s, System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var p)) return p;
        return null;
    }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#lax_string
//   "Attempts to convert a JSON value to a SQL STRING value."
private object? EvaluateLaxString(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    var s = val.ToString()!.Trim();
    try
    {
        using var doc = System.Text.Json.JsonDocument.Parse(s);
        var root = doc.RootElement;
        return root.ValueKind switch
        {
            System.Text.Json.JsonValueKind.String => root.GetString(),
            System.Text.Json.JsonValueKind.True => "true",
            System.Text.Json.JsonValueKind.False => "false",
            System.Text.Json.JsonValueKind.Number => root.GetRawText(),
            _ => null
        };
    }
    catch
    {
        if (val is bool bv) return bv ? "true" : "false";
        if (val is string sv) return sv;
        return ConvertToString(val);
    }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array_append
//   "Appends an element to the end of a JSON array."
private object? EvaluateJsonArrayAppend(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var jsonVal = Evaluate(args[0], row);
    if (jsonVal is null) return null;
    var jsonStr = jsonVal.ToString()!;
    var path = args.Count > 1 ? Evaluate(args[1], row)?.ToString() : null;
    var appendVal = Evaluate(args[args.Count > 2 ? 2 : 1], row);

    using var doc = System.Text.Json.JsonDocument.Parse(jsonStr);
    using var ms = new System.IO.MemoryStream();
    using (var writer = new System.Text.Json.Utf8JsonWriter(ms))
    {
        if (path is null || path == "$")
        {
            // Append to root array
            if (doc.RootElement.ValueKind != System.Text.Json.JsonValueKind.Array)
                return jsonStr;
            writer.WriteStartArray();
            foreach (var el in doc.RootElement.EnumerateArray())
                el.WriteTo(writer);
            WriteJsonValue(writer, appendVal);
            writer.WriteEndArray();
        }
        else
        {
            doc.RootElement.WriteTo(writer);
        }
    }
    return System.Text.Encoding.UTF8.GetString(ms.ToArray());
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_array_insert
//   "Inserts an element at the specified position in a JSON array."
private object? EvaluateJsonArrayInsert(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var jsonVal = Evaluate(args[0], row);
    if (jsonVal is null) return null;
    var jsonStr = jsonVal.ToString()!;
    var path = Evaluate(args[1], row)?.ToString() ?? "$[0]";
    var insertVal = Evaluate(args[2], row);

    // Parse the index from the path like "$[2]"
    var idxMatch = System.Text.RegularExpressions.Regex.Match(path, @"\[(\d+)\]");
    if (!idxMatch.Success) return jsonStr;
    var idx = int.Parse(idxMatch.Groups[1].Value);

    using var doc = System.Text.Json.JsonDocument.Parse(jsonStr);
    if (doc.RootElement.ValueKind != System.Text.Json.JsonValueKind.Array)
        return jsonStr;

    using var ms = new System.IO.MemoryStream();
    using (var writer = new System.Text.Json.Utf8JsonWriter(ms))
    {
        writer.WriteStartArray();
        var i = 0;
        foreach (var el in doc.RootElement.EnumerateArray())
        {
            if (i == idx) WriteJsonValue(writer, insertVal);
            el.WriteTo(writer);
            i++;
        }
        if (idx >= i) WriteJsonValue(writer, insertVal);
        writer.WriteEndArray();
    }
    return System.Text.Encoding.UTF8.GetString(ms.ToArray());
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_contains
//   "Returns TRUE if a JSON value contains a specific JSON value."
private object? EvaluateJsonContains(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var jsonVal = Evaluate(args[0], row);
    if (jsonVal is null) return null;
    var searchVal = Evaluate(args[1], row);
    if (searchVal is null) return null;
    var jsonStr = jsonVal.ToString()!;
    var searchStr = searchVal.ToString()!;

    try
    {
        using var doc = System.Text.Json.JsonDocument.Parse(jsonStr);
        using var searchDoc = System.Text.Json.JsonDocument.Parse(searchStr);
        return JsonContainsValue(doc.RootElement, searchDoc.RootElement);
    }
    catch
    {
        return false;
    }
}

private static bool JsonContainsValue(System.Text.Json.JsonElement container, System.Text.Json.JsonElement target)
{
    if (JsonElementEquals(container, target)) return true;
    if (container.ValueKind == System.Text.Json.JsonValueKind.Array)
    {
        foreach (var el in container.EnumerateArray())
            if (JsonContainsValue(el, target)) return true;
    }
    else if (container.ValueKind == System.Text.Json.JsonValueKind.Object)
    {
        foreach (var prop in container.EnumerateObject())
            if (JsonContainsValue(prop.Value, target)) return true;
    }
    return false;
}

private static bool JsonElementEquals(System.Text.Json.JsonElement a, System.Text.Json.JsonElement b)
{
    if (a.ValueKind != b.ValueKind) return false;
    return a.GetRawText() == b.GetRawText();
}

private static void WriteJsonValue(System.Text.Json.Utf8JsonWriter writer, object? val)
{
    switch (val)
    {
        case null: writer.WriteNullValue(); break;
        case bool b: writer.WriteBooleanValue(b); break;
        case long l: writer.WriteNumberValue(l); break;
        case int i: writer.WriteNumberValue(i); break;
        case double d: writer.WriteNumberValue(d); break;
        case string s:
            // If it looks like JSON, write raw
            if ((s.StartsWith("{") || s.StartsWith("[") || s.StartsWith("\"")) &&
                s.Length > 1)
            {
                try { writer.WriteRawValue(s); break; }
                catch { /* fall through to string */ }
            }
            writer.WriteStringValue(s);
            break;
        default: writer.WriteStringValue(val.ToString()); break;
    }
}

#region AEAD Encryption Functions

// In-memory AEAD implementation using .NET AES-GCM.
// Keyset format: JSON with key array, each key has id, type, value (base64), status.

private object? EvaluateKeysNewKeyset(IReadOnlyList<SqlExpression> args, RowContext row)
{
var keyType = Evaluate(args[0], row)?.ToString() ?? "AEAD_AES_GCM_256";
var keyBytes = new byte[keyType.Contains("SIV") ? 64 : 32];
System.Security.Cryptography.RandomNumberGenerator.Fill(keyBytes);
var keyId = System.Security.Cryptography.RandomNumberGenerator.GetInt32(1, int.MaxValue);
var keyset = new { primaryKeyId = keyId, key = new[] {
	new { keyData = new { keyMaterialType = "SYMMETRIC", typeUrl = KeyTypeUrl(keyType),
		value = Convert.ToBase64String(keyBytes) }, keyId = keyId, outputPrefixType = "TINK", status = "ENABLED" }
}};
return System.Text.Json.JsonSerializer.Serialize(keyset);
}

private static string KeyTypeUrl(string keyType) => keyType switch
{
"AEAD_AES_GCM_256" => "type.googleapis.com/google.crypto.tink.AesGcmKey",
"DETERMINISTIC_AEAD_AES_SIV_CMAC_256" => "type.googleapis.com/google.crypto.tink.AesSivKey",
_ => "type.googleapis.com/google.crypto.tink.AesGcmKey"
};

private object? EvaluateKeysRotateKeyset(IReadOnlyList<SqlExpression> args, RowContext row)
{
var keysetJson = Evaluate(args[0], row)?.ToString();
var keyType = Evaluate(args[1], row)?.ToString() ?? "AEAD_AES_GCM_256";
if (keysetJson is null) return null;
using var doc = System.Text.Json.JsonDocument.Parse(keysetJson);
var keys = new List<System.Text.Json.JsonElement>();
foreach (var k in doc.RootElement.GetProperty("key").EnumerateArray()) keys.Add(k);
var newKeyBytes = new byte[keyType.Contains("SIV") ? 64 : 32];
System.Security.Cryptography.RandomNumberGenerator.Fill(newKeyBytes);
var newKeyId = System.Security.Cryptography.RandomNumberGenerator.GetInt32(1, int.MaxValue);
var newKey = new { keyData = new { keyMaterialType = "SYMMETRIC", typeUrl = KeyTypeUrl(keyType),
	value = Convert.ToBase64String(newKeyBytes) }, keyId = newKeyId, outputPrefixType = "TINK", status = "ENABLED" };
// Build new keyset JSON combining old keys with new primary
var allKeys = new List<object>();
foreach (var k in keys) allKeys.Add(k);
allKeys.Add(newKey);
return System.Text.Json.JsonSerializer.Serialize(new { primaryKeyId = newKeyId, key = allKeys });
}

private object? EvaluateKeysAddKeyFromRawBytes(IReadOnlyList<SqlExpression> args, RowContext row)
{
var keysetJson = Evaluate(args[0], row)?.ToString();
var keyType = Evaluate(args[1], row)?.ToString() ?? "AES_GCM";
var rawBytes = Evaluate(args[2], row);
if (keysetJson is null) return null;
string rawB64;
if (rawBytes is byte[] rb) rawB64 = Convert.ToBase64String(rb);
else if (rawBytes is string s) rawB64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(s));
else return null;
using var doc = System.Text.Json.JsonDocument.Parse(keysetJson);
var keys = new List<System.Text.Json.JsonElement>();
int primaryId = doc.RootElement.GetProperty("primaryKeyId").GetInt32();
foreach (var k in doc.RootElement.GetProperty("key").EnumerateArray()) keys.Add(k);
var newKeyId = System.Security.Cryptography.RandomNumberGenerator.GetInt32(1, int.MaxValue);
var newKey = new { keyData = new { keyMaterialType = "SYMMETRIC", typeUrl = KeyTypeUrl(keyType),
	value = rawB64 }, keyId = newKeyId, outputPrefixType = "TINK", status = "ENABLED" };
var allKeys = new List<object>();
foreach (var k in keys) allKeys.Add(k);
allKeys.Add(newKey);
return System.Text.Json.JsonSerializer.Serialize(new { primaryKeyId = primaryId, key = allKeys });
}

private object? EvaluateKeysKeysetFromJson(IReadOnlyList<SqlExpression> args, RowContext row)
{
var json = Evaluate(args[0], row)?.ToString();
if (json is null) return null;
// Validate it's valid keyset JSON, then return as-is (our keyset format IS JSON)
using var _ = System.Text.Json.JsonDocument.Parse(json);
return json;
}

private object? EvaluateKeysKeysetToJson(IReadOnlyList<SqlExpression> args, RowContext row)
{
var keyset = Evaluate(args[0], row)?.ToString();
if (keyset is null) return null;
return keyset; // Already JSON in our implementation
}

private object? EvaluateKeysKeysetLength(IReadOnlyList<SqlExpression> args, RowContext row)
{
var keyset = Evaluate(args[0], row)?.ToString();
if (keyset is null) return null;
using var doc = System.Text.Json.JsonDocument.Parse(keyset);
return (long)doc.RootElement.GetProperty("key").GetArrayLength();
}

private object? EvaluateKeysKeysetChain(IReadOnlyList<SqlExpression> args, RowContext row)
{
// In-memory: no real KMS, just pass through the keyset
var _ = Evaluate(args[0], row); // kms_resource_name (ignored)
return Evaluate(args[1], row); // keyset passthrough
}

private (byte[] Key, byte[] Nonce)? ExtractPrimaryKey(string? keysetJson)
{
if (keysetJson is null) return null;
using var doc = System.Text.Json.JsonDocument.Parse(keysetJson);
var primaryId = doc.RootElement.GetProperty("primaryKeyId").GetInt32();
foreach (var k in doc.RootElement.GetProperty("key").EnumerateArray())
{
	if (k.GetProperty("keyId").GetInt32() == primaryId)
	{
		var b64 = k.GetProperty("keyData").GetProperty("value").GetString();
		if (b64 is null) return null;
		var keyBytes = Convert.FromBase64String(b64);
		return (keyBytes, Array.Empty<byte>());
	}
}
return null;
}

private object? EvaluateAeadEncrypt(IReadOnlyList<SqlExpression> args, RowContext row)
{
var keyset = Evaluate(args[0], row)?.ToString();
var plaintext = Evaluate(args[1], row);
var aad = Evaluate(args[2], row);
if (keyset is null || plaintext is null) return null;
var keyInfo = ExtractPrimaryKey(keyset);
if (keyInfo is null) return null;
var ptBytes = plaintext is byte[] pb ? pb : System.Text.Encoding.UTF8.GetBytes(plaintext.ToString()!);
var aadBytes = aad is byte[] ab ? ab : System.Text.Encoding.UTF8.GetBytes(aad?.ToString() ?? "");
// AES-GCM: 12 byte nonce, 16 byte tag
var nonce = new byte[12];
System.Security.Cryptography.RandomNumberGenerator.Fill(nonce);
var ciphertext = new byte[ptBytes.Length];
var tag = new byte[16];
using var aes = new System.Security.Cryptography.AesGcm(keyInfo.Value.Key[..32], 16);
aes.Encrypt(nonce, ptBytes, ciphertext, tag, aadBytes);
// Output: nonce + ciphertext + tag
var result = new byte[nonce.Length + ciphertext.Length + tag.Length];
nonce.CopyTo(result, 0);
ciphertext.CopyTo(result, nonce.Length);
tag.CopyTo(result, nonce.Length + ciphertext.Length);
return Convert.ToBase64String(result);
}

private byte[]? AeadDecryptCore(string? keyset, object? cipherObj, object? aadObj)
{
if (keyset is null || cipherObj is null) return null;
var keyInfo = ExtractPrimaryKey(keyset);
if (keyInfo is null) return null;
var cipherBytes = cipherObj is byte[] cb ? cb : Convert.FromBase64String(cipherObj.ToString()!);
var aadBytes = aadObj is byte[] ab ? ab : System.Text.Encoding.UTF8.GetBytes(aadObj?.ToString() ?? "");
// AES-GCM: first 12 = nonce, last 16 = tag, middle = ciphertext
var nonce = cipherBytes[..12];
var tag = cipherBytes[^16..];
var cipher = cipherBytes[12..^16];
var plaintext = new byte[cipher.Length];
using var aes = new System.Security.Cryptography.AesGcm(keyInfo.Value.Key[..32], 16);
aes.Decrypt(nonce, cipher, tag, plaintext, aadBytes);
return plaintext;
}

private object? EvaluateAeadDecryptBytes(IReadOnlyList<SqlExpression> args, RowContext row)
{
var keyset = Evaluate(args[0], row)?.ToString();
var cipher = Evaluate(args[1], row);
var aad = Evaluate(args[2], row);
var result = AeadDecryptCore(keyset, cipher, aad);
return result is null ? null : Convert.ToBase64String(result);
}

private object? EvaluateAeadDecryptString(IReadOnlyList<SqlExpression> args, RowContext row)
{
var keyset = Evaluate(args[0], row)?.ToString();
var cipher = Evaluate(args[1], row);
var aad = Evaluate(args[2], row);
var result = AeadDecryptCore(keyset, cipher, aad);
return result is null ? null : System.Text.Encoding.UTF8.GetString(result);
}

// Deterministic AEAD: uses AES-SIV (SIV = nonce is derived from plaintext + AAD)
// For in-memory emulation, we use HMAC-SHA256 as the SIV and AES-CBC for encryption.
private object? EvaluateDeterministicEncrypt(IReadOnlyList<SqlExpression> args, RowContext row)
{
var keyset = Evaluate(args[0], row)?.ToString();
var plaintext = Evaluate(args[1], row);
var aad = Evaluate(args[2], row);
if (keyset is null || plaintext is null) return null;
var keyInfo = ExtractPrimaryKey(keyset);
if (keyInfo is null) return null;
var ptBytes = plaintext is byte[] pb ? pb : System.Text.Encoding.UTF8.GetBytes(plaintext.ToString()!);
var aadBytes = aad is byte[] ab ? ab : System.Text.Encoding.UTF8.GetBytes(aad?.ToString() ?? "");
var keyBytes = keyInfo.Value.Key;
// Deterministic: derive nonce from HMAC of plaintext + AAD using second half of key
using var hmac = new System.Security.Cryptography.HMACSHA256(keyBytes.Length >= 64 ? keyBytes[32..] : keyBytes);
var sivInput = new byte[ptBytes.Length + aadBytes.Length];
ptBytes.CopyTo(sivInput, 0);
aadBytes.CopyTo(sivInput, ptBytes.Length);
var siv = hmac.ComputeHash(sivInput)[..12]; // Use first 12 bytes as nonce
var ciphertext = new byte[ptBytes.Length];
var tag = new byte[16];
using var aes = new System.Security.Cryptography.AesGcm(keyBytes[..32], 16);
aes.Encrypt(siv, ptBytes, ciphertext, tag, aadBytes);
var result = new byte[siv.Length + ciphertext.Length + tag.Length];
siv.CopyTo(result, 0);
ciphertext.CopyTo(result, siv.Length);
tag.CopyTo(result, siv.Length + ciphertext.Length);
return Convert.ToBase64String(result);
}

private object? EvaluateDeterministicDecryptBytes(IReadOnlyList<SqlExpression> args, RowContext row)
{
return EvaluateAeadDecryptBytes(args, row); // Same decryption logic
}

private object? EvaluateDeterministicDecryptString(IReadOnlyList<SqlExpression> args, RowContext row)
{
return EvaluateAeadDecryptString(args, row); // Same decryption logic
}

#endregion

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_count_functions
//   "In-memory exact counting â€” HLL++ functions are approximated as exact distinct counts."
private object? EvaluateHllCount(string name, IReadOnlyList<SqlExpression> args, RowContext row)
{
// In-memory: HLL_COUNT.INIT returns value, MERGE/EXTRACT return the value directly
var val = Evaluate(args[0], row);
if (name.EndsWith("EXTRACT"))
	return val is long l ? l : (val is null ? 0L : 1L);
return val;
}


#region Geography function helpers

private object? EvaluateGeographyFunction(string name, IReadOnlyList<SqlExpression> args, RowContext row)
{
    // Evaluate all arguments first
    var vals = args.Select(a => Evaluate(a, row)).ToList();

    return name switch
    {
        "ST_GEOGPOINT" => vals[0] is null || vals[1] is null ? null
            : new GeoPoint(Convert.ToDouble(vals[0]), Convert.ToDouble(vals[1])),
        "ST_GEOGFROMTEXT" or "ST_GEOGFROMWKT" => vals[0] is null ? null
            : GeoComputation.ParseWkt(Convert.ToString(vals[0])!),
        "ST_GEOGFROMGEOJSON" => vals[0] is null ? null
            : GeoComputation.ParseGeoJson(Convert.ToString(vals[0])!),
        "ST_ASTEXT" => vals[0] is null ? null : ((GeoValue)vals[0]).ToWkt(),
        "ST_ASGEOJSON" => vals[0] is null ? null : ((GeoValue)vals[0]).ToGeoJson(),
        "ST_X" => vals[0] is null ? null : vals[0] is GeoPoint px ? (object)px.Longitude : null,
        "ST_Y" => vals[0] is null ? null : vals[0] is GeoPoint py ? (object)py.Latitude : null,
        "ST_DISTANCE" => vals[0] is null || vals[1] is null ? null
            : (object?)GeoComputation.Distance((GeoValue)vals[0], (GeoValue)vals[1]),
        "ST_DWITHIN" => vals[0] is null || vals[1] is null || vals[2] is null ? null
            : (object)(GeoComputation.Distance((GeoValue)vals[0], (GeoValue)vals[1]) <= Convert.ToDouble(vals[2])),
        "ST_CONTAINS" => GeoContains(vals[0], vals[1]),
        "ST_WITHIN" => GeoContains(vals[1], vals[0]),
        "ST_INTERSECTS" => GeoIntersects(vals[0], vals[1]),
        "ST_DISJOINT" => GeoIntersects(vals[0], vals[1]) is bool b ? (object)!b : null,
        "ST_EQUALS" => vals[0] is null || vals[1] is null ? null
            : (object)(((GeoValue)vals[0]).ToWkt() == ((GeoValue)vals[1]).ToWkt()),
        "ST_AREA" => GeoArea(vals[0]),
        "ST_LENGTH" => GeoLength(vals[0]),
        "ST_PERIMETER" => GeoPerimeter(vals[0]),
        "ST_NUMPOINTS" or "ST_NPOINTS" => vals[0] is null ? null : (long)((GeoValue)vals[0]).NumPoints,
        "ST_DIMENSION" => vals[0] is null ? null : (long)((GeoValue)vals[0]).Dimension,
        "ST_ISEMPTY" => vals[0] is null ? null : ((GeoValue)vals[0]).IsEmpty,
        "ST_GEOMETRYTYPE" => vals[0] is null ? null : ((GeoValue)vals[0]).GeometryType,
        "ST_MAKELINE" => GeoMakeLine(vals),
        "ST_CENTROID" => GeoCentroid(vals[0]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_asbinary
        "ST_ASBINARY" => vals[0] is null ? null : GeoComputation.ToWkb((GeoValue)vals[0]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogfromwkb
        "ST_GEOGFROMWKB" => vals[0] is null ? null : GeoComputation.ParseWkb((byte[])vals[0]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_iscollection
        "ST_ISCOLLECTION" => vals[0] is null ? null : (object)(vals[0] is GeoMultiPoint),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_boundary
        "ST_BOUNDARY" => GeoBoundary(vals[0]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_coveredby
        "ST_COVEREDBY" => GeoContains(vals[1], vals[0]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_covers
        "ST_COVERS" => GeoContains(vals[0], vals[1]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_touches
        "ST_TOUCHES" => GeoTouches(vals[0], vals[1]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_closestpoint
        "ST_CLOSESTPOINT" => GeoClosestPoint(vals[0], vals[1]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_convexhull
        "ST_CONVEXHULL" => GeoConvexHull(vals[0]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_difference
        "ST_DIFFERENCE" => GeoDifference(vals[0], vals[1]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_intersection
        "ST_INTERSECTION" => GeoIntersection(vals[0], vals[1]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_union
        "ST_UNION" => GeoUnion(vals[0], vals[1]),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_buffer
        "ST_BUFFER" => GeoBuffer(vals[0], vals.Count > 1 ? vals[1] : null),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_simplify
        "ST_SIMPLIFY" => GeoSimplify(vals[0], vals.Count > 1 ? vals[1] : null),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_dump
        "ST_DUMP" => GeoDump(vals[0]),
        _ => throw new NotSupportedException("Unknown geography function: " + name)
    };
}

private static object? GeoContains(object? outer, object? inner)
{
    if (outer is null || inner is null) return null;
    var a = (GeoValue)outer;
    var b = (GeoValue)inner;
    if (a is GeoPolygon poly && b is GeoPoint pt)
        return GeoComputation.PointInPolygon(pt, poly);
    if (a is GeoPolygon poly2 && b is GeoPolygon innerPoly)
    {
        foreach (var ring in innerPoly.Rings)
            foreach (var p in ring)
                if (!GeoComputation.PointInPolygon(new GeoPoint(p.Lon, p.Lat), poly2)) return false;
        return true;
    }
    return false;
}

private static object? GeoIntersects(object? first, object? second)
{
    if (first is null || second is null) return null;
    var a = (GeoValue)first;
    var b = (GeoValue)second;
    if (a is GeoPolygon pa && b is GeoPolygon pb)
    {
        foreach (var p in pa.Rings[0])
            if (GeoComputation.PointInPolygon(new GeoPoint(p.Lon, p.Lat), pb)) return true;
        foreach (var p in pb.Rings[0])
            if (GeoComputation.PointInPolygon(new GeoPoint(p.Lon, p.Lat), pa)) return true;
        return false;
    }
    if (a is GeoPoint pt && b is GeoPolygon poly) return GeoComputation.PointInPolygon(pt, poly);
    if (a is GeoPolygon poly2 && b is GeoPoint pt2) return GeoComputation.PointInPolygon(pt2, poly2);
    if (a is GeoPoint pa2 && b is GeoPoint pb2)
        return pa2.Longitude == pb2.Longitude && pa2.Latitude == pb2.Latitude;
    return false;
}

private static object? GeoArea(object? val)
{
    if (val is null) return null;
    return val is GeoPolygon poly ? GeoComputation.Area(poly) : 0.0;
}

private static object? GeoLength(object? val)
{
    if (val is null) return null;
    return val is GeoLineString ls ? GeoComputation.Length(ls) : 0.0;
}

private static object? GeoPerimeter(object? val)
{
    if (val is null) return null;
    return val is GeoPolygon poly ? GeoComputation.Perimeter(poly) : 0.0;
}

private static object? GeoMakeLine(List<object?> vals)
{
    if (vals[0] is null || vals[1] is null) return null;
    var pts = new List<(double Lon, double Lat)>();
    foreach (var v in vals)
    {
        if (v is GeoPoint p) pts.Add((p.Longitude, p.Latitude));
        else if (v is GeoLineString ls) pts.AddRange(ls.Points);
    }
    return new GeoLineString(pts);
}

private static object? GeoCentroid(object? val)
{
    if (val is null) return null;
    var geo = (GeoValue)val;
    if (geo is GeoPoint pt) return pt;
    if (geo is GeoLineString ls)
        return new GeoPoint(ls.Points.Average(p => p.Lon), ls.Points.Average(p => p.Lat));
    if (geo is GeoPolygon poly)
    {
        var ring = poly.Rings[0];
        return new GeoPoint(ring.Average(p => p.Lon), ring.Average(p => p.Lat));
    }
    return null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_boundary
//   "Returns the boundary of a geography."
private static object? GeoBoundary(object? val)
{
    if (val is null) return null;
    var geo = (GeoValue)val;
    if (geo is GeoPoint) return new GeoEmpty();
    if (geo is GeoLineString ls)
    {
        if (ls.Points.Count < 2) return new GeoEmpty();
        var first = ls.Points[0];
        var last = ls.Points[^1];
        // If closed ring, boundary is empty
        if (first.Lon == last.Lon && first.Lat == last.Lat) return new GeoEmpty();
        // Return multipoint with endpoints (approximated as linestring with 2 points)
        return new GeoLineString(new[] { first, last }.ToList());
    }
    if (geo is GeoPolygon poly)
    {
        // Boundary of a polygon is its exterior ring
        return new GeoLineString(poly.Rings[0]);
    }
    return new GeoEmpty();
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_touches
//   "Returns TRUE if two geographies touch but do not intersect in their interiors."
private static object? GeoTouches(object? first, object? second)
{
    if (first is null || second is null) return null;
    var a = (GeoValue)first;
    var b = (GeoValue)second;
    // Simple approximation: touches if they share boundary points but interiors don't overlap
    if (a is GeoPoint pa && b is GeoPolygon pb)
    {
        // Point touches polygon if it's on the boundary but not inside
        var inside = GeoComputation.PointInPolygon(pa, pb);
        if (inside) return false;
        // Check if point is on the boundary (very close to any edge)
        return PointNearBoundary(pa, pb);
    }
    if (a is GeoPolygon pa2 && b is GeoPoint pb2)
        return GeoTouches(second, first);
    if (a is GeoPolygon polyA && b is GeoPolygon polyB)
    {
        var ringA = polyA.Rings[0];
        var ringB = polyB.Rings[0];
        bool sharedBoundary = false;
        for (int i = 0; i < ringA.Count - 1; i++)
        {
            var pt = new GeoPoint(ringA[i].Lon, ringA[i].Lat);
            if (PointNearBoundary(pt, polyB)) { sharedBoundary = true; continue; }
            if (GeoComputation.PointInPolygon(pt, polyB)) return false;
        }
        for (int i = 0; i < ringB.Count - 1; i++)
        {
            var pt = new GeoPoint(ringB[i].Lon, ringB[i].Lat);
            if (PointNearBoundary(pt, polyA)) { sharedBoundary = true; continue; }
            if (GeoComputation.PointInPolygon(pt, polyA)) return false;
        }
        return sharedBoundary;
    }
    return false;
}

private static bool PointNearBoundary(GeoPoint pt, GeoPolygon poly)
{
    var ring = poly.Rings[0];
    for (int i = 0; i < ring.Count - 1; i++)
    {
        var p1 = ring[i];
        var p2 = ring[i + 1];
        var dist = PointToSegmentDistance(pt.Longitude, pt.Latitude, p1.Lon, p1.Lat, p2.Lon, p2.Lat);
        if (dist < 1e-9) return true;
    }
    return false;
}

private static double PointToSegmentDistance(double px, double py, double x1, double y1, double x2, double y2)
{
    var dx = x2 - x1;
    var dy = y2 - y1;
    if (dx == 0 && dy == 0)
        return Math.Sqrt((px - x1) * (px - x1) + (py - y1) * (py - y1));
    var t = Math.Max(0, Math.Min(1, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)));
    var projX = x1 + t * dx;
    var projY = y1 + t * dy;
    return Math.Sqrt((px - projX) * (px - projX) + (py - projY) * (py - projY));
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_closestpoint
//   "Returns the point on a geography that is closest to another geography."
private static object? GeoClosestPoint(object? first, object? second)
{
    if (first is null || second is null) return null;
    var a = (GeoValue)first;
    var b = (GeoValue)second;
    // Get all points from a, find closest to b
    var targetPt = b is GeoPoint bp ? bp : (GeoValue)GeoCentroid(b)! is GeoPoint cp ? cp : new GeoPoint(0, 0);
    if (a is GeoPoint ap) return ap;
    if (a is GeoLineString ls)
    {
        GeoPoint? closest = null;
        var minDist = double.MaxValue;
        foreach (var p in ls.Points)
        {
            var pt = new GeoPoint(p.Lon, p.Lat);
            var d = GeoComputation.Distance(pt, targetPt) ?? double.MaxValue;
            if (d < minDist) { minDist = d; closest = pt; }
        }
        return closest;
    }
    if (a is GeoPolygon poly)
    {
        GeoPoint? closest = null;
        var minDist = double.MaxValue;
        foreach (var p in poly.Rings[0])
        {
            var pt = new GeoPoint(p.Lon, p.Lat);
            var d = GeoComputation.Distance(pt, targetPt) ?? double.MaxValue;
            if (d < minDist) { minDist = d; closest = pt; }
        }
        return closest;
    }
    return null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_convexhull
//   "Returns the convex hull of the input geography."
private static object? GeoConvexHull(object? val)
{
    if (val is null) return null;
    var geo = (GeoValue)val;
    if (geo is GeoPoint) return geo;
    var points = new List<(double Lon, double Lat)>();
    if (geo is GeoLineString ls) points.AddRange(ls.Points);
    else if (geo is GeoPolygon poly)
        foreach (var ring in poly.Rings)
            points.AddRange(ring);
    if (points.Count < 3) return geo;
    // Graham scan
    var hull = ComputeConvexHull(points);
    if (hull.Count >= 3)
    {
        // Close the ring
        if (hull[0] != hull[^1]) hull.Add(hull[0]);
        return new GeoPolygon(new[] { hull }.ToList());
    }
    return geo;
}

private static List<(double Lon, double Lat)> ComputeConvexHull(List<(double Lon, double Lat)> points)
{
    var sorted = points.OrderBy(p => p.Lon).ThenBy(p => p.Lat).ToList();
    var lower = new List<(double Lon, double Lat)>();
    foreach (var p in sorted)
    {
        while (lower.Count >= 2 && Cross(lower[^2], lower[^1], p) <= 0)
            lower.RemoveAt(lower.Count - 1);
        lower.Add(p);
    }
    var upper = new List<(double Lon, double Lat)>();
    foreach (var p in sorted.AsEnumerable().Reverse())
    {
        while (upper.Count >= 2 && Cross(upper[^2], upper[^1], p) <= 0)
            upper.RemoveAt(upper.Count - 1);
        upper.Add(p);
    }
    lower.RemoveAt(lower.Count - 1);
    upper.RemoveAt(upper.Count - 1);
    lower.AddRange(upper);
    return lower;
}

private static double Cross((double Lon, double Lat) o, (double Lon, double Lat) a, (double Lon, double Lat) b) =>
    (a.Lon - o.Lon) * (b.Lat - o.Lat) - (a.Lat - o.Lat) * (b.Lon - o.Lon);

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_difference
//   "Returns a geography representing the point-set difference of two geographies."
private static object? GeoDifference(object? first, object? second)
{
    if (first is null || second is null) return null;
    var a = (GeoValue)first;
    var b = (GeoValue)second;
    // Same geometry â†’ empty
    if (a.ToWkt() == b.ToWkt()) return new GeoEmpty();
    // Simple approximation: for polygons, return points of a not inside b
    if (a is GeoPolygon pa && b is GeoPolygon pb)
    {
        var filtered = pa.Rings[0].Where(p => !GeoComputation.PointInPolygon(new GeoPoint(p.Lon, p.Lat), pb)).ToList();
        if (filtered.Count < 3) return new GeoEmpty();
        if (filtered[0] != filtered[^1]) filtered.Add(filtered[0]);
        return new GeoPolygon(new[] { filtered }.ToList());
    }
    if (a is GeoPoint pt && b is GeoPolygon polyB)
        return GeoComputation.PointInPolygon(pt, polyB) ? new GeoEmpty() : (object)a;
    return a;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_intersection
//   "Returns a geography representing the point-set intersection of two geographies."
private static object? GeoIntersection(object? first, object? second)
{
    if (first is null || second is null) return null;
    var a = (GeoValue)first;
    var b = (GeoValue)second;
    // Point-point intersection
    if (a is GeoPoint ptA && b is GeoPoint ptB)
        return ptA.Longitude == ptB.Longitude && ptA.Latitude == ptB.Latitude ? (object)a : new GeoEmpty();
    if (a is GeoPolygon pa && b is GeoPolygon pb)
    {
        var filtered = pa.Rings[0].Where(p => GeoComputation.PointInPolygon(new GeoPoint(p.Lon, p.Lat), pb)).ToList();
        if (filtered.Count < 3) return new GeoEmpty();
        if (filtered[0] != filtered[^1]) filtered.Add(filtered[0]);
        return new GeoPolygon(new[] { filtered }.ToList());
    }
    if (a is GeoPoint pt && b is GeoPolygon polyB)
        return GeoComputation.PointInPolygon(pt, polyB) ? (object)a : new GeoEmpty();
    if (a is GeoPolygon polyA && b is GeoPoint pt2)
        return GeoComputation.PointInPolygon(pt2, polyA) ? (object)b : new GeoEmpty();
    return new GeoEmpty();
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_union
//   "Returns a geography representing the point-set union of two geographies."
private static object? GeoUnion(object? first, object? second)
{
    if (first is null || second is null) return null;
    var a = (GeoValue)first;
    var b = (GeoValue)second;
    // Same point returns point
    if (a is GeoPoint pa2 && b is GeoPoint pb2)
    {
        if (pa2.Longitude == pb2.Longitude && pa2.Latitude == pb2.Latitude) return a;
        return new GeoLineString(new List<(double, double)> {
            (pa2.Longitude, pa2.Latitude),
            (pb2.Longitude, pb2.Latitude)
        });
    }
    // Simple approximation: merge polygon rings
    if (a is GeoPolygon pa && b is GeoPolygon pb)
    {
        var allPoints = new List<(double Lon, double Lat)>();
        allPoints.AddRange(pa.Rings[0]);
        allPoints.AddRange(pb.Rings[0]);
        var hull = ComputeConvexHull(allPoints);
        if (hull.Count >= 3)
        {
            if (hull[0] != hull[^1]) hull.Add(hull[0]);
            return new GeoPolygon(new[] { hull }.ToList());
        }
    }
    return a;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_buffer
//   "Returns a geography that represents the buffer around the input geography."
private static object? GeoBuffer(object? val, object? distanceVal)
{
    if (val is null) return null;
    var geo = (GeoValue)val;
    var distance = distanceVal is null ? 0.0 : Convert.ToDouble(distanceVal);
    if (distance == 0) return geo;
    if (geo is GeoPoint pt)
    {
        // Approximate a circle polygon around the point
        const int segments = 32;
        const double earthRadius = 6371008.8;
        var latRad = pt.Latitude * Math.PI / 180;
        var lonRad = pt.Longitude * Math.PI / 180;
        var angularDist = distance / earthRadius;
        var ring = new List<(double Lon, double Lat)>();
        for (int i = 0; i <= segments; i++)
        {
            var bearing = 2 * Math.PI * i / segments;
            var lat2 = Math.Asin(Math.Sin(latRad) * Math.Cos(angularDist) +
                                  Math.Cos(latRad) * Math.Sin(angularDist) * Math.Cos(bearing));
            var lon2 = lonRad + Math.Atan2(Math.Sin(bearing) * Math.Sin(angularDist) * Math.Cos(latRad),
                                            Math.Cos(angularDist) - Math.Sin(latRad) * Math.Sin(lat2));
            ring.Add((lon2 * 180 / Math.PI, lat2 * 180 / Math.PI));
        }
        return new GeoPolygon(new[] { ring }.ToList());
    }
    // For other types, return the input as approximation
    return geo;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_simplify
//   "Returns a simplified version of a geography using the Douglas-Peucker algorithm."
private static object? GeoSimplify(object? val, object? toleranceVal)
{
    if (val is null) return null;
    var geo = (GeoValue)val;
    var tolerance = toleranceVal is null ? 0.0 : Convert.ToDouble(toleranceVal);
    if (tolerance <= 0) return geo;
    if (geo is GeoLineString ls)
    {
        var simplified = DouglasPeucker(ls.Points, tolerance / 111320.0); // meters to approx degrees
        return new GeoLineString(simplified);
    }
    if (geo is GeoPolygon poly)
    {
        var simplified = DouglasPeucker(poly.Rings[0], tolerance / 111320.0);
        if (simplified.Count < 4) return geo;
        return new GeoPolygon(new[] { simplified }.ToList());
    }
    return geo;
}

private static List<(double Lon, double Lat)> DouglasPeucker(IReadOnlyList<(double Lon, double Lat)> points, double epsilon)
{
    if (points.Count < 3) return points.ToList();
    var maxDist = 0.0;
    var maxIdx = 0;
    for (int i = 1; i < points.Count - 1; i++)
    {
        var d = PointToLineDistance(points[i], points[0], points[^1]);
        if (d > maxDist) { maxDist = d; maxIdx = i; }
    }
    if (maxDist > epsilon)
    {
        var left = DouglasPeucker(points.Take(maxIdx + 1).ToList(), epsilon);
        var right = DouglasPeucker(points.Skip(maxIdx).ToList(), epsilon);
        left.RemoveAt(left.Count - 1);
        left.AddRange(right);
        return left;
    }
    return new List<(double Lon, double Lat)> { points[0], points[^1] };
}

private static double PointToLineDistance((double Lon, double Lat) p, (double Lon, double Lat) a, (double Lon, double Lat) b)
{
    var dx = b.Lon - a.Lon;
    var dy = b.Lat - a.Lat;
    var lenSq = dx * dx + dy * dy;
    if (lenSq == 0) return Math.Sqrt((p.Lon - a.Lon) * (p.Lon - a.Lon) + (p.Lat - a.Lat) * (p.Lat - a.Lat));
    var t = ((p.Lon - a.Lon) * dx + (p.Lat - a.Lat) * dy) / lenSq;
    t = Math.Max(0, Math.Min(1, t));
    var projX = a.Lon + t * dx;
    var projY = a.Lat + t * dy;
    return Math.Sqrt((p.Lon - projX) * (p.Lon - projX) + (p.Lat - projY) * (p.Lat - projY));
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_dump
//   "Returns an array of geographies that comprise the input geography."
private static object? GeoDump(object? val)
{
    if (val is null) return null;
    var geo = (GeoValue)val;
    if (geo is GeoMultiPoint mp)
        return mp.Points.Select(p => (object?)p).ToList();
    // For simple types, return an array with one element
    return new List<object?> { geo };
}


// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_centroid_agg
//   "Returns the centroid of all input GEOGRAPHY values."
private object? EvaluateStCentroidAgg(List<object?> values)
{
    var geos = values.Where(v => v is GeoValue).Cast<GeoValue>().ToList();
    if (geos.Count == 0) return null;
    var centroids = geos.Select(g => GeoCentroid(g)).OfType<GeoPoint>().ToList();
    if (centroids.Count == 0) return null;
    return new GeoPoint(centroids.Average(p => p.Longitude), centroids.Average(p => p.Latitude));
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_union_agg
//   "Returns a GEOGRAPHY that represents the point set union of all input geographies."
private object? EvaluateStUnionAgg(List<object?> values)
{
    var geos = values.Where(v => v is GeoValue).ToList();
    if (geos.Count == 0) return null;
    object? result = geos[0];
    for (int i = 1; i < geos.Count; i++)
        result = GeoUnion(result, geos[i]);
    return result;
}

#endregion
private object? EvaluateUdf(string name, IReadOnlyList<SqlExpression> args, RowContext row)
{
// If name is qualified (dataset.func), resolve in specific dataset
if (name.Contains('.'))
{
	var parts = name.Split('.', 2);
	var dsName = parts[0];
	var funcName = parts[1];
	foreach (var ds in _store.Datasets.Values)
	{
		if (ds.DatasetId.Equals(dsName, StringComparison.OrdinalIgnoreCase) &&
			(ds.Routines.TryGetValue(funcName, out var r) || ds.Routines.TryGetValue(funcName.ToLowerInvariant(), out r)))
			return EvaluateUdfRoutine(r, args, row);
	}
}
// Search all datasets for UDF
foreach (var ds in _store.Datasets.Values)
{
if (ds.Routines.TryGetValue(name, out var routine) ||
ds.Routines.TryGetValue(name.ToLowerInvariant(), out routine))
{
if (routine.Language?.ToUpperInvariant() == "SQL" && routine.Body is not null)
{
var udfSql = routine.Body;
// Replace parameter references
for (int i = 0; i < args.Count && i < routine.Parameters.Count; i++)
{
var paramName = routine.Parameters[i].Name;
var argVal = Evaluate(args[i], row);
var argStr = argVal is null ? "NULL" :
argVal is string s ? "'" + s.Replace("'", "''") + "'" :
ConvertToString(argVal) ?? "NULL";
udfSql = udfSql.Replace(paramName, argStr);
}
var udfExecutor = new QueryExecutor(_store, _defaultDatasetId);
if (_parameters is not null) udfExecutor.SetParameters(_parameters);
var result = udfExecutor.Execute("SELECT " + udfSql);
if (result.Rows.Count > 0 && result.Rows[0].F?.Count > 0)
return result.Rows[0].F[0].V;
return null;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#javascript-udf-structure
//   "A JavaScript UDF lets you call code written in JavaScript from a SQL query."
if (routine.Language?.ToUpperInvariant() == "JAVASCRIPT" && routine.Body is not null)
{
var engine = _store.JsUdfEngine
	?? throw new NotSupportedException(
		"JavaScript UDFs require a JS engine. Install BigQuery.InMemoryEmulator.JsUdfs and call store.JsUdfEngine = new JintJsUdfEngine().");

var paramNames = routine.Parameters.Select(p => p.Name).ToList();
var argValues = args.Select(a => Evaluate(a, row)).ToList();
return engine.Execute(routine.Body, paramNames, argValues);
}
}
}
throw new NotSupportedException("Unknown function: " + name);
}

private object? EvaluateUdfRoutine(InMemoryRoutine routine, IReadOnlyList<SqlExpression> args, RowContext row)
{
	if (routine.Language?.ToUpperInvariant() == "SQL" && routine.Body is not null)
	{
		var udfSql = routine.Body;
		for (int i = 0; i < args.Count && i < routine.Parameters.Count; i++)
		{
			var paramName = routine.Parameters[i].Name;
			var argVal = Evaluate(args[i], row);
			var argStr = argVal is null ? "NULL" :
				argVal is string s ? "'" + s.Replace("'", "''") + "'" :
				ConvertToString(argVal) ?? "NULL";
			udfSql = udfSql.Replace(paramName, argStr);
		}
		var udfExecutor = new QueryExecutor(_store, _defaultDatasetId);
		if (_parameters is not null) udfExecutor.SetParameters(_parameters);
		var result = udfExecutor.Execute("SELECT " + udfSql);
		if (result.Rows.Count > 0 && result.Rows[0].F?.Count > 0)
			return result.Rows[0].F[0].V;
		return null;
	}
	if (routine.Language?.ToUpperInvariant() == "JAVASCRIPT" && routine.Body is not null)
	{
		var engine = _store.JsUdfEngine
			?? throw new NotSupportedException("JavaScript UDFs require a JS engine.");
		var paramNames = routine.Parameters.Select(p => p.Name).ToList();
		var argValues = args.Select(a => Evaluate(a, row)).ToList();
		return engine.Execute(routine.Body, paramNames, argValues);
	}
	return null;
}

#endregion

#region Aggregates

private object? EvaluateAggregate(AggregateCall agg, List<RowContext> rows)
{
var funcName = agg.FunctionName.ToUpperInvariant();

// COUNT(*) must not evaluate the StarExpr arg - just count rows
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#count
if (funcName == "COUNT" && agg.Arg is StarExpr)
    return (long)rows.Count;

    if (agg.AggOrderBy is { Count: > 0 })
        rows = OrderBy(rows, agg.AggOrderBy);

var values = rows.Select(r => Evaluate(agg.Arg!, r)).ToList();

if (agg.Distinct)
values = values.Where(v => v is not null).Distinct().ToList();

return funcName switch
{
"COUNT" => (long)values.Count(v => v is not null),
"SUM" => SumValues(values),
"AVG" => AvgValues(values),
"MIN" => values.Where(v => v is not null).Any()
? values.Where(v => v is not null).Aggregate((a, b) => CompareRaw(a!, b!) < 0 ? a : b)
: null,
"MAX" => values.Where(v => v is not null).Any()
? values.Where(v => v is not null).Aggregate((a, b) => CompareRaw(a!, b!) > 0 ? a : b)
: null,
"ANY_VALUE" => values.FirstOrDefault(v => v is not null),
"STRING_AGG" => EvaluateStringAgg(agg, rows),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
//   "By default, ARRAY_AGG includes NULLs." Only filtered with IGNORE NULLS.
"ARRAY_AGG" => HasIgnoreNullsMarker(agg) ? values.Where(v => v is not null).ToList() : values.ToList(),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_concat_agg
//   "Concatenates elements from expression of type ARRAY, returning a single ARRAY as a result."
"ARRAY_CONCAT_AGG" => values.Where(v => v is not null)
	.SelectMany(v => v is IList<object?> list ? list : (v is string s && s.Contains(", ") ? s.Split(", ").Select(x => (object?)x.Trim()).ToList() : [v]))
	.ToList(),
"COUNTIF" => (long)rows.Count(r => IsTruthy(Evaluate(agg.Arg!, r))),
"APPROX_COUNT_DISTINCT" => (long)values.Where(v => v is not null).Distinct().Count(),
"LOGICAL_AND" => values.All(v => v is true),
"LOGICAL_OR" => values.Any(v => v is true),
// Bitwise aggregates
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_and
//   "Performs a bitwise AND operation on expression and returns the result."
"BIT_AND" => EvaluateBitAgg(values, (a, b) => a & b),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_or
//   "Performs a bitwise OR operation on expression and returns the result."
"BIT_OR" => EvaluateBitAgg(values, (a, b) => a | b),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_xor
//   "Performs a bitwise XOR operation on expression and returns the result."
"BIT_XOR" => EvaluateBitAgg(values, (a, b) => a ^ b),
// Statistical aggregates
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions
"VAR_SAMP" or "VARIANCE" => EvaluateVariance(values, sample: true),
"VAR_POP" => EvaluateVariance(values, sample: false),
"STDDEV_SAMP" or "STDDEV" => EvaluateStddev(values, sample: true),
"STDDEV_POP" => EvaluateStddev(values, sample: false),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#corr
//   "Returns the Pearson coefficient of correlation of a set of number pairs."
"CORR" => EvaluateCorr(agg, rows),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#covar_pop
//   "Returns the population covariance of a set of number pairs."
"COVAR_POP" => EvaluateCovariance(agg, rows, sample: false),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#covar_samp
//   "Returns the sample covariance of a set of number pairs."
"COVAR_SAMP" => EvaluateCovariance(agg, rows, sample: true),
// Approximate aggregates
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_quantiles
//   "Returns the approximate boundaries for a group of expression values."
"APPROX_QUANTILES" => EvaluateApproxQuantiles(agg, rows),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_top_count
//   "Returns the approximate top elements of expression as an array of STRUCTs."
"APPROX_TOP_COUNT" => EvaluateApproxTopCount(agg, rows),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_top_sum
//   "Returns the approximate top elements of expression, based on the sum of an assigned weight."
"APPROX_TOP_SUM" => EvaluateApproxTopSum(agg, rows),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_centroid_agg
//   "Returns the centroid of all input GEOGRAPHY values."
"ST_CENTROID_AGG" => EvaluateStCentroidAgg(values),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_union_agg
//   "Returns a GEOGRAPHY that represents the point set union of all input geographies."
"ST_UNION_AGG" => EvaluateStUnionAgg(values),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_count_functions
//   "HLL_COUNT.MERGE returns the cardinality of multiple HLL++ sketches by computing their union."
"HLL_COUNT_MERGE" or "HLL_COUNT_MERGE_PARTIAL" => (long)values.Where(v => v is not null).Distinct().Count(),
_ => throw new NotSupportedException("Unsupported aggregate: " + funcName)
};
}

/// <summary>
/// Checks if an AggregateCall has the '__IGNORE_NULLS__' marker injected by the preprocessor.
/// </summary>
private static bool HasIgnoreNullsMarker(AggregateCall agg)
{
    if (agg.ExtraArgs is null) return false;
    return agg.ExtraArgs.Any(a => a is LiteralExpr { Value: "__IGNORE_NULLS__" });
}

private object? EvaluateStringAgg(AggregateCall agg, List<RowContext> rows)
{
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#string_agg
    //   "Returns a value (either STRING or BYTES) obtained by concatenating non-null values."
    var separator = agg.ExtraArgs is { Count: > 0 }
        ? Evaluate(agg.ExtraArgs[0], rows[0])?.ToString() ?? ","
        : ",";
    var values = rows.Select(r => Evaluate(agg.Arg!, r)?.ToString())
        .Where(v => v is not null).ToList();
    if (agg.Distinct)
        values = values.Distinct().ToList();
    if (agg.AggOrderBy is { Count: > 0 })
    {
        var paired = rows.Select(r => (Value: Evaluate(agg.Arg!, r)?.ToString(), Row: r))
            .Where(p => p.Value is not null).ToList();
        if (agg.Distinct)
            paired = paired.GroupBy(p => p.Value).Select(g => g.First()).ToList();
        var ordered = OrderBy(paired.Select(p => p.Row).ToList(), agg.AggOrderBy);
        values = ordered.Select(r => Evaluate(agg.Arg!, r)?.ToString()).Where(v => v is not null).ToList()!;
    }
    return string.Join(separator, values);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#var_samp
//   "Returns the sample (unbiased) variance of the values."
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#var_pop
//   "Returns the population (biased) variance of the values."
private static object? EvaluateVariance(List<object?> values, bool sample)
{
var nums = values.Where(v => v is not null).Select(v => Convert.ToDouble(v)).ToList();
if (sample && nums.Count < 2) return null;
if (!sample && nums.Count == 0) return null;
if (!sample && nums.Count == 1) return 0.0;
var mean = nums.Average();
var sumSqDiff = nums.Sum(x => (x - mean) * (x - mean));
return sample ? sumSqDiff / (nums.Count - 1) : sumSqDiff / nums.Count;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#stddev_samp
//   "Returns the sample (unbiased) standard deviation of the values."
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#stddev_pop
//   "Returns the population (biased) standard deviation of the values."
private static object? EvaluateStddev(List<object?> values, bool sample)
{
var variance = EvaluateVariance(values, sample);
if (variance is null) return null;
return Math.Sqrt((double)variance);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#bit_and
private static object? EvaluateBitAgg(List<object?> values, Func<long, long, long> op)
{
var nums = values.Where(v => v is not null).Select(v => ToLong(v)).ToList();
if (nums.Count == 0) return null;
return nums.Aggregate(op);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#corr
private object? EvaluateCorr(AggregateCall agg, List<RowContext> rows)
{
if (agg.ExtraArgs is not { Count: > 0 }) return null;
var pairs = rows
    .Select(r => (x: Evaluate(agg.Arg!, r), y: Evaluate(agg.ExtraArgs[0], r)))
    .Where(p => p.x is not null && p.y is not null)
    .Select(p => (x: Convert.ToDouble(p.x), y: Convert.ToDouble(p.y)))
    .ToList();
if (pairs.Count < 2) return null;
var meanX = pairs.Average(p => p.x);
var meanY = pairs.Average(p => p.y);
var cov = pairs.Sum(p => (p.x - meanX) * (p.y - meanY));
var stdX = Math.Sqrt(pairs.Sum(p => (p.x - meanX) * (p.x - meanX)));
var stdY = Math.Sqrt(pairs.Sum(p => (p.y - meanY) * (p.y - meanY)));
if (stdX == 0 || stdY == 0) return null;
return cov / (stdX * stdY);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#covar_pop
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#covar_samp
private object? EvaluateCovariance(AggregateCall agg, List<RowContext> rows, bool sample)
{
if (agg.ExtraArgs is not { Count: > 0 }) return null;
var pairs = rows
    .Select(r => (x: Evaluate(agg.Arg!, r), y: Evaluate(agg.ExtraArgs[0], r)))
    .Where(p => p.x is not null && p.y is not null)
    .Select(p => (x: Convert.ToDouble(p.x), y: Convert.ToDouble(p.y)))
    .ToList();
if (sample && pairs.Count < 2) return null;
if (!sample && pairs.Count == 0) return null;
var meanX = pairs.Average(p => p.x);
var meanY = pairs.Average(p => p.y);
var cov = pairs.Sum(p => (p.x - meanX) * (p.y - meanY));
return sample ? cov / (pairs.Count - 1) : cov / pairs.Count;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_quantiles
//   "Returns the approximate boundaries for a group of expression values, where number
//    represents the number of quantiles to create. Returns an array of number + 1 elements."
private object? EvaluateApproxQuantiles(AggregateCall agg, List<RowContext> rows)
{
var n = agg.ExtraArgs is { Count: > 0 } ? (int)ToLong(Evaluate(agg.ExtraArgs[0], rows[0])) : 2;
var values = rows.Select(r => Evaluate(agg.Arg!, r)).Where(v => v is not null).ToList();
if (values.Count == 0) return null;
// Sort and pick quantile boundaries
var sorted = values.OrderBy(v => Convert.ToDouble(v)).ToList();
var result = new List<object?>();
for (int i = 0; i <= n; i++)
{
    double pos = (double)i / n * (sorted.Count - 1);
    int idx = Math.Min((int)Math.Round(pos), sorted.Count - 1);
    result.Add(sorted[idx]);
}
return result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_top_count
//   "Returns the approximate top elements of expression as an array of STRUCTs with value and count."
private object? EvaluateApproxTopCount(AggregateCall agg, List<RowContext> rows)
{
var n = agg.ExtraArgs is { Count: > 0 } ? (int)ToLong(Evaluate(agg.ExtraArgs[0], rows[0])) : 1;
var values = rows.Select(r => Evaluate(agg.Arg!, r)).Where(v => v is not null).ToList();
var groups = values.GroupBy(v => v?.ToString()).OrderByDescending(g => g.Count()).Take(n);
var result = new List<object?>();
foreach (var g in groups)
{
    result.Add(new Dictionary<string, object?> { ["value"] = g.First(), ["count"] = (long)g.Count() });
}
return result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/approximate_aggregate_functions#approx_top_sum
//   "Returns the approximate top elements of expression, based on sum of assigned weight."
private object? EvaluateApproxTopSum(AggregateCall agg, List<RowContext> rows)
{
if (agg.ExtraArgs is not { Count: >= 2 }) return null;
var weightExpr = agg.ExtraArgs[0];
var n = (int)ToLong(Evaluate(agg.ExtraArgs[1], rows[0]));
var pairs = rows
    .Select(r => (val: Evaluate(agg.Arg!, r), weight: Evaluate(weightExpr, r)))
    .Where(p => p.val is not null && p.weight is not null)
    .ToList();
var groups = pairs
    .GroupBy(p => p.val?.ToString())
    .Select(g => new { Value = g.First().val, Sum = g.Sum(p => Convert.ToDouble(p.weight)) })
    .OrderByDescending(g => g.Sum)
    .Take(n);
var result = new List<object?>();
foreach (var g in groups)
{
    result.Add(new Dictionary<string, object?> { ["value"] = g.Value, ["sum"] = g.Sum });
}
return result;
}

#endregion

#region Set operations

private InMemoryBigQueryResult ExecuteSetOperation(SetOperationStatement setOp)
{
// If the left side is a SELECT with CTEs, resolve CTEs and propagate to right side
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
InMemoryBigQueryResult result;
if (setOp.Left is SelectStatement { Ctes: { Count: > 0 } } leftSel)
{
    var cteResults = ResolveCtes(leftSel);
    var leftBody = leftSel with { Ctes = null };
    var left = ExecuteSelect(leftBody, cteResults);
    var right = ExecuteWithCtes(setOp.Right, cteResults);
    result = CombineSetOperation(setOp, left, right);
}
else
{
    var leftResult = ExecuteStatement(setOp.Left);
    var rightResult = ExecuteStatement(setOp.Right);
    result = CombineSetOperation(setOp, leftResult, rightResult);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
//   "ORDER BY, LIMIT, and OFFSET after a set operation apply to the entire result."
return ApplySetOpOrderByLimitOffset(result, setOp);
}

private InMemoryBigQueryResult ExecuteWithCtes(
    SqlStatement stmt,
    Dictionary<string, (TableSchema Schema, List<Dictionary<string, object?>> Rows)> cteResults)
{
return stmt switch
{
    SelectStatement rs => ExecuteSelect(rs, cteResults),
    SetOperationStatement rSetOp =>
        CombineSetOperation(rSetOp,
            ExecuteWithCtes(rSetOp.Left, cteResults),
            ExecuteWithCtes(rSetOp.Right, cteResults)),
    _ => ExecuteStatement(stmt)
};
}

private static InMemoryBigQueryResult CombineSetOperation(
    SetOperationStatement setOp, InMemoryBigQueryResult left, InMemoryBigQueryResult right)
{
var schema = left.Schema;

var resultRows = (setOp.OpType, setOp.All) switch
{
(SetOperationType.Union, true) => left.Rows.Concat(right.Rows).ToList(),
(SetOperationType.Union, false) => left.Rows.Concat(right.Rows)
.GroupBy(r => string.Join("|", r.F?.Select(f => f?.V?.ToString() ?? "NULL") ?? Array.Empty<string>()))
.Select(g => g.First()).ToList(),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#except
//   "EXCEPT DISTINCT removes both duplicates and rows present in the right operand."
//   "EXCEPT ALL removes only one occurrence of each right-side row from the left side."
(SetOperationType.Except, false) => left.Rows
.GroupBy(r => string.Join("|", r.F?.Select(f => f?.V?.ToString() ?? "NULL") ?? Array.Empty<string>()))
.Select(g => g.First())
.Where(lr => !right.Rows.Any(rr => RowEquals(lr, rr))).ToList(),
(SetOperationType.Except, true) => ExceptAll(left.Rows, right.Rows),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#intersect
//   "INTERSECT DISTINCT returns only distinct rows present in both operands."
//   "INTERSECT ALL preserves duplicate counts: min(count_left, count_right) for each row."
(SetOperationType.Intersect, false) => left.Rows
.Where(lr => right.Rows.Any(rr => RowEquals(lr, rr)))
.GroupBy(r => string.Join("|", r.F?.Select(f => f?.V?.ToString() ?? "NULL") ?? Array.Empty<string>()))
.Select(g => g.First()).ToList(),
(SetOperationType.Intersect, true) => IntersectAll(left.Rows, right.Rows),
_ => throw new NotSupportedException("Unsupported set operation: " + setOp.OpType)
};

return new InMemoryBigQueryResult(schema, resultRows);
}

private static bool RowEquals(TableRow a, TableRow b)
{
if (a.F is null || b.F is null) return false;
if (a.F.Count != b.F.Count) return false;
for (int i = 0; i < a.F.Count; i++)
{
var av = a.F[i]?.V?.ToString();
var bv = b.F[i]?.V?.ToString();
if (av != bv) return false;
}
return true;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#except
//   "EXCEPT ALL: For each row in the right input, removes one matching row from the left input."
private static List<TableRow> ExceptAll(List<TableRow> left, List<TableRow> right)
{
var result = new List<TableRow>(left);
foreach (var rr in right)
{
    var idx = result.FindIndex(lr => RowEquals(lr, rr));
    if (idx >= 0) result.RemoveAt(idx);
}
return result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#intersect
//   "INTERSECT ALL: Returns rows that appear in both inputs, preserving min(count_left, count_right) duplicates."
private static List<TableRow> IntersectAll(List<TableRow> left, List<TableRow> right)
{
var result = new List<TableRow>();
var rightRemaining = new List<TableRow>(right);
foreach (var lr in left)
{
    var idx = rightRemaining.FindIndex(rr => RowEquals(lr, rr));
    if (idx >= 0)
    {
        result.Add(lr);
        rightRemaining.RemoveAt(idx);
    }
}
return result;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
//   "ORDER BY, LIMIT, and OFFSET after a set operation apply to the entire result, not just the last SELECT."
private InMemoryBigQueryResult ApplySetOpOrderByLimitOffset(InMemoryBigQueryResult result, SetOperationStatement setOp)
{
    if (setOp.OrderBy is null && setOp.Limit is null && setOp.Offset is null)
        return result;

    var rows = result.Rows;

    if (setOp.OrderBy is { Count: > 0 })
    {
        var schema = result.Schema;
        var dicts = rows.Select(r => ParseTypedRow(RowToDict(r, schema), schema)).ToList();
        var contexts = dicts.Select(d => new RowContext(d, null)).ToList();
        contexts = OrderBy(contexts, setOp.OrderBy.ToList());
        var fieldNames = schema.Fields.Select(f => f.Name).ToHashSet();
        rows = contexts.Select(c =>
        {
            var proj = new Dictionary<string, object?>();
            foreach (var kv in c.Fields)
                if (fieldNames.Contains(kv.Key))
                    proj[kv.Key] = kv.Value;
            return DictToTableRow(proj);
        }).ToList();
    }

    if (setOp.Offset.HasValue)
        rows = rows.Skip(setOp.Offset.Value).ToList();

    if (setOp.Limit.HasValue)
        rows = rows.Take(setOp.Limit.Value).ToList();

    return new InMemoryBigQueryResult(result.Schema, rows);
}

#endregion
#region DML execution

private InMemoryBigQueryResult ExecuteInsertValues(InsertValuesStatement insert)
{
var (iDs, iTbl) = SplitTableName(insert.TableName);
var table = ResolveTable(iDs, iTbl);
long count = 0;
lock (table.RowLock)
{
foreach (var rowValues in insert.Rows)
{
var fields = new Dictionary<string, object?>();
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
//   When column list is omitted, values map to columns in schema order.
var columns = insert.Columns ?? table.Schema.Fields.Select(f => f.Name).ToList();
for (int i = 0; i < columns.Count && i < rowValues.Count; i++)
{
fields[columns[i]] = Evaluate(rowValues[i],
new RowContext(new Dictionary<string, object?>(), null));
}
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
//   Unspecified columns are assigned NULL.
foreach (var schemaField in table.Schema.Fields)
{
	if (!fields.ContainsKey(schemaField.Name))
		fields[schemaField.Name] = null;
}
table.Rows.Add(new InMemoryRow(fields));
count++;
}
}
return EmptyResult(count);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#with_clause
//   "You can use a WITH clause with INSERT, UPDATE, DELETE, and MERGE statements."
private InMemoryBigQueryResult ExecuteWithDml(WithDmlStatement withDml)
{
// Resolve CTEs using a temporary SelectStatement wrapper
var tempSel = new SelectStatement(false, Array.Empty<SelectItem>(), null, null, null, null, null, null, null, withDml.Ctes.ToList(), null, false);
var cteResults = ResolveCtes(tempSel);
var prevCteResults = _activeCteResults;
_activeCteResults = cteResults;
try
{
return ExecuteStatement(withDml.DmlBody);
}
finally
{
_activeCteResults = prevCteResults;
}
}
private InMemoryBigQueryResult ExecuteInsertSelect(InsertSelectStatement insert)
{
var (iDs, iTbl) = SplitTableName(insert.TableName);
var table = ResolveTable(iDs, iTbl);
var result = ExecuteStatement(insert.Query);
long count = 0;
lock (table.RowLock)
{
foreach (var row in result.Rows)
{
var dict = RowToDict(row, result.Schema);
var fields = new Dictionary<string, object?>();
if (insert.Columns is not null && insert.Columns.Count > 0)
{
	for (int i = 0; i < insert.Columns.Count && i < result.Schema.Fields.Count; i++)
		fields[insert.Columns[i]] = dict.GetValueOrDefault(result.Schema.Fields[i].Name);
}
else
{
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
	// No explicit columns - map positionally using target table schema
	var targetFields = table.Schema?.Fields;
	if (targetFields != null && targetFields.Count >= result.Schema.Fields.Count)
	{
		for (int i = 0; i < result.Schema.Fields.Count; i++)
			fields[targetFields[i].Name] = dict.GetValueOrDefault(result.Schema.Fields[i].Name);
	}
	else
	{
		foreach (var f in result.Schema.Fields)
			fields[f.Name] = dict.GetValueOrDefault(f.Name);
	}
}
table.Rows.Add(new InMemoryRow(fields));
count++;
}
}
return EmptyResult(count);
}

private InMemoryBigQueryResult ExecuteUpdate(UpdateStatement update)
{
var (uDs, uTbl) = SplitTableName(update.TableName);
var table = ResolveTable(uDs, uTbl);
var alias = update.Alias ?? uTbl;
long count = 0;
lock (table.RowLock)
{
if (update.From is not null)
{
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#update_with_joins
    var sourceRows = ResolveFrom(update.From);
    foreach (var row in table.Rows)
    {
        var targetFields = new Dictionary<string, object?>(row.Fields);
        foreach (var kv in row.Fields) targetFields[alias + "." + kv.Key] = kv.Value;
        foreach (var srcCtx in sourceRows)
        {
            var combined = new Dictionary<string, object?>(targetFields);
            foreach (var kv in srcCtx.Fields) combined[kv.Key] = kv.Value;
            var ctx = new RowContext(combined, alias);
            if (update.Where is null || IsTruthy(Evaluate(update.Where, ctx)))
            {
                foreach (var (col, expr) in update.Assignments)
                {
                    var actualCol = col.Contains('.') ? col[(col.IndexOf('.') + 1)..] : col;
                    row.Fields[actualCol] = Evaluate(expr, ctx);
                }
                count++;
                break;
            }
        }
    }
}
else
{
foreach (var row in table.Rows)
{
var fields = new Dictionary<string, object?>(row.Fields);
foreach (var kv in row.Fields) fields[alias + "." + kv.Key] = kv.Value;
var ctx = new RowContext(fields, alias);
if (update.Where is null || IsTruthy(Evaluate(update.Where, ctx)))
{
foreach (var (col, expr) in update.Assignments)
{
var actualCol = col.Contains('.') ? col[(col.IndexOf('.') + 1)..] : col;
row.Fields[actualCol] = Evaluate(expr, ctx);
}
count++;
}
}
}
}
return EmptyResult(count);
}

private InMemoryBigQueryResult ExecuteDelete(DeleteStatement delete)
{
var (dDs, dTbl) = SplitTableName(delete.TableName);
var table = ResolveTable(dDs, dTbl);
var alias = delete.Alias ?? dTbl;
long count = 0;
lock (table.RowLock)
{
var toRemove = new List<InMemoryRow>();
foreach (var row in table.Rows)
{
var fields = new Dictionary<string, object?>(row.Fields);
foreach (var kv in row.Fields) fields[alias + "." + kv.Key] = kv.Value;
var ctx = new RowContext(fields, alias);
if (delete.Where is null || IsTruthy(Evaluate(delete.Where, ctx)))
{
toRemove.Add(row);
count++;
}
}
foreach (var row in toRemove) table.Rows.Remove(row);
}
return EmptyResult(count);
}

private InMemoryBigQueryResult ExecuteMerge(MergeStatement merge)
{
var (mDs, mTbl) = SplitTableName(merge.TargetTable);
var target = ResolveTable(mDs, mTbl);
var sourceRows = ResolveFrom(merge.Source);
var targetAlias = merge.TargetAlias ?? mTbl;
var sourceAlias = merge.SourceAlias ?? "source";
long count = 0;

lock (target.RowLock)
{
var matchedTargetRows = new HashSet<InMemoryRow>();

foreach (var srcCtx in sourceRows)
{
var srcFields = new Dictionary<string, object?>(srcCtx.Fields);
foreach (var kv in srcCtx.Fields) srcFields[sourceAlias + "." + kv.Key] = kv.Value;

InMemoryRow? matchedRow = null;
foreach (var targetRow in target.Rows)
{
var tgtFields = new Dictionary<string, object?>(targetRow.Fields);
foreach (var kv in targetRow.Fields) tgtFields[targetAlias + "." + kv.Key] = kv.Value;
var merged = new Dictionary<string, object?>(tgtFields);
foreach (var kv in srcFields) merged.TryAdd(kv.Key, kv.Value);
var ctx = new RowContext(merged, null);
if (IsTruthy(Evaluate(merge.On, ctx)))
{
matchedRow = targetRow;
matchedTargetRows.Add(targetRow);
break;
}
}

if (matchedRow is not null)
{
foreach (var clause in merge.WhenClauses.OfType<MergeWhenMatched>())
{
var tgtFields = new Dictionary<string, object?>(matchedRow.Fields);
foreach (var kv in matchedRow.Fields) tgtFields[targetAlias + "." + kv.Key] = kv.Value;
var merged = new Dictionary<string, object?>(tgtFields);
foreach (var kv in srcFields) merged.TryAdd(kv.Key, kv.Value);
var ctx = new RowContext(merged, null);

if (clause.And is not null && !IsTruthy(Evaluate(clause.And, ctx)))
continue;

if (clause.IsDelete)
{
target.Rows.Remove(matchedRow);
}
else if (clause.Updates is not null)
{
foreach (var (col, expr) in clause.Updates)
matchedRow.Fields[col] = Evaluate(expr, ctx);
}
count++;
break;
}
}
else
{
foreach (var clause in merge.WhenClauses.OfType<MergeWhenNotMatched>())
{
var ctx = new RowContext(srcFields, null);
if (clause.And is not null && !IsTruthy(Evaluate(clause.And, ctx)))
continue;

if (clause.Columns is not null && clause.Values is not null)
{
var fields = new Dictionary<string, object?>();
for (int i = 0; i < clause.Columns.Count && i < clause.Values.Count; i++)
fields[clause.Columns[i]] = Evaluate(clause.Values[i], ctx);
target.Rows.Add(new InMemoryRow(fields));
}
count++;
break;
}
}
}
}
return EmptyResult(count);
}

#endregion
#region DDL execution

private InMemoryBigQueryResult ExecuteCreateTable(CreateTableStatement create)
{
var (parsedDs, tblId) = SplitTableName(create.TableName);
var dsId = create.DatasetId ?? parsedDs ?? _defaultDatasetId
    ?? throw new InvalidOperationException("No dataset specified for CREATE TABLE");
if (!_store.Datasets.TryGetValue(dsId, out var ds))
{
    ds = new InMemoryDataset(dsId);
    _store.Datasets[dsId] = ds;
}

if (ds.Tables.ContainsKey(tblId))
{
    if (create.OrReplace)
        ds.Tables.TryRemove(tblId, out _);
    else if (create.IfNotExists)
        return EmptyResult();
    else
        throw new InvalidOperationException($"Table '{tblId}' already exists");
}

var schema = new TableSchema
{
    Fields = create.Columns.Select(c => new TableFieldSchema
    {
        Name = c.Name,
        Type = c.Type.ToUpperInvariant(),
        Mode = c.Mode == "REQUIRED" ? "REQUIRED" : "NULLABLE"
    }).ToList()
};

var table = new InMemoryTable(dsId, tblId, schema);
ds.Tables[tblId] = table;
return EmptyResult();
}

private InMemoryBigQueryResult ExecuteCreateTableAsSelect(CreateTableAsSelectStatement ctas)
{
var result = ExecuteSelect(ctas.Query);
var (parsedDs, tblId) = SplitTableName(ctas.TableName);
var dsId = ctas.DatasetId ?? parsedDs ?? _defaultDatasetId
    ?? throw new InvalidOperationException("No dataset specified for CTAS");
if (!_store.Datasets.TryGetValue(dsId, out var ds))
{
    ds = new InMemoryDataset(dsId);
    _store.Datasets[dsId] = ds;
}

var table = new InMemoryTable(dsId, tblId, result.Schema);
foreach (var row in result.Rows)
{
    var dict = RowToDict(row, result.Schema);
    table.Rows.Add(new InMemoryRow(dict));
}
ds.Tables[tblId] = table;
return EmptyResult();
}

private InMemoryBigQueryResult ExecuteDropTable(DropTableStatement drop)
{
var (parsedDs, tblId) = SplitTableName(drop.TableName);
var dsId = drop.DatasetId ?? parsedDs ?? _defaultDatasetId;
if (dsId is null || !_store.Datasets.TryGetValue(dsId, out var ds))
{
    if (drop.IfExists) return EmptyResult();
    throw new InvalidOperationException("Dataset not found");
}
if (!ds.Tables.TryRemove(tblId, out _) && !drop.IfExists)
    throw new InvalidOperationException($"Table '{tblId}' not found");
return EmptyResult();
}

private InMemoryBigQueryResult ExecuteAlterTable(AlterTableStatement alter)
{
var (parsedDs, tblId) = SplitTableName(alter.TableName);
var table = ResolveTable(alter.DatasetId ?? parsedDs, tblId);
switch (alter.Action)
{
    case AddColumnAction add:
        table.Schema.Fields.Add(new TableFieldSchema
        {
            Name = add.Name,
            Type = add.Type.ToUpperInvariant(),
            Mode = "NULLABLE"
        });
        break;
    case DropColumnAction drop:
        var field = table.Schema.Fields.FirstOrDefault(f =>
            f.Name.Equals(drop.Name, StringComparison.OrdinalIgnoreCase));
        if (field is not null)
{
    table.Schema.Fields.Remove(field);
    // Also remove the column data from existing rows
    foreach (var row in table.Rows)
        row.Fields.Remove(drop.Name);
}
        break;
    case RenameTableAction rename:
        var dsId = alter.DatasetId ?? parsedDs ?? _defaultDatasetId;
        if (dsId is not null && _store.Datasets.TryGetValue(dsId, out var ds))
        {
            if (ds.Tables.TryRemove(tblId, out var t))
            {
                t.TableId = rename.NewName;
                ds.Tables[rename.NewName] = t;
            }
        }
        break;
    // Phase 27: ALTER COLUMN variants
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_set_data_type
    case AlterColumnSetDataTypeAction setType:
    {
        var f = table.Schema.Fields.First(f =>
            f.Name.Equals(setType.ColumnName, StringComparison.OrdinalIgnoreCase));
        f.Type = setType.NewType;
        break;
    }
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_set_default
    case AlterColumnSetDefaultAction setDefault:
    {
        var f = table.Schema.Fields.First(f =>
            f.Name.Equals(setDefault.ColumnName, StringComparison.OrdinalIgnoreCase));
        f.DefaultValueExpression = setDefault.DefaultExpression;
        break;
    }
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_drop_default
    case AlterColumnDropDefaultAction dropDefault:
    {
        var f = table.Schema.Fields.First(f =>
            f.Name.Equals(dropDefault.ColumnName, StringComparison.OrdinalIgnoreCase));
        f.DefaultValueExpression = null;
        break;
    }
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_drop_not_null
    case AlterColumnDropNotNullAction dropNotNull:
    {
        var f = table.Schema.Fields.First(f =>
            f.Name.Equals(dropNotNull.ColumnName, StringComparison.OrdinalIgnoreCase));
        f.Mode = "NULLABLE";
        break;
    }
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_table_set_options
    case SetOptionsAction:
        // No-op â€” metadata-only in an in-memory emulator
        break;
}
return EmptyResult();
}

private InMemoryBigQueryResult ExecuteCreateView(CreateViewStatement view)
{
var (parsedDs, viewId) = SplitTableName(view.ViewName);
var dsId = view.DatasetId ?? parsedDs ?? _defaultDatasetId
    ?? throw new InvalidOperationException("No dataset for CREATE VIEW");
if (!_store.Datasets.TryGetValue(dsId, out var ds))
{
    ds = new InMemoryDataset(dsId);
    _store.Datasets[dsId] = ds;
}

var result = ExecuteSelect(view.Query);
var table = new InMemoryTable(dsId, viewId, result.Schema);
// Store the view query AST so it can be re-executed when the view is queried
// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ViewDefinition
table.ViewQuery = view.Query;
table.ViewDefinitionSql = view.ViewSql ?? "(view definition not available)";
if (view.OrReplace && ds.Tables.ContainsKey(viewId))
    ds.Tables[viewId] = table;
else
    ds.Tables[viewId] = table;
return EmptyResult();
}

private InMemoryBigQueryResult ExecuteDropView(DropViewStatement drop)
{
var (parsedDs, viewId) = SplitTableName(drop.ViewName);
var dsId = drop.DatasetId ?? parsedDs ?? _defaultDatasetId;
if (dsId is null || !_store.Datasets.TryGetValue(dsId, out var ds))
{
    if (drop.IfExists) return EmptyResult();
    throw new InvalidOperationException("Dataset not found");
}
if (!ds.Tables.TryRemove(viewId, out _) && !drop.IfExists)
    throw new InvalidOperationException($"View '{viewId}' not found");
return EmptyResult();
}

// Phase 27: TRUNCATE TABLE
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#truncate_table_statement
//   "Deletes all rows from the named table."
private InMemoryBigQueryResult ExecuteTruncateTable(TruncateTableStatement trunc)
{
var (parsedDs, tblId) = SplitTableName(trunc.TableName);
var table = ResolveTable(trunc.DatasetId ?? parsedDs, tblId);
lock (table.RowLock)
{
    table.Rows.Clear();
}
return EmptyResult();
}

// Phase 27: CREATE TABLE LIKE
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_like
//   "Creates a new table with the same schema as the source table."
private InMemoryBigQueryResult ExecuteCreateTableLike(CreateTableLikeStatement like)
{
var (parsedDs, tblId) = SplitTableName(like.TableName);
var dsId = like.DatasetId ?? parsedDs ?? _defaultDatasetId
    ?? throw new InvalidOperationException("No dataset for CREATE TABLE LIKE");
var (srcParsedDs, srcTblId) = SplitTableName(like.SourceTable);
var sourceTbl = ResolveTable(like.SourceDatasetId ?? srcParsedDs, srcTblId);
var newSchema = new TableSchema
{
    Fields = new List<TableFieldSchema>(
        sourceTbl.Schema.Fields.Select(f => new TableFieldSchema
        {
            Name = f.Name, Type = f.Type, Mode = f.Mode,
            DefaultValueExpression = f.DefaultValueExpression,
            Description = f.Description,
        }))
};
if (!_store.Datasets.TryGetValue(dsId, out var ds))
{
    ds = new InMemoryDataset(dsId);
    _store.Datasets[dsId] = ds;
}
ds.Tables[tblId] = new InMemoryTable(dsId, tblId, newSchema);
return EmptyResult();
}

// Phase 27: CREATE TABLE COPY / CLONE / SNAPSHOT
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_copy
//   "Creates a new table by copying the schema and data from the source table."
private InMemoryBigQueryResult ExecuteCreateTableCopy(CreateTableCopyStatement copy)
{
var (parsedDs, tblId) = SplitTableName(copy.TableName);
var dsId = copy.DatasetId ?? parsedDs ?? _defaultDatasetId
    ?? throw new InvalidOperationException("No dataset for CREATE TABLE COPY");
var (srcParsedDs, srcTblId) = SplitTableName(copy.SourceTable);
var sourceTbl = ResolveTable(copy.SourceDatasetId ?? srcParsedDs, srcTblId);
var newSchema = new TableSchema
{
    Fields = new List<TableFieldSchema>(
        sourceTbl.Schema.Fields.Select(f => new TableFieldSchema
        {
            Name = f.Name, Type = f.Type, Mode = f.Mode,
            DefaultValueExpression = f.DefaultValueExpression,
            Description = f.Description,
        }))
};
if (!_store.Datasets.TryGetValue(dsId, out var ds))
{
    ds = new InMemoryDataset(dsId);
    _store.Datasets[dsId] = ds;
}
var newTable = new InMemoryTable(dsId, tblId, newSchema);
lock (sourceTbl.RowLock)
{
    foreach (var row in sourceTbl.Rows)
        newTable.Rows.Add(new InMemoryRow(new Dictionary<string, object?>(row.Fields)));
}
ds.Tables[tblId] = newTable;
return EmptyResult();
}

// Phase 27: CREATE SCHEMA
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement
//   "Creates a new schema (dataset)."
private InMemoryBigQueryResult ExecuteCreateSchema(CreateSchemaStatement cs)
{
if (_store.Datasets.ContainsKey(cs.SchemaName))
{
    if (cs.IfNotExists) return EmptyResult();
    throw new InvalidOperationException($"Schema '{cs.SchemaName}' already exists");
}
_store.Datasets[cs.SchemaName] = new InMemoryDataset(cs.SchemaName);
return EmptyResult();
}

// Phase 27: DROP SCHEMA
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#drop_schema_statement
//   "Drops a schema (dataset)."
private InMemoryBigQueryResult ExecuteDropSchema(DropSchemaStatement dropSchema)
{
if (!_store.Datasets.TryRemove(dropSchema.SchemaName, out _))
{
    if (dropSchema.IfExists) return EmptyResult();
    throw new InvalidOperationException($"Schema '{dropSchema.SchemaName}' not found");
}
return EmptyResult();
}

#endregion
#region Helpers
private static (string? DatasetId, string TableId) SplitTableName(string tableName)
{
    var parts = tableName.Split('.');
    return parts.Length >= 2 ? (parts[^2], parts[^1]) : (null, parts[0]);
}

private object? EvaluateWithAggregates(SqlExpression expr, List<RowContext> groupRows)
{
    return expr switch
    {
        AggregateCall agg => EvaluateAggregate(agg, groupRows),
        ColumnRef col => EvaluateColumnRef(col, groupRows[0]),
        LiteralExpr lit => lit.Value,
        ParameterRef p => ResolveParameter(p.Name),
        BinaryExpr bin => EvaluateBinary(new BinaryExpr(
            new LiteralExpr(EvaluateWithAggregates(bin.Left, groupRows)), bin.Op,
            new LiteralExpr(EvaluateWithAggregates(bin.Right, groupRows))), groupRows[0]),
        UnaryExpr un => EvaluateUnary(new UnaryExpr(un.Op,
            new LiteralExpr(EvaluateWithAggregates(un.Operand, groupRows))), groupRows[0]),
        FunctionCall fn => EvaluateFunctionCall(new FunctionCall(fn.FunctionName,
            fn.Args.Select(a => (SqlExpression)new LiteralExpr(EvaluateWithAggregates(a, groupRows))).ToList()), groupRows[0]),
        CaseExpr ce => EvaluateCase(new CaseExpr(
            ce.Operand is not null ? new LiteralExpr(EvaluateWithAggregates(ce.Operand, groupRows)) : null,
            ce.Branches.Select(b => ((SqlExpression)new LiteralExpr(EvaluateWithAggregates(b.When, groupRows)),
                                     (SqlExpression)new LiteralExpr(EvaluateWithAggregates(b.Then, groupRows)))).ToList(),
            ce.Else is not null ? new LiteralExpr(EvaluateWithAggregates(ce.Else, groupRows)) : null), groupRows[0]),
        CastExpr cast => CastValue(EvaluateWithAggregates(cast.Expr, groupRows), cast.TargetType, cast.Safe),
        IsNullExpr isNull => EvaluateWithAggregates(isNull.Expr, groupRows) is null == !isNull.IsNot,
        IsBoolExpr isBool => EvaluateIsBool(isBool, groupRows[0]),
        ArraySubscriptExpr arrSub => EvaluateArraySubscript(
            new ArraySubscriptExpr(
                new LiteralExpr(EvaluateWithAggregates(arrSub.Array, groupRows)),
                arrSub.AccessMode,
                new LiteralExpr(EvaluateWithAggregates(arrSub.Index, groupRows))),
            groupRows[0]),
        BetweenExpr btw => EvaluateBetween(new BetweenExpr(
            new LiteralExpr(EvaluateWithAggregates(btw.Expr, groupRows)),
            new LiteralExpr(EvaluateWithAggregates(btw.Low, groupRows)),
            new LiteralExpr(EvaluateWithAggregates(btw.High, groupRows))), groupRows[0]),
        InExpr inE => EvaluateIn(new InExpr(
            new LiteralExpr(EvaluateWithAggregates(inE.Expr, groupRows)),
            inE.Values.Select(v => (SqlExpression)new LiteralExpr(EvaluateWithAggregates(v, groupRows))).ToList()), groupRows[0]),
        LikeExpr lk => EvaluateLike(new LikeExpr(
            new LiteralExpr(EvaluateWithAggregates(lk.Expr, groupRows)),
            new LiteralExpr(EvaluateWithAggregates(lk.Pattern, groupRows)),
            lk.IsNot), groupRows[0]),
        _ => Evaluate(expr, groupRows[0])
    };
}

private static bool ContainsPartitionRef(SqlExpression expr)
{
    return expr switch
    {
        ColumnRef col => col.ColumnName.Equals("_PARTITIONTIME", StringComparison.OrdinalIgnoreCase)
            || col.ColumnName.Equals("_PARTITIONDATE", StringComparison.OrdinalIgnoreCase),
        BinaryExpr bin => ContainsPartitionRef(bin.Left) || ContainsPartitionRef(bin.Right),
        UnaryExpr un => ContainsPartitionRef(un.Operand),
        FunctionCall fn => fn.Args.Any(ContainsPartitionRef),
        _ => false
    };
}


private InMemoryTable ResolveTable(string? datasetId, string tableId)
{
var dsId = datasetId ?? _defaultDatasetId;
if (dsId is null || !_store.Datasets.TryGetValue(dsId, out var ds))
throw new InvalidOperationException("Dataset '" + dsId + "' not found");
if (!ds.Tables.TryGetValue(tableId, out var table))
throw new InvalidOperationException("Table '" + tableId + "' not found");
return table;
}

private static InMemoryBigQueryResult EmptyResult(long affectedRows = 0)
{
var schema = new TableSchema
{
Fields = [new TableFieldSchema { Name = "affected_rows", Type = "INTEGER" }]
};
var rows = new List<TableRow>
{
new() { F = [new TableCell { V = affectedRows.ToString() }] }
};
return new InMemoryBigQueryResult(schema, rows) { DmlAffectedRows = affectedRows };
}

private static Dictionary<string, object?> RowToDict(TableRow row, TableSchema schema)
{
var dict = new Dictionary<string, object?>();
if (row.F is null) return dict;
for (int i = 0; i < schema.Fields.Count && i < row.F.Count; i++)
dict[schema.Fields[i].Name] = row.F[i]?.V;
return dict;
}

/// <summary>
/// Converts formatted string values in a row back to typed .NET values based on the schema.
/// Needed when subquery/CTE results are consumed by outer queries (formatted values â†’ typed values).
/// </summary>
private static Dictionary<string, object?> ParseTypedRow(Dictionary<string, object?> dict, TableSchema schema)
{
var typed = new Dictionary<string, object?>(dict.Count);
foreach (var field in schema.Fields)
{
	if (!dict.TryGetValue(field.Name, out var val) || val is null)
	{
		typed[field.Name] = null;
		continue;
	}
	typed[field.Name] = ParseTypedValue(val, field.Type);
}
return typed;
}

private static object? ParseTypedValue(object? val, string? type)
{
if (val is null) return null;
// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//   REPEATED fields are stored as lists — preserve them without string conversion.
if (val is IList<object?> list) return list;
var s = val.ToString();
if (s is null) return null;
return type?.ToUpperInvariant() switch
{
	"INTEGER" or "INT64" => long.TryParse(s, CultureInfo.InvariantCulture, out var l) ? l : val,
	"FLOAT" or "FLOAT64" => double.TryParse(s, CultureInfo.InvariantCulture, out var d) ? d : val,
	"BOOLEAN" or "BOOL" => s.Equals("true", StringComparison.OrdinalIgnoreCase) ? true
		: s.Equals("false", StringComparison.OrdinalIgnoreCase) ? false : val,
	"TIMESTAMP" => long.TryParse(s, CultureInfo.InvariantCulture, out var us)
		? DateTimeOffset.FromUnixTimeMilliseconds(us / 1000) : val,
	"DATE" => DateTime.TryParseExact(s, "yyyy-MM-dd", CultureInfo.InvariantCulture,
		System.Globalization.DateTimeStyles.None, out var dt) ? DateOnly.FromDateTime(dt) : val,
	_ => val
};
}

private static TableRow DictToTableRow(Dictionary<string, object?> dict)
{
return new TableRow
{
F = dict.Values.Select(v => new TableCell { V = FormatValue(v) }).ToList()
};
}

private static object? FormatValue(object? val)
{
return val switch
{
null => null,
bool b => b ? "true" : "false",
long l => l.ToString(),
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#floating_point_literals
//   "inf and -inf represent positive and negative infinity respectively."
double d when double.IsPositiveInfinity(d) => "Infinity",
double d when double.IsNegativeInfinity(d) => "-Infinity",
double d when double.IsNaN(d) => "NaN",
            double d => d == Math.Floor(d) ? ((long)d).ToString() : d.ToString(CultureInfo.InvariantCulture),
            // Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//   The BigQuery .NET SDK (v3.11.0) defaults to UseInt64Timestamp=true, which calls
//   long.Parse() on timestamp values. We must return epoch MICROSECONDS as an integer string.
//   SDK source: BigQueryResults.ConvertResponseRows â†’ BigQueryRow â†’ Int64TimestampConverter.
DateTimeOffset dto => (dto.ToUnixTimeMilliseconds() * 1000L).ToString(CultureInfo.InvariantCulture),
// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//   DATE values are returned as "yyyy-MM-dd" strings.
DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//   DATETIME values are returned as "yyyy-MM-ddTHH:mm:ss.FFFFFF" strings.
//   SDK source: BigQueryRow.DateTimeConverter uses DateTime.ParseExact with this format.
DateTime dt => dt.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//   TIME values are returned as "HH:mm:ss.FFFFFF" strings.
TimeSpan ts => ts.ToString(@"hh\:mm\:ss", CultureInfo.InvariantCulture),
byte[] bytes => Convert.ToBase64String(bytes),
// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//   REPEATED fields are represented as arrays of cell values.
//   Preserve list structure for CTE materialization while keeping ToString() output for SDK.
IList<object?> list => new FormattedList(list.Select(v => FormatValue(v)).ToList()),
RangeValue rv => $"[{FormatValue(rv.Start)}, {FormatValue(rv.End)})",
IDictionary<string, object?> dict => dict,
_ => val.ToString()
};
}

private List<RowContext> OrderBy(List<RowContext> rows, IReadOnlyList<OrderByItem> orderBy)
{
IOrderedEnumerable<RowContext>? ordered = null;
for (int i = 0; i < orderBy.Count; i++)
{
var item = orderBy[i];
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
//   Default: ASC -> NULLS FIRST, DESC -> NULLS LAST.
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
//   Default: ASC -> NULLS FIRST, DESC -> NULLS LAST.
bool nullsFirst = item.NullsFirst ?? !item.Descending;
// For LINQ OrderBy/OrderByDescending: null must compare as 'small' to go first in ASC,
// or 'large' to go first in DESC. XOR gives the correct polarity.
bool nullIsSmall = nullsFirst != item.Descending;
var comparer = new NullSafeComparer(nullIsSmall);
var idx = i;
if (idx == 0)
{
ordered = item.Descending
? rows.OrderByDescending(r => Evaluate(item.Expr, r), comparer)
: rows.OrderBy(r => Evaluate(item.Expr, r), comparer);
}
else
{
ordered = item.Descending
? ordered!.ThenByDescending(r => Evaluate(item.Expr, r), comparer)
: ordered!.ThenBy(r => Evaluate(item.Expr, r), comparer);
}
}
return ordered?.ToList() ?? rows;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
//   "An ordinal references the column position in the SELECT list."
private static IReadOnlyList<OrderByItem> ResolveOrderByOrdinals(IReadOnlyList<OrderByItem> orderBy, IReadOnlyList<SelectItem> columns)
{
    var resolved = new List<OrderByItem>();
    foreach (var item in orderBy)
    {
        if (item.Expr is LiteralExpr lit && lit.Value is long ordinal && ordinal >= 1 && ordinal <= columns.Count)
        {
            var col = columns[(int)ordinal - 1];
            var colName = col.Alias ?? DeriveColumnName(col.Expr);
            resolved.Add(new OrderByItem(new ColumnRef(null, colName), item.Descending));
        }
        else
        {
            resolved.Add(item);
        }
    }
    return resolved;
}

private static bool ContainsWindow(List<SelectItem> items)
=> items.Any(i => i.Expr is WindowFunction);

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
//   Window functions can appear in expressions like al * 100.0 / SUM(val) OVER ().
/// <summary>
/// Replaces aggregate expressions inside a WindowFunction's ORDER BY / PARTITION BY 
/// with ColumnRef nodes that can be resolved against the aggregated row context.
/// This is needed because window functions over grouped aggregates reference aggregated values.
/// </summary>
private static WindowFunction ResolveWindowAggregates(WindowFunction wf, IReadOnlyList<SelectItem> columns)
{
	// Build a map from aggregate expression structure to the column alias/name in the aggregated result
	var aggToName = new Dictionary<string, string>();
	foreach (var col in columns)
	{
		if (col.Expr is AggregateCall || (col.Expr is not WindowFunction && ContainsAggregate(col.Expr)))
		{
			var name = col.Alias ?? DeriveColumnName(col.Expr);
			// Use a string representation of the expression as key
			aggToName[col.Expr.ToString()!] = name;
		}
	}
	if (aggToName.Count == 0) return wf;

	var resolvedOrderBy = wf.OrderBy?.Select(o => new OrderByItem(
		ResolveAggExpr(o.Expr, aggToName), o.Descending, o.NullsFirst)).ToList();
	var resolvedPartition = wf.PartitionBy?.Select(p => ResolveAggExpr(p, aggToName)).ToList();
	return wf with { OrderBy = resolvedOrderBy, PartitionBy = resolvedPartition };
}

private static SqlExpression ResolveAggExpr(SqlExpression expr, Dictionary<string, string> aggToName)
{
	var key = expr.ToString()!;
	if (aggToName.TryGetValue(key, out var name))
		return new ColumnRef(null, name);
	return expr switch
	{
		BinaryExpr bin => new BinaryExpr(ResolveAggExpr(bin.Left, aggToName), bin.Op, ResolveAggExpr(bin.Right, aggToName)),
		_ => expr
	};
}

private static bool ContainsWindowFunction(SqlExpression expr)
{
    return expr switch
    {
        WindowFunction => true,
        BinaryExpr bin => ContainsWindowFunction(bin.Left) || ContainsWindowFunction(bin.Right),
        UnaryExpr un => ContainsWindowFunction(un.Operand),
        FunctionCall fn => fn.Args.Any(ContainsWindowFunction),
        CastExpr cast => ContainsWindowFunction(cast.Expr),
ArraySubscriptExpr sub => ContainsWindowFunction(sub.Array) || ContainsWindowFunction(sub.Index),
        CaseExpr caseExpr => (caseExpr.Branches?.Any(b => ContainsWindowFunction(b.When) || ContainsWindowFunction(b.Then)) ?? false) || (caseExpr.Else != null && ContainsWindowFunction(caseExpr.Else)),
        _ => false
    };
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
//   "An alias references the alias of a column in the SELECT list."
private static IReadOnlyList<OrderByItem> ResolveOrderByAliases(IReadOnlyList<OrderByItem> orderBy, IReadOnlyList<SelectItem> columns)
{
    var aliasMap = new Dictionary<string, SqlExpression>(StringComparer.OrdinalIgnoreCase);
    foreach (var col in columns)
    {
        if (col.Alias is not null)
            aliasMap[col.Alias] = col.Expr;
    }
    if (aliasMap.Count == 0) return orderBy;
    var resolved = new List<OrderByItem>();
    foreach (var item in orderBy)
    {
        if (item.Expr is ColumnRef cr && cr.TableAlias is null && aliasMap.TryGetValue(cr.ColumnName, out var expr) && expr is not WindowFunction && !ContainsAggregate(expr))
            resolved.Add(new OrderByItem(expr, item.Descending));
        else
            resolved.Add(item);
    }
    return resolved;
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
//   "ORDER BY can reference aggregate expressions that appear in the SELECT list."
private static IReadOnlyList<OrderByItem> ResolveOrderByExpressions(IReadOnlyList<OrderByItem> orderBy, IReadOnlyList<SelectItem> columns)
{
    var resolved = new List<OrderByItem>();
    foreach (var item in orderBy)
    {
        if (item.Expr is AggregateCall || item.Expr is FunctionCall)
        {
            // Find matching SELECT column by expression toString equality
            var match = columns.FirstOrDefault(c => ExpressionsEqual(c.Expr, item.Expr));
            if (match is not null)
            {
                var colName = match.Alias ?? DeriveColumnName(match.Expr);
                resolved.Add(new OrderByItem(new ColumnRef(null, colName), item.Descending));
                continue;
            }
        }
        resolved.Add(item);
    }
    return resolved;
}

private static bool ExpressionsEqual(SqlExpression a, SqlExpression b)
{
    if (a is null || b is null) return ReferenceEquals(a, b);
    if (a.GetType() != b.GetType()) return false;
    return a switch
    {
        AggregateCall ac when b is AggregateCall bc =>
            ac.FunctionName.Equals(bc.FunctionName, StringComparison.OrdinalIgnoreCase) &&
            ac.Distinct == bc.Distinct &&
            ExpressionsEqual(ac.Arg!, bc.Arg!),
        FunctionCall fc when b is FunctionCall gc =>
            fc.FunctionName.Equals(gc.FunctionName, StringComparison.OrdinalIgnoreCase) &&
            fc.Args.Count == gc.Args.Count &&
            fc.Args.Zip(gc.Args).All(p => ExpressionsEqual(p.First, p.Second)),
        ColumnRef cr when b is ColumnRef dr =>
            cr.ColumnName.Equals(dr.ColumnName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(cr.TableAlias, dr.TableAlias, StringComparison.OrdinalIgnoreCase),
        LiteralExpr le when b is LiteralExpr me => Equals(le.Value, me.Value),
        _ => false
    };
}
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause
//   "GROUP BY can reference SELECT list aliases."
private static IReadOnlyList<SqlExpression> ResolveGroupByAliases(IReadOnlyList<SqlExpression> groupBy, IReadOnlyList<SelectItem> columns)
{
    var aliasMap = new Dictionary<string, SqlExpression>(StringComparer.OrdinalIgnoreCase);
    foreach (var col in columns)
    {
        if (col.Alias is not null)
            aliasMap[col.Alias] = col.Expr;
    }
    var resolved = new List<SqlExpression>();
    foreach (var expr in groupBy)
    {
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause
        //   "GROUP BY can also reference column ordinals (1-based)."
        if (expr is LiteralExpr lit && lit.Value is long ordinal && ordinal >= 1 && ordinal <= columns.Count)
        {
            resolved.Add(columns[(int)ordinal - 1].Expr);
        }
        else if (expr is ColumnRef cr && cr.TableAlias is null && aliasMap.TryGetValue(cr.ColumnName, out var selectExpr))
            resolved.Add(selectExpr);
        else
            resolved.Add(expr);
    }
    return resolved;
}

private static bool IsTruthy(object? val)
{
return val switch
{
null => false,
bool b => b,
long l => l != 0,
double d => d != 0.0,
string s => !string.IsNullOrEmpty(s),
_ => true
};
}

private static int CompareValues(object? a, object? b) => CompareRaw(a, b);

private static int CompareRaw(object? a, object? b)
{
if (a is null && b is null) return 0;
if (a is null) return -1;
if (b is null) return 1;

// long <-> double coercion
if (a is long la && b is double db) return ((double)la).CompareTo(db);
if (a is double da && b is long lb) return da.CompareTo((double)lb);

// long <-> string
if (a is long la2 && b is string sb && long.TryParse(sb, out var parsed))
return la2.CompareTo(parsed);
if (a is string sa && b is long lb2 && long.TryParse(sa, out var parsed2))
return parsed2.CompareTo(lb2);

// double <-> string (scalar subqueries return formatted strings)
if (a is double da2 && b is string sb3 && double.TryParse(sb3, System.Globalization.NumberStyles.Any, CultureInfo.InvariantCulture, out var pd))
return da2.CompareTo(pd);
if (a is string sa3 && b is double db2 && double.TryParse(sa3, System.Globalization.NumberStyles.Any, CultureInfo.InvariantCulture, out var pd2))
return pd2.CompareTo(db2);

// string <-> string
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#comparison_operators
//   "Comparisons on STRING values are case sensitive."
if (a is string sa2 && b is string sb2)
return string.Compare(sa2, sb2, StringComparison.Ordinal);

// DateTimeOffset
if (a is DateTimeOffset dtoa && b is DateTimeOffset dtob) return dtoa.CompareTo(dtob);
if (a is DateTime dta && b is DateTime dtb) return dta.CompareTo(dtb);
if (a is DateOnly doa && b is DateOnly dob) return doa.CompareTo(dob);
if (a is DateOnly doa2 && b is DateTime dtb2) return doa2.ToDateTime(TimeOnly.MinValue).CompareTo(dtb2);
if (a is DateTime dta2 && b is DateOnly dob2) return dta2.CompareTo(dob2.ToDateTime(TimeOnly.MinValue));

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#comparison_operators
//   "Comparison operators work across numeric types."
if ((a is long || a is double) && (b is long || b is double)) return ToDouble(a).CompareTo(ToDouble(b));
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
//   Struct comparison is field-by-field in declaration order.
if (a is IDictionary<string, object?> da3 && b is IDictionary<string, object?> db3)
{
foreach (var key in da3.Keys)
{
if (!db3.ContainsKey(key)) return 1;
var cmp = CompareRaw(da3[key], db3[key]);
if (cmp != 0) return cmp;
}
return da3.Count.CompareTo(db3.Count);
}
if (a is IComparable ca) return ca.CompareTo(b);
return string.Compare(a.ToString(), b.ToString(), StringComparison.OrdinalIgnoreCase);
}

private static object? ArithmeticOp(object? left, object? right,
Func<long, long, long> longOp, Func<double, double, double> doubleOp)
{
if (left is null || right is null) return null;
if (left is long la && right is long lb) return longOp(la, lb);
return doubleOp(ToDouble(left), ToDouble(right));
}

private static object? Negate(object? val)
{
return val switch
{
long l => -l,
double d => -d,
null => null,
_ => -ToDouble(val)
};
}

private static object? SumValues(List<object?> values)
{
var nonNull = values.Where(v => v is not null).ToList();
if (nonNull.Count == 0) return null;
if (nonNull.All(v => v is long))
return nonNull.Cast<long>().Sum();
return nonNull.Select(v => ToDouble(v)).Sum();
}

private static object? AvgValues(List<object?> values)
{
var nonNull = values.Where(v => v is not null).ToList();
if (nonNull.Count == 0) return null;
return nonNull.Select(v => ToDouble(v)).Average();
}

private static bool ContainsAggregate(SqlExpression expr)
{
return expr switch
{
AggregateCall => true,
FunctionCall fn => fn.Args.Any(ContainsAggregate),
BinaryExpr bin => ContainsAggregate(bin.Left) || ContainsAggregate(bin.Right),
UnaryExpr un => ContainsAggregate(un.Operand),
CaseExpr ce => ce.Branches.Any(w => ContainsAggregate(w.When) || ContainsAggregate(w.Then))
|| (ce.Else is not null && ContainsAggregate(ce.Else)),
CastExpr c => ContainsAggregate(c.Expr),
ArraySubscriptExpr sub => ContainsAggregate(sub.Array) || ContainsAggregate(sub.Index),
FieldAccessExpr fa => ContainsAggregate(fa.Object),
_ => false
};
}

private static string DeriveColumnName(SqlExpression expr)
{
return expr switch
{
ColumnRef col => col.ColumnName,
FunctionCall fnc => fnc.FunctionName,
AggregateCall agc => agc.FunctionName,
WindowFunction wf => DeriveColumnName(wf.Function),
CastExpr c => DeriveColumnName(c.Expr),
ArraySubscriptExpr _ => "f0_",
LiteralExpr lit => "f0_",
_ => "f0_"
};
}

private static string InferType(object? val)
{
return val switch
{
null => "STRING",
long => "INTEGER",
double => "FLOAT",
bool => "BOOLEAN",
DateTimeOffset => "TIMESTAMP",
DateOnly => "DATE",
DateTime => "DATETIME",
TimeSpan => "TIME",
byte[] => "BYTES",
IList<object?> => "RECORD",
IDictionary<string, object?> => "RECORD",
_ => "STRING"
};
}

private static double ToDouble(object? val)
{
return val switch
{
double d => d,
long l => l,
string s => double.Parse(s, CultureInfo.InvariantCulture),
null => 0.0,
_ => Convert.ToDouble(val, CultureInfo.InvariantCulture)
};
}

private static long ToLong(object? val)
{
return val switch
{
long l => l,
double d => (long)d,
string s => long.Parse(s, CultureInfo.InvariantCulture),
null => 0L,
_ => Convert.ToInt64(val, CultureInfo.InvariantCulture)
};
}

private static string? ConvertToString(object? val)
{
return val switch
{
null => null,
bool b => b ? "true" : "false",
            // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
            //   CAST(FLOAT64 AS STRING) returns "inf", "-inf", "NaN" for special values.
            double d when double.IsPositiveInfinity(d) => "inf",
            double d when double.IsNegativeInfinity(d) => "-inf",
            double d when double.IsNaN(d) => "NaN",
            // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions
            //   BigQuery FLOAT64 whole numbers format with ".0" suffix (e.g., ROUND(2.5) → "3.0")
            double d when d == Math.Floor(d) && d >= long.MinValue && d <= long.MaxValue => $"{(long)d}.0",
            double d => d.ToString(CultureInfo.InvariantCulture),
DateTimeOffset dto => FormatTimestampAsString(dto),
DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
DateTime dt => dt.ToString("yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture),
TimeSpan ts => ts.ToString(@"hh\:mm\:ss", CultureInfo.InvariantCulture),
_ => val.ToString()
};
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_string
//   CAST(TIMESTAMP AS STRING) format: "yyyy-MM-dd HH:mm:ss[.SSSSSS]+HH[:MM]"
//   Fractional seconds are omitted if zero; trailing zeros in fractional part are trimmed.
//   Timezone offset uses short form "+HH" when minutes are zero.
private static string FormatTimestampAsString(DateTimeOffset dto)
{
    var sb = new System.Text.StringBuilder();
    sb.Append(dto.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
    long ticks = dto.TimeOfDay.Ticks % TimeSpan.TicksPerSecond;
    if (ticks > 0)
    {
        var microseconds = ticks / (TimeSpan.TicksPerMillisecond / 1000);
        var frac = microseconds.ToString("D6").TrimEnd('0');
        sb.Append('.').Append(frac);
    }
    var offset = dto.Offset;
    sb.Append(offset < TimeSpan.Zero ? '-' : '+');
    sb.Append(Math.Abs(offset.Hours).ToString("D2"));
    if (offset.Minutes != 0)
        sb.Append(':').Append(Math.Abs(offset.Minutes).ToString("D2"));
    return sb.ToString();
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
//  Vector distance helpers
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

private object? EvaluateVectorDistanceFunction(string name, IReadOnlyList<SqlExpression> args, RowContext row)
{
    var v1 = Evaluate(args[0], row);
    var v2 = Evaluate(args[1], row);

    if (v1 is null || v2 is null) return null;

    var vec1 = ToDoubleArray(v1);
    var vec2 = ToDoubleArray(v2);

    if (vec1 is null || vec2 is null || vec1.Length != vec2.Length || vec1.Length == 0)
        return null;

    var result = name switch
    {
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#cosine_distance
        //   "Computes the cosine distance between two vectors."
        //   cosine_distance = 1 - (dot(a,b) / (|a| * |b|))
        "COSINE_DISTANCE" or "APPROX_COSINE_DISTANCE" => CosineDistance(vec1, vec2),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#euclidean_distance
        //   "Computes the Euclidean distance between two vectors."
        //   euclidean_distance = sqrt(sum((a[i] - b[i])^2))
        "EUCLIDEAN_DISTANCE" or "APPROX_EUCLIDEAN_DISTANCE" => (object)EuclideanDistance(vec1, vec2),
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/search_functions#vector_search
        //   DOT_PRODUCT computes the inner product of two vectors: sum(v1[i] * v2[i]).
        "DOT_PRODUCT" or "APPROX_DOT_PRODUCT" => (object)DotProduct(vec1, vec2),
        _ => null,
    };

    // Guard against Infinity/NaN
    if (result is double d && (double.IsInfinity(d) || double.IsNaN(d)))
        return null;

    return result;
}

private static object? CosineDistance(double[] a, double[] b)
{
    double dot = 0, magA = 0, magB = 0;
    for (var i = 0; i < a.Length; i++)
    {
        dot += a[i] * b[i];
        magA += a[i] * a[i];
        magB += b[i] * b[i];
    }
    var denominator = Math.Sqrt(magA) * Math.Sqrt(magB);
    // Zero vector â†’ return null (real BigQuery produces an error)
    if (denominator == 0) return null;
    return 1.0 - (dot / denominator);
}

private static double EuclideanDistance(double[] a, double[] b)
{
    double sum = 0;
    for (var i = 0; i < a.Length; i++)
    {
        var diff = a[i] - b[i];
        sum += diff * diff;
    }
    return Math.Sqrt(sum);
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/search_functions#vector_search
//   DOT_PRODUCT computes the inner product (dot product) of two vectors.
private static double DotProduct(double[] a, double[] b)
{
    double sum = 0;
    for (var i = 0; i < a.Length; i++)
        sum += a[i] * b[i];
    return sum;
}

private static double[]? ToDoubleArray(object? value)
{
    if (value is null) return null;
    if (value is double[] dArr) return dArr;
    if (value is List<object?> list)
    {
        var arr = new double[list.Count];
        for (var i = 0; i < list.Count; i++)
        {
            if (list[i] is null) return null;
            arr[i] = Convert.ToDouble(list[i]);
        }
        return arr;
    }
    if (value is IEnumerable<object> enumerable)
    {
        var items = enumerable.ToList();
        var arr = new double[items.Count];
        for (var i = 0; i < items.Count; i++)
        {
            arr[i] = Convert.ToDouble(items[i]);
        }
        return arr;
    }
    return null;
}

// â”€â”€ String function helpers (Phase 24) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

private object? EvaluateByteLength(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    if (val is byte[] bytes) return (long)bytes.Length;
    return (long)System.Text.Encoding.UTF8.GetByteCount(val.ToString()!);
}

private object? EvaluateUnicode(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row)?.ToString();
    if (val is null) return null;
    if (val.Length == 0) return 0L;
    return (long)char.ConvertToUtf32(val, 0);
}

private object? EvaluateInitcap(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row)?.ToString();
    if (val is null) return null;
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#initcap
    //   Default delimiters: whitespace and many punctuation characters.
    var delimiters = args.Count > 1
        ? Evaluate(args[1], row)?.ToString() ?? ""
        : " \t\n\r[](){}/ |\\<>!?@\"^#$&~_,.:;*%+-";
    var result = new char[val.Length];
    var newWord = true;
    for (var i = 0; i < val.Length; i++)
    {
        if (delimiters.Contains(val[i]))
        {
            result[i] = val[i];
            newWord = true;
        }
        else if (newWord)
        {
            result[i] = char.ToUpper(val[i]);
            newWord = false;
        }
        else
        {
            result[i] = char.ToLower(val[i]);
        }
    }
    return new string(result);
}

private object? EvaluateTranslate(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var expr = Evaluate(args[0], row)?.ToString();
    var source = Evaluate(args[1], row)?.ToString();
    var target = Evaluate(args[2], row)?.ToString();
    if (expr is null || source is null || target is null) return null;
    var sb = new System.Text.StringBuilder(expr.Length);
    foreach (var c in expr)
    {
        var idx = source.IndexOf(c);
        if (idx < 0) sb.Append(c);
        else if (idx < target.Length) sb.Append(target[idx]);
        // else: character in source but not in target â†’ omitted
    }
    return sb.ToString();
}

private object? EvaluateSoundex(IReadOnlyList<SqlExpression> args, RowContext row)
{
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#soundex
    //   Standard American Soundex algorithm: first letter + 3 digits.
    var val = Evaluate(args[0], row)?.ToString();
    if (val is null) return null;
    var letters = val.Where(c => (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')).ToList();
    if (letters.Count == 0) return "";
    //                          ABCDEFGHIJKLMNOPQRSTUVWXYZ
    const string soundexMap = "01230120022455012623010202";
    var code = new char[4];
    code[0] = char.ToUpper(letters[0]);
    var lastDigit = soundexMap[char.ToUpper(letters[0]) - 'A'];
    var idx = 1;
    for (var i = 1; i < letters.Count && idx < 4; i++)
    {
        var upper = char.ToUpper(letters[i]);
        var digit = soundexMap[upper - 'A'];
        if (digit != '0' && digit != lastDigit)
        {
            code[idx++] = digit;
        }
        // H and W are transparent â€” they don't reset the last digit,
        // so adjacent consonants with the same code separated only by H/W
        // are treated as one (e.g. Ashcraft â†’ A261, not A226).
        if (upper != 'H' && upper != 'W')
        {
            lastDigit = digit;
        }
    }
    while (idx < 4) code[idx++] = '0';
    return new string(code);
}

private object? EvaluateRegexpExtractAll(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var str = Evaluate(args[0], row)?.ToString();
    var pattern = Evaluate(args[1], row)?.ToString();
    if (str is null || pattern is null) return null;
    var matches = Regex.Matches(str, pattern);
    var result = new List<object?>();
    foreach (Match m in matches)
    {
        result.Add(m.Groups.Count > 1 ? m.Groups[1].Value : m.Value);
    }
    return result;
}

private object? EvaluateNormalize(IReadOnlyList<SqlExpression> args, RowContext row, bool caseFold)
{
    var val = Evaluate(args[0], row)?.ToString();
    if (val is null) return null;
    var form = System.Text.NormalizationForm.FormC; // default NFC
    if (args.Count > 1)
    {
        var mode = Evaluate(args[1], row)?.ToString()?.ToUpperInvariant();
        form = mode switch
        {
            "NFC" => System.Text.NormalizationForm.FormC,
            "NFD" => System.Text.NormalizationForm.FormD,
            "NFKC" => System.Text.NormalizationForm.FormKC,
            "NFKD" => System.Text.NormalizationForm.FormKD,
            _ => System.Text.NormalizationForm.FormC,
        };
    }
    var normalized = val.Normalize(form);
    return caseFold ? normalized.ToLowerInvariant() : normalized;
}

private object? EvaluateToCodePoints(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    if (val is byte[] bytes)
        return bytes.Select(b => (object?)(long)b).ToList();
    var str = val.ToString()!;
    var result = new List<object?>();
    for (var i = 0; i < str.Length; i++)
    {
        var cp = char.ConvertToUtf32(str, i);
        result.Add((long)cp);
        if (char.IsHighSurrogate(str[i])) i++; // skip low surrogate
    }
    return result;
}

private object? EvaluateCodePointsToString(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    // Single code point (CHR alias)
    if (val is not List<object?> list)
        return ((char)Convert.ToInt64(val)).ToString();
    var sb = new System.Text.StringBuilder(list.Count);
    foreach (var item in list)
    {
        if (item is null) return null;
        var cp = Convert.ToInt32(item);
        sb.Append(char.ConvertFromUtf32(cp));
    }
    return sb.ToString();
}

private object? EvaluateCodePointsToBytes(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    if (val is not List<object?> list) return null;
    var bytes = new byte[list.Count];
    for (var i = 0; i < list.Count; i++)
    {
        if (list[i] is null) return null;
        bytes[i] = (byte)Convert.ToInt32(list[i]);
    }
    return bytes;
}

private object? EvaluateSafeConvertBytesToString(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    if (val is byte[] bytes)
    {
        // Replace invalid UTF-8 with U+FFFD
        var encoding = new System.Text.UTF8Encoding(false, false);
        return encoding.GetString(bytes);
    }
    return val.ToString();
}

private object? EvaluateToBase32(IReadOnlyList<SqlExpression> args, RowContext row)
{
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#to_base32
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    byte[] bytes;
    if (val is byte[] b) bytes = b;
    else bytes = System.Text.Encoding.UTF8.GetBytes(val.ToString()!);
    return Base32Encode(bytes);
}

private object? EvaluateFromBase32(IReadOnlyList<SqlExpression> args, RowContext row)
{
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#from_base32
    var val = Evaluate(args[0], row)?.ToString();
    if (val is null) return null;
    return Base32Decode(val);
}

private static string Base32Encode(byte[] data)
{
    const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    var sb = new System.Text.StringBuilder((data.Length * 8 + 4) / 5);
    int buffer = 0, bitsLeft = 0;
    foreach (var b in data)
    {
        buffer = (buffer << 8) | b;
        bitsLeft += 8;
        while (bitsLeft >= 5)
        {
            sb.Append(alphabet[(buffer >> (bitsLeft - 5)) & 0x1F]);
            bitsLeft -= 5;
        }
    }
    if (bitsLeft > 0)
        sb.Append(alphabet[(buffer << (5 - bitsLeft)) & 0x1F]);
    while (sb.Length % 8 != 0)
        sb.Append('=');
    return sb.ToString();
}

private static byte[] Base32Decode(string input)
{
    const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    input = input.TrimEnd('=').ToUpperInvariant();
    var output = new List<byte>();
    int buffer = 0, bitsLeft = 0;
    foreach (var c in input)
    {
        var val = alphabet.IndexOf(c);
        if (val < 0) continue;
        buffer = (buffer << 5) | val;
        bitsLeft += 5;
        if (bitsLeft >= 8)
        {
            output.Add((byte)(buffer >> (bitsLeft - 8)));
            bitsLeft -= 8;
        }
    }
    return output.ToArray();
}

// â”€â”€ Math function helpers (Phase 24) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

private object? EvaluateUnaryMathOrNull(IReadOnlyList<SqlExpression> args, RowContext row, Func<double, double> fn)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    return fn(ToDouble(val));
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#log
//   "Returns NULL if X or Y is NULL."
private object? EvaluateLogOrNull(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    if (args.Count > 1)
    {
        var baseVal = Evaluate(args[1], row);
        if (baseVal is null) return null;
        return Math.Log(ToDouble(val), ToDouble(baseVal));
    }
    return Math.Log(ToDouble(val));
}

private object? EvaluateBinaryMathOrNull(IReadOnlyList<SqlExpression> args, RowContext row, Func<double, double, double> fn)
{
    var v1 = Evaluate(args[0], row);
    var v2 = Evaluate(args[1], row);
    if (v1 is null || v2 is null) return null;
    return fn(ToDouble(v1), ToDouble(v2));
}

private object? EvaluateIsInf(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    return double.IsInfinity(ToDouble(val));
}

private object? EvaluateIsNan(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    return double.IsNaN(ToDouble(val));
}

private object? EvaluateSafeAdd(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var left = Evaluate(args[0], row);
    var right = Evaluate(args[1], row);
    if (left is null || right is null) return null;
    if (left is long la && right is long lb)
    {
        try { return checked(la + lb); }
        catch (OverflowException) { return null; }
    }
    return ToDouble(left) + ToDouble(right);
}

private object? EvaluateSafeSubtract(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var left = Evaluate(args[0], row);
    var right = Evaluate(args[1], row);
    if (left is null || right is null) return null;
    if (left is long la && right is long lb)
    {
        try { return checked(la - lb); }
        catch (OverflowException) { return null; }
    }
    return ToDouble(left) - ToDouble(right);
}

private object? EvaluateSafeMultiply(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var left = Evaluate(args[0], row);
    var right = Evaluate(args[1], row);
    if (left is null || right is null) return null;
    if (left is long la && right is long lb)
    {
        try { return checked(la * lb); }
        catch (OverflowException) { return null; }
    }
    return ToDouble(left) * ToDouble(right);
}

private object? EvaluateSafeNegate(IReadOnlyList<SqlExpression> args, RowContext row)
{
    var val = Evaluate(args[0], row);
    if (val is null) return null;
    if (val is long l)
    {
        try { return checked(-l); }
        catch (OverflowException) { return null; }
    }
    return -ToDouble(val);
}

private object? EvaluateRangeBucket(IReadOnlyList<SqlExpression> args, RowContext row)
{
    // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#range_bucket
    var point = Evaluate(args[0], row);
    if (point is null) return null;
    var arrVal = Evaluate(args[1], row);
    if (arrVal is not List<object?> boundaries || boundaries.Count == 0) return 0L;
    var pointD = ToDouble(point);
    if (double.IsNaN(pointD)) return null;
    long pos = 0;
    foreach (var b in boundaries)
    {
        if (b is null) return null;
        if (pointD >= ToDouble(b)) pos++;
        else break;
    }
    return pos;
}

#endregion
}

internal class RowContext
{
public Dictionary<string, object?> Fields { get; }
public string? Alias { get; }
public RowContext(Dictionary<string, object?> fields, string? alias)
{
Fields = fields;
Alias = alias;
}
}

internal class NullSafeComparer : IComparer<object?>
{
public static readonly NullSafeComparer Instance = new(nullIsSmall: true);
private readonly bool _nullIsSmall;
public NullSafeComparer(bool nullIsSmall = true) { _nullIsSmall = nullIsSmall; }
public int Compare(object? x, object? y)
{
if (x is null && y is null) return 0;
if (x is null) return _nullIsSmall ? -1 : 1;
if (y is null) return _nullIsSmall ? 1 : -1;
if (x is long la && y is double db) return ((double)la).CompareTo(db);
if (x is double da && y is long lb) return da.CompareTo((double)lb);
if (x is IDictionary<string, object?> dx && y is IDictionary<string, object?> dy) return CompareStructs(dx, dy);
if (x is IComparable cx) return cx.CompareTo(y);
return string.Compare(x.ToString(), y.ToString(), StringComparison.Ordinal);
}
private static int CompareStructs(IDictionary<string, object?> a, IDictionary<string, object?> b)
{
foreach (var key in a.Keys.OrderBy(k => k))
{
if (!b.ContainsKey(key)) return 1;
var cmp = Instance.Compare(a[key], b[key]);
if (cmp != 0) return cmp;
}
return a.Count.CompareTo(b.Count);
}
}

/// <summary>
/// A list wrapper that preserves array structure for CTE round-trip while providing
/// comma-separated ToString() output for SDK consumption.
/// </summary>
internal class FormattedList : List<object?>, IList<object?>
{
public FormattedList(List<object?> items) : base(items) { }
public override string ToString() => string.Join(", ", this.Select(v => v?.ToString() ?? "NULL"));
}
