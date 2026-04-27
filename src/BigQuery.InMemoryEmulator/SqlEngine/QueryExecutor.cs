#pragma warning disable CS8600, CS8602, CS8604

using System.Globalization;
using System.Text.RegularExpressions;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.InMemoryEmulator.SqlEngine;

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
//   Core query execution engine evaluates parsed SQL AST against in-memory data.

internal record InMemoryBigQueryResult(TableSchema Schema, List<TableRow> Rows);

internal class QueryExecutor
{
private readonly InMemoryDataStore _store;
private readonly string? _defaultDatasetId;
private IList<QueryParameter>? _parameters;

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
var stmt = SqlParser.ParseSql(sql);
return ExecuteStatement(stmt);
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
CreateTableStatement ct => ExecuteCreateTable(ct),
CreateTableAsSelectStatement ctas => ExecuteCreateTableAsSelect(ctas),
DropTableStatement dt => ExecuteDropTable(dt),
AlterTableStatement alt => ExecuteAlterTable(alt),
CreateViewStatement cv => ExecuteCreateView(cv),
DropViewStatement dv => ExecuteDropView(dv),
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
{
cteResults ??= new Dictionary<string, (TableSchema, List<Dictionary<string, object?>>)>(StringComparer.OrdinalIgnoreCase);
foreach (var cte in sel.Ctes)
{
    if (cte.RecursiveBody is not null)
    {
        // Recursive CTE: iterate until no new rows produced
        // Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with-recursive
        //   The base term runs first, then the recursive term iterates.
        var baseResult = ExecuteSelect(cte.Body, cteResults);
        var allRows = baseResult.Rows.Select(r => RowToDict(r, baseResult.Schema)).ToList();
        var currentRows = new List<Dictionary<string, object?>>(allRows);
        var recSchema = baseResult.Schema;
        const int maxIterations = 500;
        for (int iter = 0; iter < maxIterations && currentRows.Count > 0; iter++)
        {
            // Make current rows visible as the CTE
            cteResults[cte.Name] = (recSchema, currentRows);
            var recResult = ExecuteSelect(cte.RecursiveBody, cteResults);
            var newRows = recResult.Rows.Select(r => RowToDict(r, recSchema)).ToList();
            if (newRows.Count == 0) break;
            allRows.AddRange(newRows);
            currentRows = newRows;
        }
        cteResults[cte.Name] = (recSchema, allRows);
    }
    else
    {
        var cteResult = ExecuteSelect(cte.Body, cteResults);
        var cteRows = cteResult.Rows.Select(r => RowToDict(r, cteResult.Schema)).ToList();
        cteResults[cte.Name] = (cteResult.Schema, cteRows);
    }
}
}

// FROM
List<RowContext> rows;
if (sel.From is not null)
rows = ResolveFrom(sel.From, cteResults);
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
bool hasWindow = sel.Columns.Any(c => c.Expr is WindowFunction);

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
    var d = RowToDict(r, schema);
    if (i < rows.Count)
        foreach (var kv in rows[i].Fields)
            d.TryAdd(kv.Key, kv.Value);
    return d;
}).ToList();
var contexts = dicts.Select(d => new RowContext(d, null)).ToList();
contexts = OrderBy(contexts, sel.OrderBy);
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

private InMemoryBigQueryResult ExecuteGroupBy(SelectStatement sel, List<RowContext> rows)
{
var groupExprs = sel.GroupBy ?? [];

// Check for ROLLUP
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#rollup
var rollupExpr = groupExprs.OfType<RollupExpr>().FirstOrDefault();
if (rollupExpr is not null)
    return ExecuteRollup(sel, rows, rollupExpr);

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
var value = EvaluateWithAggregates(col.Expr, groupRows);
dict[name] = value;
if (schema is null)
fields.Add(new TableFieldSchema { Name = name, Type = InferType(value) });
}

schema ??= new TableSchema { Fields = fields };
resultRows.Add(dict);
}

schema ??= new TableSchema { Fields = sel.Columns.Select(c =>
new TableFieldSchema { Name = c.Alias ?? DeriveColumnName(c.Expr), Type = "STRING" }).ToList() };

if (sel.OrderBy is { Count: > 0 })
if (sel.OrderBy is { Count: > 0 })
{
var ctx2 = resultRows.Select(d => new RowContext(d, null)).ToList();
ctx2 = OrderBy(ctx2, sel.OrderBy);
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
        ctx2 = OrderBy(ctx2, sel.OrderBy);
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
	return ResolveInformationSchema(tableName, alias, isDs ?? _defaultDatasetId);
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

private List<RowContext> ResolveInformationSchema(string tableName, string alias, string? datasetId = null)
{
var dsId = datasetId ?? _defaultDatasetId;
if (dsId is null || !_store.Datasets.TryGetValue(dsId, out var ds)) return [];

if (tableName.EndsWith("TABLES", StringComparison.OrdinalIgnoreCase))
{
return ds.Tables.Values.Select(t => new RowContext(new Dictionary<string, object?>
{
["table_catalog"] = _store.ProjectId,
["table_schema"] = dsId,
["table_name"] = t.TableId,
["table_type"] = "BASE TABLE",
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
["data_type"] = f.Type,
["is_nullable"] = f.Mode != "REQUIRED" ? "YES" : "NO",
}, alias));
}
}
return rows;
}
if (tableName.EndsWith("SCHEMATA", StringComparison.OrdinalIgnoreCase))
{
return _store.Datasets.Keys.Select(name => new RowContext(new Dictionary<string, object?>
{
["catalog_name"] = _store.ProjectId,
["schema_name"] = name,
}, alias)).ToList();
}
return [];
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
var right = ResolveFrom(join.Right, cteResults);
return join.Type switch
{
JoinType.Cross => CrossJoin(left, right),
JoinType.Inner => InnerJoin(left, right, join.On!),
JoinType.Left => LeftJoin(left, right, join.On!),
JoinType.Right => RightJoin(left, right, join.On!),
JoinType.Full => FullJoin(left, right, join.On!),
_ => throw new NotSupportedException("Unsupported join type: " + join.Type)
};
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
var result = ExecuteSelect(sub.Subquery);
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

private List<RowContext> ResolveUnnest(UnnestClause unnest)
{
var alias = unnest.Alias ?? "unnest";
var value = Evaluate(unnest.Expr, new RowContext(new Dictionary<string, object?>(), null));
if (value is IEnumerable<object?> list)
return list.Select(item => new RowContext(
new Dictionary<string, object?> { [alias] = item, [alias + "." + alias] = item }, alias)).ToList();
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
foreach (var kv in expanded) cells[kv.Key] = kv.Value;
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

return (new TableSchema { Fields = schemaFields }, resultRows);
}

private (TableSchema Schema, List<TableRow> Rows) ProjectWithWindows(
SelectStatement stmt, List<RowContext> rows)
{
var schemaFields = new List<TableFieldSchema>();
var resultRows = new List<TableRow>();

foreach (var row in rows)
{
var cells = new Dictionary<string, object?>();
foreach (var item in stmt.Columns)
{
if (item.Expr is StarExpr star)
{
var expanded = ExpandStar(row, star.TableAlias);
foreach (var kv in expanded) cells[kv.Key] = kv.Value;
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

// Window aggregate functions
if (wf.Function is AggregateCall agg)
return EvaluateAggregate(agg, partition);

return null;
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
BetweenExpr btw => EvaluateBetween(btw, row),
InExpr inExpr => EvaluateIn(inExpr, row),
InSubqueryExpr inSub => EvaluateInSubquery(inSub, row),
LikeExpr like => EvaluateLike(like, row),
CastExpr cast => EvaluateCast(cast, row),
CaseExpr caseExpr => EvaluateCase(caseExpr, row),
ScalarSubquery sub => EvaluateScalarSubquery(sub),
ExistsExpr exists => EvaluateExists(exists),
ArraySubquery arraySub => EvaluateArraySubquery(arraySub),
WindowFunction wf => throw new InvalidOperationException("Window function in non-window context"),
StarExpr => throw new InvalidOperationException("Star expression in non-projection context"),
VariableRef v => _parameters?.FirstOrDefault(p => p.Name == v.Name)?.ParameterValue?.Value,
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
return null;
}

private object? EvaluateBinary(BinaryExpr bin, RowContext row)
{
// Short-circuit AND/OR
if (bin.Op == BinaryOp.And)
{
var l = Evaluate(bin.Left, row);
if (!IsTruthy(l)) return false;
return IsTruthy(Evaluate(bin.Right, row));
}
if (bin.Op == BinaryOp.Or)
{
var l = Evaluate(bin.Left, row);
if (IsTruthy(l)) return true;
return IsTruthy(Evaluate(bin.Right, row));
}

var left = Evaluate(bin.Left, row);
var right = Evaluate(bin.Right, row);

return bin.Op switch
{
BinaryOp.Eq => left is null || right is null ? (object?)(left is null && right is null) : CompareRaw(left, right) == 0,
BinaryOp.Neq => left is null || right is null ? (object?)(left is not null || right is not null) : CompareRaw(left, right) != 0,
BinaryOp.Lt => left is null || right is null ? null : CompareRaw(left, right) < 0,
BinaryOp.Lte => left is null || right is null ? null : CompareRaw(left, right) <= 0,
BinaryOp.Gt => left is null || right is null ? null : CompareRaw(left, right) > 0,
BinaryOp.Gte => left is null || right is null ? null : CompareRaw(left, right) >= 0,
BinaryOp.Add => ArithmeticOp(left, right, (a, b) => a + b, (a, b) => a + b),
BinaryOp.Sub => ArithmeticOp(left, right, (a, b) => a - b, (a, b) => a - b),
BinaryOp.Mul => ArithmeticOp(left, right, (a, b) => a * b, (a, b) => a * b),
BinaryOp.Div => ArithmeticOp(left, right, (a, b) => b == 0 ? throw new DivideByZeroException() : a / b,
(a, b) => b == 0.0 ? throw new DivideByZeroException() : a / b),
BinaryOp.Mod => ArithmeticOp(left, right, (a, b) => a % b, (a, b) => a % b),
BinaryOp.Concat => (left?.ToString() ?? "") + (right?.ToString() ?? ""),
BinaryOp.And => IsTruthy(left) && IsTruthy(right),
BinaryOp.Or => IsTruthy(left) || IsTruthy(right),
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
_ => throw new NotSupportedException("Unsupported unary operator: " + un.Op)
};
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
var found = inExpr.Values.Any(v => Equals(val, Evaluate(v, row)));
return found;
}

private object? EvaluateInSubquery(InSubqueryExpr inSub, RowContext row)
{
var val = Evaluate(inSub.Expr, row);
var result = ExecuteSelect(inSub.Subquery);
var values = result.Rows.Select(r => r.F?[0]?.V).ToList();
var found = values.Any(v => CompareRaw(val, v) == 0);
return found;
}

private object? EvaluateLike(LikeExpr like, RowContext row)
{
var val = Evaluate(like.Expr, row)?.ToString();
var pattern = Evaluate(like.Pattern, row)?.ToString();
if (val is null || pattern is null) return null;
// Convert SQL LIKE to regex
var regex = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
.Replace("%", ".*").Replace("_", ".") + "$";
var result = System.Text.RegularExpressions.Regex.IsMatch(val, regex, RegexOptions.IgnoreCase);
return like.IsNot ? !result : result;
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
double d => (long)d,
bool b => b ? 1L : 0L,
string s => long.Parse(s, CultureInfo.InvariantCulture),
_ => Convert.ToInt64(val, CultureInfo.InvariantCulture)
},
"FLOAT64" or "FLOAT" or "NUMERIC" or "BIGNUMERIC" or "DECIMAL" => val switch
{
double d => d,
long l => (double)l,
string s => double.Parse(s, CultureInfo.InvariantCulture),
_ => Convert.ToDouble(val, CultureInfo.InvariantCulture)
},
"STRING" => ConvertToString(val),
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
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#timestamp_literals
//   BigQuery accepts "UTC" as a timezone suffix, but .NET ParseExact doesn't.
//   Normalize "UTC" to "+00:00" before parsing.
string s => DateTimeOffset.Parse(NormalizeTimestampString(s), CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal),
_ => DateTimeOffset.Parse(NormalizeTimestampString(val.ToString()!), CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal)
},
"DATE" => val switch
{
DateTime dt => dt,
DateTimeOffset dto => dto.DateTime,
string s => DateTime.Parse(s, CultureInfo.InvariantCulture),
_ => DateTime.Parse(val.ToString()!, CultureInfo.InvariantCulture)
},
"BYTES" => val switch
{
byte[] b => b,
string s => Convert.FromBase64String(s),
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
if (Equals(operand, whenVal)) return Evaluate(then, row);
}
}
else
{
foreach (var (when, then) in caseExpr.Branches)
if (IsTruthy(Evaluate(when, row))) return Evaluate(then, row);
}
return caseExpr.Else is not null ? Evaluate(caseExpr.Else, row) : null;
}

private object? EvaluateScalarSubquery(ScalarSubquery sub)
{
var result = ExecuteSelect(sub.Subquery);
if (result.Rows.Count == 0) return null;
return result.Rows[0].F?[0]?.V;
}

private object? EvaluateExists(ExistsExpr exists)
{
var result = ExecuteSelect(exists.Subquery);
return result.Rows.Count > 0;
}

private object? EvaluateArraySubquery(ArraySubquery arraySub)
{
var result = ExecuteSelect(arraySub.Subquery);
return result.Rows.Select(r => r.F?[0]?.V).Cast<object?>().ToList();
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
"TRIM" => Evaluate(args[0], row)?.ToString()?.Trim(),
"LTRIM" => Evaluate(args[0], row)?.ToString()?.TrimStart(),
"RTRIM" => Evaluate(args[0], row)?.ToString()?.TrimEnd(),
"REVERSE" => Evaluate(args[0], row)?.ToString() is string s ? new string(s.Reverse().ToArray()) : null,
"REPLACE" => EvaluateReplace(args, row),
"SUBSTR" or "SUBSTRING" => EvaluateSubstr(args, row),
"CONCAT" => string.Concat(args.Select(a => Evaluate(a, row)?.ToString() ?? "")),
"STARTS_WITH" => Evaluate(args[0], row)?.ToString()?.StartsWith(Evaluate(args[1], row)?.ToString() ?? "", StringComparison.Ordinal),
"ENDS_WITH" => Evaluate(args[0], row)?.ToString()?.EndsWith(Evaluate(args[1], row)?.ToString() ?? "", StringComparison.Ordinal),
"CONTAINS_SUBSTR" => Evaluate(args[0], row)?.ToString()?.Contains(Evaluate(args[1], row)?.ToString() ?? "", StringComparison.OrdinalIgnoreCase),
"STRPOS" or "INSTR" => EvaluateStrPos(args, row),
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
"ASCII" => Evaluate(args[0], row)?.ToString() is string sa && sa.Length > 0 ? (long)sa[0] : null,
"CHR" or "CODE_POINTS_TO_STRING" => Evaluate(args[0], row) is object cv ? ((char)Convert.ToInt64(cv)).ToString() : null,
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
"SQRT" => Math.Sqrt(ToDouble(Evaluate(args[0], row))),
"LOG" => args.Count > 1 ? Math.Log(ToDouble(Evaluate(args[0], row)), ToDouble(Evaluate(args[1], row)))
: Math.Log(ToDouble(Evaluate(args[0], row))),
"LOG10" => Math.Log10(ToDouble(Evaluate(args[0], row))),
"LN" => Math.Log(ToDouble(Evaluate(args[0], row))),
"EXP" => Math.Exp(ToDouble(Evaluate(args[0], row))),
"GREATEST" => EvaluateGreatest(args, row),
"LEAST" => EvaluateLeast(args, row),
"IEEE_DIVIDE" => EvaluateIeeeDivide(args, row),
"DIV" => EvaluateIntDiv(args, row),
"RAND" => new Random().NextDouble(),
"GENERATE_UUID" => Guid.NewGuid().ToString(),

// Date/Time functions
"CURRENT_TIMESTAMP" or "NOW" => DateTimeOffset.UtcNow,
"CURRENT_DATE" => DateTime.UtcNow.Date,
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

// Conversion functions
"CAST" => args.Count >= 2 ? CastValue(Evaluate(args[0], row), Evaluate(args[1], row)?.ToString() ?? "STRING", false) : null,
"SAFE_CAST" => args.Count >= 2 ? CastValue(Evaluate(args[0], row), Evaluate(args[1], row)?.ToString() ?? "STRING", true) : null,
"TO_JSON_STRING" => System.Text.Json.JsonSerializer.Serialize(Evaluate(args[0], row)),

// Type functions
"STRUCT" => args.Select(a => Evaluate(a, row)).ToList(),
"ARRAY" => args.Select(a => Evaluate(a, row)).ToList(),

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

// Net functions (normalized from NET.HOST → NET_HOST etc.)
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions
"NET_HOST" => EvaluateNetHost(args, row),
"NET_PUBLIC_SUFFIX" => EvaluateNetPublicSuffix(args, row),
"NET_REG_DOMAIN" => EvaluateNetRegDomain(args, row),
"NET_IP_FROM_STRING" => EvaluateNetIpFromString(args, row),
"NET_IP_TO_STRING" => EvaluateNetIpToString(args, row),
"NET_IP_NET_MASK" => EvaluateNetIpNetMask(args, row),

// HLL++ approximate counting (exact in-memory implementation, normalized from HLL_COUNT.INIT → HLL_COUNT_INIT)
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_count_functions
"HLL_COUNT_INIT" or "HLL_COUNT_MERGE" or "HLL_COUNT_MERGE_PARTIAL"
or "HLL_COUNT_EXTRACT" => EvaluateHllCount(name, args, row),

// UDF
// Geography functions
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions
"ST_GEOGPOINT" or "ST_GEOGFROMTEXT" or "ST_GEOGFROMWKT" or "ST_GEOGFROMGEOJSON"
or "ST_ASTEXT" or "ST_ASGEOJSON" or "ST_X" or "ST_Y"
or "ST_DISTANCE" or "ST_DWITHIN" or "ST_CONTAINS" or "ST_WITHIN"
or "ST_INTERSECTS" or "ST_DISJOINT" or "ST_EQUALS"
or "ST_AREA" or "ST_LENGTH" or "ST_PERIMETER"
or "ST_NUMPOINTS" or "ST_NPOINTS" or "ST_DIMENSION" or "ST_ISEMPTY" or "ST_GEOMETRYTYPE"
or "ST_MAKELINE" or "ST_CENTROID"
    => EvaluateGeographyFunction(name, args, row),
_ => EvaluateUdf(name, args, row)
};
}

private object? EvaluateReplace(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
var from = Evaluate(args[1], row)?.ToString();
var to = Evaluate(args[2], row)?.ToString();
if (str is null || from is null || to is null) return null;
return str.Replace(from, to);
}

private object? EvaluateSubstr(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString();
if (str is null) return null;
var pos = (int)ToLong(Evaluate(args[1], row));
// BigQuery SUBSTR is 1-based
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

private object? EvaluateLpad(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString() ?? "";
var len = (int)ToLong(Evaluate(args[1], row));
var pad = args.Count > 2 ? Evaluate(args[2], row)?.ToString() ?? " " : " ";
if (str.Length >= len) return str[..len];
while (str.Length < len) str = pad + str;
return str[..len];
}

private object? EvaluateRpad(IReadOnlyList<SqlExpression> args, RowContext row)
{
var str = Evaluate(args[0], row)?.ToString() ?? "";
var len = (int)ToLong(Evaluate(args[1], row));
var pad = args.Count > 2 ? Evaluate(args[2], row)?.ToString() ?? " " : " ";
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
return str.Split(delimiter).Cast<object?>().ToList();
}

private object? EvaluateFormat(IReadOnlyList<SqlExpression> args, RowContext row)
{
var fmt = Evaluate(args[0], row)?.ToString();
if (fmt is null) return null;
var fmtArgs = args.Skip(1).Select(a => Evaluate(a, row)).ToArray();
// Simple %s/%d/%f replacement
var result = fmt;
int argIdx = 0;
result = System.Text.RegularExpressions.Regex.Replace(result, @"%[sdftTe]", m =>
{
if (argIdx < fmtArgs.Length)
return ConvertToString(fmtArgs[argIdx++]) ?? "NULL";
return m.Value;
});
return result;
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

private object? EvaluateGreatest(IReadOnlyList<SqlExpression> args, RowContext row)
{
object? best = null;
foreach (var arg in args)
{
var val = Evaluate(arg, row);
if (val is null) continue;
if (best is null || CompareRaw(val, best) > 0) best = val;
}
return best;
}

private object? EvaluateLeast(IReadOnlyList<SqlExpression> args, RowContext row)
{
object? best = null;
foreach (var arg in args)
{
var val = Evaluate(arg, row);
if (val is null) continue;
if (best is null || CompareRaw(val, best) < 0) best = val;
}
return best;
}

private object? EvaluateIeeeDivide(IReadOnlyList<SqlExpression> args, RowContext row)
{
var a = ToDouble(Evaluate(args[0], row));
var b = ToDouble(Evaluate(args[1], row));
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
DateTime dt => dt,
DateTimeOffset dto => dto.Date,
string s => DateTime.Parse(s, CultureInfo.InvariantCulture),
_ => DateTime.Parse(val?.ToString() ?? "", CultureInfo.InvariantCulture)
};
}
return new DateTime((int)ToLong(Evaluate(args[0], row)),
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
"DATE" => (object)dto.Date,
"TIME" => dto.TimeOfDay.ToString(),
"ISOWEEK" => (long)CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dto.DateTime, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday),
_ => throw new NotSupportedException("Unsupported EXTRACT part: " + partName)
};
}

private object? EvaluateDateAdd(IReadOnlyList<SqlExpression> args, RowContext row)
{
var date = ToDateTime(Evaluate(args[0], row));
var interval = ToLong(Evaluate(args[1], row));
var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
return AddToPart(date, interval, part);
}

private object? EvaluateDateSub(IReadOnlyList<SqlExpression> args, RowContext row)
{
var date = ToDateTime(Evaluate(args[0], row));
var interval = ToLong(Evaluate(args[1], row));
var part = Evaluate(args[2], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
return AddToPart(date, -interval, part);
}

private object? EvaluateDateDiff(IReadOnlyList<SqlExpression> args, RowContext row)
{
var date1 = ToDateTime(Evaluate(args[0], row));
var date2 = ToDateTime(Evaluate(args[1], row));
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
var ts = ToDateTimeOffset(Evaluate(args[0], row));
var interval = ToLong(Evaluate(args[1], row));
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
var ts = ToDateTimeOffset(Evaluate(args[0], row));
var interval = ToLong(Evaluate(args[1], row));
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
var ts1 = ToDateTimeOffset(Evaluate(args[0], row));
var ts2 = ToDateTimeOffset(Evaluate(args[1], row));
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
var date = ToDateTime(Evaluate(args[0], row));
var part = Evaluate(args[1], row)?.ToString()?.ToUpperInvariant() ?? "DAY";
return part switch
{
"YEAR" => new DateTime(date.Year, 1, 1),
"MONTH" => new DateTime(date.Year, date.Month, 1),
"QUARTER" => new DateTime(date.Year, ((date.Month - 1) / 3) * 3 + 1, 1),
"WEEK" => date.AddDays(-(int)date.DayOfWeek),
"DAY" => date.Date,
_ => date.Date
};
}

private object? EvaluateTimestampTrunc(IReadOnlyList<SqlExpression> args, RowContext row)
{
var ts = ToDateTimeOffset(Evaluate(args[0], row));
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
var format = Evaluate(args[0], row)?.ToString() ?? "";
var ts = ToDateTimeOffset(Evaluate(args[1], row));
return FormatTimestamp(ts, format);
}

private object? EvaluateParseTimestamp(IReadOnlyList<SqlExpression> args, RowContext row)
{
var format = Evaluate(args[0], row)?.ToString() ?? "";
var str = Evaluate(args[1], row)?.ToString() ?? "";
return ParseTimestamp(str, format);
}

private object? EvaluateFormatDate(IReadOnlyList<SqlExpression> args, RowContext row)
{
var format = Evaluate(args[0], row)?.ToString() ?? "";
var date = ToDateTime(Evaluate(args[1], row));
return FormatTimestamp(new DateTimeOffset(date, TimeSpan.Zero), format);
}

private object? EvaluateParseDate(IReadOnlyList<SqlExpression> args, RowContext row)
{
var format = Evaluate(args[0], row)?.ToString() ?? "";
var str = Evaluate(args[1], row)?.ToString() ?? "";
return ParseTimestamp(str, format).DateTime;
}

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
string s => DateTime.Parse(s, CultureInfo.InvariantCulture),
_ => DateTime.Parse(val?.ToString() ?? "", CultureInfo.InvariantCulture)
};
}

private static string FormatTimestamp(DateTimeOffset ts, string format)
{
// Convert BigQuery format specifiers to .NET
var netFormat = format
.Replace("%Y", "yyyy").Replace("%m", "MM").Replace("%d", "dd")
.Replace("%H", "HH").Replace("%M", "mm").Replace("%S", "ss")
.Replace("%F", "yyyy-MM-dd").Replace("%T", "HH:mm:ss")
.Replace("%Z", "zzz").Replace("%E4Y", "yyyy")
.Replace("%b", "MMM").Replace("%B", "MMMM")
.Replace("%j", "DDD").Replace("%A", "dddd").Replace("%a", "ddd");
return ts.ToString(netFormat, CultureInfo.InvariantCulture);
}

private static DateTimeOffset ParseTimestamp(string str, string format)
{
var netFormat = format
.Replace("%Y", "yyyy").Replace("%m", "MM").Replace("%d", "dd")
.Replace("%H", "HH").Replace("%M", "mm").Replace("%S", "ss")
.Replace("%F", "yyyy-MM-dd").Replace("%T", "HH:mm:ss")
.Replace("%Z", "zzz").Replace("%E4Y", "yyyy")
.Replace("%b", "MMM").Replace("%B", "MMMM");
if (DateTimeOffset.TryParseExact(str, netFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var result))
return result;
return DateTimeOffset.Parse(str, CultureInfo.InvariantCulture);
}

#endregion
#region Remaining function helpers

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
if (val is IEnumerable<object?> en)
return string.Join(delimiter, en.Select(v => v?.ToString() ?? ""));
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
// Handle simple JSONPath like "$.field" or "$.field.subfield"
var current = root;
var parts = path.TrimStart('$', '.').Split('.');
foreach (var part in parts)
{
if (string.IsNullOrEmpty(part)) continue;
if (current.ValueKind != System.Text.Json.JsonValueKind.Object) return null;
if (!current.TryGetProperty(part, out var next)) return null;
current = next;
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

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#nethosturl
//   "Takes a URL as a STRING and returns the host."
private object? EvaluateNetHost(IReadOnlyList<SqlExpression> args, RowContext row)
{
var url = Evaluate(args[0], row)?.ToString();
if (url is null) return null;
try { return new Uri(url.Contains("://") ? url : "http://" + url).Host; }
catch { return null; }
}

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netpublic_suffix
//   "Takes a URL and returns the public suffix (e.g., .com, .co.uk)."
private object? EvaluateNetPublicSuffix(IReadOnlyList<SqlExpression> args, RowContext row)
{
var url = Evaluate(args[0], row)?.ToString();
if (url is null) return null;
try
{
	var host = new Uri(url.Contains("://") ? url : "http://" + url).Host;
	var parts = host.Split('.');
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
	var host = new Uri(url.Contains("://") ? url : "http://" + url).Host;
	var parts = host.Split('.');
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

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_count_functions
//   "In-memory exact counting — HLL++ functions are approximated as exact distinct counts."
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

#endregion
private object? EvaluateUdf(string name, IReadOnlyList<SqlExpression> args, RowContext row)
{
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
}
}
throw new NotSupportedException("Unknown function: " + name);
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
"ARRAY_AGG" => values.Where(v => v is not null).ToList(),
"COUNTIF" => (long)rows.Count(r => IsTruthy(Evaluate(agg.Arg!, r))),
"APPROX_COUNT_DISTINCT" => (long)values.Where(v => v is not null).Distinct().Count(),
"LOGICAL_AND" => values.All(v => v is true),
"LOGICAL_OR" => values.Any(v => v is true),
// Statistical aggregates
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions
"VAR_SAMP" or "VARIANCE" => EvaluateVariance(values, sample: true),
"VAR_POP" => EvaluateVariance(values, sample: false),
"STDDEV_SAMP" or "STDDEV" => EvaluateStddev(values, sample: true),
"STDDEV_POP" => EvaluateStddev(values, sample: false),
_ => throw new NotSupportedException("Unsupported aggregate: " + funcName)
};
}

private object? EvaluateStringAgg(AggregateCall agg, List<RowContext> rows)
{
var values = rows.Select(r => Evaluate(agg.Arg!, r)?.ToString())
.Where(v => v is not null).ToList();
return string.Join(",", values);
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

#endregion

#region Set operations

private InMemoryBigQueryResult ExecuteSetOperation(SetOperationStatement setOp)
{
var left = ExecuteSelect(setOp.Left);
var right = ExecuteSelect(setOp.Right);
var schema = left.Schema;

var resultRows = (setOp.OpType, setOp.All) switch
{
(SetOperationType.Union, true) => left.Rows.Concat(right.Rows).ToList(),
(SetOperationType.Union, false) => left.Rows.Concat(right.Rows)
.GroupBy(r => string.Join("|", r.F?.Select(f => f?.V?.ToString() ?? "NULL") ?? Array.Empty<string>()))
.Select(g => g.First()).ToList(),
(SetOperationType.Except, _) => left.Rows
.Where(lr => !right.Rows.Any(rr => RowEquals(lr, rr))).ToList(),
(SetOperationType.Intersect, _) => left.Rows
.Where(lr => right.Rows.Any(rr => RowEquals(lr, rr)))
.GroupBy(r => string.Join("|", r.F?.Select(f => f?.V?.ToString() ?? "NULL") ?? Array.Empty<string>()))
.Select(g => g.First()).ToList(),
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
for (int i = 0; i < insert.Columns.Count && i < rowValues.Count; i++)
{
fields[insert.Columns[i]] = Evaluate(rowValues[i],
new RowContext(new Dictionary<string, object?>(), null));
}
table.Rows.Add(new InMemoryRow(fields));
count++;
}
}
return EmptyResult(count);
}

private InMemoryBigQueryResult ExecuteInsertSelect(InsertSelectStatement insert)
{
var (iDs, iTbl) = SplitTableName(insert.TableName);
var table = ResolveTable(iDs, iTbl);
var result = ExecuteSelect(insert.Query);
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
	// No explicit columns — map by schema field names
	foreach (var f in result.Schema.Fields)
		fields[f.Name] = dict.GetValueOrDefault(f.Name);
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
foreach (var row in table.Rows)
{
var fields = new Dictionary<string, object?>(row.Fields);
foreach (var kv in row.Fields) fields[alias + "." + kv.Key] = kv.Value;
var ctx = new RowContext(fields, alias);
if (update.Where is null || IsTruthy(Evaluate(update.Where, ctx)))
{
foreach (var (col, expr) in update.Assignments)
row.Fields[col] = Evaluate(expr, ctx);
count++;
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
        Mode = "NULLABLE"
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
                ds.Tables[rename.NewName] = t;
        }
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
return new InMemoryBigQueryResult(schema, rows);
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
/// Needed when subquery/CTE results are consumed by outer queries (formatted values → typed values).
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
		System.Globalization.DateTimeStyles.None, out var dt) ? dt : val,
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
double d => d == Math.Floor(d) && !double.IsInfinity(d) && !double.IsNaN(d)
? ((long)d).ToString()
: d.ToString(CultureInfo.InvariantCulture),
// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//   The BigQuery .NET SDK (v3.11.0) defaults to UseInt64Timestamp=true, which calls
//   long.Parse() on timestamp values. We must return epoch MICROSECONDS as an integer string.
//   SDK source: BigQueryResults.ConvertResponseRows → BigQueryRow → Int64TimestampConverter.
DateTimeOffset dto => (dto.ToUnixTimeMilliseconds() * 1000L).ToString(CultureInfo.InvariantCulture),
// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//   DATE values are returned as "yyyy-MM-dd" strings.
DateTime dt when dt.TimeOfDay == TimeSpan.Zero => dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
// Ref: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//   DATETIME values are returned as "yyyy-MM-ddTHH:mm:ss.FFFFFF" strings.
//   SDK source: BigQueryRow.DateTimeConverter uses DateTime.ParseExact with this format.
DateTime dt => dt.ToString("yyyy-MM-dd'T'HH:mm:ss.FFFFFF", CultureInfo.InvariantCulture),
byte[] bytes => Convert.ToBase64String(bytes),
IList<object?> list => string.Join(", ", list.Select(v => FormatValue(v)?.ToString() ?? "NULL")),
_ => val.ToString()
};
}

private List<RowContext> OrderBy(List<RowContext> rows, IReadOnlyList<OrderByItem> orderBy)
{
IOrderedEnumerable<RowContext>? ordered = null;
for (int i = 0; i < orderBy.Count; i++)
{
var item = orderBy[i];
var idx = i;
if (idx == 0)
{
ordered = item.Descending
? rows.OrderByDescending(r => Evaluate(item.Expr, r), NullSafeComparer.Instance)
: rows.OrderBy(r => Evaluate(item.Expr, r), NullSafeComparer.Instance);
}
else
{
ordered = item.Descending
? ordered!.ThenByDescending(r => Evaluate(item.Expr, r), NullSafeComparer.Instance)
: ordered!.ThenBy(r => Evaluate(item.Expr, r), NullSafeComparer.Instance);
}
}
return ordered?.ToList() ?? rows;
}

private static bool ContainsWindow(List<SelectItem> items)
=> items.Any(i => i.Expr is WindowFunction);

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

// string <-> string
if (a is string sa2 && b is string sb2)
return string.Compare(sa2, sb2, StringComparison.OrdinalIgnoreCase);

// DateTimeOffset
if (a is DateTimeOffset dtoa && b is DateTimeOffset dtob) return dtoa.CompareTo(dtob);
if (a is DateTime dta && b is DateTime dtb) return dta.CompareTo(dtb);

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
DateTime => "DATE",
byte[] => "BYTES",
IList<object?> => "RECORD",
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
DateTimeOffset dto => dto.ToString("yyyy-MM-dd HH:mm:ss.FFFFFF zzz", CultureInfo.InvariantCulture),
DateTime dt => dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
_ => val.ToString()
};
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
public static readonly NullSafeComparer Instance = new();
public int Compare(object? x, object? y)
{
if (x is null && y is null) return 0;
if (x is null) return -1;
if (y is null) return 1;
if (x is long la && y is double db) return ((double)la).CompareTo(db);
if (x is double da && y is long lb) return da.CompareTo((double)lb);
if (x is IComparable cx) return cx.CompareTo(y);
return string.Compare(x.ToString(), y.ToString(), StringComparison.OrdinalIgnoreCase);
}
}
