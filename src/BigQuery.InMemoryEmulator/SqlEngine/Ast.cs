namespace BigQuery.InMemoryEmulator.SqlEngine;

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
//   GoogleSQL query syntax defines SELECT, FROM, WHERE, GROUP BY, HAVING,
//   ORDER BY, LIMIT, OFFSET clauses.

/// <summary>Top-level SQL statement.</summary>
internal abstract record SqlStatement;

/// <summary>A SELECT statement, optionally with CTEs.</summary>
internal record SelectStatement(
	bool Distinct,
	IReadOnlyList<SelectItem> Columns,
	FromClause? From,
	SqlExpression? Where,
	IReadOnlyList<SqlExpression>? GroupBy,
	SqlExpression? Having,
	IReadOnlyList<OrderByItem>? OrderBy,
	int? Limit,
	int? Offset,
	IReadOnlyList<CteDefinition>? Ctes = null,
	SqlExpression? Qualify = null
) : SqlStatement;

/// <summary>A CTE: WITH name AS (SELECT ...)</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
internal record CteDefinition(string Name, SelectStatement Body, SelectStatement? RecursiveBody = null);

/// <summary>A set operation: UNION ALL, EXCEPT DISTINCT, INTERSECT DISTINCT.</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
internal record SetOperationStatement(
	SelectStatement Left,
	SetOperationType OpType,
	bool All,
	SelectStatement Right
) : SqlStatement;

internal enum SetOperationType { Union, Except, Intersect }

/// <summary>A SELECT item: expression with optional alias.</summary>
internal record SelectItem(SqlExpression Expr, string? Alias);

/// <summary>An ORDER BY item.</summary>
internal record OrderByItem(SqlExpression Expr, bool Descending);

// --- FROM clause hierarchy ---

/// <summary>Base for FROM clause nodes.</summary>
internal abstract record FromClause;

/// <summary>A table reference: dataset.table or just table.</summary>
internal record TableRef(string? DatasetId, string TableId, string? Alias) : FromClause;

/// <summary>A JOIN clause: left JOIN right ON condition.</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
internal record JoinClause(FromClause Left, JoinType Type, FromClause Right, SqlExpression? On) : FromClause;

/// <summary>UNNEST(expr) [AS alias]</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
internal record UnnestClause(SqlExpression Expr, string? Alias) : FromClause;

/// <summary>Subquery in FROM: (SELECT ...) [AS alias]</summary>
internal record SubqueryFrom(SelectStatement Subquery, string? Alias) : FromClause;

/// <summary>JOIN types supported by GoogleSQL.</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types
internal enum JoinType { Inner, Left, Right, Full, Cross }

// --- Expressions ---

/// <summary>Base for SQL expressions.</summary>
internal abstract record SqlExpression;

/// <summary>A column reference, optionally qualified: alias.column or just column.</summary>
internal record ColumnRef(string? TableAlias, string ColumnName) : SqlExpression;

/// <summary>A literal value.</summary>
internal record LiteralExpr(object? Value) : SqlExpression;

/// <summary>A parameter reference: @name.</summary>
internal record ParameterRef(string Name) : SqlExpression;

/// <summary>SELECT * or alias.*</summary>
internal record StarExpr(string? TableAlias) : SqlExpression;

/// <summary>A binary operation.</summary>
internal record BinaryExpr(SqlExpression Left, BinaryOp Op, SqlExpression Right) : SqlExpression;

/// <summary>A unary operation.</summary>
internal record UnaryExpr(UnaryOp Op, SqlExpression Operand) : SqlExpression;

/// <summary>A function call: FN(args).</summary>
internal record FunctionCall(string FunctionName, IReadOnlyList<SqlExpression> Args) : SqlExpression;

/// <summary>An aggregate call: COUNT(x), SUM(x), etc.</summary>
internal record AggregateCall(string FunctionName, SqlExpression? Arg, bool Distinct, IReadOnlyList<SqlExpression>? ExtraArgs = null, IReadOnlyList<OrderByItem>? AggOrderBy = null) : SqlExpression;

/// <summary>IS [NOT] NULL check.</summary>
internal record IsNullExpr(SqlExpression Expr, bool IsNot) : SqlExpression;

/// <summary>IS [NOT] TRUE / IS [NOT] FALSE check (three-valued logic).</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#is_operators
//   "expr IS TRUE" returns TRUE if expr evaluates to TRUE (not NULL, not FALSE)
internal record IsBoolExpr(SqlExpression Expr, bool IsNot, bool Value) : SqlExpression;

/// <summary>expr BETWEEN low AND high.</summary>
internal record BetweenExpr(SqlExpression Expr, SqlExpression Low, SqlExpression High) : SqlExpression;

/// <summary>expr IN (values).</summary>
internal record InExpr(SqlExpression Expr, IReadOnlyList<SqlExpression> Values) : SqlExpression;

/// <summary>expr [NOT] LIKE pattern.</summary>
internal record LikeExpr(SqlExpression Expr, SqlExpression Pattern, bool IsNot) : SqlExpression;

/// <summary>CAST(expr AS type) or SAFE_CAST.</summary>
internal record CastExpr(SqlExpression Expr, string TargetType, bool Safe) : SqlExpression;

/// <summary>CASE WHEN...THEN...ELSE...END.</summary>
internal record CaseExpr(
	SqlExpression? Operand,
	IReadOnlyList<(SqlExpression When, SqlExpression Then)> Branches,
	SqlExpression? Else) : SqlExpression;

/// <summary>Scalar subquery: (SELECT x FROM ...)</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#scalar_subquery
internal record ScalarSubquery(SelectStatement Subquery) : SqlExpression;

/// <summary>EXISTS (SELECT ...)</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#exists_subquery
internal record ExistsExpr(SelectStatement Subquery) : SqlExpression;

/// <summary>expr IN (SELECT ...)</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#in_subquery
internal record InSubqueryExpr(SqlExpression Expr, SelectStatement Subquery) : SqlExpression;

/// <summary>A window function: func() OVER(PARTITION BY ... ORDER BY ... [frame])</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
internal record WindowFunction(
	SqlExpression Function,
	IReadOnlyList<SqlExpression>? PartitionBy,
	IReadOnlyList<OrderByItem>? OrderBy,
	FrameSpec? Frame = null
) : SqlExpression;

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#def_window_frame
//   "ROWS: Physical frame defined by counting individual rows."
//   "RANGE: Logical frame based on a range of values in the ORDER BY column."
internal enum FrameType { Rows, Range }
internal enum FrameBoundaryType { UnboundedPreceding, Preceding, CurrentRow, Following, UnboundedFollowing }
internal record FrameBoundary(FrameBoundaryType Type, SqlExpression? Offset = null);
internal record FrameSpec(FrameType Type, FrameBoundary Start, FrameBoundary End);

/// <summary>ARRAY(SELECT ...) subquery expression.</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#array_subquery

/// <summary>ROLLUP(expr, ...) in GROUP BY clause.</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#rollup

/// <summary>PIVOT clause: PIVOT(agg FOR input_col IN (val1, val2, ...))</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#pivot_operator
internal record PivotClause(
	AggregateCall Aggregation,
	ColumnRef InputColumn,
	IReadOnlyList<(SqlExpression Value, string? Alias)> PivotValues
);

/// <summary>FROM source PIVOT(...)</summary>
internal record PivotFrom(FromClause Source, PivotClause Pivot) : FromClause;

/// <summary>UNPIVOT clause: UNPIVOT(values_col FOR name_col IN (col1, col2, ...))</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unpivot_operator
internal record UnpivotClause(
	string ValuesColumn,
	string NameColumn,
	IReadOnlyList<string> InputColumns
);

/// <summary>FROM source UNPIVOT(...)</summary>
internal record UnpivotFrom(FromClause Source, UnpivotClause Unpivot) : FromClause;

/// <summary>FROM source TABLESAMPLE SYSTEM (percent PERCENT)</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#tablesample_operator
internal record TablesampleFrom(FromClause Source, double Percent) : FromClause;

internal record RollupExpr(IReadOnlyList<SqlExpression> Exprs) : SqlExpression;
internal record ArraySubquery(SelectStatement Subquery) : SqlExpression;

// --- Enums ---

internal enum BinaryOp
{
	Eq, Neq, Lt, Gt, Lte, Gte,
	Add, Sub, Mul, Div, Mod,
	And, Or,
	Concat,
}

internal enum UnaryOp { Not, Negate }


// --- DML Statements ---
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax

/// <summary>INSERT INTO table (columns) VALUES (row1), (row2), ...</summary>
internal record InsertValuesStatement(
	string TableName,
	IReadOnlyList<string>? Columns,
	IReadOnlyList<IReadOnlyList<SqlExpression>> Rows
) : SqlStatement;

/// <summary>INSERT INTO table (columns) SELECT ...</summary>
internal record InsertSelectStatement(
	string TableName,
	IReadOnlyList<string>? Columns,
	SelectStatement Query
) : SqlStatement;

/// <summary>UPDATE table SET col=expr, ... WHERE condition</summary>
internal record UpdateStatement(
	string TableName,
	string? Alias,
	IReadOnlyList<(string Column, SqlExpression Value)> Assignments,
	SqlExpression Where
) : SqlStatement;

/// <summary>DELETE FROM table WHERE condition</summary>
internal record DeleteStatement(
	string TableName,
	string? Alias,
	SqlExpression Where
) : SqlStatement;

/// <summary>MERGE INTO target USING source ON condition WHEN MATCHED/NOT MATCHED ...</summary>
internal record MergeStatement(
	string TargetTable,
	string? TargetAlias,
	FromClause Source,
	string? SourceAlias,
	SqlExpression On,
	IReadOnlyList<MergeWhenClause> WhenClauses
) : SqlStatement;

internal abstract record MergeWhenClause;
internal record MergeWhenMatched(
	SqlExpression? And,
	IReadOnlyList<(string Column, SqlExpression Value)>? Updates,
	bool IsDelete
) : MergeWhenClause;
internal record MergeWhenNotMatched(
	SqlExpression? And,
	IReadOnlyList<string>? Columns,
	IReadOnlyList<SqlExpression> Values
) : MergeWhenClause;
// --- DDL Statements ---
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language

/// <summary>CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name (col type, ...)</summary>
internal record CreateTableStatement(
	string TableName,
	string? DatasetId,
	IReadOnlyList<(string Name, string Type)> Columns,
	bool OrReplace,
	bool IfNotExists
) : SqlStatement;

/// <summary>CREATE TABLE name AS SELECT ...</summary>
internal record CreateTableAsSelectStatement(
	string TableName,
	string? DatasetId,
	SelectStatement Query,
	bool OrReplace
) : SqlStatement;

/// <summary>DROP TABLE [IF EXISTS] name</summary>
internal record DropTableStatement(
	string TableName,
	string? DatasetId,
	bool IfExists
) : SqlStatement;

/// <summary>ALTER TABLE name ADD COLUMN col type / DROP COLUMN col / RENAME TO name</summary>
internal record AlterTableStatement(
	string TableName,
	string? DatasetId,
	AlterTableAction Action
) : SqlStatement;

internal abstract record AlterTableAction;
internal record AddColumnAction(string Name, string Type) : AlterTableAction;
internal record DropColumnAction(string Name) : AlterTableAction;
internal record RenameTableAction(string NewName) : AlterTableAction;
// Phase 27: ALTER COLUMN variants
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_set_data_type
internal record AlterColumnSetDataTypeAction(string ColumnName, string NewType) : AlterTableAction;
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_set_default
internal record AlterColumnSetDefaultAction(string ColumnName, string DefaultExpression) : AlterTableAction;
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_drop_default
internal record AlterColumnDropDefaultAction(string ColumnName) : AlterTableAction;
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_drop_not_null
internal record AlterColumnDropNotNullAction(string ColumnName) : AlterTableAction;
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_table_set_options
internal record SetOptionsAction(string OptionsText) : AlterTableAction;

/// <summary>CREATE [OR REPLACE] VIEW name AS SELECT ...</summary>
internal record CreateViewStatement(
	string ViewName,
	string? DatasetId,
	SelectStatement Query,
	bool OrReplace
) : SqlStatement;

/// <summary>DROP VIEW [IF EXISTS] name</summary>
internal record DropViewStatement(
	string ViewName,
	string? DatasetId,
	bool IfExists
) : SqlStatement;

// Phase 27: TRUNCATE TABLE
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#truncate_table_statement
internal record TruncateTableStatement(string TableName, string? DatasetId) : SqlStatement;

// Phase 27: CREATE TABLE LIKE / COPY / CLONE / SNAPSHOT
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
internal record CreateTableLikeStatement(string TableName, string? DatasetId, string SourceTable, string? SourceDatasetId) : SqlStatement;
internal record CreateTableCopyStatement(string TableName, string? DatasetId, string SourceTable, string? SourceDatasetId) : SqlStatement;

// Phase 27: CREATE/DROP SCHEMA
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement
internal record CreateSchemaStatement(string SchemaName, bool IfNotExists) : SqlStatement;
internal record DropSchemaStatement(string SchemaName, bool IfExists) : SqlStatement;

// Phase 27: Stub DDL statements (parsed but minimally executed)
internal record NoOpDdlStatement(string Description) : SqlStatement;

// --- Procedural Language AST Nodes (Phase 15) ---
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language

/// <summary>A block of statements executed sequentially.</summary>
internal record StatementBlock(IReadOnlyList<SqlStatement> Statements) : SqlStatement;

/// <summary>DECLARE var [, var2] type [DEFAULT expr]</summary>
internal record DeclareStatement(IReadOnlyList<string> VariableNames, string? TypeName, SqlExpression? DefaultValue) : SqlStatement;

/// <summary>SET var = expr</summary>
internal record SetStatement(string VariableName, SqlExpression Value) : SqlStatement;

/// <summary>IF condition THEN stmts [ELSEIF condition THEN stmts]* [ELSE stmts] END IF</summary>
internal record IfStatement(SqlExpression Condition, IReadOnlyList<SqlStatement> ThenBlock, IReadOnlyList<(SqlExpression Condition, IReadOnlyList<SqlStatement> Body)> ElseIfBlocks, IReadOnlyList<SqlStatement>? ElseBlock) : SqlStatement;

/// <summary>BEGIN stmts [EXCEPTION WHEN ERROR THEN stmts] END</summary>
internal record BeginEndBlock(IReadOnlyList<SqlStatement> Body, IReadOnlyList<SqlStatement>? ExceptionHandler) : SqlStatement;

/// <summary>RAISE [USING MESSAGE = expr]</summary>
internal record RaiseStatement(SqlExpression? Message) : SqlStatement;

/// <summary>RETURN</summary>
internal record ReturnStatement : SqlStatement;

/// <summary>ASSERT condition [AS description]</summary>
internal record AssertStatement(SqlExpression Condition, string? Description) : SqlStatement;

/// <summary>Variable reference in expressions (e.g., my_var or @@row_count)</summary>
internal record VariableRef(string Name) : SqlExpression;

/// <summary>Lambda expression: param -> body (used in ARRAY_FILTER, ARRAY_TRANSFORM).</summary>
// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_filter
internal record LambdaExpr(string ParamName, SqlExpression Body) : SqlExpression;

