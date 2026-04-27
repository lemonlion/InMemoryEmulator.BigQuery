using System.Globalization;
using System.Text.RegularExpressions;
using Superpower;
using Superpower.Model;
using Superpower.Parsers;
using SP = Superpower.Parse;

namespace BigQuery.InMemoryEmulator.SqlEngine;

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
//   GoogleSQL query syntax for SELECT statements.

/// <summary>
/// Parses GoogleSQL SELECT statements into AST nodes using Superpower parser combinators.
/// Phase 4: SELECT, FROM, WHERE, ORDER BY, LIMIT, OFFSET, basic expressions.
/// </summary>
internal static class SqlParser
{
	/// <summary>Parse a SQL string into a SqlStatement.</summary>
	public static SqlStatement ParseSql(string sql)
	{
		sql = NormalizeSql(sql);
		var tokens = SqlTokenizer.Instance.Tokenize(sql);
		var resolved = KeywordResolver.Resolve(tokens);
		return TopLevelStatement.Parse(resolved);
	}

	/// <summary>
	/// Normalizes BigQuery SQL syntax into forms the parser can handle.
	/// Rewrites special constructs like EXTRACT(part FROM expr), typed literals (DATE '...'),
	/// INTERVAL expressions, and aggregate modifiers (IGNORE NULLS).
	/// </summary>
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators
	internal static string NormalizeSql(string sql)
	{
		// EXTRACT(part FROM expr) → EXTRACT('part', expr)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#extract
		sql = Regex.Replace(sql, @"\bEXTRACT\s*\(\s*(\w+)\s+FROM\s+", "EXTRACT('$1', ", RegexOptions.IgnoreCase);

		// DATE 'string' → CAST('string' AS DATE)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#date_literals
		sql = Regex.Replace(sql, @"\bDATE\s+'([^']*)'", "CAST('$1' AS DATE)", RegexOptions.IgnoreCase);

		// TIMESTAMP 'string' → CAST('string' AS TIMESTAMP)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#timestamp_literals
		sql = Regex.Replace(sql, @"\bTIMESTAMP\s+'([^']*)'", "CAST('$1' AS TIMESTAMP)", RegexOptions.IgnoreCase);

		// DATETIME 'string' → CAST('string' AS DATETIME)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#datetime_literals
		sql = Regex.Replace(sql, @"\bDATETIME\s+'([^']*)'", "CAST('$1' AS DATETIME)", RegexOptions.IgnoreCase);

		// TIME 'string' → CAST('string' AS TIME)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#time_literals
		sql = Regex.Replace(sql, @"\bTIME\s+'([^']*)'", "CAST('$1' AS TIME)", RegexOptions.IgnoreCase);

		// INTERVAL n PART → n, 'PART' (inside function args like TIMESTAMP_ADD)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#interval_type
		sql = Regex.Replace(sql, @"\bINTERVAL\s+(\d+)\s+(\w+)", "$1, '$2'", RegexOptions.IgnoreCase);

		// ARRAY_AGG(expr IGNORE NULLS) → ARRAY_AGG(expr) — remove IGNORE NULLS modifier
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
		sql = Regex.Replace(sql, @"\bIGNORE\s+NULLS\b", "", RegexOptions.IgnoreCase);

		// RESPECT NULLS modifier — remove
		sql = Regex.Replace(sql, @"\bRESPECT\s+NULLS\b", "", RegexOptions.IgnoreCase);

		// Dotted function names: NET.HOST → NET_HOST, HLL_COUNT.EXTRACT → HLL_COUNT_EXTRACT
		// These use dot-separated namespaces that the parser can't handle (parsed as column refs).
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_count_functions
		sql = Regex.Replace(sql, @"\bNET\.(\w+)\s*\(", "NET_$1(", RegexOptions.IgnoreCase);
		sql = Regex.Replace(sql, @"\bHLL_COUNT\.(\w+)\s*\(", "HLL_COUNT_$1(", RegexOptions.IgnoreCase);

		// Bare date part keywords → string literals when used as function arguments.
		// Functions like DATE_DIFF(d1, d2, DAY) pass DAY as a bare keyword, which the parser
		// treats as a column reference (evaluates to null). Convert to 'DAY'.
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
		sql = Regex.Replace(sql,
			@",\s*\b(MICROSECOND|MILLISECOND|SECOND|MINUTE|HOUR|DAY|DAYOFWEEK|DAYOFYEAR|WEEK|ISOWEEK|MONTH|QUARTER|YEAR|ISOYEAR|DATE|DATETIME)\s*\)",
			", '$1')", RegexOptions.IgnoreCase);

		return sql;
	}

	/// <summary>Token list parser that returns a value without consuming input.</summary>
	private static TokenListParser<SqlToken, T> Constant<T>(T value) =>
		input => TokenListParserResult.Value(value, input, input);

	// --- Helpers ---

	private static readonly TokenListParser<SqlToken, string> Identifier =
		Token.EqualTo(SqlToken.Identifier).Select(t => t.ToStringValue())
		.Or(Token.EqualTo(SqlToken.BacktickIdentifier).Select(t => t.ToStringValue().Trim('`')));

	// Function names that are also SQL keywords (LEFT/RIGHT are both JOIN types and string functions)
	private static readonly TokenListParser<SqlToken, string> FunctionNameOrIdentifier =
		Identifier
		.Or(Token.EqualTo(SqlToken.Left).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Right).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Exists).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Replace).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Range).Select(t => t.ToStringValue()));

	/// <summary>Helper: matches an Identifier token whose text equals the given value (case-insensitive).</summary>
	private static TokenListParser<SqlToken, Token<SqlToken>> IdentifierMatching(string value) =>
		Token.EqualTo(SqlToken.Identifier).Where(t =>
			t.ToStringValue().Equals(value, StringComparison.OrdinalIgnoreCase));

	/// <summary>Helper: consumes all tokens until a matching RParen is found, returning their text.</summary>
	private static readonly TokenListParser<SqlToken, string> ConsumeUntilCloseParen =
		input =>
		{
			var sb = new System.Text.StringBuilder();
			var position = input;
			int depth = 0;
			while (!position.IsAtEnd)
			{
				var token = position.ConsumeToken();
				if (!token.HasValue) break;
				if (token.Value.Kind == SqlToken.RParen && depth == 0)
					return TokenListParserResult.Value(sb.ToString().Trim(), input, position);
				if (token.Value.Kind == SqlToken.LParen) depth++;
				if (token.Value.Kind == SqlToken.RParen) depth--;
				if (sb.Length > 0) sb.Append(' ');
				sb.Append(token.Value.ToStringValue());
				position = token.Remainder;
			}
			return TokenListParserResult.Value(sb.ToString().Trim(), input, position);
		};

	// Accept keywords as identifiers in alias/column positions (e.g. SELECT name, id)
	private static readonly TokenListParser<SqlToken, string> IdentifierOrKeyword =
		Identifier
		.Or(Token.EqualTo(SqlToken.Select).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.From).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Where).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Order).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.By).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.As).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.And).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Or).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Not).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Is).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Null).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.True).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.False).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Asc).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Desc).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Limit).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Offset).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Distinct).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.In).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Like).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Between).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Case).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.When).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Then).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Else).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.End).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Cast).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.SafeCast).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Join).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Inner).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Full).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Outer).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Cross).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Group).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Having).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.On).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Over).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Partition).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Row)
			.Or(Token.EqualTo(SqlToken.Into))
			.Or(Token.EqualTo(SqlToken.Values))
			.Or(Token.EqualTo(SqlToken.Set))
			.Or(Token.EqualTo(SqlToken.Using))
			.Or(Token.EqualTo(SqlToken.Matched))
			.Or(Token.EqualTo(SqlToken.Table))
			.Or(Token.EqualTo(SqlToken.View))
			.Or(Token.EqualTo(SqlToken.Column))
			.Or(Token.EqualTo(SqlToken.Replace))
			.Or(Token.EqualTo(SqlToken.Add))
			.Or(Token.EqualTo(SqlToken.Rename))
			.Or(Token.EqualTo(SqlToken.To))
			.Or(Token.EqualTo(SqlToken.If)).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Rows).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Range).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Unbounded).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Preceding).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Following).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Current).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.With).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Union).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.All).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Except).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Intersect).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Exists).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Truncate).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Schema).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Snapshot).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Clone).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.External).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Procedure).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Materialized).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Function).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Policy).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Index).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Grant).Select(t => t.ToStringValue()))
		.Or(Token.EqualTo(SqlToken.Options).Select(t => t.ToStringValue()));

	// --- Literals ---

	private static readonly TokenListParser<SqlToken, SqlExpression> NumberLiteral =
		Token.EqualTo(SqlToken.Number).Select(t =>
		{
			var s = t.ToStringValue();
			if (long.TryParse(s, out var l)) return (SqlExpression)new LiteralExpr(l);
			if (double.TryParse(s, System.Globalization.NumberStyles.Float,
				System.Globalization.CultureInfo.InvariantCulture, out var d))
				return new LiteralExpr(d);
			return new LiteralExpr(s);
		});

	private static readonly TokenListParser<SqlToken, SqlExpression> StringLiteral =
		Token.EqualTo(SqlToken.StringLiteral)
			.Select(t => (SqlExpression)new LiteralExpr(t.ToStringValue().Trim('\'')));

	private static readonly TokenListParser<SqlToken, SqlExpression> BoolLiteral =
		Token.EqualTo(SqlToken.True).Select(_ => (SqlExpression)new LiteralExpr(true))
		.Or(Token.EqualTo(SqlToken.False).Select(_ => (SqlExpression)new LiteralExpr(false)));

	private static readonly TokenListParser<SqlToken, SqlExpression> NullLiteral =
		Token.EqualTo(SqlToken.Null).Select(_ => (SqlExpression)new LiteralExpr(null));

	private static readonly TokenListParser<SqlToken, SqlExpression> ParameterExpr =
		Token.EqualTo(SqlToken.Parameter)
			.Select(t => (SqlExpression)new ParameterRef(t.ToStringValue().TrimStart('@')));

	// --- Array literal: [expr, ...] ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#array_literals
	private static readonly TokenListParser<SqlToken, SqlExpression> ArrayLiteral =
		Token.EqualTo(SqlToken.LBracket).IgnoreThen(
			SP.Ref(() => Expression!).ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
		.Then(elems => Token.EqualTo(SqlToken.RBracket)
			.Select(_ => (SqlExpression)new FunctionCall("ARRAY", elems.ToList())));

	// --- Star expression ---

	private static readonly TokenListParser<SqlToken, SqlExpression> Star =
		Token.EqualTo(SqlToken.Star).Select(_ => (SqlExpression)new StarExpr(null));

	// --- Column reference: identifier or alias.identifier ---

	private static readonly TokenListParser<SqlToken, SqlExpression> ColumnOrFunctionRef =
		FunctionNameOrIdentifier.Then(name =>
			// Check for function call: name(...)
			Token.EqualTo(SqlToken.LParen).IgnoreThen(
				SP.Ref(() => Expression!).ManyDelimitedBy(Token.EqualTo(SqlToken.Comma))
			).Then(args => Token.EqualTo(SqlToken.RParen).Select(_ =>
			{
				// Check if it's an aggregate function
				var upper = name.ToUpperInvariant();
				if (upper is "COUNT" or "SUM" or "AVG" or "MIN" or "MAX" or "ANY_VALUE"
					or "ARRAY_AGG" or "ARRAY_CONCAT_AGG" or "STRING_AGG" or "COUNTIF" or "LOGICAL_AND" or "LOGICAL_OR"
					or "APPROX_COUNT_DISTINCT" or "BIT_AND" or "BIT_OR" or "BIT_XOR"
					or "VAR_SAMP" or "VAR_POP" or "VARIANCE" or "STDDEV" or "STDDEV_SAMP" or "STDDEV_POP"
					or "CORR" or "COVAR_POP" or "COVAR_SAMP"
					or "APPROX_QUANTILES" or "APPROX_TOP_COUNT" or "APPROX_TOP_SUM")
				{
					var extraArgs = args.Length > 1 ? args.Skip(1).ToList() : null;
					return (SqlExpression)new AggregateCall(upper, args.Length > 0 ? args[0] : null, false, extraArgs);
				}
				return (SqlExpression)new FunctionCall(upper, args.ToList());
			}))
			.Try()
			// Check for qualified name: alias.column or alias.*
			.Or(Token.EqualTo(SqlToken.Dot).IgnoreThen(
				Token.EqualTo(SqlToken.Star).Select(_ => (SqlExpression)new StarExpr(name))
				.Or(Identifier.Select(col => (SqlExpression)new ColumnRef(name, col)))
			).Try())
			// Just a column reference
			.Or(Constant((SqlExpression)new ColumnRef(null, name)))
		);

	// --- COUNT(DISTINCT x) special case ---
	private static readonly TokenListParser<SqlToken, SqlExpression> CountDistinct =
		Token.EqualTo(SqlToken.Identifier)
			.Where(t => t.ToStringValue().Equals("COUNT", StringComparison.OrdinalIgnoreCase))
			.IgnoreThen(Token.EqualTo(SqlToken.LParen))
			.IgnoreThen(Token.EqualTo(SqlToken.Distinct))
			.Then(_ => SP.Ref(() => Expression!))
			.Then(expr => Token.EqualTo(SqlToken.RParen).Select(_ =>
				(SqlExpression)new AggregateCall("COUNT", expr, true)));

	// --- CAST(expr AS type) ---
	private static readonly TokenListParser<SqlToken, SqlExpression> CastExprParser =
		Token.EqualTo(SqlToken.Cast)
			.Or(Token.EqualTo(SqlToken.SafeCast))
			.Then(tok =>
				Token.EqualTo(SqlToken.LParen)
					.IgnoreThen(SP.Ref(() => Expression!))
					.Then(expr => Token.EqualTo(SqlToken.As)
						.IgnoreThen(IdentifierOrKeyword)
						.Then(type => Token.EqualTo(SqlToken.RParen).Select(_ =>
							(SqlExpression)new CastExpr(expr, type.ToUpperInvariant(),
								tok.ToStringValue().Equals("SAFE_CAST", StringComparison.OrdinalIgnoreCase))
						))
					)
			);

	// --- CASE expression ---
	private static readonly TokenListParser<SqlToken, SqlExpression> CaseExprParser =
		Token.EqualTo(SqlToken.Case).IgnoreThen(
			Token.EqualTo(SqlToken.When).IgnoreThen(SP.Ref(() => Expression!))
				.Then(when => Token.EqualTo(SqlToken.Then).IgnoreThen(SP.Ref(() => Expression!))
					.Select(then => (When: when, Then: then)))
				.AtLeastOnce()
		).Then(branches =>
			Token.EqualTo(SqlToken.Else).IgnoreThen(SP.Ref(() => Expression!))
				.Select(e => (SqlExpression?)e).OptionalOrDefault()
				.Then(elseExpr => Token.EqualTo(SqlToken.End).Select(_ =>
					(SqlExpression)new CaseExpr(null, branches.Select(b => (b.When, b.Then)).ToList(), elseExpr)
				))
		);

	// --- EXISTS (SELECT ...) ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries#exists_subquery
	private static readonly TokenListParser<SqlToken, SqlExpression> ExistsExprParser =
		Token.EqualTo(SqlToken.Exists)
			.IgnoreThen(Token.EqualTo(SqlToken.LParen))
			.IgnoreThen(SP.Ref(() => SelectStmt!))
			.Then(sub => Token.EqualTo(SqlToken.RParen).Select(_ =>
				(SqlExpression)new ExistsExpr(sub)));

	// --- Parenthesized expression or scalar subquery ---
	private static readonly TokenListParser<SqlToken, SqlExpression> Parens =
		Token.EqualTo(SqlToken.LParen).IgnoreThen(
			// Try scalar subquery first
			SP.Ref(() => SelectStmt!).Then(sub =>
				Token.EqualTo(SqlToken.RParen).Select(_ => (SqlExpression)new ScalarSubquery(sub))
			).Try()
			// Then regular parenthesized expression
			.Or(SP.Ref(() => Expression!).Then(e =>
				Token.EqualTo(SqlToken.RParen).Select(_ => e)))
		);

	// --- Unary NOT / - ---
	private static readonly TokenListParser<SqlToken, SqlExpression> UnaryNot =
		Token.EqualTo(SqlToken.Not).IgnoreThen(
			SP.Ref(() => Atom!)
		).Select(e => (SqlExpression)new UnaryExpr(UnaryOp.Not, e));

	private static readonly TokenListParser<SqlToken, SqlExpression> UnaryMinus =
		Token.EqualTo(SqlToken.Minus).IgnoreThen(
			SP.Ref(() => Atom!)
		).Select(e => (SqlExpression)new UnaryExpr(UnaryOp.Negate, e));

	// --- Atom: base expression ---
	private static readonly TokenListParser<SqlToken, SqlExpression> Atom =
		CountDistinct.Try()
		.Or(CastExprParser.Try())
		.Or(CaseExprParser.Try())
		.Or(ExistsExprParser.Try())
		.Or(ArrayLiteral.Try())
		.Or(ColumnOrFunctionRef.Try())
		.Or(NumberLiteral)
		.Or(StringLiteral)
		.Or(BoolLiteral)
		.Or(NullLiteral)
		.Or(ParameterExpr)
		.Or(Star)
		.Or(Parens)
		.Or(UnaryNot)
		.Or(UnaryMinus);

	// --- OVER (window function) suffix ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
	private static readonly TokenListParser<SqlToken, SqlExpression> OverSuffix =
		Atom.Then(expr =>
			Token.EqualTo(SqlToken.Over).IgnoreThen(Token.EqualTo(SqlToken.LParen)).IgnoreThen(
				// PARTITION BY
				Token.EqualTo(SqlToken.Partition).IgnoreThen(Token.EqualTo(SqlToken.By))
					.IgnoreThen(SP.Ref(() => Expression!).ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
					.Select(p => (IReadOnlyList<SqlExpression>?)p.ToList())
					.OptionalOrDefault()
			).Then(partitionBy =>
				// ORDER BY
				Token.EqualTo(SqlToken.Order).IgnoreThen(Token.EqualTo(SqlToken.By))
					.IgnoreThen(SP.Ref(() => OrderByItemParser!).ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
					.Select(o => (IReadOnlyList<OrderByItem>?)o.ToList())
					.OptionalOrDefault()
				.Then(orderBy =>
					Token.EqualTo(SqlToken.RParen).Select(_ =>
						(SqlExpression)new WindowFunction(expr, partitionBy, orderBy))
				)
			).Try()
			.Or(Constant(expr))
		);

	// --- IS [NOT] NULL suffix ---
	private static readonly TokenListParser<SqlToken, SqlExpression> IsNullSuffix =
		OverSuffix.Then(expr =>
			Token.EqualTo(SqlToken.Is).IgnoreThen(
				Token.EqualTo(SqlToken.Not).Select(_ => true).OptionalOrDefault(false)
			).Then(isNot =>
				Token.EqualTo(SqlToken.Null).Select(_ => (SqlExpression)new IsNullExpr(expr, isNot))
			).Try()
			.Or(Constant(expr))
		);

	// --- BETWEEN / IN / LIKE postfix ---
	private static readonly TokenListParser<SqlToken, SqlExpression> PostfixOps =
		IsNullSuffix.Then(expr =>
			// BETWEEN low AND high
			Token.EqualTo(SqlToken.Between).IgnoreThen(IsNullSuffix)
				.Then(low => Token.EqualTo(SqlToken.And).IgnoreThen(IsNullSuffix)
					.Select(high => (SqlExpression)new BetweenExpr(expr, low, high)))
				.Try()
			// IN (SELECT ...) or IN (values)
			.Or(Token.EqualTo(SqlToken.In)
				.IgnoreThen(Token.EqualTo(SqlToken.LParen))
				.IgnoreThen(
					// Try subquery first
					SP.Ref(() => SelectStmt!).Then(sub =>
						Token.EqualTo(SqlToken.RParen)
							.Select(_ => (SqlExpression)new InSubqueryExpr(expr, sub))
					).Try()
					// Then value list
					.Or(SP.Ref(() => Expression!).ManyDelimitedBy(Token.EqualTo(SqlToken.Comma))
						.Then(vals => Token.EqualTo(SqlToken.RParen)
							.Select(_ => (SqlExpression)new InExpr(expr, vals.ToList()))))
				)
				.Try())
			// [NOT] LIKE pattern
			.Or(Token.EqualTo(SqlToken.Not).IgnoreThen(Token.EqualTo(SqlToken.Like))
				.IgnoreThen(IsNullSuffix)
				.Select(pat => (SqlExpression)new LikeExpr(expr, pat, true))
				.Try())
			.Or(Token.EqualTo(SqlToken.Like)
				.IgnoreThen(IsNullSuffix)
				.Select(pat => (SqlExpression)new LikeExpr(expr, pat, false))
				.Try())
			.Or(Constant(expr))
		);

	// --- Multiplication / Division ---
	private static readonly TokenListParser<SqlToken, SqlExpression> MulDiv =
		PostfixOps.Then(left =>
			(Token.EqualTo(SqlToken.Star).Select(_ => BinaryOp.Mul)
				.Or(Token.EqualTo(SqlToken.Slash).Select(_ => BinaryOp.Div))
				.Or(Token.EqualTo(SqlToken.Percent).Select(_ => BinaryOp.Mod))
			.Then(op => SP.Ref(() => MulDiv!).Select(right => (Op: op, Right: right)))
			).Try()
			.Select(pair => (SqlExpression)new BinaryExpr(left, pair.Op, pair.Right))
			.Or(Constant(left))
		);

	// --- Addition / Subtraction / Concat ---
	private static readonly TokenListParser<SqlToken, SqlExpression> AddSub =
		MulDiv.Then(left =>
			(Token.EqualTo(SqlToken.Plus).Select(_ => BinaryOp.Add)
				.Or(Token.EqualTo(SqlToken.Minus).Select(_ => BinaryOp.Sub))
				.Or(Token.EqualTo(SqlToken.Pipe).Select(_ => BinaryOp.Concat))
			.Then(op => SP.Ref(() => AddSub!).Select(right => (Op: op, Right: right)))
			).Try()
			.Select(pair => (SqlExpression)new BinaryExpr(left, pair.Op, pair.Right))
			.Or(Constant(left))
		);

	// --- Comparison operators ---
	private static readonly TokenListParser<SqlToken, SqlExpression> Comparison =
		AddSub.Then(left =>
			(Token.EqualTo(SqlToken.Eq).Select(_ => BinaryOp.Eq)
				.Or(Token.EqualTo(SqlToken.Neq).Select(_ => BinaryOp.Neq))
				.Or(Token.EqualTo(SqlToken.Lte).Select(_ => BinaryOp.Lte))
				.Or(Token.EqualTo(SqlToken.Gte).Select(_ => BinaryOp.Gte))
				.Or(Token.EqualTo(SqlToken.Lt).Select(_ => BinaryOp.Lt))
				.Or(Token.EqualTo(SqlToken.Gt).Select(_ => BinaryOp.Gt))
			.Then(op => AddSub.Select(right => (Op: op, Right: right)))
			).Try()
			.Select(pair => (SqlExpression)new BinaryExpr(left, pair.Op, pair.Right))
			.Or(Constant(left))
		);

	// --- AND ---
	private static readonly TokenListParser<SqlToken, SqlExpression> AndExpr =
		Comparison.Then(left =>
			Token.EqualTo(SqlToken.And)
				.IgnoreThen(SP.Ref(() => AndExpr!))
				.Select(right => (SqlExpression)new BinaryExpr(left, BinaryOp.And, right))
				.Try()
			.Or(Constant(left))
		);

	// --- OR ---
	private static readonly TokenListParser<SqlToken, SqlExpression> OrExpr =
		AndExpr.Then(left =>
			Token.EqualTo(SqlToken.Or)
				.IgnoreThen(SP.Ref(() => OrExpr!))
				.Select(right => (SqlExpression)new BinaryExpr(left, BinaryOp.Or, right))
				.Try()
			.Or(Constant(left))
		);

	// The top-level expression parser
	private static readonly TokenListParser<SqlToken, SqlExpression> Expression = OrExpr;

	// --- SELECT items ---

	private static readonly TokenListParser<SqlToken, SelectItem> SelectItemParser =
		Expression.Then(expr =>
			Token.EqualTo(SqlToken.As).IgnoreThen(IdentifierOrKeyword)
				.Select(alias => new SelectItem(expr, alias))
				.Try()
			.Or(
				// Implicit alias ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â only plain identifiers, not keywords that start clauses
				Identifier
					.Select(alias => new SelectItem(expr, alias))
					.Try()
			)
			.Or(Constant(new SelectItem(expr, null)))
		);

	// --- FROM clause ---

	private static readonly TokenListParser<SqlToken, FromClause> TableReference =
		Identifier.Then(first =>
			// dataset.table or just table
			Token.EqualTo(SqlToken.Dot).IgnoreThen(Identifier)
				.Select(second => (DatasetId: (string?)first, TableId: second))
				.Try()
				.Or(Constant((DatasetId: (string?)null, TableId: first)))
		).Then(tref =>
			// Optional alias
			Token.EqualTo(SqlToken.As).IgnoreThen(IdentifierOrKeyword)
				.Select(alias => (FromClause)new TableRef(tref.DatasetId, tref.TableId, alias))
				.Try()
			.Or(Identifier
				.Select(alias => (FromClause)new TableRef(tref.DatasetId, tref.TableId, alias))
				.Try())
			.Or(Constant((FromClause)new TableRef(tref.DatasetId, tref.TableId, null)))
		);

	// --- JOIN type parser ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#join_types

	private static readonly TokenListParser<SqlToken, JoinType> JoinTypeParser =
		Token.EqualTo(SqlToken.Inner).IgnoreThen(Token.EqualTo(SqlToken.Join)).Select(_ => JoinType.Inner).Try()
		.Or(Token.EqualTo(SqlToken.Left).IgnoreThen(Token.EqualTo(SqlToken.Outer).OptionalOrDefault()).IgnoreThen(Token.EqualTo(SqlToken.Join)).Select(_ => JoinType.Left).Try())
		.Or(Token.EqualTo(SqlToken.Right).IgnoreThen(Token.EqualTo(SqlToken.Outer).OptionalOrDefault()).IgnoreThen(Token.EqualTo(SqlToken.Join)).Select(_ => JoinType.Right).Try())
		.Or(Token.EqualTo(SqlToken.Full).IgnoreThen(Token.EqualTo(SqlToken.Outer).OptionalOrDefault()).IgnoreThen(Token.EqualTo(SqlToken.Join)).Select(_ => JoinType.Full).Try())
		.Or(Token.EqualTo(SqlToken.Cross).IgnoreThen(Token.EqualTo(SqlToken.Join)).Select(_ => JoinType.Cross).Try())
		.Or(Token.EqualTo(SqlToken.Join).Select(_ => JoinType.Inner));

	// --- Single table reference (no joins) ---

	// --- UNNEST(expr) [AS alias] ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
	private static readonly TokenListParser<SqlToken, FromClause> UnnestParser =
		Token.EqualTo(SqlToken.Unnest)
			.IgnoreThen(Token.EqualTo(SqlToken.LParen))
			.IgnoreThen(SP.Ref(() => Expression!))
			.Then(expr => Token.EqualTo(SqlToken.RParen).IgnoreThen(
				Token.EqualTo(SqlToken.As).IgnoreThen(IdentifierOrKeyword)
					.Select(a => (string?)a).OptionalOrDefault()
			).Select(alias => (FromClause)new UnnestClause(expr, alias)));

	// --- (SELECT ...) [AS alias] in FROM ---
	private static readonly TokenListParser<SqlToken, FromClause> SubqueryFromParser =
		Token.EqualTo(SqlToken.LParen)
			.IgnoreThen(SP.Ref(() => SelectStmt!))
			.Then(sub => Token.EqualTo(SqlToken.RParen).IgnoreThen(
				// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#from_clause
				//   "Subqueries can have optional aliases: (SELECT ...) AS alias  or  (SELECT ...) alias"
				Token.EqualTo(SqlToken.As).IgnoreThen(IdentifierOrKeyword).Try()
					.Or(Identifier.Try())
					.Select(a => (string?)a).OptionalOrDefault()
			).Select(alias => (FromClause)new SubqueryFrom(sub, alias)));

	private static readonly TokenListParser<SqlToken, FromClause> BacktickTableReference =
		Token.EqualTo(SqlToken.BacktickIdentifier).Select(t =>
		{
			var parts = t.ToStringValue().Trim('`').Split('.');
			return parts.Length switch
			{
				// project.dataset.table — but if middle part is INFORMATION_SCHEMA, keep as dataset.IS.table
				3 when parts[1].Equals("INFORMATION_SCHEMA", StringComparison.OrdinalIgnoreCase)
					=> (DatasetId: (string?)parts[0], TableId: parts[1] + "." + parts[2]),
				3 => (DatasetId: (string?)parts[1], TableId: parts[2]),
				2 => (DatasetId: (string?)parts[0], TableId: parts[1]),
				_ => (DatasetId: (string?)null, TableId: parts[0]),
			};
		}).Then(tref =>
			Token.EqualTo(SqlToken.As).IgnoreThen(IdentifierOrKeyword)
				.Select(alias => (FromClause)new TableRef(tref.DatasetId, tref.TableId, alias))
				.Try()
			.Or(Identifier
				.Select(alias => (FromClause)new TableRef(tref.DatasetId, tref.TableId, alias))
				.Try())
			.Or(Constant((FromClause)new TableRef(tref.DatasetId, tref.TableId, null)))
		);

	private static readonly TokenListParser<SqlToken, FromClause> SingleTableRef =
		UnnestParser.Try().Or(SubqueryFromParser.Try()).Or(BacktickTableReference.Try()).Or(TableReference);

	// --- JOIN clause suffix ---

	// --- PIVOT clause ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#pivot_operator
	private static readonly TokenListParser<SqlToken, (SqlExpression Value, string? Alias)> PivotValueParser =
		SP.Ref(() => Expression!).Then(val =>
			Token.EqualTo(SqlToken.As).IgnoreThen(IdentifierOrKeyword)
				.Select(alias => (Value: val, Alias: (string?)alias))
				.Try()
				.Or(Constant((Value: val, Alias: (string?)null)))
		);

	// Parse: FUNC_NAME(expr) inside PIVOT — simplified aggregate parser
	private static readonly TokenListParser<SqlToken, AggregateCall> PivotAggParser =
		IdentifierOrKeyword.Then(name =>
			Token.EqualTo(SqlToken.LParen)
				.IgnoreThen(SP.Ref(() => Expression!))
				.Then(arg => Token.EqualTo(SqlToken.RParen).Select(_ =>
					new AggregateCall(name, arg, false)))
		);

	private static readonly TokenListParser<SqlToken, PivotClause> PivotParser =
		Token.EqualTo(SqlToken.Pivot)
			.IgnoreThen(Token.EqualTo(SqlToken.LParen))
			.IgnoreThen(
				PivotAggParser.Then(agg =>
					Token.EqualTo(SqlToken.For).IgnoreThen(
						IdentifierOrKeyword
					).Then(col =>
						Token.EqualTo(SqlToken.In)
							.IgnoreThen(Token.EqualTo(SqlToken.LParen))
							.IgnoreThen(PivotValueParser.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
							.Then(vals => Token.EqualTo(SqlToken.RParen).Select(_ =>
								new PivotClause(agg, new ColumnRef(null, col), vals.ToList())))
					)
				)
			).Then(pivot => Token.EqualTo(SqlToken.RParen).Select(_ => pivot));

	// --- UNPIVOT clause ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unpivot_operator
	// UNPIVOT(values_col FOR name_col IN (col1, col2, ...))
	private static readonly TokenListParser<SqlToken, UnpivotClause> UnpivotParser =
		Token.EqualTo(SqlToken.Unpivot)
			.IgnoreThen(Token.EqualTo(SqlToken.LParen))
			.IgnoreThen(
				IdentifierOrKeyword.Then(valCol =>
					Token.EqualTo(SqlToken.For).IgnoreThen(
						IdentifierOrKeyword
					).Then(nameCol =>
						Token.EqualTo(SqlToken.In)
							.IgnoreThen(Token.EqualTo(SqlToken.LParen))
							.IgnoreThen(IdentifierOrKeyword.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
							.Then(cols => Token.EqualTo(SqlToken.RParen).Select(_ =>
								new UnpivotClause(valCol, nameCol, cols.ToList())))
					)
				)
			).Then(unpivot => Token.EqualTo(SqlToken.RParen).Select(_ => unpivot));

	// --- TABLESAMPLE clause ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#tablesample_operator
	// TABLESAMPLE SYSTEM (N PERCENT)
	private static readonly TokenListParser<SqlToken, double> TablesampleParser =
		Token.EqualTo(SqlToken.Tablesample)
			.IgnoreThen(Identifier) // SYSTEM keyword — accept any identifier
			.IgnoreThen(Token.EqualTo(SqlToken.LParen))
			.IgnoreThen(Token.EqualTo(SqlToken.Number).Select(t => double.Parse(t.ToStringValue(), CultureInfo.InvariantCulture)))
			.Then(pct =>
				// Optionally consume PERCENT identifier
				Identifier.Try().Or(Constant(""))
					.IgnoreThen(Token.EqualTo(SqlToken.RParen))
					.Select(_ => pct)
			);

	private static readonly TokenListParser<SqlToken, FromClause> FromClauseParser =
		Token.EqualTo(SqlToken.From).IgnoreThen(
			SingleTableRef.Then(left =>
				JoinTypeParser.Then(joinType =>
					SingleTableRef.Then(right =>
						(joinType == JoinType.Cross
							? Constant((SqlExpression?)null)
							: Token.EqualTo(SqlToken.On).IgnoreThen(Expression).Select(e => (SqlExpression?)e)
						).Select(on => (JoinType: joinType, Right: right, On: on))
					)
				).AtLeastOnce()
				.Select(joins =>
				{
					FromClause result = left;
					foreach (var j in joins)
						result = new JoinClause(result, j.JoinType, j.Right, j.On);
					return result;
				})
				.Try()
				.Or(Constant(left))
			).Then(source =>
				PivotParser.Select(piv => (FromClause)new PivotFrom(source, piv)).Try()
				.Or(UnpivotParser.Select(up => (FromClause)new UnpivotFrom(source, up)).Try())
				.Or(TablesampleParser.Select(pct => (FromClause)new TablesampleFrom(source, pct)).Try())
				.Or(Constant(source))
			)
		);


	// --- WHERE clause ---

	private static readonly TokenListParser<SqlToken, SqlExpression> WhereClause =
		Token.EqualTo(SqlToken.Where).IgnoreThen(Expression);

	// --- GROUP BY clause ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause

	// ROLLUP(expr, ...) inside GROUP BY
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#rollup
	private static readonly TokenListParser<SqlToken, SqlExpression> RollupParser =
		Token.EqualTo(SqlToken.Rollup)
			.IgnoreThen(Token.EqualTo(SqlToken.LParen))
			.IgnoreThen(SP.Ref(() => Expression!).ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
			.Then(exprs => Token.EqualTo(SqlToken.RParen).Select(_ => (SqlExpression)new RollupExpr(exprs.ToList())));

	private static readonly TokenListParser<SqlToken, SqlExpression> GroupByExprParser =
		RollupParser.Try().Or(SP.Ref(() => Expression!));

	private static readonly TokenListParser<SqlToken, IReadOnlyList<SqlExpression>> GroupByClause =
		Token.EqualTo(SqlToken.Group)
			.IgnoreThen(Token.EqualTo(SqlToken.By))
			.IgnoreThen(GroupByExprParser.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
			.Select(items => (IReadOnlyList<SqlExpression>)items.ToList());

	// --- HAVING clause ---

	private static readonly TokenListParser<SqlToken, SqlExpression> HavingClause =
		Token.EqualTo(SqlToken.Having).IgnoreThen(Expression);

	// --- QUALIFY clause ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#qualify_clause
	private static readonly TokenListParser<SqlToken, SqlExpression> QualifyClause =
		Token.EqualTo(SqlToken.Qualify).IgnoreThen(Expression);

	// --- ORDER BY clause ---

    private static readonly TokenListParser<SqlToken, OrderByItem> OrderByItemParser =
        SP.Ref(() => Expression!).Then(expr =>
            Token.EqualTo(SqlToken.Desc).Select(_ => new OrderByItem(expr, true))
                .Try()
            .Or(Token.EqualTo(SqlToken.Asc).Select(_ => new OrderByItem(expr, false)).Try())
            .Or(Constant(new OrderByItem(expr, false)))
        );

	private static readonly TokenListParser<SqlToken, IReadOnlyList<OrderByItem>> OrderByClause =
		Token.EqualTo(SqlToken.Order)
			.IgnoreThen(Token.EqualTo(SqlToken.By))
			.IgnoreThen(SP.Ref(() => OrderByItemParser!).ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
			.Select(items => (IReadOnlyList<OrderByItem>)items.ToList());

	// --- LIMIT / OFFSET ---

	private static readonly TokenListParser<SqlToken, int> LimitClause =
		Token.EqualTo(SqlToken.Limit).IgnoreThen(
			Token.EqualTo(SqlToken.Number).Select(t => int.Parse(t.ToStringValue())));

	private static readonly TokenListParser<SqlToken, int> OffsetClause =
		Token.EqualTo(SqlToken.Offset).IgnoreThen(
			Token.EqualTo(SqlToken.Number).Select(t => int.Parse(t.ToStringValue())));

	// --- Full SELECT statement ---

	private static readonly TokenListParser<SqlToken, SelectStatement> SelectStmt =
		Token.EqualTo(SqlToken.Select).IgnoreThen(
			Token.EqualTo(SqlToken.Distinct).Select(_ => true).OptionalOrDefault(false)
		).Then(distinct =>
			SelectItemParser.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma))
				.Select(cols => (Distinct: distinct, Columns: cols))
		).Then(sel =>
			FromClauseParser.Select(f => (FromClause?)f).OptionalOrDefault().Select(from =>
				(sel.Distinct, sel.Columns, From: from))
		).Then(sel =>
			WhereClause.Select(w => (SqlExpression?)w).OptionalOrDefault().Select(where =>
				(sel.Distinct, sel.Columns, sel.From, Where: where))
		).Then(sel =>
			GroupByClause.Select(g => (IReadOnlyList<SqlExpression>?)g).OptionalOrDefault().Select(groupBy =>
				(sel.Distinct, sel.Columns, sel.From, sel.Where, GroupBy: groupBy))
		).Then(sel =>
			HavingClause.Select(h => (SqlExpression?)h).OptionalOrDefault().Select(having =>
				(sel.Distinct, sel.Columns, sel.From, sel.Where, sel.GroupBy, Having: having))
		).Then(sel =>
			QualifyClause.Select(q => (SqlExpression?)q).OptionalOrDefault().Select(qualify =>
				(sel.Distinct, sel.Columns, sel.From, sel.Where, sel.GroupBy, sel.Having, Qualify: qualify))
		).Then(sel =>
			OrderByClause.Select(o => (IReadOnlyList<OrderByItem>?)o).OptionalOrDefault().Select(orderBy =>
				(sel.Distinct, sel.Columns, sel.From, sel.Where, sel.GroupBy, sel.Having, sel.Qualify, OrderBy: orderBy))
		).Then(sel =>
			LimitClause.Select(l => (int?)l).OptionalOrDefault().Select(limit =>
				(sel.Distinct, sel.Columns, sel.From, sel.Where, sel.GroupBy, sel.Having, sel.Qualify, sel.OrderBy, Limit: limit))
		).Then(sel =>
			OffsetClause.Select(o => (int?)o).OptionalOrDefault().Select(offset =>
				new SelectStatement(
					sel.Distinct,
					sel.Columns.ToList(),
					sel.From,
					sel.Where,
					sel.GroupBy,
					sel.Having,
					sel.OrderBy,
					sel.Limit,
					offset,
					Qualify: sel.Qualify
				)
			)
		);

	// --- CTE definition: name AS (SELECT ...) ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause
	private static readonly TokenListParser<SqlToken, CteDefinition> CteDefParser =
		IdentifierOrKeyword.Then(name =>
			Token.EqualTo(SqlToken.As)
				.IgnoreThen(Token.EqualTo(SqlToken.LParen))
				.IgnoreThen(SelectStmt.Then(baseSel =>
					// Handle UNION ALL for recursive CTEs
					Token.EqualTo(SqlToken.Union)
						.IgnoreThen(Token.EqualTo(SqlToken.All))
						.IgnoreThen(SelectStmt)
						.Select(recSel => new CteDefinition(name, baseSel, recSel))
						.Try()
						.Or(Constant(new CteDefinition(name, baseSel)))
				))
				.Then(cte => Token.EqualTo(SqlToken.RParen).Select(_ => cte))
		);

	// --- WITH cte1 AS (...), cte2 AS (...) SELECT ... ---
	private static readonly TokenListParser<SqlToken, SelectStatement> WithSelectStmt =
		Token.EqualTo(SqlToken.With)
			.IgnoreThen(Token.EqualTo(SqlToken.Recursive).Try().Or(Constant(new Token<SqlToken>())))
			.IgnoreThen(CteDefParser.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
			.Then(ctes => SelectStmt.Select(sel =>
				new SelectStatement(sel.Distinct, sel.Columns, sel.From, sel.Where,
					sel.GroupBy, sel.Having, sel.OrderBy, sel.Limit, sel.Offset, ctes.ToList(), sel.Qualify)));

	// --- Top-level statement with optional set operations ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
	private static readonly TokenListParser<SqlToken, SqlStatement> TopLevelStatement =
		SP.Ref(() => DdlStatement!).Try()
		.Or(SP.Ref(() => DmlStatement!).Try())
		.Or(WithSelectStmt.Try().Or(SelectStmt).Then(left =>
			// UNION ALL
			Token.EqualTo(SqlToken.Union).IgnoreThen(Token.EqualTo(SqlToken.All))
				.IgnoreThen(SelectStmt)
				.Select(right => (SqlStatement)new SetOperationStatement(left, SetOperationType.Union, true, right))
				.Try()
			// UNION DISTINCT
			.Or(Token.EqualTo(SqlToken.Union).IgnoreThen(Token.EqualTo(SqlToken.Distinct))
				.IgnoreThen(SelectStmt)
				.Select(right => (SqlStatement)new SetOperationStatement(left, SetOperationType.Union, false, right))
				.Try())
			// EXCEPT DISTINCT
			.Or(Token.EqualTo(SqlToken.Except).IgnoreThen(Token.EqualTo(SqlToken.Distinct))
				.IgnoreThen(SelectStmt)
				.Select(right => (SqlStatement)new SetOperationStatement(left, SetOperationType.Except, false, right))
				.Try())
			// INTERSECT DISTINCT
			.Or(Token.EqualTo(SqlToken.Intersect).IgnoreThen(Token.EqualTo(SqlToken.Distinct))
				.IgnoreThen(SelectStmt)
				.Select(right => (SqlStatement)new SetOperationStatement(left, SetOperationType.Intersect, false, right))
				.Try())
			// No set operation
			.Or(Constant((SqlStatement)left))
		));

	// --- DML Parsers ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax

	// INSERT INTO table (col1, col2) VALUES (v1, v2), (v3, v4)
	private static readonly TokenListParser<SqlToken, SqlStatement> InsertValuesStmt =
		Token.EqualTo(SqlToken.Insert).IgnoreThen(Token.EqualTo(SqlToken.Into))
			.IgnoreThen(IdentifierOrKeyword)
			.Then(table =>
				// Optional column list
				Token.EqualTo(SqlToken.LParen)
					.IgnoreThen(IdentifierOrKeyword.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
					.Then(cols => Token.EqualTo(SqlToken.RParen).Select(_ => (IReadOnlyList<string>?)cols.ToList()))
					.OptionalOrDefault()
				.Then(cols =>
					Token.EqualTo(SqlToken.Values).IgnoreThen(
						// Each row: (expr, expr, ...)
						Token.EqualTo(SqlToken.LParen)
							.IgnoreThen(SP.Ref(() => Expression!).ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
							.Then(vals => Token.EqualTo(SqlToken.RParen).Select(_ => (IReadOnlyList<SqlExpression>)vals.ToList()))
						.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma))
					).Select(rows => (SqlStatement)new InsertValuesStatement(table, cols, rows.ToList()))
				)
			);

	// INSERT INTO table (cols) SELECT ...
	private static readonly TokenListParser<SqlToken, SqlStatement> InsertSelectStmt =
		Token.EqualTo(SqlToken.Insert).IgnoreThen(Token.EqualTo(SqlToken.Into))
			.IgnoreThen(IdentifierOrKeyword)
			.Then(table =>
				Token.EqualTo(SqlToken.LParen)
					.IgnoreThen(IdentifierOrKeyword.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
					.Then(cols => Token.EqualTo(SqlToken.RParen).Select(_ => (IReadOnlyList<string>?)cols.ToList()))
					.OptionalOrDefault()
				.Then(cols => SelectStmt.Select(q => (SqlStatement)new InsertSelectStatement(table, cols, q)))
			);

	// UPDATE table [alias] SET col=expr, ... WHERE condition
	private static readonly TokenListParser<SqlToken, SqlStatement> UpdateStmt =
		Token.EqualTo(SqlToken.Update).IgnoreThen(IdentifierOrKeyword)
			.Then(table =>
				// optional alias (identifier that is not SET)
				Token.EqualTo(SqlToken.Identifier).Select(t => (string?)t.ToStringValue()).OptionalOrDefault()
			.Then(alias =>
				Token.EqualTo(SqlToken.Set).IgnoreThen(
					IdentifierOrKeyword.Then(col =>
						Token.EqualTo(SqlToken.Eq).IgnoreThen(SP.Ref(() => Expression!))
							.Select(val => (col, val))
					).ManyDelimitedBy(Token.EqualTo(SqlToken.Comma))
				).Then(assignments =>
					Token.EqualTo(SqlToken.Where).IgnoreThen(SP.Ref(() => Expression!))
						.Select(where => (SqlStatement)new UpdateStatement(table, alias, assignments.ToList(), where))
				)
			));

	// DELETE FROM table [alias] WHERE condition
	private static readonly TokenListParser<SqlToken, SqlStatement> DeleteStmt =
		Token.EqualTo(SqlToken.Delete).IgnoreThen(Token.EqualTo(SqlToken.From))
			.IgnoreThen(IdentifierOrKeyword)
			.Then(table =>
				Token.EqualTo(SqlToken.Identifier).Select(t => (string?)t.ToStringValue()).OptionalOrDefault()
			.Then(alias =>
				Token.EqualTo(SqlToken.Where).IgnoreThen(SP.Ref(() => Expression!))
					.Select(where => (SqlStatement)new DeleteStatement(table, alias, where))
			));

	// MERGE INTO target [alias] USING source [alias] ON condition WHEN ...
	private static readonly TokenListParser<SqlToken, (string Col, SqlExpression Val)> AssignmentParser =
		IdentifierOrKeyword.Then(col =>
			Token.EqualTo(SqlToken.Eq).IgnoreThen(SP.Ref(() => Expression!))
				.Select(val => (col, val)));

	private static readonly TokenListParser<SqlToken, MergeWhenClause> MergeWhenMatchedParser =
		Token.EqualTo(SqlToken.When).IgnoreThen(Token.EqualTo(SqlToken.Matched))
			.Or(Token.EqualTo(SqlToken.Table))
			.Or(Token.EqualTo(SqlToken.View))
			.Or(Token.EqualTo(SqlToken.Column))
			.Or(Token.EqualTo(SqlToken.Replace))
			.Or(Token.EqualTo(SqlToken.Add))
			.Or(Token.EqualTo(SqlToken.Rename))
			.Or(Token.EqualTo(SqlToken.To))
			.Or(Token.EqualTo(SqlToken.If))
			.IgnoreThen(
				Token.EqualTo(SqlToken.And).IgnoreThen(SP.Ref(() => Expression!))
					.Select(e => (SqlExpression?)e).OptionalOrDefault()
			).Then(andCond =>
				Token.EqualTo(SqlToken.Then)
					.IgnoreThen(
						// DELETE
						Token.EqualTo(SqlToken.Delete)
							.Select(_ => (MergeWhenClause)new MergeWhenMatched(andCond, null, true))
						.Try()
						// UPDATE SET col=val, ...
						.Or(Token.EqualTo(SqlToken.Update).IgnoreThen(Token.EqualTo(SqlToken.Set))
							.IgnoreThen(AssignmentParser.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
							.Select(a => (MergeWhenClause)new MergeWhenMatched(andCond, a.ToList(), false)))
					)
			);

	private static readonly TokenListParser<SqlToken, MergeWhenClause> MergeWhenNotMatchedParser =
		Token.EqualTo(SqlToken.When).IgnoreThen(Token.EqualTo(SqlToken.Not))
			.IgnoreThen(Token.EqualTo(SqlToken.Matched))
			.Or(Token.EqualTo(SqlToken.Table))
			.Or(Token.EqualTo(SqlToken.View))
			.Or(Token.EqualTo(SqlToken.Column))
			.Or(Token.EqualTo(SqlToken.Replace))
			.Or(Token.EqualTo(SqlToken.Add))
			.Or(Token.EqualTo(SqlToken.Rename))
			.Or(Token.EqualTo(SqlToken.To))
			.Or(Token.EqualTo(SqlToken.If))
			.IgnoreThen(
				Token.EqualTo(SqlToken.And).IgnoreThen(SP.Ref(() => Expression!))
					.Select(e => (SqlExpression?)e).OptionalOrDefault()
			).Then(andCond =>
				Token.EqualTo(SqlToken.Then).IgnoreThen(Token.EqualTo(SqlToken.Insert))
					.IgnoreThen(
						Token.EqualTo(SqlToken.LParen)
							.IgnoreThen(IdentifierOrKeyword.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
							.Then(cols => Token.EqualTo(SqlToken.RParen).Select(_ => (IReadOnlyList<string>?)cols.ToList()))
							.OptionalOrDefault()
					).Then(cols =>
						Token.EqualTo(SqlToken.Values)
							.IgnoreThen(Token.EqualTo(SqlToken.LParen))
							.IgnoreThen(SP.Ref(() => Expression!).ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
							.Then(vals => Token.EqualTo(SqlToken.RParen)
								.Select(_ => (MergeWhenClause)new MergeWhenNotMatched(andCond, cols, vals.ToList())))
					)
			);

	private static readonly TokenListParser<SqlToken, MergeWhenClause> MergeWhenParser =
		MergeWhenMatchedParser.Try().Or(MergeWhenNotMatchedParser);

	private static readonly TokenListParser<SqlToken, SqlStatement> MergeStmt =
		Token.EqualTo(SqlToken.Merge).IgnoreThen(
			Token.EqualTo(SqlToken.Into).OptionalOrDefault()
		).IgnoreThen(IdentifierOrKeyword)
			.Then(target =>
				// optional alias
				Token.EqualTo(SqlToken.As).IgnoreThen(IdentifierOrKeyword)
					.Select(a => (string?)a).Try()
					.Or(Token.EqualTo(SqlToken.Identifier).Select(t => (string?)t.ToStringValue()).Try())
					.OptionalOrDefault()
			.Then(targetAlias =>
				Token.EqualTo(SqlToken.Using).IgnoreThen(SingleTableRef)
			.Then(source =>
				// optional source alias
				Token.EqualTo(SqlToken.As).IgnoreThen(IdentifierOrKeyword)
					.Select(a => (string?)a).Try()
					.Or(Token.EqualTo(SqlToken.Identifier).Select(t => (string?)t.ToStringValue()).Try())
					.OptionalOrDefault()
			.Then(sourceAlias =>
				Token.EqualTo(SqlToken.On).IgnoreThen(SP.Ref(() => Expression!))
			.Then(onExpr =>
				MergeWhenParser.AtLeastOnce()
					.Select(whens => (SqlStatement)new MergeStatement(target, targetAlias, source, sourceAlias, onExpr, whens.ToList()))
			)))));

	// --- DML top-level dispatcher ---
	private static readonly TokenListParser<SqlToken, SqlStatement> DmlStatement =
		InsertValuesStmt.Try()
		.Or(InsertSelectStmt.Try())
		.Or(UpdateStmt.Try())
		.Or(DeleteStmt.Try())
		.Or(MergeStmt.Try())
		.Or(SP.Ref(() => TruncateTableStmt!).Try());

	// TRUNCATE TABLE name
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#truncate_table_statement
	//   "Deletes all rows from the named table."
	private static readonly TokenListParser<SqlToken, SqlStatement> TruncateTableStmt =
		Token.EqualTo(SqlToken.Truncate).IgnoreThen(Token.EqualTo(SqlToken.Table))
			.IgnoreThen(SP.Ref(() => TableNameParser!))
			.Select(tbl => (SqlStatement)new TruncateTableStatement(tbl.TableName, tbl.DatasetId));

	// --- DDL Parsers ---
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language

	// Table name: optionally dataset.table
	private static readonly TokenListParser<SqlToken, (string? DatasetId, string TableName)> TableNameParser =
		IdentifierOrKeyword.Then(first =>
			Token.EqualTo(SqlToken.Dot).IgnoreThen(IdentifierOrKeyword)
				.Select(second => ((string?)first, second))
				.Try()
				.Or(Constant(((string?)null, first)))
		);

	// Column definition: name TYPE
	private static readonly TokenListParser<SqlToken, (string Name, string Type)> ColumnDefParser =
		IdentifierOrKeyword.Then(name =>
			IdentifierOrKeyword.Select(type => (name, type.ToUpperInvariant())));

	// CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name (col TYPE, ...) | AS SELECT | LIKE src | COPY src | CLONE src
	private static readonly TokenListParser<SqlToken, SqlStatement> CreateTableStmt =
		Token.EqualTo(SqlToken.Create)
			.IgnoreThen(
				Token.EqualTo(SqlToken.Or).IgnoreThen(Token.EqualTo(SqlToken.Replace))
					.Select(_ => true).OptionalOrDefault(false)
			).Then(orReplace =>
				Token.EqualTo(SqlToken.Table)
					.IgnoreThen(
						Token.EqualTo(SqlToken.If).IgnoreThen(Token.EqualTo(SqlToken.Not))
							.IgnoreThen(Token.EqualTo(SqlToken.Exists))
							.Select(_ => true).OptionalOrDefault(false)
					).Then(ifNotExists =>
						TableNameParser.Then(tbl =>
							// AS SELECT
							Token.EqualTo(SqlToken.As).IgnoreThen(SelectStmt)
								.Select(q => (SqlStatement)new CreateTableAsSelectStatement(tbl.TableName, tbl.DatasetId, q, orReplace))
								.Try()
							// LIKE source
							// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_like
							.Or(Token.EqualTo(SqlToken.Like)
								.IgnoreThen(TableNameParser)
								.Select(src => (SqlStatement)new CreateTableLikeStatement(tbl.TableName, tbl.DatasetId, src.TableName, src.DatasetId))
								.Try())
							// COPY source
							// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_copy
							.Or(IdentifierMatching("COPY")
								.IgnoreThen(TableNameParser)
								.Select(src => (SqlStatement)new CreateTableCopyStatement(tbl.TableName, tbl.DatasetId, src.TableName, src.DatasetId))
								.Try())
							// CLONE source
							// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_clone
							.Or(Token.EqualTo(SqlToken.Clone)
								.IgnoreThen(TableNameParser)
								.Select(src => (SqlStatement)new CreateTableCopyStatement(tbl.TableName, tbl.DatasetId, src.TableName, src.DatasetId))
								.Try())
							// (col TYPE, ...)
							.Or(Token.EqualTo(SqlToken.LParen)
								.IgnoreThen(ColumnDefParser.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
								.Then(cols => Token.EqualTo(SqlToken.RParen)
									.Select(_ => (SqlStatement)new CreateTableStatement(tbl.TableName, tbl.DatasetId, cols.ToList(), orReplace, ifNotExists))))
						)
					)
			);

	// DROP TABLE [IF EXISTS] name
	private static readonly TokenListParser<SqlToken, SqlStatement> DropTableStmt =
		Token.EqualTo(SqlToken.Drop).IgnoreThen(Token.EqualTo(SqlToken.Table))
			.IgnoreThen(
				Token.EqualTo(SqlToken.If).IgnoreThen(Token.EqualTo(SqlToken.Exists))
					.Select(_ => true).OptionalOrDefault(false)
			).Then(ifExists =>
				TableNameParser.Select(tbl =>
					(SqlStatement)new DropTableStatement(tbl.TableName, tbl.DatasetId, ifExists)));

	// ALTER TABLE name ADD COLUMN col TYPE / DROP COLUMN col / RENAME TO name / ALTER COLUMN ... / SET OPTIONS (...)
	private static readonly TokenListParser<SqlToken, SqlStatement> AlterTableStmt =
		Token.EqualTo(SqlToken.Alter).IgnoreThen(Token.EqualTo(SqlToken.Table))
			.IgnoreThen(TableNameParser)
			.Then(tbl =>
				// ADD COLUMN col TYPE
				Token.EqualTo(SqlToken.Add)
					.IgnoreThen(Token.EqualTo(SqlToken.Column).OptionalOrDefault())
					.IgnoreThen(ColumnDefParser)
					.Select(cd => (SqlStatement)new AlterTableStatement(tbl.TableName, tbl.DatasetId, new AddColumnAction(cd.Name, cd.Type)))
					.Try()
				// DROP COLUMN col
				.Or(Token.EqualTo(SqlToken.Drop)
					.IgnoreThen(Token.EqualTo(SqlToken.Column).OptionalOrDefault())
					.IgnoreThen(IdentifierOrKeyword)
					.Select(col => (SqlStatement)new AlterTableStatement(tbl.TableName, tbl.DatasetId, new DropColumnAction(col)))
					.Try())
				// RENAME TO name
				.Or(Token.EqualTo(SqlToken.Rename).IgnoreThen(Token.EqualTo(SqlToken.To))
					.IgnoreThen(IdentifierOrKeyword)
					.Select(newName => (SqlStatement)new AlterTableStatement(tbl.TableName, tbl.DatasetId, new RenameTableAction(newName)))
					.Try())
				// ALTER COLUMN col SET DATA TYPE type
				// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_set_data_type
				.Or(Token.EqualTo(SqlToken.Alter).IgnoreThen(Token.EqualTo(SqlToken.Column))
					.IgnoreThen(IdentifierOrKeyword)
					.Then(col =>
						// SET DATA TYPE type
						Token.EqualTo(SqlToken.Set)
							.IgnoreThen(IdentifierMatching("DATA"))
							.IgnoreThen(IdentifierMatching("TYPE"))
							.IgnoreThen(IdentifierOrKeyword)
							.Select(newType => (SqlStatement)new AlterTableStatement(tbl.TableName, tbl.DatasetId, new AlterColumnSetDataTypeAction(col, newType.ToUpperInvariant())))
							.Try()
						// SET DEFAULT expr (we capture a single-token expression for simplicity)
						// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_set_default
						.Or(Token.EqualTo(SqlToken.Set).IgnoreThen(IdentifierMatching("DEFAULT"))
							.IgnoreThen(SP.Ref(() => Expression!))
							.Select(expr => {
								var defaultStr = expr switch
								{
									LiteralExpr { Value: string s } => $"'{s}'",
									LiteralExpr lit => lit.Value?.ToString() ?? "NULL",
									_ => expr.ToString() ?? "NULL"
								};
								return (SqlStatement)new AlterTableStatement(tbl.TableName, tbl.DatasetId, new AlterColumnSetDefaultAction(col, defaultStr));
							})
							.Try())
						// DROP DEFAULT
						// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_drop_default
						.Or(Token.EqualTo(SqlToken.Drop).IgnoreThen(IdentifierMatching("DEFAULT"))
							.Select(_ => (SqlStatement)new AlterTableStatement(tbl.TableName, tbl.DatasetId, new AlterColumnDropDefaultAction(col)))
							.Try())
						// DROP NOT NULL
						// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_column_drop_not_null
						.Or(Token.EqualTo(SqlToken.Drop).IgnoreThen(Token.EqualTo(SqlToken.Not))
							.IgnoreThen(Token.EqualTo(SqlToken.Null))
							.Select(_ => (SqlStatement)new AlterTableStatement(tbl.TableName, tbl.DatasetId, new AlterColumnDropNotNullAction(col))))
					)
					.Try())
				// SET OPTIONS (...)
				// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_table_set_options
				.Or(Token.EqualTo(SqlToken.Set).IgnoreThen(Token.EqualTo(SqlToken.Options))
					.IgnoreThen(Token.EqualTo(SqlToken.LParen))
					.IgnoreThen(ConsumeUntilCloseParen)
					.Then(text => Token.EqualTo(SqlToken.RParen)
						.Select(_ => (SqlStatement)new AlterTableStatement(tbl.TableName, tbl.DatasetId, new SetOptionsAction(text)))))
			);

	// CREATE [OR REPLACE] VIEW name AS SELECT ...
	private static readonly TokenListParser<SqlToken, SqlStatement> CreateViewStmt =
		Token.EqualTo(SqlToken.Create)
			.IgnoreThen(
				Token.EqualTo(SqlToken.Or).IgnoreThen(Token.EqualTo(SqlToken.Replace))
					.Select(_ => true).OptionalOrDefault(false)
			).Then(orReplace =>
				Token.EqualTo(SqlToken.View).IgnoreThen(TableNameParser)
					.Then(tbl => Token.EqualTo(SqlToken.As).IgnoreThen(SelectStmt)
						.Select(q => (SqlStatement)new CreateViewStatement(tbl.TableName, tbl.DatasetId, q, orReplace)))
			);

	// DROP VIEW [IF EXISTS] name
	private static readonly TokenListParser<SqlToken, SqlStatement> DropViewStmt =
		Token.EqualTo(SqlToken.Drop).IgnoreThen(Token.EqualTo(SqlToken.View))
			.IgnoreThen(
				Token.EqualTo(SqlToken.If).IgnoreThen(Token.EqualTo(SqlToken.Exists))
					.Select(_ => true).OptionalOrDefault(false)
			).Then(ifExists =>
				TableNameParser.Select(tbl =>
					(SqlStatement)new DropViewStatement(tbl.TableName, tbl.DatasetId, ifExists)));

	// CREATE SCHEMA [IF NOT EXISTS] name
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement
	private static readonly TokenListParser<SqlToken, SqlStatement> CreateSchemaStmt =
		Token.EqualTo(SqlToken.Create).IgnoreThen(Token.EqualTo(SqlToken.Schema))
			.IgnoreThen(
				Token.EqualTo(SqlToken.If).IgnoreThen(Token.EqualTo(SqlToken.Not))
					.IgnoreThen(Token.EqualTo(SqlToken.Exists))
					.Select(_ => true).OptionalOrDefault(false)
			).Then(ifNotExists =>
				IdentifierOrKeyword.Select(name =>
					(SqlStatement)new CreateSchemaStatement(name, ifNotExists)));

	// DROP SCHEMA [IF EXISTS] name
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#drop_schema_statement
	private static readonly TokenListParser<SqlToken, SqlStatement> DropSchemaStmt =
		Token.EqualTo(SqlToken.Drop).IgnoreThen(Token.EqualTo(SqlToken.Schema))
			.IgnoreThen(
				Token.EqualTo(SqlToken.If).IgnoreThen(Token.EqualTo(SqlToken.Exists))
					.Select(_ => true).OptionalOrDefault(false)
			).Then(ifExists =>
				IdentifierOrKeyword.Select(name =>
					(SqlStatement)new DropSchemaStatement(name, ifExists)));

	// CREATE SNAPSHOT TABLE name CLONE source
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_snapshot_table
	private static readonly TokenListParser<SqlToken, SqlStatement> CreateSnapshotTableStmt =
		Token.EqualTo(SqlToken.Create).IgnoreThen(Token.EqualTo(SqlToken.Snapshot))
			.IgnoreThen(Token.EqualTo(SqlToken.Table))
			.IgnoreThen(TableNameParser)
			.Then(tbl =>
				Token.EqualTo(SqlToken.Clone).IgnoreThen(TableNameParser)
					.Select(src => (SqlStatement)new CreateTableCopyStatement(tbl.TableName, tbl.DatasetId, src.TableName, src.DatasetId)));

	// CREATE EXTERNAL TABLE name (cols) — stub: creates a regular table
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_external_table
	private static readonly TokenListParser<SqlToken, SqlStatement> CreateExternalTableStmt =
		Token.EqualTo(SqlToken.Create).IgnoreThen(Token.EqualTo(SqlToken.External))
			.IgnoreThen(Token.EqualTo(SqlToken.Table))
			.IgnoreThen(
				Token.EqualTo(SqlToken.If).IgnoreThen(Token.EqualTo(SqlToken.Not))
					.IgnoreThen(Token.EqualTo(SqlToken.Exists))
					.Select(_ => true).OptionalOrDefault(false)
			).IgnoreThen(TableNameParser)
			.Then(tbl =>
				Token.EqualTo(SqlToken.LParen)
					.IgnoreThen(ColumnDefParser.ManyDelimitedBy(Token.EqualTo(SqlToken.Comma)))
					.Then(cols => Token.EqualTo(SqlToken.RParen)
						.Select(_ => (SqlStatement)new CreateTableStatement(tbl.TableName, tbl.DatasetId, cols.ToList(), false, false))));

	// DROP EXTERNAL TABLE [IF EXISTS] name — treated like regular DROP TABLE
	private static readonly TokenListParser<SqlToken, SqlStatement> DropExternalTableStmt =
		Token.EqualTo(SqlToken.Drop).IgnoreThen(Token.EqualTo(SqlToken.External))
			.IgnoreThen(Token.EqualTo(SqlToken.Table))
			.IgnoreThen(
				Token.EqualTo(SqlToken.If).IgnoreThen(Token.EqualTo(SqlToken.Exists))
					.Select(_ => true).OptionalOrDefault(false)
			).Then(ifExists =>
				TableNameParser.Select(tbl =>
					(SqlStatement)new DropTableStatement(tbl.TableName, tbl.DatasetId, ifExists)));

	// CREATE MATERIALIZED VIEW name AS SELECT ... — treat as regular view
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_materialized_view
	private static readonly TokenListParser<SqlToken, SqlStatement> CreateMaterializedViewStmt =
		Token.EqualTo(SqlToken.Create)
			.IgnoreThen(
				Token.EqualTo(SqlToken.Or).IgnoreThen(Token.EqualTo(SqlToken.Replace))
					.Select(_ => true).OptionalOrDefault(false)
			).Then(orReplace =>
				Token.EqualTo(SqlToken.Materialized).IgnoreThen(Token.EqualTo(SqlToken.View))
					.IgnoreThen(TableNameParser)
					.Then(tbl => Token.EqualTo(SqlToken.As).IgnoreThen(SelectStmt)
						.Select(q => (SqlStatement)new CreateViewStatement(tbl.TableName, tbl.DatasetId, q, orReplace))));

	// DROP MATERIALIZED VIEW [IF EXISTS] name
	private static readonly TokenListParser<SqlToken, SqlStatement> DropMaterializedViewStmt =
		Token.EqualTo(SqlToken.Drop).IgnoreThen(Token.EqualTo(SqlToken.Materialized))
			.IgnoreThen(Token.EqualTo(SqlToken.View))
			.IgnoreThen(
				Token.EqualTo(SqlToken.If).IgnoreThen(Token.EqualTo(SqlToken.Exists))
					.Select(_ => true).OptionalOrDefault(false)
			).Then(ifExists =>
				TableNameParser.Select(tbl =>
					(SqlStatement)new DropViewStatement(tbl.TableName, tbl.DatasetId, ifExists)));

	// --- DDL dispatcher ---
	private static readonly TokenListParser<SqlToken, SqlStatement> DdlStatement =
		CreateSnapshotTableStmt.Try()
		.Or(CreateExternalTableStmt.Try())
		.Or(CreateMaterializedViewStmt.Try())
		.Or(CreateSchemaStmt.Try())
		.Or(CreateViewStmt.Try())
		.Or(CreateTableStmt.Try())
		.Or(DropExternalTableStmt.Try())
		.Or(DropMaterializedViewStmt.Try())
		.Or(DropSchemaStmt.Try())
		.Or(DropTableStmt.Try())
		.Or(DropViewStmt.Try())
		.Or(AlterTableStmt.Try());
}
