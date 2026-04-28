using Superpower;
using Superpower.Model;
using Superpower.Parsers;
using Superpower.Tokenizers;

namespace BigQuery.InMemoryEmulator.SqlEngine;

// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
//   GoogleSQL lexical structure: identifiers, keywords, literals, operators.

internal enum SqlToken
{
	// Keywords
	Select, From, Where, And, Or, Not, As, Is, Null,
	Order, By, Asc, Desc, Limit, Offset, Distinct,
	True, False, Between, In, Like, Case, When, Then, Else, End,
	Cast, SafeCast,
	Join, Inner, Left, Right, Full, Outer, Cross,
	Group, Having, On,
	With, Unnest, Exists, Over, Partition, Rows, Range,
	Unbounded, Preceding, Following, Current, Row,
	Union, All, Except, Intersect,
	Insert, Into, Values, Update, Set, Delete, Merge, Using, Matched,
	Create, Drop, Alter, Table, View, If, Replace, Column, Add, Rename, To,
	Qualify, Pivot, Unpivot, Tablesample, Rollup, Recursive, For,
	Truncate, Schema, Snapshot, Clone, External, Procedure, Materialized,
	Function, Policy, Index, Grant, Options,

	// Identifiers & literals
	Identifier,
	Number,
	StringLiteral,
	Parameter,       // @name
	BacktickIdentifier, // `project.dataset.table`

	// Operators
	Star, Comma, Dot, LParen, RParen,
	LBracket, RBracket,  // [ ]
	Eq, Neq, Lt, Gt, Lte, Gte,
	Plus, Minus, Slash, Percent,
	Pipe,            // ||
	Arrow,           // -> (lambda)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#bitwise_operators
	Ampersand,       // & (bitwise AND)
	Caret,           // ^ (bitwise XOR)
	Tilde,           // ~ (bitwise NOT)
	BitOr,           // | (bitwise OR, single pipe)
	ShiftLeft,       // <<
	ShiftRight,      // >>
}

internal static class SqlTokenizer
{
	public static Tokenizer<SqlToken> Instance { get; } = new TokenizerBuilder<SqlToken>()
		// Ignore whitespace
		.Ignore(Span.WhiteSpace)
		// Multi-char operators first
		.Match(Span.EqualTo("!="), SqlToken.Neq)
		.Match(Span.EqualTo("<>"), SqlToken.Neq)
		.Match(Span.EqualTo("<="), SqlToken.Lte)
		.Match(Span.EqualTo(">="), SqlToken.Gte)
		.Match(Span.EqualTo("||"), SqlToken.Pipe)
		.Match(Span.EqualTo("->"), SqlToken.Arrow)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#bitwise_operators
		.Match(Span.EqualTo("<<"), SqlToken.ShiftLeft)
		.Match(Span.EqualTo(">>"), SqlToken.ShiftRight)
		// Single-char operators
		.Match(Character.EqualTo('='), SqlToken.Eq)
		.Match(Character.EqualTo('<'), SqlToken.Lt)
		.Match(Character.EqualTo('>'), SqlToken.Gt)
		.Match(Character.EqualTo('*'), SqlToken.Star)
		.Match(Character.EqualTo(','), SqlToken.Comma)
		.Match(Character.EqualTo('.'), SqlToken.Dot)
		.Match(Character.EqualTo('('), SqlToken.LParen)
		.Match(Character.EqualTo(')'), SqlToken.RParen)
		.Match(Character.EqualTo('['), SqlToken.LBracket)
		.Match(Character.EqualTo(']'), SqlToken.RBracket)
		.Match(Character.EqualTo('+'), SqlToken.Plus)
		.Match(Character.EqualTo('-'), SqlToken.Minus)
		.Match(Character.EqualTo('/'), SqlToken.Slash)
		.Match(Character.EqualTo('%'), SqlToken.Percent)
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#bitwise_operators
		.Match(Character.EqualTo('&'), SqlToken.Ampersand)
		.Match(Character.EqualTo('^'), SqlToken.Caret)
		.Match(Character.EqualTo('~'), SqlToken.Tilde)
		.Match(Character.EqualTo('|'), SqlToken.BitOr)
		// Parameter: @identifier
		.Match(Character.EqualTo('@').IgnoreThen(Span.MatchedBy(
			Character.LetterOrDigit.Or(Character.EqualTo('_')).AtLeastOnce())),
			SqlToken.Parameter)
		// Backtick identifier: `anything`
		.Match(Span.MatchedBy(
			Character.EqualTo('`').IgnoreThen(
				Character.Except('`').Many())
				.Then(_ => Character.EqualTo('`'))
		), SqlToken.BacktickIdentifier)
		// String literal: 'text' or "text"
		.Match(QuotedString.SqlStyle, SqlToken.StringLiteral)
		.Match(Span.MatchedBy(
			Character.EqualTo('"').IgnoreThen(
				Span.EqualTo("\"\"").Value('"').Try()
					.Or(Character.Except('"')).Many())
				.Then(_ => Character.EqualTo('"'))
		), SqlToken.StringLiteral)
		// Numbers (integers and decimals)
		.Match(Span.MatchedBy(
			Character.Digit.AtLeastOnce()
				.Then(_ => Character.EqualTo('.').IgnoreThen(Character.Digit.AtLeastOnce()).Select(c => (char[]?)c).OptionalOrDefault())
		), SqlToken.Number)
		// Identifiers and keywords (case-insensitive keywords resolved later)
		.Match(Span.MatchedBy(
			Character.Letter.Or(Character.EqualTo('_'))
				.Then(_ => Character.LetterOrDigit.Or(Character.EqualTo('_')).Many())),
			SqlToken.Identifier)
		.Build();
}

/// <summary>
/// Resolve identifiers to keywords when they match known SQL keywords.
/// </summary>
internal static class KeywordResolver
{
	private static readonly Dictionary<string, SqlToken> Keywords = new(StringComparer.OrdinalIgnoreCase)
	{
		["SELECT"] = SqlToken.Select,
		["FROM"] = SqlToken.From,
		["WHERE"] = SqlToken.Where,
		["AND"] = SqlToken.And,
		["OR"] = SqlToken.Or,
		["NOT"] = SqlToken.Not,
		["AS"] = SqlToken.As,
		["IS"] = SqlToken.Is,
		["NULL"] = SqlToken.Null,
		["ORDER"] = SqlToken.Order,
		["BY"] = SqlToken.By,
		["ASC"] = SqlToken.Asc,
		["DESC"] = SqlToken.Desc,
		["LIMIT"] = SqlToken.Limit,
		["OFFSET"] = SqlToken.Offset,
		["DISTINCT"] = SqlToken.Distinct,
		["TRUE"] = SqlToken.True,
		["FALSE"] = SqlToken.False,
		["BETWEEN"] = SqlToken.Between,
		["IN"] = SqlToken.In,
		["LIKE"] = SqlToken.Like,
		["CASE"] = SqlToken.Case,
		["WHEN"] = SqlToken.When,
		["THEN"] = SqlToken.Then,
		["ELSE"] = SqlToken.Else,
		["END"] = SqlToken.End,
		["CAST"] = SqlToken.Cast,
		["SAFE_CAST"] = SqlToken.SafeCast,
		["JOIN"] = SqlToken.Join,
		["INNER"] = SqlToken.Inner,
		["FULL"] = SqlToken.Full,
		["OUTER"] = SqlToken.Outer,
		["CROSS"] = SqlToken.Cross,
		["GROUP"] = SqlToken.Group,
		["HAVING"] = SqlToken.Having,
		["ON"] = SqlToken.On,
		["LEFT"] = SqlToken.Left,
		["RIGHT"] = SqlToken.Right,
		["WITH"] = SqlToken.With,
		["UNNEST"] = SqlToken.Unnest,
		["EXISTS"] = SqlToken.Exists,
		["OVER"] = SqlToken.Over,
		["PARTITION"] = SqlToken.Partition,
		["ROWS"] = SqlToken.Rows,
		["RANGE"] = SqlToken.Range,
		["UNBOUNDED"] = SqlToken.Unbounded,
		["PRECEDING"] = SqlToken.Preceding,
		["FOLLOWING"] = SqlToken.Following,
		["CURRENT"] = SqlToken.Current,
		["ROW"] = SqlToken.Row,
		["UNION"] = SqlToken.Union,
		["ALL"] = SqlToken.All,
		["EXCEPT"] = SqlToken.Except,
		["INTERSECT"] = SqlToken.Intersect,
		["INSERT"] = SqlToken.Insert,
		["INTO"] = SqlToken.Into,
		["VALUES"] = SqlToken.Values,
		["UPDATE"] = SqlToken.Update,
		["SET"] = SqlToken.Set,
		["DELETE"] = SqlToken.Delete,
		["MERGE"] = SqlToken.Merge,
		["USING"] = SqlToken.Using,
		["MATCHED"] = SqlToken.Matched,
		["CREATE"] = SqlToken.Create,
		["DROP"] = SqlToken.Drop,
		["ALTER"] = SqlToken.Alter,
		["TABLE"] = SqlToken.Table,
		["VIEW"] = SqlToken.View,
		["IF"] = SqlToken.If,
		["REPLACE"] = SqlToken.Replace,
		["COLUMN"] = SqlToken.Column,
		["ADD"] = SqlToken.Add,
		["RENAME"] = SqlToken.Rename,
		["TO"] = SqlToken.To,
		["QUALIFY"] = SqlToken.Qualify,
		["PIVOT"] = SqlToken.Pivot,
		["UNPIVOT"] = SqlToken.Unpivot,
		["TABLESAMPLE"] = SqlToken.Tablesample,
		["ROLLUP"] = SqlToken.Rollup,
		["RECURSIVE"] = SqlToken.Recursive,
		["FOR"] = SqlToken.For,
		["TRUNCATE"] = SqlToken.Truncate,
		["SCHEMA"] = SqlToken.Schema,
		["SNAPSHOT"] = SqlToken.Snapshot,
		["CLONE"] = SqlToken.Clone,
		["EXTERNAL"] = SqlToken.External,
		["PROCEDURE"] = SqlToken.Procedure,
		["MATERIALIZED"] = SqlToken.Materialized,
		["FUNCTION"] = SqlToken.Function,
		["POLICY"] = SqlToken.Policy,
		["INDEX"] = SqlToken.Index,
		["GRANT"] = SqlToken.Grant,
		["OPTIONS"] = SqlToken.Options,
	};

	public static TokenList<SqlToken> Resolve(TokenList<SqlToken> tokens)
	{
		var resolved = new List<Token<SqlToken>>();
		foreach (var token in tokens)
		{
			if (token.Kind == SqlToken.Identifier &&
				Keywords.TryGetValue(token.Span.ToStringValue(), out var keyword))
			{
				resolved.Add(new Token<SqlToken>(keyword, token.Span));
			}
			else
			{
				resolved.Add(token);
			}
		}
		return new TokenList<SqlToken>(resolved.ToArray());
	}
}
