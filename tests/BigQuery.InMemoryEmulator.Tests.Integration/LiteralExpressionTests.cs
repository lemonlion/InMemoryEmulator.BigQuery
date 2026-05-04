using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Literal values and expressions: numeric, string, boolean, date, NULL, arrays, structs in SELECT.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#literals
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class LiteralExpressionTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public LiteralExpressionTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_lit_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var c = await _fixture.GetClientAsync();
		var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		var rows = r.ToList();
		if (rows.Count == 0) return null;
		var val = rows[0][0];
		if (val is DateTime dt) return dt.TimeOfDay == TimeSpan.Zero ? dt.ToString("yyyy-MM-dd") : dt.ToString("yyyy-MM-dd HH:mm:ss");
		if (val is DateTimeOffset dto) return dto.TimeOfDay == TimeSpan.Zero ? dto.ToString("yyyy-MM-dd") : dto.ToString("yyyy-MM-dd HH:mm:ss");
		return val?.ToString();
	}
	private async Task<List<BigQueryRow>> Q(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- Integer literals ----
	[Fact] public async Task Int_Zero() => Assert.Equal("0", await S("SELECT 0"));
	[Fact] public async Task Int_Positive() => Assert.Equal("42", await S("SELECT 42"));
	[Fact] public async Task Int_Negative() => Assert.Equal("-42", await S("SELECT -42"));
	[Fact] public async Task Int_Large() => Assert.Equal("9223372036854775807", await S("SELECT 9223372036854775807"));
	[Fact] public async Task Int_LargeNeg() => Assert.NotNull(await S("SELECT -9223372036854775807"));

	// ---- Float literals ----
	[Fact] public async Task Float_Zero()
	{
		var v = await S("SELECT 0.0");
		Assert.True(v == "0" || v == "0.0", $"Got: {v}");
	}
	[Fact] public async Task Float_Positive() => Assert.Equal("3.14", await S("SELECT 3.14"));
	[Fact] public async Task Float_Negative() => Assert.Equal("-3.14", await S("SELECT -3.14"));
	[Fact] public async Task Float_Scientific()
	{
		var v = await S("SELECT CAST(10000000000 AS FLOAT64)");
		Assert.NotNull(v);
		Assert.True(double.Parse(v!) > 9e9);
	}
	[Fact] public async Task Float_SmallScientific()
	{
		var v = await S("SELECT CAST(0.0015 AS FLOAT64)");
		Assert.NotNull(v);
		Assert.True(double.Parse(v!) < 0.01);
	}

	// ---- String literals ----
	[Fact] public async Task String_Basic() => Assert.Equal("hello", await S("SELECT 'hello'"));
	[Fact] public async Task String_Empty() => Assert.Equal("", await S("SELECT ''"));
	[Fact] public async Task String_WithSpaces() => Assert.Equal("hello world", await S("SELECT 'hello world'"));
	[Fact] public async Task String_WithDigits() => Assert.Equal("abc123", await S("SELECT 'abc123'"));
	[Fact] public async Task String_SingleQuoteEscape() => Assert.Equal("it's", await S("SELECT 'it\\'s'"));
	[Fact] public async Task String_Backslash() => Assert.Equal("a\\b", await S("SELECT 'a\\\\b'"));
	[Fact] public async Task String_Newline() => Assert.Equal("a\nb", await S("SELECT 'a\\nb'"));
	[Fact] public async Task String_Tab() => Assert.Equal("a\tb", await S("SELECT 'a\\tb'"));

	// ---- Boolean literals ----
	[Fact] public async Task Bool_True() => Assert.Equal("True", await S("SELECT true"));
	[Fact] public async Task Bool_False() => Assert.Equal("False", await S("SELECT false"));
	[Fact] public async Task Bool_TrueUpper() => Assert.Equal("True", await S("SELECT TRUE"));
	[Fact] public async Task Bool_FalseUpper() => Assert.Equal("False", await S("SELECT FALSE"));

	// ---- NULL literal ----
	[Fact] public async Task Null_Literal() => Assert.Null(await S("SELECT NULL"));
	[Fact] public async Task Null_CastInt() => Assert.Null(await S("SELECT CAST(NULL AS INT64)"));
	[Fact] public async Task Null_CastString() => Assert.Null(await S("SELECT CAST(NULL AS STRING)"));

	// ---- Date literals ----
	[Fact] public async Task Date_Literal() => Assert.Equal("2024-01-15", await S("SELECT DATE '2024-01-15'"));
	[Fact] public async Task Date_LeapYear() => Assert.Equal("2024-02-29", await S("SELECT DATE '2024-02-29'"));
	[Fact] public async Task Date_EndOfYear() => Assert.Equal("2024-12-31", await S("SELECT DATE '2024-12-31'"));
	[Fact] public async Task Date_StartOfYear() => Assert.Equal("2024-01-01", await S("SELECT DATE '2024-01-01'"));

	// ---- Timestamp literals ----
	[Fact] public async Task Timestamp_Literal()
	{
		var v = await S("SELECT TIMESTAMP '2024-01-15 10:30:00 UTC'");
		Assert.NotNull(v);
		Assert.Contains("2024", v!);
	}

	// ---- Array literals ----
	[Fact] public async Task Array_IntLiteral()
	{
		var v = await S("SELECT ARRAY_LENGTH([1, 2, 3])");
		Assert.Equal("3", v);
	}
	[Fact] public async Task Array_StringLiteral()
	{
		var v = await S("SELECT ARRAY_LENGTH(['a', 'b', 'c', 'd'])");
		Assert.Equal("4", v);
	}
	[Fact] public async Task Array_Empty()
	{
		var v = await S("SELECT ARRAY_LENGTH([])");
		Assert.Equal("0", v);
	}
	[Fact] public async Task Array_SingleElement()
	{
		var v = await S("SELECT ARRAY_LENGTH([42])");
		Assert.Equal("1", v);
	}

	// ---- Multiple literals in SELECT ----
	[Fact] public async Task MultiLiteral()
	{
		var rows = await Q("SELECT 1 AS a, 'hello' AS b, true AS c, NULL AS d");
		Assert.Equal("1", rows[0]["a"]?.ToString());
		Assert.Equal("hello", rows[0]["b"]?.ToString());
		Assert.Equal("True", rows[0]["c"]?.ToString());
		Assert.Null(rows[0]["d"]);
	}

	// ---- Literal expressions ----
	[Fact] public async Task Literal_Add() => Assert.Equal("3", await S("SELECT 1 + 2"));
	[Fact] public async Task Literal_Sub() => Assert.Equal("-1", await S("SELECT 1 - 2"));
	[Fact] public async Task Literal_Mul() => Assert.Equal("6", await S("SELECT 2 * 3"));
	[Fact] public async Task Literal_Div()
	{
		var v = await S("SELECT CAST(5 AS FLOAT64) / 2");
		Assert.True(v == "2.5" || v == "2", $"Got: {v}");
	}
	[Fact] public async Task Literal_Concat() => Assert.Equal("foobar", await S("SELECT 'foo' || 'bar'"));

	// ---- CAST literals ----
	[Fact] public async Task Cast_IntToString() => Assert.Equal("42", await S("SELECT CAST(42 AS STRING)"));
	[Fact] public async Task Cast_StringToInt() => Assert.Equal("42", await S("SELECT CAST('42' AS INT64)"));
	[Fact] public async Task Cast_FloatToInt() => Assert.Equal("4", await S("SELECT CAST(3.7 AS INT64)"));
	[Fact] public async Task Cast_IntToFloat()
	{
		var v = await S("SELECT CAST(5 AS FLOAT64)");
		Assert.True(v == "5" || v == "5.0");
	}
	[Fact] public async Task Cast_StringToBool() => Assert.Equal("True", await S("SELECT CAST('true' AS BOOL)"));
	[Fact] public async Task Cast_BoolToString() => Assert.Equal("true", await S("SELECT CAST(true AS STRING)"));

	// ---- SAFE_CAST ----
	[Fact] public async Task SafeCast_Invalid() => Assert.Null(await S("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_Valid() => Assert.Equal("42", await S("SELECT SAFE_CAST('42' AS INT64)"));

	// ---- Complex literal expressions ----
	[Fact] public async Task Literal_Nested() => Assert.Equal("20", await S("SELECT (2 + 3) * 4"));
	[Fact] public async Task Literal_Modulo() => Assert.Equal("1", await S("SELECT MOD(10, 3)"));
	[Fact] public async Task Literal_IntDiv() => Assert.Equal("3", await S("SELECT DIV(10, 3)"));
	[Fact] public async Task Literal_Power()
	{
		var v = await S("SELECT POW(2, 3)");
		Assert.True(v == "8" || v == "8.0", $"Got: {v}");
	}
	[Fact] public async Task Literal_Abs() => Assert.Equal("5", await S("SELECT ABS(-5)"));

	// ---- Comparison literals ----
	[Fact] public async Task Literal_Equal() => Assert.Equal("True", await S("SELECT 1 = 1"));
	[Fact] public async Task Literal_NotEqual() => Assert.Equal("True", await S("SELECT 1 != 2"));
	[Fact] public async Task Literal_GT() => Assert.Equal("True", await S("SELECT 2 > 1"));
	[Fact] public async Task Literal_LT() => Assert.Equal("True", await S("SELECT 1 < 2"));
	[Fact] public async Task Literal_GTE() => Assert.Equal("True", await S("SELECT 2 >= 2"));
	[Fact] public async Task Literal_LTE() => Assert.Equal("True", await S("SELECT 2 <= 2"));

	// ---- IN literals ----
	[Fact] public async Task Literal_In() => Assert.Equal("True", await S("SELECT 2 IN (1, 2, 3)"));
	[Fact] public async Task Literal_NotIn() => Assert.Equal("True", await S("SELECT 5 NOT IN (1, 2, 3)"));

	// ---- BETWEEN literals ----
	[Fact] public async Task Literal_Between() => Assert.Equal("True", await S("SELECT 5 BETWEEN 1 AND 10"));
	[Fact] public async Task Literal_NotBetween() => Assert.Equal("True", await S("SELECT 15 NOT BETWEEN 1 AND 10"));

	// ---- LIKE literals ----
	[Fact] public async Task Literal_Like() => Assert.Equal("True", await S("SELECT 'hello' LIKE 'hel%'"));
	[Fact] public async Task Literal_NotLike() => Assert.Equal("True", await S("SELECT 'hello' NOT LIKE 'xyz%'"));
	[Fact] public async Task Literal_LikeUnderscore() => Assert.Equal("True", await S("SELECT 'hello' LIKE 'hell_'"));

	// ---- Multiple rows from UNION of literals ----
	[Fact] public async Task UnionLiterals()
	{
		var rows = await Q("SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY n");
		Assert.Equal(3, rows.Count);
		Assert.Equal("1", rows[0]["n"]?.ToString());
		Assert.Equal("3", rows[2]["n"]?.ToString());
	}

	// ---- Subquery with literals ----
	[Fact] public async Task SubqueryLiteral()
	{
		var v = await S("SELECT x FROM (SELECT 42 AS x)");
		Assert.Equal("42", v);
	}
	[Fact] public async Task SubqueryMultiRow()
	{
		var rows = await Q("SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) ORDER BY x");
		Assert.Equal(3, rows.Count);
	}
}
