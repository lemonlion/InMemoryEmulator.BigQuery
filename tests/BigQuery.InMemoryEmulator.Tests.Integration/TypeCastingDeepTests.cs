using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Deep integration tests for type casting, SAFE_CAST, and implicit conversions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class TypeCastingDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public TypeCastingDeepTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_cast_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		if (rows.Count == 0) return null;
		var val = rows[0][0];
		return val switch
		{
			DateTime dt when dt.TimeOfDay == TimeSpan.Zero => dt.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
			DateTime dt => dt.ToString("yyyy-MM-dd'T'HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture),
			DateTimeOffset dto => dto.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture),
			_ => val?.ToString()
		};
	}

	// ============================================================
	// INT64 <-> STRING
	// ============================================================

	[Fact] public async Task Cast_Int64ToString() => Assert.Equal("42", await Scalar("SELECT CAST(42 AS STRING)"));
	[Fact] public async Task Cast_NegativeInt64ToString() => Assert.Equal("-7", await Scalar("SELECT CAST(-7 AS STRING)"));
	[Fact] public async Task Cast_StringToInt64() => Assert.Equal("123", await Scalar("SELECT CAST('123' AS INT64)"));
	[Fact] public async Task Cast_StringNegativeToInt64() => Assert.Equal("-50", await Scalar("SELECT CAST('-50' AS INT64)"));
	[Fact] public async Task SafeCast_InvalidStringToInt64() => Assert.Null(await Scalar("SELECT SAFE_CAST('abc' AS INT64)"));
	[Fact] public async Task SafeCast_EmptyStringToInt64() => Assert.Null(await Scalar("SELECT SAFE_CAST('' AS INT64)"));
	[Fact] public async Task SafeCast_FloatStringToInt64() => Assert.Null(await Scalar("SELECT SAFE_CAST('3.14' AS INT64)"));

	// ============================================================
	// FLOAT64 <-> STRING
	// ============================================================

	[Fact] public async Task Cast_Float64ToString() => Assert.Equal("3.14", await Scalar("SELECT CAST(3.14 AS STRING)"));
	[Fact] public async Task Cast_StringToFloat64() => Assert.Equal("3.14", await Scalar("SELECT CAST(CAST('3.14' AS FLOAT64) AS STRING)"));
	[Fact] public async Task SafeCast_InvalidStringToFloat64() => Assert.Null(await Scalar("SELECT SAFE_CAST('not_a_number' AS FLOAT64)"));
	[Fact] public async Task Cast_IntegerStringToFloat64() => Assert.Equal("42", await Scalar("SELECT CAST(CAST('42' AS FLOAT64) AS INT64)"));

	// ============================================================
	// INT64 <-> FLOAT64
	// ============================================================

	[Fact] public async Task Cast_Int64ToFloat64() => Assert.Equal("42", await Scalar("SELECT CAST(CAST(42 AS FLOAT64) AS INT64)"));
	[Fact] public async Task Cast_Float64ToInt64_Truncates() => Assert.Equal("3", await Scalar("SELECT CAST(3.99 AS INT64)"));
	[Fact] public async Task Cast_NegativeFloat64ToInt64() => Assert.Equal("-3", await Scalar("SELECT CAST(-3.7 AS INT64)"));

	// ============================================================
	// BOOL <-> INT64/STRING
	// ============================================================

	[Fact] public async Task Cast_TrueToInt64() => Assert.Equal("1", await Scalar("SELECT CAST(TRUE AS INT64)"));
	[Fact] public async Task Cast_FalseToInt64() => Assert.Equal("0", await Scalar("SELECT CAST(FALSE AS INT64)"));
	[Fact] public async Task Cast_Int64ToBool_NonZero() => Assert.Equal("True", await Scalar("SELECT CAST(1 AS BOOL)"));
	[Fact] public async Task Cast_Int64ToBool_Zero() => Assert.Equal("False", await Scalar("SELECT CAST(0 AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_True() => Assert.Equal("True", await Scalar("SELECT CAST('true' AS BOOL)"));
	[Fact] public async Task Cast_StringToBool_False() => Assert.Equal("False", await Scalar("SELECT CAST('false' AS BOOL)"));
	[Fact] public async Task Cast_BoolToString_True() => Assert.Equal("true", await Scalar("SELECT CAST(TRUE AS STRING)"));
	[Fact] public async Task Cast_BoolToString_False() => Assert.Equal("false", await Scalar("SELECT CAST(FALSE AS STRING)"));

	// ============================================================
	// DATE <-> STRING
	// ============================================================

	[Fact] public async Task Cast_StringToDate() => Assert.Equal("2024-03-15", await Scalar("SELECT CAST('2024-03-15' AS DATE)"));
	[Fact] public async Task Cast_DateToString() => Assert.Equal("2024-03-15", await Scalar("SELECT CAST(DATE '2024-03-15' AS STRING)"));
	[Fact] public async Task SafeCast_InvalidDate() => Assert.Null(await Scalar("SELECT SAFE_CAST('not-a-date' AS DATE)"));
	[Fact] public async Task SafeCast_InvalidDateFormat() => Assert.Null(await Scalar("SELECT SAFE_CAST('03/15/2024' AS DATE)"));

	// ============================================================
	// TIMESTAMP <-> STRING
	// ============================================================

	[Fact] public async Task Cast_StringToTimestamp()
	{
		var result = await Scalar("SELECT CAST('2024-01-15 10:30:00' AS TIMESTAMP)");
		Assert.Contains("2024-01-15", result);
	}

	[Fact] public async Task Cast_TimestampToString()
	{
		var result = await Scalar("SELECT CAST(TIMESTAMP '2024-01-15 10:30:00 UTC' AS STRING)");
		Assert.Contains("2024-01-15", result);
	}

	[Fact] public async Task SafeCast_InvalidTimestamp() => Assert.Null(await Scalar("SELECT SAFE_CAST('nope' AS TIMESTAMP)"));

	// ============================================================
	// DATE <-> TIMESTAMP / DATETIME
	// ============================================================

	[Fact] public async Task Cast_DateToTimestamp()
	{
		var result = await Scalar("SELECT CAST(DATE '2024-06-15' AS TIMESTAMP)");
		Assert.Contains("2024-06-15", result);
	}

	[Fact] public async Task Cast_TimestampToDate() => Assert.Equal("2024-06-15", await Scalar("SELECT CAST(TIMESTAMP '2024-06-15 12:30:00 UTC' AS DATE)"));

	[Fact] public async Task Cast_DateToDatetime()
	{
		var result = await Scalar("SELECT CAST(DATE '2024-06-15' AS DATETIME)");
		Assert.Contains("2024-06-15", result);
	}

	[Fact] public async Task Cast_DatetimeToDate() => Assert.Equal("2024-06-15", await Scalar("SELECT CAST(DATETIME '2024-06-15 12:30:00' AS DATE)"));

	// ============================================================
	// BYTES <-> STRING
	// ============================================================

	[Fact] public async Task Cast_StringToBytes()
	{
		var result = await Scalar("SELECT CAST('hello' AS BYTES)");
		Assert.NotNull(result);
	}

	[Fact] public async Task Cast_BytesToString() => Assert.Equal("hello", await Scalar("SELECT CAST(b'hello' AS STRING)"));

	// ============================================================
	// NUMERIC
	// ============================================================

	[Fact] public async Task Cast_StringToNumeric() => Assert.Equal("123.456", await Scalar("SELECT CAST('123.456' AS NUMERIC)"));
	[Fact] public async Task Cast_Int64ToNumeric() => Assert.Equal("42", await Scalar("SELECT CAST(42 AS NUMERIC)"));
	[Fact] public async Task Cast_Float64ToNumeric()
	{
		var result = await Scalar("SELECT CAST(3.14 AS NUMERIC)");
		Assert.StartsWith("3.14", result);
	}

	// ============================================================
	// NULL casting
	// ============================================================

	[Fact] public async Task Cast_NullToInt64() => Assert.Null(await Scalar("SELECT CAST(NULL AS INT64)"));
	[Fact] public async Task Cast_NullToString() => Assert.Null(await Scalar("SELECT CAST(NULL AS STRING)"));
	[Fact] public async Task Cast_NullToFloat64() => Assert.Null(await Scalar("SELECT CAST(NULL AS FLOAT64)"));
	[Fact] public async Task Cast_NullToBool() => Assert.Null(await Scalar("SELECT CAST(NULL AS BOOL)"));
	[Fact] public async Task Cast_NullToDate() => Assert.Null(await Scalar("SELECT CAST(NULL AS DATE)"));
	[Fact] public async Task Cast_NullToTimestamp() => Assert.Null(await Scalar("SELECT CAST(NULL AS TIMESTAMP)"));
	[Fact] public async Task Cast_NullToNumeric() => Assert.Null(await Scalar("SELECT CAST(NULL AS NUMERIC)"));

	// ============================================================
	// SAFE_CAST various
	// ============================================================

	[Fact] public async Task SafeCast_ValidInt64() => Assert.Equal("42", await Scalar("SELECT SAFE_CAST('42' AS INT64)"));
	[Fact] public async Task SafeCast_Overflow() => Assert.Null(await Scalar("SELECT SAFE_CAST('99999999999999999999' AS INT64)"));
	[Fact] public async Task SafeCast_BoolToString() => Assert.Equal("true", await Scalar("SELECT SAFE_CAST(TRUE AS STRING)"));

	// ============================================================
	// Implicit coercion in comparisons
	// ============================================================

	[Fact] public async Task ImplicitCoercion_IntComparedToFloat()
	{
		var result = await Scalar("SELECT 5 = 5.0");
		Assert.Equal("True", result);
	}

	[Fact] public async Task ImplicitCoercion_IntPlusFloat()
	{
		var result = await Scalar("SELECT 1 + 0.5");
		Assert.Equal("1.5", result);
	}

	// ============================================================
	// FORMAT_* and PARSE_* functions
	// ============================================================

	[Fact] public async Task FormatDate() => Assert.Equal("2024-03-15", await Scalar("SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-03-15')"));
	[Fact] public async Task ParseDate() => Assert.Equal("2024-03-15", await Scalar("SELECT PARSE_DATE('%Y-%m-%d', '2024-03-15')"));
	[Fact] public async Task FormatTimestamp()
	{
		var result = await Scalar("SELECT FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP '2024-03-15 10:30:00 UTC')");
		Assert.Contains("2024-03-15", result);
	}

	[Fact] public async Task ParseTimestamp()
	{
		var result = await Scalar("SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-03-15 10:30:00')");
		Assert.Contains("2024-03-15", result);
	}

	// ============================================================
	// CAST in expressions
	// ============================================================

	[Fact] public async Task Cast_InArithmetic() => Assert.Equal("15", await Scalar("SELECT CAST('10' AS INT64) + 5"));
	[Fact] public async Task Cast_InConcat() => Assert.Equal("Value: 42", await Scalar("SELECT CONCAT('Value: ', CAST(42 AS STRING))"));
	[Fact] public async Task Cast_InWhere()
	{
		var result = await Scalar("SELECT x FROM UNNEST([1,2,3]) AS x WHERE x > CAST('1' AS INT64) ORDER BY x LIMIT 1");
		Assert.Equal("2", result);
	}

	// ============================================================
	// COALESCE type unification
	// ============================================================

	[Fact] public async Task Coalesce_IntAndFloat() => Assert.Equal("3.14", await Scalar("SELECT COALESCE(NULL, 3.14)"));
	[Fact] public async Task Coalesce_StringAndNull() => Assert.Equal("hello", await Scalar("SELECT COALESCE(NULL, 'hello')"));
	[Fact] public async Task Coalesce_AllNull() => Assert.Null(await Scalar("SELECT COALESCE(NULL, NULL, NULL)"));

	// ============================================================
	// CAST chaining
	// ============================================================

	[Fact] public async Task Cast_Chain_IntToStringToFloat() => Assert.Equal("42", await Scalar("SELECT CAST(CAST(CAST(42 AS STRING) AS FLOAT64) AS INT64)"));
}
