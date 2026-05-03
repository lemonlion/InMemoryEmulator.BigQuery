using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for FORMAT() function with various type format patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class FormatFunctionDeepTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public FormatFunctionDeepTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_fmt_{Guid.NewGuid():N}"[..30];
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
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ============================================================
	// FORMAT() with integer patterns
	// ============================================================
	[Fact] public async Task Format_IntegerDecimal() => Assert.Equal("42", await Scalar("SELECT FORMAT('%d', 42)"));
	[Fact] public async Task Format_IntegerPadded() => Assert.Equal("  42", await Scalar("SELECT FORMAT('%4d', 42)"));
	[Fact] public async Task Format_IntegerZeroPadded() => Assert.Equal("0042", await Scalar("SELECT FORMAT('%04d', 42)"));
	[Fact] public async Task Format_IntegerNegative() => Assert.Equal("-7", await Scalar("SELECT FORMAT('%d', -7)"));
	[Fact] public async Task Format_IntegerHex() => Assert.Equal("ff", await Scalar("SELECT FORMAT('%x', 255)"));
	[Fact] public async Task Format_IntegerHexUpper() => Assert.Equal("FF", await Scalar("SELECT FORMAT('%X', 255)"));
	[Fact] public async Task Format_IntegerOctal() => Assert.Equal("77", await Scalar("SELECT FORMAT('%o', 63)"));

	// ============================================================
	// FORMAT() with float patterns
	// ============================================================
	[Fact] public async Task Format_FloatDefault() => Assert.Contains("3.14", await Scalar("SELECT FORMAT('%f', 3.14159)") ?? "");
	[Fact] public async Task Format_FloatPrecision2() => Assert.Equal("3.14", await Scalar("SELECT FORMAT('%.2f', 3.14159)"));
	[Fact] public async Task Format_FloatPrecision0() => Assert.Equal("3", await Scalar("SELECT FORMAT('%.0f', 3.14159)"));
	[Fact] public async Task Format_FloatScientific() => Assert.Contains("e", (await Scalar("SELECT FORMAT('%e', 12345.6789)"))?.ToLower() ?? "");
	[Fact] public async Task Format_FloatScientificUpper() => Assert.Contains("E", await Scalar("SELECT FORMAT('%E', 12345.6789)") ?? "");
	[Fact] public async Task Format_FloatGeneral() => Assert.NotNull(await Scalar("SELECT FORMAT('%g', 3.14)"));

	// ============================================================
	// FORMAT() with string patterns
	// ============================================================
	[Fact] public async Task Format_String() => Assert.Equal("hello", await Scalar("SELECT FORMAT('%s', 'hello')"));
	[Fact] public async Task Format_StringPadded() => Assert.Equal("   hi", await Scalar("SELECT FORMAT('%5s', 'hi')"));
	[Fact] public async Task Format_StringLeftAligned() => Assert.Equal("hi   ", await Scalar("SELECT FORMAT('%-5s', 'hi')"));
	[Fact] public async Task Format_MultipleArgs() => Assert.Equal("Name: Alice, Age: 30", await Scalar("SELECT FORMAT('Name: %s, Age: %d', 'Alice', 30)"));
	[Fact] public async Task Format_MixedTypes() => Assert.Equal("42 hello 3.14", await Scalar("SELECT FORMAT('%d %s %.2f', 42, 'hello', 3.14159)"));

	// ============================================================
	// FORMAT() with special values
	// ============================================================
	[Fact] public async Task Format_NullValue() => Assert.Equal("NULL", await Scalar("SELECT FORMAT('%s', NULL)"));
	[Fact] public async Task Format_EmptyString() => Assert.Equal("", await Scalar("SELECT FORMAT('%s', '')"));
	[Fact] public async Task Format_Percent() => Assert.Equal("100%", await Scalar("SELECT FORMAT('%d%%', 100)"));

	// ============================================================
	// FORMAT_TIMESTAMP
	// ============================================================
	[Fact] public async Task FormatTimestamp_FullDate() => Assert.Equal("2024-01-15", await Scalar("SELECT FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task FormatTimestamp_YearOnly() => Assert.Equal("2024", await Scalar("SELECT FORMAT_TIMESTAMP('%Y', TIMESTAMP '2024-06-15 00:00:00 UTC')"));
	[Fact] public async Task FormatTimestamp_MonthDay() => Assert.Equal("06-15", await Scalar("SELECT FORMAT_TIMESTAMP('%m-%d', TIMESTAMP '2024-06-15 00:00:00 UTC')"));
	[Fact] public async Task FormatTimestamp_TimeOnly() => Assert.Equal("10:30:00", await Scalar("SELECT FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task FormatTimestamp_DayOfWeek() => Assert.NotNull(await Scalar("SELECT FORMAT_TIMESTAMP('%A', TIMESTAMP '2024-01-15 00:00:00 UTC')"));
	[Fact] public async Task FormatTimestamp_MonthName() => Assert.NotNull(await Scalar("SELECT FORMAT_TIMESTAMP('%B', TIMESTAMP '2024-01-15 00:00:00 UTC')"));
	[Fact] public async Task FormatTimestamp_Hour12() => Assert.Equal("10", await Scalar("SELECT FORMAT_TIMESTAMP('%I', TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task FormatTimestamp_AmPm() => Assert.NotNull(await Scalar("SELECT FORMAT_TIMESTAMP('%p', TIMESTAMP '2024-01-15 10:30:00 UTC')"));
	[Fact] public async Task FormatTimestamp_NullInput() => Assert.Null(await Scalar("SELECT FORMAT_TIMESTAMP('%Y', NULL)"));

	// ============================================================
	// FORMAT_DATE
	// ============================================================
	[Fact] public async Task FormatDate_FullDate() => Assert.Equal("2024-03-15", await Scalar("SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-03-15')"));
	[Fact] public async Task FormatDate_DayMonth() => Assert.Equal("15/03", await Scalar("SELECT FORMAT_DATE('%d/%m', DATE '2024-03-15')"));
	[Fact] public async Task FormatDate_YearShort() => Assert.Equal("24", await Scalar("SELECT FORMAT_DATE('%y', DATE '2024-03-15')"));
	[Fact] public async Task FormatDate_DayOfYear() => Assert.NotNull(await Scalar("SELECT FORMAT_DATE('%j', DATE '2024-03-15')"));
	[Fact] public async Task FormatDate_NullInput() => Assert.Null(await Scalar("SELECT FORMAT_DATE('%Y', NULL)"));

	// ============================================================
	// FORMAT_DATETIME
	// ============================================================
	[Fact] public async Task FormatDatetime_Full() => Assert.Equal("2024-03-15 14:30:00", await Scalar("SELECT FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', DATETIME '2024-03-15 14:30:00')"));
	[Fact] public async Task FormatDatetime_DateOnly() => Assert.Equal("2024-03-15", await Scalar("SELECT FORMAT_DATETIME('%Y-%m-%d', DATETIME '2024-03-15 14:30:00')"));
	[Fact] public async Task FormatDatetime_TimeOnly() => Assert.Equal("14:30:00", await Scalar("SELECT FORMAT_DATETIME('%H:%M:%S', DATETIME '2024-03-15 14:30:00')"));
	[Fact] public async Task FormatDatetime_NullInput() => Assert.Null(await Scalar("SELECT FORMAT_DATETIME('%Y', NULL)"));

	// ============================================================
	// FORMAT_TIME
	// ============================================================
	[Fact] public async Task FormatTime_Full() => Assert.Equal("14:30:00", await Scalar("SELECT FORMAT_TIME('%H:%M:%S', TIME '14:30:00')"));
	[Fact] public async Task FormatTime_HourMinute() => Assert.Equal("14:30", await Scalar("SELECT FORMAT_TIME('%H:%M', TIME '14:30:00')"));
	[Fact] public async Task FormatTime_12Hour() => Assert.Equal("02", await Scalar("SELECT FORMAT_TIME('%I', TIME '14:30:00')"));
	[Fact] public async Task FormatTime_NullInput() => Assert.Null(await Scalar("SELECT FORMAT_TIME('%H', NULL)"));

	// ============================================================
	// PARSE_DATE
	// ============================================================
	[Fact] public async Task ParseDate_Standard() => Assert.Equal("2024-03-15", await Scalar("SELECT CAST(PARSE_DATE('%Y-%m-%d', '2024-03-15') AS STRING)"));
	[Fact] public async Task ParseDate_DayMonthYear() => Assert.Equal("2024-03-15", await Scalar("SELECT CAST(PARSE_DATE('%d/%m/%Y', '15/03/2024') AS STRING)"));
	[Fact] public async Task ParseDate_MonthDayYear() => Assert.Equal("2024-03-15", await Scalar("SELECT CAST(PARSE_DATE('%m-%d-%Y', '03-15-2024') AS STRING)"));
	[Fact] public async Task ParseDate_NullInput() => Assert.Null(await Scalar("SELECT PARSE_DATE('%Y-%m-%d', NULL)"));

	// ============================================================
	// PARSE_TIMESTAMP
	// ============================================================
	[Fact] public async Task ParseTimestamp_Standard() => Assert.Contains("2024-03-15", await Scalar("SELECT CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-03-15 10:30:00') AS STRING)") ?? "");
	[Fact] public async Task ParseTimestamp_NullInput() => Assert.Null(await Scalar("SELECT PARSE_TIMESTAMP('%Y-%m-%d', NULL)"));

	// ============================================================
	// PARSE_DATETIME
	// ============================================================
	[Fact] public async Task ParseDatetime_Standard() => Assert.Contains("2024-03-15", await Scalar("SELECT CAST(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2024-03-15 10:30:00') AS STRING)") ?? "");
	[Fact] public async Task ParseDatetime_NullInput() => Assert.Null(await Scalar("SELECT PARSE_DATETIME('%Y-%m-%d', NULL)"));

	// ============================================================
	// PARSE_TIME
	// ============================================================
	[Fact] public async Task ParseTime_Standard() => Assert.Contains("10:30:00", await Scalar("SELECT CAST(PARSE_TIME('%H:%M:%S', '10:30:00') AS STRING)") ?? "");
	[Fact] public async Task ParseTime_NullInput() => Assert.Null(await Scalar("SELECT PARSE_TIME('%H:%M', NULL)"));
}
