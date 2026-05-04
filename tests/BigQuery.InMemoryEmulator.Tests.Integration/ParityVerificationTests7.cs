using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Parity verification tests batch 7: SUBSTR edge cases, FORMAT_TIMESTAMP,
/// DATE/TIMESTAMP functions, ARRAY operations, CAST edge cases, STRING_AGG DISTINCT,
/// GENERATE_ARRAY, ARRAY_TO_STRING, and various expression edge cases.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ParityVerificationTests7 : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public ParityVerificationTests7(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"pv7_{Guid.NewGuid():N}"[..28];
		await _fixture.CreateDatasetAsync(_ds);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var c = await _fixture.GetClientAsync();
			await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> S(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		var rows = result.ToList();
		return rows.Count == 0 ? null : rows[0][0]?.ToString();
	}

	private async Task<List<string?>> Col(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null);
		return result.ToList().Select(r => r[0]?.ToString()).ToList();
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SUBSTR edge cases
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#substr
	//   "If position is 0, it is treated as 1."
	//   "If position is negative, the function counts from the end of value, where -1 indicates the last character."
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Substr_PositionZero_TreatedAsOne()
	{
		// Ref: SUBSTR position 0 is treated as 1
		var result = await S("SELECT SUBSTR('hello', 0)");
		Assert.Equal("hello", result);
	}

	[Fact] public async Task Substr_PositionZero_WithLength()
	{
		// Ref: SUBSTR position 0 is treated as 1, takes 3 chars from start
		var result = await S("SELECT SUBSTR('hello', 0, 3)");
		Assert.Equal("hel", result);
	}

	[Fact] public async Task Substr_PositionOne()
	{
		var result = await S("SELECT SUBSTR('hello', 1)");
		Assert.Equal("hello", result);
	}

	[Fact] public async Task Substr_PositionOne_WithLength()
	{
		var result = await S("SELECT SUBSTR('hello', 1, 3)");
		Assert.Equal("hel", result);
	}

	[Fact] public async Task Substr_NegativePosition_LastChar()
	{
		// -1 means last character
		var result = await S("SELECT SUBSTR('hello', -1)");
		Assert.Equal("o", result);
	}

	[Fact] public async Task Substr_NegativePosition_LastTwo()
	{
		var result = await S("SELECT SUBSTR('hello', -2)");
		Assert.Equal("lo", result);
	}

	[Fact] public async Task Substr_NegativePosition_WithLength()
	{
		// -3 means start at 3rd from end (l), take 2 chars
		var result = await S("SELECT SUBSTR('hello', -3, 2)");
		Assert.Equal("ll", result);
	}

	[Fact] public async Task Substr_PositionBeyondEnd()
	{
		var result = await S("SELECT SUBSTR('hello', 10)");
		Assert.Equal("", result);
	}

	[Fact] public async Task Substr_LengthExceedsRemaining()
	{
		var result = await S("SELECT SUBSTR('hello', 3, 100)");
		Assert.Equal("llo", result);
	}

	[Fact] public async Task Substr_NullInput()
	{
		var result = await S("SELECT SUBSTR(CAST(NULL AS STRING), 1)");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// FORMAT_TIMESTAMP
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#format_timestamp
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task FormatTimestamp_BasicDate()
	{
		var result = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP '2024-03-15 10:30:00 UTC')");
		Assert.Equal("2024-03-15", result);
	}

	[Fact] public async Task FormatTimestamp_BasicTime()
	{
		var result = await S("SELECT FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP '2024-03-15 10:30:45 UTC')");
		Assert.Equal("10:30:45", result);
	}

	[Fact] public async Task FormatTimestamp_FullDateTime()
	{
		var result = await S("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP '2024-03-15 10:30:45 UTC')");
		Assert.Equal("2024-03-15 10:30:45", result);
	}

	[Fact] public async Task FormatTimestamp_DayOfYear()
	{
		// March 15 is the 75th day of 2024 (leap year)
		var result = await S("SELECT FORMAT_TIMESTAMP('%j', TIMESTAMP '2024-03-15 00:00:00 UTC')");
		Assert.Equal("075", result);
	}

	[Fact] public async Task FormatTimestamp_IsoWeekNumber()
	{
		// 2024-01-01 is in ISO week 1
		var result = await S("SELECT FORMAT_TIMESTAMP('%V', TIMESTAMP '2024-01-01 00:00:00 UTC')");
		Assert.Equal("01", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// DATE functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task DateAdd_Day()
	{
		var result = await S("SELECT CAST(DATE_ADD(DATE '2024-01-15', INTERVAL 10 DAY) AS STRING)");
		Assert.Equal("2024-01-25", result);
	}

	[Fact] public async Task DateAdd_Month_EndOfMonth()
	{
		// Jan 31 + 1 month → Feb 29 (2024 is leap year)
		var result = await S("SELECT CAST(DATE_ADD(DATE '2024-01-31', INTERVAL 1 MONTH) AS STRING)");
		Assert.Equal("2024-02-29", result);
	}

	[Fact] public async Task DateAdd_Year()
	{
		var result = await S("SELECT CAST(DATE_ADD(DATE '2024-02-29', INTERVAL 1 YEAR) AS STRING)");
		Assert.Equal("2025-02-28", result);
	}

	[Fact] public async Task DateSub_Day()
	{
		var result = await S("SELECT CAST(DATE_SUB(DATE '2024-03-01', INTERVAL 1 DAY) AS STRING)");
		Assert.Equal("2024-02-29", result);
	}

	[Fact] public async Task DateDiff_Days()
	{
		var result = await S("SELECT DATE_DIFF(DATE '2024-03-15', DATE '2024-01-01', DAY)");
		Assert.Equal("74", result);
	}

	[Fact] public async Task DateDiff_Months()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_diff
		//   "DATE_DIFF with MONTH as the date part counts... the number of month boundaries between two dates."
		var result = await S("SELECT DATE_DIFF(DATE '2024-03-01', DATE '2024-01-31', MONTH)");
		Assert.Equal("2", result);
	}

	[Fact] public async Task DateDiff_Years()
	{
		var result = await S("SELECT DATE_DIFF(DATE '2024-12-31', DATE '2020-01-01', YEAR)");
		Assert.Equal("4", result);
	}

	[Fact] public async Task DateTrunc_Month()
	{
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-03-15', MONTH) AS STRING)");
		Assert.Equal("2024-03-01", result);
	}

	[Fact] public async Task DateTrunc_Year()
	{
		var result = await S("SELECT CAST(DATE_TRUNC(DATE '2024-03-15', YEAR) AS STRING)");
		Assert.Equal("2024-01-01", result);
	}

	[Fact] public async Task ExtractYear()
	{
		var result = await S("SELECT EXTRACT(YEAR FROM DATE '2024-03-15')");
		Assert.Equal("2024", result);
	}

	[Fact] public async Task ExtractMonth()
	{
		var result = await S("SELECT EXTRACT(MONTH FROM DATE '2024-03-15')");
		Assert.Equal("3", result);
	}

	[Fact] public async Task ExtractDayOfWeek()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
		//   "DAYOFWEEK: Returns values in the range [1,7] with Sunday as the first day of the week."
		// 2024-03-15 is a Friday → DAYOFWEEK = 6
		var result = await S("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-03-15')");
		Assert.Equal("6", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// TIMESTAMP functions
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task TimestampAdd_Hour()
	{
		var result = await S("SELECT CAST(TIMESTAMP_ADD(TIMESTAMP '2024-01-01 10:00:00 UTC', INTERVAL 3 HOUR) AS STRING)");
		Assert.Equal("2024-01-01 13:00:00+00", result);
	}

	[Fact] public async Task TimestampDiff_Hours()
	{
		var result = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-02 00:00:00 UTC', TIMESTAMP '2024-01-01 00:00:00 UTC', HOUR)");
		Assert.Equal("24", result);
	}

	[Fact] public async Task TimestampDiff_Minutes()
	{
		var result = await S("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-01 01:30:00 UTC', TIMESTAMP '2024-01-01 00:00:00 UTC', MINUTE)");
		Assert.Equal("90", result);
	}

	[Fact] public async Task TimestampTrunc_Day()
	{
		var result = await S("SELECT CAST(TIMESTAMP_TRUNC(TIMESTAMP '2024-03-15 14:30:00 UTC', DAY) AS STRING)");
		Assert.Equal("2024-03-15 00:00:00+00", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// ARRAY operations
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ArrayToString_Basic()
	{
		var result = await S("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], ',')");
		Assert.Equal("a,b,c", result);
	}

	[Fact] public async Task ArrayToString_WithNulls_NoNullText()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_to_string
		//   "If null_text is omitted, NULL array elements are ignored."
		var result = await S("SELECT ARRAY_TO_STRING(['a', NULL, 'c'], ',')");
		Assert.Equal("a,c", result);
	}

	[Fact] public async Task ArrayToString_WithNullText()
	{
		// Ref: When null_text is provided, NULLs are replaced with it
		var result = await S("SELECT ARRAY_TO_STRING(['a', NULL, 'c'], ',', 'N/A')");
		Assert.Equal("a,N/A,c", result);
	}

	[Fact] public async Task GenerateArray_Basic()
	{
		var result = await S("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(1, 5), ',')");
		Assert.Equal("1,2,3,4,5", result);
	}

	[Fact] public async Task GenerateArray_WithStep()
	{
		var result = await S("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(0, 10, 3), ',')");
		Assert.Equal("0,3,6,9", result);
	}

	[Fact] public async Task GenerateArray_Descending()
	{
		var result = await S("SELECT ARRAY_TO_STRING(GENERATE_ARRAY(5, 1, -1), ',')");
		Assert.Equal("5,4,3,2,1", result);
	}

	[Fact] public async Task ArrayAgg_Distinct()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(ARRAY_AGG(DISTINCT x ORDER BY x), ',')
			FROM UNNEST([1,2,2,3,3,3]) AS x");
		Assert.Equal("1,2,3", result);
	}

	[Fact] public async Task ArrayLength_Empty()
	{
		var result = await S("SELECT ARRAY_LENGTH([])");
		Assert.Equal("0", result);
	}

	[Fact] public async Task ArrayLength_Populated()
	{
		var result = await S("SELECT ARRAY_LENGTH([10, 20, 30])");
		Assert.Equal("3", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CAST edge cases
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Cast_StringToDate()
	{
		var result = await S("SELECT CAST(CAST('2024-03-15' AS DATE) AS STRING)");
		Assert.Equal("2024-03-15", result);
	}

	[Fact] public async Task Cast_StringToTimestamp()
	{
		var result = await S("SELECT CAST(CAST('2024-03-15 10:30:00 UTC' AS TIMESTAMP) AS STRING)");
		Assert.Equal("2024-03-15 10:30:00+00", result);
	}

	[Fact] public async Task Cast_IntToFloat()
	{
		var result = await S("SELECT CAST(42 AS FLOAT64)");
		Assert.Equal("42", result);
	}

	[Fact] public async Task Cast_FloatToString_Decimal()
	{
		var result = await S("SELECT CAST(3.14 AS STRING)");
		Assert.Equal("3.14", result);
	}

	[Fact] public async Task Cast_FloatToString_WholeNumber()
	{
		// Ref: FLOAT64 whole numbers include ".0" when cast to STRING
		var result = await S("SELECT CAST(42.0 AS STRING)");
		Assert.Equal("42.0", result);
	}

	[Fact] public async Task Cast_BoolToString()
	{
		var result = await S("SELECT CAST(TRUE AS STRING)");
		Assert.Equal("true", result);
	}

	[Fact] public async Task Cast_DateToString()
	{
		var result = await S("SELECT CAST(DATE '2024-03-15' AS STRING)");
		Assert.Equal("2024-03-15", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// STRING_AGG with DISTINCT and ORDER BY
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#string_agg
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task StringAgg_Distinct()
	{
		var result = await S(@"
			SELECT STRING_AGG(DISTINCT x, ',' ORDER BY x)
			FROM UNNEST(['b','a','b','c','a']) AS x");
		Assert.Equal("a,b,c", result);
	}

	[Fact] public async Task StringAgg_CustomSeparator()
	{
		var result = await S(@"
			SELECT STRING_AGG(x, ' | ' ORDER BY x)
			FROM UNNEST(['alpha','beta','gamma']) AS x");
		Assert.Equal("alpha | beta | gamma", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// LOGICAL_AND / LOGICAL_OR
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#logical_and
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task LogicalAnd_AllTrue()
	{
		var result = await S("SELECT LOGICAL_AND(x) FROM UNNEST([true, true, true]) AS x");
		Assert.Equal("True", result);
	}

	[Fact] public async Task LogicalAnd_OneFalse()
	{
		var result = await S("SELECT LOGICAL_AND(x) FROM UNNEST([true, false, true]) AS x");
		Assert.Equal("False", result);
	}

	[Fact] public async Task LogicalOr_AllFalse()
	{
		var result = await S("SELECT LOGICAL_OR(x) FROM UNNEST([false, false, false]) AS x");
		Assert.Equal("False", result);
	}

	[Fact] public async Task LogicalOr_OneTrue()
	{
		var result = await S("SELECT LOGICAL_OR(x) FROM UNNEST([false, true, false]) AS x");
		Assert.Equal("True", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// COUNTIF
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#countif
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CountIf_Basic()
	{
		var result = await S("SELECT COUNTIF(x > 3) FROM UNNEST([1,2,3,4,5]) AS x");
		Assert.Equal("2", result);
	}

	[Fact] public async Task CountIf_NoneMatch()
	{
		var result = await S("SELECT COUNTIF(x > 10) FROM UNNEST([1,2,3]) AS x");
		Assert.Equal("0", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// COALESCE type coercion
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#coalesce
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Coalesce_TwoNulls_ReturnsThird()
	{
		var result = await S("SELECT COALESCE(NULL, NULL, 'found')");
		Assert.Equal("found", result);
	}

	[Fact] public async Task Coalesce_FirstNonNull()
	{
		var result = await S("SELECT COALESCE('first', 'second')");
		Assert.Equal("first", result);
	}

	[Fact] public async Task Coalesce_AllNull()
	{
		var result = await S("SELECT COALESCE(NULL, NULL, NULL)");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// GENERATE_DATE_ARRAY  
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task GenerateDateArray_Daily()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-05'), ',')");
		Assert.Equal("2024-01-01,2024-01-02,2024-01-03,2024-01-04,2024-01-05", result);
	}

	[Fact] public async Task GenerateDateArray_Monthly()
	{
		var result = await S(@"
			SELECT ARRAY_TO_STRING(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-04-01', INTERVAL 1 MONTH), ',')");
		Assert.Equal("2024-01-01,2024-02-01,2024-03-01,2024-04-01", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REVERSE, LEFT, RIGHT
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Reverse_String()
	{
		var result = await S("SELECT REVERSE('hello')");
		Assert.Equal("olleh", result);
	}

	[Fact] public async Task Left_String()
	{
		var result = await S("SELECT LEFT('hello', 3)");
		Assert.Equal("hel", result);
	}

	[Fact] public async Task Right_String()
	{
		var result = await S("SELECT RIGHT('hello', 3)");
		Assert.Equal("llo", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REPLACE, TRANSLATE
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Replace_Basic()
	{
		var result = await S("SELECT REPLACE('hello world', 'world', 'BigQuery')");
		Assert.Equal("hello BigQuery", result);
	}

	[Fact] public async Task Replace_EmptySource()
	{
		// Ref: "If original_value is empty, the original value is returned."
		var result = await S("SELECT REPLACE('hello', '', 'x')");
		Assert.Equal("hello", result);
	}

	[Fact] public async Task Translate_Basic()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#translate
		var result = await S("SELECT TRANSLATE('hello', 'lo', 'xy')");
		Assert.Equal("hexxy", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// INSTR (find occurrence position)
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#instr
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Instr_FirstOccurrence()
	{
		var result = await S("SELECT INSTR('hello world hello', 'hello')");
		Assert.Equal("1", result);
	}

	[Fact] public async Task Instr_SecondOccurrence()
	{
		var result = await S("SELECT INSTR('hello world hello', 'hello', 1, 2)");
		Assert.Equal("13", result);
	}

	[Fact] public async Task Instr_NotFound()
	{
		var result = await S("SELECT INSTR('hello', 'xyz')");
		Assert.Equal("0", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Math edge cases
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Power_Basic()
	{
		var result = await S("SELECT POWER(2, 10)");
		Assert.Equal("1024", result);
	}

	[Fact] public async Task Sqrt_Basic()
	{
		var result = await S("SELECT SQRT(144)");
		Assert.Equal("12", result);
	}

	[Fact] public async Task Log_Base10()
	{
		var result = await S("SELECT LOG10(1000)");
		Assert.Equal("3", result);
	}

	[Fact] public async Task Abs_Negative()
	{
		var result = await S("SELECT ABS(-42)");
		Assert.Equal("42", result);
	}

	[Fact] public async Task Sign_Negative()
	{
		var result = await S("SELECT SIGN(-15)");
		Assert.Equal("-1", result);
	}

	[Fact] public async Task Sign_Positive()
	{
		var result = await S("SELECT SIGN(15)");
		Assert.Equal("1", result);
	}

	[Fact] public async Task Sign_Zero()
	{
		var result = await S("SELECT SIGN(0)");
		Assert.Equal("0", result);
	}

	[Fact] public async Task Mod_Basic()
	{
		var result = await S("SELECT MOD(10, 3)");
		Assert.Equal("1", result);
	}

	[Fact] public async Task Mod_Negative()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/mathematical_functions#mod
		//   "The sign of the result is the same as the sign of X."
		var result = await S("SELECT MOD(-10, 3)");
		Assert.Equal("-1", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// IF / IIF / NULLIF / IFNULL
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task If_True()
	{
		var result = await S("SELECT IF(1 > 0, 'yes', 'no')");
		Assert.Equal("yes", result);
	}

	[Fact] public async Task If_False()
	{
		var result = await S("SELECT IF(1 > 2, 'yes', 'no')");
		Assert.Equal("no", result);
	}

	[Fact] public async Task NullIf_Equal()
	{
		var result = await S("SELECT NULLIF(5, 5)");
		Assert.Null(result);
	}

	[Fact] public async Task NullIf_NotEqual()
	{
		var result = await S("SELECT NULLIF(5, 3)");
		Assert.Equal("5", result);
	}

	[Fact] public async Task IfNull_NotNull()
	{
		var result = await S("SELECT IFNULL('value', 'default')");
		Assert.Equal("value", result);
	}

	[Fact] public async Task IfNull_Null()
	{
		var result = await S("SELECT IFNULL(NULL, 'default')");
		Assert.Equal("default", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// SAFE_CAST
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task SafeCast_InvalidStringToInt_ReturnsNull()
	{
		var result = await S("SELECT SAFE_CAST('abc' AS INT64)");
		Assert.Null(result);
	}

	[Fact] public async Task SafeCast_ValidStringToInt()
	{
		var result = await S("SELECT SAFE_CAST('123' AS INT64)");
		Assert.Equal("123", result);
	}

	[Fact] public async Task SafeCast_InvalidStringToDate_ReturnsNull()
	{
		var result = await S("SELECT SAFE_CAST('not-a-date' AS DATE)");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CURRENT_DATE / CURRENT_TIMESTAMP (just verify non-null)
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task CurrentDate_NotNull()
	{
		var result = await S("SELECT CAST(CURRENT_DATE() AS STRING)");
		Assert.NotNull(result);
		Assert.Matches(@"\d{4}-\d{2}-\d{2}", result);
	}

	[Fact] public async Task CurrentTimestamp_NotNull()
	{
		var result = await S("SELECT CAST(CURRENT_TIMESTAMP() AS STRING)");
		Assert.NotNull(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// PARSE_DATE / PARSE_TIMESTAMP
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#parse_date
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task ParseDate_Basic()
	{
		var result = await S("SELECT CAST(PARSE_DATE('%Y-%m-%d', '2024-03-15') AS STRING)");
		Assert.Equal("2024-03-15", result);
	}

	[Fact] public async Task ParseDate_DifferentFormat()
	{
		var result = await S("SELECT CAST(PARSE_DATE('%m/%d/%Y', '03/15/2024') AS STRING)");
		Assert.Equal("2024-03-15", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// CONCAT with various types
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Concat_Strings()
	{
		var result = await S("SELECT CONCAT('hello', ' ', 'world')");
		Assert.Equal("hello world", result);
	}

	[Fact] public async Task Concat_WithNull_ReturnsNull()
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#concat
		//   "If any input argument is NULL, the function returns NULL."
		var result = await S("SELECT CONCAT('hello', NULL, 'world')");
		Assert.Null(result);
	}

	[Fact] public async Task Concat_EmptyString()
	{
		var result = await S("SELECT CONCAT('hello', '', 'world')");
		Assert.Equal("helloworld", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// LENGTH / BYTE_LENGTH / CHAR_LENGTH
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task Length_Basic()
	{
		var result = await S("SELECT LENGTH('hello')");
		Assert.Equal("5", result);
	}

	[Fact] public async Task Length_Empty()
	{
		var result = await S("SELECT LENGTH('')");
		Assert.Equal("0", result);
	}

	[Fact] public async Task CharLength_Same()
	{
		var result = await S("SELECT CHAR_LENGTH('hello')");
		Assert.Equal("5", result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// REGEXP_EXTRACT with groups
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task RegexpExtract_WithGroup()
	{
		var result = await S("SELECT REGEXP_EXTRACT('foo@bar.com', r'@(.+)')");
		Assert.Equal("bar.com", result);
	}

	[Fact] public async Task RegexpExtract_NoMatch()
	{
		var result = await S("SELECT REGEXP_EXTRACT('hello', r'(\\d+)')");
		Assert.Null(result);
	}

	// ───────────────────────────────────────────────────────────────────────────
	// Bit operations
	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#bitwise_operators
	// ───────────────────────────────────────────────────────────────────────────

	[Fact] public async Task BitAnd()
	{
		var result = await S("SELECT 12 & 10");
		Assert.Equal("8", result);
	}

	[Fact] public async Task BitOr()
	{
		var result = await S("SELECT 12 | 10");
		Assert.Equal("14", result);
	}

	[Fact] public async Task BitXor()
	{
		var result = await S("SELECT 12 ^ 10");
		Assert.Equal("6", result);
	}

	[Fact] public async Task BitNot()
	{
		var result = await S("SELECT ~0");
		Assert.Equal("-1", result);
	}

	[Fact] public async Task BitShiftLeft()
	{
		var result = await S("SELECT 1 << 10");
		Assert.Equal("1024", result);
	}

	[Fact] public async Task BitShiftRight()
	{
		var result = await S("SELECT 1024 >> 3");
		Assert.Equal("128", result);
	}
}
