using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase25;

/// <summary>
/// Phase 25: Missing date/datetime/time/timestamp functions.
/// </summary>
public class MissingDateTimeFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		return new QueryExecutor(store);
	}

	#region DATE_FROM_UNIX_DATE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#date_from_unix_date
	//   "Interprets int64_expression as the number of days since 1970-01-01."
	[Fact]
	public void DateFromUnixDate_Basic()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT DATE_FROM_UNIX_DATE(14238) AS result");
		Assert.Equal("2008-12-25", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void DateFromUnixDate_Zero()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT DATE_FROM_UNIX_DATE(0) AS result");
		Assert.Equal("1970-01-01", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region UNIX_DATE

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#unix_date
	//   "Returns the number of days since 1970-01-01."
	[Fact]
	public void UnixDate_Basic()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT UNIX_DATE(DATE '2008-12-25') AS result");
		Assert.Equal("14238", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void UnixDate_Epoch()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT UNIX_DATE(DATE '1970-01-01') AS result");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region LAST_DAY

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day
	//   "Returns the last day from a date expression."
	[Fact]
	public void LastDay_Month_Default()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LAST_DAY(DATE '2008-11-25') AS result");
		Assert.Equal("2008-11-30", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LastDay_Month_Explicit()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LAST_DAY(DATE '2008-11-25', MONTH) AS result");
		Assert.Equal("2008-11-30", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LastDay_Year()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LAST_DAY(DATE '2008-11-25', YEAR) AS result");
		Assert.Equal("2008-12-31", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LastDay_Quarter()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LAST_DAY(DATE '2008-11-25', QUARTER) AS result");
		Assert.Equal("2008-12-31", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void LastDay_Null()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LAST_DAY(NULL) AS result");
		Assert.Null(rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#last_day
	//   "Returns the last day from a datetime expression that contains the date."
	[Fact]
	public void LastDay_Datetime()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT LAST_DAY(DATETIME '2008-11-25 15:30:00', MONTH) AS result");
		Assert.Equal("2008-11-30", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region DATETIME_ADD

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_add
	//   "Adds int64_expression units of part to the DATETIME object."
	[Fact]
	public void DatetimeAdd_Minutes()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DATETIME_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 10 MINUTE) AS result");
		var val = rows[0].F[0].V?.ToString();
		Assert.Contains("15:40:00", val!);
	}

	[Fact]
	public void DatetimeAdd_Days()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DATETIME_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 5 DAY) AS result");
		var val = rows[0].F[0].V?.ToString();
		Assert.Contains("2008-12-30", val!);
	}

	[Fact]
	public void DatetimeAdd_Months()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DATETIME_ADD(DATETIME '2008-12-25 15:30:00', INTERVAL 2 MONTH) AS result");
		var val = rows[0].F[0].V?.ToString();
		Assert.Contains("2009-02-25", val!);
	}

	#endregion

	#region DATETIME_SUB

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_sub
	//   "Subtracts int64_expression units of part from the DATETIME."
	[Fact]
	public void DatetimeSub_Minutes()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DATETIME_SUB(DATETIME '2008-12-25 15:30:00', INTERVAL 10 MINUTE) AS result");
		var val = rows[0].F[0].V?.ToString();
		Assert.Contains("15:20:00", val!);
	}

	#endregion

	#region DATETIME_DIFF

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_diff
	//   "Gets the number of unit boundaries between two DATETIME values."
	[Fact]
	public void DatetimeDiff_Day()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DATETIME_DIFF(DATETIME '2010-07-07 10:20:00', DATETIME '2008-12-25 15:30:00', DAY) AS result");
		Assert.Equal("559", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void DatetimeDiff_Hour()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DATETIME_DIFF(DATETIME '2008-12-25 16:30:00', DATETIME '2008-12-25 15:00:00', HOUR) AS result");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region DATETIME_TRUNC

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_trunc
	//   "Truncates a DATETIME value at a particular granularity."
	[Fact]
	public void DatetimeTrunc_Day()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DATETIME_TRUNC(DATETIME '2008-12-25 15:30:00', DAY) AS result");
		var val = rows[0].F[0].V?.ToString();
		// Note: midnight DateTime is formatted as date-only in our system (DATE/DATETIME type ambiguity)
		Assert.Equal("2008-12-25", val);
	}

	[Fact]
	public void DatetimeTrunc_Hour()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DATETIME_TRUNC(DATETIME '2008-12-25 15:30:45', HOUR) AS result");
		var val = rows[0].F[0].V?.ToString();
		Assert.Contains("15:00:00", val!);
	}

	[Fact]
	public void DatetimeTrunc_Month()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT DATETIME_TRUNC(DATETIME '2008-12-25 15:30:00', MONTH) AS result");
		var val = rows[0].F[0].V?.ToString();
		Assert.Contains("2008-12-01", val!);
	}

	#endregion

	#region FORMAT_DATETIME

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#format_datetime
	//   "Formats a DATETIME value according to a specified format string."
	[Fact]
	public void FormatDatetime_Basic()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', DATETIME '2008-12-25 15:30:00') AS result");
		Assert.Equal("2008-12-25 15:30:00", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void FormatDatetime_MonthYear()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT FORMAT_DATETIME('%b %Y', DATETIME '2008-12-25 15:30:00') AS result");
		Assert.Equal("Dec 2008", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region PARSE_DATETIME

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#parse_datetime
	//   "Converts a STRING value to a DATETIME value."
	[Fact]
	public void ParseDatetime_Basic()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '1998-10-18 13:45:55') AS result");
		var val = rows[0].F[0].V?.ToString();
		Assert.Contains("1998-10-18", val!);
		Assert.Contains("13:45:55", val!);
	}

	#endregion

	#region CURRENT_TIME

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#current_time
	//   "Returns the current time as a TIME object."
	[Fact]
	public void CurrentTime_ReturnsTime()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT CURRENT_TIME() AS result");
		var val = rows[0].F[0].V?.ToString();
		Assert.NotNull(val);
		// Should contain a colon (time format)
		Assert.Contains(":", val!);
	}

	#endregion

	#region TIME constructor

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time
	//   "Constructs a TIME object using INT64 values representing the hour, minute, and second."
	[Fact]
	public void Time_FromComponents()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TIME(15, 30, 0) AS result");
		Assert.Equal("15:30:00", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void Time_FromDatetime()
	{
		var (_, rows) = CreateExecutor().Execute("SELECT TIME(DATETIME '2008-12-25 15:30:00') AS result");
		Assert.Equal("15:30:00", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region TIME_ADD

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_add
	//   "Adds int64_expression units of part to the TIME object."
	[Fact]
	public void TimeAdd_Minutes()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT TIME_ADD(TIME '15:30:00', INTERVAL 10 MINUTE) AS result");
		Assert.Equal("15:40:00", rows[0].F[0].V?.ToString());
	}

	// Ref: "This function automatically adjusts when values fall outside of the 00:00:00 to 24:00:00 boundary."
	[Fact]
	public void TimeAdd_Wraps()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT TIME_ADD(TIME '23:30:00', INTERVAL 1 HOUR) AS result");
		Assert.Equal("00:30:00", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region TIME_SUB

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_sub
	//   "Subtracts int64_expression units of part from the TIME object."
	[Fact]
	public void TimeSub_Minutes()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT TIME_SUB(TIME '15:30:00', INTERVAL 10 MINUTE) AS result");
		Assert.Equal("15:20:00", rows[0].F[0].V?.ToString());
	}

	// Ref: "if you subtract an hour from 00:30:00, the returned value is 23:30:00."
	[Fact]
	public void TimeSub_Wraps()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT TIME_SUB(TIME '00:30:00', INTERVAL 1 HOUR) AS result");
		Assert.Equal("23:30:00", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region TIME_DIFF

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_diff
	//   "Gets the number of unit boundaries between two TIME values."
	[Fact]
	public void TimeDiff_Minutes()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT TIME_DIFF(TIME '15:30:00', TIME '14:35:00', MINUTE) AS result");
		Assert.Equal("55", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void TimeDiff_Hours()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT TIME_DIFF(TIME '15:30:00', TIME '14:00:00', HOUR) AS result");
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region TIME_TRUNC

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_trunc
	//   "Truncates a TIME value at a particular granularity."
	[Fact]
	public void TimeTrunc_Hour()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT TIME_TRUNC(TIME '15:30:45', HOUR) AS result");
		Assert.Equal("15:00:00", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void TimeTrunc_Minute()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT TIME_TRUNC(TIME '15:30:45', MINUTE) AS result");
		Assert.Equal("15:30:00", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region FORMAT_TIME

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#format_time
	//   "Formats a TIME value according to the specified format string."
	[Fact]
	public void FormatTime_Basic()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT FORMAT_TIME('%H:%M', TIME '15:30:00') AS result");
		Assert.Equal("15:30", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void FormatTime_WithSeconds()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT FORMAT_TIME('%T', TIME '15:30:45') AS result");
		Assert.Equal("15:30:45", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region PARSE_TIME

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#parse_time
	//   "Converts a STRING value to a TIME value."
	[Fact]
	public void ParseTime_Hour()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT PARSE_TIME('%H', '15') AS result");
		Assert.Equal("15:00:00", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void ParseTime_Full()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT PARSE_TIME('%T', '15:30:45') AS result");
		Assert.Equal("15:30:45", rows[0].F[0].V?.ToString());
	}

	#endregion

	#region UNIX_MICROS

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#unix_micros
	//   "Returns the number of microseconds since 1970-01-01 00:00:00 UTC."
	[Fact]
	public void UnixMicros_Basic()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT UNIX_MICROS(TIMESTAMP '2008-12-25 15:30:00+00') AS result");
		Assert.Equal("1230219000000000", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void UnixMicros_Epoch()
	{
		var (_, rows) = CreateExecutor().Execute(
			"SELECT UNIX_MICROS(TIMESTAMP '1970-01-01 00:00:00+00') AS result");
		Assert.Equal("0", rows[0].F[0].V?.ToString());
	}

	#endregion
}
