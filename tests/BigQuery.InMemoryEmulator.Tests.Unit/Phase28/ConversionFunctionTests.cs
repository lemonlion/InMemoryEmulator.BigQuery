using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase28;

/// <summary>
/// Phase 28: Conversion functions — PARSE_NUMERIC, PARSE_BIGNUMERIC.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions
/// </summary>
public class ConversionFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		store.Datasets["ds"] = new InMemoryDataset("ds");
		return new QueryExecutor(store, "ds");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#parse_numeric
	//   "Converts a STRING to a NUMERIC value."
	[Theory]
	[InlineData("'123.45'", 123.45)]
	[InlineData("'  -  12.34 '", -12.34)]
	[InlineData("'  1,2,,3,.45 + '", 123.45)]
	[InlineData("'.1234  '", 0.1234)]
	[InlineData("'12.34e-1-'", -1.234)]
	public void ParseNumeric_ValidInputs(string input, double expected)
	{
		var exec = CreateExecutor();
		var result = exec.Execute($"SELECT PARSE_NUMERIC({input}) AS val");
		Assert.Single(result.Rows);
		Assert.Equal(expected, System.Convert.ToDouble(result.Rows[0].F[0].V), 6);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#parse_bignumeric
	//   "Converts a STRING to a BIGNUMERIC value."
	[Fact]
	public void ParseBigNumeric_ValidInput()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT PARSE_BIGNUMERIC('123.45') AS val");
		Assert.Single(result.Rows);
		Assert.Equal(123.45, System.Convert.ToDouble(result.Rows[0].F[0].V), 6);
	}

	[Fact]
	public void ParseNumeric_NullInput_ReturnsNull()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT PARSE_NUMERIC(NULL) AS val");
		Assert.Single(result.Rows);
		Assert.Null(result.Rows[0].F[0].V);
	}
}
