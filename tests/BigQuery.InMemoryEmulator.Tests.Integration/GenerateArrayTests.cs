using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for GENERATE_ARRAY and GENERATE_DATE_ARRAY patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_array
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#generate_date_array
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class GenerateArrayTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public GenerateArrayTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	private async Task<List<string?>> Column(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.Select(r => r[0]?.ToString()).ToList();
	}

	// ---- GENERATE_ARRAY basic ----
	[Fact] public async Task GA_1To5() { var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x"); Assert.Equal(new[] { "1", "2", "3", "4", "5" }, v); }
	[Fact] public async Task GA_0To4() { var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(0, 4)) AS x"); Assert.Equal(new[] { "0", "1", "2", "3", "4" }, v); }
	[Fact] public async Task GA_10To15() { var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(10, 15)) AS x"); Assert.Equal(new[] { "10", "11", "12", "13", "14", "15" }, v); }
	[Fact] public async Task GA_Neg5ToNeg1() { var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(-5, -1)) AS x"); Assert.Equal(new[] { "-5", "-4", "-3", "-2", "-1" }, v); }
	[Fact] public async Task GA_Neg2To2() { var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(-2, 2)) AS x"); Assert.Equal(new[] { "-2", "-1", "0", "1", "2" }, v); }
	[Fact] public async Task GA_Single() { var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(5, 5)) AS x"); Assert.Equal(new[] { "5" }, v); }

	// ---- GENERATE_ARRAY with step ----
	[Fact] public async Task GA_Step2() { var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10, 2)) AS x"); Assert.Equal(new[] { "1", "3", "5", "7", "9" }, v); }
	[Fact] public async Task GA_Step3() { var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(0, 15, 3)) AS x"); Assert.Equal(new[] { "0", "3", "6", "9", "12", "15" }, v); }
	[Fact] public async Task GA_Step5() { var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(0, 20, 5)) AS x"); Assert.Equal(new[] { "0", "5", "10", "15", "20" }, v); }
	[Fact] public async Task GA_Step10() { var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(0, 100, 10)) AS x"); Assert.Equal(new[] { "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100" }, v); }

	// ---- COUNT on GENERATE_ARRAY ----
	[Fact] public async Task GA_Count5() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x"));
	[Fact] public async Task GA_Count10() => Assert.Equal("10", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x"));
	[Fact] public async Task GA_Count100() => Assert.Equal("100", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 100)) AS x"));
	[Fact] public async Task GA_Count0() => Assert.Equal("0", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(5, 1)) AS x"));

	// ---- Aggregates on GENERATE_ARRAY ----
	[Fact] public async Task GA_Sum_1To10() => Assert.Equal("55", await Scalar("SELECT SUM(x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x"));
	[Fact] public async Task GA_Sum_1To100() => Assert.Equal("5050", await Scalar("SELECT SUM(x) FROM UNNEST(GENERATE_ARRAY(1, 100)) AS x"));
	[Fact] public async Task GA_Avg_1To10() => Assert.Equal("5.5", await Scalar("SELECT AVG(x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x"));
	[Fact] public async Task GA_Min_1To10() => Assert.Equal("1", await Scalar("SELECT MIN(x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x"));
	[Fact] public async Task GA_Max_1To10() => Assert.Equal("10", await Scalar("SELECT MAX(x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x"));

	// ---- Filtering GENERATE_ARRAY ----
	[Fact] public async Task GA_Filter_GT5() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x > 5"));
	[Fact] public async Task GA_Filter_Even() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE MOD(x, 2) = 0"));
	[Fact] public async Task GA_Filter_Odd() => Assert.Equal("5", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE MOD(x, 2) = 1"));
	[Fact] public async Task GA_Filter_GTE5_LTE8() => Assert.Equal("4", await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x WHERE x >= 5 AND x <= 8"));

	// ---- GENERATE_ARRAY in expressions ----
	[Fact] public async Task GA_SquareSum() => Assert.Equal("385", await Scalar("SELECT SUM(x * x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x"));
	[Fact] public async Task GA_DoubleValues() { var v = await Column("SELECT x * 2 FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x"); Assert.Equal(new[] { "2", "4", "6", "8", "10" }, v); }
	[Fact] public async Task GA_PlusOffset() { var v = await Column("SELECT x + 100 FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x"); Assert.Equal(new[] { "101", "102", "103", "104", "105" }, v); }

	// ---- GENERATE_DATE_ARRAY ----
	[Fact]
	public async Task GDA_7DaysCountMonday()
	{
		var v = await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-07')) AS d");
		Assert.Equal("7", v);
	}

	[Fact]
	public async Task GDA_31DaysJan()
	{
		var v = await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-31')) AS d");
		Assert.Equal("31", v);
	}

	[Fact]
	public async Task GDA_MonthlyStep()
	{
		var v = await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-12-01', INTERVAL 1 MONTH)) AS d");
		Assert.Equal("12", v);
	}

	[Fact]
	public async Task GDA_WeeklyStep()
	{
		var v = await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-29', INTERVAL 7 DAY)) AS d");
		Assert.Equal("5", v);
	}

	[Fact]
	public async Task GDA_FilterAfterMid()
	{
		var v = await Scalar(@"
SELECT COUNT(*) FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-31')) AS d
WHERE d > DATE '2024-01-15'");
		Assert.Equal("16", v);
	}

	[Fact]
	public async Task GDA_YearRange()
	{
		var v = await Scalar("SELECT COUNT(*) FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-12-31')) AS d");
		Assert.Equal("366", v); // 2024 is a leap year
	}

	// ---- ARRAY_LENGTH on GENERATE_ARRAY ----
	[Fact] public async Task ArrayLength_5() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 5))"));
	[Fact] public async Task ArrayLength_10() => Assert.Equal("10", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 10))"));
	[Fact] public async Task ArrayLength_100() => Assert.Equal("100", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 100))"));
	[Fact] public async Task ArrayLength_Step2() => Assert.Equal("5", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(1, 10, 2))"));
	[Fact] public async Task ArrayLength_Empty() => Assert.Equal("0", await Scalar("SELECT ARRAY_LENGTH(GENERATE_ARRAY(5, 1))"));

	// ---- Nested with other functions ----
	[Fact]
	public async Task GA_StringAgg()
	{
		var v = await Scalar("SELECT STRING_AGG(CAST(x AS STRING)) FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Contains("1", v);
		Assert.Contains("5", v);
	}

	[Fact] public async Task GA_CountDistinctMod() => Assert.Equal("3", await Scalar("SELECT COUNT(DISTINCT MOD(x, 3)) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x"));

	// ---- Multiple UNNEST in subquery ----
	[Fact]
	public async Task GA_SumOfSquaresFormula()
	{
		// sum of 1..10 squared = (10*11*21)/6 = 385
		var v = await Scalar("SELECT SUM(x * x) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x");
		Assert.Equal("385", v);
	}

	[Fact]
	public async Task GA_SumOfCubes()
	{
		// sum of cubes 1..5 = 225
		var v = await Scalar("SELECT SUM(x * x * x) FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x");
		Assert.Equal("225", v);
	}

	// ---- ORDER BY on GENERATE_ARRAY ----
	[Fact]
	public async Task GA_OrderByDesc()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x ORDER BY x DESC");
		Assert.Equal(new[] { "5", "4", "3", "2", "1" }, v);
	}

	[Fact]
	public async Task GA_OrderByExpr()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x ORDER BY MOD(x, 3), x");
		Assert.Equal(new[] { "3", "1", "4", "2", "5" }, v);
	}

	// ---- LIMIT on GENERATE_ARRAY ----
	[Fact(Skip = "Emulator limitation")]
	public async Task GA_Limit3()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x LIMIT 3");
		Assert.Equal(new[] { "1", "2", "3" }, v);
	}

	[Fact(Skip = "Emulator limitation")]
	public async Task GA_OffsetLimit()
	{
		var v = await Column("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x ORDER BY x LIMIT 3 OFFSET 2");
		Assert.Equal(new[] { "3", "4", "5" }, v);
	}
}
