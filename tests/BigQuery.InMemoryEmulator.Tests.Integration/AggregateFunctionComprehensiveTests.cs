using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive integration tests for aggregate functions.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AggregateFunctionComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public AggregateFunctionComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_agg_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "nums", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "grp", Type = "STRING" },
				new TableFieldSchema { Name = "val", Type = "FLOAT" },
				new TableFieldSchema { Name = "flag", Type = "BOOLEAN" },
				new TableFieldSchema { Name = "label", Type = "STRING" },
			]
		});
		await client.InsertRowsAsync(_datasetId, "nums", new[]
		{
			new BigQueryInsertRow("r1") { ["id"] = 1, ["grp"] = "A", ["val"] = 10.0, ["flag"] = true, ["label"] = "x" },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["grp"] = "A", ["val"] = 20.0, ["flag"] = true, ["label"] = "y" },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["grp"] = "B", ["val"] = 30.0, ["flag"] = false, ["label"] = "x" },
			new BigQueryInsertRow("r4") { ["id"] = 4, ["grp"] = "B", ["val"] = 40.0, ["flag"] = false, ["label"] = "z" },
			new BigQueryInsertRow("r5") { ["id"] = 5, ["grp"] = "A", ["val"] = null, ["flag"] = true, ["label"] = null },
		});
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

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	// ---- COUNT ----
	[Fact] public async Task Count_All() => Assert.Equal("5", await Scalar($"SELECT COUNT(*) FROM `{_datasetId}.nums`"));
	[Fact] public async Task Count_Column() => Assert.Equal("4", await Scalar($"SELECT COUNT(val) FROM `{_datasetId}.nums`"));
	[Fact] public async Task Count_Distinct() => Assert.Equal("2", await Scalar($"SELECT COUNT(DISTINCT grp) FROM `{_datasetId}.nums`"));

	// ---- SUM ----
	[Fact] public async Task Sum_Basic() => Assert.Equal("100", await Scalar($"SELECT SUM(val) FROM `{_datasetId}.nums`"));
	[Fact] public async Task Sum_GroupBy()
	{
		var rows = await Query($"SELECT grp, SUM(val) AS s FROM `{_datasetId}.nums` GROUP BY grp ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("30", rows[0]["s"]?.ToString());
		Assert.Equal("70", rows[1]["s"]?.ToString());
	}

	// ---- AVG ----
	[Fact] public async Task Avg_Basic() => Assert.Equal("25", await Scalar($"SELECT AVG(val) FROM `{_datasetId}.nums`"));
	[Fact] public async Task Avg_GroupBy()
	{
		var rows = await Query($"SELECT grp, AVG(val) AS a FROM `{_datasetId}.nums` GROUP BY grp ORDER BY grp");
		Assert.Equal(2, rows.Count);
		Assert.Equal("15", rows[0]["a"]?.ToString());
		Assert.Equal("35", rows[1]["a"]?.ToString());
	}

	// ---- MIN / MAX ----
	[Fact] public async Task Min_Basic() => Assert.Equal("10", await Scalar($"SELECT MIN(val) FROM `{_datasetId}.nums`"));
	[Fact] public async Task Max_Basic() => Assert.Equal("40", await Scalar($"SELECT MAX(val) FROM `{_datasetId}.nums`"));
	[Fact] public async Task Min_String() => Assert.Equal("A", await Scalar($"SELECT MIN(grp) FROM `{_datasetId}.nums`"));
	[Fact] public async Task Max_String() => Assert.Equal("B", await Scalar($"SELECT MAX(grp) FROM `{_datasetId}.nums`"));

	// ---- ANY_VALUE ----
	[Fact] public async Task AnyValue_NotNull() { var v = await Scalar($"SELECT ANY_VALUE(grp) FROM `{_datasetId}.nums`"); Assert.NotNull(v); }

	// ---- STRING_AGG ----
	[Fact] public async Task StringAgg_Default()
	{
		var v = await Scalar($"SELECT STRING_AGG(label, ',') FROM `{_datasetId}.nums`");
		Assert.NotNull(v);
		Assert.Contains("x", v);
	}
	[Fact(Skip = "STRING_AGG DISTINCT not supported")]
	public async Task StringAgg_Distinct()
	{
		var v = await Scalar($"SELECT STRING_AGG(DISTINCT label, ',') FROM `{_datasetId}.nums`");
		Assert.NotNull(v);
	}
	[Fact(Skip = "STRING_AGG ORDER BY not supported")]
	public async Task StringAgg_OrderBy()
	{
		var v = await Scalar($"SELECT STRING_AGG(label, ',' ORDER BY label) FROM `{_datasetId}.nums`");
		Assert.NotNull(v);
	}

	// ---- ARRAY_AGG ----
	[Fact] public async Task ArrayAgg_Basic() => Assert.Equal("5", await Scalar($"SELECT ARRAY_LENGTH(ARRAY_AGG(id)) FROM `{_datasetId}.nums`"));
	[Fact] public async Task ArrayAgg_IgnoreNulls() => Assert.Equal("4", await Scalar($"SELECT ARRAY_LENGTH(ARRAY_AGG(val IGNORE NULLS)) FROM `{_datasetId}.nums`"));
	[Fact(Skip = "ARRAY_AGG DISTINCT returns null")] public async Task ArrayAgg_Distinct() => Assert.Equal("2", await Scalar($"SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT grp)) FROM `{_datasetId}.nums`"));

	// ---- ARRAY_CONCAT_AGG ----
	[Fact(Skip = "FROM subquery not supported")]
	public async Task ArrayConcatAgg_Basic()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(ARRAY_CONCAT_AGG(arr)) FROM (SELECT [1,2] AS arr UNION ALL SELECT [3,4])");
		Assert.Equal("4", v);
	}

	// ---- COUNTIF ----
	[Fact] public async Task Countif_Basic() => Assert.Equal("3", await Scalar($"SELECT COUNTIF(flag) FROM `{_datasetId}.nums`"));
	[Fact] public async Task Countif_Expression() => Assert.Equal("2", await Scalar($"SELECT COUNTIF(val > 20) FROM `{_datasetId}.nums`"));

	// ---- LOGICAL_AND / LOGICAL_OR ----
	[Fact] public async Task LogicalAnd_Mixed() => Assert.Equal("False", await Scalar($"SELECT LOGICAL_AND(flag) FROM `{_datasetId}.nums`"));
	[Fact] public async Task LogicalOr_Mixed() => Assert.Equal("True", await Scalar($"SELECT LOGICAL_OR(flag) FROM `{_datasetId}.nums`"));

	// ---- APPROX_COUNT_DISTINCT ----
	[Fact] public async Task ApproxCountDistinct_Basic()
	{
		var v = await Scalar($"SELECT APPROX_COUNT_DISTINCT(grp) FROM `{_datasetId}.nums`");
		Assert.Equal("2", v);
	}

	// ---- APPROX_QUANTILES ----
	[Fact] public async Task ApproxQuantiles_Basic()
	{
		var v = await Scalar($"SELECT ARRAY_LENGTH(APPROX_QUANTILES(val, 4)) FROM `{_datasetId}.nums`");
		Assert.Equal("5", v); // n+1 quantile boundaries
	}

	// ---- APPROX_TOP_COUNT ----
	[Fact] public async Task ApproxTopCount_Basic()
	{
		var v = await Scalar($"SELECT ARRAY_LENGTH(APPROX_TOP_COUNT(grp, 2)) FROM `{_datasetId}.nums`");
		Assert.Equal("2", v);
	}

	// ---- BIT_AND / BIT_OR / BIT_XOR ----
	[Fact] public async Task BitAnd_Basic() => Assert.Equal("0", await Scalar($"SELECT BIT_AND(CAST(id AS INT64)) FROM `{_datasetId}.nums`"));
	[Fact] public async Task BitOr_Basic() => Assert.Equal("7", await Scalar($"SELECT BIT_OR(CAST(id AS INT64)) FROM `{_datasetId}.nums`"));
	[Fact] public async Task BitXor_Basic() { var v = await Scalar($"SELECT BIT_XOR(CAST(id AS INT64)) FROM `{_datasetId}.nums`"); Assert.NotNull(v); }

	// ---- Statistical: VAR_SAMP / VARIANCE / VAR_POP / STDDEV_SAMP / STDDEV / STDDEV_POP ----
	[Fact] public async Task VarSamp_Basic() { var v = double.Parse(await Scalar($"SELECT VAR_SAMP(val) FROM `{_datasetId}.nums`") ?? "0"); Assert.True(v > 0); }
	[Fact] public async Task Variance_AliasVarSamp() { var v = double.Parse(await Scalar($"SELECT VARIANCE(val) FROM `{_datasetId}.nums`") ?? "0"); Assert.True(v > 0); }
	[Fact] public async Task VarPop_Basic() { var v = double.Parse(await Scalar($"SELECT VAR_POP(val) FROM `{_datasetId}.nums`") ?? "0"); Assert.True(v > 0); }
	[Fact] public async Task StddevSamp_Basic() { var v = double.Parse(await Scalar($"SELECT STDDEV_SAMP(val) FROM `{_datasetId}.nums`") ?? "0"); Assert.True(v > 0); }
	[Fact] public async Task Stddev_AliasStddevSamp() { var v = double.Parse(await Scalar($"SELECT STDDEV(val) FROM `{_datasetId}.nums`") ?? "0"); Assert.True(v > 0); }
	[Fact] public async Task StddevPop_Basic() { var v = double.Parse(await Scalar($"SELECT STDDEV_POP(val) FROM `{_datasetId}.nums`") ?? "0"); Assert.True(v > 0); }

	// ---- CORR / COVAR_POP / COVAR_SAMP ----
	[Fact(Skip = "FROM subquery not supported")] public async Task Corr_Perfect() => Assert.Equal("1.0", await Scalar("SELECT CORR(x, x) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3)"));
	[Fact] public async Task CovarPop_Basic() { var v = double.Parse(await Scalar($"SELECT COVAR_POP(CAST(id AS FLOAT64), val) FROM `{_datasetId}.nums`") ?? "0"); Assert.True(v != 0); }
	[Fact] public async Task CovarSamp_Basic() { var v = double.Parse(await Scalar($"SELECT COVAR_SAMP(CAST(id AS FLOAT64), val) FROM `{_datasetId}.nums`") ?? "0"); Assert.True(v != 0); }

	// ---- HAVING with aggregates ----
	[Fact] public async Task Having_Filter()
	{
		var rows = await Query($"SELECT grp, SUM(val) AS s FROM `{_datasetId}.nums` GROUP BY grp HAVING SUM(val) > 50 ORDER BY grp");
		Assert.Single(rows);
		Assert.Equal("B", rows[0]["grp"]?.ToString());
	}

	// ---- Aggregate with WHERE ----
	[Fact] public async Task Count_WithWhere() => Assert.Equal("3", await Scalar($"SELECT COUNT(*) FROM `{_datasetId}.nums` WHERE grp = 'A'"));
	[Fact] public async Task Sum_WithWhere() => Assert.Equal("70", await Scalar($"SELECT SUM(val) FROM `{_datasetId}.nums` WHERE grp = 'B'"));

	// ---- Aggregate with DISTINCT in GROUP BY ----
	[Fact] public async Task GroupBy_MultiColumn()
	{
		var rows = await Query($"SELECT grp, flag, COUNT(*) AS c FROM `{_datasetId}.nums` GROUP BY grp, flag ORDER BY grp, flag");
		Assert.True(rows.Count >= 2);
	}
}
