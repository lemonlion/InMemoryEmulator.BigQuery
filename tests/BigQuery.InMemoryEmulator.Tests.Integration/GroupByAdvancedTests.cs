using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for GROUP BY, HAVING, aggregate edge cases,
/// and DISTINCT patterns.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class GroupByAdvancedTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public GroupByAdvancedTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_gb_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "sales", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "product", Type = "STRING" },
				new TableFieldSchema { Name = "category", Type = "STRING" },
				new TableFieldSchema { Name = "amount", Type = "FLOAT" },
				new TableFieldSchema { Name = "qty", Type = "INTEGER" },
			]
		});
		await client.InsertRowsAsync(_datasetId, "sales", new[]
		{
			new BigQueryInsertRow("r1")  { ["id"] = 1,  ["product"] = "A", ["category"] = "X", ["amount"] = 100.0, ["qty"] = 10 },
			new BigQueryInsertRow("r2")  { ["id"] = 2,  ["product"] = "B", ["category"] = "X", ["amount"] = 200.0, ["qty"] = 5 },
			new BigQueryInsertRow("r3")  { ["id"] = 3,  ["product"] = "A", ["category"] = "Y", ["amount"] = 150.0, ["qty"] = 8 },
			new BigQueryInsertRow("r4")  { ["id"] = 4,  ["product"] = "C", ["category"] = "Y", ["amount"] = 300.0, ["qty"] = 12 },
			new BigQueryInsertRow("r5")  { ["id"] = 5,  ["product"] = "B", ["category"] = "X", ["amount"] = 250.0, ["qty"] = 3 },
			new BigQueryInsertRow("r6")  { ["id"] = 6,  ["product"] = "A", ["category"] = "X", ["amount"] = 120.0, ["qty"] = 7 },
			new BigQueryInsertRow("r7")  { ["id"] = 7,  ["product"] = "C", ["category"] = "Z", ["amount"] = 400.0, ["qty"] = 2 },
			new BigQueryInsertRow("r8")  { ["id"] = 8,  ["product"] = "A", ["category"] = "Z", ["amount"] = 90.0,  ["qty"] = 15 },
		});
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Q(string sql)
	{
		var client = await _fixture.GetClientAsync();
		return (await client.ExecuteQueryAsync(sql, parameters: null)).ToList();
	}

	private async Task<string?> S(string sql)
	{
		var rows = await Q(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- GROUP BY single column ----
	[Fact] public async Task GroupBy_Count()
	{
		var rows = await Q($"SELECT product, COUNT(*) AS cnt FROM `{_datasetId}.sales` GROUP BY product ORDER BY product");
		Assert.Equal(3, rows.Count);
		Assert.Equal("A", rows[0]["product"]?.ToString());
		Assert.Equal("4", rows[0]["cnt"]?.ToString());
	}

	[Fact] public async Task GroupBy_Sum()
	{
		var rows = await Q($"SELECT product, SUM(amount) AS total FROM `{_datasetId}.sales` GROUP BY product ORDER BY product");
		Assert.Equal(3, rows.Count);
		Assert.Equal("460", rows[0]["total"]?.ToString()); // A: 100+150+120+90
	}

	[Fact] public async Task GroupBy_Avg()
	{
		var rows = await Q($"SELECT product, AVG(amount) AS avg_amt FROM `{_datasetId}.sales` GROUP BY product ORDER BY product");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task GroupBy_Min()
	{
		var rows = await Q($"SELECT product, MIN(amount) AS min_amt FROM `{_datasetId}.sales` GROUP BY product ORDER BY product");
		Assert.Equal("90", rows[0]["min_amt"]?.ToString()); // A: min is 90
	}

	[Fact] public async Task GroupBy_Max()
	{
		var rows = await Q($"SELECT product, MAX(amount) AS max_amt FROM `{_datasetId}.sales` GROUP BY product ORDER BY product");
		Assert.Equal("150", rows[0]["max_amt"]?.ToString()); // A: max is 150
	}

	// ---- GROUP BY multiple columns ----
	[Fact] public async Task GroupBy_TwoColumns()
	{
		var rows = await Q($"SELECT product, category, COUNT(*) AS cnt FROM `{_datasetId}.sales` GROUP BY product, category ORDER BY product, category");
		Assert.True(rows.Count >= 6);
	}

	// ---- HAVING ----
	[Fact] public async Task Having_Filter()
	{
		// A=4, B=2, C=2 — all three have count >= 2
		var rows = await Q($"SELECT product, COUNT(*) AS cnt FROM `{_datasetId}.sales` GROUP BY product HAVING COUNT(*) >= 2 ORDER BY product");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task Having_Sum()
	{
		var rows = await Q($"SELECT product, SUM(amount) AS total FROM `{_datasetId}.sales` GROUP BY product HAVING SUM(amount) > 400 ORDER BY product");
		Assert.True(rows.Count >= 1);
	}

	[Fact] public async Task Having_MultipleConditions()
	{
		var rows = await Q($"SELECT product, COUNT(*) AS cnt, SUM(amount) AS total FROM `{_datasetId}.sales` GROUP BY product HAVING COUNT(*) >= 2 AND SUM(amount) > 400 ORDER BY product");
		Assert.True(rows.Count >= 1);
	}

	// ---- DISTINCT ----
	[Fact] public async Task Distinct_SingleColumn()
	{
		var rows = await Q($"SELECT DISTINCT product FROM `{_datasetId}.sales` ORDER BY product");
		Assert.Equal(3, rows.Count);
	}

	[Fact] public async Task Distinct_MultiColumn()
	{
		var rows = await Q($"SELECT DISTINCT product, category FROM `{_datasetId}.sales` ORDER BY product, category");
		Assert.True(rows.Count >= 6);
	}

	// ---- COUNT(DISTINCT ...) ----
	[Fact] public async Task CountDistinct()
	{
		var v = await S($"SELECT COUNT(DISTINCT product) FROM `{_datasetId}.sales`");
		Assert.Equal("3", v);
	}

	[Fact] public async Task CountDistinct_Category()
	{
		var v = await S($"SELECT COUNT(DISTINCT category) FROM `{_datasetId}.sales`");
		Assert.Equal("3", v);
	}

	// ---- STRING_AGG ----
	[Fact] public async Task StringAgg_Basic()
	{
		var v = await S($"SELECT STRING_AGG(DISTINCT product, ',') FROM `{_datasetId}.sales`");
		Assert.NotNull(v);
		// Contains all 3 products
		Assert.Contains("A", v);
		Assert.Contains("B", v);
		Assert.Contains("C", v);
	}

	[Fact] public async Task StringAgg_PerGroup()
	{
		var rows = await Q($"SELECT category, STRING_AGG(product, ',') AS products FROM `{_datasetId}.sales` GROUP BY category ORDER BY category");
		Assert.Equal(3, rows.Count);
	}

	// ---- ARRAY_AGG ----
	[Fact] public async Task ArrayAgg_Basic()
	{
		var v = await S($"SELECT ARRAY_LENGTH(ARRAY_AGG(product)) FROM `{_datasetId}.sales`");
		Assert.Equal("8", v);
	}

	[Fact] public async Task ArrayAgg_Distinct()
	{
		var v = await S($"SELECT ARRAY_LENGTH(ARRAY_AGG(DISTINCT product)) FROM `{_datasetId}.sales`");
		Assert.Equal("3", v);
	}

	// ---- ANY_VALUE ----
	[Fact] public async Task AnyValue_NotNull()
	{
		var v = await S($"SELECT ANY_VALUE(product) FROM `{_datasetId}.sales`");
		Assert.NotNull(v);
		Assert.Contains(v, new[] { "A", "B", "C" });
	}

	// ---- COUNTIF ----
	[Fact] public async Task CountIf_Basic()
	{
		var v = await S($"SELECT COUNTIF(amount > 200) FROM `{_datasetId}.sales`");
		Assert.Equal("3", v); // 250, 300, 400
	}

	// ---- LOGICAL_AND / LOGICAL_OR ----
	[Fact] public async Task LogicalAnd()
	{
		var v = await S($"SELECT LOGICAL_AND(amount > 0) FROM `{_datasetId}.sales`");
		Assert.Equal("True", v);
	}

	[Fact] public async Task LogicalOr()
	{
		var v = await S($"SELECT LOGICAL_OR(amount > 350) FROM `{_datasetId}.sales`");
		Assert.Equal("True", v);
	}

	// ---- APPROX_COUNT_DISTINCT ----
	[Fact] public async Task ApproxCountDistinct()
	{
		var v = await S($"SELECT APPROX_COUNT_DISTINCT(product) FROM `{_datasetId}.sales`");
		Assert.Equal("3", v);
	}

	// ---- Variance/StdDev ----
	[Fact] public async Task Variance_NotNull()
	{
		var v = await S($"SELECT VARIANCE(amount) FROM `{_datasetId}.sales`");
		Assert.NotNull(v);
	}

	[Fact] public async Task Stddev_NotNull()
	{
		var v = await S($"SELECT STDDEV(amount) FROM `{_datasetId}.sales`");
		Assert.NotNull(v);
	}

	[Fact] public async Task VarPop_NotNull()
	{
		var v = await S($"SELECT VAR_POP(amount) FROM `{_datasetId}.sales`");
		Assert.NotNull(v);
	}

	[Fact] public async Task StddevPop_NotNull()
	{
		var v = await S($"SELECT STDDEV_POP(amount) FROM `{_datasetId}.sales`");
		Assert.NotNull(v);
	}

	// ---- BIT_AND / BIT_OR / BIT_XOR ----
	[Fact] public async Task BitAnd()
	{
		var v = await S("SELECT BIT_AND(x) FROM UNNEST([7, 3, 5]) AS x");
		Assert.Equal("1", v); // 111 & 011 & 101 = 001
	}

	[Fact] public async Task BitOr()
	{
		var v = await S("SELECT BIT_OR(x) FROM UNNEST([1, 2, 4]) AS x");
		Assert.Equal("7", v); // 001 | 010 | 100 = 111
	}

	[Fact] public async Task BitXor()
	{
		var v = await S("SELECT BIT_XOR(x) FROM UNNEST([1, 3]) AS x");
		Assert.Equal("2", v); // 01 ^ 11 = 10
	}

	// ---- APPROX_QUANTILES ----
	[Fact] public async Task ApproxQuantiles_Median()
	{
		var v = await S($"SELECT (APPROX_QUANTILES(amount, 2))[OFFSET(1)] FROM `{_datasetId}.sales`");
		Assert.NotNull(v);
	}

	// ---- APPROX_TOP_COUNT ----
	[Fact] public async Task ApproxTopCount_Basic()
	{
		var v = await S($"SELECT ARRAY_LENGTH(APPROX_TOP_COUNT(product, 2)) FROM `{_datasetId}.sales`");
		Assert.Equal("2", v);
	}

	// ---- GROUP BY with expression ----
	[Fact] public async Task GroupBy_Expression()
	{
		var rows = await Q($"SELECT amount > 200 AS expensive, COUNT(*) AS cnt FROM `{_datasetId}.sales` GROUP BY amount > 200 ORDER BY expensive");
		Assert.Equal(2, rows.Count);
	}

	// ---- GROUP BY ordinal ----
	[Fact] public async Task GroupBy_Ordinal()
	{
		var rows = await Q($"SELECT product, COUNT(*) AS cnt FROM `{_datasetId}.sales` GROUP BY 1 ORDER BY 1");
		Assert.Equal(3, rows.Count);
	}

	// ---- COUNT(*) with no table ----
	[Fact] public async Task Count_Star_NoFrom() => Assert.Equal("1", await S("SELECT COUNT(*)"));

	// ---- Aggregate on empty ----
	[Fact] public async Task Sum_Empty() => Assert.Null(await S("SELECT SUM(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x"));
	[Fact] public async Task Count_Empty() => Assert.Equal("0", await S("SELECT COUNT(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x"));
	[Fact] public async Task Avg_Empty() => Assert.Null(await S("SELECT AVG(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x"));
	[Fact] public async Task Min_Empty() => Assert.Null(await S("SELECT MIN(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x"));
	[Fact] public async Task Max_Empty() => Assert.Null(await S("SELECT MAX(x) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS x"));

	// ---- Aggregate ignoring NULLs ----
	[Fact] public async Task Sum_IgnoresNulls() => Assert.Equal("6", await S("SELECT SUM(x) FROM UNNEST([1, NULL, 2, NULL, 3]) AS x"));
	[Fact] public async Task Count_IgnoresNulls() => Assert.Equal("3", await S("SELECT COUNT(x) FROM UNNEST([1, NULL, 2, NULL, 3]) AS x"));
	[Fact] public async Task Avg_IgnoresNulls() => Assert.Equal("2", await S("SELECT CAST(AVG(x) AS INT64) FROM UNNEST([1, NULL, 2, NULL, 3]) AS x"));
}
