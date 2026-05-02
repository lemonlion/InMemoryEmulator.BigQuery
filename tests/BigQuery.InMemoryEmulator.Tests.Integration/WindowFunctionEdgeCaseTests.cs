using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Edge-case integration tests for window functions: frame specs, NULLs, ties, etc.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class WindowFunctionEdgeCaseTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public WindowFunctionEdgeCaseTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_wf_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "t", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "grp", Type = "STRING" },
				new TableFieldSchema { Name = "val", Type = "FLOAT" },
			]
		});
		await client.InsertRowsAsync(_datasetId, "t", new[]
		{
			new BigQueryInsertRow("a") { ["id"] = 1, ["grp"] = "A", ["val"] = 10.0 },
			new BigQueryInsertRow("b") { ["id"] = 2, ["grp"] = "A", ["val"] = 20.0 },
			new BigQueryInsertRow("c") { ["id"] = 3, ["grp"] = "A", ["val"] = 30.0 },
			new BigQueryInsertRow("d") { ["id"] = 4, ["grp"] = "A", ["val"] = 40.0 },
			new BigQueryInsertRow("e") { ["id"] = 5, ["grp"] = "B", ["val"] = 100.0 },
			new BigQueryInsertRow("f") { ["id"] = 6, ["grp"] = "B", ["val"] = 200.0 },
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

	// ---- ROWS BETWEEN frame specs ----
	[Fact] public async Task RowsBetween_UnboundedPrecedingCurrentRow()
	{
		var rows = await Q($"SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM `{_datasetId}.t` WHERE grp='A' ORDER BY id");
		Assert.Equal("10", rows[0]["s"]?.ToString());
		Assert.Equal("30", rows[1]["s"]?.ToString());
		Assert.Equal("60", rows[2]["s"]?.ToString());
		Assert.Equal("100", rows[3]["s"]?.ToString());
	}

	[Fact] public async Task RowsBetween_1Preceding1Following()
	{
		var rows = await Q($"SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM `{_datasetId}.t` WHERE grp='A' ORDER BY id");
		Assert.Equal("30", rows[0]["s"]?.ToString());   // 10+20
		Assert.Equal("60", rows[1]["s"]?.ToString());   // 10+20+30
		Assert.Equal("90", rows[2]["s"]?.ToString());   // 20+30+40
		Assert.Equal("70", rows[3]["s"]?.ToString());   // 30+40
	}

	[Fact] public async Task RowsBetween_CurrentRowUnboundedFollowing()
	{
		var rows = await Q($"SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s FROM `{_datasetId}.t` WHERE grp='A' ORDER BY id");
		Assert.Equal("100", rows[0]["s"]?.ToString());
		Assert.Equal("90", rows[1]["s"]?.ToString());
		Assert.Equal("70", rows[2]["s"]?.ToString());
		Assert.Equal("40", rows[3]["s"]?.ToString());
	}

	[Fact] public async Task RowsBetween_2Preceding0Following()
	{
		var rows = await Q($"SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS c FROM `{_datasetId}.t` WHERE grp='A' ORDER BY id");
		Assert.Equal("1", rows[0]["c"]?.ToString());
		Assert.Equal("2", rows[1]["c"]?.ToString());
		Assert.Equal("3", rows[2]["c"]?.ToString());
		Assert.Equal("3", rows[3]["c"]?.ToString());
	}

	// ---- NTH_VALUE ----
	[Fact] public async Task NthValue_Second()
	{
		var v = await S($"SELECT NTH_VALUE(val, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM `{_datasetId}.t` WHERE grp='A' LIMIT 1");
		Assert.Equal("20", v);
	}

	[Fact] public async Task NthValue_Third()
	{
		var v = await S($"SELECT NTH_VALUE(val, 3) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM `{_datasetId}.t` WHERE grp='A' LIMIT 1");
		Assert.Equal("30", v);
	}

	[Fact] public async Task NthValue_BeyondRows_IsNull()
	{
		var v = await S($"SELECT NTH_VALUE(val, 10) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM `{_datasetId}.t` WHERE grp='A' LIMIT 1");
		Assert.Null(v);
	}

	// ---- LAST_VALUE ----
	[Fact] public async Task LastValue_WithFullFrame()
	{
		var v = await S($"SELECT LAST_VALUE(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM `{_datasetId}.t` WHERE grp='A' LIMIT 1");
		Assert.Equal("40", v);
	}

	[Fact] public async Task LastValue_Partitioned()
	{
		var rows = await Q($"SELECT grp, LAST_VALUE(val) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM `{_datasetId}.t` ORDER BY grp, id");
		Assert.Equal("40", rows[0]["lv"]?.ToString());   // group A last
		Assert.Equal("200", rows[4]["lv"]?.ToString());  // group B last
	}

	// ---- Ranking on NULLs ----
	[Fact] public async Task RowNumber_WithSubquery()
	{
		var rows = await Q("SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn FROM UNNEST([3,1,2]) AS x ORDER BY rn");
		Assert.Equal("1", rows[0]["x"]?.ToString());
		Assert.Equal("2", rows[1]["x"]?.ToString());
		Assert.Equal("3", rows[2]["x"]?.ToString());
	}

	[Fact] public async Task Rank_WithNulls()
	{
		var rows = await Q("SELECT x, RANK() OVER (ORDER BY x) AS r FROM UNNEST([CAST(NULL AS INT64), 1, 1, 3]) AS x ORDER BY r");
		Assert.Equal(4, rows.Count);
	}

	[Fact] public async Task DenseRank_Partitioned()
	{
		var rows = await Q($"SELECT grp, val, DENSE_RANK() OVER (PARTITION BY grp ORDER BY val) AS dr FROM `{_datasetId}.t` ORDER BY grp, val");
		Assert.Equal("1", rows[0]["dr"]?.ToString());
	}

	// ---- AVG window ----
	[Fact] public async Task Avg_RunningPartitioned()
	{
		var rows = await Q($"SELECT id, grp, AVG(val) OVER (PARTITION BY grp ORDER BY id) AS a FROM `{_datasetId}.t` ORDER BY grp, id");
		Assert.Equal("10", rows[0]["a"]?.ToString());
		Assert.Equal("15", rows[1]["a"]?.ToString());
	}

	// ---- MIN/MAX window ----
	[Fact] public async Task Min_Window()
	{
		var v = await S($"SELECT MIN(val) OVER () FROM `{_datasetId}.t` LIMIT 1");
		Assert.Equal("10", v);
	}

	[Fact] public async Task Max_Window()
	{
		var v = await S($"SELECT MAX(val) OVER () FROM `{_datasetId}.t` LIMIT 1");
		Assert.Equal("200", v);
	}

	// ---- Multiple window functions in one query ----
	[Fact] public async Task Multiple_DifferentFrames()
	{
		var rows = await Q($"SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn, SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM `{_datasetId}.t` WHERE grp='A' ORDER BY id");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
		Assert.Equal("10", rows[0]["s"]?.ToString());
		Assert.Equal("4", rows[3]["rn"]?.ToString());
		Assert.Equal("100", rows[3]["s"]?.ToString());
	}

	// ---- CUME_DIST ----
	[Fact] public async Task CumeDist_Partitioned()
	{
		var rows = await Q($"SELECT id, grp, CUME_DIST() OVER (PARTITION BY grp ORDER BY val) AS cd FROM `{_datasetId}.t` ORDER BY grp, val");
		Assert.Equal("0.25", rows[0]["cd"]?.ToString());
		Assert.Equal("1", rows[3]["cd"]?.ToString());
	}

	// ---- PERCENT_RANK ----
	[Fact] public async Task PercentRank_Partitioned()
	{
		var rows = await Q($"SELECT id, grp, PERCENT_RANK() OVER (PARTITION BY grp ORDER BY val) AS pr FROM `{_datasetId}.t` ORDER BY grp, val");
		Assert.Equal("0", rows[0]["pr"]?.ToString());
	}

	// ---- NTILE edge cases ----
	[Fact] public async Task Ntile_MoreBucketsThanRows()
	{
		var rows = await Q($"SELECT id, NTILE(10) OVER (ORDER BY id) AS nt FROM `{_datasetId}.t` WHERE grp='B' ORDER BY id");
		Assert.Equal(2, rows.Count);
		Assert.Equal("1", rows[0]["nt"]?.ToString());
		Assert.Equal("2", rows[1]["nt"]?.ToString());
	}

	[Fact] public async Task Ntile_OneBucket()
	{
		var rows = await Q($"SELECT id, NTILE(1) OVER (ORDER BY id) AS nt FROM `{_datasetId}.t` WHERE grp='A' ORDER BY id");
		Assert.All(rows, r => Assert.Equal("1", r["nt"]?.ToString()));
	}

	// ---- LAG/LEAD with nulls ----
	[Fact] public async Task Lag_Offset3()
	{
		var rows = await Q($"SELECT id, LAG(val, 3) OVER (ORDER BY id) AS l FROM `{_datasetId}.t` WHERE grp='A' ORDER BY id");
		Assert.Null(rows[0]["l"]);
		Assert.Null(rows[1]["l"]);
		Assert.Null(rows[2]["l"]);
		Assert.Equal("10", rows[3]["l"]?.ToString());
	}

	[Fact] public async Task Lead_Offset3_Default()
	{
		var rows = await Q($"SELECT id, LEAD(val, 3, -1) OVER (ORDER BY id) AS l FROM `{_datasetId}.t` WHERE grp='A' ORDER BY id");
		Assert.Equal("40", rows[0]["l"]?.ToString());
		Assert.Equal("-1", rows[1]["l"]?.ToString());
	}

	// ---- FIRST_VALUE with frame ----
	[Fact] public async Task FirstValue_WithRowsFrame()
	{
		var rows = await Q($"SELECT id, FIRST_VALUE(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS fv FROM `{_datasetId}.t` WHERE grp='A' ORDER BY id");
		Assert.Equal("10", rows[0]["fv"]?.ToString());
		Assert.Equal("10", rows[1]["fv"]?.ToString());
		Assert.Equal("20", rows[2]["fv"]?.ToString());
		Assert.Equal("30", rows[3]["fv"]?.ToString());
	}

	// ---- COUNT DISTINCT over window ----
	[Fact] public async Task CountStar_Over()
	{
		var v = await S($"SELECT COUNT(*) OVER () FROM `{_datasetId}.t` LIMIT 1");
		Assert.Equal("6", v);
	}

	// ---- SUM partitioned unbounded ----
	[Fact] public async Task Sum_PartitionedTotal()
	{
		var rows = await Q($"SELECT grp, SUM(val) OVER (PARTITION BY grp) AS total FROM `{_datasetId}.t` ORDER BY grp, id");
		Assert.Equal("100", rows[0]["total"]?.ToString());
		Assert.Equal("300", rows[4]["total"]?.ToString());
	}

	// ---- Window with WHERE ----
	[Fact] public async Task Window_WithWhereFilter()
	{
		var rows = await Q($"SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM `{_datasetId}.t` WHERE val > 15 ORDER BY id");
		Assert.Equal("1", rows[0]["rn"]?.ToString());
	}

	// ---- Window with expression ----
	[Fact] public async Task Window_SumExpression()
	{
		var rows = await Q($"SELECT id, SUM(val * 2) OVER (PARTITION BY grp ORDER BY id) AS s FROM `{_datasetId}.t` WHERE grp='A' ORDER BY id");
		Assert.Equal("20", rows[0]["s"]?.ToString());
		Assert.Equal("60", rows[1]["s"]?.ToString());
	}
}
