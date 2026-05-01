using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive SELECT, ORDER BY, LIMIT, OFFSET, DISTINCT, GROUP BY, HAVING tests.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class SelectClauseComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public SelectClauseComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_sel_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);

		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "data", new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "category", Type = "STRING" },
				new TableFieldSchema { Name = "value", Type = "FLOAT" },
				new TableFieldSchema { Name = "flag", Type = "BOOL" },
			]
		});
		await client.InsertRowsAsync(_datasetId, "data", new[]
		{
			new BigQueryInsertRow("a") { ["id"] = 1, ["category"] = "X", ["value"] = 10.0, ["flag"] = true },
			new BigQueryInsertRow("b") { ["id"] = 2, ["category"] = "X", ["value"] = 20.0, ["flag"] = false },
			new BigQueryInsertRow("c") { ["id"] = 3, ["category"] = "Y", ["value"] = 30.0, ["flag"] = true },
			new BigQueryInsertRow("d") { ["id"] = 4, ["category"] = "Y", ["value"] = 40.0, ["flag"] = false },
			new BigQueryInsertRow("e") { ["id"] = 5, ["category"] = "Z", ["value"] = 50.0, ["flag"] = true },
			new BigQueryInsertRow("f") { ["id"] = 6, ["category"] = "Z", ["value"] = 10.0, ["flag"] = true },
			new BigQueryInsertRow("g") { ["id"] = 7, ["category"] = "X", ["value"] = 30.0, ["flag"] = false },
			new BigQueryInsertRow("h") { ["id"] = 8, ["category"] = "Y", ["value"] = 10.0, ["flag"] = true },
		});
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<List<BigQueryRow>> Query(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		return result.ToList();
	}

	private async Task<string?> Scalar(string sql)
	{
		var rows = await Query(sql);
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- SELECT * ----
	[Fact] public async Task SelectStar_AllRows() { var rows = await Query($"SELECT * FROM `{_datasetId}.data`"); Assert.Equal(8, rows.Count); }
	[Fact] public async Task SelectStar_AllColumns() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` LIMIT 1"); Assert.NotNull(rows[0]["id"]); Assert.NotNull(rows[0]["category"]); }

	// ---- SELECT specific columns ----
	[Fact] public async Task SelectColumns() { var rows = await Query($"SELECT id, category FROM `{_datasetId}.data` LIMIT 1"); Assert.NotNull(rows[0]["id"]); Assert.NotNull(rows[0]["category"]); }

	// ---- SELECT with alias ----
	[Fact] public async Task SelectWithAlias() { var rows = await Query($"SELECT id AS identifier FROM `{_datasetId}.data` LIMIT 1"); Assert.NotNull(rows[0]["identifier"]); }

	// ---- SELECT expressions ----
	[Fact] public async Task SelectExpression() { var v = await Scalar("SELECT 1 + 2 AS result"); Assert.Equal("3", v); }
	[Fact] public async Task SelectStringExpression() { var v = await Scalar("SELECT CONCAT('Hello', ' ', 'World')"); Assert.Equal("Hello World", v); }

	// ---- DISTINCT ----
	[Fact] public async Task Distinct_RemovesDuplicates() { var rows = await Query($"SELECT DISTINCT category FROM `{_datasetId}.data` ORDER BY category"); Assert.Equal(3, rows.Count); }
	[Fact] public async Task Distinct_MultiColumn() { var rows = await Query($"SELECT DISTINCT category, flag FROM `{_datasetId}.data` ORDER BY category, flag"); Assert.True(rows.Count <= 6); }

	// ---- ORDER BY ----
	[Fact] public async Task OrderBy_AscDefault() { var rows = await Query($"SELECT id FROM `{_datasetId}.data` ORDER BY id"); Assert.Equal("1", rows[0]["id"]?.ToString()); Assert.Equal("8", rows[7]["id"]?.ToString()); }
	[Fact] public async Task OrderBy_Desc() { var rows = await Query($"SELECT id FROM `{_datasetId}.data` ORDER BY id DESC"); Assert.Equal("8", rows[0]["id"]?.ToString()); }
	[Fact] public async Task OrderBy_MultiColumn() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` ORDER BY category, value DESC"); Assert.Equal("X", rows[0]["category"]?.ToString()); }
	[Fact] public async Task OrderBy_Alias() { var rows = await Query($"SELECT id, value * 2 AS dbl FROM `{_datasetId}.data` ORDER BY dbl DESC"); Assert.Equal(100.0, double.Parse(rows[0]["dbl"]?.ToString() ?? "0")); }
	[Fact] public async Task OrderBy_OrdinalPosition() { var rows = await Query($"SELECT id, value FROM `{_datasetId}.data` ORDER BY 2 DESC"); Assert.Equal(50.0, double.Parse(rows[0]["value"]?.ToString() ?? "0")); }

	// ---- LIMIT ----
	[Fact] public async Task Limit_Restricts() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` ORDER BY id LIMIT 3"); Assert.Equal(3, rows.Count); }
	[Fact] public async Task Limit_Zero() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` LIMIT 0"); Assert.Empty(rows); }
	[Fact] public async Task Limit_ExceedsRows() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` LIMIT 100"); Assert.Equal(8, rows.Count); }

	// ---- OFFSET ----
	[Fact] public async Task Offset_Skips() { var rows = await Query($"SELECT id FROM `{_datasetId}.data` ORDER BY id LIMIT 3 OFFSET 2"); Assert.Equal("3", rows[0]["id"]?.ToString()); }
	[Fact] public async Task Offset_BeyondRows() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` LIMIT 10 OFFSET 100"); Assert.Empty(rows); }

	// ---- WHERE ----
	[Fact] public async Task Where_EqualityFilter() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` WHERE category = 'X'"); Assert.Equal(3, rows.Count); }
	[Fact] public async Task Where_ComparisonFilter() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` WHERE value > 25"); Assert.Equal(4, rows.Count); }
	[Fact] public async Task Where_BoolFilter() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` WHERE flag = TRUE"); Assert.Equal(5, rows.Count); }
	[Fact] public async Task Where_AndCondition() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` WHERE category = 'X' AND flag = TRUE"); Assert.Single(rows); }
	[Fact] public async Task Where_OrCondition() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` WHERE category = 'X' OR category = 'Z'"); Assert.Equal(5, rows.Count); }
	[Fact] public async Task Where_NotCondition() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` WHERE NOT flag"); Assert.Equal(3, rows.Count); }
	[Fact] public async Task Where_IsNull() { var rows = await Query($"SELECT * FROM `{_datasetId}.data` WHERE category IS NOT NULL"); Assert.Equal(8, rows.Count); }

	// ---- GROUP BY ----
	[Fact] public async Task GroupBy_Count() { var rows = await Query($"SELECT category, COUNT(*) AS cnt FROM `{_datasetId}.data` GROUP BY category ORDER BY category"); Assert.Equal(3, rows.Count); Assert.Equal("3", rows[0]["cnt"]?.ToString()); }
	[Fact] public async Task GroupBy_Sum() { var rows = await Query($"SELECT category, SUM(value) AS total FROM `{_datasetId}.data` GROUP BY category ORDER BY category"); Assert.Equal(3, rows.Count); }
	[Fact] public async Task GroupBy_Avg() { var rows = await Query($"SELECT category, AVG(value) AS avg_val FROM `{_datasetId}.data` GROUP BY category ORDER BY category"); Assert.Equal(3, rows.Count); }
	[Fact] public async Task GroupBy_Min_Max() { var rows = await Query($"SELECT category, MIN(value) AS mn, MAX(value) AS mx FROM `{_datasetId}.data` GROUP BY category ORDER BY category"); Assert.Equal(3, rows.Count); }
	[Fact] public async Task GroupBy_MultipleColumns() { var rows = await Query($"SELECT category, flag, COUNT(*) AS cnt FROM `{_datasetId}.data` GROUP BY category, flag ORDER BY category, flag"); Assert.True(rows.Count >= 3); }

	// ---- HAVING ----
	[Fact] public async Task Having_FilterGroups() { var rows = await Query($"SELECT category, COUNT(*) AS cnt FROM `{_datasetId}.data` GROUP BY category HAVING COUNT(*) = 3 ORDER BY category"); Assert.Equal(2, rows.Count); }
	[Fact] public async Task Having_WithSum() { var rows = await Query($"SELECT category, SUM(value) AS total FROM `{_datasetId}.data` GROUP BY category HAVING SUM(value) > 50 ORDER BY total"); Assert.True(rows.Count >= 1); }

	// ---- SELECT with STRUCT ----
	[Fact] public async Task SelectStruct() { var rows = await Query("SELECT STRUCT(1 AS x, 'hello' AS y) AS s"); Assert.Single(rows); }

	// ---- SELECT with ARRAY ----
	[Fact] public async Task SelectArray() { var v = await Scalar("SELECT ARRAY_LENGTH([1, 2, 3])"); Assert.Equal("3", v); }

	// ---- Multiple SELECTs: expressions ----
	[Fact] public async Task SelectLiteral_Integer() => Assert.Equal("42", await Scalar("SELECT 42"));
	[Fact] public async Task SelectLiteral_String() => Assert.Equal("hello", await Scalar("SELECT 'hello'"));
	[Fact] public async Task SelectLiteral_Float() => Assert.NotNull(await Scalar("SELECT 3.14"));
	[Fact] public async Task SelectLiteral_Bool() => Assert.Equal("True", await Scalar("SELECT TRUE"));
	[Fact] public async Task SelectLiteral_Null() => Assert.Null(await Scalar("SELECT NULL"));

	// ---- Table alias ----
	[Fact] public async Task TableAlias_InSelect() { var rows = await Query($"SELECT d.id, d.category FROM `{_datasetId}.data` d ORDER BY d.id LIMIT 1"); Assert.NotNull(rows[0]["id"]); }

	// ---- Wildcard table ----
	[Fact] public async Task WildcardTable_MatchesAll()
	{
		var client = await _fixture.GetClientAsync();
		await client.CreateTableAsync(_datasetId, "wt_alpha", new TableSchema { Fields = [new TableFieldSchema { Name = "id", Type = "INTEGER" }] });
		await client.CreateTableAsync(_datasetId, "wt_beta", new TableSchema { Fields = [new TableFieldSchema { Name = "id", Type = "INTEGER" }] });
		await client.InsertRowsAsync(_datasetId, "wt_alpha", new[] { new BigQueryInsertRow("r1") { ["id"] = 1 } });
		await client.InsertRowsAsync(_datasetId, "wt_beta", new[] { new BigQueryInsertRow("r2") { ["id"] = 2 } });
		var rows = await Query($"SELECT * FROM `{_datasetId}.wt_*`");
		Assert.Equal(2, rows.Count);
	}

	// ---- Except columns ----
	[Fact] public async Task SelectExcept() { var rows = await Query($"SELECT * EXCEPT(flag) FROM `{_datasetId}.data` LIMIT 1"); Assert.NotNull(rows[0]["id"]); }

	// ---- Replace columns ----
	[Fact] public async Task SelectReplace() { var rows = await Query($"SELECT * REPLACE(value * 2 AS value) FROM `{_datasetId}.data` ORDER BY id LIMIT 1"); Assert.Equal("20", rows[0]["value"]?.ToString()); }

	// ---- COUNT DISTINCT ----
	[Fact] public async Task CountDistinct() { var v = await Scalar($"SELECT COUNT(DISTINCT category) FROM `{_datasetId}.data`"); Assert.Equal("3", v); }
}
