using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Comprehensive tests for aggregate functions with various grouping scenarios.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class AggregateComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _ds = null!;

	public AggregateComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_ds = $"test_agg2_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_ds);
		var c = await _fixture.GetClientAsync();
		await c.ExecuteQueryAsync($"CREATE TABLE `{_ds}.data` (id INT64, grp STRING, subgrp STRING, val FLOAT64, flag BOOL)", parameters: null);
		await c.ExecuteQueryAsync($@"INSERT INTO `{_ds}.data` (id, grp, subgrp, val, flag) VALUES
			(1,'A','X',10,true),(2,'A','X',20,false),(3,'A','Y',30,true),
			(4,'B','X',40,true),(5,'B','Y',50,false),(6,'B','Y',60,true),
			(7,'C','X',NULL,true),(8,'C','Y',80,NULL),(9,'A','X',NULL,false)", parameters: null);
	}

	public async ValueTask DisposeAsync()
	{
		try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_ds, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
		await _fixture.DisposeAsync();
	}

	private async Task<string?> Scalar(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); var rows = r.ToList(); return rows.Count > 0 ? rows[0][0]?.ToString() : null; }
	private async Task<List<BigQueryRow>> Query(string sql) { var c = await _fixture.GetClientAsync(); var r = await c.ExecuteQueryAsync(sql.Replace("{ds}", _ds), parameters: null); return r.ToList(); }

	// ---- COUNT ----
	[Fact] public async Task Count_Star() => Assert.Equal("9", await Scalar("SELECT COUNT(*) FROM `{ds}.data`"));
	[Fact] public async Task Count_Column() => Assert.Equal("7", await Scalar("SELECT COUNT(val) FROM `{ds}.data`")); // NULL excluded
	[Fact] public async Task Count_Distinct() => Assert.Equal("7", await Scalar("SELECT COUNT(DISTINCT val) FROM `{ds}.data`")); // 7 distinct non-null: 10,20,30,40,50,60,80
	[Fact] public async Task Count_GroupBy()
	{
		var rows = await Query("SELECT grp, COUNT(*) AS cnt FROM `{ds}.data` GROUP BY grp ORDER BY grp");
		Assert.Equal("4", rows[0]["cnt"]?.ToString()); // A: 4
		Assert.Equal("3", rows[1]["cnt"]?.ToString()); // B: 3
		Assert.Equal("2", rows[2]["cnt"]?.ToString()); // C: 2
	}

	// ---- SUM ----
	[Fact] public async Task Sum_All() => Assert.Equal("290", await Scalar("SELECT SUM(val) FROM `{ds}.data`"));
	[Fact] public async Task Sum_GroupBy()
	{
		var rows = await Query("SELECT grp, SUM(val) AS total FROM `{ds}.data` GROUP BY grp ORDER BY grp");
		Assert.Equal("60", rows[0]["total"]?.ToString()); // A: 10+20+30
		Assert.Equal("150", rows[1]["total"]?.ToString()); // B: 40+50+60
	}
	[Fact] public async Task Sum_Distinct() => Assert.Equal("290", await Scalar("SELECT SUM(DISTINCT val) FROM `{ds}.data`"));
	[Fact] public async Task Sum_WithNulls() => Assert.Equal("290", await Scalar("SELECT SUM(val) FROM `{ds}.data`")); // NULLs ignored

	// ---- AVG ----
	[Fact] public async Task Avg_All()
	{
		var v = double.Parse(await Scalar("SELECT AVG(val) FROM `{ds}.data`") ?? "0");
		Assert.True(v > 41 && v < 42); // 290/7 ≈ 41.43
	}
	[Fact] public async Task Avg_GroupBy()
	{
		var rows = await Query("SELECT grp, AVG(val) AS avg_val FROM `{ds}.data` GROUP BY grp ORDER BY grp");
		Assert.Equal("20", rows[0]["avg_val"]?.ToString()); // A: 60/3 = 20
	}

	// ---- MIN / MAX ----
	[Fact] public async Task Min_All() => Assert.Equal("10", await Scalar("SELECT MIN(val) FROM `{ds}.data`"));
	[Fact] public async Task Max_All() => Assert.Equal("80", await Scalar("SELECT MAX(val) FROM `{ds}.data`"));
	[Fact] public async Task Min_String() => Assert.Equal("A", await Scalar("SELECT MIN(grp) FROM `{ds}.data`"));
	[Fact] public async Task Max_String() => Assert.Equal("C", await Scalar("SELECT MAX(grp) FROM `{ds}.data`"));
	[Fact] public async Task MinMax_GroupBy()
	{
		var rows = await Query("SELECT grp, MIN(val) AS mn, MAX(val) AS mx FROM `{ds}.data` GROUP BY grp ORDER BY grp");
		Assert.Equal("10", rows[0]["mn"]?.ToString());
		Assert.Equal("30", rows[0]["mx"]?.ToString());
	}

	// ---- STRING_AGG ----
	[Fact] public async Task StringAgg_Basic()
	{
		var v = await Scalar("SELECT STRING_AGG(grp, ',') FROM `{ds}.data`");
		Assert.NotNull(v);
		Assert.Contains("A", v);
	}
	[Fact] public async Task StringAgg_OrderBy()
	{
		var v = await Scalar("SELECT STRING_AGG(grp, ',' ORDER BY grp) FROM `{ds}.data`");
		Assert.NotNull(v);
		Assert.StartsWith("A", v);
	}
	[Fact] public async Task StringAgg_GroupBy()
	{
		var rows = await Query("SELECT grp, STRING_AGG(subgrp, ',' ORDER BY subgrp) AS subs FROM `{ds}.data` GROUP BY grp ORDER BY grp");
		Assert.NotNull(rows[0]["subs"]);
	}

	// ---- COUNTIF ----
	[Fact] public async Task CountIf_Basic() => Assert.Equal("5", await Scalar("SELECT COUNTIF(flag) FROM `{ds}.data`"));
	[Fact] public async Task CountIf_GroupBy()
	{
		var rows = await Query("SELECT grp, COUNTIF(flag) AS cnt FROM `{ds}.data` GROUP BY grp ORDER BY grp");
		Assert.Equal("2", rows[0]["cnt"]?.ToString()); // A: 2 true (id 1,3)
	}

	// ---- LOGICAL_AND / LOGICAL_OR ----
	[Fact] public async Task LogicalAnd_All() => Assert.Equal("False", await Scalar("SELECT LOGICAL_AND(flag) FROM `{ds}.data`"));
	[Fact] public async Task LogicalOr_All() => Assert.Equal("True", await Scalar("SELECT LOGICAL_OR(flag) FROM `{ds}.data`"));
	[Fact] public async Task LogicalAnd_GroupBy()
	{
		var rows = await Query("SELECT grp, LOGICAL_AND(flag) AS all_true FROM `{ds}.data` GROUP BY grp ORDER BY grp");
		Assert.Equal("False", rows[0]["all_true"]?.ToString()); // A: has false
	}

	// ---- ANY_VALUE ----
	[Fact] public async Task AnyValue_Basic()
	{
		var v = await Scalar("SELECT ANY_VALUE(grp) FROM `{ds}.data`");
		Assert.NotNull(v);
		Assert.True(v == "A" || v == "B" || v == "C");
	}

	// ---- ARRAY_AGG ----
	[Fact] public async Task ArrayAgg_Basic()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(val)) FROM `{ds}.data`");
		Assert.Equal("7", v); // NULLs excluded by emulator default
	}
	[Fact] public async Task ArrayAgg_IgnoreNulls()
	{
		var v = await Scalar("SELECT ARRAY_LENGTH(ARRAY_AGG(val IGNORE NULLS)) FROM `{ds}.data`");
		Assert.Equal("7", v);
	}

	// ---- GROUP BY with HAVING ----
	[Fact] public async Task GroupBy_Having()
	{
		var rows = await Query("SELECT grp, COUNT(*) AS cnt FROM `{ds}.data` GROUP BY grp HAVING COUNT(*) >= 3 ORDER BY grp");
		Assert.Equal(2, rows.Count); // A: 4, B: 3
	}
	[Fact] public async Task GroupBy_HavingSum()
	{
		var rows = await Query("SELECT grp, SUM(val) AS total FROM `{ds}.data` GROUP BY grp HAVING SUM(val) > 100 ORDER BY grp");
		Assert.Single(rows); // B: 150
	}

	// ---- GROUP BY multiple columns ----
	[Fact] public async Task GroupBy_MultipleColumns()
	{
		var rows = await Query("SELECT grp, subgrp, COUNT(*) AS cnt FROM `{ds}.data` GROUP BY grp, subgrp ORDER BY grp, subgrp");
		Assert.True(rows.Count >= 5);
	}

	// ---- Aggregate with expressions ----
	[Fact] public async Task Agg_Expression() => Assert.Equal("580", await Scalar("SELECT SUM(val * 2) FROM `{ds}.data`"));
	[Fact] public async Task Agg_Conditional() => Assert.Equal("60", await Scalar("SELECT SUM(CASE WHEN grp = 'A' THEN val ELSE 0 END) FROM `{ds}.data`"));

	// ---- Aggregate without GROUP BY (whole table) ----
	[Fact] public async Task Agg_WholeTable_Multi()
	{
		var rows = await Query("SELECT COUNT(*) AS c, SUM(val) AS s, MIN(val) AS mn, MAX(val) AS mx FROM `{ds}.data`");
		Assert.Equal("9", rows[0]["c"]?.ToString());
		Assert.Equal("290", rows[0]["s"]?.ToString());
		Assert.Equal("10", rows[0]["mn"]?.ToString());
		Assert.Equal("80", rows[0]["mx"]?.ToString());
	}

	// ---- Aggregate on empty table ----
	[Fact] public async Task Agg_EmptyTable()
	{
		var rows = await Query("SELECT COUNT(*) AS c, SUM(val) AS s, MIN(val) AS mn FROM `{ds}.data` WHERE false");
		Assert.Equal("0", rows[0]["c"]?.ToString());
		Assert.Null(rows[0]["s"]);
		Assert.Null(rows[0]["mn"]);
	}

	// ---- Aggregate with DISTINCT ----
	[Fact] public async Task Agg_CountDistinctGroupBy()
	{
		var rows = await Query("SELECT grp, COUNT(DISTINCT subgrp) AS cnt FROM `{ds}.data` GROUP BY grp ORDER BY grp");
		Assert.Equal("2", rows[0]["cnt"]?.ToString()); // A has X,Y
	}

	// ---- GROUP BY ordinal ----
	[Fact] public async Task GroupBy_Ordinal()
	{
		var rows = await Query("SELECT grp, COUNT(*) AS cnt FROM `{ds}.data` GROUP BY 1 ORDER BY 1");
		Assert.Equal(3, rows.Count);
	}

	// ---- APPROX_COUNT_DISTINCT ----
	[Fact] public async Task ApproxCountDistinct()
	{
		var v = long.Parse(await Scalar("SELECT APPROX_COUNT_DISTINCT(grp) FROM `{ds}.data`") ?? "0");
		Assert.True(v >= 2 && v <= 4); // Should be ~3
	}

	// ---- BIT_AND / BIT_OR / BIT_XOR ----
	[Fact] public async Task BitAnd_Basic()
	{
		var v = await Scalar("SELECT BIT_AND(x) FROM UNNEST([7, 3, 5]) AS x");
		Assert.Equal("1", v); // 111 & 011 & 101 = 001
	}
	[Fact] public async Task BitOr_Basic()
	{
		var v = await Scalar("SELECT BIT_OR(x) FROM UNNEST([1, 2, 4]) AS x");
		Assert.Equal("7", v); // 001 | 010 | 100 = 111
	}
	[Fact] public async Task BitXor_Basic()
	{
		var v = await Scalar("SELECT BIT_XOR(x) FROM UNNEST([1, 3, 5]) AS x");
		Assert.Equal("7", v); // 001 ^ 011 ^ 101 = 111
	}
}
