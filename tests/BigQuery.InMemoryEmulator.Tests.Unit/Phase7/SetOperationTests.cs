using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase7;

/// <summary>
/// Unit tests for UNION ALL, UNION DISTINCT, EXCEPT DISTINCT, INTERSECT DISTINCT (Phase 7).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators
/// </summary>
public class SetOperationTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields = [new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "NULLABLE" }]
		};

		var t1 = new InMemoryTable("test_ds", "t1", schema);
		t1.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L }));
		t1.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L }));
		t1.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L }));
		ds.Tables["t1"] = t1;

		var t2 = new InMemoryTable("test_ds", "t2", schema);
		t2.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L }));
		t2.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L }));
		t2.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 4L }));
		ds.Tables["t2"] = t2;

		return new QueryExecutor(store, "test_ds");
	}

	[Fact]
	public void UnionAll_IncludesDuplicates()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT id FROM t1 UNION ALL SELECT id FROM t2");
		Assert.Equal(6, rows.Count);
	}

	[Fact]
	public void UnionDistinct_RemovesDuplicates()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT id FROM t1 UNION DISTINCT SELECT id FROM t2");
		Assert.Equal(4, rows.Count);
	}

	[Fact]
	public void ExceptDistinct_ReturnsOnlyInLeft()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT id FROM t1 EXCEPT DISTINCT SELECT id FROM t2");
		Assert.Single(rows);
		Assert.Equal("1", rows[0].F[0].V?.ToString());
	}

	[Fact]
	public void IntersectDistinct_ReturnsCommon()
	{
		var exec = CreateExecutor();
		var (_, rows) = exec.Execute("SELECT id FROM t1 INTERSECT DISTINCT SELECT id FROM t2");
		Assert.Equal(2, rows.Count);
	}
}
