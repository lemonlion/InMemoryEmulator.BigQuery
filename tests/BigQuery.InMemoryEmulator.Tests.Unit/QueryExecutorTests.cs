using BigQuery.InMemoryEmulator.SqlEngine;
using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit;

/// <summary>
/// Unit tests for the QueryExecutor (Phase 4 scope).
/// </summary>
public class QueryExecutorTests
{
	private static (InMemoryDataStore Store, InMemoryTable Table) CreateTestData()
	{
		var store = new InMemoryDataStore("test-project");
		var ds = new InMemoryDataset("test_ds");
		store.Datasets["test_ds"] = ds;

		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
				new TableFieldSchema { Name = "score", Type = "FLOAT", Mode = "NULLABLE" },
			]
		};
		var table = new InMemoryTable("test_ds", "users", schema);
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alice", ["score"] = 90.5 }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "Bob", ["score"] = 85.0 }));
		table.Rows.Add(new InMemoryRow(new Dictionary<string, object?> { ["id"] = 3L, ["name"] = "Charlie", ["score"] = 92.0 }));
		ds.Tables["users"] = table;

		return (store, table);
	}

	[Fact]
	public void SelectStar_ReturnsAllRows()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (schema, rows) = executor.Execute("SELECT * FROM users");

		Assert.Equal(3, rows.Count);
		Assert.Equal(3, schema.Fields.Count);
		Assert.Equal("id", schema.Fields[0].Name);
	}

	[Fact]
	public void WhereEquals_FiltersRows()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT * FROM users WHERE id = 2");

		Assert.Single(rows);
		Assert.Equal("Bob", rows[0].F[1].V);
	}

	[Fact]
	public void OrderByAsc_SortsRows()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT * FROM users ORDER BY name");

		Assert.Equal("Alice", rows[0].F[1].V);
		Assert.Equal("Bob", rows[1].F[1].V);
		Assert.Equal("Charlie", rows[2].F[1].V);
	}

	[Fact]
	public void Limit_TruncatesRows()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT * FROM users LIMIT 2");

		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public void Parameters_SubstitutesValues()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		executor.SetParameters([
			new QueryParameter
			{
				Name = "target_id",
				ParameterType = new QueryParameterType { Type = "INT64" },
				ParameterValue = new QueryParameterValue { Value = "1" }
			}
		]);

		var (_, rows) = executor.Execute("SELECT * FROM users WHERE id = @target_id");

		Assert.Single(rows);
		Assert.Equal("Alice", rows[0].F[1].V);
	}

	[Fact]
	public void SelectExpression_WithoutFrom_Works()
	{
		var store = new InMemoryDataStore("test-project");
		var executor = new QueryExecutor(store);
		var (_, rows) = executor.Execute("SELECT 1");

		Assert.Single(rows);
		Assert.Equal("1", rows[0].F[0].V);
	}

	[Fact]
	public void CeilResult_FormatsAsFloat()
	{
		var store = new InMemoryDataStore("test-project");
		var executor = new QueryExecutor(store);
		var (schema, rows) = executor.Execute("SELECT CEIL(2.3)");

		Assert.Single(rows);
		// FormatValue converts whole floats to integer strings for SDK compatibility
		Assert.Equal("3", rows[0].F[0].V);
		// But the schema correctly reports FLOAT type
		Assert.Equal("FLOAT", schema.Fields[0].Type);
	}

	[Fact]
	public void WhereStringComparison_Works()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT * FROM users WHERE name = 'Alice'");

		Assert.Single(rows);
		Assert.Equal("1", rows[0].F[0].V);
	}

	[Fact]
	public void OrderByDesc_SortsDescending()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT * FROM users ORDER BY score DESC");

		Assert.Equal("Charlie", rows[0].F[1].V);
		Assert.Equal("Alice", rows[1].F[1].V);
		Assert.Equal("Bob", rows[2].F[1].V);
	}

	[Fact]
	public void CountStar_ReturnsRowCount()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT COUNT(*) FROM users");

		Assert.Single(rows);
		Assert.Equal("3", rows[0].F[0].V);
	}

	[Fact]
	public void LimitOffset_Paginates()
	{
		var (store, _) = CreateTestData();
		var executor = new QueryExecutor(store, "test_ds");
		var (_, rows) = executor.Execute("SELECT * FROM users ORDER BY id LIMIT 1 OFFSET 1");

		Assert.Single(rows);
		Assert.Equal("Bob", rows[0].F[1].V);
	}
}
