using BigQuery.InMemoryEmulator.ProductionExtensions;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for the ProductionExtensions package.
/// Tests that AsAsyncEnumerable, MapAsync, and ToListAsync work through the SDK pipeline.
/// </summary>
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ProductionExtensionsTests : IDisposable
{
	private readonly InMemoryBigQueryResult _result;
	private readonly string _datasetId = "prodext_ds";

	public ProductionExtensionsTests()
	{
		_result = InMemoryBigQuery.Create("test-project", _datasetId);
		var schema = new TableSchema
		{
			Fields =
			[
				new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
				new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
			]
		};
		_result.Client.CreateTable(_datasetId, "items", schema);
		_result.Client.InsertRows(_datasetId, "items",
		[
			new BigQueryInsertRow("r1") { ["id"] = 1, ["name"] = "Alice" },
			new BigQueryInsertRow("r2") { ["id"] = 2, ["name"] = "Bob" },
			new BigQueryInsertRow("r3") { ["id"] = 3, ["name"] = "Charlie" },
		]);
	}

	public void Dispose() => _result.Dispose();

	[Fact]
	public async Task AsAsyncEnumerable_ReturnsAllRows()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			$"SELECT id, name FROM `{_datasetId}.items` ORDER BY id", parameters: null);

		var rows = new List<BigQueryRow>();
		await foreach (var row in results.AsAsyncEnumerable())
			rows.Add(row);

		Assert.Equal(3, rows.Count);
		Assert.Equal("Alice", (string)rows[0]["name"]);
		Assert.Equal("Charlie", (string)rows[2]["name"]);
	}

	[Fact]
	public async Task MapAsync_TransformsRows()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			$"SELECT id, name FROM `{_datasetId}.items` ORDER BY id", parameters: null);

		var names = new List<string>();
		await foreach (var name in results.MapAsync(row => (string)row["name"]))
			names.Add(name);

		Assert.Equal(["Alice", "Bob", "Charlie"], names);
	}

	[Fact]
	public async Task ToListAsync_WithMapper_ReturnsTypedList()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			$"SELECT id, name FROM `{_datasetId}.items` ORDER BY id", parameters: null);

		var items = await results.ToListAsync(row => new
		{
			Id = (long)row["id"],
			Name = (string)row["name"]
		});

		Assert.Equal(3, items.Count);
		Assert.Equal(1L, items[0].Id);
		Assert.Equal("Bob", items[1].Name);
	}

	[Fact]
	public async Task ToListAsync_WithoutMapper_ReturnsRows()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			$"SELECT id FROM `{_datasetId}.items`", parameters: null);

		var rows = await results.ToListAsync();

		Assert.Equal(3, rows.Count);
	}

	[Fact]
	public async Task AsAsyncEnumerable_EmptyResults()
	{
		var results = await _result.Client.ExecuteQueryAsync(
			$"SELECT id FROM `{_datasetId}.items` WHERE id > 999", parameters: null);

		var rows = new List<BigQueryRow>();
		await foreach (var row in results.AsAsyncEnumerable())
			rows.Add(row);

		Assert.Empty(rows);
	}
}
