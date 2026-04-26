using BigQuery.InMemoryEmulator;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit;

public class InMemoryDataStoreTests
{
	private readonly InMemoryDataStore _store;

	public InMemoryDataStoreTests()
	{
		_store = new InMemoryDataStore("test-project");
	}

	[Fact]
	public void CreateDataset_StoresMetadata()
	{
		// Arrange
		var dataset = new InMemoryDataset("my_dataset")
		{
			Description = "Test dataset",
			FriendlyName = "My Dataset",
			Location = "EU",
		};

		// Act
		_store.Datasets["my_dataset"] = dataset;

		// Assert
		Assert.True(_store.Datasets.ContainsKey("my_dataset"));
		var stored = _store.Datasets["my_dataset"];
		Assert.Equal("my_dataset", stored.DatasetId);
		Assert.Equal("Test dataset", stored.Description);
		Assert.Equal("My Dataset", stored.FriendlyName);
		Assert.Equal("EU", stored.Location);
	}

	[Fact]
	public void GetDataset_ReturnsStored()
	{
		// Arrange
		_store.Datasets["ds1"] = new InMemoryDataset("ds1") { Description = "First" };

		// Act
		var found = _store.Datasets.TryGetValue("ds1", out var dataset);

		// Assert
		Assert.True(found);
		Assert.Equal("First", dataset!.Description);
	}

	[Fact]
	public void GetDataset_NotFound_ReturnsFalse()
	{
		// Act
		var found = _store.Datasets.TryGetValue("nonexistent", out _);

		// Assert
		Assert.False(found);
	}

	[Fact]
	public void DeleteDataset_Removes()
	{
		// Arrange
		_store.Datasets["ds1"] = new InMemoryDataset("ds1");

		// Act
		var removed = _store.Datasets.TryRemove("ds1", out _);

		// Assert
		Assert.True(removed);
		Assert.False(_store.Datasets.ContainsKey("ds1"));
	}

	[Fact]
	public void ListDatasets_ReturnsAll()
	{
		// Arrange
		_store.Datasets["ds1"] = new InMemoryDataset("ds1");
		_store.Datasets["ds2"] = new InMemoryDataset("ds2");
		_store.Datasets["ds3"] = new InMemoryDataset("ds3");

		// Act
		var all = _store.Datasets.Keys.ToList();

		// Assert
		Assert.Equal(3, all.Count);
		Assert.Contains("ds1", all);
		Assert.Contains("ds2", all);
		Assert.Contains("ds3", all);
	}
}
