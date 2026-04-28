using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for parameterized queries using BigQueryParameter.
/// Ref: https://cloud.google.com/bigquery/docs/parameterized-queries
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ParameterizedQueryTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	public ParameterizedQueryTests(BigQuerySession session) => _session = session;
	public async ValueTask InitializeAsync() => _fixture = TestFixtureFactory.Create(_session);
	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	// ---- Named parameters ----
	[Fact]
	public async Task NamedParam_Int()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("val", BigQueryDbType.Int64, 42) };
		var result = await client.ExecuteQueryAsync("SELECT @val", parameters);
		var row = result.Single();
		Assert.Equal("42", row[0]?.ToString());
	}

	[Fact]
	public async Task NamedParam_String()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("name", BigQueryDbType.String, "hello") };
		var result = await client.ExecuteQueryAsync("SELECT @name", parameters);
		var row = result.Single();
		Assert.Equal("hello", row[0]?.ToString());
	}

	[Fact]
	public async Task NamedParam_Float()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("val", BigQueryDbType.Float64, 3.14) };
		var result = await client.ExecuteQueryAsync("SELECT @val", parameters);
		var row = result.Single();
		Assert.Equal("3.14", row[0]?.ToString());
	}

	[Fact]
	public async Task NamedParam_Bool_True()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("flag", BigQueryDbType.Bool, true) };
		var result = await client.ExecuteQueryAsync("SELECT @flag", parameters);
		var row = result.Single();
		Assert.Equal("True", row[0]?.ToString());
	}

	[Fact]
	public async Task NamedParam_Bool_False()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("flag", BigQueryDbType.Bool, false) };
		var result = await client.ExecuteQueryAsync("SELECT @flag", parameters);
		var row = result.Single();
		Assert.Equal("False", row[0]?.ToString());
	}

	// ---- Parameters in expressions ----
	[Fact]
	public async Task Param_InAddition()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("x", BigQueryDbType.Int64, 10) };
		var result = await client.ExecuteQueryAsync("SELECT @x + 5", parameters);
		var row = result.Single();
		Assert.Equal("15", row[0]?.ToString());
	}

	[Fact]
	public async Task Param_InMultiply()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("x", BigQueryDbType.Int64, 6) };
		var result = await client.ExecuteQueryAsync("SELECT @x * 7", parameters);
		var row = result.Single();
		Assert.Equal("42", row[0]?.ToString());
	}

	[Fact]
	public async Task Param_InConcat()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("name", BigQueryDbType.String, "world") };
		var result = await client.ExecuteQueryAsync("SELECT CONCAT('hello ', @name)", parameters);
		var row = result.Single();
		Assert.Equal("hello world", row[0]?.ToString());
	}

	// ---- Multiple parameters ----
	[Fact]
	public async Task TwoParams()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[]
		{
			new BigQueryParameter("a", BigQueryDbType.Int64, 10),
			new BigQueryParameter("b", BigQueryDbType.Int64, 20)
		};
		var result = await client.ExecuteQueryAsync("SELECT @a + @b", parameters);
		var row = result.Single();
		Assert.Equal("30", row[0]?.ToString());
	}

	[Fact]
	public async Task ThreeParams()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[]
		{
			new BigQueryParameter("x", BigQueryDbType.Int64, 2),
			new BigQueryParameter("y", BigQueryDbType.Int64, 3),
			new BigQueryParameter("z", BigQueryDbType.Int64, 4)
		};
		var result = await client.ExecuteQueryAsync("SELECT @x + @y + @z", parameters);
		var row = result.Single();
		Assert.Equal("9", row[0]?.ToString());
	}

	[Fact]
	public async Task MixedTypeParams()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[]
		{
			new BigQueryParameter("name", BigQueryDbType.String, "alice"),
			new BigQueryParameter("age", BigQueryDbType.Int64, 30)
		};
		var result = await client.ExecuteQueryAsync("SELECT @name, @age", parameters);
		var row = result.Single();
		Assert.Equal("alice", row[0]?.ToString());
		Assert.Equal("30", row[1]?.ToString());
	}

	// ---- Parameters in WHERE ----
	[Fact]
	public async Task Param_InWhere()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("threshold", BigQueryDbType.Int64, 3) };
		var result = await client.ExecuteQueryAsync(
			"SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x > @threshold ORDER BY x",
			parameters);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
		Assert.Equal("4", rows[0][0]?.ToString());
		Assert.Equal("5", rows[1][0]?.ToString());
	}

	[Fact]
	public async Task Param_InWhereLike()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("pattern", BigQueryDbType.String, "a%") };
		var result = await client.ExecuteQueryAsync(
			"SELECT x FROM UNNEST(['alice','bob','anna']) AS x WHERE x LIKE @pattern ORDER BY x",
			parameters);
		var rows = result.ToList();
		Assert.Equal(2, rows.Count);
	}

	[Fact]
	public async Task Param_InWhereBetween()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[]
		{
			new BigQueryParameter("lo", BigQueryDbType.Int64, 2),
			new BigQueryParameter("hi", BigQueryDbType.Int64, 4)
		};
		var result = await client.ExecuteQueryAsync(
			"SELECT x FROM UNNEST([1,2,3,4,5]) AS x WHERE x BETWEEN @lo AND @hi ORDER BY x",
			parameters);
		var rows = result.ToList();
		Assert.Equal(3, rows.Count);
		Assert.Equal("2", rows[0][0]?.ToString());
		Assert.Equal("4", rows[2][0]?.ToString());
	}

	// ---- Param in function call ----
	[Fact]
	public async Task Param_InSubstr()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[]
		{
			new BigQueryParameter("str", BigQueryDbType.String, "hello world"),
			new BigQueryParameter("pos", BigQueryDbType.Int64, 7)
		};
		var result = await client.ExecuteQueryAsync("SELECT SUBSTR(@str, @pos)", parameters);
		var row = result.Single();
		Assert.Equal("world", row[0]?.ToString());
	}

	[Fact]
	public async Task Param_InLength()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("str", BigQueryDbType.String, "hello") };
		var result = await client.ExecuteQueryAsync("SELECT LENGTH(@str)", parameters);
		var row = result.Single();
		Assert.Equal("5", row[0]?.ToString());
	}

	// ---- Param with NULL ----
	[Fact]
	public async Task Param_NullInt()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("val", BigQueryDbType.Int64, null) };
		var result = await client.ExecuteQueryAsync("SELECT @val", parameters);
		var row = result.Single();
		Assert.Null(row[0]);
	}

	[Fact]
	public async Task Param_NullString()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("val", BigQueryDbType.String, null) };
		var result = await client.ExecuteQueryAsync("SELECT @val", parameters);
		var row = result.Single();
		Assert.Null(row[0]);
	}

	// ---- Param used multiple times ----
	[Fact]
	public async Task Param_UsedTwice()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("x", BigQueryDbType.Int64, 5) };
		var result = await client.ExecuteQueryAsync("SELECT @x + @x", parameters);
		var row = result.Single();
		Assert.Equal("10", row[0]?.ToString());
	}

	[Fact]
	public async Task Param_UsedThrice()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("x", BigQueryDbType.Int64, 3) };
		var result = await client.ExecuteQueryAsync("SELECT @x * @x * @x", parameters);
		var row = result.Single();
		Assert.Equal("27", row[0]?.ToString());
	}

	// ---- Param in CASE ----
	[Fact(Skip = "Emulator limitation")]
	public async Task Param_InCase()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("val", BigQueryDbType.Int64, 2) };
		var result = await client.ExecuteQueryAsync(
			"SELECT CASE @val WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
			parameters);
		var row = result.Single();
		Assert.Equal("two", row[0]?.ToString());
	}

	// ---- Param in COALESCE ----
	[Fact]
	public async Task Param_InCoalesce()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("val", BigQueryDbType.Int64, null) };
		var result = await client.ExecuteQueryAsync("SELECT COALESCE(@val, 99)", parameters);
		var row = result.Single();
		Assert.Equal("99", row[0]?.ToString());
	}

	// ---- Date parameter ----
	[Fact]
	public async Task Param_Date()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("d", BigQueryDbType.Date, new DateTime(2024, 1, 15)) };
		var result = await client.ExecuteQueryAsync("SELECT EXTRACT(YEAR FROM @d)", parameters);
		var row = result.Single();
		Assert.Equal("2024", row[0]?.ToString());
	}

	// ---- Param in aggregate ----
	[Fact]
	public async Task Param_InCountif()
	{
		var client = await _fixture.GetClientAsync();
		var parameters = new[] { new BigQueryParameter("threshold", BigQueryDbType.Int64, 3) };
		var result = await client.ExecuteQueryAsync(
			"SELECT COUNTIF(x > @threshold) FROM UNNEST([1,2,3,4,5]) AS x",
			parameters);
		var row = result.Single();
		Assert.Equal("2", row[0]?.ToString());
	}
}
