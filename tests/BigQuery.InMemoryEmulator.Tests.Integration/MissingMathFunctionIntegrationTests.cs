using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Integration tests for missing math functions (Phase 24).
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class MissingMathFunctionIntegrationTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;
	private string _datasetId = null!;

	public MissingMathFunctionIntegrationTests(BigQuerySession session)
	{
		_session = session;
	}

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
		_datasetId = $"test_mathfn_{Guid.NewGuid():N}"[..30];
		await _fixture.CreateDatasetAsync(_datasetId);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			var client = await _fixture.GetClientAsync();
			await client.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true });
		}
		catch { }
		await _fixture.DisposeAsync();
	}

	#region Trigonometric

	[Fact]
	public async Task Sin_Zero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SIN(0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("0", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Cos_Zero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT COS(0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("1", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Tan_Zero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT TAN(0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("0", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Acos_One()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT ACOS(1) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("0", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Asin_Zero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT ASIN(0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("0", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Atan_Zero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT ATAN(0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("0", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Atan2_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT ATAN2(1, 1) AS result", parameters: null);
		var rows = results.ToList();
		var val = double.Parse(rows[0]["result"].ToString()!);
		Assert.True(Math.Abs(val - Math.PI / 4) < 1e-10);
	}

	[Fact]
	public async Task Sinh_Zero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SINH(0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("0", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Cosh_Zero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT COSH(0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("1", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Tanh_Zero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT TANH(0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("0", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Acosh_One()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT ACOSH(1) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("0", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Asinh_Zero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT ASINH(0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("0", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task Atanh_Zero()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT ATANH(0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("0", rows[0]["result"].ToString());
	}

	#endregion

	#region IS_INF / IS_NAN

	[Fact]
	public async Task IsInf_PositiveInfinity()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT IS_INF(IEEE_DIVIDE(1.0, 0.0)) AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(true, rows[0]["result"]);
	}

	[Fact]
	public async Task IsInf_Finite()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT IS_INF(1.0) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal(false, rows[0]["result"]);
	}

	[Fact]
	public async Task IsNan_True()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT IS_NAN(IEEE_DIVIDE(0.0, 0.0)) AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal(true, rows[0]["result"]);
	}

	#endregion

	#region SAFE_ arithmetic

	[Fact]
	public async Task SafeAdd_Normal()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SAFE_ADD(1, 2) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("3", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task SafeSubtract_Normal()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SAFE_SUBTRACT(10, 3) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("7", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task SafeMultiply_Normal()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SAFE_MULTIPLY(4, 5) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("20", rows[0]["result"].ToString());
	}

	[Fact]
	public async Task SafeNegate_Normal()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SAFE_NEGATE(42) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Equal("-42", rows[0]["result"].ToString());
	}

	#endregion

	#region RANGE_BUCKET

	[Fact]
	public async Task RangeBucket_Basic()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync(
			"SELECT RANGE_BUCKET(5, [0, 10, 20]) AS result",
			parameters: null);
		var rows = results.ToList();
		Assert.Equal("1", rows[0]["result"].ToString());
	}

	#endregion

	#region NULL handling

	[Fact]
	public async Task Sin_Null_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SIN(NULL) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Null(rows[0]["result"]);
	}

	[Fact]
	public async Task SafeAdd_Null_ReturnsNull()
	{
		var client = await _fixture.GetClientAsync();
		var results = await client.ExecuteQueryAsync("SELECT SAFE_ADD(1, NULL) AS result", parameters: null);
		var rows = results.ToList();
		Assert.Null(rows[0]["result"]);
	}

	#endregion
}
