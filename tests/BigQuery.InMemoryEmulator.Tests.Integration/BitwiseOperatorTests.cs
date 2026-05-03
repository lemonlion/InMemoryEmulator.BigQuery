using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class BitwiseOperatorTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public BitwiseOperatorTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_bit_{Guid.NewGuid():N}"[..30];
        await _fixture.CreateDatasetAsync(_datasetId);
    }

    public async ValueTask DisposeAsync()
    {
        try { var c = await _fixture.GetClientAsync(); await c.DeleteDatasetAsync(_datasetId, new DeleteDatasetOptions { DeleteContents = true }); } catch { }
        await _fixture.DisposeAsync();
    }

    private async Task<string?> Scalar(string sql)
    {
        var client = await _fixture.GetClientAsync();
        var result = await client.ExecuteQueryAsync(sql, parameters: null);
        var rows = result.ToList();
        return rows.Count > 0 ? rows[0][0]?.ToString() : null;
    }

    // Bitwise AND
    [Fact] public async Task BitAnd_Basic() => Assert.Equal("4", await Scalar("SELECT 12 & 6"));
    [Fact] public async Task BitAnd_Zero() => Assert.Equal("0", await Scalar("SELECT 5 & 2"));
    [Fact] public async Task BitAnd_Same() => Assert.Equal("7", await Scalar("SELECT 7 & 7"));

    // Bitwise OR
    [Fact] public async Task BitOr_Basic() => Assert.Equal("14", await Scalar("SELECT 12 | 6"));
    [Fact] public async Task BitOr_Disjoint() => Assert.Equal("7", await Scalar("SELECT 5 | 2"));
    [Fact] public async Task BitOr_Same() => Assert.Equal("7", await Scalar("SELECT 7 | 7"));

    // Bitwise XOR
    [Fact] public async Task BitXor_Basic() => Assert.Equal("10", await Scalar("SELECT 12 ^ 6"));
    [Fact] public async Task BitXor_Same() => Assert.Equal("0", await Scalar("SELECT 7 ^ 7"));

    // Bitwise NOT
    [Fact] public async Task BitNot_Basic() => Assert.Equal("-8", await Scalar("SELECT ~7"));
    [Fact] public async Task BitNot_Zero() => Assert.Equal("-1", await Scalar("SELECT ~0"));
    [Fact] public async Task BitNot_Negative() => Assert.Equal("0", await Scalar("SELECT ~(-1)"));

    // Shift left
    [Fact] public async Task ShiftLeft_Basic() => Assert.Equal("8", await Scalar("SELECT 1 << 3"));
    [Fact] public async Task ShiftLeft_Multiple() => Assert.Equal("20", await Scalar("SELECT 5 << 2"));

    // Shift right
    [Fact] public async Task ShiftRight_Basic() => Assert.Equal("2", await Scalar("SELECT 8 >> 2"));
    [Fact] public async Task ShiftRight_One() => Assert.Equal("5", await Scalar("SELECT 10 >> 1"));

    // BIT_COUNT
    [Fact] public async Task BitCount_Seven() => Assert.Equal("3", await Scalar("SELECT BIT_COUNT(7)"));
    [Fact] public async Task BitCount_Zero() => Assert.Equal("0", await Scalar("SELECT BIT_COUNT(0)"));
    [Fact] public async Task BitCount_Fifteen() => Assert.Equal("4", await Scalar("SELECT BIT_COUNT(15)"));
    [Fact] public async Task BitCount_One() => Assert.Equal("1", await Scalar("SELECT BIT_COUNT(1)"));

    // NULL propagation
    [Fact] public async Task BitAnd_Null() => Assert.Null(await Scalar("SELECT NULL & 5"));
    [Fact] public async Task BitOr_Null() => Assert.Null(await Scalar("SELECT 5 | NULL"));
    [Fact] public async Task BitXor_Null() => Assert.Null(await Scalar("SELECT NULL ^ 3"));
    [Fact] public async Task BitNot_Null() => Assert.Null(await Scalar("SELECT ~CAST(NULL AS INT64)"));
    [Fact] public async Task BitCount_Null() => Assert.Null(await Scalar("SELECT BIT_COUNT(CAST(NULL AS INT64))"));

    // Combined
    [Fact] public async Task Bitwise_Combined() => Assert.Equal("7", await Scalar("SELECT (5 | 3) & 7"));
    [Fact] public async Task Bitwise_ShiftAndMask() => Assert.Equal("8", await Scalar("SELECT (1 << 3) & 12"));
}