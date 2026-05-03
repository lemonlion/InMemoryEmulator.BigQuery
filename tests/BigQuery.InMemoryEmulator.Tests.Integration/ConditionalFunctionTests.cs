using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class ConditionalFunctionTests : IAsyncLifetime
{
    private readonly BigQuerySession _session;
    private ITestDatasetFixture _fixture = null!;
    private string _datasetId = null!;

    public ConditionalFunctionTests(BigQuerySession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _datasetId = $"test_cnd_{Guid.NewGuid():N}"[..30];
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

    // IF
    [Fact] public async Task If_True() => Assert.Equal("yes", await Scalar("SELECT IF(TRUE, 'yes', 'no')"));
    [Fact] public async Task If_False() => Assert.Equal("no", await Scalar("SELECT IF(FALSE, 'yes', 'no')"));
    [Fact] public async Task If_Expression() => Assert.Equal("big", await Scalar("SELECT IF(10 > 5, 'big', 'small')"));
    [Fact] public async Task If_Null() => Assert.Equal("no", await Scalar("SELECT IF(NULL, 'yes', 'no')"));

    // IFNull
    [Fact] public async Task IfNull_NotNull() => Assert.Equal("hello", await Scalar("SELECT IFNULL('hello', 'default')"));
    [Fact] public async Task IfNull_Null() => Assert.Equal("default", await Scalar("SELECT IFNULL(NULL, 'default')"));
    [Fact] public async Task IfNull_Int() => Assert.Equal("0", await Scalar("SELECT IFNULL(NULL, 0)"));

    // NULLIF
    [Fact] public async Task NullIf_Equal() => Assert.Null(await Scalar("SELECT NULLIF(5, 5)"));
    [Fact] public async Task NullIf_NotEqual() => Assert.Equal("5", await Scalar("SELECT NULLIF(5, 3)"));
    [Fact] public async Task NullIf_String() => Assert.Null(await Scalar("SELECT NULLIF('a', 'a')"));

    // COALESCE
    [Fact] public async Task Coalesce_First() => Assert.Equal("1", await Scalar("SELECT COALESCE(1, 2, 3)"));
    [Fact] public async Task Coalesce_Second() => Assert.Equal("2", await Scalar("SELECT COALESCE(NULL, 2, 3)"));
    [Fact] public async Task Coalesce_Third() => Assert.Equal("3", await Scalar("SELECT COALESCE(NULL, NULL, 3)"));
    [Fact] public async Task Coalesce_AllNull() => Assert.Null(await Scalar("SELECT COALESCE(NULL, NULL, NULL)"));
    [Fact] public async Task Coalesce_String() => Assert.Equal("hello", await Scalar("SELECT COALESCE(NULL, 'hello')"));

    // CASE (simple)
    [Fact] public async Task Case_Simple_Match() => Assert.Equal("one", await Scalar("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"));
    [Fact] public async Task Case_Simple_Second() => Assert.Equal("two", await Scalar("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"));
    [Fact] public async Task Case_Simple_Else() => Assert.Equal("other", await Scalar("SELECT CASE 9 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"));
    [Fact] public async Task Case_Simple_NoElse() => Assert.Null(await Scalar("SELECT CASE 9 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END"));

    // CASE (searched)
    [Fact] public async Task Case_Searched_First() => Assert.Equal("big", await Scalar("SELECT CASE WHEN 10 > 5 THEN 'big' WHEN 10 > 3 THEN 'medium' ELSE 'small' END"));
    [Fact] public async Task Case_Searched_Second() => Assert.Equal("medium", await Scalar("SELECT CASE WHEN 4 > 5 THEN 'big' WHEN 4 > 3 THEN 'medium' ELSE 'small' END"));
    [Fact] public async Task Case_Searched_Else() => Assert.Equal("small", await Scalar("SELECT CASE WHEN 2 > 5 THEN 'big' WHEN 2 > 3 THEN 'medium' ELSE 'small' END"));

    // Nested CASE
    [Fact] public async Task Case_Nested() => Assert.Equal("B", await Scalar("SELECT CASE WHEN 5 > 3 THEN CASE WHEN 5 > 4 THEN 'B' ELSE 'C' END ELSE 'D' END"));

    // CASE with NULL
    [Fact] public async Task Case_NullComparison() => Assert.Equal("null", await Scalar("SELECT CASE WHEN NULL = NULL THEN 'equal' ELSE 'null' END"));

    // Null coalesce operator (??) not supported by parser
    // Use COALESCE instead
}