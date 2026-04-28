using Google.Cloud.BigQuery.V2;
using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Network function tests.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class NetFunctionComprehensiveTests : IAsyncLifetime
{
	private readonly BigQuerySession _session;
	private ITestDatasetFixture _fixture = null!;

	public NetFunctionComprehensiveTests(BigQuerySession session) => _session = session;

	public async ValueTask InitializeAsync()
	{
		_fixture = TestFixtureFactory.Create(_session);
	}

	public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

	private async Task<string?> Scalar(string sql)
	{
		var client = await _fixture.GetClientAsync();
		var result = await client.ExecuteQueryAsync(sql, parameters: null);
		var rows = result.ToList();
		return rows.Count > 0 ? rows[0][0]?.ToString() : null;
	}

	// ---- NET.HOST ----
	[Fact] public async Task NetHost_WithPort() => Assert.Equal("example.com", await Scalar("SELECT NET.HOST('http://example.com:8080/path')"));
	[Fact] public async Task NetHost_WithoutPort() => Assert.Equal("example.com", await Scalar("SELECT NET.HOST('http://example.com/path')"));
	[Fact(Skip = "Needs investigation")] public async Task NetHost_NoScheme() => Assert.Equal("example.com", await Scalar("SELECT NET.HOST('//example.com/path')"));
	[Fact] public async Task NetHost_Null() => Assert.Null(await Scalar("SELECT NET.HOST(NULL)"));
	[Fact] public async Task NetHost_Subdomain() => Assert.Equal("www.example.com", await Scalar("SELECT NET.HOST('http://www.example.com')"));
	[Fact] public async Task NetHost_IpAddress() => Assert.Equal("192.168.1.1", await Scalar("SELECT NET.HOST('http://192.168.1.1/path')"));

	// ---- NET.PUBLIC_SUFFIX ----
	[Fact] public async Task NetPublicSuffix_Com() => Assert.Equal("com", await Scalar("SELECT NET.PUBLIC_SUFFIX('www.example.com')"));
	[Fact(Skip = "Needs investigation")] public async Task NetPublicSuffix_CoUk() => Assert.Equal("co.uk", await Scalar("SELECT NET.PUBLIC_SUFFIX('www.example.co.uk')"));
	[Fact] public async Task NetPublicSuffix_Null() => Assert.Null(await Scalar("SELECT NET.PUBLIC_SUFFIX(NULL)"));

	// ---- NET.REG_DOMAIN ----
	[Fact] public async Task NetRegDomain_Com() => Assert.Equal("example.com", await Scalar("SELECT NET.REG_DOMAIN('www.example.com')"));
	[Fact(Skip = "Needs investigation")] public async Task NetRegDomain_CoUk() => Assert.Equal("example.co.uk", await Scalar("SELECT NET.REG_DOMAIN('www.example.co.uk')"));
	[Fact] public async Task NetRegDomain_Null() => Assert.Null(await Scalar("SELECT NET.REG_DOMAIN(NULL)"));

	// ---- NET.IP_FROM_STRING / NET.IP_TO_STRING ----
	[Fact] public async Task NetIpFromString_IPv4() => Assert.NotNull(await Scalar("SELECT NET.IP_FROM_STRING('192.168.1.1')"));
	[Fact] public async Task NetIpToString_RoundTrip() => Assert.Equal("192.168.1.1", await Scalar("SELECT NET.IP_TO_STRING(NET.IP_FROM_STRING('192.168.1.1'))"));
	[Fact] public async Task NetIpFromString_Null() => Assert.Null(await Scalar("SELECT NET.IP_FROM_STRING(NULL)"));
	[Fact] public async Task NetIpToString_Null() => Assert.Null(await Scalar("SELECT NET.IP_TO_STRING(NULL)"));
	[Fact] public async Task NetSafeIpFromString_Invalid() => Assert.Null(await Scalar("SELECT NET.SAFE_IP_FROM_STRING('not_an_ip')"));
	[Fact] public async Task NetSafeIpFromString_Valid() => Assert.NotNull(await Scalar("SELECT NET.SAFE_IP_FROM_STRING('10.0.0.1')"));

	// ---- NET.IP_TRUNC ----
	[Fact(Skip = "Not yet supported")] public async Task NetIpTrunc_IPv4_24() => Assert.Equal("192.168.1.0", await Scalar("SELECT NET.IP_TO_STRING(NET.IP_TRUNC(NET.IP_FROM_STRING('192.168.1.100'), 24))"));
	[Fact(Skip = "Not yet supported")] public async Task NetIpTrunc_IPv4_16() => Assert.Equal("192.168.0.0", await Scalar("SELECT NET.IP_TO_STRING(NET.IP_TRUNC(NET.IP_FROM_STRING('192.168.1.100'), 16))"));

	// ---- NET.IP_NET_MASK ----
	[Fact(Skip = "Not yet supported")] public async Task NetIpNetMask_IPv4_24() => Assert.Equal("255.255.255.0", await Scalar("SELECT NET.IP_TO_STRING(NET.IP_NET_MASK(4, 24))"));
	[Fact(Skip = "Not yet supported")] public async Task NetIpNetMask_IPv4_16() => Assert.Equal("255.255.0.0", await Scalar("SELECT NET.IP_TO_STRING(NET.IP_NET_MASK(4, 16))"));

	// ---- NET.IPV4_FROM_INT64 / NET.IPV4_TO_INT64 ----
	[Fact] public async Task NetIpv4FromInt64_Loopback() => Assert.NotNull(await Scalar("SELECT NET.IPV4_FROM_INT64(2130706433)"));  // 127.0.0.1
	[Fact] public async Task NetIpv4ToInt64_Loopback() => Assert.Equal("2130706433", await Scalar("SELECT NET.IPV4_TO_INT64(NET.IPV4_FROM_INT64(2130706433))"));
}
