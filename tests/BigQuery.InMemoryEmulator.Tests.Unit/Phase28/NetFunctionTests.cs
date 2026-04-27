using Xunit;

namespace BigQuery.InMemoryEmulator.Tests.Unit.Phase28;

/// <summary>
/// Phase 28: NET functions — NET.IP_TRUNC, NET.IPV4_FROM_INT64, NET.IPV4_TO_INT64, NET.SAFE_IP_FROM_STRING.
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions
/// </summary>
public class NetFunctionTests
{
	private static QueryExecutor CreateExecutor()
	{
		var store = new InMemoryDataStore("test-project");
		store.Datasets["ds"] = new InMemoryDataset("ds");
		return new QueryExecutor(store, "ds");
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netip_trunc
	//   "Converts a BYTES IPv4 or IPv6 address in network byte order to a BYTES subnet address."
	[Fact]
	public void IpTrunc_TruncatesIpv4()
	{
		var exec = CreateExecutor();
		// NET.IP_TRUNC(NET.IP_FROM_STRING('192.168.1.100'), 24) should give 192.168.1.0
		var result = exec.Execute("SELECT NET.IP_TO_STRING(NET.IP_TRUNC(NET.IP_FROM_STRING('192.168.1.100'), 24)) AS val");
		Assert.Single(result.Rows);
		Assert.Equal("192.168.1.0", result.Rows[0].F[0].V);
	}

	[Fact]
	public void IpTrunc_ZeroPrefix_AllZeros()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT NET.IP_TO_STRING(NET.IP_TRUNC(NET.IP_FROM_STRING('192.168.1.100'), 0)) AS val");
		Assert.Single(result.Rows);
		Assert.Equal("0.0.0.0", result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netipv4_from_int64
	//   "Converts an IPv4 address from integer format to binary (BYTES) format."
	[Fact]
	public void Ipv4FromInt64_ConvertsToBytes()
	{
		var exec = CreateExecutor();
		// 0x0100007F = 16777343 → 1.0.0.127
		var result = exec.Execute("SELECT NET.IP_TO_STRING(NET.IPV4_FROM_INT64(0)) AS val");
		Assert.Single(result.Rows);
		Assert.Equal("0.0.0.0", result.Rows[0].F[0].V);
	}

	[Fact]
	public void Ipv4FromInt64_MaxValue()
	{
		var exec = CreateExecutor();
		// 0xFFFFFFFF = 4294967295 → 255.255.255.255
		var result = exec.Execute("SELECT NET.IP_TO_STRING(NET.IPV4_FROM_INT64(4294967295)) AS val");
		Assert.Single(result.Rows);
		Assert.Equal("255.255.255.255", result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netipv4_to_int64
	//   "Converts an IPv4 address from binary (BYTES) format to integer format."
	[Fact]
	public void Ipv4ToInt64_ConvertsBytesToInt()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT NET.IPV4_TO_INT64(NET.IP_FROM_STRING('0.0.171.205')) AS val");
		Assert.Single(result.Rows);
		// 0x00 0x00 0xAB 0xCD = 43981
		Assert.Equal("43981", result.Rows[0].F[0].V);
	}

	// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#netsafe_ip_from_string
	//   "Similar to NET.IP_FROM_STRING, but returns NULL instead of producing an error if the input is invalid."
	[Fact]
	public void SafeIpFromString_ValidIp_ReturnsBytes()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT NET.IP_TO_STRING(NET.SAFE_IP_FROM_STRING('10.0.0.1')) AS val");
		Assert.Single(result.Rows);
		Assert.Equal("10.0.0.1", result.Rows[0].F[0].V);
	}

	[Fact]
	public void SafeIpFromString_InvalidIp_ReturnsNull()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT NET.SAFE_IP_FROM_STRING('not_an_ip') AS val");
		Assert.Single(result.Rows);
		Assert.Null(result.Rows[0].F[0].V);
	}

	[Fact]
	public void SafeIpFromString_Cidr_ReturnsNull()
	{
		var exec = CreateExecutor();
		var result = exec.Execute("SELECT NET.SAFE_IP_FROM_STRING('10.0.0.1/32') AS val");
		Assert.Single(result.Rows);
		Assert.Null(result.Rows[0].F[0].V);
	}
}
