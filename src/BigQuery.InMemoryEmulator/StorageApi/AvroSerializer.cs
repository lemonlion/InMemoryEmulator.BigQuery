using System.Text;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.InMemoryEmulator.StorageApi;

/// <summary>
/// Minimal Avro serializer for converting in-memory BigQuery data to Avro binary format.
/// Supports the subset of BigQuery types needed for the Storage Read API.
/// </summary>
/// <remarks>
/// Ref: https://avro.apache.org/docs/1.12.0/specification/
///   "Avro data is always serialized with its schema."
/// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#avroschema
///   "The Avro schema is the JSON representation of the Avro schema."
/// </remarks>
internal static class AvroSerializer
{
	/// <summary>
	/// Generates an Avro JSON schema string from a BigQuery TableSchema.
	/// </summary>
	/// <remarks>
	/// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#avroschema
	///   "Avro schema for the response. The Avro schema describes the format of the data."
	/// </remarks>
	public static string ToAvroSchemaJson(TableSchema bqSchema)
	{
		var fields = new List<string>();
		foreach (var field in bqSchema.Fields)
		{
			var avroType = BigQueryTypeToAvro(field.Type);
			var isNullable = field.Mode?.Equals("REQUIRED", StringComparison.OrdinalIgnoreCase) != true;

			string fieldJson;
			if (isNullable)
				fieldJson = $"{{\"name\":\"{field.Name}\",\"type\":[\"null\",\"{avroType}\"]}}";
			else
				fieldJson = $"{{\"name\":\"{field.Name}\",\"type\":\"{avroType}\"}}";

			fields.Add(fieldJson);
		}

		return $"{{\"type\":\"record\",\"name\":\"root\",\"fields\":[{string.Join(",", fields)}]}}";
	}

	/// <summary>
	/// Serializes a list of in-memory rows into Avro binary format.
	/// Each row is concatenated as a sequence of encoded field values.
	/// </summary>
	/// <remarks>
	/// Ref: https://avro.apache.org/docs/1.12.0/specification/#data-serialization-and-deserialization
	///   "Data is serialized according to the schema."
	/// Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#avrorows
	///   "serialized_binary_rows: Binary-serialized rows in a block."
	/// </remarks>
	public static byte[] SerializeRows(IReadOnlyList<InMemoryRow> rows, TableSchema schema)
	{
		using var ms = new MemoryStream();
		foreach (var row in rows)
		{
			SerializeRow(ms, row, schema);
		}
		return ms.ToArray();
	}

	private static void SerializeRow(MemoryStream ms, InMemoryRow row, TableSchema schema)
	{
		foreach (var field in schema.Fields)
		{
			var isNullable = field.Mode?.Equals("REQUIRED", StringComparison.OrdinalIgnoreCase) != true;
			row.Fields.TryGetValue(field.Name, out var value);

			if (isNullable)
			{
				if (value is null)
				{
					// Union index 0 = null branch
					WriteLong(ms, 0);
				}
				else
				{
					// Union index 1 = type branch
					WriteLong(ms, 1);
					WriteValue(ms, value, field.Type);
				}
			}
			else
			{
				WriteValue(ms, value!, field.Type);
			}
		}
	}

	private static void WriteValue(MemoryStream ms, object value, string bqType)
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
		switch (bqType.ToUpperInvariant())
		{
			case "STRING":
			case "JSON":
			case "GEOGRAPHY":
				WriteString(ms, value?.ToString() ?? "");
				break;

			case "INTEGER":
			case "INT64":
				WriteLong(ms, Convert.ToInt64(value));
				break;

			case "FLOAT":
			case "FLOAT64":
				WriteDouble(ms, Convert.ToDouble(value));
				break;

			case "BOOLEAN":
			case "BOOL":
				ms.WriteByte(Convert.ToBoolean(value) ? (byte)1 : (byte)0);
				break;

			case "BYTES":
				if (value is byte[] bytes)
					WriteBytes(ms, bytes);
				else
					WriteBytes(ms, Convert.FromBase64String(value?.ToString() ?? ""));
				break;

			case "TIMESTAMP":
			case "DATETIME":
			case "DATE":
			case "TIME":
			case "NUMERIC":
			case "BIGNUMERIC":
				// For these types, serialize as string representation for simplicity.
				// Ref: Avro logical types are not strictly required for in-memory emulation.
				WriteString(ms, value?.ToString() ?? "");
				break;

			default:
				WriteString(ms, value?.ToString() ?? "");
				break;
		}
	}

	/// <summary>
	/// Maps BigQuery types to Avro type names.
	/// </summary>
	/// <remarks>
	/// Ref: https://cloud.google.com/bigquery/docs/reference/storage#avro_schema_details
	///   Describes how BigQuery types map to Avro types.
	/// </remarks>
	private static string BigQueryTypeToAvro(string bqType)
	{
		return bqType.ToUpperInvariant() switch
		{
			"STRING" or "JSON" or "GEOGRAPHY" => "string",
			"INTEGER" or "INT64" => "long",
			"FLOAT" or "FLOAT64" => "double",
			"BOOLEAN" or "BOOL" => "boolean",
			"BYTES" => "bytes",
			// For TIMESTAMP, DATETIME, DATE, TIME, NUMERIC, BIGNUMERIC — use string for simplicity
			"TIMESTAMP" or "DATETIME" or "DATE" or "TIME" => "string",
			"NUMERIC" or "BIGNUMERIC" => "string",
			_ => "string",
		};
	}

	/// <summary>
	/// Writes a long value in Avro zig-zag variable-length encoding.
	/// </summary>
	/// <remarks>
	/// Ref: https://avro.apache.org/docs/1.12.0/specification/#primitive-types
	///   "long values are written using variable-length zig-zag coding."
	/// </remarks>
	internal static void WriteLong(MemoryStream ms, long value)
	{
		var encoded = (ulong)((value << 1) ^ (value >> 63));
		while ((encoded & ~0x7FUL) != 0)
		{
			ms.WriteByte((byte)((encoded & 0x7F) | 0x80));
			encoded >>= 7;
		}
		ms.WriteByte((byte)encoded);
	}

	/// <summary>
	/// Writes a string value: varint length followed by UTF-8 bytes.
	/// </summary>
	private static void WriteString(MemoryStream ms, string value)
	{
		var bytes = Encoding.UTF8.GetBytes(value);
		WriteLong(ms, bytes.Length);
		ms.Write(bytes, 0, bytes.Length);
	}

	/// <summary>
	/// Writes a double value as 8 bytes in little-endian IEEE 754 format.
	/// </summary>
	private static void WriteDouble(MemoryStream ms, double value)
	{
		var bytes = BitConverter.GetBytes(value);
		if (!BitConverter.IsLittleEndian) Array.Reverse(bytes);
		ms.Write(bytes, 0, bytes.Length);
	}

	/// <summary>
	/// Writes a bytes value: varint length followed by raw bytes.
	/// </summary>
	private static void WriteBytes(MemoryStream ms, byte[] value)
	{
		WriteLong(ms, value.Length);
		ms.Write(value, 0, value.Length);
	}

	/// <summary>
	/// Reads an Avro zig-zag encoded long from a stream.
	/// </summary>
	internal static long ReadLong(Stream stream)
	{
		ulong encoded = 0;
		int shift = 0;
		int b;
		do
		{
			b = stream.ReadByte();
			if (b < 0) throw new EndOfStreamException();
			encoded |= ((ulong)(b & 0x7F)) << shift;
			shift += 7;
		} while ((b & 0x80) != 0);

		return (long)((encoded >> 1) ^ (ulong)(-(long)(encoded & 1)));
	}

	/// <summary>
	/// Reads an Avro string (varint length + UTF-8 bytes) from a stream.
	/// </summary>
	internal static string ReadString(Stream stream)
	{
		var length = (int)ReadLong(stream);
		var bytes = new byte[length];
		var totalRead = 0;
		while (totalRead < length)
		{
			var read = stream.Read(bytes, totalRead, length - totalRead);
			if (read == 0) throw new EndOfStreamException();
			totalRead += read;
		}
		return Encoding.UTF8.GetString(bytes);
	}

	/// <summary>
	/// Reads an Avro double (8 bytes little-endian) from a stream.
	/// </summary>
	internal static double ReadDouble(Stream stream)
	{
		var bytes = new byte[8];
		var totalRead = 0;
		while (totalRead < 8)
		{
			var read = stream.Read(bytes, totalRead, 8 - totalRead);
			if (read == 0) throw new EndOfStreamException();
			totalRead += read;
		}
		if (!BitConverter.IsLittleEndian) Array.Reverse(bytes);
		return BitConverter.ToDouble(bytes, 0);
	}
}
