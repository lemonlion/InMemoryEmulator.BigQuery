# BigQuery.InMemoryEmulator

[![NuGet](https://img.shields.io/nuget/v/BigQuery.InMemoryEmulator.svg)](https://www.nuget.org/packages/BigQuery.InMemoryEmulator)
[![Tests](https://github.com/lemonlion/BigQuery.InMemoryEmulator/actions/workflows/test.yml/badge.svg)](https://github.com/lemonlion/BigQuery.InMemoryEmulator/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A high-fidelity, in-memory implementation of the Google Cloud BigQuery SDK for .NET — purpose-built for fast, reliable component and integration testing. Zero production code changes required.

## Quick Start

```csharp
// Create an in-memory BigQuery instance
var result = InMemoryBigQuery.Create("test-project", "my_dataset", ds =>
{
    ds.AddTable("users", new TableSchemaBuilder
    {
        { "id", BigQueryDbType.Int64 },
        { "name", BigQueryDbType.String },
    }.Build());
});

// Use the real BigQueryClient — backed by in-memory storage
var client = result.Client;

await client.InsertRowsAsync("my_dataset", "users", new[]
{
    new BigQueryInsertRow { ["id"] = 1, ["name"] = "Alice" },
    new BigQueryInsertRow { ["id"] = 2, ["name"] = "Bob" },
});

var results = await client.ExecuteQueryAsync(
    "SELECT * FROM my_dataset.users WHERE name = @name",
    new[] { new BigQueryParameter("name", BigQueryDbType.String, "Alice") });
```

## How It Works

The emulator intercepts all HTTP calls at the `HttpMessageHandler` level inside the Google BigQuery SDK pipeline. This means:

- **Full SDK fidelity** — SDK serialization, retry logic, and pagination work exactly as in production
- **Zero production code changes** — no special interfaces, no mocking, no conditional logic
- **Real `BigQueryClient`** — your tests use the exact same client type as production code

```
Your Code → BigQueryClient → SDK HTTP Pipeline → FakeBigQueryHandler → InMemoryDataStore
```

## Installation

```bash
dotnet add package BigQuery.InMemoryEmulator
```

**Requirements**: .NET 8.0+, `Google.Cloud.BigQuery.V2` 3.x+

## DI Integration

For ASP.NET Core integration testing:

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryBigQuery(options =>
    {
        options.ProjectId = "test-project";
        options.AddDataset("my_dataset", ds =>
        {
            ds.AddTable("users", schema);
        });
    });
});
```

## Features

### Supported

- **Dataset CRUD** — Create, get, list, update, delete
- **Table CRUD** — Create with schema, get, list, update, delete
- **Streaming inserts** — With schema validation and `insertId` dedup
- **SQL queries** — GoogleSQL dialect via `ExecuteQuery` / `CreateQueryJob`
- **DML** — INSERT, UPDATE, DELETE, MERGE via SQL
- **DDL** — CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE VIEW via SQL
- **Built-in functions** — 200+ GoogleSQL functions
- **Parameterised queries** — `@param` substitution
- **Fault injection** — Simulate errors (403, 404, 429, 500, 503)
- **Request/query logging** — Record all HTTP requests and SQL queries
- **State persistence** — Export/import table state as JSON
- **DI integration** — `UseInMemoryBigQuery()` for `IServiceCollection`
- **Three test targets** — In-memory, goccy/bigquery-emulator, real BigQuery

### Not Supported

- Legacy SQL (`useLegacySql: true`)
- BigQuery Storage API (gRPC)
- Load/extract jobs (data lives in-memory only)
- BQML (`CREATE MODEL`)
- Cross-project queries
- Row-level access policies / data masking

## Documentation

See the [wiki](https://github.com/lemonlion/BigQuery.InMemoryEmulator/wiki) for full documentation.

## License

MIT — see [LICENSE](LICENSE).
