# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.4] - 2026-04-26

### Added
- Hash functions: `SHA1`, `SHA512`, `FARM_FINGERPRINT`
- Statistical aggregate functions: `STDDEV`, `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`, `VAR_POP`, `VARIANCE`
- Array functions: `ARRAY_CONCAT`, `ARRAY_REVERSE`, `ARRAY_FIRST`, `ARRAY_LAST`, `ARRAY_SLICE`
- Array literal syntax: `[expr, ...]` (tokenizer + parser support for `[` and `]` brackets)
- Dry run query validation: invalid SQL now returns errors even for dry-run jobs

### Fixed
- `QueryOptions.Labels` initialization: fixed `NullReferenceException` when using collection initializer syntax
- Dry-run invalid queries no longer silently succeed — they now return HTTP 400

### Changed
- Upgraded Google.Cloud.BigQuery.V2 SDK from 3.10.0 to 3.11.0

### Added
- Initial project scaffold (Phase 0)
- Solution structure with 3 source projects and 4 test projects
- `FakeBigQueryHandler` — HTTP interception skeleton
- `FakeBigQueryHttpClientFactory` — SDK pipeline integration
- `InMemoryDataStore`, `InMemoryDataset`, `InMemoryTable` — data model
- `InMemoryBigQuery.Create()` — entry point
- `InMemoryBigQueryBuilder` — fluent builder
- `UseInMemoryBigQuery()` — DI integration skeleton
- Test infrastructure: `ITestDatasetFixture`, `BigQuerySession`, `TestFixtureFactory`
- Three test targets: InMemory, BigQueryEmulator (Docker), BigQueryCloud
- CI workflows: test, weekly cloud parity, release
- Scripts: run-tests, start-emulator, stop-emulator
