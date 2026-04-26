# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
