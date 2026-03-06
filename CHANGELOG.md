# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0-alpha.1] - 2026-03-05

### Added
- Workspace scaffolding for runtime, channels, stores, admin, config, metrics, crypto, session, correlation, and CLI crates.
- Core transaction lifecycle state machine and `TransactionManager` prepare/commit/abort orchestration.
- Typed `Context` with audit mutation history and state transition enforcement.
- Runtime config parser/validator for channels and pipeline topology.
- SQLite-backed store implementation with migration/seed SQL and full `Store` trait wiring.
- Runtime application layer for pipeline processing and persistence lifecycle.
- Admin API DTOs, controller contracts, middleware chain, route map, and a minimal runnable TCP HTTP host.
- OpenAPI and gRPC admin contract artifacts (`docs/api/openapi.yaml`, `proto/admin.proto`).
- CLI operations for config validation, sqlite migration/rollback/seed, and transaction lookup.
- Minimal HTTP inbound/outbound channel implementation.

### Changed
- Runtime bootstrap now supports active service modes:
  - pipeline engine
  - admin host
  - both concurrently
- README command examples updated for runtime and operator workflows.

### Tested
- `cargo fmt --all`
- `cargo test --workspace --offline`
