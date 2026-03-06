# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- Configurable admin security policies: `disabled`, `legacy_bearer`, and `jwt_hs256` modes with per-endpoint RBAC role requirements.
- JWT and static bearer authentication support for HTTP/gRPC inbound channels (`auth_mode` + auth policy fields).
- Optional mTLS subject checks for admin and channel ingress using forwarded client-certificate subject headers.
- Shared auth test coverage for JWT role enforcement and mTLS subject validation paths.
- New built-in participants: `fednow-rule-validator`, `sepa-rule-validator`, `cbpr-rule-validator`, `duplicate-checker`, `routing-engine`, `rate-limiter`, `circuit-breaker`, `acknowledgement-builder`, `error-response-builder`.
- Participant-config hot-reload control-plane surface:
  - Admin HTTP `POST /reload`
  - Admin gRPC `Reload`
  - `mxctl reload` command
- Optional automatic participant reload watcher via `runtime.participant_reload_poll_ms`.
- Runtime metrics `mx_runtime_config_reload_total{result=success|error}` and `mx_runtime_config_reload_errors_total{error_type=...}`.
- Admin/CLI status responses now include uptime, store health, in-flight transaction counts, pending correlation and dead-letter counts, plus config version and last reload metadata.
- Added `mxctl channel list` command (HTTP/gRPC admin-backed channel inventory).

### Changed
- Admin HTTP/gRPC `ready`, `status`, and `tx` routes now enforce a unified auth policy engine instead of ad-hoc token checks.
- Runtime config now validates `runtime.admin_auth` settings and rejects invalid JWT auth declarations at startup.
- Runtime participant registry now supports PRD flow-control and scheme-specific participant names/config blocks.
- Runtime execution now emits participant and state-transition metrics (`mx_participant_duration_seconds`, `mx_participant_errors_total`, `mx_transaction_state_transitions_total`).
- Runtime startup now performs configurable recovery replay of in-progress transactions from the store.
- SQLite/PostgreSQL `Store::query()` now pushes filters/limits down to SQL instead of scanning and filtering in process memory.
- `replay_dead_letter` now performs real replay-consumption semantics by deleting replayed dead-letter records (SQLite/PostgreSQL/RocksDB).
- Added `mxctl deadletter list|show|replay` commands for dead-letter operations via configured store backend.
- Added `mxctl tx context` command backed by a new store API for persisted context mutation history.
- Runtime now supports participant-config-only reload with strict topology-change rejection (pipeline/participant structure changes require restart).

### Tested
- `cargo fmt --all`
- `cargo test --workspace`

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
