# Release Notes - v0.1.0-alpha.1

This release establishes the first end-to-end executable baseline aligned to the PRD.

## Included capabilities
- Pipeline-driven runtime core with prepare/commit/abort semantics.
- SQLite + PostgreSQL persistence layers (`sqlx`) with migration/seed workflow.
- RocksDB persistence backend.
- Admin API contract + controller + `axum` HTTP host + gRPC admin service.
- CLI operational baseline (`status`, `config`, `db`, `tx`) with admin API status support.
- Built-in participants: `message-logger`, `schema-validator`, `business-rule-validator`, `status-response-builder` (schema/business now powered by `mx20022` parse/validate).
- HTTP channel implementation (`axum` inbound + `reqwest` outbound).
- gRPC channel implementation (tonic inbound/outbound).
- TCP channel implementation (length-prefixed and delimiter framing).
- NATS channel implementation (subscriber inbound + publisher outbound).
- Kafka channel implementation (consumer inbound + producer outbound).
- AMQP channel implementation (consumer inbound + publisher outbound).
- File channel implementation (directory watch inbound + file writer outbound).

## Known limits
- Field-level encryption and Vault integration are not complete yet.
- Admin auth is currently a lightweight bearer gate and not full RBAC/JWT+mTLS policy yet.
- Correlation engine is wired into the runtime pipeline loop and runs a timeout scan worker.

## Recommended next increment
- Upgrade schema/business-rule participants to use `mx20022` parse/validate crates.
- Wire the correlation engine into the runtime and schedule timeout scanning.
- Expand admin auth to JWT/mTLS with role-based policies.
