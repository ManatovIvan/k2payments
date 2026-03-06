# Release Notes - v0.1.0-alpha.1

This release establishes the first end-to-end executable baseline aligned to the PRD.

## Included capabilities
- Pipeline-driven runtime core with prepare/commit/abort semantics.
- SQLite + PostgreSQL persistence layers (`sqlx`) with migration/seed workflow.
- RocksDB persistence backend.
- Admin API contract + controller + `axum` HTTP host + gRPC admin service.
- CLI operational baseline (`status`, `config`, `db`, `tx`) with admin API status support.
- Built-in participants: `message-logger`, `schema-validator`, `business-rule-validator`, `status-response-builder` (schema/business now powered by `mx20022` parse/validate).
- Additional built-in participants: `fednow-rule-validator`, `sepa-rule-validator`, `cbpr-rule-validator`, `duplicate-checker`, `routing-engine`, `rate-limiter`, `circuit-breaker`, `acknowledgement-builder`, `error-response-builder`.
- Transaction engine metrics expanded with state transition counters and participant duration/error telemetry.
- Startup recovery now replays transactions left in non-terminal states (`RECEIVED`, `PREPARING`, `PREPARED`, `COMMITTING`, `ABORTING`) with configurable enable flag and replay limit.
- Store query filtering for SQLite/PostgreSQL now executes in SQL with pushed-down `WHERE`/`LIMIT` clauses (avoids full-table in-process filtering).
- Dead-letter replay semantics now consume replayed records (entry is removed from dead-letter store on successful replay).
- `mxctl` now includes dead-letter operations (`deadletter list|show|replay`) across store backends.
- `mxctl tx context <config> <tx_id>` now prints persisted context mutation history.
- Participant config hot-reload is now available with topology safety checks:
  - Admin HTTP `POST /reload` and gRPC `Reload` endpoints.
  - `mxctl reload` command for HTTP and gRPC admin surfaces.
  - Optional runtime watcher via `runtime.participant_reload_poll_ms` for automatic reload on config content changes.
  - Runtime metrics include `mx_runtime_config_reload_total{result=...}` and `mx_runtime_config_reload_errors_total{error_type=...}`.
- Admin status surfaces now include richer operational telemetry:
  - uptime
  - store health summary
  - in-flight transaction count
  - pending correlation count
  - dead-letter count
  - config version and last reload result/timestamp
- HTTP channel implementation (`axum` inbound + `reqwest` outbound).
- gRPC channel implementation (tonic inbound/outbound).
- TCP channel implementation (length-prefixed and delimiter framing).
- NATS channel implementation (subscriber inbound + publisher outbound).
- Kafka channel implementation (consumer inbound + producer outbound).
- AMQP channel implementation (consumer inbound + publisher outbound).
- File channel implementation (directory watch inbound + file writer outbound).
- Admin and channel ingress security hardening:
  - Configurable JWT RBAC for admin HTTP + gRPC endpoints.
  - Configurable JWT/static bearer auth for HTTP + gRPC inbound channels.
  - Optional mTLS subject enforcement via forwarded client-cert subject headers.

## Known limits
- Field-level encryption and Vault integration are not complete yet.
- Direct TLS termination with peer certificate verification is not built into runtime listeners yet;
  mTLS policy currently assumes proxy-terminated TLS and forwarded client-cert subject headers.
- Correlation engine is wired into the runtime pipeline loop and runs a timeout scan worker.
