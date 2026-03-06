# Operations Guide

This guide covers runtime operations, observability, and common workflows.

## Running the runtime

```bash
cargo run -p mx20022-runtime -- --config docs/examples/basic.toml
cargo run -p mx20022-runtime -- --config docs/examples/basic.toml --serve-admin
cargo run -p mx20022-runtime -- --config docs/examples/basic.toml --serve-admin --serve-admin-grpc
```

Use `--no-pipelines` to run admin-only.

## Configuration checklist

- `runtime.name` and `runtime.instance_id` must be non-empty.
- `store.backend` must be one of `sqlite`, `postgres`, or `rocksdb`.
- Each `[[pipeline]]` must reference an existing `channel_in` and include participants.
- Use `max_concurrent` to bound the per-pipeline task pool.
- `runtime.correlation_scan_interval_ms` controls the correlation timeout scan worker (0 disables).
- `runtime.participant_reload_poll_ms` enables automatic participant-config reload polling; `0`/unset disables watcher.
- `runtime.recover_incomplete_on_startup` controls startup replay of non-terminal transactions.
- `runtime.recovery_startup_limit` bounds the number of startup recovery attempts.
- `runtime.admin_auth.mode` supports `disabled`, `legacy_bearer`, or `jwt_hs256`.
- If `runtime.admin_auth.mode = "jwt_hs256"`, set `runtime.admin_auth.jwt_hs256_secret`.
- Channel HTTP/gRPC ingress supports `auth_mode = "disabled" | "static_bearer" | "jwt_hs256"`.
- For proxy-terminated mTLS, set `require_mtls_subject`/`auth_require_mtls_subject` and pass subject headers.

## Admin endpoints

HTTP admin (default `127.0.0.1:9090`):
- `GET /health` (no auth)
- `GET /ready` (auth required unless `runtime.admin_auth.mode = "disabled"`)
- `GET /status` (auth required unless `runtime.admin_auth.mode = "disabled"`)
- `POST /reload` (auth + RBAC required unless `runtime.admin_auth.mode = "disabled"`)
- `GET /tx/:tx_id` (auth + RBAC required unless `runtime.admin_auth.mode = "disabled"`)
- `GET /metrics` (no auth)

`GET /status` now includes operational depth fields:
- `uptime_ms`
- `store_ok` / `store_details`
- `in_flight_count`
- `pending_correlation_count`
- `dead_letter_count`
- `config_version`
- `last_reload_result` / `last_reload_at`

Admin auth supports:
- Legacy bearer mode (`legacy_bearer`) for compatibility.
- JWT RBAC mode (`jwt_hs256`) with per-route role policies (`ready_roles`, `status_roles`, `tx_roles`, `reload_roles`).
- Optional mTLS subject enforcement using forwarded certificate subject headers from your TLS terminator.

gRPC admin (default `127.0.0.1:9091`) exposes health, ready, status, reload, and transaction
queries. JWT/mTLS subject checks apply via gRPC metadata with the same header names.
Use `mxctl status --admin-grpc` and `mxctl reload --admin-grpc`.

Participant config reload options:
- Manual: `mxctl reload --admin ...` or `mxctl reload --admin-grpc ...`.
- Automatic: set `runtime.participant_reload_poll_ms` to a positive interval; runtime watches for file-content changes and applies participant-config-only reloads.

## Metrics

`GET /metrics` exports Prometheus metrics:
- `mx_transactions_total`
- `mx_transaction_duration_seconds`
- `mx_transactions_active`
- `mx_transaction_state_transitions_total`
- `mx_participant_duration_seconds`
- `mx_participant_errors_total`
- `mx_runtime_config_reload_total` (labels: `result=success|error`)
- `mx_runtime_config_reload_errors_total` (labels: `error_type`, e.g. `parse`, `apply`)

Scrape the admin host with your Prometheus instance.

## Logging

The runtime uses `tracing`. Configure verbosity with `RUST_LOG`, for example:

```bash
RUST_LOG=info mxruntime --config docs/examples/basic.toml
```

## Store operations

Use `mxctl` to manage migrations and seed data:

```bash
cargo run -p mx20022-cli -- db migrate docs/examples/basic.toml
cargo run -p mx20022-cli -- db seed docs/examples/basic.toml
cargo run -p mx20022-cli -- tx context docs/examples/basic.toml TX-123
cargo run -p mx20022-cli -- reload --admin http://127.0.0.1:9090 --token admin
cargo run -p mx20022-cli -- channel list --admin http://127.0.0.1:9090 --token readonly
```

Dead-letter replay note:
- `replay_dead_letter` now removes the replayed dead-letter record from store backends.
- `mxctl deadletter` commands are available:
  - `mxctl deadletter list --config docs/examples/basic.toml --limit 50`
  - `mxctl deadletter show --config docs/examples/basic.toml --id DL-1`
  - `mxctl deadletter replay --config docs/examples/basic.toml --id DL-1`

## Incident checklist

- Check `/health` and `/ready` for store connectivity.
- Inspect `/status` for pipeline/channel wiring.
- Review logs for `pipeline processing failed` or store errors.
- Review startup log entry `startup recovery run completed` for recovery replay counts.
- Validate the store backend URL and credentials in the config.
