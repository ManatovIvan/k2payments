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

## Admin endpoints

HTTP admin (default `127.0.0.1:9090`):
- `GET /health` (no auth)
- `GET /ready` (bearer token required)
- `GET /status` (bearer token required)
- `GET /tx/:tx_id` (bearer token required; rejects `Bearer readonly`)
- `GET /metrics` (no auth)

Admin auth is a lightweight bearer gate. For production, front this with a reverse
proxy that enforces mTLS/JWT and network policy.

gRPC admin (default `127.0.0.1:9091`) exposes health, ready, status, and transaction
queries. Use `mxctl status --admin-grpc`.

## Metrics

`GET /metrics` exports Prometheus metrics:
- `mx_transactions_total`
- `mx_transaction_duration_seconds`
- `mx_transactions_active`

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
```

## Incident checklist

- Check `/health` and `/ready` for store connectivity.
- Inspect `/status` for pipeline/channel wiring.
- Review logs for `pipeline processing failed` or store errors.
- Validate the store backend URL and credentials in the config.
