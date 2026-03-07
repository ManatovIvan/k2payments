# mx20022-runtime

`mx20022-runtime` is a modular ISO 20022 payment runtime written in Rust.

It ingests messages from channel adapters, executes a configurable participant pipeline with transactional lifecycle control, persists all transaction state, and can publish outbound responses.

## Why This Project

- Deterministic pipeline processing with explicit lifecycle states (`RECEIVED` → `PREPARING` → ...).
- Pluggable channels (HTTP, gRPC, TCP, Kafka, NATS, AMQP, file) behind shared traits.
- Pluggable stores (SQLite, Postgres, RocksDB).
- Admin plane (HTTP + gRPC) with status/reload/tx inspection.
- Correlation engine for expectation matching and timeout handling.

## Workspace Layout

This is a Rust workspace (`22` crates) with clear crate boundaries:

- `crates/mx20022-runtime`: app wiring + runtime binary (`mxruntime`)
- `crates/mx20022-runtime-core`: context, participant trait, state machine, transaction manager
- `crates/mx20022-channels/*`: channel adapters
- `crates/mx20022-participants`: built-in participant implementations
- `crates/mx20022-store/*`: storage abstraction + backends
- `crates/mx20022-admin`: admin host/services/auth
- `crates/mx20022-config`: TOML parse + validation
- `crates/mx20022-correlation`: expectation/correlation engine
- `crates/mx20022-cli`: operator CLI (`mxctl`)

## Quick Start

### 1. Prerequisites

- Rust stable toolchain
- `cargo`
- `just` (optional, but convenient)

### 2. Build

```bash
cargo build --workspace
```

### 3. Validate a config

```bash
cargo run -p mx20022-cli -- config validate docs/examples/basic.toml
```

### 4. Run runtime

```bash
cargo run -p mx20022-runtime -- --config docs/examples/basic.toml
```

### 5. Run runtime with admin HTTP + gRPC

```bash
cargo run -p mx20022-runtime -- --config docs/examples/basic.toml --serve-admin --serve-admin-grpc
```

## Common Commands

Using `just`:

```bash
just fmt
just check
just test
just bench
```

Direct cargo equivalents:

```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo bench -p mx20022-runtime --no-run
```

## CLI (`mxctl`) Examples

```bash
# Validate config
cargo run -p mx20022-cli -- config validate docs/examples/basic.toml

# DB ops
cargo run -p mx20022-cli -- db migrate docs/examples/basic.toml
cargo run -p mx20022-cli -- db seed docs/examples/basic.toml

# Runtime status (HTTP admin)
cargo run -p mx20022-cli -- status --admin http://127.0.0.1:9090 --token <token>

# Runtime status (gRPC admin)
cargo run -p mx20022-cli -- status --admin-grpc http://127.0.0.1:9091

# Trigger participant reload
cargo run -p mx20022-cli -- reload --admin http://127.0.0.1:9090 --token <token>
```

## Configuration Notes

- Main config is TOML (`RuntimeConfig` in `mx20022-config`).
- Pipelines bind `channel_in`, optional `channel_out`, participants, `max_concurrent`, and optional `timeout_ms`.
- `runtime.enforce_secure_channels = true` can block plaintext channel configs unless `allow_plaintext=true` is explicitly set per channel.
- Admin auth supports `disabled`, `legacy_bearer`, and `jwt_hs256`.

Start from:

- `docs/examples/basic.toml`

## Operational Docs

- `docs/QUICKSTART.md`: fastest path to local runtime bring-up
- `architecture.md` (root): engineering architecture reference
- `contributor.md` (root): contributor workflow and standards
- `docs/OPERATIONS.md`: operator runbook
- `docs/PARTICIPANT_GUIDE.md`: participant development guide
- `docs/ARCHITECTURE.md`: condensed architecture summary

## Current Guarantees and Known Limits

- Transaction lifecycle + audit persistence are strong and heavily tested.
- Outbound channel delivery is wired and active.
- Correlation supports in-memory fast path with store fallback for cross-instance correctness.
- Full distributed correlation indexing and fully uniform TLS across all transports are still active areas.
- Kafka offsets commit after enqueue (manual commit), not after full end-to-end business completion.

## CI

GitHub Actions runs:

- `fmt`
- `clippy`
- `cargo-deny`
- workspace tests (with Postgres service)
- coverage gate (`cargo llvm-cov --fail-under-lines 55`)

See `.github/workflows/ci.yml`.

## License

Apache-2.0
