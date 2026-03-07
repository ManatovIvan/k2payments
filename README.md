# mx20022-runtime

`mx20022-runtime` is a production-focused ISO 20022 payment orchestration runtime written in Rust.

It receives financial messages from inbound channels, executes deterministic participant pipelines, persists every transaction state transition, and emits outbound responses or routed deliveries.

> `mx20022-runtime` is licensed under **AGPL-3.0-only**. Commercial licensing is available for organizations that need to use this software without AGPL obligations. Contact **licensing@k2payments.com** for details.
>
> The `mx20022` core libraries (`mx20022-model`, `mx20022-parse`, `mx20022-validate`) remain separately licensed under **Apache-2.0**.

## Why this runtime

- Deterministic transaction lifecycle with auditable state transitions.
- Pluggable transport channels: HTTP, gRPC, TCP, Kafka, NATS, AMQP, file.
- Pluggable state stores: SQLite, Postgres, RocksDB.
- Built-in participant chain for schema, duplicate, routing, rules, and response handling.
- Admin plane (HTTP + gRPC), metrics, correlation, and runtime reload controls.

## Try it in under 10 minutes

```bash
cargo build --workspace
cargo run -p mx20022-cli -- config validate docs/examples/quickstart.toml
cargo run -p mx20022-runtime -- --config docs/examples/quickstart.toml --serve-admin
```

Then follow the end-to-end `pacs.008` walk-through in [docs/QUICKSTART.md](docs/QUICKSTART.md).

## Example configs

- Baseline secure-ish local config: `docs/examples/basic.toml`
- FedNow-style gateway profile: `docs/examples/fednow-gateway.toml`
- MT-to-MX bridge profile: `docs/examples/mt-to-mx-bridge.toml`
- Fast local onboarding config: `docs/examples/quickstart.toml`

## Documentation

- [docs/QUICKSTART.md](docs/QUICKSTART.md): clone to first `pacs.008` processing.
- [docs/OPERATIONS.md](docs/OPERATIONS.md): runbook, health checks, observability, incident flow.
- [docs/PARTICIPANT_GUIDE.md](docs/PARTICIPANT_GUIDE.md): participant design, registration, testing.
- [architecture.md](architecture.md): full architecture and crate boundaries.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): condensed architecture summary.

## Commercial licensing

A draft enterprise licensing framework is documented in [COMMERCIAL_LICENSE.md](COMMERCIAL_LICENSE.md).
For commercial terms, contact **licensing@k2payments.com**.

## Contributing

All external contributors must sign the Individual CLA before contributions can be merged.
See [docs/legal/ICLA.md](docs/legal/ICLA.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

## License

- Runtime code in this repository: **AGPL-3.0-only** ([LICENSE](LICENSE))
- `mx20022` core library crates consumed by this runtime: **Apache-2.0** (separate upstream project/repository)
