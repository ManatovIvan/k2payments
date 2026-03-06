# mx20022-runtime

Payment processing runtime for ISO 20022 messages, built on top of `mx20022`.

## Current State
This repository now contains the v0.1 foundation scaffold plus concrete core modules:
- Workspace and crate layout aligned with PRD crate boundaries
- `mx20022-runtime-core` with lifecycle state machine, typed context, and transaction manager flow
- `mx20022-config` with TOML parsing and topology validation
- `mx20022-runtime` executable modes:
  - pipeline engine
  - admin host
  - both concurrently
- `mx20022-cli` operations:
  - `config validate`
  - `db migrate|rollback|seed`
  - `tx show`

## Docs
- [PRD summary](docs/PRD_SUMMARY.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Implementation plan](docs/PLAN.md)
- [Operations guide](docs/OPERATIONS.md)
- [Participant guide](docs/PARTICIPANT_GUIDE.md)

## Workspace layout
See `docs/ARCHITECTURE.md` for module responsibilities.

## Commands

```bash
cargo build -p mx20022-runtime
cargo run -p mx20022-runtime -- --config docs/examples/basic.toml
cargo run -p mx20022-runtime -- --config docs/examples/basic.toml --serve-admin
cargo run -p mx20022-runtime -- --config docs/examples/basic.toml --serve-admin --no-pipelines
cargo run -p mx20022-cli -- config validate docs/examples/basic.toml
cargo run -p mx20022-cli -- db migrate docs/examples/basic.toml
cargo run -p mx20022-cli -- db seed docs/examples/basic.toml
```

## Note about this environment
`cargo check` and `cargo build` require network access to `crates.io` unless dependencies are already cached locally.

## License
Apache-2.0
