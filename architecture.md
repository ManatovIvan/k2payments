# architecture.md

This document explains how the runtime works today, where each concern lives, and what tradeoffs are currently in place.

## 1. System Overview

`mx20022-runtime` processes inbound ISO 20022 messages through configurable participant pipelines.

At a high level:

1. Channel adapter receives inbound payload.
2. Runtime resolves target pipeline.
3. Transaction context is created and persisted.
4. Participants execute via transaction manager lifecycle.
5. Outcome and audit trail are persisted.
6. Optional outbound channel sends generated response.
7. Correlation engine registers/matches expectations.

## 2. Runtime Topology

```text
Inbound Channel(s)
   -> Runtime Engine (dispatch/concurrency)
      -> RuntimeApp::process
         -> Store.begin_transaction
         -> TransactionManager (prepare/commit/abort)
         -> Store.complete_transaction + context audit
         -> Outbound channel (optional)
         -> Correlation register/match

Admin HTTP/gRPC (optional)
   -> status/ready/reload/tx lookup
```

Main wiring entry points:

- `crates/mx20022-runtime/src/main.rs`
- `crates/mx20022-runtime/src/engine.rs`
- `crates/mx20022-runtime/src/app.rs`

## 3. Core Domain Flow

## 3.1 Transaction lifecycle

Implemented in `mx20022-runtime-core`.

Canonical states:

- `RECEIVED`
- `PREPARING`
- `PREPARED`
- `COMMITTING`
- `COMMITTED`
- `ABORTING`
- `ABORTED`
- `POISON`

Invalid transitions are rejected by the state machine.

## 3.2 Transaction manager contract

Participants implement prepare/commit/abort semantics:

- `prepare`: vote (continue or abort)
- `commit`: side effects when all prepare votes succeed
- `abort`: rollback/compensation path

This gives deterministic pipeline behavior and explicit failure surfaces.

## 3.3 Request/record model

Runtime request handling now wraps a store `TransactionRecord` directly (single canonical schema), reducing drift and copy overhead in the process path.

## 4. Crate Boundaries

## Runtime orchestration

- `mx20022-runtime`
  - Config loading
  - Inbound channel startup
  - Pipeline dispatch
  - Outbound wiring
  - Startup recovery
  - Participant config reload loop

## Transaction core

- `mx20022-runtime-core`
  - Typed context
  - Lifecycle state machine
  - Participant trait
  - Transaction manager

## Channels

- `mx20022-channels` (traits/auth)
- `mx20022-channel-http`
- `mx20022-channel-grpc`
- `mx20022-channel-tcp`
- `mx20022-channel-kafka`
- `mx20022-channel-nats`
- `mx20022-channel-amqp`
- `mx20022-channel-file`

## Persistence

- `mx20022-store` (trait + shared types)
- `mx20022-store-sqlite`
- `mx20022-store-postgres`
- `mx20022-store-rocksdb`

## Other subsystems

- `mx20022-config`: TOML parse/validation and runtime security checks
- `mx20022-admin`: admin auth, middleware, HTTP/gRPC surfaces
- `mx20022-correlation`: expectation registry/matching/timeout worker
- `mx20022-metrics`: Prometheus metric definitions
- `mx20022-cli`: operator/admin CLI (`mxctl`)

## 5. Data and Persistence Model

Store abstraction persists:

- Transaction records (`tx_id`, pipeline, source channel, message type, raw payload, state)
- Context audit entries (who wrote what, when)
- Correlation expectations
- Dead letters

Backends differ by implementation detail, but behavior is normalized at trait level.

## 6. Concurrency and Backpressure

- Inbound engine uses per-pipeline bounded channels.
- Pipeline execution uses a semaphore bound by `max_concurrent`.
- Pipeline timeout is enforced when configured (`timeout_ms`).
- Timeout outcome is forced to `POISON` with persisted context/audit.

## 7. Security Model (Current)

## Implemented controls

- Admin auth modes: disabled / legacy bearer / JWT HS256.
- Channel auth (HTTP/gRPC): disabled / static bearer / JWT HS256.
- Constant-time bearer/token comparisons.
- Secret handling with `SecretString` in critical auth config paths.
- CORS allowlists and security headers on admin/channel HTTP surfaces.
- Config-time secure channel enforcement (`runtime.enforce_secure_channels`).

## Important caveats

- TLS support is not fully uniform across every transport yet.
- Some secure-channel scenarios still rely on explicit config discipline.

## 8. Correlation Semantics

Current behavior:

- Fast in-memory index for local matching.
- Fallback to store scan for cross-instance correctness if in-memory miss occurs.
- Background timeout scanner marks timed-out expectations.

Tradeoff:

- This is correctness-oriented but not a fully distributed low-latency index.
- True shared/distributed indexing remains future architecture work.

## 9. Reliability and Failure Handling

- Startup recovery replays non-terminal transactions from store (bounded by config).
- Outbound send failure can mark committed report as poison for visibility/fail-safe handling.
- Dead letter facilities support list/show/replay via CLI.

## 10. Observability

- Structured logs via `tracing`.
- Metrics endpoint (Prometheus format).
- Admin status includes runtime/store/pipeline/channel and operational depth fields.

## 11. Extensibility Patterns

## Add a participant

- Implement in `mx20022-participants`.
- Register constructor in runtime participant registry.
- Add unit tests and pipeline integration coverage.

## Add a channel

- Implement `InboundChannel`/`OutboundChannel` in channel crate.
- Wire parsing/config in runtime engine/app.
- Add adapter tests for inbound and outbound behavior.

## Add a store backend

- Implement `Store` trait.
- Ensure behavior parity for query, update, dead-letter, correlation expectation APIs.

## 12. Deployment Shapes

Supported deployment modes:

- Pipeline engine only
- Admin plane only
- Engine + admin HTTP
- Engine + admin gRPC
- Engine + both admin surfaces

Runtime flags control service mix (`--serve-admin`, `--serve-admin-grpc`, `--no-pipelines`).

## 13. Non-Goals / Ongoing Work

Areas intentionally still evolving:

- Fully distributed correlation index
- Uniform end-to-end TLS controls across all transport adapters
- Stronger Kafka commit semantics tied to durable business completion
- Further payload ownership optimization across all layers
