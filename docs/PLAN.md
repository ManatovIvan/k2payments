# mx20022-runtime Implementation Plan

## Scope
This plan tracks delivery of `mx20022-runtime` from scaffold to the v0.1 goals in the PRD.

## Current Status
- Workspace and crate layout scaffolded.
- Core lifecycle primitives implemented in `mx20022-runtime-core`.
- Config parsing and validation skeleton implemented in `mx20022-config`.
- Config validation coverage expanded for runtime auth, pipeline references, recovery constraints, and parse/shape regressions (15+ config-focused tests).
- Runtime bootstrap binary loads TOML config and validates topology.

## Milestone 1: Core Engine (v0.1-a)
1. Finalize `Context` API
- Typed get/put error model
- Append-only mutation audit metadata
- State transition guardrails
2. Finalize transaction execution
- Prepare/commit/abort orchestration
- Participant timing and error capture
- Outcome mapping (Committed, Aborted, Poison)
3. Add deterministic tests
- Happy path commit
- Abort vote path
- Poison path on commit or abort failure

## Milestone 2: Runtime Wiring (v0.1-b)
1. Participant registry and pipeline builder
- Register built-in participants by name
- Resolve participant config blocks from TOML
- Construct ordered participant chains per pipeline
2. Transaction manager runtime loop
- Inbound message ingestion channel
- `max_concurrent` task limiter
- Pipeline dispatch by message type
3. State persistence integration
- Persist begin/update/complete lifecycle to Store trait
- Persist context audit entries

## Milestone 3: v0.1 Features (v0.1-c)
1. Channels
- HTTP inbound/outbound implementation
- gRPC inbound/outbound implementation
2. Store backends
- SQLite implementation for local/dev
- PostgreSQL implementation for production
3. Built-in participants
- `schema-validator`
- `business-rule-validator`
- `fednow-rule-validator`
- `message-logger`
- `status-response-builder`

## Milestone 4: Operations Surface (v0.1-d)
1. Endpoints
- `/health`
- `/ready`
- `/status`
2. Metrics
- Transaction throughput and duration
- Participant duration and error counters
- Channel and store health metrics
3. CLI (`mxctl`)
- `status`
- `tx show`
- `tx search`

## Milestone 5: Release Hardening (v0.1-rc)
1. Integration tests
- Pipeline harness with realistic fixtures
- End-to-end path with HTTP/gRPC + SQLite/PostgreSQL
2. Benchmarks
- Pipeline traversal baseline
- Context and store operation benchmarks
3. Delivery artifacts
- Docker image and startup docs
- Operator/developer docs complete

## Exit Criteria for v0.1
- End-to-end `pacs.008` over HTTP through configurable participant pipeline.
- Validation and status response generation to `pacs.002`.
- Full transaction audit persisted in Store.
- Health/readiness/metrics available.
- Deterministic integration tests passing in CI.
