# Architecture

Canonical architecture reference: [`/architecture.md`](../architecture.md).

## Overview

`mx20022-runtime` is a production‑grade ISO 20022 payment processing runtime built on the `mx20022` library. It receives inbound messages via channels, runs them through a participant pipeline under a three‑phase lifecycle, and delivers outbound messages with full auditability.

## High‑Level System

```
Inbound Channels ──▶ Transaction Manager ──▶ Outbound Channels
          │                 │                       │
          └──────► Context ─┼─► Participants         │
                            └─► Store / Correlation ┘
```

## Core Components

### Transaction Manager
- Owns the Context for a single transaction
- Drives the participant pipeline
- Enforces lifecycle state transitions
- Persists state transitions and participant results

### Context (Space)
- Typed, append‑only map
- Stores transaction metadata + parsed message + participant outputs
- Auditable mutation history

### Participants
- Implement `prepare / commit / abort`
- Vote in `prepare` and execute side effects in `commit`
- Can read/write Context

### Channels
- Inbound: receive messages and push into the Transaction Manager
- Outbound: deliver responses and ensure delivery confirmation
- Each channel is a feature‑flagged crate

### Store
- Durable persistence of transaction lifecycle and context audit
- Pluggable backends (PostgreSQL, SQLite, RocksDB)

### Correlation Engine
- Tracks request/response expectations
- Matches inbound responses to original requests
- Timeout handling and automated follow‑up actions

### Session Manager
- Manages long‑lived, stateful connections
- Heartbeats, reconnection, sequence numbers

## State Machine

States:
- RECEIVED → PREPARING → PREPARED → COMMITTING → COMMITTED
- RECEIVED → PREPARING → ABORTING → ABORTED
- POISON as terminal failure state

Invalid transitions are rejected and logged.

## Observability
- Prometheus metrics
- Structured logging (`tracing`)
- Health, readiness, and status endpoints

## Configuration
- Single TOML file per runtime instance
- Pipelines define channel bindings, participants, and concurrency limits
- Participant config supports hot‑reload

## Crate Responsibilities

- `mx20022-runtime-core`: Context, Participant, state machine, Transaction Manager
- `mx20022-channels/*`: Channel implementations
- `mx20022-participants`: Built‑in participants
- `mx20022-store/*`: Store backends
- `mx20022-correlation`: Correlation Engine
- `mx20022-session`: Session Manager
- `mx20022-config`: TOML parsing and reload logic
- `mx20022-metrics`: Prometheus registry
- `mx20022-admin`: Admin gRPC service (mxctl)
- `mx20022-crypto`: Field‑level encryption utilities
- `mx20022-runtime`: Binary and wiring
- `mx20022-cli`: `mxctl` operator CLI
