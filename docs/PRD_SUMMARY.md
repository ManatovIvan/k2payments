# PRD Summary

## Product
`mx20022-runtime` is a production-grade ISO 20022 payment processing runtime built on `mx20022`.

## Problem
There is no open-source runtime equivalent to jPOS for ISO 20022 message processing that is broadly usable in production with strong auditability and operational tooling.

## Solution
A Rust runtime with:
- 3-phase participant pipeline (`prepare`, `commit`, `abort`)
- Formal lifecycle state machine
- Auditable typed transaction context
- Pluggable inbound/outbound channels
- Durable transaction store and dead-letter support
- Correlation engine for request/response workflows
- Operations surface (`mxctl`, metrics, health/readiness/status)

## v0.1 Priorities
- Core transaction manager and state machine
- Context API with typed entries and audit trail
- HTTP + gRPC channels
- PostgreSQL + SQLite stores
- Baseline built-in participants for validation, logging, and status response
- Metrics and runtime health endpoints
- `mxctl` basics

## Engineering Principles
- Correctness and durability over feature velocity
- No undefined lifecycle transitions
- At-least-once processing with idempotency controls
- Structured observability from day one
- Compile-time extension model for v1 (no dynamic plugins)

## Major Constraints
- No runtime abstraction over Tokio
- Hot reload only for participant config (not topology)
- No multi-tenancy in v1/v2; use separate instances
- Public API errors are structured (`thiserror`), not opaque

## Repository Strategy
- Separate workspace/repo from `mx20022`
- Depends on `mx20022` as external library
- Independent release cadence from base toolkit
