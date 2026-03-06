# Participant Guide

This guide describes how to configure and build participants for `mx20022-runtime`.

## Lifecycle model

Participants implement a three-phase contract:
- `prepare`: validate and vote. Return `Action::Prepared` or `Action::Aborted`.
- `commit`: perform side effects after all participants prepared.
- `abort`: rollback or compensate for side effects if a prior participant fails or votes abort.

Participants are called sequentially. The runtime calls `abort` on already-prepared
participants in reverse order when a prepare error or abort vote occurs.

## Configuration

Participants are configured per pipeline in the runtime TOML.

```toml
[[pipeline]]
name = "incoming"
channel_in = "http-in"
channel_out = "http-out"
message_types = ["pacs.008"]
max_concurrent = 250

[[pipeline.participants]]
name = "schema-validator"

[[pipeline.participants]]
name = "business-rule-validator"
config = { scheme = "fednow" }

[[pipeline.participants]]
name = "duplicate-checker"
config = { keys = ["message_id", "uetr"] }

[[pipeline.participants]]
name = "routing-engine"
config = { default_route = "core-out", rules = [
  { message_type = "pacs.008", currency = "USD", destination = "fednow-out" },
  { message_type = "pacs.008", currency = "EUR", destination = "sepa-out" }
] }

[[pipeline.participants]]
name = "rate-limiter"
config = { rate_per_second = 500, burst = 1000, scope = "source_channel" }

[[pipeline.participants]]
name = "circuit-breaker"
config = { failure_threshold = 5, open_ms = 30000 }

[[pipeline.participants]]
name = "message-logger"
config = { tag = "demo" }

[[pipeline.participants]]
name = "status-response-builder"
config = { auto_pacs002 = true }
```

## Built-in participants

- `message-logger`: emits a structured log entry for each transaction.
- `schema-validator`: validates inbound XML by parsing and running `mx20022` typed constraints (requires ISO 20022 namespace).
- `business-rule-validator`: applies baseline ISO 20022 rule checks (currency/amount guardrails).
- `business-rule-validator` supports an optional `scheme` config (`fednow`, `sepa`, `cbpr`).
- `fednow-rule-validator`: fixed FedNow scheme validation.
- `sepa-rule-validator`: fixed SEPA scheme validation.
- `cbpr-rule-validator`: fixed CBPR+ scheme validation.
- `duplicate-checker`: Store-backed duplicate detection by `keys` (`message_id`, `end_to_end_id`, `uetr`).
- `routing-engine`: context route selection (`routing.destination`) using top-down `rules`; first match wins.
- `rate-limiter`: token-bucket prepare gating (`rate_per_second`, `burst`, `scope`).
- `circuit-breaker`: opens after `failure_threshold` aborts; blocks prepare for `open_ms`.
- `status-response-builder`: builds a `pacs.002` response and stores it in the context.
- `acknowledgement-builder`: builds technical `head.001`-style acknowledgement payloads on commit.
- `error-response-builder`: builds rejection payloads on abort with reason mapping from context.

## Context usage

Use `Context` to read and write typed values. Use `put_with_writer` to label entries
with the participant name for auditability.

```rust
use async_trait::async_trait;
use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

pub struct Enricher;

#[async_trait]
impl Participant for Enricher {
    fn name(&self) -> &str {
        "enricher"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        let raw = ctx.raw_message();
        ctx.put_with_writer("enricher.raw_length", self.name(), raw.len());
        Ok(Action::Prepared)
    }
}
```

## Registering custom participants

1. Implement `Participant` in a crate that is part of the workspace.
2. Add the participant to the runtime registry in `crates/mx20022-runtime/src/app.rs`.
3. Reference the participant by name in your pipeline config.

## Testing participants

Use `Context` with a minimal `ContextMeta` and call `prepare/commit/abort` directly.

```rust
use std::time::SystemTime;
use mx20022_runtime_core::context::{Context, ContextMeta};

fn test_context(raw: &str) -> Context {
    Context::new(ContextMeta {
        transaction_id: "TX-1".to_string(),
        received_at: SystemTime::now(),
        pipeline: "test".to_string(),
        source_channel: "http".to_string(),
        message_type: "pacs.008".to_string(),
        raw_message: raw.to_string(),
    })
}
```

## Operational guidance

- Keep `prepare` side-effect free whenever possible.
- Make `commit` idempotent, or record idempotency keys in the Context.
- Use `ctx.get::<T>` consistently with the type you stored to avoid type mismatch errors.

## Correlation hooks

The runtime registers correlation expectations and matches responses when
participants write the following keys to Context (Committed transactions only):

- `correlation.expectation` → `mx20022_store::Expectation`
- `correlation.lookup_key` → `mx20022_correlation::CorrelationLookupKey`
