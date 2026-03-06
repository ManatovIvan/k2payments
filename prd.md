# mx20022-runtime — Payment Processing Runtime

## Product Requirements Document v1.0

**Project:** mx20022-runtime
**Parent Project:** [mx20022](https://github.com/socrates8300/mx20022) — ISO 20022 toolkit for Rust
**Date:** March 2026
**Status:** Approved for development
**License:** Apache 2.0 (consistent with mx20022)

---

## 1. Vision

`mx20022-runtime` is an open-source, production-grade payment message processing runtime built on top of the `mx20022` library. It is the transaction processing engine that turns a parsing library into a payment platform.

If `mx20022` is the ISO 20022 equivalent of jPOS's `ISOMsg` and `ISOPackager`, then `mx20022-runtime` is the equivalent of jPOS's `TransactionManager`, `QServer`, `QMux`, and Space — the runtime that receives financial messages, pushes them through a configurable processing pipeline, and delivers them onward with the reliability, auditability, and correctness that financial infrastructure demands.

This is not an MVP. We are building the open-source standard for ISO 20022 message processing — the infrastructure that banks, payment processors, and fintechs will run in production for years. Every architectural decision prioritizes correctness, durability, and long-term maintainability over speed of delivery.

---

## 2. Foundation: What mx20022 Already Provides

Phase 2 builds on a complete Phase 1 library. The following capabilities are already shipping in [mx20022](https://github.com/socrates8300/mx20022) and are direct dependencies of the runtime:

**Message Models (mx20022-model)**
13 strongly-typed message schemas generated from official XSD: pacs.008, pacs.002, pacs.004, pacs.009, pacs.028, pain.001, pain.002, pain.013, camt.053, camt.054, camt.056, camt.029, head.001. Every element, attribute, and enum variant is a named Rust type with serde Serialize/Deserialize. Builder pattern on all struct types with required-field validation. Feature-gated by message family (pacs, pain, camt, head).

**Parsing & Serialization (mx20022-parse)**
XML round-trip via quick-xml + serde. Namespace-based message type detection via `envelope::detect_message_type()`. Deserialization to strongly-typed structs and serialization back to conformant XML.

**Validation (mx20022-validate)**
Three-layer validation: XSD constraint validation (pattern, length, range via `Validatable` trait), business rule validation (IBAN, BIC, LEI, currency, amount format), and scheme-level rules for FedNow, SEPA, and CBPR+. Structured error output with rule ID, XPath, severity, and human-readable message.

**Translation (mx20022-translate)**
SWIFT FIN MT parser for MT103, MT202, MT940. Bidirectional translation: MT103 ↔ pacs.008, MT202 ↔ pacs.009, MT940 ↔ camt.053.

**Code Generation (mx20022-codegen)**
XSD → intermediate representation → Rust source. The engine room that keeps the library current with ISO 20022 schema updates.

**CLI (mx20022-cli)**
`inspect`, `validate` (with `--scheme` flag), `translate` (bidirectional), and `codegen` commands.

The runtime treats all of the above as stable dependencies. It does not modify, fork, or re-implement any Phase 1 functionality. New message types, validation rules, or translation mappings added to mx20022 are automatically available to the runtime.

---

## 3. Market Context & Competitive Position

### The gap

There is no open-source, production-grade ISO 20022 message processing runtime. The current options:

- **Commercial middleware** (Volante, Finastra, Bottomline, IBM Financial Transaction Manager) — expensive, vendor-locked, often legacy Java/mainframe architectures. Typical licensing starts at six figures annually.
- **jPOS** — battle-tested over 25 years, but ISO 8583 only. Its architecture is the closest spiritual ancestor. Java/GPL licensing has limited commercial adoption.
- **Moov.io** — Go-based, focused on ACH/wire in the US context. Not a general-purpose MX processing engine.
- **Custom internal builds** — every bank and PSP builds bespoke message routing infrastructure. Massive duplication of effort across the industry.

### Why now

The SWIFT MT→MX migration is driving a global re-platforming wave. FedNow launched in 2023 and adoption is accelerating among smaller FIs and fintechs who need affordable infrastructure. SEPA Instant is moving toward mandatory coverage across the EU. SWIFT CBPR+ is mid-migration. PIX, UPI, and other domestic instant payment schemes all speak ISO 20022. Every participant on these networks needs infrastructure to receive, validate, route, transform, and respond to MX messages.

### Position

`mx20022-runtime` is the open-source alternative to commercial payment middleware — purpose-built for ISO 20022, written in Rust for performance and reliability, and designed by people who have built payment systems professionally.

---

## 4. Architecture

### 4.1 System Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                        mx20022-runtime                           │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌───────────┐    ┌───────────────────────────┐   ┌───────────┐ │
│  │  Inbound  │───▶│    Transaction Manager    │──▶│ Outbound  │ │
│  │  Channels │    │                           │   │ Channels  │ │
│  └───────────┘    │  ┌─────────────────────┐  │   └───────────┘ │
│                   │  │     Pipeline         │  │                 │
│  • HTTP/REST      │  │                     │  │   • HTTP/REST    │
│  • gRPC           │  │  ┌───────────────┐  │  │   • gRPC         │
│  • Kafka          │  │  │  Participant  │  │  │   • Kafka         │
│  • AMQP           │  │  │  Participant  │  │  │   • AMQP          │
│  • TCP            │  │  │  Participant  │  │  │   • TCP            │
│  • File watch     │  │  │  Participant  │  │  │   • File write     │
│  • NATS           │  │  │     ...       │  │  │   • NATS           │
│                   │  │  └───────────────┘  │  │                 │
│                   │  └─────────────────────┘  │                 │
│                   └───────────────────────────┘                 │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                     Core Services                          │ │
│  │                                                            │ │
│  │  ┌──────────┐ ┌─────────────┐ ┌───────────┐ ┌──────────┐ │ │
│  │  │ Context  │ │ Correlation │ │   Store   │ │  Timer   │ │ │
│  │  │  Space   │ │   Engine    │ │  (Audit)  │ │ Service  │ │ │
│  │  └──────────┘ └─────────────┘ └───────────┘ └──────────┘ │ │
│  │                                                            │ │
│  │  ┌──────────┐ ┌─────────────┐ ┌───────────┐ ┌──────────┐ │ │
│  │  │ Session  │ │   Crypto    │ │  Metrics  │ │  Config  │ │ │
│  │  │ Manager  │ │  Service    │ │           │ │  Engine  │ │ │
│  │  └──────────┘ └─────────────┘ └───────────┘ └──────────┘ │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
├──────────────────────────────────────────────────────────────────┤
│  mx20022 (Phase 1 library)                                       │
│  model • parse • validate • translate • codegen                  │
└──────────────────────────────────────────────────────────────────┘
```

### 4.2 Transaction Manager

The heart of the system. Receives an inbound message, wraps it in a Context, and drives it through an ordered pipeline of Participants using a three-phase lifecycle: **prepare**, **commit**, **abort**.

The three-phase model is inherited from jPOS because it has been proven over two decades of real payment processing. Each participant votes during `prepare` on whether the transaction should proceed. If all participants vote `Prepared`, the Transaction Manager calls `commit` on each in forward order. If any participant votes `Aborted`, the Transaction Manager calls `abort` on all previously-prepared participants in reverse order. This provides a consistent model for side-effect management without requiring distributed transactions.

```rust
use mx20022_runtime::prelude::*;

#[async_trait]
impl Participant for SanctionsScreener {
    fn name(&self) -> &str { "sanctions-screener" }

    async fn prepare(&self, ctx: &mut Context) -> Action {
        let msg: &Pacs008 = ctx.message()?;
        let debtor = &msg.fi_to_fi_cstmr_cdt_trf.grp_hdr./* ... */;

        match self.screening_service.check(debtor, creditor).await {
            ScreenResult::Clear => Action::Prepared,
            ScreenResult::Hit(details) => {
                ctx.put("screening.result", details);
                ctx.put("screening.hit_time", Utc::now());
                Action::Aborted
            }
        }
    }

    async fn commit(&self, ctx: &mut Context) -> Result<()> {
        self.audit.log_cleared(ctx.transaction_id()).await
    }

    async fn abort(&self, ctx: &mut Context) -> Result<()> {
        let details: &ScreeningDetails = ctx.get("screening.result")?;
        self.compliance_queue.notify(ctx.transaction_id(), details).await
    }
}
```

**Lifecycle state machine:**

```
                    ┌─────────┐
          ┌────────▶│ RECEIVED │
          │         └────┬────┘
          │              │
          │              ▼
          │         ┌─────────┐
          │    ┌───▶│PREPARING│◀──┐
          │    │    └────┬────┘   │
          │    │         │        │
          │    │    ┌────▼────┐   │
          │    │    │PREPARED │   │ (next participant)
          │    │    └────┬────┘   │
          │    │         ├────────┘
          │    │         │ (all prepared)
          │    │         ▼
          │    │    ┌──────────┐
          │    │    │COMMITTING│
          │    │    └────┬─────┘
          │    │         │
          │    │         ▼
          │    │    ┌──────────┐
          │    │    │COMMITTED │ ← terminal success
          │    │    └──────────┘
          │    │
          │    │    (participant votes abort)
          │    │         │
          │    │         ▼
          │    │    ┌──────────┐
          │    └───▶│ ABORTING │
          │         └────┬─────┘
          │              │
          │              ▼
          │         ┌──────────┐
          │         │ ABORTED  │ ← terminal failure
          │         └──────────┘
          │
          │         ┌──────────┐
          └────────▶│  POISON  │ ← terminal, after N failures
                    └──────────┘
```

Every state transition is persisted to the Store and emitted as a metric. The state machine is formalized — transitions are enforced at the type level where possible and validated at runtime otherwise. This is not optional complexity; in payment processing, an undefined state is a compliance violation.

**Concurrency model:** The Transaction Manager runs as a Tokio task pool. Each inbound message spawns a task that owns its Context and drives the pipeline. There is no shared mutable state between transactions. The `max_concurrent` configuration parameter bounds the task pool to prevent resource exhaustion. Backpressure propagates to inbound channels — if the pool is full, channels stop accepting new messages until capacity is available.

### 4.3 Context (Space)

The Context is the shared state object that flows through the pipeline. It is a typed, append-only, auditable map inspired by jPOS's Space/Context.

```rust
// Every context has immutable metadata
let tx_id: &TxId = ctx.transaction_id();
let received_at: DateTime<Utc> = ctx.received_at();
let pipeline: &str = ctx.pipeline_name();
let source_channel: &str = ctx.source_channel();

// The parsed message is always available
let msg: &Document = ctx.message()?;
let raw_xml: &str = ctx.raw_message();
let msg_type: &MessageId = ctx.message_type();

// Participants read and write typed entries
ctx.put("routing.destination", Destination::FedNow);
ctx.put("enrichment.debtor_account_status", AccountStatus::Active);

let dest: &Destination = ctx.get("routing.destination")?;

// Elapsed time is always available
let elapsed: Duration = ctx.elapsed();
```

**Design invariants:**

1. **Single owner.** The Context is not `Clone` and not `Send` across transaction boundaries. The Transaction Manager owns it and passes `&mut Context` to participants sequentially. This prevents accidental forking of transaction state.

2. **Append-only for audit.** Values can be shadowed (a participant can overwrite a key) but previous values are retained in an internal audit log. When the transaction completes, the full mutation history is persisted to the Store. This means you can always answer "what did participant X see when it made its decision?"

3. **Typed access.** `ctx.get::<T>(key)` returns `Result<&T>` using `Any` downcasting. Type mismatches are errors, not panics. Keys are `&str` with a namespacing convention: `participant_name.field_name`.

4. **Timestamps on every mutation.** Every `put` records the wall-clock time and the name of the participant that wrote it. This is metadata, not optional.

### 4.4 Channels

Channels are the I/O boundary. They abstract protocol-specific details and present a uniform interface to the Transaction Manager.

```rust
#[async_trait]
pub trait InboundChannel: Send + Sync + 'static {
    /// Unique name for this channel instance (from config).
    fn name(&self) -> &str;

    /// Start listening. Push received messages into the sender.
    /// The channel is responsible for its own connection lifecycle,
    /// reconnection, and error handling.
    async fn run(&self, sender: mpsc::Sender<InboundMessage>) -> Result<()>;

    /// Graceful shutdown. Stop accepting new messages.
    /// In-flight messages should be allowed to complete.
    async fn shutdown(&self) -> Result<()>;

    /// Health check. Returns Ok if the channel is operational.
    async fn health(&self) -> Result<ChannelHealth>;

    /// Pause accepting new messages without disconnecting.
    async fn pause(&self) -> Result<()>;

    /// Resume after pause.
    async fn resume(&self) -> Result<()>;
}

#[async_trait]
pub trait OutboundChannel: Send + Sync + 'static {
    fn name(&self) -> &str;

    /// Send a message. Returns when delivery is confirmed
    /// by the downstream system (not just buffered).
    async fn send(&self, msg: OutboundMessage) -> Result<DeliveryReceipt>;

    async fn shutdown(&self) -> Result<()>;
    async fn health(&self) -> Result<ChannelHealth>;
}
```

**Channel implementations and delivery schedule:**

| Channel | Version | Priority | Notes |
|---------|---------|----------|-------|
| HTTP/REST | v0.1 | Critical | JSON and XML payloads. Most common for fintech integrations. Supports both server (inbound) and client (outbound) modes. |
| gRPC | v0.1 | Critical | High-performance inter-service communication. Protobuf service definitions for the runtime's own message protocol plus pass-through XML payloads. |
| Kafka | v0.2 | High | Event-driven architectures. Consumer groups for inbound, producer for outbound. Exactly-once semantics via idempotent producer + transactional commits. |
| AMQP 0.9.1 (RabbitMQ) | v0.2 | High | Common in bank middleware stacks. Publisher confirms for reliable delivery. |
| File watch/write | v0.2 | High | Batch processing. Watch a directory for new files, process each file as a message. Write output files with configurable naming and rotation. |
| NATS | v0.3 | Medium | Lightweight pub/sub, increasingly popular in cloud-native payment architectures. JetStream for persistence. |
| TCP raw | v0.3 | Medium | Custom framing for legacy integrations. Length-prefixed and delimiter-based framing options. |
| WebSocket | v0.4 | Medium | Real-time notification delivery, dashboard integration. |

Each channel implementation is a separate crate, compiled only when its feature flag is enabled. A minimal deployment (HTTP + gRPC) has a small dependency footprint.

### 4.5 Correlation Engine

Payment messages are conversational. A `pacs.008` credit transfer expects a `pacs.002` status report in response. A `camt.056` cancellation request expects a `camt.029` resolution of investigation. The Correlation Engine tracks these request/response pairs as first-class concepts.

```rust
// When sending a pacs.008, register a correlation expectation
let expectation = Expectation::builder()
    .correlation_key(
        CorrelationKey::new()
            .original_message_id(&pacs008.grp_hdr.msg_id.0)
            .expected_message_type(MessageType::Pacs002)
    )
    .timeout(Duration::from_secs(30))
    .on_timeout(TimeoutAction::Escalate {
        notify: vec!["ops-team".into()],
        generate_status_inquiry: true, // auto-send pacs.028
    })
    .on_match(MatchAction::ResumeTransaction {
        original_tx_id: ctx.transaction_id().clone(),
    })
    .build()?;

correlation.register(expectation).await?;
```

When a matching response arrives on any inbound channel, the Correlation Engine:

1. Matches it to the pending expectation using configurable key fields (message ID, end-to-end ID, transaction ID, UETR).
2. Links the response Context to the original transaction Context in the Store.
3. Executes the configured `on_match` action (resume a suspended transaction, invoke a callback participant, or simply log the match).

Timeouts are checked by a dedicated timer wheel (not per-expectation `tokio::time::sleep` — that doesn't scale). When a timeout fires:

1. The expectation is moved to a `TIMED_OUT` state.
2. The configured `on_timeout` action executes. This can include automatically generating a status inquiry message (pacs.028), escalating to an operations queue, or triggering a retry of the original message.
3. The timeout event is persisted to the Store.

The Correlation Engine maintains an in-memory index for fast lookups (sub-microsecond matching) backed by the Store for durability. On restart, pending expectations are reloaded from the Store.

### 4.6 Session Manager

Manages stateful connections where multiple messages flow over a single long-lived session. This is relevant for:

- Persistent TCP connections to payment networks
- SWIFT Alliance or MQ sessions with sequence numbering
- gRPC bidirectional streaming connections
- Any transport where connection setup is expensive or requires authentication

The Session Manager handles:

- **Lifecycle:** connect → authenticate → exchange → heartbeat → disconnect
- **Sequence numbering:** Track sent/received sequence numbers. Detect gaps. Handle resync.
- **Reconnection:** Configurable backoff with jitter. Resume from last known sequence on reconnect.
- **Heartbeat:** Configurable keepalive intervals. Detect dead connections before the OS does.
- **Graceful draining:** Stop sending new messages while allowing in-flight messages to complete.

Sessions expose their state to participants via the Context, so a participant can inspect whether the outbound session is healthy before committing to send.

### 4.7 Store (Audit & Persistence)

Every transaction is persisted. This is non-negotiable in payment processing — if it isn't recorded, it didn't happen.

```rust
#[async_trait]
pub trait Store: Send + Sync + 'static {
    // Transaction lifecycle
    async fn begin_transaction(&self, record: &TransactionRecord) -> Result<()>;
    async fn update_transaction(&self, tx_id: &TxId, update: TransactionUpdate) -> Result<()>;
    async fn complete_transaction(&self, tx_id: &TxId, outcome: Outcome) -> Result<()>;

    // Context audit trail
    async fn append_context_entry(&self, tx_id: &TxId, entry: ContextEntry) -> Result<()>;

    // Queries
    async fn find_by_id(&self, tx_id: &TxId) -> Result<Option<TransactionRecord>>;
    async fn find_by_message_id(&self, msg_id: &str) -> Result<Vec<TransactionRecord>>;
    async fn find_by_end_to_end_id(&self, e2e_id: &str) -> Result<Vec<TransactionRecord>>;
    async fn find_by_uetr(&self, uetr: &str) -> Result<Vec<TransactionRecord>>;
    async fn query(&self, filter: StoreQuery) -> Result<QueryResult>;

    // Correlation state
    async fn save_expectation(&self, exp: &Expectation) -> Result<()>;
    async fn load_pending_expectations(&self) -> Result<Vec<Expectation>>;
    async fn update_expectation(&self, id: &ExpectationId, update: ExpUpdate) -> Result<()>;

    // Dead letter
    async fn save_dead_letter(&self, letter: &DeadLetter) -> Result<()>;
    async fn list_dead_letters(&self, filter: DeadLetterQuery) -> Result<Vec<DeadLetter>>;
    async fn replay_dead_letter(&self, id: &DeadLetterId) -> Result<()>;

    // Housekeeping
    async fn health(&self) -> Result<StoreHealth>;
    async fn compact(&self) -> Result<()>;
}
```

**What gets stored per transaction:**

| Field | Description |
|-------|-------------|
| `tx_id` | Runtime-generated unique transaction identifier |
| `pipeline` | Which pipeline processed this transaction |
| `source_channel` | Inbound channel name |
| `message_type` | ISO 20022 message type (e.g., pacs.008.001.13) |
| `raw_message` | Original XML as received |
| `parsed_key_fields` | Indexed fields: message_id, e2e_id, uetr, debtor_bic, creditor_bic, amount, currency |
| `state` | Current lifecycle state (received → preparing → committed/aborted/poison) |
| `context_mutations` | Ordered log of every `ctx.put()` with timestamp and participant name |
| `participant_results` | Per-participant: action returned, duration, errors |
| `outbound_message` | Final XML sent (if any) |
| `destination_channel` | Outbound channel name |
| `delivery_receipt` | Confirmation from outbound channel |
| `correlation_id` | Link to correlated transactions (e.g., original pacs.008 ↔ response pacs.002) |
| `received_at` | Wall-clock time of message receipt |
| `completed_at` | Wall-clock time of final state |
| `total_duration` | End-to-end processing time |

**Store backends and delivery schedule:**

| Backend | Version | Use Case |
|---------|---------|----------|
| PostgreSQL | v0.1 | Production deployments. JSONB for context mutations. Full-text search on message content. Partitioning by date for retention management. |
| SQLite | v0.1 | Development, testing, small single-node deployments. WAL mode for concurrent reads. |
| RocksDB | v0.3 | Embedded high-write-throughput. Useful when external database is undesirable (edge deployments, air-gapped environments). |

The Store trait is public — users can implement it for any backend (Cassandra, DynamoDB, FoundationDB, etc.).

**Retention and lifecycle:** The Store supports configurable retention policies. Transaction records can be archived (moved to cold storage) or purged after a configurable period. Regulatory requirements vary by jurisdiction — the default is retain-forever with optional TTL. The runtime never deletes transaction records without explicit operator action or policy configuration.

---

## 5. Pipeline Configuration

Pipelines are defined in TOML. This is the operator-facing interface — the configuration that defines what the runtime does.

```toml
# ─────────────────────────────────────────────────
# Runtime identity
# ─────────────────────────────────────────────────
[runtime]
name = "fednow-gateway"
instance_id = "gw-east-01"
log_level = "info"
metrics_bind = "0.0.0.0:9100"
admin_bind = "127.0.0.1:9090"

# ─────────────────────────────────────────────────
# Store
# ─────────────────────────────────────────────────
[store]
backend = "postgres"
url = "postgresql://mx:secret@db.internal:5432/mxruntime"
pool_size = 20
retention_days = 2555  # 7 years, typical regulatory minimum

# ─────────────────────────────────────────────────
# Channels
# ─────────────────────────────────────────────────
[channels.fednow-inbound]
type = "http"
mode = "server"
bind = "0.0.0.0:8443"
tls.cert = "/etc/mx/certs/server.pem"
tls.key = "/etc/mx/certs/server-key.pem"
tls.ca = "/etc/mx/certs/fednow-ca.pem"
tls.require_client_cert = true
content_type = "application/xml"
max_request_size = "10MB"

[channels.core-banking]
type = "grpc"
mode = "client"
endpoint = "https://core-banking.internal:9090"
tls.ca = "/etc/mx/certs/internal-ca.pem"
timeout_ms = 3000
retry.policy = "exponential"
retry.max_attempts = 3
retry.initial_backoff_ms = 100
retry.max_backoff_ms = 5000

[channels.audit-stream]
type = "kafka"
mode = "producer"
brokers = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]
topic = "mx.audit.transactions"
acks = "all"
compression = "lz4"
idempotent = true

[channels.swift-inbound]
type = "amqp"
mode = "consumer"
url = "amqps://swift-bridge.internal:5671"
queue = "swift.mt.inbound"
prefetch = 100
tls.ca = "/etc/mx/certs/swift-ca.pem"

[channels.batch-input]
type = "file"
mode = "watch"
directory = "/data/inbound"
pattern = "*.xml"
poll_interval_ms = 1000
move_processed_to = "/data/processed"
move_failed_to = "/data/failed"

# ─────────────────────────────────────────────────
# Pipelines
# ─────────────────────────────────────────────────
[[pipeline]]
name = "fednow-credit-transfer"
description = "Receive FedNow pacs.008, validate, screen, route to core banking"
channel_in = "fednow-inbound"
channel_out = "core-banking"
message_types = ["pacs.008", "pacs.004"]  # filter by message type
max_concurrent = 1000
timeout_ms = 5000

participants = [
    { name = "message-logger",       config = { mask_fields = ["DbtrAcct", "CdtrAcct"] } },
    { name = "schema-validator",     config = {} },
    { name = "fednow-rule-validator", config = {} },
    { name = "duplicate-checker",    config = { window_minutes = 1440, key_fields = ["MsgId", "EndToEndId"] } },
    { name = "sanctions-screener",   config = { provider = "internal", threshold = 0.85 } },
    { name = "account-lookup",       config = { source = "core-banking" } },
    { name = "fraud-rules",          config = { ruleset = "fednow-inbound-v1" } },
    { name = "routing-engine",       config = { default_destination = "core-banking" } },
    { name = "response-builder",     config = { auto_pacs002 = true } },
    { name = "audit-publisher",      config = { channel = "audit-stream" } },
    { name = "message-logger",       config = { mask_fields = ["DbtrAcct", "CdtrAcct"], tag = "outbound" } },
]

[[pipeline]]
name = "swift-mt-bridge"
description = "Receive SWIFT MT messages, translate to MX, forward"
channel_in = "swift-inbound"
channel_out = "core-banking"
message_types = ["mt103", "mt202", "mt940"]
max_concurrent = 500
timeout_ms = 10000

participants = [
    { name = "message-logger",       config = { tag = "mt-inbound" } },
    { name = "mt-parser",            config = {} },
    { name = "mt-to-mx-translator",  config = {} },
    { name = "schema-validator",     config = {} },
    { name = "mx-enricher",          config = { lookup_bic_names = true } },
    { name = "routing-engine",       config = {} },
    { name = "message-logger",       config = { tag = "mx-outbound" } },
]

[[pipeline]]
name = "batch-processor"
description = "Process bulk payment files dropped to filesystem"
channel_in = "batch-input"
channel_out = "core-banking"
max_concurrent = 50
timeout_ms = 60000

participants = [
    { name = "batch-splitter",       config = { split_on = "CdtTrfTxInf" } },
    { name = "schema-validator",     config = {} },
    { name = "business-rule-validator", config = {} },
    { name = "duplicate-checker",    config = { window_minutes = 4320 } },
    { name = "routing-engine",       config = {} },
    { name = "batch-aggregator",     config = { collect_responses = true } },
]
```

**Configuration principles:**

1. **One file, one runtime.** A single TOML file fully describes a runtime instance. No external configuration services, no distributed config stores. If you need different configs for different environments, use TOML's native features or a templating layer outside the runtime.

2. **Participants are referenced by name.** The runtime maintains a registry of available participants (both built-in and user-registered). The configuration references them by name with an inline config table. This is analogous to jPOS's deploy descriptors but more concise.

3. **Hot-reload scope.** Participant configuration (thresholds, feature flags, rule parameters) can be hot-reloaded without restart. Pipeline topology changes (adding/removing participants, changing channel bindings) require a restart. Channel configuration changes require a restart. This is the right trade-off: participant tuning is a frequent operational need, topology changes are planned events.

---

## 6. Built-In Participants

The runtime ships with a comprehensive library of reusable participants. Users compose these with custom participants to build their specific processing flows.

### 6.1 Message Handling

| Participant | Description |
|------------|-------------|
| `schema-validator` | Validates inbound MX messages using `mx20022-validate` XSD constraint validation and `Validatable` trait. Structured error output with XPath locations. |
| `business-rule-validator` | Runs business rule validation: IBAN check digits, BIC format, LEI, currency codes, amount formats. Delegates to `mx20022-validate`. |
| `fednow-rule-validator` | Applies FedNow scheme-specific rules via `FedNowValidator`. |
| `sepa-rule-validator` | Applies SEPA scheme-specific rules via `SepaValidator`. |
| `cbpr-rule-validator` | Applies SWIFT CBPR+ usage guidelines. |
| `mt-parser` | Parses inbound SWIFT FIN MT messages using `mx20022-translate::mt::parse()`. |
| `mt-to-mx-translator` | Converts MT to MX using `mx20022-translate` bidirectional mappings. Supports MT103→pacs.008, MT202→pacs.009, MT940→camt.053. |
| `mx-to-mt-translator` | Converts MX back to MT for legacy downstream systems. |
| `mx-enricher` | Adds derived or looked-up data: BIC institution names, country names from codes, structured address parsing. |
| `batch-splitter` | Splits a multi-transaction message (e.g., a pain.001 with multiple `CdtTrfTxInf` entries) into individual transaction contexts. |
| `batch-aggregator` | Collects results from individual transactions back into a batch response. |

### 6.2 Routing & Flow Control

| Participant | Description |
|------------|-------------|
| `routing-engine` | Routes messages to output channels based on configurable rules. Rule dimensions: message type, currency, BIC prefix, amount range, country code, custom context fields. Rules are evaluated top-to-bottom; first match wins. Default route is configurable. |
| `duplicate-checker` | Detects duplicate messages by configurable key fields (message ID, end-to-end ID, UETR, or composite keys). Configurable time window. Uses the Store for persistence — survives restarts. |
| `rate-limiter` | Enforces throughput limits per channel, per counterparty BIC, per message type, or per custom dimension. Token bucket algorithm. Configurable burst allowance. |
| `circuit-breaker` | Protects downstream systems from cascading failures. Tracks error rates per outbound channel. Opens circuit after configurable failure threshold. Half-open probing for recovery detection. |
| `retry-handler` | Configurable retry logic with exponential backoff and jitter. Per-error-category retry policies (retry transient errors, don't retry business rejections). |
| `throttle` | Introduces configurable processing delay. Useful for rate-limiting submissions to payment networks that enforce transaction-per-second limits. |

### 6.3 Response Building

| Participant | Description |
|------------|-------------|
| `status-response-builder` | Generates `pacs.002` Payment Status Reports from transaction outcomes. Maps internal result codes to ISO 20022 status codes (ACTC, ACCP, RJCT, etc.) and reason codes. |
| `acknowledgement-builder` | Generates technical acknowledgements at the BAH (head.001) level. |
| `error-response-builder` | Generates rejection responses with structured reason codes. Maps common failure scenarios (validation failure, screening hit, timeout) to appropriate ISO 20022 reason codes. |
| `cancellation-response-builder` | Generates `camt.029` Resolution of Investigation in response to `camt.056` cancellation requests. |

### 6.4 Observability

| Participant | Description |
|------------|-------------|
| `message-logger` | Logs message content with configurable field masking for PII/PAN. Supports per-field masking rules (e.g., mask `DbtrAcct` but not `CdtrBIC`). Configurable log level and output format. Can appear multiple times in a pipeline (tagged for inbound vs. outbound logging). |
| `metrics-emitter` | Emits custom business metrics based on message content. Example: counter per currency, histogram of transaction amounts, gauge of pending transactions by counterparty. |
| `audit-publisher` | Publishes transaction audit events to a dedicated outbound channel (typically Kafka) for external consumption by audit systems, data warehouses, or monitoring platforms. |
| `tracing-span` | Creates OpenTelemetry-compatible trace spans for distributed tracing across microservice architectures. Propagates trace context from inbound requests and attaches payment-relevant attributes. |

---

## 7. Observability & Operations

### 7.1 Metrics

The runtime exposes Prometheus-compatible metrics. These are always-on, not optional.

**Transaction metrics:**
- `mx_transactions_total` — counter, labels: pipeline, message_type, outcome (committed/aborted/poison)
- `mx_transaction_duration_seconds` — histogram, labels: pipeline, message_type
- `mx_transactions_active` — gauge, labels: pipeline
- `mx_transaction_state_transitions_total` — counter, labels: pipeline, from_state, to_state

**Participant metrics:**
- `mx_participant_duration_seconds` — histogram, labels: pipeline, participant, action (prepared/aborted)
- `mx_participant_errors_total` — counter, labels: pipeline, participant, error_type

**Channel metrics:**
- `mx_channel_messages_received_total` — counter, labels: channel
- `mx_channel_messages_sent_total` — counter, labels: channel
- `mx_channel_send_duration_seconds` — histogram, labels: channel
- `mx_channel_errors_total` — counter, labels: channel, error_type
- `mx_channel_connected` — gauge, labels: channel (1 = connected, 0 = disconnected)
- `mx_channel_backpressure_events_total` — counter, labels: channel

**Correlation metrics:**
- `mx_correlation_pending` — gauge
- `mx_correlation_matched_total` — counter
- `mx_correlation_timeouts_total` — counter
- `mx_correlation_match_duration_seconds` — histogram

**Store metrics:**
- `mx_store_write_duration_seconds` — histogram
- `mx_store_query_duration_seconds` — histogram
- `mx_store_dead_letters_total` — counter

**Runtime metrics:**
- `mx_runtime_uptime_seconds` — gauge
- `mx_runtime_config_reload_total` — counter
- `mx_runtime_config_reload_errors_total` — counter

### 7.2 Health & Readiness

Three distinct endpoints:

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `GET /health` | Is the process alive? | 200 if the event loop is running |
| `GET /ready` | Can this instance accept traffic? | 200 if all inbound channels are connected and the Store is reachable. 503 otherwise with details of what's unhealthy. |
| `GET /status` | Detailed runtime status | JSON: pipeline states, channel connection status, queue depths, correlation pending counts, store health, uptime, config version |

### 7.3 Structured Logging

All logging uses the `tracing` crate with structured fields. Every log line includes:

- `tx_id` — transaction identifier (when in transaction context)
- `pipeline` — pipeline name
- `participant` — participant name (when in participant context)
- `channel` — channel name (when in channel context)
- `msg_type` — ISO 20022 message type
- `duration_us` — elapsed microseconds

Sensitive field masking is on by default. The masking configuration specifies which ISO 20022 element names should be masked in logs. Default masked fields: `DbtrAcct`, `CdtrAcct`, `DbtrNm`, `CdtrNm`, `Adr`. Unmasking requires explicit operator action and is itself logged for audit.

Output formats: JSON (default for production), human-readable (for development). Configurable output destination: stdout, file, syslog.

### 7.4 CLI Operations Tool (mxctl)

`mxctl` is the operator's interface to a running runtime instance. It communicates with the runtime via the admin gRPC endpoint.

```bash
# ─── Runtime lifecycle ───
mxctl start --config gateway.toml          # Start the runtime
mxctl stop --graceful --timeout 30s        # Graceful shutdown
mxctl status                               # Runtime status summary
mxctl reload                               # Hot-reload participant configs

# ─── Transaction inspection ───
mxctl tx show TX-2026-00001                # Full transaction detail
mxctl tx search --e2e-id E2E-ABC-123      # Search by end-to-end ID
mxctl tx search --msg-id MSG-001           # Search by message ID
mxctl tx search --uetr 123e4567-e89b-...   # Search by UETR
mxctl tx search --since 2026-03-01 --until 2026-03-02 --pipeline fednow
mxctl tx context TX-2026-00001             # Show context mutation history
mxctl tx replay TX-2026-00001              # Replay a transaction through its pipeline
mxctl tx export TX-2026-00001 --format json # Export full transaction record

# ─── Channel management ───
mxctl channel list                         # List all channels with status
mxctl channel pause swift-inbound          # Stop accepting new messages
mxctl channel resume swift-inbound         # Resume after pause
mxctl channel drain core-banking           # Drain pending outbound messages
mxctl channel stats fednow-inbound         # Per-channel statistics

# ─── Pipeline management ───
mxctl pipeline list                        # List all pipelines
mxctl pipeline stats fednow-credit-transfer # Pipeline throughput/latency/error stats
mxctl pipeline test fednow-credit-transfer --file test.xml  # Dry-run a message

# ─── Correlation ───
mxctl correlation pending                  # List pending expectations
mxctl correlation stats                    # Match/timeout rates

# ─── Dead letters ───
mxctl deadletter list                      # List dead letters
mxctl deadletter show DL-001              # Inspect a dead letter
mxctl deadletter replay DL-001            # Replay a dead letter
mxctl deadletter replay --all --pipeline fednow  # Replay all dead letters for a pipeline
mxctl deadletter purge --older-than 90d    # Purge old dead letters

# ─── Diagnostics ───
mxctl metrics                              # Dump current metric values
mxctl connections                          # Show all active connections
mxctl config show                          # Show effective configuration
mxctl config validate gateway.toml         # Validate a config file without starting
```

---

## 8. Reliability & Error Handling

### 8.1 Processing Guarantees

The runtime provides **at-least-once processing** semantics:

1. Every inbound message is persisted to the Store in `RECEIVED` state before entering the pipeline.
2. If the process crashes mid-transaction, recovery on restart detects all transactions in non-terminal states and replays them from the beginning of the pipeline.
3. The `duplicate-checker` participant, deployed on both inbound and outbound sides, provides effective exactly-once semantics for the end-to-end flow.

**Why not exactly-once at the runtime level?** True exactly-once requires coupling the runtime to specific transport semantics (Kafka transactions, 2PC with the Store, etc.). This is achievable for specific channel combinations but not as a general guarantee. At-least-once with idempotency is the pragmatic choice used by every production payment system I've worked with, and it's what jPOS effectively provides.

### 8.2 Error Taxonomy

| Category | Examples | Default Behavior |
|----------|----------|-----------------|
| **Transient** | Network timeout, DB connection lost, downstream 503, channel temporarily unavailable | Retry with exponential backoff + jitter. Configurable max attempts. |
| **Business** | Invalid IBAN, sanctions hit, insufficient funds, duplicate detected | Abort transaction. Generate rejection response with appropriate ISO 20022 reason code. No retry. |
| **Schema** | Malformed XML, missing required field, XSD constraint violation | Abort transaction. Generate technical error response. No retry. |
| **System** | Out of memory, disk full, Store unreachable, configuration error | Alert operators. Halt pipeline. Do not attempt to process further messages until resolved. |
| **Poison** | Message causes repeated processing failures across multiple retry attempts | Move to dead letter queue after configurable N attempts. Alert operators. Continue processing other messages. |

### 8.3 Dead Letter Queue

Messages that exhaust their retry budget are moved to the dead letter store with:

- Original raw message
- Full context at time of failure
- Complete error chain (every error from every retry attempt)
- Participant that triggered the final failure
- Timestamps for each attempt

Dead letters are queryable and replayable via `mxctl`. Replay pushes the message back through the pipeline from the beginning — it does not attempt to resume mid-pipeline, because participant state may have changed.

### 8.4 Crash Recovery

On startup, the runtime:

1. Connects to the Store
2. Loads all transactions in non-terminal states (RECEIVED, PREPARING, COMMITTING, ABORTING)
3. Replays each from the beginning of its pipeline
4. Loads pending correlation expectations
5. Starts inbound channels only after recovery is complete (the `/ready` endpoint returns 503 until recovery finishes)

Recovery is logged and metered. Operators can see exactly which transactions were recovered and what happened on replay.

---

## 9. Security

### 9.1 Transport Security

- TLS 1.2+ for all network channels (TLS 1.3 preferred, 1.2 supported for legacy interop)
- Mutual TLS (mTLS) support for channel-to-channel authentication
- Certificate rotation without restart via file-watch on certificate paths
- Separate TLS configurations per channel (different CAs for different networks)

### 9.2 Data Protection

- **At rest:** Field-level encryption for sensitive data in the Store. Configurable per field. AES-256-GCM with envelope encryption (data key encrypted by a master key). Master key sourced from environment variable or external vault.
- **In logs:** PII masking on by default. Configurable masking rules per ISO 20022 element name.
- **In memory:** Sensitive fields in the Context are zeroized on transaction completion (using the `zeroize` crate). This is defense-in-depth — it limits the window of exposure in heap dumps or core dumps.
- **In transit:** TLS everywhere. No plaintext channels in production configurations.

### 9.3 Secrets Management

- **Environment variables:** Default for simple deployments. All sensitive config fields (Store passwords, TLS keys) support `env:VARIABLE_NAME` syntax in TOML.
- **HashiCorp Vault:** Planned integration for v0.3. Dynamic secret rotation.
- **File-based:** Certificate and key paths. Inotify-based reload on change.

### 9.4 Access Control

The admin interface (mxctl / admin gRPC) supports:

- **Authentication:** mTLS client certificates or bearer token (JWT).
- **Authorization:** Role-based access control with three built-in roles:
  - `operator` — start, stop, pause, resume, reload, view status
  - `auditor` — search transactions, inspect contexts, export records, view dead letters
  - `admin` — all of the above plus configuration changes, dead letter replay, purge
- **Audit:** All admin operations are logged with the authenticated identity, timestamp, and action.

### 9.5 Dependency Security

The `mx20022` library already uses `cargo-deny` for license and advisory auditing. The runtime inherits this and extends it:

- `unsafe` code is forbidden workspace-wide (inherited from mx20022's convention)
- `cargo-deny` runs in CI for every PR
- Dependabot for automated dependency updates (already configured on the mx20022 repo)
- MSRV policy: track mx20022's MSRV (currently 1.75.0)

---

## 10. Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Throughput | 10,000+ messages/sec per instance | Measured with HTTP channel, PostgreSQL store, 3-participant pipeline (validate, log, respond). This is the runtime overhead — participant business logic adds its own cost. |
| Latency (p50) | < 5ms end-to-end pipeline traversal | Excludes external I/O in participants (DB lookups, API calls). |
| Latency (p99) | < 25ms end-to-end pipeline traversal | Predictable tail latency. No GC pauses (Rust). |
| Startup time | < 2 seconds (cold start, empty recovery) | Container-friendly. Fast restart. |
| Recovery time | < 10 seconds for 1,000 pending transactions | Replay from Store on crash recovery. |
| Memory footprint | < 256MB base (no transactions in flight) | Scales with `max_concurrent` × average context size. |
| Binary size | < 50MB static binary (all channels enabled) | Single binary, no runtime dependencies. |

**Benchmarking:** The runtime includes a Criterion benchmark suite (following mx20022's convention with `just bench`). Benchmarks cover: message parsing throughput, pipeline traversal latency, Store write latency, correlation matching, and channel serialization/deserialization.

---

## 11. Deployment

### 11.1 Container (Primary Target)

Single static binary with no runtime dependencies. No JVM, no GC, no dynamic linking.

```dockerfile
FROM scratch
COPY mxruntime /mxruntime
COPY config.toml /etc/mx/config.toml
EXPOSE 8443 9090 9100
ENTRYPOINT ["/mxruntime", "--config", "/etc/mx/config.toml"]
```

Image size target: < 100MB including the binary.

### 11.2 Kubernetes

- **Helm chart** with configurable values for all TOML parameters
- **Horizontal scaling:** Multiple instances sharing Kafka consumer groups or competing AMQP consumers. Correlation Engine state is Store-backed, so any instance can match a response to a request from a different instance.
- **Readiness/liveness probes** mapped to `/ready` and `/health`
- **ConfigMap** for pipeline configuration
- **Secrets** for credentials and TLS material
- **PodDisruptionBudget** to ensure graceful rolling updates
- **Graceful shutdown:** Runtime respects SIGTERM, drains in-flight transactions, closes channels cleanly before exit

### 11.3 Bare Metal / VM

Single binary, single config file, systemd unit file. No orchestration required. This is important — many banking environments don't run Kubernetes, and some never will.

```ini
[Unit]
Description=mx20022-runtime payment processor
After=network.target postgresql.service

[Service]
Type=simple
ExecStart=/usr/local/bin/mxruntime --config /etc/mx/config.toml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536
User=mxruntime
Group=mxruntime

[Install]
WantedBy=multi-user.target
```

---

## 12. Crate Structure

```
mx20022-runtime/
├── Cargo.toml                    # Workspace root
├── crates/
│   ├── mx20022-runtime-core/     # Transaction Manager, Context, Pipeline,
│   │                             # Participant trait, lifecycle state machine
│   ├── mx20022-channels/         # Channel traits + InboundMessage/OutboundMessage types
│   │   ├── mx20022-channel-http/
│   │   ├── mx20022-channel-grpc/
│   │   ├── mx20022-channel-kafka/
│   │   ├── mx20022-channel-amqp/
│   │   ├── mx20022-channel-file/
│   │   ├── mx20022-channel-nats/
│   │   └── mx20022-channel-tcp/
│   ├── mx20022-participants/     # All built-in participants
│   ├── mx20022-store/            # Store trait
│   │   ├── mx20022-store-postgres/
│   │   ├── mx20022-store-sqlite/
│   │   └── mx20022-store-rocksdb/
│   ├── mx20022-correlation/      # Correlation Engine
│   ├── mx20022-session/          # Session Manager
│   ├── mx20022-config/           # TOML config parsing, validation, hot-reload
│   ├── mx20022-metrics/          # Prometheus metrics registry + export
│   ├── mx20022-admin/            # Admin gRPC service (for mxctl)
│   ├── mx20022-crypto/           # Field-level encryption, secret loading
│   ├── mx20022-cli/              # mxctl command-line tool
│   └── mx20022-runtime/          # Umbrella: binary entry point, wiring
├── proto/                        # Protobuf definitions for admin gRPC + gRPC channel
├── tests/                        # Integration test suite
│   ├── fixtures/                 # Test messages, configs
│   └── harness/                  # Test harness for pipeline testing
├── benches/                      # Criterion benchmarks
├── deploy/
│   ├── docker/                   # Dockerfile, docker-compose for local dev
│   ├── helm/                     # Kubernetes Helm chart
│   └── systemd/                  # systemd unit files
├── docs/
│   ├── ARCHITECTURE.md
│   ├── OPERATIONS.md             # Operator guide
│   ├── PARTICIPANT_GUIDE.md      # How to write custom participants
│   └── examples/                 # Example configurations for common deployments
├── justfile                      # Task runner (matching mx20022 convention)
├── deny.toml                     # cargo-deny config
├── rust-toolchain.toml           # Rust stable + components
└── CLAUDE.md                     # AI assistant context (matching mx20022 convention)
```

**Feature flags on the umbrella crate:**

| Feature | What it enables | Default |
|---------|----------------|---------|
| `channel-http` | HTTP inbound/outbound channel | yes |
| `channel-grpc` | gRPC inbound/outbound channel | yes |
| `channel-kafka` | Kafka consumer/producer channel | no |
| `channel-amqp` | AMQP 0.9.1 channel | no |
| `channel-file` | File watch/write channel | no |
| `channel-nats` | NATS/JetStream channel | no |
| `channel-tcp` | Raw TCP channel | no |
| `store-postgres` | PostgreSQL store backend | yes |
| `store-sqlite` | SQLite store backend | yes |
| `store-rocksdb` | RocksDB store backend | no |
| `all-channels` | All channel implementations | no |
| `all-stores` | All store backends | no |
| `full` | Everything | no |

A minimal build (HTTP + gRPC channels, PostgreSQL + SQLite stores) compiles with just the defaults.

---

## 13. Testing Strategy

### 13.1 Unit Tests

Every crate has comprehensive unit tests. Participants are tested in isolation with mock Contexts. Channels are tested against mock servers/brokers. The Store trait has a conformance test suite that every backend must pass.

### 13.2 Integration Tests

The `tests/harness/` module provides a test harness for pipeline-level testing:

```rust
use mx20022_test_harness::*;

#[tokio::test]
async fn test_fednow_credit_transfer_happy_path() {
    let harness = TestHarness::builder()
        .with_pipeline("fednow-credit-transfer")
        .with_participant(SchemaValidator::new())
        .with_participant(FedNowRuleValidator::new())
        .with_participant(StatusResponseBuilder::new())
        .with_store(InMemoryStore::new())
        .build()
        .await;

    let pacs008 = load_test_message("testdata/xml/pacs/pacs_008_001_13_minimal.xml");
    let result = harness.process(pacs008).await;

    assert_eq!(result.outcome, Outcome::Committed);
    assert!(result.response_xml.contains("pacs.002"));
    assert_eq!(result.participant_actions["schema-validator"], Action::Prepared);
}

#[tokio::test]
async fn test_sanctions_hit_aborts_transaction() {
    let harness = TestHarness::builder()
        .with_pipeline("screening-test")
        .with_participant(SchemaValidator::new())
        .with_participant(MockSanctionsScreener::always_hit())
        .with_participant(StatusResponseBuilder::new())
        .with_store(InMemoryStore::new())
        .build()
        .await;

    let pacs008 = load_test_message("testdata/xml/pacs/pacs_008_001_13_minimal.xml");
    let result = harness.process(pacs008).await;

    assert_eq!(result.outcome, Outcome::Aborted);
    assert_eq!(result.participant_actions["mock-sanctions-screener"], Action::Aborted);
}
```

The test harness reuses mx20022's existing `testdata/` fixtures for message samples.

### 13.3 End-to-End Tests

Docker Compose configuration for spinning up the runtime with real PostgreSQL, real Kafka, and real RabbitMQ. Tests send messages through actual channels and verify end-to-end processing including Store persistence, correlation, and dead letter handling.

### 13.4 Performance Tests

Criterion benchmarks for:
- Pipeline throughput (messages/sec) with varying participant counts
- Context operations (put/get latency)
- Store write and query latency per backend
- Correlation matching under load
- Channel serialization/deserialization overhead

### 13.5 Chaos Tests

Planned for v0.4+:
- Kill the runtime mid-transaction, verify recovery
- Disconnect Store mid-write, verify retry and consistency
- Saturate channel capacity, verify backpressure behavior
- Inject latency in participants, verify timeout handling

---

## 14. Resolved Decisions

These are the open questions from the draft PRD, now resolved with rationale.

### 14.1 Async Runtime: Tokio, No Abstraction

**Decision:** Tokio is the async runtime. We do not abstract over it.

**Rationale:** The Rust async ecosystem has converged on Tokio. Every library we depend on (hyper, tonic, rdkafka, lapin, sqlx, deadpool) is Tokio-native. Abstracting over the runtime would add a compatibility layer that provides no practical benefit — nobody deploying a production payment system is going to use async-std. The abstraction would also prevent us from using Tokio-specific features (io_uring support, console debugging, task-local storage) that are valuable for a high-performance runtime.

### 14.2 Configuration Hot-Reload: Scoped

**Decision:** Hot-reload for participant configuration. Restart for topology changes.

**Rationale:** Participants frequently need tuning in production — adjusting a fraud threshold, updating a sanctions screening confidence score, changing a rate limit. These changes should not require downtime. However, adding or removing participants from a pipeline, changing channel bindings, or modifying Store configuration are structural changes that affect the runtime's connection topology. These are planned operations that should go through a proper deployment process. The restart cost is < 2 seconds, which is acceptable for structural changes. Mixing structural hot-reload with the simpler participant config reload would add significant complexity to the Transaction Manager's lifecycle management with minimal operational benefit.

**Mechanism:** The config engine watches the TOML file via inotify. On change, it re-parses only the `participants[].config` sections and pushes updated config to running participants via a `config_changed(&self, new_config: &toml::Table)` method on the Participant trait. Participants that don't implement this method ignore config changes. The reload event is logged, metered, and visible via `mxctl status`.

### 14.3 Plugin System: Compile-Time for v1, WASM for v2

**Decision:** Participants are compiled into the binary. No dynamic loading in v1. WASM-based plugin system planned for a future major version.

**Rationale:** Rust's ABI is not stable. Dynamic loading via `.so`/`.dylib` would require either pinning an exact compiler version (fragile) or defining a C ABI boundary (lossy — no generics, no traits, manual memory management). Neither is acceptable for a system where correctness matters.

The compile-time model means users write their participants as Rust crates, add them to their Cargo workspace, and build a custom binary. This is the pattern used by TigerBeetle, Meilisearch, and other Rust infrastructure projects. It's simple, safe, and performant.

For the longer-term future (post-v1), a WASM-based plugin system is the right path. WASM provides sandboxing, language independence (participants could be written in Go, TypeScript, Python, etc.), and a stable ABI — without the fragility of native dynamic loading. This is a significant undertaking and belongs in a future major version.

### 14.4 State Machine: Formalized from Day One

**Decision:** The transaction lifecycle is a formal state machine with defined states, transitions, and enforcement.

**Rationale:** We are building for the long term, not for speed. In payment processing, an undefined state is a compliance violation and a potential financial loss. The state machine is:

- **States:** RECEIVED, PREPARING, PREPARED, COMMITTING, COMMITTED, ABORTING, ABORTED, POISON
- **Transitions:** Defined and enforced. Invalid transitions return errors, not panics. Every transition is persisted to the Store and emitted as a metric.
- **Enforcement:** The Context carries the current state. Participant methods are only callable in valid states (e.g., `commit` is only callable in PREPARED state). This is enforced at runtime via state checks on every method call.

This adds modest implementation complexity but eliminates an entire category of bugs that would be painful to debug in production. We do this right from the start.

### 14.5 Multi-Tenancy: Not in v1. Separate Instances.

**Decision:** No multi-tenancy in v1 or v2. Use separate runtime instances.

**Rationale:** Multi-tenancy in payment processing is a regulatory and security minefield. Different tenants may have different data residency requirements, different retention policies, different access controls, and different regulatory regimes. Implementing multi-tenancy means getting all of these right simultaneously, which adds complexity that dwarfs the operational cost of running separate instances.

The runtime is designed to be lightweight (< 256MB base memory, < 2s startup). Running separate instances per tenant on Kubernetes is trivial and provides the strongest possible isolation guarantee. If a future version adds multi-tenancy, it will be opt-in and clearly documented as sharing Store and memory space.

### 14.6 CNCF: Yes, Target Sandbox After v0.4

**Decision:** Target CNCF Sandbox submission after v0.4 when the project has demonstrated production stability and community traction.

**Rationale:** CNCF Sandbox provides visibility, credibility, and a governance framework that would accelerate adoption, especially among enterprise users who need institutional backing for open-source infrastructure dependencies. The requirements for Sandbox are modest — the project needs to demonstrate alignment with CNCF's mission (cloud-native infrastructure), have a healthy contributor base, and follow good open-source governance practices.

**Pre-submission checklist:**
- Apache 2.0 license (already done)
- CONTRIBUTING.md with clear contributor guidelines
- CODE_OF_CONDUCT.md (Contributor Covenant)
- GOVERNANCE.md with decision-making process
- At least 3 maintainers from at least 2 organizations
- CI/CD passing, releases published, documentation complete
- Evidence of adoption (production users, stars, downloads)

### 14.7 Governance: GitHub Org + RFC Process from v0.1

**Decision:** Establish a GitHub organization (`mx20022-rs` or similar), CONTRIBUTING.md, and an RFC process before the first release of the runtime.

**Rationale:** Phase 2 is big enough — and ambitious enough — that building community is a requirement, not a nice-to-have. Establishing governance early sends a signal that this is a serious project that welcomes contributions and has a transparent decision-making process.

**Structure:**
- **GitHub Organization:** All mx20022 repositories (library + runtime) under a single org. Separate from any individual's personal GitHub.
- **CONTRIBUTING.md:** How to build, test, and submit PRs. Coding standards. Commit message conventions.
- **RFC process:** For significant architectural changes (new channel types, Store backends, participant API changes), contributors submit an RFC as a markdown document in a `rfcs/` directory. RFCs are discussed in GitHub Issues and approved by maintainers. Small changes (bug fixes, documentation, minor features) don't need RFCs.
- **Maintainer roles:** Core maintainers (merge authority on all crates), crate maintainers (merge authority on specific crates, e.g., a Kafka channel maintainer), and contributors (PR submission). Start with the project creator as sole core maintainer, actively recruit a second maintainer before v0.2.
- **Decision-making:** Lazy consensus among maintainers with a 72-hour review window. Core maintainer has veto on architectural decisions. This is pragmatic for a small team and can evolve as the community grows.

### 14.8 Additional Decisions Not in Original Open Questions

**Naming:** The runtime project is `mx20022-runtime`. The binary is `mxruntime`. The CLI tool is `mxctl`. The umbrella crate is `mx20022-runtime`. This maintains namespace consistency with the `mx20022` library.

**Workspace relationship:** `mx20022-runtime` is a **separate repository and workspace** from `mx20022`. It depends on `mx20022` via crates.io (or git dependency during development). This keeps the library's compile times and release cycle independent of the runtime. The library can ship a new message type without requiring a runtime release, and vice versa.

**Error handling:** All public APIs return `Result<T, mx20022_runtime::Error>`. The error type is an enum with variants for each error category (transport, business, schema, system, poison). Errors are `Send + Sync + 'static` and implement `std::error::Error` with source chaining. We use `thiserror` for error definition, not `anyhow` — public APIs deserve structured errors.

**Logging framework:** `tracing` (already used by Tokio, hyper, tonic). Structured JSON output in production, human-readable in development. No `log` crate bridge — we commit fully to `tracing`.

**Serialization framework:** `serde` everywhere, consistent with mx20022. TOML for configuration, JSON for admin API responses and Store metadata, XML for ISO 20022 messages.

---

## 15. Release Plan

### v0.1 — Foundation (Months 1-3)

The skeleton that proves the architecture works end-to-end.

- Transaction Manager with full lifecycle state machine (RECEIVED → PREPARING → COMMITTED/ABORTED)
- Context with typed put/get, append-only audit, automatic timestamps
- Participant trait with prepare/commit/abort
- HTTP inbound and outbound channels (server and client modes)
- gRPC inbound and outbound channels
- PostgreSQL store backend
- SQLite store backend
- Built-in participants: `schema-validator`, `business-rule-validator`, `fednow-rule-validator`, `message-logger`, `status-response-builder`
- TOML configuration parser
- Prometheus metrics endpoint
- Health/readiness/status endpoints
- `mxctl` basics: start, stop, status, tx show, tx search
- Test harness for pipeline-level testing
- Criterion benchmark suite
- Docker image
- CONTRIBUTING.md, GOVERNANCE.md, CODE_OF_CONDUCT.md
- GitHub organization setup

**Demo:** Receive a `pacs.008` over HTTP, validate against FedNow rules, log it, and return a `pacs.002` status response. End-to-end in under 5ms.

### v0.2 — Messaging & Correlation (Months 4-6)

The features that make it useful for real payment network integration.

- Kafka channel (consumer + producer)
- AMQP channel (consumer + publisher)
- File watch/write channel
- Correlation Engine with configurable key matching, timeout handling, and auto-inquiry (pacs.028)
- `duplicate-checker` participant with Store-backed deduplication
- `routing-engine` participant with rule-based routing
- `circuit-breaker` participant
- `rate-limiter` participant
- `sepa-rule-validator` and `cbpr-rule-validator` participants
- Hot-reload for participant configuration
- Dead letter queue with `mxctl deadletter` commands
- `mxctl` full transaction inspection: context history, replay, export
- Integration test suite with Docker Compose (real Postgres, real Kafka, real RabbitMQ)

### v0.3 — Translation & Security (Months 7-9)

MT↔MX bridge capabilities and production security hardening.

- `mt-parser`, `mt-to-mx-translator`, `mx-to-mt-translator` participants (delegating to `mx20022-translate`)
- `mx-enricher` participant
- Session Manager for long-lived connections
- NATS channel
- TCP raw channel
- RocksDB store backend
- Field-level encryption in Store (AES-256-GCM)
- HashiCorp Vault integration for secret management
- mTLS support on all channels
- Certificate rotation without restart
- Admin API authentication (mTLS + JWT)
- Role-based access control (operator/auditor/admin)
- `batch-splitter` and `batch-aggregator` participants
- `mxctl pipeline test` for dry-run validation

### v0.4 — Operations & Hardening (Months 10-12)

Production-readiness. The release you'd trust in a regulated environment.

- Crash recovery with transaction replay from Store
- `retry-handler` participant with per-category retry policies
- `throttle` participant
- `tracing-span` participant for OpenTelemetry distributed tracing
- Chaos test suite (kill mid-transaction, disconnect Store, saturate channels)
- Kubernetes Helm chart with full production configuration
- systemd unit files and bare-metal deployment guide
- Operator guide (docs/OPERATIONS.md)
- Custom participant development guide (docs/PARTICIPANT_GUIDE.md)
- Performance optimization pass (profiling, allocation reduction, zero-copy where feasible)
- Security audit (internal or third-party)
- CNCF Sandbox submission preparation

### v0.5+ — Ecosystem (Year 2+)

Community-driven growth.

- WebSocket channel for real-time notifications
- Additional Store backends based on demand
- WASM plugin system exploration
- Admin web UI (separate project, communicates via admin gRPC)
- Language bindings for the test harness (Python, TypeScript) to lower contribution barriers
- Pre-built Docker images for common deployment patterns (FedNow gateway, SEPA gateway, MT-MX bridge)
- ISO 20022 conformance certification (if/when test suites are published)
- SWIFT Alliance Lite2 channel (community-driven, complex, requires SWIFT partnership)

---

## 16. Lessons from jPOS

jPOS has been in production for over 25 years processing real payment transactions. These lessons are earned, not theoretical.

### What we carry forward

**The participant model.** Simple, composable, testable. A participant does one thing, does it well, and is independently testable. This is the right abstraction for payment processing and we adopt it directly.

**The three-phase lifecycle (prepare/commit/abort).** It provides a natural model for vote-then-execute processing without requiring distributed transactions. Each participant can validate, then commit side effects only if everyone agrees. This maps perfectly to payment processing where you validate, screen, and route before committing.

**The Space/Context.** A shared, mutable, typed bag of state that travels through the pipeline. Participants communicate through the Context, not through direct references to each other. This keeps participants decoupled and the pipeline composable.

**Configuration-driven pipelines.** Non-developers (operations, integration teams) can modify processing behavior without code changes. The configuration is the contract between development and operations.

**Exhaustive logging and audit.** In payments, if it isn't logged, it didn't happen. And if you can't reproduce the exact state of a transaction at the moment a decision was made, you can't debug production issues or satisfy regulators.

**The Q2 self-contained runtime.** A single process that manages its own lifecycle. No external application server required. This simplifies deployment and operations enormously.

### What we improve

**TOML instead of XML configuration.** jPOS's XML deploy descriptors are verbose and error-prone. TOML is more readable, has better tooling support, and is the Rust ecosystem standard.

**Rust instead of Java.** No GC pauses. No JVM startup time (2s vs 10-30s). No classpath hell. No null pointer exceptions. The borrow checker catches entire categories of concurrency bugs at compile time. Memory footprint is 5-10x smaller.

**Apache 2.0 instead of GPL.** jPOS's GPL license has been a persistent barrier to commercial adoption. Companies that want to build proprietary payment systems on top of jPOS need a commercial license. Apache 2.0 removes this friction entirely, which should accelerate adoption.

**Modern deployment model.** Static binary, container-native, Kubernetes-ready. jPOS predates containers and its deployment model shows its age.

**Structured observability.** jPOS has logging but lacks modern observability: no Prometheus metrics, no OpenTelemetry tracing, no structured JSON logs. We build these in from day one.

**First-class testing.** jPOS testing is often integration-heavy and slow. The test harness provides fast, isolated pipeline testing with mock participants and in-memory stores.

**Formal state machine.** jPOS's transaction lifecycle is implicitly defined by the TransactionManager's control flow. We make it explicit, enforced, and documented.

---

## 17. References

- [jPOS Programmer's Guide](http://jpos.org/doc/proguide.pdf) — the spiritual ancestor
- [mx20022 GitHub Repository](https://github.com/socrates8300/mx20022) — the Phase 1 library this runtime depends on
- [mx20022 on crates.io](https://crates.io/crates/mx20022)
- [mx20022 API Documentation](https://docs.rs/mx20022)
- [SWIFT ISO 20022 Programme](https://www.swift.com/standards/iso-20022-programme)
- [FedNow Service](https://www.frbservices.org/financial-services/fednow)
- [SEPA Instant Credit Transfer](https://www.europeanpaymentscouncil.eu)
- [Tokio Runtime](https://tokio.rs)
- [TigerBeetle Design Document](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/DESIGN.md)
- [CNCF Sandbox Requirements](https://www.cncf.io/sandbox-guidelines/)
