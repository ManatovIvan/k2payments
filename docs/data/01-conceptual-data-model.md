# Conceptual Data Model

## Core Entities
- `Transaction`: One runtime processing attempt for one inbound message.
- `ContextMutation`: Immutable audit event for each context key write.
- `Expectation`: Correlation expectation for a future response message.
- `DeadLetter`: Terminal failed message record for replay/investigation.

## Relationships and Cardinality
- `Transaction 1 -> N ContextMutation`
- `Transaction 1 -> 0..1 DeadLetter`
- `Transaction 1 -> 0..N Expectation` (request may spawn one or more expected responses)
- `Expectation 0..1 -> 1 Transaction` (matched response transaction link)

## Identity
- `Transaction.tx_id` is runtime-global unique identifier.
- `Expectation.id` is globally unique correlation expectation identifier.
- `DeadLetter.id` is globally unique dead letter identifier.

## High-level Invariants
- Each transaction starts in `RECEIVED` and ends in `COMMITTED`, `ABORTED`, or `POISON`.
- Context mutation history is append-only.
- Expectations must be durable across restarts.
- Dead letters are replayable and auditable.
