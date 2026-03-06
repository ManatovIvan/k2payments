# Logical Schema Design

## Normalization
- `transactions` holds one row per transaction.
- `context_mutations` normalized into separate rows for append-only history.
- `expectations` modeled independently for correlation lifecycle.
- `dead_letters` modeled independently for replay operations.

## Key Logical Fields
- `transactions`
: `tx_id`, `pipeline`, `source_channel`, `message_type`, `raw_message`, `state`, `received_at`, `completed_at`, `key_fields_json`
- `context_mutations`
: `id`, `tx_id`, `key`, `writer`, `written_at`
- `expectations`
: `id`, `correlation_key`, `expected_message_type`, `timeout_at`, `state`, `matched_tx_id`
- `dead_letters`
: `id`, `tx_id`, `reason`, `failed_at`, `raw_message`

## Constraints
- PK on entity identifiers.
- FK from `context_mutations.tx_id` to `transactions.tx_id`.
- FK from `dead_letters.tx_id` to `transactions.tx_id`.
- Unique `dead_letters.tx_id` to enforce max one dead letter row per transaction.

## Enum-like Fields
- `transactions.state`: `RECEIVED|PREPARING|PREPARED|COMMITTING|COMMITTED|ABORTING|ABORTED|POISON`
- `expectations.state`: `PENDING|MATCHED|TIMED_OUT|CANCELLED`
