# Physical SQLite Schema

## Engine choices
- SQLite in WAL mode for concurrent readers/writer.
- `PRAGMA foreign_keys = ON`.
- `TEXT` timestamps in RFC3339 UTC format.

## Index strategy
- `transactions(message_type, received_at DESC)` for throughput analytics.
- `transactions(state, received_at DESC)` for operational queue views.
- `context_mutations(tx_id, written_at ASC)` for context playback.
- `expectations(state, timeout_at ASC)` for timeout scans.

## Query hot paths
- Lookup by `tx_id`.
- Search by `message_id`, `end_to_end_id`, `uetr` from `key_fields_json`.
- List pending expectations and dead letters.
