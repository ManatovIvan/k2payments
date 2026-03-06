# Migration Strategy

## Rules
- Each migration has `*.up.sql` and `*.down.sql`.
- Migrations are ordered and idempotent where practical.
- Reversibility is required for local/dev rollback and controlled production rollback.

## Current migration set
- `0001_initial_schema.up.sql`
- `0001_initial_schema.down.sql`
