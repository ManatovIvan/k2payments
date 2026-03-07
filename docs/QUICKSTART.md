# Quick Start (Under 10 Minutes)

This guide takes you from clone to processing your first `pacs.008` end to end.

It uses local-only defaults:

- inbound HTTP channel: `127.0.0.1:18080`
- admin HTTP host: `127.0.0.1:19090`
- SQLite store: `/tmp/mx20022-quickstart.db`
- auth disabled for local testing

Config: `docs/examples/quickstart.toml`
Message sample: `docs/examples/messages/pacs008-minimal.xml`

## 1. Build

```bash
cargo build --workspace
```

## 2. Validate config

```bash
cargo run -p mx20022-cli -- config validate docs/examples/quickstart.toml
```

## 3. Start runtime

```bash
cargo run -p mx20022-runtime -- --config docs/examples/quickstart.toml --serve-admin
```

Keep this terminal running.

## 4. Verify runtime is up

In a second terminal:

```bash
curl -s http://127.0.0.1:19090/health
curl -s http://127.0.0.1:19090/ready
curl -s http://127.0.0.1:19090/status
```

## 5. Send your first `pacs.008`

```bash
curl -i -X POST http://127.0.0.1:18080/ \
  -H 'content-type: application/xml' \
  --data-binary @docs/examples/messages/pacs008-minimal.xml
```

Expected: `202 Accepted`.

## 6. Verify it was processed through pipeline

```bash
cargo run -p mx20022-cli -- tx search \
  --config docs/examples/quickstart.toml \
  --pipeline demo \
  --limit 10
```

You should see a transaction for the submitted message in a terminal state (typically `COMMITTED`).

## 7. Check metrics

```bash
curl -s http://127.0.0.1:19090/metrics | head -n 30
```

## 8. Stop runtime

Press `Ctrl+C` in the runtime terminal.

## Next docs

- Ops runbook: `docs/OPERATIONS.md`
- Participant development: `docs/PARTICIPANT_GUIDE.md`
- Architecture: `architecture.md`
- Additional examples: `docs/examples/basic.toml`, `docs/examples/fednow-gateway.toml`, `docs/examples/mt-to-mx-bridge.toml`

## Local-only note

`docs/examples/quickstart.toml` intentionally disables auth and allows plaintext for speed.
Do not use it as a production baseline.
