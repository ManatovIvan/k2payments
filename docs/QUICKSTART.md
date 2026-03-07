# Quick Start

This guide gets you from clone to a running local runtime in a few minutes.

It uses a development config with:

- local HTTP inbound channel (`127.0.0.1:18080`)
- local admin HTTP host (`127.0.0.1:19090`)
- SQLite file store (`/tmp/mx20022-quickstart.db`)
- auth disabled for local-only testing

Config file: `docs/examples/quickstart.toml`

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

## 4. Verify health/admin

In a second terminal:

```bash
curl -s http://127.0.0.1:19090/health
curl -s http://127.0.0.1:19090/ready
curl -s http://127.0.0.1:19090/status
```

## 5. Send a test message

```bash
curl -i -X POST http://127.0.0.1:18080/ \
  -H 'content-type: application/xml' \
  --data '<Document><Msg>Hello</Msg></Document>'
```

Expected: `202 Accepted`.

## 6. Verify transaction persisted

```bash
cargo run -p mx20022-cli -- tx search \
  --config docs/examples/quickstart.toml \
  --pipeline demo \
  --limit 10
```

You should see transactions in terminal states (typically `COMMITTED` for the happy path).

## 7. View metrics

```bash
curl -s http://127.0.0.1:19090/metrics | head -n 30
```

## 8. Stop runtime

Press `Ctrl+C` in the runtime terminal.

## Useful follow-ups

- Full ops runbook: `docs/OPERATIONS.md`
- Contributor workflow: `contributor.md`
- Architecture deep dive: `architecture.md`
- More secure example config: `docs/examples/basic.toml`

## Notes

- This quickstart intentionally disables auth and allows plaintext for local use only.
- Do not use `docs/examples/quickstart.toml` as a production baseline.
