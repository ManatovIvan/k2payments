# contributor.md

This guide is for engineers making code changes in `mx20022-runtime`.

It is intentionally practical: what to run, where to change code, and what quality bar is expected before opening a PR.

## 1. Development Environment

## Requirements

- Rust stable
- `cargo`
- Optional: `just`

## Initial validation

```bash
cargo check --workspace
cargo test --workspace
```

If you use `just`:

```bash
just check
just test
```

## 2. Repository Mental Model

Use crate boundaries as design boundaries.

- Runtime orchestration: `crates/mx20022-runtime`
- Transaction semantics/state machine: `crates/mx20022-runtime-core`
- Channel traits + adapters: `crates/mx20022-channels` and `crates/mx20022-channels/*`
- Built-in participants: `crates/mx20022-participants`
- Store abstraction/backends: `crates/mx20022-store` and `crates/mx20022-store/*`
- Config parsing/validation: `crates/mx20022-config`
- Admin control plane: `crates/mx20022-admin`
- Correlation: `crates/mx20022-correlation`
- Operations CLI: `crates/mx20022-cli`

Avoid bypassing abstractions (for example, channel-specific logic leaking into runtime-core).

## 3. Typical Change Workflow

1. Create a focused branch.
2. Make the smallest coherent change set.
3. Add/adjust tests in the same commit.
4. Run quality gates locally.
5. Update docs for behavior/config/API changes.

## Suggested branch naming

- `fix/<area>-<short-desc>`
- `feat/<area>-<short-desc>`
- `refactor/<area>-<short-desc>`

## 4. Quality Gates (Local)

Minimum before pushing:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

For perf-sensitive changes:

```bash
cargo bench -p mx20022-runtime --no-run
```

For security/dependency changes:

```bash
cargo deny check
```

## 5. Testing Expectations

When changing behavior, add tests near the change.

- Unit tests: same crate/module
- Integration tests: crate-level `tests/`
- Adapter/channel tests: cover inbound and outbound paths where feasible
- Store changes: validate all supported backends affected by the change

If a test cannot run in CI (external dependency), provide a deterministic fallback unit test and note manual verification steps in the PR.

## 6. Config and API Changes

If you add config fields:

- Validate in `mx20022-config`
- Add parse/validation tests
- Document the field in `README.md` and/or operations docs

If you change admin or CLI behavior:

- Update `mx20022-admin` and `mx20022-cli` tests
- Confirm endpoint auth/rbac behavior

## 7. Security Expectations

- Never add plaintext secret logging.
- Use secure secret types where available.
- Prefer constant-time comparisons for tokens/credentials.
- Do not weaken channel/auth defaults without explicit justification.
- For transport changes, document plaintext exceptions explicitly.

## 8. Performance Expectations

- Avoid avoidable cloning/allocation on hot paths.
- Prefer bounded concurrency and explicit backpressure.
- Measure before/after for non-trivial performance changes.

## 9. PR Checklist

Before opening PR:

- [ ] Scope is focused and coherent
- [ ] `fmt`, `clippy -D warnings`, `test` all pass
- [ ] New behavior has tests
- [ ] Docs/config examples updated if needed
- [ ] CLA is signed (required for external contributors)
- [ ] Any risk, migration, or rollback notes included in PR description

## 10. Commit Style

Use explicit, imperative commit messages:

- `runtime: enforce pipeline timeout in process path`
- `channel-kafka: switch to sync manual commits after enqueue`
- `config: reject plaintext channel when secure enforcement enabled`

## 11. Where to Ask Questions

If uncertain about architecture direction:

- Check `architecture.md` first
- Then check `docs/PLAN.md` and `docs/OPERATIONS.md`
- Raise a design note in your PR rather than guessing across crate boundaries
