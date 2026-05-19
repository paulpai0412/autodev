---
name: autodev-flow
description: Initialize a consumer repo and run the DB-only autodev loop end-to-end with dry-run confirmation, bounded retries, quarantine recovery, and strict completion gates. Use when user wants autonomous project delivery following the autodev system design flow.
---

# Autodev Flow

Single-entry skill for initializing a consumer repo, running autonomous reconcile loops, and handling exceptions safely.

## Hard constraints

- Runtime truth is DB-only: SQLite `issues` and `issue_history`.
- Do not use file artifacts (ledger/request/result/checkpoint YAML/JSON) as runtime gates.
- `init` must be idempotent: preserve existing config, fill missing pieces, and summarize diffs.
- Start with a dry-run execution plan, then ask for explicit confirmation before running write/dispatch commands.
- Default development capacity is `1` unless the user explicitly overrides it.
- Completion is strict: require worker + verifier evidence and release flow state transitions.
- Quarantine auto-recovery is allowed only once, and only when transient + retryable + cooldown satisfied.

## Inputs to collect (one at a time)

1. `project_root` (default: current directory)
2. `github_repo` (`owner/repo`) for init/intake if missing
3. optional `issue_number` to start immediately
4. optional loop controls (`interval_seconds`, `iterations`, `stop_on_error`)

If an input is already inferable from repo context, do not ask for it.

## Workflow

1. **Discover context**
   - Read `README.md` and runtime docs under `docs/agents/runtime/`.
   - Confirm DB-only branch rules before planning.
   - Detect whether the target project is already initialized.

2. **Dry-run plan (must show before execution)**
   - Show exact commands to run.
   - Show expected state transitions and stop conditions.
   - Show exception policy (`retry-failed`, `quarantine`, `resume-quarantined`, `fail-quarantined`, `retry-github-sync`).

3. **Initialize and validate consumer repo**
   - Run `autodev_project.py init` (idempotent).
   - Run `autodev_project.py install-commands`.
   - Run `autodev_project.py doctor`.

4. **Start autonomous development**
   - If `issue_number` is provided, run `autodev_project.py start --issue-number <n>`.
   - Run continuous scheduling with `autodev_project.py reconcile-watch`.
   - Use `reconcile-workspace` behavior as the single scheduler contract.

5. **Exception handling during loop**
   - Retryable fail -> `retry-failed` (bounded by policy).
   - Non-retryable or retries exhausted -> `quarantine`.
   - Quarantined issue -> at most one conditional auto-resume (`resume-quarantined`), then require operator decision.
   - GitHub sync failures -> `retry-github-sync`.

6. **Completion and reporting**
   - Report issue role/stage/status from DB state.
   - Report latest root/release session pointers and next action.
   - Only mark complete when strict gate criteria pass.

## Command cookbook

Use the exact command forms from [REFERENCE.md](./REFERENCE.md). Prefer `AUTODEV_HOME` wrappers when operating from consumer repos.

## Operator safety defaults

- Never force push.
- Never bypass verifier/check/mergeability gates.
- Never auto-merge unless explicitly requested.
- Never treat local artifacts as runtime control-plane truth.
