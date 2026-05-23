# State projection v3 mapping (single source)

## Purpose

This document pins the current mapping contract for:

- SQLite issue state -> Team workflow / primary issue label / PR workflow state
- PR workflow state -> SQLite issue state
- PR workflow state -> PR workflow label

Canonical implementation source:

- `scripts/state_projection.py`

## A) PR workflow state -> SQLite state

| PR workflow state | SQLite state |
|---|---|
| `merged` | `completed` |
| `release_blocked` | `quarantined` |
| `release_failed` | `failed` |
| `verifier_fail` | `failed` |
| `verifier_blocked` | `quarantined` |

## B) SQLite state -> Team workflow / primary issue label / PR workflow state

| SQLite state | Team workflow | Issue primary label | PR workflow state |
|---|---|---|---|
| `ready` | `ready` | `ready-for-agent` | `not_opened` |
| `claimed` | `in progress` | `agent-dispatching` | `not_opened` |
| `dispatching` | `in progress` | `agent-dispatching` | `not_opened` |
| `running` | `in progress` | `agent-in-progress` | `opened` / `not_opened`* |
| `verifying` | `in progress` | `agent-in-progress` | `opened` |
| `verified` | `in progress` | `agent-in-progress` | `verifier_passed` |
| `release_pending` | `in review` | `agent-in-progress` | `verifier_passed` |
| `failed` | `in review` | `agent-in-review` | `verifier_fail` / `release_failed` |
| `quarantined` | `in review` | `agent-in-review` | `verifier_blocked` / `release_blocked` |
| `completed` | `done` | `agent-completed` | `merged` |

\* `running` uses `opened` only when a `pr_opened` fact exists.

## C) PR workflow state -> PR issue label (1:1)

| PR workflow state | PR issue label |
|---|---|
| `not_opened` | `pr-not-opened` |
| `opened` | `pr-opened` |
| `verifier_passed` | `pr-verifier-passed` |
| `verifier_fail` | `pr-verifier-failed` |
| `verifier_blocked` | `pr-verifier-blocked` |
| `release_failed` | `pr-release-failed` |
| `release_blocked` | `pr-release-blocked` |
| `merged` | `pr-merged` |

## Verification pointers

Primary coverage for projection wiring currently lives in:

- `tests/scripts/test_orchestrator_supervisor.py`

## Configuration source of truth

Runtime mapping is now config-driven from `.autodev.yaml`:

- `state_projection.pr_workflow_to_sqlite_state`
- `state_projection.sqlite_to_team_workflow`
- `state_projection.sqlite_to_primary_label`
- `state_projection.sqlite_to_pr_workflow`
- `state_projection.pr_workflow_to_label`

Loading and fallback behavior:

- Loader: `scripts/state_projection.py::load_state_projection_config(base_dir)`
- If `.autodev.yaml` is missing or malformed, code falls back to built-in defaults.
- Partial overrides are allowed; unspecified keys keep defaults.

## Mapping schema reference

```yaml
state_projection:
  pr_workflow_to_sqlite_state:
    merged: completed
    release_blocked: quarantined
    release_failed: failed
    verifier_fail: failed
    verifier_blocked: quarantined

  sqlite_to_team_workflow:
    ready: ready
    claimed: in progress
    dispatching: in progress
    running: in progress
    verifying: in progress
    verified: in progress
    release_pending: in review
    failed: in review
    quarantined: in review
    completed: done

  sqlite_to_primary_label:
    ready: ready-for-agent
    claimed: agent-dispatching
    dispatching: agent-dispatching
    running: agent-in-progress
    verifying: agent-in-progress
    verified: agent-in-progress
    release_pending: agent-in-progress
    failed: agent-in-review
    quarantined: agent-in-review
    completed: agent-completed

  sqlite_to_pr_workflow:
    ready: not_opened
    claimed: not_opened
    dispatching: not_opened
    running: not_opened
    verifying: opened
    verified: verifier_passed
    release_pending: verifier_passed
    failed: verifier_fail
    quarantined: verifier_blocked
    completed: merged

  pr_workflow_to_label:
    not_opened: pr-not-opened
    opened: pr-opened
    verifier_passed: pr-verifier-passed
    verifier_fail: pr-verifier-failed
    verifier_blocked: pr-verifier-blocked
    release_failed: pr-release-failed
    release_blocked: pr-release-blocked
    merged: pr-merged
```

## Notes for future changes

- To change mapping behavior, edit `.autodev.yaml` only.
- No code change should be required for normal mapping updates.
- If new states are introduced, add test coverage in `tests/scripts/test_orchestrator_supervisor.py`.
