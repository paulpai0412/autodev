---
description: Reconcile DB-backed issue state and report the next orchestrator/subagent action
agent: build
subtask: false
---

Run:
!`AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" reconcile --project-root "$PWD"`

Then inspect the DB-backed issue state via `/autodev-show-session` or `scripts.orchestrator_supervisor inspect --base-dir "$PWD" --issue-number <n>` when needed.

Report:
- current role/stage/status from the control plane
- the supervisor decision summary
- whether the decision requires a subagent delegation or a fresh main_orchestrator root session

Behavior notes:
- This wrapper resolves the actual consumer project root from the current directory before reconciling runtime state.
- Override `AUTODEV_HOME` first if the shared workflow repo is not installed at `~/apps/autodev`.
- Reconcile reads the SQLite control plane first; operator follow-up should inspect the DB-backed decision and active session state instead of local runtime artifacts.
- If no request is queued after reconcile, that is expected for `issue_worker` and `pr_verifier` decisions; the root orchestrator should delegate those as subagents. `release_worker` is launched only by `/autodev-release`.

Do not wait for human confirmation if the decision names `issue_worker` or `pr_verifier`. Delegate it from the current root orchestrator. Explicit dispatch is only for fresh `main_orchestrator` root sessions and independent release sessions.
