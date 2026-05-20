---
description: Launch independent autodev release worker for PR merge
agent: build
subtask: false
---

Run the independent release path for issue number `$ARGUMENTS` in the current project. If no issue number is provided, autodev selects the first verified issue waiting for release.

Run:
!`AUTODEV_HOME="${AUTODEV_HOME:-/home/timmypai/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" release --project-root "$PWD" --issue-number "$1" --auto-approve`

Then inspect session pointers when needed:
!`AUTODEV_HOME="${AUTODEV_HOME:-/home/timmypai/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" show-session --project-root "$PWD"`

Report:
- DB-backed release dispatch result
- `release_worker` session to resume
- current `state`, `current_role`, `current_stage`, `current_status`
- immediate next action

Behavior notes:
- This command is separate from `/autodev-reconcile` so human PR approval waits do not block development scheduling.
- Auto approval applies only here in the release path: it bypasses the human merge approval gate, not verifier/check/mergeability/workspace-hygiene gates.
- The command claims `verified -> release_pending` only when release/merge ownership starts.
- After this command returns a successful dispatch, treat the caller session as observer-only for release: use `inspect`/`reconcile` to track progress and do not manually launch another `release_worker` from the caller session.
- Release outcomes must be recorded through `submit-artifact --artifact-kind release_result`.
- Runtime truth is DB-only SQLite (`issues`, `issue_history`).
- Do not use local JSON/YAML runtime artifacts as control-plane gates.
