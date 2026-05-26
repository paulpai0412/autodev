---
description: Start the autodev workflow for an issue
agent: build
subtask: false
---

Run autodev for issue number `$ARGUMENTS` in the current consumer project.

Run:
!`AUTODEV_HOME="${AUTODEV_HOME:-/home/timmypai/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python "$AUTODEV_HOME/scripts/autodev_project.py" start --project-root "$PWD" --issue-number "$1"`

Then inspect latest root session when needed:
!`AUTODEV_HOME="${AUTODEV_HOME:-/home/timmypai/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python "$AUTODEV_HOME/scripts/autodev_project.py" show-session --project-root "$PWD"`

Report:
- root-session dispatch outcome
- `issue_number`
- current `state`, `current_role`, `current_stage`, `current_status`
- immediate next action

Notes:
- `$ARGUMENTS` must be a GitHub issue number, for example `32`.
- Override `AUTODEV_HOME` first if the shared workflow repo is not installed at `~/apps/autodev`.
- Runtime truth is DB-only SQLite (`issues`, `issue_history`).
- Do not use local JSON/YAML runtime artifacts as control-plane gates.
