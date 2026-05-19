---
description: Reconcile DB-backed issue state and report next action
agent: build
subtask: false
---

Run:
!`AUTODEV_HOME="${AUTODEV_HOME:-/home/timmypai/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" reconcile --project-root "$PWD"`

Then inspect session pointers when needed:
!`AUTODEV_HOME="${AUTODEV_HOME:-/home/timmypai/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" show-session --project-root "$PWD"`

Report:
- supervisor decision summary
- current `state`, `current_role`, `current_stage`, `current_status`
- whether next action is subagent delegation or fresh root session dispatch

Notes:
- Runtime truth is DB-only SQLite (`issues`, `issue_history`).
- Do not use local JSON/YAML runtime artifacts as control-plane gates.
