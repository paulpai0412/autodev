---
description: Start the autonomous development workflow for a selected AFK issue number
agent: build
subtask: false
---

Run autodev for issue number `$ARGUMENTS` in the current consumer project.

1. Execute:
!`AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" start --project-root "$PWD" --issue-number "$1"`
2. Read the DB-backed start output and then inspect the active issue with `/autodev-show-session` when needed.
3. Tell me the root-session outcome, the current issue number, and the immediate next action.

Notes:
- `$ARGUMENTS` must be a GitHub issue number, for example `32`.
- Override `AUTODEV_HOME` first if the shared workflow repo is not installed at `~/apps/autodev`.
- This wrapper resolves the actual consumer project root from the current directory before dispatching the shared workflow.
- The runtime source of truth is SQLite in `.opencode/runtime/control-plane.sqlite3`.
- Issue selection, dispatch state, and resumable session state all come from the SQLite control plane; use `/autodev-show-session` to inspect the active root session.
- GitHub intake defaults to `paulpai0412/wferp`; override it with `AUTODEV_GITHUB_REPO=<owner/repo>` when needed.

Do not start worker execution in this command session. Stop after reporting the DB-backed start output. The fresh `main_orchestrator` root session owns the selected issue end-to-end and must run worker/verifier/release roles as subagents. Future root dispatch is only for another `main_orchestrator` bootstrap or recovery handoff.
