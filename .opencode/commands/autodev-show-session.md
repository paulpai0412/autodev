---
description: Show the latest continuation root session and resume hints
agent: build
subtask: false
---

Run:
!`AUTODEV_HOME="${AUTODEV_HOME:-/home/timmypai/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python "$AUTODEV_HOME/scripts/autodev_project.py" show-session --project-root "$PWD"`

Report these fields when present:
- `status`
- `title`
- `reason`
- `sourceSessionID`
- `rootSessionID`
- `recordedAt`
- `tuiResumeCommand`
- `cliOpenCommand`
- `recommendedAction`
- `role`
- `stage`
- `issueNumber`
- `branch`
- `error`

If `status` is `success`, provide exact resume/open instruction.
If `status` is `error`, explain no active root session is available and include error.

Notes:
- Runtime truth is DB-only SQLite (`issues`, `issue_history`).
- Do not use local JSON/YAML runtime artifacts as control-plane gates.
