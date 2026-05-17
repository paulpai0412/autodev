---
description: Show the latest continuation root session and how to open it
agent: build
subtask: false
---

Run:
!`AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" show-session --project-root "$PWD"`

This wrapper resolves the actual consumer project root from the current directory before reading the SQLite-backed control plane.

Override `AUTODEV_HOME` first if the shared workflow repo is not installed at `~/apps/autodev`.

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

If `status` is `success`, tell me exactly how to inspect the root session:
1. In OpenCode TUI, run `/sessions` and switch to `rootSessionID`.
2. Or run `cliOpenCommand` from a shell.

If `status` is `error`, explain that no root session is available yet and include the recorded error.
