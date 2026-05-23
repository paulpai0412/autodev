---
description: Run autodev full-cycle loop for current project
agent: build
subtask: false
---

Run the shared full-cycle loop script against the current consumer project.

Run:
!`AUTODEV_HOME="${AUTODEV_HOME:-/home/timmypai/apps/autodev}" PROJECT_ROOT="$PWD" REPO="${AUTODEV_GITHUB_REPO:-}" bash "$AUTODEV_HOME/autodev_full_cycle.sh"`

Report the final cycle status and the latest control-plane summary.

Notes:
- This command does **not** copy the script into the consumer repo; it always runs the shared script from `AUTODEV_HOME`.
- If `AUTODEV_GITHUB_REPO` is unset, export it first (example: `owner/repo`).
