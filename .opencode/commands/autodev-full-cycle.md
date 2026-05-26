---
description: Run autodev full-cycle loop for current project
agent: build
subtask: false
---

Run the shared full-cycle loop script against the current consumer project.

Run:
!`AUTODEV_HOME="${AUTODEV_HOME:-/home/timmypai/apps/autodev}" PROJECT_ROOT="$PWD" python "$AUTODEV_HOME/scripts/autodev_full_cycle.py"`

Report the final cycle status and the latest control-plane summary.

Notes:
- This command does **not** copy the runner into the consumer repo; it always runs the shared runner from `AUTODEV_HOME`.
- Repo resolution is handled by `scripts/autodev_full_cycle.py` from the consumer project context (`REPO` env → consumer `.env` → `.autodev.yaml` → git origin).
