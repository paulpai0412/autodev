---
description: Launch independent autodev release worker for PR merge
agent: build
subtask: false
---

Run the independent release path for issue number `$ARGUMENTS` in the current project. If no issue number is provided, autodev selects the first verified issue waiting for release.

Run:
!`AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" release --project-root "$PWD" --issue-number "$1" --auto-approve`

Report the DB-backed release dispatch result and the `release_worker` session to resume.

Behavior notes:
- This command is separate from `/autodev-reconcile` so human PR approval waits do not block development scheduling.
- Auto approval applies only here in the release path: it bypasses the human merge approval gate, not verifier/check/mergeability/workspace-hygiene gates.
- The command claims `verified -> release_pending` only when release/merge ownership starts.
- Release outcomes must be recorded through `submit-artifact --artifact-kind release_result`.
