---
description: Start the autonomous development workflow for a selected AFK issue number
agent: build
subtask: false
---

Run autodev for issue number `$ARGUMENTS` in the current consumer project.

1. Execute:
!`AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" start --project-root "$PWD" --issue-number "$1"`
2. Read `docs/agents/runtime/context-checkpoint.yaml` after the runner updates it.
3. Read `.opencode/runtime/orchestrator-ledger.json` after the runner initializes it.
4. Read `.opencode/runtime/new-session-result.json` after the runner dispatches the fresh session.
5. Tell me the checkpoint update result, the supervisor ledger path, the session result path, and the immediate next action.

Notes:
- `$ARGUMENTS` must be a GitHub issue number, for example `32`.
- Override `AUTODEV_HOME` first if the shared workflow repo is not installed at `~/apps/autodev`.
- This wrapper resolves the actual consumer project root from the current directory before dispatching the shared workflow.
- The runner resolves `docs/agents/issue-packets/issue-<n>.yaml` automatically.
- If the packet does not exist locally yet, the runner attempts one GitHub intake with `python3 scripts/issue_packet_intake.py --output-dir docs/agents/issue-packets`.
- GitHub intake defaults to `paulpai0412/wferp`; override it with `AUTODEV_GITHUB_REPO=<owner/repo>` when needed.

Do not start worker execution in this command session. Stop after reporting the runner output and the generated session result. The fresh `main_orchestrator` root session owns the selected issue end-to-end and must run worker/verifier/release roles as subagents. Future root dispatch is only for another `main_orchestrator` bootstrap or recovery handoff.
