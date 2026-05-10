---
description: Reconcile runtime artifacts and report the next orchestrator/subagent action
agent: build
subtask: false
---

Run:
!`PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile --ledger .opencode/runtime/orchestrator-ledger.json --source-session-id supervisor-reconcile`

Then read:
- `.opencode/runtime/orchestrator-ledger.json`
- `.opencode/runtime/new-session-request.json` when it exists
- `.opencode/runtime/new-session-result.json` when it exists

Report:
- current role/stage/status from the ledger
- the supervisor decision summary
- whether the decision requires a subagent delegation or a fresh main_orchestrator root session

Behavior notes:
- Recovery may attempt one GitHub sync by running `python3 scripts/issue_packet_intake.py --output-dir docs/agents/issue-packets` when no eligible local next issue packet exists.
- That fallback requires `gh` auth plus network access to `paulpai0412/wferp`.
- If no request is queued after reconcile, that is expected for `issue_worker`, `pr_verifier`, and `release_worker` decisions; the root orchestrator should delegate those as subagents.

Do not wait for human confirmation if the decision names a subagent role. Delegate it from the current root orchestrator. Explicit dispatch is only for fresh `main_orchestrator` root sessions.
