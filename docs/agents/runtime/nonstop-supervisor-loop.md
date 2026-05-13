# Nonstop supervisor loop

This repo now has a runtime supervisor layer for the autonomous development workflow.

## Runtime artifacts

- `.opencode/runtime/orchestrator-ledger.json` is the machine-readable handoff artifact for the current issue, role, attempts, latest failure, and next dispatch target.
- `.opencode/runtime/control-plane.sqlite3` is the canonical control-plane store for current issue state plus append-only issue history.
- `.opencode/runtime/new-session-request.json` is the queue file consumed by the explicit `dispatch` fallback when a fresh `main_orchestrator` root session must be created.
- The configured root-session agent must survive checkpoint compaction, runtime-ledger handoff, and fresh-session dispatch; restore must not silently switch agents.
- `.opencode/runtime/new-session-result.json` records the created root session plus source-session stop status.
- `docs/agents/issue-packets/issue-<n>.yaml` remains the local execution source for selected issues, even when the issue first came from GitHub.
- `scripts/issue_packet_intake.py` is the GitHub-to-local materialization step for `ready-for-agent` issues.
- `.opencode/runtime/issue-locks/issue-<n>.json` is a duplicate-start safety projection, not canonical truth.

## Session chain contract

1. Orchestrator bootstrap writes the checkpoint, initializes the supervisor ledger, writes the first `new-session-request.json`, and immediately dispatches a fresh `main_orchestrator` root session.
2. The `main_orchestrator` root session owns the selected issue end-to-end. It delegates `issue_worker`, `pr_verifier`, and `release_worker` work to subagents rather than creating root sessions for those roles.
3. The root orchestrator launches each worker/verifier/release subagent with `task(..., run_in_background=false)` so the same root session waits for the child reply before moving on.
4. Each worker/verifier/release subagent may execute issue scope inside an issue worktree, but it must write compact artifacts back to the primary workspace's canonical repo paths recorded in the ledger so the supervisor can reconcile them deterministically.
5. `issue_worker` must not write `status: success` until its branch is pushed and the worker result contains finalized PR metadata (`pr.number` and `pr.url`). If PR creation or push is still incomplete, the worker must emit a blocked/failed artifact instead of an optimistic success result.
6. After each subagent writes its compact artifact, the orchestrator runs `PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile --ledger .opencode/runtime/orchestrator-ledger.json` and uses the returned decision to choose the next subagent or recovery action.
7. `reconcile --write-request --dispatch-now` is reserved for creating a new `main_orchestrator` root session during orchestrator bootstrap, recovery, or next-issue handoff; it must not be used to launch worker/verifier/release roles as root sessions.
8. `.opencode/runtime/new-session-result.json` records created root orchestrator sessions so operators can inspect or resume them if needed.
9. If recovery needs another `ready-for-agent` issue and no suitable local packet exists yet, the supervisor runs `python3 scripts/issue_packet_intake.py --output-dir docs/agents/issue-packets` and retries selection once.

## GitHub intake prerequisites

- `gh` must be installed and authenticated for the target repository.
- The runtime host must have network access to GitHub when intake fallback is expected.
- If GitHub is unavailable, the loop can continue only with already-materialized local issue packets.

## Default automatic routing

- `main_orchestrator` orchestrator bootstrap -> `issue_worker`
- `issue_worker success` -> `pr_verifier`
- `issue_worker blocked|failed` -> retry `issue_worker` up to 3 times, then queue `main_orchestrator` recovery
- `pr_verifier pass` -> `release_worker`
- `pr_verifier fail` -> `issue_worker` repair up to 3 worker cycles, then `main_orchestrator` recovery
- `pr_verifier blocked` -> retry `pr_verifier` only when marked retryable; otherwise `main_orchestrator` recovery
- `release_worker success` -> `main_orchestrator` selects the next ready issue and reruns orchestrator bootstrap
- `release_worker blocked` -> retry `release_worker` for transient blockers; otherwise `main_orchestrator` recovery

## Recovery principle

The supervisor prefers retry or reroute over `stop_for_human_decision`. Human intervention becomes the last resort when the orchestrator cannot select a next issue or cannot classify a terminal blocker honestly.

When `select_next_issue_packet(...)` finds no next local candidate, recovery tries one intake pass from GitHub before giving up. If intake also fails or still produces no eligible packet, the supervisor keeps control with `main_orchestrator` recovery and records the blocked state in the ledger instead of silently stalling.

## Control-plane operator commands

- `inspect` shows canonical issue state, latest decision, and latest GitHub sync attempt.
- `quarantine` moves a running issue into the explicit quarantined state.
- `resume-quarantined` performs fenced resume from `quarantined` back to `running`.
- `fail-quarantined` marks a quarantined issue as terminally failed.
- `retry-github-sync` replays only the latest failed GitHub label sync attempt for an issue and records an admin audit decision.
