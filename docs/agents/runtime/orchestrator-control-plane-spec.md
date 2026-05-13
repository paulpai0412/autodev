# Orchestrator control-plane spec

## Status

- Status: active
- Scope: autodev runtime / supervisor / control plane
- Intent: implementation-ready contract for the shipped SQLite-only control plane

## Problem

The original autodev runtime mixed truth across GitHub labels, runtime JSON files, repo-local locks, and multiple SQLite append tables. That made recovery and duplicate-start prevention harder to reason about because different surfaces could disagree about what was actually true.

The current design resolves that by keeping **SQLite as the only control-plane source of truth** for lifecycle decisions while leaving runtime files in place only as projections, evidence, or dispatch handoff artifacts.

## Goal

Define the control-plane contract that the current runtime must satisfy:

1. Keep canonical issue truth in SQLite.
2. Prevent duplicate starts across sessions and retries.
3. Preserve worker / verifier / release role separation.
4. Make recovery, retries, audit, and reconciliation deterministic.
5. Keep GitHub as a coordination surface, not the source of truth.

## Non-goals

- Do not let root sessions directly decide global issue lifecycle state.
- Do not use GitHub as the primary source of truth.
- Do not require long-lived dual control planes.
- Do not weaken verifier-owned acceptance gates.

## Final design decisions

### Control-plane ownership

- The runtime operates in a **single-writer supervisor model** per workspace.
- The supervisor runs as a **short-lived reconcile loop**, not a long-lived memory-heavy chat session.
- The central source of truth is **SQLite**, not GitHub and not repo-local JSON/YAML artifacts.
- Root orchestrators and subagents may emit **facts, results, and evidence only**. They do not own lifecycle decisions.

### Fencing and duplicate-start prevention

- Canonical in-flight fencing lives in SQLite issue state: `claimed`, `dispatching`, `running`, `verifying`, and `quarantined` all block duplicate starts.
- `.opencode/runtime/issue-locks/issue-<n>.json` is retained only as a **projection/safety artifact** for operator UX and duplicate-start messaging.
- If a live root session is known, duplicate-start rejection should include a resume hint such as `opencode --session <rootSessionID>`.

### Dispatch and issue claiming

- An issue selected by the supervisor enters a **pre-dispatch state** before the root session starts.
- The pre-dispatch states are `claimed` and `dispatching`.
- GitHub shows this state with `agent-dispatching`; it must not reuse `agent-in-progress`.
- Claim projections must be cleaned up if claim-time sync fails or dispatch is rejected before a root session exists.
- Once a root session is successfully created, the issue stays fenced as `running` even if a later GitHub label sync fails. A live root session must not be silently forgotten.

### GitHub synchronization

- GitHub state synchronization uses **labels only**.
- GitHub remains a required coordination surface for operator visibility and manual-start prevention, but it is not canonical truth.
- If a DB transition succeeds but a **claim-time / pre-root** GitHub sync fails, the supervisor must roll the issue back and release the claim projection.
- If a **post-root-start** GitHub sync fails, the supervisor keeps SQLite truth aligned with the live root session and records the sync failure for retry/recovery instead of rolling the issue back to `ready`.

### Idempotency and auditability

- Every state-changing supervisor action produces a unique command / decision ID.
- The same command ID is attached to the SQLite transition and the relevant `issue_history` entries.
- GitHub sync attempts are recorded in `issue_history(entry_type = "github_sync")`.
- Retries must be idempotent by command ID or unique history key.

### Root event ingestion

- Root sessions and runtime helpers append facts into SQLite.
- Facts are stored in `issue_history`, not a separate append table.
- Root sessions must not directly update issue lifecycle state, ranking fields, or admin decisions.
- Root-session events use stable unique keys and per-session ordering.

### Completion and verification

- A root terminal event alone does **not** complete an issue.
- The supervisor moves an issue from `running` to `verifying` after the relevant root/runtime evidence is ingested.
- Final completion requires verifier / release evidence, not only worker self-report.
- Verification is owned by `pr_verifier` / `release_worker`, not by the root session and not by ad-hoc operator edits.
- An issue in `verifying` still counts as occupying the same capacity slot until it reaches `completed` or `failed`.

## Runtime state model

### Issue lifecycle state

Required canonical states:

1. `ready`
2. `claimed`
3. `dispatching`
4. `running`
5. `verifying`
6. `completed`
7. `failed`
8. `quarantined`

### Required state transition rules

- `ready -> claimed`
  - supervisor selects issue
  - command ID created
  - GitHub `ready-for-agent` removed
  - GitHub `agent-dispatching` added

- `claimed -> dispatching`
  - root-session creation request written
  - dispatch attempt starts

- `dispatching -> running`
  - root session successfully created and acknowledged
  - `current_root_session_id` recorded in SQLite
  - GitHub should move from `agent-dispatching` to `agent-in-progress`

- `dispatching -> ready`
  - root session creation fails before start or request is rejected before any live root session exists
  - claim projection released
  - GitHub dispatch-critical labels restored

- `running -> verifying`
  - root/runtime evidence shows implementation finished and verifier work is next

- `running -> quarantined`
  - timeout, heartbeat inconsistency, orphaned execution, or other runtime inconsistency that needs fenced recovery

- `verifying -> completed`
  - verifier / release evidence passes
  - required artifacts exist
  - completion decision recorded

- `verifying -> failed`
  - verifier / release evidence fails or required artifacts are missing beyond retry policy

- `quarantined -> running`
  - fenced resume authorized by explicit operator/supervisor decision

- `quarantined -> failed`
  - recovery policy exhausted or quarantine resolves to terminal failure

## GitHub label mapping

Required coordination labels:

- `ready-for-agent`
- `agent-dispatching`
- `agent-in-progress`
- `quarantined`

### Label rules

- `ready-for-agent`
  - issue is eligible for supervisor selection

- `agent-dispatching`
  - supervisor has claimed the issue and root startup is in progress

- `agent-in-progress`
  - root session exists and the issue is actively in flight

- `quarantined`
  - the issue or root session requires controlled recovery and must not be treated as normally runnable

Only the supervisor/operator control-plane commands may apply or remove runtime coordination labels.

## Capacity model

- Capacity is bounded.
- Capacity is counted per in-flight issue.
- One issue occupies one slot across:
  - `claimed`
  - `dispatching`
  - `running`
  - `verifying`
- Capacity is released only when the issue reaches:
  - `completed`
  - `failed`

## DB model

The canonical schema is intentionally simplified.

### `issues`

Tracks current truth for each issue.

Important fields:

- `issue_number`
- `state`
- `rank_score`
- `lane`
- `current_role`
- `current_stage`
- `current_status`
- `current_root_session_id`
- `current_verifier_session_id`
- `last_history_id`
- `last_command_id`
- `last_event_at`
- `updated_at`
- `claimed_at`
- `dispatching_at`
- `running_at`
- `verifying_at`
- `completed_at`
- `failed_at`
- `quarantined_at`
- `attempts_json`
- `limits_json`
- `last_failure_json`
- `resume_snapshot_json`
- `automation_flags_json`
- `artifact_refs_json`
- `issue_packet_json`

### `issue_history`

Append-only audit/history table for every control-plane fact.

Important fields:

- `history_id`
- `issue_number`
- `entry_type`
- `role`
- `stage`
- `status`
- `session_id`
- `request_id`
- `command_id`
- `from_state`
- `to_state`
- `summary`
- `payload_json`
- `created_at`
- `unique_key`
- `session_seq`

Required `entry_type` values include:

- `state_transition`
- `session_request`
- `session_result`
- `execution_result`
- `github_sync`
- `admin_action`
- `issue_packet_ingested`
- `checkpoint_snapshot`
- `root_event`

## Runtime artifact contract

These artifacts may still exist, but they are not canonical truth:

- `.opencode/runtime/orchestrator-ledger.json`
- `.opencode/runtime/new-session-request.json`
- `.opencode/runtime/new-session-result.json`
- `.opencode/runtime/issue-locks/issue-<n>.json`
- `docs/agents/issue-packets/issue-<n>.yaml`
- compact worker / evidence / release result artifacts

Usage rules:

- request/result/ledger files are dispatch handoff artifacts
- issue locks are duplicate-start safety projections
- issue packets and execution artifacts may be ingested into SQLite, but they must not override SQLite lifecycle truth after ingestion

## Reconcile loop contract

Each reconcile cycle must:

1. Read canonical issue truth from SQLite, not from GitHub alone.
2. Rebuild deterministic ranking from stored fields.
3. Select at most the available bounded capacity.
4. Write auditable `issue_history` entries for lifecycle transitions and sync attempts.
5. Apply matching GitHub label changes synchronously for dispatch-critical transitions.
6. Roll back only pre-root-start transitions when required GitHub sync fails.
7. Preserve fenced `running` state when a root session is already known.
8. Spawn or route root/verifier work according to SQLite state.
9. Reconcile quarantined issues explicitly instead of silently skipping them.

## Ranking and selection

Issue selection must use a deterministic formula that can be recomputed later for audit and debugging.

Required inputs may include:

- dependency readiness
- age in `ready`
- lane priority
- retry / quarantine penalty
- verifier backlog pressure

The supervisor must be able to explain why one issue was chosen over another from stored fields alone.

## Recovery rules

### Supervisor restart

- A fresh supervisor invocation must rebuild state only from SQLite plus currently visible runtime artifacts/evidence.
- It must not depend on prior chat memory.
- It must not trust stale projection files over SQLite truth.

### Root timeout or heartbeat loss

- The issue transitions to `quarantined`.
- Recovery uses fenced resume, not blind duplicate restart.
- The supervisor decides whether to resume, fail, or hold the issue.

### Dispatch failure

- If dispatch fails before root startup completes:
  - revert to `ready`
  - remove `agent-dispatching`
  - restore `ready-for-agent`
  - release the issue-lock projection
  - keep the failed command in `issue_history`

- If dispatch already produced a live `rootSessionID`:
  - keep the issue fenced as `running`
  - keep the root session recorded in SQLite and the issue-lock projection
  - record any GitHub label sync failure for operator retry/recovery

## Verifier contract

- Verification is handled by fresh verifier/release workers.
- Verifier/release artifacts are ingested as evidence and results.
- The verifier/release path owns completion evidence.
- The supervisor uses that evidence to transition:
  - `verifying -> completed`
  - `verifying -> failed`

The supervisor must not treat worker self-checks alone as final acceptance evidence.

## Operator and admin operations

Manual intervention is allowed only through explicit admin operations, not ad-hoc DB mutation.

Required operator actions:

- inspect canonical issue state and latest history/sync status
- quarantine an issue intentionally
- authorize fenced resume
- mark terminal failure with reason
- replay or retry a failed GitHub sync-safe command

All admin operations must create auditable history entries.

## Implementation slices

The shipped implementation order is effectively:

1. simplify the schema to `issues` + `issue_history`
2. add canonical issue state machine and transition guards
3. move root/session/event/sync audit into `issue_history`
4. keep runtime files only as projections or handoff artifacts
5. keep duplicate-start safety via SQLite state plus issue-lock projections
6. add quarantine, fenced resume, and late-result recovery flows
7. expose operator/admin commands over auditable APIs

## Acceptance criteria

This spec is satisfied when:

1. SQLite is the only canonical lifecycle source of truth.
2. Duplicate starts for the same issue are prevented across sessions.
3. Dispatch-critical GitHub sync failure cannot leave SQLite falsely advanced.
4. A live root session cannot be silently rolled back to `ready`.
5. Root sessions can emit facts/events without owning lifecycle decisions.
6. Completion requires verifier/release-owned evidence, not only worker/root terminal status.
7. Capacity is not released before verification finishes.
8. Operators can inspect and recover quarantined issues through explicit auditable operations.
