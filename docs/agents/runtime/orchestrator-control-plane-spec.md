# Orchestrator control-plane spec

## Status

- Status: draft
- Scope: autodev runtime / scheduler / supervisor
- Intent: implementation-ready control-plane contract for centralized issue scheduling

## Problem

The current autodev runtime prevents duplicate starts per issue with repo-local locks and the GitHub `agent-in-progress` label, but the broader orchestrator control plane is still implicit in code and runtime artifacts. We need one explicit spec for scheduler ownership, DB truth, issue state transitions, GitHub sync rules, event ingestion, verification handoff, and cutover behavior.

## Goal

Define a centralized control plane that:

1. Gives one scheduler the final authority over issue lifecycle decisions.
2. Prevents duplicate dispatch across sessions and worktrees.
3. Keeps runtime truth in a DB-backed state machine instead of scattered local files.
4. Preserves the existing worker / verifier role separation.
5. Makes retries, recovery, audit, and reconciliation deterministic.

## Non-goals

- Do not let root sessions directly decide global issue state.
- Do not use GitHub as the primary source of truth.
- Do not support long-lived dual control planes during cutover.
- Do not introduce multi-layer issue/root leases in the first final design.
- Do not weaken verifier-owned acceptance gates.

## Final design decisions

### Control-plane ownership

- A **single active scheduler instance** owns global dispatch decisions.
- The scheduler runs as a **short-lived reconcile loop**, not a long-lived memory-heavy chat session.
- The central source of truth is a **DB**, not GitHub and not repo-local runtime JSON.
- Root orchestrators may emit **facts/events only**. They do not own lifecycle decisions.

### Lease and failover

- Fencing is applied **only at the scheduler instance layer**.
- The active scheduler holds a lease with heartbeat and TTL.
- If the lease TTL expires, a new scheduler may **take over immediately**.
- No issue-level lease and no root-session lease are part of the initial final design.

### Cutover strategy

- Rollout mode is **big bang**.
- There is **no kill switch** back to the old mode.
- Before cutover, existing old-model root sessions must **drain to zero**.
- The new control plane must not intentionally operate in a prolonged dual-control-plane mode.

### Dispatch and issue claiming

- An issue selected by the scheduler enters a **pre-dispatch state** before the root session starts.
- The pre-dispatch state is represented as **`claimed` / `dispatching`** in the DB.
- GitHub must also show this state via a **distinct label**, not by reusing `agent-in-progress`.
- GitHub remains a required coordination surface for operator visibility and manual start prevention.

### GitHub synchronization

- GitHub state synchronization uses **labels only**.
- If a DB state transition succeeds but the matching GitHub label update fails, the scheduler must **roll back the DB transition**.
- In this design, GitHub synchronization for dispatch-critical states is **strongly coupled** to state transitions, not best-effort.

### Idempotency and auditability

- Every scheduler decision produces a unique **command / decision ID**.
- The same command ID is attached to:
  - the DB transition
  - the GitHub sync attempt
  - the decision log entry
- Retries must be idempotent by command ID.

### Root event ingestion

- Root sessions write **directly to the DB**.
- Root sessions may **append facts/events only**.
- Root sessions must not directly update issue lifecycle state, lease ownership, ranking fields, or scheduler decisions.
- Events use **`event_id` + `session_seq`** for dedupe and per-session ordering.

### Completion and verification

- A root terminal event alone does **not** complete an issue.
- The scheduler moves an issue from `running` to **`verifying`** after a terminal root event.
- Final completion requires:
  - a root terminal event
  - required artifacts/evidence for the issue lane
- Verification is owned by a **fresh verifier worker**, not the root and not the scheduler directly.
- An issue in `verifying` still counts as **occupying the same capacity slot** until verification passes or fails.

## Runtime state model

### Scheduler state

- `active`
- `expired`
- `replaced`

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
  - scheduler selects issue
  - command ID created
  - GitHub `ready-for-agent` removed
  - GitHub `agent-dispatching` added

- `claimed -> dispatching`
  - root-session creation request written
  - dispatch attempt starts

- `dispatching -> running`
  - root session successfully created and acknowledged
  - GitHub `agent-dispatching` removed
  - GitHub `agent-in-progress` added

- `dispatching -> ready`
  - root session creation fails before start
  - dispatch-critical GitHub sync restored
  - claim released

- `running -> verifying`
  - root sends terminal event
  - verifier work is required

- `running -> quarantined`
  - timeout, heartbeat failure, or non-classifiable runtime inconsistency

- `verifying -> completed`
  - verifier evidence passes
  - lane-specific required artifacts exist
  - completion decision recorded

- `verifying -> failed`
  - verifier evidence fails or required artifacts are missing beyond retry policy

- `quarantined -> running`
  - fenced resume authorized by scheduler decision

- `quarantined -> failed`
  - recovery policy exhausted or quarantine resolves to terminal failure

## GitHub label mapping

This spec requires the following coordination labels:

- `ready-for-agent`
- `agent-dispatching`
- `agent-in-progress`
- `quarantined`

### Label rules

- `ready-for-agent`
  - issue is eligible for scheduler selection

- `agent-dispatching`
  - scheduler has claimed the issue and root startup is in progress

- `agent-in-progress`
  - root session exists and the issue is actively in-flight

- `quarantined`
  - the issue or root session requires controlled recovery and must not be treated as normally runnable

Only the scheduler may apply or remove runtime coordination labels.

## Capacity model

- Capacity is **bounded**, not unbounded.
- Capacity is counted per in-flight issue.
- One issue occupies one slot across:
  - `claimed`
  - `dispatching`
  - `running`
  - `verifying`
- Capacity is released only when the issue reaches:
  - `completed`
  - `failed`

This keeps verification debt from silently overfilling the system.

## DB model

The exact schema may vary, but the control plane must support these logical tables.

### `scheduler_leases`

Tracks active scheduler ownership.

Minimum fields:

- `scheduler_id`
- `lease_token`
- `heartbeat_at`
- `expires_at`
- `created_at`
- `replaced_by_scheduler_id`

### `issues`

Tracks canonical issue lifecycle state.

Minimum fields:

- `issue_number`
- `state`
- `rank_score`
- `lane`
- `current_root_session_id`
- `current_verifier_session_id`
- `last_command_id`
- `claimed_at`
- `running_at`
- `verifying_at`
- `completed_at`
- `failed_at`
- `quarantined_at`
- `last_event_at`

### `issue_events`

Append-only event/fact stream from root sessions.

Minimum fields:

- `event_id`
- `issue_number`
- `root_session_id`
- `session_seq`
- `event_type`
- `payload_json`
- `created_at`

Constraints:

- unique on `event_id`
- unique on `(root_session_id, session_seq)`

### `decision_log`

Append-only scheduler decision history.

Minimum fields:

- `command_id`
- `scheduler_id`
- `issue_number`
- `decision_type`
- `from_state`
- `to_state`
- `reason`
- `created_at`

### `github_sync_attempts`

Tracks GitHub coordination writes.

Minimum fields:

- `command_id`
- `issue_number`
- `intended_label_delta`
- `status`
- `attempt_count`
- `last_error`
- `updated_at`

## Reconcile loop contract

Each reconcile cycle must:

1. Acquire or confirm the scheduler lease.
2. Read eligible issues from DB truth, not from GitHub alone.
3. Rebuild deterministic ranking from stored fields.
4. Select at most the available bounded capacity.
5. Write decision records for every lifecycle transition.
6. Apply matching GitHub label changes synchronously for dispatch-critical transitions.
7. Roll back the DB transition if required GitHub sync fails.
8. Spawn or resume root/verifier work according to state.
9. Reconcile quarantined issues explicitly instead of silently skipping them.

## Ranking and selection

Issue selection must use a deterministic formula that can be recomputed later for audit and debugging.

Required inputs may include:

- dependency readiness
- age in `ready`
- lane priority
- retry / quarantine penalty
- verifier backlog pressure

The scheduler must be able to explain why one issue was chosen over another from stored fields alone.

## Recovery rules

### Scheduler crash

- A new scheduler may take over as soon as TTL expires.
- The new scheduler must not trust in-memory state from the old scheduler.
- Recovery is rebuilt from DB rows, decision log, and runtime-visible external state.

### Root timeout or heartbeat loss

- The issue transitions to `quarantined`.
- Recovery uses **fenced resume**, not blind duplicate restart.
- The scheduler decides whether to resume, fail, or hold the issue.

### Dispatch failure

- If dispatch fails before root startup completes:
  - revert to `ready`
  - remove `agent-dispatching`
  - restore `ready-for-agent`
  - keep the failed command in `decision_log`

## Verifier contract

- Verification is handled by a **fresh verifier worker**.
- The verifier reads compact refs and required artifacts.
- The verifier is the owner of completion evidence.
- The scheduler uses verifier output to transition:
  - `verifying -> completed`
  - `verifying -> failed`

The scheduler must not treat worker self-checks as final acceptance evidence.

## Operator and admin operations

Manual intervention is allowed only through explicit admin operations, not ad-hoc DB mutation.

Required operator actions:

- inspect scheduler lease status
- inspect issue state and latest decision
- quarantine an issue intentionally
- authorize fenced resume
- mark terminal failure with reason
- replay or retry a failed GitHub sync-safe command

All admin operations must create auditable decision records.

## Cutover runbook requirements

Before enabling the new control plane:

1. Stop selecting new issues in the old model.
2. Wait for old-model root sessions to drain to zero.
3. Confirm no issue remains in an ambiguous in-flight state.
4. Seed the DB-backed canonical issue state from the latest trusted runtime view.
5. Enable the scheduler lease path.
6. Enable DB-backed issue reconciliation.
7. Enable GitHub label enforcement for `agent-dispatching` and `agent-in-progress`.

Because this design has no kill switch, cutover validation must happen before the new scheduler is allowed to select work.

## Implementation slices

Recommended implementation order:

1. Introduce DB schema and repositories for leases, issues, events, decisions, and GitHub sync attempts.
2. Add canonical issue state machine and transition guards.
3. Add scheduler lease acquisition and TTL takeover.
4. Add `claimed` / `dispatching` flow with GitHub rollback-on-sync-failure semantics.
5. Move root event ingestion to append-only DB writes.
6. Add `verifying` state and verifier-owned completion transition.
7. Add quarantine and fenced resume flows.
8. Add operator/admin commands over auditable decision APIs.

## Acceptance criteria

This spec is satisfied when:

1. Only one scheduler instance can make lifecycle decisions at a time.
2. Duplicate starts for the same issue are prevented across sessions.
3. Dispatch-critical GitHub label sync failure cannot leave DB state falsely advanced.
4. Root sessions can emit facts/events without owning lifecycle decisions.
5. Completion requires verifier-owned evidence, not only worker/root terminal status.
6. Capacity is not released before verification finishes.
7. Scheduler failover can rebuild state from DB and logs without relying on prior chat memory.
8. Operators can inspect and recover quarantined issues through explicit auditable operations.
