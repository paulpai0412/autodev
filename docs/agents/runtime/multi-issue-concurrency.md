# Multi-issue concurrency

## Status

- Status: active branch direction
- Branch: `db-only-control-plane`
- Depends on: `docs/agents/runtime/db-only-control-plane-spec.md`

## Purpose

Define the bounded multi-issue concurrency model for the DB-only runtime.

## Decision summary

- Concurrency is issue-scoped, not transcript-scoped.
- One workspace supervisor remains the single writer of control-plane truth even when multiple issues are active.
- Multiple issues may be active in the same workspace.
- The same issue may not have more than one active root orchestrator or active development path.
- `ready` is the only selectable state for new development work.
- `ready -> claimed` is the DB fence that reserves a development slot before root launch.
- Development work and release work use separate bounded capacities.
- Verification completion should release development capacity.
- Quarantining one issue must not collapse unrelated issue throughput.

## Why the branch is changing direction

The legacy runtime was explicitly serial: one selected issue, one branch, one orchestrator path at a time.

That keeps reasoning simple, but it caps throughput and makes the system less useful as a control plane. The DB-only rewrite should move to bounded concurrency while preserving deterministic issue ownership.

## Concurrency unit

The unit of concurrency is the issue.

Rules:

- one issue = one root orchestrator
- one issue = one active development path
- one issue may have many history rows, but only one current owner in `issues.current_session_id`
- child worker and verifier runs stay under that issue's root ownership model

## Core scheduler contract

The bounded scheduler is derived from SQLite state, not from process-local counters or filesystem projections.

Required rules:

1. one workspace supervisor remains the only control-plane writer
2. each reconcile cycle recomputes occupancy from `issues`
3. only issues in `ready` are eligible for new development selection
4. ready issues are ranked deterministically
5. the supervisor may claim multiple ready issues in one cycle up to available development capacity
6. the `ready -> claimed` transition is the reservation fence that prevents duplicate start before a root session exists
7. every claim, redispatch, quarantine, and release handoff remains auditable in `issue_history`

## Capacity model

The runtime has two independent bounded pools:

### Development capacity

Used for issues in:

- `claimed`
- `dispatching`
- `running`
- `verifying`

These states consume a development slot because the issue is still in the active implementation loop.

Additional rules:

- `claimed` consumes a development slot even when the root session has not been launched yet.
- `dispatching` consumes a development slot while the host adapter is attempting to create or sync the root session.
- `ready`, `verified`, `release_pending`, `completed`, `failed`, and `quarantined` do not consume a development slot.
- `quarantined` remains fenced from reselection until an explicit operator action changes its state.

### Release capacity

Used for issues actively being processed by the release mechanism.

Issues that are merely waiting for approval or merge timing do not need to block development capacity.

Required rules:

- release-slot occupancy is separate from development-slot occupancy
- `verified` never consumes a release slot by itself
- `release_pending` consumes a release slot only while an active release handler owns the issue
- a `release_pending` issue that is only waiting for approval, merge window, or another external policy gate must consume no slot

## Slot release rules

- `verified` releases the development slot.
- `release_pending` does not consume a development slot.
- `quarantined` keeps the issue fenced from duplicate start but does not permanently consume a development slot.
- `completed` and `failed` consume no slots.

## Claim, launch, and fencing sequence

The scheduler must treat issue ownership as a two-step reservation and launch flow:

1. compute free development capacity from current `issues` rows
2. rank eligible `ready` issues deterministically
3. transition a selected issue from `ready` to `claimed`
4. only after the claim fence succeeds may the host adapter attempt root launch
5. if launch succeeds, record `current_session_id` and move the issue into `dispatching` and then `running`
6. if launch fails before a live root session exists, release the reservation by moving the issue back to `ready`
7. if launch partially succeeds and a live root session already exists, the issue stays fenced and auditable rather than being silently returned to `ready`

This keeps duplicate-start prevention inside SQLite even when multiple issues are being claimed in the same reconcile cycle.

## Scheduler behavior

The supervisor should be able to:

1. compute available development capacity
2. rank ready issues deterministically
3. claim and dispatch multiple ready issues in one cycle up to available capacity
4. preserve issue isolation across all in-flight issues

The scheduler must not:

- launch the same issue twice
- let one blocked issue stall unrelated ready issues
- choose issues non-deterministically when capacity is limited

Determinism requirement:

- when the same input issue set and capacity are presented twice, the selected issue order must be identical
- any ranking policy is allowed only if it is deterministic and explainable from stored issue data

## Duplicate-start protection

Duplicate-start protection is DB-only.

Required guard:

- if an issue is already in `claimed`, `dispatching`, `running`, `verifying`, or `quarantined`, the supervisor must reject any second root launch for that same issue
- issues in `verified`, `release_pending`, `completed`, or `failed` are not eligible for new development selection because `ready` is the only selectable state

This protection must not rely on file locks.

## Quarantine and recovery behavior

Quarantine is issue-local, not workspace-global.

Required rules:

- an active issue may be quarantined without pausing unrelated issues
- quarantining an issue removes it from the active development set and frees that development slot for later reconcile cycles
- a quarantined issue remains fenced from reselection until an explicit operator action resumes or fails it
- resume may either re-enter `claimed` for a fresh redispatch or re-enter `running` when the operator is deliberately reattaching to a known live session
- fail-quarantined affects only the targeted issue and must not reduce unrelated issue capacity

## Release interaction

Release is decoupled from development concurrency.

That means:

- a verified issue can wait for human approval
- the next ready issue can still enter the development loop
- release throughput can be tuned independently from development throughput
- `/autodev-release [issue-number]` can claim a verified issue into `release_pending` and launch release/merge work on its own cadence

The development loop and release loop therefore have separate backpressure:

- release congestion must not stop new `ready` issues from being selected when development capacity exists
- human approval waits must not consume development capacity
- release capacity may be saturated without reducing development-slot availability
- a `release_pending` issue only consumes release capacity while `current_session_id` or an active release status shows a release owner is running

## Monitoring implications

The monitor must reason per issue, not globally.

Required monitor behavior:

- stalled issue A does not imply issue B must stop
- quarantining one issue does not collapse unrelated issue capacity
- slot accounting must be derived from issue states and current role/status

Additional rules:

- monitor decisions must be recorded per issue, not as one global runtime health bit
- occupancy must be recomputed from SQLite on every reconcile/monitor pass rather than incremented or decremented from process-local memory alone
- recovery actions for one issue must not rewrite or clear the `current_session_id` of another issue

## Operator requirements

The operator surface must stay issue-targeted even under concurrency.

Required actions:

- inspect current development-slot occupancy and release-slot occupancy
- inspect the set of active/fenced issues and their current owners
- quarantine one issue without pausing the rest of the workspace
- resume or fail one quarantined issue without rewriting unrelated issue state
- review why an issue is waiting in `verified` or `release_pending`
- launch the independent release command for a verified issue after approval/policy allows merge

## Suggested implementation order

1. introduce explicit slot accounting in the supervisor
2. teach reconcile to select multiple ready issues up to capacity
3. add duplicate-start tests for same-issue contention
4. add monitor tests for mixed healthy/stalled multi-issue runs
5. add release-capacity tests so approval waits do not block development throughput

## Acceptance criteria

This design is satisfied when:

1. multiple issues can be in active development concurrently
2. the same issue cannot be started twice
3. verified issues do not block unrelated ready issues
4. release waiting does not consume development capacity
5. quarantine of one issue does not pause unrelated in-flight issues
6. ready selection, claim fencing, and slot accounting are derived from DB state alone
7. restart can rebuild occupancy and fenced-issue understanding from SQLite alone
8. the behavior is enforced by DB state and tests, not by filesystem projections
