# DB-only control-plane spec

## Status

- Status: active branch contract
- Branch: `db-only-control-plane`
- Scope: clean-slate runtime rewrite for `autodev`
- Intent: replace all file-backed runtime control with a SQLite-only control plane built on exactly two tables: `issues` and `issue_history`

## Branch contract

This branch does **not** preserve backward compatibility with the current file-backed runtime.

- No migration is required.
- No dual write is allowed.
- No local runtime control artifacts are allowed.
- Old runtime docs remain only as historical reference until the rewrite deletes them.

## Goals

1. Keep all workflow control in SQLite.
2. Keep the runtime restartable from SQLite alone.
3. Keep a single-writer supervisor model.
4. Keep full auditability in `issue_history`.
5. Remove filesystem state as a workflow dependency.
6. Decouple release from the per-issue development loop.
7. Make verifier-owned acceptance the gate for formal PR creation.
8. Make the orchestration core host-agnostic so OpenCode, Claude Code, and Codex are adapters rather than control-plane dependencies.
9. Support bounded multi-issue concurrency within one workspace without allowing duplicate starts for the same issue.

## Non-goals

- Do not support the old ledger/request/result file contracts.
- Do not preserve issue-lock projections.
- Do not preserve checkpoint, issue-packet, handoff, or YAML result files as runtime inputs.
- Do not keep role-specific current-session columns.
- Do not let OpenCode-specific commands, session formats, or `.opencode/` runtime assumptions shape the core DB model.
- Do not make concurrency unbounded or best-effort. Every concurrent issue must remain fenced, auditable, and capacity-accounted.

## Hard rules

- The only runtime tables are `issues` and `issue_history`.
- Workflow control must not depend on any local JSON/YAML artifact.
- The supervisor is the only writer of control-plane truth.
- Root sessions and subagents may emit facts and results only; they do not own lifecycle transitions.
- Every control-plane decision must append to `issue_history` before the corresponding `issues` snapshot update is considered complete.
- `issue_worker` must not create the final GitHub PR.
- Formal PR creation belongs to the verifier path after acceptance has passed.
- Release and merge handling must not block the per-issue development loop from continuing to the next verified issue.
- Host-specific session launch, session polling, command UX, and plugin/skill packaging must live behind adapters.
- The core orchestration engine must run without importing OpenCode-specific runtime semantics.
- Concurrency is issue-scoped: multiple issues may run concurrently, but one issue may have only one active root orchestrator and one active development path.

## Removed runtime artifact model

The rewrite removes all of these as runtime dependencies:

- `.opencode/runtime/orchestrator-ledger.json`
- `.opencode/runtime/new-session-request.json`
- `.opencode/runtime/new-session-result.json`
- `.opencode/runtime/issue-locks/issue-<n>.json`
- `docs/agents/runtime/context-checkpoint.yaml`
- `docs/agents/issue-packets/issue-<n>.yaml`
- `docs/agents/handoffs/issue-<n>.yaml`
- `docs/agents/worker-results/*.yaml`
- `docs/agents/evidence/*.yaml`
- `docs/agents/release-results/*.yaml`

If the system needs any of the information that used to live in those files, it must read it from SQLite.

## Control-plane ownership

- `issues` stores current truth.
- `issue_history` stores the append-only audit log and the full bodies of artifacts that used to be written to disk.
- The supervisor owns lifecycle transitions, retry policy, dispatch state, quarantine, and completion.
- Agent outputs enter the system as structured payloads that the supervisor records into `issue_history` and then folds into `issues`.

## Host integration ownership

- The DB-only orchestration engine is host-agnostic.
- Host-specific concerns such as command wrappers, session ids, child-agent launch, background execution, and resume UX belong to host adapters.
- OpenCode, Claude Code, and Codex are expected integration targets, but none of them may define the control-plane schema.
- The preferred distribution model is plugin-first and skill-assisted: the plugin hosts executable integration logic, and skills remain thin guidance layers.

## Product positioning

- The product is a harness and control plane for coding-agent workflows.
- It sits above runtimes such as OpenCode, Claude Code, and Codex.
- Its value is durable state, orchestration, verification policy, release policy, recovery, audit, and operator control.
- It is not positioned as a replacement coding assistant or IDE agent shell.

## Table design

### `issues`

`issues` is the current snapshot for a single issue.

Required fields:

- `issue_number`
- `title`
- `branch`
- `state`
- `current_role`
- `current_stage`
- `current_status`
- `current_session_id`
- `attempts_json`
- `limits_json`
- `last_failure_json`
- `runtime_context_json`
- `issue_packet_json`
- `latest_refs_json`
- `last_history_id`
- `last_command_id`
- `last_event_at`
- `updated_at`
- `claimed_at`
- `dispatching_at`
- `running_at`
- `verifying_at`
- `verified_at`
- `release_pending_at`
- `completed_at`
- `failed_at`
- `quarantined_at`

Field responsibilities:

- `state` is the canonical lifecycle state.
- `current_role/current_stage/current_status` are the current workflow cursor.
- `current_session_id` is the host-neutral operator resume target for the issue's current owner.
- `attempts_json`, `limits_json`, and `last_failure_json` drive retry and recovery.
- `runtime_context_json` stores compact runtime context that used to be split across ledger, checkpoint, request, and result files.
- `issue_packet_json` stores the canonical compact issue input.
- `latest_refs_json` stores pointers to the latest relevant history rows, such as the latest handoff, checkpoint, dispatch request, dispatch result, worker result, evidence packet, or release result.

There is intentionally no `current_root_session_id` and no `current_verifier_session_id`.

- The current resumable owner lives in `current_session_id`.
- Child session ids are kept only in `issue_history.payload_json`.

### `issue_history`

`issue_history` is the append-only log for every control-plane fact and every artifact body that previously lived on disk.

Required fields:

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
- `body_text`
- `content_hash`
- `created_at`
- `unique_key`
- `session_seq`

Required `entry_type` values include:

- `state_transition`
- `dispatch_request`
- `dispatch_result`
- `issue_packet`
- `handoff`
- `checkpoint`
- `worker_result`
- `evidence_packet`
- `pr_opened`
- `release_result`
- `release_decision`
- `root_event`
- `execution_result`
- `admin_action`
- `github_sync`

Storage rules:

- `payload_json` stores normalized structured fields used by the runtime.
- `body_text` stores the original artifact body when preserving a human-readable version is useful.
- `content_hash` supports idempotency and audit.
- `unique_key` and `command_id` prevent duplicate writes.

## Lifecycle model

Canonical issue states:

1. `ready`
2. `claimed`
3. `dispatching`
4. `running`
5. `verifying`
6. `verified`
7. `release_pending`
8. `completed`
9. `failed`
10. `quarantined`

Required transitions:

- `ready -> claimed`
- `claimed -> dispatching`
- `claimed -> ready`
- `claimed -> quarantined`
- `dispatching -> running`
- `dispatching -> ready`
- `dispatching -> quarantined`
- `running -> verifying`
- `running -> quarantined`
- `verifying -> verified`
- `verifying -> failed`
- `verifying -> quarantined`
- `verified -> release_pending`
- `verified -> completed`
- `release_pending -> completed`
- `release_pending -> failed`
- `quarantined -> claimed`
- `quarantined -> running`
- `quarantined -> failed`

The supervisor must reject any transition outside this set.

## Concurrency model

- Concurrency is bounded and issue-scoped.
- One workspace supervisor may manage multiple in-flight issues concurrently.
- The same issue must never have more than one active root orchestrator or development path at once.
- Scheduler capacity is configured outside the control-plane tables, for example in plugin/host configuration, but occupancy is derived from `issues` state and cursor fields.
- `ready` is the only selectable state for new development work.
- `ready -> claimed` is the DB reservation fence that prevents duplicate start before a root session has fully launched.
- Each reconcile cycle must recompute occupancy from SQLite rather than relying on filesystem locks or process-local counters alone.

Development-slot occupancy:

- Issues in `claimed`, `dispatching`, `running`, or `verifying` occupy a development slot.
- Issues in `verified`, `release_pending`, `completed`, or `failed` do not occupy a development slot.
- `quarantined` issues remain fenced from duplicate start but do not consume a development slot indefinitely.
- Quarantining one issue must not reduce development throughput for unrelated ready issues beyond the single slot already held by that issue.

Release-slot occupancy:

- Release work uses a separate bounded pool.
- A verified issue may wait for release without blocking selection of the next development issue.
- Human approval waits must not consume development capacity.
- `verified` means verifier-owned acceptance has passed and the issue is releasable, but no release handler currently owns it.
- `release_pending` means release handling has started or been explicitly queued for release ownership.
- A `release_pending` issue consumes a release slot only while an active release handler owns it; policy waits must consume no slot.

Selection rules:

- The supervisor may claim multiple ready issues in one reconcile cycle up to the available bounded capacity.
- Ranking remains deterministic and auditable.
- Duplicate start prevention is enforced by issue state plus `current_session_id`, not by filesystem locks.
- Every claim and release of capacity must be explainable from persisted issue state and history rows.

## Session model

- `main_orchestrator` remains the top-level orchestration owner for an issue.
- `issue_worker` and `pr_verifier` remain the per-issue execution units in the main development loop.
- Release handling is separated from the main development loop and may run through a dedicated release coordinator or merge worker launched by an explicit release command.
- The issue snapshot tracks only one resumable `current_session_id`.
- Child task session ids are recorded in `issue_history` so they stay auditable without expanding the `issues` schema.

## Per-issue development loop

The per-issue loop ends when an issue is verified and a formal PR exists.

Required ownership:

- `issue_worker` owns implementation, local validation, commit creation, and branch readiness.
- `issue_worker` may push a branch when remote verification needs it.
- `issue_worker` must not create the final PR.
- `pr_verifier` owns acceptance, final verification, and formal PR creation.
- `pr_verifier` writes verifier-owned evidence and records the opened PR in SQLite.

A verifier pass always moves the issue into `verified` first.

- `verified` is the canonical post-acceptance waiting state.
- `verified -> release_pending` happens only when a release handler or release queue explicitly takes ownership.
- If no separate release owner is needed, the issue may move directly from `verified -> completed`.

## Dispatch model

- Dispatch requests are stored as `issue_history(entry_type='dispatch_request')` rows.
- Dispatch results are stored as `issue_history(entry_type='dispatch_result')` rows.
- A successful root dispatch moves `claimed -> dispatching -> running` and records `current_session_id`.
- A failed pre-root dispatch returns the issue to `ready`.
- A post-root sync failure must not erase a live session from SQLite.

## Host adapter model

- The core engine exposes host-neutral operations such as start issue, reconcile, inspect, submit worker result, submit verifier result, and submit release decision.
- A host adapter translates those operations into host-native session and command behavior.
- The minimum adapter responsibilities are:
  - start a root session
  - launch a child role session or delegated agent unit
  - poll or collect normalized session outcomes
  - resume or link back to an active session
  - expose operator entrypoints in the host's plugin/command model
- Host-specific runtime metadata belongs in `issue_history.payload_json` or `issues.runtime_context_json`, not in dedicated schema columns.

## PR ownership model

- `issue_worker` returns a candidate implementation result, not a formal PR result.
- The worker result may include branch name, commit SHA, and implementation summary.
- The verifier is responsible for deciding whether the branch is reviewable.
- Only after verifier acceptance may the system create the formal PR and append `issue_history(entry_type='pr_opened')`.
- The canonical PR number, URL, and metadata belong to SQLite and are written after verifier-owned PR creation succeeds.

## Agent result model

- `issue_worker`, `pr_verifier`, and release handlers do not write local files.
- Each role returns a structured result payload.
- The supervisor records that payload as a `worker_result`, `evidence_packet`, `pr_opened`, `release_decision`, or `release_result` history row.
- Workflow gates read the normalized facts in SQLite, not file existence.

## Release model

- Release is not part of the required per-issue development loop.
- The runtime may hand verified issues to a dedicated release coordinator, merge worker, or scheduled release sweep.
- A release handler claims work from `verified` and then moves the issue into `release_pending` when release ownership actually begins; the shipped OpenCode command surface exposes this as `/autodev-release [issue-number]`.
- Human approval requirements, merge windows, and deployment policy belong to the release mechanism, not to the development loop.
- When approval is missing, the issue remains releasable without blocking development on the next issue.
- Post-merge cleanup and tracker closure remain release responsibilities.

## Selection and restart model

- Issue selection reads `issues` only.
- Supervisor restart rebuilds current runtime understanding from `issues` plus the latest relevant `issue_history` rows.
- The runtime must not require any local artifact to resume or reconcile.

## Operator model

Required operator actions remain:

- inspect current issue state and latest history
- inspect current development-slot occupancy and release-slot occupancy
- quarantine an issue
- resume a quarantined issue
- fail a quarantined issue
- retry a failed GitHub sync operation
- launch independent release/merge handling for a verified issue

All operator actions append auditable `admin_action` rows.

## Acceptance criteria

This spec is satisfied when:

1. Every workflow decision reads only `issues` and `issue_history`.
2. Restart and recovery work with no local runtime artifact present.
3. Duplicate starts are blocked by SQLite state alone.
4. Dispatch requests and results live only in `issue_history`.
5. Issue packets, handoffs, checkpoints, and worker/verifier/release outputs live only in SQLite.
6. Formal PR creation happens only after verifier-owned acceptance succeeds.
7. Release policy and human approval do not block the per-issue development loop from reaching `verified`.
8. Completion still requires verifier/release-owned evidence.
9. The branch contains no runtime code that depends on the removed file-backed contract.
