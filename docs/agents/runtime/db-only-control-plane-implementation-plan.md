# DB-only control-plane implementation plan

## Status

- Status: completed
- Branch: `db-only-control-plane`
- Depends on: `docs/agents/runtime/db-only-control-plane-spec.md`

## Planning intent

This is the branch-local execution ledger for the DB-only rewrite. It exists so later sessions can continue the remaining work without reconstructing architecture or progress from chat history.

## Implementation progress

- 2026-05-14: In progress. Current execution slice removes operator/bootstrap expectations around checkpoint and request/result runtime files so consumer-project entrypoints point at the SQLite control plane first.
- 2026-05-14: Discovery confirmed the largest remaining legacy seams are request/result file transport, issue-packet directory scanning, checkpoint updates, issue-lock projections, and artifact-path recovery logic.
- 2026-05-14: Removed issue-lock projections from active supervisor lifecycle control. Claim/redispatch fencing now persists only in SQLite issue state plus `artifact_refs_json`, and stale-root quarantine logic now reads the branch-contract `current_session_id` instead of role-specific session aliases.
- 2026-05-14: Verified the current DB-only slice with focused regressions: `tests/scripts/test_autodev_project.py`, `tests/scripts/test_orchestrator_bootstrap_runner.py`, `tests/scripts/test_orchestrator_supervisor.py`, `tests/scripts/test_orchestrator_monitor.py`, and `tests/scripts/test_control_plane_db.py` all pass.
- 2026-05-14: Made `scripts/orchestrator_monitor.py` fully DB-native by removing ledger-path inference from `collect_monitor_events`, `run_monitor_cycle`, `run_monitor_watch`, and the CLI. Monitor now runs from `--base-dir` plus SQLite state only.
- 2026-05-14: Moved supervisor operator commands (`inspect`, `quarantine`, `resume-quarantined`, `fail-quarantined`, `retry-github-sync`, `retry-failed`) off `--ledger` inference and onto explicit `--base-dir` / `--issue-number` DB-backed targeting.
- 2026-05-14: Re-verified the current rewrite cluster after the monitor/operator CLI conversion: `tests/scripts/test_autodev_project.py`, `tests/scripts/test_orchestrator_bootstrap_runner.py`, `tests/scripts/test_orchestrator_monitor.py`, `tests/scripts/test_orchestrator_supervisor.py`, and `tests/scripts/test_control_plane_db.py` pass together (`155 passed`).
- 2026-05-15: Removed supervisor checkpoint-file rewrites from active runtime paths. `scripts/orchestrator_supervisor.py` no longer mutates `docs/agents/runtime/context-checkpoint.yaml` during handoff/recovery/transition flow, and focused plus clustered DB-only regressions remained green.
- 2026-05-15: Removed session-result JSON writeback and legacy ledger resync from the DB dispatch path. `_dispatch_request_via_db(...)` now persists dispatch results only in SQLite while `_dispatch_consumed_request(...)` is reduced to a legacy request-file reader that forwards into the DB-backed dispatcher.
- 2026-05-15: Collapsed `scripts/orchestrator_supervisor.py init` into a DB-backed compatibility alias. The command now validates the packet against SQLite and delegates directly to `start_issue`, ignoring legacy `--write-request` / `--dispatch-now` artifact transport instead of creating ledger/request/result runtime files.
- 2026-05-15: Started Phase 5 YAML-gate cleanup by removing file-path-based waiting language from supervisor/reconcile prompts and transition state. Child-role progression now describes `worker_result`, `evidence_packet`, and `release_result` as SQLite-submitted facts rather than local YAML files, and focused supervisor/monitor regressions stayed green.
- 2026-05-15: Removed the last observed direct YAML artifact fallback from `scripts/orchestrator_supervisor.py`. Reconcile/monitor paths now depend on persisted SQLite artifact facts only, and the supervisor tests were updated to patch `_read_db_artifact_fact(...)` instead of the deleted file-read fallback helper.
- 2026-05-15: Removed legacy worker/evidence/release path hints from `scripts/orchestrator_requests.py` prompt text. Subagent prompts now instruct roles to use `submit-artifact` and DB-backed facts without reinforcing deprecated YAML artifact locations.
- 2026-05-15: Began migrating artifact runtime metadata from path-shaped keys toward semantic DB-ref keys. Supervisor/reconcile/monitor now write or prefer `worker_result_ref`, `evidence_packet_ref`, and `release_result_ref` while still mirroring legacy `*Path` keys during the transition so existing fixtures and operator compatibility remain intact.
- 2026-05-15: Tightened the initial ledger seed to emit semantic artifact refs only. `create_initial_ledger(...)` no longer prepopulates `workerResultPath`, `evidencePacketPath`, or `releaseResultPath`; transition-time compatibility writes remain in place while downstream fixtures and callers continue migrating.
- 2026-05-15: Removed transition-time legacy artifact-path dual writes from `scripts/orchestrator_reconcile.py`. Promotion from worker -> verifier and verifier -> release now writes only semantic refs (`evidence_packet_ref`, `release_result_ref`), and DB persistence tests no longer require `workerResultPath` compatibility storage.
- 2026-05-15: Removed runtime legacy artifact-path read fallbacks from `scripts/orchestrator_supervisor.py`, `scripts/orchestrator_monitor.py`, and `scripts/orchestrator_reconcile.py`. The active runtime now reads only semantic artifact refs and no longer carries `workerResultPath` / `evidencePacketPath` / `releaseResultPath` awareness in script code.
- 2026-05-15: Removed runtime filesystem cleanup/path leftovers from supervisor and monitor. Starting or retrying an issue no longer deletes worker/evidence/release YAML files, and `scripts/orchestrator_monitor.py` no longer carries an unused artifact-path resolver.
- 2026-05-15: Added a first-class `pr_opened` DB fact for verifier-owned PR creation. Verifier pass now records a dedicated `issue_history` / `latest_refs_json` entry before handing off to `release_worker`, instead of relying only on `evidence_packet.pr_number` inference.
- 2026-05-15: Aligned release handoff with the canonical state machine. Verifier pass now advances `verifying -> verified -> release_pending`, and release-worker retry/completion logic uses `release_pending` instead of keeping release work inside `verifying`.
- 2026-05-15: Closed the remaining release-state consistency gaps across reconcile/supervisor/lifecycle/state-machine enforcement. Queued `release_worker` stale-root fencing now quarantines from `release_pending`, verifier pass respects already-terminal issue rows, and focused regressions for `test_issue_state_machine.py`, `test_orchestrator_supervisor.py`, `test_orchestrator_monitor.py`, and `test_control_plane_db.py` pass together (`135 passed`).
- 2026-05-15: Landed the first DB-native bounded-concurrency accounting slice for Phase 6. `scripts/control_plane_db.py` now exposes development/release slot occupancy helpers and capacity-aware ready selection (`current_session_id` fenced ready rows are excluded); `scripts/orchestrator_selection.py` can return the ready set up to available development capacity; `scripts/orchestrator_supervisor.py start_issue(...)` now rejects duplicate starts when a ready row still carries an active `current_session_id`; and `scripts/orchestrator_monitor.py` reports development/release occupancy in selection-stall evidence. Focused regressions for `test_control_plane_db.py`, `test_orchestrator_supervisor.py`, and `test_orchestrator_monitor.py` pass together (`138 passed`), and the full scripts suite passes (`224 passed`).
- 2026-05-15: Completed the workspace-operator half of Phase 6. `scripts/orchestrator_supervisor.py` now exposes `reconcile_workspace_from_db(...)` plus a `reconcile-workspace` CLI that reconciles all active/fenced issues from SQLite before starting additional ready issues through the existing DB-backed `start_issue(...)` fence, with development capacity sourced from `AUTODEV_DEVELOPMENT_CAPACITY` (default `1`). `scripts/autodev_project.py reconcile` now targets the workspace reconcile path instead of selecting only one issue up front. Focused workspace regressions for `test_autodev_project.py` and `test_orchestrator_supervisor.py` pass (`124 passed`), the expanded runtime cluster passes (`161 passed`), and the full scripts suite passes (`227 passed`).
- 2026-05-15: Completed the first Phase 8 packet-removal slice. `scripts/issue_packet_intake.py` now treats `--project-root` plus SQLite ingestion as the canonical intake path; `scripts/orchestrator_selection.py run_issue_packet_intake(...)` no longer reparses packet files from stdout; and `scripts/orchestrator_bootstrap_runner.py` now treats `--issue-number` + SQLite as canonical while leaving `--issue-packet` as a compatibility-only wrapper around the same DB-backed start. Focused regressions for `test_issue_packet_intake.py`, `test_orchestrator_bootstrap_runner.py`, and `test_orchestrator_supervisor.py` pass together (`117 passed`).
- 2026-05-15: Removed the last active packet-path requirement from recovery dispatch validation and prompt construction. `scripts/orchestrator_requests.py` now tells roles to read DB-backed issue packet context instead of local packet files, queued-next-issue recovery requests no longer carry `selectedIssuePacketPath`, and `scripts/orchestrator_lifecycle.py` / `scripts/orchestrator_supervisor.py` no longer expose dead issue-lock file helper wrappers. Updated runtime/docs regressions for `test_orchestrator_supervisor.py`, `test_orchestrator_requests.py`, `test_autodev_project.py`, and `test_orchestrator_compact_payload.py` pass together (`138 passed`).
- 2026-05-15: Removed `issuePacketPath` as a live supervisor ledger dependency. `scripts/orchestrator_supervisor.py` now rebuilds DB validation and legacy packet hydration without relying on packet-path fields from the runtime ledger shape, while prompt fixtures in `test_orchestrator_requests.py` prove the DB-backed prompt contract no longer needs those path keys. Focused regressions for `test_orchestrator_requests.py`, `test_orchestrator_supervisor.py`, and `test_orchestrator_compact_payload.py` pass together (`117 passed`).
- 2026-05-15: Completed the final DB-only cleanup slice. `scripts/orchestrator_supervisor.py` no longer accepts `validation_ledger` as an external compatibility input, no longer backfills issue packets from legacy ledger-shaped data, and no longer carries ledger/request JSON read-write helpers. `scripts/control_plane_db.py` now exposes only `current_session_id` as the issue snapshot session pointer; role-specific current-session aliases were removed from runtime helper inputs and normalized rows. Dead artifact-base path helpers were removed from supervisor/selection. Probe script tests were added so the explicit coverage gate now passes. Verification: `python3 -m pytest tests/scripts -q` passes (`230 passed`), and `COVERAGE_FILE=/tmp/opencode/autodev.coverage /tmp/opencode/autodev-coverage-venv/bin/python -m pytest tests/scripts -q --cov=scripts --cov-report=term-missing --cov-fail-under=80` passes with total coverage `81.50%`.
- 2026-05-15: Completed independent release-worker dispatch. Verifier pass now stops the development loop at `verified`, root `main_orchestrator` prompts no longer launch `release_worker`, and `scripts/orchestrator_supervisor.py release` / `/autodev-release` explicitly claim `verified -> release_pending` before launching a standalone release worker for PR merge/release. Focused regressions for supervisor, request prompts, and command wrappers pass.

## Branch rules

- Reimplement instead of migrating.
- Prefer deletion over compatibility shims.
- Do not add fallback file writers or readers.
- Do not keep dual contracts alive across phases.
- The per-issue development loop ends at verifier-owned PR creation, not at merge.
- Release/merge handling must stay decoupled from the main issue-development loop.
- Keep the DB-only orchestration engine host-agnostic; host-specific runtime behavior must live behind adapters.
- Canonical post-verifier sequencing is `verifying -> verified -> release_pending -> completed|failed`, with `verified -> completed` allowed when no separate release owner is needed.

## Phase 0 - Freeze the contract

Goal: establish the branch-local rewrite rules before touching runtime code.

Deliverables:

- `docs/agents/runtime/db-only-control-plane-spec.md`
- `docs/agents/runtime/db-only-control-plane-implementation-plan.md`
- `docs/agents/runtime/host-adapter-strategy.md`
- `docs/agents/runtime/product-positioning.md`
- `docs/agents/runtime/multi-issue-concurrency.md`
- `AGENTS.md` note that these docs take precedence on this branch

Done when:

- Future sessions can recover the intended architecture from repo files alone.

## Phase 1 - Replace the SQLite API surface

Goal: make `scripts/control_plane_db.py` the only runtime persistence API needed by the rewrite.

Primary files:

- `scripts/control_plane_db.py`
- `tests/scripts/test_control_plane_db.py`
- `scripts/issue_state_machine.py`

Work:

- Redefine the schema around exactly two tables: `issues` and `issue_history`.
- Add helpers for:
  - reading/updating the `issues` snapshot
  - appending `issue_history` rows
  - idempotent command handling
  - reading latest history rows by `entry_type`
  - storing normalized result payloads and original bodies
- Replace role-specific current session columns with `current_session_id`.

Done when:

- No runtime API depends on ledger/request/result/artifact file paths.
- DB unit tests cover the new schema and append-then-update write pattern.

## Phase 2 - Extract the host adapter boundary

Goal: isolate OpenCode-specific runtime behavior behind an adapter so the core can later target Claude Code and Codex.

Primary files:

- `scripts/orchestrator_sessions.py`
- `scripts/opencode_session_trace.py`
- `scripts/autodev_project.py`
- `tests/scripts/test_orchestrator_sessions.py`
- `tests/scripts/test_opencode_session_trace.py`

Work:

- Define a host adapter interface for root launch, child launch, result polling, resume, and operator entrypoints.
- Move current OpenCode-specific process/session behavior behind an explicit OpenCode adapter.
- Normalize session ids, session outcomes, and resume links so the core stores only host-neutral values.
- Keep command/plugin packaging outside the DB-only core.
- Make the adapter contract explicit for `start`, `reconcile`, `inspect`, `doctor`, release handoff, and resume UX so `scripts/autodev_project.py install-commands` becomes packaging work rather than core-runtime behavior.

Done when:

- Core orchestration code no longer shells out to OpenCode directly.
- OpenCode-specific session and command behavior is isolated enough that a Claude Code or Codex adapter is an additive integration task.

## Phase 3 - Rewrite bootstrap, selection, and dispatch

Goal: root-session startup becomes DB-only.

Primary files:

- `scripts/orchestrator_supervisor.py`
- `scripts/orchestrator_requests.py`
- `scripts/orchestrator_selection.py`
- `scripts/autodev_project.py`
- `tests/scripts/test_orchestrator_supervisor.py`
- `tests/scripts/test_orchestrator_requests.py`

Work:

- Move issue selection, claim, dispatch request creation, and dispatch result recording into SQLite.
- Replace `--ledger`, `--new-session-request`, and `--new-session-result` driven flows with DB-native commands.
- Build prompts from DB context instead of checkpoint/ledger files.
- Record dispatch request and dispatch result rows in `issue_history`.
- Define the per-issue loop target state as `verified` rather than `merged`.
- Keep legacy entrypoints only as thin temporary wrappers when needed to preserve operator ergonomics during the rewrite; they must delegate into DB-native code and must stop being part of the runtime contract.

Done when:

- Starting a root orchestrator no longer writes or reads runtime files.
- The only startup dependency is the workspace DB.

## Phase 4 - Rewrite reconcile and monitor around DB state

Goal: the decision engine and monitor stop depending on filesystem state.

Primary files:

- `scripts/orchestrator_reconcile.py`
- `scripts/orchestrator_supervisor.py`
- `scripts/orchestrator_monitor.py`
- `tests/scripts/test_orchestrator_supervisor.py`
- `tests/scripts/test_orchestrator_monitor.py`

Work:

- Convert reconcile helpers to consume `issues` rows plus latest `issue_history` facts.
- Remove ledger-based transition helpers and revision logic.
- Make monitor detect stalls, orphaned execution, and quarantine conditions from SQLite only.
- Keep GitHub sync as an auditable side effect recorded in `issue_history`.
- Split reconcile logic into two tracks: the development loop (`running -> verifying -> verified`) and the release loop (`verified/release_pending -> completed/failed`).
- Eliminate operator/runtime dependence on `.opencode/runtime/orchestrator-ledger.json`, `.opencode/runtime/new-session-request.json`, and `.opencode/runtime/new-session-result.json` in `scripts/orchestrator_supervisor.py`, `scripts/orchestrator_monitor.py`, and `scripts/autodev_project.py reconcile`; legacy CLI flags may survive temporarily only as compatibility shims that delegate into DB-backed state.
- Move retry/quarantine/redispatch/inspect decision inputs off file-backed ledger snapshots and onto `issues` plus `issue_history` rows only.

Done when:

- Reconcile, retry, quarantine, and redispatch read only DB state.
- Monitor no longer compares DB state to local runtime artifacts.

## Phase 5 - Replace file-based agent outputs with structured DB events and verifier-owned PR creation

Goal: worker and verifier submit DB-native results; verifier owns formal PR creation.

Primary files:

- `scripts/orchestrator_requests.py`
- `scripts/orchestrator_supervisor.py`
- `scripts/orchestrator_reconcile.py`
- `tests/scripts/test_orchestrator_supervisor.py`
- `tests/scripts/test_orchestrator_monitor.py`

Work:

- Define structured result payloads for:
  - `issue_worker`
  - `pr_verifier`
  - `release_coordinator` or `merge_worker`
- Make `issue_worker` return branch-ready implementation results without creating the final PR.
- Make `pr_verifier` create the formal PR only after verifier-owned acceptance succeeds.
- Record verifier-owned PR creation as `pr_opened` and acceptance as `evidence_packet` in `issue_history`.
- Make all development-loop gates consume normalized DB facts rather than artifact paths or file existence.
- Remove runtime parsing of `docs/agents/worker-results/*.yaml`, `docs/agents/evidence/*.yaml`, and `docs/agents/release-results/*.yaml` from reconcile/monitor paths; those payloads must be submitted to SQLite first and then consumed from DB facts.
- Replace any remaining file-path-based waiting logic (for example worker/evidence/release artifact refs in supervisor prompts and transitions) with DB-backed references stored in `issue_history` / `latest_refs_json`.

Done when:

- No worker/verifier/release flow depends on YAML artifact generation.
- The worker can finish successfully without opening a PR.
- The verifier is the only role that can move the issue into a PR-backed verified state.

## Phase 6 - Add bounded multi-issue scheduling

Goal: allow multiple issues to run concurrently without losing issue isolation or deterministic control.

Primary files:

- `scripts/orchestrator_supervisor.py`
- `scripts/orchestrator_reconcile.py`
- `scripts/control_plane_db.py`
- `scripts/orchestrator_monitor.py`
- `tests/scripts/test_orchestrator_supervisor.py`
- `tests/scripts/test_orchestrator_monitor.py`
- `tests/scripts/test_control_plane_db.py`

Work:

- Add explicit capacity accounting for development slots and release slots.
- Define ready-only eligibility, deterministic ranking, and DB-derived occupancy as the scheduler contract.
- Allow the supervisor to select and claim multiple ready issues up to available development capacity.
- Make `ready -> claimed` the atomic DB fence before root launch.
- Keep one active root orchestrator per issue using DB fencing only.
- Ensure quarantine/resume is issue-local and does not collapse unrelated issue throughput.
- Make quarantined and verified issues stop blocking unrelated issue progress.
- Add tests for parallel issue selection, duplicate-start rejection, and slot release after verification.
- Add tests proving restart can rebuild slot occupancy from SQLite alone.

Done when:

- Multiple issues can progress concurrently in one workspace.
- The same issue cannot be started twice.
- Development throughput no longer depends on one global serial issue lane.
- Slot usage and same-issue fencing are explainable from `issues` and `issue_history` alone.

## Phase 7 - Add independent release coordination

Goal: merge, approval, and close happen outside the per-issue development loop.

Primary files:

- `scripts/orchestrator_supervisor.py`
- `scripts/orchestrator_reconcile.py`
- `scripts/orchestrator_monitor.py`
- `scripts/orchestrator_requests.py`
- `tests/scripts/test_orchestrator_supervisor.py`
- `tests/scripts/test_orchestrator_monitor.py`

Work:

- Introduce `verified` and `release_pending` handling in the control plane.
- Add a dedicated release coordinator or merge worker flow that scans verified issues.
- Expose an operator command that launches the release coordinator/merge worker independently of the issue root session.
- Move human approval policy, merge timing, and post-merge hygiene into that release flow.
- Ensure missing approval leaves an issue releasable without blocking selection of the next issue.

Done when:

- Merge policy no longer blocks the main issue-development loop.
- Release can run on its own cadence without reopening development-loop design decisions.
- PR merge can be triggered through a release command after human approval or release policy allows it.

## Phase 8 - Remove checkpoint, packet, handoff, and lock files from the model

Goal: delete the remaining file-backed concepts instead of merely ignoring them.

Primary files:

- `scripts/issue_packet_intake.py`
- `scripts/orchestrator_bootstrap_runner.py`
- `scripts/orchestrator_compact_payload.py`
- `scripts/orchestrator_artifacts.py`
- `scripts/orchestrator_lifecycle.py`
- `tests/scripts/test_orchestrator_bootstrap_runner.py`
- `tests/scripts/test_issue_packet_intake.py`

Work:

- Store issue packet content directly in SQLite.
- Store handoff and checkpoint bodies directly in `issue_history`.
- Remove issue-lock projections and file-based duplicate-start logic.
- Delete or collapse scripts whose only purpose was file creation/parsing.
- Remove `docs/agents/issue-packets/*.yaml` as a runtime source of truth by moving issue-packet intake and selection to SQLite-backed storage only.
- Remove `docs/agents/handoffs/*.yaml`, `docs/agents/runtime/context-checkpoint.yaml`, and `.opencode/runtime/issue-locks/*` from active runtime control; if compatibility wrappers remain temporarily, they must not be required for progress.
- Delete or neutralize file-oriented helpers that still encode the old runtime contract, including the packet/artifact parsing surfaces in `scripts/orchestrator_bootstrap_runner.py`, `scripts/orchestrator_compact_payload.py`, `scripts/orchestrator_artifacts.py`, and `scripts/orchestrator_lifecycle.py`.

Done when:

- The runtime has no code path that requires local JSON/YAML files to progress.

## Phase 9 - Delete stale docs, templates, and tests

Goal: make the repository tell only one story.

Primary files:

- `AGENTS.md`
- `README.md`
- obsolete historical runtime notes under `docs/agents/runtime/` that describe the pre-DB-only contract
- `docs/autodev-user-manual.zh-TW.md`
- all tests that still assert file-backed runtime behavior

Work:

- Rewrite docs to describe the DB-only runtime.
- Remove stale command examples that mention ledger/request/result files.
- Rewrite tests to seed SQLite fixtures directly.

Done when:

- Docs, commands, and tests no longer mention the removed runtime artifact contract except as historical notes.

## Expected removals

These paths are expected to be deleted entirely or reduced to thin wrappers before the branch is done:

- `scripts/orchestrator_bootstrap_runner.py`
- `scripts/orchestrator_compact_payload.py`
- `scripts/orchestrator_artifacts.py`
- file-oriented portions of `scripts/orchestrator_lifecycle.py`
- runtime artifact templates whose only purpose was writing local YAML/JSON files

These paths are expected to remain only as host-specific integration layers or be replaced by equivalent adapters:

- `scripts/orchestrator_sessions.py`
- `scripts/opencode_session_trace.py`
- OpenCode command wrappers under `.opencode/commands/`

These runtime concepts are expected to disappear completely:

- ledger revisions
- file-based dispatch queues
- file-based dispatch results
- issue-lock projections
- checkpoint compaction as a filesystem contract
- file existence as a workflow gate
- release as a mandatory per-issue tail stage
- worker-owned PR creation
- OpenCode-specific runtime assumptions inside core orchestration logic
- one-global-issue-at-a-time orchestration as a hard product limit

## Verification plan

Each implementation phase should end with:

1. focused DB tests for the touched API surface
2. focused supervisor/reconcile/monitor tests for the touched workflow slice
3. one restart/recovery scenario proving the new slice can recover from SQLite alone
4. one test proving that a verified issue can wait for release without blocking the next issue's development loop
5. one test proving that the core can run against a fake host adapter without OpenCode-specific behavior
6. one test proving that multiple issues can run concurrently while duplicate-start for the same issue is still blocked

## Branch exit criteria

The branch is ready to merge only when:

1. runtime control depends only on `issues` and `issue_history`
2. the old file-backed contract has been removed from code, tests, and docs
3. supervisor restart and operator recovery work without any runtime artifact on disk
4. the repo no longer teaches a dual-model runtime
5. release is modeled as an independent mechanism rather than a mandatory per-issue tail step
6. verifier-owned PR creation is enforced by code and tests
7. OpenCode is only one adapter, not a hard dependency of the core runtime
8. bounded multi-issue concurrency is enforced by code and tests
