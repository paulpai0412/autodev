 # Current Architecture (DB-only control plane)

 Status: active branch baseline (post P0/P1/P2)
 Branch: `db-only-control-plane`

 This is the single-page architecture view for maintainers.
 It reflects the current code seams and runtime behavior.

 ## 1) Runtime truth and storage

 Runtime control is DB-only and lives in SQLite:

 - `.opencode/runtime/control-plane.sqlite3`
 - tables: `issues`, `issue_history`

 Primary modules:

 - `scripts/control_plane_db.py`
   - schema ownership + public runtime DB API
 - `scripts/control_plane_repository.py`
   - low-level repository seam:
     - `update_issue_snapshot(...)`
     - `append_history_entry(...)`
     - `count_development_occupancy(...)`
     - `count_release_occupancy(...)`

 Key rules:

 - `issues.current_session_id` is the only issue-level current-session pointer.
 - child session traces remain auditable in `issue_history.payload_json`.
 - file artifacts are not runtime dependencies.

 ## 2) Host adapter seam

 The orchestration core is host-agnostic. Host behavior is isolated behind adapter interfaces.

 Modules:

 - `scripts/host_adapter.py`
   - contracts:
     - `SessionStartContext`
     - `SessionStartResult`
     - `SessionOutcome`
     - `HostAdapter` protocol
   - shared typed fallback helper:
     - `session_result_field(...)`
 - `scripts/orchestrator_sessions.py`
   - adapter registry/factory + resolution:
     - `register_host_adapter_factory(...)`
     - `host_adapter_factory(...)`
     - `resolve_host_adapter(...)`
   - env selector:
     - `AUTODEV_HOST_ADAPTER` (default `opencode`)
 - `scripts/opencode_host_adapter.py`
   - shipped OpenCode adapter implementation

 Packaging seam:

 - `scripts/autodev_host_packaging.py`
   - host command entrypoint/templating only
   - keeps bootstrap/runtime logic separate from host UX wiring

 ## 3) Selection and dependency seam

 Dependency parsing and selection projection are split into focused modules.

 Modules:

 - `scripts/issue_dependency.py`
   - canonical dependency parsing primitives
 - `scripts/issue_selection_projection.py`
   - deterministic readiness/base-branch projection:
     - `dependency_issue_numbers_for_selection(...)`
     - `resolve_issue_base_branch_from_completed(...)`
     - `readiness_rank_score(...)`
 - `scripts/orchestrator_selection.py`
   - DB-backed intake + candidate selection orchestration

 ## 4) Policy and prompt seams

 Decision classifiers and prompt generation are separated.

 Modules:

 - `scripts/orchestrator_policy.py`
   - reconcile route classifier
   - release admission and selection helpers
   - dispatch admission/restore validation
 - `scripts/orchestrator_requests.py`
   - structured prompt/request builders:
     - `PromptSpec`
     - `SessionRequestSpec`
     - role/stage-specific prompt section helpers

 ## 5) Supervisor composition boundary

 `scripts/orchestrator_supervisor.py` is the composition layer.

 It wires:

 - DB APIs (`control_plane_db`)
 - lifecycle helpers (`orchestrator_lifecycle`)
 - selection helpers (`orchestrator_selection`)
 - policy classifiers (`orchestrator_policy`)
 - request/prompt builders (`orchestrator_requests`)
 - host adapter seam (`orchestrator_sessions` / `host_adapter`)

 Role boundaries:

 - `main_orchestrator`: orchestration and routing only
 - `issue_worker`: implementation path
 - `pr_verifier`: acceptance + PR/evidence path
 - independent release root: foreground `release_worker` path

 ## 6) Capacity and concurrency model

 Scheduling is bounded and issue-scoped.

 - development capacity source: `AUTODEV_DEVELOPMENT_CAPACITY`
 - workspace scheduler entrypoint:
   - `scripts/orchestrator_supervisor.py reconcile-workspace`
 - same issue cannot be started twice (DB fence + session pointer rules)
 - release capacity is separate from development capacity

 ## 7) Operator entrypoints (current)

 Primary operator wrapper is `scripts/autodev_project.py`:

 - `init`
 - `install-commands`
 - `doctor`
 - `start`
 - `reconcile`
 - `reconcile-watch`
 - `release`
 - `show-session`

 Host command wrappers are generated from `autodev_host_packaging` and remain thin UX surfaces over the same DB-only runtime.
