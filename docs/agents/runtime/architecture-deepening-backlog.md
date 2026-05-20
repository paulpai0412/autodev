# Architecture Deepening Backlog (DB-only control plane)

Status: completed (P0/P1/P2 done on 2026-05-20)
Owner: autodev maintainers
Scope: branch `db-only-control-plane`

This backlog captures concrete deepening opportunities discovered from current runtime code, runtime docs, and tests.

## Priority order

1. Host Adapter Seam consolidation
2. Issue intake/dependency parsing deep module
3. Supervisor + lifecycle policy deepening
4. Prompt spec modularization
5. Control-plane repository seam tightening
6. Project bootstrap vs host packaging split

---

## 1) Host Adapter Seam consolidation (P0)

Status: completed (2026-05-20)

### Files

- `scripts/orchestrator_sessions.py`
- `scripts/host_adapter.py`
- `scripts/opencode_host_adapter.py`
- `scripts/opencode_session_trace.py`
- `scripts/orchestrator_supervisor.py`
- `scripts/autodev_project.py`

### Problem

Core modules still carry OpenCode-specific assumptions (`.opencode` runtime paths, default adapter wiring, host-specific session metadata). This reduces module depth at the core seam and hurts portability.

### Deepening direction

- Introduce a real adapter registry/factory seam in `orchestrator_sessions.py`.
- Narrow `host_adapter.py` interface to host-neutral typed outcomes (replace broad metadata dict usage in core paths).
- Keep session-trace parsing entirely in host adapter implementation.

### Benefits

- Higher leverage for adding Claude/Codex adapters.
- Better locality: host failures stay in adapter layer.
- Better testability: core can run against fake adapter without OpenCode behavior.

### ADR/branch contract alignment

- Aligns with `docs/agents/runtime/host-adapter-strategy.md`.
- Aligns with DB-only spec host-agnostic rule.

### Completion notes (2026-05-20)

- Consolidated typed outcome fallback behind host-neutral helper in `scripts/host_adapter.py`:
  - added `session_result_field(...)` and removed supervisor-local duplicate fallback logic.
- Updated `scripts/orchestrator_supervisor.py` to consume the host adapter outcome seam through shared helper instead of local metadata decoding.
- Kept host adapter registry/factory flow centered in `scripts/orchestrator_sessions.py` and aligned `scripts/autodev_project.py` host resolution to registry-based adapter resolution.
- Tightened host packaging seam typing in `scripts/autodev_host_packaging.py` so packaging config derives from `HostAdapter` contract directly.
- Regression status:
  - `python3 -m pytest tests/scripts/test_orchestrator_supervisor.py -q` passed.
  - `python3 -m pytest tests/scripts/test_autodev_project.py -q` passed.
  - `python3 -m pytest tests/scripts -q` passed.

---

## 2) Issue intake/dependency parsing deep module (P0)

Status: completed (2026-05-20)

### Files

- `scripts/issue_packet_intake.py`
- `scripts/orchestrator_selection.py`
- `scripts/orchestrator_supervisor.py`
- `tests/scripts/test_issue_packet_intake.py`
- `tests/scripts/test_orchestrator_selection.py`

### Problem

Dependency parsing/normalization and selection concerns are split across multiple shallow modules and wrappers. Test coverage repeats similar dependency formats in multiple files.

### Deepening direction

- Build one intake pipeline module for dependency normalization + candidate readiness projection.
- Keep supervisor selection calls thin and route through one interface.

### Benefits

- Higher leverage from one dependency contract.
- Better locality for parser/selection regressions.
- Reduced duplicated tests and fixtures.

### ADR/branch contract alignment

- Aligns with DB-only control-plane intake and deterministic selection direction.

### Completion notes (2026-05-20)

- Added deep projection module `scripts/issue_selection_projection.py` to centralize dependency normalization and readiness projection logic:
  - `dependency_issue_numbers_for_selection(...)`
  - `resolve_issue_base_branch_from_completed(...)`
  - `readiness_rank_score(...)`
- Updated `scripts/orchestrator_selection.py` to route dependency/base-branch/readiness ranking through the shared projection seam while preserving public API and selection behavior.
- Kept `scripts/issue_dependency.py` as the canonical dependency parser source and retained intake behavior compatibility.
- Regression status:
  - `python3 -m pytest tests/scripts/test_issue_packet_intake.py -q` passed.
  - `python3 -m pytest tests/scripts/test_orchestrator_selection.py -q` passed.
  - `python3 -m pytest tests/scripts/test_orchestrator_supervisor.py -q` passed.
  - `python3 -m pytest tests/scripts -q` passed.

---

## 3) Supervisor + lifecycle policy deepening (P1)

Status: completed (2026-05-20)

### Files

- `scripts/orchestrator_supervisor.py`
- `scripts/orchestrator_lifecycle.py`
- `scripts/orchestrator_reconcile.py`
- `scripts/control_plane_db.py`
- `tests/scripts/test_orchestrator_supervisor.py`

### Problem

`orchestrator_supervisor.py` remains a mega-module that mixes CLI entrypoints, dispatch/reconcile glue, lifecycle invariants, and persistence orchestration. Deletion test indicates several pass-through wrappers where complexity moves instead of concentrating.

### Deepening direction

- Create an explicit orchestration policy module with a narrow interface.
- Centralize claim/fence/quarantine/resume invariants behind one lifecycle seam.
- Keep supervisor as thin command adapter.

### Benefits

- Better module depth (small interface, rich implementation).
- Better locality for state transition bugs.
- Easier reasoning for AI agents and maintainers.

### ADR/branch contract alignment

- Aligns with DB-only spec single-writer + auditable transition model.

### Completion notes (2026-05-20)

- Added explicit orchestration policy module `scripts/orchestrator_policy.py` and moved role/stage routing plus admission/restore classifiers behind that seam.
- `scripts/orchestrator_supervisor.py` now delegates reconcile routing, release admission, request stale/alignment guards, and dispatch failure restore strategy to policy helpers instead of embedding all classifier branches inline.
- Lifecycle invariants continue to flow through `scripts/orchestrator_lifecycle.py` wrappers from supervisor; claim/fence/quarantine/resume behavior remains centralized at lifecycle seam.
- Regression status: `python3 -m pytest tests/scripts/test_orchestrator_supervisor.py -q` and `python3 -m pytest tests/scripts -q` pass after extraction.

---

## 4) Prompt spec modularization (P1)

Status: completed (2026-05-20)

### Files

- `scripts/orchestrator_requests.py`
- `tests/scripts/test_orchestrator_requests.py`

### Problem

Prompt generation currently behaves like a long template surface with string-heavy tests. Interface complexity approaches implementation complexity, making this module shallow and noisy to evolve.

### Deepening direction

- Split prompt composition into structured sections/spec objects.
- Keep renderer as final step; test section contracts directly.

### Benefits

- Better leverage for adding role/stage rules.
- Better locality for prompt contract regressions.
- Lower brittle string-assert maintenance cost.

### Completion notes (2026-05-20)

- `scripts/orchestrator_requests.py` now exposes structured prompt/request seams:
  - `PromptSpec` + `build_prompt_spec(...)` with renderer as final step.
  - `SessionRequestSpec` + `build_session_request_spec(...)` with serialization as final step.
- Role/stage prompt composition remains split into section helpers (`_bootstrap_prompt_lines`, `_issue_worker_prompt_lines`, `_pr_verifier_prompt_lines`, `_release_root_prompt_lines`, `_recovery_or_selection_prompt_lines`) and rendered only at the end.
- Added direct spec contract tests in `tests/scripts/test_orchestrator_requests.py` for:
  - decision-summary last-line rendering,
  - selected-issue projection through request spec serialization.
- Regression status: `python3 -m pytest tests/scripts/test_orchestrator_requests.py -q` and full `tests/scripts` suite pass.

---

## 5) Control-plane repository seam tightening (P2)

Status: completed (2026-05-20)

### Files

- `scripts/control_plane_db.py`
- `tests/scripts/test_control_plane_db.py`

### Problem

DB access and domain-level projection semantics are intertwined in one large module. The module has depth but still exposes too much implementation detail to callers.

### Deepening direction

- Introduce clearer internal repository seams: snapshot writes, history append, occupancy read models.
- Preserve two-table runtime truth; avoid introducing new runtime stores.

### Benefits

- Better locality for query/projection defects.
- Cleaner test surface around domain expectations.

### ADR/branch contract alignment

- Must keep `issues` + `issue_history` as single runtime control-plane truth.

### Completion notes (2026-05-20)

- Added explicit repository seam module `scripts/control_plane_repository.py` and routed `scripts/control_plane_db.py` through it for:
  - snapshot writes (`update_issue_snapshot`),
  - history append insert path (`append_history_entry`),
  - occupancy read models (`count_development_occupancy`, `count_release_occupancy`).
- Preserved existing `scripts/control_plane_db.py` public API and transaction boundaries while reducing inline SQL coupling in high-churn call paths.
- DB-only runtime contract remains unchanged: runtime truth stays in SQLite `issues` + `issue_history` only.
- Regression status:
  - `python3 -m pytest tests/scripts/test_control_plane_db.py -q` passed.
  - `python3 -m pytest tests/scripts -q` passed.

---

## 6) Project bootstrap vs host packaging split (P2)

Status: completed (2026-05-20)

### Files

- `scripts/autodev_project.py`
- `.opencode/commands/*`

### Problem

Project bootstrap concerns and host command packaging concerns are still coupled. This weakens the seam between product core and host packaging adapter.

### Deepening direction

- Keep core bootstrap (init/doctor/control-plane checks) in one module.
- Move host command install behavior behind host packaging adapter seam.

### Benefits

- Better leverage for multi-host support.
- Better locality between consumer-project contract vs host UX wiring.

### Completion notes (2026-05-20)

- Added host packaging seam module `scripts/autodev_host_packaging.py` to isolate host adapter command packaging concerns:
  - `host_packaging_config_from_adapter(...)`,
  - `resolve_host_packaging_config(...)`,
  - `command_templates(...)`.
- Simplified `scripts/autodev_project.py` by delegating:
  - `_operator_entrypoints()` to host packaging config seam,
  - `_default_commands_dir()` to host packaging config seam,
  - `_command_templates()` to host packaging template renderer.
- Kept bootstrap/doctor/runtime control-plane responsibilities in `autodev_project.py` unchanged while moving host command wiring behind a dedicated seam.
- Regression status:
  - `python3 -m pytest tests/scripts/test_autodev_project.py -q` passed.
  - `python3 -m pytest tests/scripts -q` passed.

---

## Execution notes

- Preserve DB-only runtime contract at every phase.
- Keep `issues.current_session_id` as the single issue session pointer.
- Do not reintroduce file-backed runtime dependencies.
- Preserve release-root observer-only behavior for caller sessions after successful release dispatch.
