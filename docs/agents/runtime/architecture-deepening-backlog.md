# Architecture Deepening Backlog (DB-only control plane)

Status: proposed
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

---

## 2) Issue intake/dependency parsing deep module (P0)

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

---

## 3) Supervisor + lifecycle policy deepening (P1)

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

---

## 4) Prompt spec modularization (P1)

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

---

## 5) Control-plane repository seam tightening (P2)

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

---

## 6) Project bootstrap vs host packaging split (P2)

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

---

## Execution notes

- Preserve DB-only runtime contract at every phase.
- Keep `issues.current_session_id` as the single issue session pointer.
- Do not reintroduce file-backed runtime dependencies.
- Preserve release-root observer-only behavior for caller sessions after successful release dispatch.
