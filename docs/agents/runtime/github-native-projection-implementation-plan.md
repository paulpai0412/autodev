# GitHub-native projection implementation plan (SQLite control plane → GitHub surfaces)

## Status

- Status: proposed + executable (implementation starting)
- Branch: `db-only-control-plane`
- Depends on:
  - `docs/agents/runtime/db-only-control-plane-spec.md`
  - `docs/agents/runtime/db-only-control-plane-implementation-plan.md`
  - `docs/agents/issue-tracker.md`

## Intent

Move operator-facing progress monitoring to **GitHub native UI** (Issues, comments, labels, Projects V2, PR, Release) while keeping SQLite (`issues`, `issue_history`) as the **single source of truth**.

This plan implements a one-way projection model:

- **Control plane truth**: SQLite
- **Projection surface**: GitHub
- **Recovery authority**: reconcile from SQLite facts, never from GitHub-only state

## Goals

1. Show issue progress and execution detail directly on GitHub issue pages with low-noise updates.
2. Keep PRD / user story / issue / PR / release relationships observable in GitHub-native constructs.
3. Make projection idempotent and auditable (every sync attempt recorded).
4. Add bounded retries, drift detection, circuit breaker, and read-only fallback.
5. Preserve DB-only runtime contract (no new file-backed runtime dependency).

## Non-goals

- Build a new bespoke web app UI.
- Make GitHub the authoritative workflow state store.
- Store full raw logs/transcripts in issue comments.

## Projection model

### A) GitHub issue body snapshot (authoritative summary projection)

Maintain a generated block in issue body, e.g.:

```md
<!-- autodev:projection:start -->
## Autodev status snapshot
- state: running
- role/stage/status: issue_worker / implementation / in_progress
- dependencies: #101, #103
- covers stories: #88, #89
- latest evidence ref: db:issue-history/evidence_packet:102:55
- updated_at: 2026-05-21T10:22:31Z
<!-- autodev:projection:end -->
```

Rules:

- Replace only inside marker block.
- Keep compact (summary + DB refs), no raw logs.
- Update only when projected content hash changes.

### B) Sticky status comment (timeline updates without spam)

Maintain a single bot comment per issue (upsert by marker, not append each tick):

- Marker: `<!-- autodev:status-comment -->`
- Update cadence: state transition / major gate change only.
- Content: concise checkpoint line items + DB refs.

### C) Projects V2 minimal fields (board-level visibility)

Synchronize minimal fields for operations monitoring:

- workflow state
- current stage
- owner/runtime session hint (compact)
- last sync timestamp

Only sync fields that have a stable mapping from SQLite; avoid speculative fields.

## Data mapping contract (v1)

From SQLite (`issues` + latest `issue_history` facts) to GitHub projection:

- `issues.state` → labels + Projects V2 status field
- `current_role/current_stage/current_status` → body snapshot + sticky comment + Projects V2 stage
- dependency graph (from issue packet context / parsed dependencies) → body snapshot section
- PR facts (`pr_opened`) → body snapshot + issue development panel linkage note
- release facts (`release_result`) → body snapshot + close comment summary
- verifier evidence (`evidence_packet`) → compact DB ref in body/comment

## Idempotency and sync safety

Every GitHub projection write must be guarded by deterministic keys:

- content hash of rendered projection payload
- `command_id` / `session_seq` correlation where available
- `issue_history(entry_type='github_sync')` append for every attempt/outcome

Behavior:

- same rendered payload → skip write
- transient failure → bounded retry
- repeated failure crossing threshold → open circuit breaker (read-only sync mode)

## Drift detection and reconcile

Add periodic reconcile check:

1. Read current GitHub projected surfaces (body block/comment/field values).
2. Re-render expected projection from SQLite.
3. If drift found, write correction (unless circuit open).
4. Record drift + correction in `github_sync` history rows.

GitHub manual edits policy:

- allowlist user-editable sections outside projection markers
- projection markers are autodev-owned and may be overwritten by reconcile

## Rollout phases

## Phase 0 — Plan + wiring map

Deliverables:

- this plan doc
- concrete insertion points for body/comment/project sync in existing lifecycle/supervisor paths
- test matrix for new projection behavior

## Phase 1 — Body snapshot projection

Deliverables:

- renderer for issue snapshot block
- upsert-in-body logic (marker replace or append)
- hash-based no-op skipping
- `github_sync` history recording for success/failure/skip

Acceptance:

- state transition triggers at most one body write when content changed
- repeated reconcile with identical state does not re-write

## Phase 2 — Sticky status comment projection

Deliverables:

- find/create sticky comment by marker
- update on transition gates only
- compact status lines + DB refs

Acceptance:

- timeline comment spam does not grow with each reconcile tick
- comment remains consistent after retries and process restart

## Phase 3 — Projects V2 minimal field sync

Deliverables:

- project item lookup + field mapping sync
- conditional update only on field delta
- retries + `github_sync` audit

Acceptance:

- board status matches SQLite state for sampled issues
- no blind writes when values already match

## Phase 4 — Drift reconcile + circuit breaker

Deliverables:

- periodic drift detection pass
- open/half-open/closed breaker states in runtime context
- operator visibility for sync health

Acceptance:

- broken GitHub sync does not block core workflow progression
- breaker opens on repeated failures and recovers after cooldown/probe success

## Implementation notes (branch contract alignment)

- No new runtime file artifacts.
- Persist sync facts via `issue_history` (`entry_type='github_sync'`).
- Keep any sync health projection in SQLite snapshot/runtime context only.
- Preserve host-agnostic core: isolate GitHub API invocation behind lifecycle/integration helpers, not scheduler policy code.

## Validation plan

1. Unit tests
   - body marker replacement renderer/upsert
   - sticky comment upsert selection
   - hash-based idempotency decisions
2. Integration-style script tests
   - transition triggers expected projection writes
   - retry path records `github_sync` attempts and final status
   - circuit breaker blocks writes but keeps issue lifecycle moving
3. Regression checks
   - `python3 -m pytest tests/scripts/test_control_plane_db.py -q`
   - `python3 -m pytest tests/scripts/test_orchestrator_lifecycle.py -q`
   - `python3 -m pytest tests/scripts/test_orchestrator_supervisor.py -q`

## Open items to lock while implementing

1. Exact Projects V2 field identifiers and fallback behavior when project item missing.
2. Sticky comment signature format and collision avoidance with non-autodev comments.
3. Threshold/cooldown constants for circuit breaker defaults.

These are implementation-level constants and should be finalized directly in code/tests during execution of this plan.
