# Refactor Candidate Loop

## Status

Draft workflow spec. This document records the agreed operating model for the
`phase_refactor` step that runs after `e2e_test_case_issue_loop` and before
documentation/tracker finalization.

## Purpose

Use refactor only as behavior-preserving architecture hygiene. The phase starts
with a compact audit, creates one GitHub issue per approved refactor candidate,
and routes each issue through the same worker, verifier, and release-worker
separation used by feature and E2E test case issues.

This phase does not restore `phase_full_e2e_and_qa`. E2E test case issues are
the phase-level E2E mechanism; refactor verification uses impact-based
regression over the affected buckets.

## Lifecycle

1. **Prerequisite**
   - `e2e_test_case_issue_loop` has passed or released all materialized E2E test
     case issues.
   - Product behavior is frozen for the PRD phase.

2. **Audit candidates**
   - Run a refactor audit over completed issue evidence, failure registry,
     worker/verifier suggestions, and code structure.
   - Record the result in a compact audit artifact under `docs/agents/refactor/`.

3. **Risk gate**
   - Low-risk, high/medium-value candidates may become `ready-for-agent`.
   - Medium-risk candidates need a clear regression bucket and rollback plan.
   - High-risk candidates become `ready-for-human` or are split smaller.

4. **Issue execution**
   - Create one GitHub issue per approved candidate.
   - The issue worker may perform only safety refactor changes.
   - A fresh verifier validates behavior using impact-based regression.
   - The release worker merges only after verifier evidence passes.

5. **Metadata update**
   - If a refactor resolves an E2E hotspot, update E2E catalog/registry metadata
     refs only.
   - Do not change E2E expected outcomes or acceptance criteria inside refactor.

6. **Skip path**
   - If no candidates exist, write a compact skipped audit and continue to
     `documentation_and_tracker_update`.

## Candidate sources

- Duplicated logic.
- Unstable E2E repair hotspots.
- High `failure_signature` concentration.
- Tightly coupled modules.
- Hard-to-test public surfaces.
- Repeated fixture or harness repair.
- Long or fragile functions.
- Unclear module boundaries.
- Worker or verifier suggestions, queued for audit before issue creation.

## Allowed safety refactors

- Rename symbols or files without changing public behavior.
- Extract functions, classes, or modules.
- Remove duplication.
- Simplify control flow.
- Improve boundaries and reduce coupling.
- Improve testability without changing expected behavior.

## Forbidden changes

- New product behavior.
- UX changes.
- SQL semantics changes.
- Public API or CLI contract changes.
- Expected output changes.
- Acceptance criteria changes.
- E2E expected outcome changes.

If a product or design change is required, stop refactor and create a new issue
or PRD/spec repair issue.

## Impact-based regression

The refactor verifier maps changed paths and public surfaces to affected
regression buckets:

```yaml
impact_based_regression:
  changed_paths: ["<repo-path>"]
  affected_surfaces: ["cli|api|browser|library|sql|static-html"]
  required_buckets: ["<focused-regression-bucket>"]
  uncertain_impact_fallback: "rerun_all_materialized_e2e_test_case_regression_buckets"
```

If impact is unclear, rerun all materialized E2E test case regression buckets.

## Labels

Refactor issues use canonical triage labels plus supplemental labels:

| Supplemental label | Meaning |
| --- | --- |
| `refactor-candidate` | Behavior-preserving refactor candidate. |
| `blocked-by-spec` | Needs PRD/spec decision before refactor can continue. |
| `blocked-by-risk` | Risk gate failed; split smaller or send to human. |

## Pass condition

```yaml
pass_condition:
  safety_refactor_only: true
  expected_behavior_unchanged: true
  impact_based_regression: pass
  verifier_packet: pass
  proof_of_separation: true
```

## Integration points

- `docs/agents/autonomous-development-workflow.yaml`: owns phase ordering,
  risk gate, and issue loop policy.
- `docs/agents/refactor/refactor-candidate-audit-template.yaml`: compact audit
  artifact shape.
- `docs/agents/triage-labels.md`: supplemental refactor labels.
- `docs/agents/e2e/test-case-catalog.yaml`: optional metadata refs to completed
  refactor issues.
- `docs/agents/e2e/failure-registry.yaml`: optional `refactor_issue_refs` when a
  refactor fixes a failure hotspot.
