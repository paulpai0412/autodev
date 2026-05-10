# E2E Test Case Issue Loop

## Status

Draft workflow spec. This document records the agreed operating model for
turning executable E2E test cases into GitHub issues that can be repaired,
verified, and released by the autonomous development workflow.

## Purpose

Use one GitHub issue as the execution unit for one executable E2E test case.
The issue worker may repair product behavior, test code, fixtures, harnesses,
or test data needed for that case. A fresh verifier worker owns final
acceptance. The release worker may merge and close only after verifier-owned
evidence passes.

This loop replaces the former `phase_full_e2e_and_qa` phase and complements the
existing `per_issue_e2e` phase in `docs/agents/autonomous-development-workflow.yaml`;
it does not make the main agent an E2E executor.

## Core model

| Concept | Policy |
| --- | --- |
| Issue unit | One GitHub issue per executable E2E test case. |
| Source of intent | PRD acceptance criteria, bug report, regression, or manual risk. |
| Worker scope | Product, test, fixture, harness, and test data seed repairs for the target case. |
| Final acceptance owner | Fresh verifier worker, never the issue worker or main orchestrator. |
| Release owner | Release worker after verifier evidence passes. |
| Evidence storage | Manifest/index references only; no raw logs, traces, or screenshots in repo docs or issue comments. |

## Test case timing

Generate E2E intent during feature specification, then materialize executable
test case issues only after feature implementation has passed its normal
worker/verifier loop.

During the PRD or feature spec stage, record draft test intent:

```yaml
e2e_test_intents:
  - test_case_id: "E2E-001"
    source_ac: "AC1"
    scenario: "<user-visible journey>"
    surface: "cli|api|browser|library|sql|static-html"
    expected_outcome: "<observable result>"
    risk_level: "high|medium|low"
    executable_status: "draft"
```

After feature development and PR verification pass, run an E2E materialization
gate. Only then convert draft intent into ready issues with actual setup,
commands, data, expected observations, and evidence requirements.

## Lifecycle

1. **Spec intent**
   - `grill-with-docs` or PRD drafting identifies user-visible journeys.
   - The PRD records `e2e_test_intents` mapped to acceptance criteria.
   - These intents are not ready for AFK execution yet.

2. **Feature implementation**
   - Normal issue workers implement the feature through the existing gated
     workflow.
   - Verifier evidence must pass before E2E test case issues are materialized.

3. **Materialize test case issues**
   - The orchestrator converts executable intents into one GitHub issue per
     test case.
   - Each issue starts with `needs-triage`, then becomes `ready-for-agent` only
     when setup, execution surface, expected outcome, regression bucket, and
     evidence requirements are explicit.
   - The orchestrator updates `docs/agents/e2e/test-case-catalog.yaml` when the
     issue is created.

4. **Issue worker repairs the case**
   - The issue worker runs the target case for implementation feedback.
   - If it fails, the worker may repair in-scope product/test/fixture/harness
     or test data seed problems.
   - The worker must not weaken assertions, change expected outcomes, or claim
     final E2E acceptance.
   - The worker emits a `worker_result` and, when relevant, opens a PR.

5. **Verifier reruns in a fresh session**
   - A fresh verifier worker reads the issue packet, worker result, PR diff,
     and artifact manifest references.
   - The verifier reruns the target case and required regression bucket.
   - The verifier emits an `evidence_packet` with proof of separation.

6. **Loop on failure**
   - If verifier status is `fail`, the orchestrator routes the same issue back
     to an issue worker unless loop caps are exceeded.
   - If verifier status is `blocked`, the orchestrator creates or links the
     blocking issue and updates labels/catalog state.

7. **Release**
   - The release worker checks verifier evidence, PR mergeability, required
     checks, and merge approval policy.
   - The release worker does not rerun E2E.
   - After merge, it performs post-merge workspace hygiene before closing the
     issue.

## Issue contract additions

Each E2E test case issue should include this compact contract, either directly
in the issue body or in an issue packet referenced from the issue:

```yaml
test_case:
  id: "E2E-001"
  source: {type: "prd_ac|bug_report|regression|manual_risk", ref: "<ref>"}
  surface: "cli|api|browser|library|sql|static-html"
  scenario: "<one user-visible journey>"
  setup: "<actual setup, service, fixture, or seed requirement>"
  execution: "<command, URL, driver, or manual surface path>"
  expected_outcome: "<observable pass condition>"
  regression_bucket: ["<focused checks verifier must rerun>"]
  evidence_requirements: ["target_case_result", "regression_bucket_result"]
  executable_status: "ready"
```

## Worker result additions

E2E issue workers should report repair classification without raw logs:

```yaml
test_case_repair:
  test_case_id: "E2E-001"
  attempted_target_case: true
  repaired_categories: ["product|test|fixture|harness|test_data_seed"]
  failure_signature: "<stable signature or none>"
  assertion_changed: false
  expected_outcome_changed: false
  final_acceptance_claim: false
```

## Evidence packet additions

Verifier evidence should record target case and regression results:

```yaml
test_case_verification:
  test_case_id: "E2E-001"
  target_case: "pass|blocked|fail"
  regression_bucket: "pass|blocked|fail"
  failure_signature: "<stable signature or none>"
  artifact_manifest_ref: "<bundle-ref>"
  proof_of_separation_required: true
```

## Labels

The existing canonical labels still control triage:

| Label | E2E meaning |
| --- | --- |
| `needs-triage` | Test intent exists, but issue may not be executable yet. |
| `needs-info` | Setup, expected outcome, source reference, or evidence requirements are missing. |
| `ready-for-agent` | Fully executable E2E test case issue. |
| `ready-for-human` | Requires product or test-design judgment that an AFK worker should not decide. |
| `wontfix` | Test case will not be actioned. |

E2E-specific labels such as `e2e-test-case`, `blocked-by-spec`, and
`blocked-by-infra` are documented in `docs/agents/triage-labels.md`, but those
labels must not replace the canonical triage labels.

## Guardrails

- The main orchestrator validates artifact shape, references, line caps, and
  role separation only; it must not run E2E acceptance QA directly.
- The issue worker may run the target case for feedback but must not emit final
  acceptance.
- The verifier worker must be a distinct fresh session from the worker and must
  record `proof_of_separation`.
- Raw logs, traces, SQL execution logs, screenshots, and verbose manual QA
  transcripts are stored outside repo docs and issue comments; repo artifacts
  reference them by manifest ID only.
- Worker changes must not weaken assertions or alter expected outcomes. If the
  expected behavior is wrong or ambiguous, classify the result as `spec_drift`.
- A `spec_drift` result creates or links a PRD/spec repair issue and marks the
  original test case issue blocked by spec.
- Repeated shared harness or infrastructure failures must not spin indefinitely
  inside each test case issue.
- Issue execution remains serial until a dedicated concurrency policy exists:
  one selected issue, one branch, one root orchestrator coordinating
  worker/verifier/release subagents, and one PR at a time.

## Loop control

Use these caps unless a workflow run explicitly declares stricter limits:

```yaml
loop_control:
  max_worker_verifier_cycles: 3
  max_same_root_cause_repairs: 2
  shared_failure_parent_issue_threshold: 2
```

When the same stable failure signature appears in two or more test case issues,
the orchestrator should create or link a parent infrastructure/harness issue and
update `docs/agents/e2e/failure-registry.yaml`.

## Pass condition

An E2E test case issue can be released only when verifier evidence records:

```yaml
pass_condition:
  target_case: pass
  required_regression_bucket: pass
  proof_of_separation: pass
  artifact_manifest_present: true
```

The regression bucket is hybrid: each surface has a default focused regression
set, and the test case may append additional checks. A test case must not remove
the default bucket for its surface.

## Proposed repo artifacts

| Path | Purpose |
| --- | --- |
| `docs/agents/e2e/test-case-catalog.yaml` | Operational index of E2E test case issues, status, source refs, PR refs, and latest verifier evidence. |
| `docs/agents/e2e/failure-registry.yaml` | Stable failure signatures, affected test cases, parent issue links, and current disposition. |

Both artifacts should use `schema_version: "1.0"`, a `kind`, `line_cap`, and
`raw_evidence_policy: index_only_refs_no_raw_logs_or_transcripts` when created.

## Integration points

Future implementation should update these contracts instead of inventing new
parallel formats:

- `docs/agents/autonomous-development-workflow.yaml`: run the E2E test case
  issue loop after feature verification and before `phase_refactor`.
- `docs/agents/issue-packet-template.yaml`: add optional `test_case` fields for
  executable E2E issues.
- `docs/agents/worker-result-template.yaml`: add optional `test_case_repair`
  fields.
- `docs/agents/evidence-packet-template.yaml`: add optional
  `test_case_verification` fields.
- `docs/agents/triage-labels.md`: document any E2E-specific labels while
  preserving canonical triage labels.

## Rollout plan

1. Keep this Markdown spec as the source of truth for the design.
2. Add compact YAML schemas to the packet/result/evidence templates.
3. Add `test-case-catalog.yaml` and `failure-registry.yaml` with empty initial
   state.
4. Extend the autonomous workflow runbook with the materialization and repair
   loop.
5. Trial the loop on one high-risk E2E test case before generating a large batch
   of issues.
