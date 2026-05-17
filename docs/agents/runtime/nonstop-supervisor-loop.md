# Nonstop Supervisor Loop Contract (DB-backed)

## Purpose

Define the runtime contract for a **nonstop** main orchestrator session that advances AFK issues using the SQLite control plane (`issues`, `issue_history`) as the single source of truth.

This document is authoritative for orchestrator behavior when prompts reference:

- `docs/agents/autonomous-development-workflow.yaml`
- `docs/agents/runtime/nonstop-supervisor-loop.md`

## Core rules

1. Bootstrap and reconcile from SQLite facts only.
2. Do not block on human replies when a deterministic next step exists.
3. Transition ordering is enforced by reconcile decisions, not ad-hoc agent memory.
4. Persist child-role outcomes via `submit-artifact` before advancing.
5. Main orchestrator owns dispatch sequencing; child roles own implementation/verification outputs.

## Required reconcile checkpoints

The main orchestrator must run supervisor reconcile at these checkpoints:

1. **Before first issue_worker launch**
   - Goal: persist bootstrap -> `issue_worker_execution` transition.
2. **After worker_result is written**
   - Goal: choose next role (normally `pr_verifier`).
3. **After evidence_packet is written**
   - Goal: move to `verified` and emit release wait decision.

If a child artifact is missing at a checkpoint, reconcile decides retry/recovery/quarantine according to DB attempts/limits.

## Role boundaries

### main_orchestrator

- May: validate contracts, run reconcile, dispatch next child role.
- Must not: implement issue scope directly, run final acceptance QA directly.

### issue_worker

- Owns: implementation, self-check, commit/push/PR prep.
- Must submit: `worker_result` via `submit-artifact`.

### pr_verifier

- Owns: independent acceptance verification and final evidence packet.
- Must submit: `evidence_packet` via `submit-artifact`.

### release_worker

- Launched independently via release command (not from the main issue root loop).
- Must submit: `release_result` via `submit-artifact`.

## Artifact submission contract

All child outcomes are written with:

```bash
AUTODEV_HOME="${AUTODEV_HOME:-~/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/orchestrator_supervisor.py" submit-artifact \
  --base-dir "<consumer-project-root>" \
  --issue-number <issue-number> \
  --artifact-kind <worker_result|evidence_packet|release_result> \
  --payload-json '<json>' \
  [--body-text '<text>']
```

Reconcile must treat SQLite artifact facts as authoritative and avoid runtime YAML/JSON artifact gates.

## Dispatch policy

- Child subagents run from the same root orchestrator session in foreground (`run_in_background=false`) when executing issue_worker/pr_verifier steps.
- The main orchestrator waits for each child call to finish before deciding next stage.
- Only next-issue handoff or explicit recovery may create a new root session.

## Stop conditions for this loop

For one selected issue, the nonstop loop is complete when:

1. `worker_result` stored,
2. `evidence_packet` stored with verifier result,
3. reconcile returns release wait / release command handoff.

At that point, merge/release proceeds through independent release flow.
