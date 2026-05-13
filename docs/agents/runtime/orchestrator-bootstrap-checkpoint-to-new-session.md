# Orchestrator bootstrap checkpoint-to-new-session spec

## Status

- Status: active MVP
- Owner: main orchestrator session
- Source of truth: `docs/agents/autonomous-development-workflow.yaml#context_budget_policy`

## Purpose

Define the minimum executable contract for orchestrator bootstrap in the autonomous workflow:

1. select the next ready AFK issue
2. persist a checkpoint for the selected issue
3. derive a compact payload from the checkpoint
4. write a fresh-session continuation request for explicit dispatch
5. continue into the selected issue PR flow from a fresh session

This flow reduces orchestrator context before new issue PR work by switching from checkpoint state into a fresh session instead of relying on native compaction.

## Applies to

- `main_orchestrator` only

## Does not apply to

- issue worker subagents
- PR verifier subagents
- phase verifier subagents
- release worker subagents
- no-ready-issue PRD completeness audit path

## Trigger

Trigger this flow only when all conditions are true:

1. the orchestrator is inside `issue_development_loop`
2. `issue_selection` returned an issue labeled `ready-for-agent`
3. the selected slice type is `AFK`
4. the orchestrator is about to enter `per_issue_flow`

Do not trigger from branch changes, git hooks, PR webhooks, or generic session events.

## Required inputs

- selected issue number
- selected issue branch name
- selected issue packet path
- latest prior handoff path when present
- current workflow policy path
- current orchestrator role

## Required outputs

- updated `docs/agents/runtime/context-checkpoint.yaml`
- derived compact payload embedded in the checkpoint
- updated `.opencode/runtime/orchestrator-ledger.json`
- written `.opencode/runtime/new-session-request.json`
- fresh orchestrator session ready to continue `per_issue_flow`

## Current implementation reference

- payload derivation and checkpoint rewrite helper: `scripts/orchestrator_compact_payload.py`
- orchestrator runner: `scripts/orchestrator_bootstrap_runner.py`
- nonstop supervisor and next-issue recovery: `scripts/orchestrator_supervisor.py`
- GitHub ready-issue materialization: `scripts/issue_packet_intake.py`
- OpenCode project command: `.opencode/commands/auto-dev.md`

## Algorithm

1. Run `issue_selection`.
2. If no `ready-for-agent` AFK issue exists, do not start `per_issue_flow`; return control to `prd_phase_completeness_audit` in the main workflow.
3. Populate `context-checkpoint.yaml` for the selected issue.
4. Initialize `.opencode/runtime/orchestrator-ledger.json` for the selected issue.
5. Derive `compact_payload` from the checkpoint using the rules below.
6. Write `.opencode/runtime/new-session-request.json` with a checkpoint-only bootstrap prompt.
7. Explicitly dispatch the fresh `main_orchestrator` root session from the written request and record the result under `.opencode/runtime/new-session-result.json`.
8. Continue `per_issue_flow` in the fresh session in this order:
   - `create_or_switch_issue_branch`
   - validate the selected local issue packet
   - delegate the first `issue_worker` as a subagent from the root orchestrator with `task(..., run_in_background=false)`
   - delegate remaining verifier/release roles as subagents from the same root orchestrator with `task(..., run_in_background=false)`

If any step before request generation fails, stop and report blocked; do not start worker execution.

When step 2 routes to `prd_phase_completeness_audit`, this orchestrator bootstrap contract is not the active flow. In that branch, the orchestrator should resume from PRD/workflow references and the latest phase handoff rather than forcing an issue packet-shaped payload.

## Compact payload contract

The orchestrator must not inject the full checkpoint body into the continuation prompt. It must derive a compact payload with exactly these sections:

### 1. active_target

- `issue_number`
- `branch`
- `role`
- `agent`
- `next_flow`

### 2. authoritative_refs

Must include only canonical resume entry points:

- issue packet
- prior handoff when present
- workflow policy

Must not include full transcript exports, raw logs, or ad hoc search dumps.

### 3. state_snapshot

- `completed`
- `in_progress`
- `next`
- `blockers`

Keep each list short and action-oriented.

### 4. resume_rules

Must restate these rules in compact form:

- resume from checkpoint and compact payload, not full chat history
- keep raw evidence as refs only
- do not inline logs, traces, or long transcripts

### 5. immediate_next_action

One imperative sentence that tells the fresh session what to do first.

## Checkpoint and continuation request write rules

When updating `context-checkpoint.yaml` and writing the continuation request, the orchestrator must:

- set `subject.role` to `main_orchestrator`
- record the selected issue and branch
- preserve the root-session agent in the compact payload and runtime ledger so fresh-session dispatch restores the same agent configuration
- keep the checkpoint file within the 80-line cap
- update `metadata.updated_at`
- keep refs stable and canonical
- write the continuation request under `.opencode/runtime/new-session-request.json`
- use explicit dispatch to launch the fresh `main_orchestrator` root session immediately after the request is written
- ensure the request prompt explicitly says checkpoint-only bootstrap and forbids prior transcript import

## Failure handling

### Checkpoint update failure

- status: blocked
- action: stop before continuation request write
- report: checkpoint write failure and missing fields

### Compact payload derivation failure

- status: blocked
- action: stop before continuation request write
- report: which required payload section is missing

### Continuation request write failure

- status: blocked
- action: stop before worker spawn
- report: request path write failed or required fields were missing

### Fresh-session bootstrap failure

- status: blocked
- action: stop before worker spawn
- report: explicit dispatch failed to create the root session or prompt it from checkpoint

### Recovery issue selection needs GitHub intake

- status: retryable_blocked
- action: let `scripts/orchestrator_supervisor.py reconcile --write-request` try `scripts/issue_packet_intake.py` once when no local next issue packet exists
- report: whether `gh` access was available and whether any new `docs/agents/issue-packets/issue-<n>.yaml` files were materialized

## Non-goals

- native session compaction for orchestrator bootstrap startup
- automatic `>50%` context auto-rotation outside this flow
- hook-driven orchestration beyond writing and explicitly dispatching the continuation request
- branch-change or webhook-driven passive triggering

## Active handoff contract

Orchestrator bootstrap now uses this order:

1. write checkpoint
2. write fresh-session continuation request
3. explicit dispatch creates a fresh orchestrator session from checkpoint
4. `new-session-result.json` records the root session id and resume instructions
5. fresh session continues `per_issue_flow`

This replaces the previous same-session startup behavior.
