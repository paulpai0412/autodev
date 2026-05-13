"""Prompt and session-request builders for the autodev supervisor."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, cast
from uuid import uuid4


JsonObject = dict[str, object]
NowFunc = Callable[[str | None], str]
RootSessionAgent = Callable[[JsonObject], str]
BuildPrompt = Callable[[JsonObject, str, str, str], str]
BuildSessionRequest = Callable[..., JsonObject]
WriteJson = Callable[[Path, JsonObject], None]


def build_common_prompt_lines(ledger: JsonObject, *, default_supervisor_doc_path: str) -> list[str]:
    issue = cast(dict[str, str], ledger["issue"])
    workflow = cast(dict[str, str], ledger["workflow"])
    return [
        "Bootstrap from checkpoint and runtime artifacts only.",
        f"Read {workflow['checkpointPath']} first.",
        "Read .opencode/runtime/orchestrator-ledger.json second.",
        f"Read {workflow['workflowPolicyPath']} for role boundaries and gates.",
        f"Read {default_supervisor_doc_path} for the nonstop supervisor contract.",
        f"Active issue: #{issue['number']} on branch {issue['branch']}.",
        "Do not wait for a user reply before advancing the workflow.",
    ]


def build_prompt(
    ledger: JsonObject,
    role: str,
    stage: str,
    decision_summary: str,
    *,
    default_supervisor_doc_path: str,
    default_release_result_template_path: str,
) -> str:
    issue = cast(dict[str, str], ledger["issue"])
    artifacts = cast(dict[str, str], ledger["artifacts"])
    common = build_common_prompt_lines(ledger, default_supervisor_doc_path=default_supervisor_doc_path)
    if role == "main_orchestrator" and stage == "orchestrator_bootstrap":
        lines = common + [
            "You are the fresh main_orchestrator session for the selected AFK issue.",
            f"Read {issue['issuePacketPath']} and any prior handoff when present.",
            "Confirm the issue packet and branch are still the correct target.",
            "Do not implement issue scope directly.",
            "You own orchestration for the whole selected issue inside this root session.",
            "Immediately launch the first issue_worker subagent in this same turn after validating the target; do not stop after describing intent or summarizing a plan.",
            'Run issue_worker, pr_verifier, and release_worker as subagents from this root orchestrator session with task(subagent_type="general", ..., run_in_background=false). Wait for each child task call to finish in the foreground before continuing.',
            "Choose child load_skills normally for the task at hand.",
            "After each subagent writes its compact artifact, run:",
            "PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile --ledger .opencode/runtime/orchestrator-ledger.json",
            "Use the supervisor decision to choose the next subagent role. Only main_orchestrator recovery or next-issue handoff may create another root session.",
        ]
    elif role == "issue_worker":
        lines = common + [
            f"You are the issue_worker subagent for issue #{issue['number']}.",
            f"Read {issue['issuePacketPath']} and implement only that issue scope.",
            f"Write {artifacts['workerResultPath']} using docs/agents/worker-result-template.yaml.",
            "If the worker is blocked or failed, include failure_classification with kind, retryable, routed_to, and root_cause_signature.",
            "Do not claim final acceptance; that belongs to pr_verifier.",
            "When the worker_result is written, return control to the main_orchestrator root session; do not launch a root session.",
        ]
    elif role == "pr_verifier":
        lines = common + [
            f"You are the pr_verifier subagent for issue #{issue['number']}.",
            f"Read {issue['issuePacketPath']} and {artifacts['workerResultPath']} before touching anything else.",
            f"Write {artifacts['evidencePacketPath']} using docs/agents/evidence-packet-template.yaml.",
            "Do not stop, summarize, or report verification progress until that evidence packet exists at the exact path above.",
            "If verification is blocked or fails, include failure_classification with kind, retryable, routed_to, and root_cause_signature.",
            "Final acceptance belongs to this verifier role; keep raw logs outside repo docs.",
            "When the evidence packet is written, return control to the main_orchestrator root session; do not launch a root session.",
        ]
    elif role == "release_worker":
        lines = common + [
            f"You are the release_worker subagent for issue #{issue['number']}.",
            f"Read {artifacts['evidencePacketPath']} before evaluating merge or release decisions.",
            "Read the runtime_controls block in the checkpoint and treat it as the workflow-start source of truth for merge approval override.",
            'If `approval_override_mode` is `"bypass_approval"`, skip only the human approval requirement while still enforcing verifier pass, required checks, PR mergeability, review gate, diagnostics/build gate, surface QA gate, and workspace hygiene.',
            'When bypassing approval, record `merge_approval_mode`, `human_approval_skipped`, `override_source`, and `override_scope` in the release result summary or metadata fields.',
            f"Write {artifacts['releaseResultPath']} using {default_release_result_template_path}.",
            "If release is blocked or fails, include failure_classification with kind, retryable, routed_to, and root_cause_signature.",
            "Respect required checks, mergeability, approval policy, and workspace hygiene.",
            "When the release_result is written, return control to the main_orchestrator root session; do not launch a root session.",
        ]
    else:
        lines = common + [
            "You are a recovery/select-next-issue main_orchestrator session.",
            decision_summary,
            "Advance the broader workflow without waiting for a human reply.",
            "If the current issue is blocked, create or link the blocker and continue to the next ready issue when possible.",
            "If another issue is ready, run orchestrator bootstrap for it directly with:",
            "python3 scripts/orchestrator_bootstrap_runner.py --issue-packet <path-to-selected-issue-packet> --dispatch-now",
            "That command will refresh the checkpoint, supervisor ledger, and create the next main_orchestrator root session.",
            "If no issue is ready, stop cleanly and report the blocking reason in compact form.",
        ]
    lines.append(f"Decision summary: {decision_summary}")
    return "\n".join(lines)


def build_session_request(
    ledger: JsonObject,
    *,
    role: str,
    stage: str,
    reason: str,
    title: str,
    decision_summary: str,
    now: NowFunc,
    root_session_agent: RootSessionAgent,
    build_prompt: BuildPrompt,
) -> JsonObject:
    issue = cast(dict[str, str], ledger["issue"])
    created_at = str(ledger.get("updatedAt") or now(None))
    ledger_revision = str(ledger.get("ledgerRevision") or ledger.get("updatedAt") or created_at)
    nonce = uuid4().hex
    return {
        "requestGeneration": len(cast(list[JsonObject], ledger.get("history", []))) + 1,
        "nonce": nonce,
        "requestID": nonce,
        "createdAt": created_at,
        "createdForLedgerRevision": ledger_revision,
        "reason": reason,
        "title": title,
        "agent": root_session_agent(ledger),
        "prompt": build_prompt(ledger, role, stage, decision_summary),
        "role": role,
        "stage": stage,
        "issueNumber": issue["number"],
        "branch": issue["branch"],
    }


def build_orchestrator_request(ledger: JsonObject, *, build_session_request: BuildSessionRequest) -> JsonObject:
    issue = cast(dict[str, str], ledger["issue"])
    immediate_next_action = f"Continue per_issue_flow for issue #{issue['number']} by creating or switching the issue branch."
    return build_session_request(
        ledger,
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        reason=f"orchestrator bootstrap continuation for issue #{issue['number']}",
        title=f"Continue issue #{issue['number']} on {issue['branch']}",
        decision_summary=(
            "Fresh orchestrator session must validate the selected issue target and immediately launch the first issue_worker subagent "
            f"without waiting for a human reply. Immediate next action: {immediate_next_action}"
        ),
    )


def write_session_request(request_path: Path, request: JsonObject, *, write_json: WriteJson) -> None:
    write_json(request_path, dict(request))
