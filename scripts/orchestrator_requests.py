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


def _shared_runtime_root(ledger: JsonObject, *, fallback_workflow_policy_path: str) -> str:
    workflow = cast(dict[str, str], ledger.get("workflow", {}))
    policy_path = Path(str(workflow.get("workflowPolicyPath") or fallback_workflow_policy_path))
    if policy_path.is_absolute():
        try:
            return str(policy_path.parents[2])
        except IndexError:
            return str(policy_path.parent)
    return ""


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
    workflow = cast(dict[str, str], ledger["workflow"])
    issue_backing_type = str(issue.get("backingType") or "github")
    artifacts = cast(dict[str, str], ledger["artifacts"])
    automation = cast(dict[str, object], ledger.get("automation", {}))
    primary_workspace_root = str(automation.get("primaryWorkspaceRoot") or "")
    shared_runtime_root = _shared_runtime_root(ledger, fallback_workflow_policy_path=default_supervisor_doc_path)
    queued_next_issue = cast(dict[str, object], ledger.get("queuedNextIssue", {}))
    queued_next_issue_record = cast(dict[str, object], queued_next_issue.get("record", {}))
    queued_next_issue_number = str(queued_next_issue_record.get("issue_number") or "")
    queued_next_issue_branch = str(queued_next_issue_record.get("branch") or "")
    queued_next_issue_packet = str(queued_next_issue_record.get("issue_packet_path") or "")

    def shared_runtime_command(relative_script_path: str) -> str:
        if shared_runtime_root:
            return f'PYTHONPATH="{shared_runtime_root}" python3 "{shared_runtime_root}/{relative_script_path}"'
        return f"python3 {relative_script_path}"

    def canonical_artifact_path(path_text: str) -> str:
        if not path_text:
            return path_text
        path = Path(path_text)
        if path.is_absolute() or not primary_workspace_root:
            return path_text
        return str(Path(primary_workspace_root) / path)

    worker_result_path = canonical_artifact_path(artifacts.get("workerResultPath", ""))
    evidence_packet_path = canonical_artifact_path(artifacts.get("evidencePacketPath", ""))
    release_result_path = canonical_artifact_path(artifacts.get("releaseResultPath", ""))
    common = build_common_prompt_lines(ledger, default_supervisor_doc_path=default_supervisor_doc_path)
    if role == "main_orchestrator" and stage == "orchestrator_bootstrap":
        lines = common + [
            "You are the fresh main_orchestrator session for the selected AFK issue.",
            f"Read {issue['issuePacketPath']} and any prior handoff when present.",
            "Confirm the issue packet and branch are still the correct target.",
            (
                "Treat this issue as a local-seeded workflow issue; do not assume GitHub issue operations like `gh issue view` or `gh issue close` are available."
                if issue_backing_type == "local_seeded"
                else "Treat this issue as a GitHub-backed issue when validating orchestration steps."
            ),
            "Do not implement issue scope directly.",
            "You own orchestration for the whole selected issue inside this root session.",
            "Before launching the first child subagent, run the supervisor reconcile command once in this same turn so the bootstrap -> issue_worker_execution transition is persisted to the on-disk ledger.",
            'Run issue_worker, pr_verifier, and release_worker as subagents from this root orchestrator session with task(subagent_type="general", ..., run_in_background=false). Wait for each child task call to finish in the foreground before continuing.',
            "Choose child load_skills normally for the task at hand.",
            (
                f"The authoritative shared workflow policy is {workflow['workflowPolicyPath']}."
                if workflow.get("workflowPolicyPath")
                else ""
            ),
            (
                f"The authoritative nonstop supervisor doc is {default_supervisor_doc_path}."
                if default_supervisor_doc_path
                else ""
            ),
            "Run this reconcile command before the first issue_worker launch:",
            f"{shared_runtime_command('scripts/orchestrator_supervisor.py')} reconcile --ledger .opencode/runtime/orchestrator-ledger.json",
            "After each child artifact is written, advance the queued child role with:",
            f"{shared_runtime_command('scripts/orchestrator_supervisor.py')} advance-child --ledger .opencode/runtime/orchestrator-ledger.json",
            "Use the first supervisor decision to confirm the issue_worker dispatch before you create or switch the issue branch and launch that first child subagent.",
            "Use the supervisor decision to choose the next subagent role. Only main_orchestrator recovery or next-issue handoff may create another root session.",
        ]
        lines = [line for line in lines if line]
    elif role == "issue_worker":
        lines = common + [
            f"You are the issue_worker subagent for issue #{issue['number']}.",
            f"Read {issue['issuePacketPath']} and implement only that issue scope.",
            (
                "This issue is local-seeded. Use the local issue packet as source of truth and do not rely on `gh issue view` succeeding."
                if issue_backing_type == "local_seeded"
                else "This issue is GitHub-backed. Use the local issue packet and GitHub issue metadata together when needed."
            ),
            f"Write {worker_result_path} using docs/agents/worker-result-template.yaml.",
            "Do not write status: success until the branch is pushed, the PR exists, and pr.number plus pr.url are populated in the worker_result.",
            "If implementation is done but push or PR creation has not succeeded yet, write blocked or failed instead of success so reconcile can classify the state honestly.",
            "If the worker is blocked or failed, include failure_classification with kind, retryable, routed_to, and root_cause_signature.",
            "Do not claim final acceptance; that belongs to pr_verifier.",
            "When the worker_result is written, return control to the main_orchestrator root session; do not launch a root session.",
        ]
    elif role == "pr_verifier":
        lines = common + [
            f"You are the pr_verifier subagent for issue #{issue['number']}.",
            f"Read {issue['issuePacketPath']} and {worker_result_path} before touching anything else.",
            f"Write {evidence_packet_path} using docs/agents/evidence-packet-template.yaml.",
            "Do not stop, summarize, or report verification progress until that evidence packet exists at the exact path above.",
            "If verification is blocked or fails, include failure_classification with kind, retryable, routed_to, and root_cause_signature.",
            "Final acceptance belongs to this verifier role; keep raw logs outside repo docs.",
            "When the evidence packet is written, return control to the main_orchestrator root session; do not launch a root session.",
        ]
    elif role == "release_worker":
        lines = common + [
            f"You are the release_worker subagent for issue #{issue['number']}.",
            f"Read {evidence_packet_path} before evaluating merge or release decisions.",
            (
                "This issue is local-seeded. Skip GitHub issue operations such as `gh issue close` unless a real GitHub issue is explicitly materialized."
                if issue_backing_type == "local_seeded"
                else "This issue is GitHub-backed. Apply the normal GitHub issue close/update workflow after merge when appropriate."
            ),
            "Read the runtime_controls block in the checkpoint and treat it as the workflow-start source of truth for merge approval override.",
            'If `approval_override_mode` is `"bypass_approval"`, skip only the human approval requirement while still enforcing verifier pass, required checks, PR mergeability, review gate, diagnostics/build gate, surface QA gate, and workspace hygiene.',
            'When bypassing approval, record `merge_approval_mode`, `human_approval_skipped`, `override_source`, and `override_scope` in the release result summary or metadata fields.',
            f"Write {release_result_path} using {default_release_result_template_path}.",
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
        ]
        if queued_next_issue_number and queued_next_issue_packet:
            lines.extend(
                [
                    (
                        f"Supervisor already selected issue #{queued_next_issue_number} on branch "
                        f"{queued_next_issue_branch or 'unknown'} via {canonical_artifact_path(queued_next_issue_packet)}."
                    ),
                    "Do not recompute selection from the live queue or rerun intake unless this exact selected issue is now invalid.",
                    "Bootstrap exactly this selected issue with:",
                    f"{shared_runtime_command('scripts/orchestrator_bootstrap_runner.py')} --issue-packet {canonical_artifact_path(queued_next_issue_packet)} --dispatch-now",
                    "If that exact issue is now invalid, report the contract violation compactly instead of silently picking a different issue.",
                ]
            )
        else:
            lines.extend(
                [
                    "If another issue is ready, run orchestrator bootstrap for it directly with:",
                    f"{shared_runtime_command('scripts/orchestrator_bootstrap_runner.py')} --issue-packet <path-to-selected-issue-packet> --dispatch-now",
                    "That command will refresh the checkpoint, supervisor ledger, and create the next main_orchestrator root session.",
                    "If no issue is ready, stop cleanly and report the blocking reason in compact form.",
                ]
            )
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
    request: JsonObject = {
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
    queued_next_issue = cast(dict[str, object], ledger.get("queuedNextIssue", {}))
    queued_next_issue_record = cast(dict[str, object], queued_next_issue.get("record", {}))
    selected_issue_number = str(queued_next_issue_record.get("issue_number") or "")
    selected_issue_branch = str(queued_next_issue_record.get("branch") or "")
    selected_issue_packet_path = str(queued_next_issue_record.get("issue_packet_path") or "")
    if selected_issue_number and selected_issue_packet_path:
        request["selectedIssueNumber"] = selected_issue_number
        request["selectedIssueBranch"] = selected_issue_branch
        request["selectedIssuePacketPath"] = selected_issue_packet_path
    return request


def build_orchestrator_request(ledger: JsonObject, *, build_session_request: BuildSessionRequest) -> JsonObject:
    issue = cast(dict[str, str], ledger["issue"])
    immediate_next_action = (
        f"Run supervisor reconcile for issue #{issue['number']} to persist issue_worker_execution before creating or switching the issue branch and launching the first issue_worker subagent."
    )
    return build_session_request(
        ledger,
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        reason=f"orchestrator bootstrap continuation for issue #{issue['number']}",
        title=f"Continue issue #{issue['number']} on {issue['branch']}",
        decision_summary=(
            "Fresh orchestrator session must validate the selected issue target and run the initial supervisor reconcile so the on-disk ledger advances to issue_worker_execution before launching the first issue_worker subagent "
            f"without waiting for a human reply. Immediate next action: {immediate_next_action}"
        ),
    )


def write_session_request(request_path: Path, request: JsonObject, *, write_json: WriteJson) -> None:
    write_json(request_path, dict(request))
