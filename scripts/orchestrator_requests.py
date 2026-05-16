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


def _shared_runtime_root(ledger: JsonObject, *, fallback_workflow_policy_path: str) -> str:
    workflow = cast(dict[str, object], ledger.get("workflow", {}))
    policy_path = Path(str(workflow.get("workflowPolicyPath") or fallback_workflow_policy_path))
    if policy_path.is_absolute():
        try:
            return str(policy_path.parents[2])
        except IndexError:
            return str(policy_path.parent)
    return ""


def _runtime_controls(ledger: JsonObject) -> dict[str, object]:
    workflow = cast(dict[str, object], ledger.get("workflow", {}))
    runtime_controls = workflow.get("runtimeControls", {})
    return runtime_controls if isinstance(runtime_controls, dict) else {}


def build_common_prompt_lines(ledger: JsonObject, *, default_supervisor_doc_path: str) -> list[str]:
    issue = cast(dict[str, str], ledger["issue"])
    workflow = cast(dict[str, object], ledger["workflow"])
    automation = cast(dict[str, object], ledger.get("automation", {}))
    primary_workspace_root = str(automation.get("primaryWorkspaceRoot") or "")

    shared_runtime_root = _shared_runtime_root(ledger, fallback_workflow_policy_path=default_supervisor_doc_path)

    def runtime_command(relative_script_path: str) -> str:
        if shared_runtime_root:
            return f'PYTHONPATH="{shared_runtime_root}" python3 "{shared_runtime_root}/{relative_script_path}"'
        return f"python3 {relative_script_path}"

    return [
        "Bootstrap from the SQLite-backed control plane, not from runtime JSON/YAML artifacts.",
        f"Read {workflow['workflowPolicyPath']} for role boundaries and gates.",
        f"Read {default_supervisor_doc_path} for the nonstop supervisor contract.",
        f"Active issue: #{issue['number']} on branch {issue['branch']}.",
        "Do not wait for a user reply before advancing the workflow.",
        (
            "Report worker/verifier/release outcomes by calling "
            + runtime_command("scripts/orchestrator_supervisor.py")
            + ' submit-artifact --base-dir "'
            + (primary_workspace_root or ".")
            + '" --issue-number '
            + issue["number"]
            + " --artifact-kind <worker_result|evidence_packet|release_result> --payload-json '<json>' [--body-text '<text>']"
        ),
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
    workflow = cast(dict[str, object], ledger["workflow"])
    issue_backing_type = str(issue.get("backingType") or "github")
    artifacts = cast(dict[str, str], ledger["artifacts"])
    automation = cast(dict[str, object], ledger.get("automation", {}))
    runtime_controls = _runtime_controls(ledger)
    approval_override_mode = str(runtime_controls.get("approval_override_mode") or "")
    override_source = str(runtime_controls.get("override_source") or "none")
    human_approval_skipped = bool(runtime_controls.get("human_approval_skipped") or False)
    primary_workspace_root = str(automation.get("primaryWorkspaceRoot") or "")
    shared_runtime_root = _shared_runtime_root(ledger, fallback_workflow_policy_path=default_supervisor_doc_path)
    queued_next_issue = cast(dict[str, object], ledger.get("queuedNextIssue", {}))
    queued_next_issue_record = cast(dict[str, object], queued_next_issue.get("record", {}))
    queued_next_issue_number = str(queued_next_issue_record.get("issue_number") or "")
    queued_next_issue_branch = str(queued_next_issue_record.get("branch") or "")
    def shared_runtime_command(relative_script_path: str) -> str:
        if shared_runtime_root:
            return f'PYTHONPATH="{shared_runtime_root}" python3 "{shared_runtime_root}/{relative_script_path}"'
        return f"python3 {relative_script_path}"

    def canonical_runtime_path(path_text: str) -> str:
        if not path_text:
            return path_text
        path = Path(path_text)
        if path.is_absolute() or not primary_workspace_root:
            return path_text
        return str(Path(primary_workspace_root) / path)

    common = build_common_prompt_lines(ledger, default_supervisor_doc_path=default_supervisor_doc_path)
    if role == "main_orchestrator" and stage == "orchestrator_bootstrap":
        lines = common + [
            "You are the fresh main_orchestrator session for the selected AFK issue.",
            "Read the DB-backed issue packet context for this issue and any persisted prior handoff when present.",
            "Confirm the issue scope, branch, and control-plane target are still aligned.",
            (
                "Treat this issue as a local-seeded workflow issue; do not assume GitHub issue operations like `gh issue view` or `gh issue close` are available."
                if issue_backing_type == "local_seeded"
                else "Treat this issue as a GitHub-backed issue when validating orchestration steps."
            ),
            "Do not implement issue scope directly.",
            "You own orchestration for the whole selected issue inside this root session.",
            "Before launching the first child subagent, run the supervisor reconcile path once in this same turn so the bootstrap -> issue_worker_execution transition is persisted to the DB-backed control plane.",
            'Run issue_worker and pr_verifier as subagents from this root orchestrator session with task(subagent_type="general", ..., run_in_background=false). Wait for each child task call to finish in the foreground before continuing.',
            "Do not launch release_worker from this root session; PR merge/release is claimed later by the independent release command.",
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
            "Use the DB-backed supervisor reconcile flow before the first issue_worker launch and again after each child artifact is written.",
            "Use the first supervisor decision to confirm the issue_worker dispatch before you create or switch the issue branch and launch that first child subagent.",
            "Use the supervisor decision to choose the next subagent role. Only main_orchestrator recovery or next-issue handoff may create another root session.",
        ]
        lines = [line for line in lines if line]
    elif role == "issue_worker":
        lines = common + [
            f"You are the issue_worker subagent for issue #{issue['number']}.",
            "Read the DB-backed issue packet context and implement only that issue scope.",
            (
                "This issue is local-seeded. Use the DB-backed issue packet context as source of truth and do not rely on `gh issue view` succeeding."
                if issue_backing_type == "local_seeded"
                else "This issue is GitHub-backed. Use the DB-backed issue packet context and GitHub issue metadata together when needed."
            ),
            "Persist the normalized worker_result into SQLite with submit-artifact instead of writing YAML files.",
            "Do not report status=success until the implementation branch is pushed and branch or commit metadata is populated in the worker_result payload.",
            "Do not create the formal PR; verifier-owned acceptance creates or records the PR after checking the branch.",
            "If implementation is done but branch push has not succeeded yet, submit blocked or failed instead of success so reconcile can classify the state honestly.",
            "If the worker is blocked or failed, include failure_kind, retryable, and next_recommended_step in the submitted payload.",
            "Do not claim final acceptance; that belongs to pr_verifier.",
            "After submit-artifact succeeds, return control to the main_orchestrator root session; do not launch a root session.",
        ]
    elif role == "pr_verifier":
        lines = common + [
            f"You are the pr_verifier subagent for issue #{issue['number']}.",
            "Read the DB-backed issue packet context and the persisted worker_result context before touching anything else.",
            "Persist verifier acceptance or failure as an evidence_packet via submit-artifact instead of writing YAML files.",
            "After acceptance passes, create or record the formal PR and include pr_number in the evidence_packet payload.",
            "Do not stop, summarize, or report verification progress until the evidence_packet payload is stored in SQLite.",
            "If verification is blocked or fails, include failure_kind, retryable, next_recommended_step, verifier_session_id, and pr_number in the submitted payload.",
            "Final acceptance belongs to this verifier role; keep raw logs outside repo docs.",
            "After submit-artifact succeeds, return control to the main_orchestrator root session; do not launch a root session.",
        ]
    elif role == "release_worker":
        lines = common + [
            f"You are the release_worker subagent for issue #{issue['number']}.",
            "Read the persisted evidence_packet context from SQLite before evaluating merge or release decisions.",
            (
                "This issue is local-seeded. Skip GitHub issue operations such as `gh issue close` unless a real GitHub issue is explicitly materialized."
                if issue_backing_type == "local_seeded"
                else "This issue is GitHub-backed. Apply the normal GitHub issue close/update workflow after merge when appropriate."
            ),
            "Read the release runtime controls from the DB-backed control-plane context and treat them as the source of truth for merge approval override.",
            'If `approval_override_mode` is `"bypass_approval"`, skip only the human approval requirement while still enforcing verifier pass, required checks, PR mergeability, review gate, diagnostics/build gate, surface QA gate, and workspace hygiene.',
            'When bypassing approval, record `merge_approval_mode`, `human_approval_skipped`, `override_source`, and `override_scope` in the release result summary or metadata fields.',
            (
                f"Current release override: approval_override_mode={approval_override_mode or 'none'}, override_source={override_source}, human_approval_skipped={'true' if human_approval_skipped else 'false'}."
                if approval_override_mode or human_approval_skipped or override_source != 'none'
                else "Current release override: approval_override_mode=none, override_source=none, human_approval_skipped=false."
            ),
            "Persist release outcome as release_result via submit-artifact instead of writing YAML files.",
            "If release is blocked or fails, include blocked_reason, failure_kind, retryable, and next_recommended_step in the submitted payload.",
            "Respect required checks, mergeability, approval policy, and workspace hygiene.",
            "After submit-artifact succeeds, return control to the supervisor/release command result; do not launch another root session.",
        ]
    else:
        lines = common + [
            "You are a recovery/select-next-issue main_orchestrator session.",
            decision_summary,
            "Advance the broader workflow without waiting for a human reply.",
            "If the current issue is blocked, create or link the blocker and continue to the next ready issue when possible.",
        ]
        if queued_next_issue_number:
            lines.extend(
                [
                    (
                        f"Supervisor already selected issue #{queued_next_issue_number} on branch "
                        f"{queued_next_issue_branch or 'unknown'} from DB-backed intake state."
                    ),
                    "Do not recompute selection from the live queue or rerun intake unless this exact selected issue is now invalid.",
                    "Bootstrap exactly this selected issue through the DB-backed supervisor start path.",
                    f"{shared_runtime_command('scripts/orchestrator_supervisor.py')} start-issue --issue-number {queued_next_issue_number}",
                    "If that exact issue is now invalid, report the contract violation compactly instead of silently picking a different issue.",
                ]
            )
        else:
            lines.extend(
                [
                    "If another issue is ready, start it through the DB-backed supervisor start path.",
                    f"{shared_runtime_command('scripts/orchestrator_supervisor.py')} start-issue --issue-number <selected-issue-number>",
                    "That path should create the next main_orchestrator root session from SQLite state without treating checkpoint or runtime JSON files as control-plane truth.",
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
    if selected_issue_number:
        request["selectedIssueNumber"] = selected_issue_number
        request["selectedIssueBranch"] = selected_issue_branch
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
            "Fresh orchestrator session must validate the selected issue target and run the initial supervisor reconcile so the DB-backed control plane advances to issue_worker_execution before launching the first issue_worker subagent "
            f"without waiting for a human reply. Immediate next action: {immediate_next_action}"
        ),
    )
