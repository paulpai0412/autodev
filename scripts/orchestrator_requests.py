"""Prompt and session-request builders for the autodev supervisor."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, cast
from uuid import uuid4

from scripts.runtime_exec import shell_python_command_token


JsonObject = dict[str, object]
NowFunc = Callable[[str | None], str]
RootSessionAgent = Callable[[JsonObject], str]
BuildPrompt = Callable[[JsonObject, str, str, str], str]
BuildSessionRequest = Callable[..., JsonObject]


@dataclass(frozen=True)
class PromptSpec:
    """Structured prompt body before final rendering."""

    body_lines: list[str]
    decision_summary: str

    def render(self) -> str:
        lines = [*self.body_lines]
        lines.append(f"Decision summary: {self.decision_summary}")
        return "\n".join(lines)


@dataclass(frozen=True)
class SessionRequestSpec:
    """Structured request spec before serialization."""

    payload: JsonObject

    def to_json(self) -> JsonObject:
        return dict(self.payload)


def _queued_next_issue_fields(ledger: JsonObject) -> tuple[str, str, str]:
    queued_next_issue = cast(dict[str, object], ledger.get("queuedNextIssue", {}))
    issue_number = str(queued_next_issue.get("issue_number") or "")
    branch = str(queued_next_issue.get("branch") or "")
    base_branch = str(queued_next_issue.get("base_branch") or "")
    if issue_number:
        return issue_number, branch, base_branch

    queued_next_issue_record = cast(dict[str, object], queued_next_issue.get("record", {}))
    return (
        str(queued_next_issue_record.get("issue_number") or ""),
        str(queued_next_issue_record.get("branch") or ""),
        str(queued_next_issue_record.get("base_branch") or ""),
    )


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
    if isinstance(runtime_controls, dict):
        return cast(dict[str, object], runtime_controls)
    return {}


def build_common_prompt_lines(ledger: JsonObject, *, default_supervisor_doc_path: str) -> list[str]:
    issue = cast(dict[str, str], ledger["issue"])
    workflow = cast(dict[str, object], ledger["workflow"])
    automation = cast(dict[str, object], ledger.get("automation", {}))
    primary_workspace_root = str(automation.get("primaryWorkspaceRoot") or "")
    base_branch = str(issue.get("baseBranch") or "main")

    shared_runtime_root = _shared_runtime_root(ledger, fallback_workflow_policy_path=default_supervisor_doc_path)

    def runtime_command(relative_script_path: str) -> str:
        if shared_runtime_root:
            return f'PYTHONPATH="{shared_runtime_root}" {shell_python_command_token()} "{shared_runtime_root}/{relative_script_path}"'
        return f"{shell_python_command_token()} {relative_script_path}"

    return [
        "Bootstrap from the SQLite-backed control plane, not from runtime JSON/YAML artifacts.",
        f"Read {workflow['workflowPolicyPath']} for role boundaries and gates.",
        f"Read {default_supervisor_doc_path} for the nonstop supervisor contract.",
        f"Active issue: #{issue['number']} on branch {issue['branch']}.",
        f"Branch plan: create or update target branch {issue['branch']} from base branch {base_branch}.",
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


def _bootstrap_prompt_lines(
    *,
    common: list[str],
    issue_backing_type: str,
    workflow: dict[str, object],
    default_supervisor_doc_path: str,
) -> list[str]:
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
        "If the first reconcile decision returns start_issue_required, run start-issue for this issue immediately, then run reconcile again before creating/switching branches or launching child subagents.",
        'Run issue_worker and pr_verifier as subagents from this root orchestrator session with task(subagent_type="general", ..., run_in_background=false). Wait for each child task call to finish in the foreground before continuing.',
        'When launching issue_worker, always include load_skills=["tdd", "karpathy-guidelines", "git-master"]. Do not omit tdd for code-changing issue work.',
        'If issue scope includes web UI/static HTML implementation, launch issue_worker with load_skills=["tdd", "karpathy-guidelines", "git-master", "web-design-engineer"].',
        "Do not launch release_worker from this root session; PR merge/release is claimed later by the independent release command.",
        'When launching pr_verifier, always include load_skills=["review-work", "karpathy-guidelines"].',
        'If issue scope includes web UI/static HTML verification, launch pr_verifier with load_skills=["review-work", "karpathy-guidelines", "browser-qa", "e2e-testing"].',
        "For release_worker, choose child load_skills normally for the task at hand.",
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
    return [line for line in lines if line]


def _issue_worker_prompt_lines(*, common: list[str], issue: dict[str, str], issue_backing_type: str) -> list[str]:
    return common + [
        f"You are the issue_worker subagent for issue #{issue['number']}.",
        "Read the DB-backed issue packet context and implement only that issue scope.",
        (
            "This issue is local-seeded. Use the DB-backed issue packet context as source of truth and do not rely on `gh issue view` succeeding."
            if issue_backing_type == "local_seeded"
            else "This issue is GitHub-backed. Use the DB-backed issue packet context and GitHub issue metadata together when needed."
        ),
        "Persist the normalized worker_result into SQLite with submit-artifact instead of writing YAML files.",
        'When implementing web UI/static HTML scope, include "web-design-engineer" in issue_worker load_skills before coding.',
        "Before claiming status=success, fetch and merge the latest base branch into your issue branch, resolve conflicts, and rerun focused checks so release is not blocked by stale branch divergence.",
        "Do not report status=success until the implementation branch is pushed and branch or commit metadata is populated in the worker_result payload.",
        f"Create or update the implementation branch {issue['branch']} from base branch {issue.get('baseBranch') or 'main'}; do not assume main when the base branch differs.",
        "Do not create the formal PR; verifier-owned acceptance creates or records the PR after checking the branch.",
        "If implementation is done but branch push has not succeeded yet, submit blocked or failed instead of success so reconcile can classify the state honestly.",
        "If the worker is blocked or failed, include failure_kind, retryable, and next_recommended_step in the submitted payload.",
        "Do not claim final acceptance; that belongs to pr_verifier.",
        "After submit-artifact succeeds, return control to the main_orchestrator root session; do not launch a root session.",
    ]


def _pr_verifier_prompt_lines(
    *,
    common: list[str],
    issue: dict[str, str],
    primary_workspace_root: str,
    shared_runtime_command: Callable[[str], str],
) -> list[str]:
    inspect_command = (
        f"{shared_runtime_command('scripts/orchestrator_supervisor.py')} "
        f"inspect --base-dir \"{primary_workspace_root or '.'}\" --issue-number {issue['number']}"
    )
    return common + [
        f"You are the pr_verifier subagent for issue #{issue['number']}.",
        "Read the DB-backed issue packet context and the persisted worker_result context before touching anything else.",
        "Persist verifier acceptance or failure as an evidence_packet via submit-artifact instead of writing YAML files.",
        "Evidence payload contract: for issue PR verification, include pr_number and base_branch as TOP-LEVEL evidence_packet fields, and also mirror them under subject.pr_number and subject.base_branch for readability. The supervisor preflight reads the top-level fields.",
        "Evidence payload contract: gates.surface_qa_gate must be an object {status: 'pass', evidence_ref: '<non-empty>', evidence_kind: 'browser'} when browser_e2e_gate is required. Flat gate strings are not accepted.",
        "For browser evidence_ref, use an existing issue-worktree relative file path (for example artifacts/browser-e2e/report.json), not a prose description. Save the browser QA report before submit-artifact so the referenced file exists.",
        "If your browser runtime returns browser:/ or file:// URIs, include them only when unavoidable; supervisor will normalize/stage them into worktree-managed evidence paths.",
        'Run pr_verifier with load_skills containing "review-work" for every verification run.',
        "If the worker changed web UI or static HTML surface files, browser_e2e_gate is mandatory before status=pass.",
        'When browser_e2e_gate is mandatory, include both "browser-qa" and "e2e-testing" in pr_verifier load_skills before executing checks.',
        "For mandatory browser_e2e_gate runs, execute a real browser flow using any browser automation/runtime or manual browser harness (happy path plus one refusal/error path), check console/network failures, and include compact evidence refs in the evidence_packet.",
        "Set gates.surface_qa_gate.evidence_kind to 'browser' only when a real browser was launched. smoke_test or unit_test execution does not qualify as browser evidence.",
        "If your chosen browser runtime/tooling is unavailable or the browser flow fails, submit status=blocked with failure_kind='browser_e2e_unavailable' and retryable=true. Do not downgrade to smoke_test and claim status=pass.",
        "After acceptance passes, create or record the formal PR and include top-level pr_number in the evidence_packet payload.",
        f"When creating the formal PR, use head branch {issue['branch']} and base branch {issue.get('baseBranch') or 'main'}; include top-level base_branch in the evidence_packet payload.",
        "Do not stop, summarize, or report verification progress until the evidence_packet payload is stored in SQLite.",
        "Immediately after submit-artifact for evidence_packet, run inspect and confirm latest refs/artifact status show a persisted evidence_packet fact (parse_ok=true) for this issue.",
        f"Inspect command: {inspect_command}",
        "Preflight after inspect: confirm artifact_status_json.evidence_packet.parse_ok=true, status is pass/blocked/fail as submitted, top-level pr_number/base_branch are present for PR evidence, and any browser surface_qa_gate.evidence_ref points to an existing worktree file.",
        "If inspect does not show persisted evidence_packet, if top-level pr_number/base_branch binding is missing, or if the browser evidence_ref file is missing, retry submit-artifact in the same verifier session; do not exit.",
        "Only return after SQLite persistence is visible via inspect for the same issue and the evidence payload includes top-level pr_number/base_branch for PR evidence.",
        "If verification is blocked or fails, include failure_kind, retryable, next_recommended_step, verifier_session_id, and pr_number in the submitted payload.",
        "Final acceptance belongs to this verifier role; keep raw logs outside repo docs.",
        "After submit-artifact succeeds, return control to the main_orchestrator root session; do not launch a root session.",
    ]


def _release_root_prompt_lines(
    *,
    common: list[str],
    issue: dict[str, str],
    issue_backing_type: str,
    approval_override_mode: str,
    override_source: str,
    human_approval_skipped: bool,
) -> list[str]:
    return common + [
        f"You are the independent release root session for issue #{issue['number']}.",
        'Run the actual release_worker steps as a foreground subagent from this session with task(subagent_type="general", ..., run_in_background=false). Wait for that child call to finish before you return.',
        "Do not perform the merge/close workflow directly from the release root shell without first delegating that foreground release_worker subagent.",
        'When launching the foreground release_worker subagent, choose task-appropriate load_skills for the release work. Pass [] only when no skill matches the task domain.',
        "Read the persisted evidence_packet context from SQLite before evaluating merge or release decisions.",
        (
            "This issue is local-seeded. Skip GitHub issue operations such as `gh issue close` unless a real GitHub issue is explicitly materialized."
            if issue_backing_type == "local_seeded"
            else "This issue is GitHub-backed. Apply the normal GitHub issue close/update workflow after merge when appropriate."
        ),
        "Read the release runtime controls from the DB-backed control-plane context and treat them as the source of truth for merge approval override.",
        'If `approval_override_mode` is `"bypass_approval"`, skip only the human approval requirement while still enforcing verifier pass, required checks, PR mergeability, review gate, diagnostics/build gate, surface QA gate, and workspace hygiene.',
        'When bypassing approval, record `merge_approval_mode`, `human_approval_skipped`, `override_source`, and `override_scope` in the release result summary or metadata fields.',
        "Before merge, enforce local convergence in this order: `git fetch origin --prune` -> `git switch main` -> `git pull --ff-only origin main`; if any step fails, report blocked and do not merge.",
        "If the PR is conflicted or not mergeable because the head branch is behind or diverged, first attempt to update the head branch from the base branch, resolve conflicts yourself, rerun any checks affected by the resolution, and then re-evaluate mergeability before deciding the final release state.",
        "Only submit a failed release state for merge conflicts after you attempted conflict resolution in good faith and the branch still cannot be merged safely; do not mark the release failed on the first conflicted check.",
        "Evaluate merge gates explicitly and persist them in release_result.merge_gate: checks_state (pending|failed|passed), mergeability_state (conflicted|clean), approval_state (missing|satisfied), blocked_reason, next_action.",
        "Use remote GitHub PR merge as the single merge authority; do not merge main by local push.",
        "After merge succeeds, run workspace hygiene and persist release_result.workspace_hygiene with cleanup_status plus branch/dirty/worktree cleanup fields.",
        "During workspace hygiene, do not run `git stash -a`, `git stash --all`, or any `git clean -xfd` style cleanup; these may remove control-plane/runtime state.",
        "Treat `.opencode/runtime/control-plane.sqlite3` and `.opencode/runtime/` as protected runtime state: never stash, delete, or clean them.",
        "If runtime artifact paths such as `.opencode/`, `.playwright-mcp/`, or `artifacts/` appear in workspace status, resolve hygiene by adding them to `.git/info/exclude` (or equivalent ignore configuration) instead of stash/clean.",
        (
            f"Current release override: approval_override_mode={approval_override_mode or 'none'}, override_source={override_source}, human_approval_skipped={'true' if human_approval_skipped else 'false'}."
            if approval_override_mode or human_approval_skipped or override_source != 'none'
            else "Current release override: approval_override_mode=none, override_source=none, human_approval_skipped=false."
        ),
        "Persist release outcome as release_result via submit-artifact instead of writing YAML files.",
        "If release is blocked or fails, include blocked_reason, failure_kind, retryable, and next_recommended_step in the submitted payload.",
        "Respect required checks, mergeability, approval policy, and workspace hygiene.",
        "After the foreground release_worker subagent stores release_result in SQLite, return control to the supervisor/release command result; do not launch another root session.",
        "If the release command already returned success to a different caller session, that caller is observer-only: it may inspect/reconcile but must not manually launch another release_worker.",
    ]


def _recovery_or_selection_prompt_lines(
    *,
    common: list[str],
    decision_summary: str,
    queued_next_issue_number: str,
    queued_next_issue_branch: str,
    queued_next_issue_base_branch: str,
    shared_runtime_command: Callable[[str], str],
) -> list[str]:
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
                    f"{queued_next_issue_branch or 'unknown'} from DB-backed intake state; base branch "
                    f"{queued_next_issue_base_branch or 'main'}."
                ),
                "Do not recompute selection from the live queue or rerun intake unless this exact selected issue is now invalid.",
                "Bootstrap exactly this selected issue through the DB-backed supervisor start path.",
                f"{shared_runtime_command('scripts/orchestrator_supervisor.py')} start-issue --issue-number {queued_next_issue_number}",
                "If that exact issue is now invalid, report the contract violation compactly instead of silently picking a different issue.",
            ]
        )
        return lines
    lines.extend(
        [
            "If another issue is ready, start it through the DB-backed supervisor start path.",
            f"{shared_runtime_command('scripts/orchestrator_supervisor.py')} start-issue --issue-number <selected-issue-number>",
            "That path should create the next main_orchestrator root session from SQLite state without treating checkpoint or runtime JSON files as control-plane truth.",
            "If no issue is ready, stop cleanly and report the blocking reason in compact form.",
        ]
    )
    return lines


def build_prompt_spec(
    ledger: JsonObject,
    role: str,
    stage: str,
    decision_summary: str,
    *,
    default_supervisor_doc_path: str,
    default_release_result_template_path: str,
) -> PromptSpec:
    _ = default_release_result_template_path
    issue = cast(dict[str, str], ledger["issue"])
    workflow = cast(dict[str, object], ledger["workflow"])
    issue_backing_type = str(issue.get("backingType") or "github")
    _artifacts = cast(dict[str, str], ledger["artifacts"])
    automation = cast(dict[str, object], ledger.get("automation", {}))
    runtime_controls = _runtime_controls(ledger)
    approval_override_mode = str(runtime_controls.get("approval_override_mode") or "")
    override_source = str(runtime_controls.get("override_source") or "none")
    human_approval_skipped = bool(runtime_controls.get("human_approval_skipped") or False)
    primary_workspace_root = str(automation.get("primaryWorkspaceRoot") or "")
    shared_runtime_root = _shared_runtime_root(ledger, fallback_workflow_policy_path=default_supervisor_doc_path)
    queued_next_issue_number, queued_next_issue_branch, queued_next_issue_base_branch = _queued_next_issue_fields(ledger)

    def shared_runtime_command(relative_script_path: str) -> str:
        if shared_runtime_root:
            return f'PYTHONPATH="{shared_runtime_root}" {shell_python_command_token()} "{shared_runtime_root}/{relative_script_path}"'
        return f"{shell_python_command_token()} {relative_script_path}"

    common = build_common_prompt_lines(ledger, default_supervisor_doc_path=default_supervisor_doc_path)
    if role == "main_orchestrator" and stage == "orchestrator_bootstrap":
        body_lines = _bootstrap_prompt_lines(
            common=common,
            issue_backing_type=issue_backing_type,
            workflow=workflow,
            default_supervisor_doc_path=default_supervisor_doc_path,
        )
    elif role == "issue_worker":
        body_lines = _issue_worker_prompt_lines(common=common, issue=issue, issue_backing_type=issue_backing_type)
    elif role == "pr_verifier":
        body_lines = _pr_verifier_prompt_lines(
            common=common,
            issue=issue,
            primary_workspace_root=primary_workspace_root,
            shared_runtime_command=shared_runtime_command,
        )
    elif role == "main_orchestrator" and stage == "release_root_execution":
        body_lines = _release_root_prompt_lines(
            common=common,
            issue=issue,
            issue_backing_type=issue_backing_type,
            approval_override_mode=approval_override_mode,
            override_source=override_source,
            human_approval_skipped=human_approval_skipped,
        )
    else:
        body_lines = _recovery_or_selection_prompt_lines(
            common=common,
            decision_summary=decision_summary,
            queued_next_issue_number=queued_next_issue_number,
            queued_next_issue_branch=queued_next_issue_branch,
            queued_next_issue_base_branch=queued_next_issue_base_branch,
            shared_runtime_command=shared_runtime_command,
        )
    return PromptSpec(body_lines=body_lines, decision_summary=decision_summary)


def build_prompt(
    ledger: JsonObject,
    role: str,
    stage: str,
    decision_summary: str,
    *,
    default_supervisor_doc_path: str,
    default_release_result_template_path: str,
) -> str:
    spec = build_prompt_spec(
        ledger,
        role,
        stage,
        decision_summary,
        default_supervisor_doc_path=default_supervisor_doc_path,
        default_release_result_template_path=default_release_result_template_path,
    )
    return spec.render()


def build_session_request_spec(
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
) -> SessionRequestSpec:
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
        "baseBranch": str(issue.get("baseBranch") or "main"),
    }
    selected_issue_number, selected_issue_branch, _selected_issue_base_branch = _queued_next_issue_fields(ledger)
    if selected_issue_number:
        request["selectedIssueNumber"] = selected_issue_number
        request["selectedIssueBranch"] = selected_issue_branch
    return SessionRequestSpec(payload=request)


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
    return build_session_request_spec(
        ledger,
        role=role,
        stage=stage,
        reason=reason,
        title=title,
        decision_summary=decision_summary,
        now=now,
        root_session_agent=root_session_agent,
        build_prompt=build_prompt,
    ).to_json()


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
