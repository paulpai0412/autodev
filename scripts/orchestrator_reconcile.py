"""Reconcile decision helpers for the autodev supervisor."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Protocol, cast
from uuid import uuid4


JsonObject = dict[str, object]
ReconcileResult = tuple[JsonObject, JsonObject, JsonObject | None]


def _canonical_artifact_path(path_text: str, *, base_dir: Path) -> Path:
    path = Path(path_text)
    return path if path.is_absolute() else base_dir / path


class IssuePacketRecord(Protocol):
    issue_number: str
    title: str
    branch: str
    issue_packet_path: str
    backing_type: str
    prior_handoff: str
    labels: list[str]
    parent_reference: str
    dependencies: list[str]


def queue_transition(
    ledger: JsonObject,
    *,
    next_role: str,
    next_stage: str,
    summary: str,
    updated_at: str,
    bump_ledger_revision: Callable[[JsonObject, str], None],
) -> None:
    current = cast(JsonObject, ledger["current"])
    history = cast(list[JsonObject], ledger["history"])
    history.append(
        {
            "recordedAt": updated_at,
            "fromRole": current.get("role", "unknown"),
            "fromStage": current.get("stage", "unknown"),
            "toRole": next_role,
            "toStage": next_stage,
            "reason": summary,
        }
    )
    ledger["current"] = {
        "role": next_role,
        "stage": next_stage,
        "status": "queued",
    }
    bump_ledger_revision(ledger, updated_at)
    ledger["updatedAt"] = updated_at


def set_failure(ledger: JsonObject, *, kind: str, summary: str, retryable: bool) -> None:
    ledger["lastFailure"] = {
        "kind": kind,
        "summary": summary,
        "retryable": retryable,
    }


def request_for_transition(
    ledger: JsonObject,
    *,
    next_role: str,
    next_stage: str,
    summary: str,
    build_session_request: Callable[..., JsonObject],
) -> JsonObject:
    issue = cast(dict[str, str], ledger["issue"])
    if next_role == "issue_worker":
        return build_session_request(
            ledger,
            role="issue_worker",
            stage=next_stage,
            reason=f"issue_worker dispatch for issue #{issue['number']}",
            title=f"Issue #{issue['number']} worker on {issue['branch']}",
            decision_summary=summary,
        )
    if next_role == "pr_verifier":
        artifacts = cast(dict[str, str], ledger["artifacts"])
        evidence_path = artifacts["evidencePacketPath"] or f"issue #{issue['number']} verifier evidence"
        return build_session_request(
            ledger,
            role="pr_verifier",
            stage=next_stage,
            reason=f"pr_verifier dispatch for issue #{issue['number']}",
            title=f"Verify issue #{issue['number']} using {evidence_path}",
            decision_summary=summary,
        )
    if next_role == "release_worker":
        return build_session_request(
            ledger,
            role="release_worker",
            stage=next_stage,
            reason=f"release_worker dispatch for issue #{issue['number']}",
            title=f"Release issue #{issue['number']} on {issue['branch']}",
            decision_summary=summary,
        )
    return build_session_request(
        ledger,
        role="main_orchestrator",
        stage=next_stage,
        reason=f"main_orchestrator recovery for issue #{issue['number']}",
        title=f"Recover or continue after issue #{issue['number']}",
        decision_summary=summary,
    )


def subagent_decision(
    ledger: JsonObject,
    *,
    next_role: str,
    next_stage: str,
    summary: str,
    build_prompt: Callable[[JsonObject, str, str, str], str],
) -> JsonObject:
    return {
        "action": "delegate_subagent",
        "next_role": next_role,
        "next_stage": next_stage,
        "summary": summary,
        "request_title": "",
        "subagent_prompt": build_prompt(ledger, next_role, next_stage, summary),
    }


def requeue_issue_worker(
    ledger: JsonObject,
    *,
    base_dir: Path,
    issue_number: str,
    updated_at: str,
    summary: str,
    next_stage: str,
    read_issue: Callable[[Path, str], JsonObject | None],
    transition_issue_state_if_possible: Callable[..., None],
    queue_transition_func: Callable[..., None],
    subagent_decision_func: Callable[..., JsonObject],
) -> tuple[JsonObject, None]:
    attempts = cast(dict[str, int], ledger["attempts"])
    attempts["issue_worker"] += 1
    issue_state = read_issue(base_dir, issue_number)
    if issue_state is not None and str(issue_state.get("state") or "") == "verifying":
        transition_issue_state_if_possible(
            base_dir=base_dir,
            issue_number=issue_number,
            to_state="running",
            command_id=uuid4().hex,
            updated_at=updated_at,
            reason=f"Retry issue_worker for issue #{issue_number} after verifier-directed repair.",
            from_state="verifying",
        )
    queue_transition_func(
        ledger,
        next_role="issue_worker",
        next_stage=next_stage,
        summary=summary,
        updated_at=updated_at,
    )
    return (
        subagent_decision_func(
            ledger,
            next_role="issue_worker",
            next_stage=next_stage,
            summary=summary,
        ),
        None,
    )


def queue_orchestrator_recovery(
    ledger: JsonObject,
    *,
    base_dir: Path,
    updated_at: str,
    summary: str,
    release_issue_execution: Callable[..., None],
    select_next_issue_packet: Callable[..., IssuePacketRecord | None],
    run_issue_packet_intake: Callable[[Path], bool],
    handoff_to_selected_issue: Callable[..., tuple[JsonObject, JsonObject, JsonObject]],
    request_for_transition_func: Callable[..., JsonObject],
    queue_transition_func: Callable[..., None],
    final_state: str | None = None,
) -> tuple[JsonObject, JsonObject, JsonObject]:
    current_issue = cast(dict[str, str], ledger["issue"])
    current_number = current_issue.get("number", "")
    if current_number:
        release_issue_execution(
            base_dir=base_dir,
            issue_number=current_number,
            restore_ready_for_agent=False,
            final_state=final_state,
        )
    selected_issue = select_next_issue_packet(
        base_dir,
        workflow=cast(dict[str, str], ledger["workflow"]),
        current_issue=current_issue,
    )
    if selected_issue is None and run_issue_packet_intake(base_dir):
        selected_issue = select_next_issue_packet(
            base_dir,
            workflow=cast(dict[str, str], ledger["workflow"]),
            current_issue=cast(dict[str, str], ledger["issue"]),
        )
    if selected_issue is not None:
        ledger["queuedNextIssue"] = {
            "selectedAt": updated_at,
            "reason": summary,
            "record": {
                "issue_number": selected_issue.issue_number,
                "title": selected_issue.title,
                "branch": selected_issue.branch,
                "issue_packet_path": selected_issue.issue_packet_path,
                "backing_type": selected_issue.backing_type,
                "prior_handoff": selected_issue.prior_handoff,
                "labels": list(selected_issue.labels),
                "parent_reference": selected_issue.parent_reference,
                "dependencies": list(selected_issue.dependencies),
            },
        }
        next_summary = f"{summary} Continue automatically with issue #{selected_issue.issue_number} via {selected_issue.issue_packet_path}."
        return handoff_to_selected_issue(
            ledger,
            selected_issue=selected_issue,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=next_summary,
        )
    attempts = cast(dict[str, int], ledger["attempts"])
    attempts["main_orchestrator"] += 1
    ledger.pop("queuedNextIssue", None)
    queue_transition_func(
        ledger,
        next_role="main_orchestrator",
        next_stage="issue_selection_or_recovery",
        summary=summary,
        updated_at=updated_at,
    )
    request = request_for_transition_func(
        ledger,
        next_role="main_orchestrator",
        next_stage="issue_selection_or_recovery",
        summary=summary,
    )
    return (
        ledger,
        {
            "action": "queue_next_session",
            "next_role": "main_orchestrator",
            "next_stage": "issue_selection_or_recovery",
            "summary": summary,
            "request_title": request["title"],
        },
        request,
    )


def quarantine_decision(ledger: JsonObject, *, summary: str) -> tuple[JsonObject, JsonObject, None]:
    current = cast(dict[str, str], ledger["current"])
    return (
        ledger,
        {
            "action": "hold_quarantined_issue",
            "next_role": current["role"],
            "next_stage": current["stage"],
            "summary": summary,
            "request_title": "",
        },
        None,
    )


def reconcile_issue_worker(
    ledger: JsonObject,
    *,
    base_dir: Path,
    issue: dict[str, str],
    current: dict[str, str],
    attempts: dict[str, int],
    limits: dict[str, int],
    artifacts: dict[str, str],
    updated_at: str,
    resolve_artifact_path: Callable[..., Path],
    parse_worker_result_file: Callable[[Path], JsonObject],
    is_successful_release_status: Callable[[str], bool],
    default_evidence_packet_path: Callable[[str, str], str],
    read_issue: Callable[[Path, str], JsonObject | None],
    read_artifact_fact: Callable[[dict[str, object] | None, str], dict[str, object]],
    record_artifact_status: Callable[..., None],
    set_failure_func: Callable[..., None],
    requeue_issue_worker_func: Callable[..., tuple[JsonObject, None]],
    queue_orchestrator_recovery_func: Callable[..., tuple[JsonObject, JsonObject, JsonObject]],
    queue_transition_func: Callable[..., None],
    subagent_decision_func: Callable[..., JsonObject],
) -> ReconcileResult:
    automation = cast(dict[str, object], ledger.get("automation", {}))
    primary_workspace_root = str(automation.get("primaryWorkspaceRoot") or "")
    worker_artifact_base_dir = Path(primary_workspace_root) if primary_workspace_root else base_dir
    worker_result_path = (
        _canonical_artifact_path(artifacts["workerResultPath"], base_dir=worker_artifact_base_dir)
        if primary_workspace_root
        else resolve_artifact_path(artifacts["workerResultPath"], base_dir=worker_artifact_base_dir)
    )
    if not worker_result_path.exists():
        if current.get("status") == "queued":
            summary = (
                f"Issue worker for issue #{issue['number']} is queued and has not produced {artifacts['workerResultPath']} yet. "
                "Keep the queued dispatch state unchanged."
            )
            return (
                ledger,
                {
                    "action": "no_change",
                    "next_role": current["role"],
                    "next_stage": current["stage"],
                    "summary": summary,
                    "request_title": "",
                },
                None,
            )
        summary = (
            f"Issue worker for issue #{issue['number']} ended without writing {artifacts['workerResultPath']}. "
            "Retry the worker session as a contract repair."
        )
        set_failure_func(ledger, kind="contract_invalid", summary=summary, retryable=True)
        if attempts["issue_worker"] < limits["issue_worker"]:
            decision, request = requeue_issue_worker_func(
                ledger,
                base_dir=base_dir,
                issue_number=issue["number"],
                updated_at=updated_at,
                summary=summary,
                next_stage="issue_worker_repair",
            )
            return ledger, decision, request
        return queue_orchestrator_recovery_func(
            ledger,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=summary,
            final_state="failed",
        )

    worker = parse_worker_result_file(worker_result_path)
    record_artifact_status(
        base_dir=base_dir,
        issue_number=issue["number"],
        artifact_kind="worker_result",
        artifact_path=worker_result_path,
        observed_at=updated_at,
        parsed=worker,
    )
    persisted_worker = read_artifact_fact(read_issue(base_dir, issue["number"]), "worker_result")
    if not bool(persisted_worker.get("parse_ok")):
        summary = (
            f"Issue worker for issue #{issue['number']} wrote {artifacts['workerResultPath']}, but the persisted worker_result fact is missing. "
            "Treat this as a contract-invalid worker result and retry or recover instead of advancing."
        )
        set_failure_func(ledger, kind="contract_invalid", summary=summary, retryable=True)
        return queue_orchestrator_recovery_func(ledger, base_dir=base_dir, updated_at=updated_at, summary=summary)
    status = cast(str, persisted_worker.get("status") or worker["status"])
    if is_successful_release_status(status):
        pr_number = cast(str, persisted_worker.get("pr_number") or worker["pr_number"])
        if not pr_number or pr_number == "none":
            summary = (
                f"Issue worker for issue #{issue['number']} reported success without a PR number. "
                "Route to main_orchestrator recovery instead of stalling."
            )
            set_failure_func(ledger, kind="contract_invalid", summary=summary, retryable=True)
            return queue_orchestrator_recovery_func(ledger, base_dir=base_dir, updated_at=updated_at, summary=summary)
        artifacts["evidencePacketPath"] = default_evidence_packet_path(issue["number"], pr_number)
        attempts["pr_verifier"] += 1
        summary = (
            f"Issue worker for issue #{issue['number']} succeeded. The main_orchestrator should delegate a "
            f"pr_verifier subagent for PR #{pr_number}."
        )
        set_failure_func(ledger, kind="none", summary="", retryable=True)
        queue_transition_func(
            ledger,
            next_role="pr_verifier",
            next_stage="pr_verifier_execution",
            summary=summary,
            updated_at=updated_at,
        )
        return ledger, subagent_decision_func(
            ledger,
            next_role="pr_verifier",
            next_stage="pr_verifier_execution",
            summary=summary,
        ), None

    summary = cast(str, worker["next_recommended_step"])
    failure_kind = cast(str, worker["failure_kind"] or "issue_worker_retry")
    retryable = cast(bool | None, worker["retryable"])
    set_failure_func(
        ledger,
        kind=failure_kind,
        summary=summary,
        retryable=True if retryable is None else retryable,
    )
    if attempts["issue_worker"] < limits["issue_worker"] and (retryable is None or retryable):
        retry_summary = (
            f"Issue worker for issue #{issue['number']} returned {status}. The main_orchestrator should retry "
            "with a fresh issue_worker subagent and keep the workflow moving."
        )
        decision, request = requeue_issue_worker_func(
            ledger,
            base_dir=base_dir,
            issue_number=issue["number"],
            updated_at=updated_at,
            summary=retry_summary,
            next_stage="issue_worker_repair",
        )
        return ledger, decision, request
    recovery_summary = (
        f"Issue worker for issue #{issue['number']} exhausted retries after status {status}. Route to "
        "main_orchestrator recovery so the workflow can classify the blocker or move to another ready issue."
    )
    return queue_orchestrator_recovery_func(
        ledger,
        base_dir=base_dir,
        updated_at=updated_at,
        summary=recovery_summary,
        final_state="failed",
    )


def reconcile_pr_verifier(
    ledger: JsonObject,
    *,
    base_dir: Path,
    issue: dict[str, str],
    attempts: dict[str, int],
    limits: dict[str, int],
    artifacts: dict[str, str],
    updated_at: str,
    resolve_artifact_path: Callable[..., Path],
    parse_evidence_packet_file: Callable[[Path], JsonObject],
    default_release_result_path: Callable[[str, str], str],
    read_issue: Callable[[Path, str], JsonObject | None],
    read_artifact_fact: Callable[[dict[str, object] | None, str], dict[str, object]],
    record_artifact_status: Callable[..., None],
    record_current_verifier_session: Callable[..., None],
    transition_issue_state_if_possible: Callable[..., None],
    set_failure_func: Callable[..., None],
    requeue_issue_worker_func: Callable[..., tuple[JsonObject, None]],
    queue_orchestrator_recovery_func: Callable[..., tuple[JsonObject, JsonObject, JsonObject]],
    queue_transition_func: Callable[..., None],
    subagent_decision_func: Callable[..., JsonObject],
) -> ReconcileResult:
    evidence_packet_path = resolve_artifact_path(artifacts["evidencePacketPath"], base_dir=base_dir)
    if not artifacts["evidencePacketPath"] or not evidence_packet_path.exists():
        summary = (
            f"pr_verifier for issue #{issue['number']} ended without writing {artifacts['evidencePacketPath'] or 'an evidence packet'}. "
            "Retry the verifier once before recovery."
        )
        set_failure_func(ledger, kind="contract_invalid", summary=summary, retryable=True)
        if attempts["pr_verifier"] < limits["pr_verifier"]:
            attempts["pr_verifier"] += 1
            transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=issue["number"],
                to_state="verifying",
                command_id=uuid4().hex,
                updated_at=updated_at,
                reason=f"Retry pr_verifier for issue #{issue['number']} after missing evidence packet.",
            )
            queue_transition_func(
                ledger,
                next_role="pr_verifier",
                next_stage="pr_verifier_execution",
                summary=summary,
                updated_at=updated_at,
            )
            return ledger, subagent_decision_func(
                ledger,
                next_role="pr_verifier",
                next_stage="pr_verifier_execution",
                summary=summary,
            ), None
        return queue_orchestrator_recovery_func(
            ledger,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=summary,
            final_state="failed",
        )

    evidence = parse_evidence_packet_file(evidence_packet_path)
    record_artifact_status(
        base_dir=base_dir,
        issue_number=issue["number"],
        artifact_kind="evidence_packet",
        artifact_path=evidence_packet_path,
        observed_at=updated_at,
        parsed=evidence,
    )
    persisted_evidence = read_artifact_fact(read_issue(base_dir, issue["number"]), "evidence_packet")
    if not bool(persisted_evidence.get("parse_ok")):
        summary = (
            f"pr_verifier for issue #{issue['number']} wrote {artifacts['evidencePacketPath'] or 'an evidence packet'}, but the persisted evidence_packet fact is missing. "
            "Do not advance to release_worker until SQLite has acknowledged the verifier artifact."
        )
        set_failure_func(ledger, kind="contract_invalid", summary=summary, retryable=True)
        return queue_orchestrator_recovery_func(ledger, base_dir=base_dir, updated_at=updated_at, summary=summary)
    verifier_session_id = cast(str, persisted_evidence.get("verifier_session_id") or evidence.get("verifier_session_id") or "")
    record_current_verifier_session(
        base_dir=base_dir,
        issue_number=issue["number"],
        verifier_session_id=verifier_session_id,
        updated_at=updated_at,
    )
    status = cast(str, persisted_evidence.get("status") or evidence["status"])
    if status == "pass":
        pr_number = cast(str, persisted_evidence.get("pr_number") or evidence["pr_number"])
        if not pr_number or pr_number == "none":
            summary = (
                f"Verifier for issue #{issue['number']} passed without a PR number. Route to main_orchestrator recovery instead of waiting."
            )
            set_failure_func(ledger, kind="contract_invalid", summary=summary, retryable=True)
            return queue_orchestrator_recovery_func(
                ledger,
                base_dir=base_dir,
                updated_at=updated_at,
                summary=summary,
                final_state="failed",
            )
        artifacts["releaseResultPath"] = default_release_result_path(issue["number"], pr_number)
        attempts["release_worker"] += 1
        summary = f"Verifier for issue #{issue['number']} passed. The main_orchestrator should delegate release_worker for PR #{pr_number}."
        set_failure_func(ledger, kind="none", summary="", retryable=True)
        record_current_verifier_session(
            base_dir=base_dir,
            issue_number=issue["number"],
            verifier_session_id=verifier_session_id,
            updated_at=updated_at,
        )
        queue_transition_func(
            ledger,
            next_role="release_worker",
            next_stage="release_worker_execution",
            summary=summary,
            updated_at=updated_at,
        )
        return ledger, subagent_decision_func(
            ledger,
            next_role="release_worker",
            next_stage="release_worker_execution",
            summary=summary,
        ), None

    failure_kind = cast(str, evidence["failure_kind"] or "verifier_retry")
    retryable = cast(bool | None, evidence["retryable"])
    summary = cast(str, evidence["next_recommended_step"])
    set_failure_func(
        ledger,
        kind=failure_kind,
        summary=summary,
        retryable=True if retryable is None else retryable,
    )
    if status == "fail" and attempts["issue_worker"] < limits["issue_worker"]:
        retry_summary = (
            f"Verifier for issue #{issue['number']} failed. Return the issue to a fresh issue_worker subagent instead of waiting for human intervention."
        )
        decision, request = requeue_issue_worker_func(
            ledger,
            base_dir=base_dir,
            issue_number=issue["number"],
            updated_at=updated_at,
            summary=retry_summary,
            next_stage="issue_worker_repair",
        )
        return ledger, decision, request
    if status == "blocked" and attempts["pr_verifier"] < limits["pr_verifier"] and retryable:
        attempts["pr_verifier"] += 1
        retry_summary = (
            f"Verifier for issue #{issue['number']} is retryable-blocked. Rerun a fresh pr_verifier subagent once more before escalating."
        )
        transition_issue_state_if_possible(
            base_dir=base_dir,
            issue_number=issue["number"],
            to_state="verifying",
            command_id=uuid4().hex,
            updated_at=updated_at,
            reason=f"Retry pr_verifier for issue #{issue['number']} after retryable blocked status.",
        )
        queue_transition_func(
            ledger,
            next_role="pr_verifier",
            next_stage="pr_verifier_execution",
            summary=retry_summary,
            updated_at=updated_at,
        )
        return ledger, subagent_decision_func(
            ledger,
            next_role="pr_verifier",
            next_stage="pr_verifier_execution",
            summary=retry_summary,
        ), None
    recovery_summary = (
        f"Verifier for issue #{issue['number']} ended with status {status}. Route to main_orchestrator recovery so the workflow can classify the blocker and continue with another ready issue when possible."
    )
    return queue_orchestrator_recovery_func(
        ledger,
        base_dir=base_dir,
        updated_at=updated_at,
        summary=recovery_summary,
        final_state="failed",
    )


def reconcile_release_worker(
    ledger: JsonObject,
    *,
    base_dir: Path,
    issue: dict[str, str],
    attempts: dict[str, int],
    limits: dict[str, int],
    artifacts: dict[str, str],
    updated_at: str,
    transient_release_blockers: set[str],
    resolve_artifact_path: Callable[..., Path],
    parse_release_result_file: Callable[[Path], JsonObject],
    read_artifact_fact: Callable[[dict[str, object] | None, str], dict[str, object]],
    record_artifact_status: Callable[..., None],
    read_issue: Callable[[Path, str], JsonObject | None],
    transition_issue_state_if_possible: Callable[..., None],
    set_failure_func: Callable[..., None],
    queue_orchestrator_recovery_func: Callable[..., tuple[JsonObject, JsonObject, JsonObject]],
    queue_transition_func: Callable[..., None],
    subagent_decision_func: Callable[..., JsonObject],
) -> ReconcileResult:
    release_result_path = resolve_artifact_path(artifacts["releaseResultPath"], base_dir=base_dir)
    if not artifacts["releaseResultPath"] or not release_result_path.exists():
        summary = (
            f"release_worker for issue #{issue['number']} ended without writing {artifacts['releaseResultPath'] or 'a release result'}. "
            "Retry release once before recovery."
        )
        set_failure_func(ledger, kind="contract_invalid", summary=summary, retryable=True)
        if attempts["release_worker"] < limits["release_worker"]:
            attempts["release_worker"] += 1
            transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=issue["number"],
                to_state="verifying",
                command_id=uuid4().hex,
                updated_at=updated_at,
                reason=f"Retry release_worker for issue #{issue['number']} after missing release result.",
            )
            queue_transition_func(
                ledger,
                next_role="release_worker",
                next_stage="release_worker_execution",
                summary=summary,
                updated_at=updated_at,
            )
            return ledger, subagent_decision_func(
                ledger,
                next_role="release_worker",
                next_stage="release_worker_execution",
                summary=summary,
            ), None
        return queue_orchestrator_recovery_func(
            ledger,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=summary,
            final_state="failed",
        )

    release = parse_release_result_file(release_result_path)
    record_artifact_status(
        base_dir=base_dir,
        issue_number=issue["number"],
        artifact_kind="release_result",
        artifact_path=release_result_path,
        observed_at=updated_at,
        parsed=release,
    )
    issue_state = read_issue(base_dir, issue["number"])
    persisted_release = read_artifact_fact(issue_state, "release_result")
    if not bool(persisted_release.get("parse_ok")):
        summary = (
            f"release_worker for issue #{issue['number']} wrote {artifacts['releaseResultPath'] or 'a release result'}, but the persisted release_result fact is missing. "
            "Do not complete the issue until SQLite has acknowledged the release artifact."
        )
        set_failure_func(ledger, kind="contract_invalid", summary=summary, retryable=True)
        return queue_orchestrator_recovery_func(ledger, base_dir=base_dir, updated_at=updated_at, summary=summary)
    verifier_session_id = str(issue_state.get("current_verifier_session_id") or "") if issue_state else ""
    status = cast(str, persisted_release.get("status") or release["status"])
    if status == "success":
        if issue_state and issue_state.get("state") == "running":
            transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=issue["number"],
                to_state="verifying",
                command_id=uuid4().hex,
                updated_at=updated_at,
                reason=f"Issue #{issue['number']} still occupies capacity until verifier-backed completion is recorded.",
                from_state="running",
                current_verifier_session_id=verifier_session_id or None,
            )
        if read_issue(base_dir, issue["number"]):
            transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=issue["number"],
                to_state="completed",
                command_id=uuid4().hex,
                updated_at=updated_at,
                reason=f"Release worker completed issue #{issue['number']} after verifier-owned evidence passed.",
                from_state="verifying",
                current_verifier_session_id=verifier_session_id or None,
            )
        summary = (
            f"Release worker completed issue #{issue['number']}. Hand off to main_orchestrator to select the next ready issue and keep the workflow moving."
        )
        set_failure_func(ledger, kind="none", summary="", retryable=True)
        return queue_orchestrator_recovery_func(
            ledger,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=summary,
            final_state="completed",
        )

    blocked_reason = cast(str, persisted_release.get("blocked_reason") or release["blocked_reason"])
    retryable = cast(bool | None, release["retryable"])
    failure_kind = cast(str, release["failure_kind"] or blocked_reason or "release_blocked")
    summary = cast(str, release["next_recommended_step"])
    set_failure_func(
        ledger,
        kind=failure_kind,
        summary=summary,
        retryable=True if retryable is None else retryable,
    )
    if blocked_reason in transient_release_blockers and attempts["release_worker"] < limits["release_worker"] and (retryable is None or retryable):
        attempts["release_worker"] += 1
        retry_summary = (
            f"Release worker for issue #{issue['number']} hit transient blocker {blocked_reason}. Retry the release_worker subagent instead of stalling."
        )
        transition_issue_state_if_possible(
            base_dir=base_dir,
            issue_number=issue["number"],
            to_state="verifying",
            command_id=uuid4().hex,
            updated_at=updated_at,
            reason=f"Retry release_worker for issue #{issue['number']} after transient release blocker {blocked_reason}.",
            current_verifier_session_id=verifier_session_id or None,
        )
        queue_transition_func(
            ledger,
            next_role="release_worker",
            next_stage="release_worker_execution",
            summary=retry_summary,
            updated_at=updated_at,
        )
        return ledger, subagent_decision_func(
            ledger,
            next_role="release_worker",
            next_stage="release_worker_execution",
            summary=retry_summary,
        ), None
    recovery_summary = (
        f"Release worker for issue #{issue['number']} is blocked by {blocked_reason or status}. Route to main_orchestrator recovery so the broader workflow can continue without waiting for a human reply."
    )
    return queue_orchestrator_recovery_func(
        ledger,
        base_dir=base_dir,
        updated_at=updated_at,
        summary=recovery_summary,
        final_state="failed",
    )


def reconcile_orchestrator_bootstrap(
    ledger: JsonObject,
    *,
    issue: dict[str, str],
    attempts: dict[str, int],
    updated_at: str,
    set_failure_func: Callable[..., None],
    queue_transition_func: Callable[..., None],
    subagent_decision_func: Callable[..., JsonObject],
) -> ReconcileResult:
    summary = (
        f"Issue #{issue['number']} passed orchestrator bootstrap. The main_orchestrator should delegate "
        "an issue_worker subagent and keep the workflow moving without waiting for a human reply."
    )
    attempts["issue_worker"] += 1
    set_failure_func(ledger, kind="none", summary="", retryable=True)
    queue_transition_func(
        ledger,
        next_role="issue_worker",
        next_stage="issue_worker_execution",
        summary=summary,
        updated_at=updated_at,
    )
    return (
        ledger,
        subagent_decision_func(
            ledger,
            next_role="issue_worker",
            next_stage="issue_worker_execution",
            summary=summary,
        ),
        None,
    )


def reconcile_issue_selection_or_recovery(
    ledger: JsonObject,
    *,
    base_dir: Path,
    issue: dict[str, str],
    artifacts: dict[str, str],
    updated_at: str,
    resolve_artifact_path: Callable[..., Path],
    parse_release_result_file: Callable[[Path], JsonObject],
    read_artifact_fact: Callable[[dict[str, object] | None, str], dict[str, object]],
    record_artifact_status: Callable[..., None],
    read_issue: Callable[[Path, str], JsonObject | None],
    is_successful_release_status: Callable[[str], bool],
    set_failure_func: Callable[..., None],
    queue_orchestrator_recovery_func: Callable[..., tuple[JsonObject, JsonObject, JsonObject]],
) -> ReconcileResult | None:
    release_result_path = resolve_artifact_path(artifacts["releaseResultPath"], base_dir=base_dir)
    issue_state = read_issue(base_dir, issue["number"])
    if artifacts["releaseResultPath"] and release_result_path.exists():
        release = parse_release_result_file(release_result_path)
        record_artifact_status(
            base_dir=base_dir,
            issue_number=issue["number"],
            artifact_kind="release_result",
            artifact_path=release_result_path,
            observed_at=updated_at,
            parsed=release,
        )
        persisted_release = read_artifact_fact(read_issue(base_dir, issue["number"]), "release_result")
        issue_runtime_state = str(issue_state.get("state") or "") if issue_state else ""
        if issue_runtime_state in {"failed", "ready"} and bool(persisted_release.get("parse_ok")) and is_successful_release_status(cast(str, persisted_release.get("status") or release["status"])):
            summary = (
                f"Late successful release result for issue #{issue['number']} arrived after failure recovery. "
                "Reconcile the control plane into completed and continue issue selection."
            )
            set_failure_func(ledger, kind="none", summary="", retryable=True)
            return queue_orchestrator_recovery_func(
                ledger,
                base_dir=base_dir,
                updated_at=updated_at,
                summary=summary,
                final_state="completed",
            )
    last_failure = cast(dict[str, object], ledger.get("lastFailure", {}))
    if issue_state and str(issue_state.get("state") or "") == "failed" and bool(last_failure.get("retryable")):
        summary = cast(str, last_failure.get("summary") or "") or (
            f"Issue #{issue['number']} is in retryable failed recovery. Launch a main_orchestrator recovery session and continue the workflow without waiting for a human reply."
        )
        return queue_orchestrator_recovery_func(
            ledger,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=summary,
            final_state=None,
        )
    return None


def no_change_decision(ledger: JsonObject, *, current: dict[str, str], updated_at: str, bump_ledger_revision: Callable[[JsonObject, str], None]) -> ReconcileResult:
    summary = f"Supervisor found role={current['role']} stage={current['stage']} with no automatic transition. Keep the current state unchanged."
    ledger["updatedAt"] = updated_at
    bump_ledger_revision(ledger, updated_at)
    return (
        ledger,
        {
            "action": "no_change",
            "next_role": current["role"],
            "next_stage": current["stage"],
            "summary": summary,
            "request_title": "",
        },
        None,
    )
