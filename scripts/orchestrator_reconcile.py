"""Reconcile decision helpers for the autodev supervisor."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Protocol, cast
from urllib.parse import unquote, urlparse
from uuid import uuid4


JsonObject = dict[str, object]
ReconcileResult = tuple[JsonObject, JsonObject, JsonObject | None]

def _set_artifact_ref(artifacts: dict[str, str], semantic_key: str, value: str) -> None:
    artifacts[semantic_key] = value


def _stored_fact(artifacts: dict[str, str], entry_type: str, read_artifact_fact: Callable[[str], dict[str, object]]) -> dict[str, object]:
    _ = artifacts
    return read_artifact_fact(entry_type)


def _extract_pr_number_from_fact(fact: dict[str, object]) -> str:
    pr_number = str(fact.get("pr_number") or "").strip()
    if pr_number and pr_number != "none":
        return pr_number

    pr_url = str(fact.get("pr_url") or "").strip()
    if pr_url:
        for segment in reversed(pr_url.rstrip("/").split("/")):
            if segment.isdigit():
                return segment

    pr_payload_raw = fact.get("pr")
    if isinstance(pr_payload_raw, dict):
        pr_payload = cast(dict[str, object], pr_payload_raw)
        nested_number = str(pr_payload.get("number") or "").strip()
        if nested_number and nested_number != "none":
            return nested_number
        nested_url = str(pr_payload.get("url") or "").strip()
        if nested_url:
            for segment in reversed(nested_url.rstrip("/").split("/")):
                if segment.isdigit():
                    return segment

    return ""


def _artifact_recorded_at(payload: dict[str, object], *, fallback: str) -> str:
    recorded_at = str(payload.get("completed_at") or payload.get("recorded_at") or "").strip()
    return recorded_at or fallback


def _issue_packet_requires_browser_e2e(issue_packet: dict[str, object] | None) -> bool:
    if not isinstance(issue_packet, dict):
        return False
    verifier_manual_qa_raw = issue_packet.get("verifier_manual_qa")
    if not isinstance(verifier_manual_qa_raw, dict):
        return False
    surface_raw = str(verifier_manual_qa_raw.get("surface") or "").strip().lower()
    if not surface_raw:
        return False
    return any(token in surface_raw for token in ("browser", "static-html", "static html", "web_ui", "web ui", "html"))


def _worker_result_web_surface_changed(worker_result: dict[str, object]) -> bool:
    files_changed_raw = worker_result.get("files_changed")
    paths: list[str] = []
    if isinstance(files_changed_raw, list):
        for item in files_changed_raw:
            if isinstance(item, dict):
                path_text = str(item.get("path") or "").strip().lower()
                if path_text:
                    paths.append(path_text)
            elif isinstance(item, str):
                path_text = item.strip().lower()
                if path_text:
                    paths.append(path_text)
    for path_text in paths:
        if path_text.endswith((".html", ".htm", ".css", ".tsx", ".jsx", ".vue", ".svelte")):
            return True
        if any(token in path_text for token in ("/index.html", "/public/", "/static/", "/templates/", "/frontend/", "/ui/", "/pages/", "/components/")):
            return True
    return False


def _evidence_has_browser_e2e_gate(evidence_packet: dict[str, object]) -> bool:
    gates_raw = evidence_packet.get("gates")
    if not isinstance(gates_raw, dict):
        return False

    surface_gate_raw = gates_raw.get("surface_qa_gate")

    # Preferred contract: surface_qa_gate is an object with status + evidence_ref + evidence_kind.
    # evidence_kind must be "browser" to confirm real browser execution (not headless unit tests).
    if isinstance(surface_gate_raw, dict):
        surface_gate = cast(dict[str, object], surface_gate_raw)
        status = str(surface_gate.get("status") or "").strip().lower()
        evidence_ref = str(surface_gate.get("evidence_ref") or "").strip()
        evidence_kind = str(surface_gate.get("evidence_kind") or "").strip().lower()
        if status == "pass" and bool(evidence_ref) and evidence_kind == "browser":
            return True
        # Backward-compatible: dict surface_qa_gate without evidence_kind falls through to legacy check.

    # Backward-compatible contract: legacy flat gate strings, or dict surface_qa_gate missing evidence_kind.
    surface_gate_pass = False
    if isinstance(surface_gate_raw, dict):
        surface_gate = cast(dict[str, object], surface_gate_raw)
        surface_gate_pass = str(surface_gate.get("status") or "").strip().lower() == "pass"
    elif isinstance(surface_gate_raw, str):
        surface_gate_pass = surface_gate_raw.strip().lower() == "pass"

    if not surface_gate_pass:
        return False

    browser_gate_raw = gates_raw.get("browser_e2e_gate")
    browser_gate_pass = False
    if isinstance(browser_gate_raw, str):
        browser_gate_pass = browser_gate_raw.strip().lower() == "pass"
    elif isinstance(browser_gate_raw, dict):
        browser_gate = cast(dict[str, object], browser_gate_raw)
        browser_gate_pass = str(browser_gate.get("status") or "").strip().lower() == "pass"

    browser_evidence_raw = evidence_packet.get("browser_e2e_evidence")
    has_browser_evidence = isinstance(browser_evidence_raw, dict) and bool(browser_evidence_raw)

    artifact_manifest_raw = evidence_packet.get("artifact_manifest")
    has_manifest = isinstance(artifact_manifest_raw, list) and len(artifact_manifest_raw) > 0

    return browser_gate_pass and (has_browser_evidence or has_manifest)


def _browser_e2e_gate_deficiency(evidence_packet: dict[str, object]) -> str:
    gates_raw = evidence_packet.get("gates")
    if not isinstance(gates_raw, dict):
        return "missing key: gates (object)"

    surface_gate_raw = gates_raw.get("surface_qa_gate")
    if not isinstance(surface_gate_raw, dict):
        return "missing key: gates.surface_qa_gate (object)"

    surface_gate = cast(dict[str, object], surface_gate_raw)
    status = str(surface_gate.get("status") or "").strip().lower()
    if status != "pass":
        return "invalid value: gates.surface_qa_gate.status must be 'pass'"

    evidence_ref = str(surface_gate.get("evidence_ref") or "").strip()
    if not evidence_ref:
        return "missing key: gates.surface_qa_gate.evidence_ref (non-empty string)"

    evidence_kind = str(surface_gate.get("evidence_kind") or "").strip().lower()
    if evidence_kind != "browser":
        return (
            f"invalid value: gates.surface_qa_gate.evidence_kind must be 'browser' "
            f"(got {evidence_kind!r}); smoke_test or unit_test is not accepted as browser_e2e evidence"
        )

    return "missing browser_e2e evidence contract fields"


def _browser_surface_artifact_validation_deficiency(base_dir: Path, evidence_packet: dict[str, object]) -> str:
    gates_raw = evidence_packet.get("gates")
    if not isinstance(gates_raw, dict):
        return "invalid evidence_ref: missing key gates.surface_qa_gate"

    surface_gate_raw = gates_raw.get("surface_qa_gate")
    if not isinstance(surface_gate_raw, dict):
        return "invalid evidence_ref: gates.surface_qa_gate must be an object"

    surface_gate = cast(dict[str, object], surface_gate_raw)
    status = str(surface_gate.get("status") or "").strip().lower()
    if status != "pass":
        return "invalid evidence_ref: gates.surface_qa_gate.status must be 'pass'"

    evidence_ref = str(surface_gate.get("evidence_ref") or "").strip()
    if not evidence_ref:
        return "invalid evidence_ref: gates.surface_qa_gate.evidence_ref is empty"

    normalized_ref = evidence_ref.split("#", 1)[0].split("?", 1)[0].strip()
    if not normalized_ref:
        return "invalid evidence_ref: gates.surface_qa_gate.evidence_ref is blank after normalization"

    if normalized_ref.startswith("db:"):
        if normalized_ref == "db:":
            return "invalid evidence_ref: db reference is missing target"
        return ""

    parsed_ref = urlparse(normalized_ref)
    if parsed_ref.scheme in {"file", "browser"}:
        decoded_path = unquote(parsed_ref.path or "")
        host = (parsed_ref.netloc or "").strip()
        if host and host not in {"localhost", "127.0.0.1"}:
            decoded_path = f"//{host}{decoded_path}"
        if len(decoded_path) >= 3 and decoded_path[0] == "/" and decoded_path[1].isalpha() and decoded_path[2] == ":":
            decoded_path = decoded_path[1:]
        if decoded_path:
            normalized_ref = decoded_path

    artifact_path = Path(normalized_ref)
    if not artifact_path.is_absolute():
        artifact_path = base_dir / artifact_path

    if not artifact_path.exists() or not artifact_path.is_file():
        return f"missing browser artifact: {normalized_ref}"

    return ""


class IssuePacketRecord(Protocol):
    issue_number: str
    title: str
    branch: str
    base_branch: str
    backing_type: str
    prior_handoff: str
    labels: list[str]
    parent_reference: str
    dependencies: list[str]


class IssueSelectionCandidate(Protocol):
    issue_number: str
    branch: str


def queue_transition(
    ledger: JsonObject,
    *,
    next_role: str,
    next_stage: str,
    summary: str,
    updated_at: str,
    bump_ledger_revision: Callable[[JsonObject, str], None],
) -> None:
    del summary, updated_at, bump_ledger_revision
    ledger["current"] = {
        "role": next_role,
        "stage": next_stage,
        "status": "queued",
    }


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
        return build_session_request(
            ledger,
            role="pr_verifier",
            stage=next_stage,
            reason=f"pr_verifier dispatch for issue #{issue['number']}",
            title=f"Verify issue #{issue['number']} from DB-backed evidence",
            decision_summary=summary,
        )
    if next_role == "main_orchestrator" and next_stage == "release_root_execution":
        return build_session_request(
            ledger,
            role="main_orchestrator",
            stage=next_stage,
            reason=f"release root-session dispatch for issue #{issue['number']}",
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
    select_next_issue_candidate: Callable[..., IssueSelectionCandidate | None],
    load_issue_packet_from_db: Callable[[Path, str], IssuePacketRecord | None],
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
    selected_candidate = select_next_issue_candidate(
        base_dir,
        current_issue_number=current_issue.get("number", ""),
        current_parent_reference=current_issue.get("parentReference", ""),
    )
    selected_issue: IssuePacketRecord | None = None
    if selected_candidate is not None:
        selected_issue = load_issue_packet_from_db(base_dir, selected_candidate.issue_number)
        if selected_issue is None:
            selected_candidate = None
    if selected_candidate is not None and selected_issue is not None:
        ledger["queuedNextIssue"] = {
            "issue_number": selected_issue.issue_number,
            "branch": selected_issue.branch,
            "base_branch": selected_issue.base_branch,
        }
        next_summary = f"{summary} Continue automatically with issue #{selected_issue.issue_number}."
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
    is_successful_release_status: Callable[[str], bool],
    read_artifact_fact: Callable[[str], dict[str, object]],
    record_pr_opened: Callable[..., dict[str, object]],
    set_failure_func: Callable[..., None],
    requeue_issue_worker_func: Callable[..., tuple[JsonObject, None]],
    queue_orchestrator_recovery_func: Callable[..., tuple[JsonObject, JsonObject, JsonObject]],
    queue_transition_func: Callable[..., None],
    subagent_decision_func: Callable[..., JsonObject],
) -> ReconcileResult:
    persisted_worker = _stored_fact(artifacts, "worker_result", read_artifact_fact)
    if not bool(persisted_worker.get("parse_ok")):
        if current.get("status") == "queued":
            summary = (
                f"Issue worker for issue #{issue['number']} is queued and SQLite has not recorded a worker_result yet. "
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
            f"Issue worker for issue #{issue['number']} ended without recording a worker_result in SQLite. "
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
    status = cast(str, persisted_worker.get("status") or "")
    if is_successful_release_status(status):
        pr_number = _extract_pr_number_from_fact(persisted_worker)
        attempts["pr_verifier"] += 1
        if pr_number and pr_number != "none":
            worker_session_id = str(
                persisted_worker.get("worker_session_id")
                or persisted_worker.get("session_id")
                or ""
            )
            record_pr_opened(
                base_dir=base_dir,
                issue_number=issue["number"],
                pr_number=pr_number,
                created_at=_artifact_recorded_at(persisted_worker, fallback=updated_at),
                verifier_session_id=worker_session_id,
                command_id=str(persisted_worker.get("command_id") or ""),
                summary=(
                    f"Record PR #{pr_number} opened for issue #{issue['number']} from worker_result before verifier handoff."
                ),
                payload={
                    "issue_number": issue["number"],
                    "pr_number": pr_number,
                    "head_branch": issue.get("branch", ""),
                    "base_branch": issue.get("baseBranch", "main"),
                    "source_artifact": "worker_result",
                },
            )
            summary = (
                f"Issue worker for issue #{issue['number']} succeeded. The main_orchestrator should delegate a "
                f"pr_verifier subagent for PR #{pr_number}."
            )
        else:
            summary = (
                f"Issue worker for issue #{issue['number']} succeeded. The main_orchestrator should delegate a "
                "pr_verifier subagent to verify the pushed branch and create or record the formal PR."
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

    summary = cast(str, persisted_worker.get("next_recommended_step") or "")
    failure_kind = cast(str, persisted_worker.get("failure_kind") or "issue_worker_retry")
    retryable = cast(bool | None, persisted_worker.get("retryable"))
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
    current: dict[str, str],
    attempts: dict[str, int],
    limits: dict[str, int],
    artifacts: dict[str, str],
    updated_at: str,
    read_issue: Callable[[Path, str], JsonObject | None],
    read_issue_packet: Callable[[Path, str], JsonObject],
    read_artifact_fact: Callable[[str], dict[str, object]],
    read_session_outcome: Callable[[str], object | None],
    record_pr_opened: Callable[..., dict[str, object]],
    record_current_verifier_session: Callable[..., None],
    transition_issue_state_if_possible: Callable[..., None],
    set_failure_func: Callable[..., None],
    requeue_issue_worker_func: Callable[..., tuple[JsonObject, None]],
    queue_orchestrator_recovery_func: Callable[..., tuple[JsonObject, JsonObject, JsonObject]],
    queue_transition_func: Callable[..., None],
    subagent_decision_func: Callable[..., JsonObject],
) -> ReconcileResult:
    issue_state = read_issue(base_dir, issue["number"])
    current_issue_state = str(issue_state.get("state") or "") if issue_state else ""
    verifier_session_id = str(issue_state.get("current_session_id") or "") if issue_state else ""
    persisted_evidence = _stored_fact(artifacts, "evidence_packet", read_artifact_fact)
    if not bool(persisted_evidence.get("parse_ok")):
        current_status = str(current.get("status") or "")
        outcome_status = ""
        session_terminal_without_artifact = False
        if verifier_session_id and current_status in {"running", "queued"}:
            outcome = read_session_outcome(verifier_session_id)
            if isinstance(outcome, dict):
                outcome_status = str(outcome.get("status") or "")
            else:
                outcome_status = str(getattr(outcome, "status", "") or "")
            if outcome_status and outcome_status not in {"running", "queued", "unknown"}:
                session_terminal_without_artifact = True
        session_inflight = current_status == "running" or (
            current_status == "queued" and outcome_status in {"running", "queued", "unknown"}
        )
        if session_inflight and not session_terminal_without_artifact:
            summary = (
                f"pr_verifier for issue #{issue['number']} is {current_status} and SQLite has not recorded "
                "evidence_packet yet. Keep the queued/running dispatch state unchanged."
            )
            return (
                ledger,
                {
                    "action": "no_change",
                    "next_role": current.get("role") or "pr_verifier",
                    "next_stage": current.get("stage") or "pr_verifier_execution",
                    "summary": summary,
                    "request_title": "",
                },
                None,
            )
        summary = (
            f"pr_verifier for issue #{issue['number']} ended without recording evidence_packet in SQLite. "
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
    verifier_session_id = cast(str, persisted_evidence.get("verifier_session_id") or "")
    record_current_verifier_session(
        base_dir=base_dir,
        issue_number=issue["number"],
        verifier_session_id=verifier_session_id,
        updated_at=updated_at,
    )
    status = cast(str, persisted_evidence.get("status") or "")
    if status == "pass":
        persisted_worker = _stored_fact(artifacts, "worker_result", read_artifact_fact)
        issue_packet = read_issue_packet(base_dir, issue["number"])
        browser_e2e_required = _issue_packet_requires_browser_e2e(issue_packet) or _worker_result_web_surface_changed(persisted_worker)
        if browser_e2e_required and not _evidence_has_browser_e2e_gate(persisted_evidence):
            deficiency = _browser_e2e_gate_deficiency(persisted_evidence)
            summary = (
                f"Verifier for issue #{issue['number']} reported pass without required browser_e2e_gate evidence in evidence_packet "
                f"({deficiency}). Retry pr_verifier with gates.surface_qa_gate set to "
                "{status: 'pass', evidence_ref: '<non-empty>', evidence_kind: 'browser'} "
                "or provide legacy-compatible browser_e2e evidence fields before acceptance."
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
                    reason=f"Retry pr_verifier for issue #{issue['number']} after missing browser_e2e_gate evidence.",
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
        if browser_e2e_required:
            artifact_deficiency = _browser_surface_artifact_validation_deficiency(base_dir, persisted_evidence)
            if artifact_deficiency:
                summary = (
                    f"Verifier for issue #{issue['number']} reported pass but browser artifact validation failed "
                    f"({artifact_deficiency}). Retry pr_verifier with a resolvable evidence_ref that points to an existing browser artifact."
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
                        reason=f"Retry pr_verifier for issue #{issue['number']} after browser artifact validation failure.",
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
        if current_issue_state in {"completed", "failed", "quarantined"}:
            summary = (
                f"Verifier for issue #{issue['number']} passed, but the issue is already {current_issue_state}. "
                "Preserve the terminal DB state and leave release handling to the independent release command."
            )
            set_failure_func(ledger, kind="none", summary="", retryable=True)
            queue_transition_func(
                ledger,
                next_role="main_orchestrator",
                next_stage="issue_selection_or_recovery",
                summary=summary,
                updated_at=updated_at,
            )
            return ledger, {
                "action": "release_waiting",
                "next_role": "operator",
                "next_stage": "release_command",
                "summary": summary,
                "request_title": "/autodev-release",
            }, None
        pr_number = _extract_pr_number_from_fact(persisted_evidence)
        if not pr_number or pr_number == "none":
            summary = (
                f"Verifier for issue #{issue['number']} passed without a PR number in evidence_packet. "
                "Retry pr_verifier so verifier-owned evidence can confirm the PR binding before release."
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
                    reason=f"Retry pr_verifier for issue #{issue['number']} after missing PR number in evidence_packet.",
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
        worker_pr_number = _extract_pr_number_from_fact(persisted_worker)
        if worker_pr_number and worker_pr_number != pr_number:
            summary = (
                f"Verifier for issue #{issue['number']} passed with PR #{pr_number} in evidence_packet, "
                f"but worker_result recorded PR #{worker_pr_number}. "
                "Retry pr_verifier so verifier-owned evidence can reconcile the PR binding before release."
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
                    reason=(
                        f"Retry pr_verifier for issue #{issue['number']} after PR number mismatch "
                        "between evidence_packet and worker_result."
                    ),
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
        record_pr_opened(
            base_dir=base_dir,
            issue_number=issue["number"],
            pr_number=pr_number,
            created_at=_artifact_recorded_at(persisted_evidence, fallback=updated_at),
            verifier_session_id=verifier_session_id,
            command_id=str(persisted_evidence.get("command_id") or ""),
            payload={
                "issue_number": issue["number"],
                "pr_number": pr_number,
                "head_branch": issue.get("branch", ""),
                "base_branch": issue.get("baseBranch", "main"),
                "verifier_session_id": verifier_session_id,
                "source_artifact": "evidence_packet",
            },
        )
        transition_issue_state_if_possible(
            base_dir=base_dir,
            issue_number=issue["number"],
            to_state="verified",
            command_id=uuid4().hex,
            updated_at=updated_at,
            reason=f"Verifier accepted issue #{issue['number']} and recorded PR #{pr_number}.",
            from_state="verifying",
            current_session_id=verifier_session_id or None,
        )
        summary = (
            f"Verifier for issue #{issue['number']} passed and recorded PR #{pr_number}. "
            "The development loop is complete; run the independent release command to claim PR merge/release work."
        )
        set_failure_func(ledger, kind="none", summary="", retryable=True)
        record_current_verifier_session(
            base_dir=base_dir,
            issue_number=issue["number"],
            verifier_session_id=verifier_session_id,
            updated_at=updated_at,
        )
        queue_transition_func(
            ledger,
            next_role="main_orchestrator",
            next_stage="issue_selection_or_recovery",
            summary=summary,
            updated_at=updated_at,
        )
        return ledger, {
            "action": "release_waiting",
            "next_role": "operator",
            "next_stage": "release_command",
            "summary": summary,
            "request_title": "/autodev-release",
        }, None

    failure_kind = cast(str, persisted_evidence.get("failure_kind") or "verifier_retry")
    retryable = cast(bool | None, persisted_evidence.get("retryable"))
    summary = cast(str, persisted_evidence.get("next_recommended_step") or "")
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
    current: dict[str, str],
    attempts: dict[str, int],
    limits: dict[str, int],
    artifacts: dict[str, str],
    updated_at: str,
    transient_release_blockers: set[str],
    non_terminal_release_failure_kinds: set[str],
    read_issue: Callable[[Path, str], JsonObject | None],
    read_artifact_fact: Callable[[str], dict[str, object]],
    read_session_outcome: Callable[[str], object | None],
    transition_issue_state_if_possible: Callable[..., None],
    set_failure_func: Callable[..., None],
    queue_orchestrator_recovery_func: Callable[..., tuple[JsonObject, JsonObject, JsonObject]],
    queue_transition_func: Callable[..., None],
    subagent_decision_func: Callable[..., JsonObject],
) -> ReconcileResult:
    issue_state = read_issue(base_dir, issue["number"])
    verifier_session_id = str(issue_state.get("current_session_id") or "") if issue_state else ""
    persisted_release = _stored_fact(artifacts, "release_result", read_artifact_fact)
    if not bool(persisted_release.get("parse_ok")):
        current_status = str(current.get("status") or "")
        session_terminal_without_artifact = False
        if verifier_session_id and current_status in {"running", "queued"}:
            outcome = read_session_outcome(verifier_session_id)
            outcome_status = ""
            if isinstance(outcome, dict):
                outcome_status = str(outcome.get("status") or "")
            else:
                outcome_status = str(getattr(outcome, "status", "") or "")
            if outcome_status and outcome_status not in {"running", "queued", "unknown"}:
                session_terminal_without_artifact = True
        if not session_terminal_without_artifact and (
            current_status == "running" or (current_status == "queued" and attempts["release_worker"] < limits["release_worker"])
        ):
            summary = (
                f"release_worker for issue #{issue['number']} is {current.get('status') or 'queued'} and SQLite has not recorded "
                "release_result yet. Keep the queued/running dispatch state unchanged."
            )
            return (
                ledger,
                {
                    "action": "no_change",
                    "next_role": current.get("role") or "main_orchestrator",
                    "next_stage": current.get("stage") or "release_root_execution",
                    "summary": summary,
                    "request_title": "",
                },
                None,
            )
        summary = (
            f"release_worker for issue #{issue['number']} ended without recording release_result in SQLite. "
            "Re-dispatch release_worker so the subagent can submit release_result before completion."
        )
        set_failure_func(ledger, kind="contract_invalid", summary=summary, retryable=True)
        if attempts["release_worker"] < limits["release_worker"]:
            attempts["release_worker"] += 1
            transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=issue["number"],
                to_state="release_pending",
                command_id=uuid4().hex,
                updated_at=updated_at,
                reason=f"Retry release_worker for issue #{issue['number']} after missing release result.",
                current_session_id=verifier_session_id or None,
            )
            queue_transition_func(
                ledger,
                next_role="main_orchestrator",
                next_stage="release_root_execution",
                summary=summary,
                updated_at=updated_at,
            )
            return ledger, subagent_decision_func(
                ledger,
                next_role="main_orchestrator",
                next_stage="release_root_execution",
                summary=summary,
            ), None
        return queue_orchestrator_recovery_func(
            ledger,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=summary,
            final_state="failed",
        )
    status = cast(str, persisted_release.get("status") or "")
    if status == "success":
        if read_issue(base_dir, issue["number"]):
            transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=issue["number"],
                to_state="completed",
                command_id=uuid4().hex,
                updated_at=updated_at,
                reason=f"Release worker completed issue #{issue['number']} after verifier-owned evidence passed.",
                from_state="release_pending",
                current_session_id=verifier_session_id or None,
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

    blocked_reason = cast(str, persisted_release.get("blocked_reason") or "none")
    blocked_reason = blocked_reason.strip().lower()
    if blocked_reason in {"human_approval_required", "approval_override_mode is none"}:
        blocked_reason = "release_human_approval_missing"
    retryable = cast(bool | None, persisted_release.get("retryable"))
    failure_kind = cast(str, persisted_release.get("failure_kind") or "").strip().lower()
    if not failure_kind:
        if blocked_reason == "release_human_approval_missing":
            failure_kind = "human_approval_pending"
        else:
            failure_kind = blocked_reason or "release_blocked"
    summary = cast(str, persisted_release.get("next_recommended_step") or "")
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
            to_state="release_pending",
            command_id=uuid4().hex,
            updated_at=updated_at,
            reason=f"Retry release_worker for issue #{issue['number']} after transient release blocker {blocked_reason}.",
            current_session_id=verifier_session_id or None,
        )
        queue_transition_func(
            ledger,
            next_role="main_orchestrator",
            next_stage="release_root_execution",
            summary=retry_summary,
            updated_at=updated_at,
        )
        return ledger, subagent_decision_func(
            ledger,
            next_role="main_orchestrator",
            next_stage="release_root_execution",
            summary=retry_summary,
        ), None
    if failure_kind in non_terminal_release_failure_kinds:
        recovery_summary = (
            f"Release worker for issue #{issue['number']} is blocked by {blocked_reason or failure_kind}. "
            "Keep the issue re-releasable in verified so independent release can be retried after approval/policy changes."
        )
        return queue_orchestrator_recovery_func(
            ledger,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=recovery_summary,
            final_state="verified",
        )
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
    read_issue: Callable[[Path, str], JsonObject | None],
    read_artifact_fact: Callable[[str], dict[str, object]],
    is_successful_release_status: Callable[[str], bool],
    set_failure_func: Callable[..., None],
    queue_orchestrator_recovery_func: Callable[..., tuple[JsonObject, JsonObject, JsonObject]],
) -> ReconcileResult | None:
    issue_state = read_issue(base_dir, issue["number"])
    persisted_release = _stored_fact(artifacts, "release_result", read_artifact_fact)
    persisted_evidence = _stored_fact(artifacts, "evidence_packet", read_artifact_fact)
    persisted_worker = _stored_fact(artifacts, "worker_result", read_artifact_fact)
    issue_runtime_state = str(issue_state.get("state") or "") if issue_state else ""
    if issue_runtime_state in {"failed", "ready"} and bool(persisted_release.get("parse_ok")) and is_successful_release_status(cast(str, persisted_release.get("status") or "")):
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
    if issue_runtime_state in {"failed", "ready"} and bool(persisted_evidence.get("parse_ok")) and str(persisted_evidence.get("status") or "") == "pass":
        browser_e2e_required = _worker_result_web_surface_changed(persisted_worker)
        if (not browser_e2e_required) or _evidence_has_browser_e2e_gate(persisted_evidence):
            summary = (
                f"Late successful verifier evidence for issue #{issue['number']} arrived after recovery. "
                "Reconcile the control plane into verified so independent release can continue."
            )
            set_failure_func(ledger, kind="none", summary="", retryable=True)
            return queue_orchestrator_recovery_func(
                ledger,
                base_dir=base_dir,
                updated_at=updated_at,
                summary=summary,
                final_state="verified",
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


def no_change_decision(
    ledger: JsonObject,
    *,
    current: dict[str, str],
    runtime_issue_state: str = "",
    updated_at: str,
    bump_ledger_revision: Callable[[JsonObject, str], None],
) -> ReconcileResult:
    issue = cast(dict[str, str], ledger.get("issue", {}))
    issue_number = str(issue.get("number") or "")
    if not str(current.get("role") or "") and not str(current.get("stage") or ""):
        if runtime_issue_state in {"dispatching", "running", "verifying", "release_pending"}:
            summary = (
                f"Issue #{issue_number or 'unknown'} is already active in the DB-backed control plane "
                f"(state={runtime_issue_state}) but current role/stage is missing. Keep the current state "
                "unchanged and recover from the persisted SQLite session facts."
            )
            del updated_at, bump_ledger_revision
            return (
                ledger,
                {
                    "action": "no_change",
                    "next_role": "",
                    "next_stage": "",
                    "summary": summary,
                    "request_title": "",
                },
                None,
            )
        summary = (
            f"Issue #{issue_number or 'unknown'} has no queued role/stage in the DB-backed control plane. "
            "Run start-issue first to seed orchestrator_bootstrap, then reconcile again."
        )
        del updated_at, bump_ledger_revision
        return (
            ledger,
            {
                "action": "start_issue_required",
                "next_role": "main_orchestrator",
                "next_stage": "orchestrator_bootstrap",
                "summary": summary,
                "request_title": "start-issue",
            },
            None,
        )
    summary = f"Supervisor found role={current['role']} stage={current['stage']} with no automatic transition. Keep the current state unchanged."
    del updated_at, bump_ledger_revision
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
