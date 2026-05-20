"""Policy helpers for supervisor role/stage routing."""

from __future__ import annotations

from typing import Callable, Mapping


RouteResult = tuple[object, object, object | None]
RouteHandler = Callable[[], RouteResult]


def runtime_target_state(current: dict[str, str]) -> str:
    """Return the control-plane state implied by current runtime role/stage."""
    role = current.get("role", "")
    stage = current.get("stage", "")
    if role == "main_orchestrator" and stage == "orchestrator_bootstrap":
        return "running"
    if role == "issue_worker":
        return "running"
    if role == "pr_verifier":
        return "verifying"
    if role == "main_orchestrator" and stage == "release_root_execution":
        return "release_pending"
    return ""


def reconcile_route(current: dict[str, str]) -> str:
    """Classify reconcile route from role/stage."""
    role = current.get("role", "")
    stage = current.get("stage", "")
    if role == "main_orchestrator" and stage == "orchestrator_bootstrap":
        return "orchestrator_bootstrap"
    if role == "issue_worker":
        return "issue_worker"
    if role == "pr_verifier":
        return "pr_verifier"
    if role == "main_orchestrator" and stage == "release_root_execution":
        return "release_root_execution"
    if role == "main_orchestrator" and stage == "issue_selection_or_recovery":
        return "issue_selection_or_recovery"
    return "no_change"


def dispatch_reconcile_route(
    *,
    current: dict[str, str],
    handlers: Mapping[str, RouteHandler],
) -> RouteResult:
    """Dispatch reconcile by route key using provided handlers."""
    route = reconcile_route(current)
    handler = handlers.get(route)
    if handler is None:
        fallback = handlers.get("no_change")
        if fallback is None:
            raise ValueError("missing no_change handler")
        return fallback()
    return handler()


def select_release_issue_number(
    *,
    requested_issue_number: str | None,
    verified_issue_numbers: list[str],
    idle_release_pending_issue_numbers: list[str],
) -> str:
    """Select release issue number from requested/verified/idle pending candidates."""
    if requested_issue_number:
        return requested_issue_number
    if verified_issue_numbers:
        return verified_issue_numbers[0]
    if idle_release_pending_issue_numbers:
        return idle_release_pending_issue_numbers[0]
    raise RuntimeError(
        "no verified issue is waiting for independent release; provide --issue-number after approval is ready"
    )


def release_admission_decision(
    *,
    state: str,
    current_session_id: str,
    current_status: str,
) -> str:
    """Classify release admission state for start_release."""
    if state == "verified":
        return "transition_to_release_pending"
    if state == "release_pending":
        if current_session_id or current_status:
            return "reject_active_fence"
        return "allow_idle_release_pending"
    return "reject_invalid_state"


def is_selected_issue_recovery_request(*, role: str, stage: str, selected_issue_number: str) -> bool:
    return role == "main_orchestrator" and stage == "issue_selection_or_recovery" and bool(selected_issue_number)


def validate_selected_issue_alignment(
    *,
    queued_issue_number: str,
    queued_issue_branch: str,
    selected_issue_number: str,
    selected_issue_branch: str,
) -> str:
    if not (selected_issue_number or selected_issue_branch):
        return ""
    if not queued_issue_number:
        return "stale selected issue request no longer matches queued next issue state"
    if selected_issue_number != queued_issue_number:
        return f"stale selected issue #{selected_issue_number} does not match queued next issue #{queued_issue_number}"
    if selected_issue_branch and selected_issue_branch != queued_issue_branch:
        return (
            f"stale selected issue branch {selected_issue_branch} "
            f"does not match queued next issue branch {queued_issue_branch}"
        )
    return ""


def is_bootstrap_dispatch(*, role: str, stage: str) -> bool:
    return role == "main_orchestrator" and stage == "orchestrator_bootstrap"


def is_release_root_execution(*, role: str, stage: str) -> bool:
    return role == "main_orchestrator" and stage == "release_root_execution"


def validate_request_issue_branch(*, request_issue_number: str, request_branch: str, ledger_issue_number: str, ledger_branch: str) -> str:
    if request_issue_number != ledger_issue_number:
        return (
            f"stale request issue #{request_issue_number} "
            f"does not match ledger issue #{ledger_issue_number}"
        )
    if request_branch != ledger_branch:
        return (
            f"stale request branch {request_branch} "
            f"does not match ledger branch {ledger_branch}"
        )
    return ""


def validate_request_revision(*, request_revision: str, ledger_revision: str) -> str:
    if request_revision and ledger_revision and request_revision != ledger_revision:
        return (
            f"stale request revision {request_revision} "
            f"does not match ledger revision {ledger_revision}"
        )
    return ""


def validate_completed_issue_dispatch(*, issue_number: str, completed_issue_numbers: set[str], is_recovery_request: bool) -> str:
    if issue_number in completed_issue_numbers and not is_recovery_request:
        return f"issue #{issue_number} is already completed or released; refusing to dispatch stale request"
    return ""


def dispatch_restore_strategy(*, failure_restore_state: str, current_state: str) -> str:
    if current_state in {"completed", "failed"}:
        return "skip"
    if failure_restore_state == "quarantined":
        return "quarantined"
    if failure_restore_state == "release_pending":
        return "release_pending"
    return "ready"


def validate_dispatch_admission(
    *,
    request_issue_number: str,
    request_branch: str,
    ledger_issue_number: str,
    ledger_branch: str,
    request_revision: str,
    ledger_revision: str,
    queued_issue_number: str,
    queued_issue_branch: str,
    selected_issue_number: str,
    selected_issue_branch: str,
    role: str,
    stage: str,
    issue_is_completed: bool,
    packet_exists: bool,
    packet_is_ready_for_agent: bool,
    packet_issue_number: str,
) -> str:
    issue_branch_error = validate_request_issue_branch(
        request_issue_number=request_issue_number,
        request_branch=request_branch,
        ledger_issue_number=ledger_issue_number,
        ledger_branch=ledger_branch,
    )
    if issue_branch_error:
        return issue_branch_error

    revision_error = validate_request_revision(
        request_revision=request_revision,
        ledger_revision=ledger_revision,
    )
    if revision_error:
        return revision_error

    selected_alignment_error = validate_selected_issue_alignment(
        queued_issue_number=queued_issue_number,
        queued_issue_branch=queued_issue_branch,
        selected_issue_number=selected_issue_number,
        selected_issue_branch=selected_issue_branch,
    )
    if selected_alignment_error:
        return selected_alignment_error

    is_recovery_request = is_selected_issue_recovery_request(
        role=role,
        stage=stage,
        selected_issue_number=selected_issue_number,
    )

    completed_issue_error = validate_completed_issue_dispatch(
        issue_number=request_issue_number,
        completed_issue_numbers={request_issue_number} if issue_is_completed else set(),
        is_recovery_request=is_recovery_request,
    )
    if completed_issue_error:
        return completed_issue_error

    if is_recovery_request:
        return ""

    if not packet_exists:
        return f"issue packet not recorded in SQLite for issue #{request_issue_number}"

    if not packet_is_ready_for_agent:
        return f"issue #{request_issue_number} is not ready-for-agent; refusing to dispatch"

    if packet_issue_number != request_issue_number:
        return (
            f"stored issue packet belongs to issue #{packet_issue_number}, "
            f"not request issue #{request_issue_number}"
        )

    return ""
