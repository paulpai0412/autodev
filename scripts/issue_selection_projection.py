"""Deep selection projection helpers shared by intake/selection call paths."""

from __future__ import annotations

from scripts.issue_dependency import dependency_issue_numbers


READY_FOR_AGENT_LABEL = "ready-for-agent"
AGENT_DISPATCHING_LABEL = "agent-dispatching"
AGENT_IN_PROGRESS_LABEL = "agent-in-progress"
QUARANTINED_LABEL = "quarantined"


def dependency_issue_numbers_for_selection(issue_number: str, dependencies: list[str]) -> list[str]:
    return dependency_issue_numbers(issue_number, dependencies)


def resolve_issue_base_branch_from_completed(
    *,
    issue_number: str,
    dependencies: list[str],
    default_base_branch: str,
    completed_issue_numbers: set[str],
) -> str:
    dependency_numbers = dependency_issue_numbers_for_selection(issue_number, dependencies)
    unresolved = [number for number in dependency_numbers if number not in completed_issue_numbers]
    if unresolved:
        return ""
    return default_base_branch or "main"


def readiness_rank_score(
    *,
    issue_number: str,
    labels: list[str],
    runtime_state: str | None,
    parent_reference: str,
    current_parent_reference: str,
    base_branch: str,
    current_issue_number: str,
) -> float:
    if issue_number == current_issue_number:
        return -1.0

    if READY_FOR_AGENT_LABEL not in labels:
        return -1.0
    if runtime_state not in {None, "ready", "failed", "completed"}:
        return -1.0
    if AGENT_DISPATCHING_LABEL in labels or AGENT_IN_PROGRESS_LABEL in labels or QUARANTINED_LABEL in labels:
        return -1.0
    if current_parent_reference and parent_reference != current_parent_reference:
        return -1.0
    if not base_branch:
        return -1.0

    try:
        numeric_issue = int(issue_number)
    except ValueError:
        numeric_issue = 10**9
    return float(10**6 - numeric_issue)
