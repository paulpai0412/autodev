"""Issue packet selection and intake helpers for the autodev supervisor."""

from __future__ import annotations

import subprocess
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from scripts.control_plane_db import available_development_slots, completed_issue_numbers, ingest_issue_packet, issue_rows_with_packets, issues_in_states, read_issue, read_issue_packet, read_latest_history_entry, ready_issues_for_selection, upsert_issue_ranking
from scripts.issue_selection_projection import (
    dependency_issue_numbers_for_selection as projection_dependency_issue_numbers_for_selection,
    readiness_rank_score,
    resolve_issue_base_branch_from_unblocked_dependencies,
)


JsonObject = dict[str, object]
NowFunc = Callable[[str | None], str]


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
    raw_text: str


@dataclass(frozen=True)
class IssueSelectionCandidate:
    issue_number: str
    branch: str


DEFAULT_ISSUE_INTAKE_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts/issue_packet_intake.py"
def _stackable_dependency_branch(base_dir: Path, issue_number: str) -> str:
    issue = read_issue(base_dir, issue_number)
    if issue is None:
        return ""
    state = str(issue.get("state") or "")
    if state not in {"verified", "release_pending"}:
        return ""
    pr_opened = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="pr_opened")
    if pr_opened is None:
        return ""
    return str(issue.get("branch") or "")


def resolve_issue_base_branch(
    base_dir: Path,
    *,
    issue_number: str,
    dependencies: list[str],
    default_base_branch: str,
    dependency_issue_numbers: Callable[[str, list[str]], list[str]],
) -> str:
    unblocked = completed_issue_numbers(base_dir)
    release_pending_issue_numbers = {
        str(issue.get("issue_number") or "")
        for issue in issues_in_states(base_dir, ["release_pending"])
        if str(issue.get("issue_number") or "")
    }
    unblocked.update(release_pending_issue_numbers)
    return resolve_issue_base_branch_from_unblocked_dependencies(
        issue_number=issue_number,
        dependencies=dependencies,
        default_base_branch=default_base_branch,
        unblocked_issue_numbers=unblocked,
    )


def sync_issue_packet_to_db(
    base_dir: Path,
    packet: IssuePacketRecord,
    *,
    issue_packet_record_to_json: Callable[[IssuePacketRecord], JsonObject],
    now: NowFunc,
    updated_at: str | None = None,
) -> None:
    _ = ingest_issue_packet(
        base_dir,
        issue_number=packet.issue_number,
        issue_packet=issue_packet_record_to_json(packet),
        updated_at=now(updated_at),
    )


def load_issue_packet_from_db(
    base_dir: Path,
    issue_number: str,
    *,
    issue_packet_record_from_json: Callable[[dict[str, object]], IssuePacketRecord | None],
) -> IssuePacketRecord | None:
    payload = read_issue_packet(base_dir, issue_number)
    return issue_packet_record_from_json(payload)


def completed_issue_numbers_from_control_plane(base_dir: Path) -> set[str]:
    return completed_issue_numbers(base_dir)


def _selection_timestamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def select_next_issue_candidate(
    base_dir: Path,
    *,
    current_issue_number: str,
    current_parent_reference: str,
    issue_packet_record_from_json: Callable[[dict[str, object]], IssuePacketRecord | None],
    dependency_issue_numbers: Callable[[str, list[str]], list[str]],
    development_capacity: int | None = None,
) -> IssueSelectionCandidate | None:
    selected_packets = select_issue_candidates_for_capacity(
        base_dir,
        current_issue_number=current_issue_number,
        current_parent_reference=current_parent_reference,
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=dependency_issue_numbers,
        development_capacity=development_capacity,
    )
    return selected_packets[0] if selected_packets else None


def select_issue_candidates_for_capacity(
    base_dir: Path,
    *,
    current_issue_number: str,
    current_parent_reference: str,
    issue_packet_record_from_json: Callable[[dict[str, object]], IssuePacketRecord | None],
    dependency_issue_numbers: Callable[[str, list[str]], list[str]],
    development_capacity: int | None = None,
) -> list[IssueSelectionCandidate]:
    completed = completed_issue_numbers(base_dir)
    runtime_states = {
        issue["issue_number"]: issue["state"]
        for issue in issues_in_states(base_dir, ["claimed", "dispatching", "running", "verifying", "quarantined"])
    }
    current_number = current_issue_number
    current_parent = current_parent_reference
    packet_by_issue_number: dict[str, IssuePacketRecord] = {}
    selected_packets: list[IssueSelectionCandidate] = []
    timestamp = _selection_timestamp()
    for row in issue_rows_with_packets(base_dir):
        packet = issue_packet_record_from_json(read_issue_packet(base_dir, str(row.get("issue_number") or "")))
        if packet is None:
            continue
        packet_by_issue_number[packet.issue_number] = packet
        if packet.issue_number == current_number:
            _ = upsert_issue_ranking(
                base_dir,
                issue_number=packet.issue_number,
                rank_score=-1.0,
                lane="default",
                updated_at=timestamp,
            )
            continue
        base_branch = resolve_issue_base_branch(
            base_dir,
            issue_number=packet.issue_number,
            dependencies=packet.dependencies,
            default_base_branch=packet.base_branch,
            dependency_issue_numbers=dependency_issue_numbers,
        )
        rank_score = readiness_rank_score(
            issue_number=packet.issue_number,
            labels=packet.labels,
            runtime_state=runtime_states.get(packet.issue_number),
            parent_reference=packet.parent_reference,
            current_parent_reference=current_parent,
            base_branch=base_branch,
            current_issue_number=current_number,
        )
        _ = upsert_issue_ranking(
            base_dir,
            issue_number=packet.issue_number,
            rank_score=rank_score,
            lane="default",
            updated_at=timestamp,
        )

    ready_limit = available_development_slots(base_dir, development_capacity) if development_capacity is not None else None
    for row in ready_issues_for_selection(base_dir, limit=ready_limit):
        issue_number = str(row.get("issue_number") or "")
        packet = packet_by_issue_number.get(issue_number)
        if packet is not None:
            selected_packets.append(IssueSelectionCandidate(issue_number=packet.issue_number, branch=packet.branch))
    return selected_packets


def select_next_issue_packet(
    base_dir: Path,
    *,
    current_issue_number: str,
    current_parent_reference: str,
    issue_packet_record_from_json: Callable[[dict[str, object]], IssuePacketRecord | None],
    dependency_issue_numbers: Callable[[str, list[str]], list[str]],
    development_capacity: int | None = None,
) -> IssuePacketRecord | None:
    candidate = select_next_issue_candidate(
        base_dir,
        current_issue_number=current_issue_number,
        current_parent_reference=current_parent_reference,
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=dependency_issue_numbers,
        development_capacity=development_capacity,
    )
    if candidate is None:
        return None
    return issue_packet_record_from_json(read_issue_packet(base_dir, candidate.issue_number))


def select_issue_packets_for_capacity(
    base_dir: Path,
    *,
    current_issue_number: str,
    current_parent_reference: str,
    issue_packet_record_from_json: Callable[[dict[str, object]], IssuePacketRecord | None],
    dependency_issue_numbers: Callable[[str, list[str]], list[str]],
    development_capacity: int | None = None,
) -> list[IssuePacketRecord]:
    candidates = select_issue_candidates_for_capacity(
        base_dir,
        current_issue_number=current_issue_number,
        current_parent_reference=current_parent_reference,
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=dependency_issue_numbers,
        development_capacity=development_capacity,
    )
    packets: list[IssuePacketRecord] = []
    for candidate in candidates:
        packet = issue_packet_record_from_json(read_issue_packet(base_dir, candidate.issue_number))
        if packet is not None:
            packets.append(packet)
    return packets


def dependency_issue_numbers_for_selection(issue_number: str, dependencies: list[str]) -> list[str]:
    return projection_dependency_issue_numbers_for_selection(issue_number, dependencies)


def run_issue_packet_intake(
    base_dir: Path,
    *,
    read_project_github_repo: Callable[[Path], str],
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> bool:
    script_path = DEFAULT_ISSUE_INTAKE_SCRIPT_PATH
    repo = read_project_github_repo(base_dir)
    command = [
        "python3",
        str(script_path),
        "--project-root",
        str(base_dir),
    ]
    if repo:
        command.extend(["--repo", repo])
    try:
        _ = run(
            command,
            cwd=base_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False
