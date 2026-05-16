"""Issue packet selection and intake helpers for the autodev supervisor."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Callable, Protocol

from scripts.control_plane_db import available_development_slots, completed_issue_numbers, ingest_issue_packet, issue_rows_with_packets, issues_in_states, read_issue_packet, ready_issues_for_selection, upsert_issue_ranking


JsonObject = dict[str, object]
NowFunc = Callable[[str | None], str]


class IssuePacketRecord(Protocol):
    issue_number: str
    title: str
    branch: str
    backing_type: str
    prior_handoff: str
    labels: list[str]
    parent_reference: str
    dependencies: list[str]
    raw_text: str


DEFAULT_ISSUE_INTAKE_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts/issue_packet_intake.py"
READY_FOR_AGENT_LABEL = "ready-for-agent"
AGENT_DISPATCHING_LABEL = "agent-dispatching"
AGENT_IN_PROGRESS_LABEL = "agent-in-progress"
QUARANTINED_LABEL = "quarantined"


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


def select_next_issue_packet(
    base_dir: Path,
    *,
    workflow: dict[str, str],
    current_issue: dict[str, str],
    completed_issue_numbers_func: Callable[[Path], set[str]],
    parse_issue_packet_text: Callable[[str, str], IssuePacketRecord],
    sync_issue_packet_to_db_func: Callable[..., None],
    issue_packet_record_from_json: Callable[[dict[str, object]], IssuePacketRecord | None],
    dependency_issue_numbers: Callable[[str, list[str]], list[str]],
    now: NowFunc,
    development_capacity: int | None = None,
) -> IssuePacketRecord | None:
    selected_packets = select_issue_packets_for_capacity(
        base_dir,
        workflow=workflow,
        current_issue=current_issue,
        completed_issue_numbers_func=completed_issue_numbers_func,
        parse_issue_packet_text=parse_issue_packet_text,
        sync_issue_packet_to_db_func=sync_issue_packet_to_db_func,
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=dependency_issue_numbers,
        now=now,
        development_capacity=development_capacity,
    )
    return selected_packets[0] if selected_packets else None


def select_issue_packets_for_capacity(
    base_dir: Path,
    *,
    workflow: dict[str, str],
    current_issue: dict[str, str],
    completed_issue_numbers_func: Callable[[Path], set[str]],
    parse_issue_packet_text: Callable[[str, str], IssuePacketRecord],
    sync_issue_packet_to_db_func: Callable[..., None],
    issue_packet_record_from_json: Callable[[dict[str, object]], IssuePacketRecord | None],
    dependency_issue_numbers: Callable[[str, list[str]], list[str]],
    now: NowFunc,
    development_capacity: int | None = None,
) -> list[IssuePacketRecord]:
    del parse_issue_packet_text
    del sync_issue_packet_to_db_func
    del workflow
    completed = completed_issue_numbers_func(base_dir)
    runtime_states = {
        issue["issue_number"]: issue["state"]
        for issue in issues_in_states(base_dir, ["claimed", "dispatching", "running", "verifying", "quarantined"])
    }
    current_number = current_issue.get("number", "")
    current_parent = current_issue.get("parentReference", "")
    packet_by_issue_number: dict[str, IssuePacketRecord] = {}
    selected_packets: list[IssuePacketRecord] = []
    for row in issue_rows_with_packets(base_dir):
        packet = issue_packet_record_from_json(read_issue_packet(base_dir, str(row.get("issue_number") or "")))
        if packet is None:
            continue
        packet_by_issue_number[packet.issue_number] = packet
        rank_score = -1.0
        if packet.issue_number == current_number:
            _ = upsert_issue_ranking(
                base_dir,
                issue_number=packet.issue_number,
                rank_score=rank_score,
                lane="default",
                updated_at=now(None),
            )
            continue
        if (
            READY_FOR_AGENT_LABEL in packet.labels
            and runtime_states.get(packet.issue_number) in {None, "ready", "failed", "completed"}
            and AGENT_DISPATCHING_LABEL not in packet.labels
            and AGENT_IN_PROGRESS_LABEL not in packet.labels
            and QUARANTINED_LABEL not in packet.labels
            and (not current_parent or packet.parent_reference == current_parent)
        ):
            unmet_dependencies = [
                number for number in dependency_issue_numbers(packet.issue_number, packet.dependencies) if number not in completed
            ]
            if not unmet_dependencies:
                try:
                    numeric_issue = int(packet.issue_number)
                except ValueError:
                    numeric_issue = 10**9
                rank_score = float(10**6 - numeric_issue)
        _ = upsert_issue_ranking(
            base_dir,
            issue_number=packet.issue_number,
            rank_score=rank_score,
            lane="default",
            updated_at=now(None),
        )

    ready_limit = available_development_slots(base_dir, development_capacity) if development_capacity is not None else None
    for row in ready_issues_for_selection(base_dir, limit=ready_limit):
        issue_number = str(row.get("issue_number") or "")
        packet = packet_by_issue_number.get(issue_number)
        if packet is not None:
            selected_packets.append(packet)
    return selected_packets


def run_issue_packet_intake(
    base_dir: Path,
    *,
    read_project_github_repo: Callable[[Path], str],
    parse_issue_packet_text: Callable[[str, str], IssuePacketRecord],
    sync_issue_packet_to_db_func: Callable[..., None],
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> bool:
    script_path = DEFAULT_ISSUE_INTAKE_SCRIPT_PATH
    repo = read_project_github_repo(base_dir)
    del parse_issue_packet_text
    del sync_issue_packet_to_db_func
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
