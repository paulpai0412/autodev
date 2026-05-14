"""Issue packet selection and intake helpers for the autodev supervisor."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Callable, Protocol, cast

from scripts.control_plane_db import completed_issue_numbers, ingest_issue_packet, issue_rows_with_packets, issues_in_states, read_issue_packet, ready_issues_for_selection, upsert_issue_ranking


JsonObject = dict[str, object]
NowFunc = Callable[[str | None], str]


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
    ingest_issue_packet(
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


def resolve_artifact_path(path_text: str, *, base_dir: Path, root: Path) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    del root
    return base_dir / path


def infer_artifact_base_dir(ledger_path: Path, *, root: Path) -> Path:
    resolved = ledger_path.resolve()
    if resolved.parent.name == "runtime" and resolved.parent.parent.name == ".opencode":
        return resolved.parent.parent.parent
    if resolved.parent != resolved:
        return resolved.parent
    return root


def _packet_exists_locally(base_dir: Path, packet: IssuePacketRecord) -> bool:
    path = Path(packet.issue_packet_path)
    resolved_path = path if path.is_absolute() else base_dir / path
    if resolved_path.exists():
        return True
    if not packet.raw_text:
        return False
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    _ = resolved_path.write_text(packet.raw_text, encoding="utf-8")
    return True


def completed_issue_numbers_from_control_plane(base_dir: Path, checkpoint_path: str) -> set[str]:
    del checkpoint_path
    return completed_issue_numbers(base_dir)


def checkpoint_completed_issue_numbers(text: str, *, parse_issue_numbers: Callable[[str], list[str]]) -> set[str]:
    completed: set[str] = set()
    in_completed = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 2 and stripped == "completed:":
            in_completed = True
            continue
        if in_completed and indent <= 2 and not stripped.startswith("- "):
            break
        if in_completed and stripped.startswith("- "):
            completed.update(parse_issue_numbers(stripped))
    return completed


def select_next_issue_packet(
    base_dir: Path,
    *,
    workflow: dict[str, str],
    current_issue: dict[str, str],
    completed_issue_numbers_func: Callable[[Path, str], set[str]],
    parse_issue_packet_text: Callable[[str, str], IssuePacketRecord],
    sync_issue_packet_to_db_func: Callable[..., None],
    issue_packet_record_from_json: Callable[[dict[str, object]], IssuePacketRecord | None],
    dependency_issue_numbers: Callable[[str, list[str]], list[str]],
    now: NowFunc,
) -> IssuePacketRecord | None:
    completed = completed_issue_numbers_func(base_dir, workflow["checkpointPath"])
    runtime_states = {
        issue["issue_number"]: issue["state"]
        for issue in issues_in_states(base_dir, ["claimed", "dispatching", "running", "verifying", "quarantined"])
    }
    current_number = current_issue.get("number", "")
    current_parent = current_issue.get("parentReference", "")
    packet_by_issue_number: dict[str, IssuePacketRecord] = {}
    packets_dir = base_dir / "docs/agents/issue-packets"
    if packets_dir.exists():
        for path in sorted(packets_dir.glob("issue-*.yaml")):
            packet = parse_issue_packet_text(path.read_text(encoding="utf-8"), str(path.relative_to(base_dir)))
            sync_issue_packet_to_db_func(base_dir, packet)
    for row in issue_rows_with_packets(base_dir):
        packet = issue_packet_record_from_json(read_issue_packet(base_dir, str(row.get("issue_number") or "")))
        if packet is None:
            continue
        if not _packet_exists_locally(base_dir, packet):
            _ = upsert_issue_ranking(
                base_dir,
                issue_number=packet.issue_number,
                rank_score=-1.0,
                lane="default",
                updated_at=now(None),
            )
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

    for row in ready_issues_for_selection(base_dir):
        issue_number = str(row.get("issue_number") or "")
        packet = packet_by_issue_number.get(issue_number)
        if packet is not None:
            return packet
    return None


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
    command = [
        "python3",
        str(script_path),
        "--output-dir",
        str(base_dir / "docs/agents/issue-packets"),
    ]
    if repo:
        command.extend(["--repo", repo])
    try:
        completed = run(
            command,
            cwd=base_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        for line in completed.stdout.splitlines():
            packet_ref = line.strip()
            if not packet_ref:
                continue
            packet_path = Path(packet_ref)
            resolved_path = packet_path if packet_path.is_absolute() else base_dir / packet_path
            if not resolved_path.exists():
                continue
            packet = parse_issue_packet_text(
                resolved_path.read_text(encoding="utf-8"),
                str(resolved_path.relative_to(base_dir)) if resolved_path.is_relative_to(base_dir) else str(resolved_path),
            )
            sync_issue_packet_to_db_func(base_dir, packet)
        return True
    except subprocess.CalledProcessError:
        return False
