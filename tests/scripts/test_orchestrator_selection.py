from __future__ import annotations

from pathlib import Path

from scripts.control_plane_db import ingest_issue_packet, record_pr_opened, upsert_issue_state
from scripts.issue_dependency import dependency_issue_numbers
from scripts.orchestrator_artifacts import issue_packet_record_from_json
from scripts.orchestrator_selection import select_issue_candidates_for_capacity, select_issue_packets_for_capacity


def _packet(issue_number: str, *, branch: str, dependencies: list[str]) -> dict[str, object]:
    return {
        "issue_number": issue_number,
        "title": f"Issue {issue_number}",
        "branch": branch,
        "base_branch": "main",
        "backing_type": "github",
        "prior_handoff": "",
        "labels": ["ready-for-agent"],
        "parent_reference": "none",
        "dependencies": dependencies,
        "raw_text": "kind: issue_packet\n",
    }


def test_select_blocks_child_issue_until_parent_completed(tmp_path: Path) -> None:
    ingest_issue_packet(
        tmp_path,
        issue_number="11",
        issue_packet=_packet("11", branch="agent/issue-11-parent", dependencies=["none"]),
        updated_at="2026-05-16T10:00:00+08:00",
    )
    ingest_issue_packet(
        tmp_path,
        issue_number="12",
        issue_packet=_packet("12", branch="agent/issue-12-child", dependencies=["depends on issue #11"]),
        updated_at="2026-05-16T10:01:00+08:00",
    )
    upsert_issue_state(
        tmp_path,
        issue_number="11",
        state="verified",
        command_id="verify-11",
        updated_at="2026-05-16T10:02:00+08:00",
    )
    record_pr_opened(
        tmp_path,
        issue_number="11",
        pr_number="13",
        created_at="2026-05-16T10:03:00+08:00",
        payload={"head_branch": "agent/issue-11-parent", "base_branch": "main"},
    )

    selected = select_issue_packets_for_capacity(
        tmp_path,
        current_issue_number="",
        current_parent_reference="",
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=dependency_issue_numbers,
        development_capacity=1,
    )

    assert "12" not in [packet.issue_number for packet in selected]

    upsert_issue_state(
        tmp_path,
        issue_number="11",
        state="completed",
        command_id="complete-11",
        updated_at="2026-05-16T10:05:00+08:00",
    )

    selected_after_complete = select_issue_packets_for_capacity(
        tmp_path,
        current_issue_number="",
        current_parent_reference="",
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=dependency_issue_numbers,
        development_capacity=1,
    )

    assert [packet.issue_number for packet in selected_after_complete] == ["12"]


def test_select_issue_candidates_for_capacity_returns_compact_candidate_shape(tmp_path: Path) -> None:
    ingest_issue_packet(
        tmp_path,
        issue_number="12",
        issue_packet=_packet("12", branch="agent/issue-12-child", dependencies=["none"]),
        updated_at="2026-05-20T10:01:00+08:00",
    )

    selected = select_issue_candidates_for_capacity(
        tmp_path,
        current_issue_number="",
        current_parent_reference="",
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=dependency_issue_numbers,
        development_capacity=1,
    )

    assert len(selected) == 1
    candidate = selected[0]
    assert candidate.issue_number == "12"
    assert candidate.branch == "agent/issue-12-child"
    assert not hasattr(candidate, "title")


def test_dependency_issue_numbers_parses_depends_on_hash_number() -> None:
    dependencies = ["Depends on #11"]
    numbers = dependency_issue_numbers("12", dependencies)
    assert numbers == ["11"]


def test_dependency_issue_numbers_parses_blocked_by_hash_number() -> None:
    dependencies = ["blocked by #11"]
    numbers = dependency_issue_numbers("12", dependencies)
    assert numbers == ["11"]


def test_select_blocks_child_issue_when_parent_pr_is_not_stackable(tmp_path: Path) -> None:
    ingest_issue_packet(
        tmp_path,
        issue_number="11",
        issue_packet=_packet("11", branch="agent/issue-11-parent", dependencies=["none"]),
        updated_at="2026-05-16T10:00:00+08:00",
    )
    ingest_issue_packet(
        tmp_path,
        issue_number="12",
        issue_packet=_packet("12", branch="agent/issue-12-child", dependencies=["depends on issue #11"]),
        updated_at="2026-05-16T10:01:00+08:00",
    )

    selected = select_issue_packets_for_capacity(
        tmp_path,
        current_issue_number="",
        current_parent_reference="",
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=dependency_issue_numbers,
        development_capacity=1,
    )

    assert "12" not in [packet.issue_number for packet in selected]


def test_dependency_issue_numbers_parses_publisher_blocked_by_issue_number_format() -> None:
    dependencies = ["- Blocked by issue #9"]
    numbers = dependency_issue_numbers("10", dependencies)
    assert numbers == ["9"]


def test_dependency_issue_numbers_parses_publisher_blocked_by_hash_only_format() -> None:
    dependencies = ["- Blocked by #9"]
    numbers = dependency_issue_numbers("10", dependencies)
    assert numbers == ["9"]


def test_dependency_issue_numbers_excludes_self_reference() -> None:
    dependencies = ["- Blocked by issue #10"]
    numbers = dependency_issue_numbers("10", dependencies)
    assert numbers == []


def test_select_blocks_child_issue_with_publisher_blocked_by_issue_format(tmp_path: Path) -> None:
    ingest_issue_packet(
        tmp_path,
        issue_number="9",
        issue_packet=_packet("9", branch="agent/issue-9-parent", dependencies=["none"]),
        updated_at="2026-05-19T10:00:00+08:00",
    )
    ingest_issue_packet(
        tmp_path,
        issue_number="10",
        issue_packet=_packet(
            "10",
            branch="agent/issue-10-child",
            dependencies=["## Blocked by", "- Blocked by issue #9"],
        ),
        updated_at="2026-05-19T10:01:00+08:00",
    )

    selected_before = select_issue_packets_for_capacity(
        tmp_path,
        current_issue_number="",
        current_parent_reference="",
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=dependency_issue_numbers,
        development_capacity=1,
    )

    assert "10" not in [packet.issue_number for packet in selected_before]

    upsert_issue_state(
        tmp_path,
        issue_number="9",
        state="completed",
        command_id="complete-9",
        updated_at="2026-05-19T10:02:30+08:00",
    )

    selected_after = select_issue_packets_for_capacity(
        tmp_path,
        current_issue_number="",
        current_parent_reference="",
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=dependency_issue_numbers,
        development_capacity=1,
    )

    assert "10" in [packet.issue_number for packet in selected_after]
