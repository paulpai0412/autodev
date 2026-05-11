from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts.control_plane_db import (
    append_issue_event,
    control_plane_db_path,
    ensure_control_plane_db,
    issues_in_states,
    ready_issues_for_selection,
    read_active_scheduler_lease,
    read_decision,
    read_github_sync_attempt,
    read_github_sync_attempt_by_command_id,
    read_issue,
    read_latest_decision,
    read_latest_github_sync_attempt,
    record_admin_decision,
    record_github_sync_attempt,
    transition_issue_state,
    upsert_issue_ranking,
)
from scripts.orchestrator_lease import acquire_scheduler_lease


def table_names(db_path: Path) -> set[str]:
    connection = sqlite3.connect(db_path)
    try:
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    finally:
        connection.close()
    return {row[0] for row in rows}


def test_ensure_control_plane_db_creates_required_tables(tmp_path: Path):
    db_path = ensure_control_plane_db(tmp_path)

    assert db_path == control_plane_db_path(tmp_path)
    assert {
        "scheduler_leases",
        "issues",
        "issue_events",
        "decision_log",
        "github_sync_attempts",
    }.issubset(table_names(db_path))


def test_transition_issue_state_records_issue_and_decision_log(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-1",
        scheduler_id="scheduler:test",
        reason="claim issue",
        updated_at="2026-05-11T10:00:00+08:00",
        from_state="ready",
    )

    issue = read_issue(tmp_path, "42")
    decision = read_decision(tmp_path, "cmd-1")

    assert issue is not None
    assert issue["state"] == "claimed"
    assert issue["claimed_at"] == "2026-05-11T10:00:00+08:00"
    assert decision is not None
    assert decision["from_state"] == "ready"
    assert decision["to_state"] == "claimed"


def test_append_issue_event_is_deduplicated_by_event_id_and_session_seq(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-1",
        scheduler_id="scheduler:test",
        reason="claim issue",
        updated_at="2026-05-11T10:00:00+08:00",
        from_state="ready",
    )

    append_issue_event(
        tmp_path,
        event_id="evt-1",
        issue_number="42",
        root_session_id="ses-root",
        session_seq=1,
        event_type="root_terminal",
        payload={"status": "success"},
        created_at="2026-05-11T10:05:00+08:00",
    )
    append_issue_event(
        tmp_path,
        event_id="evt-1",
        issue_number="42",
        root_session_id="ses-root",
        session_seq=1,
        event_type="root_terminal",
        payload={"status": "success"},
        created_at="2026-05-11T10:05:00+08:00",
    )

    connection = sqlite3.connect(control_plane_db_path(tmp_path))
    try:
        count = connection.execute("SELECT COUNT(*) FROM issue_events").fetchone()[0]
    finally:
        connection.close()

    assert count == 1


def test_record_github_sync_attempt_tracks_status(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    record_github_sync_attempt(
        tmp_path,
        command_id="cmd-gh",
        issue_number="42",
        add_labels=["agent-dispatching"],
        remove_labels=["ready-for-agent"],
        status="failed",
        updated_at="2026-05-11T10:10:00+08:00",
        last_error="boom",
    )

    attempt = read_github_sync_attempt(tmp_path, "cmd-gh")

    assert attempt is not None
    assert attempt["status"] == "failed"
    assert attempt["last_error"] == "boom"


def test_issues_in_states_returns_only_matching_runtime_rows(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    transition_issue_state(
        tmp_path,
        issue_number="41",
        to_state="claimed",
        command_id="cmd-41",
        scheduler_id="scheduler:test",
        reason="claim issue 41",
        updated_at="2026-05-11T10:00:00+08:00",
        from_state="ready",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-42-claim",
        scheduler_id="scheduler:test",
        reason="claim issue 42",
        updated_at="2026-05-11T10:00:00+08:00",
        from_state="ready",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="dispatching",
        command_id="cmd-42-dispatch",
        scheduler_id="scheduler:test",
        reason="dispatch issue 42",
        updated_at="2026-05-11T10:01:00+08:00",
        from_state="claimed",
    )

    rows = issues_in_states(tmp_path, ["dispatching"])

    assert [row["issue_number"] for row in rows] == ["42"]


def test_read_latest_rows_return_newest_issue_decision_sync_and_lease(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-claim",
        scheduler_id="scheduler:test",
        reason="claim issue",
        updated_at="2026-05-11T10:00:00+08:00",
        from_state="ready",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="dispatching",
        command_id="cmd-dispatch",
        scheduler_id="scheduler:test",
        reason="dispatch issue",
        updated_at="2026-05-11T10:01:00+08:00",
        from_state="claimed",
    )
    record_github_sync_attempt(
        tmp_path,
        command_id="cmd-gh-1",
        issue_number="42",
        add_labels=["agent-dispatching"],
        remove_labels=["ready-for-agent"],
        status="success",
        updated_at="2026-05-11T10:01:00+08:00",
    )
    record_github_sync_attempt(
        tmp_path,
        command_id="cmd-gh-2",
        issue_number="42",
        add_labels=["agent-in-progress"],
        remove_labels=["agent-dispatching"],
        status="failed",
        updated_at="2026-05-11T10:02:00+08:00",
        last_error="boom",
    )
    lease = acquire_scheduler_lease(
        tmp_path,
        scheduler_id="scheduler-a",
        heartbeat_at="2026-05-11T10:03:00+08:00",
        ttl_seconds=60,
    )

    latest_decision = read_latest_decision(tmp_path, "42")
    latest_sync = read_latest_github_sync_attempt(tmp_path, "42")
    active_lease = read_active_scheduler_lease(tmp_path)

    assert lease is not None
    assert latest_decision is not None
    assert latest_decision["command_id"] == "cmd-dispatch"
    assert latest_sync is not None
    assert latest_sync["command_id"] == "cmd-gh-2"
    assert latest_sync["last_error"] == "boom"
    by_command = read_github_sync_attempt_by_command_id(tmp_path, "cmd-gh-2")
    assert by_command is not None
    assert by_command["command_id"] == "cmd-gh-2"
    assert active_lease is not None
    assert active_lease["scheduler_id"] == "scheduler-a"


def test_upsert_issue_ranking_and_ready_selection_sort_by_rank_score(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    _ = upsert_issue_ranking(
        tmp_path,
        issue_number="41",
        rank_score=10,
        lane="default",
        updated_at="2026-05-11T10:00:00+08:00",
    )
    _ = upsert_issue_ranking(
        tmp_path,
        issue_number="42",
        rank_score=20,
        lane="default",
        updated_at="2026-05-11T10:00:00+08:00",
    )

    rows = ready_issues_for_selection(tmp_path)

    assert [row["issue_number"] for row in rows] == ["42", "41"]


def test_record_admin_decision_persists_latest_decision(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    record_admin_decision(
        tmp_path,
        command_id="cmd-gh:retry",
        issue_number="42",
        decision_type="admin_github_sync_retry",
        reason="retry failed github sync",
        updated_at="2026-05-11T10:00:00+08:00",
    )

    decision = read_latest_decision(tmp_path, "42")

    assert decision is not None
    assert decision["command_id"] == "cmd-gh:retry"
    assert decision["decision_type"] == "admin_github_sync_retry"
