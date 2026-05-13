from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts.control_plane_db import (
    append_issue_event,
    append_issue_history,
    completed_issue_numbers,
    control_plane_db_path,
    describe_control_plane_schema,
    ensure_control_plane_db,
    ingest_issue_packet,
    issue_rows_with_packets,
    issues_in_states,
    read_decision,
    read_github_sync_attempt,
    read_github_sync_attempt_by_command_id,
    read_issue,
    read_issue_packet,
    read_latest_decision,
    read_latest_github_sync_attempt,
    read_latest_issue_history,
    ready_issues_for_selection,
    record_admin_decision,
    record_github_sync_attempt,
    sync_issue_runtime_context,
    transition_issue_state,
    upsert_issue_ranking,
)


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
    assert table_names(db_path) == {"issues", "issue_history", "sqlite_sequence"}


def test_describe_control_plane_schema_exposes_real_issue_columns(tmp_path: Path):
    schema = describe_control_plane_schema(tmp_path)

    issue_columns = [column["name"] for column in schema["tables"]["issues"]["columns"]]
    history_columns = [column["name"] for column in schema["tables"]["issue_history"]["columns"]]

    assert schema["dbPath"] == str(control_plane_db_path(tmp_path))
    assert "artifact_refs_json" in issue_columns
    assert "artifact_status_json" in issue_columns
    assert "issue_packet_json" in issue_columns
    assert "history_id" in history_columns
    assert "payload_json" in history_columns


def test_transition_issue_state_records_issue_and_history(tmp_path: Path):
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
    latest_history = read_latest_issue_history(tmp_path, "42", entry_type="state_transition")

    assert issue is not None
    assert issue["state"] == "claimed"
    assert issue["claimed_at"] == "2026-05-11T10:00:00+08:00"
    assert decision is not None
    assert decision["from_state"] == "ready"
    assert decision["to_state"] == "claimed"
    assert latest_history is not None
    assert latest_history["command_id"] == "cmd-1"


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
        count = connection.execute("SELECT COUNT(*) FROM issue_history WHERE entry_type = 'root_event'").fetchone()[0]
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


def test_read_latest_rows_return_newest_issue_decision_and_sync(tmp_path: Path):
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

    latest_decision = read_latest_decision(tmp_path, "42")
    latest_sync = read_latest_github_sync_attempt(tmp_path, "42")

    assert latest_decision is not None
    assert latest_decision["command_id"] == "cmd-dispatch"
    assert latest_sync is not None
    assert latest_sync["command_id"] == "cmd-gh-2"
    assert latest_sync["last_error"] == "boom"
    by_command = read_github_sync_attempt_by_command_id(tmp_path, "cmd-gh-2")
    assert by_command is not None
    assert by_command["command_id"] == "cmd-gh-2"


def test_upsert_issue_ranking_and_ready_selection_sort_by_rank_score(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    (issue_packets_dir / "issue-41.yaml").write_text("issue 41", encoding="utf-8")
    (issue_packets_dir / "issue-42.yaml").write_text("issue 42", encoding="utf-8")
    _ = ingest_issue_packet(
        tmp_path,
        issue_number="41",
        issue_packet={
            "issue_number": "41",
            "title": "Issue 41",
            "branch": "agent/issue-41-demo",
            "issue_packet_path": "docs/agents/issue-packets/issue-41.yaml",
            "labels": ["ready-for-agent"],
            "parent_reference": "none",
            "dependencies": [],
        },
        updated_at="2026-05-11T09:59:00+08:00",
    )
    _ = ingest_issue_packet(
        tmp_path,
        issue_number="42",
        issue_packet={
            "issue_number": "42",
            "title": "Issue 42",
            "branch": "agent/issue-42-demo",
            "issue_packet_path": "docs/agents/issue-packets/issue-42.yaml",
            "labels": ["ready-for-agent"],
            "parent_reference": "none",
            "dependencies": [],
        },
        updated_at="2026-05-11T09:59:00+08:00",
    )
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


def test_ready_issues_for_selection_requires_materialized_local_issue_packet(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    (issue_packets_dir / "issue-42.yaml").write_text("issue 42", encoding="utf-8")
    _ = ingest_issue_packet(
        tmp_path,
        issue_number="41",
        issue_packet={
            "issue_number": "41",
            "title": "Issue 41",
            "branch": "agent/issue-41-demo",
            "issue_packet_path": "docs/agents/issue-packets/issue-41.yaml",
            "labels": ["ready-for-agent"],
            "parent_reference": "none",
            "dependencies": [],
        },
        updated_at="2026-05-11T10:00:00+08:00",
    )
    _ = ingest_issue_packet(
        tmp_path,
        issue_number="42",
        issue_packet={
            "issue_number": "42",
            "title": "Issue 42",
            "branch": "agent/issue-42-demo",
            "issue_packet_path": "docs/agents/issue-packets/issue-42.yaml",
            "labels": ["ready-for-agent"],
            "parent_reference": "none",
            "dependencies": [],
        },
        updated_at="2026-05-11T10:00:00+08:00",
    )
    _ = upsert_issue_ranking(
        tmp_path,
        issue_number="41",
        rank_score=100,
        lane="default",
        updated_at="2026-05-11T10:01:00+08:00",
    )
    _ = upsert_issue_ranking(
        tmp_path,
        issue_number="42",
        rank_score=90,
        lane="default",
        updated_at="2026-05-11T10:01:00+08:00",
    )

    rows = ready_issues_for_selection(tmp_path)

    assert [row["issue_number"] for row in rows] == ["42"]


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


def test_ingest_issue_packet_and_runtime_context_store_on_issue_row(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    _ = ingest_issue_packet(
        tmp_path,
        issue_number="42",
        issue_packet={
            "issue_number": "42",
            "title": "Demo issue",
            "branch": "agent/issue-42-demo",
            "issue_packet_path": "docs/agents/issue-packets/issue-42.yaml",
            "labels": ["ready-for-agent"],
            "parent_reference": "https://github.com/example/issues/1",
            "dependencies": [],
        },
        updated_at="2026-05-11T10:00:00+08:00",
    )
    _ = sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-11T10:01:00+08:00",
        current_role="issue_worker",
        current_stage="issue_worker_execution",
        current_status="queued",
        attempts={"issue_worker": 1},
        limits={"issue_worker": 3},
        artifact_refs={"workerResultPath": "docs/agents/worker-results/issue-42.yaml"},
    )

    packet = read_issue_packet(tmp_path, "42")
    rows = issue_rows_with_packets(tmp_path)
    issue = read_issue(tmp_path, "42")

    assert packet["branch"] == "agent/issue-42-demo"
    assert [row["issue_number"] for row in rows] == ["42"]
    assert issue is not None
    assert issue["current_role"] == "issue_worker"
    assert issue["current_stage"] == "issue_worker_execution"


def test_completed_issue_numbers_reads_completed_state_from_db(tmp_path: Path):
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
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="running",
        command_id="cmd-running",
        scheduler_id="scheduler:test",
        reason="run issue",
        updated_at="2026-05-11T10:02:00+08:00",
        from_state="dispatching",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="verifying",
        command_id="cmd-verifying",
        scheduler_id="scheduler:test",
        reason="verify issue",
        updated_at="2026-05-11T10:03:00+08:00",
        from_state="running",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="completed",
        command_id="cmd-completed",
        scheduler_id="scheduler:test",
        reason="complete issue",
        updated_at="2026-05-11T10:04:00+08:00",
        from_state="verifying",
    )

    assert completed_issue_numbers(tmp_path) == {"42"}


def test_append_issue_history_supports_generic_history_entries(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    history_id = append_issue_history(
        tmp_path,
        issue_number="42",
        entry_type="execution_result",
        created_at="2026-05-11T10:00:00+08:00",
        role="issue_worker",
        stage="issue_worker_execution",
        status="success",
        session_id="ses-worker",
        summary="worker finished",
        payload={"pr_number": "77"},
        unique_key="worker-result:42:2026-05-11T10:00:00+08:00",
    )
    latest = read_latest_issue_history(tmp_path, "42", entry_type="execution_result")

    assert history_id > 0
    assert latest is not None
    assert latest["status"] == "success"
