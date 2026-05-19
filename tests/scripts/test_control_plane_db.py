from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts.control_plane_db import (
    available_development_slots,
    available_release_slots,
    append_issue_event,
    append_issue_history,
    canonical_control_plane_base_dir,
    completed_issue_numbers,
    control_plane_db_path,
    development_slot_occupancy,
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
    read_latest_ref,
    read_latest_decision,
    read_latest_github_sync_attempt,
    read_latest_issue_history,
    read_runtime_context,
    release_slot_occupancy,
    record_pr_opened,
    ready_issues_for_selection,
    record_admin_decision,
    record_github_sync_attempt,
    sync_issue_runtime_context,
    transition_issue_state,
    upsert_issue_state,
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
    assert "current_session_id" in issue_columns
    assert "worktree_path" in issue_columns
    assert "runtime_context_json" in issue_columns
    assert "latest_refs_json" in issue_columns
    assert "issue_packet_json" in issue_columns
    assert "history_id" in history_columns
    assert "payload_json" in history_columns
    assert "body_text" in history_columns
    assert "content_hash" in history_columns


def test_control_plane_db_path_uses_canonical_project_root_from_issue_worktree(tmp_path: Path):
    issue_worktree = tmp_path / ".opencode/runtime/issue-worktrees/issue-42"
    issue_worktree.mkdir(parents=True, exist_ok=True)

    db_path = ensure_control_plane_db(issue_worktree)

    assert canonical_control_plane_base_dir(issue_worktree) == tmp_path
    assert db_path == tmp_path / ".opencode/runtime/control-plane.sqlite3"
    assert control_plane_db_path(issue_worktree) == tmp_path / ".opencode/runtime/control-plane.sqlite3"
    assert not (issue_worktree / ".opencode/runtime/control-plane.sqlite3").exists()


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
    assert issue["current_session_id"] == ""
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
    latest_ref = read_latest_ref(tmp_path, "42", "root_event")
    assert latest_ref["session_id"] == "ses-root"


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
    latest_ref = read_latest_ref(tmp_path, "42", "github_sync")

    assert attempt is not None
    assert attempt["status"] == "failed"
    assert attempt["last_error"] == "boom"
    assert latest_ref["command_id"] == "cmd-gh"
    assert latest_ref["status"] == "failed"


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
    _ = ingest_issue_packet(
        tmp_path,
        issue_number="41",
        issue_packet={
            "issue_number": "41",
            "title": "Issue 41",
            "branch": "agent/issue-41-demo",
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


def test_ready_issues_for_selection_requires_canonical_packet_fields(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    _ = ingest_issue_packet(
        tmp_path,
        issue_number="41",
        issue_packet={
            "issue_number": "41",
            "title": "Issue 41",
            "branch": "agent/issue-41-demo",
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
            "title": "",
            "branch": "agent/issue-42-demo",
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

    assert [row["issue_number"] for row in rows] == ["41"]


def test_ready_issues_for_selection_excludes_ready_rows_with_current_session_id_and_respects_limit(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    for issue_number, rank_score in (("41", 100), ("42", 90), ("43", 80)):
        _ = ingest_issue_packet(
            tmp_path,
            issue_number=issue_number,
            issue_packet={
                "issue_number": issue_number,
                "title": f"Issue {issue_number}",
                "branch": f"agent/issue-{issue_number}-demo",
                "labels": ["ready-for-agent"],
                "parent_reference": "none",
                "dependencies": [],
            },
            updated_at="2026-05-11T10:00:00+08:00",
        )
        _ = upsert_issue_ranking(
            tmp_path,
            issue_number=issue_number,
            rank_score=rank_score,
            lane="default",
            updated_at="2026-05-11T10:01:00+08:00",
        )

    _ = upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready-fenced",
        updated_at="2026-05-11T10:02:00+08:00",
        current_session_id="ses-still-fenced",
    )

    rows = ready_issues_for_selection(tmp_path, limit=2)

    assert [row["issue_number"] for row in rows] == ["41", "43"]


def test_slot_occupancy_counts_development_and_release_pools_separately(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    for issue_number, state in (
        ("41", "claimed"),
        ("42", "running"),
        ("43", "verifying"),
        ("44", "verified"),
        ("45", "release_pending"),
        ("46", "release_pending"),
        ("47", "quarantined"),
    ):
        _ = upsert_issue_state(
            tmp_path,
            issue_number=issue_number,
            state=state,
            command_id=f"cmd-{issue_number}-{state}",
            updated_at="2026-05-11T10:00:00+08:00",
        )

    _ = sync_issue_runtime_context(
        tmp_path,
        issue_number="45",
        updated_at="2026-05-11T10:01:00+08:00",
        current_role="release_worker",
        current_stage="release_worker_execution",
        current_status="queued",
    )
    _ = sync_issue_runtime_context(
        tmp_path,
        issue_number="46",
        updated_at="2026-05-11T10:01:00+08:00",
        current_role="main_orchestrator",
        current_stage="issue_selection_or_recovery",
        current_status="queued",
    )

    assert development_slot_occupancy(tmp_path) == 3
    assert release_slot_occupancy(tmp_path) == 1
    assert available_development_slots(tmp_path, 4) == 1
    assert available_development_slots(tmp_path, 2) == 0
    assert available_release_slots(tmp_path, 2) == 1


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
    latest_ref = read_latest_ref(tmp_path, "42", "admin_action")

    assert decision is not None
    assert decision["command_id"] == "cmd-gh:retry"
    assert decision["decision_type"] == "admin_github_sync_retry"
    assert latest_ref["command_id"] == "cmd-gh:retry"


def test_ingest_issue_packet_and_runtime_context_store_on_issue_row(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    _ = ingest_issue_packet(
        tmp_path,
        issue_number="42",
        issue_packet={
            "issue_number": "42",
            "title": "Demo issue",
            "branch": "agent/issue-42-demo",
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
        runtime_context={"dispatch_request": {"request_id": "req-42"}},
        artifact_refs={
            "worker_result_ref": "docs/agents/worker-results/issue-42.yaml",
        },
        worktree_path="/tmp/project/.opencode/runtime/issue-worktrees/issue-42",
    )

    packet = read_issue_packet(tmp_path, "42")
    rows = issue_rows_with_packets(tmp_path)
    issue = read_issue(tmp_path, "42")
    runtime_context = read_runtime_context(tmp_path, "42")
    latest_ref = read_latest_ref(tmp_path, "42", "issue_packet")

    assert packet["branch"] == "agent/issue-42-demo"
    assert [row["issue_number"] for row in rows] == ["42"]
    assert issue is not None
    assert issue["title"] == "Demo issue"
    assert issue["branch"] == "agent/issue-42-demo"
    assert issue["current_role"] == "issue_worker"
    assert issue["current_stage"] == "issue_worker_execution"
    assert issue["worktree_path"] == "/tmp/project/.opencode/runtime/issue-worktrees/issue-42"
    assert runtime_context["dispatch_request"]["request_id"] == "req-42"
    assert runtime_context["artifact_refs"]["worker_result_ref"] == "docs/agents/worker-results/issue-42.yaml"
    assert latest_ref["status"] == "ingested"


def test_ingest_issue_packet_records_history_before_snapshot_ref(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    issue = ingest_issue_packet(
        tmp_path,
        issue_number="42",
        issue_packet={
            "issue_number": "42",
            "title": "Demo issue",
            "branch": "agent/issue-42-demo",
            "raw_text": "issue: 42\ntitle: Demo issue\n",
        },
        updated_at="2026-05-11T10:00:00+08:00",
    )
    latest_history = read_latest_issue_history(tmp_path, "42", entry_type="issue_packet")
    latest_ref = read_latest_ref(tmp_path, "42", "issue_packet")

    assert latest_history is not None
    assert issue["last_history_id"] == latest_history["history_id"]
    assert latest_ref["history_id"] == latest_history["history_id"]
    assert latest_history["body_text"] == "issue: 42\ntitle: Demo issue\n"


def test_upsert_issue_state_uses_single_current_session_id(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    _ = transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-claim",
        scheduler_id="scheduler:test",
        reason="claim issue",
        updated_at="2026-05-11T09:59:00+08:00",
        from_state="ready",
    )
    _ = transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="dispatching",
        command_id="cmd-dispatch",
        scheduler_id="scheduler:test",
        reason="queue root session",
        updated_at="2026-05-11T09:59:30+08:00",
        from_state="claimed",
    )
    issue = transition_issue_state(tmp_path,
    issue_number="42",
    to_state="running",
    command_id="cmd-running",
    scheduler_id="scheduler:test",
    reason="start root session",
    updated_at="2026-05-11T10:00:00+08:00",
    from_state="dispatching", current_session_id="ses-root-42", )

    assert issue["current_session_id"] == "ses-root-42"
    assert "current_root_session_id" not in issue
    assert "current_verifier_session_id" not in issue

    _ = transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="verifying",
        command_id="cmd-verifying",
        scheduler_id="scheduler:test",
        reason="begin verifier stage",
        updated_at="2026-05-11T10:03:00+08:00",
        from_state="running",
    )
    verified = upsert_issue_state(tmp_path,
    issue_number="42",
    state="verified",
    command_id="cmd-verified",
    updated_at="2026-05-11T10:05:00+08:00", current_session_id="ses-verifier-42", )

    assert verified["current_session_id"] == "ses-verifier-42"
    assert "current_root_session_id" not in verified
    assert "current_verifier_session_id" not in verified
    assert verified["verified_at"] == "2026-05-11T10:05:00+08:00"


def test_append_issue_history_stores_body_text_and_content_hash(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    history_id = append_issue_history(
        tmp_path,
        issue_number="42",
        entry_type="dispatch_result",
        created_at="2026-05-11T10:00:00+08:00",
        status="success",
        summary="dispatch finished",
        payload={"rootSessionID": "ses-root-42"},
        body_text="rootSessionID: ses-root-42",
        content_hash="sha256:demo",
        unique_key="dispatch-result:42:1",
    )
    latest = read_latest_issue_history(tmp_path, "42", entry_type="dispatch_result")

    assert history_id > 0
    assert latest is not None
    assert latest["body_text"] == "rootSessionID: ses-root-42"
    assert latest["content_hash"] == "sha256:demo"


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
        to_state="verified",
        command_id="cmd-verified",
        scheduler_id="scheduler:test",
        reason="verifier accepted issue",
        updated_at="2026-05-11T10:04:00+08:00",
        from_state="verifying",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="completed",
        command_id="cmd-completed",
        scheduler_id="scheduler:test",
        reason="complete verified issue",
        updated_at="2026-05-11T10:05:00+08:00",
        from_state="verified",
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


def test_record_pr_opened_writes_history_and_latest_ref(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    opened = record_pr_opened(
        tmp_path,
        issue_number="42",
        pr_number="77",
        created_at="2026-05-11T10:00:00+08:00",
        verifier_session_id="ses-v",
        command_id="cmd-pr-opened",
        payload={"source_artifact": "evidence_packet"},
    )
    latest_history = read_latest_issue_history(tmp_path, "42", entry_type="pr_opened")
    latest_ref = read_latest_ref(tmp_path, "42", "pr_opened")

    assert opened["pr_number"] == "77"
    assert latest_history is not None
    assert latest_history["status"] == "opened"
    assert '"pr_number": "77"' in str(latest_history["body_text"])
    assert latest_ref["status"] == "opened"
    assert latest_ref["session_id"] == "ses-v"


def test_transition_issue_state_records_latest_ref_after_history_append(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    issue = transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-claim",
        scheduler_id="scheduler:test",
        reason="claim issue",
        updated_at="2026-05-11T10:00:00+08:00",
        from_state="ready",
    )
    latest_history = read_latest_issue_history(tmp_path, "42", entry_type="state_transition")
    latest_ref = read_latest_ref(tmp_path, "42", "state_transition")

    assert latest_history is not None
    assert issue["last_history_id"] == latest_history["history_id"]
    assert latest_ref["history_id"] == latest_history["history_id"]
    assert latest_ref["command_id"] == "cmd-claim"
