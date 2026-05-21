from __future__ import annotations

import json
from pathlib import Path
from subprocess import CompletedProcess

from scripts import orchestrator_lifecycle
from scripts.control_plane_db import ensure_control_plane_db, ingest_issue_packet, read_github_sync_attempt


def _seed_github_backed_issue(tmp_path: Path, issue_number: str = "42") -> None:
    ensure_control_plane_db(tmp_path)
    ingest_issue_packet(
        tmp_path,
        issue_number=issue_number,
        issue_packet={
            "issue_number": issue_number,
            "title": "Issue 42",
            "branch": "agent/issue-42",
            "labels": ["ready-for-agent"],
            "dependencies": [],
            "backing_type": "github",
        },
        updated_at="2026-05-21T10:00:00+08:00",
    )


def test_upsert_projection_block_replaces_existing_block() -> None:
    original = (
        "hello\n\n"
        "<!-- autodev:projection:start -->\n"
        "old\n"
        "<!-- autodev:projection:end -->\n"
    )
    updated, changed = orchestrator_lifecycle._upsert_projection_block(
        body=original,
        projection_markdown="new block",
    )
    assert changed is True
    assert "new block" in updated
    assert "old" not in updated


def test_sync_issue_body_projection_updates_body_and_records_sync(tmp_path: Path) -> None:
    _seed_github_backed_issue(tmp_path)

    calls: list[list[str]] = []

    def fake_run(command: list[str], **_: object) -> CompletedProcess[str]:
        calls.append(command)
        if command[:4] == ["gh", "issue", "view", "42"]:
            return CompletedProcess(command, 0, stdout=json.dumps({"body": "original"}), stderr="")
        if command[:4] == ["gh", "issue", "edit", "42"]:
            return CompletedProcess(command, 0, stdout="", stderr="")
        return CompletedProcess(command, 1, stdout="", stderr="unexpected command")

    error = orchestrator_lifecycle.sync_issue_body_projection(
        base_dir=tmp_path,
        issue_number="42",
        repo="example/repo",
        projection_markdown="## Autodev status snapshot\n- state: running",
        now=lambda explicit: explicit or "2026-05-21T10:01:00+08:00",
        run=fake_run,
        command_id="cmd-body-sync",
        updated_at="2026-05-21T10:01:00+08:00",
    )

    assert error == ""
    assert len(calls) == 2
    assert calls[0][:4] == ["gh", "issue", "view", "42"]
    assert calls[1][:4] == ["gh", "issue", "edit", "42"]
    attempt = read_github_sync_attempt(tmp_path, "cmd-body-sync")
    assert attempt is not None
    assert attempt["status"] == "success"
    assert attempt["projection_target"] == "issue_body"


def test_sync_issue_body_projection_skips_when_unchanged(tmp_path: Path) -> None:
    _seed_github_backed_issue(tmp_path)
    body = (
        "before\n\n"
        "<!-- autodev:projection:start -->\n"
        "same projection\n"
        "<!-- autodev:projection:end -->\n"
    )
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_: object) -> CompletedProcess[str]:
        calls.append(command)
        return CompletedProcess(command, 0, stdout=json.dumps({"body": body}), stderr="")

    error = orchestrator_lifecycle.sync_issue_body_projection(
        base_dir=tmp_path,
        issue_number="42",
        repo="example/repo",
        projection_markdown="same projection",
        now=lambda explicit: explicit or "2026-05-21T10:01:00+08:00",
        run=fake_run,
        command_id="cmd-body-sync-nochange",
        updated_at="2026-05-21T10:01:00+08:00",
    )

    assert error == ""
    assert len(calls) == 1
    attempt = read_github_sync_attempt(tmp_path, "cmd-body-sync-nochange")
    assert attempt is not None
    assert attempt["status"] == "skipped"
    assert "unchanged" in str(attempt["last_error"])


def test_sync_issue_status_comment_creates_then_updates_by_marker(tmp_path: Path) -> None:
    _seed_github_backed_issue(tmp_path)
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_: object) -> CompletedProcess[str]:
        calls.append(command)
        if command[:3] == ["gh", "api", "repos/example/repo/issues/42/comments"] and "--method" not in command:
            return CompletedProcess(command, 0, stdout="[]", stderr="")
        if command[:3] == ["gh", "api", "repos/example/repo/issues/42/comments"] and "POST" in command:
            return CompletedProcess(command, 0, stdout="{}", stderr="")
        if command[:3] == ["gh", "api", "repos/example/repo/issues/42/comments"] and "--method" not in command:
            return CompletedProcess(command, 0, stdout="[]", stderr="")
        if command[:3] == ["gh", "api", "repos/example/repo/issues/42/comments"]:
            return CompletedProcess(command, 0, stdout="{}", stderr="")
        return CompletedProcess(command, 1, stdout="", stderr="unexpected command")

    error = orchestrator_lifecycle.sync_issue_status_comment(
        base_dir=tmp_path,
        issue_number="42",
        repo="example/repo",
        comment_markdown="## Autodev status\n- state: dispatching",
        now=lambda explicit: explicit or "2026-05-21T10:05:00+08:00",
        run=fake_run,
        command_id="cmd-status-comment",
        updated_at="2026-05-21T10:05:00+08:00",
    )

    assert error == ""
    attempt = read_github_sync_attempt(tmp_path, "cmd-status-comment")
    assert attempt is not None
    assert attempt["status"] == "success"
    assert attempt["projection_target"] == "status_comment"


def test_sync_project_fields_projection_skips_when_project_not_configured(tmp_path: Path) -> None:
    _seed_github_backed_issue(tmp_path)

    def fake_run(command: list[str], **_: object) -> CompletedProcess[str]:
        return CompletedProcess(command, 0, stdout="{}", stderr="")

    error = orchestrator_lifecycle.sync_project_fields_projection(
        base_dir=tmp_path,
        issue_number="42",
        repo="example/repo",
        fields={"field-id-state": "running"},
        now=lambda explicit: explicit or "2026-05-21T10:06:00+08:00",
        run=fake_run,
        command_id="cmd-project-fields",
        updated_at="2026-05-21T10:06:00+08:00",
    )

    assert error == ""
    attempt = read_github_sync_attempt(tmp_path, "cmd-project-fields")
    assert attempt is not None
    assert attempt["status"] == "skipped"
    assert attempt["projection_target"] == "project_fields"
