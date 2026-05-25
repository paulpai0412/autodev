from __future__ import annotations

import json
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from scripts import orchestrator_lifecycle
from scripts.control_plane_db import ensure_control_plane_db, ingest_issue_packet, read_github_sync_attempt, record_artifact_fact


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


def test_sync_project_fields_projection_adds_issue_to_project_when_missing(tmp_path: Path) -> None:
    _seed_github_backed_issue(tmp_path)

    _ = orchestrator_lifecycle.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-21T10:06:00+08:00",
        runtime_context={
            "github_project_id": "PVT_project_1",
            "github_project_field_ids": {
                "state": "field-id-state",
                "pr_workflow": "field-id-pr",
            },
            "github_project_field_option_ids": {
                "state": {"running": "opt-running"},
                "pr_workflow": {},
            },
        },
    )

    calls: list[list[str]] = []

    def fake_run(command: list[str], **_: object) -> CompletedProcess[str]:
        calls.append(command)
        if command[:3] == ["gh", "api", "graphql"] and "number=42" in command:
            return CompletedProcess(
                command,
                0,
                stdout=json.dumps(
                    {
                        "data": {
                            "repository": {
                                "issue": {
                                    "id": "ISSUE_node_42",
                                    "projectItems": {"nodes": []},
                                }
                            }
                        }
                    }
                ),
                stderr="",
            )
        if command[:3] == ["gh", "api", "graphql"] and "content=ISSUE_node_42" in command:
            return CompletedProcess(
                command,
                0,
                stdout=json.dumps({"data": {"addProjectV2ItemById": {"item": {"id": "ITEM_42"}}}}),
                stderr="",
            )
        if command[:3] == ["gh", "api", "graphql"] and "option=opt-running" in command:
            return CompletedProcess(command, 0, stdout=json.dumps({"data": {"projectV2Item": {"id": "ITEM_42"}}}), stderr="")
        return CompletedProcess(command, 1, stdout="", stderr="unexpected command")

    error = orchestrator_lifecycle.sync_project_fields_projection(
        base_dir=tmp_path,
        issue_number="42",
        repo="example/repo",
        fields={"field-id-state": "running"},
        now=lambda explicit: explicit or "2026-05-21T10:06:00+08:00",
        run=fake_run,
        command_id="cmd-project-fields-add-item",
        updated_at="2026-05-21T10:06:00+08:00",
    )

    assert error == ""
    assert any("content=ISSUE_node_42" in part for call in calls for part in call)
    attempt = read_github_sync_attempt(tmp_path, "cmd-project-fields-add-item")
    assert attempt is not None
    assert attempt["status"] == "success"
    assert attempt["projection_target"] == "project_fields"


def test_sync_project_fields_projection_uses_autodev_yaml_fallback_when_runtime_binding_missing(tmp_path: Path) -> None:
    _seed_github_backed_issue(tmp_path)
    (tmp_path / ".autodev.yaml").write_text(
        "\n".join(
            [
                'schema_version: "1.0"',
                'project:',
                '  github_repo: example/repo',
                'github_project_id: "PVT_project_from_config"',
                'github_project_field_ids:',
                '  state: "field-id-state"',
                '  stage: "field-id-stage"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    calls: list[list[str]] = []

    def fake_run(command: list[str], **_: object) -> CompletedProcess[str]:
        calls.append(command)
        if command[:3] == ["gh", "api", "graphql"] and "number=42" in command:
            return CompletedProcess(
                command,
                0,
                stdout=json.dumps(
                    {
                        "data": {
                            "repository": {
                                "issue": {
                                    "id": "ISSUE_node_42",
                                    "projectItems": {"nodes": [{"id": "ITEM_42", "project": {"id": "PVT_project_from_config"}}]},
                                }
                            }
                        }
                    }
                ),
                stderr="",
            )
        if command[:3] == ["gh", "api", "graphql"] and "option=opt-in-progress" in command:
            return CompletedProcess(command, 0, stdout=json.dumps({"data": {"projectV2Item": {"id": "ITEM_42"}}}), stderr="")
        if command[:3] == ["gh", "api", "graphql"] and "query($project:ID!){node(id:$project)" in " ".join(command):
            return CompletedProcess(
                command,
                0,
                stdout=json.dumps(
                    {
                        "data": {
                            "node": {
                                "fields": {
                                    "nodes": [
                                        {
                                            "id": "field-id-state",
                                            "options": [{"id": "opt-in-progress", "name": "in progress"}],
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ),
                stderr="",
            )
        return CompletedProcess(command, 1, stdout="", stderr="unexpected command")

    error = orchestrator_lifecycle.sync_project_fields_projection(
        base_dir=tmp_path,
        issue_number="42",
        repo="example/repo",
        fields={"field-id-state": "in progress"},
        now=lambda explicit: explicit or "2026-05-21T10:06:00+08:00",
        run=fake_run,
        command_id="cmd-project-fields-config-fallback",
        updated_at="2026-05-21T10:06:00+08:00",
    )

    assert error == ""
    assert any("project=PVT_project_from_config" in part for call in calls for part in call)
    assert any("option=opt-in-progress" in part for call in calls for part in call)
    attempt = read_github_sync_attempt(tmp_path, "cmd-project-fields-config-fallback")
    assert attempt is not None
    assert attempt["status"] == "success"
    assert attempt["projection_target"] == "project_fields"


def test_sync_project_fields_projection_fails_for_single_select_without_option_mapping(tmp_path: Path) -> None:
    _seed_github_backed_issue(tmp_path)
    _ = orchestrator_lifecycle.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-21T10:06:00+08:00",
        runtime_context={
            "github_project_id": "PVT_project_1",
            "github_project_field_ids": {
                "state": "field-id-state",
            },
        },
    )

    calls: list[list[str]] = []

    def fake_run(command: list[str], **_: object) -> CompletedProcess[str]:
        calls.append(command)
        if command[:3] == ["gh", "api", "graphql"] and "number=42" in command:
            return CompletedProcess(
                command,
                0,
                stdout=json.dumps(
                    {
                        "data": {
                            "repository": {
                                "issue": {
                                    "id": "ISSUE_node_42",
                                    "projectItems": {"nodes": [{"id": "ITEM_42", "project": {"id": "PVT_project_1"}}]},
                                }
                            }
                        }
                    }
                ),
                stderr="",
            )
        if command[:3] == ["gh", "api", "graphql"] and "query($project:ID!){node(id:$project)" in " ".join(command):
            return CompletedProcess(
                command,
                0,
                stdout=json.dumps(
                    {
                        "data": {
                            "node": {
                                "fields": {
                                    "nodes": [
                                        {
                                            "id": "field-id-state",
                                            "options": [{"id": "opt-ready", "name": "ready"}],
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ),
                stderr="",
            )
        return CompletedProcess(command, 1, stdout="", stderr="unexpected command")

    error = orchestrator_lifecycle.sync_project_fields_projection(
        base_dir=tmp_path,
        issue_number="42",
        repo="example/repo",
        fields={"field-id-state": "in progress"},
        now=lambda explicit: explicit or "2026-05-21T10:06:00+08:00",
        run=fake_run,
        command_id="cmd-project-fields-missing-option",
        updated_at="2026-05-21T10:06:00+08:00",
    )

    assert "missing GitHub project single-select option id" in error
    attempt = read_github_sync_attempt(tmp_path, "cmd-project-fields-missing-option")
    assert attempt is not None
    assert attempt["status"] == "failed"
    assert attempt["projection_target"] == "project_fields"


def test_sync_local_main_after_release_merge_skips_missing_issue_worktree(tmp_path: Path) -> None:
    _seed_github_backed_issue(tmp_path)

    _ = orchestrator_lifecycle.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-23T13:50:00+08:00",
        runtime_context={"issue_worktree_path": str(tmp_path / ".opencode/runtime/issue-worktrees/issue-42")},
    )

    _ = orchestrator_lifecycle.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-23T13:50:01+08:00",
        artifact_refs={
            "release_result_ref": "db:release_result:history:1:sha256:test",
        },
    )

    record_artifact_fact(
        tmp_path,
        issue_number="42",
        entry_type="release_result",
        created_at="2026-05-23T13:50:02+08:00",
        command_id="release-result-test",
        payload={"status": "success", "parse_ok": True, "merge": {"merged": True, "merged_sha": "abc123"}},
    )

    error = orchestrator_lifecycle.sync_local_main_after_release_merge(
        base_dir=tmp_path,
        issue_number="42",
    )

    assert error == ""


def test_sync_local_main_after_release_merge_only_syncs_base_dir_main_for_separate_worktree(tmp_path: Path) -> None:
    _seed_github_backed_issue(tmp_path)
    issue_worktree = tmp_path / ".opencode/runtime/issue-worktrees/issue-42"
    issue_worktree.mkdir(parents=True, exist_ok=True)

    _ = orchestrator_lifecycle.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-23T14:00:00+08:00",
        runtime_context={"issue_worktree_path": str(issue_worktree)},
    )

    record_artifact_fact(
        tmp_path,
        issue_number="42",
        entry_type="release_result",
        created_at="2026-05-23T14:00:01+08:00",
        command_id="release-result-base-dir-only-test",
        payload={"status": "success", "parse_ok": True, "merge": {"merged": True, "merged_sha": "abc123"}},
    )

    calls: list[tuple[list[str], Path | None]] = []

    def fake_run(command: list[str], *, cwd: Path | None = None, **_: object) -> CompletedProcess[str]:
        calls.append((command, cwd))
        return CompletedProcess(command, 0, stdout="", stderr="")

    with patch("scripts.orchestrator_lifecycle.subprocess.run", side_effect=fake_run):
        error = orchestrator_lifecycle.sync_local_main_after_release_merge(
            base_dir=tmp_path,
            issue_number="42",
        )

    assert error == ""
    assert calls[:4] == [
        (["git", "rev-parse", "--is-inside-work-tree"], tmp_path),
        (["git", "fetch", "origin", "main"], tmp_path),
        (["git", "checkout", "main"], tmp_path),
        (["git", "pull", "--ff-only", "origin", "main"], tmp_path),
    ]
    assert all(cwd == tmp_path for _command, cwd in calls)
