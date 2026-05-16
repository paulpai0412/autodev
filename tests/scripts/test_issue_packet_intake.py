from __future__ import annotations

import json
from pathlib import Path

from scripts.control_plane_db import read_issue_packet
from scripts.issue_packet_intake import GitHubIssue, issue_packet_payload, main, render_issue_packet, sync_issue_packets_to_db
from scripts import issue_packet_intake
from subprocess import CompletedProcess
from unittest.mock import patch


def test_render_issue_packet_creates_compact_ready_packet():
    issue = GitHubIssue(
        number="42",
        title="Add governed SQL traceability",
        body="1. Preserve verifier evidence\n2. Keep raw logs out of repo docs\nParent: https://github.com/paulpai0412/wferp/issues/1",
        url="https://github.com/paulpai0412/wferp/issues/42",
        labels=["ready-for-agent"],
    )

    packet = render_issue_packet(issue, prepared_at="2026-05-10T12:00:00+08:00")

    assert 'kind: issue_packet' in packet
    assert 'number: "42"' in packet
    assert 'labels: ["ready-for-agent"]' in packet
    assert 'branch: {name: "agent/issue-42-add-governed-sql-traceability", base: "main"}' in packet
    assert 'prepared_at: "2026-05-10T12:00:00+08:00"' in packet


def test_issue_packet_payload_contains_db_ready_fields():
    issue = GitHubIssue(
        number="42",
        title="Add governed SQL traceability",
        body="- observable behavior",
        url="https://github.com/paulpai0412/wferp/issues/42",
        labels=["ready-for-agent"],
    )

    payload = issue_packet_payload(issue, prepared_at="2026-05-10T12:00:00+08:00")

    assert payload["issue_number"] == "42"
    assert payload["branch"] == "agent/issue-42-add-governed-sql-traceability"
    assert payload["backing_type"] == "github"
    assert "kind: issue_packet" in str(payload["raw_text"])


def test_sync_issue_packets_to_db_ingests_packets(tmp_path: Path):
    (tmp_path / ".autodev.yaml").write_text('schema_version: "1.0"\nproject:\n  name: demo\n', encoding="utf-8")
    issues = [
        GitHubIssue(
            number="42",
            title="Add governed SQL traceability",
            body="- observable behavior",
            url="https://github.com/paulpai0412/wferp/issues/42",
            labels=["ready-for-agent"],
        )
    ]

    written = sync_issue_packets_to_db(issues, project_root=tmp_path)
    packet = read_issue_packet(tmp_path, "42")

    assert written == ["42"]
    assert packet["issue_number"] == "42"
    assert packet["branch"] == "agent/issue-42-add-governed-sql-traceability"


def test_main_reads_json_fixture(tmp_path: Path, capsys):
    fixture_path = tmp_path / "issues.json"
    (tmp_path / ".autodev.yaml").write_text('schema_version: "1.0"\nproject:\n  name: demo\n', encoding="utf-8")
    fixture_path.write_text(
        json.dumps(
            [
                {
                    "number": 42,
                    "title": "Add governed SQL traceability",
                    "body": "- observable behavior",
                    "url": "https://github.com/paulpai0412/wferp/issues/42",
                    "labels": ["ready-for-agent"],
                }
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(["--issues-json", str(fixture_path), "--project-root", str(tmp_path)])

    captured = capsys.readouterr()
    packet = read_issue_packet(tmp_path, "42")
    assert exit_code == 0
    assert "issue-42" in captured.out
    assert packet["issue_number"] == "42"


def test_main_discovers_consumer_project_from_project_root(tmp_path: Path, capsys):
    consumer_root = tmp_path / "consumer-project"
    nested_root = consumer_root / "packages/app"
    fixture_path = tmp_path / "issues.json"
    nested_root.mkdir(parents=True)
    (consumer_root / ".autodev.yaml").write_text('schema_version: "1.0"\nproject:\n  name: demo\n', encoding="utf-8")
    fixture_path.write_text(
        json.dumps(
            [
                {
                    "number": 42,
                    "title": "Add governed SQL traceability",
                    "body": "- observable behavior",
                    "url": "https://github.com/paulpai0412/wferp/issues/42",
                    "labels": ["ready-for-agent"],
                }
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(["--issues-json", str(fixture_path), "--project-root", str(nested_root)])

    captured = capsys.readouterr()
    packet = read_issue_packet(consumer_root, "42")
    assert exit_code == 0
    assert "issue-42" in captured.out
    assert packet["issue_number"] == "42"


def test_main_requires_consumer_project_root(tmp_path: Path, capsys):
    fixture_path = tmp_path / "issues.json"
    fixture_path.write_text(
        json.dumps(
            [
                {
                    "number": 42,
                    "title": "Add governed SQL traceability",
                    "body": "- observable behavior",
                    "url": "https://github.com/paulpai0412/wferp/issues/42",
                    "labels": ["ready-for-agent"],
                }
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(["--issues-json", str(fixture_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "could not find .autodev.yaml" in captured.out


def test_main_blocks_when_runtime_db_is_tracked(tmp_path: Path, capsys):
    fixture_path = tmp_path / "issues.json"
    (tmp_path / ".autodev.yaml").write_text('schema_version: "1.0"\nproject:\n  name: demo\n', encoding="utf-8")
    (tmp_path / "AGENTS.md").write_text("# AGENTS.md\n", encoding="utf-8")
    (tmp_path / ".opencode/runtime").mkdir(parents=True)
    (tmp_path / ".opencode/runtime/control-plane.sqlite3").write_text("", encoding="utf-8")
    fixture_path.write_text(
        json.dumps(
            [
                {
                    "number": 42,
                    "title": "Add governed SQL traceability",
                    "body": "- observable behavior",
                    "url": "https://github.com/paulpai0412/wferp/issues/42",
                    "labels": ["ready-for-agent"],
                }
            ]
        ),
        encoding="utf-8",
    )

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return CompletedProcess(args=args, returncode=0, stdout="true\n", stderr="")
        if args[:3] == ["git", "ls-files", ".opencode/runtime"]:
            return CompletedProcess(
                args=args,
                returncode=0,
                stdout=".opencode/runtime/.gitkeep\n.opencode/runtime/control-plane.sqlite3\n",
                stderr="",
            )
        raise AssertionError(f"unexpected command: {args}")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run):
        exit_code = main(["--issues-json", str(fixture_path), "--project-root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert f"[issue-packet-intake] project-root={tmp_path}" in captured.out
    assert "BLOCKED: tracked autodev runtime files must be removed from git index" in captured.out
