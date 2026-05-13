from __future__ import annotations

import json
from pathlib import Path

from scripts.issue_packet_intake import main, render_issue_packet, sync_issue_packets, GitHubIssue


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


def test_sync_issue_packets_writes_packet_files(tmp_path: Path):
    issues = [
        GitHubIssue(
            number="42",
            title="Add governed SQL traceability",
            body="- observable behavior",
            url="https://github.com/paulpai0412/wferp/issues/42",
            labels=["ready-for-agent"],
        )
    ]

    written = sync_issue_packets(issues, output_dir=tmp_path)

    assert written == [tmp_path / "issue-42.yaml"]
    assert written[0].exists()


def test_main_reads_json_fixture(tmp_path: Path, capsys):
    fixture_path = tmp_path / "issues.json"
    output_dir = tmp_path / "issue-packets"
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

    exit_code = main(["--issues-json", str(fixture_path), "--output-dir", str(output_dir)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "issue-42.yaml" in captured.out
    assert (output_dir / "issue-42.yaml").exists()


def test_main_discovers_consumer_project_output_dir_from_project_root(tmp_path: Path, capsys):
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
    output_dir = consumer_root / "docs/agents/issue-packets"
    assert exit_code == 0
    assert "issue-42.yaml" in captured.out
    assert (output_dir / "issue-42.yaml").exists()


def test_main_requires_consumer_project_or_output_dir(tmp_path: Path, capsys):
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
