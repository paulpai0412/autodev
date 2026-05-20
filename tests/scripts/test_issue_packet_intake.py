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
        body="1. Preserve verifier evidence\n2. Keep raw logs out of repo docs\nParent: https://github.com/paulpai0412/autodev/issues/1",
        url="https://github.com/paulpai0412/autodev/issues/42",
        labels=["ready-for-agent"],
    )

    packet = render_issue_packet(issue, prepared_at="2026-05-10T12:00:00+08:00")

    assert 'kind: issue_packet' in packet
    assert 'number: "42"' in packet
    assert 'labels: ["ready-for-agent"]' in packet
    assert 'branch: {name: "agent/issue-42-add-governed-sql-traceability", base: "main"}' in packet
    assert 'prepared_at: "2026-05-10T12:00:00+08:00"' in packet
    assert '<fill-from-issue-or-worker-discovery>' not in packet
    assert 'scope:' in packet
    assert 'relevant_paths:' in packet


def test_issue_packet_payload_contains_db_ready_fields():
    issue = GitHubIssue(
        number="42",
        title="Add governed SQL traceability",
        body="- observable behavior",
        url="https://github.com/paulpai0412/autodev/issues/42",
        labels=["ready-for-agent"],
    )

    payload = issue_packet_payload(issue, prepared_at="2026-05-10T12:00:00+08:00")

    assert payload["issue_number"] == "42"
    assert payload["branch"] == "agent/issue-42-add-governed-sql-traceability"
    assert payload["base_branch"] == "main"
    assert payload["backing_type"] == "github"
    assert "kind: issue_packet" in str(payload["raw_text"])


def test_issue_packet_payload_preserves_explicit_base_branch():
    issue = GitHubIssue(
        number="43",
        title="Build child feature",
        body="Base Branch: agent/issue-42-parent\n- observable behavior",
        url="https://github.com/paulpai0412/autodev/issues/43",
        labels=["ready-for-agent"],
    )

    payload = issue_packet_payload(issue, prepared_at="2026-05-10T12:00:00+08:00")

    assert payload["base_branch"] == "agent/issue-42-parent"
    assert 'base: "agent/issue-42-parent"' in str(payload["raw_text"])


def test_render_issue_packet_infers_scope_and_relevant_paths_from_issue_body():
    issue = GitHubIssue(
        number="44",
        title="Tag vocab items",
        body=(
            "## Scope\n"
            "- Add tag CRUD in `index.html`\n"
            "- Keep default flow in `smoke_test.js`\n"
            "\n"
            "Use `docs/agents/issue-tracker.md` for tracking.\n"
        ),
        url="https://github.com/paulpai0412/autodev/issues/44",
        labels=["ready-for-agent"],
    )

    packet = render_issue_packet(issue, prepared_at="2026-05-10T12:00:00+08:00")

    assert 'in: ["Add tag CRUD in `index.html`", "Keep default flow in `smoke_test.js`"]' in packet
    assert 'relevant_paths: ["index.html", "smoke_test.js", "docs/agents/issue-tracker.md"]' in packet


def test_sync_issue_packets_to_db_ingests_packets(tmp_path: Path):
    (tmp_path / ".autodev.yaml").write_text('schema_version: "1.0"\nproject:\n  name: demo\n', encoding="utf-8")
    issues = [
        GitHubIssue(
            number="42",
            title="Add governed SQL traceability",
            body="- observable behavior",
            url="https://github.com/paulpai0412/autodev/issues/42",
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
                    "url": "https://github.com/paulpai0412/autodev/issues/42",
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
                    "url": "https://github.com/paulpai0412/autodev/issues/42",
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


def test_main_requires_consumer_project_root(tmp_path: Path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fixture_path = tmp_path / "issues.json"
    fixture_path.write_text(
        json.dumps(
            [
                {
                    "number": 42,
                    "title": "Add governed SQL traceability",
                    "body": "- observable behavior",
                    "url": "https://github.com/paulpai0412/autodev/issues/42",
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
                    "url": "https://github.com/paulpai0412/autodev/issues/42",
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


def test_infer_dependencies_publisher_blocked_by_issue_number_format():
    from scripts.issue_packet_intake import infer_dependencies

    issue = GitHubIssue(
        number="10",
        title="Runtime intake bridge",
        body="## Blocked by\n- Blocked by issue #9\n",
        url="https://github.com/paulpai0412/autodev/issues/10",
        labels=["ready-for-agent"],
    )

    deps = infer_dependencies(issue)

    assert "- Blocked by issue #9" in deps
    assert "## Blocked by" not in deps


def test_infer_dependencies_publisher_blocked_by_hash_only_format():
    from scripts.issue_packet_intake import infer_dependencies

    issue = GitHubIssue(
        number="10",
        title="Runtime intake bridge",
        body="## Blocked by\n- Blocked by #9\n",
        url="https://github.com/paulpai0412/autodev/issues/10",
        labels=["ready-for-agent"],
    )

    deps = infer_dependencies(issue)

    assert "- Blocked by #9" in deps
    assert "## Blocked by" not in deps


def test_infer_dependencies_no_deps_returns_none_sentinel():
    from scripts.issue_packet_intake import infer_dependencies

    issue = GitHubIssue(
        number="10",
        title="Runtime intake bridge",
        body="Some description without dependency lines.\n",
        url="https://github.com/paulpai0412/autodev/issues/10",
        labels=["ready-for-agent"],
    )

    deps = infer_dependencies(issue)

    assert deps == ["none"]


def test_issue_packet_payload_dependencies_ingested_from_publisher_format(tmp_path: Path):
    issue = GitHubIssue(
        number="10",
        title="Runtime intake bridge",
        body="## Blocked by\n- Blocked by issue #9\n",
        url="https://github.com/paulpai0412/autodev/issues/10",
        labels=["ready-for-agent"],
    )

    payload = issue_packet_payload(issue, prepared_at="2026-05-19T10:00:00+08:00")

    deps = payload["dependencies"]
    assert isinstance(deps, list)
    assert any("Blocked by issue #9" in str(d) for d in deps)
    assert all("## Blocked by" not in str(d) for d in deps)
