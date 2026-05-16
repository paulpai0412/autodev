from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from pytest import CaptureFixture, MonkeyPatch

import scripts.orchestrator_bootstrap_runner as orchestrator_bootstrap_runner
from scripts.control_plane_db import ingest_issue_packet, read_issue_packet
from scripts.orchestrator_supervisor import parse_issue_packet_text
from scripts.orchestrator_bootstrap_runner import resolve_issue_number, run_orchestrator_bootstrap


SAMPLE_ISSUE_PACKET = """schema_version: "1.0"
kind: issue_packet
line_cap: 80

issue:
  number: "42"
  title: "Demo issue"
  url: "https://github.com/example/issues/42"
  labels: [ready-for-agent]
  parent: {type: "prd", reference: "https://github.com/example/issues/1"}

branch: {name: "agent/issue-42-demo", base: "main"}

bootstrap_context:
  required_reads: ["AGENTS.md"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: ["scripts"]
  prior_handoff: "docs/agents/handoffs/issue-41.yaml"
"""


def test_resolve_issue_number_accepts_issue_number_variants(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    _ = ingest_issue_packet(
        tmp_path,
        issue_number=issue_packet.issue_number,
        issue_packet=orchestrator_bootstrap_runner._issue_packet_to_json(issue_packet),
        updated_at="2026-05-07T17:00:00+08:00",
    )
    assert resolve_issue_number("42", base_dir=tmp_path) == "42"
    assert resolve_issue_number("#42", base_dir=tmp_path) == "42"
    assert resolve_issue_number("issue-42", base_dir=tmp_path) == "42"


def test_resolve_issue_number_requires_db_packet(tmp_path: Path):
    try:
        resolve_issue_number("42", base_dir=tmp_path)
    except RuntimeError as error:
        assert "not recorded in SQLite" in str(error)
    else:
        raise AssertionError("expected DB-backed bootstrap resolution to require a stored issue packet")


def test_run_orchestrator_bootstrap_delegates_to_db_start(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    _ = ingest_issue_packet(
        tmp_path,
        issue_number=issue_packet.issue_number,
        issue_packet=orchestrator_bootstrap_runner._issue_packet_to_json(issue_packet),
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch(
        "scripts.orchestrator_bootstrap_runner.start_issue",
        return_value={"status": "success", "rootSessionID": "ses_root_test"},
    ) as start_issue_mock:
        result = run_orchestrator_bootstrap(
            base_dir=tmp_path,
            issue_number="42",
            updated_at="2026-05-07T17:00:00+08:00",
        )

    issue_packet = read_issue_packet(tmp_path, "42")

    assert result.issue_number == "42"
    assert result.branch == "agent/issue-42-demo"
    assert result.session_result["status"] == "success"
    assert result.session_result["rootSessionID"] == "ses_root_test"
    assert "DB-backed root session" in result.immediate_next_action
    assert issue_packet["issue_number"] == "42"
    assert issue_packet["branch"] == "agent/issue-42-demo"
    start_issue_mock.assert_called_once_with(
        base_dir=tmp_path,
        issue_number="42",
        source_session_id="orchestrator-bootstrap",
        updated_at="2026-05-07T17:00:00+08:00",
    )


def test_run_orchestrator_bootstrap_accepts_issue_number_without_packet_file(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    _ = ingest_issue_packet(
        tmp_path,
        issue_number=issue_packet.issue_number,
        issue_packet=orchestrator_bootstrap_runner._issue_packet_to_json(issue_packet),
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch(
        "scripts.orchestrator_bootstrap_runner.start_issue",
        return_value={"status": "success", "rootSessionID": "ses_root_test"},
    ) as start_issue_mock:
        result = run_orchestrator_bootstrap(
            base_dir=tmp_path,
            issue_number="42",
            updated_at="2026-05-07T17:00:00+08:00",
        )

    assert result.issue_number == "42"
    assert result.branch == "agent/issue-42-demo"
    start_issue_mock.assert_called_once()


def test_main_accepts_issue_number_and_reports_db_backed_start(tmp_path: Path, capsys: CaptureFixture[str], monkeypatch: MonkeyPatch):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    _ = ingest_issue_packet(
        tmp_path,
        issue_number=issue_packet.issue_number,
        issue_packet=orchestrator_bootstrap_runner._issue_packet_to_json(issue_packet),
        updated_at="2026-05-07T17:00:00+08:00",
    )
    monkeypatch.setattr(orchestrator_bootstrap_runner, "ROOT", tmp_path)

    with patch(
        "scripts.orchestrator_bootstrap_runner.start_issue",
        return_value={"status": "success", "rootSessionID": "ses_root_test"},
    ):
        exit_code = orchestrator_bootstrap_runner.main(
            [
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--updated-at",
                "2026-05-07T17:00:00+08:00",
            ]
        )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "delegated to DB-backed start-issue for issue #42" in captured.out
    assert "next action -> Inspect the DB-backed root session" in captured.out


def test_main_reports_db_backed_start_error_status(tmp_path: Path, capsys: CaptureFixture[str]):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    _ = ingest_issue_packet(
        tmp_path,
        issue_number=issue_packet.issue_number,
        issue_packet=orchestrator_bootstrap_runner._issue_packet_to_json(issue_packet),
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch(
        "scripts.orchestrator_bootstrap_runner.start_issue",
        return_value={"status": "error", "error": "dispatch failed"},
    ):
        exit_code = orchestrator_bootstrap_runner.main(
            [
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:00:00+08:00",
            ]
        )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "DB-backed start recorded error for issue #42" in captured.out
