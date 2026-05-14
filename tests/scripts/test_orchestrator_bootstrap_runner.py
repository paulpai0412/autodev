from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from pytest import CaptureFixture, MonkeyPatch

import scripts.orchestrator_bootstrap_runner as orchestrator_bootstrap_runner
from scripts.control_plane_db import read_issue_packet
from scripts.orchestrator_bootstrap_runner import resolve_issue_packet_path, run_orchestrator_bootstrap


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


def test_resolve_issue_packet_path_accepts_issue_number(tmp_path: Path):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True)
    issue_packet_path = issue_packets_dir / "issue-42.yaml"
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")

    assert resolve_issue_packet_path("42", base_dir=tmp_path) == issue_packet_path
    assert resolve_issue_packet_path("#42", base_dir=tmp_path) == issue_packet_path
    assert resolve_issue_packet_path("issue-42", base_dir=tmp_path) == issue_packet_path


def test_resolve_issue_packet_path_runs_intake_when_missing(tmp_path: Path, monkeypatch: MonkeyPatch):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"

    def fake_intake(base_dir: Path) -> bool:
        issue_packet_path.parent.mkdir(parents=True)
        _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
        return base_dir == tmp_path

    monkeypatch.setattr(orchestrator_bootstrap_runner, "run_issue_packet_intake", fake_intake)

    assert resolve_issue_packet_path("42", base_dir=tmp_path) == issue_packet_path


def test_run_orchestrator_bootstrap_syncs_issue_packet_and_delegates_to_db_start(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    ledger_path = tmp_path / "orchestrator-ledger.json"
    request_path = tmp_path / "new-session-request.json"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text("legacy checkpoint\n", encoding="utf-8")

    with patch(
        "scripts.orchestrator_bootstrap_runner.start_issue",
        return_value={"status": "success", "rootSessionID": "ses_root_test"},
    ) as start_issue_mock:
        result = run_orchestrator_bootstrap(
            issue_packet_path=issue_packet_path,
            checkpoint_path=checkpoint_path,
            ledger_path=ledger_path,
            new_session_request_path=request_path,
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
        approval_override_mode=None,
        override_source=None,
        human_approval_skipped=None,
        updated_at="2026-05-07T17:00:00+08:00",
    )
    assert checkpoint_path.read_text(encoding="utf-8") == "legacy checkpoint\n"
    assert not ledger_path.exists()
    assert not request_path.exists()


def test_run_orchestrator_bootstrap_forwards_workflow_start_approval_override(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    ledger_path = tmp_path / "orchestrator-ledger.json"
    request_path = tmp_path / "new-session-request.json"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text("legacy checkpoint\n", encoding="utf-8")

    with patch(
        "scripts.orchestrator_bootstrap_runner.start_issue",
        return_value={"status": "success", "rootSessionID": "ses_root_test"},
    ) as start_issue_mock:
        _ = run_orchestrator_bootstrap(
            issue_packet_path=issue_packet_path,
            checkpoint_path=checkpoint_path,
            ledger_path=ledger_path,
            new_session_request_path=request_path,
            approval_override_mode="bypass_approval",
            override_source="user_requested_autodev_start",
            human_approval_skipped=True,
            updated_at="2026-05-07T17:00:00+08:00",
        )

    assert start_issue_mock.call_args.kwargs["approval_override_mode"] == "bypass_approval"
    assert start_issue_mock.call_args.kwargs["override_source"] == "user_requested_autodev_start"
    assert start_issue_mock.call_args.kwargs["human_approval_skipped"] is True


def test_run_orchestrator_bootstrap_rejects_external_issue_packet_path(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    external_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = workspace / "context-checkpoint.yaml"
    ledger_path = workspace / ".opencode/runtime/orchestrator-ledger.json"
    request_path = workspace / ".opencode/runtime/new-session-request.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = external_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text("legacy checkpoint\n", encoding="utf-8")

    try:
        run_orchestrator_bootstrap(
            issue_packet_path=external_packet_path,
            checkpoint_path=checkpoint_path,
            ledger_path=ledger_path,
            new_session_request_path=request_path,
            updated_at="2026-05-07T17:00:00+08:00",
        )
    except RuntimeError as error:
        assert "must live under" in str(error)
    else:
        raise AssertionError("expected bootstrap wrapper to reject external issue packet path")


def test_main_accepts_issue_number_and_reports_db_backed_start(tmp_path: Path, capsys: CaptureFixture[str], monkeypatch: MonkeyPatch):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True)
    _ = (issue_packets_dir / "issue-42.yaml").write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    ledger_path = tmp_path / "orchestrator-ledger.json"
    request_path = tmp_path / "new-session-request.json"
    _ = checkpoint_path.write_text("legacy checkpoint\n", encoding="utf-8")
    monkeypatch.setattr(orchestrator_bootstrap_runner, "ROOT", tmp_path)

    with patch(
        "scripts.orchestrator_bootstrap_runner.start_issue",
        return_value={"status": "success", "rootSessionID": "ses_root_test"},
    ):
        exit_code = orchestrator_bootstrap_runner.main(
            [
                "--issue-number",
                "42",
                "--checkpoint",
                str(checkpoint_path),
                "--ledger",
                str(ledger_path),
                "--new-session-request",
                str(request_path),
                "--updated-at",
                "2026-05-07T17:00:00+08:00",
            ]
        )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "delegated to DB-backed start-issue for issue #42" in captured.out
    assert "next action -> Inspect the DB-backed root session" in captured.out


def test_main_reports_db_backed_start_error_status(tmp_path: Path, capsys: CaptureFixture[str]):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text("legacy checkpoint\n", encoding="utf-8")

    with patch(
        "scripts.orchestrator_bootstrap_runner.start_issue",
        return_value={"status": "error", "error": "dispatch failed"},
    ):
        exit_code = orchestrator_bootstrap_runner.main(
            [
                "--issue-packet",
                str(issue_packet_path),
                "--checkpoint",
                str(checkpoint_path),
                "--ledger",
                str(ledger_path),
                "--new-session-request",
                str(request_path),
                "--dispatch-now",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:00:00+08:00",
            ]
        )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "DB-backed start recorded error for issue #42" in captured.out
