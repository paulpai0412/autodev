from __future__ import annotations

import io
from pathlib import Path
import json
from subprocess import CompletedProcess
from typing import cast
from unittest.mock import patch

from pytest import CaptureFixture, MonkeyPatch

import scripts.orchestrator_bootstrap_runner as orchestrator_bootstrap_runner
from scripts.orchestrator_bootstrap_runner import resolve_issue_packet_path, run_orchestrator_bootstrap
from scripts.control_plane_db import read_github_sync_attempt, read_issue
from scripts.orchestrator_supervisor import issue_lock_path, parse_issue_packet_text


class FakePopen:
    def __init__(self, stdout: str, stderr: str = "", *, returncode: int | None = None):
        self.stdout = io.StringIO(stdout)
        self.stderr = io.StringIO(stderr)
        self._returncode = returncode

    def poll(self) -> int | None:
        return self._returncode

    def terminate(self) -> None:
        self._returncode = -15


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


SAMPLE_CHECKPOINT = """schema_version: "1.0"
kind: context_checkpoint
line_cap: 80

subject:
  issue_number: "6"
  branch: "agent/issue-6-old"
  role: "main_orchestrator"
  checkpoint_reason: "selected_afk_issue"

context_budget:
  warning_at_percent: 45
  stop_and_rotate_at_percent: 50
  measured_percent_used: "unknown"
  must_rotate_now: false

resume_policy:
  checkpoint_only_cross_session_resume: true
  do_not_import_full_prior_transcript: true
  raw_evidence_policy: "index_only; raw logs/traces stay in artifact bundle"

state:
  completed:
    - "Issue #41 already merged."
  in_progress:
    - "Old state."
  next:
    - "Old next step."
  blockers:
    - "none"

refs:
  issue_packet: "docs/agents/issue-packets/issue-6.yaml"
  worker_result: ""
  evidence_packet: ""
  handoff: "docs/agents/handoffs/issue-5.yaml"
  artifact_bundle: ""

metadata:
  updated_by: "Build"
  updated_at: "2026-05-07T16:00:00+08:00"
"""


def test_parse_issue_packet_text_reads_issue_branch_and_handoff():
    record = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")

    assert record.issue_number == "42"
    assert record.branch == "agent/issue-42-demo"
    assert record.issue_packet_path == "docs/agents/issue-packets/issue-42.yaml"
    assert record.prior_handoff == "docs/agents/handoffs/issue-41.yaml"


def test_run_orchestrator_bootstrap_updates_checkpoint_and_returns_next_action(tmp_path: Path):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    ledger_path = tmp_path / "orchestrator-ledger.json"
    request_path = tmp_path / "new-session-request.json"
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")

    result = run_orchestrator_bootstrap(
        issue_packet_path=issue_packet_path,
        checkpoint_path=checkpoint_path,
        ledger_path=ledger_path,
        new_session_request_path=request_path,
        updated_at="2026-05-07T17:00:00+08:00",
    )

    updated = checkpoint_path.read_text(encoding="utf-8")
    request = request_path.read_text(encoding="utf-8")
    request_payload = cast(dict[str, object], json.loads(request))
    ledger = cast(dict[str, object], json.loads(ledger_path.read_text(encoding="utf-8")))
    assert result.issue_number == "42"
    assert result.branch == "agent/issue-42-demo"
    assert result.ledger_path == ledger_path
    assert result.new_session_request_path == request_path
    assert result.immediate_next_action.startswith("Continue per_issue_flow for issue #42")
    assert 'issue_number: "42"' in updated
    assert 'branch: "agent/issue-42-demo"' in updated
    assert 'handoff: "docs/agents/handoffs/issue-41.yaml"' in updated
    assert 'agent: "build"' in updated
    assert '"reason": "orchestrator bootstrap continuation for issue #42"' in request
    assert '"title": "Continue issue #42 on agent/issue-42-demo"' in request
    assert result.immediate_next_action in request
    assert "Immediately launch the first issue_worker subagent in this same turn" in request
    assert "run_in_background=false" in request
    assert "Wait for each child task call to finish in the foreground before continuing." in request
    assert "Do not include karpathy-guidelines in load_skills for child subagents" not in request
    assert request_payload["requestGeneration"] == 2
    assert request_payload["nonce"]
    assert request_payload["agent"] == "build"
    assert request_payload["createdForLedgerRevision"] == ledger["ledgerRevision"]
    issue_state = cast(dict[str, object], ledger["issue"])
    current_state = cast(dict[str, object], ledger["current"])
    automation = cast(dict[str, object], ledger["automation"])
    workflow = cast(dict[str, object], ledger["workflow"])
    assert issue_state["number"] == "42"
    assert current_state["stage"] == "orchestrator_bootstrap"
    assert str(automation["supervisorDocPath"]).endswith("docs/agents/runtime/nonstop-supervisor-loop.md")
    assert str(workflow["workflowPolicyPath"]).endswith("docs/agents/autonomous-development-workflow.yaml")
    assert str(workflow["releaseResultTemplatePath"]).endswith("docs/agents/release-result-template.yaml")


def test_run_orchestrator_bootstrap_records_workflow_start_approval_override(tmp_path: Path):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    ledger_path = tmp_path / "orchestrator-ledger.json"
    request_path = tmp_path / "new-session-request.json"
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")

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

    updated = checkpoint_path.read_text(encoding="utf-8")

    assert '  approval_override_mode: "bypass_approval"' in updated
    assert '  override_source: "user_requested_autodev_start"' in updated
    assert '  human_approval_skipped: true' in updated


def test_main_reports_continuation_request_written(tmp_path: Path, capsys: CaptureFixture[str]):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    ledger_path = tmp_path / "orchestrator-ledger.json"
    request_path = tmp_path / "new-session-request.json"
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")

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
            "--updated-at",
            "2026-05-07T17:00:00+08:00",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "orchestrator bootstrap: updated checkpoint" in captured.out
    assert "orchestrator bootstrap: wrote supervisor ledger" in captured.out
    assert "orchestrator bootstrap: wrote continuation request" in captured.out


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


def test_main_accepts_issue_number(tmp_path: Path, capsys: CaptureFixture[str], monkeypatch: MonkeyPatch):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True)
    _ = (issue_packets_dir / "issue-42.yaml").write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    ledger_path = tmp_path / "orchestrator-ledger.json"
    request_path = tmp_path / "new-session-request.json"
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")
    monkeypatch.setattr(orchestrator_bootstrap_runner, "ROOT", tmp_path)

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
    assert "orchestrator bootstrap: updated checkpoint" in captured.out
    assert json.loads(ledger_path.read_text(encoding="utf-8"))["issue"]["number"] == "42"


def test_main_accepts_issue_number_using_consumer_project_base_dir(
    tmp_path: Path,
    capsys: CaptureFixture[str],
    monkeypatch: MonkeyPatch,
):
    consumer_root = tmp_path / "consumer-project"
    checkpoint_path = consumer_root / "docs/agents/runtime/context-checkpoint.yaml"
    ledger_path = consumer_root / ".opencode/runtime/orchestrator-ledger.json"
    request_path = consumer_root / ".opencode/runtime/new-session-request.json"
    _ = checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")

    def fake_intake(base_dir: Path) -> bool:
        issue_packet_path = base_dir / "docs/agents/issue-packets/issue-42.yaml"
        issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
        _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
        return True

    monkeypatch.setattr(orchestrator_bootstrap_runner, "run_issue_packet_intake", fake_intake)

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
    assert "orchestrator bootstrap: updated checkpoint" in captured.out
    assert json.loads(ledger_path.read_text(encoding="utf-8"))["issue"]["issuePacketPath"] == str(
        consumer_root / "docs/agents/issue-packets/issue-42.yaml"
    )


def test_main_can_dispatch_immediately(tmp_path: Path, capsys: CaptureFixture[str]):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_test"}\n'),
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
    assert "orchestrator bootstrap: dispatched fresh root session" in captured.out
    session_result_path = request_path.parent / "new-session-result.json"
    assert not request_path.exists()
    assert json.loads(session_result_path.read_text(encoding="utf-8"))["rootSessionID"] == "ses_root_test"


def test_main_dispatch_reports_error_result_cleanly(tmp_path: Path, capsys: CaptureFixture[str]):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value=None):
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
    assert "orchestrator bootstrap: dispatch recorded error session result" in captured.out


def test_run_orchestrator_bootstrap_rejects_duplicate_issue_lock(tmp_path: Path):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")
    issue_lock_path(tmp_path, "42").parent.mkdir(parents=True, exist_ok=True)
    _ = issue_lock_path(tmp_path, "42").write_text(
        '{"sourceSessionID": "ses_existing", "createdAt": "2026-05-07T16:59:00+08:00"}\n',
        encoding="utf-8",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        try:
            _ = run_orchestrator_bootstrap(
                issue_packet_path=issue_packet_path,
                checkpoint_path=checkpoint_path,
                ledger_path=ledger_path,
                new_session_request_path=request_path,
                updated_at="2026-05-07T17:00:00+08:00",
            )
        except RuntimeError as error:
            assert "already in progress" in str(error)
        else:
            raise AssertionError("expected duplicate issue lock to be rejected")


def test_run_orchestrator_bootstrap_duplicate_issue_lock_reports_resume_command(tmp_path: Path):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")
    issue_lock_path(tmp_path, "42").parent.mkdir(parents=True, exist_ok=True)
    _ = issue_lock_path(tmp_path, "42").write_text(
        '{"sourceSessionID": "ses_existing", "rootSessionID": "ses_root_active", "createdAt": "2026-05-07T16:59:00+08:00"}\n',
        encoding="utf-8",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        try:
            _ = run_orchestrator_bootstrap(
                issue_packet_path=issue_packet_path,
                checkpoint_path=checkpoint_path,
                ledger_path=ledger_path,
                new_session_request_path=request_path,
                updated_at="2026-05-07T17:00:00+08:00",
            )
        except RuntimeError as error:
            assert str(error) == (
                "issue #42 is already in progress via ses_root_active since 2026-05-07T16:59:00+08:00; "
                "refusing duplicate start. Resume with: opencode --session ses_root_active."
            )
        else:
            raise AssertionError("expected duplicate issue lock to be rejected with resume hint")


def test_run_orchestrator_bootstrap_claims_issue_lock(tmp_path: Path):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        _ = run_orchestrator_bootstrap(
            issue_packet_path=issue_packet_path,
            checkpoint_path=checkpoint_path,
            ledger_path=ledger_path,
            new_session_request_path=request_path,
            updated_at="2026-05-07T17:00:00+08:00",
        )

    assert issue_lock_path(tmp_path, "42").exists()


def test_run_orchestrator_bootstrap_records_claimed_control_plane_state(tmp_path: Path):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        _ = run_orchestrator_bootstrap(
            issue_packet_path=issue_packet_path,
            checkpoint_path=checkpoint_path,
            ledger_path=ledger_path,
            new_session_request_path=request_path,
            updated_at="2026-05-07T17:00:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "claimed"


def test_run_orchestrator_bootstrap_clears_stale_issue_artifacts(tmp_path: Path):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    stale_worker = tmp_path / "docs/agents/worker-results/issue-42.yaml"
    stale_handoff = tmp_path / "docs/agents/handoffs/issue-42.yaml"
    stale_evidence = tmp_path / "docs/agents/evidence/issue-42-pr-77.yaml"
    stale_release = tmp_path / "docs/agents/release-results/issue-42-pr-77.yaml"
    for path in [stale_worker, stale_handoff, stale_evidence, stale_release]:
        path.parent.mkdir(parents=True, exist_ok=True)
        _ = path.write_text("stale\n", encoding="utf-8")
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        _ = run_orchestrator_bootstrap(
            issue_packet_path=issue_packet_path,
            checkpoint_path=checkpoint_path,
            ledger_path=ledger_path,
            new_session_request_path=request_path,
            updated_at="2026-05-07T17:00:00+08:00",
        )

    assert not stale_worker.exists()
    assert not stale_handoff.exists()
    assert not stale_evidence.exists()
    assert not stale_release.exists()


def test_bootstrap_dispatch_error_releases_issue_lock(tmp_path: Path, capsys: CaptureFixture[str]):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    config_path = tmp_path / ".autodev.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")
    _ = config_path.write_text('schema_version: "1.0"\nproject:\n  github_repo: example/repo\n', encoding="utf-8")

    with patch("scripts.orchestrator_supervisor.subprocess.run", return_value=CompletedProcess(args=["gh"], returncode=0, stdout="", stderr="")), patch(
        "scripts.orchestrator_supervisor._resolve_opencode_cli", return_value=None
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
    assert "dispatch recorded error session result" in captured.out
    assert not issue_lock_path(tmp_path, "42").exists()


def test_bootstrap_claim_github_sync_failure_rolls_back_control_plane_state(tmp_path: Path):
    issue_packet_path = tmp_path / "issue-42.yaml"
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT, encoding="utf-8")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", side_effect=lambda **_: "sync failed"):
        try:
            run_orchestrator_bootstrap(
                issue_packet_path=issue_packet_path,
                checkpoint_path=checkpoint_path,
                ledger_path=ledger_path,
                new_session_request_path=request_path,
                updated_at="2026-05-07T17:00:00+08:00",
            )
        except RuntimeError as error:
            assert "failed to sync GitHub in-progress state" in str(error)
        else:
            raise AssertionError("expected bootstrap to fail when GitHub sync fails")

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "ready"
    assert not issue_lock_path(tmp_path, "42").exists()
