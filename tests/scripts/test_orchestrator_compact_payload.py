from __future__ import annotations

from pathlib import Path

from pytest import CaptureFixture

from scripts.orchestrator_compact_payload import (
    CHECKPOINT_LINE_CAP,
    derive_compact_payload,
    CompactPayload,
    main,
    parse_checkpoint_text,
    update_checkpoint_text,
)


SAMPLE_CHECKPOINT_WITHOUT_PAYLOAD = """schema_version: \"1.0\"
kind: context_checkpoint
line_cap: 80

subject:
  issue_number: \"42\"
  branch: \"agent/issue-42-demo\"
  role: \"main_orchestrator\"
  checkpoint_reason: \"selected_afk_issue\"

context_budget:
  warning_at_percent: 45
  stop_and_rotate_at_percent: 50
  measured_percent_used: \"unknown\"
  must_rotate_now: false

resume_policy:
  checkpoint_only_cross_session_resume: true
  do_not_import_full_prior_transcript: true
  raw_evidence_policy: \"index_only; raw logs/traces stay in artifact bundle\"

state:
  completed:
    - \"Issue #41 already merged.\"
  in_progress:
    - \"Old cross-session wording.\"
  next:
    - \"Old next step wording.\"
  blockers:
    - \"none\"

refs:
  issue_packet: \"docs/agents/issue-packets/issue-42.yaml\"
  worker_result: \"\"
  evidence_packet: \"\"
  handoff: \"docs/agents/handoffs/issue-41.yaml\"
  artifact_bundle: \"\"

metadata:
  updated_by: \"Build\"
  updated_at: \"2026-05-07T16:00:00+08:00\"
"""


def test_update_checkpoint_text_inserts_compact_payload_and_standardizes_state():
    updated = update_checkpoint_text(
        SAMPLE_CHECKPOINT_WITHOUT_PAYLOAD,
        updated_at="2026-05-07T16:30:00+08:00",
    )

    assert "compact_payload:" in updated
    assert '  active_target: {issue_number: "42", branch: "agent/issue-42-demo", role: "main_orchestrator", agent: "build", next_flow: "per_issue_flow"}' in updated
    assert '  in_progress:' in updated
    assert '    - "Prepare the orchestrator session to enter issue #42 PR flow."' in updated
    assert '    - "Run supervisor reconcile for issue #42 to persist issue_worker_execution before creating or switching the issue branch and launching the first issue_worker subagent."' in updated
    assert '  immediate_next_action: "Run supervisor reconcile for issue #42 to persist issue_worker_execution before creating or switching the issue branch and launching the first issue_worker subagent."' in updated
    assert '  updated_at: "2026-05-07T16:30:00+08:00"' in updated
    assert len(updated.splitlines()) <= CHECKPOINT_LINE_CAP


def test_update_checkpoint_text_can_record_workflow_start_approval_override():
    updated = update_checkpoint_text(
        SAMPLE_CHECKPOINT_WITHOUT_PAYLOAD,
        approval_override_mode="bypass_approval",
        override_source="user_requested_autodev_start",
        human_approval_skipped=True,
        updated_at="2026-05-07T16:30:00+08:00",
    )

    assert '  approval_override_mode: "bypass_approval"' in updated
    assert '  override_source: "user_requested_autodev_start"' in updated
    assert '  human_approval_skipped: true' in updated


def test_update_checkpoint_text_preserves_explicit_runtime_state_overrides():
    updated = update_checkpoint_text(
        SAMPLE_CHECKPOINT_WITHOUT_PAYLOAD,
        in_progress=["Continue supervisor recovery for issue #42."],
        next_steps=["Select the next ready issue packet or remain in orchestrator recovery if none are available."],
        blockers=["No eligible local issue packet found."],
        worker_result="docs/agents/worker-results/issue-42.yaml",
        evidence_packet="docs/agents/evidence/issue-42-pr-88.yaml",
        artifact_bundle="docs/agents/release-results/issue-42-pr-88.yaml",
        updated_at="2026-05-07T16:30:00+08:00",
    )

    assert '    - "Continue supervisor recovery for issue #42."' in updated
    assert '    - "Select the next ready issue packet or remain in orchestrator recovery if none are available."' in updated
    assert '    - "No eligible local issue packet found."' in updated
    assert '  worker_result: "docs/agents/worker-results/issue-42.yaml"' in updated
    assert '  evidence_packet: "docs/agents/evidence/issue-42-pr-88.yaml"' in updated
    assert '  artifact_bundle: "docs/agents/release-results/issue-42-pr-88.yaml"' in updated


def test_update_checkpoint_text_real_issue20_shape_stays_within_line_cap():
    checkpoint_path = Path("docs/agents/runtime/context-checkpoint.yaml")
    original = checkpoint_path.read_text(encoding="utf-8")

    updated = update_checkpoint_text(
        original,
        issue_number="20",
        branch="agent/issue-20-reconstruct-governed-query-traceability",
        role="main_orchestrator",
        issue_packet="docs/agents/issue-packets/issue-20.yaml",
        handoff="docs/agents/handoffs/issue-6.yaml",
        updated_at="2026-05-08T23:30:00+08:00",
    )

    assert len(updated.splitlines()) <= CHECKPOINT_LINE_CAP
    assert 'issue_number: "20"' in updated
    assert 'issue_packet: "docs/agents/issue-packets/issue-20.yaml"' in updated
    assert 'handoff: "docs/agents/handoffs/issue-6.yaml"' in updated


def test_derive_compact_payload_reads_runtime_checkpoint_contract():
    checkpoint_path = Path("docs/agents/runtime/context-checkpoint.yaml")
    record = parse_checkpoint_text(checkpoint_path.read_text(encoding="utf-8"))

    payload: CompactPayload = derive_compact_payload(record)

    assert payload["active_target"]["issue_number"] == record.issue_number
    assert payload["active_target"]["next_flow"] == "per_issue_flow"
    assert payload["active_target"]["agent"] == "build"
    assert payload["authoritative_refs"][0] == record.issue_packet
    assert payload["authoritative_refs"][-1] == "docs/agents/autonomous-development-workflow.yaml"
    assert payload["immediate_next_action"] == record.next_steps[0]


def test_main_prints_compact_payload_for_preview(tmp_path: Path, capsys: CaptureFixture[str]):
    checkpoint_path = tmp_path / "context-checkpoint.yaml"
    _ = checkpoint_path.write_text(SAMPLE_CHECKPOINT_WITHOUT_PAYLOAD, encoding="utf-8")

    exit_code = main(["--checkpoint", str(checkpoint_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "compact_payload:" in captured.out
    assert 'issue_number: "42"' in captured.out
