from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from pytest import CaptureFixture

from scripts.control_plane_db import ingest_issue_packet, read_issue, sync_issue_runtime_context, transition_issue_state, upsert_issue_ranking, upsert_issue_state
from scripts import orchestrator_monitor
from scripts.orchestrator_monitor import collect_monitor_events, main, run_monitor_watch
import scripts.orchestrator_supervisor as orchestrator_supervisor
from scripts.orchestrator_supervisor import create_initial_ledger, parse_issue_packet_text, reconcile_ledger


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
  context_budget: {warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: ["scripts"]
  prior_handoff: "docs/agents/handoffs/issue-41.yaml"
"""


def _issue_packet_text(issue_number: str, *, prior_handoff: str = "docs/agents/handoffs/issue-41.yaml") -> str:
    return (
        SAMPLE_ISSUE_PACKET.replace('number: "42"', f'number: "{issue_number}"')
        .replace('issues/42', f'issues/{issue_number}')
        .replace('agent/issue-42-demo', f'agent/issue-{issue_number}-demo')
        .replace('Demo issue', f'Issue {issue_number}')
        .replace('docs/agents/handoffs/issue-41.yaml', prior_handoff)
    )


def _release_result_text(issue_number: str, pr_number: str) -> str:
    return f'''schema_version: "1.0"
kind: release_result
line_cap: 60
raw_evidence_policy: index_only_refs_no_raw_logs_or_transcripts
subject:
  issue_number: "{issue_number}"
  pr_number: "{pr_number}"
  branch: "agent/issue-{issue_number}-demo"
status: "success"
blocked_reason: "none"
summary:
  outcome: "merged"
  next_recommended_step: "continue"
failure_classification: {{kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}}
merge:
  attempted: true
  merged: true
  merged_sha: "abc"
role_boundary:
  actor_role: "release_worker"
  may_run_final_acceptance_qa: false
  may_merge_only_after_verifier_pass: true
metadata:
  worker: "release-worker"
  worker_session_id: "ses-release"
  completed_at: "2026-05-07T17:20:00+08:00"
'''


def _runtime_context_text(issue_number: str) -> str:
    return f'''schema_version: "1.0"
kind: runtime_context
line_cap: 80

subject:
  issue_number: "{issue_number}"
  branch: "agent/issue-{issue_number}-demo"
  role: "main_orchestrator"
  control_plane_reason: "selected_afk_issue"

context_budget:
  warning_at_percent: 45
  stop_and_rotate_at_percent: 50
  measured_percent_used: "unknown"
  must_rotate_now: false

resume_policy:
  cross_session_resume: true
  do_not_import_full_prior_transcript: true
  raw_evidence_policy: "index_only"

state:
  completed:
    - "Issue #{issue_number} released."
  in_progress: []
  next: []
  blockers: []

refs:
  issue_packet: "docs/agents/issue-packets/issue-{issue_number}.yaml"
  worker_result: ""
  evidence_packet: ""
  handoff: "docs/agents/handoffs/issue-{issue_number}.yaml"
  artifact_bundle: ""

metadata:
  updated_by: "Build"
  updated_at: "2026-05-07T17:00:00+08:00"
'''


def _seed_release_handoff(tmp_path: Path) -> Path:
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    issue_31_path = packets_dir / "issue-31.yaml"
    issue_32_path = packets_dir / "issue-32.yaml"
    issue_31_path.write_text(_issue_packet_text("31"), encoding="utf-8")
    issue_32_path.write_text(_issue_packet_text("32", prior_handoff="docs/agents/handoffs/issue-31.yaml"), encoding="utf-8")

    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(_release_result_text("31", "88"), encoding="utf-8")
    orchestrator_supervisor.ensure_issue_row(
        tmp_path,
        issue_number="31",
        updated_at="2026-05-07T17:20:00+08:00",
    )
    orchestrator_supervisor.submit_artifact(
        base_dir=tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        body_text=release_path.read_text(encoding="utf-8"),
        updated_at="2026-05-07T17:20:00+08:00",
    )

    _seed_ready_issue(tmp_path, "32", updated_at="2026-05-07T17:00:00+08:00")

    return issue_31_path


def _sync_runtime_phase(
    tmp_path: Path,
    issue_number: str,
    *,
    role: str,
    stage: str,
    status: str,
    updated_at: str,
    automation_flags: dict[str, object] | None = None,
    artifact_refs: dict[str, object] | None = None,
    artifact_status: dict[str, object] | None = None,
) -> None:
    _ = sync_issue_runtime_context(
        tmp_path,
        issue_number=issue_number,
        updated_at=updated_at,
        current_role=role,
        current_stage=stage,
        current_status=status,
        automation_flags=automation_flags,
        artifact_refs=artifact_refs,
        artifact_status=artifact_status,
    )


def _seed_ready_issue(tmp_path: Path, issue_number: str, *, updated_at: str) -> None:
    issue_packet_path = tmp_path / f"docs/agents/issue-packets/issue-{issue_number}.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text(issue_number), encoding="utf-8")
    packet = parse_issue_packet_text(
        issue_packet_path.read_text(encoding="utf-8"),
        f"docs/agents/issue-packets/issue-{issue_number}.yaml",
    )
    _ = ingest_issue_packet(
        tmp_path,
        issue_number=issue_number,
        issue_packet={
            "issue_number": packet.issue_number,
            "title": packet.title,
            "branch": packet.branch,
            "labels": packet.labels,
            "parent_reference": packet.parent_reference,
            "dependencies": packet.dependencies,
        },
        updated_at=updated_at,
    )
    _ = upsert_issue_state(
        tmp_path,
        issue_number=issue_number,
        state="ready",
        command_id=f"cmd-ready-{issue_number}",
        updated_at=updated_at,
    )
    _ = upsert_issue_ranking(
        tmp_path,
        issue_number=issue_number,
        rank_score=float(10**6 - int(issue_number)),
        lane="default",
        updated_at=updated_at,
    )


def test_happy_path_release_hands_off_to_next_issue_worker_cycle(tmp_path: Path):
    issue_31_path = _seed_release_handoff(tmp_path)
    issue_packet = parse_issue_packet_text(issue_31_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        primary_workspace_root=str(tmp_path),
        updated_at="2026-05-07T17:00:00+08:00",
    )
    cast(dict[str, object], ledger["current"]).update({"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"})
    cast(dict[str, object], ledger["artifacts"]).update({
        "release_result_ref": "docs/agents/release-results/issue-31-pr-88.yaml",
    })

    next_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:21:00+08:00",)

    assert decision["action"] in {"queue_next_issue", "queue_next_session"}
    assert request is not None
    assert request["issueNumber"] == "32"
    assert cast(dict[str, object], next_ledger["issue"])["number"] == "32"

    bootstrapped_ledger, bootstrap_decision, bootstrap_request = reconcile_ledger(next_ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:22:00+08:00",)

    assert cast(dict[str, object], bootstrapped_ledger["current"])["role"] == "issue_worker"
    assert bootstrap_decision["action"] == "delegate_subagent"
    assert bootstrap_decision["next_role"] == "issue_worker"
    assert bootstrap_request is None
    assert read_issue(tmp_path, "31") is not None
    assert read_issue(tmp_path, "32") is not None


def test_release_handoff_uses_sqlite_packet_when_packet_file_is_missing(tmp_path: Path):
    issue_31_path = _seed_release_handoff(tmp_path)
    missing_next_packet_path = tmp_path / "docs/agents/issue-packets/issue-32.yaml"
    next_issue_packet = parse_issue_packet_text(
        missing_next_packet_path.read_text(encoding="utf-8"),
        "docs/agents/issue-packets/issue-32.yaml",
    )
    next_issue_payload: dict[str, object] = {
        "issue_number": next_issue_packet.issue_number,
        "title": next_issue_packet.title,
        "branch": next_issue_packet.branch,
        "backing_type": next_issue_packet.backing_type,
        "prior_handoff": next_issue_packet.prior_handoff,
        "labels": next_issue_packet.labels,
        "parent_reference": next_issue_packet.parent_reference,
        "dependencies": next_issue_packet.dependencies,
        "raw_text": next_issue_packet.raw_text,
    }
    _ = ingest_issue_packet(
        tmp_path,
        issue_number="32",
        issue_packet=next_issue_payload,
        updated_at="2026-05-07T17:00:00+08:00",
    )
    missing_next_packet_path.unlink()

    issue_packet = parse_issue_packet_text(issue_31_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        primary_workspace_root=str(tmp_path),
        updated_at="2026-05-07T17:00:00+08:00",
    )
    cast(dict[str, object], ledger["current"]).update({"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"})
    cast(dict[str, object], ledger["artifacts"]).update({
        "release_result_ref": "docs/agents/release-results/issue-31-pr-88.yaml",
    })

    next_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:21:00+08:00",)

    assert decision["action"] == "queue_next_issue"
    assert request is not None
    assert request["issueNumber"] == "32"
    assert not missing_next_packet_path.exists()


def test_collect_monitor_events_reports_healthy_runtime(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text("42"), encoding="utf-8")
    issue_packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    _ = upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        status="queued",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    events = collect_monitor_events(
        base_dir=tmp_path,
        now="2026-05-07T17:00:30+08:00",
    )

    assert len(events) == 1
    assert events[0]["rule_id"] == "RUNTIME_HEALTHY"


def test_collect_monitor_events_reports_empty_control_plane_when_no_issue_exists(tmp_path: Path):
    events = collect_monitor_events(
        base_dir=tmp_path,
        now="2026-05-07T17:00:30+08:00",
    )

    assert len(events) == 1
    assert events[0]["rule_id"] == "CONTROL_PLANE_EMPTY"


def test_collect_monitor_events_respects_explicit_issue_number(tmp_path: Path):
    _ = upsert_issue_state(
        tmp_path,
        issue_number="41",
        state="ready",
        command_id="cmd-41-ready",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    _sync_runtime_phase(
        tmp_path,
        "41",
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        status="queued",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    _ = upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="quarantined",
        command_id="cmd-42-quarantined",
        updated_at="2026-05-07T17:01:00+08:00",
    )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="issue_worker",
        stage="issue_worker_execution",
        status="queued",
        updated_at="2026-05-07T17:01:00+08:00",
    )

    events = collect_monitor_events(
        base_dir=tmp_path,
        issue_number="42",
        now="2026-05-07T17:02:00+08:00",
    )

    assert any(event["rule_id"] == "ISSUE_QUARANTINED" for event in events)
    assert all(cast(dict[str, object], event["evidence"]).get("issue_number") == "42" for event in events)


def test_collect_monitor_events_reports_stale_heartbeat(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text("42"), encoding="utf-8")
    issue_packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    cast(dict[str, object], ledger["current"]).update({"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"})
    ledger["updatedAt"] = "2026-05-07T17:00:00+08:00"
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-claim",
        scheduler_id="scheduler:test",
        reason="claim",
        updated_at="2026-05-07T17:00:00+08:00",
        from_state="ready",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="dispatching",
        command_id="cmd-dispatch",
        scheduler_id="scheduler:test",
        reason="dispatch",
        updated_at="2026-05-07T17:01:00+08:00",
        from_state="claimed",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="running",
        command_id="cmd-run",
        scheduler_id="scheduler:test",
        reason="run",
        updated_at="2026-05-07T17:02:00+08:00",
        from_state="dispatching",
    )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="issue_worker",
        stage="issue_worker_execution",
        status="queued",
        updated_at="2026-05-07T17:02:00+08:00",
    )

    events = collect_monitor_events(
        base_dir=tmp_path,
        now="2026-05-07T17:20:01+08:00",
        heartbeat_timeout_seconds=900,
    )

    assert any(event["rule_id"] == "ROOT_HEARTBEAT_STALLED" for event in events)


def test_collect_monitor_events_uses_wall_clock_when_now_is_omitted(tmp_path: Path, monkeypatch):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text("42"), encoding="utf-8")
    issue_packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    cast(dict[str, object], ledger["current"]).update({"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"})
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-claim",
        scheduler_id="scheduler:test",
        reason="claim",
        updated_at="2026-05-07T17:00:00+08:00",
        from_state="ready",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="dispatching",
        command_id="cmd-dispatch",
        scheduler_id="scheduler:test",
        reason="dispatch",
        updated_at="2026-05-07T17:01:00+08:00",
        from_state="claimed",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="running",
        command_id="cmd-run",
        scheduler_id="scheduler:test",
        reason="run",
        updated_at="2026-05-07T17:02:00+08:00",
        from_state="dispatching",
    )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="issue_worker",
        stage="issue_worker_execution",
        status="queued",
        updated_at="2026-05-07T17:02:00+08:00",
    )

    real_datetime = orchestrator_monitor.datetime

    class FrozenDateTime:
        @classmethod
        def now(cls):
            return cls()

        def astimezone(self):
            return self

        def isoformat(self, *, timespec: str = "auto") -> str:
            return "2026-05-07T17:20:01+08:00"

        @staticmethod
        def fromisoformat(value: str):
            return real_datetime.fromisoformat(value)

    monkeypatch.setattr(orchestrator_monitor, "datetime", FrozenDateTime)

    events = collect_monitor_events(
        base_dir=tmp_path,
        now=None,
        heartbeat_timeout_seconds=900,
    )

    stalled = [event for event in events if event["rule_id"] == "ROOT_HEARTBEAT_STALLED"]

    assert stalled
    assert cast(dict[str, object], stalled[0]["evidence"])["now"] == "2026-05-07T17:20:01+08:00"


def test_collect_monitor_events_reports_stale_dispatch_without_root_session_id(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text("42"), encoding="utf-8")
    issue_packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    cast(dict[str, object], ledger["current"]).update({"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"})
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-claim",
        scheduler_id="scheduler:test",
        reason="claim",
        updated_at="2026-05-07T17:00:00+08:00",
        from_state="ready",
    )
    transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="dispatching",
        command_id="cmd-dispatch",
        scheduler_id="scheduler:test",
        reason="dispatch",
        updated_at="2026-05-07T17:01:00+08:00",
        from_state="claimed",
    )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="issue_worker",
        stage="issue_worker_execution",
        status="queued",
        updated_at="2026-05-07T17:01:00+08:00",
        artifact_refs={"worker_result_ref": "docs/agents/worker-results/issue-42.yaml"},
    )

    events = collect_monitor_events(
        base_dir=tmp_path,
        now="2026-05-07T17:20:01+08:00",
        heartbeat_timeout_seconds=900,
    )

    assert any(event["rule_id"] == "DISPATCH_STALLED" for event in events)


def test_collect_monitor_events_reports_selection_stall_and_missing_artifact(tmp_path: Path):
    _ = _seed_release_handoff(tmp_path)
    issue_packet = parse_issue_packet_text(_issue_packet_text("31"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    cast(dict[str, object], ledger["current"]).update({"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"})
    ledger["updatedAt"] = "2026-05-07T17:00:00+08:00"
    cast(dict[str, object], ledger["artifacts"]).update({
        "release_result_ref": "docs/agents/release-results/issue-31-pr-404.yaml",
    })
    _ = upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="completed",
        command_id="cmd-complete",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    _sync_runtime_phase(
        tmp_path,
        "31",
        role="main_orchestrator",
        stage="issue_selection_or_recovery",
        status="queued",
        updated_at="2026-05-07T17:00:00+08:00",
        automation_flags={
            "continueWithoutHuman": True,
            "queueNextSessionOnIdle": True,
        },
        artifact_refs={"release_result_ref": "docs/agents/release-results/issue-31-pr-404.yaml"},
    )
    _seed_ready_issue(tmp_path, "32", updated_at="2026-05-07T17:01:00+08:00")

    events = collect_monitor_events(
        base_dir=tmp_path,
        now="2026-05-07T17:10:30+08:00",
        selection_timeout_seconds=300,
    )

    rule_ids = {str(event["rule_id"]) for event in events}
    stalled = next(event for event in events if str(event["rule_id"]) == "SELECTION_STALLED")
    evidence = cast(dict[str, object], stalled["evidence"])
    missing_artifacts = {
        cast(dict[str, object], event["evidence"])["artifact_key"]
        for event in events
        if str(event["rule_id"]) == "ARTIFACT_MISSING"
    }
    assert "SELECTION_STALLED" in rule_ids
    assert "ARTIFACT_MISSING" in rule_ids
    assert missing_artifacts == {"evidence_packet"}
    assert evidence["development_slot_occupancy"] == 0
    assert evidence["release_slot_occupancy"] == 0


def test_collect_monitor_events_surfaces_release_child_session_trace(tmp_path: Path):
    _ = upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-root-42",
    )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="main_orchestrator",
        stage="release_root_execution",
        status="running",
        updated_at="2026-05-07T17:00:00+08:00",
        artifact_status={
            "worker_result": {"parse_ok": True},
            "evidence_packet": {"parse_ok": True},
            "release_result": {"parse_ok": True},
        },
    )
    _ = sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        runtime_context={
            "release_child_session": {
                "childRole": "release_worker",
                "childSessionID": "ses-release-worker-42",
                "childSessionStatus": "stop",
                "rootSessionID": "ses-release-root-42",
                "recordedAt": "2026-05-07T17:00:00+08:00",
            }
        },
    )

    events = collect_monitor_events(
        base_dir=tmp_path,
        issue_number="42",
        now="2026-05-07T17:00:30+08:00",
    )

    tracked = next(event for event in events if event["rule_id"] == "RELEASE_CHILD_SESSION_TRACKED")
    evidence = cast(dict[str, object], tracked["evidence"])
    assert evidence["childRole"] == "release_worker"
    assert evidence["childSessionID"] == "ses-release-worker-42"


def test_monitor_cli_writes_jsonl_log_and_returns_non_zero_for_critical_issue(
    tmp_path: Path,
    capsys: CaptureFixture[str],
):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text("42"), encoding="utf-8")
    issue_packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    cast(dict[str, object], ledger["current"]).update({"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"})
    ledger["updatedAt"] = "2026-05-07T17:00:00+08:00"
    _ = upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="completed",
        command_id="cmd-complete",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="main_orchestrator",
        stage="issue_selection_or_recovery",
        status="queued",
        updated_at="2026-05-07T17:00:00+08:00",
        automation_flags={
            "continueWithoutHuman": True,
            "queueNextSessionOnIdle": True,
        },
    )
    _seed_ready_issue(tmp_path, "43", updated_at="2026-05-07T17:01:00+08:00")
    monitor_log_path = tmp_path / ".opencode/runtime/monitor.log"
    monitor_alerts_path = tmp_path / ".opencode/runtime/monitor-alerts.jsonl"

    exit_code = main(
        [
            "--base-dir",
            str(tmp_path),
            "--monitor-log",
            str(monitor_log_path),
            "--monitor-alerts",
            str(monitor_alerts_path),
            "--now",
            "2026-05-07T17:10:30+08:00",
            "--selection-timeout-seconds",
            "300",
        ]
    )

    captured = capsys.readouterr()
    log_lines = monitor_log_path.read_text(encoding="utf-8").strip().splitlines()
    alert_lines = monitor_alerts_path.read_text(encoding="utf-8").strip().splitlines()

    assert exit_code == 1
    assert captured.out
    assert log_lines
    assert alert_lines
    severities = [str(json.loads(line)["severity"]) for line in log_lines]
    alert_severities = [str(json.loads(line)["severity"]) for line in alert_lines]
    assert "critical" in severities
    assert all(severity in {"warning", "critical"} for severity in alert_severities)


def test_run_monitor_cycle_does_not_write_alerts_for_healthy_state(tmp_path: Path) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text("42"), encoding="utf-8")
    issue_packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    _ = upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        status="queued",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    monitor_log_path = tmp_path / ".opencode/runtime/monitor.log"
    monitor_alerts_path = tmp_path / ".opencode/runtime/monitor-alerts.jsonl"

    events, exit_code = orchestrator_monitor.run_monitor_cycle(
        base_dir=tmp_path,
        monitor_log_path=monitor_log_path,
        monitor_alerts_path=monitor_alerts_path,
        now="2026-05-07T17:00:30+08:00",
        heartbeat_timeout_seconds=900,
        selection_timeout_seconds=300,
    )

    assert exit_code == 0
    assert [str(event["rule_id"]) for event in events] == ["RUNTIME_HEALTHY"]
    assert monitor_log_path.exists()
    assert not monitor_alerts_path.exists()


def test_run_monitor_cycle_reports_stalled_issue_worker_without_auto_redispatch(tmp_path: Path) -> None:
    _ = upsert_issue_state(tmp_path,
    issue_number="42",
    state="running",
    command_id="cmd-running",
    updated_at="2026-05-07T17:00:00+08:00", current_session_id="ses-old-root", )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="issue_worker",
        stage="issue_worker_execution",
        status="queued",
        updated_at="2026-05-07T17:00:00+08:00",
        automation_flags={
            "continueWithoutHuman": True,
            "queueNextSessionOnIdle": True,
        },
        artifact_refs={"worker_result_ref": "docs/agents/worker-results/issue-42.yaml"},
    )
    monitor_log_path = tmp_path / ".opencode/runtime/monitor.log"

    events, exit_code = orchestrator_monitor.run_monitor_cycle(
        base_dir=tmp_path,
        monitor_log_path=monitor_log_path,
        now="2026-05-07T17:20:01+08:00",
        heartbeat_timeout_seconds=900,
        selection_timeout_seconds=300,
    )

    rule_ids = [str(event["rule_id"]) for event in events]
    runtime_issue = read_issue(tmp_path, "42")

    assert exit_code == 1
    assert "ROOT_HEARTBEAT_STALLED" in rule_ids
    assert runtime_issue is not None
    assert str(runtime_issue.get("state") or "") == "running"


def test_run_monitor_cycle_uses_persisted_artifact_fact_without_auto_advancing(tmp_path: Path) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text("42"), encoding="utf-8")
    issue_packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    cast(dict[str, object], ledger["current"]).update({"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"})
    cast(dict[str, object], ledger["artifacts"]).update({"worker_result_ref": "docs/agents/worker-results/issue-42.yaml"})
    worker_result_path = tmp_path / "docs/agents/worker-results/issue-42.yaml"
    worker_result_path.parent.mkdir(parents=True, exist_ok=True)
    worker_result_path.write_text(
        """schema_version: \"1.0\"
kind: worker_result
line_cap: 80
status: \"success\"
failure_classification: {kind: \"none\", retryable: true, routed_to: \"pr_verifier\", root_cause_signature: \"none\"}
summary:
  objective: \"demo\"
  outcome: \"done\"
files_changed:
  - path: \"foo\"
    summary: \"bar\"
verification:
  note: \"n\"
  gates:
    tdd_gate: \"pass\"
    implementation_self_check_gate: \"pass\"
    git_gate: \"pass\"
  implementation_self_checks:
    - command: \"pytest\"
      result: \"pass\"
      evidence_ref: \"db:issue-history/worker-result\"
      summary: \"ok\"
  final_acceptance_claim: false
evidence_packet_refs:
  worker_artifact_bundle: \"\"
  verifier_packet: \"docs/agents/evidence/issue-42-pr-77.yaml\"
  raw_evidence_policy: \"stored_outside_main_agent_context\"
role_boundary:
  actor_role: \"issue_worker\"
  may_execute_implementation_self_checks: true
  may_execute_final_acceptance_qa: false
  may_emit_final_verification: false
  verifier_packet_required_for_completion: true
pr:
  number: \"77\"
  url: \"https://example/pr/77\"
  ready_for_review: true
blockers:
  - \"none\"
next_recommended_step: \"Spawn verifier\"
metadata:
  worker: \"w\"
  worker_session_id: \"ses\"
  completed_at: \"2026-05-07T17:10:00+08:00\"
""",
        encoding="utf-8",
    )

    _ = upsert_issue_state(tmp_path,
    issue_number="42",
    state="running",
    command_id="cmd-running",
    updated_at="2026-05-07T17:00:00+08:00", current_session_id="ses-root-42", )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="issue_worker",
        stage="issue_worker_execution",
        status="queued",
        updated_at="2026-05-07T17:00:00+08:00",
        automation_flags={
            "continueWithoutHuman": True,
            "queueNextSessionOnIdle": True,
        },
        artifact_refs={"worker_result_ref": "docs/agents/worker-results/issue-42.yaml"},
        artifact_status={
            "worker_result": {
                "path": "docs/agents/worker-results/issue-42.yaml",
                "observed_at": "2026-05-07T17:10:00+08:00",
                "parse_ok": True,
                "status": "success",
                "pr_number": "77",
                "completed_at": "2026-05-07T17:10:00+08:00",
            }
        },
    )
    monitor_log_path = tmp_path / ".opencode/runtime/monitor.log"

    events, exit_code = orchestrator_monitor.run_monitor_cycle(
        base_dir=tmp_path,
        monitor_log_path=monitor_log_path,
        now="2026-05-07T17:11:00+08:00",
        heartbeat_timeout_seconds=900,
        selection_timeout_seconds=300,
    )

    rule_ids = [str(event["rule_id"]) for event in events]
    runtime_issue = read_issue(tmp_path, "42")

    assert exit_code == 0
    assert rule_ids == ["RUNTIME_HEALTHY"]
    assert runtime_issue is not None
    assert str(runtime_issue.get("current_role") or "") == "issue_worker"


def test_collect_monitor_events_reports_stale_queued_pr_verifier_heartbeat(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text("42"), encoding="utf-8")
    issue_packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    cast(dict[str, object], ledger["current"]).update({"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"})
    cast(dict[str, object], ledger["artifacts"]).update({"evidence_packet_ref": "docs/agents/evidence/issue-42-pr-77.yaml"})
    _ = upsert_issue_state(tmp_path,
    issue_number="42",
    state="verifying",
    command_id="cmd-verifying",
    updated_at="2026-05-07T17:02:00+08:00", current_session_id="ses-root-42", )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="pr_verifier",
        stage="pr_verifier_execution",
        status="queued",
        updated_at="2026-05-07T17:02:00+08:00",
    )

    events = collect_monitor_events(
        base_dir=tmp_path,
        now="2026-05-07T17:20:01+08:00",
        heartbeat_timeout_seconds=900,
    )

    assert any(event["rule_id"] == "ROOT_HEARTBEAT_STALLED" for event in events)


def test_run_monitor_cycle_reports_stalled_pr_verifier_without_auto_redispatch(tmp_path: Path) -> None:
    _ = upsert_issue_state(tmp_path,
    issue_number="42",
    state="verifying",
    command_id="cmd-verifying",
    updated_at="2026-05-07T17:00:00+08:00", current_session_id="ses-old-root", )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="pr_verifier",
        stage="pr_verifier_execution",
        status="queued",
        updated_at="2026-05-07T17:00:00+08:00",
        automation_flags={
            "continueWithoutHuman": True,
            "queueNextSessionOnIdle": True,
        },
        artifact_refs={"evidence_packet_ref": "docs/agents/evidence/issue-42-pr-77.yaml"},
    )
    monitor_log_path = tmp_path / ".opencode/runtime/monitor.log"

    events, exit_code = orchestrator_monitor.run_monitor_cycle(
        base_dir=tmp_path,
        monitor_log_path=monitor_log_path,
        now="2026-05-07T17:20:01+08:00",
        heartbeat_timeout_seconds=900,
        selection_timeout_seconds=300,
    )

    rule_ids = [str(event["rule_id"]) for event in events]
    runtime_issue = read_issue(tmp_path, "42")

    assert exit_code == 1
    assert "ROOT_HEARTBEAT_STALLED" in rule_ids
    assert runtime_issue is not None
    assert str(runtime_issue.get("state") or "") == "verifying"


def test_run_monitor_watch_appends_multiple_cycles_without_sleeping(tmp_path: Path, monkeypatch) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text("42"), encoding="utf-8")
    issue_packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    _ = upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        status="queued",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    monitor_log_path = tmp_path / ".opencode/runtime/monitor.log"
    sleep_calls: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(orchestrator_monitor.time, "sleep", fake_sleep)

    exit_code = run_monitor_watch(
        base_dir=tmp_path,
        monitor_log_path=monitor_log_path,
        now="2026-05-07T17:00:30+08:00",
        heartbeat_timeout_seconds=900,
        selection_timeout_seconds=300,
        interval_seconds=0.5,
        iterations=2,
        stop_on_critical=False,
    )

    log_lines = monitor_log_path.read_text(encoding="utf-8").strip().splitlines()

    assert exit_code == 0
    assert len(log_lines) == 2
    assert sleep_calls == [0.5]


def test_watch_mode_stops_early_on_critical_issue(tmp_path: Path, monkeypatch) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(_issue_packet_text("42"), encoding="utf-8")
    issue_packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path), updated_at="2026-05-07T17:00:00+08:00")
    cast(dict[str, object], ledger["current"]).update({"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"})
    ledger["updatedAt"] = "2026-05-07T17:00:00+08:00"
    _ = upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="completed",
        command_id="cmd-complete",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    _sync_runtime_phase(
        tmp_path,
        "42",
        role="main_orchestrator",
        stage="issue_selection_or_recovery",
        status="queued",
        updated_at="2026-05-07T17:00:00+08:00",
        automation_flags={
            "continueWithoutHuman": True,
            "queueNextSessionOnIdle": True,
        },
    )
    _seed_ready_issue(tmp_path, "43", updated_at="2026-05-07T17:01:00+08:00")
    monitor_log_path = tmp_path / ".opencode/runtime/monitor.log"
    sleep_calls: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(orchestrator_monitor.time, "sleep", fake_sleep)

    exit_code = run_monitor_watch(
        base_dir=tmp_path,
        monitor_log_path=monitor_log_path,
        now="2026-05-07T17:10:30+08:00",
        heartbeat_timeout_seconds=900,
        selection_timeout_seconds=300,
        interval_seconds=0.5,
        iterations=3,
        stop_on_critical=True,
    )

    log_lines = monitor_log_path.read_text(encoding="utf-8").strip().splitlines()

    assert exit_code == 1
    assert len(log_lines) >= 1
    assert sleep_calls == []
