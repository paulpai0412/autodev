from __future__ import annotations

import io
import json
import sqlite3
import subprocess
from pathlib import Path
from subprocess import CompletedProcess
from typing import cast
from contextlib import redirect_stdout
from unittest.mock import patch

import pytest
from pytest import CaptureFixture
import scripts.orchestrator_supervisor as orchestrator_supervisor
from scripts.control_plane_db import read_github_sync_attempt, read_issue, read_latest_github_sync_attempt, read_latest_issue_history, read_latest_ref
from scripts.host_adapter import SessionOutcome, SessionStartContext, SessionStartResult


def _artifact_status(issue: dict[str, object] | None) -> dict[str, object]:
    assert issue is not None
    raw = str(issue.get("artifact_status_json") or "{}")
    payload = json.loads(raw)
    return payload if isinstance(payload, dict) else {}


def _submit_artifact(
    tmp_path: Path,
    *,
    issue_number: str,
    artifact_kind: str,
    payload: dict[str, object],
    updated_at: str,
    body_text: str = "",
) -> None:
    orchestrator_supervisor.ensure_issue_row(tmp_path, issue_number=issue_number, updated_at=updated_at)
    normalized_payload = dict(payload)
    if artifact_kind == "worker_result" and not str(normalized_payload.get("branch") or "").strip():
        issue = orchestrator_supervisor.read_issue(tmp_path, issue_number) or {}
        normalized_payload["branch"] = str(issue.get("branch") or f"agent/issue-{issue_number}-demo")
    if artifact_kind == "evidence_packet" and str(normalized_payload.get("status") or "").lower() == "pass":
        normalized_payload.setdefault(
            "gates",
            {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": "test fixture surface QA evidence",
                }
            },
        )
    orchestrator_supervisor.submit_artifact(
        base_dir=tmp_path,
        issue_number=issue_number,
        artifact_kind=artifact_kind,
        payload=normalized_payload,
        body_text=body_text,
        updated_at=updated_at,
    )


def _ingest_issue_packet_text(tmp_path: Path, issue_number: str, text: str) -> None:
    packet = parse_issue_packet_text(text, f"docs/agents/issue-packets/issue-{issue_number}.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, packet, updated_at="2026-05-07T17:00:00+08:00")


def _seed_db_issue_from_ledger(tmp_path: Path, ledger: dict[str, object], *, updated_at: str) -> None:
    issue = cast(dict[str, str], ledger.get("issue", {}))
    issue_number = str(issue.get("number") or "")
    assert issue_number
    orchestrator_supervisor.ensure_issue_row(tmp_path, issue_number=issue_number, updated_at=updated_at)
    orchestrator_supervisor._sync_runtime_phase_metadata(
        base_dir=tmp_path,
        issue_number=issue_number,
        current=cast(dict[str, str], ledger.get("current", {})),
        attempts=cast(dict[str, int], ledger.get("attempts", {})),
        limits=cast(dict[str, int], ledger.get("limits", {})),
        last_failure=cast(dict[str, object], ledger.get("lastFailure", {})),
        workflow=cast(dict[str, object], ledger.get("workflow", {})),
        automation=cast(dict[str, object], ledger.get("automation", {})),
        artifacts=cast(dict[str, object], ledger.get("artifacts", {})),
        queued_next_issue=cast(dict[str, object] | None, ledger.get("queuedNextIssue")),
        updated_at=updated_at,
    )
    orchestrator_supervisor._sync_runtime_phase_to_control_plane_state(
        base_dir=tmp_path,
        issue_number=issue_number,
        ledger=cast(dict[str, object], ledger),
        current=cast(dict[str, str], ledger.get("current", {})),
        updated_at=updated_at,
    )


def _record_db_dispatch_request(tmp_path: Path, request: object, *, created_at: str) -> None:
    orchestrator_supervisor._record_dispatch_request_history(
        base_dir=tmp_path,
        request=cast(orchestrator_supervisor.SessionRequest, cast(object, request)),
        created_at=created_at,
    )

from scripts.orchestrator_supervisor import (
    build_session_request,
    build_orchestrator_request,
    create_initial_ledger,
    parse_issue_packet_text,
    reconcile_ledger,
    run_issue_packet_intake,
    select_issue_candidates_for_capacity,
    select_next_issue_packet,
    select_next_issue_candidate,
    validate_session_request_for_dispatch,
)


class FakePopen:
    def __init__(self, stdout: str, stderr: str = "", *, returncode: int | None = None):
        self.stdout = io.StringIO(stdout)
        self.stderr = io.StringIO(stderr)
        self._returncode = returncode
        self.terminated = False

    def poll(self) -> int | None:
        return self._returncode

    def terminate(self) -> None:
        self.terminated = True
        if self._returncode is None:
            self._returncode = -15


class FakeHostAdapter:
    def __init__(self, start_result: SessionStartResult):
        self.start_result = start_result
        self.start_calls: list[SessionStartContext] = []
        self.child_calls: list[tuple[str, SessionStartContext]] = []

    def start_root_session(self, context: SessionStartContext):
        self.start_calls.append(context)
        return self.start_result

    def start_child_role(self, role: str, context: SessionStartContext):
        self.child_calls.append((role, context))
        metadata = dict(self.start_result.metadata)
        metadata.setdefault("executionMode", "foreground_child_role")
        metadata.setdefault("childRole", role)
        return SessionStartResult(
            status=self.start_result.status,
            session_id=self.start_result.session_id,
            launch_title=self.start_result.launch_title,
            error=self.start_result.error,
            resume_hint=self.start_result.resume_hint,
            resume_command=self.start_result.resume_command,
            readability_status=self.start_result.readability_status,
            metadata=metadata,
        )

    def read_session_outcome(self, runtime_session_id: str) -> SessionOutcome | None:
        del runtime_session_id
        return None

    def resume_link(self, runtime_session_id: str) -> str:
        return f"resume://{runtime_session_id}"

    def operator_entrypoints(self) -> dict[str, str]:
        return {}

    def capabilities(self) -> dict[str, object]:
        return {}


def successful_host_adapter(
    *,
    session_id: str,
    resume_command: str | None = None,
    resume_hint: str = "resume in host",
    readability_status: str = "verified_same_repo_probe",
    metadata: dict[str, object] | None = None,
) -> FakeHostAdapter:
    return FakeHostAdapter(
        SessionStartResult(
            status="success",
            session_id=session_id,
            resume_hint=resume_hint,
            resume_command=resume_command or f"opencode --session {session_id}",
            readability_status=readability_status,
            tui_resume_command="/sessions",
            stop_continuation_status="root_session_detached",
            stop_continuation_attempts=0,
            metadata=metadata
            or {"tuiResumeCommand": "/sessions", "stopContinuationStatus": "root_session_detached", "stopContinuationAttempts": 0},
        )
    )


SAMPLE_ISSUE_PACKET = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"scripts\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"
"""


def test_reconcile_bootstrap_queues_issue_worker(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:05:00+08:00",)
    current = cast(dict[str, object], updated_ledger["current"])

    assert current["role"] == "issue_worker"
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "issue_worker"
    assert "issue_worker subagent" in cast(str, decision.get("subagent_prompt", ""))
    assert request is None


def test_parse_issue_packet_text_marks_missing_issue_url_as_local_seeded() -> None:
    packet = parse_issue_packet_text(
        """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"31\"
  title: \"Local seeded issue\"
  labels: [ready-for-agent]
  parent: {type: \"github-issue\", reference: \"none\"}

branch: {name: \"agent/issue-31-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"scripts\"]
  prior_handoff: \"none\"
""",
        "docs/agents/issue-packets/issue-31.yaml",
    )

    assert packet.backing_type == "local_seeded"


def test_create_initial_ledger_persists_issue_backing_type() -> None:
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    issue = cast(dict[str, object], ledger["issue"])

    assert issue["backingType"] == "github"


def test_reconcile_worker_success_queues_pr_verifier(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "42"\n  branch: "agent/issue-42-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed: []\n  in_progress: []\n  next: []\n  blockers:\n    - "none"\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-42.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-41.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path),
    updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    cast(dict[str, int], ledger["attempts"])["issue_worker"] = 1

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
      evidence_ref: \"local\"
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
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "next_recommended_step": "Spawn verifier",
            "failure_kind": "none",
            "retryable": True,
            "completed_at": "2026-05-07T17:10:00+08:00",
            "worker_session_id": "ses",
        },
        updated_at="2026-05-07T17:10:00+08:00",
        body_text=worker_result_path.read_text(encoding="utf-8"),
    )
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = str(worker_result_path.relative_to(tmp_path))

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:11:00+08:00",)
    current = cast(dict[str, object], updated_ledger["current"])
    artifacts = cast(dict[str, object], updated_ledger["artifacts"])

    assert current["role"] == "pr_verifier"
    assert artifacts["evidence_packet_ref"] == ""
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert "pr_verifier subagent" in cast(str, decision.get("subagent_prompt", ""))
    assert request is None
    issue = read_issue(tmp_path, "42")
    latest_pr_opened = read_latest_issue_history(tmp_path, "42", entry_type="pr_opened")
    latest_sync = read_latest_github_sync_attempt(tmp_path, "42")
    artifact_status = _artifact_status(issue)
    assert cast(dict[str, object], artifact_status["worker_result"])["parse_ok"] is True
    assert cast(dict[str, object], artifact_status["worker_result"])["status"] == "success"
    assert cast(dict[str, object], artifact_status["worker_result"])["pr_number"] == "77"
    assert latest_pr_opened is not None
    assert latest_pr_opened["status"] == "opened"
    opened_payload = json.loads(str(latest_pr_opened["payload_json"] or "{}"))
    assert opened_payload["source_artifact"] == "worker_result"
    assert latest_sync is not None
    assert latest_sync["projection_target"] == "labels"
    assert latest_sync["command_id"] == "reconcile:42:issue-worker-pr-opened:labels"


def test_reconcile_worker_success_accepts_nested_pr_payload(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path),
    updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    cast(dict[str, int], ledger["attempts"])["issue_worker"] = 1

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr": {"number": 77, "url": "https://example/pr/77"},
            "next_recommended_step": "Spawn verifier",
            "failure_kind": "none",
            "retryable": True,
            "worker_session_id": "ses",
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:11:00+08:00",)
    issue = read_issue(tmp_path, "42")
    artifact_status = _artifact_status(issue)

    assert cast(dict[str, object], updated_ledger["current"])["role"] == "pr_verifier"
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert request is None
    assert cast(dict[str, object], artifact_status["worker_result"])["pr_number"] == "77"
    assert cast(dict[str, object], artifact_status["worker_result"])["pr_url"] == "https://example/pr/77"


def test_reconcile_stale_bootstrap_with_existing_worker_artifact_self_heals_to_issue_worker(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path),
    updated_at="2026-05-07T17:00:00+08:00",)

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
      evidence_ref: \"local\"
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
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "next_recommended_step": "Spawn verifier",
            "failure_kind": "none",
            "retryable": True,
            "completed_at": "2026-05-07T17:10:00+08:00",
            "worker_session_id": "ses",
        },
        updated_at="2026-05-07T17:10:00+08:00",
        body_text=worker_result_path.read_text(encoding="utf-8"),
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:11:00+08:00",)

    current = cast(dict[str, object], updated_ledger["current"])

    assert current["role"] == "pr_verifier"
    assert current["stage"] == "pr_verifier_execution"
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert request is None


def test_reconcile_worker_success_requires_persisted_worker_fact(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    cast(dict[str, int], ledger["attempts"])["issue_worker"] = 1

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
      evidence_ref: \"local\"
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
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "",
            "next_recommended_step": "Spawn verifier",
            "failure_kind": "none",
            "retryable": True,
            "completed_at": "2026-05-07T17:10:00+08:00",
            "worker_session_id": "ses",
        },
        updated_at="2026-05-07T17:10:00+08:00",
        body_text=worker_result_path.read_text(encoding="utf-8"),
    )
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = str(worker_result_path.relative_to(tmp_path))

    with patch("scripts.orchestrator_supervisor._read_db_artifact_fact", return_value={}):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",)

    assert updated_ledger is not None
    assert decision["action"] == "no_change"
    assert request is None
    assert cast(dict[str, object], updated_ledger["current"])["role"] == "issue_worker"
    assert cast(dict[str, object], updated_ledger["current"])["status"] == "queued"


def test_reconcile_worker_success_requires_canonical_primary_workspace_artifact(tmp_path: Path):
    primary_root = tmp_path / "primary-workspace"
    worktree_root = tmp_path / "worker-worktree"
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        primary_workspace_root=str(primary_root),
        updated_at="2026-05-07T17:00:00+08:00",
    )
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "done"}
    cast(dict[str, int], ledger["attempts"])["issue_worker"] = 1

    worktree_result_path = worktree_root / "docs/agents/worker-results/issue-42.yaml"
    worktree_result_path.parent.mkdir(parents=True, exist_ok=True)
    worktree_result_path.write_text(
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
      evidence_ref: \"local\"
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
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=worktree_root,
    updated_at="2026-05-07T17:11:00+08:00",)

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "issue_worker"
    assert request is None
    assert cast(dict[str, object], updated_ledger["lastFailure"])["kind"] == "contract_invalid"
    assert cast(dict[str, object], updated_ledger["lastFailure"])["owner_role"] == "issue_worker"
    assert cast(dict[str, object], updated_ledger["lastFailure"])["artifact_kind"] == "worker_result"
    assert "ended without recording a worker_result in SQLite" in cast(str, decision["summary"])


def test_reconcile_worker_success_without_pr_number_queues_pr_verifier(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    cast(dict[str, int], ledger["attempts"])["issue_worker"] = 1

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
      evidence_ref: \"local\"
      summary: \"ok\"
  final_acceptance_claim: false
evidence_packet_refs:
  worker_artifact_bundle: \"\"
  verifier_packet: \"\"
  raw_evidence_policy: \"stored_outside_main_agent_context\"
role_boundary:
  actor_role: \"issue_worker\"
  may_execute_implementation_self_checks: true
  may_execute_final_acceptance_qa: false
  may_emit_final_verification: false
  verifier_packet_required_for_completion: true
pr:
  number: \"\"
  url: \"\"
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
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "",
            "next_recommended_step": "Spawn verifier",
            "failure_kind": "none",
            "retryable": True,
            "completed_at": "2026-05-07T17:10:00+08:00",
            "worker_session_id": "ses",
        },
        updated_at="2026-05-07T17:10:00+08:00",
        body_text=worker_result_path.read_text(encoding="utf-8"),
    )
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = str(worker_result_path.relative_to(tmp_path))

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:11:00+08:00",)

    issue = read_issue(tmp_path, "42")
    artifact_status = _artifact_status(issue)

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert request is None
    assert cast(dict[str, object], updated_ledger["lastFailure"])["kind"] == "none"
    assert "verify the pushed branch" in cast(str, decision["summary"])
    assert cast(dict[str, object], artifact_status["worker_result"])["parse_ok"] is True
    assert cast(dict[str, object], artifact_status["worker_result"])["pr_number"] == ""


def test_reconcile_worker_success_refreshes_running_heartbeat_before_stale_quarantine(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    cast(dict[str, int], ledger["attempts"])["issue_worker"] = 1
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="running",
    command_id="cmd-running",
    updated_at="2026-05-07T17:00:00+08:00", current_session_id="ses-root-42", )
    connection = sqlite3.connect(tmp_path / ".opencode/runtime/control-plane.sqlite3")
    try:
        _ = connection.execute(
            "UPDATE issues SET last_event_at = ? WHERE issue_number = ?",
            ("2026-05-07T17:00:00+08:00", "42"),
        )
        connection.commit()
    finally:
        connection.close()

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
      evidence_ref: \"local\"
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
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "next_recommended_step": "Spawn verifier",
            "failure_kind": "none",
            "retryable": True,
            "completed_at": "2026-05-07T17:10:00+08:00",
            "worker_session_id": "ses",
        },
        updated_at="2026-05-07T17:10:00+08:00",
        body_text=worker_result_path.read_text(encoding="utf-8"),
    )
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = str(worker_result_path.relative_to(tmp_path))

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:16:00+08:00",)

    issue = read_issue(tmp_path, "42")
    current = cast(dict[str, object], updated_ledger["current"])

    assert current["role"] == "pr_verifier"
    assert decision["action"] == "delegate_subagent"
    assert request is None
    assert issue is not None
    assert issue["state"] == "running"
    assert issue["last_event_at"] == "2026-05-07T17:10:00+08:00"


def test_reconcile_stale_worker_artifact_does_not_refresh_heartbeat_before_quarantine(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    cast(dict[str, int], ledger["attempts"])["issue_worker"] = 2
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="running",
    command_id="cmd-running",
    updated_at="2026-05-07T17:20:00+08:00", current_session_id="ses-root-42", )

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
      evidence_ref: \"local\"
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
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = str(worker_result_path.relative_to(tmp_path))

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:36:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is ledger
    assert decision["action"] == "hold_quarantined_issue"
    assert request is None
    assert issue is not None
    assert issue["state"] == "quarantined"
    assert issue["last_event_at"] == "2026-05-07T17:36:00+08:00"


def test_reconcile_keeps_queued_issue_worker_without_result_unchanged(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    attempts = cast(dict[str, int], ledger["attempts"])
    attempts["issue_worker"] = 1

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:12:00+08:00",)
    current = cast(dict[str, object], updated_ledger["current"])

    assert current == {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    assert attempts["issue_worker"] == 1
    assert decision["action"] == "no_change"
    assert request is None


@pytest.mark.parametrize("runtime_state", ["dispatching", "running"])
def test_reconcile_active_runtime_issue_without_current_role_stage_does_not_request_start_issue(tmp_path: Path, runtime_state: str):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "", "stage": "", "status": ""}

    orchestrator_supervisor.ensure_issue_row(tmp_path, issue_number="42", updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state=runtime_state,
        command_id=f"cmd-{runtime_state}",
        updated_at="2026-05-07T17:04:00+08:00",
        current_session_id="ses-root-42",
    )

    connection = sqlite3.connect(tmp_path / ".opencode/runtime/control-plane.sqlite3")
    try:
        _ = connection.execute(
            "UPDATE issues SET current_role = '', current_stage = '', current_status = '' WHERE issue_number = ?",
            ("42",),
        )
        connection.commit()
    finally:
        connection.close()

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:05:00+08:00",
    )

    current = cast(dict[str, object], updated_ledger["current"])

    assert current == {"role": "", "stage": "", "status": ""}
    assert decision["action"] == "no_change"
    assert decision["request_title"] == ""
    assert f"state={runtime_state}" in str(decision["summary"])
    assert "start-issue" not in str(decision["summary"])
    assert request is None


def test_build_orchestrator_request_includes_nonce_generation_and_ledger_revision():
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    request = build_orchestrator_request(ledger)

    assert request["requestGeneration"] == 2
    assert request["nonce"]
    assert request["requestID"] == request["nonce"]
    assert request["agent"] == "build"
    assert request["createdForLedgerRevision"] == ledger["ledgerRevision"]


def test_build_orchestrator_request_uses_ledger_root_session_agent_override():
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, root_session_agent="build", updated_at="2026-05-07T17:00:00+08:00")

    request = build_orchestrator_request(ledger)

    assert request["agent"] == "build"


def test_build_orchestrator_request_uses_ledger_shared_doc_paths():
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    cast(dict[str, object], ledger["automation"])["supervisorDocPath"] = "/shared/docs/agents/runtime/nonstop-supervisor-loop.md"
    cast(dict[str, object], ledger["workflow"])["workflowPolicyPath"] = "/shared/docs/agents/autonomous-development-workflow.yaml"
    cast(dict[str, object], ledger["workflow"])["releaseResultTemplatePath"] = "/shared/docs/agents/release-result-template.yaml"

    request = build_orchestrator_request(ledger)

    assert "/shared/docs/agents/runtime/nonstop-supervisor-loop.md" in request["prompt"]
    assert "/shared/docs/agents/autonomous-development-workflow.yaml" in request["prompt"]
    assert "Bootstrap from the SQLite-backed control plane" in request["prompt"]
    assert "Use the DB-backed supervisor reconcile flow before the first issue_worker launch" in request["prompt"]


def test_build_orchestrator_request_requires_foreground_child_subagents():
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    request = build_orchestrator_request(ledger)

    assert 'task(subagent_type="general", ..., run_in_background=false)' in request["prompt"]
    assert "Wait for each child task call to finish in the foreground before continuing." in request["prompt"]
    assert "Do not include karpathy-guidelines in load_skills for child subagents" not in request["prompt"]


def test_build_orchestrator_request_includes_base_branch_plan():
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        base_branch="agent/issue-41-parent",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    request = build_orchestrator_request(ledger)

    assert request["branch"] == "agent/issue-42-demo"
    assert request.get("baseBranch") == "agent/issue-41-parent"
    assert "target branch agent/issue-42-demo from base branch agent/issue-41-parent" in request["prompt"]


def test_start_issue_records_db_backed_dispatch_result(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_test", resume_command="opencode --session ses_root_test"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""), patch(
        "scripts.orchestrator_supervisor._sync_issue_body_projection",
        return_value="",
    ) as sync_body_projection:
        result = orchestrator_supervisor.start_issue(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="autodev-start",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    latest = orchestrator_supervisor.read_latest_dispatch_result(tmp_path, issue_number="42")
    runtime_transition = read_latest_issue_history(tmp_path, "42", entry_type="runtime_transition")
    stack_base = read_latest_issue_history(tmp_path, "42", entry_type="stack_base_resolved")

    assert result.get("status") == "success"
    assert result.get("rootSessionID") == "ses_root_test"
    assert latest is not None
    assert latest.get("rootSessionID") == "ses_root_test"
    assert runtime_transition is not None
    assert runtime_transition["role"] == "issue_worker"
    assert runtime_transition["stage"] == "issue_worker_execution"
    assert runtime_transition["body_text"] == "main_orchestrator/orchestrator_bootstrap -> issue_worker/issue_worker_execution"
    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_role"] == "issue_worker"
    assert issue["current_stage"] == "issue_worker_execution"
    assert issue["current_session_id"] == "ses_root_test"
    assert stack_base is not None
    assert '"base_branch": "main"' in str(stack_base["payload_json"])
    runtime_context = orchestrator_supervisor.read_runtime_context(tmp_path, "42")
    assert runtime_context.get("issue_worktree_path") == str(tmp_path)
    assert sync_body_projection.call_count == 2


def test_start_issue_body_projection_failure_does_not_block_dispatch(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_test", resume_command="opencode --session ses_root_test"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""), patch(
        "scripts.orchestrator_supervisor._sync_issue_body_projection",
        side_effect=["body projection failed at dispatch", ""],
    ):
        result = orchestrator_supervisor.start_issue(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="autodev-start",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    latest_admin = read_latest_issue_history(tmp_path, "42", entry_type="admin_action")
    issue = read_issue(tmp_path, "42")
    assert result.get("status") == "success"
    assert issue is not None
    assert issue["state"] == "running"
    assert latest_admin is not None
    payload = json.loads(str(latest_admin["payload_json"] or "{}"))
    assert payload["decision_type"] == "admin_github_projection_failure"
    assert "issue_body" in str(latest_admin["summary"])


def test_start_issue_projection_bundle_runs_comment_and_project_sync(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_test", resume_command="opencode --session ses_root_test"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""), patch(
        "scripts.orchestrator_supervisor._sync_issue_body_projection",
        return_value="",
    ) as sync_body_projection, patch(
        "scripts.orchestrator_supervisor._sync_issue_status_comment",
        return_value="",
    ) as sync_status_comment, patch(
        "scripts.orchestrator_supervisor._sync_project_fields_projection",
        return_value="",
    ) as sync_project_fields:
        result = orchestrator_supervisor.start_issue(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="autodev-start",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "success"
    assert sync_body_projection.call_count == 2
    assert sync_status_comment.call_count == 2
    assert sync_project_fields.call_count == 2


def test_sync_project_fields_projection_includes_pr_workflow_status(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:01:00+08:00",
        runtime_context={
            "github_project_id": "PVT_project_1",
            "github_project_field_ids": {
                "state": "PVTF_state",
                "stage": "PVTF_stage",
                "pr_workflow": "PVTF_pr_workflow",
            },
        },
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "88",
        },
        updated_at="2026-05-07T17:02:00+08:00",
    )

    captured_fields: dict[str, str] = {}

    def fake_sync_fields(*, fields: dict[str, str], **_kwargs: object) -> str:
        captured_fields.update(fields)
        return ""

    with patch("scripts.orchestrator_supervisor._read_project_github_repo", return_value="example/repo"), patch(
        "scripts.orchestrator_supervisor._lifecycle_helpers.sync_project_fields_projection",
        side_effect=fake_sync_fields,
    ):
        error = orchestrator_supervisor._sync_project_fields_projection(
            base_dir=tmp_path,
            issue_number="42",
            command_id="cmd-project-sync",
            updated_at="2026-05-07T17:03:00+08:00",
        )

    assert error == ""
    assert captured_fields["PVTF_state"] == "running"
    assert captured_fields["PVTF_stage"] == ""
    assert captured_fields["PVTF_pr_workflow"] == "verifier_passed"


def test_sync_project_fields_projection_sets_release_pending_pr_workflow_to_verifier_passed(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:01:00+08:00",
        runtime_context={
            "github_project_id": "PVT_project_1",
            "github_project_field_ids": {
                "state": "PVTF_state",
                "stage": "PVTF_stage",
                "pr_workflow": "PVTF_pr_workflow",
            },
        },
    )

    captured_fields: dict[str, str] = {}

    def fake_sync_fields(*, fields: dict[str, str], **_kwargs: object) -> str:
        captured_fields.update(fields)
        return ""

    with patch("scripts.orchestrator_supervisor._read_project_github_repo", return_value="example/repo"), patch(
        "scripts.orchestrator_supervisor._lifecycle_helpers.sync_project_fields_projection",
        side_effect=fake_sync_fields,
    ):
        error = orchestrator_supervisor._sync_project_fields_projection(
            base_dir=tmp_path,
            issue_number="42",
            command_id="cmd-project-sync",
            updated_at="2026-05-07T17:03:00+08:00",
        )

    assert error == ""
    assert captured_fields["PVTF_state"] == "release_pending"
    assert captured_fields["PVTF_pr_workflow"] == "verifier_passed"


def test_sync_projected_issue_labels_uses_autodev_yaml_override_for_release_pending(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    config_path = tmp_path / ".autodev.yaml"
    _ = config_path.write_text(
        '\n'.join(
            [
                'schema_version: "1.0"',
                'project:',
                '  name: demo',
                '  root: /tmp/demo',
                '  github_repo: example/repo',
                'state_projection:',
                '  sqlite_to_primary_label:',
                '    release_pending: agent-in-review',
                '',
            ]
        ),
        encoding="utf-8",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value="") as sync_labels:
        error = orchestrator_supervisor._sync_projected_issue_labels(
            base_dir=tmp_path,
            issue_number="42",
            command_id="cmd-project-sync",
            updated_at="2026-05-07T17:03:00+08:00",
        )

    assert error == ""
    sync_kwargs = sync_labels.call_args.kwargs
    assert sync_kwargs["add_labels"] == ["agent-in-review", "pr-verifier-passed"]


def test_inspect_project_pr_workflow_reflects_merged_release(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "merge": {"merged": True, "merged_sha": "abc123"},
            "pr_number": "88",
        },
        updated_at="2026-05-07T17:00:30+08:00",
    )

    payload = orchestrator_supervisor.inspect_control_plane(base_dir=tmp_path, issue_number="42")
    pr_workflow = cast(dict[str, object], payload["projectPrWorkflow"])

    assert pr_workflow["status"] == "merged"
    assert pr_workflow["prNumber"] == "88"


def test_inspect_control_plane_projects_failure_context_and_recovery_cursor(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    ledger["lastFailure"] = {
        "kind": "browser_e2e_unavailable",
        "summary": "Retry verifier from failed recovery.",
        "retryable": True,
        "owner_role": "pr_verifier",
        "owner_stage": "pr_verifier_execution",
        "owner_state": "verifying",
        "artifact_kind": "evidence_packet",
        "failure_class": "verifier",
        "recovery_cursor": {
            "resume_role": "pr_verifier",
            "resume_stage": "pr_verifier_execution",
            "resume_state": "verifying",
            "resume_strategy": "retry_same_role",
        },
    }
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:05:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:06:00+08:00",
        current_session_id="",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:07:00+08:00",
        current_role="main_orchestrator",
        current_stage="issue_selection_or_recovery",
        current_status="queued",
        last_failure=cast(dict[str, object], ledger["lastFailure"]),
        runtime_context={
            "failure_context": cast(dict[str, object], ledger["lastFailure"]),
            "recovery_cursor": cast(dict[str, object], cast(dict[str, object], ledger["lastFailure"]).get("recovery_cursor", {})),
        },
    )

    payload = orchestrator_supervisor.inspect_control_plane(base_dir=tmp_path, issue_number="42")

    failure_context = cast(dict[str, object], payload["failureContext"])
    recovery_cursor = cast(dict[str, object], payload["recoveryCursor"])
    assert failure_context["owner_role"] == "pr_verifier"
    assert failure_context["artifact_kind"] == "evidence_packet"
    assert recovery_cursor["resume_role"] == "pr_verifier"
    assert recovery_cursor["resume_stage"] == "pr_verifier_execution"


def test_start_issue_succeeds_without_legacy_runtime_files(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    assert not (tmp_path / ".opencode/runtime/orchestrator-ledger.json").exists()
    assert not (tmp_path / ".opencode/runtime/new-session-request.json").exists()
    assert not (tmp_path / ".opencode/runtime/new-session-result.json").exists()
    assert not (tmp_path / "docs/agents/runtime/context-checkpoint.yaml").exists()

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_test", resume_command="opencode --session ses_root_test"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        result = orchestrator_supervisor.start_issue(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="autodev-start",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    latest = orchestrator_supervisor.read_latest_dispatch_result(tmp_path, issue_number="42")

    assert result.get("status") == "success"
    assert result.get("rootSessionID") == "ses_root_test"
    assert issue is not None
    assert issue["state"] == "running"
    assert latest is not None
    assert latest.get("rootSessionID") == "ses_root_test"
    assert not (tmp_path / "docs/agents/runtime/context-checkpoint.yaml").exists()
    runtime_context = orchestrator_supervisor.read_runtime_context(tmp_path, "42")
    assert runtime_context.get("issue_worktree_path") == str(tmp_path)


def test_start_issue_creates_issue_specific_worktree_in_real_git_repo(tmp_path: Path):
    _ = subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, check=True, capture_output=True, text=True)
    _ = subprocess.run(["git", "config", "user.email", "autodev@example.com"], cwd=tmp_path, check=True, capture_output=True, text=True)
    _ = subprocess.run(["git", "config", "user.name", "autodev"], cwd=tmp_path, check=True, capture_output=True, text=True)
    readme = tmp_path / "README.md"
    readme.write_text("demo\n", encoding="utf-8")
    _ = subprocess.run(["git", "add", "README.md"], cwd=tmp_path, check=True, capture_output=True, text=True)
    _ = subprocess.run(["git", "commit", "-m", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)

    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_test", resume_command="opencode --session ses_root_test"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        result = orchestrator_supervisor.start_issue(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="autodev-start",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    worktree_path = tmp_path / ".opencode/runtime/issue-worktrees/issue-42"
    issue = read_issue(tmp_path, "42")
    runtime_context = orchestrator_supervisor.read_runtime_context(tmp_path, "42")
    exclude_path = orchestrator_supervisor._git_path(worktree_path, "info/exclude")

    assert result.get("status") == "success"
    assert worktree_path.exists()
    assert (worktree_path / ".git").exists()
    assert issue is not None
    assert issue.get("worktree_path") == str(worktree_path)
    assert runtime_context.get("issue_worktree_path") == str(worktree_path)
    assert exclude_path is not None
    exclude_text = exclude_path.read_text(encoding="utf-8")
    for pattern in orchestrator_supervisor.WORKTREE_LOCAL_EXCLUDE_PATTERNS:
        assert pattern in exclude_text


def test_ensure_issue_worktree_backfills_local_exclude_for_existing_worktree(tmp_path: Path):
    _ = subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, check=True, capture_output=True, text=True)
    _ = subprocess.run(["git", "config", "user.email", "autodev@example.com"], cwd=tmp_path, check=True, capture_output=True, text=True)
    _ = subprocess.run(["git", "config", "user.name", "autodev"], cwd=tmp_path, check=True, capture_output=True, text=True)
    readme = tmp_path / "README.md"
    readme.write_text("demo\n", encoding="utf-8")
    _ = subprocess.run(["git", "add", "README.md"], cwd=tmp_path, check=True, capture_output=True, text=True)
    _ = subprocess.run(["git", "commit", "-m", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)

    worktree_path = tmp_path / ".opencode/runtime/issue-worktrees/issue-42"
    _ = subprocess.run(
        ["git", "worktree", "add", "-b", "agent/issue-42-demo", str(worktree_path), "main"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    exclude_path = orchestrator_supervisor._git_path(worktree_path, "info/exclude")
    assert exclude_path is not None
    exclude_path.write_text("", encoding="utf-8")

    resolved = orchestrator_supervisor._ensure_issue_worktree(
        base_dir=tmp_path,
        issue_number="42",
        branch="agent/issue-42-demo",
        base_branch="main",
        updated_at="2026-05-07T17:10:00+08:00",
    )

    assert resolved == worktree_path
    exclude_text = exclude_path.read_text(encoding="utf-8")
    for pattern in orchestrator_supervisor.WORKTREE_LOCAL_EXCLUDE_PATTERNS:
        assert pattern in exclude_text


def test_start_issue_from_issue_worktree_uses_root_control_plane_db(tmp_path: Path):
    _ = subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, check=True, capture_output=True, text=True)
    _ = subprocess.run(["git", "config", "user.email", "autodev@example.com"], cwd=tmp_path, check=True, capture_output=True, text=True)
    _ = subprocess.run(["git", "config", "user.name", "autodev"], cwd=tmp_path, check=True, capture_output=True, text=True)
    readme = tmp_path / "README.md"
    readme.write_text("demo\n", encoding="utf-8")
    _ = subprocess.run(["git", "add", "README.md"], cwd=tmp_path, check=True, capture_output=True, text=True)
    _ = subprocess.run(["git", "commit", "-m", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)

    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    issue_worktree = tmp_path / ".opencode/runtime/issue-worktrees/issue-42"

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_test", resume_command="opencode --session ses_root_test"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        result = orchestrator_supervisor.start_issue(
            base_dir=issue_worktree,
            issue_number="42",
            source_session_id="autodev-start",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    issue_from_worktree_base = read_issue(issue_worktree, "42")
    runtime_context = orchestrator_supervisor.read_runtime_context(tmp_path, "42")

    assert result.get("status") == "success"
    assert issue is not None
    assert issue_from_worktree_base is not None
    assert issue_from_worktree_base["current_session_id"] == "ses_root_test"
    assert issue["current_session_id"] == "ses_root_test"
    assert issue["worktree_path"] == str(issue_worktree)
    assert runtime_context.get("issue_worktree_path") == str(issue_worktree)
    assert not (issue_worktree / ".opencode/runtime/control-plane.sqlite3").exists()


def test_start_issue_rejects_packet_issue_number_mismatch(tmp_path: Path):
    malicious_packet_text = SAMPLE_ISSUE_PACKET.replace('number: "42"', 'number: "../../etc/passwd"')
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(malicious_packet_text, encoding="utf-8")

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        try:
            orchestrator_supervisor.start_issue(
                base_dir=tmp_path,
                issue_number="42",
                source_session_id="autodev-start",
                updated_at="2026-05-07T17:10:00+08:00",
            )
        except RuntimeError as error:
            assert "not recorded in SQLite" in str(error)
        else:
            raise AssertionError("expected start_issue to require a DB-backed issue packet")


def test_show_latest_session_preserves_recorded_dispatch_resume_command(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_test", resume_command="opencode --session ses_root_test"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        orchestrator_supervisor.start_issue(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="autodev-start",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    payload = orchestrator_supervisor.show_latest_session(base_dir=tmp_path)

    assert payload is not None
    assert payload.get("status") == "success"
    assert payload.get("rootSessionID") == "ses_root_test"
    assert payload.get("cliOpenCommand") == "opencode --session ses_root_test"


def test_show_latest_session_returns_host_neutral_db_state_when_issue_is_running(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )

    payload = orchestrator_supervisor.show_latest_session(base_dir=tmp_path, issue_number="42")

    assert payload is not None
    assert payload.get("rootSessionID") == "ses-root-42"
    assert payload.get("issueNumber") == "42"
    assert payload.get("role") == ""
    assert payload.get("stage") == ""
    assert "cliOpenCommand" not in payload
    assert "recommendedAction" not in payload


def test_show_latest_session_hides_stale_root_dispatch_when_runtime_is_release_root_session(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        current_role="main_orchestrator",
        current_stage="release_root_execution",
        current_status="queued",
    )
    orchestrator_supervisor.append_issue_history(
        tmp_path,
        issue_number="42",
        entry_type="dispatch_result",
        created_at="2026-05-07T16:30:00+08:00",
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        status="success",
        session_id="ses-root-42",
        payload={
            "status": "success",
            "rootSessionID": "ses-root-42",
            "issueNumber": "42",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "recordedAt": "2026-05-07T16:30:00+08:00",
        },
        unique_key="dispatch-result:stale-root-42",
    )

    payload = orchestrator_supervisor.show_latest_session(base_dir=tmp_path, issue_number="42")

    assert payload is None


def test_show_latest_session_prefers_latest_session_result_for_release_root_session(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:10:00+08:00",
        current_session_id="ses-release-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:10:00+08:00",
        current_role="main_orchestrator",
        current_stage="release_root_execution",
        current_status="running",
    )
    orchestrator_supervisor.append_issue_history(
        tmp_path,
        issue_number="42",
        entry_type="dispatch_result",
        created_at="2026-05-07T16:30:00+08:00",
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        status="success",
        session_id="ses-root-42",
        payload={
            "status": "success",
            "rootSessionID": "ses-root-42",
            "issueNumber": "42",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "recordedAt": "2026-05-07T16:30:00+08:00",
        },
        unique_key="dispatch-result:stale-root-42",
    )
    orchestrator_supervisor.append_issue_history(
        tmp_path,
        issue_number="42",
        entry_type="session_result",
        created_at="2026-05-07T17:09:00+08:00",
        role="main_orchestrator",
        stage="release_root_execution",
        status="success",
        session_id="ses-release-42",
        payload={
            "status": "success",
            "rootSessionID": "ses-release-42",
            "issueNumber": "42",
            "role": "main_orchestrator",
            "stage": "release_root_execution",
            "recordedAt": "2026-05-07T17:09:00+08:00",
            "sourceSessionID": "ses-trigger-42",
        },
        unique_key="session-result:release-worker-42",
    )

    payload = orchestrator_supervisor.show_latest_session(base_dir=tmp_path, issue_number="42")

    assert payload is not None
    assert payload.get("rootSessionID") == "ses-release-42"
    assert payload.get("role") == "main_orchestrator"
    assert payload.get("stage") == "release_root_execution"


def test_show_latest_session_workspace_fallback_prefers_newer_session_result(tmp_path: Path) -> None:
    orchestrator_supervisor.append_issue_history(
        tmp_path,
        issue_number="42",
        entry_type="dispatch_result",
        created_at="2026-05-07T16:30:00+08:00",
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        status="success",
        session_id="ses-root-42",
        payload={
            "status": "success",
            "rootSessionID": "ses-root-42",
            "issueNumber": "42",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "recordedAt": "2026-05-07T16:30:00+08:00",
        },
        unique_key="dispatch-result:root-42",
    )
    orchestrator_supervisor.append_issue_history(
        tmp_path,
        issue_number="42",
        entry_type="session_result",
        created_at="2026-05-07T17:10:00+08:00",
        role="main_orchestrator",
        stage="issue_selection_or_recovery",
        status="success",
        session_id="ses-main-42",
        payload={
            "status": "success",
            "rootSessionID": "ses-main-42",
            "issueNumber": "42",
            "role": "main_orchestrator",
            "stage": "issue_selection_or_recovery",
            "recordedAt": "2026-05-07T17:10:00+08:00",
        },
        unique_key="session-result:main-42",
    )

    payload = orchestrator_supervisor.show_latest_session(base_dir=tmp_path)

    assert payload is not None
    assert payload.get("rootSessionID") == "ses-main-42"
    assert payload.get("stage") == "issue_selection_or_recovery"


def test_show_latest_session_prefers_running_issue_over_newer_quarantined_fence(tmp_path: Path) -> None:
    # Quarantined issue has a newer timestamp but should not shadow an actively
    # running issue when operators request the latest resumable session.
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="23",
        state="quarantined",
        command_id="cmd-quarantined-23",
        updated_at="2026-05-07T17:20:00+08:00",
        current_session_id="ses-quarantined-23",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="25",
        state="running",
        command_id="cmd-running-25",
        updated_at="2026-05-07T17:10:00+08:00",
        current_session_id="ses-running-25",
    )
    orchestrator_supervisor.append_issue_history(
        tmp_path,
        issue_number="23",
        entry_type="dispatch_result",
        created_at="2026-05-07T17:20:00+08:00",
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        status="success",
        session_id="ses-quarantined-23",
        payload={
            "status": "success",
            "rootSessionID": "ses-quarantined-23",
            "issueNumber": "23",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "recordedAt": "2026-05-07T17:20:00+08:00",
        },
        unique_key="dispatch-result:quarantined-23",
    )
    orchestrator_supervisor.append_issue_history(
        tmp_path,
        issue_number="25",
        entry_type="dispatch_result",
        created_at="2026-05-07T17:10:00+08:00",
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        status="success",
        session_id="ses-running-25",
        payload={
            "status": "success",
            "rootSessionID": "ses-running-25",
            "issueNumber": "25",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "recordedAt": "2026-05-07T17:10:00+08:00",
        },
        unique_key="dispatch-result:running-25",
    )

    payload = orchestrator_supervisor.show_latest_session(base_dir=tmp_path)

    assert payload is not None
    assert payload.get("issueNumber") == "25"
    assert payload.get("rootSessionID") == "ses-running-25"


def test_reconcile_workspace_reconciles_active_issues_and_starts_ready_issue_with_free_capacity(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AUTODEV_DEVELOPMENT_CAPACITY", "2")
    monkeypatch.setenv("AUTODEV_RELEASE_CAPACITY", "1")
    monkeypatch.setenv("AUTODEV_RELEASE_BACKFILL_MODE", "auto")

    for issue_number in ("41", "42", "43"):
        packet_text = (
            SAMPLE_ISSUE_PACKET.replace('"42"', f'"{issue_number}"')
            .replace('issue-42', f'issue-{issue_number}')
            .replace('Demo issue', f'Issue {issue_number}')
            .replace('agent/issue-42-demo', f'agent/issue-{issue_number}-demo')
        )
        orchestrator_supervisor._sync_issue_packet_to_db(
            tmp_path,
            parse_issue_packet_text(packet_text, f"docs/agents/issue-packets/issue-{issue_number}.yaml"),
            updated_at="2026-05-07T17:00:00+08:00",
        )

    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="41",
        state="running",
        command_id="cmd-running-41",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-41",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="41",
        updated_at="2026-05-07T17:00:00+08:00",
        current_role="issue_worker",
        current_stage="issue_worker_execution",
        current_status="queued",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="43",
        state="verified",
        command_id="cmd-verified-43",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-v-43",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="43",
        pr_number="143",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v-43",
        command_id="cmd-pr-43",
        payload={"issue_number": "43", "pr_number": "143"},
    )

    adapter = successful_host_adapter(session_id="ses-shared", resume_command="opencode --session ses-shared")
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter), patch(
        "scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""
    ), patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=True) as run_intake:
        payload = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:10:00+08:00",
            source_session_id="workspace-reconcile",
        )

    issue_42 = read_issue(tmp_path, "42")
    issue_43 = read_issue(tmp_path, "43")

    assert payload["status"] == "success"
    assert payload["intake_status"] == "success"
    assert payload["intake_error"] == ""
    assert payload["development_capacity"] == 2
    assert payload["release_capacity"] == 1
    assert payload["release_backfill_mode"] == "auto"
    assert payload["auto_release_approval_mode"] == "human_required"
    assert payload["active_issue_numbers"] == ["41", "43"]
    assert [entry["issue_number"] for entry in cast(list[dict[str, object]], payload["reconciled_issues"])] == ["41", "43"]
    run_intake.assert_called_once_with(tmp_path)
    started = cast(list[dict[str, object]], payload["started_issues"])
    assert [entry["issue_number"] for entry in started] == ["42"]
    started_releases = cast(list[dict[str, object]], payload["started_releases"])
    assert [entry["issue_number"] for entry in started_releases] == ["43"]
    assert cast(list[dict[str, object]], payload["release_backfill_errors"]) == []
    assert issue_42 is not None
    assert issue_42["state"] == "running"
    assert issue_42["current_session_id"] == "ses-shared"
    assert issue_43 is not None
    assert issue_43["state"] == "release_pending"
    assert issue_43["current_session_id"] == "ses-shared"


def test_reconcile_workspace_syncs_project_fields_for_ready_issues_after_intake(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AUTODEV_DEVELOPMENT_CAPACITY", "1")
    monkeypatch.setenv("AUTODEV_RELEASE_BACKFILL_MODE", "manual")

    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _ = orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        runtime_context={
            "github_project_id": "PVT_project_1",
            "github_project_field_ids": {
                "state": "PVTF_state",
                "stage": "PVTF_stage",
                "pr_workflow": "PVTF_pr_workflow",
            },
        },
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=True), patch(
        "scripts.orchestrator_supervisor._sync_project_fields_projection",
        return_value="",
    ) as sync_project_fields, patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses-ready-42", resume_command="opencode --session ses-ready-42"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        _ = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:10:00+08:00",
            source_session_id="workspace-reconcile",
        )

    intake_call = next(
        (
            call
            for call in sync_project_fields.call_args_list
            if str(call.kwargs.get("command_id") or "") == "intake:42:ready:project-fields"
        ),
        None,
    )
    assert intake_call is not None
    assert intake_call.kwargs["issue_number"] == "42"


def test_reconcile_workspace_records_admin_decision_when_ready_project_sync_fails(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AUTODEV_DEVELOPMENT_CAPACITY", "1")
    monkeypatch.setenv("AUTODEV_RELEASE_BACKFILL_MODE", "manual")

    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _ingest_issue_packet_text(
        tmp_path,
        "41",
        SAMPLE_ISSUE_PACKET.replace('"42"', '"41"').replace("issue-42", "issue-41").replace("agent/issue-42-demo", "agent/issue-41-demo"),
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="41",
        state="running",
        command_id="cmd-running-41",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-running-41",
    )
    _ = orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        runtime_context={
            "github_project_id": "PVT_project_1",
            "github_project_field_ids": {
                "state": "PVTF_state",
                "stage": "PVTF_stage",
                "pr_workflow": "PVTF_pr_workflow",
            },
        },
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=True), patch(
        "scripts.orchestrator_supervisor._sync_project_fields_projection",
        return_value="project sync failed",
    ), patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses-ready-42", resume_command="opencode --session ses-ready-42"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        _ = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:10:00+08:00",
            source_session_id="workspace-reconcile",
        )

    latest_decision = orchestrator_supervisor.read_latest_decision(tmp_path, "42")
    assert latest_decision is not None
    assert latest_decision["decision_type"] == "admin_github_projection_failure"
    assert "after intake for issue #42" in str(latest_decision["reason"])


def test_reconcile_workspace_respects_full_development_capacity(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AUTODEV_DEVELOPMENT_CAPACITY", "1")
    monkeypatch.setenv("AUTODEV_RELEASE_CAPACITY", "1")
    monkeypatch.setenv("AUTODEV_RELEASE_BACKFILL_MODE", "auto")

    for issue_number in ("41", "42", "43"):
        packet_text = (
            SAMPLE_ISSUE_PACKET.replace('"42"', f'"{issue_number}"')
            .replace('issue-42', f'issue-{issue_number}')
            .replace('Demo issue', f'Issue {issue_number}')
            .replace('agent/issue-42-demo', f'agent/issue-{issue_number}-demo')
        )
        orchestrator_supervisor._sync_issue_packet_to_db(
            tmp_path,
            parse_issue_packet_text(packet_text, f"docs/agents/issue-packets/issue-{issue_number}.yaml"),
            updated_at="2026-05-07T17:00:00+08:00",
        )

    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="41",
        state="running",
        command_id="cmd-running-41",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-41",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="41",
        updated_at="2026-05-07T17:00:00+08:00",
        current_role="issue_worker",
        current_stage="issue_worker_execution",
        current_status="queued",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="43",
        state="verified",
        command_id="cmd-verified-43",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-v-43",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="43",
        pr_number="143",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v-43",
        command_id="cmd-pr-43",
        payload={"issue_number": "43", "pr_number": "143"},
    )

    adapter = successful_host_adapter(session_id="ses-release-43", resume_command="opencode --session ses-release-43")

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter), patch(
        "scripts.orchestrator_supervisor._sync_issue_progress_label",
        return_value="",
    ), patch(
        "scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False
    ) as run_intake:
        payload = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:10:00+08:00",
            source_session_id="workspace-reconcile",
        )

    issue_42 = read_issue(tmp_path, "42")
    issue_43 = read_issue(tmp_path, "43")

    assert payload["status"] == "success"
    assert payload["intake_status"] == "failed"
    assert payload["intake_error"] == ""
    assert payload["development_capacity"] == 1
    assert payload["release_capacity"] == 1
    assert payload["release_backfill_mode"] == "auto"
    assert payload["auto_release_approval_mode"] == "human_required"
    assert cast(list[dict[str, object]], payload["started_issues"]) == []
    started_releases = cast(list[dict[str, object]], payload["started_releases"])
    assert [entry["issue_number"] for entry in started_releases] == ["43"]
    assert cast(list[dict[str, object]], payload["release_backfill_errors"]) == []
    run_intake.assert_called_once_with(tmp_path)
    assert issue_42 is not None
    assert issue_42["state"] == "ready"
    assert issue_43 is not None
    assert issue_43["state"] == "release_pending"
    assert issue_43["current_session_id"] == "ses-release-43"


def test_reconcile_workspace_manual_release_backfill_mode_skips_release_starts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AUTODEV_RELEASE_BACKFILL_MODE", "manual")
    _ingest_issue_packet_text(tmp_path, "43", SAMPLE_ISSUE_PACKET.replace('"42"', '"43"').replace("issue-42", "issue-43"))
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="43",
        state="verified",
        command_id="cmd-verified-43",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-v-43",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="43",
        pr_number="143",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v-43",
        command_id="cmd-pr-43",
        payload={"issue_number": "43", "pr_number": "143"},
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=True):
        payload = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:10:00+08:00",
            source_session_id="workspace-reconcile",
        )

    issue_43 = read_issue(tmp_path, "43")
    assert payload["release_backfill_mode"] == "manual"
    assert cast(list[dict[str, object]], payload["started_releases"]) == []
    assert cast(list[dict[str, object]], payload["release_backfill_errors"]) == []
    assert issue_43 is not None
    assert issue_43["state"] == "verified"


def test_reconcile_workspace_auto_release_bypass_approval_injects_override(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AUTODEV_RELEASE_BACKFILL_MODE", "auto")
    monkeypatch.setenv("AUTODEV_AUTO_RELEASE_APPROVAL_MODE", "bypass_approval")
    _ingest_issue_packet_text(tmp_path, "43", SAMPLE_ISSUE_PACKET.replace('"42"', '"43"').replace("issue-42", "issue-43"))
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="43",
        state="verified",
        command_id="cmd-verified-43",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-v-43",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="43",
        pr_number="143",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v-43",
        command_id="cmd-pr-43",
        payload={"issue_number": "43", "pr_number": "143"},
    )

    adapter = successful_host_adapter(session_id="ses-release-43", resume_command="opencode --session ses-release-43")

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter), patch(
        "scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=True
    ):
        payload = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:10:00+08:00",
            source_session_id="workspace-reconcile",
        )

    issue_43 = read_issue(tmp_path, "43")

    assert payload["release_backfill_mode"] == "auto"
    assert payload["auto_release_approval_mode"] == "bypass_approval"
    started_releases = cast(list[dict[str, object]], payload["started_releases"])
    assert [entry["issue_number"] for entry in started_releases] == ["43"]
    assert issue_43 is not None
    runtime_context = json.loads(str(issue_43["runtime_context_json"]))
    assert runtime_context["release_runtime_controls"] == {
        "approval_override_mode": "bypass_approval",
        "default_merge_approval_mode": "human_required",
        "override_source": "workspace_reconcile_auto_release",
        "human_approval_skipped": True,
    }
    assert len(adapter.child_calls) == 1
    _, child_context = adapter.child_calls[0]
    assert "Current release override: approval_override_mode=bypass_approval" in child_context.prompt
    assert "override_source=workspace_reconcile_auto_release" in child_context.prompt


def test_auto_release_backfill_bypass_approval_can_complete_end_to_end(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AUTODEV_RELEASE_BACKFILL_MODE", "auto")
    monkeypatch.setenv("AUTODEV_AUTO_RELEASE_APPROVAL_MODE", "bypass_approval")
    _ingest_issue_packet_text(tmp_path, "43", SAMPLE_ISSUE_PACKET.replace('"42"', '"43"').replace("issue-42", "issue-43"))
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="43",
        state="verified",
        command_id="cmd-verified-43",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-v-43",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="43",
        pr_number="143",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v-43",
        command_id="cmd-pr-43",
        payload={"issue_number": "43", "pr_number": "143"},
    )

    adapter = successful_host_adapter(session_id="ses-release-43", resume_command="opencode --session ses-release-43")

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter), patch(
        "scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=True
    ):
        payload = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:10:00+08:00",
            source_session_id="workspace-reconcile",
        )

    issue_43 = read_issue(tmp_path, "43")
    assert payload["auto_release_approval_mode"] == "bypass_approval"
    assert issue_43 is not None
    assert issue_43["state"] == "release_pending"
    assert issue_43["current_session_id"] == "ses-release-43"

    _submit_artifact(
        tmp_path,
        issue_number="43",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": False,
            "merge": {"attempted": True, "merged": True, "merged_sha": "abc143"},
            "merge_approval_mode": "bypass_approval",
            "human_approval_skipped": True,
            "override_source": "workspace_reconcile_auto_release",
            "pr_number": 143,
            "pr_url": "https://github.com/example/repo/pull/143",
            "base_branch": "main",
            "head_branch": "agent/issue-43-demo",
        },
        updated_at="2026-05-07T17:12:00+08:00",
        body_text="Auto release merged successfully with bypass approval override.",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value="") as sync_labels, patch(
        "scripts.orchestrator_lifecycle.subprocess.run",
        return_value=CompletedProcess(["git"], 0, stdout="", stderr=""),
    ):
        orchestrator_supervisor.release_issue_execution(
            base_dir=tmp_path,
            issue_number="43",
            restore_ready_for_agent=False,
            final_state="completed",
            updated_at="2026-05-07T17:13:00+08:00",
        )

    issue_43 = read_issue(tmp_path, "43")
    assert issue_43 is not None
    assert issue_43["state"] == "completed"
    assert issue_43["current_session_id"] == ""
    runtime_context = json.loads(str(issue_43["runtime_context_json"]))
    assert runtime_context["release_runtime_controls"] == {
        "approval_override_mode": "bypass_approval",
        "default_merge_approval_mode": "human_required",
        "override_source": "workspace_reconcile_auto_release",
        "human_approval_skipped": True,
    }
    latest_release_result = read_latest_issue_history(tmp_path, "43", entry_type="release_result")
    assert latest_release_result is not None
    latest_payload = json.loads(str(latest_release_result["payload_json"]))
    assert latest_payload["merge_approval_mode"] == "bypass_approval"
    assert latest_payload["human_approval_skipped"] is True
    assert latest_payload["override_source"] == "workspace_reconcile_auto_release"
    sync_labels.assert_called_once()


def test_reconcile_workspace_reports_intake_exception_without_blocking_scheduler(tmp_path: Path) -> None:
    orchestrator_supervisor.ensure_control_plane_db(tmp_path)
    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", side_effect=RuntimeError("gh unavailable")):
        payload = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:10:00+08:00",
            source_session_id="workspace-reconcile",
        )

    assert payload["status"] == "success"
    assert payload["intake_status"] == "failed"
    assert payload["intake_error"] == "gh unavailable"


def test_reconcile_workspace_logs_db_creation_when_control_plane_db_missing(tmp_path: Path, capsys: CaptureFixture[str]) -> None:
    db_path = tmp_path / ".opencode/runtime/control-plane.sqlite3"
    with pytest.raises(
        RuntimeError,
        match=r"control-plane DB missing .* refusing to recreate during active reconcile workflow",
    ):
        _ = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:10:00+08:00",
            source_session_id="workspace-reconcile",
        )

    captured = capsys.readouterr()
    assert f"[autodev:reconcile] control-plane-db-missing-before-command={db_path}" in captured.err
    assert "control-plane-db-created=" not in captured.err


def test_reconcile_workspace_skips_db_creation_log_when_control_plane_db_exists(tmp_path: Path, capsys: CaptureFixture[str]) -> None:
    orchestrator_supervisor.ensure_control_plane_db(tmp_path)

    _ = orchestrator_supervisor.reconcile_workspace_from_db(
        base_dir=tmp_path,
        updated_at="2026-05-07T17:10:00+08:00",
        source_session_id="workspace-reconcile",
    )

    captured = capsys.readouterr()
    assert "control-plane-db-created=" not in captured.err
    assert "control-plane-db-missing-before-command=" not in captured.err


def test_start_release_claims_verified_issue_and_launches_independent_release_root_session(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verified",
        command_id="cmd-verified",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-v",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="42",
        pr_number="77",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v",
        command_id="cmd-pr",
        payload={"issue_number": "42", "pr_number": "77"},
    )
    adapter = successful_host_adapter(session_id="ses-release-42", resume_command="opencode --session ses-release-42")

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.start_release(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="manual-release",
            approval_override_mode="bypass_approval",
            override_source="user_requested_autodev_release",
            human_approval_skipped=True,
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    request = read_latest_issue_history(tmp_path, "42", entry_type="dispatch_request")
    admin_action = read_latest_issue_history(tmp_path, "42", entry_type="admin_action")

    assert result.get("status") == "success"
    assert result.get("role") == "main_orchestrator"
    assert issue is not None
    assert issue["state"] == "release_pending"
    assert issue["current_session_id"] == "ses-release-42"
    assert issue["current_role"] == "main_orchestrator"
    assert issue["current_status"] == "running"
    runtime_context = json.loads(str(issue["runtime_context_json"]))
    assert runtime_context["release_runtime_controls"] == {
        "approval_override_mode": "bypass_approval",
        "default_merge_approval_mode": "human_required",
        "override_source": "user_requested_autodev_release",
        "human_approval_skipped": True,
    }
    assert request is not None
    assert request["role"] == "main_orchestrator"
    assert admin_action is not None
    assert admin_action["status"] == ""
    assert admin_action["to_state"] == "release_pending"
    assert result.get("executionMode") == "foreground_child_role"
    assert result.get("childRole") == "release_worker"
    assert result.get("childSessionID") == ""
    assert len(adapter.child_calls) == 1
    child_role, child_context = adapter.child_calls[0]
    assert child_role == "release_worker"
    assert child_context.role == "main_orchestrator"
    assert "independent release root session" in child_context.prompt
    assert "foreground release_worker subagent" in child_context.prompt
    assert "Current release override: approval_override_mode=bypass_approval" in child_context.prompt


def test_start_release_logs_db_creation_when_control_plane_db_missing(tmp_path: Path, capsys: CaptureFixture[str]) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verified",
        command_id="cmd-verified",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="42",
        pr_number="77",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v",
        command_id="cmd-pr",
        payload={"issue_number": "42", "pr_number": "77"},
    )
    db_path = tmp_path / ".opencode/runtime/control-plane.sqlite3"
    db_path.unlink()
    adapter = successful_host_adapter(session_id="ses-release-42", resume_command="opencode --session ses-release-42")

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        with pytest.raises(
            RuntimeError,
            match=r"control-plane DB missing .* refusing to recreate during active release workflow",
        ):
            _ = orchestrator_supervisor.start_release(
                base_dir=tmp_path,
                issue_number="42",
                source_session_id="manual-release",
                updated_at="2026-05-07T17:10:00+08:00",
            )

    captured = capsys.readouterr()
    assert f"[autodev:release] control-plane-db-missing-before-command={db_path}" in captured.err
    assert "control-plane-db-created=" not in captured.err


def test_submit_artifact_fails_fast_when_control_plane_db_is_missing(tmp_path: Path) -> None:
    db_path = tmp_path / ".opencode/runtime/control-plane.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with pytest.raises(
        RuntimeError,
        match=r"control-plane DB missing .* refusing to recreate during active submit-artifact workflow",
    ):
        _ = orchestrator_supervisor.submit_artifact(
            base_dir=tmp_path,
            issue_number="42",
            artifact_kind="release_result",
            payload={
                "status": "blocked",
                "blocked_reason": "required_checks_pending",
                "next_recommended_step": "wait checks",
            },
            updated_at="2026-05-08T10:00:00+08:00",
        )


def test_advance_child_fails_fast_when_control_plane_db_is_missing(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / ".opencode/runtime/control-plane.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with pytest.raises(
        RuntimeError,
        match=r"control-plane DB missing .* refusing to recreate during active advance-child workflow",
    ):
        _ = orchestrator_supervisor.main(
            [
                "advance-child",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    captured = capsys.readouterr()
    assert f"[autodev:advance-child] control-plane-db-missing-before-command={db_path}" in captured.err


def test_start_release_skips_db_creation_log_when_control_plane_db_exists(tmp_path: Path, capsys: CaptureFixture[str]) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verified",
        command_id="cmd-verified",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="42",
        pr_number="77",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v",
        command_id="cmd-pr",
        payload={"issue_number": "42", "pr_number": "77"},
    )
    adapter = successful_host_adapter(session_id="ses-release-42", resume_command="opencode --session ses-release-42")

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        _ = orchestrator_supervisor.start_release(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="manual-release",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    captured = capsys.readouterr()
    assert "control-plane-db-created=" not in captured.err
    assert "control-plane-db-missing-before-command=" not in captured.err


def test_dispatch_session_request_surfaces_child_session_tracking_for_release_root_execution() -> None:
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-release-42",
        "requestID": "request-release-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "independent release root-session dispatch for issue #42",
        "title": "Release issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Run release root execution.",
        "role": "main_orchestrator",
        "stage": "release_root_execution",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    adapter = successful_host_adapter(
        session_id="ses-release-root-42",
        resume_command="opencode --session ses-release-root-42",
        metadata={
            "tuiResumeCommand": "/sessions",
            "stopContinuationStatus": "root_session_detached",
            "stopContinuationAttempts": 0,
            "executionMode": "foreground_child_role",
            "childRole": "release_worker",
            "childSessionID": "ses-release-worker-42",
            "childSessionStatus": "stop",
        },
    )

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("/tmp/demo"),
            source_session_id="manual-release",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "success"
    assert result.get("executionMode") == "foreground_child_role"
    assert result.get("childRole") == "release_worker"
    assert result.get("childSessionID") == "ses-release-worker-42"
    assert result.get("childSessionStatus") == "stop"


def test_dispatch_request_from_db_persists_release_child_session_trace_in_history_payload(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    request = build_session_request(
        ledger,
        role="main_orchestrator",
        stage="release_root_execution",
        reason="independent release root-session dispatch for issue #42",
        title="Release issue #42 on agent/issue-42-demo",
        decision_summary="Launch release root now.",
    )

    adapter = successful_host_adapter(
        session_id="ses-release-root-42",
        resume_command="opencode --session ses-release-root-42",
        metadata={
            "tuiResumeCommand": "/sessions",
            "stopContinuationStatus": "root_session_detached",
            "stopContinuationAttempts": 0,
            "executionMode": "foreground_child_role",
            "childRole": "release_worker",
            "childSessionID": "ses-release-worker-42",
            "childSessionStatus": "stop",
        },
    )

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_request_from_db(
            request,
            base_dir=tmp_path,
            source_session_id="manual-release",
            updated_at="2026-05-07T17:10:00+08:00",
            failure_restore_state="release_pending",
        )

    session_result = read_latest_issue_history(tmp_path, "42", entry_type="session_result")
    runtime_context = orchestrator_supervisor.read_runtime_context(tmp_path, "42")
    latest_release_child_session = read_latest_ref(tmp_path, "42", "release_child_session")

    assert result.get("childSessionID") == "ses-release-worker-42"
    assert session_result is not None
    session_payload = json.loads(str(session_result["payload_json"]))
    assert session_payload["childSessionID"] == "ses-release-worker-42"
    assert session_payload["childSessionStatus"] == "stop"
    assert session_payload["childRole"] == "release_worker"
    assert runtime_context["release_child_session"]["childSessionID"] == "ses-release-worker-42"
    assert latest_release_child_session["childRole"] == "release_worker"


def test_start_release_refuses_verified_issue_without_pr_opened_fact(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verified",
        command_id="cmd-verified",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    try:
        orchestrator_supervisor.start_release(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="manual-release",
            updated_at="2026-05-07T17:10:00+08:00",
        )
    except RuntimeError as error:
        assert "no verifier-owned pr_opened fact" in str(error)
    else:
        raise AssertionError("expected missing pr_opened fact to block release")

    issue = read_issue(tmp_path, "42")
    assert issue is not None
    assert issue["state"] == "verified"


def test_start_release_backfills_pr_opened_from_passed_evidence_packet(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verified",
        command_id="cmd-verified",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        runtime_context={"resolved_base_branch": "main"},
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:05:00+08:00",
    )
    adapter = successful_host_adapter(session_id="ses-release-42", resume_command="opencode --session ses-release-42")

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.start_release(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="manual-release",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    latest_pr_opened = read_latest_issue_history(tmp_path, "42", entry_type="pr_opened")

    assert result.get("status") == "success"
    assert latest_pr_opened is not None
    assert latest_pr_opened["status"] == "opened"
    payload = json.loads(str(latest_pr_opened["payload_json"]))
    assert payload["pr_number"] == "77"
    assert payload["source_artifact"] == "release_evidence_packet_fallback"
    assert payload["base_branch"] == "main"


def test_start_release_repairs_stale_release_pending_fence_without_dispatch_evidence(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "41", SAMPLE_ISSUE_PACKET.replace('"42"', '"41"').replace("issue-42", "issue-41"))
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="41",
        state="verified",
        command_id="cmd-verified-41",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="41",
        pr_number="71",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v-41",
        command_id="cmd-pr-41",
        payload={"issue_number": "41", "pr_number": "71"},
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending-42",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        current_role="main_orchestrator",
        current_stage="release_root_execution",
        current_status="queued",
    )
    adapter = successful_host_adapter(session_id="ses-release-41", resume_command="opencode --session ses-release-41")

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.start_release(
            base_dir=tmp_path,
            issue_number="41",
            source_session_id="manual-release",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue_41 = read_issue(tmp_path, "41")
    issue_42 = read_issue(tmp_path, "42")
    repair_event = read_latest_issue_history(tmp_path, "42", entry_type="runtime_transition")

    assert result.get("status") == "success"
    assert result.get("role") == "main_orchestrator"
    assert issue_41 is not None
    assert issue_41["state"] == "release_pending"
    assert issue_42 is not None
    assert issue_42["state"] == "release_pending"
    assert issue_42["current_session_id"] == ""
    assert issue_42["current_status"] == ""
    assert repair_event is not None
    assert "Cleared stale release_worker fence" in str(repair_event.get("summary") or "")


def test_start_release_recovers_pending_approval_after_remote_approval_and_merge(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _ = (tmp_path / ".autodev.yaml").write_text(
        'schema_version: "1.0"\nproject:\n  github_repo: example/repo\n',
        encoding="utf-8",
    )
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "pending_approval"}
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-pending-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        current_role="main_orchestrator",
        current_stage="release_root_execution",
        current_status="pending_approval",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="42",
        pr_number="77",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v",
        command_id="cmd-pr",
        payload={"issue_number": "42", "pr_number": "77"},
    )
    adapter = successful_host_adapter(session_id="ses-release-42", resume_command="opencode --session ses-release-42")

    with patch(
        "scripts.orchestrator_supervisor._read_release_pending_pr_merge_status",
        return_value={
            "pr_number": "77",
            "merged": True,
            "reviewDecision": "APPROVED",
            "mergeStateStatus": "MERGED",
            "url": "https://github.com/example/repo/pull/77",
            "mergedAt": "2026-05-07T17:09:00+08:00",
        },
    ), patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.start_release(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="manual-release",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    repair_event = read_latest_issue_history(tmp_path, "42", entry_type="runtime_transition")

    assert result.get("status") == "success"
    assert issue is not None
    assert issue["state"] == "release_pending"
    assert issue["current_session_id"] == "ses-release-42"
    assert issue["current_status"] == "running"
    assert repair_event is not None
    assert "approved and merged" in str(repair_event.get("summary") or "")


def test_reconcile_workspace_auto_release_recovers_pending_approval_after_remote_merge(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AUTODEV_RELEASE_BACKFILL_MODE", "auto")
    _ingest_issue_packet_text(tmp_path, "43", SAMPLE_ISSUE_PACKET.replace('"42"', '"43"').replace("issue-42", "issue-43"))
    _ = (tmp_path / ".autodev.yaml").write_text(
        'schema_version: "1.0"\nproject:\n  github_repo: example/repo\n',
        encoding="utf-8",
    )
    issue_packet = parse_issue_packet_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"43"').replace("issue-42", "issue-43"),
        "docs/agents/issue-packets/issue-43.yaml",
    )
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "pending_approval"}
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="43",
        state="release_pending",
        command_id="cmd-release-pending-43",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-pending-43",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="43",
        updated_at="2026-05-07T17:00:00+08:00",
        current_role="main_orchestrator",
        current_stage="release_root_execution",
        current_status="pending_approval",
    )
    orchestrator_supervisor.record_pr_opened(
        base_dir=tmp_path,
        issue_number="43",
        pr_number="143",
        created_at="2026-05-07T17:00:00+08:00",
        verifier_session_id="ses-v-43",
        command_id="cmd-pr-43",
        payload={"issue_number": "43", "pr_number": "143"},
    )
    adapter = successful_host_adapter(session_id="ses-release-43", resume_command="opencode --session ses-release-43")

    with patch(
        "scripts.orchestrator_supervisor._read_release_pending_pr_merge_status",
        return_value={
            "pr_number": "143",
            "merged": True,
            "reviewDecision": "APPROVED",
            "mergeStateStatus": "MERGED",
            "url": "https://github.com/example/repo/pull/143",
            "mergedAt": "2026-05-07T17:09:00+08:00",
        },
    ), patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter), patch(
        "scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=True
    ):
        payload = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:10:00+08:00",
            source_session_id="workspace-reconcile",
        )

    issue_43 = read_issue(tmp_path, "43")
    started_releases = cast(list[dict[str, object]], payload["started_releases"])

    assert [entry["issue_number"] for entry in started_releases] == ["43"]
    assert issue_43 is not None
    assert issue_43["state"] == "release_pending"
    assert issue_43["current_session_id"] == "ses-release-43"
    assert issue_43["current_status"] == "running"


def test_reconcile_release_pending_approval_without_release_result_stays_waiting(tmp_path: Path) -> None:
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "pending_approval"}
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:01:00+08:00",
        current_session_id="ses-release-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:02:00+08:00",
    )

    assert updated_ledger is not None
    assert decision["action"] == "release_waiting"
    assert decision["next_role"] == "operator"
    assert request is None


def test_validate_session_request_rejects_completed_issue(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="completed",
        command_id="cmd-completed",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    request = build_orchestrator_request(ledger)

    error = validate_session_request_for_dispatch(request, ledger, base_dir=tmp_path)

    assert "already completed or released" in error


def test_validate_session_request_rejects_stale_revision(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    request = build_orchestrator_request(ledger)
    ledger["ledgerRevision"] = "2026-05-07T17:10:00+08:00"

    error = validate_session_request_for_dispatch(request, ledger, base_dir=tmp_path)

    assert "stale request revision" in error


def test_validate_session_request_rejects_stale_issue_or_branch(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    request = build_orchestrator_request(ledger)

    stale_issue_request = cast(dict[str, object], dict(request))
    stale_issue_request["issueNumber"] = "41"
    stale_issue_session_request = cast(orchestrator_supervisor.SessionRequest, cast(object, stale_issue_request))
    issue_error = validate_session_request_for_dispatch(
        stale_issue_session_request,
        ledger,
        base_dir=tmp_path,
    )
    assert "stale request issue #41" in issue_error

    stale_branch_request = cast(dict[str, object], dict(request))
    stale_branch_request["branch"] = "agent/issue-42-alt"
    stale_branch_session_request = cast(orchestrator_supervisor.SessionRequest, cast(object, stale_branch_request))
    branch_error = validate_session_request_for_dispatch(
        stale_branch_session_request,
        ledger,
        base_dir=tmp_path,
    )
    assert "stale request branch agent/issue-42-alt" in branch_error


def test_validate_session_request_allows_recovery_request_with_selected_next_issue(tmp_path: Path):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    issue_31 = issue_packets_dir / "issue-31.yaml"
    issue_32 = issue_packets_dir / "issue-32.yaml"
    issue_31.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    issue_32.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"32"').replace('issue-42', 'issue-32').replace('Demo issue', 'Issue 32').replace('agent/issue-42-demo', 'agent/issue-32-demo'),
        encoding="utf-8",
    )
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    next_issue_packet = parse_issue_packet_text(issue_32.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-32.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    ledger["queuedNextIssue"] = {
        "issue_number": next_issue_packet.issue_number,
        "branch": next_issue_packet.branch,
        "base_branch": next_issue_packet.base_branch,
    }
    request = build_session_request(
        ledger,
        role="main_orchestrator",
        stage="issue_selection_or_recovery",
        reason="main_orchestrator recovery for issue #31",
        title="Recover or continue after issue #31",
        decision_summary="Consume preselected issue #32.",
    )

    error = validate_session_request_for_dispatch(request, ledger, base_dir=tmp_path)

    assert error == ""
    assert request.get("selectedIssueNumber") == "32"
    assert request.get("selectedIssueBranch") == "agent/issue-32-demo"
    assert "selectedIssueBaseBranch" not in request
    assert "selectedIssuePacketPath" not in request


def test_sync_runtime_phase_metadata_clears_stale_queued_next_issue_projection(tmp_path: Path) -> None:
    orchestrator_supervisor._sync_runtime_phase_metadata(
        base_dir=tmp_path,
        issue_number="42",
        current={"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"},
        attempts={"main_orchestrator": 1},
        limits={"main_orchestrator": 3},
        last_failure={},
        workflow={},
        automation={},
        artifacts={},
        queued_next_issue={"issue_number": "43", "branch": "agent/issue-43-demo", "base_branch": "main"},
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with_queued_next_issue = orchestrator_supervisor.read_runtime_context(tmp_path, "42")
    assert cast(dict[str, object], with_queued_next_issue).get("queuedNextIssue") is not None

    orchestrator_supervisor._sync_runtime_phase_metadata(
        base_dir=tmp_path,
        issue_number="42",
        current={"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"},
        attempts={"main_orchestrator": 1},
        limits={"main_orchestrator": 3},
        last_failure={},
        workflow={},
        automation={},
        artifacts={},
        updated_at="2026-05-07T17:01:00+08:00",
    )

    without_queued_next_issue = orchestrator_supervisor.read_runtime_context(tmp_path, "42")
    assert "queuedNextIssue" not in cast(dict[str, object], without_queued_next_issue)


def test_normalize_runtime_phase_projection_preserves_current_and_manages_queued_next_issue(tmp_path: Path) -> None:
    current = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}

    normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
        base_dir=tmp_path,
        issue_number="42",
        current=current,
        queued_next_issue={"issue_number": "43", "branch": "agent/issue-43-demo", "base_branch": "main"},
    )

    assert normalized_current == current
    assert cast(dict[str, object], runtime_context)["queuedNextIssue"] == {
        "issue_number": "43",
        "branch": "agent/issue-43-demo",
        "base_branch": "main",
    }

    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        runtime_context={"queuedNextIssue": {"issue_number": "43", "branch": "agent/issue-43-demo", "base_branch": "main"}},
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verified",
        command_id="cmd-verified",
        updated_at="2026-05-07T17:00:01+08:00",
    )

    normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
        base_dir=tmp_path,
        issue_number="42",
        current=current,
        queued_next_issue=None,
    )

    assert normalized_current == current
    assert runtime_context == {"queuedNextIssue": None}


def test_normalize_runtime_phase_projection_clears_terminal_states(tmp_path: Path) -> None:
    current = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}

    for state in ("failed", "completed"):
        orchestrator_supervisor.upsert_issue_state(
            tmp_path,
            issue_number="42",
            state=state,
            command_id=f"cmd-{state}",
            updated_at="2026-05-07T17:00:00+08:00",
        )

        normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
            base_dir=tmp_path,
            issue_number="42",
            current=current,
            queued_next_issue=None,
        )

        assert normalized_current == {"role": "", "stage": "", "status": ""}
        assert runtime_context is None


def test_normalize_runtime_phase_projection_preserves_ready_state_whitelist_and_clears_stale_ready_projection(tmp_path: Path) -> None:
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
        base_dir=tmp_path,
        issue_number="42",
        current={"role": "main_orchestrator", "stage": "orchestrator_bootstrap", "status": "queued"},
        queued_next_issue=None,
    )

    assert normalized_current == {"role": "main_orchestrator", "stage": "orchestrator_bootstrap", "status": "queued"}
    assert runtime_context is None

    normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
        base_dir=tmp_path,
        issue_number="42",
        current={"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"},
        queued_next_issue=None,
    )

    assert normalized_current == {"role": "", "stage": "", "status": ""}
    assert runtime_context is None


def test_normalize_runtime_phase_projection_clears_stale_verified_projection_but_keeps_release_waiting_handoff(tmp_path: Path) -> None:
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verified",
        command_id="cmd-verified",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
        base_dir=tmp_path,
        issue_number="42",
        current={"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"},
        queued_next_issue=None,
    )

    assert normalized_current == {"role": "", "stage": "", "status": ""}
    assert runtime_context is None

    normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
        base_dir=tmp_path,
        issue_number="42",
        current={"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"},
        queued_next_issue=None,
    )

    assert normalized_current == {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    assert runtime_context is None


def test_normalize_runtime_phase_projection_clears_stale_release_pending_projection_but_keeps_release_root_active(tmp_path: Path) -> None:
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
        base_dir=tmp_path,
        issue_number="42",
        current={"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"},
        queued_next_issue=None,
    )

    assert normalized_current == {"role": "", "stage": "", "status": ""}
    assert runtime_context is None

    normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
        base_dir=tmp_path,
        issue_number="42",
        current={"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"},
        queued_next_issue=None,
    )

    assert normalized_current == {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    assert runtime_context is None

    normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
        base_dir=tmp_path,
        issue_number="42",
        current={"role": "main_orchestrator", "stage": "release_root_execution", "status": "running"},
        queued_next_issue=None,
    )

    assert normalized_current == {"role": "main_orchestrator", "stage": "release_root_execution", "status": "running"}
    assert runtime_context is None


def test_normalize_runtime_phase_projection_enforces_active_state_whitelists(tmp_path: Path) -> None:
    cases = [
        (
            "dispatching",
            {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"},
            {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"},
        ),
        (
            "dispatching",
            {"role": "main_orchestrator", "stage": "orchestrator_bootstrap", "status": "running"},
            {"role": "", "stage": "", "status": ""},
        ),
        (
            "running",
            {"role": "main_orchestrator", "stage": "orchestrator_bootstrap", "status": "running"},
            {"role": "main_orchestrator", "stage": "orchestrator_bootstrap", "status": "running"},
        ),
        (
            "running",
            {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"},
            {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"},
        ),
        (
            "running",
            {"role": "issue_worker", "stage": "issue_worker_repair", "status": "queued"},
            {"role": "issue_worker", "stage": "issue_worker_repair", "status": "queued"},
        ),
        (
            "running",
            {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"},
            {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"},
        ),
        (
            "verifying",
            {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"},
            {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"},
        ),
        (
            "verifying",
            {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"},
            {"role": "", "stage": "", "status": ""},
        ),
    ]

    for state, current, expected in cases:
        orchestrator_supervisor.upsert_issue_state(
            tmp_path,
            issue_number="42",
            state=state,
            command_id=f"cmd-{state}-{current['role'] or 'empty'}",
            updated_at="2026-05-07T17:00:00+08:00",
        )

        normalized_current, runtime_context = orchestrator_supervisor._normalize_runtime_phase_projection(
            base_dir=tmp_path,
            issue_number="42",
            current=current,
            queued_next_issue=None,
        )

        assert normalized_current == expected
        assert runtime_context is None


def test_validate_session_request_allows_recovery_request_with_legacy_record_shaped_queued_next_issue(tmp_path: Path):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    issue_31 = issue_packets_dir / "issue-31.yaml"
    issue_32 = issue_packets_dir / "issue-32.yaml"
    issue_31.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    issue_32.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"32"').replace('issue-42', 'issue-32').replace('Demo issue', 'Issue 32').replace('agent/issue-42-demo', 'agent/issue-32-demo'),
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    next_issue_packet = parse_issue_packet_text(issue_32.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-32.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    ledger["queuedNextIssue"] = {
        "record": {
            "issue_number": next_issue_packet.issue_number,
            "branch": next_issue_packet.branch,
            "base_branch": next_issue_packet.base_branch,
        }
    }
    request = build_session_request(
        ledger,
        role="main_orchestrator",
        stage="issue_selection_or_recovery",
        reason="main_orchestrator recovery for issue #31",
        title="Recover or continue after issue #31",
        decision_summary="Consume preselected issue #32.",
    )

    error = validate_session_request_for_dispatch(request, ledger, base_dir=tmp_path)

    assert error == ""
    assert request.get("selectedIssueNumber") == "32"
    assert request.get("selectedIssueBranch") == "agent/issue-32-demo"


def test_reconcile_verifier_fail_routes_back_to_issue_worker(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    attempts = cast(dict[str, int], ledger["attempts"])
    attempts["issue_worker"] = 1
    attempts["pr_verifier"] = 1
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    evidence_path = tmp_path / "docs/agents/evidence/issue-42-pr-77.yaml"
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(
        """schema_version: \"1.0\"
kind: evidence_packet
line_cap: 60
raw_evidence_policy: index_only_manifest_no_raw_logs_or_traces
subject:
  type: \"issue_pr\"
  issue_number: \"42\"
  pr_number: \"77\"
  phase: \"\"
  branch: \"agent/issue-42-demo\"
  sha: \"abc\"
verifier:
  actor: \"v\"
  actor_role: \"pr_verifier\"
  verifier_session_id: \"ses-v\"
  started_at: \"2026-05-07T17:12:00+08:00\"
  completed_at: \"2026-05-07T17:13:00+08:00\"
proof_of_separation:
  worker_result_ref: \"docs/agents/worker-results/issue-42.yaml\"
  worker_actor: \"w\"
  worker_session_id: \"ses-w\"
  verifier_actor: \"v\"
  verifier_session_id: \"ses-v\"
  verifier_is_distinct_from_worker: true
  verifier_read_worker_result_only: true
status: \"fail\"
failure_classification: {kind: \"verification_failed\", retryable: true, routed_to: \"issue_worker\", root_cause_signature: \"diag\"}
test_case_verification: {applies: false, test_case_id: \"\", target_case: \"n/a\", regression_bucket: \"n/a\", failure_signature: \"none\", artifact_manifest_ref: \"\"}
acceptance_criteria_matrix:
  - {ac_id: \"AC1\", status: \"fail\", evidence_ref: \"bundle\", note: \"broken\"}
gates:
  diagnostics_and_build_gate: {status: \"fail\", evidence_ref: \"bundle\"}
  surface_qa_gate: {status: \"not_applicable\", evidence_ref: \"\"}
  review_gate: {status: \"not_applicable\", evidence_ref: \"\"}
  security_gate: {status: \"pass\", evidence_ref: \"bundle\"}
role_boundary:
  acceptance_qa_owner: \"pr_verifier\"
  main_agent_ran_issue_qa: false
  worker_self_checks_are_not_final_acceptance: true
artifact_manifest:
  bundle_ref: \"bundle\"
  retention: \"\"
  items:
    - {id: \"1\", kind: \"summary\", executor_role: \"pr_verifier\", path: \"bundle\", sha256: \"\", description: \"desc\"}
compact_summary:
  outcome: \"fail\"
  automated_checks: \"fail\"
  manual_qa: \"n/a\"
  risks_or_limitations: [\"none\"]
next_recommended_step: \"Return to worker\"
""",
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "fail",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Return to worker",
            "failure_kind": "verification_failed",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:30+08:00",
        body_text=evidence_path.read_text(encoding="utf-8"),
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:15:00+08:00",)
    current = cast(dict[str, object], updated_ledger["current"])
    issue = read_issue(tmp_path, "42")

    assert current["role"] == "issue_worker"
    assert current["stage"] == "issue_worker_repair"
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "issue_worker"
    assert request is None
    assert issue is not None
    assert issue["state"] == "running"


def test_reconcile_issue_worker_exhaustion_marks_issue_failed(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "done"}
    attempts = cast(dict[str, int], ledger["attempts"])
    limits = cast(dict[str, int], ledger["limits"])
    attempts["issue_worker"] = limits["issue_worker"]
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:15:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "failed"


def test_reconcile_release_success_selects_next_ready_issue(tmp_path: Path):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    issue_31 = issue_packets_dir / "issue-31.yaml"
    issue_32 = issue_packets_dir / "issue-32.yaml"
    issue_31.write_text(SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'), encoding="utf-8")
    issue_32.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"32"')
        .replace('issue-42', 'issue-32')
        .replace('Demo issue', 'Issue 32')
        .replace('agent/issue-42-demo', 'agent/issue-32-demo')
        .replace('prior_handoff: "docs/agents/handoffs/issue-41.yaml"', 'prior_handoff: "docs/agents/handoffs/issue-31.yaml"')
        + 'implementation_notes:\n  constraints:\n    - "demo"\n  risks: ["none"]\n  dependencies:\n    - "Issue #31 is released and closed; issue #34 remains blocked by issue #32."\n',
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "31", issue_31.read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "32", issue_32.read_text(encoding="utf-8"))

    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, root_session_agent="build",
    updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-31-pr-88.yaml"

    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nraw_evidence_policy: index_only_refs_no_raw_logs_or_transcripts\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "success"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:22:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:21:00+08:00",)
    issue = cast(dict[str, object], updated_ledger["issue"])
    automation = cast(dict[str, object], updated_ledger["automation"])
    assert issue["number"] == "32"
    assert automation["rootSessionAgent"] == "build"
    assert decision["action"] == "queue_next_issue"
    assert request is not None
    assert request["issueNumber"] == "32"
    assert request["agent"] == "build"
    assert read_issue(tmp_path, "31") is not None


def test_reconcile_release_completed_selects_next_ready_issue(tmp_path: Path):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    issue_31 = issue_packets_dir / "issue-31.yaml"
    issue_32 = issue_packets_dir / "issue-32.yaml"
    issue_31.write_text(SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'), encoding="utf-8")
    issue_32.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"32"')
        .replace('issue-42', 'issue-32')
        .replace('Demo issue', 'Issue 32')
        .replace('agent/issue-42-demo', 'agent/issue-32-demo')
        .replace('prior_handoff: "docs/agents/handoffs/issue-41.yaml"', 'prior_handoff: "docs/agents/handoffs/issue-31.yaml"'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "31", issue_31.read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "32", issue_32.read_text(encoding="utf-8"))

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, root_session_agent="build",
    updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-31-pr-88.yaml"

    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nraw_evidence_policy: index_only_refs_no_raw_logs_or_transcripts\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "completed"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: false\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "completed",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": False,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:21:00+08:00",)
    issue = cast(dict[str, object], updated_ledger["issue"])
    persisted_issue = read_issue(tmp_path, "31")

    assert issue["number"] == "32"
    assert decision["action"] == "queue_next_issue"
    assert request is not None
    assert request["issueNumber"] == "32"
    assert persisted_issue is not None
    assert persisted_issue["state"] == "completed"


def test_reconcile_release_success_syncs_control_plane_runtime_phase_after_handoff(tmp_path: Path):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    issue_31 = issue_packets_dir / "issue-31.yaml"
    issue_32 = issue_packets_dir / "issue-32.yaml"
    issue_31.write_text(SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'), encoding="utf-8")
    issue_32.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"32"')
        .replace('issue-42', 'issue-32')
        .replace('Demo issue', 'Issue 32')
        .replace('agent/issue-42-demo', 'agent/issue-32-demo')
        .replace('prior_handoff: "docs/agents/handoffs/issue-41.yaml"', 'prior_handoff: "docs/agents/handoffs/issue-31.yaml"'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "31", issue_31.read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "32", issue_32.read_text(encoding="utf-8"))

    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, root_session_agent="build",
    updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-31-pr-88.yaml"

    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nraw_evidence_policy: index_only_refs_no_raw_logs_or_transcripts\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "success"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:21:00+08:00",)

    next_issue = read_issue(tmp_path, "32")

    assert decision["action"] == "queue_next_issue"
    assert request is not None
    assert cast(dict[str, object], updated_ledger["current"])["role"] == "main_orchestrator"
    assert next_issue is not None
    assert next_issue["current_role"] == "main_orchestrator"
    assert next_issue["current_stage"] == "orchestrator_bootstrap"
    assert next_issue["current_status"] == "queued"


def test_reconcile_recovery_waits_for_db_packet_when_next_issue_is_not_recorded(tmp_path: Path):
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )
    current_packet = tmp_path / "docs/agents/issue-packets/issue-31.yaml"
    current_packet.parent.mkdir(parents=True, exist_ok=True)
    current_packet.write_text(SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'), encoding="utf-8")
    _ingest_issue_packet_text(tmp_path, "31", current_packet.read_text(encoding="utf-8"))

    issue_packet = parse_issue_packet_text(current_packet.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-31-pr-88.yaml"

    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nraw_evidence_policy: index_only_refs_no_raw_logs_or_transcripts\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "success"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=True):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:21:00+08:00",)
    issue = cast(dict[str, object], updated_ledger["issue"])

    assert issue["number"] == "31"
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert request["issueNumber"] == "31"


def test_queue_orchestrator_recovery_updates_control_plane_for_recovery_stage(tmp_path: Path):
    current_packet = tmp_path / "docs/agents/issue-packets/issue-31.yaml"
    current_packet.parent.mkdir(parents=True, exist_ok=True)
    current_packet.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(current_packet.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        updated_at="2026-05-07T17:00:00+08:00",
    )
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-31-pr-88.yaml"

    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nraw_evidence_policy: index_only_refs_no_raw_logs_or_transcripts\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "success"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:21:00+08:00",)

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None


def test_select_next_issue_packet_skips_unreleased_dependencies(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    (packets_dir / "issue-30.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"30"').replace('issue-42', 'issue-30').replace('Demo issue', 'Issue 30').replace('agent/issue-42-demo', 'agent/issue-30-demo'),
        encoding="utf-8",
    )
    (packets_dir / "issue-31.yaml").write_text(
        """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"31\"
  title: \"Issue 31\"
  url: \"https://github.com/example/issues/31\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-31-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"scripts\"]
  prior_handoff: \"none\"

implementation_notes:
  constraints:
    - \"demo\"
  risks: [\"none\"]
  dependencies:
    - \"Blocked by issue #99 until it is released\"
""",
        encoding="utf-8",
    )

    selected = select_next_issue_packet(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
    )

    assert selected is None


def test_select_next_issue_packet_skips_issue_with_execution_lock(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    (packets_dir / "issue-30.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"30"').replace('issue-42', 'issue-30').replace('Demo issue', 'Issue 30').replace('agent/issue-42-demo', 'agent/issue-30-demo'),
        encoding="utf-8",
    )
    (packets_dir / "issue-31.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "30", packet_30 := (packets_dir / "issue-30.yaml").read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "31", (packets_dir / "issue-31.yaml").read_text(encoding="utf-8"))
    selected = select_next_issue_packet(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
    )

    assert selected is not None
    assert selected.issue_number == "31"


def test_select_next_issue_candidate_returns_compact_candidate_shape(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    (packets_dir / "issue-30.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"30"').replace('issue-42', 'issue-30').replace('Demo issue', 'Issue 30').replace('agent/issue-42-demo', 'agent/issue-30-demo'),
        encoding="utf-8",
    )
    (packets_dir / "issue-31.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "30", (packets_dir / "issue-30.yaml").read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "31", (packets_dir / "issue-31.yaml").read_text(encoding="utf-8"))

    selected = select_next_issue_candidate(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
    )

    assert selected is not None
    assert selected.issue_number == "31"
    assert selected.branch == "agent/issue-31-demo"
    assert not hasattr(selected, "title")


def test_select_next_issue_packet_skips_issue_marked_in_flight_in_control_plane_db(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    (packets_dir / "issue-30.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"30"').replace('issue-42', 'issue-30').replace('Demo issue', 'Issue 30').replace('agent/issue-42-demo', 'agent/issue-30-demo'),
        encoding="utf-8",
    )
    (packets_dir / "issue-31.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "30", (packets_dir / "issue-30.yaml").read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "31", (packets_dir / "issue-31.yaml").read_text(encoding="utf-8"))
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    selected = select_next_issue_packet(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
    )

    assert selected is None


def test_select_next_issue_packet_skips_issue_marked_quarantined_in_control_plane_db(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    (packets_dir / "issue-30.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"30"').replace('issue-42', 'issue-30').replace('Demo issue', 'Issue 30').replace('agent/issue-42-demo', 'agent/issue-30-demo'),
        encoding="utf-8",
    )
    (packets_dir / "issue-31.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "30", (packets_dir / "issue-30.yaml").read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "31", (packets_dir / "issue-31.yaml").read_text(encoding="utf-8"))
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="quarantined",
        command_id="cmd-quarantined",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    selected = select_next_issue_packet(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
    )

    assert selected is None


def test_select_next_issue_packet_prefers_highest_db_ranked_ready_issue(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    (packets_dir / "issue-30.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"30"').replace('issue-42', 'issue-30').replace('Demo issue', 'Issue 30').replace('agent/issue-42-demo', 'agent/issue-30-demo'),
        encoding="utf-8",
    )
    (packets_dir / "issue-31.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    (packets_dir / "issue-32.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"32"').replace('issue-42', 'issue-32').replace('Demo issue', 'Issue 32').replace('agent/issue-42-demo', 'agent/issue-32-demo'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "30", (packets_dir / "issue-30.yaml").read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "31", (packets_dir / "issue-31.yaml").read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "32", (packets_dir / "issue-32.yaml").read_text(encoding="utf-8"))

    selected = select_next_issue_packet(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
    )

    assert selected is not None
    assert selected.issue_number == "31"


def test_select_next_issue_packet_downgrades_stale_db_rank_for_now_ineligible_issue(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    (packets_dir / "issue-30.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"30"').replace('issue-42', 'issue-30').replace('Demo issue', 'Issue 30').replace('agent/issue-42-demo', 'agent/issue-30-demo'),
        encoding="utf-8",
    )
    (packets_dir / "issue-31.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    (packets_dir / "issue-32.yaml").write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"32"').replace('issue-42', 'issue-32').replace('Demo issue', 'Issue 32').replace('agent/issue-42-demo', 'agent/issue-32-demo').replace('labels: [ready-for-agent]', 'labels: [agent-in-progress]'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "30", (packets_dir / "issue-30.yaml").read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "31", (packets_dir / "issue-31.yaml").read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "32", (packets_dir / "issue-32.yaml").read_text(encoding="utf-8"))
    _ = orchestrator_supervisor.upsert_issue_ranking(
        tmp_path,
        issue_number="32",
        rank_score=999999,
        lane="default",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    selected = select_next_issue_packet(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
    )
    issue = orchestrator_supervisor.read_issue(tmp_path, "32")

    assert selected is not None
    assert selected.issue_number == "31"


def test_select_next_issue_packet_skips_db_packet_when_local_file_is_missing(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    packet_30 = packets_dir / "issue-30.yaml"
    packet_31 = packets_dir / "issue-31.yaml"
    packet_30.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"30"').replace('issue-42', 'issue-30').replace('Demo issue', 'Issue 30').replace('agent/issue-42-demo', 'agent/issue-30-demo'),
        encoding="utf-8",
    )
    packet_31.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "30", packet_30.read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "31", packet_31.read_text(encoding="utf-8"))

    selected = select_next_issue_packet(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
    )

    assert selected is not None
    assert selected.issue_number == "31"

    packet_31.unlink()

    selected_after_delete = select_next_issue_packet(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
    )
    issue_31 = orchestrator_supervisor.read_issue(tmp_path, "31")

    assert selected_after_delete is not None
    assert selected_after_delete.issue_number == "31"
    assert issue_31 is not None
    assert issue_31["rank_score"] > 0
    assert not packet_31.exists()


def test_start_issue_requires_db_packet_when_local_yaml_exists_but_is_not_ingested(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        try:
            orchestrator_supervisor.start_issue(
                base_dir=tmp_path,
                issue_number="42",
                source_session_id="autodev-start",
                updated_at="2026-05-07T17:10:00+08:00",
            )
        except RuntimeError as error:
            assert "not recorded in SQLite" in str(error)
        else:
            raise AssertionError("expected start_issue to require a DB-backed issue packet")


def test_start_issue_rejects_ready_issue_with_existing_current_session_id(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready-fenced",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-existing",
    )

    try:
        orchestrator_supervisor.start_issue(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="autodev-start",
            updated_at="2026-05-07T17:10:00+08:00",
        )
    except RuntimeError as error:
        assert "active current session fence" in str(error)
        assert "ses-existing" not in str(error)
    else:
        raise AssertionError("expected start_issue to reject a ready row that still has a current_session_id")


def test_run_issue_packet_intake_uses_consumer_project_github_repo(tmp_path: Path):
    _ = (tmp_path / ".autodev.yaml").write_text(
        'schema_version: "1.0"\nproject:\n  name: demo\n  github_repo: owner/demo-repo\n',
        encoding="utf-8",
    )

    with patch(
        "scripts.orchestrator_supervisor.subprocess.run",
        return_value=CompletedProcess(args=["python"], returncode=0, stdout="", stderr=""),
    ) as run:
        result = run_issue_packet_intake(tmp_path)

    assert result is True
    command = run.call_args.args[0]
    kwargs = run.call_args.kwargs
    assert command[0]
    assert command[1] == str(orchestrator_supervisor.DEFAULT_ISSUE_INTAKE_SCRIPT_PATH)
    assert "--repo" in command
    assert "owner/demo-repo" in command
    assert "--project-root" in command
    assert str(tmp_path) in command
    assert kwargs["cwd"] == tmp_path


def test_dispatch_db_request_launches_root_session_without_legacy_runtime_files(tmp_path: Path, capsys) -> None:
    request = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "req-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor._record_dispatch_request_history(
        base_dir=tmp_path,
        request=cast(orchestrator_supervisor.SessionRequest, cast(object, request)),
        created_at="2026-05-07T17:00:00+08:00",
    )
    adapter = successful_host_adapter(session_id="ses_root_test")
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        exit_code = orchestrator_supervisor.main(
            [
                "dispatch",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(capsys.readouterr().out))
    issue = read_issue(tmp_path, "42")

    assert exit_code == 0
    assert session_result["status"] == "success"
    assert session_result["rootSessionID"] == "ses_root_test"
    assert session_result["sourceSessionID"] == "ses_source_test"
    assert session_result["cliOpenCommand"] == "opencode --session ses_root_test"
    assert session_result["stopContinuationStatus"] == "root_session_detached"
    assert issue is not None
    assert issue["current_session_id"] == "ses_root_test"
    assert not (tmp_path / ".opencode/runtime/new-session-result.json").exists()
    assert not (tmp_path / ".opencode/runtime/new-session-request.json").exists()
    assert len(adapter.start_calls) == 1
    assert adapter.start_calls[0].agent == "build"


def test_select_issue_packets_for_capacity_respects_development_slot_limit(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    for issue_number in ("30", "31", "32"):
        packet_text = (
            SAMPLE_ISSUE_PACKET.replace('"42"', f'"{issue_number}"')
            .replace('issue-42', f'issue-{issue_number}')
            .replace('Demo issue', f'Issue {issue_number}')
            .replace('agent/issue-42-demo', f'agent/issue-{issue_number}-demo')
        )
        packet_path = packets_dir / f"issue-{issue_number}.yaml"
        packet_path.write_text(packet_text, encoding="utf-8")
        _ingest_issue_packet_text(tmp_path, issue_number, packet_text)

    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="running",
        command_id="cmd-running-31",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    assert orchestrator_supervisor.select_issue_packets_for_capacity(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
        development_capacity=1,
    ) == []

    selected = orchestrator_supervisor.select_issue_packets_for_capacity(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
        development_capacity=2,
    )

    assert [packet.issue_number for packet in selected] == ["32"]


def test_select_issue_candidates_for_capacity_respects_development_slot_limit(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    for issue_number in ("30", "31", "32"):
        packet_text = (
            SAMPLE_ISSUE_PACKET.replace('"42"', f'"{issue_number}"')
            .replace('issue-42', f'issue-{issue_number}')
            .replace('Demo issue', f'Issue {issue_number}')
            .replace('agent/issue-42-demo', f'agent/issue-{issue_number}-demo')
        )
        packet_path = packets_dir / f"issue-{issue_number}.yaml"
        packet_path.write_text(packet_text, encoding="utf-8")
        _ingest_issue_packet_text(tmp_path, issue_number, packet_text)

    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="running",
        command_id="cmd-running-31",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    assert select_issue_candidates_for_capacity(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
        development_capacity=1,
    ) == []

    selected = select_issue_candidates_for_capacity(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
        development_capacity=2,
    )

    assert [candidate.issue_number for candidate in selected] == ["32"]
    assert [candidate.branch for candidate in selected] == ["agent/issue-32-demo"]


def test_select_issue_packets_for_capacity_does_not_count_release_pending_against_development(tmp_path: Path):
    packets_dir = tmp_path / "docs/agents/issue-packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    for issue_number in ("30", "31", "32"):
        packet_text = (
            SAMPLE_ISSUE_PACKET.replace('"42"', f'"{issue_number}"')
            .replace('issue-42', f'issue-{issue_number}')
            .replace('Demo issue', f'Issue {issue_number}')
            .replace('agent/issue-42-demo', f'agent/issue-{issue_number}-demo')
        )
        packet_path = packets_dir / f"issue-{issue_number}.yaml"
        packet_path.write_text(packet_text, encoding="utf-8")
        _ingest_issue_packet_text(tmp_path, issue_number, packet_text)

    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="release_pending",
        command_id="cmd-release-pending-31",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="31",
        updated_at="2026-05-07T17:00:30+08:00",
        current_role="main_orchestrator",
        current_stage="release_root_execution",
        current_status="queued",
    )

    selected = orchestrator_supervisor.select_issue_packets_for_capacity(
        tmp_path,
        current_issue_number="30",
        current_parent_reference="https://github.com/example/issues/1",
        development_capacity=1,
    )

    assert [packet.issue_number for packet in selected] == ["32"]


def test_init_command_delegates_to_db_start_without_legacy_runtime_files(tmp_path: Path, capsys) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_init", resume_command="opencode --session ses_root_init"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        exit_code = orchestrator_supervisor.main(
            [
                "init",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--source-session-id",
                "ses_source_init",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    output = capsys.readouterr().out
    issue = read_issue(tmp_path, "42")
    latest = orchestrator_supervisor.read_latest_dispatch_result(tmp_path, issue_number="42")

    assert exit_code == 0
    assert "delegated supervisor init to DB-backed start-issue for issue #42" in output
    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_session_id"] == "ses_root_init"
    assert latest is not None
    assert latest.get("rootSessionID") == "ses_root_init"
    assert not (tmp_path / ".opencode/runtime/orchestrator-ledger.json").exists()
    assert not (tmp_path / ".opencode/runtime/new-session-request.json").exists()
    assert not (tmp_path / ".opencode/runtime/new-session-result.json").exists()


def test_dispatch_session_request_updates_control_plane_running_state(tmp_path: Path):
    request = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
        "requestID": "req-42",
    }
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="claimed",
        command_id="cmd-claim",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="",
    )
    _record_db_dispatch_request(tmp_path, request, created_at="2026-05-07T17:00:00+08:00")
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=successful_host_adapter(session_id="ses_root_test")), patch(
        "scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""
    ):
        orchestrator_supervisor.dispatch_request_from_db(
            cast(orchestrator_supervisor.SessionRequest, cast(object, request)),
            base_dir=tmp_path,
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_session_id"] == "ses_root_test"


def test_dispatch_session_request_appends_root_start_event(tmp_path: Path):
    request = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
        "requestID": "req-42",
    }
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="claimed",
        command_id="cmd-claim",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="",
    )
    _record_db_dispatch_request(tmp_path, request, created_at="2026-05-07T17:00:00+08:00")
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=successful_host_adapter(session_id="ses_root_test")), patch(
        "scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""
    ):
        orchestrator_supervisor.dispatch_request_from_db(
            cast(orchestrator_supervisor.SessionRequest, cast(object, request)),
            base_dir=tmp_path,
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    row = read_latest_issue_history(tmp_path, "42", entry_type="root_event")

    assert row is not None
    assert row["status"] == "root_session_started"
    assert row["session_id"] == "ses_root_test"
    assert row["session_seq"] == 1
    assert row["created_at"] == "2026-05-07T17:10:00+08:00"


def test_append_issue_event_is_idempotent_for_same_event_id(tmp_path: Path):
    orchestrator_supervisor.append_issue_event(
        tmp_path,
        event_id="issue:42:ses-root-42:root_session_started:2026-05-07T17:10:00+08:00",
        issue_number="42",
        root_session_id="ses-root-42",
        session_seq=1,
        event_type="root_session_started",
        payload={"role": "main_orchestrator"},
        created_at="2026-05-07T17:10:00+08:00",
    )
    orchestrator_supervisor.append_issue_event(
        tmp_path,
        event_id="issue:42:ses-root-42:root_session_started:2026-05-07T17:10:00+08:00",
        issue_number="42",
        root_session_id="ses-root-42",
        session_seq=1,
        event_type="root_session_started",
        payload={"role": "main_orchestrator"},
        created_at="2026-05-07T17:10:00+08:00",
    )

    row = read_latest_issue_history(tmp_path, "42", entry_type="root_event")

    assert row is not None
    assert row["request_id"] == "issue:42:ses-root-42:root_session_started:2026-05-07T17:10:00+08:00"


def test_dispatch_running_label_sync_failure_keeps_running_root_session_fenced(tmp_path: Path):
    request = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
        "requestID": "req-42",
    }
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    config_path = tmp_path / ".autodev.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = config_path.write_text('schema_version: "1.0"\nproject:\n  github_repo: example/repo\n', encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="claimed",
        command_id="cmd-claim",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="",
    )
    _record_db_dispatch_request(tmp_path, request, created_at="2026-05-07T17:00:00+08:00")
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=successful_host_adapter(session_id="ses_root_test")), patch(
        "scripts.orchestrator_supervisor.subprocess.run",
        side_effect=[
            CompletedProcess(args=["gh"], returncode=1, stdout="", stderr="label sync failed"),
            CompletedProcess(args=["gh"], returncode=0, stdout="", stderr=""),
            CompletedProcess(args=["gh"], returncode=0, stdout="", stderr=""),
        ],
    ):
        result = orchestrator_supervisor.dispatch_request_from_db(
            cast(orchestrator_supervisor.SessionRequest, cast(object, request)),
            base_dir=tmp_path,
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    row = read_latest_issue_history(tmp_path, "42", entry_type="root_event")
    artifact_refs = json.loads(str(issue["artifact_refs_json"])) if issue is not None else {}

    assert result.get("status") == "success"
    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_session_id"] == "ses_root_test"
    assert artifact_refs["rootSessionID"] == "ses_root_test"
    assert "label sync failed" in str(result.get("recommendedAction") or "")
    assert row is not None
    assert row["status"] == "root_session_started"


def test_dispatch_validation_failure_without_ledger_still_restores_ready_state(tmp_path: Path):
    request = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "req-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }
    orchestrator_supervisor.transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-claim",
        scheduler_id="scheduler:test",
        reason="claim issue",
        updated_at="2026-05-07T17:00:00+08:00",
        from_state="ready",
    )

    result = orchestrator_supervisor.dispatch_request_from_db(
        cast(orchestrator_supervisor.SessionRequest, cast(object, request)),
        base_dir=tmp_path,
        source_session_id="ses_source_test",
        updated_at="2026-05-07T17:10:00+08:00",
    )

    issue = read_issue(tmp_path, "42")
    attempt = orchestrator_supervisor.read_latest_github_sync_attempt(tmp_path, "42")

    assert result.get("status") == "rejected"
    assert issue is not None
    assert issue["state"] == "ready"
    assert attempt is not None
    assert attempt["status"] == "skipped"


def test_release_issue_execution_rejects_invalid_transition(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    try:
        orchestrator_supervisor.release_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            restore_ready_for_agent=True,
            updated_at="2026-05-07T17:01:00+08:00",
        )
    except ValueError as error:
        assert "cannot release issue #42" in str(error)
    else:
        raise AssertionError("expected release_issue_execution to reject invalid running -> ready transition")


def test_release_issue_execution_clears_session_ids_on_completed_terminal_state(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verifying",
        command_id="cmd-verifying",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-v-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        artifact_refs={
            "issueNumber": "42",
            "rootSessionID": "ses-root-42",
            "verifierSessionID": "ses-v-42",
            "status": "verifying",
        },
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value="") as sync_labels:
        orchestrator_supervisor.release_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            restore_ready_for_agent=False,
            final_state="completed",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "completed"
    assert issue["current_session_id"] == ""
    assert issue["current_role"] == ""
    assert issue["current_stage"] == ""
    assert issue["current_status"] == ""
    artifact_refs = json.loads(str(issue["artifact_refs_json"]))
    assert "rootSessionID" not in artifact_refs
    assert "verifierSessionID" not in artifact_refs
    assert "status" not in artifact_refs
    sync_labels.assert_called_once()
    sync_kwargs = sync_labels.call_args.kwargs
    assert sync_kwargs["add_labels"] == ["agent-in-progress", "pr-opened"]
    assert "ready-for-agent" in sync_kwargs["remove_labels"]
    assert "agent-dispatching" in sync_kwargs["remove_labels"]


def test_release_issue_execution_clears_session_ids_on_failed_terminal_state(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        artifact_refs={
            "issueNumber": "42",
            "rootSessionID": "ses-root-42",
            "verifierSessionID": "ses-v-42",
            "status": "root_session_started",
        },
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value="") as sync_labels:
        orchestrator_supervisor.release_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            restore_ready_for_agent=False,
            final_state="failed",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "failed"
    assert issue["current_session_id"] == ""
    assert issue["current_role"] == ""
    assert issue["current_stage"] == ""
    assert issue["current_status"] == ""
    artifact_refs = json.loads(str(issue["artifact_refs_json"]))
    assert "rootSessionID" not in artifact_refs
    assert "verifierSessionID" not in artifact_refs
    assert "status" not in artifact_refs
    sync_labels.assert_called_once()
    sync_kwargs = sync_labels.call_args.kwargs
    assert sync_kwargs["add_labels"] == ["agent-in-progress", "pr-not-opened"]
    assert "ready-for-agent" in sync_kwargs["remove_labels"]
    assert "agent-dispatching" in sync_kwargs["remove_labels"]


def test_release_issue_execution_returns_release_pending_to_verified_for_non_terminal_release_block(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        artifact_refs={
            "issueNumber": "42",
            "rootSessionID": "ses-root-42",
            "verifierSessionID": "ses-v-42",
            "status": "release_worker_running",
        },
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value="") as sync_labels:
        orchestrator_supervisor.release_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            restore_ready_for_agent=False,
            final_state="verified",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "verified"
    assert issue["current_session_id"] == ""
    assert issue["current_role"] == ""
    assert issue["current_stage"] == ""
    assert issue["current_status"] == ""
    artifact_refs = json.loads(str(issue["artifact_refs_json"]))
    assert "rootSessionID" not in artifact_refs
    assert "verifierSessionID" not in artifact_refs
    assert "status" not in artifact_refs
    sync_labels.assert_called_once()
    sync_kwargs = sync_labels.call_args.kwargs
    assert sync_kwargs["add_labels"] == ["agent-in-progress", "pr-verifier-passed"]
    assert "ready-for-agent" in sync_kwargs["remove_labels"]
    assert "agent-dispatching" in sync_kwargs["remove_labels"]


def test_release_issue_execution_completed_syncs_local_main_and_branch(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        artifact_refs={
            "issueNumber": "42",
            "branch": "agent/issue-42-demo",
            "status": "release_worker_running",
        },
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "merge": {"merged": True, "merged_sha": "abc123"},
        },
        updated_at="2026-05-07T17:00:30+08:00",
    )

    calls: list[list[str]] = []

    config_path = tmp_path / ".autodev.yaml"
    _ = config_path.write_text('schema_version: "1.0"\nproject:\n  github_repo: example/repo\n', encoding="utf-8")

    def fake_run(command, cwd, check, capture_output, text):
        calls.append(cast(list[str], command))
        return CompletedProcess(command, 0, stdout="", stderr="")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value="") as sync_labels, patch(
        "scripts.orchestrator_lifecycle.subprocess.run",
        side_effect=fake_run,
    ):
        orchestrator_supervisor.release_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            restore_ready_for_agent=False,
            final_state="completed",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "completed"
    assert issue["current_session_id"] == ""
    assert calls == [
        ["git", "rev-parse", "--is-inside-work-tree"],
        ["git", "fetch", "origin", "main"],
        ["git", "checkout", "main"],
        ["git", "pull", "--ff-only", "origin", "main"],
        ["git", "rev-parse", "--verify", "agent/issue-42-demo"],
        ["git", "checkout", "agent/issue-42-demo"],
        ["git", "merge", "--ff-only", "main"],
        ["git", "checkout", "main"],
        ["gh", "issue", "close", "42", "--repo", "example/repo", "--comment", "Closing after merged release completed in autodev."],
    ]
    sync_labels.assert_called_once()
    latest_sync = read_latest_github_sync_attempt(tmp_path, "42")
    assert latest_sync is not None
    assert latest_sync["status"] == "success"


def test_release_issue_execution_completed_raises_when_local_main_sync_fails(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "merge": {"merged": True, "merged_sha": "abc123"},
        },
        updated_at="2026-05-07T17:00:30+08:00",
    )

    def fake_run(command, cwd, check, capture_output, text):
        command_list = cast(list[str], command)
        if command_list == ["git", "pull", "--ff-only", "origin", "main"]:
            return CompletedProcess(command_list, 1, stdout="", stderr="pull failed")
        return CompletedProcess(command_list, 0, stdout="", stderr="")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""), patch(
        "scripts.orchestrator_lifecycle.subprocess.run",
        side_effect=fake_run,
    ):
        try:
            orchestrator_supervisor.release_issue_execution(
                base_dir=tmp_path,
                issue_number="42",
                restore_ready_for_agent=False,
                final_state="completed",
                updated_at="2026-05-07T17:01:00+08:00",
            )
        except RuntimeError as error:
            assert "failed local main sync after release merge" in str(error)
            assert "pull failed" in str(error)
        else:
            raise AssertionError("expected release_issue_execution to fail when local main sync fails")

    issue = read_issue(tmp_path, "42")
    latest_admin_action = read_latest_issue_history(tmp_path, "42", entry_type="admin_action")
    assert issue is not None
    assert issue["state"] == "release_pending"
    assert latest_admin_action is not None
    assert latest_admin_action["command_id"].endswith(":admin-local-main-sync-failed")
    payload = json.loads(str(latest_admin_action["payload_json"] or "{}"))
    assert payload["decision_type"] == "admin_local_main_sync_failure"
    assert "failed local main sync after release merge" in str(latest_admin_action["summary"])


def test_release_issue_execution_completed_raises_when_github_issue_close_fails(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    config_path = tmp_path / ".autodev.yaml"
    _ = config_path.write_text('schema_version: "1.0"\nproject:\n  github_repo: example/repo\n', encoding="utf-8")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "merged": True,
            "pr_number": 88,
            "merge": {"merged": True, "merged_sha": "abc123"},
        },
        updated_at="2026-05-07T17:00:30+08:00",
    )

    def fake_run(command, cwd, check, capture_output, text):
        command_list = cast(list[str], command)
        if command_list[:3] == ["gh", "issue", "close"]:
            return CompletedProcess(command_list, 1, stdout="", stderr="close failed")
        return CompletedProcess(command_list, 0, stdout="", stderr="")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""), patch(
        "scripts.orchestrator_lifecycle.subprocess.run",
        side_effect=fake_run,
    ):
        try:
            orchestrator_supervisor.release_issue_execution(
                base_dir=tmp_path,
                issue_number="42",
                restore_ready_for_agent=False,
                final_state="completed",
                updated_at="2026-05-07T17:01:00+08:00",
            )
        except RuntimeError as error:
            assert "close failed" in str(error)
        else:
            raise AssertionError("expected release_issue_execution to fail when GitHub issue close fails")

    issue = read_issue(tmp_path, "42")
    latest_admin_action = read_latest_issue_history(tmp_path, "42", entry_type="admin_action")
    latest_sync = read_latest_github_sync_attempt(tmp_path, "42")
    assert issue is not None
    assert issue["state"] == "release_pending"
    assert latest_admin_action is not None
    assert latest_admin_action["command_id"].endswith(":admin-github-issue-close-failed")
    payload = json.loads(str(latest_admin_action["payload_json"] or "{}"))
    assert payload["decision_type"] == "admin_github_issue_close_failure"
    assert latest_sync is not None
    assert latest_sync["status"] == "failed"
    assert "close failed" in str(latest_sync["last_error"])


def test_release_issue_execution_completed_skips_issue_close_for_local_seeded_issue(tmp_path: Path):
    _ingest_issue_packet_text(
        tmp_path,
        "42",
        SAMPLE_ISSUE_PACKET.replace('type: "github-issue"', 'type: "local-seeded-issue"').replace('"github"', '"local_seeded"', 1),
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "merge": {"merged": True, "merged_sha": "abc123"},
        },
        updated_at="2026-05-07T17:00:30+08:00",
    )

    calls: list[list[str]] = []

    def fake_run(command, cwd, check, capture_output, text):
        calls.append(cast(list[str], command))
        return CompletedProcess(command, 0, stdout="", stderr="")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""), patch(
        "scripts.orchestrator_lifecycle.subprocess.run",
        side_effect=fake_run,
    ):
        orchestrator_supervisor.release_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            restore_ready_for_agent=False,
            final_state="completed",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    assert all(command[:3] != ["gh", "issue", "close"] for command in calls)
    latest_sync = read_latest_github_sync_attempt(tmp_path, "42")
    assert latest_sync is not None
    assert latest_sync["status"] == "skipped"


def test_release_issue_execution_completed_cleans_issue_worktree_and_runtime_context(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    issue_worktree = tmp_path / ".opencode" / "runtime" / "issue-worktrees" / "issue-42"
    issue_worktree.mkdir(parents=True, exist_ok=True)

    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        runtime_context={"issue_worktree_path": str(issue_worktree)},
        worktree_path=str(issue_worktree),
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "merge": {"merged": True, "merged_sha": "abc123"},
        },
        updated_at="2026-05-07T17:00:30+08:00",
    )

    calls: list[list[str]] = []

    config_path = tmp_path / ".autodev.yaml"
    _ = config_path.write_text('schema_version: "1.0"\nproject:\n  github_repo: example/repo\n', encoding="utf-8")

    def fake_run(command, cwd, check, capture_output, text):
        command_list = cast(list[str], command)
        calls.append(command_list)
        if command_list[:3] == ["git", "worktree", "remove"]:
            issue_worktree.rmdir()
        return CompletedProcess(command_list, 0, stdout="", stderr="")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""), patch(
        "scripts.orchestrator_lifecycle.subprocess.run",
        side_effect=fake_run,
    ):
        orchestrator_supervisor.release_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            restore_ready_for_agent=False,
            final_state="completed",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    assert issue is not None
    assert issue["state"] == "completed"
    assert calls[-1] == ["git", "worktree", "remove", str(issue_worktree), "--force"]
    runtime_context = json.loads(str(issue["runtime_context_json"]))
    assert runtime_context.get("issue_worktree_path") == ""
    assert str(issue.get("worktree_path") or "") == ""


def test_release_issue_execution_completed_syncs_project_fields_when_project_binding_exists(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        runtime_context={
            "github_project_id": "PVT_project_1",
            "github_project_field_ids": {
                "state": "PVTF_state",
            },
        },
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "merge": {"merged": True, "merged_sha": "abc123"},
        },
        updated_at="2026-05-07T17:00:30+08:00",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""), patch(
        "scripts.orchestrator_supervisor._sync_project_fields_projection",
        return_value="",
    ) as sync_project_fields, patch(
        "scripts.orchestrator_lifecycle.subprocess.run",
        return_value=CompletedProcess(args=["git"], returncode=0, stdout="", stderr=""),
    ):
        orchestrator_supervisor.release_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            restore_ready_for_agent=False,
            final_state="completed",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    sync_project_fields.assert_called_once()
    kwargs = sync_project_fields.call_args.kwargs
    assert kwargs["issue_number"] == "42"
    assert kwargs["command_id"] == "release:42:completed:project-fields"


def test_release_issue_execution_completed_raises_when_worktree_cleanup_fails(tmp_path: Path):
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    issue_worktree = tmp_path / ".opencode" / "runtime" / "issue-worktrees" / "issue-42"
    issue_worktree.mkdir(parents=True, exist_ok=True)
    config_path = tmp_path / ".autodev.yaml"
    _ = config_path.write_text('schema_version: "1.0"\nproject:\n  github_repo: example/repo\n', encoding="utf-8")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-pending",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-release-42",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        runtime_context={"issue_worktree_path": str(issue_worktree)},
        worktree_path=str(issue_worktree),
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "merge": {"merged": True, "merged_sha": "abc123"},
        },
        updated_at="2026-05-07T17:00:30+08:00",
    )

    def fake_run(command, cwd, check, capture_output, text):
        command_list = cast(list[str], command)
        if command_list[:3] == ["git", "worktree", "remove"]:
            return CompletedProcess(command_list, 1, stdout="", stderr="remove failed")
        return CompletedProcess(command_list, 0, stdout="", stderr="")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""), patch(
        "scripts.orchestrator_lifecycle.subprocess.run",
        side_effect=fake_run,
    ):
        try:
            orchestrator_supervisor.release_issue_execution(
                base_dir=tmp_path,
                issue_number="42",
                restore_ready_for_agent=False,
                final_state="completed",
                updated_at="2026-05-07T17:01:00+08:00",
            )
        except RuntimeError as error:
            assert "remove failed" in str(error)
        else:
            raise AssertionError("expected release_issue_execution to fail when issue worktree cleanup fails")

    issue = read_issue(tmp_path, "42")
    latest_admin_action = read_latest_issue_history(tmp_path, "42", entry_type="admin_action")
    assert issue is not None
    assert issue["state"] == "release_pending"
    assert latest_admin_action is not None
    assert latest_admin_action["command_id"].endswith(":admin-issue-worktree-cleanup-failed")
    payload = json.loads(str(latest_admin_action["payload_json"] or "{}"))
    assert payload["decision_type"] == "admin_issue_worktree_cleanup_failure"


def test_retry_github_sync_command_rejects_stale_failed_attempt(tmp_path: Path):
    config_path = tmp_path / ".autodev.yaml"
    _ = config_path.write_text('schema_version: "1.0"\nproject:\n  github_repo: example/repo\n', encoding="utf-8")
    orchestrator_supervisor.record_github_sync_attempt(
        tmp_path,
        command_id="cmd-old",
        issue_number="42",
        add_labels=["agent-in-progress"],
        remove_labels=["agent-dispatching"],
        status="failed",
        updated_at="2026-05-07T17:02:00+08:00",
        last_error="sync failed",
    )
    orchestrator_supervisor.record_github_sync_attempt(
        tmp_path,
        command_id="cmd-new",
        issue_number="42",
        add_labels=["agent-in-review"],
        remove_labels=["agent-in-progress"],
        status="failed",
        updated_at="2026-05-07T17:03:00+08:00",
        last_error="newer sync failed",
    )

    try:
        orchestrator_supervisor.main([
            "retry-github-sync",
            "--base-dir",
            str(tmp_path),
            "--command-id",
            "cmd-old",
        ])
    except ValueError as error:
        assert "is stale" in str(error)
    else:
        raise AssertionError("expected retry-github-sync to reject stale failed attempt")


def test_dispatch_session_request_reports_missing_opencode_cli():
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    adapter = FakeHostAdapter(SessionStartResult(status="error", error='OpenCode CLI not found in PATH. Install or expose the core "opencode" (or "opencode-desktop") executable before running autodev dispatch.'))
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("."),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "error"
    assert "OpenCode CLI not found in PATH" in str(result.get("error", ""))


def test_dispatch_session_request_terminates_when_session_id_never_arrives():
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }
    adapter = FakeHostAdapter(SessionStartResult(status="error", error="opencode run did not emit a sessionID before timeout"))
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("."),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "error"
    assert "did not emit a sessionID" in str(result.get("error", ""))


def test_dispatch_session_request_returns_success_with_degraded_probe_status_when_same_repo_session_read_probe_fails():
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    adapter = FakeHostAdapter(
        SessionStartResult(
            status="success",
            session_id="ses_root_stdout",
            resume_hint=(
                "Open /sessions in OpenCode TUI and switch to ses_root_stdout, or run opencode --session ses_root_stdout. "
                "Warning: same-repo session_read probe failed (Session not found: ses_root_stdout); "
                "treat this root session as degraded readability and avoid immediate rollback."
            ),
            resume_command="opencode --session ses_root_stdout",
            readability_status="degraded_same_repo_probe",
            metadata={"sameRepoProbeDetail": "Session not found: ses_root_stdout"},
        )
    )
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("/tmp/demo"),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "success"
    assert result.get("rootSessionID") == "ses_root_stdout"
    assert result.get("sessionReadabilityStatus") == "degraded_same_repo_probe"
    assert "opencode --session ses_root_stdout" in str(result.get("cliOpenCommand", ""))


def test_dispatch_session_request_retries_without_source_session_on_prefill_error():
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    class RetryWithoutSourceHostAdapter(FakeHostAdapter):
        def __init__(self):
            super().__init__(
                SessionStartResult(
                    status="error",
                    error="Bad Request: This model does not support assistant message prefill. The conversation must end with a user message.",
                    retry_without_source_session=True,
                    metadata={"retryWithoutSourceSession": True},
                )
            )
            self.call_count = 0

        def start_root_session(self, context: SessionStartContext):
            self.start_calls.append(context)
            self.call_count += 1
            if self.call_count == 1:
                return SessionStartResult(
                    status="error",
                    error="Bad Request: This model does not support assistant message prefill. The conversation must end with a user message.",
                    retry_without_source_session=True,
                    metadata={"retryWithoutSourceSession": True},
                )
            return SessionStartResult(
                status="success",
                session_id="ses-retried",
                resume_hint="resume in host",
                resume_command="opencode --session ses-retried",
                readability_status="verified_same_repo_probe",
                tui_resume_command="/sessions",
                stop_continuation_status="root_session_detached",
                stop_continuation_attempts=0,
                metadata={"tuiResumeCommand": "/sessions", "stopContinuationStatus": "root_session_detached", "stopContinuationAttempts": 0},
            )

    adapter = RetryWithoutSourceHostAdapter()
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("/tmp/demo"),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "success"
    assert result.get("rootSessionID") == "ses-retried"
    assert len(adapter.start_calls) == 2
    assert adapter.start_calls[0].source_session_id == "ses_source_test"
    assert adapter.start_calls[1].source_session_id == ""


def test_dispatch_session_request_extracts_session_id_from_run_stdout_without_db_lookup():
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    adapter = FakeHostAdapter(
        SessionStartResult(
            status="success",
            session_id="ses_root_stdout",
            resume_hint="resume in host",
            resume_command="resume://ses_root_stdout",
            readability_status="verified_same_repo_probe",
            metadata={"tuiResumeCommand": "/sessions", "stopContinuationStatus": "root_session_detached", "stopContinuationAttempts": 0},
        )
    )
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("/tmp/demo"),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "success"
    assert result.get("rootSessionID") == "ses_root_stdout"
    assert result.get("sessionReadabilityStatus") == "verified_same_repo_probe"
    assert len(adapter.start_calls) == 1


def test_dispatch_session_request_falls_back_to_session_db_lookup():
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    adapter = FakeHostAdapter(
        SessionStartResult(
            status="success",
            session_id="ses_root_db",
            launch_title="Continue issue #42 on agent/issue-42-demo [request-42]",
            resume_hint="resume in host",
            resume_command="resume://ses_root_db",
            readability_status="verified_same_repo_probe",
            metadata={"tuiResumeCommand": "/sessions", "stopContinuationStatus": "root_session_detached", "stopContinuationAttempts": 0},
        )
    )
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("/tmp/demo"),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "success"
    assert result.get("rootSessionID") == "ses_root_db"
    assert result.get("launchTitle") == "Continue issue #42 on agent/issue-42-demo [request-42]"


def test_dispatch_session_request_uses_unique_launch_title_for_db_fallback_lookup():
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42abcdef",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    adapter = FakeHostAdapter(
        SessionStartResult(
            status="success",
            session_id="ses_root_db",
            resume_hint="resume in host",
            resume_command="resume://ses_root_db",
            readability_status="verified_same_repo_probe",
            metadata={"tuiResumeCommand": "/sessions", "stopContinuationStatus": "root_session_detached", "stopContinuationAttempts": 0},
        )
    )
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("/tmp/demo"),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "success"
    assert len(adapter.start_calls) == 1
    assert adapter.start_calls[0].title == "Continue issue #42 on agent/issue-42-demo [request-42abcdef]"


def test_dispatch_session_request_waits_thirty_seconds_for_db_fallback_lookup():
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    adapter = FakeHostAdapter(
        SessionStartResult(
            status="success",
            session_id="ses_root_db",
            resume_hint="resume in host",
            resume_command="resume://ses_root_db",
            readability_status="verified_same_repo_probe",
            metadata={"tuiResumeCommand": "/sessions", "stopContinuationStatus": "root_session_detached", "stopContinuationAttempts": 0},
        )
    )
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("/tmp/demo"),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "success"


def test_dispatch_session_request_preserves_explicit_cli_agent_override():
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "custom-primary",
        "prompt": "Bootstrap from the SQLite-backed control plane only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    adapter = FakeHostAdapter(
        SessionStartResult(
            status="success",
            session_id="ses_root_test",
            resume_hint="resume in host",
            resume_command="resume://ses_root_test",
            readability_status="verified_same_repo_probe",
            metadata={"tuiResumeCommand": "/sessions", "stopContinuationStatus": "root_session_detached", "stopContinuationAttempts": 0},
        )
    )
    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=adapter):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("."),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    context = adapter.start_calls[0]
    assert result.get("status") == "success"
    assert context.agent == "custom-primary"


def test_dispatch_rejects_completed_issue_without_launching_opencode(tmp_path: Path, capsys) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="completed",
        command_id="cmd-completed",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    request = build_orchestrator_request(ledger)
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor._record_dispatch_request_history(
        base_dir=tmp_path,
        request=request,
        created_at=str(request.get("createdAt") or "2026-05-07T17:00:00+08:00"),
    )

    with patch("scripts.orchestrator_supervisor.subprocess.run") as mocked_run:
        exit_code = orchestrator_supervisor.main(
            [
                "dispatch",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(capsys.readouterr().out))

    mocked_run.assert_not_called()
    assert exit_code == 0
    assert session_result["status"] == "rejected"
    assert "already completed or released" in cast(str, session_result["error"])


def test_dispatch_allows_main_orchestrator_recovery_for_failed_issue_without_claim_transition(tmp_path: Path, capsys) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    orchestrator_supervisor._sync_runtime_phase_metadata(
        base_dir=tmp_path,
        issue_number="42",
        current=cast(dict[str, str], ledger["current"]),
        attempts=cast(dict[str, int], ledger.get("attempts", {})),
        limits=cast(dict[str, int], ledger.get("limits", {})),
        last_failure=cast(dict[str, object], ledger.get("lastFailure", {})),
        workflow=cast(dict[str, object], ledger.get("workflow", {})),
        automation=cast(dict[str, object], ledger.get("automation", {})),
        artifacts=cast(dict[str, object], ledger.get("artifacts", {})),
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    queued_next_issue: dict[str, object] = {
        "issue_number": "42",
        "branch": "agent/issue-42-demo",
        "base_branch": "main",
    }
    orchestrator_supervisor._sync_runtime_phase_metadata(
        base_dir=tmp_path,
        issue_number="42",
        current=cast(dict[str, str], ledger["current"]),
        attempts=cast(dict[str, int], ledger.get("attempts", {})),
        limits=cast(dict[str, int], ledger.get("limits", {})),
        last_failure=cast(dict[str, object], ledger.get("lastFailure", {})),
        workflow=cast(dict[str, object], ledger.get("workflow", {})),
        automation=cast(dict[str, object], ledger.get("automation", {})),
        artifacts=cast(dict[str, object], ledger.get("artifacts", {})),
        queued_next_issue=queued_next_issue,
        updated_at="2026-05-07T17:10:00+08:00",
    )
    db_ledger = orchestrator_supervisor._validation_ledger_from_db(base_dir=tmp_path, issue_number="42")
    assert db_ledger is not None
    request = orchestrator_supervisor.build_session_request(
        db_ledger,
        role="main_orchestrator",
        stage="issue_selection_or_recovery",
        reason="main_orchestrator recovery for issue #42",
        title="Recover or continue after issue #42",
        decision_summary="Recovery prompt",
    )
    orchestrator_supervisor._record_dispatch_request_history(
        base_dir=tmp_path,
        request=cast(orchestrator_supervisor.SessionRequest, cast(object, request)),
        created_at="2026-05-07T17:10:00+08:00",
    )

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_recovery", resume_command="opencode --session ses_root_recovery"),
    ):
        exit_code = orchestrator_supervisor.main(
            [
                "dispatch",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(capsys.readouterr().out))
    issue = read_issue(tmp_path, "42")

    assert exit_code == 0
    assert session_result["status"] == "success"
    assert session_result["rootSessionID"] == "ses_root_recovery"
    assert issue is not None
    assert issue["state"] == "failed"


def test_reconcile_reports_subagent_decision_without_launching_root_session(tmp_path: Path, capsys) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")

    with patch("scripts.orchestrator_supervisor.subprocess.run") as mocked_run:
        exit_code = orchestrator_supervisor.main(
            [
                "reconcile",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    payload = cast(dict[str, object], json.loads(capsys.readouterr().out))

    assert exit_code == 0
    mocked_run.assert_not_called()
    assert cast(dict[str, object], payload["decision"])["action"] == "delegate_subagent"
    assert cast(dict[str, object], payload["decision"])["next_role"] == "issue_worker"


def test_advance_child_rejects_bootstrap_ledger(tmp_path: Path, capsys) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")

    exit_code = orchestrator_supervisor.main(
        [
            "advance-child",
            "--base-dir",
            str(tmp_path),
            "--issue-number",
            "42",
            "--updated-at",
            "2026-05-07T17:10:00+08:00",
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 2
    assert "advance-child requires the DB-backed issue to already be queued on a child role" in captured.err


def test_advance_child_accepts_issue_worker_and_advances_to_verifier(tmp_path: Path) -> None:
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path),
    updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    cast(dict[str, int], ledger["attempts"])["issue_worker"] = 1
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")

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
      evidence_ref: \"local\"
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
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "next_recommended_step": "Spawn verifier",
            "failure_kind": "none",
            "retryable": True,
            "completed_at": "2026-05-07T17:10:00+08:00",
            "worker_session_id": "ses",
        },
        updated_at="2026-05-07T17:10:00+08:00",
        body_text=worker_result_path.read_text(encoding="utf-8"),
    )

    exit_code = orchestrator_supervisor.main(
        [
            "advance-child",
            "--base-dir",
            str(tmp_path),
            "--issue-number",
            "42",
            "--updated-at",
            "2026-05-07T17:11:00+08:00",
        ]
    )

    issue = read_issue(tmp_path, "42")

    assert exit_code == 0
    assert issue is not None
    assert issue["current_role"] == "pr_verifier"
    assert issue["current_stage"] == "pr_verifier_execution"


def test_reconcile_verifier_marks_issue_verified_for_independent_release(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"
    evidence_path = tmp_path / "docs/agents/evidence/issue-42-pr-77.yaml"
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(
        """schema_version: \"1.0\"
kind: evidence_packet
line_cap: 60
subject:
  issue_number: \"42\"
  pr_number: \"77\"
verifier:
  verifier_session_id: \"ses-v\"
status: \"pass\"
failure_classification: {kind: \"none\", retryable: true, routed_to: \"release_worker\", root_cause_signature: \"none\"}
next_recommended_step: \"Release it\"
""",
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {"surface_qa_gate": {"status": "pass", "evidence_ref": "db:evidence:42"}},
        },
        updated_at="2026-05-07T17:11:00+08:00",
        body_text=evidence_path.read_text(encoding="utf-8"),
    )
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="running",
    command_id="cmd-running",
    updated_at="2026-05-07T17:09:00+08:00", current_session_id="ses-root-42", )
    updated_ledger, decision, _ = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:11:00+08:00",)

    issue = read_issue(tmp_path, "42")
    latest_root_event = read_latest_issue_history(tmp_path, "42", entry_type="root_event")
    latest_sync = read_latest_github_sync_attempt(tmp_path, "42")
    artifact_status = _artifact_status(issue)

    assert cast(dict[str, object], updated_ledger["current"])["role"] == "main_orchestrator"
    assert decision["action"] == "release_waiting"
    assert decision["next_role"] == "operator"
    assert decision["next_stage"] == "release_command"
    assert issue is not None
    assert issue["state"] == "verified"
    assert issue["current_session_id"] == "ses-v"
    assert cast(dict[str, object], artifact_status["evidence_packet"])["parse_ok"] is True
    assert cast(dict[str, object], artifact_status["evidence_packet"])["status"] == "pass"
    assert cast(dict[str, object], artifact_status["evidence_packet"])["pr_number"] == "77"
    assert latest_sync is not None
    assert latest_sync["projection_target"] == "labels"
    assert latest_sync["command_id"] == "reconcile:42:pr-verifier-release-waiting:labels"
    assert latest_root_event is not None
    assert latest_root_event["status"] == "root_terminal"
    assert latest_root_event["session_seq"] == 2


def test_reconcile_worker_success_syncs_project_fields_when_project_binding_exists(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, primary_workspace_root=str(tmp_path),
    updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    cast(dict[str, int], ledger["attempts"])["issue_worker"] = 1

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "next_recommended_step": "Spawn verifier",
            "failure_kind": "none",
            "retryable": True,
            "completed_at": "2026-05-07T17:10:00+08:00",
            "worker_session_id": "ses",
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _ = orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:10:30+08:00",
        runtime_context={
            "github_project_id": "PVT_project_1",
            "github_project_field_ids": {
                "state": "PVTF_state",
            },
        },
    )

    with patch("scripts.orchestrator_supervisor._sync_project_fields_projection", return_value="") as sync_project_fields:
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",)

    assert cast(dict[str, object], updated_ledger["current"])["role"] == "pr_verifier"
    assert decision["action"] == "delegate_subagent"
    assert request is None
    sync_project_fields.assert_called_once()
    kwargs = sync_project_fields.call_args.kwargs
    assert kwargs["issue_number"] == "42"
    assert kwargs["command_id"] == "reconcile:42:issue-worker-pr-opened:project-fields"


def test_reconcile_verifier_release_waiting_syncs_project_fields_when_project_binding_exists(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {"surface_qa_gate": {"status": "pass", "evidence_ref": "db:evidence:42"}},
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="running",
    command_id="cmd-running",
    updated_at="2026-05-07T17:09:00+08:00", current_session_id="ses-root-42", )
    _ = orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:10:30+08:00",
        runtime_context={
            "github_project_id": "PVT_project_1",
            "github_project_field_ids": {
                "state": "PVTF_state",
            },
        },
    )

    with patch("scripts.orchestrator_supervisor._sync_project_fields_projection", return_value="") as sync_project_fields:
        updated_ledger, decision, _ = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",)

    assert cast(dict[str, object], updated_ledger["current"])["role"] == "main_orchestrator"
    assert decision["action"] == "release_waiting"
    sync_project_fields.assert_called_once()
    kwargs = sync_project_fields.call_args.kwargs
    assert kwargs["issue_number"] == "42"
    assert kwargs["command_id"] == "reconcile:42:pr-verifier-release-waiting:project-fields"


def test_reconcile_verifier_requires_persisted_evidence_fact(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"
    evidence_path = tmp_path / "docs/agents/evidence/issue-42-pr-77.yaml"
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(
        """schema_version: \"1.0\"
kind: evidence_packet
line_cap: 60
subject:
  issue_number: \"42\"
  pr_number: \"77\"
verifier:
  verifier_session_id: \"ses-v\"
status: \"pass\"
failure_classification: {kind: \"none\", retryable: true, routed_to: \"release_worker\", root_cause_signature: \"none\"}
next_recommended_step: \"Release it\"
""",
        encoding="utf-8",
    )
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="running",
    command_id="cmd-running",
    updated_at="2026-05-07T17:09:00+08:00", current_session_id="ses-root-42", )

    with patch("scripts.orchestrator_supervisor._read_db_artifact_fact", return_value={}):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",)

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert request is None
    assert decision["next_role"] == "pr_verifier"
    assert cast(dict[str, object], updated_ledger["lastFailure"])["kind"] == "contract_invalid"
    assert cast(dict[str, object], updated_ledger["lastFailure"])["owner_role"] == "pr_verifier"
    assert cast(dict[str, object], updated_ledger["lastFailure"])["artifact_kind"] == "evidence_packet"
    assert "ended without recording evidence_packet in SQLite" in cast(str, decision["summary"])


def test_reconcile_verifier_pass_without_evidence_pr_number_retries_verifier(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "branch": "agent/issue-42-demo",
            "pr_number": "77",
            "pr_url": "https://example/pr/77",
            "next_recommended_step": "delegate_pr_verifier_subagent",
            "failure_kind": "none",
            "retryable": True,
            "completed_at": "2026-05-07T17:10:00+08:00",
            "worker_session_id": "ses-w",
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {"surface_qa_gate": {"status": "pass", "evidence_ref": "db:evidence:42"}},
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")
    latest_pr_opened = read_latest_issue_history(tmp_path, "42", entry_type="pr_opened")

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert request is None
    assert cast(dict[str, object], updated_ledger["lastFailure"])["kind"] == "contract_invalid"
    assert "without a PR number in evidence_packet" in cast(str, decision["summary"])
    assert issue is not None
    assert issue["state"] == "verifying"
    assert latest_pr_opened is None


def test_submit_evidence_packet_promotes_subject_pr_binding_to_top_level(tmp_path: Path):
    orchestrator_supervisor.ensure_issue_row(tmp_path, issue_number="42", updated_at="2026-05-07T17:00:00+08:00")

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "subject": {
                "issue": "42",
                "pr_number": "77",
                "base_branch": "main",
            },
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "gates": {"surface_qa_gate": {"status": "pass", "evidence_ref": "db:evidence:42"}},
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")
    artifact_status = _artifact_status(issue)
    evidence_packet = cast(dict[str, object], artifact_status["evidence_packet"])

    assert evidence_packet["parse_ok"] is True
    assert evidence_packet["pr_number"] == "77"
    assert evidence_packet["base_branch"] == "main"


def test_submit_evidence_packet_rejects_prose_browser_evidence_ref(tmp_path: Path):
    orchestrator_supervisor.ensure_issue_row(tmp_path, issue_number="42", updated_at="2026-05-07T17:00:00+08:00")

    with pytest.raises(ValueError, match="not a prose description"):
        _submit_artifact(
            tmp_path,
            issue_number="42",
            artifact_kind="evidence_packet",
            payload={
                "status": "pass",
                "pr_number": "77",
                "base_branch": "main",
                "verifier_session_id": "ses-v",
                "next_recommended_step": "Release it",
                "gates": {
                    "surface_qa_gate": {
                        "status": "pass",
                        "evidence_ref": "Playwright headless browser run verified happy path",
                        "evidence_kind": "browser",
                    }
                },
            },
            updated_at="2026-05-07T17:11:00+08:00",
        )


def test_reconcile_verifier_pass_with_mismatched_pr_number_retries_verifier(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-88.yaml"

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "branch": "agent/issue-42-demo",
            "pr_number": "77",
            "pr_url": "https://example/pr/77",
            "next_recommended_step": "delegate_pr_verifier_subagent",
            "failure_kind": "none",
            "retryable": True,
            "completed_at": "2026-05-07T17:10:00+08:00",
            "worker_session_id": "ses-w",
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "88",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {"surface_qa_gate": {"status": "pass", "evidence_ref": "db:evidence:42"}},
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")
    latest_pr_opened = read_latest_issue_history(tmp_path, "42", entry_type="pr_opened")

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert request is None
    assert cast(dict[str, object], updated_ledger["lastFailure"])["kind"] == "contract_invalid"
    assert "but worker_result recorded PR #77" in cast(str, decision["summary"])
    assert issue is not None
    assert issue["state"] == "verifying"
    assert latest_pr_opened is None


def test_reconcile_verifier_pass_without_browser_gate_evidence_retries_when_ui_surface_required(tmp_path: Path):
    issue_packet_text = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"frontend\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"

verifier_manual_qa:
  surface: \"browser\"
  required_gates: [\"diagnostics_and_build_gate\", \"surface_qa_gate\", \"review_gate\"]
"""
    issue_packet = parse_issue_packet_text(issue_packet_text, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", issue_packet_text)
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"
    attempts = cast(dict[str, int], ledger["attempts"])
    starting_attempts = attempts["pr_verifier"]

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "worker_session_id": "ses-w",
            "files_changed": [{"path": "frontend/src/App.tsx", "summary": "UI update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {"surface_qa_gate": {"status": "not_applicable", "evidence_ref": "db:evidence:missing-browser-gate", "evidence_kind": "unit_test"}},
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert decision["next_stage"] == "pr_verifier_execution"
    assert "browser_e2e_gate evidence" in cast(str, decision["summary"])
    assert request is None
    assert attempts["pr_verifier"] == starting_attempts + 1
    assert issue is not None
    assert issue["state"] == "verifying"


def test_reconcile_verifier_pass_with_unit_test_evidence_kind_retries_when_browser_required(tmp_path: Path):
    issue_packet_text = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"frontend\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"

verifier_manual_qa:
  surface: \"browser\"
  required_gates: [\"diagnostics_and_build_gate\", \"surface_qa_gate\", \"review_gate\"]
"""
    issue_packet = parse_issue_packet_text(issue_packet_text, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", issue_packet_text)
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"
    attempts = cast(dict[str, int], ledger["attempts"])
    starting_attempts = attempts["pr_verifier"]

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "worker_session_id": "ses-w",
            "files_changed": [{"path": "frontend/src/App.tsx", "summary": "UI update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": "smoke_test.js 19/19",
                    "evidence_kind": "unit_test",
                }
            },
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert decision["next_stage"] == "pr_verifier_execution"
    assert "browser_e2e_gate evidence" in cast(str, decision["summary"])
    assert "unit_test" in cast(str, decision["summary"])
    assert request is None
    assert attempts["pr_verifier"] == starting_attempts + 1
    assert issue is not None
    assert issue["state"] == "verifying"



    issue_packet_text = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"frontend\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"

verifier_manual_qa:
  surface: \"browser\"
  required_gates: [\"diagnostics_and_build_gate\", \"surface_qa_gate\", \"review_gate\"]
"""
    issue_packet = parse_issue_packet_text(issue_packet_text, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", issue_packet_text)
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    browser_artifact = tmp_path / "artifacts/browser-e2e/report.html"
    browser_artifact.parent.mkdir(parents=True, exist_ok=True)
    browser_artifact.write_text("ok", encoding="utf-8")

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "worker_session_id": "ses-w",
            "files_changed": [{"path": "frontend/src/App.tsx", "summary": "UI update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {"surface_qa_gate": {"status": "pass", "evidence_ref": "artifacts/browser-e2e/report.html", "evidence_kind": "browser"}},
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "release_waiting"
    assert request is None
    assert issue is not None
    assert issue["state"] == "verified"


def test_reconcile_release_worker_running_without_release_result_stays_no_change(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "running"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-running",
        updated_at="2026-05-07T17:10:00+08:00",
        current_session_id="ses-release-42",
    )

    with patch("scripts.orchestrator_supervisor._read_db_artifact_fact", return_value={}):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:11:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "no_change"
    assert decision["next_role"] == "main_orchestrator"
    assert decision["next_stage"] == "release_root_execution"
    assert "Keep the queued/running dispatch state unchanged" in cast(str, decision["summary"])
    assert request is None
    assert issue is not None
    assert issue["state"] == "release_pending"


def test_reconcile_verifier_running_without_evidence_packet_stays_no_change(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "running"}
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verifying",
        command_id="cmd-verifier-running",
        updated_at="2026-05-07T17:10:00+08:00",
        current_session_id="ses-verifier-42",
    )

    with patch("scripts.orchestrator_supervisor._read_db_artifact_fact", return_value={}):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:11:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "no_change"
    assert decision["next_role"] == "pr_verifier"
    assert decision["next_stage"] == "pr_verifier_execution"
    assert "Keep the queued/running dispatch state unchanged" in cast(str, decision["summary"])
    assert request is None
    assert issue is not None
    assert issue["state"] == "verifying"


def test_reconcile_verifier_queued_without_evidence_packet_running_outcome_stays_no_change(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"
    attempts = cast(dict[str, int], ledger["attempts"])
    starting_attempts = attempts["pr_verifier"]
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verifying",
        command_id="cmd-verifier-queued",
        updated_at="2026-05-07T17:10:00+08:00",
        current_session_id="ses-verifier-42",
    )

    class RunningOutcomeHostAdapter(FakeHostAdapter):
        def read_session_outcome(self, runtime_session_id: str):
            del runtime_session_id
            return SessionOutcome(status="running", session_id="ses-verifier-42")

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=RunningOutcomeHostAdapter(successful_host_adapter(session_id="ses-ignored").start_result)), patch(
        "scripts.orchestrator_supervisor._read_db_artifact_fact", return_value={}
    ):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:11:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "no_change"
    assert decision["next_role"] == "pr_verifier"
    assert decision["next_stage"] == "pr_verifier_execution"
    assert "Keep the queued/running dispatch state unchanged" in cast(str, decision["summary"])
    assert request is None
    assert attempts["pr_verifier"] == starting_attempts
    assert issue is not None
    assert issue["state"] == "verifying"


def test_reconcile_verifier_pass_retries_when_browser_artifact_missing(tmp_path: Path):
    issue_packet_text = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"frontend\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"

verifier_manual_qa:
  surface: \"browser\"
  required_gates: [\"diagnostics_and_build_gate\", \"surface_qa_gate\", \"review_gate\"]
"""
    issue_packet = parse_issue_packet_text(issue_packet_text, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", issue_packet_text)
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "worker_session_id": "ses-w",
            "files_changed": [{"path": "frontend/src/App.tsx", "summary": "UI update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": "artifacts/browser-e2e/missing.png",
                    "evidence_kind": "browser",
                }
            },
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert request is None
    assert "missing browser artifact" in cast(str, decision["summary"])
    assert cast(dict[str, object], updated_ledger["lastFailure"])["kind"] == "contract_invalid"
    assert issue is not None
    assert issue["state"] == "verifying"


def test_reconcile_verifier_pass_accepts_browser_artifact_in_issue_worktree(tmp_path: Path):
    issue_packet_text = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"frontend\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"

verifier_manual_qa:
  surface: \"browser\"
  required_gates: [\"diagnostics_and_build_gate\", \"surface_qa_gate\", \"review_gate\"]
"""
    issue_packet = parse_issue_packet_text(issue_packet_text, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", issue_packet_text)
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    issue_worktree = tmp_path / ".opencode/runtime/issue-worktrees/issue-42"
    cast(dict[str, object], ledger["automation"])["primaryWorkspaceRoot"] = str(issue_worktree)
    browser_artifact = issue_worktree / "evidence/issue42-browser.png"
    browser_artifact.parent.mkdir(parents=True, exist_ok=True)
    browser_artifact.write_text("ok", encoding="utf-8")

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "worker_session_id": "ses-w",
            "files_changed": [{"path": "frontend/src/App.tsx", "summary": "UI update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": "evidence/issue42-browser.png",
                    "evidence_kind": "browser",
                }
            },
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )
    _ = orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:09:30+08:00",
        runtime_context={"issue_worktree_path": str(issue_worktree)},
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "release_waiting"
    assert request is None
    assert issue is not None
    assert issue["state"] == "verified"


def test_reconcile_verifier_pass_accepts_when_browser_artifact_exists(tmp_path: Path):
    issue_packet_text = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"frontend\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"

verifier_manual_qa:
  surface: \"browser\"
  required_gates: [\"diagnostics_and_build_gate\", \"surface_qa_gate\", \"review_gate\"]
"""
    issue_packet = parse_issue_packet_text(issue_packet_text, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", issue_packet_text)
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    browser_artifact = tmp_path / "artifacts/browser-e2e/report.html"
    browser_artifact.parent.mkdir(parents=True, exist_ok=True)
    browser_artifact.write_text("ok", encoding="utf-8")

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "worker_session_id": "ses-w",
            "files_changed": [{"path": "frontend/src/App.tsx", "summary": "UI update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": "artifacts/browser-e2e/report.html",
                    "evidence_kind": "browser",
                }
            },
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "release_waiting"
    assert request is None
    assert issue is not None
    assert issue["state"] == "verified"


def test_reconcile_verifier_pass_accepts_file_uri_browser_evidence_ref(tmp_path: Path):
    issue_packet_text = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"frontend\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"

verifier_manual_qa:
  surface: \"browser\"
  required_gates: [\"diagnostics_and_build_gate\", \"surface_qa_gate\", \"review_gate\"]
"""
    issue_packet = parse_issue_packet_text(issue_packet_text, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", issue_packet_text)
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    browser_artifact = tmp_path / "artifacts/browser-e2e/report.html"
    browser_artifact.parent.mkdir(parents=True, exist_ok=True)
    browser_artifact.write_text("ok", encoding="utf-8")

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "worker_session_id": "ses-w",
            "files_changed": [{"path": "frontend/src/App.tsx", "summary": "UI update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": browser_artifact.as_uri(),
                    "evidence_kind": "browser",
                }
            },
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "release_waiting"
    assert request is None
    assert issue is not None
    assert issue["state"] == "verified"


def test_reconcile_verifier_pass_accepts_browser_uri_browser_evidence_ref(tmp_path: Path):
    issue_packet_text = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"frontend\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"

verifier_manual_qa:
  surface: \"browser\"
  required_gates: [\"diagnostics_and_build_gate\", \"surface_qa_gate\", \"review_gate\"]
"""
    issue_packet = parse_issue_packet_text(issue_packet_text, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", issue_packet_text)
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    browser_artifact = tmp_path / "artifacts/browser-e2e/report.html"
    browser_artifact.parent.mkdir(parents=True, exist_ok=True)
    browser_artifact.write_text("ok", encoding="utf-8")

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "worker_session_id": "ses-w",
            "files_changed": [{"path": "frontend/src/App.tsx", "summary": "UI update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": f"browser:{browser_artifact.as_posix()}",
                    "evidence_kind": "browser",
                }
            },
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "release_waiting"
    assert request is None
    assert issue is not None
    assert issue["state"] == "verified"


def test_reconcile_verifier_pass_retries_with_invalid_browser_evidence_ref(tmp_path: Path):
    issue_packet_text = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"frontend\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"

verifier_manual_qa:
  surface: \"browser\"
  required_gates: [\"diagnostics_and_build_gate\", \"surface_qa_gate\", \"review_gate\"]
"""
    issue_packet = parse_issue_packet_text(issue_packet_text, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", issue_packet_text)
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "worker_session_id": "ses-w",
            "files_changed": [{"path": "frontend/src/App.tsx", "summary": "UI update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": "#trace",
                    "evidence_kind": "browser",
                }
            },
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert request is None
    assert "invalid evidence_ref" in cast(str, decision["summary"])
    assert cast(dict[str, object], updated_ledger["lastFailure"])["kind"] == "contract_invalid"
    assert issue is not None
    assert issue["state"] == "verifying"


def test_reconcile_verifier_pass_does_not_validate_browser_artifact_when_not_required(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "branch": "agent/issue-42-demo",
            "worker_session_id": "ses-w",
            "files_changed": [{"path": "scripts/orchestrator_reconcile.py", "summary": "logic update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Release it",
            "failure_kind": "none",
            "retryable": True,
            "gates": {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": "artifacts/browser-e2e/missing.png",
                    "evidence_kind": "browser",
                }
            },
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_session_id="ses-root-42",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "release_waiting"
    assert request is None
    assert issue is not None
    assert issue["state"] == "verified"


def test_reconcile_release_worker_terminal_without_release_result_redispatches_subagent(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "running"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    attempts = cast(dict[str, int], ledger["attempts"])
    initial_attempts = attempts["release_worker"]
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="release_pending",
        command_id="cmd-release-running",
        updated_at="2026-05-07T17:10:00+08:00",
        current_session_id="ses-release-42",
    )

    class TerminalOutcomeHostAdapter(FakeHostAdapter):
        def read_session_outcome(self, runtime_session_id: str):
            del runtime_session_id
            return SessionOutcome(status="completed", session_id="ses-release-42")

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=TerminalOutcomeHostAdapter(successful_host_adapter(session_id="ses-ignored").start_result)), patch(
        "scripts.orchestrator_supervisor._read_db_artifact_fact", return_value={}
    ), patch("scripts.orchestrator_supervisor._transition_issue_state_if_possible"):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:11:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "main_orchestrator"
    assert decision["next_stage"] == "release_root_execution"
    assert "without recording release_result" in cast(str, decision["summary"])
    assert request is None
    assert attempts["release_worker"] == initial_attempts + 1
    assert issue is not None
    assert issue["state"] == "release_pending"


def test_reconcile_allows_early_return_without_extra_cleanup_work(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:05:00+08:00",)

    del updated_ledger, decision, request
    assert True


def test_reconcile_holds_quarantined_issue_without_auto_retry(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "done"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="quarantined",
        command_id="cmd-quarantined",
        updated_at="2026-05-07T17:04:00+08:00",
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:05:00+08:00",)

    assert updated_ledger is ledger
    assert decision["action"] == "hold_quarantined_issue"
    assert request is None


def test_reconcile_quarantines_running_issue_when_root_event_goes_stale(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="running",
    command_id="cmd-running",
    updated_at="2026-05-07T17:00:00+08:00", current_session_id="ses-root-42", )
    connection = sqlite3.connect(tmp_path / ".opencode/runtime/control-plane.sqlite3")
    try:
        _ = connection.execute(
            "UPDATE issues SET last_event_at = ? WHERE issue_number = ?",
            ("2026-05-07T17:00:00+08:00", "42"),
        )
        connection.commit()
    finally:
        connection.close()

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:16:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is ledger
    assert decision["action"] == "hold_quarantined_issue"
    assert request is None
    assert issue is not None
    assert issue["state"] == "quarantined"


def test_reconcile_quarantines_running_issue_without_root_session_id(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:01:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is ledger
    assert decision["action"] == "hold_quarantined_issue"
    assert request is None
    assert issue is not None
    assert issue["state"] == "quarantined"


def test_resume_quarantined_issue_execution_restores_running(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="quarantined",
        command_id="cmd-quarantined",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value="") as sync_labels:
        orchestrator_supervisor.resume_quarantined_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            reason="operator approved fenced resume",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "running"
    sync_labels.assert_called_once()
    sync_kwargs = sync_labels.call_args.kwargs
    assert sync_kwargs["add_labels"] == ["agent-in-progress", "pr-not-opened"]
    assert "ready-for-agent" in sync_kwargs["remove_labels"]
    assert "agent-dispatching" in sync_kwargs["remove_labels"]


def test_redispatch_quarantined_command_creates_fresh_root_session(tmp_path: Path, capsys) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="quarantined",
    command_id="cmd-quarantined",
    updated_at="2026-05-07T17:00:00+08:00", current_session_id="ses-old-root", )
    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_retry", resume_command="opencode --session ses_root_retry"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value="") as sync_labels:
        exit_code = orchestrator_supervisor.main(
            [
                "redispatch-quarantined",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--reason",
                "operator approved fresh root-session redispatch",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(capsys.readouterr().out))
    issue = read_issue(tmp_path, "42")
    artifact_refs = json.loads(str(issue["artifact_refs_json"])) if issue is not None else {}

    assert exit_code == 0
    assert session_result["status"] == "success"
    assert session_result["rootSessionID"] == "ses_root_retry"
    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_session_id"] == "ses_root_retry"
    assert artifact_refs["rootSessionID"] == "ses_root_retry"
    assert artifact_refs["status"] == "root_session_started"
    assert sync_labels.call_count >= 2
    dispatch_sync_kwargs = sync_labels.call_args_list[0].kwargs
    assert dispatch_sync_kwargs["add_labels"] == ["agent-dispatching", "pr-not-opened"]
    assert "ready-for-agent" in dispatch_sync_kwargs["remove_labels"]
    assert "agent-in-progress" in dispatch_sync_kwargs["remove_labels"]


def test_redispatch_quarantined_command_backfills_missing_workflow_paths(tmp_path: Path, capsys) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="quarantined",
        command_id="cmd-quarantined",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-old-root",
    )

    connection = sqlite3.connect(tmp_path / ".opencode/runtime/control-plane.sqlite3")
    try:
        _ = connection.execute(
            "UPDATE issues SET resume_snapshot_json = '{}' WHERE issue_number = ?",
            ("42",),
        )
        connection.commit()
    finally:
        connection.close()

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_retry", resume_command="opencode --session ses_root_retry"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        exit_code = orchestrator_supervisor.main(
            [
                "redispatch-quarantined",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--reason",
                "operator approved fresh root-session redispatch",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(capsys.readouterr().out))
    issue = read_issue(tmp_path, "42")

    assert exit_code == 0
    assert session_result["status"] == "success"
    assert session_result["rootSessionID"] == "ses_root_retry"
    assert issue is not None
    assert issue["state"] == "running"


def test_db_issue_to_ledger_backfills_missing_workflow_paths(tmp_path: Path) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")

    connection = sqlite3.connect(tmp_path / ".opencode/runtime/control-plane.sqlite3")
    try:
        _ = connection.execute(
            "UPDATE issues SET resume_snapshot_json = '{}' WHERE issue_number = ?",
            ("42",),
        )
        connection.commit()
    finally:
        connection.close()

    issue = read_issue(tmp_path, "42")
    assert issue is not None
    runtime_context = orchestrator_supervisor.read_runtime_context(tmp_path, "42")
    reconstructed = orchestrator_supervisor._db_issue_to_ledger(
        cast(dict[str, object], issue),
        runtime_context=cast(dict[str, object], runtime_context),
    )
    workflow = cast(dict[str, object], reconstructed["workflow"])

    assert workflow["workflowPolicyPath"] == orchestrator_supervisor.DEFAULT_WORKFLOW_POLICY_PATH
    assert workflow["releaseResultTemplatePath"] == orchestrator_supervisor.DEFAULT_RELEASE_RESULT_TEMPLATE_PATH


def test_db_issue_to_ledger_normalizes_missing_pr_verifier_attempts_and_limits(tmp_path: Path) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")

    connection = sqlite3.connect(tmp_path / ".opencode/runtime/control-plane.sqlite3")
    try:
        _ = connection.execute(
            "UPDATE issues SET attempts_json = ?, limits_json = ? WHERE issue_number = ?",
            (json.dumps({"issue_worker": 2}, ensure_ascii=False), json.dumps({"issue_worker": 1}, ensure_ascii=False), "42"),
        )
        connection.commit()
    finally:
        connection.close()

    issue = read_issue(tmp_path, "42")
    assert issue is not None
    runtime_context = orchestrator_supervisor.read_runtime_context(tmp_path, "42")
    reconstructed = orchestrator_supervisor._db_issue_to_ledger(
        cast(dict[str, object], issue),
        runtime_context=cast(dict[str, object], runtime_context),
    )

    attempts = cast(dict[str, int], reconstructed["attempts"])
    limits = cast(dict[str, int], reconstructed["limits"])

    assert attempts["issue_worker"] == 2
    assert limits["issue_worker"] == 1
    assert attempts["pr_verifier"] == 0
    assert limits["pr_verifier"] == orchestrator_supervisor.MAX_ROLE_ATTEMPTS


def test_redispatch_quarantined_command_does_not_write_legacy_runtime_files(tmp_path: Path) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="quarantined",
    command_id="cmd-quarantined",
    updated_at="2026-05-07T17:00:00+08:00", current_session_id="ses-old-root", )

    with patch("scripts.orchestrator_supervisor._default_host_adapter",
        return_value=successful_host_adapter(session_id="ses_root_retry", resume_command="opencode --session ses_root_retry"),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        exit_code = orchestrator_supervisor.main(
            [
                "redispatch-quarantined",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--reason",
                "operator approved fresh root-session redispatch",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    assert exit_code == 0
    assert not (tmp_path / ".opencode/runtime/new-session-request.json").exists()
    assert not (tmp_path / ".opencode/runtime/new-session-result.json").exists()


def test_redispatch_quarantined_command_restores_quarantine_when_dispatch_fails(tmp_path: Path, capsys) -> None:
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    _seed_db_issue_from_ledger(tmp_path, ledger, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="quarantined",
    command_id="cmd-quarantined",
    updated_at="2026-05-07T17:00:00+08:00", current_session_id="ses-old-root", )

    with patch(
        "scripts.orchestrator_supervisor._default_host_adapter",
        return_value=FakeHostAdapter(SessionStartResult(status="error", error='OpenCode CLI not found in PATH. Install or expose the core "opencode" (or "opencode-desktop") executable before running autodev dispatch.')),
    ), patch(
        "scripts.orchestrator_supervisor._sync_issue_progress_label",
        return_value="",
    ):
        exit_code = orchestrator_supervisor.main(
            [
                "redispatch-quarantined",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--reason",
                "operator approved fresh root-session redispatch",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(capsys.readouterr().out))
    issue = read_issue(tmp_path, "42")

    assert exit_code == 0
    assert session_result["status"] == "error"
    assert issue is not None
    assert issue["state"] == "quarantined"
    assert issue["current_session_id"] == ""


def test_dispatch_failure_quarantine_rollback_removes_ready_label(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "42", SAMPLE_ISSUE_PACKET)
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "redispatch",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "dispatch",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    with patch(
        "scripts.orchestrator_supervisor.dispatch_session_request",
        return_value={
            "status": "error",
            "error": "dispatch failed",
            "recordedAt": "2026-05-07T17:01:00+08:00",
        },
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value="") as sync_labels:
        _ = orchestrator_supervisor.dispatch_request_from_db(
            request,
            base_dir=tmp_path,
            source_session_id="ses-source",
            updated_at="2026-05-07T17:01:00+08:00",
            failure_restore_state="quarantined",
        )

    assert sync_labels.call_count == 1
    sync_kwargs = sync_labels.call_args.kwargs
    assert sync_kwargs["add_labels"] == ["agent-in-review", "pr-verifier-blocked"]
    assert "ready-for-agent" in sync_kwargs["remove_labels"]
    assert "agent-dispatching" in sync_kwargs["remove_labels"]
    assert "agent-in-progress" in sync_kwargs["remove_labels"]


def test_fail_quarantined_issue_execution_marks_issue_failed(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="quarantined",
        command_id="cmd-quarantined",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        orchestrator_supervisor.fail_quarantined_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            reason="recovery policy exhausted",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "failed"


def test_retry_failed_issue_execution_restores_ready_when_failure_is_retryable(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-stale-root",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        current_role="main_orchestrator",
        current_stage="issue_selection_or_recovery",
        current_status="queued",
        last_failure={"kind": "approval_blocked", "retryable": True, "summary": "safe to retry"},
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        payload = orchestrator_supervisor.retry_failed_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            reason="approval override fixed",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    latest_decision = orchestrator_supervisor.read_latest_decision(tmp_path, "42")

    assert payload["status"] == "success"
    assert issue is not None
    assert issue["state"] == "ready"
    assert issue["current_session_id"] == ""
    assert issue["current_role"] == ""
    assert issue["current_stage"] == ""
    assert issue["current_status"] == ""
    assert latest_decision is not None
    assert latest_decision["decision_type"] == "admin_retry_failed_issue"
    assert latest_decision["to_state"] == "ready"


def test_retry_failed_issue_execution_skips_local_main_sync_after_late_release_success(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        last_failure={"kind": "environment_worktree_constraint", "retryable": True, "summary": "safe to retry"},
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "merge": {"merged": True, "merged_sha": "abc123"},
        },
        updated_at="2026-05-07T17:00:30+08:00",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""), patch(
        "scripts.orchestrator_lifecycle.subprocess.run",
        side_effect=AssertionError("local-main sync should not run from failed->ready retry path"),
    ):
        payload = orchestrator_supervisor.retry_failed_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            reason="auto-recovery loop: retry failed issue",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert payload["status"] == "success"
    assert issue is not None
    assert issue["state"] == "ready"


def test_clear_ready_issue_session_fence_clears_stale_current_session_id(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready-fenced",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-stale-root",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        current_role="main_orchestrator",
        current_stage="orchestrator_bootstrap",
        current_status="running",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        payload = orchestrator_supervisor.clear_ready_issue_session_fence(
            base_dir=tmp_path,
            issue_number="42",
            reason="root session is no longer active",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    latest_decision = orchestrator_supervisor.read_latest_decision(tmp_path, "42")

    assert payload["status"] == "success"
    assert payload["cleared_session_id"] == "ses-stale-root"
    assert issue is not None
    assert issue["state"] == "ready"
    assert issue["current_session_id"] == ""
    assert issue["current_role"] == ""
    assert issue["current_stage"] == ""
    assert issue["current_status"] == ""
    assert latest_decision is not None
    assert latest_decision["decision_type"] == "admin_clear_ready_session_fence"
    assert latest_decision["from_state"] == "ready"
    assert latest_decision["to_state"] == "ready"


def test_clear_ready_issue_session_fence_rejects_unfenced_ready_issue(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    try:
        orchestrator_supervisor.clear_ready_issue_session_fence(
            base_dir=tmp_path,
            issue_number="42",
            reason="no-op repair should be rejected",
            updated_at="2026-05-07T17:01:00+08:00",
        )
    except ValueError as error:
        assert "does not have a current session fence" in str(error)
    else:
        raise AssertionError("expected unfenced ready issue to be rejected")


def test_retry_failed_issue_execution_ignores_stale_issue_artifacts(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        last_failure={"kind": "approval_blocked", "retryable": True, "summary": "safe to retry"},
    )
    stale_worker = tmp_path / "docs/agents/worker-results/issue-42.yaml"
    stale_handoff = tmp_path / "docs/agents/handoffs/issue-42.yaml"
    stale_evidence = tmp_path / "docs/agents/evidence/issue-42-pr-77.yaml"
    stale_release = tmp_path / "docs/agents/release-results/issue-42-pr-77.yaml"
    for path in [stale_worker, stale_handoff, stale_evidence, stale_release]:
        path.parent.mkdir(parents=True, exist_ok=True)
        _ = path.write_text("stale\n", encoding="utf-8")

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        _ = orchestrator_supervisor.retry_failed_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            reason="approval override fixed",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = orchestrator_supervisor.read_issue(tmp_path, "42")

    assert issue is not None
    assert str(issue.get("state") or "") == "ready"
    assert stale_worker.exists()
    assert stale_handoff.exists()
    assert stale_evidence.exists()
    assert stale_release.exists()


def test_retry_failed_command_rejects_non_retryable_issue(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
        last_failure={"kind": "policy_blocked", "retryable": False, "summary": "manual follow-up"},
    )

    try:
        orchestrator_supervisor.main([
            "retry-failed",
            "--base-dir",
            str(tmp_path),
            "--issue-number",
            "42",
            "--reason",
            "operator retry",
        ])
    except ValueError as error:
        assert "not retryable" in str(error)
    else:
        raise AssertionError("expected retry-failed to reject non-retryable issues")


def test_quarantine_command_marks_issue_quarantined(tmp_path: Path):
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        exit_code = orchestrator_supervisor.main(
            [
                "quarantine",
                "--base-dir",
                str(tmp_path),
                "--issue-number",
                "42",
                "--reason",
                "heartbeat timeout",
                "--updated-at",
                "2026-05-07T17:01:00+08:00",
            ]
        )

    issue = read_issue(tmp_path, "42")

    assert exit_code == 0
    assert issue is not None
    assert issue["state"] == "quarantined"


def test_reconcile_bootstrap_rebuilds_running_state_from_db_dispatch_result_root_id(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="dispatching",
        command_id="cmd-dispatching",
        updated_at="2026-05-07T17:04:00+08:00",
    )
    orchestrator_supervisor._record_dispatch_result_history(
        base_dir=tmp_path,
        session_result={
            "status": "success",
            "rootSessionID": "ses-root-42",
            "recordedAt": "2026-05-07T17:04:30+08:00",
            "issueNumber": "42",
            "branch": "agent/issue-42-demo",
            "sourceSessionID": "ses-source-42",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "reason": "bootstrap dispatch result",
            "title": "Continue issue #42 on agent/issue-42-demo",
        },
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:05:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert cast(dict[str, object], updated_ledger["current"])["role"] == "issue_worker"
    assert decision["next_role"] == "issue_worker"
    assert request is None
    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_session_id"] == "ses-root-42"


def test_record_dispatch_result_history_does_not_collide_on_shared_source_session_id(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    issue_43_packet = parse_issue_packet_text(
        SAMPLE_ISSUE_PACKET.replace('number: "42"', 'number: "43"').replace("issue-42-demo", "issue-43-demo"),
        "docs/agents/issue-packets/issue-43.yaml",
    )
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_43_packet, updated_at="2026-05-07T17:00:00+08:00")

    orchestrator_supervisor._record_dispatch_result_history(
        base_dir=tmp_path,
        session_result={
            "status": "error",
            "error": "dispatch failed for 42",
            "recordedAt": "2026-05-07T17:04:30+08:00",
            "issueNumber": "42",
            "branch": "agent/issue-42-demo",
            "sourceSessionID": "workspace_reconcile",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "reason": "orchestrator bootstrap continuation for issue #42",
            "title": "Continue issue #42 on agent/issue-42-demo",
        },
    )
    orchestrator_supervisor._record_dispatch_result_history(
        base_dir=tmp_path,
        session_result={
            "status": "error",
            "error": "dispatch failed for 43",
            "recordedAt": "2026-05-07T17:04:31+08:00",
            "issueNumber": "43",
            "branch": "agent/issue-43-demo",
            "sourceSessionID": "workspace_reconcile",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "reason": "orchestrator bootstrap continuation for issue #43",
            "title": "Continue issue #43 on agent/issue-43-demo",
        },
    )

    latest_42 = orchestrator_supervisor.read_latest_dispatch_result(tmp_path, issue_number="42")
    latest_43 = orchestrator_supervisor.read_latest_dispatch_result(tmp_path, issue_number="43")

    assert latest_42 is not None
    assert latest_43 is not None
    assert latest_42.get("issueNumber") == "42"
    assert latest_42.get("error") == "dispatch failed for 42"
    assert latest_43.get("issueNumber") == "43"
    assert latest_43.get("error") == "dispatch failed for 43"


def test_start_issue_records_ready_rollback_reason_on_dispatch_error(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    with patch(
        "scripts.orchestrator_supervisor.dispatch_session_request",
        return_value={
            "status": "error",
            "error": "dispatch failed",
            "recordedAt": "2026-05-07T17:10:00+08:00",
            "issueNumber": "42",
            "branch": "agent/issue-42-demo",
            "sourceSessionID": "workspace_reconcile",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "reason": "orchestrator bootstrap continuation for issue #42",
            "title": "Continue issue #42 on agent/issue-42-demo",
        },
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        _ = orchestrator_supervisor.start_issue(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="workspace_reconcile",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    rollback = read_latest_issue_history(tmp_path, "42", entry_type="admin_action")
    assert issue is not None
    assert issue["state"] == "ready"
    assert rollback is not None
    payload = json.loads(str(rollback["payload_json"] or "{}"))
    assert payload["rollback_reason"] == "dispatch_error:dispatch failed"
    assert payload["restored_ready_for_agent"] is True


def test_start_issue_treats_error_with_root_session_as_degraded_success_and_skips_ready_rollback(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    with patch(
        "scripts.orchestrator_supervisor.dispatch_session_request",
        return_value={
            "status": "error",
            "error": "root session ses_root_stdout was created but failed same-repo session_read probe: Session not found: ses_root_stdout",
            "recordedAt": "2026-05-07T17:10:00+08:00",
            "issueNumber": "42",
            "branch": "agent/issue-42-demo",
            "sourceSessionID": "workspace_reconcile",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "reason": "orchestrator bootstrap continuation for issue #42",
            "title": "Continue issue #42 on agent/issue-42-demo",
            "rootSessionID": "ses_root_stdout",
            "sessionReadabilityStatus": "degraded_same_repo_probe",
        },
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        result = orchestrator_supervisor.start_issue(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="workspace_reconcile",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    rollback = read_latest_issue_history(tmp_path, "42", entry_type="admin_action")
    degraded_event = read_latest_issue_history(tmp_path, "42", entry_type="admin_action")
    assert result.get("status") == "success"
    assert result.get("rootSessionID") == "ses_root_stdout"
    assert result.get("sessionReadabilityStatus") == "degraded_same_repo_probe"
    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_session_id"] == "ses_root_stdout"
    assert rollback is not None
    rollback_payload = json.loads(str(rollback["payload_json"] or "{}"))
    assert rollback_payload.get("decision_type") == "same_repo_probe_degraded"
    assert degraded_event is not None


def test_start_issue_blocks_rollback_when_latest_dispatch_result_has_active_root(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor._record_dispatch_result_history(
        base_dir=tmp_path,
        session_result={
            "status": "success",
            "rootSessionID": "ses-live-root",
            "recordedAt": "2026-05-07T17:09:59+08:00",
            "issueNumber": "42",
            "branch": "agent/issue-42-demo",
            "sourceSessionID": "workspace_reconcile",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "reason": "bootstrap dispatch result",
            "title": "Continue issue #42 on agent/issue-42-demo",
        },
    )

    with patch(
        "scripts.orchestrator_supervisor.dispatch_session_request",
        return_value={
            "status": "error",
            "error": "dispatch failed",
            "recordedAt": "2026-05-07T17:10:00+08:00",
            "issueNumber": "42",
            "branch": "agent/issue-42-demo",
            "sourceSessionID": "workspace_reconcile",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "reason": "orchestrator bootstrap continuation for issue #42",
            "title": "Continue issue #42 on agent/issue-42-demo",
        },
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        try:
            _ = orchestrator_supervisor.start_issue(
                base_dir=tmp_path,
                issue_number="42",
                source_session_id="workspace_reconcile",
                updated_at="2026-05-07T17:10:00+08:00",
            )
        except RuntimeError as error:
            assert "rollback blocked" in str(error)
            assert "ses-live-root" in str(error)
        else:
            raise AssertionError("expected rollback guard to block")


def test_start_issue_rejects_target_branch_equal_to_base_branch_and_rolls_back_ready(tmp_path: Path):
    same_branch_packet = SAMPLE_ISSUE_PACKET.replace('agent/issue-42-demo', 'main')
    issue_packet = parse_issue_packet_text(same_branch_packet, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    try:
        orchestrator_supervisor.start_issue(
            base_dir=tmp_path,
            issue_number="42",
            source_session_id="workspace_reconcile",
            updated_at="2026-05-07T17:10:00+08:00",
        )
    except RuntimeError as error:
        assert "must differ from base branch" in str(error)
    else:
        raise AssertionError("expected branch/base-branch guardrail to reject start")

    issue = read_issue(tmp_path, "42")
    rollback = read_latest_issue_history(tmp_path, "42", entry_type="admin_action")
    assert issue is not None
    assert issue["state"] == "ready"
    assert issue["last_command_id"] == "start-issue:42:worktree-rollback"
    assert issue["current_session_id"] == ""
    assert rollback is not None
    payload = json.loads(str(rollback["payload_json"] or "{}"))
    assert payload["decision_type"] == "admin_worktree_prepare_failure"
    assert payload["restored_ready_for_agent"] is True
    assert str(payload["rollback_reason"]).startswith("worktree_prepare_error:")


def test_start_issue_rolls_back_ready_when_worktree_prepare_fails(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    with patch(
        "scripts.orchestrator_supervisor._ensure_issue_worktree",
        side_effect=RuntimeError("failed to prepare worktree for issue #42: simulated git failure"),
    ):
        try:
            orchestrator_supervisor.start_issue(
                base_dir=tmp_path,
                issue_number="42",
                source_session_id="workspace_reconcile",
                updated_at="2026-05-07T17:10:00+08:00",
            )
        except RuntimeError as error:
            assert "simulated git failure" in str(error)
        else:
            raise AssertionError("expected start_issue to fail when worktree preparation fails")

    issue = read_issue(tmp_path, "42")
    rollback = read_latest_issue_history(tmp_path, "42", entry_type="admin_action")
    assert issue is not None
    assert issue["state"] == "ready"
    assert issue["last_command_id"] == "start-issue:42:worktree-rollback"
    assert issue["current_session_id"] == ""
    last_failure = json.loads(str(issue.get("last_failure_json") or "{}"))
    assert last_failure["kind"] == "worktree_prepare_failed"
    assert last_failure["retryable"] is True
    assert "simulated git failure" in str(last_failure["summary"])
    assert rollback is not None
    payload = json.loads(str(rollback["payload_json"] or "{}"))
    assert payload["decision_type"] == "admin_worktree_prepare_failure"
    assert payload["restored_ready_for_agent"] is True
    assert str(payload["rollback_reason"]).startswith("worktree_prepare_error:")


def test_reconcile_issue_worker_without_root_session_evidence_keeps_dispatching_state(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="dispatching",
        command_id="cmd-dispatching",
        updated_at="2026-05-07T17:04:00+08:00",
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:05:00+08:00",)

    issue = read_issue(tmp_path, "42")
    current = cast(dict[str, object], updated_ledger["current"])

    assert current == {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    assert decision["action"] == "no_change"
    assert request is None
    assert issue is not None
    assert issue["state"] == "dispatching"


def test_reconcile_quarantines_stale_dispatching_issue_worker_without_root_session_evidence(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="dispatching",
        command_id="cmd-dispatching",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:16:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is ledger
    assert decision["action"] == "hold_quarantined_issue"
    assert request is None
    assert issue is not None
    assert issue["state"] == "quarantined"


def test_reconcile_auto_recovers_quarantined_dispatching_orphan_to_ready(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="dispatching",
        command_id="cmd-dispatching",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        _ = reconcile_ledger(
            ledger,
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:16:00+08:00",
        )

    issue_after_first = read_issue(tmp_path, "42")
    assert issue_after_first is not None
    assert issue_after_first["state"] == "quarantined"

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:17:00+08:00",
        )

    issue_after_second = read_issue(tmp_path, "42")
    assert updated_ledger is ledger
    assert decision["action"] == "no_change"
    assert request is None
    assert issue_after_second is not None
    assert issue_after_second["state"] == "ready"
    assert issue_after_second["current_session_id"] == ""


def test_reconcile_does_not_auto_recover_quarantined_dispatching_when_dispatch_result_has_live_root(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="dispatching",
        command_id="cmd-dispatching",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        _ = reconcile_ledger(
            ledger,
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:16:00+08:00",
        )

    orchestrator_supervisor._record_dispatch_result_history(
        base_dir=tmp_path,
        session_result={
            "status": "success",
            "rootSessionID": "ses-root-42",
            "recordedAt": "2026-05-07T17:16:30+08:00",
            "issueNumber": "42",
            "branch": "agent/issue-42-demo",
            "sourceSessionID": "workspace_reconcile",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "reason": "bootstrap dispatch result",
            "title": "Continue issue #42 on agent/issue-42-demo",
        },
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:17:00+08:00",
        )

    issue_after_second = read_issue(tmp_path, "42")
    assert updated_ledger is ledger
    assert decision["action"] == "hold_quarantined_issue"
    assert request is None
    assert issue_after_second is not None
    assert issue_after_second["state"] == "quarantined"


def test_reconcile_recovers_stale_claimed_issue_without_dispatch_evidence_to_ready(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="claimed",
        command_id="cmd-claimed",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:16:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is ledger
    assert decision["action"] == "no_change"
    assert request is None
    assert issue is not None
    assert issue["state"] == "ready"
    assert issue["current_session_id"] == ""


def test_reconcile_does_not_recover_recent_claimed_issue_without_dispatch_evidence(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="claimed",
        command_id="cmd-claimed",
        updated_at="2026-05-07T17:12:00+08:00",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:16:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is ledger
    assert decision["action"] == "no_change"
    assert request is None
    assert issue is not None
    assert issue["state"] == "claimed"


def test_reconcile_workspace_releases_stale_claimed_capacity_before_starting_ready_issue(tmp_path: Path):
    claimed_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ready_packet = parse_issue_packet_text(
        SAMPLE_ISSUE_PACKET.replace("number: 42", "number: 43").replace("branch: agent/issue-42-demo", "branch: agent/issue-43-demo"),
        "docs/agents/issue-packets/issue-43.yaml",
    )
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, claimed_packet, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, ready_packet, updated_at="2026-05-07T17:00:00+08:00")

    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="claimed",
        command_id="cmd-claimed",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="43",
        state="ready",
        command_id="cmd-ready",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch("scripts.orchestrator_supervisor._default_host_adapter", return_value=successful_host_adapter(session_id="ses-root-43")), patch(
        "scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""
    ):
        payload = orchestrator_supervisor.reconcile_workspace_from_db(
            base_dir=tmp_path,
            updated_at="2026-05-07T17:16:00+08:00",
        )

    claimed_issue = read_issue(tmp_path, "42")
    started_issue = read_issue(tmp_path, "43")

    assert claimed_issue is not None
    assert claimed_issue["state"] != "claimed"
    assert started_issue is not None
    started = cast(list[dict[str, object]], payload["started_issues"])
    assert started


def test_reconcile_quarantines_stale_queued_pr_verifier_with_stale_root_heartbeat(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="verifying",
    command_id="cmd-verifying",
    updated_at="2026-05-07T17:00:00+08:00", current_session_id="ses-root-42", )
    connection = sqlite3.connect(tmp_path / ".opencode/runtime/control-plane.sqlite3")
    try:
        _ = connection.execute(
            "UPDATE issues SET last_event_at = ? WHERE issue_number = ?",
            ("2026-05-07T17:00:00+08:00", "42"),
        )
        connection.commit()
    finally:
        connection.close()

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:16:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is ledger
    assert decision["action"] == "hold_quarantined_issue"
    assert request is None
    assert issue is not None
    assert issue["state"] == "quarantined"


def test_reconcile_quarantines_stale_queued_release_worker_with_stale_root_heartbeat(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-77.yaml"
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="release_pending",
    command_id="cmd-verifying",
    updated_at="2026-05-07T17:00:00+08:00", current_session_id="ses-root-42", )
    connection = sqlite3.connect(tmp_path / ".opencode/runtime/control-plane.sqlite3")
    try:
        _ = connection.execute(
            "UPDATE issues SET last_event_at = ? WHERE issue_number = ?",
            ("2026-05-07T17:00:00+08:00", "42"),
        )
        connection.commit()
    finally:
        connection.close()

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:16:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is ledger
    assert decision["action"] == "hold_quarantined_issue"
    assert request is None
    assert issue is not None
    assert issue["state"] == "quarantined"


def test_reconcile_root_heartbeat_timeout_respects_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTODEV_ROOT_HEARTBEAT_TIMEOUT_SECONDS", "1800")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verifying",
        command_id="cmd-verifying",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )
    connection = sqlite3.connect(tmp_path / ".opencode/runtime/control-plane.sqlite3")
    try:
        _ = connection.execute(
            "UPDATE issues SET last_event_at = ? WHERE issue_number = ?",
            ("2026-05-07T17:00:00+08:00", "42"),
        )
        connection.commit()
    finally:
        connection.close()

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:16:00+08:00",
    )

    issue = read_issue(tmp_path, "42")
    assert updated_ledger is ledger
    assert decision["action"] != "hold_quarantined_issue"
    assert request is None
    assert issue is not None
    assert issue["state"] == "verifying"


def test_reconcile_does_not_quarantine_on_first_same_repo_probe_degraded_start(tmp_path: Path) -> None:
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )
    orchestrator_supervisor._record_dispatch_result_history(
        base_dir=tmp_path,
        session_result={
            "status": "success",
            "rootSessionID": "ses-root-42",
            "recordedAt": "2026-05-07T17:10:00+08:00",
            "issueNumber": "42",
            "branch": "agent/issue-42-demo",
            "sourceSessionID": "workspace_reconcile",
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "reason": "bootstrap dispatch result",
            "title": "Continue issue #42 on agent/issue-42-demo",
            "sessionReadabilityStatus": "degraded_same_repo_probe",
        },
    )
    orchestrator_supervisor._record_same_repo_probe_degraded_event(
        base_dir=tmp_path,
        issue_number="42",
        session_result=cast(
            orchestrator_supervisor.SessionResult,
            cast(object, {
                "status": "success",
                "rootSessionID": "ses-root-42",
                "recordedAt": "2026-05-07T17:10:00+08:00",
                "issueNumber": "42",
                "sessionReadabilityStatus": "degraded_same_repo_probe",
                "error": "same-repo probe timeout",
            }),
        ),
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")
    assert updated_ledger is ledger
    assert decision["action"] == "no_change"
    assert request is None
    assert issue is not None
    assert issue["state"] == "running"


def test_reconcile_quarantines_on_second_consecutive_same_repo_probe_degraded_start(tmp_path: Path) -> None:
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )
    first_result: dict[str, object] = {
        "status": "success",
        "rootSessionID": "ses-root-41",
        "recordedAt": "2026-05-07T17:09:00+08:00",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
        "sourceSessionID": "workspace_reconcile",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "reason": "bootstrap dispatch result",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "sessionReadabilityStatus": "degraded_same_repo_probe",
    }
    second_result: dict[str, object] = {
        "status": "success",
        "rootSessionID": "ses-root-42",
        "recordedAt": "2026-05-07T17:10:00+08:00",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
        "sourceSessionID": "workspace_reconcile",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "reason": "bootstrap dispatch result",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "sessionReadabilityStatus": "degraded_same_repo_probe",
    }
    orchestrator_supervisor._record_dispatch_result_history(
        base_dir=tmp_path,
        session_result=cast(orchestrator_supervisor.SessionResult, cast(object, first_result)),
    )
    orchestrator_supervisor._record_dispatch_result_history(
        base_dir=tmp_path,
        session_result=cast(orchestrator_supervisor.SessionResult, cast(object, second_result)),
    )
    orchestrator_supervisor._record_same_repo_probe_degraded_event(
        base_dir=tmp_path,
        issue_number="42",
        session_result=cast(orchestrator_supervisor.SessionResult, cast(object, {**first_result, "error": "same-repo probe timeout #1"})),
    )
    orchestrator_supervisor._record_same_repo_probe_degraded_event(
        base_dir=tmp_path,
        issue_number="42",
        session_result=cast(orchestrator_supervisor.SessionResult, cast(object, {**second_result, "error": "same-repo probe timeout #2"})),
    )

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:11:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    assert updated_ledger is ledger
    assert decision["action"] == "hold_quarantined_issue"
    assert request is None
    assert issue is not None
    assert issue["state"] == "quarantined"


def test_reconcile_same_repo_probe_degraded_respects_env_override_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTODEV_SAME_REPO_PROBE_DEGRADED_LIMIT", "3")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )
    first_result: dict[str, object] = {
        "status": "success",
        "rootSessionID": "ses-root-41",
        "recordedAt": "2026-05-07T17:09:00+08:00",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
        "sourceSessionID": "workspace_reconcile",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "reason": "bootstrap dispatch result",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "sessionReadabilityStatus": "degraded_same_repo_probe",
    }
    second_result: dict[str, object] = {
        "status": "success",
        "rootSessionID": "ses-root-42",
        "recordedAt": "2026-05-07T17:10:00+08:00",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
        "sourceSessionID": "workspace_reconcile",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "reason": "bootstrap dispatch result",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "sessionReadabilityStatus": "degraded_same_repo_probe",
    }
    orchestrator_supervisor._record_dispatch_result_history(
        base_dir=tmp_path,
        session_result=cast(orchestrator_supervisor.SessionResult, cast(object, first_result)),
    )
    orchestrator_supervisor._record_dispatch_result_history(
        base_dir=tmp_path,
        session_result=cast(orchestrator_supervisor.SessionResult, cast(object, second_result)),
    )
    orchestrator_supervisor._record_same_repo_probe_degraded_event(
        base_dir=tmp_path,
        issue_number="42",
        session_result=cast(orchestrator_supervisor.SessionResult, cast(object, {**first_result, "error": "same-repo probe timeout #1"})),
    )
    orchestrator_supervisor._record_same_repo_probe_degraded_event(
        base_dir=tmp_path,
        issue_number="42",
        session_result=cast(orchestrator_supervisor.SessionResult, cast(object, {**second_result, "error": "same-repo probe timeout #2"})),
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")
    assert updated_ledger is ledger
    assert decision["action"] == "no_change"
    assert request is None
    assert issue is not None
    assert issue["state"] == "running"


def test_reconcile_release_success_keeps_issue_completed(tmp_path: Path):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    issue_31 = issue_packets_dir / "issue-31.yaml"
    issue_31.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #29 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-29.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "success"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="31",
    state="release_pending",
    command_id="cmd-verifying",
    updated_at="2026-05-07T17:19:00+08:00", current_session_id="ses-v", )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:21:00+08:00",)

    issue = read_issue(tmp_path, "31")

    assert updated_ledger is not None
    assert decision["action"] in {"queue_next_session", "queue_next_issue"}
    assert request is not None
    assert issue is not None
    assert issue["state"] == "completed"
    assert issue["current_session_id"] == ""


def test_reconcile_ignores_stale_session_result_for_different_issue_after_queue_next_issue(tmp_path: Path):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    issue_31 = issue_packets_dir / "issue-31.yaml"
    issue_31.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    issue_32 = issue_packets_dir / "issue-32.yaml"
    issue_32.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"32"').replace('issue-42', 'issue-32').replace('Demo issue', 'Issue 32').replace('agent/issue-42-demo', 'agent/issue-32-demo'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "31", issue_31.read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "32", issue_32.read_text(encoding="utf-8"))
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "success"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="31",
    state="release_pending",
    command_id="cmd-verifying",
    updated_at="2026-05-07T17:19:00+08:00", current_session_id="ses-v", )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:21:00+08:00",)

    next_ledger, next_decision, next_request = reconcile_ledger(cast(dict[str, object], updated_ledger), artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:22:00+08:00",)

    next_issue = cast(dict[str, object], next_ledger["issue"])

    assert decision["action"] in {"queue_next_session", "queue_next_issue"}
    assert request is not None
    assert next_issue["number"] == "32"
    assert next_decision["action"] == "delegate_subagent"
    assert next_decision["next_role"] == "issue_worker"
    assert next_request is None
    assert cast(dict[str, object], next_ledger.get("lastSessionResult", {})) == {}


def test_reconcile_recovery_consumes_persisted_selected_next_issue_before_reselecting(tmp_path: Path):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    issue_31 = issue_packets_dir / "issue-31.yaml"
    issue_32 = issue_packets_dir / "issue-32.yaml"
    issue_33 = issue_packets_dir / "issue-33.yaml"
    issue_31.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    issue_32.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"32"').replace('issue-42', 'issue-32').replace('Demo issue', 'Issue 32').replace('agent/issue-42-demo', 'agent/issue-32-demo'),
        encoding="utf-8",
    )
    issue_33.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"33"').replace('issue-42', 'issue-33').replace('Demo issue', 'Issue 33').replace('agent/issue-42-demo', 'agent/issue-33-demo'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "31", issue_31.read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "32", issue_32.read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "33", issue_33.read_text(encoding="utf-8"))
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    selected_issue_packet = parse_issue_packet_text(issue_32.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-32.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    ledger["queuedNextIssue"] = {
        "issue_number": selected_issue_packet.issue_number,
        "branch": selected_issue_packet.branch,
        "base_branch": selected_issue_packet.base_branch,
    }
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="completed",
        command_id="cmd-completed",
        updated_at="2026-05-07T17:21:00+08:00",
    )
    next_ledger, next_decision, next_request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:22:00+08:00",)

    next_issue = cast(dict[str, object], next_ledger["issue"])

    assert next_issue["number"] == "32"
    assert next_decision["action"] == "queue_next_issue"
    assert next_request is not None
    assert next_request["issueNumber"] == "32"
    assert next_ledger.get("queuedNextIssue") is None


def test_reconcile_recovery_discards_persisted_selected_issue_when_it_is_no_longer_ready(tmp_path: Path):
    issue_packets_dir = tmp_path / "docs/agents/issue-packets"
    issue_packets_dir.mkdir(parents=True, exist_ok=True)
    issue_31 = issue_packets_dir / "issue-31.yaml"
    issue_32 = issue_packets_dir / "issue-32.yaml"
    issue_33 = issue_packets_dir / "issue-33.yaml"
    issue_31.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        encoding="utf-8",
    )
    issue_32.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"32"')
        .replace('issue-42', 'issue-32')
        .replace('Demo issue', 'Issue 32')
        .replace('agent/issue-42-demo', 'agent/issue-32-demo')
        .replace('labels: [ready-for-agent]', 'labels: [agent-in-progress]'),
        encoding="utf-8",
    )
    issue_33.write_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"33"').replace('issue-42', 'issue-33').replace('Demo issue', 'Issue 33').replace('agent/issue-42-demo', 'agent/issue-33-demo'),
        encoding="utf-8",
    )
    _ingest_issue_packet_text(tmp_path, "31", issue_31.read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "32", issue_32.read_text(encoding="utf-8"))
    _ingest_issue_packet_text(tmp_path, "33", issue_33.read_text(encoding="utf-8"))
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    stale_selected_issue_packet = parse_issue_packet_text(issue_32.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-32.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    ledger["queuedNextIssue"] = {
        "issue_number": stale_selected_issue_packet.issue_number,
        "branch": stale_selected_issue_packet.branch,
        "base_branch": stale_selected_issue_packet.base_branch,
    }
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="completed",
        command_id="cmd-completed",
        updated_at="2026-05-07T17:21:00+08:00",
    )
    next_ledger, next_decision, next_request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:22:00+08:00",)

    next_issue = cast(dict[str, object], next_ledger["issue"])

    assert next_issue["number"] == "33"
    assert next_decision["action"] == "queue_next_issue"
    assert next_request is not None
    assert next_request["issueNumber"] == "33"
    assert next_ledger.get("queuedNextIssue") is None


def test_reconcile_skips_runtime_phase_rebuild_for_completed_issue(tmp_path: Path):
    issue_packet = parse_issue_packet_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'),
        "docs/agents/issue-packets/issue-31.yaml",
    )
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00",)
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "pass"}
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-31-pr-88.yaml"

    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )

    evidence_path = tmp_path / "docs/agents/evidence/issue-31-pr-88.yaml"
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(
        'schema_version: "1.0"\nkind: evidence_packet\nline_cap: 60\nraw_evidence_policy: index_only_manifest_no_raw_logs_or_traces\n\nsubject:\n  type: "issue_pr"\n  issue_number: "31"\n  pr_number: "88"\n  phase: "verification"\n  branch: "agent/issue-31-demo"\n  sha: "abc"\n\nverifier:\n  actor: "OpenCode pr_verifier"\n  actor_role: "pr_verifier"\n  verifier_session_id: "ses-v"\n  started_at: "2026-05-07T17:10:00+08:00"\n  completed_at: "2026-05-07T17:20:00+08:00"\n\nproof_of_separation:\n  worker_result_ref: "docs/agents/worker-results/issue-31.yaml"\n  worker_actor: "OpenCode issue_worker"\n  worker_session_id: "ses-w"\n  verifier_actor: "OpenCode pr_verifier"\n  verifier_session_id: "ses-v"\n  verifier_is_distinct_from_worker: true\n  verifier_read_worker_result_only: false\n\nstatus: "pass"\nfailure_classification: {kind: "none", retryable: true, routed_to: "none", root_cause_signature: "none"}\n\ntest_case_verification: {applies: false, test_case_id: "", target_case: "n/a", regression_bucket: "n/a", failure_signature: "none", artifact_manifest_ref: ""}\n\nacceptance_criteria_matrix:\n  - {ac_id: "AC1", status: "pass", evidence_ref: "docs/agents/issue-tracker.md:1", note: "ok"}\n\ngates:\n  diagnostics_and_build_gate: {status: "pass", evidence_ref: "npm test", note: "ok"}\n  surface_qa_gate: {status: "pass", evidence_ref: "tracker", note: "ok"}\n  review_gate: {status: "pass", evidence_ref: "gh pr view", note: "ok"}\n\nrole_boundary:\n  acceptance_qa_owner: "pr_verifier"\n  main_agent_ran_issue_qa: false\n  worker_self_checks_are_not_final_acceptance: true\n\nartifact_manifest:\n  bundle_ref: "docs/agents/evidence/issue-31-pr-88.yaml"\n  retention: "repo artifact retained with PR evidence"\n  items: []\n\ncompact_summary:\n  outcome: "ok"\n  automated_checks: "ok"\n  manual_qa: "ok"\n  risks_or_limitations: ["none"]\n\npr:\n  number: "88"\n  url: "https://example.invalid/pr/88"\n\nnext_recommended_step: "Continue to release_worker."\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="31",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "88",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Continue to release_worker.",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=evidence_path.read_text(encoding="utf-8"),
    )

    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="31",
    state="completed",
    command_id="cmd-completed",
    updated_at="2026-05-07T17:19:00+08:00", current_session_id="ses-v", )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:21:00+08:00",)

    issue = read_issue(tmp_path, "31")

    assert updated_ledger is not None
    assert decision["action"] == "release_waiting"
    assert decision["next_role"] == "operator"
    assert decision["next_stage"] == "release_command"
    assert request is None
    assert issue is not None
    assert issue["state"] == "completed"


def test_reconcile_pr_verifier_accepts_nested_compact_summary_next_step(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"

    worker_result_path = tmp_path / "docs/agents/worker-results/issue-42.yaml"
    worker_result_path.parent.mkdir(parents=True, exist_ok=True)
    worker_result_path.write_text(
        """schema_version: "1.0"
kind: worker_result
line_cap: 80
status: "success"
next_recommended_step: "Run verifier"
pr:
  number: "77"
failure_classification: {kind: "none", retryable: true, routed_to: "pr_verifier", root_cause_signature: "none"}
metadata:
  completed_at: "2026-05-07T17:10:00+08:00"
""",
        encoding="utf-8",
    )
    evidence_path = tmp_path / "docs/agents/evidence/issue-42-pr-77.yaml"
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(
        """schema_version: "1.0"
kind: evidence_packet
line_cap: 60
status: "pass"
subject:
  issue_number: "42"
  pr_number: "77"
verifier:
  verifier_session_id: "ses-v"
compact_summary:
  outcome: "ok"
  next_recommended_step: "Advance to release"
failure_classification: {kind: "none", retryable: true, routed_to: "none", root_cause_signature: "none"}
""",
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "next_recommended_step": "Advance to release",
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:11:00+08:00",
        body_text=evidence_path.read_text(encoding="utf-8"),
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:11:00+08:00",)

    artifacts = cast(dict[str, object], updated_ledger["artifacts"])
    latest_pr_opened = read_latest_issue_history(tmp_path, "42", entry_type="pr_opened")

    assert decision["action"] == "release_waiting"
    assert decision["next_role"] == "operator"
    assert decision["next_stage"] == "release_command"
    assert request is None
    assert artifacts["release_result_ref"] == ""
    assert latest_pr_opened is not None
    assert latest_pr_opened["status"] == "opened"
    assert '"pr_number": "77"' in str(latest_pr_opened["body_text"])


def test_reconcile_release_blocked_exhaustion_marks_issue_failed(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    attempts = cast(dict[str, int], ledger["attempts"])
    limits = cast(dict[str, int], ledger["limits"])
    attempts["release_worker"] = limits["release_worker"]
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "blocked"\nblocked_reason: "policy_blocked"\nsummary:\n  outcome: "blocked"\n  next_recommended_step: "manual follow-up"\nfailure_classification: {kind: "release_blocked", retryable: false, routed_to: "main_orchestrator", root_cause_signature: "policy"}\nmerge:\n  attempted: true\n  merged: false\n  merged_sha: ""\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="release_pending",
    command_id="cmd-verifying",
    updated_at="2026-05-07T17:19:00+08:00", current_session_id="ses-v", )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:21:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "failed"
    assert issue["current_session_id"] == ""
    runtime_context = orchestrator_supervisor.read_runtime_context(tmp_path, "42")
    failure_context = cast(dict[str, object], runtime_context.get("failure_context", {}))
    recovery_cursor = cast(dict[str, object], runtime_context.get("recovery_cursor", {}))
    assert failure_context["owner_role"] == "main_orchestrator"
    assert failure_context["owner_stage"] == "release_root_execution"
    assert failure_context["artifact_kind"] == "release_result"
    assert recovery_cursor["resume_stage"] == "release_root_execution"


def test_reconcile_release_human_approval_block_stays_release_pending(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    attempts = cast(dict[str, int], ledger["attempts"])
    attempts["release_worker"] = 1
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "blocked"\nblocked_reason: "approval_override_mode is none"\nsummary:\n  outcome: "blocked"\n  next_recommended_step: "human_approves_pr_merge_then_supervisor_retries_release"\nfailure_classification: {kind: "human_approval_pending", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "approval"}\nmerge:\n  attempted: true\n  merged: false\n  merged_sha: ""\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="release_pending",
    command_id="cmd-release-pending",
    updated_at="2026-05-07T17:19:00+08:00", current_session_id="ses-v", )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "blocked",
            "blocked_reason": "approval_override_mode is none",
            "next_recommended_step": "human_approves_pr_merge_then_supervisor_retries_release",
            "failure_kind": "human_approval_pending",
            "retryable": True,
        },
        updated_at="2026-05-07T17:20:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
    updated_at="2026-05-07T17:21:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "release_waiting"
    assert decision["next_role"] == "operator"
    assert decision["next_stage"] == "release_command"
    assert request is None
    assert issue is not None
    assert issue["state"] == "release_pending"
    assert issue["current_session_id"] == "ses-v"
    assert issue["current_role"] == "main_orchestrator"
    assert issue["current_stage"] == "release_root_execution"
    assert issue["current_status"] == "pending_approval"


def test_submit_artifact_release_result_normalizes_human_approval_block_reason(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "52", SAMPLE_ISSUE_PACKET.replace('"42"', '"52"').replace("issue-42", "issue-52"))

    persisted = orchestrator_supervisor.submit_artifact(
        base_dir=tmp_path,
        issue_number="52",
        artifact_kind="release_result",
        payload={
            "status": "blocked",
            "blocked_reason": "approval_override_mode is none",
            "next_recommended_step": "wait for approval and retry release",
            "retryable": True,
        },
        updated_at="2026-05-08T10:00:00+08:00",
    )

    assert persisted["blocked_reason"] == "release_human_approval_missing"
    assert persisted["failure_kind"] == "human_approval_pending"

    issue = read_issue(tmp_path, "52")
    artifact_status = _artifact_status(issue)
    release_snapshot = cast(dict[str, object], artifact_status["release_result"])
    assert release_snapshot["blocked_reason"] == "release_human_approval_missing"
    merge_gate = cast(dict[str, object], release_snapshot["merge_gate"])
    assert merge_gate["approval_state"] == "missing"
    assert merge_gate["blocked_reason"] == "release_human_approval_missing"


def test_submit_artifact_evidence_packet_normalizes_ref_to_worktree_relative_path(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "57", SAMPLE_ISSUE_PACKET.replace('"42"', '"57"').replace("issue-42", "issue-57"))

    browser_artifact = tmp_path / "artifacts" / "browser-e2e" / "report.html"
    browser_artifact.parent.mkdir(parents=True, exist_ok=True)
    browser_artifact.write_text("ok", encoding="utf-8")

    persisted = orchestrator_supervisor.submit_artifact(
        base_dir=tmp_path,
        issue_number="57",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "gates": {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": browser_artifact.as_uri(),
                    "evidence_kind": "browser",
                }
            },
            "artifact_manifest": [browser_artifact.as_uri()],
        },
        updated_at="2026-05-08T10:00:00+08:00",
    )

    gates = cast(dict[str, object], persisted.get("gates") or {})
    surface_gate = cast(dict[str, object], gates.get("surface_qa_gate") or {})
    normalized_ref = str(surface_gate.get("evidence_ref") or "")

    assert normalized_ref == "artifacts/browser-e2e/report.html"
    assert (tmp_path / normalized_ref).exists()

    manifest = cast(list[object], persisted.get("artifact_manifest") or [])
    assert manifest == ["artifacts/browser-e2e/report.html"]


def test_submit_artifact_evidence_packet_stages_external_artifact_into_issue_worktree(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "58", SAMPLE_ISSUE_PACKET.replace('"42"', '"58"').replace("issue-42", "issue-58"))

    external_dir = tmp_path.parent / f"{tmp_path.name}-external-evidence"
    external_dir.mkdir(parents=True, exist_ok=True)
    external_artifact = external_dir / "browser-report.json"
    external_artifact.write_text('{"status":"ok"}', encoding="utf-8")

    persisted = orchestrator_supervisor.submit_artifact(
        base_dir=tmp_path,
        issue_number="58",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "gates": {
                "surface_qa_gate": {
                    "status": "pass",
                    "evidence_ref": external_artifact.as_uri(),
                    "evidence_kind": "browser",
                }
            },
            "artifact_manifest": [f"browser:{external_artifact.as_posix()}"],
        },
        updated_at="2026-05-08T10:01:00+08:00",
    )

    gates = cast(dict[str, object], persisted.get("gates") or {})
    surface_gate = cast(dict[str, object], gates.get("surface_qa_gate") or {})
    normalized_ref = str(surface_gate.get("evidence_ref") or "")

    assert normalized_ref.startswith(".opencode/runtime/evidence/issue-58/browser/")
    assert normalized_ref.endswith("-browser-report.json")
    assert (tmp_path / normalized_ref).exists()

    manifest = cast(list[object], persisted.get("artifact_manifest") or [])
    assert manifest == [normalized_ref]


def test_submit_artifact_release_result_blocked_requires_next_step(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "53", SAMPLE_ISSUE_PACKET.replace('"42"', '"53"').replace("issue-42", "issue-53"))

    try:
        orchestrator_supervisor.submit_artifact(
            base_dir=tmp_path,
            issue_number="53",
            artifact_kind="release_result",
            payload={
                "status": "blocked",
                "blocked_reason": "pr_not_mergeable",
            },
            updated_at="2026-05-08T10:01:00+08:00",
        )
        assert False, "expected ValueError for missing next_recommended_step"
    except ValueError as error:
        assert "next_recommended_step" in str(error)


def test_submit_artifact_release_result_normalizes_merge_gate_and_hygiene(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "54", SAMPLE_ISSUE_PACKET.replace('"42"', '"54"').replace("issue-42", "issue-54"))

    persisted = orchestrator_supervisor.submit_artifact(
        base_dir=tmp_path,
        issue_number="54",
        artifact_kind="release_result",
        payload={
            "status": "blocked",
            "blocked_reason": "required_checks_pending",
            "next_recommended_step": "wait checks and retry release",
            "retryable": True,
        },
        updated_at="2026-05-08T10:02:00+08:00",
    )

    merge_gate = cast(dict[str, object], persisted["merge_gate"])
    assert merge_gate["checks_state"] == "pending"
    assert merge_gate["mergeability_state"] == "clean"
    assert merge_gate["approval_state"] == "satisfied"

    workspace_hygiene = cast(dict[str, object], persisted["workspace_hygiene"])
    assert workspace_hygiene["cleanup_status"] == "pass"
    assert workspace_hygiene["blocked_reason"] == "none"
    assert persisted["retryable"] is True


def test_submit_artifact_release_result_defaults_retryable_false_for_non_transient_blocker(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "56", SAMPLE_ISSUE_PACKET.replace('"42"', '"56"').replace("issue-42", "issue-56"))

    persisted = orchestrator_supervisor.submit_artifact(
        base_dir=tmp_path,
        issue_number="56",
        artifact_kind="release_result",
        payload={
            "status": "blocked",
            "blocked_reason": "policy_blocked",
            "next_recommended_step": "manual policy decision",
        },
        updated_at="2026-05-08T10:04:00+08:00",
    )

    assert persisted["retryable"] is False


def test_inspect_control_plane_includes_release_gate_view(tmp_path: Path) -> None:
    _ingest_issue_packet_text(tmp_path, "55", SAMPLE_ISSUE_PACKET.replace('"42"', '"55"').replace("issue-42", "issue-55"))
    _submit_artifact(
        tmp_path,
        issue_number="55",
        artifact_kind="release_result",
        payload={
            "status": "blocked",
            "blocked_reason": "pr_not_mergeable",
            "next_recommended_step": "rebase branch and retry release",
            "retryable": True,
        },
        updated_at="2026-05-08T10:03:00+08:00",
    )

    output = io.StringIO()
    with redirect_stdout(output):
        exit_code = orchestrator_supervisor.main([
            "inspect",
            "--base-dir",
            str(tmp_path),
            "--issue-number",
            "55",
        ])

    payload = cast(dict[str, object], json.loads(output.getvalue()))
    release_gate = cast(dict[str, object], payload["releaseGate"])
    merge_gate = cast(dict[str, object], release_gate["merge_gate"])

    assert exit_code == 0
    assert release_gate["status"] == "blocked"
    assert release_gate["blocked_reason"] == "pr_not_mergeable"
    assert merge_gate["mergeability_state"] == "conflicted"


def test_reconcile_late_successful_release_result_recovers_failed_issue_to_completed(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="failed",
    command_id="cmd-failed",
    updated_at="2026-05-07T17:21:00+08:00", current_session_id="ses-v", )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "completed"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: false\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "completed",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": False,
        },
        updated_at="2026-05-07T17:22:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)

    issue = read_issue(tmp_path, "42")
    artifact_status = _artifact_status(issue)

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "completed"
    assert issue["current_session_id"] == ""
    assert cast(dict[str, object], artifact_status["release_result"])["parse_ok"] is True
    assert cast(dict[str, object], artifact_status["release_result"])["status"] == "completed"
    assert cast(dict[str, object], artifact_status["release_result"])["blocked_reason"] == "none"


def test_reconcile_late_release_success_requires_persisted_release_fact(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="failed",
    command_id="cmd-failed",
    updated_at="2026-05-07T17:21:00+08:00", current_session_id="ses-v", )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "completed"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: false\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False), patch(
        "scripts.orchestrator_supervisor._read_db_artifact_fact", return_value={}
    ):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "failed"


def test_reconcile_late_successful_release_result_without_blocked_reason_recovers_issue(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="failed",
    command_id="cmd-failed",
    updated_at="2026-05-07T17:21:00+08:00", current_session_id="ses-v", )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "success"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": False,
        },
        updated_at="2026-05-07T17:22:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "completed"


def test_reconcile_release_result_ignores_nested_blocked_reason_when_top_level_missing(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="failed",
    command_id="cmd-failed",
    updated_at="2026-05-07T17:21:00+08:00", current_session_id="ses-v", )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "success"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nworkspace_hygiene:\n  cleanup_status: "pass"\n  blocked_reason: "none"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "success",
            "blocked_reason": "",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": False,
        },
        updated_at="2026-05-07T17:22:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "completed"


def test_reconcile_late_successful_release_result_recovers_ready_issue_to_completed(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="ready",
    command_id="cmd-ready",
    updated_at="2026-05-07T17:21:00+08:00", current_session_id="ses-v", )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "completed"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: false\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "completed",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": False,
        },
        updated_at="2026-05-07T17:22:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "completed"
    assert issue["current_session_id"] == ""


def test_reconcile_without_queued_role_recovers_failed_issue_from_completed_release_result(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "", "stage": "", "status": ""}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="failed",
    command_id="cmd-failed",
    updated_at="2026-05-07T17:21:00+08:00", current_session_id="", )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "completed"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: false\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "completed",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": False,
        },
        updated_at="2026-05-07T17:22:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        first_ledger, first_decision, first_request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)
        second_ledger, second_decision, second_request = reconcile_ledger(first_ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:24:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert first_decision["action"] == "queue_next_session"
    assert first_decision["next_stage"] == "issue_selection_or_recovery"
    assert first_request is not None
    assert cast(dict[str, object], first_ledger["current"])["stage"] == "issue_selection_or_recovery"
    assert second_decision["action"] == "queue_next_session"
    assert second_request is not None
    assert issue is not None
    assert issue["state"] == "completed"
    assert issue["current_stage"] == ""


def test_reconcile_issue_selection_or_recovery_recovers_completed_release_without_main_orchestrator_attempt_counter(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    ledger["attempts"] = {"issue_worker": 1, "pr_verifier": 1, "release_worker": 1}
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:21:00+08:00",
        current_session_id="",
    )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "completed"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: false\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="release_result",
        payload={
            "status": "completed",
            "blocked_reason": "none",
            "next_recommended_step": "continue",
            "failure_kind": "none",
            "retryable": False,
        },
        updated_at="2026-05-07T17:22:00+08:00",
        body_text=release_path.read_text(encoding="utf-8"),
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "completed"
    assert cast(dict[str, int], updated_ledger["attempts"])["main_orchestrator"] == 1


def test_reconcile_issue_selection_or_recovery_queues_retryable_failed_issue_recovery(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    ledger["lastFailure"] = {
        "kind": "approval_blocked",
        "summary": "Obtain required human approval, then rerun release_worker.",
        "retryable": True,
        "owner_role": "main_orchestrator",
        "owner_stage": "release_root_execution",
        "owner_state": "release_pending",
        "artifact_kind": "release_result",
        "failure_class": "release",
        "recovery_cursor": {
            "resume_role": "main_orchestrator",
            "resume_stage": "release_root_execution",
            "resume_state": "release_pending",
            "resume_strategy": "retry_same_phase",
        },
    }
    cast(dict[str, str], ledger["artifacts"])["release_result_ref"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(tmp_path,
    issue_number="42",
    state="failed",
    command_id="cmd-failed",
    updated_at="2026-05-07T17:21:00+08:00", current_session_id="ses-v", )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert request["role"] == "main_orchestrator"
    assert request["stage"] == "release_root_execution"
    assert "role=main_orchestrator" in cast(str, decision["summary"])
    assert "stage=release_root_execution" in cast(str, decision["summary"])
    assert "state=release_pending" in cast(str, decision["summary"])
    assert "strategy=retry_same_phase" in cast(str, decision["summary"])


def test_reconcile_issue_selection_or_recovery_directly_requeues_failed_worker(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    attempts = cast(dict[str, int], ledger["attempts"])
    starting_attempts = attempts["issue_worker"]
    ledger["lastFailure"] = {
        "kind": "contract_invalid",
        "summary": "Retry worker from failed recovery.",
        "retryable": True,
        "owner_role": "issue_worker",
        "owner_stage": "issue_worker_repair",
        "owner_state": "running",
        "artifact_kind": "worker_result",
        "failure_class": "worker",
        "recovery_cursor": {
            "resume_role": "issue_worker",
            "resume_stage": "issue_worker_repair",
            "resume_state": "running",
            "resume_strategy": "retry_same_role",
        },
    }
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed-worker",
        updated_at="2026-05-07T17:21:00+08:00",
        current_session_id="",
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "issue_worker"
    assert decision["next_stage"] == "issue_worker_repair"
    assert request is None
    assert issue is not None
    assert issue["state"] == "running"
    assert attempts["issue_worker"] == starting_attempts


def test_reconcile_issue_selection_or_recovery_directly_requeues_failed_verifier(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    attempts = cast(dict[str, int], ledger["attempts"])
    starting_attempts = attempts["pr_verifier"]
    ledger["lastFailure"] = {
        "kind": "browser_e2e_unavailable",
        "summary": "Retry verifier from failed recovery.",
        "retryable": True,
        "owner_role": "pr_verifier",
        "owner_stage": "pr_verifier_execution",
        "owner_state": "verifying",
        "artifact_kind": "evidence_packet",
        "failure_class": "verifier",
        "recovery_cursor": {
            "resume_role": "pr_verifier",
            "resume_stage": "pr_verifier_execution",
            "resume_state": "verifying",
            "resume_strategy": "retry_same_role",
        },
    }
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed-verifier",
        updated_at="2026-05-07T17:21:00+08:00",
        current_session_id="",
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert decision["next_stage"] == "pr_verifier_execution"
    assert request is None
    assert issue is not None
    assert issue["state"] == "verifying"
    assert attempts["pr_verifier"] == starting_attempts
    assert "Recovery cursor: role=unknown" not in cast(str, decision["summary"])
    assert "strategy=unspecified" not in cast(str, decision["summary"])


def test_reconcile_issue_selection_or_recovery_recovers_ready_issue_to_verified_from_late_evidence(tmp_path: Path):
    issue_packet_text = """schema_version: \"1.0\"
kind: issue_packet
line_cap: 80

issue:
  number: \"42\"
  title: \"Demo issue\"
  url: \"https://github.com/example/issues/42\"
  labels: [ready-for-agent]
  parent: {type: \"prd\", reference: \"https://github.com/example/issues/1\"}

branch: {name: \"agent/issue-42-demo\", base: \"main\"}

bootstrap_context:
  required_reads: [\"AGENTS.md\"]
  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}
  relevant_paths: [\"frontend\"]
  prior_handoff: \"docs/agents/handoffs/issue-41.yaml\"

verifier_manual_qa:
  surface: \"browser\"
  required_gates: [\"diagnostics_and_build_gate\", \"surface_qa_gate\", \"review_gate\"]
"""
    issue_packet = parse_issue_packet_text(issue_packet_text, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["worker_result_ref"] = "docs/agents/worker-results/issue-42.yaml"
    cast(dict[str, str], ledger["artifacts"])["evidence_packet_ref"] = "docs/agents/evidence/issue-42-pr-77.yaml"
    _ingest_issue_packet_text(tmp_path, "42", issue_packet_text)

    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready",
        updated_at="2026-05-07T17:21:00+08:00",
    )

    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="worker_result",
        payload={
            "status": "success",
            "pr_number": "77",
            "files_changed": [{"path": "index.html", "summary": "UI update"}],
            "failure_kind": "none",
            "retryable": True,
        },
        updated_at="2026-05-07T17:10:00+08:00",
    )
    _submit_artifact(
        tmp_path,
        issue_number="42",
        artifact_kind="evidence_packet",
        payload={
            "status": "pass",
            "pr_number": "77",
            "verifier_session_id": "ses-v",
            "failure_kind": "none",
            "retryable": True,
            "gates": {
                "surface_qa_gate": {"status": "pass", "evidence_ref": "artifacts/browser-e2e/report.html"},
                "browser_e2e_gate": "pass",
            },
            "browser_e2e_evidence": {"method": "chrome"},
        },
        updated_at="2026-05-07T17:11:00+08:00",
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(ledger, artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:23:00+08:00",)

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "verified"
    assert issue["current_role"] == "main_orchestrator"
    assert issue["current_stage"] == "issue_selection_or_recovery"
    assert issue["current_status"] == "queued"


def test_inspect_command_prints_control_plane_snapshot(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AUTODEV_RELEASE_CAPACITY", "3")
    monkeypatch.setenv("AUTODEV_RELEASE_BACKFILL_MODE", "manual")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    issue_43_packet = parse_issue_packet_text(
        SAMPLE_ISSUE_PACKET.replace('"42"', '"43"').replace("issue-42", "issue-43").replace("Demo issue", "Issue 43"),
        "docs/agents/issue-packets/issue-43.yaml",
    )
    orchestrator_supervisor._sync_issue_packet_to_db(tmp_path, issue_43_packet, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="43",
        state="verified",
        command_id="cmd-verified-43",
        updated_at="2026-05-07T17:00:30+08:00",
    )
    orchestrator_supervisor.transition_issue_state(
        tmp_path,
        issue_number="42",
        to_state="claimed",
        command_id="cmd-claim",
        scheduler_id="scheduler:test",
        reason="claim issue",
        updated_at="2026-05-07T17:01:00+08:00",
        from_state="ready",
    )
    orchestrator_supervisor.record_github_sync_attempt(
        tmp_path,
        command_id="cmd-gh",
        issue_number="42",
        add_labels=["agent-dispatching"],
        remove_labels=["ready-for-agent"],
        status="failed",
        updated_at="2026-05-07T17:02:00+08:00",
        last_error="sync failed",
    )
    _ = orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:03:00+08:00",
        runtime_context={
            "release_child_session": {
                "childRole": "release_worker",
                "childSessionID": "ses-release-worker-42",
                "childSessionStatus": "stop",
                "rootSessionID": "ses-release-root-42",
                "recordedAt": "2026-05-07T17:03:00+08:00",
            }
        },
    )
    orchestrator_supervisor.record_latest_ref_snapshot(
        tmp_path,
        issue_number="42",
        entry_type="release_child_session",
        history_id=999,
        created_at="2026-05-07T17:03:00+08:00",
        command_id="cmd-release-child",
        session_id="ses-release-root-42",
        status="stop",
        extra={
            "childRole": "release_worker",
            "childSessionID": "ses-release-worker-42",
            "childSessionStatus": "stop",
            "rootSessionID": "ses-release-root-42",
            "recordedAt": "2026-05-07T17:03:00+08:00",
        },
    )
    output = io.StringIO()
    with redirect_stdout(output):
        exit_code = orchestrator_supervisor.main([
            "inspect",
            "--base-dir",
            str(tmp_path),
            "--issue-number",
            "42",
        ])

    payload = cast(dict[str, object], json.loads(output.getvalue()))
    schema = cast(dict[str, object], payload["schema"])
    tables = cast(dict[str, object], schema["tables"])
    issue_table = cast(dict[str, object], tables["issues"])
    issue_columns = cast(list[dict[str, object]], issue_table["columns"])
    issue_column_names = [str(column["name"]) for column in issue_columns]

    assert exit_code == 0
    assert set(payload) == {
        "schema",
        "issue",
        "latestDecision",
        "latestGitHubSyncAttempt",
        "projectPrWorkflow",
        "releaseGate",
        "releaseChildSession",
        "latestReleaseChildSession",
        "releaseBackfill",
        "failureContext",
        "recoveryCursor",
    }
    assert schema["dbPath"] == str(tmp_path / ".opencode/runtime/control-plane.sqlite3")
    assert "artifact_refs_json" in issue_column_names
    assert "artifact_status_json" in issue_column_names
    assert "issue_packet_json" in issue_column_names
    assert cast(dict[str, object], payload["issue"])["issue_number"] == "42"
    assert cast(dict[str, object], payload["latestDecision"])["command_id"] == "cmd-claim"
    assert cast(dict[str, object], payload["latestGitHubSyncAttempt"])["command_id"] == "cmd-gh"
    assert cast(dict[str, object], payload["latestGitHubSyncAttempt"])["projection_target"] == "labels"
    assert cast(dict[str, object], payload["projectPrWorkflow"])["status"] == "not_opened"
    assert cast(dict[str, object], payload["releaseChildSession"])["childSessionID"] == "ses-release-worker-42"
    assert cast(dict[str, object], payload["latestReleaseChildSession"])["command_id"] == "cmd-release-child"
    assert payload["failureContext"] == {}
    assert payload["recoveryCursor"] == {}
    release_backfill = cast(dict[str, object], payload["releaseBackfill"])
    assert release_backfill["mode"] == "manual"
    assert release_backfill["releaseCapacity"] == 3
    assert release_backfill["availableReleaseSlots"] == 3
    assert release_backfill["verifiedWaitingCount"] == 1


def test_retry_github_sync_command_replays_failed_attempt(tmp_path: Path):
    config_path = tmp_path / ".autodev.yaml"
    _ = config_path.write_text('schema_version: "1.0"\nproject:\n  github_repo: example/repo\n', encoding="utf-8")
    orchestrator_supervisor.record_github_sync_attempt(
        tmp_path,
        command_id="cmd-gh",
        issue_number="42",
        add_labels=["agent-in-progress"],
        remove_labels=["agent-dispatching"],
        status="failed",
        updated_at="2026-05-07T17:02:00+08:00",
        last_error="sync failed",
    )

    output = io.StringIO()
    with patch(
        "scripts.orchestrator_supervisor.subprocess.run",
        return_value=CompletedProcess(args=["gh"], returncode=0, stdout="", stderr=""),
    ) as mocked_run, redirect_stdout(output):
        exit_code = orchestrator_supervisor.main([
            "retry-github-sync",
            "--base-dir",
            str(tmp_path),
            "--command-id",
            "cmd-gh",
            "--updated-at",
            "2026-05-07T17:03:00+08:00",
        ])

    payload = cast(dict[str, object], json.loads(output.getvalue()))
    attempt = read_github_sync_attempt(tmp_path, "cmd-gh")
    latest_decision = orchestrator_supervisor.read_latest_decision(tmp_path, "42")

    assert exit_code == 0
    mocked_run.assert_called_once()
    assert payload["status"] == "success"
    assert attempt is not None
    assert attempt["status"] == "success"
    assert attempt["attempt_count"] == 2
    assert latest_decision is not None
    assert latest_decision["decision_type"] == "admin_github_sync_retry"


def test_retry_github_sync_command_replays_failed_project_fields_attempt(tmp_path: Path):
    orchestrator_supervisor.record_github_sync_attempt(
        tmp_path,
        command_id="cmd-project-fields",
        issue_number="42",
        add_labels=[],
        remove_labels=[],
        status="failed",
        updated_at="2026-05-07T17:02:00+08:00",
        last_error="project sync failed",
        projection_target="project_fields",
        projection_payload={"project_id": "PVT_project_1", "issue_number": "42"},
    )

    def fake_sync_project_fields_projection(
        *,
        base_dir: Path,
        issue_number: str,
        command_id: str | None = None,
        updated_at: str | None = None,
    ) -> str:
        assert command_id is not None
        orchestrator_supervisor.record_github_sync_attempt(
            base_dir,
            command_id=command_id,
            issue_number=issue_number,
            add_labels=[],
            remove_labels=[],
            status="success",
            updated_at=str(updated_at or "2026-05-07T17:03:00+08:00"),
            projection_target="project_fields",
            projection_payload={"issue_number": issue_number},
        )
        return ""

    output = io.StringIO()
    with patch(
        "scripts.orchestrator_supervisor._sync_project_fields_projection",
        side_effect=fake_sync_project_fields_projection,
    ) as sync_project_fields, redirect_stdout(output):
        exit_code = orchestrator_supervisor.main([
            "retry-github-sync",
            "--base-dir",
            str(tmp_path),
            "--command-id",
            "cmd-project-fields",
            "--updated-at",
            "2026-05-07T17:03:00+08:00",
        ])

    payload = cast(dict[str, object], json.loads(output.getvalue()))
    attempt = read_github_sync_attempt(tmp_path, "cmd-project-fields")
    latest_decision = orchestrator_supervisor.read_latest_decision(tmp_path, "42")

    assert exit_code == 0
    sync_project_fields.assert_called_once()
    kwargs = sync_project_fields.call_args.kwargs
    assert kwargs["issue_number"] == "42"
    assert kwargs["command_id"] == "cmd-project-fields"
    assert payload["status"] == "success"
    assert attempt is not None
    assert attempt["status"] == "success"
    assert attempt["attempt_count"] == 2
    assert latest_decision is not None
    assert latest_decision["decision_type"] == "admin_github_sync_retry"


def test_retry_github_sync_command_rejects_non_retryable_projection_target(tmp_path: Path):
    orchestrator_supervisor.record_github_sync_attempt(
        tmp_path,
        command_id="cmd-issue-body",
        issue_number="42",
        add_labels=[],
        remove_labels=[],
        status="failed",
        updated_at="2026-05-07T17:02:00+08:00",
        last_error="body sync failed",
        projection_target="issue_body",
    )

    try:
        orchestrator_supervisor.main([
            "retry-github-sync",
            "--base-dir",
            str(tmp_path),
            "--command-id",
            "cmd-issue-body",
        ])
    except ValueError as error:
        assert "is not retryable" in str(error)
    else:
        raise AssertionError("expected retry-github-sync to reject unsupported projection_target")


def test_retry_github_sync_command_rejects_non_failed_attempt(tmp_path: Path):
    orchestrator_supervisor.record_github_sync_attempt(
        tmp_path,
        command_id="cmd-gh",
        issue_number="42",
        add_labels=["agent-dispatching"],
        remove_labels=["ready-for-agent"],
        status="success",
        updated_at="2026-05-07T17:02:00+08:00",
    )

    try:
        orchestrator_supervisor.main([
            "retry-github-sync",
            "--base-dir",
            str(tmp_path),
            "--command-id",
            "cmd-gh",
        ])
    except ValueError as error:
        assert "is not failed" in str(error)
    else:
        raise AssertionError("expected retry-github-sync to reject non-failed attempts")
