from __future__ import annotations

import io
import json
import sqlite3
from pathlib import Path
from subprocess import CompletedProcess
from typing import cast
from contextlib import redirect_stdout
from unittest.mock import patch

import scripts.orchestrator_supervisor as orchestrator_supervisor
from scripts.control_plane_db import read_github_sync_attempt, read_issue, read_latest_issue_history


def _artifact_status(issue: dict[str, object] | None) -> dict[str, object]:
    assert issue is not None
    raw = str(issue.get("artifact_status_json") or "{}")
    payload = json.loads(raw)
    return payload if isinstance(payload, dict) else {}
from scripts.orchestrator_supervisor import (
    build_orchestrator_request,
    create_initial_ledger,
    issue_lock_path,
    parse_issue_packet_text,
    reconcile_ledger,
    run_issue_packet_intake,
    select_next_issue_packet,
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

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:05:00+08:00",
    )
    current = cast(dict[str, object], updated_ledger["current"])

    assert current["role"] == "issue_worker"
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "issue_worker"
    assert "issue_worker subagent" in cast(str, decision.get("subagent_prompt", ""))
    assert request is None


def test_reconcile_worker_success_queues_pr_verifier(tmp_path: Path):
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
    cast(dict[str, str], ledger["artifacts"])["workerResultPath"] = str(worker_result_path.relative_to(tmp_path))

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )
    current = cast(dict[str, object], updated_ledger["current"])
    artifacts = cast(dict[str, object], updated_ledger["artifacts"])

    assert current["role"] == "pr_verifier"
    assert artifacts["evidencePacketPath"] == "docs/agents/evidence/issue-42-pr-77.yaml"
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "pr_verifier"
    assert "pr_verifier subagent" in cast(str, decision.get("subagent_prompt", ""))
    assert request is None
    issue = read_issue(tmp_path, "42")
    artifact_status = _artifact_status(issue)
    assert cast(dict[str, object], artifact_status["worker_result"])["parse_ok"] is True
    assert cast(dict[str, object], artifact_status["worker_result"])["status"] == "success"
    assert cast(dict[str, object], artifact_status["worker_result"])["pr_number"] == "77"


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
    cast(dict[str, str], ledger["artifacts"])["workerResultPath"] = str(worker_result_path.relative_to(tmp_path))

    with patch("scripts.orchestrator_supervisor._artifact_fact", return_value={}):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:11:00+08:00",
        )

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert cast(dict[str, object], updated_ledger["lastFailure"])["kind"] == "contract_invalid"
    assert "persisted worker_result fact is missing" in cast(str, decision["summary"])


def test_reconcile_worker_success_without_pr_number_routes_to_recovery(tmp_path: Path):
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
    cast(dict[str, str], ledger["artifacts"])["workerResultPath"] = str(worker_result_path.relative_to(tmp_path))

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")
    artifact_status = _artifact_status(issue)

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert cast(dict[str, object], updated_ledger["lastFailure"])["kind"] == "contract_invalid"
    assert "reported success without a PR number" in cast(str, decision["summary"])
    assert cast(dict[str, object], artifact_status["worker_result"])["parse_ok"] is True
    assert cast(dict[str, object], artifact_status["worker_result"])["pr_number"] == ""


def test_reconcile_worker_success_refreshes_running_heartbeat_before_stale_quarantine(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    cast(dict[str, int], ledger["attempts"])["issue_worker"] = 1
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
        current_root_session_id="ses-root-42",
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
    cast(dict[str, str], ledger["artifacts"])["workerResultPath"] = str(worker_result_path.relative_to(tmp_path))

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:16:00+08:00",
    )

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
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:20:00+08:00",
        current_root_session_id="ses-root-42",
    )

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
    cast(dict[str, str], ledger["artifacts"])["workerResultPath"] = str(worker_result_path.relative_to(tmp_path))

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:36:00+08:00",
        )

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

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:12:00+08:00",
    )
    current = cast(dict[str, object], updated_ledger["current"])

    assert current == {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    assert attempts["issue_worker"] == 1
    assert decision["action"] == "no_change"
    assert request is None


def test_reconcile_retries_queued_issue_worker_when_child_session_aborted(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    attempts = cast(dict[str, int], ledger["attempts"])
    attempts["issue_worker"] = 1
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:04:00+08:00",
        current_root_session_id="ses-root-42",
    )

    with patch(
        "scripts.orchestrator_supervisor._read_issue_worker_abort_summary",
        return_value={
            "root_session_id": "ses-root-42",
            "session_id": "ses-worker-42",
            "abort_reason": "Aborted",
            "latest_assistant_status": "MessageAbortedError",
        },
    ):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:05:00+08:00",
        )

    current = cast(dict[str, object], updated_ledger["current"])
    last_failure = cast(dict[str, object], updated_ledger["lastFailure"])

    assert current["role"] == "issue_worker"
    assert current["stage"] == "issue_worker_repair"
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "issue_worker"
    assert request is None
    assert attempts["issue_worker"] == 2
    assert last_failure["kind"] == "contract_invalid"
    assert "aborted in child session ses-worker-42" in cast(str, last_failure["summary"])


def test_reconcile_retries_queued_issue_worker_when_child_session_stops_before_messages(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    attempts = cast(dict[str, int], ledger["attempts"])
    attempts["issue_worker"] = 1
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:04:00+08:00",
        current_root_session_id="ses-root-42",
    )

    with patch(
        "scripts.orchestrator_supervisor._find_latest_child_session_summary",
        return_value={
            "session_id": "ses-worker-42",
            "latest_assistant_status": "no_assistant_message",
            "message_count": 0,
            "part_count": 0,
        },
    ):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:05:00+08:00",
        )

    current = cast(dict[str, object], updated_ledger["current"])
    last_failure = cast(dict[str, object], updated_ledger["lastFailure"])

    assert current["role"] == "issue_worker"
    assert current["stage"] == "issue_worker_repair"
    assert decision["action"] == "delegate_subagent"
    assert decision["next_role"] == "issue_worker"
    assert request is None
    assert attempts["issue_worker"] == 2
    assert last_failure["kind"] == "contract_invalid"
    assert "stopped before producing any messages or tool parts" in cast(str, last_failure["summary"])


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


def test_build_orchestrator_request_requires_foreground_child_subagents():
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    request = build_orchestrator_request(ledger)

    assert 'task(subagent_type="general", ..., run_in_background=false)' in request["prompt"]
    assert "Wait for each child task call to finish in the foreground before continuing." in request["prompt"]
    assert "Do not include karpathy-guidelines in load_skills for child subagents" not in request["prompt"]


def test_validate_session_request_rejects_completed_issue(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        checkpoint_path="docs/agents/runtime/context-checkpoint.yaml",
        updated_at="2026-05-07T17:00:00+08:00",
    )
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


def test_reconcile_verifier_fail_routes_back_to_issue_worker(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    attempts = cast(dict[str, int], ledger["attempts"])
    attempts["issue_worker"] = 1
    attempts["pr_verifier"] = 1
    cast(dict[str, str], ledger["artifacts"])["evidencePacketPath"] = "docs/agents/evidence/issue-42-pr-77.yaml"

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

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:15:00+08:00",
    )
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
    cast(dict[str, str], ledger["artifacts"])["workerResultPath"] = "docs/agents/worker-results/issue-42.yaml"

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:15:00+08:00",
    )

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

    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        checkpoint_path="docs/agents/runtime/context-checkpoint.yaml",
        root_session_agent="build",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    ledger["current"] = {"role": "release_worker", "stage": "release_worker_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-31-pr-88.yaml"

    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nraw_evidence_policy: index_only_refs_no_raw_logs_or_transcripts\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "success"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:21:00+08:00",
    )
    issue = cast(dict[str, object], updated_ledger["issue"])
    automation = cast(dict[str, object], updated_ledger["automation"])
    updated_checkpoint = checkpoint_path.read_text(encoding="utf-8")

    assert issue["number"] == "32"
    assert automation["rootSessionAgent"] == "build"
    assert decision["action"] == "queue_next_issue"
    assert request is not None
    assert request["issueNumber"] == "32"
    assert request["agent"] == "build"
    assert 'agent: "build"' in updated_checkpoint
    assert read_issue(tmp_path, "31") is not None


def test_reconcile_recovery_runs_issue_intake_when_local_packet_missing(tmp_path: Path):
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )
    current_packet = tmp_path / "docs/agents/issue-packets/issue-31.yaml"
    current_packet.parent.mkdir(parents=True, exist_ok=True)
    current_packet.write_text(SAMPLE_ISSUE_PACKET.replace('"42"', '"31"').replace('issue-42', 'issue-31').replace('Demo issue', 'Issue 31').replace('agent/issue-42-demo', 'agent/issue-31-demo'), encoding="utf-8")

    issue_packet = parse_issue_packet_text(current_packet.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        checkpoint_path="docs/agents/runtime/context-checkpoint.yaml",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    ledger["current"] = {"role": "release_worker", "stage": "release_worker_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-31-pr-88.yaml"

    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nraw_evidence_policy: index_only_refs_no_raw_logs_or_transcripts\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "success"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )

    def fake_intake(_: Path) -> bool:
        intake_packet = tmp_path / "docs/agents/issue-packets/issue-32.yaml"
        intake_packet.write_text(
            SAMPLE_ISSUE_PACKET.replace('"42"', '"32"').replace('issue-42', 'issue-32').replace('Demo issue', 'Issue 32').replace('agent/issue-42-demo', 'agent/issue-32-demo'),
            encoding="utf-8",
        )
        return True

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", side_effect=fake_intake):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:21:00+08:00",
        )
    issue = cast(dict[str, object], updated_ledger["issue"])

    assert issue["number"] == "32"
    assert decision["action"] == "queue_next_issue"
    assert request is not None
    assert request["issueNumber"] == "32"


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
        workflow={"checkpointPath": "docs/agents/runtime/context-checkpoint.yaml"},
        current_issue={"number": "30", "parentReference": "https://github.com/example/issues/1"},
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
    selected = select_next_issue_packet(
        tmp_path,
        workflow={"checkpointPath": "docs/agents/runtime/context-checkpoint.yaml"},
        current_issue={"number": "30", "parentReference": "https://github.com/example/issues/1"},
    )

    assert selected is not None
    assert selected.issue_number == "31"


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
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    selected = select_next_issue_packet(
        tmp_path,
        workflow={"checkpointPath": "docs/agents/runtime/context-checkpoint.yaml"},
        current_issue={"number": "30", "parentReference": "https://github.com/example/issues/1"},
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
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="quarantined",
        command_id="cmd-quarantined",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    selected = select_next_issue_packet(
        tmp_path,
        workflow={"checkpointPath": "docs/agents/runtime/context-checkpoint.yaml"},
        current_issue={"number": "30", "parentReference": "https://github.com/example/issues/1"},
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

    selected = select_next_issue_packet(
        tmp_path,
        workflow={"checkpointPath": "docs/agents/runtime/context-checkpoint.yaml"},
        current_issue={"number": "30", "parentReference": "https://github.com/example/issues/1"},
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
    _ = orchestrator_supervisor.upsert_issue_ranking(
        tmp_path,
        issue_number="32",
        rank_score=999999,
        lane="default",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    selected = select_next_issue_packet(
        tmp_path,
        workflow={"checkpointPath": "docs/agents/runtime/context-checkpoint.yaml"},
        current_issue={"number": "30", "parentReference": "https://github.com/example/issues/1"},
    )
    issue = orchestrator_supervisor.read_issue(tmp_path, "32")

    assert selected is not None
    assert selected.issue_number == "31"


def test_run_issue_packet_intake_uses_consumer_project_github_repo(tmp_path: Path):
    _ = (tmp_path / ".autodev.yaml").write_text(
        'schema_version: "1.0"\nproject:\n  name: demo\n  github_repo: owner/demo-repo\n',
        encoding="utf-8",
    )

    with patch(
        "scripts.orchestrator_supervisor.subprocess.run",
        return_value=CompletedProcess(args=["python3"], returncode=0, stdout="", stderr=""),
    ) as run:
        result = run_issue_packet_intake(tmp_path)

    assert result is True
    command = run.call_args.args[0]
    kwargs = run.call_args.kwargs
    assert command[:2] == ["python3", str(orchestrator_supervisor.DEFAULT_ISSUE_INTAKE_SCRIPT_PATH)]
    assert "--repo" in command
    assert "owner/demo-repo" in command
    assert str(tmp_path / "docs/agents/issue-packets") in command
    assert kwargs["cwd"] == tmp_path


def test_dispatch_session_request_writes_success_result_and_syncs_ledger(tmp_path: Path):
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    request = {
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    _ = request_path.write_text(json.dumps(request, indent=2), encoding="utf-8")
    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_test"}\n'),
    ) as mocked_spawn:
        exit_code = orchestrator_supervisor.main(
            [
                "dispatch",
                "--request",
                str(request_path),
                "--session-result",
                str(session_result_path),
                "--ledger",
                str(ledger_path),
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(session_result_path.read_text(encoding="utf-8")))
    synced_ledger = cast(dict[str, object], json.loads(ledger_path.read_text(encoding="utf-8")))
    issue = read_issue(tmp_path, "42")

    mocked_spawn.assert_called_once()
    assert exit_code == 0
    assert session_result["status"] == "success"
    assert session_result["rootSessionID"] == "ses_root_test"
    assert session_result["sourceSessionID"] == "ses_source_test"
    assert session_result["cliOpenCommand"] == "opencode --session ses_root_test"
    assert session_result["stopContinuationStatus"] == "root_session_detached"
    assert cast(dict[str, object], synced_ledger["lastSessionResult"])["rootSessionID"] == "ses_root_test"
    assert issue is not None
    assert issue["current_root_session_id"] == "ses_root_test"
    spawn_command = mocked_spawn.call_args.args[0]
    assert "--agent" not in spawn_command


def test_dispatch_session_request_updates_control_plane_running_state(tmp_path: Path):
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    request = {
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
        "requestID": "req-42",
    }
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    _ = request_path.write_text(json.dumps(request, indent=2), encoding="utf-8")
    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_test"}\n'),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        orchestrator_supervisor._dispatch_consumed_request(
            request_path,
            ledger_path=ledger_path,
            session_result_path=session_result_path,
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_root_session_id"] == "ses_root_test"


def test_dispatch_session_request_appends_root_start_event(tmp_path: Path):
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    request = {
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
        "requestID": "req-42",
    }
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    _ = request_path.write_text(json.dumps(request, indent=2), encoding="utf-8")
    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_test"}\n'),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        orchestrator_supervisor._dispatch_consumed_request(
            request_path,
            ledger_path=ledger_path,
            session_result_path=session_result_path,
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
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    request = {
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
        "requestID": "req-42",
    }
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    config_path = tmp_path / ".autodev.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    _ = config_path.write_text('schema_version: "1.0"\nproject:\n  github_repo: example/repo\n', encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    _ = request_path.write_text(json.dumps(request, indent=2), encoding="utf-8")
    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_test"}\n'),
    ), patch(
        "scripts.orchestrator_supervisor.subprocess.run",
        side_effect=[
            CompletedProcess(args=["gh"], returncode=1, stdout="", stderr="label sync failed"),
            CompletedProcess(args=["gh"], returncode=0, stdout="", stderr=""),
            CompletedProcess(args=["gh"], returncode=0, stdout="", stderr=""),
        ],
    ):
        result = orchestrator_supervisor._dispatch_consumed_request(
            request_path,
            ledger_path=ledger_path,
            session_result_path=session_result_path,
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    row = read_latest_issue_history(tmp_path, "42", entry_type="root_event")

    assert result.get("status") == "success"
    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_root_session_id"] == "ses_root_test"
    assert issue_lock_path(tmp_path, "42").exists()
    assert "label sync failed" in str(result.get("recommendedAction") or "")
    assert row is not None
    assert row["status"] == "root_session_started"


def test_dispatch_validation_failure_without_ledger_still_restores_ready_state(tmp_path: Path):
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "req-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }
    request_path.parent.mkdir(parents=True, exist_ok=True)
    _ = request_path.write_text(json.dumps(request, indent=2), encoding="utf-8")
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

    result = orchestrator_supervisor._dispatch_consumed_request(
        request_path,
        ledger_path=ledger_path,
        session_result_path=session_result_path,
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


def test_retry_github_sync_command_rejects_stale_failed_attempt(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    config_path = tmp_path / ".autodev.yaml"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
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
        add_labels=["quarantined"],
        remove_labels=["agent-in-progress"],
        status="failed",
        updated_at="2026-05-07T17:03:00+08:00",
        last_error="newer sync failed",
    )

    try:
        orchestrator_supervisor.main([
            "retry-github-sync",
            "--ledger",
            str(ledger_path),
            "--command-id",
            "cmd-old",
        ])
    except ValueError as error:
        assert "is stale" in str(error)
    else:
        raise AssertionError("expected retry-github-sync to reject stale failed attempt")


def test_resolve_opencode_cli_prefers_path_binary():
    with patch("scripts.orchestrator_supervisor.shutil.which", side_effect=["/usr/bin/opencode", None]):
        assert orchestrator_supervisor._resolve_opencode_cli() == "/usr/bin/opencode"


def test_resolve_opencode_cli_falls_back_to_desktop_binary():
    with patch("scripts.orchestrator_supervisor.shutil.which", side_effect=[None, "/usr/bin/opencode-desktop"]), patch(
        "scripts.orchestrator_supervisor.Path.home", return_value=Path("/tmp/no-opencode-home")
    ):
        assert orchestrator_supervisor._resolve_opencode_cli() == "/usr/bin/opencode-desktop"


def test_resolve_opencode_cli_falls_back_to_known_install_path(tmp_path: Path):
    known_binary = tmp_path / ".opencode/bin/opencode"
    known_binary.parent.mkdir(parents=True, exist_ok=True)
    _ = known_binary.write_text("#!/bin/sh\n", encoding="utf-8")

    with patch("scripts.orchestrator_supervisor.shutil.which", side_effect=[None, None]), patch(
        "scripts.orchestrator_supervisor.Path.home", return_value=tmp_path
    ):
        assert orchestrator_supervisor._resolve_opencode_cli() == str(known_binary)


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
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value=None):
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
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }
    fake_process = FakePopen("", stderr="", returncode=None)

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=fake_process,
    ), patch(
        "scripts.orchestrator_supervisor._read_initial_session_id",
        return_value=(None, "", ""),
    ), patch(
        "scripts.orchestrator_supervisor._wait_for_session_id_in_db",
        return_value=None,
    ):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("."),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "error"
    assert "did not emit a sessionID" in str(result.get("error", ""))
    assert fake_process.terminated is True


def test_dispatch_session_request_fails_when_same_repo_session_read_probe_fails():
    request: orchestrator_supervisor.SessionRequest = {
        "requestGeneration": 1,
        "nonce": "nonce-42",
        "requestID": "request-42",
        "createdAt": "2026-05-07T17:00:00+08:00",
        "createdForLedgerRevision": "2026-05-07T17:00:00+08:00",
        "reason": "orchestrator bootstrap continuation for issue #42",
        "title": "Continue issue #42 on agent/issue-42-demo",
        "agent": "build",
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_stdout"}\n'),
    ), patch(
        "scripts.orchestrator_supervisor._probe_same_repo_session_readability",
        return_value=(False, "Session not found: ses_root_stdout"),
    ):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("/tmp/demo"),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "error"
    assert result.get("rootSessionID") == "ses_root_stdout"
    assert result.get("sessionReadabilityStatus") == "failed_same_repo_probe"
    assert "failed same-repo session_read probe" in str(result.get("error", ""))


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
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_stdout"}\n'),
    ), patch(
        "scripts.orchestrator_supervisor._wait_for_session_id_in_db",
        side_effect=AssertionError("DB fallback must not run when stdout already contains sessionID"),
    ) as mocked_wait_for_db, patch(
        "scripts.orchestrator_supervisor._probe_same_repo_session_readability",
        return_value=(True, "ok"),
    ):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("/tmp/demo"),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "success"
    assert result.get("rootSessionID") == "ses_root_stdout"
    assert result.get("sessionReadabilityStatus") == "verified_same_repo_probe"
    mocked_wait_for_db.assert_not_called()


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
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen("", stderr="", returncode=None),
    ), patch(
        "scripts.orchestrator_supervisor._read_initial_session_id",
        return_value=(None, "", ""),
    ), patch(
        "scripts.orchestrator_supervisor._wait_for_session_id_in_db",
        return_value="ses_root_db",
    ), patch(
        "scripts.orchestrator_supervisor._probe_same_repo_session_readability",
        return_value=(True, "ok"),
    ):
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
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen("", stderr="", returncode=None),
    ), patch(
        "scripts.orchestrator_supervisor._read_initial_session_id",
        return_value=(None, "", ""),
    ), patch(
        "scripts.orchestrator_supervisor._wait_for_session_id_in_db",
        return_value="ses_root_db",
    ) as mocked_wait_for_db, patch(
        "scripts.orchestrator_supervisor._probe_same_repo_session_readability",
        return_value=(True, "ok"),
    ):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("/tmp/demo"),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    assert result.get("status") == "success"
    mocked_wait_for_db.assert_called_once()
    assert mocked_wait_for_db.call_args.kwargs["title"] == "Continue issue #42 on agent/issue-42-demo [request-42abcdef]"


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
        "prompt": "Bootstrap from checkpoint only.",
        "role": "main_orchestrator",
        "stage": "orchestrator_bootstrap",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_test"}\n'),
    ) as mocked_spawn, patch(
        "scripts.orchestrator_supervisor._probe_same_repo_session_readability",
        return_value=(True, "ok"),
    ):
        result = orchestrator_supervisor.dispatch_session_request(
            request,
            workdir=Path("."),
            source_session_id="ses_source_test",
            updated_at="2026-05-07T17:10:00+08:00",
        )

    command = mocked_spawn.call_args.args[0]
    assert result.get("status") == "success"
    assert ["--agent", "custom-primary"] == command[6:8]


def test_dispatch_rejects_completed_issue_without_launching_opencode(tmp_path: Path):
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        checkpoint_path="docs/agents/runtime/context-checkpoint.yaml",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="completed",
        command_id="cmd-completed",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    request = build_orchestrator_request(ledger)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    request_path.write_text(json.dumps(request, indent=2), encoding="utf-8")
    ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

    with patch("scripts.orchestrator_supervisor.subprocess.run") as mocked_run:
        exit_code = orchestrator_supervisor.main(
            [
                "dispatch",
                "--request",
                str(request_path),
                "--session-result",
                str(session_result_path),
                "--ledger",
                str(ledger_path),
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(session_result_path.read_text(encoding="utf-8")))

    mocked_run.assert_not_called()
    assert exit_code == 0
    assert session_result["status"] == "rejected"
    assert "already completed or released" in cast(str, session_result["error"])


def test_dispatch_allows_main_orchestrator_recovery_for_failed_issue_without_claim_transition(tmp_path: Path):
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        checkpoint_path="docs/agents/runtime/context-checkpoint.yaml",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    request = {
        "requestGeneration": 2,
        "nonce": "nonce-recovery-42",
        "requestID": "request-recovery-42",
        "createdAt": "2026-05-07T17:10:00+08:00",
        "createdForLedgerRevision": str(ledger["ledgerRevision"]),
        "reason": "main_orchestrator recovery for issue #42",
        "title": "Recover or continue after issue #42",
        "agent": "hephaestus",
        "prompt": "Recovery prompt",
        "role": "main_orchestrator",
        "stage": "issue_selection_or_recovery",
        "issueNumber": "42",
        "branch": "agent/issue-42-demo",
    }
    request_path.write_text(json.dumps(request, indent=2), encoding="utf-8")

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_recovery"}\n'),
    ):
        exit_code = orchestrator_supervisor.main(
            [
                "dispatch",
                "--request",
                str(request_path),
                "--session-result",
                str(session_result_path),
                "--ledger",
                str(ledger_path),
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(session_result_path.read_text(encoding="utf-8")))
    issue = read_issue(tmp_path, "42")

    assert exit_code == 0
    assert session_result["status"] == "success"
    assert session_result["rootSessionID"] == "ses_root_recovery"
    assert issue is not None
    assert issue["state"] == "failed"


def test_reconcile_with_dispatch_now_does_not_create_root_session_for_subagent_role(tmp_path: Path):
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

    with patch("scripts.orchestrator_supervisor.subprocess.run") as mocked_run:
        exit_code = orchestrator_supervisor.main(
            [
                "reconcile",
                "--ledger",
                str(ledger_path),
                "--request",
                str(request_path),
                "--session-result",
                str(session_result_path),
                "--write-request",
                "--dispatch-now",
                "--source-session-id",
                "ses_reconcile_source",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    assert exit_code == 0
    mocked_run.assert_not_called()
    assert not request_path.exists()
    assert not session_result_path.exists()


def test_reconcile_verifier_marks_issue_verifying(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["evidencePacketPath"] = "docs/agents/evidence/issue-42-pr-77.yaml"
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
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_root_session_id="ses-root-42",
    )
    updated_ledger, _, _ = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:11:00+08:00",
    )

    issue = read_issue(tmp_path, "42")
    latest_root_event = read_latest_issue_history(tmp_path, "42", entry_type="root_event")
    artifact_status = _artifact_status(issue)

    assert cast(dict[str, object], updated_ledger["current"])["role"] == "release_worker"
    assert issue is not None
    assert issue["state"] == "verifying"
    assert issue["current_verifier_session_id"] == "ses-v"
    assert cast(dict[str, object], artifact_status["evidence_packet"])["parse_ok"] is True
    assert cast(dict[str, object], artifact_status["evidence_packet"])["status"] == "pass"
    assert cast(dict[str, object], artifact_status["evidence_packet"])["pr_number"] == "77"
    assert latest_root_event is not None
    assert latest_root_event["status"] == "root_terminal"
    assert latest_root_event["session_seq"] == 2


def test_reconcile_verifier_requires_persisted_evidence_fact(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "pr_verifier", "stage": "pr_verifier_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["evidencePacketPath"] = "docs/agents/evidence/issue-42-pr-77.yaml"
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
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:09:00+08:00",
        current_root_session_id="ses-root-42",
    )

    with patch("scripts.orchestrator_supervisor._artifact_fact", return_value={}):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:11:00+08:00",
        )

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert cast(dict[str, object], updated_ledger["lastFailure"])["kind"] == "contract_invalid"
    assert "persisted evidence_packet fact is missing" in cast(str, decision["summary"])


def test_reconcile_allows_early_return_without_extra_cleanup_work(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:05:00+08:00",
    )

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

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:05:00+08:00",
    )

    assert updated_ledger is ledger
    assert decision["action"] == "hold_quarantined_issue"
    assert request is None


def test_reconcile_quarantines_running_issue_when_root_event_goes_stale(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
        current_root_session_id="ses-root-42",
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

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:16:00+08:00",
        )

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

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:01:00+08:00",
    )

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

    with patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        orchestrator_supervisor.resume_quarantined_issue_execution(
            base_dir=tmp_path,
            issue_number="42",
            reason="operator approved fenced resume",
            updated_at="2026-05-07T17:01:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert issue is not None
    assert issue["state"] == "running"


def test_redispatch_quarantined_command_creates_fresh_root_session(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="quarantined",
        command_id="cmd-quarantined",
        updated_at="2026-05-07T17:00:00+08:00",
        current_root_session_id="ses-old-root",
    )
    orchestrator_supervisor.write_issue_lock(
        issue_lock_path(tmp_path, "42"),
        {
            "issueNumber": "42",
            "branch": "agent/issue-42-demo",
            "rootSessionID": "ses-old-root",
            "status": "quarantined",
            "createdAt": "2026-05-07T17:00:00+08:00",
        },
    )

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_retry"}\n'),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        exit_code = orchestrator_supervisor.main(
            [
                "redispatch-quarantined",
                "--ledger",
                str(ledger_path),
                "--request",
                str(request_path),
                "--session-result",
                str(session_result_path),
                "--reason",
                "operator approved fresh root-session redispatch",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(session_result_path.read_text(encoding="utf-8")))
    synced_ledger = cast(dict[str, object], json.loads(ledger_path.read_text(encoding="utf-8")))
    issue = read_issue(tmp_path, "42")
    lock_payload = cast(dict[str, object], json.loads(issue_lock_path(tmp_path, "42").read_text(encoding="utf-8")))

    assert exit_code == 0
    assert not request_path.exists()
    assert session_result["status"] == "success"
    assert session_result["rootSessionID"] == "ses_root_retry"
    assert cast(dict[str, object], synced_ledger["current"])["role"] == "main_orchestrator"
    assert cast(dict[str, object], synced_ledger["current"])["stage"] == "orchestrator_bootstrap"
    assert cast(dict[str, object], synced_ledger["lastSessionResult"])["rootSessionID"] == "ses_root_retry"
    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_root_session_id"] == "ses_root_retry"
    assert lock_payload["rootSessionID"] == "ses_root_retry"
    assert lock_payload["status"] == "root_session_started"


def test_redispatch_quarantined_defaults_runtime_paths_from_consumer_ledger(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    consumer_runtime_dir = ledger_path.parent
    consumer_request_path = consumer_runtime_dir / "new-session-request.json"
    consumer_session_result_path = consumer_runtime_dir / "new-session-result.json"
    framework_runtime_dir = tmp_path / "framework-runtime"
    framework_request_path = framework_runtime_dir / "new-session-request.json"
    framework_session_result_path = framework_runtime_dir / "new-session-result.json"
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="quarantined",
        command_id="cmd-quarantined",
        updated_at="2026-05-07T17:00:00+08:00",
        current_root_session_id="ses-old-root",
    )

    with patch.object(orchestrator_supervisor, "DEFAULT_REQUEST_PATH", framework_request_path), patch.object(
        orchestrator_supervisor,
        "DEFAULT_SESSION_RESULT_PATH",
        framework_session_result_path,
    ), patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value="/usr/bin/opencode"), patch(
        "scripts.orchestrator_supervisor._spawn_detached_opencode_run",
        return_value=FakePopen('{"type":"step_start","sessionID":"ses_root_retry"}\n'),
    ), patch("scripts.orchestrator_supervisor._sync_issue_progress_label", return_value=""):
        exit_code = orchestrator_supervisor.main(
            [
                "redispatch-quarantined",
                "--ledger",
                str(ledger_path),
                "--reason",
                "operator approved fresh root-session redispatch",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(consumer_session_result_path.read_text(encoding="utf-8")))

    assert exit_code == 0
    assert consumer_session_result_path.exists()
    assert not consumer_request_path.exists()
    assert session_result["status"] == "success"
    assert session_result["rootSessionID"] == "ses_root_retry"
    assert not framework_request_path.exists()
    assert not framework_session_result_path.exists()


def test_redispatch_quarantined_command_restores_quarantine_when_dispatch_fails(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    request_path = tmp_path / ".opencode/runtime/new-session-request.json"
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    issue_packet_path = tmp_path / "docs/agents/issue-packets/issue-42.yaml"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    issue_packet_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    issue_packet_path.write_text(SAMPLE_ISSUE_PACKET, encoding="utf-8")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="quarantined",
        command_id="cmd-quarantined",
        updated_at="2026-05-07T17:00:00+08:00",
        current_root_session_id="ses-old-root",
    )

    with patch("scripts.orchestrator_supervisor._resolve_opencode_cli", return_value=None), patch(
        "scripts.orchestrator_supervisor._sync_issue_progress_label",
        return_value="",
    ):
        exit_code = orchestrator_supervisor.main(
            [
                "redispatch-quarantined",
                "--ledger",
                str(ledger_path),
                "--request",
                str(request_path),
                "--session-result",
                str(session_result_path),
                "--reason",
                "operator approved fresh root-session redispatch",
                "--source-session-id",
                "ses_source_test",
                "--updated-at",
                "2026-05-07T17:10:00+08:00",
            ]
        )

    session_result = cast(dict[str, object], json.loads(session_result_path.read_text(encoding="utf-8")))
    issue = read_issue(tmp_path, "42")

    assert exit_code == 0
    assert session_result["status"] == "error"
    assert not request_path.exists()
    assert issue is not None
    assert issue["state"] == "quarantined"
    assert issue["current_root_session_id"] == ""
    assert not issue_lock_path(tmp_path, "42").exists()


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
    )
    orchestrator_supervisor.sync_issue_runtime_context(
        tmp_path,
        issue_number="42",
        updated_at="2026-05-07T17:00:00+08:00",
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
    assert latest_decision is not None
    assert latest_decision["decision_type"] == "admin_retry_failed_issue"
    assert latest_decision["to_state"] == "ready"


def test_retry_failed_issue_execution_clears_stale_issue_artifacts(tmp_path: Path):
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

    assert not stale_worker.exists()
    assert not stale_handoff.exists()
    assert not stale_evidence.exists()
    assert not stale_release.exists()


def test_retry_failed_command_rejects_non_retryable_issue(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
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
            "--ledger",
            str(ledger_path),
            "--reason",
            "operator retry",
        ])
    except ValueError as error:
        assert "not retryable" in str(error)
    else:
        raise AssertionError("expected retry-failed to reject non-retryable issues")


def test_quarantine_command_marks_issue_quarantined(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
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
                "--ledger",
                str(ledger_path),
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


def test_reconcile_bootstrap_rebuilds_running_state_from_session_result_root_id(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="dispatching",
        command_id="cmd-dispatching",
        updated_at="2026-05-07T17:04:00+08:00",
    )
    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    session_result_path.parent.mkdir(parents=True, exist_ok=True)
    session_result_path.write_text(
        json.dumps(
            {
                "status": "success",
                "rootSessionID": "ses-root-42",
                "recordedAt": "2026-05-07T17:04:30+08:00",
            }
        ),
        encoding="utf-8",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=session_result_path,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:05:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert cast(dict[str, object], updated_ledger["current"])["role"] == "issue_worker"
    assert decision["next_role"] == "issue_worker"
    assert request is None
    assert issue is not None
    assert issue["state"] == "running"
    assert issue["current_root_session_id"] == "ses-root-42"


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

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:05:00+08:00",
    )

    issue = read_issue(tmp_path, "42")
    current = cast(dict[str, object], updated_ledger["current"])

    assert current == {"role": "issue_worker", "stage": "issue_worker_execution", "status": "queued"}
    assert decision["action"] == "no_change"
    assert request is None
    assert issue is not None
    assert issue["state"] == "dispatching"


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
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        checkpoint_path="docs/agents/runtime/context-checkpoint.yaml",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    ledger["current"] = {"role": "release_worker", "stage": "release_worker_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "success"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="verifying",
        command_id="cmd-verifying",
        updated_at="2026-05-07T17:19:00+08:00",
        current_verifier_session_id="ses-v",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:21:00+08:00",
    )

    issue = read_issue(tmp_path, "31")

    assert updated_ledger is not None
    assert decision["action"] in {"queue_next_session", "queue_next_issue"}
    assert request is not None
    assert issue is not None
    assert issue["state"] == "completed"
    assert issue["current_verifier_session_id"] == "ses-v"


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
    checkpoint_path = tmp_path / "docs/agents/runtime/context-checkpoint.yaml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        'schema_version: "1.0"\nkind: context_checkpoint\nline_cap: 80\n\nsubject:\n  issue_number: "31"\n  branch: "agent/issue-31-demo"\n  role: "main_orchestrator"\n  checkpoint_reason: "selected_afk_issue"\n\ncontext_budget:\n  warning_at_percent: 45\n  stop_and_rotate_at_percent: 50\n  measured_percent_used: "unknown"\n  must_rotate_now: false\n\nresume_policy:\n  checkpoint_only_cross_session_resume: true\n  do_not_import_full_prior_transcript: true\n  raw_evidence_policy: "index_only"\n\nstate:\n  completed:\n    - "Issue #31 released."\n  in_progress: []\n  next: []\n  blockers: []\n\nrefs:\n  issue_packet: "docs/agents/issue-packets/issue-31.yaml"\n  worker_result: ""\n  evidence_packet: ""\n  handoff: "docs/agents/handoffs/issue-31.yaml"\n  artifact_bundle: ""\n\nmetadata:\n  updated_by: "Build"\n  updated_at: "2026-05-07T17:00:00+08:00"\n',
        encoding="utf-8",
    )

    issue_packet = parse_issue_packet_text(issue_31.read_text(encoding="utf-8"), "docs/agents/issue-packets/issue-31.yaml")
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        checkpoint_path="docs/agents/runtime/context-checkpoint.yaml",
        updated_at="2026-05-07T17:00:00+08:00",
    )
    ledger["current"] = {"role": "release_worker", "stage": "release_worker_execution", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path = tmp_path / "docs/agents/release-results/issue-31-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "31"\n  pr_number: "88"\n  branch: "agent/issue-31-demo"\nstatus: "success"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: true, routed_to: "main_orchestrator", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="31",
        state="verifying",
        command_id="cmd-verifying",
        updated_at="2026-05-07T17:19:00+08:00",
        current_verifier_session_id="ses-v",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:21:00+08:00",
    )

    session_result_path = tmp_path / ".opencode/runtime/new-session-result.json"
    session_result_path.parent.mkdir(parents=True, exist_ok=True)
    session_result_path.write_text(
        json.dumps(
            {
                "status": "success",
                "sourceSessionID": "supervisor-redispatch",
                "rootSessionID": "ses-root-31",
                "issueNumber": "31",
                "branch": "agent/issue-31-demo",
                "role": "main_orchestrator",
                "stage": "orchestrator_bootstrap",
                "recordedAt": "2026-05-07T17:20:30+08:00",
            }
        ),
        encoding="utf-8",
    )

    next_ledger, next_decision, next_request = reconcile_ledger(
        cast(dict[str, object], updated_ledger),
        session_result_path=session_result_path,
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:22:00+08:00",
    )

    next_issue = cast(dict[str, object], next_ledger["issue"])

    assert decision["action"] in {"queue_next_session", "queue_next_issue"}
    assert request is not None
    assert next_issue["number"] == "32"
    assert next_decision["action"] == "delegate_subagent"
    assert next_decision["next_role"] == "issue_worker"
    assert next_request is None
    assert cast(dict[str, object], next_ledger.get("lastSessionResult", {})) == {}


def test_reconcile_release_blocked_exhaustion_marks_issue_failed(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "release_worker", "stage": "release_worker_execution", "status": "queued"}
    attempts = cast(dict[str, int], ledger["attempts"])
    limits = cast(dict[str, int], ledger["limits"])
    attempts["release_worker"] = limits["release_worker"]
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "blocked"\nblocked_reason: "policy_blocked"\nsummary:\n  outcome: "blocked"\n  next_recommended_step: "manual follow-up"\nfailure_classification: {kind: "release_blocked", retryable: false, routed_to: "main_orchestrator", root_cause_signature: "policy"}\nmerge:\n  attempted: true\n  merged: false\n  merged_sha: ""\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:20:00+08:00"\n',
        encoding="utf-8",
    )
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verifying",
        command_id="cmd-verifying",
        updated_at="2026-05-07T17:19:00+08:00",
        current_verifier_session_id="ses-v",
    )

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=tmp_path / "missing.json",
        artifact_base_dir=tmp_path,
        updated_at="2026-05-07T17:21:00+08:00",
    )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "failed"
    assert issue["current_verifier_session_id"] == "ses-v"


def test_reconcile_late_successful_release_result_recovers_failed_issue_to_completed(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:21:00+08:00",
        current_verifier_session_id="ses-v",
    )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "completed"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: false\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:23:00+08:00",
        )

    issue = read_issue(tmp_path, "42")
    artifact_status = _artifact_status(issue)

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "completed"
    assert issue["current_verifier_session_id"] == "ses-v"
    assert cast(dict[str, object], artifact_status["release_result"])["parse_ok"] is True
    assert cast(dict[str, object], artifact_status["release_result"])["status"] == "completed"
    assert cast(dict[str, object], artifact_status["release_result"])["blocked_reason"] == "none"


def test_reconcile_late_release_success_requires_persisted_release_fact(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:21:00+08:00",
        current_verifier_session_id="ses-v",
    )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "completed"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: false\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False), patch(
        "scripts.orchestrator_supervisor._artifact_fact", return_value={}
    ):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:23:00+08:00",
        )

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
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:21:00+08:00",
        current_verifier_session_id="ses-v",
    )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "success"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:23:00+08:00",
        )

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
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:21:00+08:00",
        current_verifier_session_id="ses-v",
    )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "success"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: true\n  merged: true\n  merged_sha: "abc"\nworkspace_hygiene:\n  cleanup_status: "pass"\n  blocked_reason: "none"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:23:00+08:00",
        )

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
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready",
        updated_at="2026-05-07T17:21:00+08:00",
        current_verifier_session_id="ses-v",
    )
    release_path = tmp_path / "docs/agents/release-results/issue-42-pr-88.yaml"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text(
        'schema_version: "1.0"\nkind: release_result\nline_cap: 60\nsubject:\n  issue_number: "42"\n  pr_number: "88"\n  branch: "agent/issue-42-demo"\nstatus: "completed"\nblocked_reason: "none"\nsummary:\n  outcome: "merged"\n  next_recommended_step: "continue"\nfailure_classification: {kind: "none", retryable: false, routed_to: "none", root_cause_signature: "none"}\nmerge:\n  attempted: false\n  merged: true\n  merged_sha: "abc"\nrole_boundary:\n  actor_role: "release_worker"\n  may_run_final_acceptance_qa: false\n  may_merge_only_after_verifier_pass: true\nmetadata:\n  worker: "r"\n  worker_session_id: "ses-r"\n  completed_at: "2026-05-07T17:22:00+08:00"\n',
        encoding="utf-8",
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:23:00+08:00",
        )

    issue = read_issue(tmp_path, "42")

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert issue is not None
    assert issue["state"] == "completed"
    assert issue["current_verifier_session_id"] == "ses-v"


def test_reconcile_issue_selection_or_recovery_queues_retryable_failed_issue_recovery(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger["current"] = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
    ledger["lastFailure"] = {
        "kind": "approval_blocked",
        "summary": "Obtain required human approval, then rerun release_worker.",
        "retryable": True,
    }
    cast(dict[str, str], ledger["artifacts"])["releaseResultPath"] = "docs/agents/release-results/issue-42-pr-88.yaml"
    orchestrator_supervisor.upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="failed",
        command_id="cmd-failed",
        updated_at="2026-05-07T17:21:00+08:00",
        current_verifier_session_id="ses-v",
    )

    with patch("scripts.orchestrator_supervisor.run_issue_packet_intake", return_value=False):
        updated_ledger, decision, request = reconcile_ledger(
            ledger,
            session_result_path=tmp_path / "missing.json",
            artifact_base_dir=tmp_path,
            updated_at="2026-05-07T17:23:00+08:00",
        )

    assert updated_ledger is not None
    assert decision["action"] == "queue_next_session"
    assert request is not None
    assert request["role"] == "main_orchestrator"
    assert request["stage"] == "issue_selection_or_recovery"


def test_inspect_command_prints_control_plane_snapshot(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
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
    output = io.StringIO()
    with redirect_stdout(output):
        exit_code = orchestrator_supervisor.main([
            "inspect",
            "--ledger",
            str(ledger_path),
        ])

    payload = cast(dict[str, object], json.loads(output.getvalue()))

    assert exit_code == 0
    assert set(payload) == {"issue", "latestDecision", "latestGitHubSyncAttempt"}
    assert cast(dict[str, object], payload["issue"])["issue_number"] == "42"
    assert cast(dict[str, object], payload["latestDecision"])["command_id"] == "cmd-claim"
    assert cast(dict[str, object], payload["latestGitHubSyncAttempt"])["command_id"] == "cmd-gh"


def test_retry_github_sync_command_replays_failed_attempt(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    config_path = tmp_path / ".autodev.yaml"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
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
            "--ledger",
            str(ledger_path),
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


def test_retry_github_sync_command_rejects_non_failed_attempt(tmp_path: Path):
    issue_packet = parse_issue_packet_text(SAMPLE_ISSUE_PACKET, "docs/agents/issue-packets/issue-42.yaml")
    ledger = create_initial_ledger(issue_packet=issue_packet, updated_at="2026-05-07T17:00:00+08:00")
    ledger_path = tmp_path / ".opencode/runtime/orchestrator-ledger.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    _ = ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
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
            "--ledger",
            str(ledger_path),
            "--command-id",
            "cmd-gh",
        ])
    except ValueError as error:
        assert "is not failed" in str(error)
    else:
        raise AssertionError("expected retry-github-sync to reject non-failed attempts")
