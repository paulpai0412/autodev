#!/usr/bin/env python3
"""Runtime supervisor for nonstop autonomous issue dispatch."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType
from uuid import uuid4
from typing import Callable, NotRequired, Protocol, TypedDict, cast

from scripts.control_plane_db import (
    append_issue_event,
    append_issue_history,
    completed_issue_numbers,
    control_plane_db_path,
    describe_control_plane_schema,
    ensure_control_plane_db,
    ensure_issue_row,
    ingest_issue_packet,
    issue_rows_with_packets,
    list_issues,
    issues_in_states,
    ready_issues_for_selection,
    read_latest_history_entry,
    read_issue_packet,
    record_admin_decision,
    read_latest_decision,
    read_latest_github_sync_attempt,
    read_github_sync_attempt_by_command_id,
    read_issue,
    read_runtime_context,
    record_github_sync_attempt,
    sync_issue_runtime_context,
    transition_issue_state,
    upsert_issue_ranking,
    upsert_issue_state,
)
from scripts.host_adapter import HostAdapter, SessionStartContext
from scripts.orchestrator_compact_payload import write_checkpoint_file


JsonObject = dict[str, object]


class IssuePacketRecord(Protocol):
    issue_number: str
    title: str
    branch: str
    issue_packet_path: str
    backing_type: str
    prior_handoff: str
    labels: list[str]
    parent_reference: str
    dependencies: list[str]
    raw_text: str


def _normalize_requested_issue_number(issue_number: str) -> str:
    normalized = issue_number.strip().removeprefix("#").removeprefix("issue-")
    if not normalized.isdigit():
        raise RuntimeError(f"issue number must be numeric, got {issue_number!r}")
    return normalized


def _validate_start_packet_issue_number(*, requested_issue_number: str, packet: IssuePacketRecord) -> str:
    packet_issue_number = str(packet.issue_number).strip()
    if packet_issue_number != requested_issue_number or not packet_issue_number.isdigit():
        raise RuntimeError(
            f"issue packet number mismatch for requested issue #{requested_issue_number}: got {packet_issue_number!r}"
        )
    return packet_issue_number


def _load_artifact_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_artifacts.py")
    spec = importlib.util.spec_from_file_location("orchestrator_artifacts", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load artifact helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_artifact_helpers = _load_artifact_helpers()
_dependency_issue_numbers = cast(Callable[[str, list[str]], list[str]], _artifact_helpers._dependency_issue_numbers)
_extract_nested_scalar = cast(Callable[[str, str, str], str], _artifact_helpers._extract_nested_scalar)
_is_successful_release_status = cast(Callable[[str], bool], _artifact_helpers._is_successful_release_status)
_parse_issue_numbers = cast(Callable[[str], list[str]], _artifact_helpers._parse_issue_numbers)
default_evidence_packet_path = cast(Callable[[str, str], str], _artifact_helpers.default_evidence_packet_path)
default_release_result_path = cast(Callable[[str, str], str], _artifact_helpers.default_release_result_path)
default_worker_result_path = cast(Callable[[str], str], _artifact_helpers.default_worker_result_path)
issue_packet_record_from_json = cast(Callable[[dict[str, object]], IssuePacketRecord | None], _artifact_helpers.issue_packet_record_from_json)
issue_packet_record_to_json = cast(Callable[[IssuePacketRecord], JsonObject], _artifact_helpers.issue_packet_record_to_json)
parse_evidence_packet_file = cast(Callable[[Path], JsonObject], _artifact_helpers.parse_evidence_packet_file)
parse_issue_packet_text = cast(Callable[[str, str], IssuePacketRecord], _artifact_helpers.parse_issue_packet_text)
parse_release_result_file = cast(Callable[[Path], JsonObject], _artifact_helpers.parse_release_result_file)
parse_worker_result_file = cast(Callable[[Path], JsonObject], _artifact_helpers.parse_worker_result_file)


def _artifact_status_snapshot(
    *,
    artifact_kind: str,
    artifact_path: Path,
    observed_at: str,
    parsed: JsonObject,
) -> JsonObject:
    snapshot: JsonObject = {
        "path": str(artifact_path),
        "observed_at": observed_at,
        "parse_ok": True,
    }
    if artifact_kind == "worker_result":
        snapshot.update(
            {
                "status": str(parsed.get("status") or ""),
                "pr_number": str(parsed.get("pr_number") or ""),
                "completed_at": str(parsed.get("completed_at") or ""),
            }
        )
    elif artifact_kind == "evidence_packet":
        snapshot.update(
            {
                "status": str(parsed.get("status") or ""),
                "pr_number": str(parsed.get("pr_number") or ""),
                "verifier_session_id": str(parsed.get("verifier_session_id") or ""),
            }
        )
    elif artifact_kind == "release_result":
        snapshot.update(
            {
                "status": str(parsed.get("status") or ""),
                "blocked_reason": str(parsed.get("blocked_reason") or ""),
            }
        )
    return snapshot


def _read_artifact_status(issue: dict[str, object] | None) -> dict[str, object]:
    if not issue:
        return {}
    raw = str(issue.get("artifact_status_json") or "{}")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _artifact_fact(issue: dict[str, object] | None, artifact_kind: str) -> dict[str, object]:
    artifact_status = _read_artifact_status(issue)
    payload = artifact_status.get(artifact_kind, {})
    return payload if isinstance(payload, dict) else {}


def _record_artifact_status(
    *,
    base_dir: Path,
    issue_number: str,
    artifact_kind: str,
    artifact_path: Path,
    observed_at: str,
    parsed: JsonObject,
) -> None:
    issue = read_issue(base_dir, issue_number) or {}
    artifact_status = _read_artifact_status(issue)
    artifact_status[artifact_kind] = _artifact_status_snapshot(
        artifact_kind=artifact_kind,
        artifact_path=artifact_path,
        observed_at=observed_at,
        parsed=parsed,
    )
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=observed_at,
        artifact_status=artifact_status,
    )


def _load_session_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_sessions.py")
    spec = importlib.util.spec_from_file_location("orchestrator_sessions", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load session helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_session_helpers = _load_session_helpers()


def _load_lifecycle_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_lifecycle.py")
    spec = importlib.util.spec_from_file_location("orchestrator_lifecycle", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load lifecycle helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_lifecycle_helpers = _load_lifecycle_helpers()


def _load_request_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_requests.py")
    spec = importlib.util.spec_from_file_location("orchestrator_requests", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load request helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_request_helpers = _load_request_helpers()


def _load_selection_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_selection.py")
    spec = importlib.util.spec_from_file_location("orchestrator_selection", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load selection helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_selection_helpers = _load_selection_helpers()


def _load_reconcile_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_reconcile.py")
    spec = importlib.util.spec_from_file_location("orchestrator_reconcile", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load reconcile helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_reconcile_helpers = _load_reconcile_helpers()


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER_PATH = ROOT / ".opencode/runtime/orchestrator-ledger.json"
DEFAULT_REQUEST_PATH = ROOT / ".opencode/runtime/new-session-request.json"
DEFAULT_SESSION_RESULT_PATH = ROOT / ".opencode/runtime/new-session-result.json"
DEFAULT_ISSUE_INTAKE_SCRIPT_PATH = ROOT / "scripts/issue_packet_intake.py"
DEFAULT_CHECKPOINT_PATH = "docs/agents/runtime/context-checkpoint.yaml"
DEFAULT_WORKFLOW_POLICY_PATH = str(ROOT / "docs/agents/autonomous-development-workflow.yaml")
DEFAULT_SUPERVISOR_DOC_PATH = str(ROOT / "docs/agents/runtime/nonstop-supervisor-loop.md")
DEFAULT_RELEASE_RESULT_TEMPLATE_PATH = str(ROOT / "docs/agents/release-result-template.yaml")
DEFAULT_ROOT_SESSION_AGENT = "build"
READY_FOR_AGENT_LABEL = "ready-for-agent"
AGENT_DISPATCHING_LABEL = "agent-dispatching"
AGENT_IN_PROGRESS_LABEL = "agent-in-progress"
QUARANTINED_LABEL = "quarantined"
MAX_ROLE_ATTEMPTS = 3
ROOT_HEARTBEAT_TIMEOUT_SECONDS = 900
TRANSIENT_RELEASE_BLOCKERS = {
    "required_checks_pending",
    "required_checks_failed",
    "pr_not_mergeable",
    "workspace_hygiene_failed",
    "transient_tool_failure",
}


def _clear_issue_runtime_artifacts(*, base_dir: Path, issue_number: str) -> None:
    artifact_paths = [
        base_dir / "docs/agents/worker-results" / f"issue-{issue_number}.yaml",
        base_dir / "docs/agents/handoffs" / f"issue-{issue_number}.yaml",
    ]
    artifact_paths.extend((base_dir / "docs/agents/evidence").glob(f"issue-{issue_number}-pr-*.yaml"))
    artifact_paths.extend((base_dir / "docs/agents/release-results").glob(f"issue-{issue_number}-pr-*.yaml"))
    for artifact_path in artifact_paths:
        artifact_path.unlink(missing_ok=True)


class SessionRequest(TypedDict):
    requestGeneration: int
    nonce: str
    requestID: str
    createdAt: str
    createdForLedgerRevision: str
    reason: str
    title: str
    agent: str
    prompt: str
    role: str
    stage: str
    issueNumber: str
    branch: str
    selectedIssueNumber: NotRequired[str]
    selectedIssueBranch: NotRequired[str]
    selectedIssuePacketPath: NotRequired[str]


class SessionResult(TypedDict, total=False):
    status: str
    sourceSessionID: str
    rootSessionID: str
    launchTitle: str
    title: str
    reason: str
    error: str
    tuiResumeCommand: str
    cliOpenCommand: str
    recommendedAction: str
    sessionReadabilityStatus: str
    stopContinuationStatus: str
    stopContinuationAttempts: int
    role: str
    stage: str
    issueNumber: str
    branch: str
    recordedAt: str


def _session_request_body(request: SessionRequest) -> str:
    return json.dumps(dict(request), ensure_ascii=False, indent=2)


def _session_result_body(result: SessionResult) -> str:
    return json.dumps(dict(result), ensure_ascii=False, indent=2)


def _content_hash(text: str) -> str:
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


def _record_dispatch_request_history(
    *,
    base_dir: Path,
    request: SessionRequest,
    created_at: str,
) -> int:
    request_id = str(request.get("requestID") or "")
    body_text = _session_request_body(request)
    return append_issue_history(
        base_dir,
        issue_number=request["issueNumber"],
        entry_type="dispatch_request",
        created_at=created_at,
        role=request["role"],
        stage=request["stage"],
        status="queued",
        request_id=request_id,
        command_id=request_id,
        summary=request["reason"],
        payload=dict(request),
        body_text=body_text,
        content_hash=_content_hash(body_text),
        unique_key=f"dispatch-request:{request_id or created_at}",
    )


def _record_dispatch_result_history(
    *,
    base_dir: Path,
    session_result: SessionResult,
) -> int:
    issue_number = str(session_result.get("issueNumber") or "")
    recorded_at = str(session_result.get("recordedAt") or "")
    request_id = str(session_result.get("sourceSessionID") or "")
    root_session_id = str(session_result.get("rootSessionID") or "")
    status = str(session_result.get("status") or "")
    if not issue_number or not recorded_at:
        return 0
    body_text = _session_result_body(session_result)
    return append_issue_history(
        base_dir,
        issue_number=issue_number,
        entry_type="dispatch_result",
        created_at=recorded_at,
        role=str(session_result.get("role") or ""),
        stage=str(session_result.get("stage") or ""),
        status=status,
        session_id=root_session_id,
        request_id=request_id,
        command_id=request_id,
        summary=str(session_result.get("reason") or status),
        payload=dict(session_result),
        body_text=body_text,
        content_hash=_content_hash(body_text),
        unique_key=f"dispatch-result:{request_id or recorded_at}",
    )


def _record_runtime_transition_history(
    *,
    base_dir: Path,
    issue_number: str,
    recorded_at: str,
    from_role: str,
    from_stage: str,
    to_role: str,
    to_stage: str,
    reason: str,
) -> int:
    body_text = f"{from_role}/{from_stage} -> {to_role}/{to_stage}"
    unique_key = f"runtime-transition:{issue_number}:{recorded_at}:{from_role}:{from_stage}:{to_role}:{to_stage}"
    payload = json.dumps(
        {
            "transition_type": "runtime_role_stage",
            "from_role": from_role,
            "from_stage": from_stage,
            "to_role": to_role,
            "to_stage": to_stage,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    ensure_control_plane_db(base_dir)
    connection = sqlite3.connect(control_plane_db_path(base_dir))
    try:
        cursor = connection.execute(
            "SELECT history_id FROM issue_history WHERE unique_key = ?",
            (unique_key,),
        )
        existing = cursor.fetchone()
        if existing is not None:
            return int(existing[0])
        cursor = connection.execute(
            """
            INSERT INTO issue_history (
                issue_number,
                entry_type,
                role,
                stage,
                status,
                summary,
                payload_json,
                body_text,
                content_hash,
                created_at,
                unique_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                issue_number,
                "runtime_transition",
                to_role,
                to_stage,
                "queued",
                reason,
                payload,
                body_text,
                _content_hash(body_text),
                recorded_at,
                unique_key,
            ),
        )
        connection.commit()
        history_id_raw = cursor.lastrowid
        if history_id_raw is None:
            raise RuntimeError("failed to insert runtime_transition history row")
        return int(history_id_raw)
    finally:
        connection.close()


def _backfill_runtime_transition_history(*, base_dir: Path, ledger: JsonObject) -> None:
    issue = cast(dict[str, str], ledger.get("issue", {}))
    issue_number = issue.get("number", "")
    if not issue_number:
        return
    history = cast(list[JsonObject], ledger.get("history", []))
    for entry in history:
        recorded_at = str(entry.get("recordedAt") or "")
        from_role = str(entry.get("fromRole") or "")
        from_stage = str(entry.get("fromStage") or "")
        to_role = str(entry.get("toRole") or "")
        to_stage = str(entry.get("toStage") or "")
        reason = str(entry.get("reason") or "")
        if not (recorded_at and from_role and to_role and to_stage):
            continue
        _record_runtime_transition_history(
            base_dir=base_dir,
            issue_number=issue_number,
            recorded_at=recorded_at,
            from_role=from_role,
            from_stage=from_stage,
            to_role=to_role,
            to_stage=to_stage,
            reason=reason,
        )


def read_latest_dispatch_result(base_dir: Path, *, issue_number: str | None = None) -> SessionResult | None:
    row = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="dispatch_result")
    if row is None:
        return None
    payload = json.loads(str(row.get("payload_json") or "{}"))
    return cast(SessionResult, cast(object, payload)) if isinstance(payload, dict) else None


def show_latest_session(*, base_dir: Path, issue_number: str | None = None) -> SessionResult | None:
    if issue_number:
        issue = read_issue(base_dir, issue_number)
        if issue is not None:
            current_session_id = str(issue.get("current_session_id") or "")
            result = read_latest_dispatch_result(base_dir, issue_number=issue_number)
            if result is not None and current_session_id and not result.get("cliOpenCommand"):
                result["cliOpenCommand"] = _default_host_adapter().resume_link(current_session_id)
            return result
    active_issues = list_issues(base_dir, require_current_session=True)
    if active_issues:
        latest_issue = active_issues[0]
        latest_issue_number = str(latest_issue.get("issue_number") or "")
        result = read_latest_dispatch_result(base_dir, issue_number=latest_issue_number)
        current_session_id = str(latest_issue.get("current_session_id") or "")
        if result is not None and current_session_id and not result.get("cliOpenCommand"):
            result["cliOpenCommand"] = _default_host_adapter().resume_link(current_session_id)
        return result
    return read_latest_dispatch_result(base_dir)


def start_issue(
    *,
    base_dir: Path,
    issue_number: str,
    source_session_id: str,
    approval_override_mode: str | None = None,
    override_source: str | None = None,
    human_approval_skipped: bool | None = None,
    updated_at: str | None = None,
) -> SessionResult:
    ensure_control_plane_db(base_dir)
    normalized_issue_number = _normalize_requested_issue_number(issue_number)
    packet = _load_issue_packet_from_db(base_dir, normalized_issue_number)
    if packet is None:
        issue_packet_path = base_dir / "docs/agents/issue-packets" / f"issue-{normalized_issue_number}.yaml"
        if not issue_packet_path.exists() and not run_issue_packet_intake(base_dir):
            raise RuntimeError(f"issue packet not found for issue #{normalized_issue_number}")
        if not issue_packet_path.exists():
            raise RuntimeError(f"issue packet not found for issue #{normalized_issue_number}: {issue_packet_path}")
        packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), str(issue_packet_path.relative_to(base_dir)))
        _validate_start_packet_issue_number(requested_issue_number=normalized_issue_number, packet=packet)
        _sync_issue_packet_to_db(base_dir, packet, updated_at=updated_at)

    issue_number = _validate_start_packet_issue_number(requested_issue_number=normalized_issue_number, packet=packet)
    issue_state = read_issue(base_dir, issue_number)
    state = str(issue_state.get("state") or "ready") if issue_state else "ready"
    if state != "ready":
        current_session_id = str(issue_state.get("current_session_id") or "") if issue_state else ""
        resume_suffix = f" Resume with: {_default_host_adapter().resume_link(current_session_id)}." if current_session_id else ""
        raise RuntimeError(f"issue #{issue_number} is already {state}; refusing duplicate start.{resume_suffix}")

    timestamp = _now(updated_at)
    _clear_issue_runtime_artifacts(base_dir=base_dir, issue_number=issue_number)
    ensure_issue_row(base_dir, issue_number=issue_number, updated_at=timestamp)
    _ = upsert_issue_state(
        base_dir=base_dir,
        issue_number=issue_number,
        state="claimed",
        command_id=f"start-issue:{issue_number}:claimed",
        updated_at=timestamp,
    )
    sync_error = _sync_issue_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
        add_labels=[AGENT_DISPATCHING_LABEL],
        remove_labels=[READY_FOR_AGENT_LABEL, AGENT_IN_PROGRESS_LABEL, QUARANTINED_LABEL],
        command_id=f"start-issue:{issue_number}:labels",
        updated_at=timestamp,
    )
    if sync_error:
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="ready",
            command_id=f"start-issue:{issue_number}:labels-rollback",
            updated_at=timestamp,
            current_session_id="",
        )
        raise RuntimeError(f"failed to sync GitHub in-progress state for issue #{issue_number}: {sync_error}")
    ledger = create_initial_ledger(
        issue_packet=packet,
        checkpoint_path=str(base_dir / DEFAULT_CHECKPOINT_PATH),
        workflow_policy_path=DEFAULT_WORKFLOW_POLICY_PATH,
        primary_workspace_root=str(base_dir),
        root_session_agent=DEFAULT_ROOT_SESSION_AGENT,
        updated_at=timestamp,
    )
    workflow = cast(dict[str, object], ledger["workflow"])
    workflow["runtimeControls"] = {
        "approval_override_mode": approval_override_mode or "",
        "default_merge_approval_mode": "human_required",
        "override_source": override_source or "none",
        "human_approval_skipped": bool(human_approval_skipped),
    }
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=timestamp,
        runtime_context={
            "workflow_start_runtime_controls": cast(dict[str, object], workflow["runtimeControls"]),
        },
    )
    request = build_orchestrator_request(ledger)
    _record_dispatch_request_history(base_dir=base_dir, request=request, created_at=str(request.get("createdAt") or timestamp))
    dispatch_command_id = str(request.get("requestID") or uuid4().hex)
    _transition_issue_state_if_possible(
        base_dir=base_dir,
        issue_number=issue_number,
        to_state="dispatching",
        command_id=dispatch_command_id,
        updated_at=timestamp,
        reason=f"Dispatch root session request for issue #{issue_number}.",
        from_state="claimed",
    )
    session_result = dispatch_session_request(
        request,
        workdir=base_dir,
        source_session_id=source_session_id,
        updated_at=timestamp,
    )
    if session_result.get("status") == "success":
        root_session_id = str(session_result.get("rootSessionID") or "")
        recorded_at = str(session_result.get("recordedAt") or timestamp)
        _transition_issue_state_if_possible(
            base_dir=base_dir,
            issue_number=issue_number,
            to_state="running",
            command_id=f"{dispatch_command_id}:running",
            updated_at=recorded_at,
            reason=f"Root session {root_session_id} acknowledged for issue #{issue_number}.",
            from_state="dispatching",
            current_session_id=root_session_id,
        )
        _append_root_issue_event(
            base_dir=base_dir,
            issue_number=issue_number,
            root_session_id=root_session_id,
            event_type="root_session_started",
            created_at=recorded_at,
            payload=cast(JsonObject, cast(object, dict(session_result))),
            session_seq=1,
        )
        sync_error = _sync_issue_progress_label(
            base_dir=base_dir,
            issue_number=issue_number,
            add_labels=[AGENT_IN_PROGRESS_LABEL],
            remove_labels=[AGENT_DISPATCHING_LABEL],
            command_id=f"start-issue:{issue_number}:running-labels",
            updated_at=recorded_at,
        )
        if sync_error:
            session_result["recommendedAction"] = (
                f"Resume the active root session with {_default_host_adapter().resume_link(root_session_id)}. "
                f"GitHub running-label sync failed and may need retry: {sync_error}"
            )
        # Persist the initial bootstrap -> issue_worker handoff in SQLite before the
        # first issue_worker launch so verifier evidence can come from the control plane.
        ledger["lastSessionResult"] = dict(session_result)
        _bump_ledger_revision(ledger, recorded_at)
        reconcile_ledger(
            ledger,
            session_result_path=base_dir / ".opencode/runtime/_no-session-result.json",
            artifact_base_dir=base_dir,
            updated_at=recorded_at,
        )
    else:
        failure_updated_at = str(session_result.get("recordedAt") or timestamp)
        release_issue_execution(
            base_dir=base_dir,
            issue_number=issue_number,
            restore_ready_for_agent=True,
            updated_at=failure_updated_at,
        )
    _record_dispatch_result_history(base_dir=base_dir, session_result=session_result)
    return session_result


class SupervisorDecision(TypedDict):
    action: str
    next_role: str
    next_stage: str
    summary: str
    request_title: str
    subagent_prompt: NotRequired[str]


def _role_execution_mode(role: str) -> str:
    return "root_session" if role == "main_orchestrator" else "orchestrator_subagent"


def _root_session_agent(ledger: JsonObject) -> str:
    automation = cast(dict[str, object], ledger.get("automation", {}))
    configured = automation.get("rootSessionAgent")
    return configured if isinstance(configured, str) and configured else DEFAULT_ROOT_SESSION_AGENT


def _cli_agent_name(agent_name: str) -> str | None:
    normalized = agent_name.strip()
    if not normalized:
        return None
    if normalized.lower() == DEFAULT_ROOT_SESSION_AGENT:
        return None
    return normalized


def _now(updated_at: str | None = None) -> str:
    return updated_at or datetime.now().astimezone().isoformat(timespec="seconds")


def _parse_iso8601(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _root_event_id(*, issue_number: str, root_session_id: str, event_type: str, created_at: str) -> str:
    return ":".join(["issue", issue_number, root_session_id or "unknown-root", event_type, created_at])


def _append_root_issue_event(
    *,
    base_dir: Path,
    issue_number: str,
    root_session_id: str,
    event_type: str,
    created_at: str,
    payload: JsonObject,
    session_seq: int,
) -> None:
    if not root_session_id or not created_at:
        return
    append_issue_event(
        base_dir,
        event_id=_root_event_id(
            issue_number=issue_number,
            root_session_id=root_session_id,
            event_type=event_type,
            created_at=created_at,
        ),
        issue_number=issue_number,
        root_session_id=root_session_id,
        session_seq=session_seq,
        event_type=event_type,
        payload=dict(payload),
        created_at=created_at,
    )


def _sync_root_issue_event_from_session_result(ledger: JsonObject, *, base_dir: Path) -> None:
    last_session_result = cast(JsonObject, ledger.get("lastSessionResult", {}))
    root_session_id = str(last_session_result.get("rootSessionID") or "")
    recorded_at = str(last_session_result.get("recordedAt") or "")
    if not root_session_id or not recorded_at:
        return
    issue = cast(dict[str, str], ledger.get("issue", {}))
    issue_number = issue.get("number", "")
    if not issue_number:
        return
    runtime_issue = read_issue(base_dir, issue_number) or {}
    current_state = str(runtime_issue.get("state") or "")
    current_session_id = str(runtime_issue.get("current_session_id") or "")
    if current_state and not current_session_id:
        upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state=current_state,
            command_id=f"session-result-hydrate:{issue_number}:{recorded_at}",
            updated_at=recorded_at,
            current_session_id=root_session_id,
        )
    _append_root_issue_event(
        base_dir=base_dir,
        issue_number=issue_number,
        root_session_id=root_session_id,
        event_type="root_session_started",
        created_at=recorded_at,
        payload=last_session_result,
        session_seq=1,
    )


def _append_root_terminal_event_for_verifier_handoff(
    *,
    base_dir: Path,
    ledger: JsonObject,
    runtime_issue: dict[str, object],
    updated_at: str,
) -> None:
    issue = cast(dict[str, str], ledger["issue"])
    current = cast(dict[str, str], ledger["current"])
    root_session_id = str(runtime_issue.get("current_root_session_id") or "")
    if not root_session_id:
        return
    _append_root_issue_event(
        base_dir=base_dir,
        issue_number=issue["number"],
        root_session_id=root_session_id,
        event_type="root_terminal",
        created_at=updated_at,
        payload={
            "issueNumber": issue["number"],
            "role": current["role"],
            "stage": current["stage"],
            "reason": "verifier_handoff",
        },
        session_seq=2,
    )


def _quarantine_stale_running_issue(
    *,
    base_dir: Path,
    ledger: JsonObject,
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if str(runtime_issue.get("state") or "") != "running":
        return False
    root_session_id = str(runtime_issue.get("current_root_session_id") or "")
    last_event_at = str(runtime_issue.get("last_event_at") or "")
    if not root_session_id or not last_event_at:
        return False
    current_time = _parse_iso8601(updated_at)
    last_event_time = _parse_iso8601(last_event_at)
    if current_time is None or last_event_time is None:
        return False
    if current_time - last_event_time <= timedelta(seconds=ROOT_HEARTBEAT_TIMEOUT_SECONDS):
        return False

    issue = cast(dict[str, str], ledger["issue"])
    quarantine_issue_execution(
        base_dir=base_dir,
        issue_number=issue["number"],
        reason=(
            f"Root session {root_session_id} heartbeat timed out after last event at {last_event_at}; "
            "move issue into quarantined until fenced resume or terminal failure."
        ),
        updated_at=updated_at,
    )
    return True


def _quarantine_running_issue_without_root_session(
    *,
    base_dir: Path,
    ledger: JsonObject,
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if str(runtime_issue.get("state") or "") != "running":
        return False
    if str(runtime_issue.get("current_root_session_id") or ""):
        return False

    issue = cast(dict[str, str], ledger["issue"])
    quarantine_issue_execution(
        base_dir=base_dir,
        issue_number=issue["number"],
        reason=(
            f"Issue #{issue['number']} is marked running without a recorded root session id; "
            "treat it as an orphaned dispatch and require fenced resume or terminal failure."
        ),
        updated_at=updated_at,
    )
    return True


def _quarantine_stale_dispatching_issue_without_root_session(
    *,
    base_dir: Path,
    ledger: JsonObject,
    current: dict[str, str],
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if current.get("role") != "issue_worker" or current.get("status") != "queued":
        return False
    if str(runtime_issue.get("state") or "") != "dispatching":
        return False
    if str(runtime_issue.get("current_root_session_id") or ""):
        return False

    dispatching_at = str(runtime_issue.get("dispatching_at") or runtime_issue.get("updated_at") or "")
    current_time = _parse_iso8601(updated_at)
    dispatching_time = _parse_iso8601(dispatching_at)
    if current_time is None or dispatching_time is None:
        return False
    if current_time - dispatching_time <= timedelta(seconds=ROOT_HEARTBEAT_TIMEOUT_SECONDS):
        return False

    issue = cast(dict[str, str], ledger["issue"])
    quarantine_issue_execution(
        base_dir=base_dir,
        issue_number=issue["number"],
        reason=(
            f"Issue #{issue['number']} stayed in dispatching without a recorded root session id since {dispatching_at}; "
            "treat it as an orphaned queued issue_worker dispatch and quarantine before redispatch."
        ),
        updated_at=updated_at,
    )
    return True


def _quarantine_stale_queued_subagent_with_stale_root(
    *,
    base_dir: Path,
    ledger: JsonObject,
    current: dict[str, str],
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if current.get("status") != "queued":
        return False
    if current.get("role") not in {"issue_worker", "pr_verifier", "release_worker"}:
        return False
    if str(runtime_issue.get("state") or "") not in {"running", "verifying"}:
        return False
    root_session_id = str(runtime_issue.get("current_root_session_id") or "")
    last_event_at = str(runtime_issue.get("last_event_at") or "")
    if not root_session_id or not last_event_at:
        return False
    current_time = _parse_iso8601(updated_at)
    last_event_time = _parse_iso8601(last_event_at)
    if current_time is None or last_event_time is None:
        return False
    if current_time - last_event_time <= timedelta(seconds=ROOT_HEARTBEAT_TIMEOUT_SECONDS):
        return False

    issue = cast(dict[str, str], ledger["issue"])
    quarantine_issue_execution(
        base_dir=base_dir,
        issue_number=issue["number"],
        reason=(
            f"Queued {current['role']} for issue #{issue['number']} outlived root session heartbeat {root_session_id} after last event at {last_event_at}; "
            "treat it as an orphaned queued subagent and quarantine before redispatch."
        ),
        updated_at=updated_at,
    )
    return True


def _refresh_running_issue_heartbeat_from_worker_result(
    *,
    base_dir: Path,
    issue_number: str,
    runtime_issue: dict[str, object],
    worker_result_path: Path,
    updated_at: str,
) -> bool:
    if str(runtime_issue.get("state") or "") != "running":
        return False
    current_root_session_id = str(runtime_issue.get("current_session_id") or "")
    if not current_root_session_id or not worker_result_path.is_file():
        return False
    try:
        worker_result = parse_worker_result_file(worker_result_path)
    except (OSError, ValueError):
        return False
    completed_at = str(worker_result.get("completed_at") or "")
    if not completed_at:
        return False
    completed_at_time = _parse_iso8601(completed_at)
    if completed_at_time is None:
        return False
    last_event_at = str(runtime_issue.get("last_event_at") or "")
    last_event_time = _parse_iso8601(last_event_at) if last_event_at else None
    if last_event_time is not None and completed_at_time <= last_event_time:
        return False
    upsert_issue_state(
        base_dir,
        issue_number=issue_number,
        state="running",
        command_id=f"worker-result-heartbeat:{issue_number}:{completed_at}",
        updated_at=completed_at,
        current_session_id=current_root_session_id,
    )
    return True


def _record_current_verifier_session(
    *,
    base_dir: Path,
    issue_number: str,
    verifier_session_id: str,
    updated_at: str,
    fallback_state: str = "verifying",
) -> None:
    if not verifier_session_id:
        return
    issue_state = read_issue(base_dir, issue_number)
    state = str(issue_state.get("state") or fallback_state) if issue_state else fallback_state
    upsert_issue_state(
        base_dir,
        issue_number=issue_number,
        state=state,
        command_id=uuid4().hex,
        updated_at=updated_at,
        current_session_id=verifier_session_id,
    )

def issue_lock_path(base_dir: Path, issue_number: str) -> Path:
    lock_path = cast(Callable[[Path, str], Path], _lifecycle_helpers.issue_lock_path)
    return lock_path(base_dir, issue_number)


def has_issue_execution_lock(base_dir: Path, issue_number: str) -> bool:
    has_lock = cast(Callable[[Path, str], bool], _lifecycle_helpers.has_issue_execution_lock)
    return has_lock(base_dir, issue_number)


def read_issue_lock(path: Path) -> JsonObject:
    read_lock = cast(Callable[[Path], JsonObject], _lifecycle_helpers.read_issue_lock)
    return read_lock(path)


def write_issue_lock(path: Path, payload: JsonObject) -> None:
    write_lock = cast(Callable[[Path, JsonObject], None], _lifecycle_helpers.write_issue_lock)
    write_lock(path, payload)


def update_issue_execution_claim(*, base_dir: Path, issue_number: str, updates: JsonObject) -> None:
    update_claim = cast(Callable[..., None], _lifecycle_helpers.update_issue_execution_claim)
    update_claim(base_dir=base_dir, issue_number=issue_number, updates=updates, now=_now)


def clear_issue_execution_claim_projection(*, base_dir: Path, issue_number: str, updated_at: str) -> None:
    clear_claim = cast(Callable[..., None], _lifecycle_helpers.clear_issue_execution_claim_projection)
    clear_claim(base_dir=base_dir, issue_number=issue_number, updated_at=updated_at)


def _sync_issue_progress_label(
    *,
    base_dir: Path,
    issue_number: str,
    add_labels: list[str],
    remove_labels: list[str],
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    sync_labels = cast(Callable[..., str], _lifecycle_helpers.sync_issue_progress_label)
    return sync_labels(
        base_dir=base_dir,
        issue_number=issue_number,
        add_labels=add_labels,
        remove_labels=remove_labels,
        now=_now,
        run=subprocess.run,
        command_id=command_id,
        updated_at=updated_at,
    )


def _default_host_adapter() -> HostAdapter:
    factory = cast(Callable[[], HostAdapter], _session_helpers.default_host_adapter)
    return factory()


def _dispatch_launch_title(request: SessionRequest) -> str:
    request_id = str(request.get("requestID") or uuid4().hex)
    return f"{request['title']} [{request_id}]"


def _scheduler_id(base_dir: Path) -> str:
    scheduler = cast(Callable[[Path], str], _lifecycle_helpers.scheduler_id)
    return scheduler(base_dir)


def _transition_issue_state_if_possible(
    *,
    base_dir: Path,
    issue_number: str,
    to_state: str,
    command_id: str,
    updated_at: str,
    reason: str,
    from_state: str | None = None,
    current_session_id: str | None = None,
) -> None:
    transition = cast(Callable[..., None], _lifecycle_helpers.transition_issue_state_if_possible)
    transition(
        base_dir=base_dir,
        issue_number=issue_number,
        to_state=to_state,
        command_id=command_id,
        updated_at=updated_at,
        reason=reason,
        from_state=from_state,
        current_session_id=current_session_id,
    )


def _rebuild_issue_state_from_runtime_phase(
    *,
    base_dir: Path,
    issue_number: str,
    desired_state: str,
    updated_at: str,
) -> None:
    sequences = {
        "running": ["ready", "claimed", "dispatching", "running"],
        "verifying": ["ready", "claimed", "dispatching", "running", "verifying"],
    }
    sequence = sequences.get(desired_state)
    if sequence is None:
        return

    runtime_issue = read_issue(base_dir, issue_number)
    current_state = str(runtime_issue.get("state") or "ready") if runtime_issue else "ready"
    if current_state == desired_state or current_state == "quarantined":
        return
    if current_state not in sequence:
        raise ValueError(f"cannot rebuild issue #{issue_number} from {current_state!r} to {desired_state!r}")

    start_index = sequence.index(current_state)
    for index in range(start_index + 1, len(sequence)):
        from_state = sequence[index - 1]
        to_state = sequence[index]
        _transition_issue_state_if_possible(
            base_dir=base_dir,
            issue_number=issue_number,
            to_state=to_state,
            command_id=f"runtime-rebuild:{issue_number}:{to_state}:{index}",
            updated_at=updated_at,
            reason=f"Rebuild control-plane state for issue #{issue_number} from runtime phase into {to_state}.",
            from_state=from_state,
        )


def _sync_runtime_phase_to_control_plane_state(
    *,
    base_dir: Path,
    issue_number: str,
    ledger: JsonObject,
    current: dict[str, str],
    updated_at: str,
) -> None:
    desired_state = ""
    if current["role"] == "main_orchestrator" and current["stage"] == "orchestrator_bootstrap":
        desired_state = "running"
    elif current["role"] == "issue_worker":
        desired_state = "running"
    elif current["role"] in {"pr_verifier", "release_worker"}:
        desired_state = "verifying"

    if not desired_state:
        return

    runtime_issue = read_issue(base_dir, issue_number)
    current_state = str(runtime_issue.get("state") or "") if runtime_issue else ""
    current_session_id = str(runtime_issue.get("current_session_id") or "") if runtime_issue else ""
    last_session_result = cast(JsonObject, ledger.get("lastSessionResult", {}))
    session_result_root_session_id = str(last_session_result.get("rootSessionID") or "")
    if current_state in {"quarantined", "completed", "failed"}:
        return
    if current_state == desired_state:
        return
    if (
        current["role"] == "issue_worker"
        and current.get("status") == "queued"
        and current_state == "dispatching"
        and not current_session_id
        and not session_result_root_session_id
    ):
        return
    if desired_state == "running" and not current_session_id and not session_result_root_session_id:
        return
    _rebuild_issue_state_from_runtime_phase(
        base_dir=base_dir,
        issue_number=issue_number,
        desired_state=desired_state,
        updated_at=updated_at,
    )


def _sync_runtime_phase_metadata(
    *,
    base_dir: Path,
    issue_number: str,
    current: dict[str, str],
    attempts: dict[str, int],
    limits: dict[str, int],
    last_failure: dict[str, object],
    workflow: dict[str, object],
    automation: dict[str, object],
    artifacts: dict[str, object],
    updated_at: str,
) -> None:
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=updated_at,
        current_role=current.get("role", ""),
        current_stage=current.get("stage", ""),
        current_status=current.get("status", ""),
        attempts=attempts,
        limits=limits,
        last_failure=last_failure,
        resume_snapshot=workflow,
        automation_flags=automation,
        artifact_refs=artifacts,
    )


def _write_checkpoint_if_present(
    checkpoint_path: Path,
    *,
    issue_number: str | None = None,
    branch: str | None = None,
    role: str | None = None,
    agent: str | None = None,
    issue_packet: str | None = None,
    handoff: str | None = None,
    worker_result: str | None = None,
    evidence_packet: str | None = None,
    artifact_bundle: str | None = None,
    completed: list[str] | None = None,
    in_progress: list[str] | None = None,
    next_steps: list[str] | None = None,
    blockers: list[str] | None = None,
    approval_override_mode: str | None = None,
    override_source: str | None = None,
    human_approval_skipped: bool | None = None,
    workflow_policy_path: str = DEFAULT_WORKFLOW_POLICY_PATH,
    updated_at: str | None = None,
) -> None:
    if not checkpoint_path.exists():
        return
    _ = write_checkpoint_file(
        checkpoint_path,
        issue_number=issue_number,
        branch=branch,
        role=role,
        agent=agent,
        issue_packet=issue_packet,
        handoff=handoff,
        worker_result=worker_result,
        evidence_packet=evidence_packet,
        artifact_bundle=artifact_bundle,
        completed=completed,
        in_progress=in_progress,
        next_steps=next_steps,
        blockers=blockers,
        approval_override_mode=approval_override_mode,
        override_source=override_source,
        human_approval_skipped=human_approval_skipped,
        workflow_policy_path=workflow_policy_path,
        updated_at=updated_at,
    )


def _json_dict(raw: object) -> dict[str, object]:
    return cast(dict[str, object], raw) if isinstance(raw, dict) else {}


def _load_json_dict(raw: object) -> dict[str, object]:
    if isinstance(raw, str):
        try:
            return _json_dict(json.loads(raw))
        except json.JSONDecodeError:
            return {}
    return _json_dict(raw)


def _db_issue_to_ledger(issue: dict[str, object], *, runtime_context: dict[str, object]) -> JsonObject:
    issue_number = str(issue.get("issue_number") or "")
    issue_packet = _load_json_dict(issue.get("issue_packet_json"))
    attempts = _load_json_dict(issue.get("attempts_json"))
    limits = _load_json_dict(issue.get("limits_json"))
    last_failure = _load_json_dict(issue.get("last_failure_json"))
    resume_snapshot = _load_json_dict(issue.get("resume_snapshot_json"))
    automation_flags = _load_json_dict(issue.get("automation_flags_json"))
    artifact_refs = _load_json_dict(issue.get("artifact_refs_json"))
    return {
        "schemaVersion": "1.0",
        "automation": {
            "continueWithoutHuman": bool(automation_flags.get("continueWithoutHuman", True)),
            "queueNextSessionOnIdle": bool(automation_flags.get("queueNextSessionOnIdle", True)),
            "primaryWorkspaceRoot": str(automation_flags.get("primaryWorkspaceRoot") or ""),
            "rootSessionAgent": str(automation_flags.get("rootSessionAgent") or DEFAULT_ROOT_SESSION_AGENT),
            "supervisorDocPath": str(automation_flags.get("supervisorDocPath") or DEFAULT_SUPERVISOR_DOC_PATH),
        },
        "issue": {
            "number": issue_number,
            "title": str(issue.get("title") or issue_packet.get("title") or ""),
            "branch": str(issue.get("branch") or issue_packet.get("branch") or ""),
            "issuePacketPath": str(issue_packet.get("issue_packet_path") or f"docs/agents/issue-packets/issue-{issue_number}.yaml"),
            "backingType": str(issue_packet.get("backing_type") or "github"),
            "priorHandoffPath": str(issue_packet.get("prior_handoff") or ""),
            "parentReference": str(issue_packet.get("parent_reference") or ""),
        },
        "workflow": resume_snapshot,
        "artifacts": artifact_refs,
        "current": {
            "role": str(issue.get("current_role") or ""),
            "stage": str(issue.get("current_stage") or ""),
            "status": str(issue.get("current_status") or ""),
        },
        "attempts": attempts,
        "limits": limits,
        "lastFailure": last_failure,
        "lastSessionResult": {},
        "history": [],
        "ledgerRevision": str(issue.get("updated_at") or issue.get("last_event_at") or ""),
        "updatedAt": str(issue.get("updated_at") or issue.get("last_event_at") or ""),
        "runtimeContext": runtime_context,
    }


def reconcile_issue_from_db(*, base_dir: Path, issue_number: str, updated_at: str | None = None) -> tuple[JsonObject, SupervisorDecision, SessionRequest | None]:
    issue = read_issue(base_dir, issue_number)
    if issue is None:
        raise RuntimeError(f"issue #{issue_number} not found in control plane")
    runtime_context = read_runtime_context(base_dir, issue_number)
    return reconcile_ledger(
        _db_issue_to_ledger(cast(dict[str, object], issue), runtime_context=cast(dict[str, object], runtime_context)),
        session_result_path=base_dir / ".opencode/runtime/__db_only_no_session_result__.json",
        artifact_base_dir=base_dir,
        updated_at=updated_at,
    )


def _run_reconcile_issue_cli(*, base_dir: Path, issue_number: str, updated_at: str | None) -> int:
    _, decision, request = reconcile_issue_from_db(base_dir=base_dir, issue_number=issue_number, updated_at=updated_at)
    payload: JsonObject = {"status": "success", "decision": decision}
    if request is not None:
        payload["request"] = request
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def _recover_stale_bootstrap_with_worker_artifact(
    *,
    ledger: JsonObject,
    base_dir: Path,
    updated_at: str,
) -> bool:
    current = cast(dict[str, str], ledger.get("current", {}))
    if current.get("role") != "main_orchestrator" or current.get("stage") != "orchestrator_bootstrap":
        return False
    issue = cast(dict[str, str], ledger.get("issue", {}))
    artifacts = cast(dict[str, str], ledger.get("artifacts", {}))
    worker_result_ref = str(artifacts.get("workerResultPath") or "")
    if not worker_result_ref:
        return False
    automation = cast(dict[str, object], ledger.get("automation", {}))
    primary_workspace_root = str(automation.get("primaryWorkspaceRoot") or "")
    worker_artifact_base_dir = Path(primary_workspace_root) if primary_workspace_root else base_dir
    worker_result_path = (
        Path(worker_result_ref)
        if Path(worker_result_ref).is_absolute()
        else worker_artifact_base_dir / worker_result_ref
    )
    if not worker_result_path.exists():
        return False

    history = cast(list[JsonObject], ledger.get("history", []))
    history.append(
        {
            "recordedAt": updated_at,
            "fromRole": current.get("role", "main_orchestrator"),
            "fromStage": current.get("stage", "orchestrator_bootstrap"),
            "toRole": "issue_worker",
            "toStage": "issue_worker_execution",
            "reason": (
                f"Recovered stale bootstrap ledger for issue #{issue.get('number', '')} after detecting an existing worker_result artifact."
            ),
        }
    )
    ledger["current"] = {
        "role": "issue_worker",
        "stage": "issue_worker_execution",
        "status": "queued",
    }
    attempts = cast(dict[str, int], ledger.get("attempts", {}))
    attempts["issue_worker"] = max(int(attempts.get("issue_worker", 0)), 1)
    _bump_ledger_revision(ledger, updated_at)
    ledger["updatedAt"] = updated_at
    return True


def _ledger_issue_number(ledger: JsonObject, fallback_issue_number: str) -> str:
    issue = cast(dict[str, str], ledger.get("issue", {}))
    return issue.get("number", "") or fallback_issue_number


def _sync_issue_packet_to_db(base_dir: Path, packet: IssuePacketRecord, *, updated_at: str | None = None) -> None:
    sync_packet = cast(Callable[..., None], _selection_helpers.sync_issue_packet_to_db)
    sync_packet(
        base_dir,
        packet,
        issue_packet_record_to_json=issue_packet_record_to_json,
        now=_now,
        updated_at=updated_at,
    )


def _load_issue_packet_from_db(base_dir: Path, issue_number: str) -> IssuePacketRecord | None:
    load_packet = cast(Callable[..., IssuePacketRecord | None], _selection_helpers.load_issue_packet_from_db)
    return load_packet(base_dir, issue_number, issue_packet_record_from_json=issue_packet_record_from_json)


def _resolve_artifact_path(path_text: str, *, base_dir: Path) -> Path:
    resolve_path = cast(Callable[..., Path], _selection_helpers.resolve_artifact_path)
    return resolve_path(path_text, base_dir=base_dir, root=ROOT)


def _infer_artifact_base_dir(ledger_path: Path) -> Path:
    infer_base = cast(Callable[..., Path], _selection_helpers.infer_artifact_base_dir)
    return infer_base(ledger_path, root=ROOT)


def _completed_issue_numbers(base_dir: Path, checkpoint_path: str) -> set[str]:
    completed_func = cast(Callable[[Path, str], set[str]], _selection_helpers.completed_issue_numbers_from_control_plane)
    return completed_func(base_dir, checkpoint_path)


def _checkpoint_completed_issue_numbers(text: str) -> set[str]:
    parse_completed = cast(Callable[..., set[str]], _selection_helpers.checkpoint_completed_issue_numbers)
    return parse_completed(text, parse_issue_numbers=_parse_issue_numbers)


def select_next_issue_packet(base_dir: Path, *, workflow: dict[str, str], current_issue: dict[str, str]) -> IssuePacketRecord | None:
    select_packet = cast(Callable[..., IssuePacketRecord | None], _selection_helpers.select_next_issue_packet)
    return select_packet(
        base_dir,
        workflow=workflow,
        current_issue=current_issue,
        completed_issue_numbers_func=_completed_issue_numbers,
        parse_issue_packet_text=parse_issue_packet_text,
        sync_issue_packet_to_db_func=_sync_issue_packet_to_db,
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=_dependency_issue_numbers,
        now=_now,
    )


def run_issue_packet_intake(base_dir: Path) -> bool:
    run_intake = cast(Callable[..., bool], _selection_helpers.run_issue_packet_intake)
    return run_intake(
        base_dir,
        read_project_github_repo=_read_project_github_repo,
        parse_issue_packet_text=parse_issue_packet_text,
        sync_issue_packet_to_db_func=_sync_issue_packet_to_db,
        run=subprocess.run,
    )


def _read_json(path: Path) -> JsonObject:
    return cast(JsonObject, json.loads(path.read_text(encoding="utf-8")))


def _write_json(path: Path, payload: JsonObject) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_text(f"{json.dumps(payload, indent=2, ensure_ascii=False)}\n", encoding="utf-8")


def _read_project_github_repo(base_dir: Path) -> str:
    read_repo = cast(Callable[[Path], str], _lifecycle_helpers.read_project_github_repo)
    return read_repo(base_dir)


def claim_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    branch: str,
    source_session_id: str,
    updated_at: str | None = None,
) -> None:
    claim = cast(Callable[..., None], _lifecycle_helpers.claim_issue_execution)
    claim(
        base_dir=base_dir,
        issue_number=issue_number,
        branch=branch,
        source_session_id=source_session_id,
        now=_now,
        sync_progress_label=_sync_issue_progress_label,
        transition_state=_transition_issue_state_if_possible,
        updated_at=updated_at,
    )


def release_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    restore_ready_for_agent: bool,
    final_state: str | None = None,
    updated_at: str | None = None,
) -> None:
    release = cast(Callable[..., None], _lifecycle_helpers.release_issue_execution)
    release(
        base_dir=base_dir,
        issue_number=issue_number,
        restore_ready_for_agent=restore_ready_for_agent,
        now=_now,
        sync_progress_label=_sync_issue_progress_label,
        transition_state=_transition_issue_state_if_possible,
        final_state=final_state,
        updated_at=updated_at,
    )


def quarantine_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    updated_at: str | None = None,
) -> None:
    quarantine = cast(Callable[..., None], _lifecycle_helpers.quarantine_issue_execution)
    quarantine(
        base_dir=base_dir,
        issue_number=issue_number,
        reason=reason,
        now=_now,
        sync_progress_label=_sync_issue_progress_label,
        transition_state=_transition_issue_state_if_possible,
        updated_at=updated_at,
    )


def resume_quarantined_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    updated_at: str | None = None,
) -> None:
    resume = cast(Callable[..., None], _lifecycle_helpers.resume_quarantined_issue_execution)
    resume(
        base_dir=base_dir,
        issue_number=issue_number,
        reason=reason,
        now=_now,
        sync_progress_label=_sync_issue_progress_label,
        transition_state=_transition_issue_state_if_possible,
        updated_at=updated_at,
    )


def redispatch_quarantined_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    branch: str,
    source_session_id: str,
    reason: str,
    updated_at: str | None = None,
) -> None:
    redispatch = cast(Callable[..., None], _lifecycle_helpers.redispatch_quarantined_issue_execution)
    redispatch(
        base_dir=base_dir,
        issue_number=issue_number,
        branch=branch,
        source_session_id=source_session_id,
        reason=reason,
        now=_now,
        sync_progress_label=_sync_issue_progress_label,
        transition_state=_transition_issue_state_if_possible,
        updated_at=updated_at,
    )


def fail_quarantined_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    updated_at: str | None = None,
) -> None:
    fail = cast(Callable[..., None], _lifecycle_helpers.fail_quarantined_issue_execution)
    fail(
        base_dir=base_dir,
        issue_number=issue_number,
        reason=reason,
        now=_now,
        transition_state=_transition_issue_state_if_possible,
        release_issue=release_issue_execution,
        updated_at=updated_at,
    )


def create_initial_ledger(
    *,
    issue_packet: IssuePacketRecord,
    checkpoint_path: str = DEFAULT_CHECKPOINT_PATH,
    workflow_policy_path: str = DEFAULT_WORKFLOW_POLICY_PATH,
    primary_workspace_root: str | None = None,
    root_session_agent: str = DEFAULT_ROOT_SESSION_AGENT,
    updated_at: str | None = None,
) -> JsonObject:
    timestamp = _now(updated_at)
    return {
        "schemaVersion": "1.0",
        "automation": {
            "continueWithoutHuman": True,
            "queueNextSessionOnIdle": True,
            "primaryWorkspaceRoot": primary_workspace_root or "",
            "rootSessionAgent": root_session_agent,
            "supervisorDocPath": DEFAULT_SUPERVISOR_DOC_PATH,
        },
        "issue": {
            "number": issue_packet.issue_number,
            "title": issue_packet.title,
            "branch": issue_packet.branch,
            "issuePacketPath": issue_packet.issue_packet_path,
            "backingType": issue_packet.backing_type,
            "priorHandoffPath": issue_packet.prior_handoff,
            "parentReference": issue_packet.parent_reference,
        },
        "workflow": {
            "checkpointPath": checkpoint_path,
            "workflowPolicyPath": workflow_policy_path,
            "releaseResultTemplatePath": DEFAULT_RELEASE_RESULT_TEMPLATE_PATH,
        },
        "artifacts": {
            "workerResultPath": default_worker_result_path(issue_packet.issue_number),
            "evidencePacketPath": "",
            "releaseResultPath": "",
            "lastSessionResultPath": str(DEFAULT_SESSION_RESULT_PATH.relative_to(ROOT)),
        },
        "current": {
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "status": "queued",
        },
        "attempts": {
            "main_orchestrator": 1,
            "issue_worker": 0,
            "pr_verifier": 0,
            "release_worker": 0,
            "source_session_stop": 0,
        },
        "limits": {
            "main_orchestrator": MAX_ROLE_ATTEMPTS,
            "issue_worker": MAX_ROLE_ATTEMPTS,
            "pr_verifier": MAX_ROLE_ATTEMPTS,
            "release_worker": MAX_ROLE_ATTEMPTS,
            "source_session_stop": MAX_ROLE_ATTEMPTS,
        },
        "lastFailure": {
            "kind": "none",
            "summary": "",
            "retryable": True,
        },
        "lastSessionResult": {},
        "history": [
            {
                "recordedAt": timestamp,
                "fromRole": "system",
                "toRole": "main_orchestrator",
                "toStage": "orchestrator_bootstrap",
                "reason": f"Initialize nonstop supervisor ledger for issue #{issue_packet.issue_number}.",
            }
        ],
        "ledgerRevision": timestamp,
        "updatedAt": timestamp,
    }


def write_ledger_file(ledger_path: Path, ledger: JsonObject) -> None:
    _write_json(ledger_path, ledger)


def _consume_session_request(request_path: Path) -> SessionRequest:
    payload = cast(object, _read_json(request_path))
    request_path.unlink(missing_ok=True)
    return cast(SessionRequest, payload)


def _reject_session_request(
    request: SessionRequest,
    *,
    source_session_id: str,
    error: str,
    updated_at: str | None = None,
) -> SessionResult:
    timestamp = _now(updated_at)
    return {
        "status": "rejected",
        "sourceSessionID": source_session_id,
        "title": request["title"],
        "reason": request["reason"],
        "role": request["role"],
        "stage": request["stage"],
        "issueNumber": request["issueNumber"],
        "branch": request["branch"],
        "error": error,
        "recordedAt": timestamp,
    }


def validate_session_request_for_dispatch(
    request: SessionRequest,
    ledger: JsonObject,
    *,
    base_dir: Path,
) -> str:
    issue = cast(dict[str, str], ledger["issue"])
    queued_next_issue = cast(dict[str, object], ledger.get("queuedNextIssue", {}))
    queued_next_issue_record = cast(dict[str, object], queued_next_issue.get("record", {}))
    selected_issue_number = str(request.get("selectedIssueNumber") or "")
    selected_issue_branch = str(request.get("selectedIssueBranch") or "")
    selected_issue_packet_path = str(request.get("selectedIssuePacketPath") or "")
    if request["issueNumber"] != issue.get("number"):
        return f"stale request issue #{request['issueNumber']} does not match ledger issue #{issue.get('number', '')}"
    if request["branch"] != issue.get("branch"):
        return f"stale request branch {request['branch']} does not match ledger branch {issue.get('branch', '')}"

    ledger_revision = str(ledger.get("ledgerRevision") or ledger.get("updatedAt") or "")
    request_revision = request.get("createdForLedgerRevision", "")
    if request_revision and ledger_revision and request_revision != ledger_revision:
        return f"stale request revision {request_revision} does not match ledger revision {ledger_revision}"

    queued_issue_number = str(queued_next_issue_record.get("issue_number") or "")
    queued_issue_branch = str(queued_next_issue_record.get("branch") or "")
    queued_issue_packet_path = str(queued_next_issue_record.get("issue_packet_path") or "")
    if selected_issue_number or selected_issue_branch or selected_issue_packet_path:
        if not queued_issue_number or not queued_issue_packet_path:
            return "stale selected issue request no longer matches queued next issue state"
        if selected_issue_number != queued_issue_number:
            return f"stale selected issue #{selected_issue_number} does not match queued next issue #{queued_issue_number}"
        if selected_issue_branch and selected_issue_branch != queued_issue_branch:
            return f"stale selected issue branch {selected_issue_branch} does not match queued next issue branch {queued_issue_branch}"
        if selected_issue_packet_path != queued_issue_packet_path:
            return (
                f"stale selected issue packet {selected_issue_packet_path} does not match queued next issue packet {queued_issue_packet_path}"
            )

    completed = _completed_issue_numbers(base_dir, cast(dict[str, str], ledger["workflow"])["checkpointPath"])
    is_selected_issue_recovery_request = (
        request.get("role") == "main_orchestrator"
        and request.get("stage") == "issue_selection_or_recovery"
        and bool(selected_issue_number)
        and bool(selected_issue_packet_path)
    )
    if request["issueNumber"] in completed and not is_selected_issue_recovery_request:
        return f"issue #{request['issueNumber']} is already completed or released; refusing to dispatch stale request"

    packet = _load_issue_packet_from_db(base_dir, request["issueNumber"])
    if packet is None:
        issue_packet_path = _resolve_artifact_path(issue["issuePacketPath"], base_dir=base_dir)
        if not issue_packet_path.exists():
            return f"issue packet not found for issue #{request['issueNumber']}: {issue['issuePacketPath']}"
        packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), issue["issuePacketPath"])
        _sync_issue_packet_to_db(base_dir, packet)
    if READY_FOR_AGENT_LABEL not in packet.labels:
        return f"issue #{request['issueNumber']} is not ready-for-agent; refusing to dispatch"
    if packet.issue_number != request["issueNumber"]:
        return f"issue packet {issue['issuePacketPath']} belongs to issue #{packet.issue_number}, not request issue #{request['issueNumber']}"
    return ""


def dispatch_session_request(
    request: SessionRequest,
    *,
    workdir: Path,
    source_session_id: str,
    updated_at: str | None = None,
) -> SessionResult:
    timestamp = _now(updated_at)
    launch_title = _dispatch_launch_title(request)
    adapter = _default_host_adapter()
    start_context = SessionStartContext(
        title=launch_title,
        prompt=request["prompt"],
        agent=request["agent"],
        workdir=workdir,
        source_session_id=source_session_id,
        role=request["role"],
        stage=request["stage"],
        issue_number=request["issueNumber"],
        branch=request["branch"],
        started_at_iso=timestamp,
    )
    start_result = adapter.start_root_session(start_context)
    stop_attempts_raw = start_result.metadata.get("stopContinuationAttempts")
    stop_attempts = (
        int(stop_attempts_raw)
        if isinstance(stop_attempts_raw, (int, float, str)) and not isinstance(stop_attempts_raw, bool)
        else 0
    )
    if start_result.status != "success":
        return {
            "status": "error",
            "sourceSessionID": source_session_id,
            "launchTitle": launch_title,
            "title": request["title"],
            "reason": request["reason"],
            "role": request["role"],
            "stage": request["stage"],
            "issueNumber": request["issueNumber"],
            "branch": request["branch"],
            "error": start_result.error,
            "rootSessionID": start_result.session_id,
            "sessionReadabilityStatus": start_result.readability_status,
            "recordedAt": timestamp,
        }
    root_session_id = start_result.session_id
    return {
        "status": "success",
        "sourceSessionID": source_session_id,
        "rootSessionID": root_session_id,
        "launchTitle": launch_title,
        "title": request["title"],
        "reason": request["reason"],
        "role": request["role"],
        "stage": request["stage"],
        "issueNumber": request["issueNumber"],
        "branch": request["branch"],
        "tuiResumeCommand": str(start_result.metadata.get("tuiResumeCommand") or "/sessions"),
        "cliOpenCommand": start_result.resume_command,
        "recommendedAction": start_result.resume_hint,
        "sessionReadabilityStatus": start_result.readability_status,
        "stopContinuationStatus": str(start_result.metadata.get("stopContinuationStatus") or "root_session_detached"),
        "stopContinuationAttempts": stop_attempts,
        "recordedAt": timestamp,
    }


def write_session_result(session_result_path: Path, session_result: SessionResult) -> None:
    _write_json(session_result_path, dict(session_result))


def default_session_result_path_for_request(request_path: Path) -> Path:
    if request_path == DEFAULT_REQUEST_PATH:
        return DEFAULT_SESSION_RESULT_PATH
    return request_path.parent / "new-session-result.json"


def default_session_request_path_for_ledger(ledger_path: Path) -> Path:
    if ledger_path == DEFAULT_LEDGER_PATH:
        return DEFAULT_REQUEST_PATH
    return ledger_path.parent / "new-session-request.json"


def default_session_result_path_for_ledger(ledger_path: Path) -> Path:
    if ledger_path == DEFAULT_LEDGER_PATH:
        return DEFAULT_SESSION_RESULT_PATH
    return ledger_path.parent / "new-session-result.json"


def _cli_option_was_provided(argv: list[str], option: str) -> bool:
    return option in argv or any(argument.startswith(f"{option}=") for argument in argv)


def _resolve_cli_request_path(*, argv: list[str], ledger_path: Path, raw_request_path: str) -> Path:
    if _cli_option_was_provided(argv, "--request"):
        return Path(raw_request_path)
    return default_session_request_path_for_ledger(ledger_path)


def _resolve_cli_session_result_path(
    *,
    argv: list[str],
    ledger_path: Path,
    request_path: Path,
    raw_session_result_path: str,
) -> Path:
    if _cli_option_was_provided(argv, "--session-result"):
        return Path(raw_session_result_path)
    if _cli_option_was_provided(argv, "--request"):
        return default_session_result_path_for_request(request_path)
    return default_session_result_path_for_ledger(ledger_path)

def _handoff_to_selected_issue(
    current_ledger: JsonObject,
    *,
    selected_issue: IssuePacketRecord,
    base_dir: Path,
    updated_at: str,
    summary: str,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest]:
    workflow = cast(dict[str, str], current_ledger["workflow"])
    checkpoint_path = _resolve_artifact_path(workflow["checkpointPath"], base_dir=base_dir)
    _write_checkpoint_if_present(
        checkpoint_path,
        issue_number=selected_issue.issue_number,
        branch=selected_issue.branch,
        role="main_orchestrator",
        agent=_root_session_agent(current_ledger),
        issue_packet=selected_issue.issue_packet_path,
        handoff=selected_issue.prior_handoff,
        workflow_policy_path=workflow["workflowPolicyPath"],
        updated_at=updated_at,
    )
    next_ledger = create_initial_ledger(
        issue_packet=selected_issue,
        checkpoint_path=workflow["checkpointPath"],
        workflow_policy_path=workflow["workflowPolicyPath"],
        primary_workspace_root=str(base_dir),
        root_session_agent=_root_session_agent(current_ledger),
        updated_at=updated_at,
    )
    cast(list[JsonObject], next_ledger["history"]).append(
        {
            "recordedAt": updated_at,
            "fromRole": "main_orchestrator",
            "fromStage": "issue_selection_or_recovery",
            "toRole": "main_orchestrator",
                "toStage": "orchestrator_bootstrap",
            "reason": summary,
        }
    )
    request = build_orchestrator_request(next_ledger)
    return next_ledger, {
        "action": "queue_next_issue",
        "next_role": "main_orchestrator",
        "next_stage": "orchestrator_bootstrap",
        "summary": summary,
        "request_title": request["title"],
    }, request


def _sync_session_result(ledger: JsonObject, session_result_path: Path) -> None:
    if not session_result_path.exists():
        return
    session_result = _read_json(session_result_path)
    ledger_issue = cast(JsonObject, ledger.get("issue", {}))
    ledger_issue_number = str(ledger_issue.get("number") or "")
    ledger_branch = str(ledger_issue.get("branch") or "")
    session_issue_number = str(session_result.get("issueNumber") or "")
    session_branch = str(session_result.get("branch") or "")
    if session_issue_number and ledger_issue_number and session_issue_number != ledger_issue_number:
        return
    if session_branch and ledger_branch and session_branch != ledger_branch:
        return
    previous = cast(JsonObject, ledger.get("lastSessionResult", {}))
    if previous.get("recordedAt") == session_result.get("recordedAt"):
        return
    ledger["lastSessionResult"] = session_result
    stop_attempts = session_result.get("stopContinuationAttempts")
    if isinstance(stop_attempts, int):
        cast(dict[str, int], ledger["attempts"])["source_session_stop"] = stop_attempts
    recorded_at = session_result.get("recordedAt")
    if isinstance(recorded_at, str) and recorded_at:
        _bump_ledger_revision(ledger, recorded_at)


def _record_session_result_history(base_dir: Path, ledger: JsonObject, session_result: JsonObject) -> None:
    issue = cast(dict[str, str], ledger.get("issue", {}))
    issue_number = str(session_result.get("issueNumber") or issue.get("number") or "")
    recorded_at = str(session_result.get("recordedAt") or "")
    if not issue_number or not recorded_at:
        return
    request_id = str(session_result.get("sourceSessionID") or "")
    root_session_id = str(session_result.get("rootSessionID") or "")
    status = str(session_result.get("status") or "")
    _ = append_issue_history(
        base_dir,
        issue_number=issue_number,
        entry_type="session_result",
        created_at=recorded_at,
        role=str(session_result.get("role") or ""),
        stage=str(session_result.get("stage") or ""),
        status=status,
        session_id=root_session_id,
        request_id=request_id,
        command_id=request_id,
        summary=str(session_result.get("reason") or status),
        payload=dict(session_result),
        unique_key=f"session-result:{request_id}:{recorded_at}",
    )


def _bump_ledger_revision(ledger: JsonObject, updated_at: str) -> None:
    ledger["ledgerRevision"] = updated_at


def inspect_control_plane(
    *,
    base_dir: Path,
    issue_number: str,
) -> JsonObject:
    ensure_control_plane_db(base_dir)
    return {
        "schema": describe_control_plane_schema(base_dir),
        "issue": read_issue(base_dir, issue_number) or {},
        "latestDecision": read_latest_decision(base_dir, issue_number) or {},
        "latestGitHubSyncAttempt": read_latest_github_sync_attempt(base_dir, issue_number) or {},
    }


def retry_failed_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    updated_at: str | None = None,
) -> JsonObject:
    ensure_control_plane_db(base_dir)
    issue = read_issue(base_dir, issue_number)
    if issue is None:
        raise ValueError(f"unknown issue #{issue_number}")
    if str(issue.get("state") or "") != "failed":
        raise ValueError(f"issue #{issue_number} is not failed")

    last_failure = json.loads(str(issue.get("last_failure_json") or "{}"))
    if not isinstance(last_failure, dict) or not bool(last_failure.get("retryable")):
        raise ValueError(f"issue #{issue_number} is not retryable")

    timestamp = _now(updated_at)
    command_id = uuid4().hex
    _clear_issue_runtime_artifacts(base_dir=base_dir, issue_number=issue_number)
    clear_issue_execution_claim_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
    _ = upsert_issue_state(
        base_dir,
        issue_number=issue_number,
        state="ready",
        command_id=command_id,
        updated_at=timestamp,
    )
    sync_error = _sync_issue_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
        add_labels=[READY_FOR_AGENT_LABEL],
        remove_labels=[AGENT_DISPATCHING_LABEL, AGENT_IN_PROGRESS_LABEL, QUARANTINED_LABEL],
        command_id=command_id,
        updated_at=timestamp,
    )
    if sync_error:
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="failed",
            command_id=f"{command_id}:rollback",
            updated_at=timestamp,
        )

    record_admin_decision(
        base_dir,
        command_id=f"{command_id}:retry-failed",
        issue_number=issue_number,
        decision_type="admin_retry_failed_issue",
        reason=(
            f"Retry failed issue #{issue_number}: {reason}"
            if not sync_error
            else f"Retry failed issue #{issue_number} failed during label sync: {sync_error}"
        ),
        updated_at=timestamp,
        from_state="failed",
        to_state="ready" if not sync_error else "failed",
    )
    return {
        "issue_number": issue_number,
        "status": "success" if not sync_error else "failed",
        "last_error": sync_error,
        "issue": read_issue(base_dir, issue_number) or {},
    }


def redispatch_quarantined_issue(
    *,
    ledger_path: Path,
    request_path: Path,
    session_result_path: Path,
    reason: str,
    source_session_id: str,
    issue_number: str | None = None,
    updated_at: str | None = None,
) -> SessionResult:
    ledger = _read_json(ledger_path)
    base_dir = _infer_artifact_base_dir(ledger_path)
    issue = cast(dict[str, str], ledger["issue"])
    target_issue_number = issue_number or issue["number"]
    if target_issue_number != issue["number"]:
        raise ValueError(
            f"ledger issue #{issue['number']} does not match redispatch target #{target_issue_number}"
        )

    timestamp = _now(updated_at)
    redispatch_quarantined_issue_execution(
        base_dir=base_dir,
        issue_number=target_issue_number,
        branch=issue["branch"],
        source_session_id=source_session_id,
        reason=reason,
        updated_at=timestamp,
    )

    cast(dict[str, int], ledger["attempts"])["main_orchestrator"] += 1
    _set_failure(ledger, kind="none", summary="", retryable=True)
    _queue_transition(
        ledger,
        next_role="main_orchestrator",
        next_stage="orchestrator_bootstrap",
        summary=(
            f"Operator authorized fresh root-session redispatch for quarantined issue #{target_issue_number}. "
            "Launch a new main_orchestrator bootstrap session instead of reusing the stale root session."
        ),
        updated_at=timestamp,
    )
    write_ledger_file(ledger_path, ledger)

    request = build_orchestrator_request(ledger)
    write_session_request(request_path, request)
    return _dispatch_consumed_request(
        request_path,
        ledger_path=ledger_path,
        session_result_path=session_result_path,
        source_session_id=source_session_id,
        updated_at=updated_at,
        failure_restore_state="quarantined",
    )


def retry_github_sync_attempt(
    *,
    base_dir: Path,
    command_id: str,
    updated_at: str | None = None,
) -> JsonObject:
    attempt = read_github_sync_attempt_by_command_id(base_dir, command_id)
    if attempt is None:
        raise ValueError(f"unknown github sync attempt {command_id!r}")
    if str(attempt.get("status") or "") != "failed":
        raise ValueError(f"github sync attempt {command_id!r} is not failed")
    issue_number = str(attempt.get("issue_number") or "")
    latest_attempt = read_latest_github_sync_attempt(base_dir, issue_number)
    if latest_attempt is not None and str(latest_attempt.get("command_id") or "") != command_id:
        raise ValueError(f"github sync attempt {command_id!r} is stale for issue #{issue_number}")

    delta = json.loads(str(attempt.get("intended_label_delta") or "{}"))
    add_labels = delta.get("add", [])
    remove_labels = delta.get("remove", [])
    if not isinstance(add_labels, list) or not isinstance(remove_labels, list):
        raise ValueError(f"github sync attempt {command_id!r} has invalid intended_label_delta")

    sync_error = _sync_issue_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
        add_labels=[str(label) for label in add_labels],
        remove_labels=[str(label) for label in remove_labels],
        command_id=command_id,
        updated_at=updated_at,
    )
    record_admin_decision(
        base_dir,
        command_id=f"{command_id}:retry",
        issue_number=issue_number,
        decision_type="admin_github_sync_retry",
        reason=(
            f"Retry GitHub sync-safe command {command_id} for issue #{issue_number}."
            if not sync_error
            else f"Retry GitHub sync-safe command {command_id} for issue #{issue_number} failed again: {sync_error}"
        ),
        updated_at=_now(updated_at),
    )
    refreshed = read_github_sync_attempt_by_command_id(base_dir, command_id) or {}
    return {
        "command_id": command_id,
        "issue_number": issue_number,
        "status": "success" if not sync_error else "failed",
        "last_error": sync_error,
        "attempt": refreshed,
    }


def _dispatch_consumed_request(
    request_path: Path,
    *,
    ledger_path: Path,
    session_result_path: Path,
    source_session_id: str,
    updated_at: str | None,
    failure_restore_state: str = "ready",
) -> SessionResult:
    request = _consume_session_request(request_path)
    ledger = _read_json(ledger_path) if ledger_path.exists() else {}
    base_dir = _infer_artifact_base_dir(ledger_path)
    ensure_control_plane_db(base_dir)
    dispatch_command_id = request.get("requestID") or uuid4().hex
    dispatch_timestamp = _now(updated_at)
    _ = append_issue_history(
        base_dir,
        issue_number=request["issueNumber"],
        entry_type="session_request",
        created_at=str(request.get("createdAt") or dispatch_timestamp),
        role=request["role"],
        stage=request["stage"],
        status="queued",
        request_id=request.get("requestID", ""),
        command_id=request.get("requestID", ""),
        summary=request["reason"],
        payload=dict(request),
        unique_key=f"session-request:{request.get('requestID', '')}",
    )
    validation_error = validate_session_request_for_dispatch(request, ledger, base_dir=base_dir) if ledger else "ledger not found"
    if validation_error:
        session_result = _reject_session_request(
            request,
            source_session_id=source_session_id,
            error=validation_error,
            updated_at=updated_at,
        )
    else:
        runtime_issue = read_issue(base_dir, request["issueNumber"])
        runtime_state = str(runtime_issue.get("state") or "") if runtime_issue else ""
        is_bootstrap_dispatch = request.get("role") == "main_orchestrator" and request.get("stage") == "orchestrator_bootstrap"
        if is_bootstrap_dispatch:
            if runtime_issue is None:
                _ = ensure_issue_row(base_dir, issue_number=request["issueNumber"], state="claimed", updated_at=dispatch_timestamp)
            elif runtime_state == "ready":
                _ = upsert_issue_state(
                    base_dir,
                    issue_number=request["issueNumber"],
                    state="claimed",
                    command_id=f"{dispatch_command_id}:seed-claimed",
                    updated_at=dispatch_timestamp,
                )
            _transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                to_state="dispatching",
                command_id=dispatch_command_id,
                updated_at=dispatch_timestamp,
                reason=f"Dispatch root session request for issue #{request['issueNumber']}.",
                from_state="claimed",
            )
        session_result = dispatch_session_request(
            request,
            workdir=base_dir,
            source_session_id=source_session_id,
            updated_at=updated_at,
        )
        root_session_id = session_result.get("rootSessionID")
        if isinstance(root_session_id, str) and root_session_id and is_bootstrap_dispatch:
            recorded_at = str(session_result.get("recordedAt") or dispatch_timestamp)
            _transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                to_state="running",
                command_id=f"{dispatch_command_id}:running",
                updated_at=recorded_at,
                reason=f"Root session {root_session_id} acknowledged for issue #{request['issueNumber']}.",
                from_state="dispatching",
                current_session_id=root_session_id,
            )
            _append_root_issue_event(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                root_session_id=root_session_id,
                event_type="root_session_started",
                created_at=recorded_at,
                payload=cast(JsonObject, cast(object, dict(session_result))),
                session_seq=1,
            )
            update_issue_execution_claim(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                updates={
                    "rootSessionID": root_session_id,
                    "status": "root_session_started",
                    "recordedAt": recorded_at,
                },
            )
            sync_error = _sync_issue_progress_label(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                add_labels=[AGENT_IN_PROGRESS_LABEL],
                remove_labels=[AGENT_DISPATCHING_LABEL],
                command_id=f"{dispatch_command_id}:running-labels",
                updated_at=dispatch_timestamp,
            )
            if sync_error:
                resume_link = _default_host_adapter().resume_link(root_session_id)
                session_result["recommendedAction"] = (
                    f"Resume the active root session with {resume_link}. "
                    f"GitHub running-label sync failed and may need retry: {sync_error}"
                )
    if session_result.get("status") != "success":
        current_issue_state = read_issue(base_dir, request["issueNumber"])
        current_state = str(current_issue_state.get("state") or "") if current_issue_state else ""
        if current_state not in {"completed", "failed"}:
            failure_updated_at = str(session_result.get("recordedAt") or dispatch_timestamp)
            if failure_restore_state == "quarantined":
                clear_issue_execution_claim_projection(
                    base_dir=base_dir,
                    issue_number=request["issueNumber"],
                    updated_at=failure_updated_at,
                )
                _ = upsert_issue_state(
                    base_dir,
                    issue_number=request["issueNumber"],
                    state="quarantined",
                    command_id=f"{dispatch_command_id}:quarantine-rollback",
                    updated_at=failure_updated_at,
                    current_session_id="",
                )
                _ = _sync_issue_progress_label(
                    base_dir=base_dir,
                    issue_number=request["issueNumber"],
                    add_labels=[QUARANTINED_LABEL],
                    remove_labels=[AGENT_DISPATCHING_LABEL, AGENT_IN_PROGRESS_LABEL],
                    command_id=f"{dispatch_command_id}:quarantine-labels",
                    updated_at=failure_updated_at,
                )
            else:
                release_issue_execution(
                    base_dir=base_dir,
                    issue_number=request["issueNumber"],
                    restore_ready_for_agent=True,
                    updated_at=failure_updated_at,
                )
    write_session_result(session_result_path, session_result)
    _record_session_result_history(
        base_dir,
        ledger if ledger else {"issue": {"number": request["issueNumber"]}},
        cast(JsonObject, cast(object, dict(session_result))),
    )
    if ledger_path.exists():
        synced_ledger = _read_json(ledger_path)
        _sync_session_result(synced_ledger, session_result_path)
        write_ledger_file(ledger_path, synced_ledger)
    return session_result


def _build_common_prompt_lines(ledger: JsonObject) -> list[str]:
    build_common = cast(Callable[..., list[str]], _request_helpers.build_common_prompt_lines)
    automation = cast(dict[str, object], ledger.get("automation", {}))
    supervisor_doc_path = str(automation.get("supervisorDocPath") or DEFAULT_SUPERVISOR_DOC_PATH)
    return build_common(ledger, default_supervisor_doc_path=supervisor_doc_path)


def _build_prompt(ledger: JsonObject, role: str, stage: str, decision_summary: str) -> str:
    build_prompt_impl = cast(Callable[..., str], _request_helpers.build_prompt)
    automation = cast(dict[str, object], ledger.get("automation", {}))
    workflow = cast(dict[str, object], ledger.get("workflow", {}))
    return build_prompt_impl(
        ledger,
        role,
        stage,
        decision_summary,
        default_supervisor_doc_path=str(automation.get("supervisorDocPath") or DEFAULT_SUPERVISOR_DOC_PATH),
        default_release_result_template_path=str(workflow.get("releaseResultTemplatePath") or DEFAULT_RELEASE_RESULT_TEMPLATE_PATH),
    )


def build_session_request(
    ledger: JsonObject,
    *,
    role: str,
    stage: str,
    reason: str,
    title: str,
    decision_summary: str,
) -> SessionRequest:
    build_request = cast(Callable[..., JsonObject], _request_helpers.build_session_request)
    request = build_request(
        ledger,
        role=role,
        stage=stage,
        reason=reason,
        title=title,
        decision_summary=decision_summary,
        now=_now,
        root_session_agent=_root_session_agent,
        build_prompt=_build_prompt,
    )
    return cast(SessionRequest, cast(object, request))


def build_orchestrator_request(ledger: JsonObject) -> SessionRequest:
    build_orchestrator = cast(Callable[..., JsonObject], _request_helpers.build_orchestrator_request)
    request = build_orchestrator(ledger, build_session_request=build_session_request)
    return cast(SessionRequest, cast(object, request))


def write_session_request(request_path: Path, request: SessionRequest) -> None:
    write_request = cast(Callable[..., None], _request_helpers.write_session_request)
    write_request(request_path, request, write_json=_write_json)


def _queue_transition(
    ledger: JsonObject,
    *,
    next_role: str,
    next_stage: str,
    summary: str,
    updated_at: str,
) -> None:
    issue = cast(dict[str, str], ledger["issue"])
    current_before = cast(dict[str, str], ledger["current"])
    queue_transition = cast(Callable[..., None], _reconcile_helpers.queue_transition)
    queue_transition(
        ledger,
        next_role=next_role,
        next_stage=next_stage,
        summary=summary,
        updated_at=updated_at,
        bump_ledger_revision=_bump_ledger_revision,
    )
    automation = cast(dict[str, object], ledger.get("automation", {}))
    primary_workspace_root = str(automation.get("primaryWorkspaceRoot") or "")
    transition_base_dir = Path(primary_workspace_root) if primary_workspace_root else ROOT
    _record_runtime_transition_history(
        base_dir=transition_base_dir,
        issue_number=issue.get("number", ""),
        recorded_at=updated_at,
        from_role=current_before.get("role", "unknown"),
        from_stage=current_before.get("stage", "unknown"),
        to_role=next_role,
        to_stage=next_stage,
        reason=summary,
    )
    workflow = cast(dict[str, str], ledger["workflow"])
    checkpoint_base_dir = Path(primary_workspace_root) if primary_workspace_root else ROOT
    checkpoint_path = _resolve_artifact_path(workflow["checkpointPath"], base_dir=checkpoint_base_dir)
    current = cast(dict[str, str], ledger["current"])
    artifacts = cast(dict[str, str], ledger.get("artifacts", {}))
    role = current.get("role", "")
    stage = current.get("stage", "")
    in_progress: list[str] | None = None
    next_steps: list[str] | None = None
    if role == "issue_worker":
        in_progress = [f"Issue worker is executing issue #{issue.get('number', '')}." ]
        next_steps = [f"Wait for docs/agents/worker-results/issue-{issue.get('number', '')}.yaml before routing to verification."]
    elif role == "pr_verifier":
        in_progress = [f"PR verifier is validating issue #{issue.get('number', '')}." ]
        next_steps = [f"Wait for {artifacts.get('evidencePacketPath') or 'the evidence packet'} before routing to release_worker."]
    elif role == "release_worker":
        in_progress = [f"Release worker is finalizing issue #{issue.get('number', '')}." ]
        next_steps = [f"Wait for {artifacts.get('releaseResultPath') or 'the release result'} before selecting the next issue."]
    elif role == "main_orchestrator" and stage == "issue_selection_or_recovery":
        in_progress = [f"Continue supervisor recovery for issue #{issue.get('number', '')}." ]
        next_steps = ["Select the next ready issue packet or remain in orchestrator recovery if none are available."]
    _write_checkpoint_if_present(
        checkpoint_path,
        issue_number=issue.get("number") or None,
        branch=issue.get("branch") or None,
        role=role or None,
        agent=_root_session_agent(ledger),
        issue_packet=issue.get("issuePacketPath") or None,
        handoff=issue.get("priorHandoffPath") if "priorHandoffPath" in issue else None,
        worker_result=artifacts.get("workerResultPath"),
        evidence_packet=artifacts.get("evidencePacketPath"),
        artifact_bundle=artifacts.get("releaseResultPath"),
        in_progress=in_progress,
        next_steps=next_steps,
        blockers=(
            [str(cast(dict[str, object], ledger.get("lastFailure", {})).get("summary") or "none")]
            if cast(dict[str, object], ledger.get("lastFailure", {})).get("summary")
            else None
        ),
        workflow_policy_path=workflow["workflowPolicyPath"],
        updated_at=updated_at,
    )


def _set_failure(ledger: JsonObject, *, kind: str, summary: str, retryable: bool) -> None:
    set_failure = cast(Callable[..., None], _reconcile_helpers.set_failure)
    set_failure(ledger, kind=kind, summary=summary, retryable=retryable)


def _request_for_transition(
    ledger: JsonObject,
    *,
    next_role: str,
    next_stage: str,
    summary: str,
) -> SessionRequest:
    request_for_transition = cast(Callable[..., JsonObject], _reconcile_helpers.request_for_transition)
    request = request_for_transition(
        ledger,
        next_role=next_role,
        next_stage=next_stage,
        summary=summary,
        build_session_request=build_session_request,
    )
    return cast(SessionRequest, cast(object, request))


def _subagent_decision(ledger: JsonObject, *, next_role: str, next_stage: str, summary: str) -> SupervisorDecision:
    subagent_decision = cast(Callable[..., JsonObject], _reconcile_helpers.subagent_decision)
    decision = subagent_decision(
        ledger,
        next_role=next_role,
        next_stage=next_stage,
        summary=summary,
        build_prompt=_build_prompt,
    )
    return cast(SupervisorDecision, cast(object, decision))


def _requeue_issue_worker(
    ledger: JsonObject,
    *,
    base_dir: Path,
    issue_number: str,
    updated_at: str,
    summary: str,
    next_stage: str,
) -> tuple[SupervisorDecision, None]:
    requeue_issue_worker = cast(Callable[..., tuple[JsonObject, None]], _reconcile_helpers.requeue_issue_worker)
    decision, request = requeue_issue_worker(
        ledger,
        base_dir=base_dir,
        issue_number=issue_number,
        updated_at=updated_at,
        summary=summary,
        next_stage=next_stage,
        read_issue=read_issue,
        transition_issue_state_if_possible=_transition_issue_state_if_possible,
        queue_transition_func=_queue_transition,
        subagent_decision_func=_subagent_decision,
    )
    return cast(SupervisorDecision, cast(object, decision)), request


def _queue_orchestrator_recovery(
    ledger: JsonObject,
    *,
    base_dir: Path,
    updated_at: str,
    summary: str,
    final_state: str | None = None,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest]:
    queue_orchestrator_recovery = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject]], _reconcile_helpers.queue_orchestrator_recovery)
    next_ledger, decision, request = queue_orchestrator_recovery(
        ledger,
        base_dir=base_dir,
        updated_at=updated_at,
        summary=summary,
        release_issue_execution=release_issue_execution,
        select_next_issue_packet=select_next_issue_packet,
        run_issue_packet_intake=run_issue_packet_intake,
        handoff_to_selected_issue=_handoff_to_selected_issue,
        request_for_transition_func=_request_for_transition,
        queue_transition_func=_queue_transition,
        final_state=final_state,
    )
    workflow = cast(dict[str, str], next_ledger["workflow"])
    checkpoint_path = _resolve_artifact_path(workflow["checkpointPath"], base_dir=base_dir)
    current = cast(dict[str, str], next_ledger["current"])
    issue = cast(dict[str, str], next_ledger["issue"])
    artifacts = cast(dict[str, str], next_ledger.get("artifacts", {}))
    _write_checkpoint_if_present(
        checkpoint_path,
        issue_number=issue.get("number") or None,
        branch=issue.get("branch") or None,
        role=current.get("role") or None,
        agent=_root_session_agent(next_ledger),
        issue_packet=issue.get("issuePacketPath") or None,
        handoff=issue.get("priorHandoffPath") if "priorHandoffPath" in issue else None,
        worker_result=artifacts.get("workerResultPath"),
        evidence_packet=artifacts.get("evidencePacketPath"),
        artifact_bundle=artifacts.get("releaseResultPath"),
        completed=[summary] if final_state == "completed" else None,
        in_progress=(
            [f"Continue supervisor recovery for issue #{issue.get('number', '')}."]
            if current.get("role") == "main_orchestrator" and current.get("stage") == "issue_selection_or_recovery"
            else None
        ),
        next_steps=(
            ["Select the next ready issue packet or remain in orchestrator recovery if none are available."]
            if current.get("role") == "main_orchestrator" and current.get("stage") == "issue_selection_or_recovery"
            else None
        ),
        blockers=(
            [str(cast(dict[str, object], next_ledger.get("lastFailure", {})).get("summary") or "none")]
            if cast(dict[str, object], next_ledger.get("lastFailure", {})).get("summary")
            else None
        ),
        workflow_policy_path=workflow["workflowPolicyPath"],
        updated_at=updated_at,
    )
    return (
        next_ledger,
        cast(SupervisorDecision, cast(object, decision)),
        cast(SessionRequest, cast(object, request)),
    )


def _consume_queued_next_issue(
    ledger: JsonObject,
    *,
    base_dir: Path,
    updated_at: str,
    summary: str,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest] | None:
    queued_next_issue = cast(dict[str, object], ledger.get("queuedNextIssue", {}))
    queued_next_issue_record = cast(dict[str, object], queued_next_issue.get("record", {}))
    issue_number = str(queued_next_issue_record.get("issue_number") or "")
    issue_packet_path = str(queued_next_issue_record.get("issue_packet_path") or "")
    if not issue_number or not issue_packet_path:
        return None
    selected_issue = issue_packet_record_from_json(cast(dict[str, object], queued_next_issue_record))
    if selected_issue is None:
        return None
    issue_packet_file = _resolve_artifact_path(selected_issue.issue_packet_path, base_dir=base_dir)
    if not issue_packet_file.exists():
        return None
    if issue_number in _completed_issue_numbers(base_dir, cast(dict[str, str], ledger["workflow"])["checkpointPath"]):
        return None
    revalidated_issue = select_next_issue_packet(
        base_dir,
        workflow=cast(dict[str, str], ledger["workflow"]),
        current_issue=cast(dict[str, str], ledger["issue"]),
    )
    if revalidated_issue is None:
        ledger.pop("queuedNextIssue", None)
        return None
    if revalidated_issue.issue_number != selected_issue.issue_number:
        ledger.pop("queuedNextIssue", None)
        next_ledger, decision, request = _handoff_to_selected_issue(
            ledger,
            selected_issue=revalidated_issue,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=(
                f"Queued next issue #{selected_issue.issue_number} is no longer ready. Continue automatically with revalidated issue #{revalidated_issue.issue_number}."
            ),
        )
        return next_ledger, decision, request
    next_ledger, decision, request = _handoff_to_selected_issue(
        ledger,
        selected_issue=selected_issue,
        base_dir=base_dir,
        updated_at=updated_at,
        summary=summary,
    )
    return next_ledger, decision, request


def _quarantine_decision(ledger: JsonObject, *, summary: str) -> tuple[JsonObject, SupervisorDecision, None]:
    quarantine_decision = cast(Callable[..., tuple[JsonObject, JsonObject, None]], _reconcile_helpers.quarantine_decision)
    next_ledger, decision, request = quarantine_decision(ledger, summary=summary)
    return next_ledger, cast(SupervisorDecision, cast(object, decision)), request


def reconcile_ledger(
    ledger: JsonObject,
    *,
    session_result_path: Path = DEFAULT_SESSION_RESULT_PATH,
    artifact_base_dir: Path | None = None,
    updated_at: str | None = None,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest | None]:
    timestamp = _now(updated_at)
    _sync_session_result(ledger, session_result_path)
    base_dir = artifact_base_dir or ROOT
    ensure_control_plane_db(base_dir)
    automation = cast(dict[str, object], ledger.get("automation", {}))
    if not str(automation.get("primaryWorkspaceRoot") or ""):
        automation["primaryWorkspaceRoot"] = str(base_dir)
    issue = cast(dict[str, str], ledger["issue"])
    ensure_issue_row(base_dir, issue_number=issue["number"], updated_at=timestamp)
    _sync_runtime_phase_metadata(
        base_dir=base_dir,
        issue_number=issue["number"],
        current=cast(dict[str, str], ledger["current"]),
        attempts=cast(dict[str, int], ledger.get("attempts", {})),
        limits=cast(dict[str, int], ledger.get("limits", {})),
        last_failure=cast(dict[str, object], ledger.get("lastFailure", {})),
        workflow=cast(dict[str, object], ledger.get("workflow", {})),
        automation=cast(dict[str, object], ledger.get("automation", {})),
        artifacts=cast(dict[str, object], ledger.get("artifacts", {})),
        updated_at=timestamp,
    )
    _sync_root_issue_event_from_session_result(ledger, base_dir=base_dir)
    pre_sync_runtime_issue = read_issue(base_dir, issue["number"])
    current = cast(dict[str, str], ledger["current"])
    if _recover_stale_bootstrap_with_worker_artifact(
        ledger=ledger,
        base_dir=base_dir,
        updated_at=timestamp,
    ):
        current = cast(dict[str, str], ledger["current"])
    if pre_sync_runtime_issue and str(pre_sync_runtime_issue.get("state") or "") == "running" and current["role"] in {"pr_verifier", "release_worker"}:
        _append_root_terminal_event_for_verifier_handoff(
            base_dir=base_dir,
            ledger=ledger,
            runtime_issue=cast(dict[str, object], pre_sync_runtime_issue),
            updated_at=timestamp,
        )
    attempts = cast(dict[str, int], ledger["attempts"])
    limits = cast(dict[str, int], ledger["limits"])
    artifacts = cast(dict[str, str], ledger["artifacts"])
    _sync_runtime_phase_to_control_plane_state(
        base_dir=base_dir,
        issue_number=issue["number"],
        ledger=ledger,
        current=current,
        updated_at=timestamp,
    )
    runtime_issue = read_issue(base_dir, issue["number"])
    try:
        if runtime_issue and _quarantine_running_issue_without_root_session(
            base_dir=base_dir,
            ledger=ledger,
            runtime_issue=cast(dict[str, object], runtime_issue),
            updated_at=timestamp,
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if runtime_issue and _quarantine_stale_dispatching_issue_without_root_session(
            base_dir=base_dir,
            ledger=ledger,
            current=current,
            runtime_issue=cast(dict[str, object], runtime_issue),
            updated_at=timestamp,
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if (
            runtime_issue
            and current["role"] == "issue_worker"
            and _refresh_running_issue_heartbeat_from_worker_result(
                base_dir=base_dir,
                issue_number=issue["number"],
                runtime_issue=cast(dict[str, object], runtime_issue),
                worker_result_path=_resolve_artifact_path(artifacts["workerResultPath"], base_dir=base_dir),
                updated_at=timestamp,
            )
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if runtime_issue and _quarantine_stale_running_issue(
            base_dir=base_dir,
            ledger=ledger,
            runtime_issue=cast(dict[str, object], runtime_issue),
            updated_at=timestamp,
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if runtime_issue and _quarantine_stale_queued_subagent_with_stale_root(
            base_dir=base_dir,
            ledger=ledger,
            current=current,
            runtime_issue=cast(dict[str, object], runtime_issue),
            updated_at=timestamp,
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if runtime_issue and runtime_issue.get("state") == "quarantined":
            summary = (
                f"Issue #{issue['number']} is quarantined. Hold automatic retries and require an explicit fenced resume or terminal failure decision."
            )
            return _quarantine_decision(ledger, summary=summary)

        if runtime_issue and runtime_issue.get("state") == "running" and current["role"] == "pr_verifier":
            _transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=issue["number"],
                to_state="verifying",
                command_id=uuid4().hex,
                updated_at=timestamp,
                reason=f"Issue #{issue['number']} is now waiting on verifier-owned evidence.",
                from_state="running",
            )

        if current["role"] == "main_orchestrator" and current["stage"] == "orchestrator_bootstrap":
            reconcile_orchestrator_bootstrap = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _reconcile_helpers.reconcile_orchestrator_bootstrap)
            next_ledger, decision, request = reconcile_orchestrator_bootstrap(
                ledger,
                issue=issue,
                attempts=attempts,
                updated_at=timestamp,
                set_failure_func=_set_failure,
                queue_transition_func=_queue_transition,
                subagent_decision_func=_subagent_decision,
            )
            _sync_runtime_phase_metadata(
                base_dir=base_dir,
                issue_number=_ledger_issue_number(next_ledger, issue["number"]),
                current=cast(dict[str, str], next_ledger["current"]),
                attempts=cast(dict[str, int], next_ledger.get("attempts", {})),
                limits=cast(dict[str, int], next_ledger.get("limits", {})),
                last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})),
                workflow=cast(dict[str, object], next_ledger.get("workflow", {})),
                automation=cast(dict[str, object], next_ledger.get("automation", {})),
                artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})),
                updated_at=timestamp,
            )
            return next_ledger, cast(SupervisorDecision, cast(object, decision)), cast(SessionRequest | None, cast(object, request))

        if current["role"] == "issue_worker":
            reconcile_issue_worker = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _reconcile_helpers.reconcile_issue_worker)
            next_ledger, decision, request = reconcile_issue_worker(
                ledger,
                base_dir=base_dir,
                issue=issue,
                current=current,
                attempts=attempts,
                limits=limits,
                artifacts=artifacts,
                updated_at=timestamp,
                resolve_artifact_path=_resolve_artifact_path,
                parse_worker_result_file=parse_worker_result_file,
                is_successful_release_status=_is_successful_release_status,
                default_evidence_packet_path=default_evidence_packet_path,
                read_issue=read_issue,
                read_artifact_fact=_artifact_fact,
                record_artifact_status=_record_artifact_status,
                set_failure_func=_set_failure,
                requeue_issue_worker_func=_requeue_issue_worker,
                queue_orchestrator_recovery_func=_queue_orchestrator_recovery,
                queue_transition_func=_queue_transition,
                subagent_decision_func=_subagent_decision,
            )
            _sync_runtime_phase_metadata(
                base_dir=base_dir,
                issue_number=_ledger_issue_number(next_ledger, issue["number"]),
                current=cast(dict[str, str], next_ledger["current"]),
                attempts=cast(dict[str, int], next_ledger.get("attempts", {})),
                limits=cast(dict[str, int], next_ledger.get("limits", {})),
                last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})),
                workflow=cast(dict[str, object], next_ledger.get("workflow", {})),
                automation=cast(dict[str, object], next_ledger.get("automation", {})),
                artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})),
                updated_at=timestamp,
            )
            return next_ledger, cast(SupervisorDecision, cast(object, decision)), cast(SessionRequest | None, cast(object, request))

        if current["role"] == "pr_verifier":
            reconcile_pr_verifier = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _reconcile_helpers.reconcile_pr_verifier)
            next_ledger, decision, request = reconcile_pr_verifier(
                ledger,
                base_dir=base_dir,
                issue=issue,
                attempts=attempts,
                limits=limits,
                artifacts=artifacts,
                updated_at=timestamp,
                resolve_artifact_path=_resolve_artifact_path,
                parse_evidence_packet_file=parse_evidence_packet_file,
                default_release_result_path=default_release_result_path,
                read_issue=read_issue,
                read_artifact_fact=_artifact_fact,
                record_artifact_status=_record_artifact_status,
                record_current_verifier_session=_record_current_verifier_session,
                transition_issue_state_if_possible=_transition_issue_state_if_possible,
                set_failure_func=_set_failure,
                requeue_issue_worker_func=_requeue_issue_worker,
                queue_orchestrator_recovery_func=_queue_orchestrator_recovery,
                queue_transition_func=_queue_transition,
                subagent_decision_func=_subagent_decision,
            )
            _sync_runtime_phase_metadata(
                base_dir=base_dir,
                issue_number=_ledger_issue_number(next_ledger, issue["number"]),
                current=cast(dict[str, str], next_ledger["current"]),
                attempts=cast(dict[str, int], next_ledger.get("attempts", {})),
                limits=cast(dict[str, int], next_ledger.get("limits", {})),
                last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})),
                workflow=cast(dict[str, object], next_ledger.get("workflow", {})),
                automation=cast(dict[str, object], next_ledger.get("automation", {})),
                artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})),
                updated_at=timestamp,
            )
            return next_ledger, cast(SupervisorDecision, cast(object, decision)), cast(SessionRequest | None, cast(object, request))

        if current["role"] == "release_worker":
            reconcile_release_worker = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _reconcile_helpers.reconcile_release_worker)
            next_ledger, decision, request = reconcile_release_worker(
                ledger,
                base_dir=base_dir,
                issue=issue,
                attempts=attempts,
                limits=limits,
                artifacts=artifacts,
                updated_at=timestamp,
                transient_release_blockers=TRANSIENT_RELEASE_BLOCKERS,
                resolve_artifact_path=_resolve_artifact_path,
                parse_release_result_file=parse_release_result_file,
                read_artifact_fact=_artifact_fact,
                record_artifact_status=_record_artifact_status,
                read_issue=read_issue,
                transition_issue_state_if_possible=_transition_issue_state_if_possible,
                set_failure_func=_set_failure,
                queue_orchestrator_recovery_func=_queue_orchestrator_recovery,
                queue_transition_func=_queue_transition,
                subagent_decision_func=_subagent_decision,
            )
            _sync_runtime_phase_metadata(
                base_dir=base_dir,
                issue_number=_ledger_issue_number(next_ledger, issue["number"]),
                current=cast(dict[str, str], next_ledger["current"]),
                attempts=cast(dict[str, int], next_ledger.get("attempts", {})),
                limits=cast(dict[str, int], next_ledger.get("limits", {})),
                last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})),
                workflow=cast(dict[str, object], next_ledger.get("workflow", {})),
                automation=cast(dict[str, object], next_ledger.get("automation", {})),
                artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})),
                updated_at=timestamp,
            )
            return next_ledger, cast(SupervisorDecision, cast(object, decision)), cast(SessionRequest | None, cast(object, request))

        if current["role"] == "main_orchestrator" and current["stage"] == "issue_selection_or_recovery":
            queued_next_issue_result = _consume_queued_next_issue(
                ledger,
                base_dir=base_dir,
                updated_at=timestamp,
                summary=(
                    f"Resume deterministic next-issue handoff selected earlier for issue #{issue['number']}."
                ),
            )
            if queued_next_issue_result is not None:
                next_ledger, decision, request = queued_next_issue_result
                _sync_runtime_phase_metadata(
                    base_dir=base_dir,
                    issue_number=_ledger_issue_number(next_ledger, issue["number"]),
                    current=cast(dict[str, str], next_ledger["current"]),
                    attempts=cast(dict[str, int], next_ledger.get("attempts", {})),
                    limits=cast(dict[str, int], next_ledger.get("limits", {})),
                    last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})),
                    workflow=cast(dict[str, object], next_ledger.get("workflow", {})),
                    automation=cast(dict[str, object], next_ledger.get("automation", {})),
                    artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})),
                    updated_at=timestamp,
                )
                return next_ledger, cast(SupervisorDecision, cast(object, decision)), cast(SessionRequest | None, cast(object, request))
            reconcile_issue_selection_or_recovery = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None] | None], _reconcile_helpers.reconcile_issue_selection_or_recovery)
            recovery_result = reconcile_issue_selection_or_recovery(
                ledger,
                base_dir=base_dir,
                issue=issue,
                artifacts=artifacts,
                updated_at=timestamp,
                resolve_artifact_path=_resolve_artifact_path,
                parse_release_result_file=parse_release_result_file,
                read_artifact_fact=_artifact_fact,
                record_artifact_status=_record_artifact_status,
                read_issue=read_issue,
                is_successful_release_status=_is_successful_release_status,
                set_failure_func=_set_failure,
                queue_orchestrator_recovery_func=_queue_orchestrator_recovery,
            )
            if recovery_result is not None:
                next_ledger, decision, request = recovery_result
                _sync_runtime_phase_metadata(
                    base_dir=base_dir,
                    issue_number=_ledger_issue_number(next_ledger, issue["number"]),
                    current=cast(dict[str, str], next_ledger["current"]),
                    attempts=cast(dict[str, int], next_ledger.get("attempts", {})),
                    limits=cast(dict[str, int], next_ledger.get("limits", {})),
                    last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})),
                    workflow=cast(dict[str, object], next_ledger.get("workflow", {})),
                    automation=cast(dict[str, object], next_ledger.get("automation", {})),
                    artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})),
                    updated_at=timestamp,
                )
                return next_ledger, cast(SupervisorDecision, cast(object, decision)), cast(SessionRequest | None, cast(object, request))

        no_change_decision = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _reconcile_helpers.no_change_decision)
        next_ledger, decision, request = no_change_decision(
            ledger,
            current=current,
            updated_at=timestamp,
            bump_ledger_revision=_bump_ledger_revision,
        )
        _sync_runtime_phase_metadata(
            base_dir=base_dir,
            issue_number=_ledger_issue_number(next_ledger, issue["number"]),
            current=cast(dict[str, str], next_ledger["current"]),
            attempts=cast(dict[str, int], next_ledger.get("attempts", {})),
            limits=cast(dict[str, int], next_ledger.get("limits", {})),
            last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})),
            workflow=cast(dict[str, object], next_ledger.get("workflow", {})),
            automation=cast(dict[str, object], next_ledger.get("automation", {})),
            artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})),
            updated_at=timestamp,
        )
        return next_ledger, cast(SupervisorDecision, cast(object, decision)), cast(SessionRequest | None, cast(object, request))
    finally:
        pass


def _run_reconcile_cli(
    *,
    ledger_path: Path,
    request_path: Path,
    session_result_path: Path,
    updated_at: str | None,
    write_request: bool,
    dispatch_now: bool,
    source_session_id: str,
    child_only: bool,
) -> int:
    ledger = _read_json(ledger_path)
    if child_only:
        current = cast(dict[str, object], ledger.get("current", {}))
        current_role = str(current.get("role") or "")
        if current_role not in {"issue_worker", "pr_verifier", "release_worker"}:
            print(
                f"advance-child requires the on-disk ledger to already be queued on a child role (found {current_role or 'unknown'}).",
                file=sys.stderr,
            )
            return 2
    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=session_result_path,
        artifact_base_dir=_infer_artifact_base_dir(ledger_path),
        updated_at=updated_at,
    )
    write_ledger_file(ledger_path, updated_ledger)
    if write_request and request is not None:
        write_session_request(request_path, request)
        print(f"wrote session request {request_path}")
        if dispatch_now:
            _ = _dispatch_consumed_request(
                request_path,
                ledger_path=ledger_path,
                session_result_path=session_result_path,
                source_session_id=source_session_id,
                updated_at=updated_at,
            )
            print(f"wrote session result {session_result_path}")
    print(json.dumps(decision, indent=2, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create the nonstop supervisor ledger for a selected issue")
    _ = init_parser.add_argument("--issue-packet", required=True, help="Path to the selected AFK issue packet")
    _ = init_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = init_parser.add_argument("--request", default=str(DEFAULT_REQUEST_PATH), help="Optional path to new-session-request.json")
    _ = init_parser.add_argument("--write-request", action="store_true", help="Also write the initial orchestrator bootstrap request")
    _ = init_parser.add_argument("--dispatch-now", action="store_true", help="Immediately launch the fresh session after writing the request")
    _ = init_parser.add_argument("--source-session-id", default="supervisor_init", help="Source session id to record when dispatching immediately")
    _ = init_parser.add_argument("--workflow-policy-path", default=DEFAULT_WORKFLOW_POLICY_PATH)
    _ = init_parser.add_argument("--checkpoint-path", default=DEFAULT_CHECKPOINT_PATH)
    _ = init_parser.add_argument("--updated-at")

    reconcile_parser = subparsers.add_parser("reconcile", help="Read runtime artifacts and queue the next session")
    _ = reconcile_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = reconcile_parser.add_argument("--request", default=str(DEFAULT_REQUEST_PATH), help="Path to new-session-request.json")
    _ = reconcile_parser.add_argument("--session-result", default=str(DEFAULT_SESSION_RESULT_PATH), help="Path to new-session-result.json")
    _ = reconcile_parser.add_argument("--write-request", action="store_true", help="Persist the computed next-session request")
    _ = reconcile_parser.add_argument("--dispatch-now", action="store_true", help="Immediately launch the fresh session after writing the request")
    _ = reconcile_parser.add_argument("--source-session-id", default="supervisor_reconcile", help="Source session id to record when dispatching immediately")
    _ = reconcile_parser.add_argument("--updated-at")

    advance_child_parser = subparsers.add_parser(
        "advance-child",
        help="Advance a queued child role after its compact artifact has been written",
    )
    _ = advance_child_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = advance_child_parser.add_argument("--request", default=str(DEFAULT_REQUEST_PATH), help="Path to new-session-request.json")
    _ = advance_child_parser.add_argument("--session-result", default=str(DEFAULT_SESSION_RESULT_PATH), help="Path to new-session-result.json")
    _ = advance_child_parser.add_argument("--write-request", action="store_true", help="Persist the computed next-session request when child advancement queues a new root session")
    _ = advance_child_parser.add_argument("--dispatch-now", action="store_true", help="Immediately launch the fresh root session after writing the request")
    _ = advance_child_parser.add_argument("--source-session-id", default="supervisor_advance_child", help="Source session id to record when dispatching immediately")
    _ = advance_child_parser.add_argument("--updated-at")

    dispatch_parser = subparsers.add_parser("dispatch", help="Launch the next session explicitly without relying on session.idle plugins")
    _ = dispatch_parser.add_argument("--request", default=str(DEFAULT_REQUEST_PATH), help="Path to new-session-request.json")
    _ = dispatch_parser.add_argument("--session-result", default=str(DEFAULT_SESSION_RESULT_PATH), help="Path to new-session-result.json")
    _ = dispatch_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = dispatch_parser.add_argument("--source-session-id", default="manual_dispatch", help="Source session id to record in the session result")
    _ = dispatch_parser.add_argument("--updated-at")

    start_issue_parser = subparsers.add_parser(
        "start-issue",
        help="Start a root orchestrator session from DB-backed issue state",
    )
    _ = start_issue_parser.add_argument("--issue-number", required=True, help="Issue number to dispatch")
    _ = start_issue_parser.add_argument(
        "--source-session-id",
        default="supervisor_start_issue",
        help="Source session id to record in the dispatch result",
    )
    _ = start_issue_parser.add_argument("--approval-override-mode", help="Workflow-start merge approval override mode")
    _ = start_issue_parser.add_argument("--override-source", help="Workflow-start approval override source")
    _ = start_issue_parser.add_argument(
        "--human-approval-skipped",
        action="store_true",
        help="Record that human approval is intentionally skipped for this workflow run",
    )
    _ = start_issue_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = start_issue_parser.add_argument("--updated-at")

    show_session_parser = subparsers.add_parser(
        "show-session",
        help="Show the latest DB-backed dispatch result for an issue or workspace",
    )
    _ = show_session_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = show_session_parser.add_argument("--issue-number", help="Optional issue number filter")

    quarantine_parser = subparsers.add_parser("quarantine", help="Move an issue into quarantined state")
    _ = quarantine_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = quarantine_parser.add_argument("--issue-number", help="Explicit issue number override")
    _ = quarantine_parser.add_argument("--reason", required=True, help="Why the issue is being quarantined")
    _ = quarantine_parser.add_argument("--updated-at")

    resume_parser = subparsers.add_parser("resume-quarantined", help="Fenced resume for a quarantined issue")
    _ = resume_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = resume_parser.add_argument("--issue-number", help="Explicit issue number override")
    _ = resume_parser.add_argument("--reason", required=True, help="Why the issue is allowed to resume")
    _ = resume_parser.add_argument("--updated-at")

    redispatch_parser = subparsers.add_parser(
        "redispatch-quarantined",
        help="Create a fresh root session for a quarantined issue",
    )
    _ = redispatch_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = redispatch_parser.add_argument("--request", default=str(DEFAULT_REQUEST_PATH), help="Path to new-session-request.json")
    _ = redispatch_parser.add_argument("--session-result", default=str(DEFAULT_SESSION_RESULT_PATH), help="Path to new-session-result.json")
    _ = redispatch_parser.add_argument("--issue-number", help="Explicit issue number override")
    _ = redispatch_parser.add_argument("--reason", required=True, help="Why the quarantined issue is safe to redispatch")
    _ = redispatch_parser.add_argument(
        "--source-session-id",
        default="supervisor_redispatch_quarantined",
        help="Source session id to record in the new session result",
    )
    _ = redispatch_parser.add_argument("--updated-at")

    fail_quarantine_parser = subparsers.add_parser("fail-quarantined", help="Mark a quarantined issue as failed")
    _ = fail_quarantine_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = fail_quarantine_parser.add_argument("--issue-number", help="Explicit issue number override")
    _ = fail_quarantine_parser.add_argument("--reason", required=True, help="Why the quarantined issue is terminally failed")
    _ = fail_quarantine_parser.add_argument("--updated-at")

    inspect_parser = subparsers.add_parser("inspect", help="Inspect control-plane issue, decision, and GitHub sync state")
    _ = inspect_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = inspect_parser.add_argument("--issue-number", help="Explicit issue number override")

    retry_sync_parser = subparsers.add_parser("retry-github-sync", help="Retry a failed GitHub label sync attempt by command id")
    _ = retry_sync_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = retry_sync_parser.add_argument("--command-id", required=True, help="Failed GitHub sync command id to replay")
    _ = retry_sync_parser.add_argument("--updated-at")

    retry_failed_parser = subparsers.add_parser("retry-failed", help="Move a retryable failed issue back to ready-for-agent")
    _ = retry_failed_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = retry_failed_parser.add_argument("--issue-number", help="Explicit issue number override")
    _ = retry_failed_parser.add_argument("--reason", required=True, help="Why the failed issue is safe to retry")
    _ = retry_failed_parser.add_argument("--updated-at")

    return parser


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    args = build_parser().parse_args(raw_argv)

    if cast(str, args.command) == "init":
        issue_packet_path = Path(cast(str, args.issue_packet))
        ledger_path = Path(cast(str, args.ledger))
        request_path = Path(cast(str, args.request))
        record = parse_issue_packet_text(
            issue_packet_path.read_text(encoding="utf-8"),
            str(issue_packet_path),
        )
        ledger = create_initial_ledger(
            issue_packet=record,
            checkpoint_path=cast(str, args.checkpoint_path),
            workflow_policy_path=cast(str, args.workflow_policy_path),
            primary_workspace_root=str(_infer_artifact_base_dir(ledger_path)),
            updated_at=cast(str | None, args.updated_at),
        )
        write_ledger_file(ledger_path, ledger)
        print(f"wrote supervisor ledger {ledger_path}")
        if cast(bool, args.write_request):
            request = build_orchestrator_request(ledger)
            write_session_request(request_path, request)
            print(f"wrote session request {request_path}")
            if cast(bool, args.dispatch_now):
                session_result_path = default_session_result_path_for_request(request_path)
                _ = _dispatch_consumed_request(
                    request_path,
                    ledger_path=ledger_path,
                    session_result_path=session_result_path,
                    source_session_id=cast(str, args.source_session_id),
                    updated_at=cast(str | None, args.updated_at),
                )
                print(f"wrote session result {session_result_path}")
        return 0

    if cast(str, args.command) == "dispatch":
        ledger_path = Path(cast(str, args.ledger))
        request_path = _resolve_cli_request_path(
            argv=raw_argv,
            ledger_path=ledger_path,
            raw_request_path=cast(str, args.request),
        )
        session_result_path = _resolve_cli_session_result_path(
            argv=raw_argv,
            ledger_path=ledger_path,
            request_path=request_path,
            raw_session_result_path=cast(str, args.session_result),
        )
        session_result = _dispatch_consumed_request(
            request_path,
            ledger_path=ledger_path,
            session_result_path=session_result_path,
            source_session_id=cast(str, args.source_session_id),
            updated_at=cast(str | None, args.updated_at),
        )
        print(f"wrote session result {session_result_path}")
        print(json.dumps(session_result, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "start-issue":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        session_result = start_issue(
            base_dir=base_dir,
            issue_number=cast(str, args.issue_number),
            source_session_id=cast(str, args.source_session_id),
            approval_override_mode=cast(str | None, getattr(args, "approval_override_mode", None)),
            override_source=cast(str | None, getattr(args, "override_source", None)),
            human_approval_skipped=cast(bool, getattr(args, "human_approval_skipped", False)),
            updated_at=cast(str | None, args.updated_at),
        )
        print(json.dumps(session_result, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "show-session":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        session_result = show_latest_session(
            base_dir=base_dir,
            issue_number=cast(str | None, getattr(args, "issue_number", None)),
        )
        if session_result is None:
            print(json.dumps({"status": "missing", "error": "no DB-backed dispatch result found"}, ensure_ascii=False))
            return 1
        print(json.dumps(session_result, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "reconcile-issue":
        return _run_reconcile_issue_cli(
            base_dir=Path(cast(str, args.base_dir)).resolve(),
            issue_number=cast(str, args.issue_number),
            updated_at=cast(str | None, args.updated_at),
        )

    if cast(str, args.command) == "redispatch-quarantined":
        ledger_path = Path(cast(str, args.ledger))
        request_path = _resolve_cli_request_path(
            argv=raw_argv,
            ledger_path=ledger_path,
            raw_request_path=cast(str, args.request),
        )
        session_result_path = _resolve_cli_session_result_path(
            argv=raw_argv,
            ledger_path=ledger_path,
            request_path=request_path,
            raw_session_result_path=cast(str, args.session_result),
        )
        session_result = redispatch_quarantined_issue(
            ledger_path=ledger_path,
            request_path=request_path,
            session_result_path=session_result_path,
            issue_number=cast(str | None, getattr(args, "issue_number", None)),
            reason=cast(str, args.reason),
            source_session_id=cast(str, args.source_session_id),
            updated_at=cast(str | None, args.updated_at),
        )
        print(f"wrote session result {session_result_path}")
        print(json.dumps(session_result, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) in {"quarantine", "resume-quarantined", "fail-quarantined"}:
        ledger_path = Path(cast(str, args.ledger))
        ledger = _read_json(ledger_path)
        base_dir = _infer_artifact_base_dir(ledger_path)
        issue = cast(dict[str, str], ledger["issue"])
        issue_number = cast(str | None, getattr(args, "issue_number", None)) or issue["number"]
        reason = cast(str, args.reason)
        updated_at = cast(str | None, args.updated_at)
        if cast(str, args.command) == "quarantine":
            quarantine_issue_execution(base_dir=base_dir, issue_number=issue_number, reason=reason, updated_at=updated_at)
            print(f"quarantined issue #{issue_number}")
            return 0
        if cast(str, args.command) == "resume-quarantined":
            resume_quarantined_issue_execution(base_dir=base_dir, issue_number=issue_number, reason=reason, updated_at=updated_at)
            print(f"resumed quarantined issue #{issue_number}")
            return 0
        fail_quarantined_issue_execution(base_dir=base_dir, issue_number=issue_number, reason=reason, updated_at=updated_at)
        print(f"failed quarantined issue #{issue_number}")
        return 0

    if cast(str, args.command) == "inspect":
        ledger_path = Path(cast(str, args.ledger))
        ledger = _read_json(ledger_path)
        base_dir = _infer_artifact_base_dir(ledger_path)
        issue = cast(dict[str, str], ledger["issue"])
        issue_number = cast(str | None, getattr(args, "issue_number", None)) or issue["number"]
        print(json.dumps(inspect_control_plane(base_dir=base_dir, issue_number=issue_number), indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "retry-github-sync":
        ledger_path = Path(cast(str, args.ledger))
        base_dir = _infer_artifact_base_dir(ledger_path)
        payload = retry_github_sync_attempt(
            base_dir=base_dir,
            command_id=cast(str, args.command_id),
            updated_at=cast(str | None, args.updated_at),
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "retry-failed":
        ledger_path = Path(cast(str, args.ledger))
        ledger = _read_json(ledger_path)
        base_dir = _infer_artifact_base_dir(ledger_path)
        issue = cast(dict[str, str], ledger["issue"])
        payload = retry_failed_issue_execution(
            base_dir=base_dir,
            issue_number=cast(str | None, getattr(args, "issue_number", None)) or issue["number"],
            reason=cast(str, args.reason),
            updated_at=cast(str | None, args.updated_at),
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    ledger_path = Path(cast(str, args.ledger))
    request_path = _resolve_cli_request_path(
        argv=raw_argv,
        ledger_path=ledger_path,
        raw_request_path=cast(str, args.request),
    )
    session_result_path = _resolve_cli_session_result_path(
        argv=raw_argv,
        ledger_path=ledger_path,
        request_path=request_path,
        raw_session_result_path=cast(str, args.session_result),
    )
    return _run_reconcile_cli(
        ledger_path=ledger_path,
        request_path=request_path,
        session_result_path=session_result_path,
        updated_at=cast(str | None, args.updated_at),
        write_request=cast(bool, args.write_request),
        dispatch_now=cast(bool, args.dispatch_now),
        source_session_id=cast(str, args.source_session_id),
        child_only=cast(str, args.command) == "advance-child",
    )


if __name__ == "__main__":
    raise SystemExit(main())
