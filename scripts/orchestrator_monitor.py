#!/usr/bin/env python3
"""Cross-check orchestrator ledger, control-plane DB, and artifacts."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import cast

from scripts.control_plane_db import read_issue, ready_issues_for_selection
from scripts.orchestrator_supervisor import (
    DEFAULT_LEDGER_PATH,
    ROOT_HEARTBEAT_TIMEOUT_SECONDS,
    default_session_request_path_for_ledger,
    default_session_result_path_for_ledger,
    quarantine_issue_execution,
    reconcile_ledger,
    redispatch_quarantined_issue,
    write_ledger_file,
    write_session_request,
)


JsonObject = dict[str, object]
AUTO_RECOVERY_SOURCE_SESSION_ID = "monitor_auto_redispatch"


def _read_json(path: Path) -> JsonObject:
    payload = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return cast(JsonObject, payload)


def _json_object(value: object) -> JsonObject:
    return cast(JsonObject, value) if isinstance(value, dict) else {}


def _parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _infer_base_dir(ledger_path: Path) -> Path:
    resolved = ledger_path.resolve()
    if resolved.parent.name == "runtime" and resolved.parent.parent.name == ".opencode":
        return resolved.parent.parent.parent
    return resolved.parent


def _resolve_artifact_path(base_dir: Path, artifact_path: str) -> Path:
    path = Path(artifact_path)
    if path.is_absolute():
        return path
    return base_dir / path


def _build_event(*, rule_id: str, severity: str, summary: str, evidence: JsonObject) -> JsonObject:
    return {
        "rule_id": rule_id,
        "severity": severity,
        "summary": summary,
        "evidence": evidence,
    }


def _required_artifact_keys(*, current_role: str, current_stage: str, runtime_state: str, artifacts: JsonObject) -> list[str]:
    required: list[str] = []
    if current_role in {"pr_verifier", "release_worker"} or runtime_state in {"verifying"}:
        if str(artifacts.get("workerResultPath") or ""):
            required.append("workerResultPath")
    if current_role == "release_worker" or runtime_state in {"completed", "failed", "quarantined"}:
        if str(artifacts.get("evidencePacketPath") or ""):
            required.append("evidencePacketPath")
    if current_role == "main_orchestrator" and current_stage == "issue_selection_or_recovery":
        if str(artifacts.get("releaseResultPath") or ""):
            required.append("releaseResultPath")
    elif current_role == "release_worker" or runtime_state == "completed":
        if str(artifacts.get("releaseResultPath") or ""):
            required.append("releaseResultPath")
    return required


def _current_role_artifact_key(current_role: str) -> str:
    if current_role == "issue_worker":
        return "workerResultPath"
    if current_role == "pr_verifier":
        return "evidencePacketPath"
    if current_role == "release_worker":
        return "releaseResultPath"
    return ""


def _role_artifact_missing(*, base_dir: Path, current_role: str, artifacts: JsonObject) -> bool:
    artifact_key = _current_role_artifact_key(current_role)
    if not artifact_key:
        return False
    artifact_ref = str(artifacts.get(artifact_key) or "")
    if not artifact_ref:
        return False
    return not _resolve_artifact_path(base_dir, artifact_ref).exists()


def _auto_recovery_enabled(automation: JsonObject) -> bool:
    return bool(automation.get("continueWithoutHuman")) and bool(automation.get("queueNextSessionOnIdle"))


def _should_auto_redispatch_issue_worker(
    *,
    ledger: JsonObject,
    runtime_issue: JsonObject,
    current: JsonObject,
    events: list[JsonObject],
    base_dir: Path,
) -> bool:
    current_role = str(current.get("role") or "")
    current_status = str(current.get("status") or "")
    runtime_state = str(runtime_issue.get("state") or "")
    if current_role != "issue_worker" or current_status != "queued":
        return False
    if runtime_state not in {"dispatching", "running"}:
        return False
    if not _auto_recovery_enabled(_json_object(ledger.get("automation", {}))):
        return False
    if not _role_artifact_missing(base_dir=base_dir, current_role=current_role, artifacts=_json_object(ledger.get("artifacts", {}))):
        return False
    rule_ids = {str(event.get("rule_id") or "") for event in events}
    return bool({"ROOT_HEARTBEAT_STALLED", "DISPATCH_STALLED"} & rule_ids)


def _should_auto_redispatch_queued_subagent(
    *,
    ledger: JsonObject,
    runtime_issue: JsonObject,
    current: JsonObject,
    events: list[JsonObject],
    base_dir: Path,
) -> bool:
    current_role = str(current.get("role") or "")
    current_status = str(current.get("status") or "")
    runtime_state = str(runtime_issue.get("state") or "")
    if current_role not in {"issue_worker", "pr_verifier", "release_worker"} or current_status != "queued":
        return False
    if runtime_state not in {"dispatching", "running", "verifying"}:
        return False
    if not _auto_recovery_enabled(_json_object(ledger.get("automation", {}))):
        return False
    if not _role_artifact_missing(base_dir=base_dir, current_role=current_role, artifacts=_json_object(ledger.get("artifacts", {}))):
        return False
    rule_ids = {str(event.get("rule_id") or "") for event in events}
    return bool({"ROOT_HEARTBEAT_STALLED", "DISPATCH_STALLED"} & rule_ids)


def _auto_recover_stalled_issue_worker(
    *,
    ledger_path: Path,
    base_dir: Path,
    ledger: JsonObject,
    events: list[JsonObject],
    recorded_at: str,
) -> tuple[list[JsonObject], list[JsonObject] | None]:
    issue = _json_object(ledger.get("issue", {}))
    current = _json_object(ledger.get("current", {}))
    issue_number = str(issue.get("number") or "")
    runtime_issue = cast(JsonObject, read_issue(base_dir, issue_number) or {}) if issue_number else {}
    if not issue_number or not _should_auto_redispatch_queued_subagent(
        ledger=ledger,
        runtime_issue=runtime_issue,
        current=current,
        events=events,
        base_dir=base_dir,
    ):
        return [], None

    trigger_rule_ids = sorted({str(event.get("rule_id") or "") for event in events if str(event.get("severity") or "") == "critical"})
    reason = (
        f"Monitor detected stalled queued issue_worker for issue #{issue_number} via {', '.join(trigger_rule_ids)}; "
        "quarantine and redispatch automatically to restore nonstop flow."
    )
    try:
        quarantine_issue_execution(
            base_dir=base_dir,
            issue_number=issue_number,
            reason=reason,
            updated_at=recorded_at,
        )
        session_result = redispatch_quarantined_issue(
            ledger_path=ledger_path,
            request_path=default_session_request_path_for_ledger(ledger_path),
            session_result_path=default_session_result_path_for_ledger(ledger_path),
            reason=reason,
            source_session_id=AUTO_RECOVERY_SOURCE_SESSION_ID,
            updated_at=recorded_at,
        )
    except Exception as error:
        return [
            _build_event(
                rule_id="AUTO_RECOVERY_FAILED",
                severity="critical",
                summary=f"Automatic redispatch failed for stalled issue #{issue_number}.",
                evidence={
                    "issue_number": issue_number,
                    "trigger_rule_ids": trigger_rule_ids,
                    "error": str(error),
                },
            )
        ], None

    if str(session_result.get("status") or "") != "success":
        return [
            _build_event(
                rule_id="AUTO_RECOVERY_FAILED",
                severity="critical",
                summary=f"Automatic redispatch recorded {session_result.get('status', 'unknown')} for stalled issue #{issue_number}.",
                evidence={
                    "issue_number": issue_number,
                    "trigger_rule_ids": trigger_rule_ids,
                    "session_result": session_result,
                },
            )
        ], None

    post_events = collect_monitor_events(
        ledger_path=ledger_path,
        base_dir=base_dir,
        now=recorded_at,
    )
    return [
        _build_event(
            rule_id="AUTO_RECOVERY_REDISPATCHED",
            severity="info",
            summary=f"Automatically redispatched stalled issue #{issue_number} and created a fresh root session.",
            evidence={
                "issue_number": issue_number,
                "trigger_rule_ids": trigger_rule_ids,
                "root_session_id": str(session_result.get("rootSessionID") or ""),
                "source_session_id": AUTO_RECOVERY_SOURCE_SESSION_ID,
            },
        )
    ], post_events


def _should_auto_advance_completed_child(
    *,
    ledger: JsonObject,
    current: JsonObject,
    base_dir: Path,
) -> bool:
    if not _auto_recovery_enabled(_json_object(ledger.get("automation", {}))):
        return False
    current_role = str(current.get("role") or "")
    current_status = str(current.get("status") or "")
    if current_role not in {"issue_worker", "pr_verifier", "release_worker"} or current_status != "queued":
        return False
    artifacts = _json_object(ledger.get("artifacts", {}))
    artifact_key = _current_role_artifact_key(current_role)
    artifact_ref = str(artifacts.get(artifact_key) or "")
    if not artifact_ref:
        return False
    return _resolve_artifact_path(base_dir, artifact_ref).exists()


def _auto_advance_completed_child(
    *,
    ledger_path: Path,
    base_dir: Path,
    ledger: JsonObject,
    recorded_at: str,
) -> tuple[list[JsonObject], list[JsonObject] | None]:
    current = _json_object(ledger.get("current", {}))
    if not _should_auto_advance_completed_child(
        ledger=ledger,
        current=current,
        base_dir=base_dir,
    ):
        return [], None

    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=default_session_result_path_for_ledger(ledger_path),
        artifact_base_dir=base_dir,
        updated_at=recorded_at,
    )
    write_ledger_file(ledger_path, updated_ledger)
    request_path = default_session_request_path_for_ledger(ledger_path)
    if request is not None:
        write_session_request(request_path, request)

    post_events = collect_monitor_events(
        ledger_path=ledger_path,
        base_dir=base_dir,
        now=recorded_at,
    )
    return [
        _build_event(
            rule_id="AUTO_ADVANCED_CHILD_ARTIFACT",
            severity="info",
            summary=(
                f"Automatically advanced issue #{str(_json_object(updated_ledger.get('issue', {})).get('number') or '')} after {str(current.get('role') or '')} wrote its artifact."
            ),
            evidence={
                "previous_role": str(current.get("role") or ""),
                "previous_stage": str(current.get("stage") or ""),
                "decision": decision,
                "request_written": request is not None,
                "request_path": str(request_path) if request is not None else "",
            },
        )
    ], post_events


def collect_monitor_events(
    *,
    ledger_path: Path,
    base_dir: Path | None = None,
    now: str | None = None,
    heartbeat_timeout_seconds: int = ROOT_HEARTBEAT_TIMEOUT_SECONDS,
    selection_timeout_seconds: int = 300,
) -> list[JsonObject]:
    actual_base_dir = base_dir or _infer_base_dir(ledger_path)
    ledger = _read_json(ledger_path)
    issue = _json_object(ledger.get("issue", {}))
    current = _json_object(ledger.get("current", {}))
    automation = _json_object(ledger.get("automation", {}))
    artifacts = _json_object(ledger.get("artifacts", {}))
    issue_number = str(issue.get("number") or "")
    ledger_updated_at = str(ledger.get("updatedAt") or "")
    runtime_issue = read_issue(actual_base_dir, issue_number) if issue_number else None
    events: list[JsonObject] = []

    if runtime_issue is None:
        events.append(
            _build_event(
                rule_id="CONTROL_PLANE_MISSING_ISSUE",
                severity="critical",
                summary=f"Ledger issue #{issue_number or 'unknown'} has no control-plane row.",
                evidence={"issue_number": issue_number, "ledger_path": str(ledger_path)},
            )
        )
        return events

    if (
        str(runtime_issue.get("current_role") or "") != str(current.get("role") or "")
        or str(runtime_issue.get("current_stage") or "") != str(current.get("stage") or "")
        or str(runtime_issue.get("current_status") or "") != str(current.get("status") or "")
    ):
        events.append(
            _build_event(
                rule_id="LEDGER_DB_DRIFT",
                severity="warning",
                summary=f"Ledger and SQLite runtime phase disagree for issue #{issue_number}.",
                evidence={
                    "ledger": {
                        "role": str(current.get("role") or ""),
                        "stage": str(current.get("stage") or ""),
                        "status": str(current.get("status") or ""),
                    },
                    "control_plane": {
                        "role": str(runtime_issue.get("current_role") or ""),
                        "stage": str(runtime_issue.get("current_stage") or ""),
                        "status": str(runtime_issue.get("current_status") or ""),
                    },
                },
            )
        )

    for artifact_key in _required_artifact_keys(
        current_role=str(current.get("role") or ""),
        current_stage=str(current.get("stage") or ""),
        runtime_state=str(runtime_issue.get("state") or ""),
        artifacts=artifacts,
    ):
        artifact_ref = str(artifacts.get(artifact_key) or "")
        if not artifact_ref:
            continue
        resolved_artifact_path = _resolve_artifact_path(actual_base_dir, artifact_ref)
        if not resolved_artifact_path.exists():
            events.append(
                _build_event(
                    rule_id="ARTIFACT_MISSING",
                    severity="critical",
                    summary=f"{artifact_key} for issue #{issue_number} is missing.",
                    evidence={
                        "artifact_key": artifact_key,
                        "artifact_path": str(resolved_artifact_path),
                        "issue_number": issue_number,
                    },
                )
            )

    current_role = str(current.get("role") or "")
    current_stage = str(current.get("stage") or "")
    current_status = str(current.get("status") or "")
    runtime_state = str(runtime_issue.get("state") or "")
    effective_now = now or datetime.now().astimezone().isoformat(timespec="seconds")
    now_time = _parse_timestamp(effective_now)
    last_event_time = _parse_timestamp(str(runtime_issue.get("last_event_at") or ""))
    ledger_time = _parse_timestamp(ledger_updated_at)

    if current_role in {"issue_worker", "pr_verifier", "release_worker"} and current_status == "queued" and runtime_state in {"running", "verifying"} and now_time and last_event_time:
        if now_time - last_event_time > timedelta(seconds=heartbeat_timeout_seconds):
            events.append(
                _build_event(
                    rule_id="ROOT_HEARTBEAT_STALLED",
                    severity="critical",
                    summary=f"Issue #{issue_number} heartbeat stalled while queued subagent work is still waiting to progress.",
                    evidence={
                        "issue_number": issue_number,
                        "current_role": current_role,
                        "last_event_at": str(runtime_issue.get("last_event_at") or ""),
                        "now": effective_now,
                        "heartbeat_timeout_seconds": heartbeat_timeout_seconds,
                    },
                )
            )

    dispatching_time = _parse_timestamp(str(runtime_issue.get("dispatching_at") or runtime_issue.get("updated_at") or ""))
    if (
        current_role == "issue_worker"
        and current_status == "queued"
        and runtime_state == "dispatching"
        and not str(runtime_issue.get("current_root_session_id") or "")
        and now_time
        and dispatching_time
        and _role_artifact_missing(base_dir=actual_base_dir, current_role=current_role, artifacts=artifacts)
    ):
        if now_time - dispatching_time > timedelta(seconds=heartbeat_timeout_seconds):
            events.append(
                _build_event(
                    rule_id="DISPATCH_STALLED",
                    severity="critical",
                    summary=f"Issue #{issue_number} dispatch stalled before any root session id was recorded.",
                    evidence={
                        "issue_number": issue_number,
                        "dispatching_at": str(runtime_issue.get("dispatching_at") or runtime_issue.get("updated_at") or ""),
                        "now": effective_now,
                        "heartbeat_timeout_seconds": heartbeat_timeout_seconds,
                    },
                )
            )

    if runtime_state == "quarantined":
        events.append(
            _build_event(
                rule_id="ISSUE_QUARANTINED",
                severity="critical",
                summary=f"Issue #{issue_number} is quarantined and needs explicit operator attention.",
                evidence={"issue_number": issue_number, "state": runtime_state},
            )
        )

    ready_queue = ready_issues_for_selection(actual_base_dir)
    if current_role == "main_orchestrator" and current_stage == "issue_selection_or_recovery":
        if ready_queue and bool(automation.get("queueNextSessionOnIdle")) and now_time and ledger_time:
            if now_time - ledger_time > timedelta(seconds=selection_timeout_seconds):
                events.append(
                    _build_event(
                        rule_id="SELECTION_STALLED",
                        severity="critical",
                        summary="Ready issues exist, but the orchestrator has not advanced the queue.",
                        evidence={
                            "issue_number": issue_number,
                            "ready_issue_numbers": [str(row.get("issue_number") or "") for row in ready_queue],
                            "ledger_updated_at": ledger_updated_at,
                            "selection_timeout_seconds": selection_timeout_seconds,
                        },
                    )
                )
        elif not ready_queue:
            events.append(
                _build_event(
                    rule_id="READY_QUEUE_EMPTY",
                    severity="info",
                    summary="No ready issues are currently available for auto-selection.",
                    evidence={"issue_number": issue_number},
                )
            )

    if not events:
        events.append(
            _build_event(
                rule_id="RUNTIME_HEALTHY",
                severity="info",
                summary=f"No monitor anomalies detected for issue #{issue_number}.",
                evidence={"issue_number": issue_number, "ledger_path": str(ledger_path)},
            )
        )

    return events


def append_monitor_log(*, monitor_log_path: Path, events: list[JsonObject], recorded_at: str) -> None:
    monitor_log_path.parent.mkdir(parents=True, exist_ok=True)
    with monitor_log_path.open("a", encoding="utf-8") as handle:
        for event in events:
            payload = {"recorded_at": recorded_at, **event}
            _ = handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def append_monitor_alerts(*, monitor_alerts_path: Path, events: list[JsonObject]) -> None:
    alert_events = [event for event in events if str(event.get("severity") or "") in {"warning", "critical"}]
    if not alert_events:
        return
    monitor_alerts_path.parent.mkdir(parents=True, exist_ok=True)
    with monitor_alerts_path.open("a", encoding="utf-8") as handle:
        for event in alert_events:
            _ = handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")


def _recorded_at(now: str | None) -> str:
    return now or datetime.now().astimezone().isoformat(timespec="seconds")


def run_monitor_cycle(
    *,
    ledger_path: Path,
    base_dir: Path | None,
    monitor_log_path: Path,
    now: str | None,
    heartbeat_timeout_seconds: int,
    selection_timeout_seconds: int,
    monitor_alerts_path: Path | None = None,
) -> tuple[list[JsonObject], int]:
    initial_events = collect_monitor_events(
        ledger_path=ledger_path,
        base_dir=base_dir,
        now=now,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        selection_timeout_seconds=selection_timeout_seconds,
    )
    recorded_at = _recorded_at(now)
    actual_base_dir = base_dir or _infer_base_dir(ledger_path)
    ledger = _read_json(ledger_path)
    advance_events, post_advance_events = _auto_advance_completed_child(
        ledger_path=ledger_path,
        base_dir=actual_base_dir,
        ledger=ledger,
        recorded_at=recorded_at,
    )
    if post_advance_events is not None:
        ledger = _read_json(ledger_path)
    recovery_events, post_recovery_events = _auto_recover_stalled_issue_worker(
        ledger_path=ledger_path,
        base_dir=actual_base_dir,
        ledger=ledger,
        events=post_advance_events if post_advance_events is not None else initial_events,
        recorded_at=recorded_at,
    )
    events = initial_events + advance_events + (post_advance_events or []) + recovery_events + (post_recovery_events or [])
    append_monitor_log(monitor_log_path=monitor_log_path, events=events, recorded_at=recorded_at)
    if monitor_alerts_path is not None:
        append_monitor_alerts(monitor_alerts_path=monitor_alerts_path, events=events)
    for event in events:
        print(json.dumps({"recorded_at": recorded_at, **event}, ensure_ascii=False, sort_keys=True))
    effective_events = post_recovery_events if post_recovery_events is not None else (post_advance_events if post_advance_events is not None else initial_events + advance_events + recovery_events)
    exit_code = 1 if any(str(event.get("severity") or "") == "critical" for event in effective_events) else 0
    return events, exit_code


def run_monitor_watch(
    *,
    ledger_path: Path,
    base_dir: Path | None,
    monitor_log_path: Path,
    now: str | None,
    heartbeat_timeout_seconds: int,
    selection_timeout_seconds: int,
    interval_seconds: float,
    iterations: int,
    stop_on_critical: bool,
    monitor_alerts_path: Path | None = None,
) -> int:
    iteration = 0
    final_exit_code = 0
    while iterations <= 0 or iteration < iterations:
        _, exit_code = run_monitor_cycle(
            ledger_path=ledger_path,
            base_dir=base_dir,
            monitor_log_path=monitor_log_path,
            now=now,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
            selection_timeout_seconds=selection_timeout_seconds,
            monitor_alerts_path=monitor_alerts_path,
        )
        final_exit_code = max(final_exit_code, exit_code)
        iteration += 1
        if stop_on_critical and exit_code != 0:
            return exit_code
        if iterations > 0 and iteration >= iterations:
            break
        time.sleep(interval_seconds)
    return final_exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = parser.add_argument("--base-dir", help="Consumer project root containing .opencode/runtime")
    _ = parser.add_argument("--monitor-log", help="Append JSONL monitor events to this file")
    _ = parser.add_argument("--monitor-alerts", help="Append warning and critical monitor events to this file")
    _ = parser.add_argument("--now", help="Override current timestamp for deterministic checks")
    _ = parser.add_argument(
        "--heartbeat-timeout-seconds",
        type=int,
        default=ROOT_HEARTBEAT_TIMEOUT_SECONDS,
        help="Stale running-session threshold",
    )
    _ = parser.add_argument(
        "--selection-timeout-seconds",
        type=int,
        default=300,
        help="Stale issue-selection threshold",
    )
    _ = parser.add_argument("--watch", action="store_true", help="Run monitor continuously")
    _ = parser.add_argument(
        "--interval-seconds",
        type=float,
        default=30.0,
        help="Polling interval when --watch is enabled",
    )
    _ = parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of monitor cycles to run; use 0 with --watch for infinite polling",
    )
    _ = parser.add_argument(
        "--stop-on-critical",
        action="store_true",
        help="Exit watch mode immediately after a critical event",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ledger_path = Path(str(args.ledger))
    base_dir = Path(str(args.base_dir)) if args.base_dir else None
    monitor_log_path = (
        Path(str(args.monitor_log)) if args.monitor_log else (base_dir or _infer_base_dir(ledger_path)) / ".opencode/runtime/monitor.log"
    )
    monitor_alerts_path = (
        Path(str(args.monitor_alerts)) if args.monitor_alerts else monitor_log_path.with_name("monitor-alerts.jsonl")
    )
    watch = cast(bool, args.watch)
    iterations = cast(int, args.iterations)
    if watch:
        watch_iterations = iterations if iterations != 1 else 0
        return run_monitor_watch(
            ledger_path=ledger_path,
            base_dir=base_dir,
            monitor_log_path=monitor_log_path,
            now=cast(str | None, args.now),
            heartbeat_timeout_seconds=cast(int, args.heartbeat_timeout_seconds),
            selection_timeout_seconds=cast(int, args.selection_timeout_seconds),
            interval_seconds=cast(float, args.interval_seconds),
            iterations=watch_iterations,
            stop_on_critical=cast(bool, args.stop_on_critical),
            monitor_alerts_path=monitor_alerts_path,
        )
    _, exit_code = run_monitor_cycle(
        ledger_path=ledger_path,
        base_dir=base_dir,
        monitor_log_path=monitor_log_path,
        now=cast(str | None, args.now),
        heartbeat_timeout_seconds=cast(int, args.heartbeat_timeout_seconds),
        selection_timeout_seconds=cast(int, args.selection_timeout_seconds),
        monitor_alerts_path=monitor_alerts_path,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
