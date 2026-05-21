#!/usr/bin/env python3
"""Cross-check DB-backed control-plane state and persisted runtime facts."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import Any
from typing import cast

from scripts.control_plane_db import control_plane_db_path, development_slot_occupancy, list_issues, read_issue, ready_issues_for_selection, release_slot_occupancy
from scripts.orchestrator_supervisor import root_heartbeat_timeout_seconds


JsonObject = dict[str, object]

def _json_object(value: object) -> JsonObject:
    return cast(JsonObject, value) if isinstance(value, dict) else {}


def _parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _build_event(*, rule_id: str, severity: str, summary: str, evidence: JsonObject) -> JsonObject:
    return {
        "rule_id": rule_id,
        "severity": severity,
        "summary": summary,
        "evidence": evidence,
    }


def _load_issue_json(issue: dict[str, Any] | None, key: str) -> JsonObject:
    if not issue:
        return {}
    raw = issue.get(key)
    if isinstance(raw, dict):
        return cast(JsonObject, raw)
    try:
        payload = json.loads(str(raw or "{}"))
    except json.JSONDecodeError:
        return {}
    return cast(JsonObject, payload) if isinstance(payload, dict) else {}


def _release_child_session(issue: dict[str, Any] | None) -> JsonObject:
    runtime_context = _load_issue_json(issue, "runtime_context_json")
    payload = runtime_context.get("release_child_session")
    return _json_object(payload)


def _select_monitored_issue(*, base_dir: Path, issue_number: str | None) -> dict[str, Any] | None:
    if issue_number:
        return read_issue(base_dir, issue_number)

    issues = list_issues(base_dir)
    if not issues:
        return None

    for issue in issues:
        if any(
            str(issue.get(field) or "")
            for field in ("current_role", "current_stage", "current_status")
        ):
            return issue

    for issue in issues:
        if str(issue.get("state") or "") in {"claimed", "dispatching", "running", "verifying", "quarantined"}:
            return issue

    return issues[0]


def _required_artifact_keys(*, current_role: str, current_stage: str, runtime_state: str) -> list[str]:
    required: list[str] = []
    if current_role in {"pr_verifier"} or (current_role == "main_orchestrator" and current_stage == "release_root_execution") or runtime_state in {"verifying"}:
        required.append("worker_result")
    if (current_role == "main_orchestrator" and current_stage == "release_root_execution") or runtime_state in {"completed", "failed", "quarantined"}:
        required.append("evidence_packet")
    if current_role == "main_orchestrator" and current_stage == "issue_selection_or_recovery":
        required.append("release_result")
    elif (current_role == "main_orchestrator" and current_stage == "release_root_execution") or runtime_state == "completed":
        required.append("release_result")
    return required


def _current_role_artifact_key(current_role: str, current_stage: str) -> str:
    if current_role == "issue_worker":
        return "worker_result"
    if current_role == "pr_verifier":
        return "evidence_packet"
    if current_role == "main_orchestrator" and current_stage == "release_root_execution":
        return "release_result"
    return ""


def _artifact_status_key(artifact_key: str) -> str:
    if artifact_key == "worker_result":
        return "worker_result"
    if artifact_key == "evidence_packet":
        return "evidence_packet"
    if artifact_key == "release_result":
        return "release_result"
    return ""


def _artifact_fact_missing(*, issue: dict[str, Any], artifact_key: str) -> bool:
    if not artifact_key:
        return False
    artifact_status = _load_issue_json(issue, "artifact_status_json")
    status_key = _artifact_status_key(artifact_key)
    persisted = artifact_status.get(status_key)
    if not isinstance(persisted, dict):
        return True
    return not bool(persisted.get("parse_ok"))


def _auto_recovery_enabled(automation: JsonObject) -> bool:
    return bool(automation.get("continueWithoutHuman")) and bool(automation.get("queueNextSessionOnIdle"))


def collect_monitor_events(
    *,
    base_dir: Path,
    issue_number: str | None = None,
    now: str | None = None,
    heartbeat_timeout_seconds: int | None = None,
    selection_timeout_seconds: int = 300,
) -> list[JsonObject]:
    effective_heartbeat_timeout_seconds = heartbeat_timeout_seconds if heartbeat_timeout_seconds is not None else root_heartbeat_timeout_seconds()
    runtime_issue = _select_monitored_issue(base_dir=base_dir, issue_number=issue_number)
    events: list[JsonObject] = []

    if runtime_issue is None:
        if issue_number:
            events.append(
                _build_event(
                    rule_id="CONTROL_PLANE_MISSING_ISSUE",
                    severity="critical",
                    summary=f"Issue #{issue_number} has no control-plane row.",
                    evidence={"issue_number": issue_number},
                )
            )
            return events
        events.append(
            _build_event(
                rule_id="CONTROL_PLANE_EMPTY",
                severity="info",
                summary="No control-plane issues are currently available for monitoring.",
                evidence={"base_dir": str(base_dir)},
            )
        )
        return events

    current = {
        "role": str(runtime_issue.get("current_role") or ""),
        "stage": str(runtime_issue.get("current_stage") or ""),
        "status": str(runtime_issue.get("current_status") or ""),
    }
    automation = _load_issue_json(runtime_issue, "automation_flags_json")
    monitored_issue_number = str(runtime_issue.get("issue_number") or issue_number or "")
    development_occupancy = development_slot_occupancy(base_dir)
    release_occupancy = release_slot_occupancy(base_dir)

    for artifact_key in _required_artifact_keys(
        current_role=str(current.get("role") or ""),
        current_stage=str(current.get("stage") or ""),
        runtime_state=str(runtime_issue.get("state") or ""),
    ):
        if _artifact_fact_missing(issue=runtime_issue, artifact_key=artifact_key):
            events.append(
                _build_event(
                    rule_id="ARTIFACT_MISSING",
                    severity="critical",
                    summary=f"{artifact_key} for issue #{monitored_issue_number} has no persisted DB artifact fact yet.",
                    evidence={
                        "artifact_key": artifact_key,
                        "issue_number": monitored_issue_number,
                    },
                )
            )

    current_role = str(current.get("role") or "")
    current_stage = str(current.get("stage") or "")
    current_status = str(current.get("status") or "")
    runtime_state = str(runtime_issue.get("state") or "")
    release_child_session = _release_child_session(runtime_issue)
    effective_now = now or datetime.now().astimezone().isoformat(timespec="seconds")
    now_time = _parse_timestamp(effective_now)
    last_event_time = _parse_timestamp(str(runtime_issue.get("last_event_at") or ""))
    issue_time = _parse_timestamp(str(runtime_issue.get("updated_at") or runtime_issue.get("last_event_at") or ""))

    if current_role in {"issue_worker", "pr_verifier", "release_worker"} and current_status == "queued" and runtime_state in {"running", "verifying"} and now_time and last_event_time:
        if now_time - last_event_time > timedelta(seconds=effective_heartbeat_timeout_seconds):
            events.append(
                _build_event(
                    rule_id="ROOT_HEARTBEAT_STALLED",
                    severity="critical",
                    summary=f"Issue #{monitored_issue_number} heartbeat stalled while queued subagent work is still waiting to progress.",
                    evidence={
                        "issue_number": monitored_issue_number,
                        "current_role": current_role,
                        "last_event_at": str(runtime_issue.get("last_event_at") or ""),
                        "now": effective_now,
                        "heartbeat_timeout_seconds": effective_heartbeat_timeout_seconds,
                    },
                )
            )

    dispatching_time = _parse_timestamp(str(runtime_issue.get("dispatching_at") or runtime_issue.get("updated_at") or ""))
    if (
        current_role == "issue_worker"
        and current_status == "queued"
        and runtime_state == "dispatching"
        and not str(runtime_issue.get("current_session_id") or "")
        and now_time
        and dispatching_time
    and _artifact_fact_missing(issue=runtime_issue, artifact_key=_current_role_artifact_key(current_role, current_stage))
    ):
        if now_time - dispatching_time > timedelta(seconds=effective_heartbeat_timeout_seconds):
            events.append(
                _build_event(
                    rule_id="DISPATCH_STALLED",
                    severity="critical",
                    summary=f"Issue #{monitored_issue_number} dispatch stalled before any root session id was recorded.",
                    evidence={
                        "issue_number": monitored_issue_number,
                        "dispatching_at": str(runtime_issue.get("dispatching_at") or runtime_issue.get("updated_at") or ""),
                        "now": effective_now,
                        "heartbeat_timeout_seconds": effective_heartbeat_timeout_seconds,
                    },
                )
            )

    if runtime_state == "quarantined":
        events.append(
            _build_event(
                rule_id="ISSUE_QUARANTINED",
                severity="critical",
                summary=f"Issue #{monitored_issue_number} is quarantined and needs explicit operator attention.",
                evidence={"issue_number": monitored_issue_number, "state": runtime_state},
            )
        )

    ready_queue = ready_issues_for_selection(base_dir)
    if current_role == "main_orchestrator" and current_stage == "issue_selection_or_recovery":
        if ready_queue and bool(automation.get("queueNextSessionOnIdle")) and now_time and issue_time:
            if now_time - issue_time > timedelta(seconds=selection_timeout_seconds):
                events.append(
                    _build_event(
                        rule_id="SELECTION_STALLED",
                        severity="critical",
                        summary="Ready issues exist, but the orchestrator has not advanced the queue.",
                        evidence={
                            "issue_number": monitored_issue_number,
                            "ready_issue_numbers": [str(row.get("issue_number") or "") for row in ready_queue],
                            "issue_updated_at": str(runtime_issue.get("updated_at") or runtime_issue.get("last_event_at") or ""),
                            "selection_timeout_seconds": selection_timeout_seconds,
                            "development_slot_occupancy": development_occupancy,
                            "release_slot_occupancy": release_occupancy,
                        },
                    )
                )
        elif not ready_queue:
            events.append(
                _build_event(
                    rule_id="READY_QUEUE_EMPTY",
                    severity="info",
                    summary="No ready issues are currently available for auto-selection.",
                    evidence={
                        "issue_number": monitored_issue_number,
                        "development_slot_occupancy": development_occupancy,
                        "release_slot_occupancy": release_occupancy,
                    },
                )
            )

    if not events:
        events.append(
            _build_event(
                rule_id="RUNTIME_HEALTHY",
                severity="info",
                summary=f"No monitor anomalies detected for issue #{monitored_issue_number}.",
                evidence={"issue_number": monitored_issue_number},
            )
        )

    if current_role == "main_orchestrator" and current_stage == "release_root_execution" and release_child_session:
        events.append(
            _build_event(
                rule_id="RELEASE_CHILD_SESSION_TRACKED",
                severity="info",
                summary=f"Issue #{monitored_issue_number} release root session is tracking a foreground release_worker child session.",
                evidence={"issue_number": monitored_issue_number, **release_child_session},
            )
        )

    return events


def append_monitor_log(*, monitor_log_path: Path, events: list[JsonObject], recorded_at: str) -> None:
    """Write operator diagnostics only; workflow control remains in SQLite."""
    monitor_log_path.parent.mkdir(parents=True, exist_ok=True)
    with monitor_log_path.open("a", encoding="utf-8") as handle:
        for event in events:
            payload = {"recorded_at": recorded_at, **event}
            _ = handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def append_monitor_alerts(*, monitor_alerts_path: Path, events: list[JsonObject]) -> None:
    """Write operator alerts only; these files are never workflow gates."""
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
    base_dir: Path,
    issue_number: str | None = None,
    monitor_log_path: Path,
    now: str | None,
    heartbeat_timeout_seconds: int,
    selection_timeout_seconds: int,
    monitor_alerts_path: Path | None = None,
) -> tuple[list[JsonObject], int]:
    events = collect_monitor_events(
        base_dir=base_dir,
        issue_number=issue_number,
        now=now,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        selection_timeout_seconds=selection_timeout_seconds,
    )
    recorded_at = _recorded_at(now)
    append_monitor_log(monitor_log_path=monitor_log_path, events=events, recorded_at=recorded_at)
    if monitor_alerts_path is not None:
        append_monitor_alerts(monitor_alerts_path=monitor_alerts_path, events=events)
    for event in events:
        print(json.dumps({"recorded_at": recorded_at, **event}, ensure_ascii=False, sort_keys=True))
    exit_code = 1 if any(str(event.get("severity") or "") == "critical" for event in events) else 0
    return events, exit_code


def run_monitor_watch(
    *,
    base_dir: Path,
    issue_number: str | None = None,
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
            base_dir=base_dir,
            issue_number=issue_number,
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
    _ = parser.add_argument("--base-dir", default=".", help="Consumer project root containing the SQLite control plane")
    _ = parser.add_argument("--issue-number", help="Specific issue number to monitor; defaults to the most active DB-backed issue")
    _ = parser.add_argument("--monitor-log", help="Append JSONL monitor events to this file")
    _ = parser.add_argument("--monitor-alerts", help="Append warning and critical monitor events to this file")
    _ = parser.add_argument("--now", help="Override current timestamp for deterministic checks")
    _ = parser.add_argument(
        "--heartbeat-timeout-seconds",
        type=int,
        default=root_heartbeat_timeout_seconds(),
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
    base_dir = Path(str(args.base_dir)).resolve()
    default_monitor_log_path = control_plane_db_path(base_dir).with_name("monitor.log")
    monitor_log_path = (
        Path(str(args.monitor_log)) if args.monitor_log else default_monitor_log_path
    )
    monitor_alerts_path = (
        Path(str(args.monitor_alerts)) if args.monitor_alerts else monitor_log_path.with_name("monitor-alerts.jsonl")
    )
    watch = cast(bool, args.watch)
    iterations = cast(int, args.iterations)
    if watch:
        watch_iterations = iterations if iterations != 1 else 0
        return run_monitor_watch(
            base_dir=base_dir,
            issue_number=cast(str | None, args.issue_number),
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
        base_dir=base_dir,
        issue_number=cast(str | None, args.issue_number),
        monitor_log_path=monitor_log_path,
        now=cast(str | None, args.now),
        heartbeat_timeout_seconds=cast(int, args.heartbeat_timeout_seconds),
        selection_timeout_seconds=cast(int, args.selection_timeout_seconds),
        monitor_alerts_path=monitor_alerts_path,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
