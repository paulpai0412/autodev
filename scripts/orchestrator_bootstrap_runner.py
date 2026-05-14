#!/usr/bin/env python3
"""Compatibility wrapper that delegates legacy bootstrap calls into DB-backed startup."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from scripts.orchestrator_supervisor import (
    DEFAULT_LEDGER_PATH,
    _infer_artifact_base_dir,
    _sync_issue_packet_to_db,
    parse_issue_packet_text,
    run_issue_packet_intake,
    start_issue,
)
from scripts.orchestrator_compact_payload import DEFAULT_CHECKPOINT_PATH, DEFAULT_WORKFLOW_POLICY_PATH


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NEW_SESSION_REQUEST_PATH = ROOT / ".opencode/runtime/new-session-request.json"
DEFAULT_ISSUE_PACKETS_DIR = ROOT / "docs/agents/issue-packets"


@dataclass
class RunnerResult:
    issue_number: str
    branch: str
    immediate_next_action: str
    session_result: dict[str, object]


def _normalize_issue_packet_ref(issue_packet_path: Path) -> str:
    if not issue_packet_path.is_absolute():
        return str(issue_packet_path)
    try:
        return str(issue_packet_path.relative_to(ROOT))
    except ValueError:
        return str(issue_packet_path)


def _normalize_issue_number(issue_number: str) -> str:
    normalized = issue_number.strip().removeprefix("#")
    if normalized.startswith("issue-"):
        normalized = normalized.removeprefix("issue-")
    if not normalized.isdigit():
        raise ValueError(f"issue number must be numeric, got {issue_number!r}")
    return normalized


def resolve_issue_packet_path(issue_number: str, *, base_dir: Path | None = None) -> Path:
    normalized = _normalize_issue_number(issue_number)
    actual_base_dir = base_dir or ROOT
    packet_path = actual_base_dir / "docs/agents/issue-packets" / f"issue-{normalized}.yaml"
    if packet_path.exists():
        return packet_path

    if run_issue_packet_intake(actual_base_dir) and packet_path.exists():
        return packet_path

    message = f"issue packet not found for issue #{normalized}: {packet_path}. "
    message += "Ensure gh is authenticated and the GitHub issue has the ready-for-agent label."
    raise RuntimeError(message)


def run_orchestrator_bootstrap(
    *,
    issue_packet_path: Path,
    checkpoint_path: Path,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
    workflow_policy_path: str = DEFAULT_WORKFLOW_POLICY_PATH,
    new_session_request_path: Path = DEFAULT_NEW_SESSION_REQUEST_PATH,
    dispatch_now: bool = False,
    source_session_id: str = "orchestrator-bootstrap",
    approval_override_mode: str | None = None,
    override_source: str | None = None,
    human_approval_skipped: bool | None = None,
    updated_at: str | None = None,
) -> RunnerResult:
    del checkpoint_path, new_session_request_path, dispatch_now, workflow_policy_path
    issue_packet = parse_issue_packet_text(
        issue_packet_path.read_text(encoding="utf-8"),
        _normalize_issue_packet_ref(issue_packet_path),
    )
    base_dir = _infer_artifact_base_dir(ledger_path)
    _sync_issue_packet_to_db(base_dir, issue_packet, updated_at=updated_at)
    session_result = start_issue(
        base_dir=base_dir,
        issue_number=issue_packet.issue_number,
        source_session_id=source_session_id,
        approval_override_mode=approval_override_mode,
        override_source=override_source,
        human_approval_skipped=human_approval_skipped,
        updated_at=updated_at,
    )

    return RunnerResult(
        issue_number=issue_packet.issue_number,
        branch=issue_packet.branch,
        immediate_next_action="Inspect the DB-backed root session via scripts.orchestrator_supervisor show-session or /autodev-show-session.",
        session_result=cast(dict[str, object], cast(object, dict(session_result))),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    issue_target = parser.add_mutually_exclusive_group(required=True)
    _ = issue_target.add_argument("--issue-number", help="GitHub issue number, e.g. 32")
    _ = issue_target.add_argument("--issue-packet", help="Path to the selected AFK issue packet")
    _ = parser.add_argument("--checkpoint", default=str(DEFAULT_CHECKPOINT_PATH), help="Path to context-checkpoint.yaml")
    _ = parser.add_argument(
        "--ledger",
        default=str(DEFAULT_LEDGER_PATH),
        help="Path to .opencode/runtime/orchestrator-ledger.json",
    )
    _ = parser.add_argument(
        "--new-session-request",
        default=str(DEFAULT_NEW_SESSION_REQUEST_PATH),
        help="Path to .opencode/runtime/new-session-request.json",
    )
    _ = parser.add_argument("--dispatch-now", action="store_true", help="Explicitly launch the fresh root session immediately")
    _ = parser.add_argument("--source-session-id", default="orchestrator-bootstrap", help="Source session id to record when dispatching immediately")
    _ = parser.add_argument("--approval-override-mode", help="Workflow-start merge approval override mode")
    _ = parser.add_argument("--override-source", help="Workflow-start approval override source")
    _ = parser.add_argument("--human-approval-skipped", action="store_true", help="Record that human approval is intentionally skipped for this workflow run")
    _ = parser.add_argument(
        "--workflow-policy-path",
        default=DEFAULT_WORKFLOW_POLICY_PATH,
        help="Canonical workflow policy ref for authoritative_refs",
    )
    _ = parser.add_argument("--updated-at", help="Fixed timestamp for deterministic updates")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    issue_packet_arg = cast(str | None, args.issue_packet)
    issue_number_arg = cast(str | None, args.issue_number)
    checkpoint_path = Path(cast(str, args.checkpoint))
    ledger_path = Path(cast(str, args.ledger))
    new_session_request_path = Path(cast(str, args.new_session_request))
    issue_packet_path = Path(issue_packet_arg) if issue_packet_arg else resolve_issue_packet_path(
        cast(str, issue_number_arg),
        base_dir=_infer_artifact_base_dir(ledger_path),
    )
    try:
        result = run_orchestrator_bootstrap(
            issue_packet_path=issue_packet_path,
            checkpoint_path=checkpoint_path,
            ledger_path=ledger_path,
            new_session_request_path=new_session_request_path,
            dispatch_now=cast(bool, args.dispatch_now),
            source_session_id=cast(str, args.source_session_id),
            approval_override_mode=cast(str | None, args.approval_override_mode),
            override_source=cast(str | None, args.override_source),
            human_approval_skipped=cast(bool, args.human_approval_skipped),
            workflow_policy_path=cast(str, args.workflow_policy_path),
            updated_at=cast(str | None, args.updated_at),
        )
    except (ValueError, RuntimeError) as error:
        print(f"ERROR: {error}")
        return 1

    status = str(result.session_result.get("status", "unknown"))
    if status == "success":
        print(f"orchestrator bootstrap: delegated to DB-backed start-issue for issue #{result.issue_number}")
    else:
        print(f"orchestrator bootstrap: DB-backed start recorded {status} for issue #{result.issue_number}")
    print(f"orchestrator bootstrap: next action -> {result.immediate_next_action}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
