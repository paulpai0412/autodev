#!/usr/bin/env python3
"""Run the orchestrator checkpoint-to-new-session bootstrap for a selected AFK issue."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import json
from typing import cast

from scripts.orchestrator_supervisor import (
    DEFAULT_ROOT_SESSION_AGENT,
    DEFAULT_LEDGER_PATH,
    _infer_artifact_base_dir,
    _dispatch_consumed_request,
    build_orchestrator_request,
    claim_issue_execution,
    create_initial_ledger,
    default_session_result_path_for_request,
    parse_issue_packet_text,
    run_issue_packet_intake,
    write_ledger_file,
    write_session_request,
)
from scripts.orchestrator_compact_payload import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_WORKFLOW_POLICY_PATH,
    derive_compact_payload,
    parse_checkpoint_text,
    write_checkpoint_file,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NEW_SESSION_REQUEST_PATH = ROOT / ".opencode/runtime/new-session-request.json"
DEFAULT_ISSUE_PACKETS_DIR = ROOT / "docs/agents/issue-packets"


@dataclass
class RunnerResult:
    checkpoint_path: Path
    ledger_path: Path
    new_session_request_path: Path
    issue_number: str
    branch: str
    immediate_next_action: str
    new_session_result_path: Path | None = None


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
    updated_at: str | None = None,
) -> RunnerResult:
    issue_packet = parse_issue_packet_text(
        issue_packet_path.read_text(encoding="utf-8"),
        _normalize_issue_packet_ref(issue_packet_path),
    )
    base_dir = _infer_artifact_base_dir(ledger_path)
    claim_issue_execution(
        base_dir=base_dir,
        issue_number=issue_packet.issue_number,
        branch=issue_packet.branch,
        source_session_id=source_session_id,
        updated_at=updated_at,
    )

    _ = write_checkpoint_file(
        checkpoint_path,
        issue_number=issue_packet.issue_number,
        branch=issue_packet.branch,
        role="main_orchestrator",
        agent=DEFAULT_ROOT_SESSION_AGENT,
        issue_packet=issue_packet.issue_packet_path,
        handoff=issue_packet.prior_handoff,
        workflow_policy_path=workflow_policy_path,
        updated_at=updated_at,
    )

    checkpoint_record = parse_checkpoint_text(checkpoint_path.read_text(encoding="utf-8"))
    payload = derive_compact_payload(checkpoint_record, workflow_policy_path=workflow_policy_path)
    ledger = create_initial_ledger(
        issue_packet=issue_packet,
        checkpoint_path=str(checkpoint_path),
        workflow_policy_path=workflow_policy_path,
        root_session_agent=DEFAULT_ROOT_SESSION_AGENT,
        updated_at=updated_at,
    )
    write_ledger_file(ledger_path, ledger)
    request = build_orchestrator_request(ledger)
    write_session_request(new_session_request_path, request)
    new_session_result_path: Path | None = None
    if dispatch_now:
        resolved_session_result_path = default_session_result_path_for_request(new_session_request_path)
        _ = _dispatch_consumed_request(
            new_session_request_path,
            ledger_path=ledger_path,
            session_result_path=resolved_session_result_path,
            source_session_id=source_session_id,
            updated_at=updated_at,
        )
        new_session_result_path = resolved_session_result_path

    return RunnerResult(
        checkpoint_path=checkpoint_path,
        ledger_path=ledger_path,
        new_session_request_path=new_session_request_path,
        issue_number=issue_packet.issue_number,
        branch=issue_packet.branch,
        immediate_next_action=payload["immediate_next_action"],
        new_session_result_path=new_session_result_path,
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
    issue_packet_path = Path(issue_packet_arg) if issue_packet_arg else resolve_issue_packet_path(cast(str, issue_number_arg))
    checkpoint_path = Path(cast(str, args.checkpoint))
    ledger_path = Path(cast(str, args.ledger))
    new_session_request_path = Path(cast(str, args.new_session_request))
    try:
        result = run_orchestrator_bootstrap(
            issue_packet_path=issue_packet_path,
            checkpoint_path=checkpoint_path,
            ledger_path=ledger_path,
            new_session_request_path=new_session_request_path,
            dispatch_now=cast(bool, args.dispatch_now),
            source_session_id=cast(str, args.source_session_id),
            workflow_policy_path=cast(str, args.workflow_policy_path),
            updated_at=cast(str | None, args.updated_at),
        )
    except (ValueError, RuntimeError) as error:
        print(f"ERROR: {error}")
        return 1

    print(f"orchestrator bootstrap: updated checkpoint {result.checkpoint_path}")
    print(f"orchestrator bootstrap: wrote supervisor ledger {result.ledger_path}")
    print(f"orchestrator bootstrap: wrote continuation request {result.new_session_request_path}")
    if result.new_session_result_path is not None:
        session_result = cast(dict[str, object], json.loads(result.new_session_result_path.read_text(encoding="utf-8")))
        status = str(session_result.get("status", "unknown"))
        if status == "success":
            print(f"orchestrator bootstrap: dispatched fresh root session and wrote session result {result.new_session_result_path}")
        else:
            print(f"orchestrator bootstrap: dispatch recorded {status} session result {result.new_session_result_path}")
    print(f"orchestrator bootstrap: next action -> {result.immediate_next_action}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
