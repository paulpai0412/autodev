#!/usr/bin/env python3
"""DB-backed bootstrap entrypoint for root orchestrator startup."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import cast

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.control_plane_db import read_issue_packet
from scripts.orchestrator_supervisor import start_issue


ROOT = Path(__file__).resolve().parents[1]


@dataclass
class RunnerResult:
    issue_number: str
    branch: str
    immediate_next_action: str
    session_result: dict[str, object]


def _issue_packet_to_json(issue_packet: object) -> dict[str, object]:
    record = cast(object, issue_packet)
    return {
        "issue_number": str(getattr(record, "issue_number")),
        "title": str(getattr(record, "title")),
        "branch": str(getattr(record, "branch")),
        "base_branch": str(getattr(record, "base_branch", "main")),
        "backing_type": str(getattr(record, "backing_type")),
        "prior_handoff": str(getattr(record, "prior_handoff")),
        "labels": list(cast(list[str], getattr(record, "labels"))),
        "parent_reference": str(getattr(record, "parent_reference")),
        "dependencies": list(cast(list[str], getattr(record, "dependencies"))),
        "raw_text": str(getattr(record, "raw_text")),
    }


def _normalize_issue_number(issue_number: str) -> str:
    normalized = issue_number.strip().removeprefix("#")
    if normalized.startswith("issue-"):
        normalized = normalized.removeprefix("issue-")
    if not normalized.isdigit():
        raise ValueError(f"issue number must be numeric, got {issue_number!r}")
    return normalized

def resolve_issue_number(issue_number: str, *, base_dir: Path | None = None) -> str:
    normalized = _normalize_issue_number(issue_number)
    actual_base_dir = base_dir or ROOT
    payload = read_issue_packet(actual_base_dir, normalized)
    if not payload:
        message = f"issue packet not recorded in SQLite for issue #{normalized}. "
        message += "Run issue intake or sync the packet first."
        raise RuntimeError(message)
    return normalized


def run_orchestrator_bootstrap(
    *,
    base_dir: Path,
    issue_number: str,
    source_session_id: str = "orchestrator-bootstrap",
    updated_at: str | None = None,
) -> RunnerResult:
    resolved_issue_number = resolve_issue_number(issue_number, base_dir=base_dir)
    payload = read_issue_packet(base_dir, resolved_issue_number)
    if not payload:
        raise RuntimeError(
            f"issue packet not recorded in SQLite for issue #{resolved_issue_number}. Run intake or sync the packet first."
        )
    session_result = start_issue(
        base_dir=base_dir,
        issue_number=resolved_issue_number,
        source_session_id=source_session_id,
        updated_at=updated_at,
    )

    return RunnerResult(
        issue_number=resolved_issue_number,
        branch=str(payload.get("branch") or ""),
        immediate_next_action="Inspect the DB-backed root session via scripts.orchestrator_supervisor show-session or /autodev-show-session.",
        session_result=cast(dict[str, object], cast(object, dict(session_result))),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--issue-number", required=True, help="GitHub issue number, e.g. 32")
    _ = parser.add_argument(
        "--base-dir",
        default=".",
        help="Consumer project root containing the SQLite control plane",
    )
    _ = parser.add_argument("--source-session-id", default="orchestrator-bootstrap", help="Source session id to record when dispatching immediately")
    _ = parser.add_argument("--updated-at", help="Fixed timestamp for deterministic updates")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    base_dir = Path(cast(str, args.base_dir)).resolve()
    try:
        normalized_issue_number = resolve_issue_number(cast(str, args.issue_number), base_dir=base_dir)
        result = run_orchestrator_bootstrap(
            base_dir=base_dir,
            issue_number=normalized_issue_number,
            source_session_id=cast(str, args.source_session_id),
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
