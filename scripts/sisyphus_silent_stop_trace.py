#!/usr/bin/env python3
"""Standalone repro for Sisyphus-Junior silent-stop startup failures."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.subagent_startup_repro import run_once


DEFAULT_CHILD_PROMPT = """Inspect the current repository and do a real job before stopping.

Required steps:
1. Read package.json.
2. Read src/App.tsx.
3. Read src/App.test.tsx if it exists.
4. Produce a short implementation summary that names the main user-visible behavior.
5. Produce a short testing summary that names at least one existing verification path.

Constraints:
- Use direct read-only tools only.
- Do not call task.
- Do not modify files.
- Do not stop after a single-word reply.
- End with a concise 2-section response titled exactly:
  Summary
  Verification
"""


def diagnose_trace(result: dict[str, object]) -> str:
    outcome = str(result.get("child_outcome") or "")
    launch = cast(dict[str, Any], result.get("root_task_launch") or {})
    session_id = str(cast(dict[str, Any], result.get("child_summary") or {}).get("session_id") or "unknown child session")
    task_id = str(launch.get("task_id") or "")
    background_task_id = str(launch.get("background_task_id") or "")

    if outcome == "startup_failed_before_messages":
        return (
            f"OpenCode launched Sisyphus-Junior ({session_id}) for task {task_id or 'unknown task'}"
            f" / background task {background_task_id or 'unknown background task'}, but the child session never emitted"
            " assistant messages or tool parts. The supervisor must treat this as a startup failure and retry instead"
            " of leaving the issue_worker queued forever."
        )
    if outcome == "aborted":
        return f"Sisyphus-Junior child session {session_id} started and then aborted."
    if outcome == "started":
        return f"Sisyphus-Junior child session {session_id} started normally; no silent stop reproduced in this run."
    if outcome == "missing":
        return "No child session appeared before the timeout expired."
    return f"Observed child outcome: {outcome or 'unknown'}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", default=".", help="Directory for the root OpenCode run")
    parser.add_argument("--category", default="deep", help="task() category used to spawn Sisyphus-Junior")
    parser.add_argument("--skill", action="append", default=[], help="Child load_skills entry (repeatable)")
    parser.add_argument(
        "--child-prompt",
        default=DEFAULT_CHILD_PROMPT,
        help="Prompt sent to the child subagent",
    )
    parser.add_argument("--child-prompt-file", default="", help="Optional file whose contents become the child prompt")
    parser.add_argument("--root-timeout", type=float, default=15.0)
    parser.add_argument("--db-timeout", type=float, default=5.0)
    parser.add_argument("--child-timeout", type=float, default=30.0)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--settle-timeout", type=float, default=5.0)
    args = parser.parse_args()

    workdir = Path(args.workdir).resolve()
    title = f"sisyphus-silent-stop-trace-{uuid4().hex[:10]}"
    result = run_once(
        workdir=workdir,
        title=title,
        category=args.category,
        skills=list(args.skill),
        child_description="Trace Sisyphus-Junior silent stop",
        child_title_contains="Trace Sisyphus-Junior silent stop",
        child_prompt=str(args.child_prompt),
        child_prompt_file=str(args.child_prompt_file),
        root_timeout=float(args.root_timeout),
        db_timeout=float(args.db_timeout),
        child_timeout=float(args.child_timeout),
        poll_interval=float(args.poll_interval),
        settle_timeout=float(args.settle_timeout),
    )
    result["diagnosis"] = diagnose_trace(result)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
