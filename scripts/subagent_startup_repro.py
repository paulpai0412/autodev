#!/usr/bin/env python3
"""Launch a root OpenCode session that reproduces real task()-spawned subagent startup."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import re
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.opencode_session_trace import (
    find_latest_child_session_summary,
    read_session_summary,
    session_summary_abort_reason,
)
from scripts.orchestrator_sessions import (
    extract_session_id_from_run_output,
    find_session_id_in_db,
    opencode_db_path,
    read_initial_session_id,
    spawn_detached_opencode_run,
    stream_supports_fileno,
    wait_for_session_id_in_db,
)


TASK_ID_PATTERN = re.compile(r"task_id:\s*(\S+)", re.IGNORECASE)
BACKGROUND_TASK_ID_PATTERN = re.compile(r"(?:background_task_id|Background Task ID):\s*(\S+)", re.IGNORECASE)


def session_outcome(summary: dict[str, object] | None) -> str:
    if not summary:
        return "missing"
    status = str(summary.get("latest_assistant_status") or "")
    message_count = int(cast(int | str, summary.get("message_count") or 0))
    part_count = int(cast(int | str, summary.get("part_count") or 0))
    if session_summary_abort_reason(summary):
        return "aborted"
    if status == "no_assistant_message" and message_count == 0 and part_count == 0:
        return "startup_failed_before_messages"
    if message_count > 0 or part_count > 0:
        return "started"
    return "unknown"


def resolve_opencode_cli() -> str | None:
    cli = shutil.which("opencode")
    if cli:
        return cli
    for candidate in [Path.home() / ".opencode/bin/opencode", Path.home() / ".local/bin/opencode", Path.home() / "bin/opencode"]:
        if candidate.exists():
            return str(candidate)
    return shutil.which("opencode-desktop")


def _format_sleep_seconds(seconds: float) -> str:
    return str(int(seconds)) if seconds.is_integer() else str(seconds)


def _format_timestamp_ms(timestamp_ms: int) -> str:
    if timestamp_ms <= 0:
        return ""
    return datetime.fromtimestamp(timestamp_ms / 1000).astimezone().isoformat(timespec="milliseconds")


def build_repro_prompt(
    *,
    child_subagent_type: str,
    category: str,
    load_skills: list[str],
    description: str,
    child_prompt: str,
    run_in_background: bool = True,
    parent_keepalive_seconds: float = 0.0,
) -> str:
    serialized_skills = json.dumps(load_skills, ensure_ascii=False)
    run_in_background_text = "true" if run_in_background else "false"
    prompt_lines = [
        "Follow these steps exactly in order.",
        "1. Use the task tool exactly once with these arguments:",
        f'   - subagent_type: "{child_subagent_type}"',
        f'   - category: "{category}"',
        f"   - load_skills: {serialized_skills}",
        f'   - description: "{description}"',
        f"   - prompt: {json.dumps(child_prompt, ensure_ascii=False)}",
        f"   - run_in_background: {run_in_background_text}",
    ]
    if not run_in_background:
        prompt_lines.extend(
            [
                "2. Wait for the task tool call to finish in the foreground, then stop.",
                "Do not call any other tools.",
            ]
        )
    elif parent_keepalive_seconds > 0:
        prompt_lines.extend(
            [
                f'2. After the task tool returns, use the bash tool exactly once to run the command "sleep {_format_sleep_seconds(parent_keepalive_seconds)}".',
                "3. After the bash sleep command finishes, stop.",
                "Do not call any tools other than that single task call and that single bash call.",
            ]
        )
    else:
        prompt_lines.extend(
            [
                "2. After the task tool returns, stop immediately.",
                "Do not call any other tools.",
            ]
        )
    return "\n".join(prompt_lines)


def resolve_child_prompt(*, child_prompt: str, child_prompt_file: str = "") -> str:
    if child_prompt_file:
        return Path(child_prompt_file).read_text(encoding="utf-8")
    return child_prompt


def _load_session_parts(session_id: str, *, db_path: Path | None = None) -> list[dict[str, Any]]:
    database_path = db_path or opencode_db_path()
    if not database_path.exists():
        return []
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            "SELECT data FROM part WHERE session_id = ? ORDER BY time_created",
            (session_id,),
        ).fetchall()
    parsed_parts: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(str(row[0]))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            parsed_parts.append(payload)
    return parsed_parts


def extract_task_launch_details(parts: list[dict[str, Any]]) -> dict[str, str]:
    launch_details = {
        "task_id": "",
        "background_task_id": "",
        "task_output": "",
    }
    for part in parts:
        if part.get("type") != "tool" or part.get("tool") != "task":
            continue
        state = part.get("state")
        if not isinstance(state, dict):
            continue
        output = str(state.get("output") or "")
        if not output:
            continue
        launch_details["task_output"] = output
        task_match = TASK_ID_PATTERN.search(output)
        background_match = BACKGROUND_TASK_ID_PATTERN.search(output)
        if task_match:
            launch_details["task_id"] = task_match.group(1)
        if background_match:
            launch_details["background_task_id"] = background_match.group(1)
        if launch_details["task_id"] or launch_details["background_task_id"]:
            break
    return launch_details


def summarize_background_task_observation(parts: list[dict[str, Any]], background_task_id: str) -> dict[str, object]:
    if not background_task_id:
        return {
            "background_task_id": "",
            "status": "missing_background_task_id",
            "matched_part_count": 0,
            "latest_match_excerpt": "",
            "stale_poller_hit": "unknown",
        }

    matched_excerpts: list[str] = []
    status = "launched_only"
    stale_poller_hit = "not_observed"

    for part in parts:
        text = ""
        if part.get("type") == "text":
            text = str(part.get("text") or "")
        elif part.get("type") == "tool":
            state = part.get("state")
            if isinstance(state, dict):
                text = str(state.get("output") or "")
        if background_task_id not in text:
            continue

        matched_excerpts.append(" ".join(text.split())[:400])
        lowered = text.lower()
        if "stale" in lowered and "poller" in lowered:
            stale_poller_hit = "hit"
        elif stale_poller_hit == "not_observed" and "stale" in lowered:
            stale_poller_hit = "mentioned_without_poller"

        if "failed" in lowered or "aborted" in lowered:
            status = "failed"
        elif status != "failed" and ("completed" in lowered or "succeeded" in lowered):
            status = "completed"

    return {
        "background_task_id": background_task_id,
        "status": status,
        "matched_part_count": len(matched_excerpts),
        "latest_match_excerpt": matched_excerpts[-1] if matched_excerpts else "",
        "stale_poller_hit": stale_poller_hit,
    }


def summarize_child_observation(summary: dict[str, object] | None) -> dict[str, object]:
    if summary is None:
        return {
            "session_id": "",
            "latest_assistant_status": "missing",
            "last_update_ms": 0,
            "last_update_iso": "",
            "duration_ms": 0,
        }

    last_update_ms = int(cast(int | str, summary.get("time_updated") or 0))
    return {
        "session_id": str(summary.get("session_id") or ""),
        "latest_assistant_status": str(summary.get("latest_assistant_status") or ""),
        "last_update_ms": last_update_ms,
        "last_update_iso": _format_timestamp_ms(last_update_ms),
        "duration_ms": int(cast(int | str, summary.get("duration_ms") or 0)),
    }


def poll_child_session(
    parent_session_id: str,
    *,
    directory: str,
    title_contains: str,
    timeout_seconds: float,
    interval_seconds: float,
) -> dict[str, object] | None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        summary = find_latest_child_session_summary(
            parent_session_id,
            title_contains=title_contains,
            directory=directory,
        )
        if summary is not None:
            return summary
        time.sleep(interval_seconds)
    return None


def wait_for_child_settle(
    session_id: str,
    *,
    settle_timeout_seconds: float,
    interval_seconds: float,
) -> dict[str, object] | None:
    deadline = time.monotonic() + settle_timeout_seconds
    last_summary: dict[str, object] | None = None
    while time.monotonic() < deadline:
        summary = read_session_summary(session_id)
        if summary is None:
            time.sleep(interval_seconds)
            continue
        last_summary = summary
        status = str(summary.get("latest_assistant_status") or "")
        if status in {"MessageAbortedError", "stop"}:
            return summary
        if int(cast(int | str, summary.get("message_count") or 0)) == 0 and int(cast(int | str, summary.get("part_count") or 0)) == 0:
            time.sleep(interval_seconds)
            continue
        time.sleep(interval_seconds)
    return last_summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", default=".", help="Directory for the root OpenCode run")
    parser.add_argument("--title", help="Explicit title for the root reproduction session")
    parser.add_argument("--root-agent", default="", help="Optional root OpenCode agent name passed to opencode run")
    parser.add_argument("--child-subagent-type", default="general", help="Subagent type passed to the task tool")
    parser.add_argument("--category", default="deep", help="task() category for the child subagent")
    parser.add_argument("--skill", action="append", default=[], help="Child load_skills entry (repeatable)")
    parser.add_argument("--child-description", default="Repro child session", help="Child task description")
    parser.add_argument("--child-title-contains", default="Repro child session", help="Substring used to locate the child session title")
    parser.add_argument(
        "--child-prompt",
        default="Reply with the single word READY and then stop.",
        help="Prompt sent to the child subagent through task()",
    )
    parser.add_argument("--child-prompt-file", default="", help="Optional file whose contents become the child prompt")
    parser.add_argument(
        "--parent-keepalive-seconds",
        type=float,
        default=0.0,
        help="Seconds for the parent session to stay alive after dispatch before stopping",
    )
    parser.add_argument(
        "--run-in-background",
        default="true",
        choices=["true", "false"],
        help="Whether the parent should dispatch the child task in background mode",
    )
    parser.add_argument("--root-timeout", type=float, default=15.0, help="Seconds to wait for the root session id")
    parser.add_argument("--db-timeout", type=float, default=5.0, help="Seconds to wait for DB fallback lookup")
    parser.add_argument("--child-timeout", type=float, default=30.0, help="Seconds to wait for the child session to appear")
    parser.add_argument("--poll-interval", type=float, default=0.5, help="Polling interval when waiting for child session")
    parser.add_argument("--settle-timeout", type=float, default=5.0, help="Seconds to keep polling the child session after it first appears")
    parser.add_argument("--matrix-file", help="Optional JSON file describing multiple child cases to run")
    args = parser.parse_args()

    if args.matrix_file:
        return run_matrix(Path(args.matrix_file), args)

    workdir = Path(args.workdir).resolve()
    cli = resolve_opencode_cli()
    if not cli:
        raise SystemExit("OpenCode CLI not found")

    title = args.title or f"subagent-startup-repro-{uuid4().hex[:10]}"
    prompt = build_repro_prompt(
        child_subagent_type=args.child_subagent_type,
        category=args.category,
        load_skills=list(args.skill),
        description=args.child_description,
        child_prompt=resolve_child_prompt(child_prompt=args.child_prompt, child_prompt_file=args.child_prompt_file),
        run_in_background=args.run_in_background == "true",
        parent_keepalive_seconds=args.parent_keepalive_seconds,
    )
    command = [cli, "run", "--format", "json", "--title", title, prompt]
    if args.root_agent:
        command[4:4] = ["--agent", args.root_agent]

    started_at_ms = int(time.time() * 1000)
    process = spawn_detached_opencode_run(command, workdir=workdir)
    stdout_session_id, stdout_text, stderr_text = read_initial_session_id(
        process,
        timeout_seconds=args.root_timeout,
        extract_session_id=extract_session_id_from_run_output,
        supports_fileno=stream_supports_fileno,
    )
    root_session_id = stdout_session_id or wait_for_session_id_in_db(
        title=title,
        workdir=workdir,
        created_after_ms=started_at_ms,
        timeout_seconds=args.db_timeout,
        find_session_id=find_session_id_in_db,
    )

    if not root_session_id:
        raise SystemExit((stderr_text or stdout_text).strip() or "failed to locate root session id")

    time.sleep(1.0)
    root_summary = read_session_summary(root_session_id)
    root_parts = _load_session_parts(root_session_id)
    launch_details = extract_task_launch_details(root_parts)
    child_summary = poll_child_session(
        root_session_id,
        directory=str(workdir),
        title_contains=args.child_title_contains,
        timeout_seconds=args.child_timeout,
        interval_seconds=args.poll_interval,
    )
    if child_summary is not None:
        settled = wait_for_child_settle(
            str(child_summary.get("session_id") or ""),
            settle_timeout_seconds=args.settle_timeout,
            interval_seconds=args.poll_interval,
        )
        if settled is not None:
            child_summary = settled
    refreshed_root_summary = read_session_summary(root_session_id)
    refreshed_root_parts = _load_session_parts(root_session_id)
    if refreshed_root_summary is not None:
        root_summary = refreshed_root_summary
    root_parts = refreshed_root_parts
    launch_details = extract_task_launch_details(root_parts)

    output = {
        "title": title,
        "workdir": str(workdir),
        "root_agent": args.root_agent,
        "child_subagent_type": args.child_subagent_type,
        "run_in_background": args.run_in_background == "true",
        "parent_keepalive_seconds": args.parent_keepalive_seconds,
        "root_session_id": root_session_id,
        "root_summary": root_summary,
        "root_task_launch": launch_details,
        "child_summary": child_summary,
        "child_abort_reason": session_summary_abort_reason(child_summary),
        "child_outcome": session_outcome(child_summary),
        "child_observation": summarize_child_observation(child_summary),
        "background_task_observation": summarize_background_task_observation(
            root_parts,
            launch_details.get("background_task_id", ""),
        ),
        "stdout_text": stdout_text,
        "stderr_text": stderr_text,
        "process_poll": process.poll(),
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


def run_once(
    *,
    workdir: Path,
    title: str,
    root_agent: str,
    child_subagent_type: str,
    category: str,
    skills: list[str],
    child_description: str,
    child_title_contains: str,
    child_prompt: str,
    child_prompt_file: str,
    run_in_background: bool,
    parent_keepalive_seconds: float,
    root_timeout: float,
    db_timeout: float,
    child_timeout: float,
    poll_interval: float,
    settle_timeout: float,
) -> dict[str, object]:
    cli = resolve_opencode_cli()
    if not cli:
        raise SystemExit("OpenCode CLI not found")
    prompt = build_repro_prompt(
        child_subagent_type=child_subagent_type,
        category=category,
        load_skills=skills,
        description=child_description,
        child_prompt=resolve_child_prompt(child_prompt=child_prompt, child_prompt_file=child_prompt_file),
        run_in_background=run_in_background,
        parent_keepalive_seconds=parent_keepalive_seconds,
    )
    command = [cli, "run", "--format", "json", "--title", title, prompt]
    if root_agent:
        command[4:4] = ["--agent", root_agent]
    started_at_ms = int(time.time() * 1000)
    process = spawn_detached_opencode_run(command, workdir=workdir)
    stdout_session_id, stdout_text, stderr_text = read_initial_session_id(
        process,
        timeout_seconds=root_timeout,
        extract_session_id=extract_session_id_from_run_output,
        supports_fileno=stream_supports_fileno,
    )
    root_session_id = stdout_session_id or wait_for_session_id_in_db(
        title=title,
        workdir=workdir,
        created_after_ms=started_at_ms,
        timeout_seconds=db_timeout,
        find_session_id=find_session_id_in_db,
    )
    if not root_session_id:
        return {
            "title": title,
            "workdir": str(workdir),
            "root_agent": root_agent,
            "child_subagent_type": child_subagent_type,
            "run_in_background": run_in_background,
            "parent_keepalive_seconds": parent_keepalive_seconds,
            "root_session_id": "",
            "root_summary": None,
            "root_task_launch": {"task_id": "", "background_task_id": "", "task_output": ""},
            "child_summary": None,
            "child_abort_reason": "",
            "child_outcome": "root_session_missing",
            "child_observation": summarize_child_observation(None),
            "background_task_observation": summarize_background_task_observation([], ""),
            "stdout_text": stdout_text,
            "stderr_text": stderr_text,
            "process_poll": process.poll(),
        }

    time.sleep(1.0)
    root_summary = read_session_summary(root_session_id)
    root_parts = _load_session_parts(root_session_id)
    launch_details = extract_task_launch_details(root_parts)
    child_summary = poll_child_session(
        root_session_id,
        directory=str(workdir),
        title_contains=child_title_contains,
        timeout_seconds=child_timeout,
        interval_seconds=poll_interval,
    )
    if child_summary is not None:
        settled = wait_for_child_settle(
            str(child_summary.get("session_id") or ""),
            settle_timeout_seconds=settle_timeout,
            interval_seconds=poll_interval,
        )
        if settled is not None:
            child_summary = settled
    refreshed_root_summary = read_session_summary(root_session_id)
    refreshed_root_parts = _load_session_parts(root_session_id)
    if refreshed_root_summary is not None:
        root_summary = refreshed_root_summary
    root_parts = refreshed_root_parts
    launch_details = extract_task_launch_details(root_parts)
    return {
        "title": title,
        "workdir": str(workdir),
        "root_agent": root_agent,
        "child_subagent_type": child_subagent_type,
        "run_in_background": run_in_background,
        "parent_keepalive_seconds": parent_keepalive_seconds,
        "root_session_id": root_session_id,
        "root_summary": root_summary,
        "root_task_launch": launch_details,
        "child_summary": child_summary,
        "child_abort_reason": session_summary_abort_reason(child_summary),
        "child_outcome": session_outcome(child_summary),
        "child_observation": summarize_child_observation(child_summary),
        "background_task_observation": summarize_background_task_observation(
            root_parts,
            launch_details.get("background_task_id", ""),
        ),
        "stdout_text": stdout_text,
        "stderr_text": stderr_text,
        "process_poll": process.poll(),
    }


def run_matrix(matrix_path: Path, args: argparse.Namespace) -> int:
    payload = json.loads(matrix_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"invalid matrix file: {matrix_path}")
    cases = payload.get("cases", [])
    if not isinstance(cases, list):
        raise SystemExit(f"matrix cases must be a list: {matrix_path}")
    workdir = Path(args.workdir).resolve()
    prefix = str(payload.get("titlePrefix") or f"subagent-startup-matrix-{uuid4().hex[:8]}")
    results: list[dict[str, object]] = []
    for index, raw_case in enumerate(cases, start=1):
        if not isinstance(raw_case, dict):
            continue
        case_name = str(raw_case.get("name") or f"case-{index}")
        result = run_once(
            workdir=workdir,
            title=f"{prefix}-{case_name}",
            root_agent=str(raw_case.get("rootAgent") or args.root_agent),
            child_subagent_type=str(raw_case.get("childSubagentType") or args.child_subagent_type),
            category=str(raw_case.get("category") or args.category),
            skills=[str(skill) for skill in raw_case.get("skills", args.skill)],
            child_description=str(raw_case.get("childDescription") or f"Repro child session {case_name}"),
            child_title_contains=str(raw_case.get("childTitleContains") or "Repro child session"),
            child_prompt=str(raw_case.get("childPrompt") or args.child_prompt),
            child_prompt_file=str(raw_case.get("childPromptFile") or args.child_prompt_file),
            run_in_background=bool(raw_case.get("runInBackground", args.run_in_background == "true")),
            parent_keepalive_seconds=float(raw_case.get("parentKeepaliveSeconds") or args.parent_keepalive_seconds),
            root_timeout=float(raw_case.get("rootTimeout") or args.root_timeout),
            db_timeout=float(raw_case.get("dbTimeout") or args.db_timeout),
            child_timeout=float(raw_case.get("childTimeout") or args.child_timeout),
            poll_interval=float(raw_case.get("pollInterval") or args.poll_interval),
            settle_timeout=float(raw_case.get("settleTimeout") or args.settle_timeout),
        )
        result["case"] = raw_case
        results.append(result)
    print(json.dumps({"matrix": str(matrix_path), "results": results}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
