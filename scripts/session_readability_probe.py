#!/usr/bin/env python3
"""Empirically probe when a newly launched OpenCode session becomes readable."""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from uuid import uuid4

from scripts.opencode_host_adapter import (
    extract_session_id_from_run_output,
    find_session_id_in_db,
    opencode_db_path,
    read_initial_session_id,
    spawn_detached_opencode_run,
    stream_supports_fileno,
    wait_for_session_id_in_db,
)
from scripts.runtime_exec import opencode_cli_fallback_candidates


def resolve_opencode_cli() -> str | None:
    cli = shutil.which("opencode")
    if cli:
        return cli

    for candidate in opencode_cli_fallback_candidates():
        if candidate.exists():
            return str(candidate)

    opencode_exe = shutil.which("opencode.exe")
    if opencode_exe:
        return opencode_exe

    return shutil.which("opencode-desktop")


def poll_session_tables(session_id: str, *, timeout_seconds: float, interval_seconds: float) -> dict[str, object]:
    db_path = opencode_db_path()
    result: dict[str, object] = {
        "db_path": str(db_path),
        "session_row_seen_at_ms": None,
        "message_row_seen_at_ms": None,
        "part_row_seen_at_ms": None,
        "session_message_row_seen_at_ms": None,
        "session_count": 0,
        "message_count": 0,
        "part_count": 0,
        "session_message_count": 0,
    }
    if not db_path.exists():
        result["db_missing"] = True
        return result

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        now_ms = int(time.time() * 1000)
        with closing(sqlite3.connect(db_path)) as conn:
            session_count = conn.execute("SELECT COUNT(*) FROM session WHERE id = ?", (session_id,)).fetchone()[0]
            message_count = conn.execute("SELECT COUNT(*) FROM message WHERE session_id = ?", (session_id,)).fetchone()[0]
            part_count = conn.execute("SELECT COUNT(*) FROM part WHERE session_id = ?", (session_id,)).fetchone()[0]
            session_message_count = conn.execute(
                "SELECT COUNT(*) FROM session_message WHERE session_id = ?",
                (session_id,),
            ).fetchone()[0]

        result["session_count"] = session_count
        result["message_count"] = message_count
        result["part_count"] = part_count
        result["session_message_count"] = session_message_count

        if session_count and result["session_row_seen_at_ms"] is None:
            result["session_row_seen_at_ms"] = now_ms
        if message_count and result["message_row_seen_at_ms"] is None:
            result["message_row_seen_at_ms"] = now_ms
        if part_count and result["part_row_seen_at_ms"] is None:
            result["part_row_seen_at_ms"] = now_ms
        if session_message_count and result["session_message_row_seen_at_ms"] is None:
            result["session_message_row_seen_at_ms"] = now_ms

        if session_count and message_count and part_count and session_message_count:
            break
        time.sleep(interval_seconds)

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", default=".", help="Directory to launch the new session in")
    parser.add_argument("--title", help="Explicit session title (defaults to a unique probe title)")
    parser.add_argument(
        "--prompt",
        default="Reply with the single word READY and then stop.",
        help="Prompt to send to opencode run",
    )
    parser.add_argument("--agent", help="Optional explicit agent name")
    parser.add_argument("--initial-timeout", type=float, default=10.0, help="Seconds to wait for stdout sessionID")
    parser.add_argument("--db-timeout", type=float, default=5.0, help="Seconds to wait for DB fallback lookup")
    parser.add_argument("--poll-timeout", type=float, default=10.0, help="Seconds to poll DB materialization")
    parser.add_argument("--poll-interval", type=float, default=0.2, help="Seconds between DB polls")
    args = parser.parse_args()

    workdir = Path(args.workdir).resolve()
    title = args.title or f"session-readability-probe-{uuid4().hex[:10]}"
    cli = resolve_opencode_cli()
    if not cli:
        raise SystemExit("OpenCode CLI not found")

    command = [cli, "run", "--format", "json", "--title", title]
    if args.agent:
        command.extend(["--agent", args.agent])
    command.append(args.prompt)

    started_at_ms = int(time.time() * 1000)
    process = spawn_detached_opencode_run(command, workdir=workdir)
    stdout_session_id, stdout_text, stderr_text = read_initial_session_id(
        process,
        timeout_seconds=args.initial_timeout,
        extract_session_id=extract_session_id_from_run_output,
        supports_fileno=stream_supports_fileno,
    )
    db_session_id = wait_for_session_id_in_db(
        title=title,
        workdir=workdir,
        created_after_ms=started_at_ms,
        timeout_seconds=args.db_timeout,
        find_session_id=find_session_id_in_db,
    )

    session_ids = [candidate for candidate in [stdout_session_id, db_session_id] if candidate]
    unique_session_ids = sorted(set(session_ids))
    table_snapshots = {
        session_id: poll_session_tables(
            session_id,
            timeout_seconds=args.poll_timeout,
            interval_seconds=args.poll_interval,
        )
        for session_id in unique_session_ids
    }

    output = {
        "title": title,
        "workdir": str(workdir),
        "started_at_ms": started_at_ms,
        "command": command,
        "stdout_session_id": stdout_session_id,
        "db_session_id": db_session_id,
        "ids_match": bool(stdout_session_id and db_session_id and stdout_session_id == db_session_id),
        "stdout_text": stdout_text,
        "stderr_text": stderr_text,
        "process_poll": process.poll(),
        "table_snapshots": table_snapshots,
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
