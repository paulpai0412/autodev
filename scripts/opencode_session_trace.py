"""Helpers for inspecting OpenCode session outcomes from the local session DB."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import cast

from scripts.orchestrator_sessions import opencode_db_path, read_session_summary


JsonObject = dict[str, object]


def _int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def session_summary_abort_reason(summary: JsonObject | None) -> str:
    if not summary:
        return ""
    if str(summary.get("latest_assistant_status") or "") != "MessageAbortedError":
        return ""
    error_payload = summary.get("latest_assistant_error")
    if not isinstance(error_payload, dict):
        return "MessageAbortedError"
    data_payload = error_payload.get("data")
    if isinstance(data_payload, dict):
        message = str(data_payload.get("message") or "")
        if message:
            return message
    message = str(error_payload.get("message") or "")
    if message:
        return message
    return str(error_payload.get("name") or "MessageAbortedError")


def session_summary_startup_failure_reason(summary: JsonObject | None) -> str:
    if not summary:
        return ""
    if str(summary.get("latest_assistant_status") or "") != "no_assistant_message":
        return ""
    if _int_value(summary.get("message_count") or 0) != 0:
        return ""
    if _int_value(summary.get("part_count") or 0) != 0:
        return ""
    session_id = str(summary.get("session_id") or "unknown session")
    return f"Child session {session_id} stopped before producing any messages or tool parts"


def find_latest_child_session_summary(
    parent_session_id: str,
    *,
    title_contains: str = "",
    agent: str = "",
    directory: str = "",
    db_path: Path | None = None,
) -> JsonObject | None:
    database_path = db_path or opencode_db_path()
    if not database_path.exists():
        return None

    clauses = ["parent_id = ?"]
    params: list[object] = [parent_session_id]
    if title_contains:
        clauses.append("title LIKE ?")
        params.append(f"%{title_contains}%")
    if agent:
        clauses.append("agent = ?")
        params.append(agent)
    if directory:
        clauses.append("directory = ?")
        params.append(directory)

    query = (
        "SELECT id FROM session WHERE "
        + " AND ".join(clauses)
        + " ORDER BY time_created DESC LIMIT 1"
    )
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(query, params).fetchone()
    if row is None or not row[0]:
        return None
    summary = read_session_summary(str(row[0]), db_path=database_path)
    return cast(JsonObject | None, summary)
