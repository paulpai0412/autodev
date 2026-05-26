from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from scripts.runtime_exec import default_opencode_data_home


def opencode_db_path() -> Path:
    data_home = default_opencode_data_home()
    return data_home / "opencode" / "opencode.db"


def _parse_json(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _load_messages(connection: sqlite3.Connection, session_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT data FROM message WHERE session_id = ? ORDER BY time_created",
        (session_id,),
    ).fetchall()
    return [_parse_json(str(row[0])) for row in rows]


def _load_parts(connection: sqlite3.Connection, session_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT data FROM part WHERE session_id = ? ORDER BY time_created",
        (session_id,),
    ).fetchall()
    return [_parse_json(str(row[0])) for row in rows]


def read_session_summary(session_id: str, *, db_path: Path | None = None) -> dict[str, object] | None:
    database_path = db_path or opencode_db_path()
    if not database_path.exists():
        return None
    with closing(sqlite3.connect(database_path)) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT * FROM session WHERE id = ?", (session_id,)).fetchone()
        if row is None:
            return None
        session_row = dict(row)
        messages = _load_messages(connection, session_id)
        parts = _load_parts(connection, session_id)
    latest_assistant: dict[str, Any] | None = None
    first_user_text = ""
    for message in messages:
        role = message.get("role")
        if role == "user" and not first_user_text:
            first_user_text = str(message.get("text") or "")
        if role == "assistant":
            latest_assistant = message
    latest_assistant_status = "no_assistant_message"
    latest_assistant_error: dict[str, object] = {}
    latest_assistant_finish = ""
    latest_assistant_tools: list[str] = []
    if latest_assistant is not None:
        latest_assistant_finish = str(latest_assistant.get("finish") or "")
        error_payload = latest_assistant.get("error")
        if isinstance(error_payload, dict):
            latest_assistant_error = dict(error_payload)
        if latest_assistant_error:
            latest_assistant_status = str(latest_assistant_error.get("name") or "error")
        elif latest_assistant_finish:
            latest_assistant_status = latest_assistant_finish
        else:
            latest_assistant_status = "unknown"
    for part in parts:
        if part.get("type") == "tool":
            tool_name = str(part.get("tool") or "")
            if tool_name:
                latest_assistant_tools.append(tool_name)
    model_payload = _parse_json(str(session_row.get("model") or ""))
    created_at = int(session_row.get("time_created") or 0)
    updated_at = int(session_row.get("time_updated") or 0)
    return {
        "session_id": str(session_row.get("id") or ""),
        "parent_id": str(session_row.get("parent_id") or ""),
        "title": str(session_row.get("title") or ""),
        "directory": str(session_row.get("directory") or ""),
        "agent": str(session_row.get("agent") or ""),
        "model": model_payload,
        "time_created": created_at,
        "time_updated": updated_at,
        "duration_ms": max(0, updated_at - created_at),
        "message_count": len(messages),
        "part_count": len(parts),
        "first_user_text": first_user_text,
        "latest_assistant_status": latest_assistant_status,
        "latest_assistant_finish": latest_assistant_finish,
        "latest_assistant_error": latest_assistant_error,
        "tool_sequence": latest_assistant_tools,
    }
