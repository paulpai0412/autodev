"""Internal repository seams for control_plane_db.

This module keeps DB-only runtime truth unchanged while grouping low-level
SQLite operations into clearer seams: snapshot writes, history appends, and
occupancy read models.
"""

from __future__ import annotations

import sqlite3
from typing import Any


def update_issue_snapshot(
    connection: sqlite3.Connection,
    *,
    issue_number: str,
    updates: dict[str, Any],
) -> None:
    if not updates:
        return
    assignments = ", ".join(f"{column} = ?" for column in updates)
    connection.execute(
        f"UPDATE issues SET {assignments} WHERE issue_number = ?",
        list(updates.values()) + [issue_number],
    )


def append_history_entry(
    connection: sqlite3.Connection,
    *,
    issue_number: str,
    entry_type: str,
    created_at: str,
    role: str = "",
    stage: str = "",
    status: str = "",
    session_id: str = "",
    request_id: str = "",
    command_id: str = "",
    from_state: str = "",
    to_state: str = "",
    summary: str = "",
    payload_json: str = "{}",
    body_text: str = "",
    content_hash: str = "",
    unique_key: str = "",
    session_seq: int = 0,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO issue_history (
            issue_number,
            entry_type,
            role,
            stage,
            status,
            session_id,
            request_id,
            command_id,
            from_state,
            to_state,
            summary,
            payload_json,
            body_text,
            content_hash,
            created_at,
            unique_key,
            session_seq
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            issue_number,
            entry_type,
            role,
            stage,
            status,
            session_id,
            request_id,
            command_id,
            from_state,
            to_state,
            summary,
            payload_json,
            body_text,
            content_hash,
            created_at,
            unique_key,
            session_seq,
        ),
    )
    history_id_raw = cursor.lastrowid
    if history_id_raw is None:
        raise RuntimeError("failed to insert issue_history row")
    return int(history_id_raw)


def count_development_occupancy(connection: sqlite3.Connection, *, states: tuple[str, ...]) -> int:
    placeholders = ", ".join("?" for _ in states)
    row = connection.execute(
        f"SELECT COUNT(*) AS c FROM issues WHERE state IN ({placeholders})",
        states,
    ).fetchone()
    return int(row[0]) if row is not None else 0


def count_release_occupancy(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS c FROM issues
        WHERE state = 'release_pending'
          AND current_role = 'main_orchestrator'
          AND current_stage = 'release_root_execution'
          AND (current_status != '' OR current_session_id != '')
        """
    ).fetchone()
    return int(row[0]) if row is not None else 0
