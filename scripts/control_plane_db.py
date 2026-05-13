#!/usr/bin/env python3
"""SQLite-backed control-plane storage for autodev orchestrator state."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from scripts.issue_state_machine import is_known_issue_state, require_transition


CONTROL_PLANE_DB_PATH = Path(".opencode/runtime/control-plane.sqlite3")

ISSUE_COLUMNS: dict[str, str] = {
    "issue_number": "TEXT PRIMARY KEY",
    "state": "TEXT NOT NULL DEFAULT 'ready'",
    "rank_score": "REAL NOT NULL DEFAULT 0",
    "lane": "TEXT NOT NULL DEFAULT 'default'",
    "current_role": "TEXT NOT NULL DEFAULT ''",
    "current_stage": "TEXT NOT NULL DEFAULT ''",
    "current_status": "TEXT NOT NULL DEFAULT ''",
    "current_root_session_id": "TEXT NOT NULL DEFAULT ''",
    "current_verifier_session_id": "TEXT NOT NULL DEFAULT ''",
    "last_history_id": "INTEGER NOT NULL DEFAULT 0",
    "last_command_id": "TEXT NOT NULL DEFAULT ''",
    "last_event_at": "TEXT NOT NULL DEFAULT ''",
    "updated_at": "TEXT NOT NULL DEFAULT ''",
    "claimed_at": "TEXT NOT NULL DEFAULT ''",
    "dispatching_at": "TEXT NOT NULL DEFAULT ''",
    "running_at": "TEXT NOT NULL DEFAULT ''",
    "verifying_at": "TEXT NOT NULL DEFAULT ''",
    "completed_at": "TEXT NOT NULL DEFAULT ''",
    "failed_at": "TEXT NOT NULL DEFAULT ''",
    "quarantined_at": "TEXT NOT NULL DEFAULT ''",
    "attempts_json": "TEXT NOT NULL DEFAULT '{}'",
    "limits_json": "TEXT NOT NULL DEFAULT '{}'",
    "last_failure_json": "TEXT NOT NULL DEFAULT '{}'",
    "resume_snapshot_json": "TEXT NOT NULL DEFAULT '{}'",
    "automation_flags_json": "TEXT NOT NULL DEFAULT '{}'",
    "artifact_refs_json": "TEXT NOT NULL DEFAULT '{}'",
    "artifact_status_json": "TEXT NOT NULL DEFAULT '{}'",
    "issue_packet_json": "TEXT NOT NULL DEFAULT '{}'",
}


def control_plane_db_path(base_dir: Path) -> Path:
    return base_dir / CONTROL_PLANE_DB_PATH


def _connect(base_dir: Path) -> sqlite3.Connection:
    db_path = control_plane_db_path(base_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _json_loads_dict(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        payload = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _column_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _ensure_issue_columns(connection: sqlite3.Connection) -> None:
    existing = _column_names(connection, "issues")
    for column, definition in ISSUE_COLUMNS.items():
        if column in existing:
            continue
        connection.execute(f"ALTER TABLE issues ADD COLUMN {column} {definition}")


def _ensure_base_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS issues (
            issue_number TEXT PRIMARY KEY,
            state TEXT NOT NULL DEFAULT 'ready'
        );

        CREATE TABLE IF NOT EXISTS issue_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_number TEXT NOT NULL,
            entry_type TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            stage TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            session_id TEXT NOT NULL DEFAULT '',
            request_id TEXT NOT NULL DEFAULT '',
            command_id TEXT NOT NULL DEFAULT '',
            from_state TEXT NOT NULL DEFAULT '',
            to_state TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            unique_key TEXT NOT NULL DEFAULT '',
            session_seq INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS issue_history_issue_idx
        ON issue_history(issue_number, created_at DESC, history_id DESC);

        CREATE INDEX IF NOT EXISTS issue_history_command_idx
        ON issue_history(command_id, created_at DESC, history_id DESC);

        CREATE INDEX IF NOT EXISTS issue_history_entry_type_idx
        ON issue_history(entry_type, created_at DESC, history_id DESC);

        CREATE UNIQUE INDEX IF NOT EXISTS issue_history_unique_key_idx
        ON issue_history(unique_key)
        WHERE unique_key != '';

        CREATE UNIQUE INDEX IF NOT EXISTS issue_history_session_seq_idx
        ON issue_history(session_id, session_seq, entry_type)
        WHERE session_id != '' AND session_seq > 0;
        """
    )
    _ensure_issue_columns(connection)


def _update_issue_last_history_ref(
    connection: sqlite3.Connection,
    *,
    issue_number: str,
    history_id: int,
    created_at: str,
) -> None:
    connection.execute(
        """
        UPDATE issues
        SET last_history_id = ?,
            last_event_at = CASE
                WHEN last_event_at = '' OR last_event_at < ? THEN ?
                ELSE last_event_at
            END,
            updated_at = CASE
                WHEN updated_at = '' OR updated_at < ? THEN ?
                ELSE updated_at
            END
        WHERE issue_number = ?
        """,
        (history_id, created_at, created_at, created_at, created_at, issue_number),
    )


def _ensure_issue_exists(
    connection: sqlite3.Connection,
    *,
    issue_number: str,
    state: str = "ready",
    updated_at: str = "",
) -> None:
    connection.execute(
        """
        INSERT INTO issues (issue_number, state, last_event_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(issue_number) DO NOTHING
        """,
        (issue_number, state, updated_at, updated_at),
    )


def _append_history_entry(
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
    payload: dict[str, Any] | None = None,
    unique_key: str = "",
    session_seq: int = 0,
) -> int:
    _ensure_issue_exists(connection, issue_number=issue_number, updated_at=created_at)
    existing_history_id: int | None = None
    if unique_key:
        existing = connection.execute(
            "SELECT history_id FROM issue_history WHERE unique_key = ?",
            (unique_key,),
        ).fetchone()
        if existing is not None:
            existing_history_id = int(existing["history_id"])
    elif session_id and session_seq > 0:
        existing = connection.execute(
            "SELECT history_id FROM issue_history WHERE session_id = ? AND session_seq = ? AND entry_type = ?",
            (session_id, session_seq, entry_type),
        ).fetchone()
        if existing is not None:
            existing_history_id = int(existing["history_id"])

    if existing_history_id is not None:
        return existing_history_id

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
            created_at,
            unique_key,
            session_seq
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            _json_dumps(payload or {}),
            created_at,
            unique_key,
            session_seq,
        ),
    )
    history_id_raw = cursor.lastrowid
    if history_id_raw is None:
        raise RuntimeError("failed to insert issue_history row")
    history_id = int(history_id_raw)
    _update_issue_last_history_ref(
        connection,
        issue_number=issue_number,
        history_id=history_id,
        created_at=created_at,
    )
    return history_id


def ensure_control_plane_db(base_dir: Path) -> Path:
    db_path = control_plane_db_path(base_dir)
    with _connect(base_dir) as connection:
        _ensure_base_schema(connection)
    return db_path


def _read_issue_row(connection: sqlite3.Connection, issue_number: str) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM issues WHERE issue_number = ?",
        (issue_number,),
    ).fetchone()
    return dict(row) if row else None


def read_issue(base_dir: Path, issue_number: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        return _read_issue_row(connection, issue_number)


def issues_in_states(base_dir: Path, states: list[str]) -> list[dict[str, Any]]:
    ensure_control_plane_db(base_dir)
    if not states:
        return []
    placeholders = ", ".join("?" for _ in states)
    with _connect(base_dir) as connection:
        rows = connection.execute(
            f"SELECT * FROM issues WHERE state IN ({placeholders}) ORDER BY issue_number ASC",
            states,
        ).fetchall()
    return [dict(row) for row in rows]


def issue_rows_with_packets(base_dir: Path) -> list[dict[str, Any]]:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        rows = connection.execute(
            "SELECT * FROM issues WHERE issue_packet_json != '{}' ORDER BY issue_number ASC"
        ).fetchall()
    return [dict(row) for row in rows]


def completed_issue_numbers(base_dir: Path) -> set[str]:
    return {str(row["issue_number"]) for row in issues_in_states(base_dir, ["completed"])}


def ensure_issue_row(
    base_dir: Path,
    *,
    issue_number: str,
    state: str = "ready",
    rank_score: float = 0,
    lane: str = "default",
    updated_at: str,
) -> dict[str, Any]:
    if not is_known_issue_state(state):
        raise ValueError(f"unknown issue state {state!r}")
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        connection.execute(
            """
            INSERT INTO issues (issue_number, state, rank_score, lane, last_event_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(issue_number) DO NOTHING
            """,
            (issue_number, state, rank_score, lane, updated_at, updated_at),
        )
        row = _read_issue_row(connection, issue_number)
    return row or {}


def upsert_issue_ranking(
    base_dir: Path,
    *,
    issue_number: str,
    rank_score: float,
    lane: str,
    updated_at: str,
) -> dict[str, Any]:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        _ensure_issue_exists(connection, issue_number=issue_number, updated_at=updated_at)
        connection.execute(
            "UPDATE issues SET rank_score = ?, lane = ?, updated_at = ? WHERE issue_number = ?",
            (rank_score, lane, updated_at, issue_number),
        )
        row = _read_issue_row(connection, issue_number)
    return row or {}


def ready_issues_for_selection(base_dir: Path) -> list[dict[str, Any]]:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        rows = connection.execute(
            "SELECT * FROM issues WHERE state = 'ready' AND rank_score >= 0 ORDER BY rank_score DESC, issue_number ASC"
        ).fetchall()
    return [dict(row) for row in rows]


def sync_issue_runtime_context(
    base_dir: Path,
    *,
    issue_number: str,
    updated_at: str,
    current_role: str | None = None,
    current_stage: str | None = None,
    current_status: str | None = None,
    attempts: dict[str, Any] | None = None,
    limits: dict[str, Any] | None = None,
    last_failure: dict[str, Any] | None = None,
    resume_snapshot: dict[str, Any] | None = None,
    automation_flags: dict[str, Any] | None = None,
    artifact_refs: dict[str, Any] | None = None,
    artifact_status: dict[str, Any] | None = None,
    issue_packet: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        _ensure_issue_exists(connection, issue_number=issue_number, updated_at=updated_at)
        updates: dict[str, Any] = {"updated_at": updated_at}
        if current_role is not None:
            updates["current_role"] = current_role
        if current_stage is not None:
            updates["current_stage"] = current_stage
        if current_status is not None:
            updates["current_status"] = current_status
        if attempts is not None:
            updates["attempts_json"] = _json_dumps(attempts)
        if limits is not None:
            updates["limits_json"] = _json_dumps(limits)
        if last_failure is not None:
            updates["last_failure_json"] = _json_dumps(last_failure)
        if resume_snapshot is not None:
            updates["resume_snapshot_json"] = _json_dumps(resume_snapshot)
        if automation_flags is not None:
            updates["automation_flags_json"] = _json_dumps(automation_flags)
        if artifact_refs is not None:
            updates["artifact_refs_json"] = _json_dumps(artifact_refs)
        if artifact_status is not None:
            updates["artifact_status_json"] = _json_dumps(artifact_status)
        if issue_packet is not None:
            updates["issue_packet_json"] = _json_dumps(issue_packet)
        assignments = ", ".join(f"{column} = ?" for column in updates)
        connection.execute(
            f"UPDATE issues SET {assignments} WHERE issue_number = ?",
            list(updates.values()) + [issue_number],
        )
        row = _read_issue_row(connection, issue_number)
    return row or {}


def append_issue_history(
    base_dir: Path,
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
    payload: dict[str, Any] | None = None,
    unique_key: str = "",
    session_seq: int = 0,
) -> int:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        return _append_history_entry(
            connection,
            issue_number=issue_number,
            entry_type=entry_type,
            created_at=created_at,
            role=role,
            stage=stage,
            status=status,
            session_id=session_id,
            request_id=request_id,
            command_id=command_id,
            from_state=from_state,
            to_state=to_state,
            summary=summary,
            payload=payload,
            unique_key=unique_key,
            session_seq=session_seq,
        )


def read_latest_issue_history(
    base_dir: Path,
    issue_number: str,
    *,
    entry_type: str | None = None,
    entry_types: list[str] | None = None,
) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    clauses = ["issue_number = ?"]
    params: list[str] = [issue_number]
    if entry_type is not None:
        clauses.append("entry_type = ?")
        params.append(entry_type)
    elif entry_types:
        placeholders = ", ".join("?" for _ in entry_types)
        clauses.append(f"entry_type IN ({placeholders})")
        params.extend(entry_types)
    query = (
        f"SELECT * FROM issue_history WHERE {' AND '.join(clauses)} "
        "ORDER BY created_at DESC, history_id DESC LIMIT 1"
    )
    with _connect(base_dir) as connection:
        row = connection.execute(query, params).fetchone()
    return dict(row) if row else None


def ingest_issue_packet(
    base_dir: Path,
    *,
    issue_number: str,
    issue_packet: dict[str, Any],
    updated_at: str,
) -> dict[str, Any]:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        _ensure_issue_exists(connection, issue_number=issue_number, updated_at=updated_at)
        connection.execute(
            "UPDATE issues SET issue_packet_json = ?, updated_at = ? WHERE issue_number = ?",
            (_json_dumps(issue_packet), updated_at, issue_number),
        )
        _append_history_entry(
            connection,
            issue_number=issue_number,
            entry_type="issue_packet_ingested",
            created_at=updated_at,
            status="ingested",
            summary=f"Ingest issue packet for issue #{issue_number} into SQLite control plane.",
            payload=issue_packet,
            unique_key=f"issue-packet:{issue_number}:{updated_at}",
        )
        row = _read_issue_row(connection, issue_number)
    return row or {}


def read_issue_packet(base_dir: Path, issue_number: str) -> dict[str, Any]:
    issue = read_issue(base_dir, issue_number) or {}
    return _json_loads_dict(issue.get("issue_packet_json"))


def transition_issue_state(
    base_dir: Path,
    *,
    issue_number: str,
    to_state: str,
    command_id: str,
    scheduler_id: str,
    reason: str,
    updated_at: str,
    from_state: str | None = None,
    current_root_session_id: str | None = None,
    current_verifier_session_id: str | None = None,
) -> dict[str, Any]:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        _ensure_issue_exists(connection, issue_number=issue_number, state=from_state or "ready", updated_at=updated_at)
        current = _read_issue_row(connection, issue_number) or {}
        actual_from_state = str(current.get("state") or "ready")
        expected_from_state = from_state or actual_from_state
        if actual_from_state != expected_from_state:
            raise ValueError(
                f"issue #{issue_number} expected state {expected_from_state!r}, found {actual_from_state!r}"
            )
        if str(current.get("last_command_id") or "") == command_id and actual_from_state == to_state:
            return current

        require_transition(actual_from_state, to_state)
        updates: dict[str, Any] = {
            "state": to_state,
            "last_command_id": command_id,
            "last_event_at": updated_at,
            "updated_at": updated_at,
        }
        if to_state == "claimed":
            updates["claimed_at"] = updated_at
        if to_state == "dispatching":
            updates["dispatching_at"] = updated_at
        if to_state == "running":
            updates["running_at"] = updated_at
        if to_state == "verifying":
            updates["verifying_at"] = updated_at
        if to_state == "completed":
            updates["completed_at"] = updated_at
        if to_state == "failed":
            updates["failed_at"] = updated_at
        if to_state == "quarantined":
            updates["quarantined_at"] = updated_at
        if current_root_session_id is not None:
            updates["current_root_session_id"] = current_root_session_id
        if current_verifier_session_id is not None:
            updates["current_verifier_session_id"] = current_verifier_session_id

        assignments = ", ".join(f"{column} = ?" for column in updates)
        connection.execute(
            f"UPDATE issues SET {assignments} WHERE issue_number = ?",
            list(updates.values()) + [issue_number],
        )
        _append_history_entry(
            connection,
            issue_number=issue_number,
            entry_type="state_transition",
            created_at=updated_at,
            command_id=command_id,
            from_state=actual_from_state,
            to_state=to_state,
            summary=reason,
            status=to_state,
            payload={"decision_type": "state_transition", "scheduler_id": scheduler_id},
            unique_key=f"state-transition:{command_id}",
        )
        updated = _read_issue_row(connection, issue_number)
    return updated or {}


def upsert_issue_state(
    base_dir: Path,
    *,
    issue_number: str,
    state: str,
    command_id: str,
    updated_at: str,
    current_root_session_id: str | None = None,
    current_verifier_session_id: str | None = None,
) -> dict[str, Any]:
    if not is_known_issue_state(state):
        raise ValueError(f"unknown issue state {state!r}")
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        _ensure_issue_exists(connection, issue_number=issue_number, state=state, updated_at=updated_at)
        existing = _read_issue_row(connection, issue_number) or {}
        previous_state = str(existing.get("state") or state)
        updates: dict[str, Any] = {
            "state": state,
            "last_command_id": command_id,
            "last_event_at": updated_at,
            "updated_at": updated_at,
        }
        if current_root_session_id is not None:
            updates["current_root_session_id"] = current_root_session_id
        if current_verifier_session_id is not None:
            updates["current_verifier_session_id"] = current_verifier_session_id
        if state == "claimed":
            updates["claimed_at"] = updated_at
        if state == "dispatching":
            updates["dispatching_at"] = updated_at
        if state == "running":
            updates["running_at"] = updated_at
        if state == "verifying":
            updates["verifying_at"] = updated_at
        if state == "completed":
            updates["completed_at"] = updated_at
        if state == "failed":
            updates["failed_at"] = updated_at
        if state == "quarantined":
            updates["quarantined_at"] = updated_at
        assignments = ", ".join(f"{column} = ?" for column in updates)
        connection.execute(
            f"UPDATE issues SET {assignments} WHERE issue_number = ?",
            list(updates.values()) + [issue_number],
        )
        if previous_state != state or str(existing.get("last_command_id") or "") != command_id:
            _append_history_entry(
                connection,
                issue_number=issue_number,
                entry_type="state_transition",
                created_at=updated_at,
                command_id=command_id,
                from_state=previous_state,
                to_state=state,
                summary=f"Upsert issue #{issue_number} state to {state}.",
                status=state,
                payload={"decision_type": "state_transition", "scheduler_id": "upsert"},
                unique_key=f"state-upsert:{command_id}:{issue_number}",
            )
        row = _read_issue_row(connection, issue_number)
    return row or {}


def record_github_sync_attempt(
    base_dir: Path,
    *,
    command_id: str,
    issue_number: str,
    add_labels: list[str],
    remove_labels: list[str],
    status: str,
    updated_at: str,
    last_error: str = "",
) -> None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        previous = connection.execute(
            """
            SELECT payload_json FROM issue_history
            WHERE entry_type = 'github_sync' AND command_id = ?
            ORDER BY created_at DESC, history_id DESC
            LIMIT 1
            """,
            (command_id,),
        ).fetchone()
        previous_payload = _json_loads_dict(previous["payload_json"]) if previous else {}
        attempt_count = int(previous_payload.get("attempt_count") or 0) + 1
        delta = {"add": add_labels, "remove": remove_labels}
        _append_history_entry(
            connection,
            issue_number=issue_number,
            entry_type="github_sync",
            created_at=updated_at,
            status=status,
            command_id=command_id,
            summary=last_error,
            payload={
                "intended_label_delta": delta,
                "attempt_count": attempt_count,
                "last_error": last_error,
            },
            unique_key=f"github-sync:{command_id}:{attempt_count}",
        )


def append_issue_event(
    base_dir: Path,
    *,
    event_id: str,
    issue_number: str,
    root_session_id: str,
    session_seq: int,
    event_type: str,
    payload: dict[str, Any],
    created_at: str,
) -> None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        existing = connection.execute(
            "SELECT history_id FROM issue_history WHERE unique_key = ?",
            (event_id,),
        ).fetchone()
        if existing is not None:
            return
        event_payload = dict(payload)
        event_payload.setdefault("event_type", event_type)
        _append_history_entry(
            connection,
            issue_number=issue_number,
            entry_type="root_event",
            created_at=created_at,
            session_id=root_session_id,
            request_id=event_id,
            status=event_type,
            summary=event_type,
            payload=event_payload,
            unique_key=event_id,
            session_seq=session_seq,
        )
        connection.execute(
            "UPDATE issues SET last_event_at = ?, updated_at = ? WHERE issue_number = ?",
            (created_at, created_at, issue_number),
        )


def _history_row_to_decision(row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = _json_loads_dict(row["payload_json"])
    entry_type = str(row["entry_type"])
    return {
        "command_id": str(row["command_id"]),
        "scheduler_id": str(payload.get("scheduler_id") or ("admin" if entry_type == "admin_action" else "")),
        "issue_number": str(row["issue_number"]),
        "decision_type": str(payload.get("decision_type") or entry_type),
        "from_state": str(row["from_state"]),
        "to_state": str(row["to_state"]),
        "reason": str(row["summary"]),
        "created_at": str(row["created_at"]),
    }


def _history_row_to_github_sync(row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = _json_loads_dict(row["payload_json"])
    delta = payload.get("intended_label_delta", {})
    return {
        "command_id": str(row["command_id"]),
        "issue_number": str(row["issue_number"]),
        "intended_label_delta": _json_dumps(delta if isinstance(delta, dict) else {}),
        "status": str(row["status"]),
        "attempt_count": int(payload.get("attempt_count") or 1),
        "last_error": str(payload.get("last_error") or row["summary"] or ""),
        "updated_at": str(row["created_at"]),
    }


def read_latest_decision(base_dir: Path, issue_number: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        row = connection.execute(
            """
            SELECT * FROM issue_history
            WHERE issue_number = ? AND entry_type IN ('state_transition', 'admin_action')
            ORDER BY created_at DESC, history_id DESC
            LIMIT 1
            """,
            (issue_number,),
        ).fetchone()
    return _history_row_to_decision(row)


def read_latest_github_sync_attempt(base_dir: Path, issue_number: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        row = connection.execute(
            """
            SELECT * FROM issue_history
            WHERE issue_number = ? AND entry_type = 'github_sync'
            ORDER BY created_at DESC, history_id DESC
            LIMIT 1
            """,
            (issue_number,),
        ).fetchone()
    return _history_row_to_github_sync(row)


def read_github_sync_attempt(base_dir: Path, command_id: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        row = connection.execute(
            """
            SELECT * FROM issue_history
            WHERE command_id = ? AND entry_type = 'github_sync'
            ORDER BY created_at DESC, history_id DESC
            LIMIT 1
            """,
            (command_id,),
        ).fetchone()
    return _history_row_to_github_sync(row)


def read_decision(base_dir: Path, command_id: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        row = connection.execute(
            """
            SELECT * FROM issue_history
            WHERE command_id = ? AND entry_type IN ('state_transition', 'admin_action')
            ORDER BY created_at DESC, history_id DESC
            LIMIT 1
            """,
            (command_id,),
        ).fetchone()
    return _history_row_to_decision(row)


def read_github_sync_attempt_by_command_id(base_dir: Path, command_id: str) -> dict[str, Any] | None:
    return read_github_sync_attempt(base_dir, command_id)


def record_admin_decision(
    base_dir: Path,
    *,
    command_id: str,
    issue_number: str,
    decision_type: str,
    reason: str,
    updated_at: str,
    from_state: str = "",
    to_state: str = "",
) -> None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        _append_history_entry(
            connection,
            issue_number=issue_number,
            entry_type="admin_action",
            created_at=updated_at,
            command_id=command_id,
            from_state=from_state,
            to_state=to_state,
            summary=reason,
            payload={"decision_type": decision_type, "scheduler_id": "admin"},
            unique_key=f"admin-action:{command_id}",
        )
