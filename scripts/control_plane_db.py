#!/usr/bin/env python3
"""SQLite-backed control-plane storage for autodev orchestrator state."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from scripts import control_plane_repository
from scripts.issue_state_machine import is_known_issue_state, require_transition


CONTROL_PLANE_DB_PATH = Path(".opencode/runtime/control-plane.sqlite3")

ISSUE_COLUMNS: dict[str, str] = {
    "issue_number": "TEXT PRIMARY KEY",
    "title": "TEXT NOT NULL DEFAULT ''",
    "branch": "TEXT NOT NULL DEFAULT ''",
    "state": "TEXT NOT NULL DEFAULT 'ready'",
    "rank_score": "REAL NOT NULL DEFAULT 0",
    "lane": "TEXT NOT NULL DEFAULT 'default'",
    "current_role": "TEXT NOT NULL DEFAULT ''",
    "current_stage": "TEXT NOT NULL DEFAULT ''",
    "current_status": "TEXT NOT NULL DEFAULT ''",
    "current_session_id": "TEXT NOT NULL DEFAULT ''",
    "worktree_path": "TEXT NOT NULL DEFAULT ''",
    "last_history_id": "INTEGER NOT NULL DEFAULT 0",
    "last_command_id": "TEXT NOT NULL DEFAULT ''",
    "last_event_at": "TEXT NOT NULL DEFAULT ''",
    "updated_at": "TEXT NOT NULL DEFAULT ''",
    "claimed_at": "TEXT NOT NULL DEFAULT ''",
    "dispatching_at": "TEXT NOT NULL DEFAULT ''",
    "running_at": "TEXT NOT NULL DEFAULT ''",
    "verifying_at": "TEXT NOT NULL DEFAULT ''",
    "verified_at": "TEXT NOT NULL DEFAULT ''",
    "release_pending_at": "TEXT NOT NULL DEFAULT ''",
    "completed_at": "TEXT NOT NULL DEFAULT ''",
    "failed_at": "TEXT NOT NULL DEFAULT ''",
    "quarantined_at": "TEXT NOT NULL DEFAULT ''",
    "attempts_json": "TEXT NOT NULL DEFAULT '{}'",
    "limits_json": "TEXT NOT NULL DEFAULT '{}'",
    "last_failure_json": "TEXT NOT NULL DEFAULT '{}'",
    "runtime_context_json": "TEXT NOT NULL DEFAULT '{}'",
    "latest_refs_json": "TEXT NOT NULL DEFAULT '{}'",
    "resume_snapshot_json": "TEXT NOT NULL DEFAULT '{}'",
    "automation_flags_json": "TEXT NOT NULL DEFAULT '{}'",
    "artifact_refs_json": "TEXT NOT NULL DEFAULT '{}'",
    "artifact_status_json": "TEXT NOT NULL DEFAULT '{}'",
    "issue_packet_json": "TEXT NOT NULL DEFAULT '{}'",
}

DEVELOPMENT_SLOT_STATES = ("claimed", "dispatching", "running", "verifying")


def canonical_control_plane_base_dir(base_dir: Path) -> Path:
    resolved = base_dir.resolve()
    for candidate in (resolved, *resolved.parents):
        if candidate.name == "runtime" and candidate.parent.name == ".opencode":
            return candidate.parent.parent.resolve()
        parent = candidate.parent
        runtime_dir = parent.parent
        opencode_dir = runtime_dir.parent
        if parent.name == "issue-worktrees" and runtime_dir.name == "runtime" and opencode_dir.name == ".opencode":
            return opencode_dir.parent.resolve()
    return resolved


def control_plane_db_path(base_dir: Path) -> Path:
    canonical_base_dir = canonical_control_plane_base_dir(base_dir)
    return canonical_base_dir / CONTROL_PLANE_DB_PATH


def _connect(base_dir: Path) -> sqlite3.Connection:
    db_path = control_plane_db_path(base_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


@contextmanager
def _connection(base_dir: Path) -> Iterator[sqlite3.Connection]:
    connection = _connect(base_dir)
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


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


def _canonical_issue_packet_is_ready(issue_packet: dict[str, Any]) -> bool:
    issue_number = str(issue_packet.get("issue_number") or "")
    title = str(issue_packet.get("title") or "")
    branch = str(issue_packet.get("branch") or "")
    return bool(issue_number and title and branch)


def _content_hash(text: str) -> str:
    if not text:
        return ""
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


def _normalize_issue_row(row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


def _update_issue_snapshot(
    connection: sqlite3.Connection,
    *,
    issue_number: str,
    updates: dict[str, Any],
) -> None:
    control_plane_repository.update_issue_snapshot(
        connection,
        issue_number=issue_number,
        updates=updates,
    )


def _record_latest_ref(
    connection: sqlite3.Connection,
    *,
    issue_number: str,
    entry_type: str,
    history_id: int,
    created_at: str,
    command_id: str = "",
    session_id: str = "",
    status: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    issue = _read_issue_row(connection, issue_number) or {}
    latest_refs = _json_loads_dict(issue.get("latest_refs_json"))
    ref_payload: dict[str, Any] = {
        "history_id": history_id,
        "created_at": created_at,
        "command_id": command_id,
        "session_id": session_id,
        "status": status,
    }
    if extra:
        ref_payload.update(extra)
    latest_refs[entry_type] = ref_payload
    _update_issue_snapshot(
        connection,
        issue_number=issue_number,
        updates={"latest_refs_json": _json_dumps(latest_refs)},
    )


def _merge_runtime_context(
    existing: dict[str, Any],
    *,
    resume_snapshot: dict[str, Any] | None,
    automation_flags: dict[str, Any] | None,
    artifact_refs: dict[str, Any] | None,
    artifact_status: dict[str, Any] | None,
    runtime_context: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(existing)
    if resume_snapshot is not None:
        merged["resume_snapshot"] = resume_snapshot
    if automation_flags is not None:
        merged["automation_flags"] = automation_flags
    if artifact_refs is not None:
        merged["artifact_refs"] = artifact_refs
    if artifact_status is not None:
        merged["artifact_status"] = artifact_status
    if runtime_context is not None:
        for key, value in runtime_context.items():
            if value is None:
                merged.pop(str(key), None)
            else:
                merged[str(key)] = value
    return merged


def _ensure_base_schema(connection: sqlite3.Connection) -> None:
    issue_columns_sql = ",\n            ".join(f"{column} {definition}" for column, definition in ISSUE_COLUMNS.items())
    connection.execute(f"CREATE TABLE IF NOT EXISTS issues (\n            {issue_columns_sql}\n        )")
    existing_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(issues)").fetchall()
    }
    for column, definition in ISSUE_COLUMNS.items():
        if column in existing_columns:
            continue
        connection.execute(f"ALTER TABLE issues ADD COLUMN {column} {definition}")
    connection.executescript(
        """
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
            body_text TEXT NOT NULL DEFAULT '',
            content_hash TEXT NOT NULL DEFAULT '',
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
    body_text: str = "",
    content_hash: str = "",
    unique_key: str = "",
    session_seq: int = 0,
    update_issue_last_history: bool = True,
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

    try:
        history_id = control_plane_repository.append_history_entry(
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
            payload_json=_json_dumps(payload or {}),
            body_text=body_text,
            content_hash=content_hash,
            unique_key=unique_key,
            session_seq=session_seq,
        )
    except sqlite3.IntegrityError:
        if unique_key:
            existing = connection.execute(
                "SELECT history_id FROM issue_history WHERE unique_key = ?",
                (unique_key,),
            ).fetchone()
            if existing is not None:
                return int(existing["history_id"])
        if session_id and session_seq > 0:
            existing = connection.execute(
                "SELECT history_id FROM issue_history WHERE session_id = ? AND session_seq = ? AND entry_type = ?",
                (session_id, session_seq, entry_type),
            ).fetchone()
            if existing is not None:
                return int(existing["history_id"])
        raise
    if update_issue_last_history:
        _update_issue_last_history_ref(
            connection,
            issue_number=issue_number,
            history_id=history_id,
            created_at=created_at,
        )
    return history_id


def ensure_control_plane_db(base_dir: Path) -> Path:
    db_path = control_plane_db_path(base_dir)
    with _connection(base_dir) as connection:
        _ensure_base_schema(connection)
    return db_path


def describe_control_plane_schema(base_dir: Path) -> dict[str, Any]:
    db_path = ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
        table_rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name ASC"
        ).fetchall()
        tables = [str(row[0]) for row in table_rows]

        def describe_table(table_name: str) -> dict[str, Any]:
            column_rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
            return {
                "columns": [
                    {
                        "name": str(row[1]),
                        "type": str(row[2]),
                        "notNull": bool(row[3]),
                        "default": None if row[4] is None else str(row[4]),
                        "primaryKeyOrdinal": int(row[5]),
                    }
                    for row in column_rows
                ]
            }

        return {
            "dbPath": str(db_path),
            "tables": {table_name: describe_table(table_name) for table_name in tables},
        }


def _read_issue_row(connection: sqlite3.Connection, issue_number: str) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM issues WHERE issue_number = ?",
        (issue_number,),
    ).fetchone()
    return _normalize_issue_row(row)


def read_issue(base_dir: Path, issue_number: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
        return _read_issue_row(connection, issue_number)


def issues_in_states(base_dir: Path, states: list[str]) -> list[dict[str, Any]]:
    ensure_control_plane_db(base_dir)
    if not states:
        return []
    placeholders = ", ".join("?" for _ in states)
    with _connection(base_dir) as connection:
        rows = connection.execute(
            f"SELECT * FROM issues WHERE state IN ({placeholders}) ORDER BY issue_number ASC",
            states,
        ).fetchall()
    return [normalized for row in rows if (normalized := _normalize_issue_row(row)) is not None]


def list_issues(
    base_dir: Path,
    *,
    states: list[str] | None = None,
    require_current_session: bool = False,
) -> list[dict[str, Any]]:
    ensure_control_plane_db(base_dir)
    clauses: list[str] = []
    params: list[str] = []
    if states:
        placeholders = ", ".join("?" for _ in states)
        clauses.append(f"state IN ({placeholders})")
        params.extend(states)
    if require_current_session:
        clauses.append("current_session_id != ''")
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        "SELECT * FROM issues "
        f"{where_clause} "
        "ORDER BY "
        "CASE WHEN last_event_at != '' THEN last_event_at ELSE updated_at END DESC, "
        "issue_number ASC"
    )
    with _connection(base_dir) as connection:
        rows = connection.execute(query, params).fetchall()
    return [normalized for row in rows if (normalized := _normalize_issue_row(row)) is not None]


def issue_rows_with_packets(base_dir: Path) -> list[dict[str, Any]]:
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
        rows = connection.execute(
            "SELECT * FROM issues WHERE issue_packet_json != '{}' ORDER BY issue_number ASC"
        ).fetchall()
    return [normalized for row in rows if (normalized := _normalize_issue_row(row)) is not None]


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
    with _connection(base_dir) as connection:
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
    with _connection(base_dir) as connection:
        _ensure_issue_exists(connection, issue_number=issue_number, updated_at=updated_at)
        connection.execute(
            "UPDATE issues SET rank_score = ?, lane = ?, updated_at = ? WHERE issue_number = ?",
            (rank_score, lane, updated_at, issue_number),
        )
        row = _read_issue_row(connection, issue_number)
    return row or {}


def development_issues(base_dir: Path) -> list[dict[str, Any]]:
    return issues_in_states(base_dir, list(DEVELOPMENT_SLOT_STATES))


def development_slot_occupancy(base_dir: Path) -> int:
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
        return control_plane_repository.count_development_occupancy(
            connection,
            states=DEVELOPMENT_SLOT_STATES,
        )


def available_development_slots(base_dir: Path, capacity: int) -> int:
    return max(0, capacity - development_slot_occupancy(base_dir))


def release_slot_occupancy(base_dir: Path) -> int:
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
        return control_plane_repository.count_release_occupancy(connection)


def available_release_slots(base_dir: Path, capacity: int) -> int:
    return max(0, capacity - release_slot_occupancy(base_dir))


def ready_issues_for_selection(base_dir: Path, *, limit: int | None = None) -> list[dict[str, Any]]:
    ensure_control_plane_db(base_dir)
    if limit is not None and limit <= 0:
        return []
    with _connection(base_dir) as connection:
        rows = connection.execute(
            "SELECT * FROM issues WHERE state = 'ready' AND rank_score >= 0 AND current_session_id = '' ORDER BY rank_score DESC, issue_number ASC"
        ).fetchall()
    ready_rows: list[dict[str, Any]] = []
    for row in rows:
        payload = _normalize_issue_row(row)
        if payload is None:
            continue
        issue_packet = _json_loads_dict(payload.get("issue_packet_json"))
        if not _canonical_issue_packet_is_ready(issue_packet):
            continue
        ready_rows.append(payload)
        if limit is not None and len(ready_rows) >= limit:
            break
    return ready_rows


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
    runtime_context: dict[str, Any] | None = None,
    resume_snapshot: dict[str, Any] | None = None,
    automation_flags: dict[str, Any] | None = None,
    artifact_refs: dict[str, Any] | None = None,
    artifact_status: dict[str, Any] | None = None,
    issue_packet: dict[str, Any] | None = None,
    worktree_path: str | None = None,
) -> dict[str, Any]:
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
        _ensure_issue_exists(connection, issue_number=issue_number, updated_at=updated_at)
        existing = _read_issue_row(connection, issue_number) or {}
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
        merged_runtime_context = _merge_runtime_context(
            _json_loads_dict(existing.get("runtime_context_json")),
            resume_snapshot=resume_snapshot,
            automation_flags=automation_flags,
            artifact_refs=artifact_refs,
            artifact_status=artifact_status,
            runtime_context=runtime_context,
        )
        if (
            runtime_context is not None
            or resume_snapshot is not None
            or automation_flags is not None
            or artifact_refs is not None
            or artifact_status is not None
        ):
            updates["runtime_context_json"] = _json_dumps(merged_runtime_context)
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
            updates["title"] = str(issue_packet.get("title") or existing.get("title") or "")
            updates["branch"] = str(issue_packet.get("branch") or existing.get("branch") or "")
        if worktree_path is not None:
            updates["worktree_path"] = worktree_path
        _update_issue_snapshot(connection, issue_number=issue_number, updates=updates)
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
    body_text: str = "",
    content_hash: str = "",
    unique_key: str = "",
    session_seq: int = 0,
    update_issue_last_history: bool = True,
) -> int:
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
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
            body_text=body_text,
            content_hash=content_hash,
            unique_key=unique_key,
            session_seq=session_seq,
            update_issue_last_history=update_issue_last_history,
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
    with _connection(base_dir) as connection:
        row = connection.execute(query, params).fetchone()
    return dict(row) if row else None


def read_latest_history_entry(
    base_dir: Path,
    *,
    issue_number: str | None = None,
    entry_type: str | None = None,
    entry_types: list[str] | None = None,
) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    clauses: list[str] = []
    params: list[str] = []
    if issue_number is not None:
        clauses.append("issue_number = ?")
        params.append(issue_number)
    if entry_type is not None:
        clauses.append("entry_type = ?")
        params.append(entry_type)
    elif entry_types:
        placeholders = ", ".join("?" for _ in entry_types)
        clauses.append(f"entry_type IN ({placeholders})")
        params.extend(entry_types)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        f"SELECT * FROM issue_history {where_clause} "
        "ORDER BY created_at DESC, history_id DESC LIMIT 1"
    )
    with _connection(base_dir) as connection:
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
    with _connection(base_dir) as connection:
        _ensure_issue_exists(connection, issue_number=issue_number, updated_at=updated_at)
        body_text = str(issue_packet.get("raw_text") or "")
        history_id = _append_history_entry(
            connection,
            issue_number=issue_number,
            entry_type="issue_packet",
            created_at=updated_at,
            status="ingested",
            summary=f"Ingest issue packet for issue #{issue_number} into SQLite control plane.",
            payload=issue_packet,
            body_text=body_text,
            content_hash=_content_hash(body_text),
            unique_key=f"issue-packet:{issue_number}:{updated_at}",
        )
        _record_latest_ref(
            connection,
            issue_number=issue_number,
            entry_type="issue_packet",
            history_id=history_id,
            created_at=updated_at,
            status="ingested",
        )
        _update_issue_snapshot(
            connection,
            issue_number=issue_number,
            updates={
                "issue_packet_json": _json_dumps(issue_packet),
                "title": str(issue_packet.get("title") or ""),
                "branch": str(issue_packet.get("branch") or ""),
                "updated_at": updated_at,
                "last_event_at": updated_at,
            },
        )
        row = _read_issue_row(connection, issue_number)
    return row or {}


def read_issue_packet(base_dir: Path, issue_number: str) -> dict[str, Any]:
    issue = read_issue(base_dir, issue_number) or {}
    return _json_loads_dict(issue.get("issue_packet_json"))


def read_latest_ref(base_dir: Path, issue_number: str, entry_type: str) -> dict[str, Any]:
    issue = read_issue(base_dir, issue_number) or {}
    latest_refs = _json_loads_dict(issue.get("latest_refs_json"))
    ref = latest_refs.get(entry_type)
    return dict(ref) if isinstance(ref, dict) else {}


def read_artifact_fact(base_dir: Path, issue_number: str, entry_type: str) -> dict[str, Any]:
    issue = read_issue(base_dir, issue_number) or {}
    artifact_status = _json_loads_dict(issue.get("artifact_status_json"))
    fact = artifact_status.get(entry_type)
    return dict(fact) if isinstance(fact, dict) else {}


def read_runtime_context(base_dir: Path, issue_number: str) -> dict[str, Any]:
    issue = read_issue(base_dir, issue_number) or {}
    return _json_loads_dict(issue.get("runtime_context_json"))


def read_release_child_session(base_dir: Path, issue_number: str) -> dict[str, Any]:
    runtime_context = read_runtime_context(base_dir, issue_number)
    release_child_session = runtime_context.get("release_child_session")
    return dict(release_child_session) if isinstance(release_child_session, dict) else {}


def record_latest_ref_snapshot(
    base_dir: Path,
    *,
    issue_number: str,
    entry_type: str,
    history_id: int,
    created_at: str,
    command_id: str = "",
    session_id: str = "",
    status: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
        _record_latest_ref(
            connection,
            issue_number=issue_number,
            entry_type=entry_type,
            history_id=history_id,
            created_at=created_at,
            command_id=command_id,
            session_id=session_id,
            status=status,
            extra=extra,
        )


def record_artifact_fact(
    base_dir: Path,
    *,
    issue_number: str,
    entry_type: str,
    created_at: str,
    payload: dict[str, Any],
    summary: str = "",
    session_id: str = "",
    command_id: str = "",
    body_text: str = "",
    source: str = "db",
    artifact_path: str = "",
    unique_key: str = "",
) -> dict[str, Any]:
    ensure_control_plane_db(base_dir)
    artifact_body = body_text or _json_dumps(payload)
    artifact_content_hash = _content_hash(artifact_body)
    dedupe_key = unique_key or f"{entry_type}:{issue_number}:{artifact_content_hash}"
    status = str(payload.get("status") or "")
    with _connection(base_dir) as connection:
        history_id = _append_history_entry(
            connection,
            issue_number=issue_number,
            entry_type=entry_type,
            created_at=created_at,
            session_id=session_id,
            command_id=command_id,
            status=status,
            summary=summary or status or f"Record {entry_type} for issue #{issue_number}.",
            payload=payload,
            body_text=artifact_body,
            content_hash=artifact_content_hash,
            unique_key=dedupe_key,
        )
        _record_latest_ref(
            connection,
            issue_number=issue_number,
            entry_type=entry_type,
            history_id=history_id,
            created_at=created_at,
            command_id=command_id,
            session_id=session_id,
            status=status,
        )
        issue = _read_issue_row(connection, issue_number) or {}
        artifact_status = _json_loads_dict(issue.get("artifact_status_json"))
        snapshot: dict[str, Any] = {
            "observed_at": created_at,
            "parse_ok": True,
            "source": source,
            "history_id": history_id,
            "content_hash": artifact_content_hash,
        }
        if artifact_path:
            snapshot["path"] = artifact_path
        snapshot.update(payload)
        _update_issue_snapshot(
            connection,
            issue_number=issue_number,
            updates={
                "artifact_status_json": _json_dumps({**artifact_status, entry_type: snapshot}),
                "updated_at": created_at,
                "last_event_at": created_at,
            },
        )
    return snapshot


def record_pr_opened(
    base_dir: Path,
    *,
    issue_number: str,
    pr_number: str,
    created_at: str,
    verifier_session_id: str = "",
    command_id: str = "",
    summary: str = "",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_control_plane_db(base_dir)
    pr_payload: dict[str, Any] = {"pr_number": pr_number}
    if payload:
        pr_payload.update(payload)
    body_text = _json_dumps(pr_payload)
    content_hash = _content_hash(body_text)
    dedupe_key = f"pr-opened:{issue_number}:{pr_number}"
    with _connection(base_dir) as connection:
        history_id = _append_history_entry(
            connection,
            issue_number=issue_number,
            entry_type="pr_opened",
            created_at=created_at,
            session_id=verifier_session_id,
            command_id=command_id,
            status="opened",
            summary=summary or f"Record PR #{pr_number} opened for issue #{issue_number}.",
            payload=pr_payload,
            body_text=body_text,
            content_hash=content_hash,
            unique_key=dedupe_key,
        )
        _record_latest_ref(
            connection,
            issue_number=issue_number,
            entry_type="pr_opened",
            history_id=history_id,
            created_at=created_at,
            command_id=command_id,
            session_id=verifier_session_id,
            status="opened",
        )
        _update_issue_snapshot(
            connection,
            issue_number=issue_number,
            updates={
                "updated_at": created_at,
                "last_event_at": created_at,
            },
        )
    return {
        "history_id": history_id,
        "pr_number": pr_number,
        "session_id": verifier_session_id,
        "status": "opened",
        "created_at": created_at,
    }


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
    current_session_id: str | None = None,
) -> dict[str, Any]:
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
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
        if to_state == "verified":
            updates["verified_at"] = updated_at
        if to_state == "release_pending":
            updates["release_pending_at"] = updated_at
        if to_state == "completed":
            updates["completed_at"] = updated_at
        if to_state == "failed":
            updates["failed_at"] = updated_at
        if to_state == "quarantined":
            updates["quarantined_at"] = updated_at
        if current_session_id is not None:
            updates["current_session_id"] = current_session_id

        history_id = _append_history_entry(
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
        _record_latest_ref(
            connection,
            issue_number=issue_number,
            entry_type="state_transition",
            history_id=history_id,
            created_at=updated_at,
            command_id=command_id,
            status=to_state,
        )
        _update_issue_snapshot(connection, issue_number=issue_number, updates=updates)
        updated = _read_issue_row(connection, issue_number)
    return updated or {}


def claim_issue_if_ready(
    base_dir: Path,
    *,
    issue_number: str,
    command_id: str,
    scheduler_id: str,
    reason: str,
    updated_at: str,
) -> dict[str, Any]:
    """Atomically claim an issue only when it is ready and unfenced.

    This helper closes the read-then-write race by using a conditional UPDATE
    (`state='ready' AND current_session_id=''`) inside a single transaction.
    """

    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
        _ensure_issue_exists(connection, issue_number=issue_number, state="ready", updated_at=updated_at)
        history_id = _append_history_entry(
            connection,
            issue_number=issue_number,
            entry_type="state_transition",
            created_at=updated_at,
            command_id=command_id,
            from_state="ready",
            to_state="claimed",
            summary=reason,
            status="claimed",
            payload={"decision_type": "state_transition", "scheduler_id": scheduler_id},
            unique_key=f"state-transition:{command_id}",
        )
        cursor = connection.execute(
            """
            UPDATE issues
               SET state = 'claimed',
                   claimed_at = ?,
                   last_command_id = ?,
                   last_event_at = ?,
                   updated_at = ?
             WHERE issue_number = ?
               AND state = 'ready'
               AND current_session_id = ''
            """,
            (updated_at, command_id, updated_at, updated_at, issue_number),
        )
        if cursor.rowcount != 1:
            current = _read_issue_row(connection, issue_number) or {}
            actual_state = str(current.get("state") or "ready")
            current_session_id = str(current.get("current_session_id") or "")
            if current_session_id:
                raise ValueError(
                    f"issue #{issue_number} still has an active current session fence; refusing duplicate start."
                )
            raise ValueError(
                f"issue #{issue_number} expected state 'ready', found {actual_state!r}; refusing duplicate start."
            )
        _record_latest_ref(
            connection,
            issue_number=issue_number,
            entry_type="state_transition",
            history_id=history_id,
            created_at=updated_at,
            command_id=command_id,
            status="claimed",
        )
        claimed = _read_issue_row(connection, issue_number)
    return claimed or {}


def upsert_issue_state(
    base_dir: Path,
    *,
    issue_number: str,
    state: str,
    command_id: str,
    updated_at: str,
    current_session_id: str | None = None,
) -> dict[str, Any]:
    if not is_known_issue_state(state):
        raise ValueError(f"unknown issue state {state!r}")
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
        _ensure_issue_exists(connection, issue_number=issue_number, state=state, updated_at=updated_at)
        existing = _read_issue_row(connection, issue_number) or {}
        previous_state = str(existing.get("state") or state)
        updates: dict[str, Any] = {
            "state": state,
            "last_command_id": command_id,
            "last_event_at": updated_at,
            "updated_at": updated_at,
        }
        if current_session_id is not None:
            updates["current_session_id"] = current_session_id
        if state == "claimed":
            updates["claimed_at"] = updated_at
        if state == "dispatching":
            updates["dispatching_at"] = updated_at
        if state == "running":
            updates["running_at"] = updated_at
        if state == "verifying":
            updates["verifying_at"] = updated_at
        if state == "verified":
            updates["verified_at"] = updated_at
        if state == "release_pending":
            updates["release_pending_at"] = updated_at
        if state == "completed":
            updates["completed_at"] = updated_at
        if state == "failed":
            updates["failed_at"] = updated_at
        if state == "quarantined":
            updates["quarantined_at"] = updated_at
        if previous_state != state or str(existing.get("last_command_id") or "") != command_id:
            history_id = _append_history_entry(
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
            _record_latest_ref(
                connection,
                issue_number=issue_number,
                entry_type="state_transition",
                history_id=history_id,
                created_at=updated_at,
                command_id=command_id,
                status=state,
            )
        _update_issue_snapshot(connection, issue_number=issue_number, updates=updates)
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
    projection_target: str = "labels",
    projection_payload: dict[str, Any] | None = None,
) -> None:
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
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
        history_id = _append_history_entry(
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
                "projection_target": projection_target,
                "projection_payload": projection_payload if isinstance(projection_payload, dict) else {},
            },
            unique_key=f"github-sync:{command_id}:{attempt_count}",
        )
        _record_latest_ref(
            connection,
            issue_number=issue_number,
            entry_type="github_sync",
            history_id=history_id,
            created_at=updated_at,
            command_id=command_id,
            status=status,
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
    with _connection(base_dir) as connection:
        existing = connection.execute(
            "SELECT history_id FROM issue_history WHERE unique_key = ?",
            (event_id,),
        ).fetchone()
        if existing is not None:
            return
        event_payload = dict(payload)
        event_payload.setdefault("event_type", event_type)
        history_id = _append_history_entry(
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
        _record_latest_ref(
            connection,
            issue_number=issue_number,
            entry_type="root_event",
            history_id=history_id,
            created_at=created_at,
            session_id=root_session_id,
            status=event_type,
        )
        _update_issue_snapshot(
            connection,
            issue_number=issue_number,
            updates={"last_event_at": created_at, "updated_at": created_at},
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
    projection_payload = payload.get("projection_payload", {})
    return {
        "command_id": str(row["command_id"]),
        "issue_number": str(row["issue_number"]),
        "intended_label_delta": _json_dumps(delta if isinstance(delta, dict) else {}),
        "projection_target": str(payload.get("projection_target") or "labels"),
        "projection_payload": _json_dumps(projection_payload if isinstance(projection_payload, dict) else {}),
        "status": str(row["status"]),
        "attempt_count": int(payload.get("attempt_count") or 1),
        "last_error": str(payload.get("last_error") or row["summary"] or ""),
        "updated_at": str(row["created_at"]),
    }


def read_latest_decision(base_dir: Path, issue_number: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connection(base_dir) as connection:
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
    with _connection(base_dir) as connection:
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
    with _connection(base_dir) as connection:
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
    with _connection(base_dir) as connection:
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
    with _connection(base_dir) as connection:
        history_id = _append_history_entry(
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
        _record_latest_ref(
            connection,
            issue_number=issue_number,
            entry_type="admin_action",
            history_id=history_id,
            created_at=updated_at,
            command_id=command_id,
            status=decision_type,
        )
