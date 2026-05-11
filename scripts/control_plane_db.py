#!/usr/bin/env python3
"""SQLite-backed control-plane storage for autodev orchestrator state."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from scripts.issue_state_machine import is_known_issue_state, require_transition


CONTROL_PLANE_DB_PATH = Path(".opencode/runtime/control-plane.sqlite3")


def control_plane_db_path(base_dir: Path) -> Path:
    return base_dir / CONTROL_PLANE_DB_PATH


def _connect(base_dir: Path) -> sqlite3.Connection:
    db_path = control_plane_db_path(base_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def ensure_control_plane_db(base_dir: Path) -> Path:
    db_path = control_plane_db_path(base_dir)
    with _connect(base_dir) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS scheduler_leases (
                scheduler_id TEXT PRIMARY KEY,
                lease_token TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                replaced_by_scheduler_id TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'active'
            );

            CREATE TABLE IF NOT EXISTS issues (
                issue_number TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                rank_score REAL NOT NULL DEFAULT 0,
                lane TEXT NOT NULL DEFAULT 'default',
                current_root_session_id TEXT NOT NULL DEFAULT '',
                current_verifier_session_id TEXT NOT NULL DEFAULT '',
                last_command_id TEXT NOT NULL DEFAULT '',
                claimed_at TEXT NOT NULL DEFAULT '',
                dispatching_at TEXT NOT NULL DEFAULT '',
                running_at TEXT NOT NULL DEFAULT '',
                verifying_at TEXT NOT NULL DEFAULT '',
                completed_at TEXT NOT NULL DEFAULT '',
                failed_at TEXT NOT NULL DEFAULT '',
                quarantined_at TEXT NOT NULL DEFAULT '',
                last_event_at TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS issue_events (
                event_id TEXT PRIMARY KEY,
                issue_number TEXT NOT NULL,
                root_session_id TEXT NOT NULL,
                session_seq INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(root_session_id, session_seq)
            );

            CREATE TABLE IF NOT EXISTS decision_log (
                command_id TEXT PRIMARY KEY,
                scheduler_id TEXT NOT NULL,
                issue_number TEXT NOT NULL,
                decision_type TEXT NOT NULL,
                from_state TEXT NOT NULL,
                to_state TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS github_sync_attempts (
                command_id TEXT PRIMARY KEY,
                issue_number TEXT NOT NULL,
                intended_label_delta TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 1,
                last_error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            """
        )
    return db_path


def read_issue(base_dir: Path, issue_number: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        row = connection.execute(
            "SELECT * FROM issues WHERE issue_number = ?",
            (issue_number,),
        ).fetchone()
    return dict(row) if row else None


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
            INSERT INTO issues (issue_number, state, rank_score, lane, last_event_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(issue_number) DO NOTHING
            """,
            (issue_number, state, rank_score, lane, updated_at),
        )
        row = connection.execute(
            "SELECT * FROM issues WHERE issue_number = ?",
            (issue_number,),
        ).fetchone()
    return dict(row) if row else {}


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
        existing = connection.execute(
            "SELECT * FROM issues WHERE issue_number = ?",
            (issue_number,),
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO issues (issue_number, state, rank_score, lane, last_event_at)
                VALUES (?, 'ready', ?, ?, ?)
                """,
                (issue_number, rank_score, lane, updated_at),
            )
        else:
            connection.execute(
                "UPDATE issues SET rank_score = ?, lane = ? WHERE issue_number = ?",
                (rank_score, lane, issue_number),
            )
        row = connection.execute(
            "SELECT * FROM issues WHERE issue_number = ?",
            (issue_number,),
        ).fetchone()
    return dict(row) if row else {}


def ready_issues_for_selection(base_dir: Path) -> list[dict[str, Any]]:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        rows = connection.execute(
            "SELECT * FROM issues WHERE state = 'ready' AND rank_score >= 0 ORDER BY rank_score DESC, issue_number ASC"
        ).fetchall()
    return [dict(row) for row in rows]


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
        row = connection.execute(
            "SELECT * FROM issues WHERE issue_number = ?",
            (issue_number,),
        ).fetchone()
        if row is None:
            row = connection.execute(
                """
                INSERT INTO issues (issue_number, state, last_event_at)
                VALUES (?, ?, ?)
                RETURNING *
                """,
                (issue_number, from_state or "ready", updated_at),
            ).fetchone()

        current = dict(row)
        actual_from_state = str(current["state"])
        expected_from_state = from_state or actual_from_state
        if actual_from_state != expected_from_state:
            raise ValueError(
                f"issue #{issue_number} expected state {expected_from_state!r}, found {actual_from_state!r}"
            )
        if current.get("last_command_id") == command_id and actual_from_state == to_state:
            return current

        require_transition(actual_from_state, to_state)
        updates: dict[str, Any] = {
            "state": to_state,
            "last_command_id": command_id,
            "last_event_at": updated_at,
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
        values = list(updates.values()) + [issue_number]
        connection.execute(
            f"UPDATE issues SET {assignments} WHERE issue_number = ?",
            values,
        )
        connection.execute(
            """
            INSERT OR REPLACE INTO decision_log (
                command_id,
                scheduler_id,
                issue_number,
                decision_type,
                from_state,
                to_state,
                reason,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (command_id, scheduler_id, issue_number, "state_transition", actual_from_state, to_state, reason, updated_at),
        )
        updated = connection.execute(
            "SELECT * FROM issues WHERE issue_number = ?",
            (issue_number,),
        ).fetchone()
    return dict(updated) if updated else {}


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
        existing = connection.execute(
            "SELECT * FROM issues WHERE issue_number = ?",
            (issue_number,),
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO issues (
                    issue_number,
                    state,
                    last_command_id,
                    last_event_at,
                    current_root_session_id,
                    current_verifier_session_id
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    issue_number,
                    state,
                    command_id,
                    updated_at,
                    current_root_session_id or "",
                    current_verifier_session_id or "",
                ),
            )
        else:
            updates: dict[str, Any] = {
                "state": state,
                "last_command_id": command_id,
                "last_event_at": updated_at,
            }
            if current_root_session_id is not None:
                updates["current_root_session_id"] = current_root_session_id
            if current_verifier_session_id is not None:
                updates["current_verifier_session_id"] = current_verifier_session_id
            assignments = ", ".join(f"{column} = ?" for column in updates)
            connection.execute(
                f"UPDATE issues SET {assignments} WHERE issue_number = ?",
                list(updates.values()) + [issue_number],
            )
        row = connection.execute(
            "SELECT * FROM issues WHERE issue_number = ?",
            (issue_number,),
        ).fetchone()
    return dict(row) if row else {}


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
    delta = json.dumps(
        {
            "add": add_labels,
            "remove": remove_labels,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    with _connect(base_dir) as connection:
        existing = connection.execute(
            "SELECT attempt_count FROM github_sync_attempts WHERE command_id = ?",
            (command_id,),
        ).fetchone()
        attempt_count = int(existing["attempt_count"]) + 1 if existing else 1
        connection.execute(
            """
            INSERT OR REPLACE INTO github_sync_attempts (
                command_id,
                issue_number,
                intended_label_delta,
                status,
                attempt_count,
                last_error,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (command_id, issue_number, delta, status, attempt_count, last_error, updated_at),
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
        connection.execute(
            """
            INSERT OR IGNORE INTO issue_events (
                event_id,
                issue_number,
                root_session_id,
                session_seq,
                event_type,
                payload_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, issue_number, root_session_id, session_seq, event_type, json.dumps(payload, ensure_ascii=False, sort_keys=True), created_at),
        )
        connection.execute(
            "UPDATE issues SET last_event_at = ? WHERE issue_number = ?",
            (created_at, issue_number),
        )


def read_latest_decision(base_dir: Path, issue_number: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        row = connection.execute(
            "SELECT * FROM decision_log WHERE issue_number = ? ORDER BY created_at DESC, command_id DESC LIMIT 1",
            (issue_number,),
        ).fetchone()
    return dict(row) if row else None


def read_latest_github_sync_attempt(base_dir: Path, issue_number: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        row = connection.execute(
            "SELECT * FROM github_sync_attempts WHERE issue_number = ? ORDER BY updated_at DESC, command_id DESC LIMIT 1",
            (issue_number,),
        ).fetchone()
    return dict(row) if row else None


def read_active_scheduler_lease(base_dir: Path) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        row = connection.execute(
            "SELECT * FROM scheduler_leases WHERE state = 'active' ORDER BY heartbeat_at DESC, scheduler_id DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def read_github_sync_attempt(base_dir: Path, command_id: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        row = connection.execute(
            "SELECT * FROM github_sync_attempts WHERE command_id = ?",
            (command_id,),
        ).fetchone()
    return dict(row) if row else None


def read_decision(base_dir: Path, command_id: str) -> dict[str, Any] | None:
    ensure_control_plane_db(base_dir)
    with _connect(base_dir) as connection:
        row = connection.execute(
            "SELECT * FROM decision_log WHERE command_id = ?",
            (command_id,),
        ).fetchone()
    return dict(row) if row else None


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
        connection.execute(
            """
            INSERT OR REPLACE INTO decision_log (
                command_id,
                scheduler_id,
                issue_number,
                decision_type,
                from_state,
                to_state,
                reason,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (command_id, "admin", issue_number, decision_type, from_state, to_state, reason, updated_at),
        )
