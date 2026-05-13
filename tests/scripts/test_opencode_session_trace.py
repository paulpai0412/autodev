from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from scripts.opencode_session_trace import (
    find_latest_child_session_summary,
    read_session_summary,
    session_summary_abort_reason,
    session_summary_startup_failure_reason,
)


def _create_trace_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE session (
                id TEXT PRIMARY KEY,
                parent_id TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                directory TEXT NOT NULL DEFAULT '',
                agent TEXT NOT NULL DEFAULT '',
                model TEXT NOT NULL DEFAULT '{}',
                time_created INTEGER NOT NULL DEFAULT 0,
                time_updated INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE message (
                session_id TEXT NOT NULL,
                data TEXT NOT NULL,
                time_created INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE part (
                session_id TEXT NOT NULL,
                data TEXT NOT NULL,
                time_created INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        connection.commit()
    finally:
        connection.close()


def test_read_session_summary_extracts_aborted_assistant_status(tmp_path: Path):
    db_path = tmp_path / "opencode.db"
    _create_trace_db(db_path)
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "INSERT INTO session (id, parent_id, title, directory, agent, model, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "ses-worker-1",
                "ses-root-1",
                "Issue 42 worker (@Sisyphus-Junior subagent)",
                str(tmp_path),
                "Sisyphus-Junior",
                json.dumps({"providerID": "github-copilot", "id": "gpt-5.4", "variant": "medium"}),
                1000,
                1025,
            ),
        )
        connection.execute(
            "INSERT INTO message (session_id, data, time_created) VALUES (?, ?, ?)",
            (
                "ses-worker-1",
                json.dumps({"role": "user", "text": "Do the issue work"}),
                1001,
            ),
        )
        connection.execute(
            "INSERT INTO message (session_id, data, time_created) VALUES (?, ?, ?)",
            (
                "ses-worker-1",
                json.dumps({
                    "role": "assistant",
                    "finish": "",
                    "error": {"name": "MessageAbortedError", "data": {"message": "Aborted"}},
                }),
                1002,
            ),
        )
        connection.execute(
            "INSERT INTO part (session_id, data, time_created) VALUES (?, ?, ?)",
            (
                "ses-worker-1",
                json.dumps({"type": "tool", "tool": "read"}),
                1003,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    summary = read_session_summary("ses-worker-1", db_path=db_path)

    assert summary is not None
    assert summary["latest_assistant_status"] == "MessageAbortedError"
    assert summary["tool_sequence"] == ["read"]
    assert session_summary_abort_reason(summary) == "Aborted"


def test_find_latest_child_session_summary_returns_latest_matching_child(tmp_path: Path):
    db_path = tmp_path / "opencode.db"
    _create_trace_db(db_path)
    connection = sqlite3.connect(db_path)
    try:
        for session_id, created_at in [("ses-worker-old", 1000), ("ses-worker-new", 2000)]:
            connection.execute(
                "INSERT INTO session (id, parent_id, title, directory, agent, model, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    session_id,
                    "ses-root-1",
                    "Issue 42 worker (@Sisyphus-Junior subagent)",
                    str(tmp_path),
                    "Sisyphus-Junior",
                    json.dumps({}),
                    created_at,
                    created_at + 5,
                ),
            )
            connection.execute(
                "INSERT INTO message (session_id, data, time_created) VALUES (?, ?, ?)",
                (session_id, json.dumps({"role": "user", "text": "work"}), created_at + 1),
            )
            connection.execute(
                "INSERT INTO message (session_id, data, time_created) VALUES (?, ?, ?)",
                (session_id, json.dumps({"role": "assistant", "finish": "stop"}), created_at + 2),
            )
        connection.commit()
    finally:
        connection.close()

    summary = find_latest_child_session_summary(
        "ses-root-1",
        title_contains="Issue 42 worker",
        directory=str(tmp_path),
        db_path=db_path,
    )

    assert summary is not None
    assert summary["session_id"] == "ses-worker-new"


def test_session_summary_startup_failure_reason_detects_silent_startup_stop(tmp_path: Path):
    db_path = tmp_path / "opencode.db"
    _create_trace_db(db_path)
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "INSERT INTO session (id, parent_id, title, directory, agent, model, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "ses-worker-startup-fail",
                "ses-root-1",
                "Issue 42 worker (@Sisyphus-Junior subagent)",
                str(tmp_path),
                "Sisyphus-Junior",
                json.dumps({}),
                3000,
                3005,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    summary = read_session_summary("ses-worker-startup-fail", db_path=db_path)

    assert summary is not None
    assert summary["latest_assistant_status"] == "no_assistant_message"
    assert session_summary_abort_reason(summary) == ""
    assert session_summary_startup_failure_reason(summary) == (
        "Child session ses-worker-startup-fail stopped before producing any messages or tool parts"
    )
