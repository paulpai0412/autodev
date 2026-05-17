from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from pytest import CaptureFixture, MonkeyPatch

import scripts.session_readability_probe as readability_probe
import scripts.sisyphus_silent_stop_trace as sisyphus_trace
import scripts.subagent_silent_stop_probe as silent_stop_probe


class FakeProcess:
    def poll(self) -> int:
        return 0


def _create_opencode_probe_db(db_path: Path, session_id: str) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as connection, connection:
        connection.execute("CREATE TABLE session (id TEXT PRIMARY KEY)")
        connection.execute("CREATE TABLE message (session_id TEXT)")
        connection.execute("CREATE TABLE part (session_id TEXT)")
        connection.execute("CREATE TABLE session_message (session_id TEXT)")
        connection.execute("INSERT INTO session (id) VALUES (?)", (session_id,))
        connection.execute("INSERT INTO message (session_id) VALUES (?)", (session_id,))
        connection.execute("INSERT INTO part (session_id) VALUES (?)", (session_id,))
        connection.execute("INSERT INTO session_message (session_id) VALUES (?)", (session_id,))


def test_poll_session_tables_reports_missing_db(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(readability_probe, "opencode_db_path", lambda: tmp_path / "missing.db")

    result = readability_probe.poll_session_tables("ses_missing", timeout_seconds=0.01, interval_seconds=0.01)

    assert result["db_missing"] is True
    assert result["session_count"] == 0


def test_poll_session_tables_records_all_seen_rows(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    db_path = tmp_path / "opencode.db"
    _create_opencode_probe_db(db_path, "ses_probe")
    monkeypatch.setattr(readability_probe, "opencode_db_path", lambda: db_path)

    result = readability_probe.poll_session_tables("ses_probe", timeout_seconds=0.01, interval_seconds=0.01)

    assert result["session_count"] == 1
    assert result["message_count"] == 1
    assert result["part_count"] == 1
    assert result["session_message_count"] == 1
    assert result["session_row_seen_at_ms"] is not None
    assert result["session_message_row_seen_at_ms"] is not None


def test_session_readability_main_emits_probe_summary(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setattr(readability_probe, "resolve_opencode_cli", lambda: "/bin/opencode")
    monkeypatch.setattr(readability_probe, "spawn_detached_opencode_run", lambda command, *, workdir: FakeProcess())
    monkeypatch.setattr(
        readability_probe,
        "read_initial_session_id",
        lambda process, *, timeout_seconds, extract_session_id, supports_fileno: ("ses_stdout", '{"sessionID":"ses_stdout"}', ""),
    )
    monkeypatch.setattr(readability_probe, "wait_for_session_id_in_db", lambda **kwargs: "ses_stdout")
    monkeypatch.setattr(
        readability_probe,
        "poll_session_tables",
        lambda session_id, *, timeout_seconds, interval_seconds: {"session_count": 1, "session_id": session_id},
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "session_readability_probe.py",
            "--workdir",
            str(tmp_path),
            "--title",
            "probe-title",
            "--agent",
            "build",
        ],
    )

    assert readability_probe.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["title"] == "probe-title"
    assert payload["ids_match"] is True
    assert payload["command"][:6] == ["/bin/opencode", "run", "--format", "json", "--title", "probe-title"]
    assert payload["table_snapshots"]["ses_stdout"]["session_count"] == 1


def test_session_readability_main_requires_opencode_cli(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(readability_probe, "resolve_opencode_cli", lambda: None)
    monkeypatch.setattr("sys.argv", ["session_readability_probe.py"])

    try:
        readability_probe.main()
    except SystemExit as error:
        assert str(error) == "OpenCode CLI not found"
    else:
        raise AssertionError("expected missing CLI to abort")


def test_launch_replay_from_session_reconstructs_command(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    source_summary = {
        "first_user_text": "Investigate issue",
        "agent": "build",
        "model": {"providerID": "openai", "id": "gpt-5.5", "variant": "fast"},
        "error": {"message": "aborted"},
    }
    monkeypatch.setattr(silent_stop_probe, "read_session_summary", lambda session_id: source_summary if session_id == "ses-source" else {"session_id": session_id})
    monkeypatch.setattr(silent_stop_probe, "session_summary_abort_reason", lambda summary: "aborted" if summary else "missing")
    monkeypatch.setattr(silent_stop_probe, "resolve_opencode_cli", lambda: "/bin/opencode")
    monkeypatch.setattr(silent_stop_probe, "spawn_detached_opencode_run", lambda command, *, workdir: FakeProcess())
    monkeypatch.setattr(
        silent_stop_probe,
        "read_initial_session_id",
        lambda process, *, timeout_seconds, extract_session_id, supports_fileno: ("ses-replay", '{"sessionID":"ses-replay"}', ""),
    )
    monkeypatch.setattr(silent_stop_probe.time, "sleep", lambda seconds: None)

    result = silent_stop_probe.launch_replay_from_session(
        "ses-source",
        workdir=tmp_path,
        title="replay-title",
        agent=None,
        model=None,
        variant=None,
        timeout_seconds=1.0,
    )

    assert result["replay_session_id"] == "ses-replay"
    assert result["source_abort_reason"] == "aborted"
    assert result["command"] == [
        "/bin/opencode",
        "run",
        "--format",
        "json",
        "--title",
        "replay-title",
        "--agent",
        "build",
        "--model",
        "openai/gpt-5.5",
        "--variant",
        "fast",
        "Investigate issue",
    ]


def test_launch_replay_from_session_requires_source_prompt(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(silent_stop_probe, "read_session_summary", lambda session_id: {"first_user_text": ""})

    try:
        silent_stop_probe.launch_replay_from_session(
            "ses-source",
            workdir=tmp_path,
            title=None,
            agent=None,
            model=None,
            variant=None,
            timeout_seconds=1.0,
        )
    except SystemExit as error:
        assert "has no user prompt text" in str(error)
    else:
        raise AssertionError("expected missing source prompt to abort")


def test_subagent_silent_stop_main_inspects_and_replays(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setattr(silent_stop_probe, "read_session_summary", lambda session_id: {"session_id": session_id})
    monkeypatch.setattr(
        silent_stop_probe,
        "find_latest_child_session_summary",
        lambda parent_session_id, *, title_contains, agent, directory: {"parent": parent_session_id, "title_contains": title_contains, "agent": agent, "directory": directory},
    )
    monkeypatch.setattr(silent_stop_probe, "launch_replay_from_session", lambda *args, **kwargs: {"replay_session_id": "ses-replay"})
    monkeypatch.setattr(
        "sys.argv",
        [
            "subagent_silent_stop_probe.py",
            "--session-id",
            "ses-one",
            "--parent-session-id",
            "ses-parent",
            "--title-contains",
            "worker",
            "--agent",
            "build",
            "--directory",
            str(tmp_path),
            "--replay-session-id",
            "ses-source",
            "--workdir",
            str(tmp_path),
        ],
    )

    assert silent_stop_probe.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["inspected_sessions"] == [{"session_id": "ses-one"}]
    assert payload["latest_child"]["parent"] == "ses-parent"
    assert payload["replay"]["replay_session_id"] == "ses-replay"


def test_diagnose_trace_explains_known_child_outcomes() -> None:
    startup_failed = sisyphus_trace.diagnose_trace(
        {
            "child_outcome": "startup_failed_before_messages",
            "root_task_launch": {"task_id": "task-1", "background_task_id": "bg-1"},
            "child_summary": {"session_id": "ses-child"},
        }
    )
    assert "startup failure" in startup_failed
    assert "ses-child" in startup_failed
    assert "aborted" in sisyphus_trace.diagnose_trace({"child_outcome": "aborted", "child_summary": {"session_id": "ses-child"}})
    assert "started normally" in sisyphus_trace.diagnose_trace({"child_outcome": "started", "child_summary": {"session_id": "ses-child"}})
    assert "No child session appeared" in sisyphus_trace.diagnose_trace({"child_outcome": "missing"})
    assert "unknown" in sisyphus_trace.diagnose_trace({"child_outcome": ""})


def test_sisyphus_trace_main_adds_diagnosis(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        sisyphus_trace,
        "run_once",
        lambda **kwargs: {
            "child_outcome": "started",
            "child_summary": {"session_id": "ses-child"},
            "kwargs": {key: str(value) if isinstance(value, Path) else value for key, value in kwargs.items()},
        },
    )
    child_prompt_file = tmp_path / "prompt.txt"
    child_prompt_file.write_text("child prompt", encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        [
            "sisyphus_silent_stop_trace.py",
            "--workdir",
            str(tmp_path),
            "--category",
            "deep",
            "--skill",
            "karpathy-guidelines",
            "--child-prompt-file",
            str(child_prompt_file),
        ],
    )

    assert sisyphus_trace.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["diagnosis"] == "Sisyphus-Junior child session ses-child started normally; no silent stop reproduced in this run."
    assert payload["kwargs"]["skills"] == ["karpathy-guidelines"]
