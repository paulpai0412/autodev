from __future__ import annotations

import io
import json
import sqlite3
import subprocess
from pathlib import Path
from typing import Any, cast

import pytest

import scripts.opencode_host_adapter as adapter
from scripts.host_adapter import SessionStartContext


def _create_opencode_db(path: Path) -> None:
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


def _session_context(tmp_path: Path, *, agent: str = "build") -> SessionStartContext:
    return SessionStartContext(
        title="Issue 42 root",
        prompt="Do work.",
        agent=agent,
        workdir=tmp_path,
        source_session_id="ses-source",
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        issue_number="42",
        branch="agent/issue-42-demo",
        started_at_iso="2026-05-15T12:00:00+08:00",
    )


class _FakeProcess:
    def __init__(self, *, stdout: io.StringIO | None = None, stderr: io.StringIO | None = None, poll_result: int | None = None):
        self.stdout = stdout
        self.stderr = stderr
        self._poll_result = poll_result
        self.terminated = False

    def poll(self) -> int | None:
        return self._poll_result

    def terminate(self) -> None:
        self.terminated = True
        self._poll_result = -15


def test_extract_session_id_from_run_output_ignores_non_json_lines() -> None:
    output = '\nhello\n{"type":"status"}\n{"sessionID":"ses-123"}\n'

    assert adapter.extract_session_id_from_run_output(output) == "ses-123"


def test_extract_session_id_from_run_output_raises_when_missing() -> None:
    with pytest.raises(RuntimeError, match="did not emit a sessionID"):
        adapter.extract_session_id_from_run_output('{"type":"status"}')


def test_find_session_id_in_db_returns_latest_match(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "opencode.db"
    _create_opencode_db(db_path)
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "INSERT INTO session (id, title, directory, time_created, time_updated) VALUES (?, ?, ?, ?, ?)",
            ("ses-old", "Issue 42 root", str(tmp_path), 1000, 1000),
        )
        connection.execute(
            "INSERT INTO session (id, title, directory, time_created, time_updated) VALUES (?, ?, ?, ?, ?)",
            ("ses-new", "Issue 42 root", str(tmp_path), 2000, 2000),
        )
        connection.commit()
    finally:
        connection.close()

    monkeypatch.setattr(adapter, "opencode_db_path", lambda: db_path)

    assert adapter.find_session_id_in_db(title="Issue 42 root", workdir=tmp_path, created_after_ms=1500) == "ses-new"


def test_wait_for_session_id_in_db_retries_until_found(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[int] = []
    monotonic_values = iter([0.0, 0.1, 0.2, 0.3])

    def fake_find_session_id(*, title: str, workdir: Path, created_after_ms: int) -> str | None:
        del title, workdir, created_after_ms
        calls.append(1)
        return None if len(calls) < 3 else "ses-found"

    monkeypatch.setattr(adapter.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(adapter.time, "sleep", lambda _seconds: None)

    assert (
        adapter.wait_for_session_id_in_db(
            title="Issue 42 root",
            workdir=tmp_path,
            created_after_ms=123,
            timeout_seconds=1.0,
            find_session_id=fake_find_session_id,
        )
        == "ses-found"
    )


def test_read_initial_session_id_supports_non_fileno_streams() -> None:
    process = _FakeProcess(
        stdout=io.StringIO('{"sessionID":"ses-root"}\n'),
        stderr=io.StringIO("stderr text\n"),
    )

    session_id, stdout_text, stderr_text = adapter.read_initial_session_id(
        cast(Any, process),
        timeout_seconds=1.0,
        extract_session_id=adapter.extract_session_id_from_run_output,
        supports_fileno=lambda _stream: False,
    )

    assert session_id == "ses-root"
    assert '"sessionID":"ses-root"' in stdout_text
    assert stderr_text == "stderr text\n"


def test_read_session_summary_extracts_finish_and_tool_sequence(tmp_path: Path) -> None:
    db_path = tmp_path / "opencode.db"
    _create_opencode_db(db_path)
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "INSERT INTO session (id, parent_id, title, directory, agent, model, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "ses-root",
                "",
                "Issue 42 root",
                str(tmp_path),
                "build",
                json.dumps({"id": "gpt-5.4"}),
                1000,
                1020,
            ),
        )
        connection.execute(
            "INSERT INTO message (session_id, data, time_created) VALUES (?, ?, ?)",
            ("ses-root", json.dumps({"role": "user", "text": "please work"}), 1001),
        )
        connection.execute(
            "INSERT INTO message (session_id, data, time_created) VALUES (?, ?, ?)",
            ("ses-root", json.dumps({"role": "assistant", "finish": "stop"}), 1002),
        )
        connection.execute(
            "INSERT INTO part (session_id, data, time_created) VALUES (?, ?, ?)",
            ("ses-root", json.dumps({"type": "tool", "tool": "task"}), 1003),
        )
        connection.commit()
    finally:
        connection.close()

    summary = adapter.read_session_summary("ses-root", db_path=db_path)

    assert summary is not None
    assert summary["first_user_text"] == "please work"
    assert summary["latest_assistant_status"] == "stop"
    assert summary["tool_sequence"] == ["task"]
    assert summary["duration_ms"] == 20


def test_extract_same_repo_session_read_probe_result_handles_success_and_missing() -> None:
    ok, detail = adapter._extract_same_repo_session_read_probe_result(
        '{"type":"tool_use","part":{"tool":"session_read","state":{"output":"Session: ses-root"}}}'
    )
    missing_ok, missing_detail = adapter._extract_same_repo_session_read_probe_result(
        '{"type":"tool_use","part":{"tool":"session_read","state":{"output":"Session not found: ses-root"}}}'
    )

    assert ok is True
    assert detail == "Session: ses-root"
    assert missing_ok is False
    assert missing_detail == "Session not found: ses-root"


def test_probe_same_repo_session_readability_retries_session_not_found(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    outputs = iter(
        [
            subprocess.CompletedProcess(args=["opencode"], returncode=0, stdout='{"type":"tool_use","part":{"tool":"session_read","state":{"output":"Session not found: ses-root"}}}', stderr=""),
            subprocess.CompletedProcess(args=["opencode"], returncode=0, stdout='{"type":"tool_use","part":{"tool":"session_read","state":{"output":"Session: ses-root"}}}', stderr=""),
        ]
    )

    monkeypatch.setattr(adapter.subprocess, "run", lambda *args, **kwargs: next(outputs))
    monkeypatch.setattr(adapter.time, "sleep", lambda _seconds: None)

    ok, detail = adapter.probe_same_repo_session_readability(
        "/fake/opencode",
        workdir=tmp_path,
        root_session_id="ses-root",
        max_attempts=2,
    )

    assert ok is True
    assert detail == "Session: ses-root"


def test_open_code_host_adapter_start_root_session_returns_degraded_success_when_probe_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    process = _FakeProcess(stdout=io.StringIO(), stderr=io.StringIO(), poll_result=None)
    monkeypatch.setattr(adapter, "spawn_detached_opencode_run", lambda command, workdir: process)
    monkeypatch.setattr(adapter, "read_initial_session_id", lambda *args, **kwargs: ("ses-root", "", ""))
    monkeypatch.setattr(adapter, "probe_same_repo_session_readability", lambda *args, **kwargs: (False, "Session not found: ses-root"))

    host = adapter.OpenCodeHostAdapter(cli_resolver=lambda: "/fake/opencode")
    result = host.start_root_session(_session_context(tmp_path, agent="hephaestus"))

    assert result.status == "success"
    assert result.session_id == "ses-root"
    assert result.readability_status == "degraded_same_repo_probe"
    assert "Warning: same-repo session_read probe failed" in result.resume_hint
    assert process.terminated is False


def test_open_code_host_adapter_sets_retry_flag_for_prefill_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    process = _FakeProcess(stdout=io.StringIO(), stderr=io.StringIO("Bad Request: This model does not support assistant message prefill."), poll_result=None)
    monkeypatch.setattr(adapter, "spawn_detached_opencode_run", lambda command, workdir: process)
    monkeypatch.setattr(adapter, "read_initial_session_id", lambda *args, **kwargs: (None, "", "Bad Request: This model does not support assistant message prefill."))
    monkeypatch.setattr(adapter, "wait_for_session_id_in_db", lambda *args, **kwargs: None)

    host = adapter.OpenCodeHostAdapter(cli_resolver=lambda: "/fake/opencode")
    result = host.start_root_session(_session_context(tmp_path, agent="build"))

    assert result.status == "error"
    assert result.should_retry_without_source_session is True


def test_open_code_host_adapter_start_root_session_success_omits_build_agent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    process = _FakeProcess(stdout=io.StringIO(), stderr=io.StringIO(), poll_result=0)

    def fake_spawn(command: list[str], *, workdir: Path):
        captured["command"] = command
        captured["workdir"] = workdir
        return process

    monkeypatch.setattr(adapter, "spawn_detached_opencode_run", fake_spawn)
    monkeypatch.setattr(adapter, "read_initial_session_id", lambda *args, **kwargs: ("ses-root", "stdout", "stderr"))
    monkeypatch.setattr(adapter, "probe_same_repo_session_readability", lambda *args, **kwargs: (True, "Session: ses-root"))

    host = adapter.OpenCodeHostAdapter(cli_resolver=lambda: "/fake/opencode")
    result = host.start_root_session(_session_context(tmp_path, agent="build"))

    assert result.status == "success"
    assert result.session_id == "ses-root"
    assert result.resume_command == "opencode --session ses-root"
    assert captured["workdir"] == tmp_path
    assert captured["command"] == ["/fake/opencode", "run", "--format", "json", "--title", "Issue 42 root", "Do work."]


def test_open_code_host_adapter_start_child_role_marks_foreground_child_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    process = _FakeProcess(stdout=io.StringIO(), stderr=io.StringIO(), poll_result=0)

    monkeypatch.setattr(adapter, "spawn_detached_opencode_run", lambda command, workdir: process)
    monkeypatch.setattr(adapter, "read_initial_session_id", lambda *args, **kwargs: ("ses-child", "stdout", "stderr"))
    monkeypatch.setattr(adapter, "probe_same_repo_session_readability", lambda *args, **kwargs: (True, "Session: ses-child"))
    monkeypatch.setattr(
        adapter,
        "wait_for_child_session_summary",
        lambda *args, **kwargs: {"session_id": "ses-grandchild", "latest_assistant_status": "stop"},
    )

    host = adapter.OpenCodeHostAdapter(cli_resolver=lambda: "/fake/opencode")
    result = host.start_child_role("release_worker", _session_context(tmp_path, agent="build"))

    assert result.status == "success"
    assert result.session_id == "ses-child"
    assert result.execution_mode == "foreground_child_role"
    assert result.child_role == "release_worker"
    assert result.child_session_id == "ses-grandchild"
    assert result.child_session_status == "stop"
    assert result.metadata["executionMode"] == "foreground_child_role"
    assert result.metadata["childRole"] == "release_worker"
    assert result.metadata["childSessionID"] == "ses-grandchild"
    assert result.metadata["childSessionStatus"] == "stop"


def test_open_code_host_adapter_read_session_outcome_extracts_error_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        adapter,
        "read_session_summary",
        lambda session_id: {
            "session_id": session_id,
            "latest_assistant_status": "MessageAbortedError",
            "time_created": 1000,
            "time_updated": 1010,
            "latest_assistant_error": {"name": "MessageAbortedError", "message": "Aborted"},
        },
    )

    host = adapter.OpenCodeHostAdapter(cli_resolver=lambda: "/fake/opencode")
    outcome = host.read_session_outcome("ses-root")

    assert outcome is not None
    assert outcome.status == "MessageAbortedError"
    assert outcome.error_kind == "MessageAbortedError"
    assert outcome.error == "Aborted"
    assert outcome.resume_hint == "opencode --session ses-root"


def test_open_code_host_adapter_capabilities_include_commands_dir() -> None:
    host = adapter.OpenCodeHostAdapter(cli_resolver=lambda: "/fake/opencode")

    capabilities = host.capabilities()

    assert capabilities["host"] == "opencode"
    assert capabilities["commands_dir"] == str(adapter.default_opencode_commands_dir().expanduser())


def test_resolve_opencode_cli_uses_windows_appdata_fallback_candidates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = tmp_path / "Programs" / "opencode" / "opencode.exe"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("", encoding="utf-8")

    monkeypatch.setattr(adapter.shutil, "which", lambda _name: None)
    monkeypatch.setattr(adapter, "opencode_cli_fallback_candidates", lambda: [candidate])

    resolved = adapter.resolve_opencode_cli()

    assert resolved == str(candidate)
