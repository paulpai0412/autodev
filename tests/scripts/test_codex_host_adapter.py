from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import scripts.codex_host_adapter as adapter
from scripts.host_adapter import SessionStartContext


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


def test_extract_thread_id_from_exec_events_reads_thread_started() -> None:
    events = [{"type": "thread.started", "thread_id": "thread-123"}]  # type: list[dict[str, str | object]]

    assert adapter.extract_thread_id_from_exec_events(events) == "thread-123"


def test_extract_thread_id_from_exec_events_raises_when_missing() -> None:
    with pytest.raises(RuntimeError, match="did not emit"):
        adapter.extract_thread_id_from_exec_events([{"type": "turn.started"}])


def test_start_root_session_returns_error_when_codex_cli_missing(tmp_path: Path) -> None:
    host = adapter.CodexHostAdapter(cli_resolver=lambda: None)

    result = host.start_root_session(_session_context(tmp_path))

    assert result.status == "error"
    assert "Codex CLI not found" in result.error


def test_start_root_session_success_reads_thread_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_stream(command: list[str], *, workdir: Path) -> tuple[list[dict[str, str | object]], str]:
        captured["command"] = command
        captured["workdir"] = workdir
        return ([{"type": "thread.started", "thread_id": "thread-ok"}, {"type": "turn.completed"}], "")

    monkeypatch.setattr(
        adapter,
        "_stream_json_lines",
        fake_stream,
    )

    host = adapter.CodexHostAdapter(cli_resolver=lambda: "/fake/codex")
    result = host.start_root_session(_session_context(tmp_path))

    assert result.status == "success"
    assert result.session_id == "thread-ok"
    assert result.resume_command == "codex exec resume thread-ok"
    assert result.tui_resume_command == "codex resume"
    assert result.readability_status == "verified_json_event_stream"
    command = captured["command"]
    assert isinstance(command, list)
    assert "--sandbox" in command
    assert "workspace-write" in command


def test_start_root_session_adds_runtime_dir_when_present(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    runtime_dir = tmp_path / ".opencode" / "runtime"
    runtime_dir.mkdir(parents=True)
    workdir = runtime_dir / "issue-worktrees" / "issue-64"
    workdir.mkdir(parents=True)

    captured: dict[str, object] = {}

    def fake_stream(command: list[str], *, workdir: Path) -> tuple[list[dict[str, str | object]], str]:
        captured["command"] = command
        captured["workdir"] = workdir
        return ([{"type": "thread.started", "thread_id": "thread-add-dir"}, {"type": "turn.completed"}], "")

    monkeypatch.setattr(adapter, "_stream_json_lines", fake_stream)
    host = adapter.CodexHostAdapter(cli_resolver=lambda: "/fake/codex")
    context = _session_context(workdir)

    result = host.start_root_session(context)

    assert result.status == "success"
    command = captured["command"]
    assert isinstance(command, list)
    assert "--add-dir" in command
    add_dir_index = command.index("--add-dir")
    assert command[add_dir_index + 1] == str(runtime_dir)


def test_start_root_session_can_bypass_sandbox_via_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_stream(command: list[str], *, workdir: Path) -> tuple[list[dict[str, str | object]], str]:
        captured["command"] = command
        captured["workdir"] = workdir
        return ([{"type": "thread.started", "thread_id": "thread-bypass"}, {"type": "turn.completed"}], "")

    monkeypatch.setattr(adapter, "_stream_json_lines", fake_stream)
    monkeypatch.setenv("AUTODEV_CODEX_BYPASS_SANDBOX", "1")
    host = adapter.CodexHostAdapter(cli_resolver=lambda: "/fake/codex")

    result = host.start_root_session(_session_context(tmp_path))

    assert result.status == "success"
    command = captured["command"]
    assert isinstance(command, list)
    assert "--dangerously-bypass-approvals-and-sandbox" in command
    assert "--sandbox" not in command


def test_start_child_role_marks_foreground_child_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def fake_stream(command: list[str], *, workdir: Path) -> tuple[list[dict[str, str | object]], str]:
        del command, workdir
        return ([{"type": "thread.started", "thread_id": "thread-child"}, {"type": "turn.completed"}], "")

    monkeypatch.setattr(
        adapter,
        "_stream_json_lines",
        fake_stream,
    )

    host = adapter.CodexHostAdapter(cli_resolver=lambda: "/fake/codex")
    result = host.start_child_role("release_worker", _session_context(tmp_path))

    assert result.status == "success"
    assert result.execution_mode == "foreground_child_role"
    assert result.child_role == "release_worker"
    assert result.child_session_id == "thread-child"


def test_read_session_outcome_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_events(session_id: str) -> list[dict[str, str | object]]:
        return [
            {"type": "thread.started", "thread_id": session_id},
            {"type": "item.completed", "item": {"type": "agent_message", "text": "Done"}},
            {"type": "turn.completed", "usage": {"input_tokens": 1}},
        ]

    monkeypatch.setattr(
        adapter,
        "load_latest_session_events",
        fake_events,
    )
    host = adapter.CodexHostAdapter(cli_resolver=lambda: "/fake/codex")

    outcome = host.read_session_outcome("thread-ok")

    assert outcome is not None
    assert outcome.status == "completed"
    assert outcome.resume_hint == "codex exec resume thread-ok"
    assert outcome.metadata["latestAgentMessage"] == "Done"


def test_read_session_outcome_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_events(session_id: str) -> list[dict[str, str | object]]:
        return [
            {"type": "thread.started", "thread_id": session_id},
            {"type": "turn.failed", "error": {"kind": "network", "message": "boom"}},
        ]

    monkeypatch.setattr(
        adapter,
        "load_latest_session_events",
        fake_events,
    )
    host = adapter.CodexHostAdapter(cli_resolver=lambda: "/fake/codex")

    outcome = host.read_session_outcome("thread-fail")

    assert outcome is not None
    assert outcome.status == "failed"
    assert outcome.error_kind == "network"
    assert outcome.error == "boom"


def test_load_latest_session_events_reads_codex_jsonl(tmp_path: Path) -> None:
    sessions_dir = tmp_path / "sessions"
    log_file = sessions_dir / "2026" / "05" / "26" / "rollout-2026-05-26T00-00-00-thread-abc.jsonl"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    payload: list[dict[str, Any]] = [
        {"type": "event_msg", "payload": {"type": "turn.started"}},
        {"type": "event_msg", "payload": {"type": "turn.completed"}},
    ]
    log_file.write_text("\n".join(json.dumps(item) for item in payload) + "\n", encoding="utf-8")

    events = adapter.load_latest_session_events("thread-abc", sessions_dir=sessions_dir)

    assert events == [{"type": "turn.started"}, {"type": "turn.completed"}]


def test_capabilities_include_commands_dir() -> None:
    host = adapter.CodexHostAdapter(cli_resolver=lambda: "/fake/codex")

    capabilities = host.capabilities()

    assert capabilities["host"] == "codex"
    assert capabilities["commands_dir"] == str(adapter.default_codex_commands_dir().expanduser())
