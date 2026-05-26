"""Codex host adapter implementation."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import Callable, cast

from scripts.host_adapter import HostAdapter, SessionOutcome, SessionStartContext, SessionStartResult
from scripts.runtime_exec import codex_cli_fallback_candidates, default_codex_commands_dir, default_codex_sessions_dir


def resolve_codex_cli() -> str | None:
    codex_cli = shutil.which("codex")
    if codex_cli:
        return codex_cli
    for candidate in codex_cli_fallback_candidates():
        if candidate.exists():
            return str(candidate)
    codex_exe = shutil.which("codex.exe")
    if codex_exe:
        return codex_exe
    return None


def _stream_json_lines(command: list[str], *, workdir: Path) -> tuple[list[dict[str, object]], str]:
    resolved_workdir = workdir.resolve()
    env = os.environ.copy()
    env["PWD"] = str(resolved_workdir)
    completed = subprocess.run(
        command,
        cwd=str(resolved_workdir),
        capture_output=True,
        text=True,
        env=env,
    )
    stdout_text = completed.stdout or ""
    events: list[dict[str, object]] = []
    for raw_line in stdout_text.splitlines():
        line = raw_line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            events.append(cast(dict[str, object], parsed))
    if completed.returncode != 0:
        stderr_text = (completed.stderr or "").strip()
        error_text = stderr_text or stdout_text.strip() or f"codex command failed with exit code {completed.returncode}"
        raise RuntimeError(error_text)
    return events, stdout_text


def extract_thread_id_from_exec_events(events: Iterable[dict[str, str | object]]) -> str:
    for event in events:
        event_type = str(event.get("type") or "")
        if event_type != "thread.started":
            continue
        thread_id = event.get("thread_id")
        if isinstance(thread_id, str) and thread_id:
            return thread_id
    raise RuntimeError("codex exec --json did not emit a thread.started.thread_id")


def _latest_item_text(events: list[dict[str, str | object]]) -> str:
    for event in reversed(events):
        event_type = str(event.get("type") or "")
        if event_type == "item.completed":
            item = event.get("item")
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "") != "agent_message":
                continue
            text = item.get("text")
            if isinstance(text, str) and text:
                return text
            continue
        if event_type == "agent_message":
            message = event.get("message")
            if isinstance(message, str) and message:
                return message
    return ""


def _turn_failed_error(events: list[dict[str, str | object]]) -> tuple[str, str]:
    for event in reversed(events):
        event_type = str(event.get("type") or "")
        if event_type not in {"turn.failed", "task_failed"}:
            continue
        error_payload = event.get("error")
        if isinstance(error_payload, dict):
            kind = str(error_payload.get("kind") or "")
            message = str(error_payload.get("message") or "")
            return kind, message
        return event_type, ""
    return "", ""


def load_latest_session_events(session_id: str, *, sessions_dir: Path | None = None) -> list[dict[str, str | object]]:
    base = sessions_dir or default_codex_sessions_dir().expanduser()
    if not base.exists():
        return []
    matches: list[Path] = []
    for path in base.rglob("*.jsonl"):
        if session_id in path.name:
            matches.append(path)
    if not matches:
        return []
    target = sorted(matches)[-1]
    events: list[dict[str, str | object]] = []
    for raw_line in target.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            if parsed.get("type") == "event_msg":
                payload = parsed.get("payload")
                if isinstance(payload, dict):
                    events.append(cast(dict[str, str | object], payload))
            else:
                events.append(cast(dict[str, str | object], parsed))
    return events


def _resolve_runtime_dir(workdir: Path) -> Path | None:
    resolved = workdir.resolve()
    for candidate in [resolved, *resolved.parents]:
        if candidate.name == "runtime" and candidate.parent.name == ".opencode":
            return candidate
    for candidate in [resolved, *resolved.parents]:
        runtime_dir = candidate / ".opencode" / "runtime"
        if runtime_dir.exists():
            return runtime_dir
    return None


def _build_codex_exec_command(cli_command: str, context: SessionStartContext) -> list[str]:
    command = [cli_command, "exec", "--json", "--skip-git-repo-check"]
    bypass_sandbox = os.environ.get("AUTODEV_CODEX_BYPASS_SANDBOX", "").strip().lower() in {"1", "true", "yes", "on"}
    if bypass_sandbox:
        command.append("--dangerously-bypass-approvals-and-sandbox")
    else:
        sandbox_mode = os.environ.get("AUTODEV_CODEX_SANDBOX_MODE", "workspace-write").strip() or "workspace-write"
        command.extend(["--sandbox", sandbox_mode])

    runtime_dir = _resolve_runtime_dir(context.workdir)
    if runtime_dir is not None:
        command.extend(["--add-dir", str(runtime_dir)])

    command.append(context.prompt)
    return command


class CodexHostAdapter(HostAdapter):
    def __init__(self, *, cli_resolver: Callable[[], str | None] = resolve_codex_cli) -> None:
        self._cli_resolver = cli_resolver

    def start_root_session(self, context: SessionStartContext) -> SessionStartResult:
        cli_command = self._cli_resolver()
        launch_title = context.title
        if not cli_command:
            return SessionStartResult(
                status="error",
                launch_title=launch_title,
                error="Codex CLI not found in PATH. Install or expose the `codex` executable before running autodev dispatch.",
            )

        command = _build_codex_exec_command(cli_command, context)
        stdout_text = ""
        try:
            events, stdout_text = _stream_json_lines(command, workdir=context.workdir)
            thread_id = extract_thread_id_from_exec_events(events)
        except RuntimeError as error:
            return SessionStartResult(
                status="error",
                launch_title=launch_title,
                error=str(error),
                metadata={"command": command, "stdout": stdout_text},
            )

        return SessionStartResult(
            status="success",
            session_id=thread_id,
            launch_title=launch_title,
            resume_hint=f"Resume the Codex thread with `codex exec resume {thread_id}`.",
            resume_command=f"codex exec resume {thread_id}",
            readability_status="verified_json_event_stream",
            tui_resume_command="codex resume",
            stop_continuation_status="root_session_detached",
            stop_continuation_attempts=0,
            execution_mode="root_session",
            metadata={
                "command": command,
                "eventCount": len(events),
                "lastItemText": _latest_item_text(events),
            },
        )

    def start_child_role(self, role: str, context: SessionStartContext) -> SessionStartResult:
        result = self.start_root_session(context)
        metadata = dict(result.metadata)
        metadata["executionMode"] = "foreground_child_role"
        metadata["childRole"] = role
        return SessionStartResult(
            status=result.status,
            session_id=result.session_id,
            launch_title=result.launch_title,
            error=result.error,
            resume_hint=result.resume_hint,
            resume_command=result.resume_command,
            readability_status=result.readability_status,
            retry_without_source_session=result.retry_without_source_session,
            tui_resume_command=result.tui_resume_command,
            stop_continuation_status=result.stop_continuation_status,
            stop_continuation_attempts=result.stop_continuation_attempts,
            execution_mode="foreground_child_role",
            child_role=role,
            child_session_id=result.session_id,
            child_session_status="unknown",
            metadata=metadata,
        )

    def read_session_outcome(self, runtime_session_id: str) -> SessionOutcome | None:
        events = load_latest_session_events(runtime_session_id)
        if not events:
            return None
        status = "unknown"
        for event in reversed(events):
            event_type = str(event.get("type") or "")
            if event_type in {"turn.completed", "task_complete"}:
                status = "completed"
                break
            if event_type in {"turn.failed", "task_failed"}:
                status = "failed"
                break
        error_kind, error_message = _turn_failed_error(events)
        return SessionOutcome(
            status=status,
            session_id=runtime_session_id,
            error_kind=error_kind,
            error=error_message,
            resume_hint=self.resume_link(runtime_session_id),
            metadata={
                "eventCount": len(events),
                "latestAgentMessage": _latest_item_text(events),
            },
        )

    def resume_link(self, runtime_session_id: str) -> str:
        return f"codex exec resume {runtime_session_id}"

    def operator_entrypoints(self) -> dict[str, str]:
        return {
            "start": "autodev-start.md",
            "reconcile": "autodev-reconcile.md",
            "release": "autodev-release.md",
            "inspect": "autodev-show-session.md",
            "doctor": "autodev-doctor.md",
            "full_cycle": "autodev-full-cycle.md",
        }

    def capabilities(self) -> dict[str, object]:
        return {
            "host": "codex",
            "commands_dir": str(default_codex_commands_dir().expanduser()),
            "background_sessions": True,
            "subagents": False,
            "plugin_commands": True,
        }
