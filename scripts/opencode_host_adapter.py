"""OpenCode host adapter implementation."""

from __future__ import annotations

import io
import json
import os
import selectors
import shutil
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import IO, Any, Callable, Protocol, cast

from scripts import opencode_db_path, read_session_summary
from scripts.host_adapter import HostAdapter, SessionOutcome, SessionStartContext, SessionStartResult


class FindSessionID(Protocol):
    def __call__(self, *, title: str, workdir: Path, created_after_ms: int) -> str | None: ...


def resolve_opencode_cli() -> str | None:
    opencode_cli = shutil.which("opencode")
    if opencode_cli:
        return opencode_cli
    known_install_paths = [
        Path.home() / ".opencode/bin/opencode",
        Path.home() / ".local/bin/opencode",
        Path.home() / "bin/opencode",
    ]
    for candidate in known_install_paths:
        if candidate.exists():
            return str(candidate)
    return shutil.which("opencode-desktop")


def extract_session_id_from_run_output(output: str) -> str:
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            event = cast(dict[str, object], json.loads(stripped))
        except json.JSONDecodeError:
            continue
        session_id = event.get("sessionID")
        if isinstance(session_id, str) and session_id:
            return session_id
    raise RuntimeError("opencode run --format json did not emit a sessionID")


def spawn_detached_opencode_run(command: list[str], *, workdir: Path) -> subprocess.Popen[str]:
    resolved_workdir = workdir.resolve()
    env = os.environ.copy()
    env["PWD"] = str(resolved_workdir)
    log_dir = resolved_workdir / ".opencode/runtime/session-logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"opencode-run-{int(time.time() * 1000)}.log"
    log_handle = log_path.open("a", encoding="utf-8")
    try:
        return subprocess.Popen(
            command,
            cwd=str(resolved_workdir),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
            close_fds=True,
            env=env,
        )
    finally:
        log_handle.close()


def stream_supports_fileno(stream: IO[str]) -> bool:
    try:
        _ = stream.fileno()
    except (AttributeError, OSError, ValueError, io.UnsupportedOperation):
        return False
    return True


def find_session_id_in_db(*, title: str, workdir: Path, created_after_ms: int) -> str | None:
    db_path = opencode_db_path()
    if not db_path.exists():
        return None
    try:
        connection = sqlite3.connect(db_path)
    except sqlite3.Error:
        return None
    try:
        row = connection.execute(
            "SELECT id FROM session WHERE title = ? AND directory = ? AND time_created >= ? ORDER BY time_created DESC LIMIT 1",
            (title, str(workdir), created_after_ms),
        ).fetchone()
    except sqlite3.Error:
        return None
    finally:
        connection.close()
    if row and isinstance(row[0], str) and row[0]:
        return row[0]
    return None


def wait_for_session_id_in_db(
    *,
    title: str,
    workdir: Path,
    created_after_ms: int,
    timeout_seconds: float,
    find_session_id: FindSessionID,
) -> str | None:
    end_time = time.monotonic() + timeout_seconds
    while time.monotonic() < end_time:
        session_id = find_session_id(title=title, workdir=workdir, created_after_ms=created_after_ms)
        if session_id:
            return session_id
        time.sleep(0.2)
    return None


def wait_for_child_session_summary(
    parent_session_id: str,
    *,
    directory: str,
    timeout_seconds: float = 5.0,
    poll_interval_seconds: float = 0.25,
) -> dict[str, object] | None:
    from scripts.opencode_session_trace import find_latest_child_session_summary

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        summary = find_latest_child_session_summary(
            parent_session_id,
            directory=directory,
        )
        if summary is not None:
            return summary
        time.sleep(poll_interval_seconds)
    return None


def read_initial_session_id(
    process: subprocess.Popen[str],
    *,
    timeout_seconds: float,
    extract_session_id: Callable[[str], str],
    supports_fileno: Callable[[IO[str]], bool],
) -> tuple[str | None, str, str]:
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    stdout_pipe = process.stdout
    stderr_pipe = process.stderr
    if stdout_pipe is None or stderr_pipe is None:
        return None, "", ""
    if not supports_fileno(stdout_pipe) or not supports_fileno(stderr_pipe):
        stdout_text = stdout_pipe.read()
        stderr_text = stderr_pipe.read()
        try:
            root_session_id = extract_session_id(stdout_text)
        except RuntimeError:
            root_session_id = None
        return root_session_id, stdout_text, stderr_text
    selector = selectors.DefaultSelector()
    selector.register(stdout_pipe, selectors.EVENT_READ, "stdout")
    selector.register(stderr_pipe, selectors.EVENT_READ, "stderr")
    end_time = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < end_time:
            events = selector.select(timeout=max(0.0, end_time - time.monotonic()))
            if not events:
                continue
            for key, _mask in events:
                stream = cast(IO[str], key.fileobj)
                line = stream.readline()
                if key.data == "stdout":
                    stdout_lines.append(line)
                    try:
                        root_session_id = extract_session_id("".join(stdout_lines))
                    except RuntimeError:
                        root_session_id = None
                    if root_session_id:
                        return root_session_id, "".join(stdout_lines), "".join(stderr_lines)
                else:
                    stderr_lines.append(line)
            if process.poll() is not None:
                break
    finally:
        selector.close()
    return None, "".join(stdout_lines), "".join(stderr_lines)


def _extract_same_repo_session_read_probe_result(stdout_text: str) -> tuple[bool | None, str]:
    for line in stdout_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            event = cast(dict[str, object], json.loads(stripped))
        except json.JSONDecodeError:
            continue
        if event.get("type") != "tool_use":
            continue
        part = event.get("part")
        if not isinstance(part, dict) or part.get("tool") != "session_read":
            continue
        state = part.get("state")
        if not isinstance(state, dict):
            return False, "session_read probe returned invalid tool state"
        output = state.get("output")
        if not isinstance(output, str):
            return False, "session_read probe returned no output"
        stripped_output = output.strip()
        if stripped_output.startswith("Session not found:"):
            return False, stripped_output
        if stripped_output:
            return True, stripped_output
        return False, "session_read probe returned empty output"
    return None, "session_read probe did not emit a session_read tool result"


def probe_same_repo_session_readability(
    cli_command: str,
    *,
    workdir: Path,
    root_session_id: str,
    timeout_seconds: float = 30.0,
    max_attempts: int = 3,
    retry_delay_seconds: float = 0.5,
) -> tuple[bool, str]:
    resolved_workdir = workdir.resolve()
    probe_env = os.environ.copy()
    probe_env["PWD"] = str(resolved_workdir)
    prompt = f"Use the session_read tool to read session {root_session_id} with limit 1. Stop immediately after the tool call."
    for attempt in range(1, max_attempts + 1):
        probe_command = [
            cli_command,
            "run",
            "--format",
            "json",
            "--title",
            f"session-readability-check-{root_session_id[-8:]}-{attempt}",
            prompt,
        ]
        try:
            completed = subprocess.run(
                probe_command,
                cwd=str(resolved_workdir),
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                env=probe_env,
            )
        except subprocess.TimeoutExpired:
            return False, f"session_read probe timed out after {timeout_seconds} seconds"
        except OSError as error:
            return False, str(error)
        probe_ok, probe_detail = _extract_same_repo_session_read_probe_result(completed.stdout)
        if probe_ok is True:
            return True, probe_detail
        if probe_ok is False and probe_detail.startswith("Session not found:") and attempt < max_attempts:
            time.sleep(retry_delay_seconds)
            continue
        stderr_text = completed.stderr.strip()
        if stderr_text and probe_detail:
            return False, f"{probe_detail} | stderr: {stderr_text}"
        if stderr_text:
            return False, stderr_text
        if completed.returncode != 0 and probe_detail:
            return False, probe_detail
        if probe_detail:
            return False, probe_detail
        return False, "session_read probe failed without output"
    return False, f"session_read probe could not read {root_session_id} from {resolved_workdir}"


class OpenCodeHostAdapter(HostAdapter):
    def __init__(self, *, cli_resolver: Callable[[], str | None] = resolve_opencode_cli) -> None:
        self._cli_resolver = cli_resolver

    def start_root_session(self, context: SessionStartContext) -> SessionStartResult:
        cli_command = self._cli_resolver()
        launch_title = context.title
        if not cli_command:
            return SessionStartResult(
                status="error",
                launch_title=launch_title,
                error='OpenCode CLI not found in PATH. Install or expose the core "opencode" (or "opencode-desktop") executable before running autodev dispatch.',
            )
        command = [cli_command, "run", "--format", "json", "--title", launch_title]
        normalized_agent = context.agent.strip()
        if normalized_agent and normalized_agent.lower() != "build":
            command.extend(["--agent", normalized_agent])
        command.append(context.prompt)
        started_at_ms = int(time.time() * 1000)
        try:
            process = spawn_detached_opencode_run(command, workdir=context.workdir)
        except OSError as error:
            return SessionStartResult(status="error", launch_title=launch_title, error=str(error))
        session_id, stdout_text, stderr_text = read_initial_session_id(
            process,
            timeout_seconds=10.0,
            extract_session_id=extract_session_id_from_run_output,
            supports_fileno=stream_supports_fileno,
        )
        if not session_id:
            session_id = wait_for_session_id_in_db(
                title=launch_title,
                workdir=context.workdir,
                created_after_ms=started_at_ms,
                timeout_seconds=30.0,
                find_session_id=find_session_id_in_db,
            )
        if not session_id:
            if process.poll() is None:
                process.terminate()
            error_text = (stderr_text or stdout_text).strip() or "opencode run did not emit a sessionID before timeout"
            retry_without_source_session = self._is_prefill_error_message(error_text)
            return SessionStartResult(
                status="error",
                launch_title=launch_title,
                error=error_text,
                metadata={"retryWithoutSourceSession": retry_without_source_session},
            )
        readable, readability_detail = probe_same_repo_session_readability(
            cli_command,
            workdir=context.workdir,
            root_session_id=session_id,
        )
        if not readable:
            if process.poll() is None:
                process.terminate()
            return SessionStartResult(
                status="error",
                session_id=session_id,
                launch_title=launch_title,
                error=f"root session {session_id} was created but failed same-repo session_read probe: {readability_detail}",
                readability_status="failed_same_repo_probe",
            )
        return SessionStartResult(
            status="success",
            session_id=session_id,
            launch_title=launch_title,
            resume_hint=f"Open /sessions in OpenCode TUI and switch to {session_id}, or run opencode --session {session_id}.",
            resume_command=f"opencode --session {session_id}",
            readability_status="verified_same_repo_probe",
            metadata={"stopContinuationStatus": "root_session_detached", "stopContinuationAttempts": 0, "command": command},
        )

    @staticmethod
    def _is_prefill_error_message(error_text: str) -> bool:
        lowered = error_text.lower()
        return "assistant message prefill" in lowered or "conversation must end with a user message" in lowered

    def start_child_role(self, role: str, context: SessionStartContext) -> SessionStartResult:
        result = self.start_root_session(context)
        metadata = dict(result.metadata)
        metadata["executionMode"] = "foreground_child_role"
        metadata["childRole"] = role
        if result.status == "success" and result.session_id:
            child_summary = wait_for_child_session_summary(
                result.session_id,
                directory=str(context.workdir),
            )
            if child_summary is not None:
                metadata["childSessionID"] = str(child_summary.get("session_id") or "")
                metadata["childSessionStatus"] = str(child_summary.get("latest_assistant_status") or "")
                metadata["childSessionSummary"] = child_summary
        return SessionStartResult(
            status=result.status,
            session_id=result.session_id,
            launch_title=result.launch_title,
            error=result.error,
            resume_hint=result.resume_hint,
            resume_command=result.resume_command,
            readability_status=result.readability_status,
            metadata=metadata,
        )

    def read_session_outcome(self, runtime_session_id: str) -> SessionOutcome | None:
        summary = read_session_summary(runtime_session_id)
        if summary is None:
            return None
        error_payload = summary.get("latest_assistant_error")
        error_kind = ""
        error = ""
        if isinstance(error_payload, dict):
            error_kind = str(error_payload.get("name") or "")
            error = str(error_payload.get("message") or "")
        return SessionOutcome(
            status=str(summary.get("latest_assistant_status") or "unknown"),
            session_id=str(summary.get("session_id") or ""),
            started_at=str(summary.get("time_created") or ""),
            ended_at=str(summary.get("time_updated") or ""),
            error_kind=error_kind,
            error=error,
            resume_hint=self.resume_link(runtime_session_id),
            metadata=summary,
        )

    def resume_link(self, runtime_session_id: str) -> str:
        return f"opencode --session {runtime_session_id}"

    def operator_entrypoints(self) -> dict[str, str]:
        return {
            "start": "autodev-start.md",
            "reconcile": "autodev-reconcile.md",
            "release": "autodev-release.md",
            "inspect": "autodev-show-session.md",
            "doctor": "autodev-doctor.md",
        }

    def capabilities(self) -> dict[str, object]:
        return {
            "host": "opencode",
            "commands_dir": str((Path.home() / ".config/opencode/commands").expanduser()),
            "background_sessions": True,
            "subagents": True,
            "plugin_commands": True,
        }
