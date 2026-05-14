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

from scripts.host_adapter import HostAdapter, SessionOutcome, SessionStartContext, SessionStartResult


class FindSessionID(Protocol):
    def __call__(self, *, title: str, workdir: Path, created_after_ms: int) -> str | None: ...


def opencode_db_path() -> Path:
    data_home = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local/share"))
    return data_home / "opencode" / "opencode.db"


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


def _parse_json(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _load_messages(connection: sqlite3.Connection, session_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT data FROM message WHERE session_id = ? ORDER BY time_created",
        (session_id,),
    ).fetchall()
    return [_parse_json(str(row[0])) for row in rows]


def _load_parts(connection: sqlite3.Connection, session_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT data FROM part WHERE session_id = ? ORDER BY time_created",
        (session_id,),
    ).fetchall()
    return [_parse_json(str(row[0])) for row in rows]


def read_session_summary(session_id: str, *, db_path: Path | None = None) -> dict[str, object] | None:
    database_path = db_path or opencode_db_path()
    if not database_path.exists():
        return None
    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT * FROM session WHERE id = ?", (session_id,)).fetchone()
        if row is None:
            return None
        session_row = dict(row)
        messages = _load_messages(connection, session_id)
        parts = _load_parts(connection, session_id)
    latest_assistant: dict[str, Any] | None = None
    first_user_text = ""
    for message in messages:
        role = message.get("role")
        if role == "user" and not first_user_text:
            first_user_text = str(message.get("text") or "")
        if role == "assistant":
            latest_assistant = message
    latest_assistant_status = "no_assistant_message"
    latest_assistant_error: dict[str, object] = {}
    latest_assistant_finish = ""
    latest_assistant_tools: list[str] = []
    if latest_assistant is not None:
        latest_assistant_finish = str(latest_assistant.get("finish") or "")
        error_payload = latest_assistant.get("error")
        if isinstance(error_payload, dict):
            latest_assistant_error = dict(error_payload)
        if latest_assistant_error:
            latest_assistant_status = str(latest_assistant_error.get("name") or "error")
        elif latest_assistant_finish:
            latest_assistant_status = latest_assistant_finish
        else:
            latest_assistant_status = "unknown"
    for part in parts:
        if part.get("type") == "tool":
            tool_name = str(part.get("tool") or "")
            if tool_name:
                latest_assistant_tools.append(tool_name)
    model_payload = _parse_json(str(session_row.get("model") or ""))
    created_at = int(session_row.get("time_created") or 0)
    updated_at = int(session_row.get("time_updated") or 0)
    return {
        "session_id": str(session_row.get("id") or ""),
        "parent_id": str(session_row.get("parent_id") or ""),
        "title": str(session_row.get("title") or ""),
        "directory": str(session_row.get("directory") or ""),
        "agent": str(session_row.get("agent") or ""),
        "model": model_payload,
        "time_created": created_at,
        "time_updated": updated_at,
        "duration_ms": max(0, updated_at - created_at),
        "message_count": len(messages),
        "part_count": len(parts),
        "first_user_text": first_user_text,
        "latest_assistant_status": latest_assistant_status,
        "latest_assistant_finish": latest_assistant_finish,
        "latest_assistant_error": latest_assistant_error,
        "tool_sequence": latest_assistant_tools,
    }


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
            return SessionStartResult(
                status="error",
                launch_title=launch_title,
                error=(stderr_text or stdout_text).strip() or "opencode run did not emit a sessionID before timeout",
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

    def start_child_role(self, role: str, context: SessionStartContext) -> SessionStartResult:
        child_context = SessionStartContext(
            title=context.title,
            prompt=context.prompt,
            agent=context.agent,
            workdir=context.workdir,
            source_session_id=context.source_session_id,
            role=role,
            stage=context.stage,
            issue_number=context.issue_number,
            branch=context.branch,
            started_at_iso=context.started_at_iso,
        )
        return self.start_root_session(child_context)

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
            "inspect": "autodev-show-session.md",
            "doctor": "autodev-doctor.md",
        }

    def capabilities(self) -> dict[str, object]:
        return {
            "host": "opencode",
            "background_sessions": True,
            "subagents": True,
            "plugin_commands": True,
        }
