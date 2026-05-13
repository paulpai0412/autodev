"""OpenCode session launch helpers for the autodev supervisor."""

from __future__ import annotations

import io
import json
import os
import selectors
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import IO, Callable, Protocol, cast


class FindSessionID(Protocol):
    def __call__(self, *, title: str, workdir: Path, created_after_ms: int) -> str | None: ...


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

    return subprocess.Popen(
        command,
        cwd=str(resolved_workdir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        env=env,
    )


def stream_supports_fileno(stream: IO[str]) -> bool:
    try:
        _ = stream.fileno()
    except (AttributeError, OSError, ValueError, io.UnsupportedOperation):
        return False
    return True


def opencode_db_path() -> Path:
    data_home = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local/share"))
    return data_home / "opencode" / "opencode.db"


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
