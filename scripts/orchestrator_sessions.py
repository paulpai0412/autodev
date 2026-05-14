"""Host adapter facade for runtime session operations."""

from __future__ import annotations

from scripts.host_adapter import HostAdapter, SessionOutcome, SessionStartContext, SessionStartResult
from scripts.opencode_host_adapter import (
    OpenCodeHostAdapter,
    extract_session_id_from_run_output,
    find_session_id_in_db,
    opencode_db_path,
    probe_same_repo_session_readability,
    read_initial_session_id,
    read_session_summary,
    resolve_opencode_cli,
    spawn_detached_opencode_run,
    stream_supports_fileno,
    wait_for_session_id_in_db,
)


def default_host_adapter() -> HostAdapter:
    return OpenCodeHostAdapter()


__all__ = [
    "HostAdapter",
    "OpenCodeHostAdapter",
    "SessionOutcome",
    "SessionStartContext",
    "SessionStartResult",
    "default_host_adapter",
    "extract_session_id_from_run_output",
    "find_session_id_in_db",
    "opencode_db_path",
    "probe_same_repo_session_readability",
    "read_initial_session_id",
    "read_session_summary",
    "resolve_opencode_cli",
    "spawn_detached_opencode_run",
    "stream_supports_fileno",
    "wait_for_session_id_in_db",
]
