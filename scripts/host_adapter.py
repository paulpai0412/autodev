"""Host adapter boundary for runtime-specific session operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class SessionStartContext:
    title: str
    prompt: str
    agent: str
    workdir: Path
    source_session_id: str
    role: str
    stage: str
    issue_number: str
    branch: str
    started_at_iso: str


@dataclass(frozen=True)
class SessionStartResult:
    status: str
    session_id: str = ""
    launch_title: str = ""
    error: str = ""
    resume_hint: str = ""
    resume_command: str = ""
    readability_status: str = ""
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def should_retry_without_source_session(self) -> bool:
        """Signal whether dispatch should retry once without source-session linkage.

        Some hosts reject continuation/pre-filled contexts with errors like
        "assistant message prefill". In that case, the orchestrator can recover
        by launching a fresh root session without source-session affinity.
        """

        value = self.metadata.get("retryWithoutSourceSession")
        return bool(value)


@dataclass(frozen=True)
class SessionOutcome:
    status: str
    session_id: str = ""
    started_at: str = ""
    ended_at: str = ""
    error_kind: str = ""
    error: str = ""
    resume_hint: str = ""
    metadata: dict[str, object] = field(default_factory=dict)


class HostAdapter(Protocol):
    def start_root_session(self, context: SessionStartContext) -> SessionStartResult: ...

    def start_child_role(self, role: str, context: SessionStartContext) -> SessionStartResult: ...

    def read_session_outcome(self, runtime_session_id: str) -> SessionOutcome | None: ...

    def resume_link(self, runtime_session_id: str) -> str: ...

    def operator_entrypoints(self) -> dict[str, str]: ...

    def capabilities(self) -> dict[str, object]: ...
