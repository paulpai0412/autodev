"""Host adapter facade for runtime session operations."""

from __future__ import annotations

from scripts.host_adapter import HostAdapter, SessionOutcome, SessionStartContext, SessionStartResult
from scripts.opencode_host_adapter import OpenCodeHostAdapter


def default_host_adapter() -> HostAdapter:
    return OpenCodeHostAdapter()


__all__ = [
    "HostAdapter",
    "SessionOutcome",
    "SessionStartContext",
    "SessionStartResult",
    "default_host_adapter",
]
