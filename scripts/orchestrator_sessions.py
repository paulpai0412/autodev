"""Host adapter facade for runtime session operations."""

from __future__ import annotations

import os
from typing import Callable

from scripts.host_adapter import HostAdapter, SessionOutcome, SessionStartContext, SessionStartResult


HostAdapterFactory = Callable[[], HostAdapter]


_HOST_ADAPTER_FACTORIES: dict[str, HostAdapterFactory] = {}


def _register_builtin_adapters() -> None:
    if "opencode" in _HOST_ADAPTER_FACTORIES:
        return

    def _build_opencode_adapter() -> HostAdapter:
        from scripts.opencode_host_adapter import OpenCodeHostAdapter

        return OpenCodeHostAdapter()

    _HOST_ADAPTER_FACTORIES["opencode"] = _build_opencode_adapter


def register_host_adapter_factory(name: str, factory: HostAdapterFactory) -> None:
    normalized = name.strip().lower()
    if not normalized:
        raise ValueError("host adapter name must be non-empty")
    _HOST_ADAPTER_FACTORIES[normalized] = factory


def host_adapter_factory(name: str) -> HostAdapterFactory:
    _register_builtin_adapters()
    normalized = name.strip().lower()
    try:
        return _HOST_ADAPTER_FACTORIES[normalized]
    except KeyError as error:
        available = ", ".join(sorted(_HOST_ADAPTER_FACTORIES.keys()))
        raise ValueError(f"unknown host adapter {name!r}; available adapters: {available}") from error


def configured_host_adapter_name() -> str:
    configured = os.environ.get("AUTODEV_HOST_ADAPTER", "opencode")
    return configured.strip().lower() or "opencode"


def resolve_host_adapter(name: str | None = None) -> HostAdapter:
    adapter_name = name.strip().lower() if isinstance(name, str) else configured_host_adapter_name()
    return host_adapter_factory(adapter_name)()


def default_host_adapter() -> HostAdapter:
    return resolve_host_adapter()


__all__ = [
    "HostAdapter",
    "SessionOutcome",
    "SessionStartContext",
    "SessionStartResult",
    "HostAdapterFactory",
    "configured_host_adapter_name",
    "register_host_adapter_factory",
    "host_adapter_factory",
    "resolve_host_adapter",
    "default_host_adapter",
]
