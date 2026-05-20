from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

import scripts.opencode_host_adapter as opencode_host_adapter
import scripts.orchestrator_sessions as orchestrator_sessions
from scripts.host_adapter import SessionOutcome, SessionStartContext, SessionStartResult


def test_spawn_detached_opencode_run_sets_pwd_to_resolved_workdir(tmp_path: Path):
    workdir = tmp_path / "consumer-project"
    workdir.mkdir()

    with patch.dict(
        "scripts.opencode_host_adapter.os.environ",
        {"PATH": "/usr/bin", "PWD": "/wrong-directory"},
        clear=True,
    ), patch("scripts.opencode_host_adapter.subprocess.Popen") as mocked_popen:
        opencode_host_adapter.spawn_detached_opencode_run(["opencode", "run"], workdir=workdir)

    kwargs = mocked_popen.call_args.kwargs

    assert kwargs["cwd"] == str(workdir.resolve())
    assert kwargs["env"]["PWD"] == str(workdir.resolve())
    assert kwargs["env"]["PATH"] == "/usr/bin"
    assert kwargs["stdout"] is not subprocess.PIPE
    assert kwargs["stderr"] == subprocess.STDOUT
    assert kwargs["start_new_session"] is True
    assert kwargs["close_fds"] is True
    assert (workdir / ".opencode/runtime/session-logs").is_dir()


class _FakeHostAdapter:
    def start_root_session(self, context: SessionStartContext) -> SessionStartResult:
        del context
        return SessionStartResult(status="success", session_id="ses-fake")

    def start_child_role(self, role: str, context: SessionStartContext) -> SessionStartResult:
        del role, context
        return SessionStartResult(status="success", session_id="ses-fake-child")

    def read_session_outcome(self, runtime_session_id: str) -> SessionOutcome | None:
        del runtime_session_id
        return None

    def resume_link(self, runtime_session_id: str) -> str:
        return f"resume://{runtime_session_id}"

    def operator_entrypoints(self) -> dict[str, str]:
        return {}

    def capabilities(self) -> dict[str, object]:
        return {}


@pytest.fixture(autouse=True)
def _reset_adapter_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orchestrator_sessions, "_HOST_ADAPTER_FACTORIES", {})


def test_resolve_host_adapter_uses_registered_factory() -> None:
    orchestrator_sessions.register_host_adapter_factory("fake", lambda: _FakeHostAdapter())

    adapter = orchestrator_sessions.resolve_host_adapter("fake")

    assert isinstance(adapter, _FakeHostAdapter)


def test_configured_host_adapter_name_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTODEV_HOST_ADAPTER", "  Fake  ")

    assert orchestrator_sessions.configured_host_adapter_name() == "fake"
