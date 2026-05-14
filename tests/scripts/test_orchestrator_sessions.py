from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import scripts.orchestrator_sessions as orchestrator_sessions


def test_spawn_detached_opencode_run_sets_pwd_to_resolved_workdir(tmp_path: Path):
    workdir = tmp_path / "consumer-project"
    workdir.mkdir()

    with patch.dict(
        "scripts.opencode_host_adapter.os.environ",
        {"PATH": "/usr/bin", "PWD": "/wrong-directory"},
        clear=True,
    ), patch("scripts.opencode_host_adapter.subprocess.Popen") as mocked_popen:
        orchestrator_sessions.spawn_detached_opencode_run(["opencode", "run"], workdir=workdir)

    kwargs = mocked_popen.call_args.kwargs

    assert kwargs["cwd"] == str(workdir.resolve())
    assert kwargs["env"]["PWD"] == str(workdir.resolve())
    assert kwargs["env"]["PATH"] == "/usr/bin"
    assert kwargs["stdout"] is not subprocess.PIPE
    assert kwargs["stderr"] == subprocess.STDOUT
    assert kwargs["start_new_session"] is True
    assert kwargs["close_fds"] is True
    assert (workdir / ".opencode/runtime/session-logs").is_dir()
