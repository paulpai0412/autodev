from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import scripts.orchestrator_sessions as orchestrator_sessions


def test_spawn_detached_opencode_run_sets_pwd_to_resolved_workdir(tmp_path: Path):
    workdir = tmp_path / "consumer-project"
    workdir.mkdir()

    with patch.dict(
        "scripts.orchestrator_sessions.os.environ",
        {"PATH": "/usr/bin", "PWD": "/wrong-directory"},
        clear=True,
    ), patch("scripts.orchestrator_sessions.subprocess.Popen") as mocked_popen:
        orchestrator_sessions.spawn_detached_opencode_run(["opencode", "run"], workdir=workdir)

    kwargs = mocked_popen.call_args.kwargs

    assert kwargs["cwd"] == str(workdir.resolve())
    assert kwargs["env"]["PWD"] == str(workdir.resolve())
    assert kwargs["env"]["PATH"] == "/usr/bin"
