"""Host packaging seam for autodev command installation.

Keeps host adapter discovery and command template rendering isolated from
bootstrap/doctor/runtime orchestration logic in autodev_project.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scripts.host_adapter import HostAdapter
from scripts.orchestrator_sessions import default_host_adapter
from scripts.runtime_exec import shell_python_command_token


@dataclass(frozen=True)
class HostPackagingConfig:
    commands_dir: Path
    entrypoints: dict[str, str]


def host_packaging_config_from_adapter(*, adapter: HostAdapter, fallback_commands_dir: Path) -> HostPackagingConfig:
    entrypoints = {str(key): str(value) for key, value in adapter.operator_entrypoints().items()}
    commands_dir_value = adapter.capabilities().get("commands_dir")
    if isinstance(commands_dir_value, str) and commands_dir_value:
        commands_dir = Path(commands_dir_value).expanduser()
    else:
        commands_dir = fallback_commands_dir
    return HostPackagingConfig(commands_dir=commands_dir, entrypoints=entrypoints)


def resolve_host_packaging_config(*, fallback_commands_dir: Path) -> HostPackagingConfig:
    adapter = default_host_adapter()
    return host_packaging_config_from_adapter(adapter=adapter, fallback_commands_dir=fallback_commands_dir)


def command_templates(*, root: Path, entrypoints: dict[str, str]) -> dict[str, str]:
    autodev_home = f'${{AUTODEV_HOME:-{root}}}'
    python_cmd = shell_python_command_token()
    start_filename = entrypoints.get("start", "autodev-start.md")
    reconcile_filename = entrypoints.get("reconcile", "autodev-reconcile.md")
    release_filename = entrypoints.get("release", "autodev-release.md")
    inspect_filename = entrypoints.get("inspect", "autodev-show-session.md")
    doctor_filename = entrypoints.get("doctor", "autodev-doctor.md")
    full_cycle_filename = entrypoints.get("full_cycle", "autodev-full-cycle.md")
    return {
        start_filename: f"""---
description: Start autodev workflow for the current project and issue number
agent: build
subtask: false
---

Run autodev for issue number `$ARGUMENTS` in the current project.

1. Execute:
!`AUTODEV_HOME="{autodev_home}" PYTHONPATH="$AUTODEV_HOME" {python_cmd} "$AUTODEV_HOME/scripts/autodev_project.py" start --project-root "$PWD" --issue-number "$1"`
2. Report the DB-backed dispatch result, current root session, and next recommended action from the command output.

Notes:
- This is an autodev-owned global command. It discovers the target project from the current directory.
- Override `AUTODEV_HOME` first if the shared workflow repo is not installed at `~/apps/autodev`.
- Entrypoint: `scripts/autodev_project.py start`.
""",
        reconcile_filename: f"""---
description: Reconcile autodev runtime state for the current project
agent: build
subtask: false
---

Run:
!`AUTODEV_HOME="{autodev_home}" PYTHONPATH="$AUTODEV_HOME" {python_cmd} "$AUTODEV_HOME/scripts/autodev_project.py" reconcile --project-root "$PWD"`

Report the supervisor decision and whether it requires a subagent or fresh main orchestrator session.

Set `AUTODEV_HOME` first if the shared workflow repo is not installed at `~/apps/autodev`.
""",
        release_filename: f"""---
description: Launch independent autodev release worker for PR merge
agent: build
subtask: false
---

Run the independent release path for issue number `$ARGUMENTS` in the current project. If no issue number is provided, autodev selects the first verified issue waiting for release.

Run:
!`AUTODEV_HOME="{autodev_home}" PYTHONPATH="$AUTODEV_HOME" {python_cmd} "$AUTODEV_HOME/scripts/autodev_project.py" release --project-root "$PWD" --issue-number "$1"`

Report the DB-backed release dispatch result and the release_worker session to resume.

Notes:
- This is separate from `/autodev-reconcile` so human PR approval waits do not block development scheduling.
- Entrypoint: `scripts/autodev_project.py release`.
""",
        inspect_filename: f"""---
description: Show the latest autodev root session for the current project
agent: build
subtask: false
---

Run:
!`AUTODEV_HOME="{autodev_home}" PYTHONPATH="$AUTODEV_HOME" {python_cmd} "$AUTODEV_HOME/scripts/autodev_project.py" show-session --project-root "$PWD"`

Report how to inspect or resume the latest root session.

Set `AUTODEV_HOME` first if the shared workflow repo is not installed at `~/apps/autodev`.
""",
        doctor_filename: f"""---
description: Check whether the current project is ready for autodev
agent: build
subtask: false
---

Run:
!`AUTODEV_HOME="{autodev_home}" PYTHONPATH="$AUTODEV_HOME" {python_cmd} "$AUTODEV_HOME/scripts/autodev_project.py" doctor --project-root "$PWD"`

Report any missing config, runtime state, or command install problems.

Set `AUTODEV_HOME` first if the shared workflow repo is not installed at `~/apps/autodev`.
""",
        full_cycle_filename: f"""---
description: Run autodev full-cycle loop for current project
agent: build
subtask: false
---

Run the shared full-cycle loop script against the current consumer project.

Run:
!`AUTODEV_HOME="{autodev_home}" PROJECT_ROOT="$PWD" {python_cmd} "$AUTODEV_HOME/scripts/autodev_full_cycle.py"`

Report the final cycle status and the latest control-plane summary.

Notes:
- This command does **not** copy the runner into the consumer repo; it always runs the shared runner from `AUTODEV_HOME`.
- Repo resolution is handled by `scripts/autodev_full_cycle.py` from the consumer project context (`REPO` env → consumer `.env` → `.autodev.yaml` → git origin).
""",
    }
