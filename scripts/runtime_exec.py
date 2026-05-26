from __future__ import annotations

import os
import platform
import sys
from pathlib import Path


def resolved_python_executable() -> str:
    override = os.environ.get("AUTODEV_PYTHON", "").strip()
    if override:
        return override
    executable = (sys.executable or "").strip()
    if executable:
        return executable
    return "python"


def shell_python_command_token() -> str:
    return "python"


def default_opencode_commands_dir() -> Path:
    if platform.system().lower().startswith("win"):
        appdata = os.environ.get("APPDATA", "").strip()
        if appdata:
            return Path(appdata) / "opencode" / "commands"
    return Path.home() / ".config" / "opencode" / "commands"


def default_opencode_data_home() -> Path:
    if platform.system().lower().startswith("win"):
        local = os.environ.get("LOCALAPPDATA", "").strip()
        if local:
            return Path(local)
        appdata = os.environ.get("APPDATA", "").strip()
        if appdata:
            return Path(appdata)
    xdg_data_home = os.environ.get("XDG_DATA_HOME", "").strip()
    if xdg_data_home:
        return Path(xdg_data_home)
    return Path.home() / ".local" / "share"


def opencode_cli_fallback_candidates() -> list[Path]:
    candidates = [
        Path.home() / ".opencode" / "bin" / "opencode",
        Path.home() / ".local" / "bin" / "opencode",
        Path.home() / "bin" / "opencode",
    ]
    if platform.system().lower().startswith("win"):
        local = os.environ.get("LOCALAPPDATA", "").strip()
        appdata = os.environ.get("APPDATA", "").strip()
        for base in [local, appdata]:
            if base:
                root = Path(base)
                candidates.extend(
                    [
                        root / "Programs" / "opencode" / "opencode.exe",
                        root / "opencode" / "opencode.exe",
                        root / "Microsoft" / "WindowsApps" / "opencode.exe",
                    ]
                )
    return candidates
