#!/usr/bin/env python3
"""Manage autodev consumer project setup, commands, and checks."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import cast

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import autodev_host_packaging
from scripts.control_plane_db import canonical_control_plane_base_dir, ensure_control_plane_db
from scripts.orchestrator_supervisor import show_latest_session
from scripts.control_plane_db import list_issues
from scripts.orchestrator_sessions import configured_host_adapter_name, resolve_host_adapter
from scripts.opencode_host_adapter import resolve_opencode_cli
from scripts.codex_host_adapter import resolve_codex_cli
from scripts.issue_state_machine import ISSUE_STATES
from scripts.state_projection import (
    PR_WORKFLOW_LABELS,
    TEAM_WORKFLOW_STATES,
    default_state_projection_config_lines,
)
from scripts.runtime_exec import default_opencode_commands_dir, resolved_python_executable, shell_python_command_token


ROOT = Path(__file__).resolve().parents[1]
AGENTS_BEGIN = "<!-- AUTODEV:BEGIN -->"
AGENTS_END = "<!-- AUTODEV:END -->"
DEFAULT_REPO_DESCRIPTION = "Autodev consumer project"
GITHUB_MONITORING_BEGIN = "# AUTODEV_GITHUB_MONITORING:BEGIN"
GITHUB_MONITORING_END = "# AUTODEV_GITHUB_MONITORING:END"
DEFAULT_GITHUB_PROJECT_TITLE = "Autodev Control Plane"
CONFIG_TEMPLATE_PATH = ROOT / ".autodev_example.yaml"

BOOTSTRAP_LABELS = [
    ("needs-triage", "D4C5F9", "Maintainer needs to evaluate this issue"),
    ("needs-info", "F9D0C4", "Waiting on reporter for more information"),
    ("ready-for-agent", "0E8A16", "Fully specified and ready for an AFK agent"),
    ("ready-for-human", "BFDADC", "Requires human implementation"),
    ("wontfix", "FFFFFF", "Will not be actioned"),
    ("agent-dispatching", "FBCA04", "Claimed by scheduler and dispatch in progress"),
    ("agent-in-progress", "5319E7", "Issue actively running in autodev"),
    ("agent-in-review", "1D76DB", "Issue is in review or blocked release/quarantine handling"),
    ("agent-completed", "0E8A16", "Issue completed by autodev"),
    (PR_WORKFLOW_LABELS["not_opened"], "C2E0C6", "PR not opened yet"),
    (PR_WORKFLOW_LABELS["opened"], "BFD4F2", "PR opened"),
    (PR_WORKFLOW_LABELS["verifier_passed"], "0E8A16", "Verifier passed"),
    (PR_WORKFLOW_LABELS["verifier_fail"], "B60205", "Verifier failed"),
    (PR_WORKFLOW_LABELS["verifier_blocked"], "FBCA04", "Verifier blocked"),
    (PR_WORKFLOW_LABELS["release_failed"], "B60205", "Release failed"),
    (PR_WORKFLOW_LABELS["release_blocked"], "FBCA04", "Release blocked"),
    (PR_WORKFLOW_LABELS["merged"], "0E8A16", "PR merged"),
]

DOMAIN_DOCS = {
    "docs/agents/domain.md": "# Domain context\n\nDescribe the project domain language, high-value paths, and gotchas for autodev workers.\n",
    "docs/agents/issue-tracker.md": "# Issue tracker\n\nDescribe the GitHub repository, labels, PR policy, and evidence conventions for this project.\n",
    "docs/agents/triage-labels.md": "# Triage labels\n\nDocument labels that control ready-for-agent, blocked, human-review, and release states.\n",
}

ARTIFACT_DIRS = [
    "docs/agents/runtime",
    ".opencode/runtime",
]

LOCAL_ENV_GITIGNORE_LINES = [
    "# local env and agent files",
    ".env",
    "AGENTS.md",
]


RUNTIME_GITIGNORE_LINES = [
    "# autodev runtime state",
    ".autodev.yaml",
    ".opencode/runtime/*",
    ".opencode/runtime/control-plane.sqlite3",
    "!.opencode/runtime/.gitkeep",
]

LOCAL_ARTIFACT_GITIGNORE_LINES = [
    "# local tool and test artifacts",
    ".playwright-mcp/",
    "artifacts/",
]

REQUIRED_GITIGNORE_LINES = [
    *LOCAL_ENV_GITIGNORE_LINES[1:],
    *RUNTIME_GITIGNORE_LINES[1:],
    *LOCAL_ARTIFACT_GITIGNORE_LINES[1:],
]

TRACKED_RUNTIME_BLOCK_PREFIX = "tracked autodev runtime files must be removed from git index:"

DEFAULT_ENV_VARS: dict[str, str] = {
    "AUTODEV_RELEASE_BACKFILL_MODE": "auto",
    "AUTODEV_AUTO_RELEASE_APPROVAL_MODE": "bypass_approval",
    "AUTODEV_DEVELOPMENT_CAPACITY": "1",
    "AUTODEV_RELEASE_CAPACITY": "1",
    "AUTODEV_HOST_ADAPTER": "opencode",
}


def _host_adapter():
    return resolve_host_adapter()


def _operator_entrypoints() -> dict[str, str]:
    return autodev_host_packaging.host_packaging_config_from_adapter(
        adapter=_host_adapter(),
        fallback_commands_dir=default_opencode_commands_dir(),
    ).entrypoints


def _default_commands_dir() -> Path:
    return autodev_host_packaging.host_packaging_config_from_adapter(
        adapter=_host_adapter(),
        fallback_commands_dir=default_opencode_commands_dir(),
    ).commands_dir


def _command_templates() -> dict[str, str]:
    return autodev_host_packaging.command_templates(root=ROOT, entrypoints=_operator_entrypoints())


@dataclass
class ActionReport:
    actions: list[str]
    findings: list[str]

    def has_findings(self) -> bool:
        return bool(self.findings)


@dataclass
class GitHubProjectSetup:
    owner: str
    title: str
    number: int
    project_id: str
    field_ids: dict[str, str]
    field_option_ids: dict[str, dict[str, str]]


@dataclass
class ConfiguredGitHubMonitoring:
    owner: str
    title: str
    number: int
    project_id: str
    field_ids: dict[str, str]
    field_option_ids: dict[str, dict[str, str]]


AUTODEV_ISSUE_STATES = TEAM_WORKFLOW_STATES

AUTODEV_PROJECT_STATUS_STATES = [state for state in ISSUE_STATES if state != "closed"]

AUTODEV_PROJECT_STATUS_COLORS: dict[str, str] = {
    "ready": "GRAY",
    "claimed": "BLUE",
    "dispatching": "BLUE",
    "running": "BLUE",
    "verifying": "YELLOW",
    "verified": "GREEN",
    "release_pending": "ORANGE",
    "completed": "GREEN",
    "failed": "RED",
    "quarantined": "PURPLE",
}

AUTODEV_PR_WORKFLOW_STATES = [
    "not_opened",
    "opened",
    "verifier_passed",
    "verifier_fail",
    "verifier_blocked",
    "release_failed",
    "release_blocked",
    "merged",
]


def _project_root(path: str | None) -> Path:
    return Path(path or ".").resolve()


def _consumer_project_root(path: str | None) -> Path:
    candidate = _project_root(path)
    for current in (candidate, *candidate.parents):
        if (current / ".autodev.yaml").exists():
            return current
    return candidate


def _canonical_project_root(path: str | None) -> Path:
    return canonical_control_plane_base_dir(_consumer_project_root(path))


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_text(text, encoding="utf-8")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _parse_dotenv(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            continue
        normalized_value = value.strip()
        if len(normalized_value) >= 2 and normalized_value[0] == normalized_value[-1] and normalized_value[0] in {'"', "'"}:
            normalized_value = normalized_value[1:-1]
        values[normalized_key] = normalized_value
    return values


def _dotenv_text(values: dict[str, str]) -> str:
    lines = [
        "# Autogenerated by autodev init",
        "# You can modify these values for your environment.",
    ]
    for key in sorted(values):
        lines.append(f"{key}={values[key]}")
    return "\n".join(lines) + "\n"


def _load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        return _parse_dotenv(_read_text(path))
    except OSError:
        return {}


def _ensure_env_file(
    root: Path,
    *,
    github_repo: str,
    project_id: str,
    dry_run: bool,
    check: bool,
    report: ActionReport,
) -> None:
    env_path = root / ".env"
    existing = _load_dotenv(env_path)
    desired = dict(existing)
    desired["AUTODEV_GITHUB_REPO"] = github_repo
    if project_id:
        desired["AUTODEV_GITHUB_PROJECT_ID"] = project_id
    for key, value in DEFAULT_ENV_VARS.items():
        desired.setdefault(key, value)

    desired_text = _dotenv_text(desired)
    existing_text = _read_text(env_path) if env_path.exists() else ""
    if desired_text == existing_text:
        return

    report.actions.append("create/update .env for autodev runtime defaults")
    if dry_run or check:
        return
    _write_text(env_path, desired_text)


def _runtime_gitignore_is_configured(root: Path) -> bool:
    gitignore = root / ".gitignore"
    if not gitignore.exists():
        return False
    text = _read_text(gitignore)
    return all(line in text.splitlines() for line in REQUIRED_GITIGNORE_LINES)


def _ensure_runtime_gitignore(root: Path, *, dry_run: bool, check: bool, report: ActionReport) -> None:
    if _runtime_gitignore_is_configured(root):
        return
    report.actions.append("update .gitignore for autodev runtime state and local artifacts")
    if dry_run or check:
        return
    gitignore = root / ".gitignore"
    original = _read_text(gitignore) if gitignore.exists() else ""
    existing_lines = original.splitlines()
    existing_line_set = set(existing_lines)
    additions: list[str] = []

    for block in (LOCAL_ENV_GITIGNORE_LINES, RUNTIME_GITIGNORE_LINES, LOCAL_ARTIFACT_GITIGNORE_LINES):
        missing = [line for line in block[1:] if line not in existing_line_set]
        if not missing:
            continue
        if block[0] not in existing_line_set:
            additions.append(block[0])
            existing_line_set.add(block[0])
        additions.extend(missing)
        existing_line_set.update(missing)

    if not additions:
        return

    separator = "" if not original or original.endswith("\n") else "\n"
    block = "\n".join(additions) + "\n"
    _write_text(gitignore, f"{original}{separator}{block}")


def _tracked_runtime_paths(root: Path) -> list[str]:
    if not _is_git_repo(root):
        return []
    result = _run_command(["git", "ls-files", ".opencode/runtime"], cwd=root, check=False)
    if result.returncode != 0:
        return []
    stdout = cast(str, result.stdout or "")
    return [line.strip() for line in stdout.splitlines() if line.strip() and line.strip() != ".opencode/runtime/.gitkeep"]


def _print_runtime_path_confirmation(*, project_root: Path, command: str) -> None:
    runtime_db = project_root / ".opencode/runtime/control-plane.sqlite3"
    print(f"[autodev:{command}] project-root={project_root}")
    print(f"[autodev:{command}] runtime-db={runtime_db}")
    print(f"[autodev:{command}] supervisor-base-dir={project_root}")


def _enforce_runtime_db_untracked(*, project_root: Path, command: str) -> bool:
    report = doctor_project(project_root)
    tracked_findings = [
        finding
        for finding in report.findings
        if finding.startswith(TRACKED_RUNTIME_BLOCK_PREFIX)
    ]
    if not tracked_findings:
        print(f"[autodev:{command}] doctor tracked-runtime check: pass")
        return True
    for finding in tracked_findings:
        print(f"[autodev:{command}] BLOCKED: {finding}", file=sys.stderr)
    print(
        f"[autodev:{command}] Run `PYTHONPATH=. {shell_python_command_token()} scripts/autodev_project.py doctor --project-root \"{project_root}\"` and untrack runtime DB before retrying.",
        file=sys.stderr,
    )
    return False


def _run_command(args: list[str], *, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, check=check, capture_output=True, text=True)


def _is_windows_platform() -> bool:
    return platform.system().lower().startswith("win")


def _windows_preflight_findings() -> list[str]:
    findings: list[str] = []

    resolved_python = resolved_python_executable()
    python_available = Path(resolved_python).exists() or shutil.which(resolved_python) is not None
    if not python_available:
        findings.append(
            "windows preflight: Python executable not found. "
            f"Resolved AUTODEV_PYTHON/sys.executable to `{resolved_python}`. "
            "Install Python 3 and ensure it is in PATH, or set AUTODEV_PYTHON explicitly."
        )

    if shutil.which("git") is None:
        findings.append(
            "windows preflight: `git` not found in PATH. "
            "Install Git for Windows and enable PATH integration."
        )

    if shutil.which("gh") is None:
        findings.append(
            "windows preflight: `gh` (GitHub CLI) not found in PATH. "
            "Install GitHub CLI for Windows and run `gh auth login`."
        )

    host_adapter = configured_host_adapter_name()
    if host_adapter == "opencode" and not resolve_opencode_cli():
        findings.append(
            "windows preflight: OpenCode CLI not found (tried PATH and common Windows AppData locations). "
            "Install OpenCode Desktop/CLI and ensure `opencode` or `opencode.exe` is discoverable."
        )
    if host_adapter == "codex" and not resolve_codex_cli():
        findings.append(
            "windows preflight: Codex CLI not found (tried PATH and common Windows AppData locations). "
            "Install Codex CLI and ensure `codex` or `codex.exe` is discoverable."
        )

    return findings


def _repo_https_url(github_repo: str) -> str:
    return f"https://github.com/{github_repo}.git"


def _github_owner(github_repo: str) -> str:
    owner, _, _repo = github_repo.partition("/")
    return owner


def _default_github_project_title(github_repo: str) -> str:
    return f"{DEFAULT_GITHUB_PROJECT_TITLE} ({github_repo})"


def _validate_github_repo(github_repo: str) -> str:
    normalized = github_repo.strip()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", normalized):
        raise ValueError(f"github_repo must be owner/repo, got {github_repo!r}")
    return normalized


def _parse_optional_json(stdout: str) -> dict[str, object] | list[object]:
    text = stdout.strip()
    if not text:
        return {}
    parsed = json.loads(text)
    if isinstance(parsed, (dict, list)):
        return parsed
    return {}


def _as_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return default
        try:
            return int(stripped)
        except ValueError:
            return default
    return default


def _find_project_by_title(*, owner: str, title: str) -> tuple[int, str] | None:
    result = _run_command(
        [
            "gh",
            "project",
            "list",
            "--owner",
            owner,
            "--limit",
            "100",
            "--format",
            "json",
        ],
        check=False,
    )
    if result.returncode != 0:
        return None
    parsed = _parse_optional_json(cast(str, result.stdout or ""))
    projects = parsed if isinstance(parsed, list) else []
    for project in projects:
        if not isinstance(project, dict):
            continue
        if str(project.get("title") or "") == title:
            number = int(project.get("number") or 0)
            project_id = str(project.get("id") or "")
            if number > 0 and project_id:
                return number, project_id
    return None


def _find_project_by_id(*, owner: str, project_id: str) -> tuple[int, str] | None:
    target_id = project_id.strip()
    if not target_id:
        return None

    result = _run_command(
        [
            "gh",
            "project",
            "list",
            "--owner",
            owner,
            "--limit",
            "100",
            "--format",
            "json",
        ],
        check=False,
    )
    if result.returncode != 0:
        return None
    parsed = _parse_optional_json(cast(str, result.stdout or ""))
    projects = parsed if isinstance(parsed, list) else []
    for project in projects:
        if not isinstance(project, dict):
            continue
        candidate_id = str(project.get("id") or "").strip()
        if candidate_id != target_id:
            continue
        number = int(project.get("number") or 0)
        if number > 0:
            return number, candidate_id
    return None


def _find_project_by_number(*, owner: str, project_number: int) -> tuple[int, str] | None:
    if project_number <= 0:
        return None

    result = _run_command(
        [
            "gh",
            "project",
            "list",
            "--owner",
            owner,
            "--limit",
            "100",
            "--format",
            "json",
        ],
        check=False,
    )
    if result.returncode != 0:
        return None
    parsed = _parse_optional_json(cast(str, result.stdout or ""))
    projects = parsed if isinstance(parsed, list) else []
    for project in projects:
        if not isinstance(project, dict):
            continue
        candidate_number = int(project.get("number") or 0)
        if candidate_number != project_number:
            continue
        candidate_id = str(project.get("id") or "").strip()
        if candidate_id:
            return candidate_number, candidate_id
    return None


def _create_project(*, owner: str, title: str) -> tuple[int, str]:
    result = _run_command(
        [
            "gh",
            "project",
            "create",
            "--owner",
            owner,
            "--title",
            title,
            "--format",
            "json",
        ],
        check=False,
    )
    if result.returncode != 0:
        detail = cast(str, result.stderr or "").strip() or cast(str, result.stdout or "").strip()
        raise RuntimeError(f"failed to create GitHub project {title!r}: {detail}")
    parsed = _parse_optional_json(cast(str, result.stdout or ""))
    payload = cast(dict[str, object], parsed) if isinstance(parsed, dict) else {}
    number = _as_int(payload.get("number"), 0)
    project_id = str(payload.get("id") or "")
    if number <= 0 or not project_id:
        raise RuntimeError(f"failed to parse created project payload for {title!r}")
    return number, project_id


def _ensure_project_linked(*, owner: str, project_number: int, github_repo: str) -> None:
    _ = _run_command(
        [
            "gh",
            "project",
            "link",
            str(project_number),
            "--owner",
            owner,
            "--repo",
            github_repo,
        ],
        check=False,
    )


def _ensure_project_field(
    *,
    owner: str,
    project_number: int,
    field_name: str,
    data_type: str,
    single_select_options: list[str] | None = None,
) -> str:
    listed = _run_command(
        [
            "gh",
            "project",
            "field-list",
            str(project_number),
            "--owner",
            owner,
            "--limit",
            "100",
            "--format",
            "json",
        ],
        check=False,
    )
    if listed.returncode != 0:
        detail = cast(str, listed.stderr or "").strip() or cast(str, listed.stdout or "").strip()
        raise RuntimeError(f"failed to list project fields: {detail}")
    listed_payload = _parse_optional_json(cast(str, listed.stdout or ""))
    if isinstance(listed_payload, list):
        fields = listed_payload
    elif isinstance(listed_payload, dict):
        raw_fields = listed_payload.get("fields")
        fields = raw_fields if isinstance(raw_fields, list) else []
    else:
        fields = []
    for field in fields:
        if not isinstance(field, dict):
            continue
        if str(field.get("name") or "") == field_name:
            field_id = str(field.get("id") or "")
            if field_id:
                return field_id

    create_args = [
        "gh",
        "project",
        "field-create",
        str(project_number),
        "--owner",
        owner,
        "--name",
        field_name,
        "--data-type",
        data_type,
        "--format",
        "json",
    ]
    if data_type == "SINGLE_SELECT":
        options = [item.strip() for item in (single_select_options or []) if item.strip()]
        if not options:
            raise RuntimeError(f"single-select field {field_name!r} requires at least one option")
        create_args.extend(["--single-select-options", ",".join(options)])

    created = _run_command(create_args, check=False)
    if created.returncode != 0:
        detail = cast(str, created.stderr or "").strip() or cast(str, created.stdout or "").strip()
        raise RuntimeError(f"failed to create project field {field_name!r}: {detail}")
    created_payload = _parse_optional_json(cast(str, created.stdout or ""))
    payload = cast(dict[str, object], created_payload) if isinstance(created_payload, dict) else {}
    field_id = str(payload.get("id") or "")
    if not field_id:
        raise RuntimeError(f"failed to parse project field id for {field_name!r}")
    return field_id


def _project_field_option_map(*, owner: str, project_number: int, field_name: str) -> dict[str, str]:
    listed = _run_command(
        [
            "gh",
            "project",
            "field-list",
            str(project_number),
            "--owner",
            owner,
            "--limit",
            "100",
            "--format",
            "json",
        ],
        check=False,
    )
    if listed.returncode != 0:
        detail = cast(str, listed.stderr or "").strip() or cast(str, listed.stdout or "").strip()
        raise RuntimeError(f"failed to list project fields for option map: {detail}")
    listed_payload = _parse_optional_json(cast(str, listed.stdout or ""))
    if isinstance(listed_payload, list):
        fields = listed_payload
    elif isinstance(listed_payload, dict):
        raw_fields = listed_payload.get("fields")
        fields = raw_fields if isinstance(raw_fields, list) else []
    else:
        fields = []
    for field in fields:
        if not isinstance(field, dict):
            continue
        if str(field.get("name") or "") != field_name:
            continue
        options_payload = field.get("options")
        if (not isinstance(options_payload, list)) and isinstance(field.get("settings"), dict):
            settings = cast(dict[str, object], field.get("settings"))
            options_payload = settings.get("options")
        if not isinstance(options_payload, list):
            node_payload = field.get("node")
            if isinstance(node_payload, dict):
                node_settings = node_payload.get("settings")
                if isinstance(node_settings, dict):
                    options_payload = node_settings.get("options")
        options = options_payload if isinstance(options_payload, list) else []
        mapping: dict[str, str] = {}
        for option in options:
            if not isinstance(option, dict):
                continue
            option_name = str(option.get("name") or "").strip()
            option_id = str(option.get("id") or "").strip()
            if option_name and option_id:
                mapping[option_name] = option_id
        return mapping
    return {}


def _option_id_map_for_states(*, owner: str, project_number: int, field_name: str, states: list[str]) -> dict[str, str]:
    options = _project_field_option_map(owner=owner, project_number=project_number, field_name=field_name)
    mapping: dict[str, str] = {}
    for state in states:
        option_id = str(options.get(state, "")).strip()
        if option_id:
            mapping[state] = option_id
    return mapping


def _quote_graphql_string(raw: str) -> str:
    return raw.replace("\\", "\\\\").replace('"', '\\"')


def _sync_single_select_field_options(
    *,
    field_id: str,
    options: list[dict[str, str]],
) -> None:
    if not field_id:
        raise RuntimeError("missing project field id for single-select option sync")
    if not options:
        raise RuntimeError("single-select field option sync requires at least one option")

    fragments: list[str] = []
    for option in options:
        name = str(option.get("name") or "").strip()
        if not name:
            continue
        color = str(option.get("color") or "GRAY").strip().upper() or "GRAY"
        description = str(option.get("description") or "")
        option_id = str(option.get("id") or "").strip()
        parts: list[str] = []
        if option_id:
            parts.append(f'id: "{_quote_graphql_string(option_id)}"')
        parts.extend(
            [
                f'name: "{_quote_graphql_string(name)}"',
                f"color: {color}",
                f'description: "{_quote_graphql_string(description)}"',
            ]
        )
        fragments.append("{ " + ", ".join(parts) + " }")

    if not fragments:
        raise RuntimeError("single-select field option sync produced no valid options")

    mutation = "\n".join(
        [
            "mutation {",
            "  updateProjectV2Field(",
            f'    input: {{ fieldId: "{_quote_graphql_string(field_id)}", singleSelectOptions: [ {", ".join(fragments)} ] }}',
            "  ) {",
            "    projectV2Field {",
            "      ... on ProjectV2SingleSelectField {",
            "        id",
            "      }",
            "    }",
            "  }",
            "}",
        ]
    )
    synced = _run_command(
        [
            "gh",
            "api",
            "graphql",
            "-f",
            f"query={mutation}",
        ],
        check=False,
    )
    if synced.returncode != 0:
        detail = cast(str, synced.stderr or "").strip() or cast(str, synced.stdout or "").strip()
        raise RuntimeError(f"failed to sync project single-select options: {detail}")


def _sync_status_lifecycle_options(*, owner: str, project_number: int, status_field_id: str) -> None:
    existing = _project_field_option_map(owner=owner, project_number=project_number, field_name="Status")
    desired = [
        {
            "id": existing.get(state, ""),
            "name": state,
            "color": AUTODEV_PROJECT_STATUS_COLORS.get(state, "GRAY"),
            "description": "",
        }
        for state in AUTODEV_PROJECT_STATUS_STATES
    ]
    _sync_single_select_field_options(field_id=status_field_id, options=desired)


def _monitoring_block(*, setup: GitHubProjectSetup) -> str:
    state_mapping = setup.field_option_ids.get("state", {})
    pr_workflow_mapping = setup.field_option_ids.get("pr_workflow", {})
    return "\n".join(
        [
            GITHUB_MONITORING_BEGIN,
            f'github_project_owner: "{setup.owner}"',
            f'github_project_title: "{setup.title}"',
            f"github_project_number: {setup.number}",
            f'github_project_id: "{setup.project_id}"',
            "github_project_field_ids:",
            f'  state: "{setup.field_ids.get("state", "")}"',
            f'  stage: "{setup.field_ids.get("stage", "")}"',
            f'  pr_workflow: "{setup.field_ids.get("pr_workflow", "")}"',
            "github_project_field_option_ids:",
            "  state:",
            *[f'    {key}: "{state_mapping[key]}"' for key in sorted(state_mapping)],
            "  pr_workflow:",
            *[f'    {key}: "{pr_workflow_mapping[key]}"' for key in sorted(pr_workflow_mapping)],
            GITHUB_MONITORING_END,
            "",
        ]
    )


def _replace_monitoring_block(config_text: str, *, setup: GitHubProjectSetup) -> str:
    block = _monitoring_block(setup=setup)
    if (GITHUB_MONITORING_BEGIN in config_text) != (GITHUB_MONITORING_END in config_text):
        raise ValueError("unbalanced autodev github monitoring markers in .autodev.yaml")
    if GITHUB_MONITORING_BEGIN in config_text:
        before, rest = config_text.split(GITHUB_MONITORING_BEGIN, 1)
        _middle, after = rest.split(GITHUB_MONITORING_END, 1)
        merged = f"{before}{block}{after.lstrip(chr(10))}"
    else:
        separator = "" if config_text.endswith("\n") else "\n"
        merged = f"{config_text}{separator}{block}"
    return merged


def _extract_project_number_from_config(config_text: str) -> int:
    for raw_line in config_text.splitlines():
        line = raw_line.strip()
        if not line.startswith("github_project_number:"):
            continue
        return _as_int(line.split(":", 1)[1].strip(), 0)
    return 0


def _extract_monitoring_from_config(config_text: str) -> tuple[str, dict[str, str], dict[str, dict[str, str]]]:
    monitoring = _extract_configured_monitoring(config_text)
    return monitoring.project_id, monitoring.field_ids, monitoring.field_option_ids


def _extract_configured_monitoring(config_text: str) -> ConfiguredGitHubMonitoring:
    owner = ""
    title = ""
    number = 0
    project_id = ""
    field_ids: dict[str, str] = {}
    field_option_ids: dict[str, dict[str, str]] = {"state": {}, "pr_workflow": {}}
    in_monitoring_block = False
    in_field_ids_block = False
    in_option_block = False
    current_option_section = ""
    for raw_line in config_text.splitlines():
        line = raw_line.strip()
        if line == GITHUB_MONITORING_BEGIN:
            in_monitoring_block = True
            in_field_ids_block = False
            in_option_block = False
            current_option_section = ""
            continue
        if line == GITHUB_MONITORING_END:
            in_monitoring_block = False
            in_field_ids_block = False
            in_option_block = False
            current_option_section = ""
            continue
        if not in_monitoring_block:
            continue
        if line.startswith("github_project_owner:"):
            owner = line.split(":", 1)[1].strip().strip('"')
        if line.startswith("github_project_title:"):
            title = line.split(":", 1)[1].strip().strip('"')
        if line.startswith("github_project_number:"):
            number = _as_int(line.split(":", 1)[1].strip(), 0)
        if line.startswith("github_project_id:"):
            project_id = line.split(":", 1)[1].strip().strip('"')

        if line == "github_project_field_ids:":
            in_field_ids_block = True
            in_option_block = False
            current_option_section = ""
            continue

        if in_field_ids_block and not raw_line.startswith("  "):
            in_field_ids_block = False

        if in_field_ids_block and raw_line.startswith("  ") and ":" in line:
            key, value = line.split(":", 1)
            field_key = key.strip()
            field_value = value.strip().strip('"')
            if field_key in {"state", "stage", "pr_workflow"} and field_value:
                field_ids[field_key] = field_value
            continue

        if line == "github_project_field_option_ids:":
            in_option_block = True
            in_field_ids_block = False
            current_option_section = ""
            continue

        if in_option_block and raw_line.startswith("  ") and line.endswith(":"):
            section_name = line[:-1]
            current_option_section = section_name if section_name in {"state", "pr_workflow"} else ""
            continue

        if in_option_block and raw_line.startswith("    ") and ":" in line and current_option_section:
            key, value = line.split(":", 1)
            option_name = key.strip()
            option_id = value.strip().strip('"')
            if option_name and option_id:
                field_option_ids.setdefault(current_option_section, {})[option_name] = option_id
            continue

        if in_option_block and not raw_line.startswith(" "):
            in_option_block = False
            current_option_section = ""
    return ConfiguredGitHubMonitoring(
        owner=owner,
        title=title,
        number=number,
        project_id=project_id,
        field_ids=field_ids,
        field_option_ids=field_option_ids,
    )


def _ensure_issue_runtime_project_binding(
    *,
    root: Path,
    project_id: str,
    field_ids: dict[str, str],
    field_option_ids: dict[str, dict[str, str]],
    dry_run: bool,
    check: bool,
    report: ActionReport,
) -> None:
    if dry_run or check:
        report.actions.append("sync issue runtime context with GitHub project binding")
        return
    try:
        from scripts.control_plane_db import issues_in_states, sync_issue_runtime_context

        runtime_rows = issues_in_states(
            root,
            [
                "ready",
                "claimed",
                "dispatching",
                "running",
                "verifying",
                "verified",
                "release_pending",
                "failed",
                "quarantined",
            ],
        )
        for row in runtime_rows:
            issue_number = str(row.get("issue_number") or "")
            if not issue_number:
                continue
            _ = sync_issue_runtime_context(
                root,
                issue_number=issue_number,
                updated_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                runtime_context={
                    "github_project_id": project_id,
                    "github_project_field_ids": {
                        "state": field_ids.get("state", ""),
                        "stage": field_ids.get("stage", ""),
                        "pr_workflow": field_ids.get("pr_workflow", ""),
                    },
                    "github_project_field_option_ids": {
                        "state": dict(field_option_ids.get("state", {})),
                        "pr_workflow": dict(field_option_ids.get("pr_workflow", {})),
                    },
                },
            )
    except Exception as error:  # pragma: no cover - defensive post-init sync
        report.findings.append(f"failed to sync issue runtime project binding: {error}")


def _is_git_repo(root: Path) -> bool:
    result = _run_command(["git", "rev-parse", "--is-inside-work-tree"], cwd=root, check=False)
    stdout = cast(str, result.stdout or "")
    return result.returncode == 0 and stdout.strip() == "true"


def _has_head_commit(root: Path) -> bool:
    result = _run_command(["git", "rev-parse", "--verify", "HEAD"], cwd=root, check=False)
    return result.returncode == 0


def _git_ref_exists(root: Path, ref: str) -> bool:
    result = _run_command(["git", "rev-parse", "--verify", "--quiet", ref], cwd=root, check=False)
    return result.returncode == 0


def _git_remote_url(root: Path, remote: str) -> str | None:
    result = _run_command(["git", "remote", "get-url", remote], cwd=root, check=False)
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _github_repo_exists(github_repo: str) -> bool:
    result = _run_command(["gh", "repo", "view", github_repo], check=False)
    return result.returncode == 0


def _ensure_git_repository(root: Path, *, dry_run: bool, check: bool, report: ActionReport) -> None:
    if _is_git_repo(root):
        return
    report.actions.append("initialize git repository")
    if dry_run or check:
        return
    _ = _run_command(["git", "init", "-b", "main"], cwd=root)


def _ensure_main_branch_baseline(root: Path, *, dry_run: bool, check: bool, report: ActionReport) -> None:
    if not _is_git_repo(root):
        return
    if _has_head_commit(root):
        return

    if _git_remote_url(root, "origin") is not None:
        if dry_run or check:
            report.actions.append("attempt to seed local main from origin/main")
        else:
            _ = _run_command(["git", "fetch", "origin", "main"], cwd=root, check=False)
        if _git_ref_exists(root, "refs/remotes/origin/main"):
            report.actions.append("reset local main from origin/main for bootstrap baseline")
            if dry_run or check:
                return
            _ = _run_command(["git", "checkout", "-B", "main", "refs/remotes/origin/main"], cwd=root)
            return

    report.actions.append("create initial main baseline commit for worktree compatibility")
    if dry_run or check:
        return
    readme = root / "README.md"
    if not readme.exists():
        _write_text(readme, f"# {root.name}\n")
        _ = _run_command(["git", "add", "README.md"], cwd=root)
    _ = _run_command(["git", "commit", "--allow-empty", "-m", "chore: bootstrap repository"], cwd=root)


def _ensure_git_remote(root: Path, *, github_repo: str, dry_run: bool, check: bool, force: bool, report: ActionReport) -> None:
    expected_url = _repo_https_url(github_repo)
    current_url = _git_remote_url(root, "origin")
    if current_url is None:
        report.actions.append(f"add git remote origin {expected_url}")
        if dry_run or check:
            return
        _ = _run_command(["git", "remote", "add", "origin", expected_url], cwd=root)
        return
    if current_url == expected_url:
        return
    if not force:
        report.findings.append(f"origin remote points to {current_url}; expected {expected_url}. Rerun init with --force to update it")
        return
    report.actions.append(f"update git remote origin {expected_url}")
    if dry_run or check:
        return
    _ = _run_command(["git", "remote", "set-url", "origin", expected_url], cwd=root)


def _ensure_github_repo(github_repo: str, *, dry_run: bool, check: bool, report: ActionReport) -> None:
    if _github_repo_exists(github_repo):
        return
    report.actions.append(f"create GitHub repository {github_repo}")
    if dry_run or check:
        return
    _ = _run_command(["gh", "repo", "create", github_repo, "--private", "--description", DEFAULT_REPO_DESCRIPTION])


def _ensure_github_labels(github_repo: str, *, dry_run: bool, check: bool, report: ActionReport) -> None:
    for name, color, description in BOOTSTRAP_LABELS:
        report.actions.append(f"ensure GitHub label {name}")
        if dry_run or check:
            continue
        _ = _run_command(
            [
                "gh",
                "label",
                "create",
                name,
                "--repo",
                github_repo,
                "--color",
                color,
                "--description",
                description,
                "--force",
            ]
        )


def _bootstrap_project_repository(root: Path, *, github_repo: str, dry_run: bool, check: bool, force: bool, report: ActionReport) -> None:
    if dry_run or check:
        report.actions.append("initialize git repository")
        report.actions.append(f"add git remote origin {_repo_https_url(github_repo)}")
        report.actions.append(f"create GitHub repository {github_repo}")
        report.actions.append("attempt to seed local main from origin/main")
        report.actions.append("create initial main baseline commit for worktree compatibility")
        for name, _, _ in BOOTSTRAP_LABELS:
            report.actions.append(f"ensure GitHub label {name}")
        return
    try:
        _ensure_git_repository(root, dry_run=dry_run, check=check, report=report)
        _ensure_git_remote(root, github_repo=github_repo, dry_run=dry_run, check=check, force=force, report=report)
        _ensure_github_repo(github_repo, dry_run=dry_run, check=check, report=report)
        _ensure_main_branch_baseline(root, dry_run=dry_run, check=check, report=report)
        _ensure_github_labels(github_repo, dry_run=dry_run, check=check, report=report)
    except FileNotFoundError as error:
        missing = str(getattr(error, "filename", "") or error)
        report.findings.append(f"missing bootstrap dependency: {missing}")
    except subprocess.CalledProcessError as error:
        stderr = cast(str, error.stderr or "").strip()
        stdout = cast(str, error.stdout or "").strip()
        detail = stderr or stdout or str(error)
        report.findings.append(f"bootstrap command failed: {' '.join(cast(list[str], error.cmd))}: {detail}")


def _legacy_config_text(root: Path, github_repo: str) -> str:
    project_name = root.name
    lines = [
        'schema_version: "1.0"',
        "",
        "project:",
        f"  name: {project_name}",
        f"  root: {root}",
        f"  github_repo: {github_repo}",
        "",
        "context:",
        "  required_reads:",
        "    - AGENTS.md",
        "    - CONTEXT.md",
        "    - docs/agents/domain.md",
        "    - docs/agents/issue-tracker.md",
        "    - docs/agents/triage-labels.md",
        "",
        "runtime:",
        "  control_plane_db: .opencode/runtime/control-plane.sqlite3",
        "",
        *default_state_projection_config_lines(),
        "",
    ]
    return "\n".join(lines)


def _strip_monitoring_block(config_text: str) -> str:
    if (GITHUB_MONITORING_BEGIN in config_text) != (GITHUB_MONITORING_END in config_text):
        return config_text
    if GITHUB_MONITORING_BEGIN not in config_text:
        return config_text
    before, rest = config_text.split(GITHUB_MONITORING_BEGIN, 1)
    _middle, after = rest.split(GITHUB_MONITORING_END, 1)
    return f"{before}{after.lstrip(chr(10))}"


def _config_text(root: Path, github_repo: str) -> str:
    if not CONFIG_TEMPLATE_PATH.exists():
        return _legacy_config_text(root, github_repo)

    config = _read_text(CONFIG_TEMPLATE_PATH)
    config = _strip_monitoring_block(config)
    config = re.sub(r'(?m)^  name:\s*.*$', lambda _match: f"  name: {root.name}", config, count=1)
    config = re.sub(r'(?m)^  root:\s*.*$', lambda _match: f"  root: {root}", config, count=1)
    config = re.sub(r'(?m)^  github_repo:\s*.*$', lambda _match: f"  github_repo: {github_repo}", config, count=1)
    config = re.sub(r'(?m)^github_project_id:\s*.*$', 'github_project_id: ""', config, count=1)
    config = re.sub(
        r'(?ms)^github_project_field_ids:\n(?:  [^\n]*\n)+',
        'github_project_field_ids:\n  state: ""\n  stage: ""\n  pr_workflow: ""\n',
        config,
        count=1,
    )
    if "state_projection:" not in config:
        suffix = "\n".join(default_state_projection_config_lines())
        config = f"{config.rstrip()}\n\n{suffix}\n"
    elif not config.endswith("\n"):
        config = f"{config}\n"
    return config


def _managed_agents_block() -> str:
    return "\n".join(
        [
            AGENTS_BEGIN,
            "## Autodev",
            "",
            "This project is connected to the shared autodev workflow.",
            "",
            "- Project config: `.autodev.yaml`",
            f"- Workflow source: `{ROOT}`",
            f"- Main workflow policy: `{ROOT / 'docs/agents/autonomous-development-workflow.yaml'}`",
            "- Runtime state: `.opencode/runtime/control-plane.sqlite3`",
            "- SQLite is the only runtime control-plane source of truth; local YAML/JSON artifacts are not required for progress.",
            "",
            "Do not copy workflow implementation, templates, commands, or runner scripts into this repo.",
            AGENTS_END,
            "",
        ]
    )


def _replace_managed_block(original: str, block: str) -> str:
    if original.count(AGENTS_BEGIN) > 1 or original.count(AGENTS_END) > 1:
        raise ValueError("duplicate autodev managed markers in AGENTS.md")
    if (AGENTS_BEGIN in original) != (AGENTS_END in original):
        raise ValueError("unbalanced autodev managed markers in AGENTS.md")
    if AGENTS_BEGIN not in original:
        separator = "" if original.endswith("\n") or not original else "\n"
        return f"{original}{separator}\n{block}"
    before, rest = original.split(AGENTS_BEGIN, 1)
    _, after = rest.split(AGENTS_END, 1)
    return f"{before}{block}{after.lstrip(chr(10))}"


def init_project(
    root: Path,
    *,
    github_repo: str,
    dry_run: bool,
    check: bool,
    force: bool,
    create_github_project: bool = True,
    github_project_title: str = DEFAULT_GITHUB_PROJECT_TITLE,
    github_project_owner: str = "",
) -> ActionReport:
    github_repo = _validate_github_repo(github_repo)
    repo_owner = _github_owner(github_repo)
    requested_owner = github_project_owner.strip()
    project_owner = repo_owner
    report = ActionReport(actions=[], findings=[])
    if requested_owner and requested_owner != repo_owner:
        report.findings.append(
            f"ignoring --github-project-owner={requested_owner!r}; using consumer repo owner {repo_owner!r}"
        )
    project_title = github_project_title.strip() or _default_github_project_title(github_repo)
    config_path = root / ".autodev.yaml"
    expected_config = _config_text(root, github_repo)

    if config_path.exists():
        config_text = _read_text(config_path)
        if 'schema_version: "1.0"' not in config_text:
            report.findings.append("unsupported or missing .autodev.yaml schema_version")
    else:
        report.actions.append("create .autodev.yaml")
        if not dry_run and not check:
            _write_text(config_path, expected_config)

    for directory in ARTIFACT_DIRS:
        path = root / directory
        if not path.exists():
            report.actions.append(f"create {directory}/")
            if not dry_run and not check:
                path.mkdir(parents=True, exist_ok=True)

    _ensure_runtime_gitignore(root, dry_run=dry_run, check=check, report=report)

    gitkeep = root / ".opencode/runtime/.gitkeep"
    if not gitkeep.exists():
        report.actions.append("create .opencode/runtime/.gitkeep")
        if not dry_run and not check:
            _write_text(gitkeep, "")

    control_plane_db = root / ".opencode/runtime/control-plane.sqlite3"
    if not control_plane_db.exists():
        report.actions.append("create .opencode/runtime/control-plane.sqlite3")
        if not dry_run and not check:
            _ = ensure_control_plane_db(root)

    for relative_path, starter in DOMAIN_DOCS.items():
        path = root / relative_path
        if not path.exists():
            report.actions.append(f"create {relative_path}")
            if not dry_run and not check:
                _write_text(path, starter)

    agents_path = root / "AGENTS.md"
    original_agents = _read_text(agents_path) if agents_path.exists() else "# AGENTS.md\n"
    try:
        updated_agents = _replace_managed_block(original_agents, _managed_agents_block())
    except ValueError as error:
        report.findings.append(str(error))
    else:
        if updated_agents != original_agents:
            report.actions.append("update AGENTS.md autodev managed block")
            if not dry_run and not check:
                _write_text(agents_path, updated_agents)

    _bootstrap_project_repository(root, github_repo=github_repo, dry_run=dry_run, check=check, force=force, report=report)

    if create_github_project:
        report.actions.append(f"ensure GitHub Project '{project_title}' for {project_owner}")
        if not dry_run and not check:
            try:
                found = _find_project_by_title(owner=project_owner, title=project_title)
                if found is None:
                    project_number, project_id = _create_project(owner=project_owner, title=project_title)
                else:
                    project_number, project_id = found

                _ensure_project_linked(owner=project_owner, project_number=project_number, github_repo=github_repo)

                field_ids = {
                    "state": _ensure_project_field(
                        owner=project_owner,
                        project_number=project_number,
                        field_name="Status",
                        data_type="SINGLE_SELECT",
                        single_select_options=AUTODEV_PROJECT_STATUS_STATES,
                    ),
                    "stage": _ensure_project_field(
                        owner=project_owner,
                        project_number=project_number,
                        field_name="Current Stage",
                        data_type="TEXT",
                    ),
                    "pr_workflow": _ensure_project_field(
                        owner=project_owner,
                        project_number=project_number,
                        field_name="PR Workflow",
                        data_type="SINGLE_SELECT",
                        single_select_options=AUTODEV_PR_WORKFLOW_STATES,
                    ),
                }
                _sync_status_lifecycle_options(
                    owner=project_owner,
                    project_number=project_number,
                    status_field_id=field_ids["state"],
                )
                field_option_ids = {
                    "state": _option_id_map_for_states(
                        owner=project_owner,
                        project_number=project_number,
                        field_name="Status",
                        states=AUTODEV_PROJECT_STATUS_STATES,
                    ),
                    "pr_workflow": _option_id_map_for_states(
                        owner=project_owner,
                        project_number=project_number,
                        field_name="PR Workflow",
                        states=AUTODEV_PR_WORKFLOW_STATES,
                    ),
                }

                config_before = _read_text(config_path) if config_path.exists() else expected_config
                setup = GitHubProjectSetup(
                    owner=project_owner,
                    title=project_title,
                    number=project_number,
                    project_id=project_id,
                    field_ids=field_ids,
                    field_option_ids=field_option_ids,
                )
                config_after = _replace_monitoring_block(config_before, setup=setup)
                if config_after != config_before:
                    report.actions.append("update .autodev.yaml github monitoring block")
                    _write_text(config_path, config_after)

                _ensure_issue_runtime_project_binding(
                    root=root,
                    project_id=project_id,
                    field_ids=field_ids,
                    field_option_ids=field_option_ids,
                    dry_run=dry_run,
                    check=check,
                    report=report,
                )
            except Exception as error:
                report.findings.append(f"failed to set up GitHub project monitoring: {error}")
    else:
        config_for_link = _read_text(config_path) if config_path.exists() else expected_config
        configured_monitoring = _extract_configured_monitoring(config_for_link)
        configured_project_number = configured_monitoring.number or _extract_project_number_from_config(config_for_link)
        configured_project_id = configured_monitoring.project_id
        configured_project_owner = project_owner
        if configured_monitoring.owner and configured_monitoring.owner != project_owner:
            report.findings.append(
                "configured github_project_owner differs from consumer repo owner; "
                f"using {project_owner!r} for project operations"
            )
        configured_project_title = configured_monitoring.title

        has_configured_monitoring = bool(configured_project_number > 0 or configured_project_id or configured_project_title)
        if has_configured_monitoring:
            report.actions.append("ensure repository is linked to configured GitHub Project")

        if has_configured_monitoring and not dry_run and not check:
            try:
                project_number_to_link = configured_project_number
                project_id_to_sync = configured_project_id

                if project_number_to_link <= 0 and configured_project_id:
                    found_by_id = _find_project_by_id(owner=configured_project_owner, project_id=configured_project_id)
                    if found_by_id is not None:
                        project_number_to_link, project_id_to_sync = found_by_id

                if project_number_to_link > 0 and not project_id_to_sync:
                    found_by_number = _find_project_by_number(owner=configured_project_owner, project_number=project_number_to_link)
                    if found_by_number is not None:
                        project_number_to_link, project_id_to_sync = found_by_number

                if project_number_to_link <= 0 and configured_project_title:
                    found_by_title = _find_project_by_title(owner=configured_project_owner, title=configured_project_title)
                    if found_by_title is not None:
                        project_number_to_link, project_id_to_sync = found_by_title

                if project_number_to_link > 0:
                    _ensure_project_linked(
                        owner=configured_project_owner,
                        project_number=project_number_to_link,
                        github_repo=github_repo,
                    )

                    if project_id_to_sync:
                        field_ids = {
                            "state": _ensure_project_field(
                                owner=configured_project_owner,
                                project_number=project_number_to_link,
                                field_name="Status",
                                data_type="SINGLE_SELECT",
                                single_select_options=AUTODEV_PROJECT_STATUS_STATES,
                            ),
                            "stage": _ensure_project_field(
                                owner=configured_project_owner,
                                project_number=project_number_to_link,
                                field_name="Current Stage",
                                data_type="TEXT",
                            ),
                            "pr_workflow": _ensure_project_field(
                                owner=configured_project_owner,
                                project_number=project_number_to_link,
                                field_name="PR Workflow",
                                data_type="SINGLE_SELECT",
                                single_select_options=AUTODEV_PR_WORKFLOW_STATES,
                            ),
                        }
                        _sync_status_lifecycle_options(
                            owner=configured_project_owner,
                            project_number=project_number_to_link,
                            status_field_id=field_ids["state"],
                        )
                        field_option_ids = {
                            "state": _option_id_map_for_states(
                                owner=configured_project_owner,
                                project_number=project_number_to_link,
                                field_name="Status",
                                states=AUTODEV_PROJECT_STATUS_STATES,
                            ),
                            "pr_workflow": _option_id_map_for_states(
                                owner=configured_project_owner,
                                project_number=project_number_to_link,
                                field_name="PR Workflow",
                                states=AUTODEV_PR_WORKFLOW_STATES,
                            ),
                        }
                        setup = GitHubProjectSetup(
                            owner=configured_project_owner,
                            title=configured_project_title,
                            number=project_number_to_link,
                            project_id=project_id_to_sync,
                            field_ids=field_ids,
                            field_option_ids=field_option_ids,
                        )
                        config_after = _replace_monitoring_block(config_for_link, setup=setup)
                        if config_after != config_for_link:
                            report.actions.append("update .autodev.yaml github monitoring block")
                            _write_text(config_path, config_after)
                            config_for_link = config_after

                        _ensure_issue_runtime_project_binding(
                            root=root,
                            project_id=project_id_to_sync,
                            field_ids=field_ids,
                            field_option_ids=field_option_ids,
                            dry_run=dry_run,
                            check=check,
                            report=report,
                        )
            except Exception as error:
                report.findings.append(f"failed to link repository to configured GitHub project: {error}")

    config_text_for_env = _read_text(config_path) if config_path.exists() else expected_config
    project_id_for_env, _field_ids_for_env, _field_option_ids_for_env = _extract_monitoring_from_config(config_text_for_env)
    _ensure_env_file(
        root,
        github_repo=github_repo,
        project_id=project_id_for_env,
        dry_run=dry_run,
        check=check,
        report=report,
    )

    commands_report = install_commands(root / ".opencode/commands", dry_run=(dry_run or check), force=False)
    report.actions.extend(commands_report.actions)
    report.findings.extend(commands_report.findings)

    return report


def install_commands(commands_dir: Path, *, dry_run: bool, force: bool) -> ActionReport:
    report = ActionReport(actions=[], findings=[])
    for filename, content in _command_templates().items():
        path = commands_dir / filename
        if path.exists():
            existing = _read_text(path)
            if existing == content:
                continue
            if not force and "autodev-owned global command" not in existing and "scripts/autodev_project.py" not in existing:
                report.findings.append(f"refusing to overwrite non-autodev command: {path}")
                continue
            report.actions.append(f"update {path}")
        else:
            report.actions.append(f"create {path}")
        if not dry_run:
            _write_text(path, content)
    return report


def doctor_project(root: Path) -> ActionReport:
    report = ActionReport(actions=[], findings=[])
    if not (root / ".autodev.yaml").exists():
        report.findings.append("missing .autodev.yaml")
    else:
        config_text = _read_text(root / ".autodev.yaml")
        project_id, field_ids, field_option_ids = _extract_monitoring_from_config(config_text)
        has_monitoring_block = GITHUB_MONITORING_BEGIN in config_text and GITHUB_MONITORING_END in config_text
        if has_monitoring_block:
            if not project_id:
                report.findings.append("missing github_project_id in .autodev.yaml monitoring block")
            for field_key in ("state", "stage", "pr_workflow"):
                if not str(field_ids.get(field_key) or "").strip():
                    report.findings.append(f"missing github_project_field_ids.{field_key} in .autodev.yaml monitoring block")
            for option_group in ("state", "pr_workflow"):
                if not field_option_ids.get(option_group):
                    report.findings.append(f"missing github_project_field_option_ids.{option_group} mapping in .autodev.yaml monitoring block")
    if not (root / ".opencode/runtime/control-plane.sqlite3").exists():
        report.findings.append("missing .opencode/runtime/control-plane.sqlite3")
    if _is_git_repo(root) and not _runtime_gitignore_is_configured(root):
        report.findings.append("missing .gitignore entries for autodev runtime/tool artifacts")
    tracked_runtime = _tracked_runtime_paths(root)
    if tracked_runtime:
        report.findings.append(
            "tracked autodev runtime files must be removed from git index: " + ", ".join(tracked_runtime)
        )
    agents_path = root / "AGENTS.md"
    if agents_path.exists():
        text = _read_text(agents_path)
        if text.count(AGENTS_BEGIN) > 1 or text.count(AGENTS_END) > 1:
            report.findings.append("duplicate autodev managed markers in AGENTS.md")
        elif (AGENTS_BEGIN in text) != (AGENTS_END in text):
            report.findings.append("unbalanced autodev managed markers in AGENTS.md")
    else:
        report.findings.append("missing AGENTS.md")
    if _is_windows_platform():
        report.findings.extend(_windows_preflight_findings())
    return report


def _show_session_result(project_root: Path) -> tuple[int, str]:
    payload = show_latest_session(base_dir=project_root)
    if payload is None:
        return 1, "no DB-backed autodev session found\n"
    return 0, f"{json.dumps(payload, ensure_ascii=False)}\n"


def _reconcile_issue_number(project_root: Path) -> str:
    active_states = ["claimed", "dispatching", "running", "verifying", "verified", "release_pending", "quarantined", "failed"]
    active_with_session = list_issues(project_root, states=active_states, require_current_session=True)
    if active_with_session:
        return str(active_with_session[0].get("issue_number") or "")
    active_issues = list_issues(project_root, states=active_states)
    if active_issues:
        return str(active_issues[0].get("issue_number") or "")
    raise RuntimeError("no DB-backed autodev issue is currently active for reconcile")


def _print_report(report: ActionReport, *, json_output: bool) -> None:
    if json_output:
        print(json.dumps({"status": "fail" if report.has_findings() else "pass", "actions": report.actions, "findings": report.findings}, ensure_ascii=False))
        return
    for action in report.actions:
        print(action)
    for finding in report.findings:
        print(finding)
    if not report.actions and not report.findings:
        print("autodev project: no changes needed")


def _bootstrap_args(project_root: Path, issue_number: str) -> list[str]:
    normalized = issue_number.strip().removeprefix("#").removeprefix("issue-")
    return [
        "start-issue",
        "--base-dir",
        str(project_root),
        "--issue-number",
        normalized,
        "--source-session-id",
        "autodev-start",
    ]


def _shared_workflow_env(project_root: Path | None = None) -> dict[str, str]:
    env = os.environ.copy()
    if project_root is not None:
        env_path = project_root / ".env"
        env.update(_load_dotenv(env_path))
    pythonpath = env.get("PYTHONPATH", "")
    root_str = str(ROOT)
    env["PYTHONPATH"] = f"{root_str}{os.pathsep}{pythonpath}" if pythonpath else root_str
    return env


def _reconcile_workspace_command(project_root: Path) -> list[str]:
    return [
        resolved_python_executable(),
        "-m",
        "scripts.orchestrator_supervisor",
        "reconcile-workspace",
        "--base-dir",
        str(project_root),
    ]


def _run_reconcile_workspace(project_root: Path) -> int:
    return subprocess.run(
        _reconcile_workspace_command(project_root),
        cwd=project_root,
        env=_shared_workflow_env(project_root),
    ).returncode


def reconcile_watch(
    project_root: Path,
    *,
    interval_seconds: float,
    iterations: int,
    stop_on_error: bool,
) -> int:
    if interval_seconds < 0:
        raise ValueError("--interval-seconds must be non-negative")
    if iterations < 0:
        raise ValueError("--iterations must be non-negative")

    cycle = 0
    final_exit_code = 0
    while iterations == 0 or cycle < iterations:
        exit_code = _run_reconcile_workspace(project_root)
        final_exit_code = max(final_exit_code, exit_code)
        cycle += 1
        if stop_on_error and exit_code != 0:
            return exit_code
        if iterations > 0 and cycle >= iterations:
            break
        time.sleep(interval_seconds)
    return final_exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Initialize an autodev consumer project")
    _ = init.add_argument("--project-root", default=".")
    _ = init.add_argument("--github-repo", default="paulpai0412/autodev")
    _ = init.add_argument(
        "--create-github-project",
        action="store_true",
        dest="create_github_project",
        help="Create/link a GitHub Project and persist project+field bindings for monitoring (default: enabled)",
    )
    _ = init.add_argument(
        "--no-create-github-project",
        action="store_false",
        dest="create_github_project",
        help="Skip creating/linking GitHub Project during init",
    )
    init.set_defaults(create_github_project=True)
    _ = init.add_argument(
        "--github-project-title",
        default="",
        help="GitHub Project title used when project setup is enabled (default: derived from --github-repo)",
    )
    _ = init.add_argument(
        "--github-project-owner",
        default="",
        help="GitHub owner login for project operations (defaults to repo owner)",
    )
    _ = init.add_argument("--dry-run", action="store_true")
    _ = init.add_argument("--check", action="store_true")
    _ = init.add_argument("--force", action="store_true")
    _ = init.add_argument("--json", action="store_true")

    install = subparsers.add_parser("install-commands", help="Install autodev-owned global host commands")
    _ = install.add_argument("--commands-dir", default=str(_default_commands_dir()))
    _ = install.add_argument("--dry-run", action="store_true")
    _ = install.add_argument("--force", action="store_true")
    _ = install.add_argument("--json", action="store_true")

    doctor = subparsers.add_parser("doctor", help="Check whether a project is ready for autodev")
    _ = doctor.add_argument("--project-root", default=".")
    _ = doctor.add_argument("--json", action="store_true")

    start = subparsers.add_parser("start", help="Start autodev workflow for a project issue")
    _ = start.add_argument("--project-root", default=".")
    _ = start.add_argument("--issue-number", required=True)

    reconcile = subparsers.add_parser("reconcile", help="Reconcile autodev runtime state")
    _ = reconcile.add_argument("--project-root", default=".")

    reconcile_watch_parser = subparsers.add_parser("reconcile-watch", help="Continuously reconcile autodev runtime state")
    _ = reconcile_watch_parser.add_argument("--project-root", default=".")
    _ = reconcile_watch_parser.add_argument(
        "--interval-seconds",
        type=float,
        default=30.0,
        help="Seconds to wait between reconcile cycles",
    )
    _ = reconcile_watch_parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Number of reconcile cycles to run; 0 means run until interrupted",
    )
    _ = reconcile_watch_parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Exit immediately when a reconcile cycle returns a non-zero status",
    )

    release = subparsers.add_parser("release", help="Launch independent release worker for PR merge")
    _ = release.add_argument("--project-root", default=".")
    _ = release.add_argument("--issue-number", default="")
    _ = release.add_argument(
        "--auto-approve",
        action="store_true",
        help="Bypass only the human merge approval gate during release; all verifier/check/mergeability gates still apply",
    )

    show = subparsers.add_parser("show-session", help="Show latest autodev root session")
    _ = show.add_argument("--project-root", default=".")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    command = cast(str, args.command)
    json_output = cast(bool, getattr(args, "json", False))

    if command == "init":
        check_mode = cast(bool, args.check)
        report = init_project(
            _project_root(cast(str, args.project_root)),
            github_repo=cast(str, args.github_repo),
            dry_run=cast(bool, args.dry_run),
            check=check_mode,
            force=cast(bool, args.force),
            create_github_project=cast(bool, args.create_github_project),
            github_project_title=cast(str, args.github_project_title),
            github_project_owner=cast(str, args.github_project_owner),
        )
        _print_report(report, json_output=json_output)
        return 1 if check_mode and (report.actions or report.findings) else (1 if report.has_findings() else 0)
    if command == "install-commands":
        report = install_commands(Path(cast(str, args.commands_dir)).expanduser(), dry_run=cast(bool, args.dry_run), force=cast(bool, args.force))
        _print_report(report, json_output=json_output)
        return 1 if report.has_findings() else 0
    if command == "doctor":
        report = doctor_project(_canonical_project_root(cast(str, args.project_root)))
        _print_report(report, json_output=json_output)
        return 1 if report.has_findings() else 0
    if command == "start":
        project_root = _canonical_project_root(cast(str, args.project_root))
        _print_runtime_path_confirmation(project_root=project_root, command=command)
        if not _enforce_runtime_db_untracked(project_root=project_root, command=command):
            return 1
        return subprocess.run(
            [resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", *_bootstrap_args(project_root, cast(str, args.issue_number))],
            cwd=project_root,
            env=_shared_workflow_env(project_root),
        ).returncode
    if command == "reconcile":
        project_root = _canonical_project_root(cast(str, args.project_root))
        _print_runtime_path_confirmation(project_root=project_root, command=command)
        if not _enforce_runtime_db_untracked(project_root=project_root, command=command):
            return 1
        return _run_reconcile_workspace(project_root)
    if command == "reconcile-watch":
        project_root = _canonical_project_root(cast(str, args.project_root))
        _print_runtime_path_confirmation(project_root=project_root, command=command)
        if not _enforce_runtime_db_untracked(project_root=project_root, command=command):
            return 1
        return reconcile_watch(
            project_root,
            interval_seconds=cast(float, args.interval_seconds),
            iterations=cast(int, args.iterations),
            stop_on_error=cast(bool, args.stop_on_error),
        )
    if command == "release":
        project_root = _canonical_project_root(cast(str, args.project_root))
        _print_runtime_path_confirmation(project_root=project_root, command=command)
        if not _enforce_runtime_db_untracked(project_root=project_root, command=command):
            return 1
        release_args = [
            resolved_python_executable(),
            "-m",
            "scripts.orchestrator_supervisor",
            "release",
            "--base-dir",
            str(project_root),
            "--source-session-id",
            "autodev-release",
        ]
        issue_number = cast(str, args.issue_number).strip()
        if issue_number:
            release_args.extend(["--issue-number", issue_number])
        if cast(bool, args.auto_approve):
            release_args.extend(
                [
                    "--approval-override-mode",
                    "bypass_approval",
                    "--override-source",
                    "user_requested_autodev_release",
                    "--human-approval-skipped",
                ]
            )
        return subprocess.run(
            release_args,
            cwd=project_root,
            env=_shared_workflow_env(project_root),
        ).returncode
    if command == "show-session":
        exit_code, output = _show_session_result(_canonical_project_root(cast(str, args.project_root)))
        print(output, end="")
        return exit_code
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
