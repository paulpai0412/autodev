#!/usr/bin/env python3
"""Manage autodev consumer project setup, commands, and checks."""

from __future__ import annotations

import argparse
import json
import os
import re
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
from scripts.orchestrator_sessions import resolve_host_adapter


ROOT = Path(__file__).resolve().parents[1]
AGENTS_BEGIN = "<!-- AUTODEV:BEGIN -->"
AGENTS_END = "<!-- AUTODEV:END -->"
DEFAULT_REPO_DESCRIPTION = "Autodev consumer project"

BOOTSTRAP_LABELS = [
    ("needs-triage", "D4C5F9", "Maintainer needs to evaluate this issue"),
    ("needs-info", "F9D0C4", "Waiting on reporter for more information"),
    ("ready-for-agent", "0E8A16", "Fully specified and ready for an AFK agent"),
    ("ready-for-human", "BFDADC", "Requires human implementation"),
    ("wontfix", "FFFFFF", "Will not be actioned"),
    ("agent-dispatching", "FBCA04", "Claimed by scheduler and dispatch in progress"),
    ("agent-in-progress", "5319E7", "Issue actively running in autodev"),
    ("quarantined", "B60205", "Requires controlled recovery before it can continue"),
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

RUNTIME_GITIGNORE_LINES = [
    "# autodev runtime state",
    ".opencode/runtime/*",
    ".opencode/runtime/control-plane.sqlite3",
    "!.opencode/runtime/.gitkeep",
]

TRACKED_RUNTIME_BLOCK_PREFIX = "tracked autodev runtime files must be removed from git index:"


def _host_adapter():
    return resolve_host_adapter()


def _operator_entrypoints() -> dict[str, str]:
    return autodev_host_packaging.host_packaging_config_from_adapter(
        adapter=_host_adapter(),
        fallback_commands_dir=Path.home() / ".config/opencode/commands",
    ).entrypoints


def _default_commands_dir() -> Path:
    return autodev_host_packaging.host_packaging_config_from_adapter(
        adapter=_host_adapter(),
        fallback_commands_dir=Path.home() / ".config/opencode/commands",
    ).commands_dir


def _command_templates() -> dict[str, str]:
    return autodev_host_packaging.command_templates(root=ROOT, entrypoints=_operator_entrypoints())


@dataclass
class ActionReport:
    actions: list[str]
    findings: list[str]

    def has_findings(self) -> bool:
        return bool(self.findings)


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


def _runtime_gitignore_is_configured(root: Path) -> bool:
    gitignore = root / ".gitignore"
    if not gitignore.exists():
        return False
    text = _read_text(gitignore)
    return all(line in text.splitlines() for line in RUNTIME_GITIGNORE_LINES[1:])


def _ensure_runtime_gitignore(root: Path, *, dry_run: bool, check: bool, report: ActionReport) -> None:
    if _runtime_gitignore_is_configured(root):
        return
    report.actions.append("update .gitignore for autodev runtime state")
    if dry_run or check:
        return
    gitignore = root / ".gitignore"
    original = _read_text(gitignore) if gitignore.exists() else ""
    separator = "" if not original or original.endswith("\n") else "\n"
    block = "\n".join(RUNTIME_GITIGNORE_LINES) + "\n"
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
        f"[autodev:{command}] Run `PYTHONPATH=. python3 scripts/autodev_project.py doctor --project-root \"{project_root}\"` and untrack runtime DB before retrying.",
        file=sys.stderr,
    )
    return False


def _run_command(args: list[str], *, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, check=check, capture_output=True, text=True)


def _repo_https_url(github_repo: str) -> str:
    return f"https://github.com/{github_repo}.git"


def _validate_github_repo(github_repo: str) -> str:
    normalized = github_repo.strip()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", normalized):
        raise ValueError(f"github_repo must be owner/repo, got {github_repo!r}")
    return normalized


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


def _config_text(root: Path, github_repo: str) -> str:
    project_name = root.name
    return "\n".join(
        [
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
        ]
    )


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


def init_project(root: Path, *, github_repo: str, dry_run: bool, check: bool, force: bool) -> ActionReport:
    github_repo = _validate_github_repo(github_repo)
    report = ActionReport(actions=[], findings=[])
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
    if not (root / ".opencode/runtime/control-plane.sqlite3").exists():
        report.findings.append("missing .opencode/runtime/control-plane.sqlite3")
    if _is_git_repo(root) and not _runtime_gitignore_is_configured(root):
        report.findings.append("missing .gitignore entries for .opencode/runtime/*")
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


def _shared_workflow_env() -> dict[str, str]:
    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH", "")
    root_str = str(ROOT)
    env["PYTHONPATH"] = f"{root_str}{os.pathsep}{pythonpath}" if pythonpath else root_str
    return env


def _reconcile_workspace_command(project_root: Path) -> list[str]:
    return [
        "python3",
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
        env=_shared_workflow_env(),
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
            ["python3", "-m", "scripts.orchestrator_supervisor", *_bootstrap_args(project_root, cast(str, args.issue_number))],
            cwd=project_root,
            env=_shared_workflow_env(),
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
            "python3",
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
            env=_shared_workflow_env(),
        ).returncode
    if command == "show-session":
        exit_code, output = _show_session_result(_canonical_project_root(cast(str, args.project_root)))
        print(output, end="")
        return exit_code
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
