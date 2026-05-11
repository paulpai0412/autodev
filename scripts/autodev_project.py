#!/usr/bin/env python3
"""Manage autodev consumer project setup, commands, checks, and migration."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from scripts.control_plane_db import ensure_control_plane_db


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_COMMANDS_DIR = Path.home() / ".config/opencode/commands"
AGENTS_BEGIN = "<!-- AUTODEV:BEGIN -->"
AGENTS_END = "<!-- AUTODEV:END -->"
CHECKPOINT_TEMPLATE_PATH = ROOT / "docs/agents/runtime/context-checkpoint.yaml"

DOMAIN_DOCS = {
    "docs/agents/domain.md": "# Domain context\n\nDescribe the project domain language, high-value paths, and gotchas for autodev workers.\n",
    "docs/agents/issue-tracker.md": "# Issue tracker\n\nDescribe the GitHub repository, labels, PR policy, and evidence conventions for this project.\n",
    "docs/agents/triage-labels.md": "# Triage labels\n\nDocument labels that control ready-for-agent, blocked, human-review, and release states.\n",
}

ARTIFACT_DIRS = [
    "docs/agents/issue-packets",
    "docs/agents/handoffs",
    "docs/agents/worker-results",
    "docs/agents/evidence",
    "docs/agents/release-results",
    "docs/agents/runtime",
    ".opencode/runtime",
]

LEGACY_PATHS = [
    ".opencode/commands/auto-dev.md",
    ".opencode/commands/supervisor-reconcile.md",
    ".opencode/commands/show-last-root-session.md",
    ".opencode/plugins/session-continuation.ts",
    ".opencode/plugins/session-continuation-tui.ts",
    "scripts/orchestrator_bootstrap_runner.py",
    "scripts/orchestrator_supervisor.py",
    "scripts/orchestrator_compact_payload.py",
    "scripts/agent_context_budget_check.py",
    "tests/scripts/test_orchestrator_bootstrap_runner.py",
    "tests/scripts/test_orchestrator_supervisor.py",
    "tests/scripts/test_orchestrator_compact_payload.py",
    "tests/scripts/test_agent_context_budget_check.py",
    "tests/opencode/session-continuation.test.js",
    "tests/opencode/session-continuation-tui.test.js",
    "docs/agents/autonomous-development-workflow.yaml",
    "docs/agents/issue-packet-template.yaml",
    "docs/agents/worker-result-template.yaml",
    "docs/agents/evidence-packet-template.yaml",
    "docs/agents/release-result-template.yaml",
    "docs/agents/runtime/context-checkpoint.yaml",
    ".opencode/runtime/orchestrator-ledger.json",
    ".opencode/runtime/new-session-request.json",
    ".opencode/runtime/new-session-result.json",
    ".opencode/runtime/compact-result.json",
]

HISTORICAL_ARTIFACT_DIRS = [
    "docs/agents/issue-packets",
    "docs/agents/handoffs",
    "docs/agents/worker-results",
    "docs/agents/evidence",
    "docs/agents/release-results",
]

def _command_templates() -> dict[str, str]:
    autodev_home = str(ROOT)
    return {
        "autodev-start.md": f"""---
description: Start autodev workflow for the current project and issue number
agent: build
subtask: false
---

Run autodev for issue number `$ARGUMENTS` in the current project.

1. Execute:
!`PYTHONPATH="{autodev_home}" python3 "{autodev_home}/scripts/autodev_project.py" start --project-root "$PWD" --issue-number "$1"`
2. Report the checkpoint, ledger, session result, and next action from the command output.

Notes:
- This is an autodev-owned global command. It discovers the target project from the current directory.
- Entrypoint: `scripts/autodev_project.py start`.
""",
        "autodev-reconcile.md": f"""---
description: Reconcile autodev runtime state for the current project
agent: build
subtask: false
---

Run:
!`PYTHONPATH="{autodev_home}" python3 "{autodev_home}/scripts/autodev_project.py" reconcile --project-root "$PWD"`

Report the supervisor decision and whether it requires a subagent or fresh main orchestrator session.
""",
        "autodev-show-session.md": f"""---
description: Show the latest autodev root session for the current project
agent: build
subtask: false
---

Run:
!`PYTHONPATH="{autodev_home}" python3 "{autodev_home}/scripts/autodev_project.py" show-session --project-root "$PWD"`

Report how to inspect or resume the latest root session.
""",
        "autodev-doctor.md": f"""---
description: Check whether the current project is ready for autodev
agent: build
subtask: false
---

Run:
!`PYTHONPATH="{autodev_home}" python3 "{autodev_home}/scripts/autodev_project.py" doctor --project-root "$PWD"`

Report any missing config, legacy residue, or command install problems.
""",
    }


@dataclass
class ActionReport:
    actions: list[str]
    findings: list[str]

    def has_findings(self) -> bool:
        return bool(self.findings)


def _project_root(path: str | None) -> Path:
    return Path(path or ".").resolve()


def _rel(path: Path, root: Path) -> str:
    return str(path.relative_to(root))


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_text(text, encoding="utf-8")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _ensure_checkpoint_file(root: Path) -> None:
    checkpoint_path = root / "docs/agents/runtime/context-checkpoint.yaml"
    if checkpoint_path.exists():
        return
    _write_text(checkpoint_path, _read_text(CHECKPOINT_TEMPLATE_PATH))


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
            "artifacts:",
            "  issue_packets: docs/agents/issue-packets",
            "  handoffs: docs/agents/handoffs",
            "  worker_results: docs/agents/worker-results",
            "  evidence: docs/agents/evidence",
            "  release_results: docs/agents/release-results",
            "  runtime: .opencode/runtime",
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
            "- Runtime artifacts: `.opencode/runtime/`",
            "- Issue artifacts: `docs/agents/issue-packets/`, `handoffs/`, `worker-results/`, `evidence/`, `release-results/`",
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
    del force
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

    gitkeep = root / ".opencode/runtime/.gitkeep"
    if not gitkeep.exists():
        report.actions.append("create .opencode/runtime/.gitkeep")
        if not dry_run and not check:
            _write_text(gitkeep, "")

    control_plane_db = root / ".opencode/runtime/control-plane.sqlite3"
    if not control_plane_db.exists():
        report.actions.append("create .opencode/runtime/control-plane.sqlite3")
        if not dry_run and not check:
            ensure_control_plane_db(root)

    checkpoint_path = root / "docs/agents/runtime/context-checkpoint.yaml"
    if not checkpoint_path.exists():
        report.actions.append("create docs/agents/runtime/context-checkpoint.yaml")
        if not dry_run and not check:
            _ensure_checkpoint_file(root)

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


def legacy_paths(root: Path) -> list[Path]:
    return [root / relative for relative in LEGACY_PATHS if (root / relative).exists()]


def historical_dirs(root: Path) -> list[Path]:
    return [root / relative for relative in HISTORICAL_ARTIFACT_DIRS if (root / relative).exists()]


def doctor_project(root: Path) -> ActionReport:
    report = ActionReport(actions=[], findings=[])
    if not (root / ".autodev.yaml").exists():
        report.findings.append("missing .autodev.yaml")
    if not (root / ".opencode/runtime/control-plane.sqlite3").exists():
        report.findings.append("missing .opencode/runtime/control-plane.sqlite3")
    agents_path = root / "AGENTS.md"
    if agents_path.exists():
        text = _read_text(agents_path)
        if text.count(AGENTS_BEGIN) > 1 or text.count(AGENTS_END) > 1:
            report.findings.append("duplicate autodev managed markers in AGENTS.md")
        elif (AGENTS_BEGIN in text) != (AGENTS_END in text):
            report.findings.append("unbalanced autodev managed markers in AGENTS.md")
    else:
        report.findings.append("missing AGENTS.md")
    for path in legacy_paths(root):
        report.findings.append(f"legacy residue: {_rel(path, root)}")
    return report


def _git_status_for(paths: list[Path], root: Path) -> list[str]:
    if not paths:
        return []
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--", *[_rel(path, root) for path in paths]],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return ["git status unavailable for legacy file safety check"]
    return [line for line in result.stdout.splitlines() if line.strip()]


def migrate_project(root: Path, *, dry_run: bool, remove_legacy: bool, skip_git_clean_check: bool) -> ActionReport:
    report = ActionReport(actions=[], findings=[])
    removable = legacy_paths(root)
    preserved = historical_dirs(root)

    for path in removable:
        report.actions.append(f"would remove {_rel(path, root)}" if dry_run or not remove_legacy else f"remove {_rel(path, root)}")
    for path in preserved:
        report.actions.append(f"preserve {_rel(path, root)}/")

    if remove_legacy and removable and not skip_git_clean_check:
        dirty = _git_status_for(removable, root)
        if dirty:
            report.findings.append("legacy files are dirty or git status is unavailable; rerun after commit/stash or use --skip-git-clean-check")
            report.findings.extend(dirty)
            return report

    if remove_legacy and not dry_run:
        for path in removable:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
    return report


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
        "--issue-packet",
        str(project_root / "docs/agents/issue-packets" / f"issue-{normalized}.yaml"),
        "--checkpoint",
        str(project_root / "docs/agents/runtime/context-checkpoint.yaml"),
        "--ledger",
        str(project_root / ".opencode/runtime/orchestrator-ledger.json"),
        "--new-session-request",
        str(project_root / ".opencode/runtime/new-session-request.json"),
        "--workflow-policy-path",
        str(ROOT / "docs/agents/autonomous-development-workflow.yaml"),
        "--dispatch-now",
        "--source-session-id",
        "autodev-start",
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Initialize an autodev consumer project")
    _ = init.add_argument("--project-root", default=".")
    _ = init.add_argument("--github-repo", default="paulpai0412/wferp")
    _ = init.add_argument("--dry-run", action="store_true")
    _ = init.add_argument("--check", action="store_true")
    _ = init.add_argument("--force", action="store_true")
    _ = init.add_argument("--json", action="store_true")

    install = subparsers.add_parser("install-commands", help="Install autodev-owned global OpenCode commands")
    _ = install.add_argument("--commands-dir", default=str(DEFAULT_COMMANDS_DIR))
    _ = install.add_argument("--dry-run", action="store_true")
    _ = install.add_argument("--force", action="store_true")
    _ = install.add_argument("--json", action="store_true")

    doctor = subparsers.add_parser("doctor", help="Check whether a project is ready for autodev")
    _ = doctor.add_argument("--project-root", default=".")
    _ = doctor.add_argument("--json", action="store_true")

    migrate = subparsers.add_parser("migrate", help="Report or remove legacy local workflow files")
    _ = migrate.add_argument("--project-root", default=".")
    _ = migrate.add_argument("--dry-run", action="store_true")
    _ = migrate.add_argument("--remove-legacy", action="store_true")
    _ = migrate.add_argument("--skip-git-clean-check", action="store_true")
    _ = migrate.add_argument("--json", action="store_true")

    start = subparsers.add_parser("start", help="Start autodev workflow for a project issue")
    _ = start.add_argument("--project-root", default=".")
    _ = start.add_argument("--issue-number", required=True)

    reconcile = subparsers.add_parser("reconcile", help="Reconcile autodev runtime state")
    _ = reconcile.add_argument("--project-root", default=".")

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
        report = doctor_project(_project_root(cast(str, args.project_root)))
        _print_report(report, json_output=json_output)
        return 1 if report.has_findings() else 0
    if command == "migrate":
        report = migrate_project(
            _project_root(cast(str, args.project_root)),
            dry_run=cast(bool, args.dry_run),
            remove_legacy=cast(bool, args.remove_legacy),
            skip_git_clean_check=cast(bool, args.skip_git_clean_check),
        )
        _print_report(report, json_output=json_output)
        return 1 if report.has_findings() else 0
    if command == "start":
        project_root = _project_root(cast(str, args.project_root))
        _ensure_checkpoint_file(project_root)
        return subprocess.run(
            ["python3", str(ROOT / "scripts/orchestrator_bootstrap_runner.py"), *_bootstrap_args(project_root, cast(str, args.issue_number))],
            cwd=project_root,
        ).returncode
    if command == "reconcile":
        project_root = _project_root(cast(str, args.project_root))
        return subprocess.run(
            ["python3", str(ROOT / "scripts/orchestrator_supervisor.py"), "reconcile", "--ledger", ".opencode/runtime/orchestrator-ledger.json", "--source-session-id", "autodev-reconcile"],
            cwd=project_root,
        ).returncode
    if command == "show-session":
        result_path = _project_root(cast(str, args.project_root)) / ".opencode/runtime/new-session-result.json"
        if not result_path.exists():
            print(f"no autodev session result found: {result_path}")
            return 1
        print(_read_text(result_path), end="")
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
