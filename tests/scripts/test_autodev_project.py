from __future__ import annotations

import json
import os
from pathlib import Path
from subprocess import CompletedProcess
import subprocess
from unittest.mock import patch

from pytest import CaptureFixture

from scripts import autodev_project
from scripts.control_plane_db import upsert_issue_state


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_text(text, encoding="utf-8")


def completed(args: list[str], *, returncode: int = 0, stdout: str = "", stderr: str = "") -> CompletedProcess[str]:
    return CompletedProcess(args=args, returncode=returncode, stdout=stdout, stderr=stderr)


def fake_init_bootstrap_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
    status_options = [{"id": f"opt_state_{state}", "name": state} for state in autodev_project.AUTODEV_PROJECT_STATUS_STATES]
    pr_options = [{"id": f"opt_pr_{state}", "name": state} for state in autodev_project.AUTODEV_PR_WORKFLOW_STATES]
    if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
        return completed(args, returncode=128, stderr="not a git repository")
    if args[:4] == ["git", "rev-parse", "--verify", "HEAD"]:
        return completed(args, returncode=128, stderr="fatal: Needed a single revision")
    if args[:4] == ["git", "remote", "get-url", "origin"]:
        return completed(args, returncode=2, stderr="no such remote")
    if args[:3] == ["gh", "repo", "view"]:
        return completed(args, returncode=1, stderr="not found")
    if args[:3] == ["git", "init", "-b"]:
        return completed(args, stdout="initialized")
    if args[:3] == ["git", "remote", "add"]:
        return completed(args)
    if args[:3] == ["gh", "repo", "create"]:
        return completed(args, stdout="created")
    if args[:3] == ["gh", "label", "create"]:
        return completed(args)
    if args[:4] == ["gh", "project", "list", "--owner"]:
        return completed(args, stdout="[]")
    if args[:4] == ["gh", "project", "create", "--owner"]:
        return completed(args, stdout=json.dumps({"id": "PVT_project_1", "number": 7, "title": "Autodev Control Plane (paulpai0412/autodev)"}))
    if args[:4] == ["gh", "project", "link", "7"]:
        return completed(args)
    if args[:4] == ["gh", "project", "field-list", "7"]:
        return completed(
            args,
            stdout=json.dumps(
                [
                    {
                        "id": "PVTF_state",
                        "name": "Status",
                        "options": status_options,
                    },
                    {"id": "PVTF_stage", "name": "Current Stage", "options": []},
                    {
                        "id": "PVTF_pr_workflow",
                        "name": "PR Workflow",
                        "options": pr_options,
                    },
                ]
            ),
        )
    if args[:4] == ["gh", "project", "field-create", "7"]:
        if "Status" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_state"}))
        if "Current Stage" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_stage"}))
        if "PR Workflow" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_pr_workflow"}))
    if args[:3] == ["gh", "api", "graphql"]:
        return completed(args, stdout=json.dumps({"data": {"updateProjectV2Field": {"projectV2Field": {"id": "PVTF_state"}}}}))
    raise AssertionError(f"unexpected command: {args}")


def fake_init_with_project_bootstrap_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
    if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
        return completed(args, returncode=128, stderr="not a git repository")
    if args[:4] == ["git", "rev-parse", "--verify", "HEAD"]:
        return completed(args, returncode=128, stderr="fatal: Needed a single revision")
    if args[:4] == ["git", "remote", "get-url", "origin"]:
        return completed(args, returncode=2, stderr="no such remote")
    if args[:3] == ["gh", "repo", "view"]:
        return completed(args, returncode=1, stderr="not found")
    if args[:3] == ["git", "init", "-b"]:
        return completed(args, stdout="initialized")
    if args[:3] == ["git", "remote", "add"]:
        return completed(args)
    if args[:3] == ["gh", "repo", "create"]:
        return completed(args, stdout="created")
    if args[:3] == ["gh", "label", "create"]:
        return completed(args)
    if args[:4] == ["gh", "project", "list", "--owner"]:
        return completed(args, stdout="[]")
    if args[:4] == ["gh", "project", "create", "--owner"]:
        return completed(args, stdout=json.dumps({"id": "PVT_project_1", "number": 7, "title": "Autodev Control Plane (paulpai0412/autodev)"}))
    if args[:4] == ["gh", "project", "link", "7"]:
        return completed(args)
    if args[:4] == ["gh", "project", "field-list", "7"]:
        return completed(args, stdout="[]")
    if args[:4] == ["gh", "project", "field-create", "7"]:
        if "Status" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_state"}))
        if "Current Stage" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_stage"}))
        if "PR Workflow" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_pr_workflow"}))
    if args[:3] == ["gh", "api", "graphql"]:
        return completed(args, stdout=json.dumps({"data": {"updateProjectV2Field": {"projectV2Field": {"id": "PVTF_state"}}}}))
    raise AssertionError(f"unexpected command: {args}")


def fake_init_with_existing_project_link_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
    if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
        return completed(args, returncode=128, stderr="not a git repository")
    if args[:4] == ["git", "rev-parse", "--verify", "HEAD"]:
        return completed(args, returncode=128, stderr="fatal: Needed a single revision")
    if args[:4] == ["git", "remote", "get-url", "origin"]:
        return completed(args, returncode=2, stderr="no such remote")
    if args[:3] == ["gh", "repo", "view"]:
        return completed(args, returncode=1, stderr="not found")
    if args[:3] == ["git", "init", "-b"]:
        return completed(args, stdout="initialized")
    if args[:3] == ["git", "remote", "add"]:
        return completed(args)
    if args[:3] == ["gh", "repo", "create"]:
        return completed(args, stdout="created")
    if args[:3] == ["gh", "label", "create"]:
        return completed(args)
    if args[:4] == ["gh", "project", "field-list", "7"]:
        return completed(
            args,
            stdout=json.dumps(
                [
                    {
                        "id": "PVTF_state",
                        "name": "Status",
                        "options": [{"id": "opt_ready", "name": "ready"}],
                    },
                    {"id": "PVTF_stage", "name": "Current Stage", "options": []},
                    {
                        "id": "PVTF_pr_workflow",
                        "name": "PR Workflow",
                        "options": [{"id": "opt_release_pending", "name": "release_pending"}],
                    },
                ]
            ),
        )
    if args[:4] == ["gh", "project", "link", "7"]:
        return completed(args)
    if args[:3] == ["gh", "api", "graphql"]:
        return completed(args, stdout=json.dumps({"data": {"updateProjectV2Field": {"projectV2Field": {"id": "PVTF_state"}}}}))
    raise AssertionError(f"unexpected command: {args}")


def fake_init_with_linked_repo_project_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
    status_options = [{"id": f"opt_state_{state}", "name": state} for state in autodev_project.AUTODEV_PROJECT_STATUS_STATES]
    pr_options = [{"id": f"opt_pr_{state}", "name": state} for state in autodev_project.AUTODEV_PR_WORKFLOW_STATES]
    if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
        return completed(args, returncode=128, stderr="not a git repository")
    if args[:4] == ["git", "rev-parse", "--verify", "HEAD"]:
        return completed(args, returncode=128, stderr="fatal: Needed a single revision")
    if args[:4] == ["git", "remote", "get-url", "origin"]:
        return completed(args, returncode=2, stderr="no such remote")
    if args[:3] == ["gh", "repo", "view"]:
        return completed(args, returncode=1, stderr="not found")
    if args[:3] == ["git", "init", "-b"]:
        return completed(args, stdout="initialized")
    if args[:3] == ["git", "remote", "add"]:
        return completed(args)
    if args[:3] == ["gh", "repo", "create"]:
        return completed(args, stdout="created")
    if args[:3] == ["gh", "label", "create"]:
        return completed(args)
    if args[:4] == ["gh", "project", "create", "--owner"]:
        raise AssertionError(f"project create should not be called when repo already linked: {args}")
    if args[:4] == ["gh", "project", "link", "9"]:
        return completed(args)
    if args[:4] == ["gh", "project", "field-list", "9"]:
        return completed(
            args,
            stdout=json.dumps(
                [
                    {
                        "id": "PVTF_state",
                        "name": "Status",
                        "options": status_options,
                    },
                    {"id": "PVTF_stage", "name": "Current Stage", "options": []},
                    {
                        "id": "PVTF_pr_workflow",
                        "name": "PR Workflow",
                        "options": pr_options,
                    },
                ]
            ),
        )
    if args[:4] == ["gh", "project", "field-create", "9"]:
        if "Status" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_state"}))
        if "Current Stage" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_stage"}))
        if "PR Workflow" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_pr_workflow"}))
    if args[:3] == ["gh", "api", "graphql"]:
        raw = " ".join(args)
        if "repository(owner:" in raw and "projectsV2" in raw:
            return completed(
                args,
                stdout=json.dumps(
                    {
                        "data": {
                            "repository": {
                                "projectsV2": {
                                    "nodes": [
                                        {
                                            "id": "PVT_project_linked_9",
                                            "number": 9,
                                            "title": "Existing Linked Project",
                                            "owner": {"login": "paulpai0412"},
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ),
            )
        return completed(args, stdout=json.dumps({"data": {"updateProjectV2Field": {"projectV2Field": {"id": "PVTF_state"}}}}))
    raise AssertionError(f"unexpected command: {args}")


def fake_init_with_multiple_linked_projects_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
    status_options = [{"id": f"opt_state_{state}", "name": state} for state in autodev_project.AUTODEV_PROJECT_STATUS_STATES]
    pr_options = [{"id": f"opt_pr_{state}", "name": state} for state in autodev_project.AUTODEV_PR_WORKFLOW_STATES]
    if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
        return completed(args, returncode=128, stderr="not a git repository")
    if args[:4] == ["git", "rev-parse", "--verify", "HEAD"]:
        return completed(args, returncode=128, stderr="fatal: Needed a single revision")
    if args[:4] == ["git", "remote", "get-url", "origin"]:
        return completed(args, returncode=2, stderr="no such remote")
    if args[:3] == ["gh", "repo", "view"]:
        return completed(args, returncode=1, stderr="not found")
    if args[:3] == ["git", "init", "-b"]:
        return completed(args, stdout="initialized")
    if args[:3] == ["git", "remote", "add"]:
        return completed(args)
    if args[:3] == ["gh", "repo", "create"]:
        return completed(args, stdout="created")
    if args[:3] == ["gh", "label", "create"]:
        return completed(args)
    if args[:4] == ["gh", "project", "create", "--owner"]:
        raise AssertionError(f"project create should not be called when repo already linked: {args}")
    if args[:4] == ["gh", "project", "link", "11"]:
        return completed(args)
    if args[:4] == ["gh", "project", "field-list", "11"]:
        return completed(
            args,
            stdout=json.dumps(
                [
                    {
                        "id": "PVTF_state",
                        "name": "Status",
                        "options": status_options,
                    },
                    {"id": "PVTF_stage", "name": "Current Stage", "options": []},
                    {
                        "id": "PVTF_pr_workflow",
                        "name": "PR Workflow",
                        "options": pr_options,
                    },
                ]
            ),
        )
    if args[:4] == ["gh", "project", "field-create", "11"]:
        if "Status" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_state"}))
        if "Current Stage" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_stage"}))
        if "PR Workflow" in args:
            return completed(args, stdout=json.dumps({"id": "PVTF_pr_workflow"}))
    if args[:3] == ["gh", "api", "graphql"]:
        raw = " ".join(args)
        if "repository(owner:" in raw and "projectsV2" in raw:
            return completed(
                args,
                stdout=json.dumps(
                    {
                        "data": {
                            "repository": {
                                "projectsV2": {
                                    "nodes": [
                                        {
                                            "id": "PVT_first",
                                            "number": 10,
                                            "title": "Another Linked Project",
                                            "owner": {"login": "paulpai0412"},
                                        },
                                        {
                                            "id": "PVT_target",
                                            "number": 11,
                                            "title": "Autodev Control Plane (paulpai0412/autodev)",
                                            "owner": {"login": "paulpai0412"},
                                        },
                                    ]
                                }
                            }
                        }
                    }
                ),
            )
        return completed(args, stdout=json.dumps({"data": {"updateProjectV2Field": {"projectV2Field": {"id": "PVTF_state"}}}}))
    raise AssertionError(f"unexpected command: {args}")


def test_init_creates_project_contract_dirs_and_agents_managed_block(tmp_path: Path):
    write(tmp_path / "AGENTS.md", "# Project Agents\n\nKeep this project-specific guidance.\n")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_init_bootstrap_run) as run:
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev",
            ]
        )

    assert exit_code == 0
    config = read(tmp_path / ".autodev.yaml")
    assert 'schema_version: "1.0"' in config
    assert "# Optional: enable GitHub Projects V2 projection by filling these values." in config
    assert "github_repo: paulpai0412/autodev" in config
    assert 'github_project_id: ""' in config
    assert '  pr_workflow: ""' in config
    assert "control_plane_db: .opencode/runtime/control-plane.sqlite3" in config
    assert "opencode_initial_session_id_timeout_seconds: 60" in config
    assert "state_projection:" in config
    assert "sqlite_to_primary_label:" in config
    assert "release_pending: agent-in-progress" in config
    assert "pr_workflow_to_label:" in config
    assert "AUTODEV_GITHUB_MONITORING:BEGIN" in config
    assert 'github_project_title: "Autodev Control Plane (paulpai0412/autodev)"' in config
    assert 'github_project_id: "PVT_project_1"' in config
    assert '  state: "PVTF_state"' in config
    assert '  stage: "PVTF_stage"' in config
    assert '  pr_workflow: "PVTF_pr_workflow"' in config
    assert (tmp_path / ".opencode/runtime/.gitkeep").exists()
    assert (tmp_path / ".opencode/runtime/control-plane.sqlite3").exists()
    entrypoints = autodev_project._operator_entrypoints()
    assert (tmp_path / ".opencode/commands" / entrypoints["start"]).exists()
    assert (tmp_path / ".opencode/commands" / entrypoints["reconcile"]).exists()
    assert (tmp_path / ".opencode/commands" / entrypoints["release"]).exists()
    assert (tmp_path / ".opencode/commands" / entrypoints["inspect"]).exists()
    assert (tmp_path / ".opencode/commands" / entrypoints["doctor"]).exists()
    assert (tmp_path / ".opencode/commands" / entrypoints["full_cycle"]).exists()
    start_command = read(tmp_path / ".opencode/commands" / entrypoints["start"])
    assert 'PYTHONPATH="$AUTODEV_HOME" python "$AUTODEV_HOME/scripts/autodev_project.py" start --project-root "$PWD" --issue-number "$1"' in start_command
    gitignore = read(tmp_path / ".gitignore")
    assert ".env" in gitignore
    assert "AGENTS.md" in gitignore
    assert ".autodev.yaml" in gitignore
    assert ".opencode/runtime/*" in gitignore
    assert "!.opencode/runtime/.gitkeep" in gitignore
    assert ".playwright-mcp/" in gitignore
    assert "artifacts/" in gitignore
    agents = read(tmp_path / "AGENTS.md")
    assert "Keep this project-specific guidance." in agents
    assert "<!-- AUTODEV:BEGIN -->" in agents
    assert "Do not copy workflow implementation" in agents
    commands = [call.args[0] for call in run.call_args_list]
    assert ["git", "init", "-b", "main"] in commands
    assert ["git", "remote", "add", "origin", "https://github.com/paulpai0412/autodev.git"] in commands
    assert ["gh", "repo", "create", "paulpai0412/autodev", "--private", "--description", autodev_project.DEFAULT_REPO_DESCRIPTION] in commands
    label_commands = [command for command in commands if isinstance(command, list) and command[:3] == ["gh", "label", "create"]]
    assert len(label_commands) == len(autodev_project.BOOTSTRAP_LABELS)


def test_init_can_opt_out_of_github_project_creation(tmp_path: Path):
    write(tmp_path / "AGENTS.md", "# Project Agents\n\nKeep this project-specific guidance.\n")

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return completed(args, returncode=128, stderr="not a git repository")
        if args[:4] == ["git", "rev-parse", "--verify", "HEAD"]:
            return completed(args, returncode=128, stderr="fatal: Needed a single revision")
        if args[:4] == ["git", "remote", "get-url", "origin"]:
            return completed(args, returncode=2, stderr="no such remote")
        if args[:3] == ["gh", "repo", "view"]:
            return completed(args, returncode=1, stderr="not found")
        if args[:3] == ["git", "init", "-b"]:
            return completed(args, stdout="initialized")
        if args[:3] == ["git", "remote", "add"]:
            return completed(args)
        if args[:3] == ["gh", "repo", "create"]:
            return completed(args, stdout="created")
        if args[:3] == ["gh", "label", "create"]:
            return completed(args)
        if args[:3] == ["gh", "project"]:
            raise AssertionError(f"project command should not be called: {args}")
        return completed(args)

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run):
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev",
                "--no-create-github-project",
            ]
        )

    assert exit_code == 0
    config = read(tmp_path / ".autodev.yaml")
    assert "AUTODEV_GITHUB_MONITORING:BEGIN" not in config


def test_init_create_github_project_writes_monitoring_block(tmp_path: Path):
    write(tmp_path / "AGENTS.md", "# Project Agents\n")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_init_with_project_bootstrap_run):
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev",
            ]
        )

    assert exit_code == 0
    config = read(tmp_path / ".autodev.yaml")
    assert "AUTODEV_GITHUB_MONITORING:BEGIN" in config
    assert 'github_project_id: "PVT_project_1"' in config
    assert 'state: "PVTF_state"' in config
    assert 'stage: "PVTF_stage"' in config
    assert 'pr_workflow: "PVTF_pr_workflow"' in config


def test_init_reuses_linked_repo_project_without_creating_new_project(tmp_path: Path):
    write(tmp_path / "AGENTS.md", "# Project Agents\n")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_init_with_linked_repo_project_run) as run:
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev",
            ]
        )

    assert exit_code == 0
    commands = [call.args[0] for call in run.call_args_list]
    assert [
        "gh",
        "project",
        "link",
        "9",
        "--owner",
        "paulpai0412",
        "--repo",
        "paulpai0412/autodev",
    ] in commands
    assert [
        "gh",
        "project",
        "create",
        "--owner",
        "paulpai0412",
        "--title",
        "Autodev Control Plane (paulpai0412/autodev)",
        "--format",
        "json",
    ] not in commands

    config = read(tmp_path / ".autodev.yaml")
    assert 'github_project_number: 9' in config
    assert 'github_project_id: "PVT_project_linked_9"' in config
    assert 'github_project_title: "Existing Linked Project"' in config


def test_init_reuses_title_matched_project_when_multiple_linked_projects_exist(tmp_path: Path):
    write(tmp_path / "AGENTS.md", "# Project Agents\n")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_init_with_multiple_linked_projects_run) as run:
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev",
            ]
        )

    assert exit_code == 0
    commands = [call.args[0] for call in run.call_args_list]
    assert [
        "gh",
        "project",
        "link",
        "11",
        "--owner",
        "paulpai0412",
        "--repo",
        "paulpai0412/autodev",
    ] in commands
    assert [
        "gh",
        "project",
        "create",
        "--owner",
        "paulpai0412",
        "--title",
        "Autodev Control Plane (paulpai0412/autodev)",
        "--format",
        "json",
    ] not in commands

    config = read(tmp_path / ".autodev.yaml")
    assert 'github_project_number: 11' in config
    assert 'github_project_id: "PVT_target"' in config
    assert 'github_project_title: "Autodev Control Plane (paulpai0412/autodev)"' in config


def test_init_links_repo_to_existing_github_project_when_monitoring_exists(tmp_path: Path):
    write(
        tmp_path / ".autodev.yaml",
        "\n".join(
            [
                'schema_version: "1.0"',
                "project:",
                "  name: demo",
                f"  root: {tmp_path}",
                "  github_repo: paulpai0412/autodev",
                "",
                "# AUTODEV_GITHUB_MONITORING:BEGIN",
                'github_project_owner: "paulpai0412"',
                'github_project_title: "Autodev Control Plane (paulpai0412/autodev)"',
                "github_project_number: 7",
                'github_project_id: "PVT_project_1"',
                "github_project_field_ids:",
                '  state: "PVTF_state"',
                '  stage: "PVTF_stage"',
                '  pr_workflow: "PVTF_pr_workflow"',
                "github_project_field_option_ids:",
                "  state:",
                '    ready: "opt_ready"',
                "  pr_workflow:",
                '    release_pending: "opt_release_pending"',
                "# AUTODEV_GITHUB_MONITORING:END",
                "",
            ]
        ),
    )
    write(tmp_path / "AGENTS.md", "# Project Agents\n")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_init_with_existing_project_link_run) as run:
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev",
                "--no-create-github-project",
            ]
        )

    assert exit_code == 0
    commands = [call.args[0] for call in run.call_args_list]
    assert [
        "gh",
        "project",
        "link",
        "7",
        "--owner",
        "paulpai0412",
        "--repo",
        "paulpai0412/autodev",
    ] in commands
    config = read(tmp_path / ".autodev.yaml")
    assert 'github_project_id: "PVT_project_1"' in config
    env_text = read(tmp_path / ".env")
    assert "AUTODEV_GITHUB_PROJECT_ID=PVT_project_1" in env_text


def test_init_resolves_existing_github_project_by_title_and_backfills_monitoring_block(tmp_path: Path):
    write(
        tmp_path / ".autodev.yaml",
        "\n".join(
            [
                'schema_version: "1.0"',
                "project:",
                "  name: demo",
                f"  root: {tmp_path}",
                "  github_repo: paulpai0412/autodev",
                "",
                "# AUTODEV_GITHUB_MONITORING:BEGIN",
                'github_project_owner: "paulpai0412"',
                'github_project_title: "Autodev Control Plane (paulpai0412/autodev)"',
                'github_project_id: "PVT_stale_project"',
                "github_project_field_ids:",
                '  state: ""',
                '  stage: ""',
                '  pr_workflow: ""',
                "github_project_field_option_ids:",
                "  state:",
                "  pr_workflow:",
                "# AUTODEV_GITHUB_MONITORING:END",
                "",
            ]
        ),
    )
    write(tmp_path / "AGENTS.md", "# Project Agents\n")

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return completed(args, returncode=128, stderr="not a git repository")
        if args[:4] == ["git", "rev-parse", "--verify", "HEAD"]:
            return completed(args, returncode=128, stderr="fatal: Needed a single revision")
        if args[:4] == ["git", "remote", "get-url", "origin"]:
            return completed(args, returncode=2, stderr="no such remote")
        if args[:3] == ["gh", "repo", "view"]:
            return completed(args, returncode=1, stderr="not found")
        if args[:3] == ["git", "init", "-b"]:
            return completed(args, stdout="initialized")
        if args[:3] == ["git", "remote", "add"]:
            return completed(args)
        if args[:3] == ["gh", "repo", "create"]:
            return completed(args, stdout="created")
        if args[:3] == ["gh", "label", "create"]:
            return completed(args)
        if args[:4] == ["gh", "project", "list", "--owner"]:
            return completed(
                args,
                stdout=json.dumps(
                    [
                        {
                            "id": "PVT_project_1",
                            "number": 7,
                            "title": "Autodev Control Plane (paulpai0412/autodev)",
                        }
                    ]
                ),
            )
        if args[:4] == ["gh", "project", "field-list", "7"]:
            return completed(
                args,
                stdout=json.dumps(
                    [
                        {
                            "id": "PVTF_state",
                            "name": "Status",
                            "options": [{"id": "opt_ready", "name": "ready"}],
                        },
                        {"id": "PVTF_stage", "name": "Current Stage", "options": []},
                        {
                            "id": "PVTF_pr_workflow",
                            "name": "PR Workflow",
                            "options": [{"id": "opt_release_pending", "name": "release_pending"}],
                        },
                    ]
                ),
            )
        if args[:4] == ["gh", "project", "link", "7"]:
            return completed(args)
        if args[:3] == ["gh", "api", "graphql"]:
            return completed(args, stdout=json.dumps({"data": {"updateProjectV2Field": {"projectV2Field": {"id": "PVTF_state"}}}}))
        raise AssertionError(f"unexpected command: {args}")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run):
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev",
                "--no-create-github-project",
            ]
        )

    assert exit_code == 0
    config = read(tmp_path / ".autodev.yaml")
    assert "github_project_number: 7" in config
    assert 'github_project_id: "PVT_project_1"' in config
    assert '  state: "PVTF_state"' in config
    assert '  stage: "PVTF_stage"' in config
    assert '  pr_workflow: "PVTF_pr_workflow"' in config
    env_text = read(tmp_path / ".env")
    assert "AUTODEV_GITHUB_PROJECT_ID=PVT_project_1" in env_text


def test_extract_monitoring_prefers_managed_block_over_legacy_top_level_fields() -> None:
    config_text = "\n".join(
        [
            'schema_version: "1.0"',
            'github_project_id: "PVT_legacy"',
            'github_project_field_ids:',
            '  state: "PVTF_legacy_state"',
            '  stage: "PVTF_legacy_stage"',
            '',
            '# AUTODEV_GITHUB_MONITORING:BEGIN',
            'github_project_owner: "paulpai0412"',
            'github_project_title: "vocab1 Project"',
            'github_project_number: 10',
            'github_project_id: "PVT_managed"',
            'github_project_field_ids:',
            '  state: "PVTF_managed_state"',
            '  stage: "PVTF_managed_stage"',
            '  pr_workflow: "PVTF_managed_pr"',
            'github_project_field_option_ids:',
            '  state:',
            '    ready: "opt_ready"',
            '  pr_workflow:',
            '    opened: "opt_opened"',
            '# AUTODEV_GITHUB_MONITORING:END',
            '',
        ]
    )

    project_id, field_ids, field_option_ids = autodev_project._extract_monitoring_from_config(config_text)

    assert project_id == "PVT_managed"
    assert field_ids == {
        "state": "PVTF_managed_state",
        "stage": "PVTF_managed_stage",
        "pr_workflow": "PVTF_managed_pr",
    }
    assert field_option_ids == {
        "state": {"ready": "opt_ready"},
        "pr_workflow": {"opened": "opt_opened"},
    }


def test_init_ignores_mismatched_github_project_owner_override(tmp_path: Path):
    write(tmp_path / "AGENTS.md", "# Project Agents\n")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_init_with_project_bootstrap_run):
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "tcci-timmy/letter",
                "--github-project-owner",
                "paulpai0412",
            ]
        )

    assert exit_code == 1
    config = read(tmp_path / ".autodev.yaml")
    assert 'github_project_owner: "tcci-timmy"' in config
    assert 'github_project_title: "Autodev Control Plane (tcci-timmy/letter)"' in config


def test_init_dry_run_writes_nothing(tmp_path: Path):
    with patch("scripts.autodev_project.subprocess.run") as run:
        exit_code = autodev_project.main(["init", "--project-root", str(tmp_path), "--dry-run"])

    assert exit_code == 0
    assert not (tmp_path / ".autodev.yaml").exists()
    assert not (tmp_path / ".autodev.yaml").exists()
    run.assert_not_called()


def test_install_commands_writes_autodev_prefixed_global_commands(tmp_path: Path):
    commands_dir = tmp_path / "commands"
    entrypoints = autodev_project._operator_entrypoints()

    exit_code = autodev_project.main(
        ["install-commands", "--commands-dir", str(commands_dir)]
    )

    assert exit_code == 0
    start_command = read(commands_dir / entrypoints["start"])
    assert "description: Start autodev workflow" in start_command
    assert "scripts/autodev_project.py start" in start_command
    assert '--issue-number "$1"' in start_command
    assert f'AUTODEV_HOME="${{AUTODEV_HOME:-{autodev_project.ROOT}}}"' in start_command
    assert 'PYTHONPATH="$AUTODEV_HOME" python "$AUTODEV_HOME/scripts/autodev_project.py" start' in start_command
    assert str(tmp_path) not in start_command
    assert (commands_dir / entrypoints["reconcile"]).exists()
    assert (commands_dir / entrypoints["release"]).exists()
    assert (commands_dir / entrypoints["inspect"]).exists()
    assert (commands_dir / entrypoints["doctor"]).exists()
    assert (commands_dir / entrypoints["full_cycle"]).exists()


def test_install_commands_uses_fake_host_adapter_entrypoints(tmp_path: Path):
    commands_dir = tmp_path / "commands"

    class FakeAdapter:
        def operator_entrypoints(self) -> dict[str, str]:
            return {
                "start": "fake-start.md",
                "reconcile": "fake-reconcile.md",
                "release": "fake-release.md",
                "inspect": "fake-inspect.md",
                "doctor": "fake-doctor.md",
                "full_cycle": "fake-full-cycle.md",
            }

        def capabilities(self) -> dict[str, object]:
            return {"commands_dir": str(commands_dir)}

    with patch("scripts.autodev_project._host_adapter", return_value=FakeAdapter()):
        exit_code = autodev_project.main(["install-commands", "--commands-dir", str(commands_dir)])

    assert exit_code == 0
    assert (commands_dir / "fake-start.md").exists()
    assert (commands_dir / "fake-reconcile.md").exists()
    assert (commands_dir / "fake-release.md").exists()
    assert (commands_dir / "fake-inspect.md").exists()
    assert (commands_dir / "fake-doctor.md").exists()
    assert (commands_dir / "fake-full-cycle.md").exists()


def test_install_commands_defaults_to_host_adapter_commands_dir(tmp_path: Path):
    commands_dir = tmp_path / "host-commands"

    class FakeAdapter:
        def operator_entrypoints(self) -> dict[str, str]:
            return {
                "start": "fake-start.md",
                "reconcile": "fake-reconcile.md",
                "release": "fake-release.md",
                "inspect": "fake-inspect.md",
                "doctor": "fake-doctor.md",
                "full_cycle": "fake-full-cycle.md",
            }

        def capabilities(self) -> dict[str, object]:
            return {"commands_dir": str(commands_dir)}

    with patch("scripts.autodev_project._host_adapter", return_value=FakeAdapter()):
        exit_code = autodev_project.main(["install-commands"])

    assert exit_code == 0
    assert (commands_dir / "fake-start.md").exists()
    assert (commands_dir / "fake-reconcile.md").exists()
    assert (commands_dir / "fake-release.md").exists()
    assert (commands_dir / "fake-inspect.md").exists()
    assert (commands_dir / "fake-doctor.md").exists()
    assert (commands_dir / "fake-full-cycle.md").exists()


def test_default_commands_dir_uses_windows_appdata(tmp_path: Path):
    with patch("scripts.runtime_exec.platform.system", return_value="Windows"), patch.dict(
        os.environ,
        {"APPDATA": str(tmp_path)},
        clear=False,
    ):
        commands_dir = autodev_project._default_commands_dir()

    assert commands_dir == tmp_path / "opencode" / "commands"


def test_default_commands_dir_uses_codex_commands_dir_when_adapter_is_codex(tmp_path: Path):
    class FakeCodexAdapter:
        def operator_entrypoints(self) -> dict[str, str]:
            return {
                "start": "autodev-start.md",
                "reconcile": "autodev-reconcile.md",
                "release": "autodev-release.md",
                "inspect": "autodev-show-session.md",
                "doctor": "autodev-doctor.md",
                "full_cycle": "autodev-full-cycle.md",
            }

        def capabilities(self) -> dict[str, object]:
            return {"host": "codex", "commands_dir": str(tmp_path / "codex" / "commands")}

    with patch("scripts.autodev_project._host_adapter", return_value=FakeCodexAdapter()):
        commands_dir = autodev_project._default_commands_dir()

    assert commands_dir == tmp_path / "codex" / "commands"


def test_repo_local_commands_use_autodev_project_wrappers():
    start_command = read(autodev_project.ROOT / ".opencode/commands/autodev-start.md")
    reconcile_command = read(autodev_project.ROOT / ".opencode/commands/autodev-reconcile.md")
    release_command = read(autodev_project.ROOT / ".opencode/commands/autodev-release.md")
    show_command = read(autodev_project.ROOT / ".opencode/commands/autodev-show-session.md")
    full_cycle_command = read(autodev_project.ROOT / ".opencode/commands/autodev-full-cycle.md")

    assert f'AUTODEV_HOME="${{AUTODEV_HOME:-{autodev_project.ROOT}}}"' in start_command
    assert 'PYTHONPATH="$AUTODEV_HOME" python "$AUTODEV_HOME/scripts/autodev_project.py" start --project-root "$PWD" --issue-number "$1"' in start_command
    assert 'PYTHONPATH="$AUTODEV_HOME" python "$AUTODEV_HOME/scripts/autodev_project.py" reconcile --project-root "$PWD"' in reconcile_command
    assert 'PYTHONPATH="$AUTODEV_HOME" python "$AUTODEV_HOME/scripts/autodev_project.py" release --project-root "$PWD" --issue-number "$1" --auto-approve' in release_command
    assert 'PYTHONPATH="$AUTODEV_HOME" python "$AUTODEV_HOME/scripts/autodev_project.py" show-session --project-root "$PWD"' in show_command
    assert 'python "$AUTODEV_HOME/scripts/autodev_full_cycle.py"' in full_cycle_command


def test_doctor_reports_missing_control_plane_db(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")

    exit_code = autodev_project.main(["doctor", "--project-root", str(tmp_path)])

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "missing .opencode/runtime/control-plane.sqlite3" in captured.out


def test_doctor_reports_incomplete_github_monitoring_block(tmp_path: Path, capsys: CaptureFixture[str]):
    write(
        tmp_path / ".autodev.yaml",
        '\n'.join(
            [
                'schema_version: "1.0"',
                'project:',
                '  name: demo',
                '# AUTODEV_GITHUB_MONITORING:BEGIN',
                'github_project_title: "Autodev Control Plane (paulpai0412/autodev)"',
                'github_project_field_ids:',
                '  state: "PVTF_state"',
                '# AUTODEV_GITHUB_MONITORING:END',
                '',
            ]
        ),
    )
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")

    exit_code = autodev_project.main(["doctor", "--project-root", str(tmp_path)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "missing github_project_id in .autodev.yaml monitoring block" in captured.out
    assert "missing github_project_field_ids.stage" in captured.out
    assert "missing github_project_field_ids.pr_workflow" in captured.out


def test_doctor_reports_tracked_runtime_files(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")
    write(tmp_path / ".gitignore", "# existing\n")
    write(tmp_path / ".opencode/runtime/control-plane.sqlite3", "")

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return completed(args, stdout="true\n")
        if args[:3] == ["git", "ls-files", ".opencode/runtime"]:
            return completed(args, stdout=".opencode/runtime/.gitkeep\n.opencode/runtime/control-plane.sqlite3\n")
        raise AssertionError(f"unexpected command: {args}")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run):
        exit_code = autodev_project.main(["doctor", "--project-root", str(tmp_path)])

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "missing .gitignore entries for autodev runtime/tool artifacts" in captured.out
    assert "tracked autodev runtime files must be removed from git index: .opencode/runtime/control-plane.sqlite3" in captured.out


def test_ensure_runtime_gitignore_backfills_local_artifact_lines_without_duplication(tmp_path: Path) -> None:
    write(
        tmp_path / ".gitignore",
        "# existing\n.opencode/runtime/*\n.opencode/runtime/control-plane.sqlite3\n!.opencode/runtime/.gitkeep\n",
    )

    report = autodev_project.ActionReport(actions=[], findings=[])

    autodev_project._ensure_runtime_gitignore(tmp_path, dry_run=False, check=False, report=report)

    gitignore = read(tmp_path / ".gitignore")

    assert ".env" in gitignore
    assert "AGENTS.md" in gitignore
    assert ".autodev.yaml" in gitignore
    assert gitignore.count(".opencode/runtime/*") == 1
    assert gitignore.count(".opencode/runtime/control-plane.sqlite3") == 1
    assert ".playwright-mcp/" in gitignore
    assert "artifacts/" in gitignore


def test_runtime_gitignore_requires_explicit_control_plane_db_line(tmp_path: Path) -> None:
    write(tmp_path / ".gitignore", ".opencode/runtime/*\n!.opencode/runtime/.gitkeep\n")

    assert autodev_project._runtime_gitignore_is_configured(tmp_path) is False


def test_runtime_gitignore_requires_autodev_yaml_line(tmp_path: Path) -> None:
    write(
        tmp_path / ".gitignore",
        "# local env and agent files\n.env\nAGENTS.md\n"
        "# autodev runtime state\n.opencode/runtime/*\n.opencode/runtime/control-plane.sqlite3\n!.opencode/runtime/.gitkeep\n"
        "# local tool and test artifacts\n.playwright-mcp/\nartifacts/\n",
    )

    assert autodev_project._runtime_gitignore_is_configured(tmp_path) is False


def test_doctor_passes_freshly_initialized_project(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_init_bootstrap_run):
        init_exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev-demo-todo",
            ]
        )
    assert init_exit_code == 0
    _ = capsys.readouterr()

    exit_code = autodev_project.main(["doctor", "--project-root", str(tmp_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == "autodev project: no changes needed\n"


def test_doctor_windows_preflight_reports_missing_tools(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")
    write(tmp_path / ".opencode/runtime/control-plane.sqlite3", "")

    def fake_which(command: str) -> str | None:
        if command in {"git", "gh", "python", "python3", "opencode", "opencode.exe", "opencode-desktop"}:
            return None
        return f"/mock/{command}"

    with patch("scripts.autodev_project.platform.system", return_value="Windows"), patch(
        "scripts.autodev_project.shutil.which", side_effect=fake_which
    ), patch("scripts.autodev_project.resolved_python_executable", return_value="python"), patch(
        "scripts.autodev_project.resolve_opencode_cli", return_value=None
    ):
        exit_code = autodev_project.main(["doctor", "--project-root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "windows preflight: Python executable not found" in captured.out
    assert "windows preflight: `git` not found in PATH" in captured.out
    assert "windows preflight: `gh` (GitHub CLI) not found in PATH" in captured.out
    assert "windows preflight: OpenCode CLI not found" in captured.out


def test_doctor_windows_preflight_passes_when_tools_exist(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")
    write(tmp_path / ".opencode/runtime/control-plane.sqlite3", "")

    def fake_which(command: str) -> str | None:
        mapping = {
            "git": "C:/Program Files/Git/cmd/git.exe",
            "gh": "C:/Program Files/GitHub CLI/gh.exe",
            "opencode": None,
            "opencode.exe": "C:/Users/demo/AppData/Local/opencode/opencode.exe",
            "opencode-desktop": None,
            "python": "C:/Python311/python.exe",
        }
        return mapping.get(command, f"C:/mock/{command}.exe")

    with patch("scripts.autodev_project.platform.system", return_value="Windows"), patch(
        "scripts.autodev_project.shutil.which", side_effect=fake_which
    ), patch("scripts.autodev_project.resolved_python_executable", return_value="python"), patch(
        "scripts.autodev_project.resolve_opencode_cli", return_value="C:/Users/demo/AppData/Local/opencode/opencode.exe"
    ):
        exit_code = autodev_project.main(["doctor", "--project-root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == "autodev project: no changes needed\n"


def test_doctor_windows_preflight_reports_missing_codex_cli_when_host_is_codex(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")
    write(tmp_path / ".opencode/runtime/control-plane.sqlite3", "")

    def fake_which(command: str) -> str | None:
        mapping = {
            "git": "C:/Program Files/Git/cmd/git.exe",
            "gh": "C:/Program Files/GitHub CLI/gh.exe",
            "python": "C:/Python311/python.exe",
            "python3": "C:/Python311/python.exe",
            "codex": None,
            "codex.exe": None,
        }
        return mapping.get(command, f"/mock/{command}")

    with patch("scripts.autodev_project.platform.system", return_value="Windows"), patch(
        "scripts.autodev_project.shutil.which", side_effect=fake_which
    ), patch("scripts.autodev_project.resolved_python_executable", return_value="python"), patch(
        "scripts.autodev_project.configured_host_adapter_name", return_value="codex"
    ), patch(
        "scripts.autodev_project.resolve_codex_cli", return_value=None
    ):
        exit_code = autodev_project.main(["doctor", "--project-root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "windows preflight: Codex CLI not found" in captured.out


def test_direct_script_doctor_works_without_pythonpath(tmp_path: Path):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")
    write(tmp_path / ".opencode/runtime/control-plane.sqlite3", "")

    env = os.environ.copy()
    env.pop("PYTHONPATH", None)

    completed = subprocess.run(
        [
            autodev_project.resolved_python_executable(),
            str(autodev_project.ROOT / "scripts/autodev_project.py"),
            "doctor",
            "--project-root",
            str(tmp_path),
        ],
        cwd=autodev_project.ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert completed.returncode == 0
    assert completed.stdout == "autodev project: no changes needed\n"
    assert completed.stderr == ""


def test_init_updates_origin_when_force_is_set(tmp_path: Path):
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return completed(args, stdout="true\n")
        if args[:4] == ["git", "rev-parse", "--verify", "HEAD"]:
            return completed(args, stdout="abc123\n")
        if args[:4] == ["git", "remote", "get-url", "origin"]:
            return completed(args, stdout="https://github.com/example/old.git\n")
        if args[:3] == ["gh", "repo", "view"]:
            return completed(args, stdout="repo exists")
        if args[:4] == ["git", "remote", "set-url", "origin"]:
            return completed(args)
        if args[:3] == ["gh", "label", "create"]:
            return completed(args)
        raise AssertionError(f"unexpected command: {args}")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run) as run:
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev-demo-todo",
                "--force",
                "--no-create-github-project",
            ]
        )

    assert exit_code == 0
    commands = [call.args[0] for call in run.call_args_list]
    assert ["git", "remote", "set-url", "origin", "https://github.com/paulpai0412/autodev-demo-todo.git"] in commands


def test_init_reports_origin_mismatch_without_force(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return completed(args, stdout="true\n")
        if args[:4] == ["git", "rev-parse", "--verify", "HEAD"]:
            return completed(args, stdout="abc123\n")
        if args[:4] == ["git", "remote", "get-url", "origin"]:
            return completed(args, stdout="https://github.com/example/old.git\n")
        if args[:3] == ["gh", "repo", "view"]:
            return completed(args, stdout="repo exists")
        if args[:3] == ["gh", "label", "create"]:
            return completed(args)
        raise AssertionError(f"unexpected command: {args}")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run):
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev-demo-todo",
            ]
        )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "origin remote points to https://github.com/example/old.git; expected https://github.com/paulpai0412/autodev-demo-todo.git" in captured.out


def test_init_seeds_local_main_from_origin_for_unborn_repo(tmp_path: Path):
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return completed(args, stdout="true\n")
        if args[:4] == ["git", "rev-parse", "--verify", "HEAD"]:
            return completed(args, returncode=128, stderr="fatal: Needed a single revision")
        if args[:4] == ["git", "remote", "get-url", "origin"]:
            return completed(args, stdout="https://github.com/paulpai0412/autodev-demo-todo.git\n")
        if args[:4] == ["gh", "repo", "view", "paulpai0412/autodev-demo-todo"]:
            return completed(args, stdout="repo exists\n")
        if args[:4] == ["git", "fetch", "origin", "main"]:
            return completed(args)
        if args[:5] == ["git", "rev-parse", "--verify", "--quiet", "refs/remotes/origin/main"]:
            return completed(args, stdout="db3001170851e85a95aadcc5f68097521ca1addb\n")
        if args[:5] == ["git", "checkout", "-B", "main", "refs/remotes/origin/main"]:
            return completed(args)
        if args[:3] == ["gh", "label", "create"]:
            return completed(args)
        raise AssertionError(f"unexpected command: {args}")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run) as run:
        exit_code = autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "paulpai0412/autodev-demo-todo",
                "--no-create-github-project",
            ]
        )

    assert exit_code == 0
    commands = [call.args[0] for call in run.call_args_list]
    assert ["git", "fetch", "origin", "main"] in commands
    assert ["git", "checkout", "-B", "main", "refs/remotes/origin/main"] in commands


def test_init_rejects_invalid_github_repo_slug(tmp_path: Path):
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")

    try:
        autodev_project.main(
            [
                "init",
                "--project-root",
                str(tmp_path),
                "--github-repo",
                "bad slug",
            ]
        )
    except ValueError as error:
        assert "github_repo must be owner/repo" in str(error)
    else:
        raise AssertionError("expected invalid github_repo slug to be rejected")


def test_config_text_supports_windows_root_backslashes() -> None:
    root = Path(r"D:\myai\letter")

    config = autodev_project._config_text(root, "tcci-timmy/letter")

    assert f"  root: {root}" in config
    assert "  github_repo: tcci-timmy/letter" in config


def test_main_reports_json_when_requested(tmp_path: Path, capsys: CaptureFixture[str]):
    exit_code = autodev_project.main(
        ["doctor", "--project-root", str(tmp_path), "--json"]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    payload = json.loads(captured.out)
    assert payload["status"] == "fail"
    assert "missing .autodev.yaml" in payload["findings"]


def test_start_uses_consumer_project_artifact_paths(tmp_path: Path):
    with patch(
        "scripts.autodev_project.subprocess.run",
        return_value=CompletedProcess(args=[autodev_project.resolved_python_executable()], returncode=0),
    ) as run:
        exit_code = autodev_project.main(
            ["start", "--project-root", str(tmp_path), "--issue-number", "34"]
        )

    assert exit_code == 0
    command = run.call_args.args[0]
    kwargs = run.call_args.kwargs
    assert command[:4] == [autodev_project.resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", "start-issue"]
    assert ["--base-dir", str(tmp_path)] in [command[i:i+2] for i in range(len(command)-1)]
    assert ["--issue-number", "34"] in [command[i:i+2] for i in range(len(command)-1)]
    assert ["--source-session-id", "autodev-start"] in [command[i:i+2] for i in range(len(command)-1)]
    assert "--approval-override-mode" not in command
    assert "--override-source" not in command
    assert "--human-approval-skipped" not in command
    assert kwargs["cwd"] == tmp_path
    assert kwargs["env"]["PYTHONPATH"].split(autodev_project.os.pathsep)[0] == str(autodev_project.ROOT)


def test_release_auto_approve_uses_release_only_override(tmp_path: Path):
    with patch(
        "scripts.autodev_project.subprocess.run",
        return_value=CompletedProcess(args=[autodev_project.resolved_python_executable()], returncode=0),
    ) as run:
        exit_code = autodev_project.main(
            ["release", "--project-root", str(tmp_path), "--issue-number", "34", "--auto-approve"]
        )

    assert exit_code == 0
    command = run.call_args.args[0]
    kwargs = run.call_args.kwargs
    assert command[:4] == [autodev_project.resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", "release"]
    assert ["--base-dir", str(tmp_path)] in [command[i:i+2] for i in range(len(command)-1)]
    assert ["--issue-number", "34"] in [command[i:i+2] for i in range(len(command)-1)]
    assert ["--source-session-id", "autodev-release"] in [command[i:i+2] for i in range(len(command)-1)]
    assert ["--approval-override-mode", "bypass_approval"] in [command[i:i+2] for i in range(len(command)-1)]
    assert ["--override-source", "user_requested_autodev_release"] in [command[i:i+2] for i in range(len(command)-1)]
    assert "--human-approval-skipped" in command
    assert kwargs["cwd"] == tmp_path


def test_reconcile_uses_consumer_project_runtime_paths_and_dispatches_next_session(tmp_path: Path):
    upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )
    with patch(
        "scripts.autodev_project.subprocess.run",
        return_value=CompletedProcess(args=[autodev_project.resolved_python_executable()], returncode=0),
    ) as run:
        exit_code = autodev_project.main(
            ["reconcile", "--project-root", str(tmp_path)]
    )

    assert exit_code == 0
    command = run.call_args.args[0]
    kwargs = run.call_args.kwargs
    assert command[:4] == [autodev_project.resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", "reconcile-workspace"]
    assert ["--base-dir", str(tmp_path)] in [command[i:i+2] for i in range(len(command)-1)]
    assert kwargs["cwd"] == tmp_path
    assert kwargs["env"]["PYTHONPATH"].split(autodev_project.os.pathsep)[0] == str(autodev_project.ROOT)


def test_start_resolves_consumer_project_root_from_nested_directory(tmp_path: Path):
    nested = tmp_path / "packages/app"
    nested.mkdir(parents=True)
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')

    with patch(
        "scripts.autodev_project.subprocess.run",
        return_value=CompletedProcess(args=[autodev_project.resolved_python_executable()], returncode=0),
    ) as run:
        exit_code = autodev_project.main(
            ["start", "--project-root", str(nested), "--issue-number", "34"]
        )

    assert exit_code == 0
    kwargs = run.call_args.kwargs
    command = run.call_args.args[0]
    assert kwargs["cwd"] == tmp_path
    assert ["--base-dir", str(tmp_path)] in [command[i:i+2] for i in range(len(command)-1)]
    assert not (nested / "docs").exists()
    assert not (tmp_path / "docs/agents/runtime/context-checkpoint.yaml").exists()


def test_start_resolves_canonical_project_root_from_issue_worktree_path(tmp_path: Path):
    issue_worktree = tmp_path / ".opencode/runtime/issue-worktrees/issue-42"
    issue_worktree.mkdir(parents=True, exist_ok=True)
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')

    with patch(
        "scripts.autodev_project.subprocess.run",
        return_value=CompletedProcess(args=[autodev_project.resolved_python_executable()], returncode=0),
    ) as run:
        exit_code = autodev_project.main(
            ["start", "--project-root", str(issue_worktree), "--issue-number", "34"]
        )

    assert exit_code == 0
    kwargs = run.call_args.kwargs
    command = run.call_args.args[0]
    assert kwargs["cwd"] == tmp_path
    assert ["--base-dir", str(tmp_path)] in [command[i:i+2] for i in range(len(command)-1)]


def test_reconcile_resolves_consumer_project_root_from_nested_directory(tmp_path: Path):
    nested = tmp_path / "packages/app"
    nested.mkdir(parents=True)
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="running",
        command_id="cmd-running",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-42",
    )

    with patch(
        "scripts.autodev_project.subprocess.run",
        return_value=CompletedProcess(args=[autodev_project.resolved_python_executable()], returncode=0),
    ) as run:
        exit_code = autodev_project.main(["reconcile", "--project-root", str(nested)])

    assert exit_code == 0
    assert run.call_args.kwargs["cwd"] == tmp_path


def test_reconcile_uses_workspace_db_runtime_paths(tmp_path: Path):
    upsert_issue_state(
        tmp_path,
        issue_number="41",
        state="running",
        command_id="cmd-running-41",
        updated_at="2026-05-07T17:00:00+08:00",
        current_session_id="ses-root-41",
    )
    upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="verifying",
        command_id="cmd-running-42",
        updated_at="2026-05-07T17:05:00+08:00",
        current_session_id="ses-root-42",
    )

    with patch(
        "scripts.autodev_project.subprocess.run",
        return_value=CompletedProcess(args=[autodev_project.resolved_python_executable()], returncode=0),
    ) as run:
        exit_code = autodev_project.main(["reconcile", "--project-root", str(tmp_path)])

    assert exit_code == 0
    command = run.call_args.args[0]
    assert command[:4] == [autodev_project.resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", "reconcile-workspace"]
    assert ["--base-dir", str(tmp_path)] in [command[i:i+2] for i in range(len(command)-1)]


def test_reconcile_allows_ready_issue_without_active_session(tmp_path: Path):
    upsert_issue_state(
        tmp_path,
        issue_number="42",
        state="ready",
        command_id="cmd-ready-42",
        updated_at="2026-05-07T17:00:00+08:00",
    )

    with patch(
        "scripts.autodev_project.subprocess.run",
        return_value=CompletedProcess(args=[autodev_project.resolved_python_executable()], returncode=0),
    ) as run:
        exit_code = autodev_project.main(["reconcile", "--project-root", str(tmp_path)])

    assert exit_code == 0
    command = run.call_args.args[0]
    assert command[:4] == [autodev_project.resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", "reconcile-workspace"]


def test_reconcile_watch_runs_bounded_workspace_reconcile_cycles(tmp_path: Path):
    calls: list[list[str]] = []
    sleeps: list[float] = []

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        calls.append(args)
        return CompletedProcess(args=args, returncode=0)

    def record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run), patch(
        "scripts.autodev_project.time.sleep",
        side_effect=record_sleep,
    ):
        exit_code = autodev_project.main(
            [
                "reconcile-watch",
                "--project-root",
                str(tmp_path),
                "--iterations",
                "3",
                "--interval-seconds",
                "0.25",
            ]
        )

    assert exit_code == 0
    reconcile_calls = [
        command
        for command in calls
        if command[:4]
        == [autodev_project.resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", "reconcile-workspace"]
    ]
    assert len(reconcile_calls) == 3
    assert sleeps == [0.25, 0.25]
    for command in reconcile_calls:
        assert command[:4] == [autodev_project.resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", "reconcile-workspace"]
        assert ["--base-dir", str(tmp_path)] in [command[i:i+2] for i in range(len(command)-1)]


def test_reconcile_watch_stops_on_error_when_requested(tmp_path: Path):
    calls: list[list[str]] = []

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        calls.append(args)
        return CompletedProcess(args=args, returncode=2)

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run), patch(
        "scripts.autodev_project.time.sleep",
    ) as sleep:
        exit_code = autodev_project.main(
            [
                "reconcile-watch",
                "--project-root",
                str(tmp_path),
                "--iterations",
                "3",
                "--stop-on-error",
            ]
        )

    assert exit_code == 2
    reconcile_calls = [
        command
        for command in calls
        if command[:4]
        == [autodev_project.resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", "reconcile-workspace"]
    ]
    assert len(reconcile_calls) == 1
    sleep.assert_not_called()


def test_reconcile_watch_fails_fast_when_runtime_db_missing(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")
    runtime_db = tmp_path / ".opencode/runtime/control-plane.sqlite3"
    runtime_db.parent.mkdir(parents=True, exist_ok=True)
    if runtime_db.exists():
        runtime_db.unlink()

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return completed(args, stdout="false\n")
        if args[:4] == [autodev_project.resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", "reconcile-workspace"]:
            return completed(
                args,
                returncode=1,
                stderr=(
                    f"[autodev:reconcile] control-plane-db-missing-before-command={runtime_db}\n"
                    "RuntimeError: control-plane DB missing; refusing to recreate\n"
                ),
            )
        raise AssertionError(f"unexpected command: {args}")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run), patch(
        "scripts.autodev_project.time.sleep"
    ) as sleep:
        exit_code = autodev_project.main(
            [
                "reconcile-watch",
                "--project-root",
                str(tmp_path),
                "--iterations",
                "3",
                "--stop-on-error",
            ]
        )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert f"[autodev:reconcile-watch] project-root={tmp_path}" in captured.out
    assert f"[autodev:reconcile-watch] runtime-db={runtime_db}" in captured.out
    sleep.assert_not_called()


def test_reconcile_blocks_when_runtime_db_is_tracked(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")
    write(tmp_path / ".opencode/runtime/control-plane.sqlite3", "")
    write(tmp_path / ".gitignore", "# existing\n")

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return completed(args, stdout="true\n")
        if args[:3] == ["git", "ls-files", ".opencode/runtime"]:
            return completed(args, stdout=".opencode/runtime/.gitkeep\n.opencode/runtime/control-plane.sqlite3\n")
        raise AssertionError(f"unexpected command: {args}")

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run):
        exit_code = autodev_project.main(["reconcile", "--project-root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert f"[autodev:reconcile] project-root={tmp_path}" in captured.out
    assert f"[autodev:reconcile] runtime-db={tmp_path / '.opencode/runtime/control-plane.sqlite3'}" in captured.out
    assert "tracked autodev runtime files must be removed from git index" in captured.err


def test_start_prints_path_confirmation_before_dispatch(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")
    write(tmp_path / ".opencode/runtime/control-plane.sqlite3", "")

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return completed(args, stdout="false\n")
        return CompletedProcess(args=args, returncode=0)

    with patch("scripts.autodev_project.subprocess.run", side_effect=fake_run):
        exit_code = autodev_project.main(["start", "--project-root", str(tmp_path), "--issue-number", "34"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"[autodev:start] project-root={tmp_path}" in captured.out
    assert f"[autodev:start] runtime-db={tmp_path / '.opencode/runtime/control-plane.sqlite3'}" in captured.out


def test_reconcile_allows_workspace_intake_without_preexisting_db_issue(tmp_path: Path):
    with patch(
        "scripts.autodev_project.subprocess.run",
        return_value=CompletedProcess(args=[autodev_project.resolved_python_executable()], returncode=0),
    ) as run:
        exit_code = autodev_project.main(["reconcile", "--project-root", str(tmp_path)])

    assert exit_code == 0
    command = run.call_args.args[0]
    kwargs = run.call_args.kwargs
    assert command[:4] == [autodev_project.resolved_python_executable(), "-m", "scripts.orchestrator_supervisor", "reconcile-workspace"]
    assert ["--base-dir", str(tmp_path)] in [command[i:i + 2] for i in range(len(command) - 1)]
    assert kwargs["cwd"] == tmp_path


def test_show_session_resolves_consumer_project_root_from_nested_directory(tmp_path: Path, capsys: CaptureFixture[str]):
    nested = tmp_path / "packages/app"
    nested.mkdir(parents=True)
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    with patch("scripts.autodev_project.show_latest_session", return_value={"status": "success"}):
        exit_code = autodev_project.main(["show-session", "--project-root", str(nested)])
        captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == '{"status": "success"}\n'


def test_doctor_resolves_consumer_project_root_from_nested_directory(tmp_path: Path, capsys: CaptureFixture[str]):
    nested = tmp_path / "packages/app"
    nested.mkdir(parents=True)
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")
    write(tmp_path / ".opencode/runtime/control-plane.sqlite3", "")

    exit_code = autodev_project.main(["doctor", "--project-root", str(nested)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == "autodev project: no changes needed\n"
