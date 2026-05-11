from __future__ import annotations

import json
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from pytest import CaptureFixture

from scripts import autodev_project


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_text(text, encoding="utf-8")


def completed(args: list[str], *, returncode: int = 0, stdout: str = "", stderr: str = "") -> CompletedProcess[str]:
    return CompletedProcess(args=args, returncode=returncode, stdout=stdout, stderr=stderr)


def fake_init_bootstrap_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
    if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
        return completed(args, returncode=128, stderr="not a git repository")
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
                "paulpai0412/wferp",
            ]
        )

    assert exit_code == 0
    config = read(tmp_path / ".autodev.yaml")
    assert 'schema_version: "1.0"' in config
    assert "github_repo: paulpai0412/wferp" in config
    assert "issue_packets: docs/agents/issue-packets" in config
    assert (tmp_path / "docs/agents/issue-packets").is_dir()
    assert (tmp_path / "docs/agents/handoffs").is_dir()
    assert (tmp_path / "docs/agents/runtime/context-checkpoint.yaml").exists()
    assert (tmp_path / ".opencode/runtime/.gitkeep").exists()
    assert (tmp_path / ".opencode/runtime/control-plane.sqlite3").exists()
    agents = read(tmp_path / "AGENTS.md")
    assert "Keep this project-specific guidance." in agents
    assert "<!-- AUTODEV:BEGIN -->" in agents
    assert "Do not copy workflow implementation" in agents
    commands = [call.args[0] for call in run.call_args_list]
    assert ["git", "init", "-b", "main"] in commands
    assert ["git", "remote", "add", "origin", "https://github.com/paulpai0412/wferp.git"] in commands
    assert ["gh", "repo", "create", "paulpai0412/wferp", "--private", "--description", autodev_project.DEFAULT_REPO_DESCRIPTION] in commands
    label_commands = [command for command in commands if isinstance(command, list) and command[:3] == ["gh", "label", "create"]]
    assert len(label_commands) == len(autodev_project.BOOTSTRAP_LABELS)


def test_init_dry_run_writes_nothing(tmp_path: Path):
    with patch("scripts.autodev_project.subprocess.run") as run:
        exit_code = autodev_project.main(["init", "--project-root", str(tmp_path), "--dry-run"])

    assert exit_code == 0
    assert not (tmp_path / ".autodev.yaml").exists()
    assert not (tmp_path / "docs").exists()
    run.assert_not_called()


def test_install_commands_writes_autodev_prefixed_global_commands(tmp_path: Path):
    commands_dir = tmp_path / "commands"

    exit_code = autodev_project.main(
        ["install-commands", "--commands-dir", str(commands_dir)]
    )

    assert exit_code == 0
    start_command = read(commands_dir / "autodev-start.md")
    assert "description: Start autodev workflow" in start_command
    assert "scripts/autodev_project.py start" in start_command
    assert '--issue-number "$1"' in start_command
    assert str(autodev_project.ROOT) in start_command
    assert str(tmp_path) not in start_command
    assert (commands_dir / "autodev-reconcile.md").exists()
    assert (commands_dir / "autodev-show-session.md").exists()
    assert (commands_dir / "autodev-doctor.md").exists()


def test_doctor_reports_legacy_residue(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / ".opencode/commands/auto-dev.md", "legacy command\n")

    exit_code = autodev_project.main(["doctor", "--project-root", str(tmp_path)])

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "legacy residue" in captured.out
    assert ".opencode/commands/auto-dev.md" in captured.out


def test_doctor_reports_missing_control_plane_db(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".autodev.yaml", 'schema_version: "1.0"\nproject:\n  name: demo\n')
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")

    exit_code = autodev_project.main(["doctor", "--project-root", str(tmp_path)])

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "missing .opencode/runtime/control-plane.sqlite3" in captured.out


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

    exit_code = autodev_project.main(["doctor", "--project-root", str(tmp_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "legacy residue: docs/agents/runtime/context-checkpoint.yaml" not in captured.out


def test_init_updates_origin_when_force_is_set(tmp_path: Path):
    write(tmp_path / "AGENTS.md", "# AGENTS.md\n")

    def fake_run(args: list[str], **_kwargs: object) -> CompletedProcess[str]:
        if args[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return completed(args, stdout="true\n")
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


def test_migrate_dry_run_lists_legacy_files_but_preserves_history(tmp_path: Path, capsys: CaptureFixture[str]):
    write(tmp_path / ".opencode/commands/auto-dev.md", "legacy command\n")
    write(tmp_path / "docs/agents/evidence/issue-1-pr-1.yaml", "historical evidence\n")

    exit_code = autodev_project.main(["migrate", "--project-root", str(tmp_path), "--dry-run"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "would remove .opencode/commands/auto-dev.md" in captured.out
    assert "preserve docs/agents/evidence/" in captured.out
    assert (tmp_path / ".opencode/commands/auto-dev.md").exists()
    assert (tmp_path / "docs/agents/evidence/issue-1-pr-1.yaml").exists()


def test_migrate_remove_legacy_deletes_only_legacy_files(tmp_path: Path):
    write(tmp_path / ".opencode/commands/auto-dev.md", "legacy command\n")
    write(tmp_path / "docs/agents/evidence/issue-1-pr-1.yaml", "historical evidence\n")

    exit_code = autodev_project.main(
        ["migrate", "--project-root", str(tmp_path), "--remove-legacy", "--skip-git-clean-check"]
    )

    assert exit_code == 0
    assert not (tmp_path / ".opencode/commands/auto-dev.md").exists()
    assert (tmp_path / "docs/agents/evidence/issue-1-pr-1.yaml").exists()


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
        return_value=CompletedProcess(args=["python3"], returncode=0),
    ) as run:
        exit_code = autodev_project.main(
            ["start", "--project-root", str(tmp_path), "--issue-number", "34"]
        )

    assert exit_code == 0
    command = run.call_args.args[0]
    assert str(tmp_path / "docs/agents/issue-packets/issue-34.yaml") in command
    assert str(tmp_path / "docs/agents/runtime/context-checkpoint.yaml") in command
    assert str(tmp_path / ".opencode/runtime/orchestrator-ledger.json") in command
    assert str(autodev_project.ROOT / "docs/agents/autonomous-development-workflow.yaml") in command
    assert (tmp_path / "docs/agents/runtime/context-checkpoint.yaml").exists()
