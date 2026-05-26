#!/usr/bin/env python3
"""Cross-platform autodev full-cycle loop runner."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

AUTODEV_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(AUTODEV_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(AUTODEV_PACKAGE_ROOT))

from scripts.runtime_exec import resolved_python_executable


class FullCycleRunner:
    def __init__(self) -> None:
        self.script_dir = Path(__file__).resolve().parents[1]
        self.autodev_home = Path(os.environ.get("AUTODEV_HOME", str(self.script_dir))).resolve()
        self.project_root = self._resolve_consumer_project_root(os.environ.get("PROJECT_ROOT", ""))
        self._load_consumer_autodev_yaml_settings(self.project_root)
        self._load_consumer_env(self.project_root)
        self.repo = self._resolve_repo(self.project_root)
        self.interval_seconds = int(os.environ.get("INTERVAL_SECONDS", "180") or "180")
        self.max_cycles = int(os.environ.get("MAX_CYCLES", "0") or "0")
        self.auto_approve_release = os.environ.get("AUTO_APPROVE_RELEASE", "1") == "1"
        self.auto_label_ready = os.environ.get("AUTO_LABEL_READY", "0") == "1"
        self.heartbeat_seconds = int(os.environ.get("HEARTBEAT_SECONDS", "10") or "10")
        self.resume_max_attempts = int(os.environ.get("RESUME_MAX_ATTEMPTS", "2") or "2")
        self.redispatch_max_attempts = int(os.environ.get("REDISPATCH_MAX_ATTEMPTS", "2") or "2")
        self.auto_fail_quarantined = os.environ.get("AUTO_FAIL_QUARANTINED", "1") == "1"
        self.bootstrap_done = os.environ.get("BOOTSTRAP_DONE", "0") == "1"
        self.python_exec = resolved_python_executable()

        self.autodev_project_py = self.autodev_home / "scripts" / "autodev_project.py"
        self.intake_py = self.autodev_home / "scripts" / "issue_packet_intake.py"
        self.supervisor_py = self.autodev_home / "scripts" / "orchestrator_supervisor.py"
        self.autodev_config_path = self.project_root / ".autodev.yaml"
        self.db_path = self.project_root / ".opencode/runtime/control-plane.sqlite3"
        self.state_dir = self.project_root / ".opencode/runtime/full-cycle-state"
        self.log_path = self.project_root / ".opencode/runtime/full-cycle.log"

    @staticmethod
    def _resolve_consumer_project_root(project_root: str) -> Path:
        start = Path(project_root or os.getcwd()).resolve()
        if not start.exists():
            raise SystemExit(f"project root candidate does not exist: {start}")
        current = start
        for candidate in [current, *current.parents]:
            if (candidate / ".autodev.yaml").exists():
                return candidate
        return start

    @staticmethod
    def _parse_env_file(path: Path) -> dict[str, str]:
        values: dict[str, str] = {}
        if not path.exists():
            return values
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            normalized = value.strip().strip("\"").strip("'")
            values[key.strip()] = normalized
        return values

    @staticmethod
    def _parse_runtime_mapping(root: Path, section: str) -> dict[str, str]:
        config = root / ".autodev.yaml"
        if not config.exists():
            return {}

        values: dict[str, str] = {}
        in_runtime = False
        in_section = False

        for raw_line in config.read_text(encoding="utf-8").splitlines():
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            indent = len(raw_line) - len(raw_line.lstrip(" "))
            if indent == 0:
                in_runtime = stripped == "runtime:"
                in_section = False
                continue

            if not in_runtime:
                continue

            if indent == 2 and stripped.endswith(":"):
                in_section = stripped == f"{section}:"
                continue

            if indent <= 2:
                in_section = False
                continue

            if in_section and indent >= 4 and ":" in stripped:
                key, raw_value = stripped.split(":", 1)
                values[key.strip()] = raw_value.strip()

        return values

    @staticmethod
    def _normalize_config_value(raw_value: str, *, coerce_bool: bool) -> str:
        normalized = raw_value.strip().strip("\"").strip("'")
        if not normalized:
            return ""

        if coerce_bool:
            lowered = normalized.lower()
            if lowered in {"true", "yes", "on"}:
                return "1"
            if lowered in {"false", "no", "off"}:
                return "0"

        return normalized

    def _load_consumer_autodev_yaml_settings(self, root: Path) -> None:
        for key, raw_value in self._parse_runtime_mapping(root, "env").items():
            normalized = self._normalize_config_value(raw_value, coerce_bool=False)
            if normalized:
                os.environ.setdefault(key, normalized)

        full_cycle_to_env = {
            "interval_seconds": "INTERVAL_SECONDS",
            "max_cycles": "MAX_CYCLES",
            "auto_approve_release": "AUTO_APPROVE_RELEASE",
            "auto_label_ready": "AUTO_LABEL_READY",
            "heartbeat_seconds": "HEARTBEAT_SECONDS",
            "resume_max_attempts": "RESUME_MAX_ATTEMPTS",
            "redispatch_max_attempts": "REDISPATCH_MAX_ATTEMPTS",
            "auto_fail_quarantined": "AUTO_FAIL_QUARANTINED",
            "bootstrap_done": "BOOTSTRAP_DONE",
        }

        full_cycle_values = self._parse_runtime_mapping(root, "full_cycle")
        for config_key, env_key in full_cycle_to_env.items():
            if config_key not in full_cycle_values:
                continue
            normalized = self._normalize_config_value(full_cycle_values[config_key], coerce_bool=True)
            if normalized:
                os.environ.setdefault(env_key, normalized)

    def _load_consumer_env(self, root: Path) -> None:
        env = self._parse_env_file(root / ".env")
        for key, value in env.items():
            os.environ.setdefault(key, value)

    @staticmethod
    def _repo_from_autodev_yaml(root: Path) -> str:
        config = root / ".autodev.yaml"
        if not config.exists():
            return ""
        in_project = False
        for raw_line in config.read_text(encoding="utf-8").splitlines():
            stripped = raw_line.strip()
            if not stripped:
                continue
            indent = len(raw_line) - len(raw_line.lstrip(" "))
            if indent == 0 and stripped == "project:":
                in_project = True
                continue
            if in_project and indent == 0:
                break
            if in_project and indent >= 2 and stripped.startswith("github_repo:"):
                return stripped.split(":", 1)[1].strip().strip("\"").strip("'")
        return ""

    @staticmethod
    def _repo_from_git_remote(root: Path) -> str:
        try:
            completed = subprocess.run(
                ["git", "-C", str(root), "remote", "get-url", "origin"],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError:
            return ""
        remote = (completed.stdout or "").strip()
        if "github.com" not in remote:
            return ""
        suffix = remote.split("github.com", 1)[1].lstrip(":/")
        if suffix.endswith(".git"):
            suffix = suffix[:-4]
        parts = [p for p in suffix.split("/") if p]
        if len(parts) >= 2:
            return f"{parts[0]}/{parts[1]}"
        return ""

    def _resolve_repo(self, root: Path) -> str:
        value = self._repo_from_autodev_yaml(root)
        if value:
            return value
        value = os.environ.get("AUTODEV_GITHUB_REPO", "").strip()
        if value:
            return value
        value = os.environ.get("REPO", "").strip()
        if value:
            return value
        return self._repo_from_git_remote(root)

    @staticmethod
    def _timestamp() -> str:
        return datetime.now().astimezone().isoformat(timespec="seconds")

    def log(self, message: str) -> None:
        line = f"[{self._timestamp()}] {message}"
        print(line)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def run_cmd(self, args: list[str]) -> int:
        self.log("RUN: " + " ".join(args))
        try:
            completed = subprocess.run(args, cwd=self.project_root, check=False)
        except OSError as error:
            self.log(f"WARN: command failed to execute: {error}")
            return 1
        if completed.returncode != 0:
            self.log(f"WARN: command failed (exit={completed.returncode}): {' '.join(args)}")
        return completed.returncode

    def _gh_no_pager_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["GH_PAGER"] = "cat"
        env["PAGER"] = "cat"
        env["LESS"] = "FRX"
        return env

    def _print_gh_list(self, heading: str, args: list[str], empty_message: str) -> None:
        self.log(heading)
        completed = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=False,
            env=self._gh_no_pager_env(),
        )
        output = (completed.stdout or "").strip()
        if output:
            print(output)
        else:
            print(empty_message)
        if completed.returncode != 0:
            stderr = (completed.stderr or "").strip()
            if stderr:
                self.log(f"WARN: {' '.join(args)} failed (exit={completed.returncode}): {stderr}")

    def _query_issue_numbers(self, state: str, *, require_empty_session: bool = False) -> list[str]:
        if not self.db_path.exists():
            return []
        connection = sqlite3.connect(self.db_path)
        try:
            if state == "ready" and require_empty_session:
                rows = connection.execute(
                    """
                    select issue_number
                    from issues
                    where state='ready' and current_session_id='' and rank_score >= 0
                    order by rank_score desc, cast(issue_number as integer)
                    """
                ).fetchall()
            else:
                rows = connection.execute(
                    "select issue_number from issues where state = ? order by cast(issue_number as integer)",
                    (state,),
                ).fetchall()
        finally:
            connection.close()
        return [str(row[0]) for row in rows if row and row[0] is not None]

    def require_tools(self) -> None:
        missing: list[str] = []
        if shutil.which("gh") is None:
            missing.append("gh")
        if not Path(self.python_exec).exists() and shutil.which(self.python_exec) is None:
            missing.append(self.python_exec)
        for required in [self.autodev_project_py, self.intake_py, self.supervisor_py]:
            if not required.exists():
                missing.append(str(required))
        if missing:
            raise SystemExit("missing required dependencies: " + ", ".join(missing))
        if not self.repo:
            raise SystemExit("unable to resolve GitHub repo (REPO/.env/.autodev.yaml/git remote)")

    def open_issue_numbers(self) -> list[str]:
        completed = subprocess.run(
            ["gh", "issue", "list", "--repo", self.repo, "--state", "open", "--limit", "200", "--json", "number", "--jq", ".[].number"],
            capture_output=True,
            text=True,
            check=False,
            env=self._gh_no_pager_env(),
        )
        return [line.strip() for line in (completed.stdout or "").splitlines() if line.strip()]

    def open_issue_count(self) -> int:
        completed = subprocess.run(
            ["gh", "issue", "list", "--repo", self.repo, "--state", "open", "--limit", "200", "--json", "number", "--jq", "length"],
            capture_output=True,
            text=True,
            check=False,
            env=self._gh_no_pager_env(),
        )
        try:
            return int((completed.stdout or "0").strip() or "0")
        except ValueError:
            return 0

    def first_ready_issue_number(self) -> str:
        from_db = self._query_issue_numbers("ready", require_empty_session=True)
        if from_db:
            return from_db[0]
        completed = subprocess.run(
            [
                "gh",
                "issue",
                "list",
                "--repo",
                self.repo,
                "--state",
                "open",
                "--label",
                "ready-for-agent",
                "--limit",
                "200",
                "--json",
                "number",
                "--jq",
                ".[0].number // empty",
            ],
            capture_output=True,
            text=True,
            check=False,
            env=self._gh_no_pager_env(),
        )
        return (completed.stdout or "").strip()

    def _state_file(self, issue_number: str) -> Path:
        return self.state_dir / f"issue-{issue_number}.json"

    def _read_recovery_state(self, issue_number: str) -> tuple[int, int]:
        path = self._state_file(issue_number)
        if not path.exists():
            return 0, 0
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return 0, 0
        return int(payload.get("resume_fail_count", 0) or 0), int(payload.get("redispatch_fail_count", 0) or 0)

    def _write_recovery_state(self, issue_number: str, resume_count: int, redispatch_count: int) -> None:
        path = self._state_file(issue_number)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "resume_fail_count": resume_count,
                    "redispatch_fail_count": redispatch_count,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def _reset_recovery_state(self, issue_number: str) -> None:
        path = self._state_file(issue_number)
        if path.exists():
            path.unlink()

    def _is_issue_still_quarantined(self, issue_number: str) -> bool:
        if not self.db_path.exists():
            return False
        connection = sqlite3.connect(self.db_path)
        try:
            row = connection.execute("select state from issues where issue_number=?", (issue_number,)).fetchone()
        finally:
            connection.close()
        return bool(row and str(row[0]) == "quarantined")

    def autodev_bootstrap_once(self) -> None:
        if self.bootstrap_done:
            self.log("Bootstrap already completed; skip autodev init/doctor")
            return
        self.run_cmd(
            [
                self.python_exec,
                str(self.autodev_project_py),
                "init",
                "--project-root",
                str(self.project_root),
                "--github-repo",
                self.repo,
                "--json",
            ]
        )
        self.run_cmd([self.python_exec, str(self.autodev_project_py), "doctor", "--project-root", str(self.project_root), "--json"])
        self.bootstrap_done = True

    def autodev_intake(self) -> None:
        if self.auto_label_ready:
            self.log("AUTO_LABEL_READY=1 => add ready-for-agent to all open issues")
            for issue_number in self.open_issue_numbers():
                self.run_cmd(["gh", "issue", "edit", issue_number, "--repo", self.repo, "--add-label", "ready-for-agent"])
        self.run_cmd([self.python_exec, str(self.intake_py), "--project-root", str(self.project_root), "--repo", self.repo])

    def autodev_start_one(self) -> None:
        issue = self.first_ready_issue_number()
        if not issue:
            self.log("No ready-for-agent issue found for explicit start step")
            return
        self.run_cmd(
            [
                self.python_exec,
                str(self.autodev_project_py),
                "start",
                "--project-root",
                str(self.project_root),
                "--issue-number",
                issue,
            ]
        )

    def autodev_recovery(self) -> None:
        for issue in self._query_issue_numbers("failed"):
            self.run_cmd(
                [
                    self.python_exec,
                    str(self.supervisor_py),
                    "retry-failed",
                    "--base-dir",
                    str(self.project_root),
                    "--issue-number",
                    issue,
                    "--reason",
                    "auto-recovery loop: retry failed issue",
                ]
            )

        if self.db_path.exists():
            connection = sqlite3.connect(self.db_path)
            try:
                rows = connection.execute(
                    "select issue_number from issues where state='ready' and ifnull(current_session_id,'')<>''"
                ).fetchall()
            finally:
                connection.close()
            for (issue,) in rows:
                self.run_cmd(
                    [
                        self.python_exec,
                        str(self.supervisor_py),
                        "clear-ready-session-fence",
                        "--base-dir",
                        str(self.project_root),
                        "--issue-number",
                        str(issue),
                        "--reason",
                        "auto-recovery loop: clear stale ready fence",
                    ]
                )

        for issue in self._query_issue_numbers("quarantined"):
            resume_fail_count, redispatch_fail_count = self._read_recovery_state(issue)
            if resume_fail_count < self.resume_max_attempts:
                rc = self.run_cmd(
                    [
                        self.python_exec,
                        str(self.supervisor_py),
                        "resume-quarantined",
                        "--base-dir",
                        str(self.project_root),
                        "--issue-number",
                        issue,
                        "--reason",
                        "auto-recovery loop: resume quarantined issue",
                    ]
                )
                if rc == 0 and not self._is_issue_still_quarantined(issue):
                    self._reset_recovery_state(issue)
                    continue
                resume_fail_count += 1
                self._write_recovery_state(issue, resume_fail_count, redispatch_fail_count)
                continue

            if redispatch_fail_count < self.redispatch_max_attempts:
                rc = self.run_cmd(
                    [
                        self.python_exec,
                        str(self.supervisor_py),
                        "redispatch-quarantined",
                        "--base-dir",
                        str(self.project_root),
                        "--issue-number",
                        issue,
                        "--reason",
                        "auto-recovery loop: redispatch quarantined issue",
                        "--source-session-id",
                        "full-cycle-auto-recovery",
                    ]
                )
                if rc == 0 and not self._is_issue_still_quarantined(issue):
                    self._reset_recovery_state(issue)
                    continue
                redispatch_fail_count += 1
                self._write_recovery_state(issue, resume_fail_count, redispatch_fail_count)
                continue

            if self.auto_fail_quarantined:
                self.run_cmd(
                    [
                        self.python_exec,
                        str(self.supervisor_py),
                        "fail-quarantined",
                        "--base-dir",
                        str(self.project_root),
                        "--issue-number",
                        issue,
                        "--reason",
                        "auto-recovery loop: exceeded resume/redispatch limits",
                    ]
                )
                self._reset_recovery_state(issue)

    def autodev_reconcile(self) -> None:
        self.run_cmd([self.python_exec, str(self.autodev_project_py), "reconcile", "--project-root", str(self.project_root)])

    def autodev_release_verified(self) -> None:
        for issue in self._query_issue_numbers("verified"):
            command = [
                self.python_exec,
                str(self.autodev_project_py),
                "release",
                "--project-root",
                str(self.project_root),
                "--issue-number",
                issue,
            ]
            if self.auto_approve_release:
                command.append("--auto-approve")
            self.run_cmd(command)

    def print_db_snapshot(self) -> None:
        if not self.db_path.exists():
            self.log(f"DB snapshot skipped: {self.db_path} does not exist yet")
            return
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute(
                """
                select issue_number, state, current_role, current_stage, current_status, current_session_id, updated_at
                from issues
                order by cast(issue_number as integer)
                """
            ).fetchall()
        finally:
            connection.close()
        self.log("Control-plane snapshot (issues table):")
        print(json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2))

    def print_github_snapshot(self) -> None:
        self._print_gh_list(
            "GitHub snapshot (open issues, screen output only):",
            ["gh", "issue", "list", "--repo", self.repo, "--state", "open", "--limit", "200"],
            "(no open issues)",
        )
        self._print_gh_list(
            "GitHub snapshot (open PRs, screen output only):",
            ["gh", "pr", "list", "--repo", self.repo, "--state", "open"],
            "(no open pull requests)",
        )

    def print_autodev_yaml_settings(self) -> None:
        self.log(f"Loaded consumer config from {self.autodev_config_path}")
        if not self.autodev_config_path.exists():
            print("(missing .autodev.yaml)")
            return
        content = self.autodev_config_path.read_text(encoding="utf-8").strip()
        print("----- BEGIN .autodev.yaml -----")
        if content:
            print(content)
        else:
            print("(empty .autodev.yaml)")
        print("----- END .autodev.yaml -----")

    def print_startup_github_issue_list(self) -> None:
        self._print_gh_list(
            "Startup GitHub issue list (screen output only):",
            ["gh", "issue", "list", "--repo", self.repo, "--state", "open", "--limit", "200"],
            "(no open issues)",
        )

    def sleep_with_heartbeat(self, total_seconds: int, open_count: int) -> None:
        if total_seconds <= 0:
            return
        tick = max(1, self.heartbeat_seconds)
        remaining = total_seconds
        while remaining > 0:
            step = min(tick, remaining)
            self.log(f"Heartbeat: next cycle in {remaining}s (open issues: {open_count})")
            time.sleep(step)
            remaining -= step

    def run(self) -> int:
        self.require_tools()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        self.log("Start autodev full cycle")
        self.log(f"FULL_CYCLE_LOG_PATH={self.log_path}")
        self.log(f"PROJECT_ROOT={self.project_root}")
        self.log(f"AUTODEV_HOME={self.autodev_home}")
        self.log(f"REPO={self.repo}")
        self.print_autodev_yaml_settings()
        self.print_startup_github_issue_list()

        self.autodev_bootstrap_once()

        cycle = 0
        while True:
            cycle += 1
            self.log(f"===== CYCLE {cycle} =====")

            self.autodev_intake()
            self.autodev_start_one()
            self.autodev_recovery()
            self.autodev_reconcile()
            self.autodev_release_verified()

            self.print_db_snapshot()
            self.print_github_snapshot()

            open_count = self.open_issue_count()
            self.log(f"Open issue count: {open_count}")
            if open_count == 0:
                self.log("All GitHub issues are closed. Done.")
                break
            if self.max_cycles and cycle >= self.max_cycles:
                self.log(f"Reached MAX_CYCLES={self.max_cycles}. Stop loop.")
                break
            self.log(f"Sleep {self.interval_seconds}s before next cycle")
            self.sleep_with_heartbeat(self.interval_seconds, open_count)
        return 0


def main() -> int:
    runner = FullCycleRunner()
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
